package monthlydraft

import (
	"context"
	"errors"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestRunningPreparationSelectsWithoutStoppingOrCreatingCustomerTask(t *testing.T) {
	f, cfg := preparationFixture(t)
	ctx := context.Background()
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	plan := mustGenerationPlan(t, f)
	if _, err := d.PrepareMonthlyInput(ctx, plan, f.raw, "", true); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("pre-start preparation admitted", err)
	}
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	// The external writer must remain fenced; this change never bypasses the
	// process lock by opening another deployment/connection owner.
	if _, err := PrepareGenerationInput(ctx, cfg, plan, f.raw, "", true); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("external writer unexpectedly acquired the live process lock", err)
	}
	before := map[string]string{}
	for _, path := range []string{cfg.ReceiverPath, cfg.FundingPath, cfg.AuthorityPath} {
		raw, err := os.ReadFile(path)
		if err != nil {
			t.Fatal(err)
		}
		before[path] = generationHash(raw)
	}
	preview, err := d.PrepareMonthlyInput(ctx, plan, f.raw, "", false)
	if err != nil || preview.Applied || preview.SelectionHead != "" {
		t.Fatal("running inspection failed", preview, err)
	}
	preparationCounts(t, f, 0, 0)
	for _, path := range []string{cfg.ReceiverPath, cfg.FundingPath, cfg.AuthorityPath} {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != before[path] {
			t.Fatal("running inspection changed a database", path, err)
		}
	}
	applied, err := d.PrepareMonthlyInput(ctx, plan, f.raw, "", true)
	if err != nil || !applied.Applied || !applied.Changed || applied.SelectionHead == "" || applied.PlanSHA256 != preview.PlanSHA256 {
		t.Fatal("running selection failed", applied, err)
	}
	preparationCounts(t, f, 1, 1)
	if d.runtime.Phase() != "running" {
		t.Fatal("preparation stopped the service")
	}
	state, err := d.ReadMonthly(ctx, 1, 1, "2026-09")
	if err != nil || state.State != "not_requested" {
		t.Fatal("selection pretended to have generated or published a bill", state, err)
	}
	if _, err := d.PrepareMonthlyInput(ctx, plan, f.raw, "", true); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal("stale selection head accepted", err)
	}
	again, err := d.PrepareMonthlyInput(ctx, plan, f.raw, applied.SelectionHead, true)
	if err != nil || !again.Applied || again.Changed || again.SelectionHead != applied.SelectionHead {
		t.Fatal("idempotent preparation changed the selection", again, err)
	}
	preparationCounts(t, f, 1, 1)
	// Only an explicit customer-generation call starts a task. Keep the original
	// per-account financial oracle; selecting in a live process must not change
	// the calculated amounts or make an unreviewed draft a customer publication.
	if _, err := d.SubmitMonthly(ctx, 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	job, err := d.g.tasks.Find(ctx, generationRequest(f.plan, plan))
	if err != nil {
		t.Fatal(err)
	}
	waitLocalDraft(t, d.g, job)
	if err := d.runtime.Stop(ctx); err != nil {
		t.Fatal(err)
	}
	artifact, err := d.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	if _, err := d.PrepareMonthlyInput(ctx, plan, f.raw, applied.SelectionHead, true); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("stopped service accepted preparation", err)
	}
	for _, path := range []string{cfg.FundingPath, cfg.AuthorityPath} {
		raw, err := os.ReadFile(path)
		if err != nil || generationHash(raw) != before[path] {
			t.Fatal("preparation/generation changed source or authorization", path, err)
		}
	}
}

func TestRunningPreparationRetainsAllFinancialAndPermissionGuards(t *testing.T) {
	for _, kind := range []string{"scope", "permission", "funding", "money", "writer_failure", "canceled", "busy"} {
		t.Run(kind, func(t *testing.T) {
			f, cfg := preparationFixture(t)
			ctx := context.Background()
			if kind == "scope" {
				cfg.Rollout.Targets = []GenerationTarget{{GroupID: 2, Month: "2026-09"}}
			}
			d, err := OpenDeployment(ctx, cfg)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { closeDeploymentTest(t, d) })
			if err := d.Start(ctx); err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "permission":
				db := generationDB(t, cfg.AuthorityPath, false)
				if _, err := db.Exec(`UPDATE customer_groups SET portal_auth_ver=2 WHERE id=1`); err != nil {
					t.Fatal(err)
				}
			case "funding":
				f.plan.Windows[0].NewerHeadID = generationHash([]byte("changed synthetic funding"))
			case "money":
				f.input.Accounts[0].Movements[0].DeltaQuota++
				f.raw = draftJSON(t, f.input)
				f.plan.PreparationSHA256 = generationHash(f.raw)
			case "writer_failure":
				if _, err := f.receiver.Exec(`CREATE TRIGGER running_prepare_failure BEFORE INSERT ON portal_monthly_generation_selections BEGIN SELECT RAISE(ABORT,'synthetic write failure'); END`); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			case "busy":
				generationSlots <- struct{}{}
				defer func() { <-generationSlots }()
			}
			plan := mustGenerationPlan(t, f)
			if _, err := d.PrepareMonthlyInput(ctx, plan, f.raw, "", true); err == nil {
				t.Fatal("unsafe running preparation accepted", kind)
			}
			preparationCounts(t, f, 0, 0)
			if d.runtime.Phase() != "running" {
				t.Fatal("a rejected preparation stopped the service", kind)
			}
			if kind == "writer_failure" {
				if _, err := f.receiver.Exec(`DROP TRIGGER running_prepare_failure`); err != nil {
					t.Fatal(err)
				}
				if result, err := d.PrepareMonthlyInput(context.Background(), plan, f.raw, "", true); err != nil || !result.Applied {
					t.Fatal("write failure could not recover without a restart", result, err)
				}
			}
		})
	}
}

func TestRunningPreparationStopCancelsAndDrainsBeforeClosingHandles(t *testing.T) {
	_, cfg := preparationFixture(t)
	d, err := OpenDeployment(context.Background(), cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if err := d.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	ctx, done, err := d.runtime.beginPreparation(context.Background(), 1, "2026-09")
	if err != nil {
		t.Fatal(err)
	}
	defer done()
	if view, err := d.ReadMonthly(context.Background(), 1, 1, "2026-09"); err != nil || view.State != "not_prepared" {
		t.Fatal("active preparation blocked independent metadata reads", view, err)
	}
	closing, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if err := d.Close(closing); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal("shutdown ignored active preparation", err)
	}
	if !errors.Is(ctx.Err(), context.Canceled) {
		t.Fatal("shutdown did not cancel preparation")
	}
	if err := d.receiver.Ping(); err != nil {
		t.Fatal("shutdown deadline closed active receiver", err)
	}
	if _, err := OpenDeployment(context.Background(), cfg); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("shutdown timeout released the live process lock", err)
	}
	if _, _, err := d.runtime.beginPreparation(context.Background(), 1, "2026-09"); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("stopping runtime admitted more preparation", err)
	}
}

func TestRunningPreparationConcurrentSelectionIsBoundedAndAtomic(t *testing.T) {
	f, cfg := preparationFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	plan := mustGenerationPlan(t, f)
	start := make(chan struct{})
	results := make(chan error, 12)
	var wg sync.WaitGroup
	for range 12 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			_, err := d.PrepareMonthlyInput(ctx, plan, f.raw, "", true)
			results <- err
		}()
	}
	close(start)
	wg.Wait()
	close(results)
	succeeded := 0
	for err := range results {
		if err == nil {
			succeeded++
		} else if !errors.Is(err, ErrGenerationBusy) && !errors.Is(err, ErrGenerationSelectionConflict) {
			t.Fatal("unexpected concurrent selection error", err)
		}
	}
	if succeeded != 1 {
		t.Fatal("concurrent preparation duplicated or lost admission", succeeded)
	}
	preparationCounts(t, f, 1, 1)
	if d.runtime.Phase() != "running" {
		t.Fatal("concurrent preparation stopped service")
	}
}

func TestRunningPreparationRejectsMalformedAndReadOnlyCalls(t *testing.T) {
	f, cfg := preparationFixture(t)
	ctx := context.Background()
	var absent *Deployment
	if _, err := absent.PrepareMonthlyInput(ctx, nil, nil, "", false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("nil deployment accepted")
	}
	unscoped := &LocalRuntime{phase: "running", ctx: ctx}
	if _, _, err := unscoped.beginPreparation(ctx, 1, "2026-09"); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("legacy unscoped runtime silently enabled preparation", err)
	}
	d, err := OpenDeployment(ctx, cfg)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, d) })
	if err := d.Start(ctx); err != nil {
		t.Fatal(err)
	}
	plan := mustGenerationPlan(t, f)
	for _, bad := range [][]byte{nil, []byte(`{}`), append([]byte(`{"unknown":1,`), plan[1:]...)} {
		if _, err := d.PrepareMonthlyInput(ctx, bad, f.raw, "", true); err == nil {
			t.Fatal("noncanonical plan accepted")
		}
	}
	if _, err := d.PrepareMonthlyInput(ctx, plan, f.raw, strings.Repeat("z", 64), true); err == nil {
		t.Fatal("malformed selection head accepted")
	}
	readonly, err := openDeployment(ctx, cfg, true)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { closeDeploymentTest(t, readonly) })
	if _, err := readonly.PrepareMonthlyInput(ctx, plan, f.raw, "", true); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("read-only deployment acquired preparation writes", err)
	}
	preparationCounts(t, f, 0, 0)
}
