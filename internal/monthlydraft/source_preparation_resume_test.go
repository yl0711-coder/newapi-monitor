package monthlydraft

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

type preparationResumeFixture struct {
	f   generationFixture
	r   *LocalRuntime
	p   *onDemandPreparation
	now time.Time
	req SourcePreparationRequest
}

func failedResumeFixture(t *testing.T) *preparationResumeFixture {
	t.Helper()
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) {
		return false, errors.New("synthetic temporary source outage")
	})
	x := &preparationResumeFixture{f: f, r: r, p: r.preparation, now: time.Now().Add(-10 * time.Minute), req: SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}}
	x.p.now = func() time.Time { return x.now }
	if _, err := x.p.SubmitMonthly(t.Context(), 1, 1, x.req.Month); err != nil {
		t.Fatal(err)
	}
	failResumeEpoch(t, x)
	return x
}

func failResumeEpoch(t *testing.T, x *preparationResumeFixture) {
	t.Helper()
	for range 3 {
		if err := x.p.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		x.now = x.now.Add(31 * time.Second)
	}
	state, _, failures := preparationState(t, x.f.receiver)
	if state != "failed" || failures != 3 {
		t.Fatal(state, failures)
	}
}

func resumeReceiptCount(t *testing.T, db *sql.DB) int {
	t.Helper()
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM portal_monthly_source_resumptions`).Scan(&n); err != nil {
		t.Fatal(err)
	}
	return n
}

func TestPreparationResumePreviewCooldownAndImmutableAudit(t *testing.T) {
	x := failedResumeFixture(t)
	before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
	if err != nil {
		t.Fatal(err)
	}
	changes := 0
	if err := x.f.receiver.QueryRow(`SELECT total_changes()`).Scan(&changes); err != nil {
		t.Fatal(err)
	}
	plan, err := x.p.previewResume(t.Context(), x.req)
	if err != nil || plan.Ordinal != 1 || plan.ParentSHA256 != "" {
		t.Fatal(plan, err)
	}
	var afterChanges int
	if err := x.f.receiver.QueryRow(`SELECT total_changes()`).Scan(&afterChanges); err != nil || afterChanges != changes || resumeReceiptCount(t, x.f.receiver) != 0 {
		t.Fatal("preview wrote or queued work", err)
	}
	if out, err := x.p.applyResume(t.Context(), plan); !errors.Is(err, ErrPreparationResumeCooldown) || out.Applied {
		t.Fatal("cooldown bypassed", out, err)
	}
	x.now = time.UnixMilli(plan.EarliestMS)
	out, err := x.p.applyResume(t.Context(), plan)
	if err != nil || !out.Applied || out.AlreadyApplied || !generationHex(out.ReceiptSHA256, 32) {
		t.Fatal(out, err)
	}
	after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
	if err != nil || after.State != "queued" || after.Failures != 0 || after.Steps != before.Steps || after.CreatedMS != before.CreatedMS || resumeReceiptCount(t, x.f.receiver) != 1 {
		t.Fatal("resume reset age/work or omitted receipt", after, err)
	}
	receipts, hashes, err := loadPreparationResumptions(t.Context(), x.f.receiver, x.req, x.now)
	if err != nil || len(receipts) != 1 || receipts[0].Failure != before || hashes[0] != out.ReceiptSHA256 {
		t.Fatal("original failure audit lost", err)
	}
	for _, q := range []string{`UPDATE portal_monthly_source_resumptions SET sha256=sha256`, `DELETE FROM portal_monthly_source_resumptions`} {
		if _, err := x.f.receiver.Exec(q); err == nil {
			t.Fatal("receipt mutable")
		}
	}
}

func TestPreparationResumeReplayNeverResetsActiveFailuresAndLifetimeStops(t *testing.T) {
	x := failedResumeFixture(t)
	created := int64(0)
	if err := x.f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	for ordinal := 1; ordinal <= 2; ordinal++ {
		plan, err := x.p.previewResume(t.Context(), x.req)
		if err != nil || plan.Ordinal != ordinal {
			t.Fatal(plan, err)
		}
		x.now = time.UnixMilli(plan.EarliestMS)
		first, err := x.p.applyResume(t.Context(), plan)
		if err != nil {
			t.Fatal(err)
		}
		if err := x.p.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		for range 3 {
			out, err := x.p.applyResume(t.Context(), plan)
			if err != nil || !out.AlreadyApplied || out.ReceiptSHA256 != first.ReceiptSHA256 {
				t.Fatal("replay was not acknowledgment only", out, err)
			}
			if _, err := x.p.SubmitMonthly(t.Context(), 1, 1, x.req.Month); err != nil {
				t.Fatal(err)
			}
			state, steps, failures := preparationState(t, x.f.receiver)
			if state != "queued" || steps != ordinal*3+1 || failures != 1 {
				t.Fatal("replay/customer POST reset active budget", state, steps, failures)
			}
		}
		for range 2 {
			x.now = x.now.Add(31 * time.Second)
			if err := x.p.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
		}
		x.now = x.now.Add(31 * time.Second)
	}
	x.now = x.now.Add(2 * time.Minute)
	if _, err := x.p.previewResume(t.Context(), x.req); !errors.Is(err, ErrPreparationResumeDenied) {
		t.Fatal("third activation admitted", err)
	}
	state, steps, failures := preparationState(t, x.f.receiver)
	if state != "failed" || steps != 9 || failures != 3 || resumeReceiptCount(t, x.f.receiver) != 2 {
		t.Fatal(state, steps, failures)
	}
	var after int64
	if err := x.f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation`).Scan(&after); err != nil || after != created {
		t.Fatal("24h budget origin changed", err)
	}
}

func TestPreparationResumeRejectsUnsafeFailuresAndPreservesRows(t *testing.T) {
	for _, scenario := range []string{"money", "integrity", "authority", "budget", "descriptor", "not_exhausted", "running", "lease", "age", "steps", "future_next", "selected", "cancel"} {
		t.Run(scenario, func(t *testing.T) {
			x := failedResumeFixture(t)
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			exec := func(q string, args ...any) {
				if _, err := x.f.receiver.Exec(q, args...); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "money", "integrity":
				exec(`UPDATE portal_monthly_source_preparation SET failure_code='needs_review'`)
			case "authority":
				exec(`UPDATE portal_monthly_source_preparation SET failure_code='state_changed'`)
			case "budget":
				exec(`UPDATE portal_monthly_source_preparation SET failure_code='budget_exhausted'`)
			case "descriptor":
				exec(`UPDATE portal_monthly_source_preparation SET failure_code='invalid_descriptor'`)
			case "not_exhausted":
				exec(`UPDATE portal_monthly_source_preparation SET failures=1`)
			case "running":
				exec(`UPDATE portal_monthly_source_preparation SET state='running'`)
			case "lease":
				exec(`UPDATE portal_monthly_source_preparation SET lease='retained-active-lease',lease_until=?`, x.now.Add(time.Minute).UnixMilli())
			case "age":
				x.now = x.now.Add(25 * time.Hour)
			case "steps":
				exec(`UPDATE portal_monthly_source_preparation SET steps=4096`)
			case "future_next":
				exec(`UPDATE portal_monthly_source_preparation SET next_at=?`, int64(math.MaxInt64))
			case "selected":
				if _, err := StageGenerationVersion(ctx, x.f.receiver, x.f.plan, x.f.raw, ""); err != nil {
					t.Fatal(err)
				}
			case "cancel":
				cancel()
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil {
				t.Fatal(err)
			}
			if plan, err := x.p.previewResume(ctx, x.req); err == nil || plan.FailureSHA256 != "" {
				t.Fatal("unsafe task resumable", scenario, plan, err)
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil || after != before || resumeReceiptCount(t, x.f.receiver) != 0 {
				t.Fatal("rejected preview mutated job", err)
			}
		})
	}
}

func TestPreparationResumeCASAndAtomicFailureDoNotLeaveReceipt(t *testing.T) {
	for _, scenario := range []string{"stale", "receipt_write", "queue_write", "active_group", "active_limit", "forged_plan", "forged_parent"} {
		t.Run(scenario, func(t *testing.T) {
			x := failedResumeFixture(t)
			plan, err := x.p.previewResume(t.Context(), x.req)
			if err != nil {
				t.Fatal(err)
			}
			x.now = time.UnixMilli(plan.EarliestMS)
			switch scenario {
			case "stale":
				_, err = x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=steps+1`)
			case "receipt_write":
				_, err = x.f.receiver.Exec(`CREATE TRIGGER fail_resume_receipt BEFORE INSERT ON portal_monthly_source_resumptions BEGIN SELECT RAISE(ABORT,'synthetic receipt failure'); END`)
			case "queue_write":
				_, err = x.f.receiver.Exec(`CREATE TRIGGER fail_resume_queue BEFORE UPDATE ON portal_monthly_source_preparation BEGIN SELECT RAISE(ABORT,'synthetic queue failure'); END`)
			case "active_group", "active_limit":
				n := 1
				if scenario == "active_limit" {
					n = 8
				}
				for i := 0; i < n; i++ {
					gid := int64(1)
					if n == 8 {
						gid = int64(100 + i)
					}
					raw := []byte(`{}`)
					_, err = x.f.receiver.Exec(`INSERT INTO portal_monthly_source_preparation(key,group_id,payload,state,created_at,next_at) VALUES(?,?,?,'queued',?,?)`, generationHash([]byte(fmt.Sprintf("other active %d", i))), gid, raw, x.now.UnixMilli(), x.now.UnixMilli())
					if err != nil {
						t.Fatal(err)
					}
				}
			case "forged_plan":
				plan.EarliestMS--
			case "forged_parent":
				plan.ParentSHA256 = generationHash([]byte("forged receipt parent"))
			}
			if err != nil {
				t.Fatal(err)
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil {
				t.Fatal(err)
			}
			if out, err := x.p.applyResume(t.Context(), plan); err == nil || out.Applied || out.ReceiptSHA256 != "" {
				t.Fatal("unsafe activation committed", scenario, out, err)
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil || after != before || resumeReceiptCount(t, x.f.receiver) != 0 {
				t.Fatal("half activation remained", err)
			}
		})
	}
}

func TestPreparationResumeConcurrentReplayAndRestartAudit(t *testing.T) {
	x := failedResumeFixture(t)
	plan, err := x.p.previewResume(t.Context(), x.req)
	if err != nil {
		t.Fatal(err)
	}
	x.now = time.UnixMilli(plan.EarliestMS)
	var wg sync.WaitGroup
	results := make(chan error, 6)
	for range 6 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := x.p.applyResume(t.Context(), plan); results <- err }()
	}
	wg.Wait()
	close(results)
	accepted := 0
	for err := range results {
		if err == nil {
			accepted++
		} else if !errors.Is(err, ErrGenerationSelectionConflict) && !errors.Is(err, ErrPreparationResumeDenied) {
			t.Fatal("unexpected concurrent result", err)
		}
	}
	if accepted == 0 || resumeReceiptCount(t, x.f.receiver) != 1 {
		t.Fatal("duplicate/lost audit", accepted)
	}
	// New runtime shares retained receiver but re-installs no new source and
	// must recognize the exact old activation without resetting state.
	r, err := NewControlledLocalRuntime(x.f.g, x.p.auth, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: x.req.Month}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnableOnDemandPreparation(t.Context(), x.p.auth, x.p.admit, x.p.step); err != nil {
		t.Fatal(err)
	}
	r.preparation.now = x.p.now
	out, err := r.preparation.applyResume(t.Context(), plan)
	if err != nil || !out.AlreadyApplied {
		t.Fatal("restart lost idempotent audit", out, err)
	}
	if state, steps, failures := preparationState(t, x.f.receiver); state != "queued" || steps != 3 || failures != 0 {
		t.Fatal(state, steps, failures)
	}
}

func TestPreparationResumeReceiptCorruptionIsNotSilentlyRepaired(t *testing.T) {
	x := failedResumeFixture(t)
	plan, err := x.p.previewResume(t.Context(), x.req)
	if err != nil {
		t.Fatal(err)
	}
	x.now = time.UnixMilli(plan.EarliestMS)
	if _, err := x.p.applyResume(t.Context(), plan); err != nil {
		t.Fatal(err)
	}
	var raw []byte
	if err := x.f.receiver.QueryRow(`SELECT payload FROM portal_monthly_source_resumptions`).Scan(&raw); err != nil {
		t.Fatal(err)
	}
	var receipt SourcePreparationResumeReceipt
	if json.Unmarshal(raw, &receipt) != nil {
		t.Fatal("receipt invalid")
	}
	receipt.Failure.Steps++
	raw, _ = json.Marshal(receipt)
	if _, err := x.f.receiver.Exec(`DROP TRIGGER source_resumption_no_update; UPDATE portal_monthly_source_resumptions SET payload=?`, raw); err != nil {
		t.Fatal(err)
	}
	if _, err := x.p.applyResume(t.Context(), plan); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("tampered receipt accepted", err)
	}
}

func TestPreparationResumePartialRestoreCannotRestartLifetimeBudget(t *testing.T) {
	for _, scenario := range []string{"orphan_audit", "reset_steps", "reset_creation"} {
		t.Run(scenario, func(t *testing.T) {
			x := failedResumeFixture(t)
			plan, err := x.p.previewResume(t.Context(), x.req)
			if err != nil {
				t.Fatal(err)
			}
			x.now = time.UnixMilli(plan.EarliestMS)
			if _, err := x.p.applyResume(t.Context(), plan); err != nil {
				t.Fatal(err)
			}
			q := `DELETE FROM portal_monthly_source_preparation`
			if scenario == "reset_steps" {
				q = `UPDATE portal_monthly_source_preparation SET steps=0`
			}
			if scenario == "reset_creation" {
				q = `UPDATE portal_monthly_source_preparation SET created_at=created_at+1`
			}
			if _, err := x.f.receiver.Exec(q); err != nil {
				t.Fatal(err)
			}
			if out, err := x.p.applyResume(t.Context(), plan); !errors.Is(err, ErrGenerationIntegrity) || out.Applied {
				t.Fatal("inconsistent task acknowledged", out, err)
			}
			if scenario == "orphan_audit" {
				if _, err := x.p.SubmitMonthly(t.Context(), 1, 1, x.req.Month); err == nil {
					t.Fatal("customer resubmission recreated an audited task")
				}
				var count int
				if err := x.f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&count); err != nil || count != 0 {
					t.Fatal("recreated task", count, err)
				}
			}
			if resumeReceiptCount(t, x.f.receiver) != 1 {
				t.Fatal("original receipt was changed")
			}
		})
	}
}

type capturedResumeFixture struct {
	f   capturedHistoryFixture
	d   *Deployment
	now time.Time
}

func newCapturedResumeFixture(t *testing.T, bound bool) *capturedResumeFixture {
	t.Helper()
	f := newCapturedHistoryFixture(t)
	if bound {
		syntheticCapturedRecovery(t, &f)
	}
	d := f.open(t)
	_, ref := capturedPreparationConfigFixture(t, f)
	if err := d.enableCapturedSourcePreparation(t.Context(), "captured-scope", ref, capturedPreparationTestOpener(f)); err != nil {
		t.Fatal(err)
	}
	x := &capturedResumeFixture{f: f, d: d, now: time.Now().Add(-10 * time.Minute)}
	p := d.runtime.preparation
	p.now = func() time.Time { return x.now }
	actual := p.step
	p.step = func(context.Context, SourcePreparationRequest) (bool, error) {
		return false, errors.New("synthetic transient source outage")
	}
	if _, err := p.SubmitMonthly(t.Context(), f.req.Owner.GroupID, f.req.Owner.AuthVersion, f.req.Month); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		if err := p.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		x.now = x.now.Add(31 * time.Second)
	}
	p.step = actual
	return x
}

func TestPreparationResumeDeploymentLifecycleAuthorityAndBoundRoots(t *testing.T) {
	for _, scenario := range []string{"running", "stopped", "closed", "read_only", "partial_install", "authority", "bound_root"} {
		t.Run(scenario, func(t *testing.T) {
			x := newCapturedResumeFixture(t, scenario == "bound_root")
			if scenario == "bound_root" {
				if _, err := x.d.PreviewSourcePreparationResume(t.Context(), 1, 1, x.f.req.Month); !errors.Is(err, ErrPreparationResumeDenied) {
					t.Fatal("bound root got queue-only activation", err)
				}
				return
			}
			plan, err := x.d.PreviewSourcePreparationResume(t.Context(), 1, 1, x.f.req.Month)
			if err != nil {
				t.Fatal(err)
			}
			x.now = time.UnixMilli(plan.EarliestMS)
			switch scenario {
			case "running":
				if err := x.d.Start(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "stopped":
				if err := x.d.runtime.Stop(t.Context()); err != nil {
					t.Fatal(err)
				}
			case "closed":
				closeDeploymentTest(t, x.d)
			case "read_only":
				x.d.readOnly = true
			case "partial_install":
				x.d.preparationInstallFailed = true
			case "authority":
				if err := x.f.auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
					t.Fatal(err)
				}
			}
			if _, err := x.d.PreviewSourcePreparationResume(t.Context(), 1, 1, x.f.req.Month); err == nil {
				t.Fatal("unsafe deployment preview allowed", scenario)
			}
			if out, err := x.d.ResumeSourcePreparation(t.Context(), plan); err == nil || out.Applied {
				t.Fatal("unsafe deployment resumed", scenario, out, err)
			}
			if resumeReceiptCount(t, x.f.f.receiver) != 0 {
				t.Fatal("denied deployment wrote receipt")
			}
		})
	}
}

func TestPreparationResumeCapturedQueueActuallyPublishesAndDownloadsExactMoney(t *testing.T) {
	x := newCapturedResumeFixture(t, false)
	plan, err := x.d.PreviewSourcePreparationResume(t.Context(), 1, 1, x.f.req.Month)
	if err != nil {
		t.Fatal(err)
	}
	x.now = time.UnixMilli(plan.EarliestMS)
	out, err := x.d.ResumeSourcePreparation(t.Context(), plan)
	if err != nil || !out.Applied {
		t.Fatal(out, err)
	}
	x.d.runtime.preparation.now = time.Now
	request, own, foreign := capturedHistoryHTTP(t, x.f, x.d)
	if err := x.d.Start(t.Context()); err != nil {
		t.Fatal(err)
	}
	if got := request("POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, own); got.Code != 202 {
		t.Fatal("customer could not observe/resubmit resumed request", got.Code, got.Body.String())
	}
	deadline := time.Now().Add(20 * time.Second)
	var record monitor.PortalMonthlyBill
	for {
		err := x.f.auth.First(&record, "group_id=? AND month=?", x.f.req.Owner.GroupID, x.f.req.Month).Error
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			state, steps, failures := preparationState(t, x.f.f.receiver)
			t.Fatal("resumed task never published", err, state, steps, failures)
		}
		time.Sleep(20 * time.Millisecond)
	}
	closeDeploymentTest(t, x.d)
	assertCapturedHistoryCustomerDownload(t, request, own, foreign)
	var evidence monitor.MonthlyBillEvidence
	if json.Unmarshal([]byte(record.Evidence), &evidence) != nil || len(evidence.Users) != 2 {
		t.Fatal("invalid published evidence")
	}
	for _, u := range evidence.Users {
		if u.OpeningQuota != 500000000 || u.ClosingQuota != 499000000 || u.ConsumeQuota != 1500000 || u.RefundQuota != 500000 || u.TokenUsage == nil || len(u.TokenUsage.Rows) != 3 {
			t.Fatal("resume changed money or bypassed token audit", u.UserID)
		}
	}
	var after monitor.PortalMonthlyBill
	if err := x.f.auth.First(&after, "group_id=? AND month=?", record.GroupID, record.Month).Error; err != nil || after != record {
		t.Fatal("reading changed frozen bill", err)
	}
	if resumeReceiptCount(t, x.f.f.receiver) != 1 {
		t.Fatal("missing recovery audit")
	}
}
