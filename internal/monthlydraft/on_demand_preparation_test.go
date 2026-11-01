package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
)

func onDemandFixture(t *testing.T, step SourcePreparationStep) (generationFixture, *gorm.DB, *gorm.DB, *LocalRuntime) {
	t.Helper()
	f := selectionFixture(t)
	auth, ro, _ := usageAuthorityFixture(t)
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	selector, err := GenerationSelector(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent, err = NewUsageGenerationGuard(ro, selector)
	if err != nil {
		t.Fatal(err)
	}
	r, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: f.plan.Funding.Month}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r.EnableOnDemandPreparation(context.Background(), ro, func(_ context.Context, req SourcePreparationRequest) error {
		if req.Owner != owner || req.Month != f.plan.Funding.Month {
			return ErrGenerationInput
		}
		return nil
	}, step); err != nil {
		t.Fatal(err)
	}
	return f, auth, ro, r
}

func preparationState(t *testing.T, db *sql.DB) (string, int, int) {
	t.Helper()
	var state string
	var steps, failures int
	if err := db.QueryRow(`SELECT state,steps,failures FROM portal_monthly_source_preparation ORDER BY key LIMIT 1`).Scan(&state, &steps, &failures); err != nil {
		t.Fatal(err)
	}
	return state, steps, failures
}

func TestOnDemandPreparationNoManualSelectionJourney(t *testing.T) {
	var calls atomic.Int32
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	r.preparation.step = func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		calls.Add(1)
		if req.Owner != f.plan.Owner || req.Month != f.plan.Funding.Month {
			return false, ErrGenerationChanged
		}
		// One connection proves source work holds no receiver transaction.
		var n int
		if err := f.receiver.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_generation_selections`).Scan(&n); err != nil {
			return false, err
		}
		if n != 0 {
			return false, ErrGenerationIntegrity
		}
		_, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, "")
		return err == nil, err
	}
	f.receiver.SetMaxOpenConns(1)
	ctx := context.Background()
	view, err := r.preparation.ReadMonthly(ctx, 1, 1, f.plan.Funding.Month)
	if err != nil || view.State != "not_requested" {
		t.Fatal(view, err)
	}
	var n int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&n); err != nil || n != 0 {
		t.Fatal("status created work", n, err)
	}
	if err := r.Start(ctx); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	view, err = r.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month)
	if err != nil {
		t.Fatal(view, err)
	}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		view, err = r.ReadMonthly(ctx, 1, 1, f.plan.Funding.Month)
		if err != nil {
			t.Fatal(err)
		}
		if view.State == "pending_verification" {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if view.State != "pending_verification" || calls.Load() != 1 {
		t.Fatal("missing-source request did not reach existing draft verification", view, calls.Load())
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "submitted" || steps != 1 || failures != 0 {
		t.Fatal(state, steps, failures)
	}
	// Synthetic source preparation must never masquerade as publication.
	if view.State == "available" {
		t.Fatal("draft advertised as a bill")
	}
	selected, err := ReadGenerationSelection(ctx, f.receiver, 1, f.plan.Funding.Month)
	if err != nil || selected.Request == nil {
		t.Fatal(selected, err)
	}
	job, err := f.g.tasks.Find(ctx, *selected.Request)
	if err != nil {
		t.Fatal(err)
	}
	// DraftReady may be committed just before the worker releases its run slot.
	// Join the actual owner before deliberately calling the private Run API;
	// a customer status read is not proof that the goroutine has drained.
	stopRuntime(t, r)
	artifact, err := f.g.Run(ctx, job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
}

func TestOnDemandPreparationFirstClickAdmissionIsReadOnlyAndCannotBypassChanges(t *testing.T) {
	for _, scenario := range []string{"allowed", "policy_rejected", "outside_scope", "revoked_during_admission", "withdrawn_during_admission", "selected_during_admission", "already_selected", "cancelled_during_admission"} {
		t.Run(scenario, func(t *testing.T) {
			work := 0
			f, auth, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { work++; return false, nil })
			p := r.preparation
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			if scenario == "already_selected" {
				if _, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, ""); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "outside_scope" {
				p.scope = &generationScope{exact: map[GenerationTarget]struct{}{{GroupID: 2, Month: f.plan.Funding.Month}: {}}}
			}
			admitted := 0
			p.admit = func(ctx context.Context, req SourcePreparationRequest) error {
				admitted++
				if req.Owner != f.plan.Owner || req.Month != f.plan.Funding.Month {
					t.Fatal("unexpected admission scope")
				}
				switch scenario {
				case "policy_rejected", "already_selected":
					return ErrGenerationSelectionMissing
				case "revoked_during_admission":
					return auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error
				case "withdrawn_during_admission", "selected_during_admission":
					selected, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, "")
					if err == nil && scenario == "withdrawn_during_admission" {
						_, err = WithdrawGenerationSelection(ctx, f.receiver, req.Owner.GroupID, req.Month, selected.Head)
					}
					return err
				case "cancelled_during_admission":
					cancel()
				}
				return nil
			}
			view, err := p.ReadMonthly(ctx, 1, 1, f.plan.Funding.Month)
			switch scenario {
			case "allowed", "selected_during_admission", "already_selected":
				if err != nil || view.State != "not_requested" {
					t.Fatal("valid first click not available", view, err)
				}
			case "withdrawn_during_admission":
				if err != nil || view.State != "needs_preparation" {
					t.Fatal("withdrawal advertised a new first request", view, err)
				}
			default:
				if err == nil || view.State != "" {
					t.Fatal("unsupported/revoked scope offered generation", view, err)
				}
			}
			wantAdmissions := 1
			if scenario == "outside_scope" || scenario == "already_selected" {
				wantAdmissions = 0
			}
			if admitted != wantAdmissions || work != 0 {
				t.Fatal("status executed source work or skipped its policy", admitted, work)
			}
			var jobs, tasks int
			if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&jobs); err != nil {
				t.Fatal(err)
			}
			if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_draft_tasks`).Scan(&tasks); err != nil || jobs != 0 || tasks != 0 {
				t.Fatal("first status read queued work", jobs, tasks, err)
			}
		})
	}
}

func TestOnDemandPreparationDedupResumeAndNoRetryBudgetReset(t *testing.T) {
	ctx := context.Background()
	f, _, ro, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) {
		return false, errors.New("private source failure")
	})
	p := r.preparation
	now := time.Now()
	p.now = func() time.Time { return now }
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for range 8 {
		wg.Add(1)
		go func() { defer wg.Done(); _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); errs <- err }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	var n int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_source_preparation`).Scan(&n); err != nil || n != 1 {
		t.Fatal(n, err)
	}
	// Construct a new owner against the retained receiver, without re-enqueueing.
	r2, err := NewControlledLocalRuntime(f.g, ro, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: f.plan.Funding.Month}}})
	if err != nil {
		t.Fatal(err)
	}
	if err := r2.EnableOnDemandPreparation(ctx, ro, p.admit, p.step); err != nil {
		t.Fatal(err)
	}
	p = r2.preparation
	p.now = func() time.Time { return now }
	for i := 1; i <= 3; i++ {
		if err := p.runOne(ctx); err != nil {
			t.Fatal(err)
		}
		state, steps, failures := preparationState(t, f.receiver)
		if failures != i || steps != i || (i < 3 && state != "queued") || (i == 3 && state != "failed") {
			t.Fatal(state, steps, failures)
		}
		if _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
			t.Fatal(err)
		}
		now = now.Add(31 * time.Second)
	}
	if err := p.runOne(ctx); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "failed" || steps != 3 || failures != 3 {
		t.Fatal("repeated submission reset failures", state, steps, failures)
	}
	view, err := p.ReadMonthly(ctx, 1, 1, f.plan.Funding.Month)
	if err != nil || view.State != "needs_review" {
		t.Fatal(view, err)
	}
}

func TestOnDemandPreparationReadyRequiresSelectedEvidence(t *testing.T) {
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { return true, nil })
	p := r.preparation
	if _, err := p.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if err := p.runOne(context.Background()); err != nil {
		t.Fatal(err)
	}
	state, _, failures := preparationState(t, f.receiver)
	if state != "failed" || failures != 1 {
		t.Fatal("producer declaration trusted as evidence", state, failures)
	}
	generationAdmissionCounts(t, f.receiver, 0, 0)
}

func TestOnDemandPreparationRevocationWithdrawalAndScope(t *testing.T) {
	for _, scenario := range []string{"revoked", "withdrawn", "scope", "invalid_month"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			f, auth, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return false, nil })
			p := r.preparation
			month := f.plan.Funding.Month
			if scenario == "invalid_month" {
				if _, err := p.SubmitMonthly(context.Background(), 1, 1, time.Now().Format("2006-01")); err == nil {
					t.Fatal("current month admitted")
				}
				return
			}
			if _, err := p.SubmitMonthly(context.Background(), 1, 1, month); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "revoked":
				if err := auth.Model(&monitor.CustomerGroup{}).Where("id=1").Update("portal_auth_ver", 2).Error; err != nil {
					t.Fatal(err)
				}
			case "withdrawn":
				s := selectFixture(t, f, "")
				if _, err := WithdrawGenerationSelection(context.Background(), f.receiver, 1, month, s.Head); err != nil {
					t.Fatal(err)
				}
			case "scope":
				p.scope = &generationScope{exact: map[GenerationTarget]struct{}{{GroupID: 2, Month: month}: {}}}
			}
			if err := p.runOne(context.Background()); err != nil {
				t.Fatal(err)
			}
			state, _, _ := preparationState(t, f.receiver)
			if calls != 0 || state != "failed" {
				t.Fatal("old authority/source reactivated", calls, state)
			}
			if scenario == "revoked" {
				if _, err := p.ReadMonthly(context.Background(), 1, 1, month); !errors.Is(err, monitor.MonthlyGenerationFailure("state_changed")) {
					t.Fatal("metadata reread bypassed revoked authority", err)
				}
			}
			generationAdmissionCounts(t, f.receiver, 0, 0)
		})
	}
}

func TestOnDemandPreparationLeaseRecoveryAndPartialSteps(t *testing.T) {
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	p := r.preparation
	now := time.Now()
	p.now = func() time.Time { return now }
	ctx := context.Background()
	if _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	job, err := p.claim(ctx)
	if err != nil || job.key == "" {
		t.Fatal(job, err)
	}
	other, err := p.claim(ctx)
	if err != nil || other.key != "" {
		t.Fatal("live lease claimed twice", other, err)
	}
	now = now.Add(61 * time.Second)
	if err := p.runOne(ctx); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "queued" || steps != 2 || failures != 1 {
		t.Fatal(state, steps, failures)
	}
	for range 4 {
		now = now.Add(2 * time.Second)
		if err := p.runOne(ctx); err != nil {
			t.Fatal(err)
		}
	}
	_, steps, failures = preparationState(t, f.receiver)
	if steps != 6 || failures != 1 {
		t.Fatal("partial progress consumed failures", steps, failures)
	}
}

func TestOnDemandPreparationShutdownJoinsSourceStep(t *testing.T) {
	started, finished := make(chan struct{}), make(chan struct{})
	f, _, _, r := onDemandFixture(t, func(ctx context.Context, _ SourcePreparationRequest) (bool, error) {
		close(started)
		<-ctx.Done()
		close(finished)
		return false, ctx.Err()
	})
	if err := r.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { stopRuntime(t, r) })
	if _, err := r.SubmitMonthly(context.Background(), 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(5 * time.Second):
		t.Fatal("source worker not started")
	}
	stopRuntime(t, r)
	select {
	case <-finished:
	default:
		t.Fatal("stop returned before source work drained")
	}
	if err := r.EnableOnDemandPreparation(context.Background(), r.preparation.auth, r.preparation.admit, r.preparation.step); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("stopped runtime changed wiring", err)
	}
}

func TestOnDemandPreparationRestartAfterSelectionSkipsProducer(t *testing.T) {
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) {
		t.Error("retained selection was needlessly recollected")
		return false, ErrGenerationEvidence
	})
	p := r.preparation
	ctx := context.Background()
	if _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	// Simulate a crash after source selection committed but before the job's
	// result was saved. Next execution must submit the retained exact input.
	if _, err := StageGenerationVersion(ctx, f.receiver, f.plan, f.raw, ""); err != nil {
		t.Fatal(err)
	}
	if err := p.runOne(ctx); err != nil {
		t.Fatal(err)
	}
	state, steps, failures := preparationState(t, f.receiver)
	if state != "submitted" || steps != 1 || failures != 0 {
		t.Fatal(state, steps, failures)
	}
	if _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
}

func TestOnDemandPreparationCorruptJobDoesNotStarveOtherOrganization(t *testing.T) {
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	p := r.preparation
	p.scope = nil // Explicit isolated fixture only, still protected by authority.
	p.admit = func(context.Context, SourcePreparationRequest) error { return nil }
	ctx := context.Background()
	if _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if _, err := p.SubmitMonthly(ctx, 2, 1, f.plan.Funding.Month); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET payload='{}',next_at=0 WHERE group_id=1`); err != nil {
		t.Fatal(err)
	}
	if err := p.runOne(ctx); err != nil {
		t.Fatal(err)
	}
	if err := p.runOne(ctx); err != nil {
		t.Fatal(err)
	}
	var state string
	var steps int
	if err := f.receiver.QueryRow(`SELECT state,steps FROM portal_monthly_source_preparation WHERE group_id=2`).Scan(&state, &steps); err != nil {
		t.Fatal(err)
	}
	if state != "queued" || steps != 1 {
		t.Fatal("unrelated org starved", state, steps)
	}
	if _, err := p.SubmitMonthly(ctx, 2, 2, f.plan.Funding.Month); err == nil {
		t.Fatal("foreign auth version accepted")
	}
}

func TestOnDemandPreparationIdleReceiverIsReadOnly(t *testing.T) {
	f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { return false, nil })
	// A write-prohibited handle can perform idle scans. No schema creation or
	// writer reservation is allowed during these polls.
	r.preparation.db = generationDB(t, f.receiverPath, true)
	for range 3 {
		if err := r.preparation.runOne(context.Background()); err != nil {
			t.Fatal("idle scan reserved writer", err)
		}
	}
}

func TestOnDemandPreparationBoundsCannotBeResetByCustomer(t *testing.T) {
	for _, scenario := range []string{"steps", "age", "crashes"} {
		t.Run(scenario, func(t *testing.T) {
			calls := 0
			f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { calls++; return false, nil })
			p := r.preparation
			ctx := context.Background()
			if _, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month); err != nil {
				t.Fatal(err)
			}
			query := `UPDATE portal_monthly_source_preparation SET steps=4096`
			switch scenario {
			case "age":
				query = `UPDATE portal_monthly_source_preparation SET created_at=0`
			case "crashes":
				query = `UPDATE portal_monthly_source_preparation SET state='running',failures=2,lease_until=0`
			}
			if _, err := f.receiver.Exec(query); err != nil {
				t.Fatal(err)
			}
			if err := p.runOne(ctx); err != nil {
				t.Fatal(err)
			}
			view, err := p.SubmitMonthly(ctx, 1, 1, f.plan.Funding.Month)
			if err != nil || view.State != "needs_review" || calls != 0 {
				t.Fatal(view, err, calls)
			}
		})
	}
}
