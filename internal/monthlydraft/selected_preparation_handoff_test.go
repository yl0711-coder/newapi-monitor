package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"math"
	"os"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

func TestSelectedPreparationHandoffOldBudgetFailureDoesNotHideFirstGeneration(t *testing.T) {
	for _, limit := range []string{"age", "steps"} {
		t.Run(limit, func(t *testing.T) {
			work := 0
			f, _, _, r := onDemandFixture(t, func(context.Context, SourcePreparationRequest) (bool, error) { work++; return false, nil })
			p := r.preparation
			p.now = func() time.Time { return time.Now().Add(-25 * time.Hour) }
			if limit == "steps" {
				p.now = time.Now
			}
			if _, err := p.SubmitMonthly(t.Context(), 1, 1, f.plan.Funding.Month); err != nil {
				t.Fatal(err)
			}
			if limit == "steps" {
				// SETUP ONLY: emulate the retained coordinator limit; this test
				// does not claim to have executed 4096 source queries.
				if _, err := f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET steps=4096`); err != nil {
					t.Fatal(err)
				}
			}
			p.now = time.Now
			if err := p.runOne(t.Context()); err != nil {
				t.Fatal(err)
			}
			req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
			before, err := failedPreparationSnapshot(t.Context(), f.receiver, req)
			if err != nil || before.State != "failed" || before.Code != "budget_exhausted" {
				t.Fatal("fixture did not reach actual budget stop", before, err)
			}
			// A trusted earlier preparation may have committed its validated
			// selection before a crash prevented queue finalization.
			if _, err := StageGenerationVersion(t.Context(), f.receiver, f.plan, f.raw, ""); err != nil {
				t.Fatal(err)
			}
			view, err := p.ReadMonthly(t.Context(), 1, 1, req.Month)
			if err != nil || view.State != "not_requested" {
				t.Fatal("old preparation failure hides ready first-generation input", view, err)
			}
			after, err := failedPreparationSnapshot(t.Context(), f.receiver, req)
			if err != nil || after != before || work != 0 {
				t.Fatal("read recovered by rewriting counters or rerunning source work", after, work, err)
			}
		})
	}
}

type selectedHandoffRacingService struct {
	base       monitor.MonthlyGenerationService
	reads      int
	beforeRead func(int)
}

func (s *selectedHandoffRacingService) ReadMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	s.reads++
	s.beforeRead(s.reads)
	return s.base.ReadMonthly(ctx, gid, version, month)
}

func (s *selectedHandoffRacingService) SubmitMonthly(ctx context.Context, gid, version int64, month string) (monitor.MonthlyGenerationView, error) {
	return s.base.SubmitMonthly(ctx, gid, version, month)
}

func TestSelectedPreparationHandoffRechecksChangedSelectionAndRealTask(t *testing.T) {
	for _, kind := range []string{"withdrawn", "task_created", "task_invalidated", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			x := failedResumeFixture(t)
			selected, err := StageGenerationVersion(t.Context(), x.f.receiver, x.f.plan, x.f.raw, "")
			if err != nil {
				t.Fatal(err)
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(t.Context())
			defer cancel()
			probe := &selectedHandoffRacingService{base: x.p.base, beforeRead: func(reads int) {
				if reads != 2 {
					return
				}
				switch kind {
				case "withdrawn":
					if _, err := WithdrawGenerationSelection(t.Context(), x.f.receiver, 1, x.req.Month, selected.Head); err != nil {
						t.Fatal(err)
					}
				case "task_created", "task_invalidated":
					job, err := x.f.g.Submit(t.Context(), x.f.plan, x.f.raw)
					if err != nil {
						t.Fatal(err)
					}
					if kind == "task_invalidated" {
						if err := x.f.g.tasks.Invalidate(t.Context(), job.Request.Owner, job.ID); err != nil {
							t.Fatal(err)
						}
					}
				case "canceled":
					cancel()
				}
			}}
			x.p.base = probe
			view, err := x.p.ReadMonthly(ctx, 1, 1, x.req.Month)
			if kind == "canceled" {
				if err == nil || view.State != "" {
					t.Fatal("canceled status advertised first generation", view, err)
				}
			} else {
				want := "needs_preparation"
				if kind == "task_created" {
					want = "queued"
				}
				if err != nil || view.State != want {
					t.Fatal("stale first-click view masked real current state", kind, view, err)
				}
			}
			if probe.reads != 2 {
				t.Fatal("missing authoritative final metadata read", probe.reads)
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil || after != before {
				t.Fatal("metadata race changed old preparation failure", err)
			}
		})
	}
}

func TestSelectedPreparationHandoffTransientFailurePreservesAllOriginalAttempts(t *testing.T) {
	for _, code := range []string{"temporarily_unavailable", "temporarily_busy", "lease_expired"} {
		t.Run(code, func(t *testing.T) {
			x := failedResumeFixture(t)
			if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET failure_code=?`, code); err != nil {
				t.Fatal(err)
			}
			if _, err := StageGenerationVersion(t.Context(), x.f.receiver, x.f.plan, x.f.raw, ""); err != nil {
				t.Fatal(err)
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				view, err := x.p.ReadMonthly(t.Context(), 1, 1, x.req.Month)
				if err != nil || view.State != "not_requested" {
					t.Fatal("selected source not offered", view, err)
				}
			}
			if evictionCount(t, x.f.receiver, "portal_monthly_draft_tasks") != 0 {
				t.Fatal("status submitted or generated a task")
			}
			for range 2 {
				if _, err := x.p.SubmitMonthly(t.Context(), 1, 1, x.req.Month); err != nil {
					t.Fatal(err)
				}
			}
			if evictionCount(t, x.f.receiver, "portal_monthly_draft_tasks") != 1 {
				t.Fatal("duplicate generation admission")
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil || after != before || resumeReceiptCount(t, x.f.receiver) != 0 {
				t.Fatal("handoff reset original age/work/failures or created a resume epoch", after, err)
			}
		})
	}
}

func TestSelectedPreparationHandoffWorksWithReadOnlySingleConnectionReceiver(t *testing.T) {
	x := failedResumeFixture(t)
	if _, err := StageGenerationVersion(t.Context(), x.f.receiver, x.f.plan, x.f.raw, ""); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(x.f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	ro := generationDB(t, x.f.receiverPath, true)
	ro.SetMaxOpenConns(1)
	ro.SetMaxIdleConns(1)
	g, err := NewGeneration(ro, x.f.g.funding, x.f.g.root.Name(), x.f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.readOnly = true
	base, err := NewUsageHTTPService(g, x.p.auth)
	if err != nil {
		t.Fatal(err)
	}
	p := *x.p
	p.db, p.base = ro, base
	for range 3 {
		view, err := p.ReadMonthly(t.Context(), 1, 1, x.req.Month)
		if err != nil || view.State != "not_requested" {
			t.Fatal("handoff required a writer or second receiver connection", view, err)
		}
	}
	after, err := os.ReadFile(x.f.receiverPath)
	if err != nil || !bytes.Equal(before, after) {
		t.Fatal("status updated receiver file", err)
	}
}

func TestSelectedPreparationHandoffStillBlocksMissingRevokedAndUnsafeFailures(t *testing.T) {
	for _, kind := range []string{"no_selection", "withdrawn", "authority", "money", "descriptor", "active_lease", "bad_next", "not_exhausted", "canceled"} {
		t.Run(kind, func(t *testing.T) {
			x := failedResumeFixture(t)
			if kind != "no_selection" {
				selected, err := StageGenerationVersion(t.Context(), x.f.receiver, x.f.plan, x.f.raw, "")
				if err != nil {
					t.Fatal(err)
				}
				if kind == "withdrawn" {
					if _, err := WithdrawGenerationSelection(t.Context(), x.f.receiver, 1, x.req.Month, selected.Head); err != nil {
						t.Fatal(err)
					}
				}
			}
			ctx := t.Context()
			version := int64(1)
			switch kind {
			case "authority":
				version = 2 // stale/foreign session version, not a private runtime mutation
			case "money", "descriptor":
				code := "needs_review"
				if kind == "descriptor" {
					code = "invalid_descriptor"
				}
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET failure_code=?`, code); err != nil {
					t.Fatal(err)
				}
			case "active_lease":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET lease='unexpected-retained-lease',lease_until=?`, x.now.Add(time.Minute).UnixMilli()); err != nil {
					t.Fatal(err)
				}
			case "bad_next":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET next_at=?`, int64(math.MaxInt64)); err != nil {
					t.Fatal(err)
				}
			case "not_exhausted":
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_source_preparation SET failures=1`); err != nil {
					t.Fatal(err)
				}
			case "canceled":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil {
				t.Fatal(err)
			}
			view, err := x.p.ReadMonthly(ctx, 1, version, x.req.Month)
			if err == nil && view.State == "not_requested" {
				t.Fatal("unsafe handoff offered", kind, view)
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil || after != before || evictionCount(t, x.f.receiver, "portal_monthly_draft_tasks") != 0 {
				t.Fatal("rejected status changed failure or submitted work", err)
			}
		})
	}
}

func TestSelectedPreparationHandoffSelectedCorruptionCannotGenerateOrPublish(t *testing.T) {
	for _, kind := range []string{"input_missing", "input_corrupt", "source_missing", "source_corrupt"} {
		t.Run(kind, func(t *testing.T) {
			x := failedResumeFixture(t)
			selected, err := StageGenerationVersion(t.Context(), x.f.receiver, x.f.plan, x.f.raw, "")
			if err != nil {
				t.Fatal(err)
			}
			before, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil {
				t.Fatal(err)
			}
			switch kind {
			case "input_missing":
				// SETUP ONLY: simulate missing bytes in a restored copy. Normal
				// retention cannot delete this unpublished selected input.
				if _, err := x.f.receiver.Exec(`DROP TRIGGER monthly_generation_no_delete`); err != nil {
					t.Fatal(err)
				}
				if _, err := x.f.receiver.Exec(`DELETE FROM portal_monthly_generation_inputs`); err != nil {
					t.Fatal(err)
				}
			case "input_corrupt":
				if _, err := x.f.receiver.Exec(`DROP TRIGGER monthly_generation_no_replace`); err != nil {
					t.Fatal(err)
				}
				if _, err := x.f.receiver.Exec(`UPDATE portal_monthly_generation_inputs SET preparation='{}'`); err != nil {
					t.Fatal(err)
				}
			case "source_missing":
				if err := os.Remove(x.f.input.Sources[0].Path); err != nil {
					t.Fatal(err)
				}
			case "source_corrupt":
				if err := os.WriteFile(x.f.input.Sources[0].Path, []byte(`[]`), 0600); err != nil {
					t.Fatal(err)
				}
			}
			// GET remains metadata-only. A button never approves money: the
			// actual admission and Run still re-read every original boundary.
			if _, err := x.p.ReadMonthly(t.Context(), 1, 1, x.req.Month); err != nil {
				t.Fatal(err)
			}
			_, err = x.p.SubmitMonthly(t.Context(), 1, 1, x.req.Month)
			if err == nil {
				job, err := x.f.g.tasks.Find(t.Context(), *selected.Request)
				if err != nil {
					t.Fatal(err)
				}
				if artifact, err := x.f.g.Run(t.Context(), job.Request.Owner, job.ID); err == nil || artifact.Version != 0 {
					t.Fatal("damaged input produced an artifact", kind, err)
				}
			}
			after, err := failedPreparationSnapshot(t.Context(), x.f.receiver, x.req)
			if err != nil || after != before {
				t.Fatal("failed original queue was modified", err)
			}
		})
	}
}

func TestSelectedPreparationHandoffCustomerClickPublishesAfterExpiredQueue(t *testing.T) {
	f := newCapturedHistoryFixture(t)
	addCapturedHistoryReconstruction(t, &f)
	var before preparationFailedSnapshot
	var req SourcePreparationRequest
	testCapturedHistoryPublicationAdapter(t, f, true, true, true, true, func(d *Deployment) {
		p := d.runtime.preparation
		p.now = func() time.Time { return time.Now().Add(-25 * time.Hour) }
		if _, err := p.SubmitMonthly(t.Context(), 1, 1, f.req.Month); err != nil {
			t.Fatal(err)
		}
		p.now = time.Now
		if err := p.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		var err error
		req = f.req
		before, err = failedPreparationSnapshot(t.Context(), d.receiver, req)
		if err != nil || before.State != "failed" || before.Code != "budget_exhausted" {
			t.Fatal("old job did not stop", before, err)
		}
		// Complete trusted original normalization/selection without resuming,
		// modifying or pretending to have succeeded the old preparation row.
		files, _, err := f.spool.Advance(t.Context(), req)
		if err != nil {
			t.Fatal(err)
		}
		prepared, err := d.g.prepareCapturedHistoryInput(t.Context(), req, files, f.sources, "captured-scope")
		if err != nil {
			t.Fatal(err)
		}
		var plan GenerationPlan
		if err := json.Unmarshal(prepared.Plan, &plan); err != nil {
			t.Fatal(err)
		}
		if _, err := StageGenerationVersion(t.Context(), d.receiver, plan, prepared.Preparation, ""); err != nil {
			t.Fatal(err)
		}
	})
	after, err := failedPreparationSnapshot(t.Context(), f.f.receiver, req)
	if err != nil || after != before {
		t.Fatal("actual customer publication changed original exhausted queue", after, err)
	}
}

func TestSelectedPreparationHandoffRequiresEveryRegisteredPredecessor(t *testing.T) {
	f := newRegisteredPrefixFixture(t)
	d, calls, _ := f.open(t, nil)
	p := d.runtime.preparation
	p.now = func() time.Time { return time.Now().Add(-25 * time.Hour) }
	if _, err := p.SubmitMonthly(t.Context(), 1, 1, "2026-09"); err != nil {
		t.Fatal(err)
	}
	p.now = time.Now
	if err := p.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	req := SourcePreparationRequest{Owner: f.f.plans[1].Owner, Month: "2026-09"}
	before, err := failedPreparationSnapshot(t.Context(), d.receiver, req)
	if err != nil || before.Code != "budget_exhausted" {
		t.Fatal(before, err)
	}
	if _, err := StageGenerationVersion(t.Context(), d.receiver, f.f.plans[1], f.f.inputs[1], ""); err != nil {
		t.Fatal(err)
	}
	view, err := p.ReadMonthly(t.Context(), 1, 1, req.Month)
	if err != nil || view.State != "needs_review" {
		t.Fatal("target alone bypassed missing registered predecessor", view, err)
	}
	if _, err := StageGenerationVersion(t.Context(), d.receiver, f.f.plans[0], f.f.inputs[0], ""); err != nil {
		t.Fatal(err)
	}
	view, err = p.ReadMonthly(t.Context(), 1, 1, req.Month)
	if err != nil || view.State != "not_requested" {
		t.Fatal("complete original prefix still hidden by old failure", view, err)
	}
	after, err := failedPreparationSnapshot(t.Context(), d.receiver, req)
	if err != nil || after != before || len(*calls) != 0 || evictionCount(t, d.receiver, "portal_monthly_draft_tasks") != 0 {
		t.Fatal("prefix status ran source work or modified old failure", err)
	}
}
