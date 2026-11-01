package monthlytasks

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"strings"
	"testing"
	"time"
)

func TestTaskWorkCandidatesReadOnlyPolicyAndLease(t *testing.T) {
	for _, scenario := range []string{"queued", "live", "expired", "exhausted_lease", "cooldown", "transient", "evidence", "reconcile", "ready", "invalidated"} {
		t.Run(scenario, func(t *testing.T) {
			f := newFixture(t)
			ctx := context.Background()
			job := submit(t, f.store, requestFixture())
			want, retry := 0, false
			if scenario == "queued" {
				want = 1
			} else {
				lease := claim(t, f.store, job)
				switch scenario {
				case "expired":
					f.clock.Add(31000)
					want = 1
				case "exhausted_lease":
					for range 2 {
						f.clock.Add(31000)
						_ = claim(t, f.store, job)
					}
					f.clock.Add(31000)
					want = 1 // Claim must fence and mark exhaustion, not calculate.
				case "cooldown", "transient", "evidence", "reconcile":
					code := "internal_failure"
					if scenario == "evidence" {
						code = "evidence_incomplete"
					}
					if scenario == "reconcile" {
						code = "reconciliation_failed"
					}
					if err := f.store.Fail(ctx, lease, code); err != nil {
						t.Fatal(err)
					}
					if scenario != "cooldown" {
						f.clock.Add(31000)
					}
					if scenario == "transient" {
						want, retry = 1, true
					}
				case "ready":
					if err := f.store.Complete(ctx, lease, strings.Repeat("f", 64)); err != nil {
						t.Fatal(err)
					}
				case "invalidated":
					if err := f.store.Invalidate(ctx, job.Request.Owner, job.ID); err != nil {
						t.Fatal(err)
					}
				}
			}
			ro, err := sql.Open("sqlite", "file:"+f.path+"?mode=ro")
			if err != nil {
				t.Fatal(err)
			}
			defer ro.Close()
			reader, err := New(ro)
			if err != nil {
				t.Fatal(err)
			}
			reader.now = f.store.now
			before, err := os.ReadFile(f.path)
			if err != nil {
				t.Fatal(err)
			}
			for range 3 {
				got, err := reader.WorkCandidates(ctx)
				if err != nil || len(got) != want {
					t.Fatal("candidate selection", len(got), want, err)
				}
				if want == 1 && (got[0].Task.ID != job.ID || got[0].Retry != retry) {
					t.Fatal("wrong candidate")
				}
			}
			after, err := os.ReadFile(f.path)
			if err != nil || !bytes.Equal(before, after) {
				t.Fatal("candidate scan wrote receiver", err)
			}
			if scenario == "exhausted_lease" {
				_, _, err := f.store.Claim(ctx, job.Request.Owner, job.ID)
				if !errors.Is(err, ErrExhausted) {
					t.Fatal("exhausted attempt reclaimed", err)
				}
			}
		})
	}
}

func TestTaskWorkCandidatesRejectCorruptionAndClock(t *testing.T) {
	f := newFixture(t)
	_ = submit(t, f.store, requestFixture())
	f.clock.Add(-1)
	if out, err := f.store.WorkCandidates(context.Background()); !errors.Is(err, ErrClock) || out != nil {
		t.Fatal("backward clock accepted", err)
	}
	f.clock.Add(1)
	if _, err := f.db.Exec(`UPDATE portal_monthly_draft_tasks SET descriptor='{}'`); err != nil {
		t.Fatal(err)
	}
	if out, err := f.store.WorkCandidates(context.Background()); !errors.Is(err, ErrCorrupt) || out != nil {
		t.Fatal("corrupt queue accepted", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if out, err := f.store.WorkCandidates(ctx); !errors.Is(err, context.Canceled) || out != nil {
		t.Fatal("cancelled scan returned data", err)
	}
}

func TestTaskTransientRetryRechecksPolicyAndBudget(t *testing.T) {
	f := newFixture(t)
	ctx := context.Background()
	job := submit(t, f.store, requestFixture())
	lease := claim(t, f.store, job)
	if err := f.store.Fail(ctx, lease, "internal_failure"); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RetryTransient(ctx, job.Request.Owner, job.ID); !errors.Is(err, ErrCooldown) {
		t.Fatal("cooldown bypassed", err)
	}
	f.clock.Add((31 * time.Second).Milliseconds())
	if jobs, err := f.store.WorkCandidates(ctx); err != nil || len(jobs) != 1 {
		t.Fatal("transient not eligible", err)
	}
	if _, err := f.db.Exec(`UPDATE portal_monthly_draft_tasks SET failure_code='evidence_incomplete'`); err != nil {
		t.Fatal(err)
	}
	if err := f.store.RetryTransient(ctx, job.Request.Owner, job.ID); !errors.Is(err, ErrState) {
		t.Fatal("stale snapshot retried evidence failure", err)
	}
	// Existing explicit operator retry semantics are unchanged.
	if err := f.store.Retry(ctx, job.Request.Owner, job.ID); err != nil {
		t.Fatal(err)
	}
	for attempt := 2; attempt <= 3; attempt++ {
		lease = claim(t, f.store, job)
		if err := f.store.Fail(ctx, lease, "source_unavailable"); err != nil {
			t.Fatal(err)
		}
		f.clock.Add(31000)
		err := f.store.RetryTransient(ctx, job.Request.Owner, job.ID)
		if attempt == 2 && err != nil {
			t.Fatal(err)
		}
		if attempt == 3 && !errors.Is(err, ErrExhausted) {
			t.Fatal("attempt budget bypassed", err)
		}
	}
	if jobs, err := f.store.WorkCandidates(ctx); err != nil || len(jobs) != 0 {
		t.Fatal("exhausted task rescheduled", err)
	}
}
