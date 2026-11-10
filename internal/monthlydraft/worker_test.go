package monthlydraft

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func waitLocalDraft(t *testing.T, g *Generation, task monthlytasks.Task) {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		got, err := g.tasks.Get(context.Background(), task.Request.Owner, task.ID)
		if err != nil {
			t.Fatal(err)
		}
		if got.State == monthlytasks.DraftReady {
			return
		}
		if got.State == monthlytasks.Failed || got.State == monthlytasks.Invalidated {
			t.Fatal("generation failed", got.State, got.FailureCode)
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("local worker did not complete")
}

func testLocalWorker(t *testing.T, g *Generation) *LocalWorker {
	t.Helper()
	w, err := NewLocalWorker(g)
	if err != nil {
		t.Fatal(err)
	}
	// Test-only accelerated scheduling. Production constructor remains 5s..60s.
	w.pollMin, w.pollMax = 20*time.Millisecond, 100*time.Millisecond
	return w
}

func ageSyntheticFailure(t *testing.T, f generationFixture) {
	t.Helper()
	// Preserve valid retry_at == updated_at + 30s; only a synthetic test receiver.
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_tasks SET created_at=MIN(created_at,updated_at-32000),updated_at=updated_at-31000,retry_at=retry_at-31000 WHERE state='failed' AND retry_at>0`); err != nil {
		t.Fatal(err)
	}
}

func TestLocalWorkerStartupDuplicateOwnersAndCachedMoney(t *testing.T) {
	f := generationFixtureNew(t)
	var calculations atomic.Int64
	calculate := f.g.calculate
	f.g.calculate = func(ctx context.Context, req monthlysource.FundingMonthRequest, windows []monthlysource.FundingWindowObservation, raw []byte, now time.Time) (Review, error) {
		calculations.Add(1)
		return calculate(ctx, req, windows, raw, now)
	}
	job := generationSubmit(t, f)
	w, other := testLocalWorker(t, f.g), testLocalWorker(t, f.g)
	ctx, cancel := context.WithCancel(context.Background())
	done, second := make(chan error, 1), make(chan error, 1)
	go func() { done <- w.Serve(ctx) }()
	go func() { second <- other.Serve(ctx) }()
	t.Cleanup(func() { cancel(); <-done; <-second })
	for range 100 {
		w.Wake()
		other.Wake()
	}
	waitLocalDraft(t, f.g, job)
	if calculations.Load() != 1 {
		t.Fatal("duplicate worker recalculated money", calculations.Load())
	}
	if err := w.Serve(ctx); !errors.Is(err, ErrGenerationBusy) {
		t.Fatal("same runner entered twice", err)
	}
	// DraftReady is published before the worker releases its run slot; under
	// the race detector the cached read can arrive in between. Busy is the
	// documented answer then, so wait for the slot instead of failing.
	var artifact GenerationArtifact
	var err error
	for deadline := time.Now().Add(10 * time.Second); ; time.Sleep(10 * time.Millisecond) {
		artifact, err = f.g.Run(context.Background(), job.Request.Owner, job.ID)
		if !errors.Is(err, ErrGenerationBusy) || time.Now().After(deadline) {
			break
		}
	}
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	if calculations.Load() != 1 {
		t.Fatal("cached read recalculated")
	}
}

func TestLocalWorkerRetryBudgetPersistsAcrossRestart(t *testing.T) {
	f := generationFixtureNew(t)
	job := generationSubmit(t, f)
	f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		return Review{}, errors.New("synthetic temporary failure")
	}
	for attempt := 1; attempt <= 3; attempt++ {
		w := testLocalWorker(t, f.g) // process-local state is deliberately lost
		worked, err := w.step(context.Background())
		if err != nil || !worked {
			t.Fatal("expected bounded attempt", attempt, err)
		}
		got, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
		if err != nil || got.State != monthlytasks.Failed || got.Attempts != attempt {
			t.Fatal("wrong persistent attempts", got, err)
		}
		if jobs, err := f.g.tasks.WorkCandidates(context.Background()); err != nil || len(jobs) != 0 {
			t.Fatal("cooldown not respected", err)
		}
		ageSyntheticFailure(t, f)
	}
	for range 3 {
		w := testLocalWorker(t, f.g)
		if worked, err := w.step(context.Background()); err != nil || worked {
			t.Fatal("restart reset retry budget", err)
		}
	}
}

func TestLocalWorkerEvidenceFailureStaysPaused(t *testing.T) {
	f := generationFixtureNew(t)
	job := generationSubmit(t, f)
	f.g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		return Review{}, ErrGenerationEvidence
	}
	w := testLocalWorker(t, f.g)
	if worked, err := w.step(context.Background()); err != nil || !worked {
		t.Fatal(err)
	}
	ageSyntheticFailure(t, f)
	for range 3 {
		if worked, err := w.step(context.Background()); err != nil || worked {
			t.Fatal("reconciliation failure retried", err)
		}
	}
	got, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || got.Attempts != 1 || got.FailureCode != "reconciliation_failed" {
		t.Fatal(got, err)
	}
}

func TestLocalWorkerTransientPrecheckDoesNotStarveOtherTask(t *testing.T) {
	f := generationFixtureNew(t)
	_ = generationSubmit(t, f)
	f.plan.Owner.GroupID = 2
	for i := range f.input.Accounts {
		f.input.Accounts[i].GroupID = 2
	}
	f.raw = draftJSON(t, f.input)
	f.plan.PreparationSHA256 = generationHash(f.raw)
	_ = generationSubmit(t, f)
	jobs, err := f.g.tasks.WorkCandidates(context.Background())
	if err != nil || len(jobs) != 2 {
		t.Fatal(err)
	}
	blocked := jobs[0].Task.ID
	blockedGroup := jobs[0].Task.Request.Owner.GroupID
	f.g.verifyCurrent = func(_ context.Context, r monthlytasks.Request) error {
		if r.Owner.GroupID == blockedGroup {
			return errors.New("synthetic auth storage unavailable")
		}
		return nil
	}
	w := testLocalWorker(t, f.g)
	w.pollMin, w.pollMax = time.Minute, time.Minute
	for range 2 {
		if worked, err := w.step(context.Background()); err != nil || !worked {
			t.Fatal(err)
		}
	}
	first, err := f.g.tasks.Get(context.Background(), jobs[0].Task.Request.Owner, blocked)
	if err != nil || first.State != monthlytasks.Queued || first.Attempts != 0 {
		t.Fatal("temporary auth error revoked/claimed task", first, err)
	}
	second, err := f.g.tasks.Get(context.Background(), jobs[1].Task.Request.Owner, jobs[1].Task.ID)
	if err != nil || second.State != monthlytasks.DraftReady {
		t.Fatal("other task starved", second, err)
	}
}

func TestLocalWorkerExpiredLeaseRecoveryFencesOldOwner(t *testing.T) {
	f := generationFixtureNew(t)
	job := generationSubmit(t, f)
	_, old, err := f.g.tasks.Claim(context.Background(), job.Request.Owner, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Simulate elapsed wall clock after the old process exited; retain all lease
	// invariants. Existing task-store tests also cover actual OS process exits.
	if _, err := f.receiver.Exec(`UPDATE portal_monthly_draft_tasks SET created_at=created_at-31000,updated_at=updated_at-31000,started_at=started_at-31000,lease_until=lease_until-31000 WHERE state='running'`); err != nil {
		t.Fatal(err)
	}
	w := testLocalWorker(t, f.g)
	if worked, err := w.step(context.Background()); err != nil || !worked {
		t.Fatal(err)
	}
	got, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || got.State != monthlytasks.DraftReady || got.Attempts != 2 {
		t.Fatal(got, err)
	}
	if err := f.g.tasks.Renew(context.Background(), old); !errors.Is(err, monthlytasks.ErrLeaseLost) {
		t.Fatal("old lease revived", err)
	}
}

func TestLocalWorkerShutdownAndRestart(t *testing.T) {
	f := generationFixtureNew(t)
	job := generationSubmit(t, f)
	calculate := f.g.calculate
	entered := make(chan struct{})
	f.g.calculate = func(ctx context.Context, _ monthlysource.FundingMonthRequest, _ []monthlysource.FundingWindowObservation, _ []byte, _ time.Time) (Review, error) {
		close(entered)
		<-ctx.Done()
		return Review{}, ctx.Err()
	}
	w := testLocalWorker(t, f.g)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- w.Serve(ctx) }()
	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("worker did not start")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("worker did not stop")
	}
	if w.Status().Running {
		t.Fatal("worker still running after join")
	}
	f.g.calculate = calculate
	ageSyntheticFailure(t, f)
	restarted := testLocalWorker(t, f.g)
	if worked, err := restarted.step(context.Background()); err != nil || !worked {
		t.Fatal(err)
	}
	got, err := f.g.tasks.Get(context.Background(), job.Request.Owner, job.ID)
	if err != nil || got.State != monthlytasks.DraftReady || got.Attempts != 2 {
		t.Fatal(got, err)
	}
}

func TestLocalWorkerWakeCannotBypassBackoff(t *testing.T) {
	f := generationFixtureNew(t)
	w := testLocalWorker(t, f.g)
	for range 1000 {
		w.Wake()
	}
	if len(w.wake) != 1 {
		t.Fatal("unbounded notifications")
	}
	start := time.Now()
	if err := w.wait(context.Background(), start, 60*time.Millisecond, true); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < 55*time.Millisecond {
		t.Fatal("wake bypassed database backoff")
	}
	start = time.Now()
	if err := w.wait(context.Background(), start, time.Second, false); err != nil {
		t.Fatal(err)
	}
	if time.Since(start) < w.pollMin || time.Since(start) > 500*time.Millisecond {
		t.Fatal("wake did not respect bounded minimum")
	}
}
