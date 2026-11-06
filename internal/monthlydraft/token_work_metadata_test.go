package monthlydraft

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
)

func TestTokenWorkMetadataCannotRequestSuccessfulObservationCredit(t *testing.T) {
	f, r, p, _ := workFixture(t)
	req := SourcePreparationRequest{Owner: f.plan.Owner, Month: f.plan.Funding.Month}
	scope, err := p.funding.scope(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := monthlysource.EnsureFundingMonthCapture(t.Context(), p.funding.capture, req.Key(), scope); err != nil {
		t.Fatal(err)
	}
	cp, err := monthlysource.ReadTokenWorkCheckpoint(t.Context(), p.funding.capture, req.Key(), scope)
	if err != nil {
		t.Fatal(err)
	}
	d := &tokenWorkDispatch{p: r.preparation, job: sourcePreparationJob{key: req.Key(), request: req}}
	ctx := context.WithValue(t.Context(), tokenDispatchKey{}, d)
	if err := recordTokenWork(ctx, req, cp, cp); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("metadata-only step bypassed the successful-observation guard", err)
	}
	if d.receipt != nil {
		t.Fatal("metadata-only progress received observation credit")
	}
	var counts int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_counts`).Scan(&counts); err != nil || counts != 0 {
		t.Fatal("metadata-only step changed work counters", err)
	}
}

func TestTokenWorkRecoveringQueueCountsOnlySuccessfulChildOriginals(t *testing.T) {
	f, r, p, now := workFixture(t)
	if err := monthlysource.InitRecoveringTokenMonthStore(t.Context(), p.funding.capture); err != nil {
		t.Fatal(err)
	}
	p.funding.tokenRecovery = true
	// An ordinary original must already be credited before the later day
	// needs a split. Starting with a split and no saved ledger misses the bug.
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	if observed, credited := workCounts(t, f); observed != 1 || credited != 1 {
		t.Fatal("ordinary original missing")
	}
	*now = now.Add(1100 * time.Millisecond)
	calls := 0
	state := p.funding.source.Driver().(*tokenPreparationDriver).base
	state.onQuery = func() error {
		if f.receiver.Stats().InUse != 0 || p.funding.capture.Stats().InUse != 0 {
			return errors.New("SQLite held across source operation")
		}
		calls++
		// First source view: identity, wallet, then user-total aggregation.
		if calls == 3 {
			return &mysql.MySQLError{Number: 3024, Message: "synthetic daily aggregation deadline"}
		}
		return nil
	}
	var created int64
	if err := f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation`).Scan(&created); err != nil {
		t.Fatal(err)
	}
	if err := r.preparation.runOne(t.Context()); err != nil {
		t.Fatal(err)
	}
	status, steps, failures := preparationState(t, f.receiver)
	var roots, counts int
	if err := p.funding.capture.QueryRow(`SELECT count(*) FROM monthly_token_recovery_bindings`).Scan(&roots); err != nil {
		t.Fatal(err)
	}
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_token_work_counts`).Scan(&counts); err != nil {
		t.Fatal(err)
	}
	if status != "queued" || steps != 1 || failures != 0 || roots != 1 || counts != 1 {
		t.Fatal("classified split consumed a failure or invented an original", status, steps, failures, roots, counts)
	}
	for i := 1; i <= 2; i++ {
		*now = now.Add(1100 * time.Millisecond)
		if err := r.preparation.runOne(t.Context()); err != nil {
			t.Fatal(err)
		}
		status, steps, failures = preparationState(t, f.receiver)
		observed, credited := workCounts(t, f)
		if status != "queued" || steps != 1 || failures != 0 || observed != i+1 || credited != i+1 {
			t.Fatal("child original lost, duplicated or charged as coordinator work", status, steps, failures, observed, credited)
		}
	}
	var after int64
	if err := f.receiver.QueryRow(`SELECT created_at FROM portal_monthly_source_preparation`).Scan(&after); err != nil || after != created {
		t.Fatal("metadata recovery reset job age", err)
	}
	var selected int
	if err := f.receiver.QueryRow(`SELECT count(*) FROM portal_monthly_generation_selections`).Scan(&selected); err != nil || selected != 0 {
		t.Fatal("source-work recovery approved money", err)
	}
}
