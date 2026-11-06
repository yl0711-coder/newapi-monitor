package monthlydraft

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func generationAdmissionCounts(t *testing.T, db *sql.DB, wantTasks, wantInputs int) {
	t.Helper()
	var tasks, inputs int
	if err := db.QueryRow(`SELECT (SELECT COUNT(*) FROM portal_monthly_draft_tasks),
(SELECT COUNT(*) FROM portal_monthly_generation_inputs)`).Scan(&tasks, &inputs); err != nil || tasks != wantTasks || inputs != wantInputs {
		t.Fatalf("admission counts tasks=%d inputs=%d, want %d/%d: %v", tasks, inputs, wantTasks, wantInputs, err)
	}
}

func TestGenerationAdmissionBusyCannotFillReceiver(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	initial := generationSubmit(t, f)
	for i := range generationRecordLimit + 5 {
		p := f.plan
		p.Owner.AuthVersion += int64(i + 1)
		j, err := f.g.Submit(ctx, p, f.raw)
		if !errors.Is(err, monthlytasks.ErrBusy) || j != (monthlytasks.Task{}) {
			t.Fatal("rejected request returned task", j, err)
		}
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
	if got := generationSubmit(t, f); !reflect.DeepEqual(got, initial) {
		t.Fatal("busy requests changed original task", got)
	}
	if err := f.g.tasks.Invalidate(ctx, initial.Request.Owner, initial.ID); err != nil {
		t.Fatal(err)
	}
	if j, err := f.g.Submit(ctx, f.plan, f.raw); !errors.Is(err, monthlytasks.ErrStale) || j != (monthlytasks.Task{}) {
		t.Fatal("invalidated input resurrected", j, err)
	}
	f.plan.Owner.AuthVersion++
	_ = generationSubmit(t, f)
	generationAdmissionCounts(t, f.receiver, 2, 2)
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("admission modified source database", err)
	}
}

func TestGenerationAdmissionWriteFailuresRollBackBoth(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	for _, table := range []string{"portal_monthly_draft_tasks", "portal_monthly_generation_inputs"} {
		// Table names are a fixed test allowlist; never customer SQL.
		if _, err := f.receiver.Exec(`CREATE TRIGGER admission_write_failure BEFORE INSERT ON ` + table + `
BEGIN SELECT RAISE(ABORT,'synthetic receiver write failure'); END`); err != nil {
			t.Fatal(err)
		}
		if j, err := f.g.Submit(ctx, f.plan, f.raw); err == nil || j != (monthlytasks.Task{}) {
			t.Fatal("receiver write failure returned success", j, err)
		}
		generationAdmissionCounts(t, f.receiver, 0, 0)
		if _, err := f.receiver.Exec(`DROP TRIGGER admission_write_failure`); err != nil {
			t.Fatal(err)
		}
	}
	_ = generationSubmit(t, f)
	generationAdmissionCounts(t, f.receiver, 1, 1)
}

func TestGenerationAdmissionLegacyOrphanAndMissingEstablishedInput(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	p, raw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// Reproduce an exact retained orphan from the previous LOCAL implementation.
	// Reuse is allowed; dropping old inputs or rewriting their bytes is not.
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, raw, f.raw, false)
	}); err != nil {
		t.Fatal(err)
	}
	generationAdmissionCounts(t, f.receiver, 0, 1)
	j := generationSubmit(t, f)
	generationAdmissionCounts(t, f.receiver, 1, 1)
	if j.Request != generationRequest(p, raw) {
		t.Fatal("legacy exact input identity changed")
	}
	if err := f.g.tasks.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	f.plan.Owner.AuthVersion++
	p, raw, _, err = canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	// A task created without its manifest is NOT a legacy orphan. Treat it as
	// incomplete/corrupt rather than silently reconstructing established inputs.
	missing, err := f.g.tasks.Submit(ctx, generationRequest(p, raw))
	if err != nil {
		t.Fatal(err)
	}
	if got, err := f.g.Submit(ctx, f.plan, f.raw); !errors.Is(err, ErrGenerationIntegrity) || got != (monthlytasks.Task{}) {
		t.Fatal("missing established input was silently repaired", got, err)
	}
	generationAdmissionCounts(t, f.receiver, 2, 1)
	if got, err := f.g.tasks.Get(ctx, missing.Request.Owner, missing.ID); err != nil || got != missing {
		t.Fatal("failed validation changed established task", got, err)
	}
}

func TestGenerationAdmissionConcurrentConnectionsReuseBoth(t *testing.T) {
	f := generationFixtureNew(t)
	secondDB := generationDB(t, f.receiverPath, false)
	second, err := NewGeneration(secondDB, f.funding, f.root, f.g.verifyCurrent)
	if err != nil {
		t.Fatal(err)
	}
	defer second.Close()
	type outcome struct {
		task monthlytasks.Task
		err  error
	}
	results := make(chan outcome, 16)
	var wg sync.WaitGroup
	for i := range cap(results) {
		g := []*Generation{f.g, second}[i%2]
		wg.Add(1)
		go func() {
			defer wg.Done()
			j, err := g.Submit(context.Background(), f.plan, f.raw)
			results <- outcome{j, err}
		}()
	}
	wg.Wait()
	close(results)
	var first monthlytasks.Task
	for result := range results {
		if result.err != nil {
			t.Fatal(result.err)
		}
		if first.ID == "" {
			first = result.task
		}
		if first != result.task {
			t.Fatal("duplicate tasks under concurrent admission")
		}
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
	saved, err := loadGeneration(context.Background(), f.receiver, first.Request, time.Now())
	if err != nil || saved.plan.PreparationSHA256 != generationHash(f.raw) || len(saved.artifact) != 0 {
		t.Fatal("concurrent admission corrupted inputs or ran generation", err)
	}
}

func TestGenerationArtifactCommitFailureKeepsEmptyResult(t *testing.T) {
	f := generationFixtureNew(t)
	j := generationSubmit(t, f)
	f.receiver.SetMaxOpenConns(1)
	for _, query := range []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE artifact_parent (id INTEGER PRIMARY KEY)`,
		`CREATE TABLE artifact_deferred (parent_id INTEGER REFERENCES artifact_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER artifact_commit_failure AFTER UPDATE ON portal_monthly_generation_inputs
WHEN NEW.artifact IS NOT NULL BEGIN INSERT INTO artifact_deferred(parent_id) VALUES(99); END`,
	} {
		if _, err := f.receiver.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if a, err := f.g.Run(context.Background(), j.Request.Owner, j.ID); err == nil || len(a.Reconciliation.Accounts) != 0 {
		t.Fatal("failed artifact COMMIT returned financial result", err)
	}
	saved, err := loadGeneration(context.Background(), f.receiver, j.Request, time.Now())
	if err != nil || len(saved.artifact) != 0 || saved.digest != "" {
		t.Fatal("uncommitted artifact leaked into reusable receiver", err)
	}
	state, err := f.g.tasks.Get(context.Background(), j.Request.Owner, j.ID)
	if err != nil || state.State != monthlytasks.Failed || state.ResultSHA256 != "" {
		t.Fatal("artifact commit failure left success or unusable connection", state, err)
	}
	generationAdmissionCounts(t, f.receiver, 1, 1)
}
