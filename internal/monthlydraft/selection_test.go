package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func selectionFixture(t *testing.T) generationFixture {
	t.Helper()
	f := generationFixtureNew(t)
	if err := InitGenerationSelections(context.Background(), f.receiver); err != nil {
		t.Fatal(err)
	}
	return f
}

func selectFixture(t *testing.T, f generationFixture, head string) GenerationSelection {
	t.Helper()
	out, err := SelectGenerationVersion(context.Background(), f.receiver, f.plan, f.raw, head)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func TestGenerationSelectionPersistentReadOnlyAndScope(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	selector, err := GenerationSelector(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := selector(ctx, 1, "2026-09"); err == nil || errors.Is(err, ErrGenerationChanged) {
		t.Fatal("uninitialized registry silently accepted", err)
	}
	if err := InitGenerationSelections(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	if _, err := selector(ctx, 1, "2026-09"); !errors.Is(err, ErrGenerationSelectionMissing) {
		t.Fatal(err)
	}
	selected := selectFixture(t, f, "")
	if same := selectFixture(t, f, selected.Head); !reflect.DeepEqual(same, selected) {
		t.Fatal("idempotent selection changed")
	}
	if err := InitGenerationSelections(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	ro := generationDB(t, f.receiverPath, true)
	reader, err := GenerationSelector(ro)
	if err != nil {
		t.Fatal(err)
	}
	for range 3 {
		got, err := reader(ctx, 1, "2026-09")
		if err != nil || got != *selected.Request {
			t.Fatal("persisted selection changed", err)
		}
	}
	for _, scope := range []struct {
		gid   int64
		month string
	}{{2, "2026-09"}, {1, "2026-08"}} {
		if _, err := reader(ctx, scope.gid, scope.month); !errors.Is(err, ErrGenerationSelectionMissing) {
			t.Fatal("foreign scope selected", err)
		}
	}
	after, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(before, after) {
		t.Fatal("read modified registry")
	}
	for _, query := range []string{"DELETE FROM portal_monthly_generation_selections", "UPDATE portal_monthly_generation_selections SET month='2026-08'"} {
		if _, err := f.receiver.Exec(query); err == nil {
			t.Fatal("immutable history changed")
		}
	}
}

func TestGenerationSelectionSwitchConflictWithdrawalAndNoReactivation(t *testing.T) {
	f := selectionFixture(t)
	ctx := context.Background()
	a := selectFixture(t, f, "")
	original := f.plan
	f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
	if _, err := SelectGenerationVersion(ctx, f.receiver, f.plan, f.raw, ""); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal(err)
	}
	b := selectFixture(t, f, a.Head)
	if b.Head == a.Head {
		t.Fatal("version switch lost")
	}
	if _, err := SelectGenerationVersion(ctx, f.receiver, original, f.raw, b.Head); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("A-B-A reactivated task", err)
	}
	if _, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", a.Head); !errors.Is(err, ErrGenerationSelectionConflict) {
		t.Fatal(err)
	}
	w, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", b.Head)
	if err != nil || w.Head == b.Head || w.Request != nil {
		t.Fatal(err)
	}
	again, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", w.Head)
	if err != nil || !reflect.DeepEqual(w, again) {
		t.Fatal(err)
	}
	reader, _ := GenerationSelector(f.receiver)
	if _, err := reader(ctx, 1, "2026-09"); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("withdrawal not recognized", err)
	}
	if _, err := SelectGenerationVersion(ctx, f.receiver, f.plan, f.raw, w.Head); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("withdrawn input reused", err)
	}
	f.plan.Funding.SourceIdentity = strings.Repeat("e", 64)
	selected := selectFixture(t, f, w.Head)
	if _, err := reader(ctx, 1, "2026-09"); err != nil || selected.Head == w.Head {
		t.Fatal("new version unavailable", err)
	}
}

func TestGenerationSelectionConcurrentCompareAndSwap(t *testing.T) {
	f := selectionFixture(t)
	first := selectFixture(t, f, "")
	var wg sync.WaitGroup
	results := make(chan error, 2)
	for _, identity := range []string{"d", "e"} {
		wg.Add(1)
		go func(identity string) {
			defer wg.Done()
			p := f.plan
			p.Funding.SourceIdentity = strings.Repeat(identity, 64)
			_, err := SelectGenerationVersion(context.Background(), f.receiver, p, f.raw, first.Head)
			results <- err
		}(identity)
	}
	wg.Wait()
	close(results)
	success, conflicts := 0, 0
	for err := range results {
		if err == nil {
			success++
		} else if errors.Is(err, ErrGenerationSelectionConflict) {
			conflicts++
		} else {
			t.Fatal(err)
		}
	}
	if success != 1 || conflicts != 1 {
		t.Fatal("concurrent selections overwrote each other", success, conflicts)
	}
	var count int
	if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM portal_monthly_generation_selections`).Scan(&count); err != nil || count != 2 {
		t.Fatal(count, err)
	}
}

func TestGenerationSelectionRejectsCorruptionAndCancelledWrites(t *testing.T) {
	for _, scenario := range []string{"payload", "predecessor", "deleted_prefix", "oversize", "cancelled", "invalid_input"} {
		t.Run(scenario, func(t *testing.T) {
			f := selectionFixture(t)
			ctx := context.Background()
			first := selectFixture(t, f, "")
			exec := func(query string, args ...any) {
				t.Helper()
				if _, err := f.receiver.Exec(query, args...); err != nil {
					t.Fatal(err)
				}
			}
			if scenario == "cancelled" || scenario == "invalid_input" {
				callCtx, cancel := context.WithCancel(ctx)
				if scenario == "cancelled" {
					cancel()
				} else {
					defer cancel()
					f.plan.PreparationSHA256 = strings.Repeat("0", 64)
				}
				out, err := SelectGenerationVersion(callCtx, f.receiver, f.plan, f.raw, first.Head)
				if err == nil || out.Head != "" {
					t.Fatal("invalid write accepted", err)
				}
				got, err := ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
				if err != nil || got.Head != first.Head {
					t.Fatal("failed write changed selection", err)
				}
				return
			}
			// Deliberate corruption in synthetic receiver; normal SQL is blocked.
			exec(`DROP TRIGGER monthly_generation_selection_no_update`)
			switch scenario {
			case "payload":
				exec(`UPDATE portal_monthly_generation_selections SET payload='{}'`)
			case "predecessor":
				exec(`UPDATE portal_monthly_generation_selections SET id=?`, strings.Repeat("0", 64))
			case "oversize":
				exec(`PRAGMA ignore_check_constraints=ON`)
				exec(`UPDATE portal_monthly_generation_selections SET payload=?`, bytes.Repeat([]byte("x"), 2049))
			case "deleted_prefix":
				f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
				selectFixture(t, f, first.Head)
				exec(`DROP TRIGGER monthly_generation_selection_no_delete`)
				exec(`DELETE FROM portal_monthly_generation_selections WHERE id=?`, first.Head)
			}
			reader, _ := GenerationSelector(f.receiver)
			got, err := reader(ctx, 1, "2026-09")
			if !errors.Is(err, ErrGenerationIntegrity) || got != (monthlytasks.Request{}) {
				t.Fatal("corrupt selection served", err)
			}
		})
	}
}

func TestGenerationSelectionCommitFailureHasNoPartialSwitch(t *testing.T) {
	f := selectionFixture(t)
	ctx := context.Background()
	first := selectFixture(t, f, "")
	f.receiver.SetMaxOpenConns(1)
	for _, query := range []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE selection_parent(id INTEGER PRIMARY KEY)`,
		`CREATE TABLE selection_deferred(parent_id INTEGER REFERENCES selection_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER selection_commit_failure AFTER INSERT ON portal_monthly_generation_selections BEGIN INSERT INTO selection_deferred(parent_id) VALUES(99); END`,
	} {
		if _, err := f.receiver.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
	out, err := SelectGenerationVersion(ctx, f.receiver, f.plan, f.raw, first.Head)
	if err == nil || out.Head != "" {
		t.Fatal("failed commit returned new selection", err)
	}
	got, err := ReadGenerationSelection(ctx, f.receiver, 1, "2026-09")
	if err != nil || got.Head != first.Head {
		t.Fatal("partial switch became visible", err)
	}
	var count int
	if err := f.receiver.QueryRow(`SELECT COUNT(*) FROM selection_deferred`).Scan(&count); err != nil || count != 0 {
		t.Fatal("half commit", count, err)
	}
	if _, err := f.receiver.Exec(`DROP TRIGGER selection_commit_failure`); err != nil {
		t.Fatal(err)
	}
	selectFixture(t, f, first.Head)
}

func TestGenerationSelectionRealGuardAndGeneration(t *testing.T) {
	_, usage, _ := usageAuthorityFixture(t)
	f := selectionFixture(t)
	ctx := context.Background()
	owner, _, err := LoadUsageGenerationOwner(ctx, usage, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	selected := selectFixture(t, f, "")
	reader, err := GenerationSelector(generationDB(t, f.receiverPath, true))
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(usage, reader)
	if err != nil {
		t.Fatal(err)
	}
	f.g.verifyCurrent = guard
	task := generationSubmit(t, f)
	artifact, err := f.g.Run(ctx, owner, task.ID)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, artifact)
	cached, err := f.g.Run(ctx, owner, task.ID)
	if err != nil || !reflect.DeepEqual(cached, artifact) {
		t.Fatal("cache changed money", err)
	}
	if _, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", selected.Head); err != nil {
		t.Fatal(err)
	}
	blocked, err := f.g.Run(ctx, owner, task.ID)
	if !errors.Is(err, ErrGenerationChanged) || !reflect.DeepEqual(blocked, GenerationArtifact{}) {
		t.Fatal("withdrawn draft served", err)
	}
	state, err := f.g.tasks.Get(ctx, owner, task.ID)
	if err != nil || state.State != monthlytasks.Invalidated {
		t.Fatal("withdrawal not sticky", err)
	}
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("generation changed funding source", err)
	}
}

func TestGenerationSelectionCapacityStillAllowsWithdrawal(t *testing.T) {
	f := selectionFixture(t)
	ctx := context.Background()
	first := selectFixture(t, f, "")
	// Fill metadata using synthetic, independent orgs in one test transaction.
	err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		for gid := int64(2); gid <= generationSelectionLimit; gid++ {
			req := *first.Request
			req.Owner.GroupID = gid
			raw, err := json.Marshal(generationSelectionRecord{Version: 1, GroupID: gid, Month: req.Month, Request: &req})
			if err != nil {
				return err
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_selections(id,group_id,month,payload) VALUES(?,?,?,?)`, generationHash(raw), gid, req.Month, raw); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Funding.SourceIdentity = strings.Repeat("d", 64)
	if out, err := SelectGenerationVersion(ctx, f.receiver, f.plan, f.raw, first.Head); !errors.Is(err, ErrGenerationCapacity) || out.Head != "" {
		t.Fatal("capacity bypassed", err)
	}
	w, err := WithdrawGenerationSelection(ctx, f.receiver, 1, "2026-09", first.Head)
	if err != nil || w.Head == first.Head || w.Request != nil {
		t.Fatal("capacity prevented emergency withdrawal", err)
	}
	reader, _ := GenerationSelector(f.receiver)
	if _, err := reader(ctx, 1, "2026-09"); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal(err)
	}
	if out, err := SelectGenerationVersion(ctx, f.receiver, f.plan, f.raw, w.Head); !errors.Is(err, ErrGenerationCapacity) || out.Head != "" {
		t.Fatal("withdrawal reserve reused for new version", err)
	}
}
