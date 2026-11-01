package monthlydraft

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func completedArchiveFixture(t *testing.T) (generationFixture, monthlytasks.Task, monthlytasks.Task, ArchiveReceipt, time.Time) {
	t.Helper()
	f := generationFixtureNew(t)
	old, latest := retentionReady(t, f, 1), retentionReady(t, f, 2)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	return f, old, latest, receipt, asOf
}

func completedArchiveEvict(t *testing.T, f generationFixture, j monthlytasks.Task, receipt ArchiveReceipt, asOf time.Time) {
	t.Helper()
	if err := InitCompletedGenerationRetention(context.Background(), f.receiver); err != nil {
		t.Fatal(err)
	}
	if err := ChangeCompletedGenerationPayload(context.Background(), f.receiver, receipt, j.Request.InputSHA256, asOf, false); err != nil {
		t.Fatal(err)
	}
}

func TestCompletedArchiveReadOnlyColdReadAndExactRestore(t *testing.T) {
	f, old, latest, receipt, asOf := completedArchiveFixture(t)
	ctx := context.Background()
	before, err := f.g.Run(ctx, old.Request.Owner, old.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err := InitGenerationRetention(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("v1 initialization enabled ready eviction", err)
	}
	completedArchiveEvict(t, f, old, receipt, asOf)
	if err := InitGenerationRetention(ctx, f.receiver); err != nil {
		t.Fatal("v1 init after v2 opt-in", err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("v1 operation silently broadened scope", err)
	}
	if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); err != nil {
		t.Fatal("repeat release", err)
	}
	r := archiveInventory(t, f, asOf)
	if r.ArchivedInputs != 1 || r.InputCount != 1 || r.ReviewRequired != 0 {
		t.Fatal("unexpected archive inventory", r)
	}
	retentionItem(t, r, old, "archived_payload")
	retentionItem(t, r, latest, "keep_latest_ready")
	fileBefore, err := os.ReadFile(f.receiverPath)
	if err != nil {
		t.Fatal(err)
	}
	// Real read-only SQLite: archive reads cannot silently restore payloads,
	// rewrite task timestamps, recalculate wallet values, or publish anything.
	g, err := NewGeneration(generationDB(t, f.receiverPath, true), f.funding, f.root, func(context.Context, monthlytasks.Request) error { return nil })
	if err != nil {
		t.Fatal(err)
	}
	defer g.Close()
	g.calculate = func(context.Context, monthlysource.FundingMonthRequest, []monthlysource.FundingWindowObservation, []byte, time.Time) (Review, error) {
		return Review{}, errors.New("archive reads must not calculate money")
	}
	for range 2 {
		after, err := g.Run(ctx, old.Request.Owner, old.ID)
		if err != nil || !reflect.DeepEqual(before, after) {
			t.Fatal("cold draft differs or attempts to write/recalculate", err)
		}
		assertGenerationAmounts(t, after)
	}
	fileAfter, err := os.ReadFile(f.receiverPath)
	if err != nil || !bytes.Equal(fileBefore, fileAfter) || evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 {
		t.Fatal("cold read changed receiver", err)
	}
	if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, true); err != nil {
		t.Fatal("restore completed draft", err)
	}
	after, err := f.g.Run(ctx, old.Request.Owner, old.ID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("restored hot draft changed", err)
	}
	state, err := f.g.tasks.Get(ctx, old.Request.Owner, old.ID)
	if err != nil || state != old {
		t.Fatal("read/restore altered completed task metadata", err)
	}
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("funding evidence changed", err)
	}
}

func TestCompletedArchiveRechecksLastIntactDraftBeforeRelease(t *testing.T) {
	for _, scenario := range []string{"latest_revoked", "latest_corrupt", "latest_wrong_binding"} {
		t.Run(scenario, func(t *testing.T) {
			f, old, latest, receipt, asOf := completedArchiveFixture(t)
			ctx := context.Background()
			if err := InitCompletedGenerationRetention(ctx, f.receiver); err != nil {
				t.Fatal(err)
			}
			switch scenario {
			case "latest_revoked":
				if err := f.g.tasks.Invalidate(ctx, latest.Request.Owner, latest.ID); err != nil {
					t.Fatal(err)
				}
			case "latest_corrupt":
				if _, err := f.receiver.Exec(`DROP TRIGGER monthly_generation_no_replace; UPDATE portal_monthly_generation_inputs SET artifact='{}' WHERE id=?`, latest.Request.InputSHA256); err != nil {
					t.Fatal(err)
				}
			case "latest_wrong_binding":
				wrong := latest.Request
				wrong.Owner.GroupID++
				if _, err := f.g.tasks.Submit(ctx, wrong); err != nil {
					t.Fatal(err)
				}
			}
			if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationInput) {
				t.Fatal("stale copy unpinned last intact hot draft", err)
			}
			if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 2 || evictionCount(t, f.receiver, "portal_monthly_generation_archives") != 0 || evictionCount(t, f.receiver, "portal_monthly_generation_release_permits") != 0 {
				t.Fatal("rejected release changed retained state")
			}
		})
	}
}

func TestCompletedArchiveColdReadRejectsForeignRevokedOrBrokenEvidence(t *testing.T) {
	for _, scenario := range []string{"foreign_owner", "revoked_before", "revoked_after_read", "invalidated_at_return", "missing_archive", "corrupt_archive", "changed_source"} {
		t.Run(scenario, func(t *testing.T) {
			f, old, _, receipt, asOf := completedArchiveFixture(t)
			completedArchiveEvict(t, f, old, receipt, asOf)
			owner := old.Request.Owner
			calls := 0
			switch scenario {
			case "foreign_owner":
				owner.MemberManifest = generationHash([]byte("different membership"))
			case "revoked_before":
				f.g.verifyCurrent = func(context.Context, monthlytasks.Request) error { return ErrGenerationChanged }
			case "revoked_after_read", "invalidated_at_return":
				f.g.verifyCurrent = func(ctx context.Context, _ monthlytasks.Request) error {
					calls++
					if scenario == "revoked_after_read" && calls == 2 {
						return ErrGenerationChanged
					}
					if scenario == "invalidated_at_return" && calls == 3 {
						return f.g.tasks.Invalidate(ctx, old.Request.Owner, old.ID)
					}
					return nil
				}
			case "missing_archive":
				if err := os.Remove(filepath.Join(receipt.Directory, "manifest.json")); err != nil {
					t.Fatal(err)
				}
			case "corrupt_archive":
				if err := os.WriteFile(filepath.Join(receipt.Directory, old.Request.InputSHA256+".artifact"), []byte("{}"), 0600); err != nil {
					t.Fatal(err)
				}
			case "changed_source":
				if err := os.WriteFile(f.input.Sources[0].Path, []byte("changed evidence"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			if a, err := f.g.Run(context.Background(), owner, old.ID); err == nil || !reflect.DeepEqual(a, GenerationArtifact{}) {
				t.Fatal("invalid cold read returned financial data", err)
			}
			if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 {
				t.Fatal("failure restored/recalculated a payload")
			}
		})
	}
}

func TestCompletedArchiveRestoreCommitFailureAndRecovery(t *testing.T) {
	f, old, _, receipt, asOf := completedArchiveFixture(t)
	completedArchiveEvict(t, f, old, receipt, asOf)
	f.receiver.SetMaxOpenConns(1)
	for _, query := range []string{
		`PRAGMA foreign_keys=ON`,
		`CREATE TABLE ready_parent(id INTEGER PRIMARY KEY)`,
		`CREATE TABLE ready_deferred(parent_id INTEGER REFERENCES ready_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
		`CREATE TRIGGER ready_commit_failure AFTER INSERT ON portal_monthly_generation_inputs BEGIN INSERT INTO ready_deferred(parent_id) VALUES(99); END`,
	} {
		if _, err := f.receiver.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	ctx := context.Background()
	if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, true); err == nil {
		t.Fatal("failed restore commit accepted")
	}
	if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 {
		t.Fatal("failed restoration leaked uncommitted data")
	}
	a, err := f.g.Run(ctx, old.Request.Owner, old.ID)
	if err != nil {
		t.Fatal("failed restoration broke archive read", err)
	}
	assertGenerationAmounts(t, a)
	if _, err := f.receiver.Exec(`DROP TRIGGER ready_commit_failure`); err != nil {
		t.Fatal(err)
	}
	if err := ChangeCompletedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, true); err != nil {
		t.Fatal("restore retry", err)
	}
}
