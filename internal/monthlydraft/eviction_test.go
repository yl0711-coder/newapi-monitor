package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

func evictionCopy(t *testing.T, f generationFixture, asOf time.Time) ArchiveReceipt {
	t.Helper()
	r := archiveInventory(t, f, asOf)
	out, err := CopyGenerationArchive(context.Background(), f.receiver, archiveDirectory(t), r.SnapshotSHA256, asOf)
	if err != nil {
		t.Fatal(err)
	}
	return out
}

func evictionCount(t *testing.T, db *sql.DB, table string) int {
	t.Helper()
	allowed := map[string]bool{"portal_monthly_generation_inputs": true, "portal_monthly_generation_archives": true,
		"portal_monthly_generation_release_permits": true, "portal_monthly_draft_tasks": true}
	if !allowed[table] {
		t.Fatal("unknown fixture table")
	}
	var count int
	if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&count); err != nil {
		t.Fatal(err)
	}
	return count
}

func TestEvictionRoundtripKeepsTasksMoneyAndInvalidation(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	j := retentionReady(t, f, 1)
	if err := f.g.tasks.Invalidate(ctx, j.Request.Owner, j.ID); err != nil {
		t.Fatal(err)
	}
	j, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil {
		t.Fatal(err)
	}
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	var plan, input, artifact []byte
	if err := f.receiver.QueryRow(`SELECT plan,preparation,artifact FROM portal_monthly_generation_inputs WHERE id=?`, j.Request.InputSHA256).Scan(&plan, &input, &artifact); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("eviction without opt-in", err)
	}
	for range 2 {
		if err := InitGenerationRetention(ctx, f.receiver); err != nil {
			t.Fatal(err)
		}
	}
	// Ordinary generation initialization must preserve the explicit retention mode.
	if err := InitGenerationReceiver(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, false); err != nil {
			t.Fatal(err)
		}
	}
	if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 0 || evictionCount(t, f.receiver, "portal_monthly_generation_archives") != 1 ||
		evictionCount(t, f.receiver, "portal_monthly_generation_release_permits") != 0 || evictionCount(t, f.receiver, "portal_monthly_draft_tasks") != 1 {
		t.Fatal("unexpected eviction state")
	}
	r := archiveInventory(t, f, asOf)
	if r.ArchivedInputs != 1 || r.ReviewRequired != 0 || r.InputCount != 0 {
		t.Fatal("archived payload mistaken for missing input", r)
	}
	retentionItem(t, r, j, "archived_payload")
	if _, err := loadGeneration(ctx, f.receiver, j.Request, time.Now()); !errors.Is(err, ErrGenerationArchived) {
		t.Fatal("missing archived payload not identified", err)
	}
	if err := generationWrite(ctx, f.receiver, func(ctx context.Context, tx *sql.Tx) error {
		return retainGenerationPlan(ctx, tx, plan, input, false)
	}); !errors.Is(err, ErrGenerationArchived) {
		t.Fatal("admission recreated archived input", err)
	}
	if _, err := f.g.Submit(ctx, f.plan, f.raw); !errors.Is(err, monthlytasks.ErrStale) {
		t.Fatal("invalidated task resurrected", err)
	}
	for range 2 {
		if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, true); err != nil {
			t.Fatal("restore", err)
		}
	}
	saved, err := loadGeneration(ctx, f.receiver, j.Request, time.Now())
	if err != nil || !bytes.Equal(plan, saved.planRaw) || !bytes.Equal(input, saved.inputRaw) || !bytes.Equal(artifact, saved.artifact) {
		t.Fatal("restored bytes differ", err)
	}
	a, err := decodeGenerationArtifact(saved)
	if err != nil {
		t.Fatal(err)
	}
	assertGenerationAmounts(t, a)
	after, err := f.g.tasks.Get(ctx, j.Request.Owner, j.ID)
	if err != nil || !reflect.DeepEqual(j, after) {
		t.Fatal("restore changed task state", err)
	}
	if _, err := f.g.Run(ctx, j.Request.Owner, j.ID); err == nil {
		t.Fatal("restoration activated invalidated task")
	}
	if _, err := f.receiver.Exec(`DELETE FROM portal_monthly_generation_inputs WHERE id=?`, j.Request.InputSHA256); err == nil {
		t.Fatal("permanent reference incorrectly authorizes unverified direct deletion")
	}
	source, err := os.ReadFile(f.path)
	if err != nil || generationHash(source) != f.sourceHash {
		t.Fatal("funding evidence changed", err)
	}
}

func TestEvictionAtomicRollbackAndArchiveDamage(t *testing.T) {
	f := generationFixtureNew(t)
	j := archiveInvalidated(t, f)
	ctx := context.Background()
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	if err := InitGenerationRetention(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	// Fail after reference creation and before DELETE can complete.
	if _, err := f.receiver.Exec(`CREATE TRIGGER injected_release_failure BEFORE DELETE ON portal_monthly_generation_inputs BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, false); err == nil {
		t.Fatal("injected failure ignored")
	}
	if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 || evictionCount(t, f.receiver, "portal_monthly_generation_archives") != 0 || evictionCount(t, f.receiver, "portal_monthly_generation_release_permits") != 0 {
		t.Fatal("partial transaction committed")
	}
	if _, err := f.receiver.Exec(`DROP TRIGGER injected_release_failure`); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, false); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(receipt.Directory, j.Request.InputSHA256+".preparation")
	original, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("damaged"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, true); err == nil {
		t.Fatal("damaged copy restored")
	}
	if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 0 {
		t.Fatal("failed restore partially inserted input")
	}
	if err := os.WriteFile(path, original, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := f.receiver.Exec(`CREATE TRIGGER injected_restore_failure BEFORE INSERT ON portal_monthly_generation_inputs BEGIN SELECT RAISE(ABORT,'injected failure'); END`); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, true); err == nil {
		t.Fatal("restore failure ignored")
	}
	if _, err := f.receiver.Exec(`DROP TRIGGER injected_restore_failure`); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, true); err != nil {
		t.Fatal("recovery after failed restore", err)
	}
}

func TestEvictionProtectsReadyAndChangedTaskState(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	old := retentionReady(t, f, 1)
	retentionReady(t, f, 2)
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	if err := InitGenerationRetention(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("superseded completed draft released", err)
	}
	if err := f.g.tasks.Invalidate(ctx, old.Request.Owner, old.ID); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, old.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationChanged) {
		t.Fatal("changed task accepted with old archive", err)
	}
	if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 2 || evictionCount(t, f.receiver, "portal_monthly_generation_archives") != 0 {
		t.Fatal("protected data changed")
	}
}

func TestEvictionReleasesCapacityAndRefusesOverfullRestore(t *testing.T) {
	f := generationFixtureNew(t)
	ctx := context.Background()
	var first monthlytasks.Task
	for i := range generationRecordLimit {
		f.plan.Owner.AuthVersion = int64(i + 1)
		j := archiveInvalidated(t, f)
		if i == 0 {
			first = j
		}
	}
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	if err := InitGenerationRetention(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	f.plan.Owner.AuthVersion = 33
	if _, err := f.g.Submit(ctx, f.plan, f.raw); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("fixture should initially be full", err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, first.Request.InputSHA256, asOf, false); err != nil {
		t.Fatal(err)
	}
	added := archiveInvalidated(t, f)
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, first.Request.InputSHA256, asOf, true); !errors.Is(err, ErrGenerationCapacity) {
		t.Fatal("restore bypassed capacity", err)
	}
	newReceipt := evictionCopy(t, f, asOf)
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, newReceipt, added.Request.InputSHA256, asOf, false); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, first.Request.InputSHA256, asOf, true); err != nil {
		t.Fatal("restore after releasing a slot", err)
	}
	if evictionCount(t, f.receiver, "portal_monthly_draft_tasks") != 33 || evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 32 {
		t.Fatal("capacity or tombstones changed unexpectedly")
	}
}

func TestEvictionCommitFailureNeverExposesHalfAppliedChange(t *testing.T) {
	for _, restore := range []bool{false, true} {
		name, action := "release", "DELETE"
		if restore {
			name, action = "restore", "INSERT"
		}
		t.Run(name, func(t *testing.T) {
			f := generationFixtureNew(t)
			j := archiveInvalidated(t, f)
			ctx := context.Background()
			asOf := time.Now().Add(45 * 24 * time.Hour)
			receipt := evictionCopy(t, f, asOf)
			if err := InitGenerationRetention(ctx, f.receiver); err != nil {
				t.Fatal(err)
			}
			if restore {
				if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, false); err != nil {
					t.Fatal(err)
				}
			}
			before := archiveInventory(t, f, asOf)
			f.receiver.SetMaxOpenConns(1)
			for _, query := range []string{
				`PRAGMA foreign_keys=ON`,
				`CREATE TABLE eviction_parent(id INTEGER PRIMARY KEY)`,
				`CREATE TABLE eviction_deferred(parent_id INTEGER REFERENCES eviction_parent(id) DEFERRABLE INITIALLY DEFERRED)`,
				`CREATE TRIGGER eviction_commit_failure AFTER ` + action + ` ON portal_monthly_generation_inputs BEGIN INSERT INTO eviction_deferred(parent_id) VALUES(99); END`,
			} {
				if _, err := f.receiver.Exec(query); err != nil {
					t.Fatal(err)
				}
			}
			if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, restore); err == nil {
				t.Fatal("failed COMMIT returned success")
			}
			after := archiveInventory(t, f, asOf)
			if !reflect.DeepEqual(before, after) || evictionCount(t, f.receiver, "portal_monthly_generation_release_permits") != 0 {
				t.Fatal("failed COMMIT left half-applied state or poisoned connection")
			}
			if _, err := f.receiver.Exec(`DROP TRIGGER eviction_commit_failure`); err != nil {
				t.Fatal(err)
			}
			if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, restore); err != nil {
				t.Fatal("retry after commit failure", err)
			}
		})
	}
}

func TestEvictionRejectsUnrecognizedGuardAndRecentRelease(t *testing.T) {
	f := generationFixtureNew(t)
	j := archiveInvalidated(t, f)
	ctx := context.Background()
	asOf := time.Now().Add(45 * 24 * time.Hour)
	receipt := evictionCopy(t, f, asOf)
	if err := InitGenerationRetention(ctx, f.receiver); err != nil {
		t.Fatal(err)
	}
	if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, time.Now(), false); !errors.Is(err, ErrGenerationInput) {
		t.Fatal("archive's future timestamp bypassed fresh age check", err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := ChangeArchivedGenerationPayload(cancelled, f.receiver, receipt, j.Request.InputSHA256, asOf, false); !errors.Is(err, context.Canceled) {
		t.Fatal("cancellation ignored", err)
	}
	for _, query := range []string{
		`DROP TRIGGER monthly_generation_no_delete`,
		`CREATE TRIGGER monthly_generation_no_delete BEFORE DELETE ON portal_monthly_generation_inputs BEGIN SELECT RAISE(ABORT,'custom guard'); END`,
	} {
		if _, err := f.receiver.Exec(query); err != nil {
			t.Fatal(err)
		}
	}
	if err := InitGenerationRetention(ctx, f.receiver); !errors.Is(err, ErrGenerationIntegrity) {
		t.Fatal("unknown guard replaced", err)
	}
	if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 || evictionCount(t, f.receiver, "portal_monthly_generation_archives") != 0 {
		t.Fatal("rejected operations changed payload")
	}
}

func TestEvictionDoesNotConfuseOversizeOrEmptyArtifactWithSQLNull(t *testing.T) {
	for _, size := range []int{0, generationArtifactLimit + 1} {
		f := generationFixtureNew(t)
		j := archiveInvalidated(t, f)
		ctx := context.Background()
		asOf := time.Now().Add(45 * 24 * time.Hour)
		receipt := evictionCopy(t, f, asOf)
		if err := InitGenerationRetention(ctx, f.receiver); err != nil {
			t.Fatal(err)
		}
		f.receiver.SetMaxOpenConns(1)
		if _, err := f.receiver.Exec(`PRAGMA ignore_check_constraints=ON`); err != nil {
			t.Fatal(err)
		}
		if _, err := f.receiver.Exec(`UPDATE portal_monthly_generation_inputs SET artifact=zeroblob(?) WHERE id=?`, size, j.Request.InputSHA256); err != nil {
			t.Fatal(err)
		}
		if err := ChangeArchivedGenerationPayload(ctx, f.receiver, receipt, j.Request.InputSHA256, asOf, false); !errors.Is(err, ErrGenerationIntegrity) {
			t.Fatal("corrupt non-null artifact mistaken for archived SQL NULL", size, err)
		}
		if evictionCount(t, f.receiver, "portal_monthly_generation_inputs") != 1 || evictionCount(t, f.receiver, "portal_monthly_generation_archives") != 0 {
			t.Fatal("unmatched corrupt input released")
		}
	}
}
