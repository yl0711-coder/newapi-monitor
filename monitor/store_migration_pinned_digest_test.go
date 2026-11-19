package monitor

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func pinnedDigestFixture(t *testing.T) (mainPath, factsPath, backupDir, snapshotDir string, ref preMigrationPlanReference) {
	t.Helper()
	dir := t.TempDir()
	mainPath, factsPath = filepath.Join(dir, "monitor.db"), filepath.Join(dir, "usage-facts.db")
	backupDir = filepath.Join(dir, "backups")
	main := createLegacyWALStore(t, mainPath, legacyMainSchema, "INSERT INTO legacy_guard(id,value) VALUES(1,'safe')")
	facts := createLegacyWALStore(t, factsPath, legacyFactsSchema, "INSERT INTO legacy_facts_guard(id,value) VALUES(1,'safe')")
	defer main.Close()
	defer facts.Close()
	m := &Monitor{cfg: Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: backupDir}}
	result, err := m.createPreMigrationSnapshot(t.Context(), mainPath, factsPath, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	ref, found, err := readPreMigrationPlanReference(backupDir, mainPath, factsPath)
	if err != nil || !found {
		t.Fatalf("read pinned manifest reference: found=%v err=%v", found, err)
	}
	return mainPath, factsPath, backupDir, result.SnapshotDir, ref
}

func TestPinnedSnapshotDigestReusesOnlyVerifiedBytes(t *testing.T) {
	main, facts, backups, snapshot, ref := pinnedDigestFixture(t)
	manifest, _, err := loadPreMigrationSnapshot(t.Context(), snapshot, ref.ManifestSHA256)
	if err != nil {
		t.Fatal(err)
	}
	for _, store := range manifest.Stores {
		path := filepath.Join(snapshot, store.SnapshotFile)
		if verified, err := verifyPinnedSQLiteDigest(t.Context(), path, store); err != nil || !verified {
			t.Fatalf("expected single-file digest verification for %s: %v, %v", store.Role, verified, err)
		}
	}
	path := filepath.Join(snapshot, preMigrationFactsSnapshotName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 1 // same-size corruption, not just a failed size check
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: Settings{StoreBackupDir: backups, UsageFactsStorePath: facts}}
	err = m.openStore(main)
	if err == nil || !strings.Contains(err.Error(), "哈希/大小") {
		m.Close()
		t.Fatalf("corrupt pinned bytes did not fail the pre-migration gate: %v", err)
	}
	db := openReadOnlyTestStore(t, main)
	if sqliteHasColumn(t, db, "metric_samples", "traffic_class_version") {
		t.Fatal("main database was migrated despite the invalid paired snapshot")
	}
}

func TestPinnedSnapshotDigestRequiresOriginalManifest(t *testing.T) {
	_, _, _, snapshot, ref := pinnedDigestFixture(t)
	manifest, _, err := loadAndVerifyPreMigrationSnapshot(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	manifest.Stores[0].TableRows["legacy_guard"]++ // local manifest and READY can match, pinned digest cannot
	if _, err := marshalAndSyncSnapshotManifest(snapshot, manifest); err != nil {
		t.Fatal(err)
	}
	if _, _, err := loadPreMigrationSnapshot(t.Context(), snapshot, ref.ManifestSHA256); err == nil ||
		!strings.Contains(err.Error(), "迁移计划引用与快照 manifest 不一致") {
		t.Fatalf("pinned verification accepted a rewritten manifest: %v", err)
	}
	if _, _, err := loadAndVerifyPreMigrationSnapshot(t.Context(), snapshot); err == nil ||
		!strings.Contains(err.Error(), "行数不一致") {
		t.Fatalf("standalone restore verification did not check table counts: %v", err)
	}
}

func TestPinnedSnapshotDigestFallsBackWithSQLiteSidecar(t *testing.T) {
	_, _, _, snapshot, _ := pinnedDigestFixture(t)
	manifest, _, err := loadAndVerifyPreMigrationSnapshot(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	store := manifest.Stores[0]
	path := filepath.Join(snapshot, store.SnapshotFile)
	if err := os.WriteFile(path+"-shm", nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if verified, err := verifyPinnedSQLiteDigest(t.Context(), path, store); err != nil || verified {
		t.Fatalf("sidecar did not disable hash-only validation: %v, %v", verified, err)
	}
	if err := verifyPreMigrationSnapshotMember(t.Context(), path, store, true); err != nil {
		t.Fatalf("unchanged snapshot with harmless sidecar should still pass full validation: %v", err)
	}
	store.TableRows["legacy_guard"]++
	if err := verifyPreMigrationSnapshotMember(t.Context(), path, store, true); err == nil ||
		!strings.Contains(err.Error(), "行数不一致") {
		t.Fatalf("sidecar fallback did not validate row counts: %v", err)
	}
}

func TestPinnedSnapshotDigestRejectsDataBearingWALIncludingSameRowCount(t *testing.T) {
	path := filepath.Join(t.TempDir(), "snapshot.db")
	writer := createLegacyWALStore(t, path, legacyFactsSchema,
		"INSERT INTO legacy_facts_guard(id,value) VALUES(1,'original')")
	defer writer.Close()
	if _, err := writer.ExecContext(t.Context(), "PRAGMA wal_checkpoint(TRUNCATE)"); err != nil {
		t.Fatal(err)
	}
	inventory, err := inspectSQLiteLogicalInventory(t.Context(), writer)
	if err != nil {
		t.Fatal(err)
	}
	hash, size, err := fileSHA256(path)
	if err != nil {
		t.Fatal(err)
	}
	store := preMigrationSnapshotStore{
		SizeBytes: size, FileSHA256: hash, SchemaSHA256: inventory.SchemaSHA256, TableRows: inventory.TableRows,
	}
	for _, statement := range []string{
		"UPDATE legacy_facts_guard SET value='only in WAL' WHERE id=1",
		"INSERT INTO legacy_facts_guard(id,value) VALUES(2,'only in WAL')",
	} {
		if _, err := writer.ExecContext(t.Context(), statement); err != nil {
			t.Fatal(err)
		}
		gotHash, gotSize, err := fileSHA256(path)
		if err != nil || gotHash != hash || gotSize != size {
			t.Fatalf("test requires unchanged main file plus uncheckpointed WAL: %v", err)
		}
		wal, err := os.Stat(path + "-wal")
		if err != nil || wal.Size() == 0 {
			t.Fatalf("test requires a nonempty WAL: %v", err)
		}
		for _, pinned := range []bool{true, false} {
			if err := verifyPreMigrationSnapshotMember(t.Context(), path, store, pinned); err == nil ||
				!strings.Contains(err.Error(), "异常 SQLite 边文件 -wal") {
				t.Fatalf("data-bearing WAL accepted (pinned=%v): %v", pinned, err)
			}
		}
		checked, err := preflightStartupStoreIntegrity(t.Context(), path)
		if err != nil || !checked {
			t.Fatalf("unexpected rejection on the same file when checked as a live WAL store: %v", err)
		}
	}
}

func TestPinnedSnapshotDigestBlocksStartupAndRestoreWithUnexpectedJournal(t *testing.T) {
	main, facts, backups, snapshot, _ := pinnedDigestFixture(t)
	sidecar := filepath.Join(snapshot, preMigrationFactsSnapshotName) + "-journal"
	contents := []byte("not included in the sealed facts backup")
	if err := os.WriteFile(sidecar, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(t.TempDir(), "restore")
	if err := RestorePreMigrationSnapshot(t.Context(), snapshot, target); err == nil ||
		!strings.Contains(err.Error(), "异常 SQLite 边文件 -journal") {
		t.Fatalf("unexpected journal did not block restore: %v", err)
	}
	if _, err := os.Lstat(target); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("rejected restore created a target: %v", err)
	}
	m := &Monitor{cfg: Settings{StoreBackupDir: backups, UsageFactsStorePath: facts}}
	if err := m.openStore(main); err == nil || !strings.Contains(err.Error(), "异常 SQLite 边文件 -journal") {
		m.Close()
		t.Fatalf("unexpected journal did not block startup: %v", err)
	}
	db := openReadOnlyTestStore(t, main)
	if sqliteHasColumn(t, db, "metric_samples", "traffic_class_version") {
		t.Fatal("database was migrated after failed snapshot verification")
	}
	if after, err := os.ReadFile(sidecar); err != nil || string(after) != string(contents) {
		t.Fatalf("failed verification modified its failure evidence: %v", err)
	}
}

func TestPinnedSnapshotDigestRequiresReadyAndBothMembers(t *testing.T) {
	for _, name := range []string{preMigrationReadyName, preMigrationMainSnapshotName, preMigrationFactsSnapshotName} {
		t.Run(name, func(t *testing.T) {
			main, facts, backups, snapshot, _ := pinnedDigestFixture(t)
			if err := os.Remove(filepath.Join(snapshot, name)); err != nil {
				t.Fatal(err)
			}
			m := &Monitor{cfg: Settings{StoreBackupDir: backups, UsageFactsStorePath: facts}}
			if err := m.openStore(main); err == nil {
				m.Close()
				t.Fatal("pinned verification accepted a missing artifact")
			}
			db := openReadOnlyTestStore(t, main)
			if sqliteHasColumn(t, db, "metric_samples", "traffic_class_version") {
				t.Fatal("main database was migrated despite the incomplete snapshot set")
			}
			if got := preMigrationSnapshotDirs(t, backups); len(got) != 1 || got[0] != snapshot {
				t.Fatalf("failed verification silently replaced the pinned snapshot: %v", got)
			}
		})
	}
}

func TestPinnedSnapshotDigestRejectsSymlinkAndHonorsCancellation(t *testing.T) {
	_, _, _, snapshot, _ := pinnedDigestFixture(t)
	manifest, _, err := loadAndVerifyPreMigrationSnapshot(t.Context(), snapshot)
	if err != nil {
		t.Fatal(err)
	}
	store := manifest.Stores[0]
	path := filepath.Join(snapshot, store.SnapshotFile)
	link := filepath.Join(snapshot, "link.db")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	if verified, err := verifyPinnedSQLiteDigest(t.Context(), link, store); err == nil || verified {
		t.Fatalf("pinned digest allowed a symlink: %v, %v", verified, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if verified, err := verifyPinnedSQLiteDigest(ctx, path, store); !errors.Is(err, context.Canceled) || verified {
		t.Fatalf("canceled digest read did not fail closed: %v, %v", verified, err)
	}
}

// Benchmark only a snapshot member's verifier, not an entire startup or a cold
// production disk. Both sub-benchmarks read the same small synthetic file.
func BenchmarkPinnedSnapshotMemberVerification(b *testing.B) {
	path := filepath.Join(b.TempDir(), "synthetic.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	_, err = db.ExecContext(b.Context(), `
CREATE TABLE fixture(id INTEGER PRIMARY KEY, payload BLOB);
WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i+1 FROM n WHERE i < 24000)
INSERT INTO fixture(payload) SELECT zeroblob(1024) FROM n`)
	if err != nil {
		b.Fatal(err)
	}
	inventory, err := inspectSQLiteLogicalInventory(b.Context(), db)
	if err != nil {
		b.Fatal(err)
	}
	if err := db.Close(); err != nil {
		b.Fatal(err)
	}
	hash, size, err := verifySQLiteSnapshotFile(b.Context(), path, inventory)
	if err != nil {
		b.Fatal(err)
	}
	store := preMigrationSnapshotStore{
		SizeBytes: size, FileSHA256: hash, SchemaSHA256: inventory.SchemaSHA256, TableRows: inventory.TableRows,
	}
	verified, err := verifyPinnedSQLiteDigest(b.Context(), path, store)
	if err != nil || !verified {
		b.Fatalf("benchmark fixture must qualify for digest verification: %t, %v", verified, err)
	}
	for _, mode := range []struct {
		name   string
		pinned bool
	}{{"full", false}, {"pinned_digest", true}} {
		b.Run(mode.name, func(b *testing.B) {
			b.SetBytes(size)
			for b.Loop() {
				if err := verifyPreMigrationSnapshotMember(b.Context(), path, store, mode.pinned); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
