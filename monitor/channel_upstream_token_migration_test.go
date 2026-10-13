package monitor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestUpstreamTokenMigrationPinsFreshDualStoreRollbackPoint(t *testing.T) {
	for _, sourceV2 := range []bool{false, true} {
		name, plan := "base", preMigrationPlanID
		if sourceV2 {
			name, plan = "source-v2", preMigrationCombinedPlanID
		}
		t.Run(name, func(t *testing.T) {
			if !strings.Contains(plan, "20261013-v59-upstream-token-attribution-from-v58") {
				t.Fatal("adding token tables must advance both migration plans before AutoMigrate")
			}
			priorPlan := strings.Replace(plan, "20261013-v59-upstream-token-attribution-from-v58", "20261008-v58", 1)
			dir := t.TempDir()
			mainPath, factsPath := filepath.Join(dir, "monitor.db"), filepath.Join(dir, "usage-facts.db")
			backupDir := filepath.Join(dir, "backups")
			ddl := "CREATE TABLE token_rollback_guard (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
			mainWriter := createLegacyWALStore(t, mainPath, ddl, "INSERT INTO token_rollback_guard VALUES (1,1)")
			factsWriter := createLegacyWALStore(t, factsPath, ddl, "INSERT INTO token_rollback_guard VALUES (1,1)")
			cfg := Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: backupDir,
				StoreMigrationBackupRetention: 3, LocalSnapshotOnly: true, NginxSourceV2Enabled: sourceV2}
			m := &Monitor{cfg: cfg}
			prior, err := m.createPreMigrationSnapshot(context.Background(), mainPath, factsPath, time.Now().Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			rewritePreMigrationSnapshotPlanForTest(t, backupDir, mainPath, factsPath, prior.SnapshotDir, priorPlan)
			if _, err := mainWriter.Exec("UPDATE token_rollback_guard SET value=2"); err != nil {
				t.Fatal(err)
			}
			if _, err := factsWriter.Exec("UPDATE token_rollback_guard SET value=2"); err != nil {
				t.Fatal(err)
			}
			if err := m.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			snapshots := preMigrationSnapshotDirs(t, backupDir)
			if len(snapshots) != 2 {
				t.Fatalf("must retain old point and create current upgrade point: %d", len(snapshots))
			}
			var current string
			for _, snapshot := range snapshots {
				manifest, _, err := loadAndVerifyPreMigrationSnapshot(context.Background(), snapshot)
				if err != nil {
					t.Fatal(err)
				}
				if manifest.MigrationPlan == plan {
					current = snapshot
				}
			}
			if current == "" || current == prior.SnapshotDir {
				t.Fatal("token upgrade reused stale pre-v59 data")
			}
			for _, filename := range []string{preMigrationMainSnapshotName, preMigrationFactsSnapshotName} {
				fresh := openReadOnlyTestStore(t, filepath.Join(current, filename))
				old := openReadOnlyTestStore(t, filepath.Join(prior.SnapshotDir, filename))
				var latest, earlier int
				if err := fresh.QueryRow("SELECT value FROM token_rollback_guard WHERE id=1").Scan(&latest); err != nil || latest != 2 {
					t.Fatal("fresh paired snapshot omitted latest committed WAL", latest, err)
				}
				if err := old.QueryRow("SELECT value FROM token_rollback_guard WHERE id=1").Scan(&earlier); err != nil || earlier != 1 {
					t.Fatal("prior rollback point changed", earlier, err)
				}
			}
			snapshotMain := openReadOnlyTestStore(t, filepath.Join(current, preMigrationMainSnapshotName))
			for _, table := range []string{"channel_upstream_token_configs", "channel_upstream_token_scan_checkpoints", "channel_upstream_token_work_states"} {
				if sqliteHasTable(t, snapshotMain, table) || !sqliteHasTable(t, mainWriter, table) {
					t.Fatalf("table %s must be added only after the fresh backup", table)
				}
			}
			restored := filepath.Join(dir, "restored-private-pair")
			if err := RestorePreMigrationSnapshot(context.Background(), current, restored); err != nil {
				t.Fatal(err)
			}
			for _, filename := range []string{filepath.Base(mainPath), filepath.Base(factsPath)} {
				db := openReadOnlyTestStore(t, filepath.Join(restored, filename))
				var latest int
				if err := db.QueryRow("SELECT value FROM token_rollback_guard WHERE id=1").Scan(&latest); err != nil || latest != 2 {
					t.Fatal("restore lost latest committed value", latest, err)
				}
			}
			m.Close()
			restarted := &Monitor{cfg: cfg}
			if err := restarted.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if got := len(preMigrationSnapshotDirs(t, backupDir)); got != 2 {
				t.Fatal("same-plan restart must reuse the original rollback point", got)
			}
		})
	}
}

func TestUpstreamTokenMigrationBackupFailureDoesNotAddTables(t *testing.T) {
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	old := createLegacyWALStore(t, mainPath, "CREATE TABLE token_rollback_guard (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)", "INSERT INTO token_rollback_guard VALUES (1,7)")
	blocked := filepath.Join(dir, "not-a-backup-directory")
	if err := os.WriteFile(blocked, []byte("synthetic backup obstruction"), 0o600); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: Settings{LocalSnapshotOnly: true, StoreBackupDir: blocked}}
	if err := m.openStore(mainPath); err == nil {
		m.Close()
		t.Fatal("backup failure must stop startup before schema writes")
	}
	for _, table := range []string{"channel_upstream_token_configs", "channel_upstream_token_scan_checkpoints", "channel_upstream_token_work_states"} {
		if sqliteHasTable(t, old, table) {
			t.Fatal("unprotected migration added table", table)
		}
	}
	var sentinel int
	if err := old.QueryRow("SELECT value FROM token_rollback_guard WHERE id=1").Scan(&sentinel); err != nil || sentinel != 7 {
		t.Fatal("backup failure changed original data", sentinel, err)
	}
}
