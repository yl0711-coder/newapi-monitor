package monitor

import (
	"context"
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestMonthlyStatementMigrationPinsLatestDualStoreRollback(t *testing.T) {
	for _, sourceV2 := range []bool{false, true} {
		name, plan := "base", preMigrationPlanID
		if sourceV2 {
			name, plan = "source-v2", preMigrationCombinedPlanID
		}
		t.Run(name, func(t *testing.T) {
			const prefix = "20261031-v60-portal-monthly-statements-from-"
			priorPlan := strings.Replace(plan, prefix, "", 1)
			if priorPlan == plan || !strings.HasPrefix(priorPlan, "main-facts-schema-20261013-v59-") {
				t.Fatal("monthly tables must advance the actual v59 migration plan")
			}
			dir := t.TempDir()
			mainPath, factsPath := filepath.Join(dir, "monitor.db"), filepath.Join(dir, "usage-facts.db")
			ddl := "CREATE TABLE monthly_rollback_guard (id INTEGER PRIMARY KEY, value INTEGER NOT NULL)"
			main := createLegacyWALStore(t, mainPath, ddl, "INSERT INTO monthly_rollback_guard VALUES (1,1)")
			facts := createLegacyWALStore(t, factsPath, ddl, "INSERT INTO monthly_rollback_guard VALUES (1,1)")
			cfg := Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: filepath.Join(dir, "backups"),
				StoreMigrationBackupRetention: 3, LocalSnapshotOnly: true, NginxSourceV2Enabled: sourceV2}
			// Protection is required even with the feature OFF: openStore still
			// adds its tables before any generation endpoint is installed.
			m := &Monitor{cfg: cfg}
			prior, err := m.createPreMigrationSnapshot(t.Context(), mainPath, factsPath, time.Now().Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			rewritePreMigrationSnapshotPlanForTest(t, cfg.StoreBackupDir, mainPath, factsPath, prior.SnapshotDir, priorPlan)
			for _, db := range []*sql.DB{main, facts} {
				if _, err := db.Exec("UPDATE monthly_rollback_guard SET value=2"); err != nil {
					t.Fatal(err)
				}
			}
			if err := m.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			defer m.Close()
			snapshots := preMigrationSnapshotDirs(t, cfg.StoreBackupDir)
			if len(snapshots) != 2 {
				t.Fatal("stale v59 rollback point was reused", len(snapshots))
			}
			var current string
			for _, snapshot := range snapshots {
				manifest, _, err := loadAndVerifyPreMigrationSnapshot(t.Context(), snapshot)
				if err != nil {
					t.Fatal(err)
				}
				if manifest.MigrationPlan == plan {
					current = snapshot
				}
			}
			if current == "" || current == prior.SnapshotDir {
				t.Fatal("missing current paired rollback point")
			}
			for _, file := range []string{preMigrationMainSnapshotName, preMigrationFactsSnapshotName} {
				old := openReadOnlyTestStore(t, filepath.Join(prior.SnapshotDir, file))
				fresh := openReadOnlyTestStore(t, filepath.Join(current, file))
				var earlier, latest int
				if old.QueryRow("SELECT value FROM monthly_rollback_guard WHERE id=1").Scan(&earlier) != nil || earlier != 1 ||
					fresh.QueryRow("SELECT value FROM monthly_rollback_guard WHERE id=1").Scan(&latest) != nil || latest != 2 {
					t.Fatal("paired snapshot lost latest committed WAL or rewrote the old point")
				}
			}
			freshMain := openReadOnlyTestStore(t, filepath.Join(current, preMigrationMainSnapshotName))
			for _, table := range []string{"portal_monthly_bills", "portal_monthly_bill_withdrawals"} {
				if sqliteHasTable(t, freshMain, table) || !sqliteHasTable(t, main, table) {
					t.Fatal("monthly table added before protected backup", table)
				}
			}
			restored := filepath.Join(dir, "restored-pair")
			if err := RestorePreMigrationSnapshot(context.Background(), current, restored); err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{filepath.Base(mainPath), filepath.Base(factsPath)} {
				db := openReadOnlyTestStore(t, filepath.Join(restored, file))
				var latest int
				if db.QueryRow("SELECT value FROM monthly_rollback_guard WHERE id=1").Scan(&latest) != nil || latest != 2 {
					t.Fatal("restore did not recover the latest paired data")
				}
			}
			m.Close()
			restarted := &Monitor{cfg: cfg}
			if err := restarted.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			defer restarted.Close()
			if len(preMigrationSnapshotDirs(t, cfg.StoreBackupDir)) != 2 {
				t.Fatal("restart replaced the pinned current rollback point")
			}
		})
	}
}

func TestMonthlyStatementMigrationBackupFailureAddsNoTables(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "monitor.db")
	db := createLegacyWALStore(t, path, "CREATE TABLE monthly_rollback_guard (value INTEGER)", "INSERT INTO monthly_rollback_guard VALUES (7)")
	blocked := filepath.Join(dir, "blocked-backup")
	if err := os.WriteFile(blocked, []byte("synthetic obstruction"), 0600); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: Settings{LocalSnapshotOnly: true, StoreBackupDir: blocked}}
	if err := m.openStore(path); err == nil {
		m.Close()
		t.Fatal("backup failure allowed migration")
	}
	for _, table := range []string{"portal_monthly_bills", "portal_monthly_bill_withdrawals"} {
		if sqliteHasTable(t, db, table) {
			t.Fatal("unprotected monthly table was added", table)
		}
	}
	var value int
	if db.QueryRow("SELECT value FROM monthly_rollback_guard").Scan(&value) != nil || value != 7 {
		t.Fatal("failed backup changed original data")
	}
}
