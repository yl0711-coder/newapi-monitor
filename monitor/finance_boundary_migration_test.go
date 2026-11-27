package monitor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	glebarez "github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Exercise the actual v68 -> v69 index upgrade, not just the test helper which
// derives older v67 plans. Persistent paired files are needed for WAL/restore.
func TestFinanceGiftBoundaryIndexPinsV68RollbackBeforeV69(t *testing.T) {
	for _, combined := range []bool{false, true} {
		name, plan := "base", preMigrationPlanID
		if combined {
			name, plan = "source-v2", preMigrationCombinedPlanID
		}
		t.Run(name, func(t *testing.T) {
			const prefix = "main-facts-schema-20261126-v69-finance-gift-boundary-range-from-v68-"
			if !strings.HasPrefix(plan, prefix) {
				t.Fatal("index migration must advance both exact v68 families")
			}
			previous := "main-facts-schema-20261125-v68-" + strings.TrimPrefix(plan, prefix)
			dir := t.TempDir()
			mainPath, factsPath := filepath.Join(dir, "monitor.db"), filepath.Join(dir, "facts.db")
			main := createLegacyWALStore(t, mainPath, "CREATE TABLE legacy_index_marker(id INTEGER PRIMARY KEY,value INTEGER)", "INSERT INTO legacy_index_marker VALUES(1,1)")
			facts := createLegacyWALStore(t, factsPath, "CREATE TABLE legacy_index_marker(id INTEGER PRIMARY KEY,value INTEGER)", "INSERT INTO legacy_index_marker VALUES(1,1)")
			legacy, err := gorm.Open(glebarez.Dialector{Conn: facts}, &gorm.Config{})
			if err != nil {
				t.Fatal(err)
			}
			if err = legacy.AutoMigrate(&FinanceGiftBoundaryState{}); err != nil {
				t.Fatal(err)
			}
			if err = legacy.Exec("DROP INDEX idx_finance_gift_state_hour_user").Error; err != nil {
				t.Fatal(err)
			}
			if err = legacy.Create(&FinanceGiftBoundaryState{SourceEpoch: "original", HourTs: 3600, UserID: 31, Status: "complete", Rows: 3, ConsumeQuota: 11}).Error; err != nil {
				t.Fatal(err)
			}
			cfg := Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: filepath.Join(dir, "backups"), StoreMigrationBackupRetention: 3, LocalSnapshotOnly: true, NginxSourceV2Enabled: combined}
			m := &Monitor{cfg: cfg}
			t.Cleanup(m.Close)
			prior, err := m.createPreMigrationSnapshot(t.Context(), mainPath, factsPath, time.Now().Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			rewritePreMigrationSnapshotPlanForTest(t, cfg.StoreBackupDir, mainPath, factsPath, prior.SnapshotDir, previous)
			if _, err = main.Exec("UPDATE legacy_index_marker SET value=2"); err != nil {
				t.Fatal(err)
			}
			if _, err = facts.Exec("UPDATE finance_gift_boundary_states SET rows=4;UPDATE legacy_index_marker SET value=2"); err != nil {
				t.Fatal(err)
			}
			if err = m.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			if !m.usageFactsDB.Migrator().HasIndex(&FinanceGiftBoundaryState{}, "idx_finance_gift_state_hour_user") {
				t.Fatal("upgraded range index missing")
			}
			dirs := preMigrationSnapshotDirs(t, cfg.StoreBackupDir)
			if len(dirs) != 2 {
				t.Fatal("v69 must pin a fresh pair before adding the index", dirs)
			}
			var current string
			for _, d := range dirs {
				manifest, _, err := loadAndVerifyPreMigrationSnapshot(t.Context(), d)
				if err != nil {
					t.Fatal(err)
				}
				if manifest.MigrationPlan == plan {
					current = d
				}
			}
			if current == "" || current == prior.SnapshotDir {
				t.Fatal("old rollback pair reused")
			}
			fresh := openReadOnlyTestStore(t, filepath.Join(current, preMigrationFactsSnapshotName))
			var indexes, rows int
			if fresh.QueryRow("SELECT COUNT(*) FROM sqlite_master WHERE type='index' AND name='idx_finance_gift_state_hour_user'").Scan(&indexes) != nil || indexes != 0 {
				t.Fatal("backup taken after index migration")
			}
			if fresh.QueryRow("SELECT rows FROM finance_gift_boundary_states WHERE source_epoch='original' AND hour_ts=3600 AND user_id=31").Scan(&rows) != nil || rows != 4 {
				t.Fatal("fresh WAL content lost")
			}
			old := openReadOnlyTestStore(t, filepath.Join(prior.SnapshotDir, preMigrationFactsSnapshotName))
			if old.QueryRow("SELECT rows FROM finance_gift_boundary_states WHERE source_epoch='original' AND hour_ts=3600 AND user_id=31").Scan(&rows) != nil || rows != 3 {
				t.Fatal("old snapshot overwritten")
			}
			// An old-image SELECT still reads the same fields after the additive
			// index migration, without any new source-epoch predicate.
			if facts.QueryRow("SELECT rows FROM finance_gift_boundary_states WHERE hour_ts>=3600 AND hour_ts<7200 AND user_id=31").Scan(&rows) != nil || rows != 4 {
				t.Fatal("old read incompatible with upgraded file")
			}
			restored := filepath.Join(dir, "restored")
			if err = RestorePreMigrationSnapshot(context.Background(), current, restored); err != nil {
				t.Fatal(err)
			}
			for _, file := range []string{"monitor.db", "facts.db"} {
				db := openReadOnlyTestStore(t, filepath.Join(restored, file))
				var value int
				if db.QueryRow("SELECT value FROM legacy_index_marker WHERE id=1").Scan(&value) != nil || value != 2 {
					t.Fatal("paired restore lost latest data", file)
				}
			}
			m.Close()
			next := &Monitor{cfg: cfg}
			t.Cleanup(next.Close)
			if err = next.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			if len(preMigrationSnapshotDirs(t, cfg.StoreBackupDir)) != 2 {
				t.Fatal("same-plan restart created another snapshot")
			}
		})
	}
}
