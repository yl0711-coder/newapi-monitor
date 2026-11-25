package monitor

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func priorV67MigrationPlanForTest(t *testing.T, plan string) string {
	t.Helper()
	const current = "main-facts-schema-20261125-v68-upstream-acquisition-orion-checkpoint-log-pace-from-v67-"
	if !strings.HasPrefix(plan, current) {
		t.Fatal("upstream acquisition schema must advance both v67 plan families")
		return ""
	}
	return "main-facts-schema-20261109-v67-" + strings.TrimPrefix(plan, current)
}

func TestUpstreamAcquisitionMigrationPinsFreshSnapshotAndKeepsRollbackCompatible(t *testing.T) {
	for _, combined := range []bool{false, true} {
		name := "base"
		if combined {
			name = "source-v2"
		}
		t.Run(name, func(t *testing.T) {
			expectedPlan := preMigrationPlanID
			if combined {
				expectedPlan = preMigrationCombinedPlanID
			}
			previousPlan := priorV67MigrationPlanForTest(t, expectedPlan)
			if previousPlan == expectedPlan {
				t.Fatal("both migration families must advance beyond v67")
				return
			}
			dir := t.TempDir()
			mainPath := filepath.Join(dir, "monitor.db")
			factsPath := filepath.Join(dir, "facts.db")
			backups := filepath.Join(dir, "backups")
			old := createLegacyWALStore(t, mainPath,
				"CREATE TABLE upstream_host_circuits (host_key TEXT PRIMARY KEY, consecutive_failures INTEGER, open_count INTEGER, open_until INTEGER, last_failure_at INTEGER, last_status INTEGER, updated_at INTEGER)",
				"INSERT INTO upstream_host_circuits VALUES ('fixture.test:443',3,2,1800003600,1800000000,429,1800000000)")
			createLegacyWALStore(t, factsPath, "CREATE TABLE legacy_fact_marker (id INTEGER PRIMARY KEY, value TEXT)", "INSERT INTO legacy_fact_marker VALUES (1,'preserve-facts')")
			cfg := Settings{StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: backups, StoreMigrationBackupRetention: 3, NginxSourceV2Enabled: combined}
			m := &Monitor{cfg: cfg, startupTimer: newStartupTimer()}
			prior, err := m.createPreMigrationSnapshot(context.Background(), mainPath, factsPath, time.Now().Add(-time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			rewritePreMigrationSnapshotPlanForTest(t, backups, mainPath, factsPath, prior.SnapshotDir, previousPlan)
			if _, err = old.Exec("UPDATE upstream_host_circuits SET open_until=1800007200"); err != nil {
				t.Fatal(err)
			}
			if err = m.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(m.Close)
			snapshots := preMigrationSnapshotDirs(t, backups)
			if len(snapshots) != 2 {
				t.Fatalf("upgrade reused old rollback point: %v", snapshots)
			}
			current := ""
			for _, snapshot := range snapshots {
				manifest, _, verifyErr := loadAndVerifyPreMigrationSnapshot(context.Background(), snapshot)
				if verifyErr != nil {
					t.Fatal(verifyErr)
				}
				if manifest.MigrationPlan == expectedPlan {
					current = snapshot
				}
			}
			if current == "" || current == prior.SnapshotDir {
				t.Fatal("fresh upgrade snapshot not established")
				return
			}
			rollback := openReadOnlyTestStore(t, filepath.Join(current, preMigrationMainSnapshotName))
			if sqliteHasColumn(t, rollback, "upstream_host_circuits", "log_interval_ms") {
				t.Fatal("snapshot taken after schema change")
			}
			var cooldown int64
			if err = rollback.QueryRow("SELECT open_until FROM upstream_host_circuits WHERE host_key='fixture.test:443'").Scan(&cooldown); err != nil || cooldown != 1800007200 {
				t.Fatal("fresh rollback snapshot lost latest legacy cooldown", cooldown, err)
			}
			for _, column := range []string{"log_interval_ms", "log_next_start_ms", "log_successes"} {
				if !m.storeDB.Migrator().HasColumn(&UpstreamHostCircuit{}, column) {
					t.Fatal("missing upgraded pacing column", column)
				}
			}
			if !m.storeDB.Migrator().HasTable(&OrionUsageCheckpoint{}) || !m.storeDB.Migrator().HasTable(&OrionUsagePage{}) {
				t.Fatal("missing Orion staging schema")
			}
			if err = m.storeDB.Model(&UpstreamHostCircuit{}).Where("host_key=?", "fixture.test:443").Update("log_interval_ms", 30000).Error; err != nil {
				t.Fatal(err)
			}
			// Old-image SQL uses only old columns. An additive upgrade must not
			// break it or let an old-column upsert erase learned log pacing.
			if _, err = old.Exec("INSERT INTO upstream_host_circuits (host_key,consecutive_failures) VALUES ('fixture.test:443',4) ON CONFLICT(host_key) DO UPDATE SET consecutive_failures=excluded.consecutive_failures"); err != nil {
				t.Fatal(err)
			}
			var interval int64
			if err = old.QueryRow("SELECT log_interval_ms FROM upstream_host_circuits WHERE host_key='fixture.test:443'").Scan(&interval); err != nil || interval != 30000 {
				t.Fatal("legacy write erased new pacing state", interval, err)
			}
			m.Close()
			restarted := &Monitor{cfg: cfg}
			if err = restarted.openStore(mainPath); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(restarted.Close)
			if count := len(preMigrationSnapshotDirs(t, backups)); count != 2 {
				t.Fatal("same-plan restart copied databases again", count)
			}
		})
	}
}
