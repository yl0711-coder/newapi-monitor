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
			const mergedPrefix = "main-facts-schema-20261109-v67-portal-monthly-statements-from-20261016-v66-token-delivery-v1-from-v65-token-detection-review-v2-from-v64-token-detection-v1-from-v63-upstream-token-attribution-from-v62-frt-sum-v1-from-v61-token-facts-v1-from-v60-incident-ingest-v1-from-v59-incident-core-v1-from-v58"
			if !strings.HasPrefix(plan, mergedPrefix) {
				t.Fatal("the merged schema must advance both parent migration plans before AutoMigrate")
			}
			suffix := strings.TrimPrefix(plan, mergedPrefix)
			for _, parent := range []struct{ name, prefix string }{
				{"local-v66", "main-facts-schema-20261016-v66-token-delivery-v1-from-v65-token-detection-review-v2-from-v64-token-detection-v1-from-v63-upstream-token-attribution-from-v62-frt-sum-v1-from-v61-token-facts-v1-from-v60-incident-ingest-v1-from-v59-incident-core-v1-from-v58"},
				{"main-monthly-v60", "main-facts-schema-20261031-v60-portal-monthly-statements-from-20261013-v59-upstream-token-attribution-from-v58"},
				{"token-detection-review-v65", "main-facts-schema-20261015-v65-token-detection-review-v2-from-v64-token-detection-v1-from-v63-upstream-token-attribution-from-v62-frt-sum-v1-from-v61-token-facts-v1-from-v60-incident-ingest-v1-from-v59-incident-core-v1-from-v58"},
				{"token-detection-v64", "main-facts-schema-20261015-v64-token-detection-v1-from-v63-upstream-token-attribution-from-v62-frt-sum-v1-from-v61-token-facts-v1-from-v60-incident-ingest-v1-from-v59-incident-core-v1-from-v58"},
				{"merged-v63", "main-facts-schema-20261015-v63-upstream-token-attribution-from-v62-frt-sum-v1-from-v61-token-facts-v1-from-v60-incident-ingest-v1-from-v59-incident-core-v1-from-v58"},
				{"common-v58", "main-facts-schema-20261008-v58"},
				{"main-v59", "main-facts-schema-20261013-v59-upstream-token-attribution-from-v58"},
				{"local-v62", "main-facts-schema-20261015-v62-frt-sum-v1-from-v61-token-facts-v1-from-v60-incident-ingest-v1-from-v59-incident-core-v1-from-v58"},
			} {
				t.Run(parent.name, func(t *testing.T) {
					parentSuffix := suffix
					if parent.name == "main-monthly-v60" || parent.name == "main-v59" || parent.name == "common-v58" {
						parentSuffix = strings.SplitN(suffix, "-frt-sum-recompute-v2", 2)[0]
					}
					priorPlan := parent.prefix + parentSuffix
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
						t.Fatal("merged upgrade reused a parent plan's stale rollback data")
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
					for _, table := range []string{"channel_upstream_token_configs", "channel_upstream_token_scan_checkpoints", "channel_upstream_token_work_states", "incidents", "incident_ingest_cursors", "token_metering_facts", "token_detection_currents", "token_detection_revisions", "token_detection_references", "token_detection_states", "token_detection_buckets", "token_evidence_exports", "token_delivery_audits", "token_notify_outboxes", "token_notify_targets", "token_delivery_states", "portal_monthly_bills", "portal_monthly_bill_withdrawals"} {
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
