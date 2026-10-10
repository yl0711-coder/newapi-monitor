package monitor

import (
	"context"
	"path/filepath"
	"testing"

	"gorm.io/gorm/schema"
)

func TestEvidenceStoreProofMigrationBacksUpOldCursorBeforeAddingColumns(t *testing.T) {
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	backupDir := filepath.Join(dir, "backups")
	table := (schema.NamingStrategy{}).TableName("CloudWatchNginxCursor")
	old := createLegacyWALStore(t, mainPath,
		"CREATE TABLE "+quoteSQLiteIdentifier(table)+" (id INTEGER PRIMARY KEY, semantics_version INTEGER, evidence_through_ts INTEGER)",
		"INSERT INTO "+quoteSQLiteIdentifier(table)+" (id,semantics_version,evidence_through_ts) VALUES (1,1,1800000000)")
	m := &Monitor{cfg: Settings{StoreBackupDir: backupDir, StoreMigrationBackupRetention: 3}}
	if err := m.openStore(mainPath); err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	snapshots := preMigrationSnapshotDirs(t, backupDir)
	if len(snapshots) != 1 {
		t.Fatalf("evidence cursor upgrade must pin one pre-migration snapshot: %v", snapshots)
	}
	manifest, _, err := loadAndVerifyPreMigrationSnapshot(context.Background(), snapshots[0])
	if err != nil || manifest.MigrationPlan != preMigrationPlanID {
		t.Fatalf("wrong migration backup: plan=%q err=%v", manifest.MigrationPlan, err)
	}
	snapshot := openReadOnlyTestStore(t, filepath.Join(snapshots[0], preMigrationMainSnapshotName))
	for _, column := range []string{"evidence_store_id", "evidence_gap_from_ts", "evidence_gap_to_ts", "evidence_gap_detected_at", "evidence_gap_reason"} {
		if sqliteHasColumn(t, snapshot, table, column) || !sqliteHasColumn(t, old, table, column) {
			t.Fatalf("column %s was not added strictly after the rollback snapshot", column)
		}
	}
	var through int64
	if err := snapshot.QueryRow("SELECT evidence_through_ts FROM " + quoteSQLiteIdentifier(table) + " WHERE id = 1").Scan(&through); err != nil || through != 1800000000 {
		t.Fatalf("pre-migration backup lost the legacy cursor: through=%d err=%v", through, err)
	}
}
