package monitor

import (
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestTokenPendingMigrationBoundedRollbackRestartAndLiveFacts(t *testing.T) {
	m, now, path := tokenMeteringFixture(t)
	if err := m.storeDB.AutoMigrate(&TokenDetectionCurrent{}, &TokenDetectionRetry{}); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 70; id++ {
		detectionFact(t, m, id, now-300, 3, 20000, 0, 0)
	}
	db := m.storeDB
	if err := db.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal(err)
	}
	check := func(db *gorm.DB, cursor, count int64, ready bool) {
		t.Helper()
		var state struct {
			CursorID int64
			Ready    bool
		}
		if err := db.Table(tokenPendingMigrationTable).Where("id=1").Take(&state).Error; err != nil || state.CursorID != cursor || state.Ready != ready {
			t.Fatalf("checkpoint=%+v want cursor=%d ready=%t: %v", state, cursor, ready, err)
		}
		var members, groups int64
		if err := db.Table(tokenReportPendingMembers).Count(&members).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table(tokenReportPendingGroups).Select("COALESCE(SUM(requests),0)").Scan(&groups).Error; err != nil || members != count || groups != count {
			t.Fatalf("members=%d groups=%d want=%d: %v", members, groups, count, err)
		}
	}
	check(db, 32, 32, false)
	// New collection is projected immediately, while historical repair is paused.
	detectionFact(t, m, 71, now-10, 3, 20000, 0, 0)
	check(db, 32, 33, false)
	if err := db.Exec(`CREATE TRIGGER fail_pending_migration BEFORE INSERT ON ` + tokenReportPendingMembers + ` WHEN NEW.log_id=40 BEGIN SELECT RAISE(ABORT,'injected migration write failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { _, _, err := migrateTokenPendingBatch(tx); return err }); err == nil {
		t.Fatal("injected failure was swallowed")
	}
	check(db, 32, 33, false)
	if err := db.Exec(`DROP TRIGGER fail_pending_migration`).Error; err != nil {
		t.Fatal(err)
	}
	// Reopen the file with a new connection; startup must resume the durable prefix.
	resumed, err := gorm.Open(sqlite.Open(path), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, _ := resumed.DB()
	defer sqlDB.Close()
	if err := resumed.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal(err)
	}
	check(resumed, 64, 65, false)
	if err := resumed.Transaction(func(tx *gorm.DB) error { _, _, err := migrateTokenPendingBatch(tx); return err }); err != nil {
		t.Fatal(err)
	}
	check(resumed, 71, 71, true)
	if err := resumed.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal(err)
	}
	check(resumed, 71, 71, true)
}

func TestTokenPendingMigrationUpgradesIntermediateEmptyHeadProjection(t *testing.T) {
	m, now, _ := detectionFixture(t)
	db := m.storeDB
	f := detectionFact(t, m, 1, now-30, 3, 20000, 0, 0)
	hash := tokenDetectionPolicyHash(detectionPolicy())
	if err := db.Create(&TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: 1, CreatedAt: f.CreatedAt, FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: hash, DetectionState: "", ReferenceGeneration: 0, SourceComplete: false}).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate the intermediate schema's stale classification with all columns present.
	if err := db.Table(tokenReportPendingMembers).Where("log_id=1").Update("base_pending", 0).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Migrator().DropTable(tokenPendingMigrationTable); err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(initTokenReportPendingIndex); err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: f.SourceEpoch, From: now - 300, To: now}
	if count, err := tokenReportPendingCount(db, q, hash); err != nil || count != 1 {
		t.Fatalf("empty head not repaired: count=%d error=%v", count, err)
	}
	q.From, q.To = now, now+300
	if count, err := tokenReportPendingCount(db, q, hash); err != nil || count != 0 {
		t.Fatalf("pending outside selected range: count=%d error=%v", count, err)
	}
}
