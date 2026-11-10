package monitor

import (
	"testing"

	"gorm.io/gorm"
)

func TestTokenReportProjectionMigrationBoundedWithLiveWritesAndRollback(t *testing.T) {
	m, now, _ := tokenMeteringFixture(t)
	db := m.storeDB
	if err := db.AutoMigrate(&TokenDetectionState{}, &TokenDetectionRetry{}, &TokenDetectionBucket{}, &TokenDetectionCurrent{}, &TokenDetectionRevision{}, &TokenDetectionReference{}); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 70; id++ {
		if err := db.Create(&TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, CreatedAt: now - 600, DetectionState: "evidence_insufficient", ResultJSON: `{"rules":[]}`}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	check := func(cursor, entries, summaries, counts int64, ready bool) {
		t.Helper()
		var state struct {
			CursorID int64
			Ready    bool
		}
		if err := db.Table("token_report_state_migration_v1").Where("id=1").Take(&state).Error; err != nil || state.CursorID != cursor || state.Ready != ready {
			t.Fatalf("projection checkpoint=%+v want=%d/%t %v", state, cursor, ready, err)
		}
		for _, item := range []struct {
			table string
			want  int64
		}{{tokenReportIndexTable, entries}, {tokenReportSummaryTable, summaries}} {
			var got int64
			if err := db.Table(item.table).Count(&got).Error; err != nil || got != item.want {
				t.Fatalf("%s count=%d want=%d %v", item.table, got, item.want, err)
			}
		}
		var got int64
		if err := db.Table(tokenReportStatusCounts).Select("COALESCE(SUM(requests),0)").Where("generation=(SELECT generation FROM " + tokenReportStatusCheckpoint + " WHERE id=1)").Scan(&got).Error; err != nil || got != counts {
			t.Fatalf("counter=%d want=%d %v", got, counts, err)
		}
	}
	check(32, 32, 32, 32, false)
	if ready, err := tokenReportProjectionsReady(db); err != nil || ready {
		t.Fatalf("partial reports marked ready=%t %v", ready, err)
	}
	// One live result is ahead of the prefix; mutation/deletion behind it must
	// update counts immediately without double counting when migration catches up.
	if err := db.Create(&TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: 71, CreatedAt: now - 10, DetectionState: "candidate", Candidate: true, ResultJSON: `{}`}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&TokenDetectionCurrent{}).Where("log_id=2").Update("candidate", true).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("log_id=3").Delete(&TokenDetectionCurrent{}).Error; err != nil {
		t.Fatal(err)
	}
	check(32, 32, 32, 31, false)
	if err := db.Exec(`CREATE TRIGGER fail_report_prefix BEFORE INSERT ON ` + tokenReportSummaryTable + ` WHEN NEW.log_id=40 BEGIN SELECT RAISE(ABORT,'injected report prefix failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Transaction(func(tx *gorm.DB) error { _, err := migrateTokenReportStates(tx); return err }); err == nil {
		t.Fatal("failed projection prefix committed")
	}
	check(32, 32, 32, 31, false)
	if err := db.Exec(`DROP TRIGGER fail_report_prefix`).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	check(64, 64, 64, 63, false)
	if err := db.Transaction(func(tx *gorm.DB) error {
		if _, err := migrateTokenReportStates(tx); err != nil {
			return err
		}
		_, _, err := migrateTokenStatusCounts(tx)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	check(71, 70, 70, 70, true)
	if ready, err := tokenReportProjectionsReady(db); err != nil || !ready {
		t.Fatalf("completed projections ready=%t %v", ready, err)
	}
	for _, status := range []string{"candidate", "insufficient"} {
		q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 900, To: now}
		var want int64
		if err := db.Table(tokenReportIndexTable).Where("status=?", status).Count(&want).Error; err != nil {
			t.Fatal(err)
		}
		if got, err := tokenReportStatusCount(db, q, status); err != nil || got != want {
			t.Fatalf("%s got=%d want=%d %v", status, got, want, err)
		}
	}
}
