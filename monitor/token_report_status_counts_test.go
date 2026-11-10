package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenReportStatusRebuildCompleteWindowWithLiveChanges(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	rows := make([]TokenDetectionCurrent, 90)
	for i := range rows {
		rows[i] = TokenDetectionCurrent{SourceEpoch: epoch, LogID: int64(i + 1), CreatedAt: 300 + int64(i*6), UserID: int64(i%2 + 1), DetectionState: "evidence_insufficient", ResultJSON: `{"rules":[]}`}
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenReportStatusCounts(db, true); err != nil {
		t.Fatal(err)
	}
	verify := func(want int64) {
		t.Helper()
		q := tokenDetectionQuery{SourceEpoch: epoch, From: 300, To: 900, Status: "insufficient", Limit: 17}
		got, err := tokenReportStatusCount(db, q, "insufficient")
		if err != nil || got != want {
			t.Fatalf("whole=%d want=%d err=%v", got, want, err)
		}
		var sum int64
		for from := int64(300); from < 900; from += 200 {
			part := q
			part.From, part.To = from, from+200
			n, err := tokenReportStatusCount(db, part, "insufficient")
			if err != nil {
				t.Fatal(err)
			}
			sum += n
		}
		if sum != got {
			t.Fatalf("whole=%d parts=%d", got, sum)
		}
		var paged int64
		var cursorAt, cursorID int64
		for {
			pageQuery := tokenDetectionRange(db, q).Order("created_at,log_id").Limit(q.Limit)
			if paged > 0 {
				pageQuery = pageQuery.Where("(created_at,log_id)>(?,?)", cursorAt, cursorID)
			}
			var page []TokenDetectionCurrent
			if err := pageQuery.Find(&page).Error; err != nil {
				t.Fatal(err)
			}
			if len(page) == 0 {
				break
			}
			paged += int64(len(page))
			cursorAt, cursorID = page[len(page)-1].CreatedAt, page[len(page)-1].LogID
		}
		if paged != want {
			t.Fatalf("pagination=%d want=%d", paged, want)
		}
		var all []TokenDetectionCurrent
		if err := tokenDetectionRange(db, q).Order("created_at,log_id").Find(&all).Error; err != nil || int64(len(all)) != want {
			t.Fatalf("filtered=%d want=%d err=%v", len(all), want, err)
		}
		if report, err := m.tokenDetectionReport(context.Background(), q); err != nil {
			t.Fatal(err)
		} else if s := report["summary"].(tokenDetectionSummary); s.Insufficient != want || s.Observed != want {
			t.Fatalf("summary=%+v want=%d", s, want)
		}
		q.UserID = 1
		var filtered int64
		if err := tokenDetectionRange(db, q).Count(&filtered).Error; err != nil {
			t.Fatal(err)
		}
		if n, err := tokenReportStatusCount(db, q, "insufficient"); err != nil || n != filtered {
			t.Fatalf("identity=%d want=%d err=%v", n, filtered, err)
		}
	}
	verify(90)
	if err := db.Transaction(func(tx *gorm.DB) error { _, _, err := migrateTokenStatusCounts(tx); return err }); err != nil {
		t.Fatal(err)
	}
	verify(90)
	if err := db.Create(&TokenDetectionCurrent{SourceEpoch: epoch, LogID: 91, CreatedAt: 850, UserID: 1, DetectionState: "evidence_insufficient", ResultJSON: `{"rules":[]}`}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=2", epoch).Update("candidate", true).Error; err != nil {
		t.Fatal(err)
	}
	verify(90)
	for i := 0; i < 4; i++ {
		if err := db.Transaction(func(tx *gorm.DB) error { _, _, err := migrateTokenStatusCounts(tx); return err }); err != nil {
			t.Fatal(err)
		}
	}
	verify(90)
	// A projection being rebuilt must not hide heads that have not been visited.
	if err := db.Exec("UPDATE token_report_state_migration_v1 SET ready=0,cursor_id=32 WHERE id=1").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM " + tokenReportIndexTable + " WHERE log_id>32").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("DELETE FROM " + tokenReportSummaryTable + " WHERE log_id>32").Error; err != nil {
		t.Fatal(err)
	}
	verify(90)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := tokenReportStatusCount(db.WithContext(ctx), tokenDetectionQuery{SourceEpoch: epoch, From: 300, To: 900}, "insufficient"); err == nil {
		t.Fatal("cancelled count returned a total")
	}
}

func TestTokenReportCountUnavailableIsNull(t *testing.T) {
	body, err := json.Marshal(tokenDetectionSummary{StatusCountUnavailable: errTokenReportCountNotReady.Error()})
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	if value["insufficient_requests"] != nil || value["status_count_unavailable"] == "" {
		t.Fatalf("unknown count masquerades as zero: %s", body)
	}
}

func TestTokenReportTruncatedSummaryNeverPublishesPartialTotals(t *testing.T) {
	s := tokenDetectionSummary{Truncated: true, Observed: 5000, Candidates: 7, Insufficient: 300,
		Amounts: map[string]tokenDetectionAmounts{"partial": {Count: 7}}, RuleHits: map[string]int64{"usage_format": 7},
		RuleEligible: map[string]int64{"usage_format": 5000}, Dimensions: map[string]int{"user": 3}}
	body, err := json.Marshal(s)
	if err != nil {
		t.Fatal(err)
	}
	var value map[string]any
	if err = json.Unmarshal(body, &value); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"observed_results", "candidate_requests", "insufficient_requests", "amounts", "rule_eligible", "rule_hits", "unique_candidate_dimensions"} {
		v, exists := value[key]
		if !exists || v != nil {
			t.Fatalf("partial %s published: %s", key, body)
		}
	}
}

func TestTokenReportRebuildingCountEnforcesBudget(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	epoch := m.cfg.TokenFactsSourceEpoch
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("UPDATE " + tokenReportStatusCheckpoint + " SET ready=0 WHERE id=1").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`WITH RECURSIVE ids(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM ids WHERE n<?)
	INSERT INTO `+tokenReportIndexTable+`(source_epoch,log_id,created_at,user_id,status) SELECT ?,n,600,1,'insufficient' FROM ids`, tokenReportCountLimit+1, epoch).Error; err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: 600, To: 900}
	if count, err := tokenReportStatusCount(db, q, "insufficient"); count != 0 || !errors.Is(err, errTokenReportCountNotReady) {
		t.Fatal("partial count published", count, err)
	}
	if err := db.Exec("DELETE FROM "+tokenReportIndexTable+" WHERE log_id=?", tokenReportCountLimit+1).Error; err != nil {
		t.Fatal(err)
	}
	if count, err := tokenReportStatusCount(db, q, "insufficient"); count != tokenReportCountLimit || err != nil {
		t.Fatal(count, err)
	}
}

func TestTokenReportStatusCountsBoundariesMutationsAndRestart(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	epoch := m.cfg.TokenFactsSourceEpoch
	for id, at := range []int64{300, 301, 599, 600, 899, 900, 901, 1199, 1200} {
		row := TokenDetectionCurrent{SourceEpoch: epoch, LogID: int64(id + 1), CreatedAt: at, UserID: int64(id % 2), SourceComplete: false, DetectionState: "evaluated", ResultJSON: `{"rules":[]}`}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	verify := func(conn *gorm.DB) {
		t.Helper()
		for _, limits := range [][2]int64{{300, 1200}, {301, 1199}, {599, 901}, {600, 900}, {301, 599}, {599, 600}, {300, 300}, {0, 1500}} {
			for _, filter := range []bool{false, true} {
				q := tokenDetectionQuery{SourceEpoch: epoch, From: limits[0], To: limits[1], UserSet: filter}
				for _, status := range []string{"insufficient", "not_hit", "invalid", "candidate"} {
					var want int64
					if err := tokenReportIdentityRange(conn.Table(tokenReportIndexTable), q).Where("status=?", status).Count(&want).Error; err != nil {
						t.Fatal(err)
					}
					got, err := tokenReportStatusCount(conn, q, status)
					if err != nil || got != want {
						t.Fatalf("range=%v zero-user-filter=%t status=%s got=%d want=%d err=%v", limits, filter, status, got, want, err)
					}
				}
			}
		}
	}
	verify(db)
	abort := errors.New("abort snapshot change")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Exec("UPDATE token_detection_currents SET created_at=1000,candidate=1 WHERE source_epoch=? AND log_id=2", epoch).Error; err != nil {
			return err
		}
		if err := tx.Exec("DELETE FROM token_detection_currents WHERE source_epoch=? AND log_id=5", epoch).Error; err != nil {
			return err
		}
		verify(tx)
		return abort
	}); !errors.Is(err, abort) {
		t.Fatal(err)
	}
	verify(db)
	for _, sql := range []string{
		"UPDATE token_detection_currents SET source_complete=1 WHERE log_id=2",
		"UPDATE token_detection_currents SET created_at=NULL WHERE log_id=3",
		"UPDATE token_detection_currents SET created_at=601 WHERE log_id=3",
		"UPDATE token_detection_currents SET result_json='invalid',source_complete=1 WHERE log_id=4",
		"DELETE FROM token_detection_currents WHERE log_id=5",
	} {
		if err := db.Exec(sql).Error; err != nil {
			t.Fatal(err)
		}
		verify(db)
	}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: 300, To: 1200, Status: "insufficient"}
	if _, err := tokenReportInsufficientCount(db, q); err == nil {
		t.Fatal("invalid evidence hidden by counters")
	}
	if err := initTokenDetectionReportIndex(db); err != nil {
		t.Fatal(err)
	}
	verify(db)
}

func TestTokenMeteringSchedulerProgressWhileSnapshotReadersOccupied(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	start := (now-tokenMeteringLagSeconds)/300*300 - 300
	state := TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: start, ScanThrough: start, SourceMode: "complete"}
	if err := m.tokenStore().Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	pool, err := m.tokenReadStore().DB()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	for i := 0; i < 2; i++ {
		conn, err := pool.Conn(ctx)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
	}
	failure := &tokenMeteringReadError{err: context.DeadlineExceeded, queryStarted: true}
	if delay := m.tokenMeteringRoundDelay(ctx, 1, failure, now); delay != 2*time.Second {
		t.Fatal("snapshot readers idled independent collection", delay)
	}
	w := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: start, EndAt: start + 300, NextAt: now + 30}
	if err := m.tokenStore().Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	if delay := m.tokenMeteringRoundDelay(ctx, 1, failure, now); delay > 10*time.Second || delay <= 0 {
		t.Fatal("source window backoff blocked the independent tail", delay)
	}
	if err := m.tokenStore().First(&w, "source_epoch=? AND start_at=?", state.SourceEpoch, start).Error; err != nil || w.NextAt != now+30 {
		t.Fatal("failed window lost its own backoff", w, err)
	}
}
