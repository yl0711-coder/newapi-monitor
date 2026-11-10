package monitor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type tokenSummaryReadLogger struct {
	logger.Interface
	reads *atomic.Int64
}

func (l tokenSummaryReadLogger) Trace(_ context.Context, _ time.Time, query func() (string, int64), _ error) {
	q, _ := query()
	if strings.HasPrefix(q, "SELECT source_epoch,log_id,user_id,channel_id,model,user_group,created_at,result_json FROM "+tokenReportSummaryTable) {
		l.reads.Add(1)
	}
}

func TestTokenReportSummaryCacheInvalidationAndReadSnapshot(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	body := `{"candidate":false,"source_complete":true,"rules":[]}`
	head := TokenDetectionCurrent{SourceEpoch: epoch, LogID: 1, CreatedAt: now - 1, UserID: 2, ChannelID: 3, SourceComplete: true, DetectionState: "not_hit", ResultJSON: body}
	if err := db.Create(&head).Error; err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	m.tokenReadStore().Config.Logger = tokenSummaryReadLogger{Interface: logger.Default, reads: &reads}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: now - 10, To: now + 400, Limit: 25}
	read := func(scope tokenDetectionQuery) tokenReportAggregates {
		t.Helper()
		var got tokenReportAggregates
		if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
			var err error
			got.Summary, got.Analysis, err = m.tokenDetectionReportAggregates(tx, scope, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
		var heads []TokenDetectionCurrent
		if err := tokenDetectionRange(db, scope).Order("created_at,log_id").Limit(5001).Find(&heads).Error; err != nil {
			t.Fatal(err)
		}
		wantSummary, e1 := tokenDetectionSummarize(heads)
		wantAnalysis, e2 := tokenReportAnalyze(heads)
		if e1 != nil || e2 != nil || !reflect.DeepEqual(got.Summary, wantSummary) || !reflect.DeepEqual(got.Analysis, wantAnalysis) {
			t.Fatal("cached aggregates differ from current authoritative results", e1, e2)
		}
		return got
	}
	first := read(q)
	first.Summary.RuleHits["response-only"] = 100
	first.Summary.Gaps = append(first.Summary.Gaps, "response-only")
	page := q
	page.AfterAt, page.AfterID, page.Limit = now-1, 1, 50
	read(page)
	if reads.Load() != 1 {
		t.Fatal("pagination reloaded unchanged summary rows", reads.Load())
	}
	// A new frozen reference does not change the compact summary operands.
	if err := db.Model(&head).Update("result_json", `{"candidate":false,"source_complete":true,"rules":[],"reference":{"through":123}}`).Error; err != nil {
		t.Fatal(err)
	}
	read(q)
	if reads.Load() != 1 {
		t.Fatal("unchanged semantic projection invalidated pagination")
	}
	rollback := errors.New("injected rollback")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.Model(&head).Update("user_id", 9).Error; err != nil {
			return err
		}
		return rollback
	}); !errors.Is(err, rollback) {
		t.Fatal(err)
	}
	read(q)
	if reads.Load() != 1 {
		t.Fatal("rolled-back write changed a committed summary version")
	}
	// Keep an old read snapshot open across a real independent writer commit.
	// The old transaction must see its own old aggregate, then the next request
	// must invalidate it and see the newly committed customer dimension.
	if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
		before, _, err := m.tokenDetectionReportAggregates(tx, q, false)
		if err != nil {
			return err
		}
		if err := db.Model(&head).Update("user_id", 9).Error; err != nil {
			return err
		}
		after, _, err := m.tokenDetectionReportAggregates(tx, q, false)
		if !reflect.DeepEqual(before, after) {
			t.Fatal("concurrent commit tore the reader's snapshot")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	read(q)
	if reads.Load() != 2 {
		t.Fatal("committed dimension change did not invalidate the summary", reads.Load())
	}
	filtered := q
	filtered.UserID = 2
	if read(filtered).Summary.Observed != 0 {
		t.Fatal("summary cache crossed user filters")
	}
	filtered.UserID = 9
	if read(filtered).Summary.Observed != 1 {
		t.Fatal("summary cache lost changed user membership")
	}
	for _, update := range []map[string]any{{"created_at": now + 350}, {"source_epoch": "another-epoch"}, {"source_epoch": epoch, "created_at": now - 1}} {
		if err := db.Model(&TokenDetectionCurrent{}).Where("log_id=1").Updates(update).Error; err != nil {
			t.Fatal(err)
		}
		read(q)
	}
	if err := db.Where("log_id=1").Delete(&TokenDetectionCurrent{}).Error; err != nil {
		t.Fatal(err)
	}
	if read(q).Summary.Observed != 0 {
		t.Fatal("deleted result remained in summary")
	}
	// Errors never become reusable cache entries; repairing the same identity
	// must immediately restore a valid result without waiting for a TTL.
	head.ResultJSON = "invalid-json"
	if err := db.Create(&head).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
		_, _, err := m.tokenDetectionReportAggregates(tx, q, false)
		return err
	}); err == nil {
		t.Fatal("cached summary hid corrupt current evidence")
	}
	if err := db.Model(&head).Update("result_json", body).Error; err != nil {
		t.Fatal(err)
	}
	read(q)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
				s, a, err := m.tokenDetectionReportAggregates(tx, q, false)
				s.RuleHits["response-only"]++
				a.StatusCounts["response-only"]++
				return err
			})
			if err != nil {
				t.Error(err)
			}
		}()
	}
	wg.Wait()
	read(q)
}

func TestTokenReportSummaryCacheTruncatedBoundaryAndBound(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	if err := db.Exec(`WITH RECURSIVE ids(n) AS (VALUES(1) UNION ALL SELECT n+1 FROM ids WHERE n<5010)
		INSERT INTO token_detection_currents(source_epoch,log_id,created_at,user_id,channel_id,model,user_group,candidate,source_complete,result_json)
		SELECT ?,n,?+n,2,3,'synthetic','group',0,1,'{"candidate":false,"source_complete":true,"rules":[]}' FROM ids`, epoch, now).Error; err != nil {
		t.Fatal(err)
	}
	var reads atomic.Int64
	m.tokenReadStore().Config.Logger = tokenSummaryReadLogger{Interface: logger.Default, reads: &reads}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: now, To: now + 9000}
	read := func() {
		t.Helper()
		if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
			s, _, err := m.tokenDetectionReportAggregates(tx, q, false)
			if s.Observed != 5000 || !s.Truncated {
				t.Fatal("truncated population changed", s.Observed, s.Truncated)
			}
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	read()
	if err := db.Create(&TokenDetectionCurrent{SourceEpoch: epoch, LogID: 6000, CreatedAt: now + 8500, ResultJSON: "{}"}).Error; err != nil {
		t.Fatal(err)
	}
	read()
	if reads.Load() != 1 {
		t.Fatal("unrelated new tail rows reloaded the unchanged first 5000")
	}
	if err := db.Model(&TokenDetectionCurrent{}).Where("log_id=6000").Update("created_at", now).Error; err != nil {
		t.Fatal(err)
	}
	read()
	if reads.Load() != 2 {
		t.Fatal("late row entering the cached prefix did not invalidate it")
	}
	for i := int64(0); i < 20; i++ {
		q.UserID = 100 + i
		if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
			_, _, err := m.tokenDetectionReportAggregates(tx, q, false)
			return err
		}); err != nil {
			t.Fatal(err)
		}
	}
	m.tokenReportSummaryCache.mu.Lock()
	defer m.tokenReportSummaryCache.mu.Unlock()
	if len(m.tokenReportSummaryCache.entries) > 16 {
		t.Fatal("summary cache exceeded its entry bound")
	}
	for _, entry := range m.tokenReportSummaryCache.entries {
		if len(entry.data) > 256<<10 {
			t.Fatal("summary cache exceeded its payload bound")
		}
	}
}

func TestTokenReportSummaryCandidateUsesCandidateIndex(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	defer m.Close()
	q := tokenDetectionQuery{
		SourceEpoch: m.cfg.TokenFactsSourceEpoch,
		From:        now - 3600,
		To:          now,
		Status:      "candidate",
		Limit:       50,
	}
	query := tokenReportFilters(tokenReportSummaryRange(m.tokenReadStore(), q), q).
		Select("user_id,channel_id,model,user_group,created_at,result_json").
		Order("created_at,log_id").Limit(5001).
		Session(&gorm.Session{DryRun: true}).Find(&[]struct {
		UserID     int64
		ChannelID  int64
		Model      string
		UserGroup  string
		CreatedAt  int64
		ResultJSON string
	}{})
	if query.Error != nil {
		t.Fatal(query.Error)
	}
	sql := query.Statement.SQL.String()
	if !strings.Contains(sql, "INDEXED BY idx_token_report_summary_candidate_v1") {
		t.Fatalf("candidate summary query did not pin the selective index: %s", sql)
	}
	var plan []struct{ Detail string }
	if err := m.tokenReadStore().Raw("EXPLAIN QUERY PLAN "+sql, query.Statement.Vars...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range plan {
		if strings.Contains(row.Detail, "idx_token_report_summary_candidate_v1") {
			return
		}
	}
	t.Fatalf("candidate summary query plan ignored selective index: %v", plan)
}

func TestTokenReportSummaryStatusUsesEntryIndex(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	defer m.Close()
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	if err := db.Create(&TokenDetectionCurrent{SourceEpoch: epoch, LogID: 1, CreatedAt: now - 10, SourceComplete: true, DetectionState: "not_hit", ResultJSON: `{"candidate":false,"source_complete":true,"rules":[]}`}).Error; err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: now - 60, To: now + 60, Status: "not_hit", Limit: 50}
	if err := m.tokenReadStore().Transaction(func(tx *gorm.DB) error {
		summary, _, err := m.tokenDetectionReportAggregates(tx, q, false)
		if err != nil {
			return err
		}
		if summary.Observed != 1 {
			t.Fatalf("status join returned %d rows", summary.Observed)
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	query := tokenReportSummaryRange(m.tokenReadStore(), q)
	if !tokenReportSummaryStatusJoined(query) {
		t.Fatal("status summary query did not use the compact entry join")
	}
	query = tokenReportFilters(query, q).
		Select("token_detection_currents.user_id,token_detection_currents.channel_id,token_detection_currents.model,token_detection_currents.user_group,token_detection_currents.created_at,token_detection_currents.result_json").
		Order("token_report_entries.created_at,token_report_entries.log_id").Limit(5001).
		Session(&gorm.Session{DryRun: true}).Find(&[]struct {
		UserID     int64
		ChannelID  int64
		Model      string
		UserGroup  string
		CreatedAt  int64
		ResultJSON string
	}{})
	if query.Error != nil {
		t.Fatal(query.Error)
	}
	var plan []struct{ Detail string }
	if err := m.tokenReadStore().Raw("EXPLAIN QUERY PLAN "+query.Statement.SQL.String(), query.Statement.Vars...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range plan {
		if strings.Contains(row.Detail, "idx_token_report_entry_scope_v1") {
			return
		}
	}
	t.Fatalf("status summary query plan ignored entry index: %v", plan)
}
