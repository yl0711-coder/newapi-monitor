package monitor

import (
	"context"
	"database/sql"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func financeGiftBoundarySource(t *testing.T) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", t.TempDir()+"/gift-boundary-source.db")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`CREATE TABLE logs(
		id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,quota INTEGER,
		token_id INTEGER,token_name TEXT,request_id TEXT,content TEXT,other TEXT)`); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestFetchFinanceGiftBoundaryEventsKeepsOnlyMonetaryOrdering(t *testing.T) {
	source := financeGiftBoundarySource(t)
	rows := []struct {
		id, user, at, kind, quota, token int64
		tokenName, requestID, content    string
	}{
		{1, 7, 100, 2, 500_000, 1, "customer", "req-1", "secret prompt"},
		{2, 8, 120, 2, 700_000, 2, "other", "req-2", "other prompt"},
		{3, 7, 200, 6, 100_000, 1, "customer", "req-1", "refund"},
		{4, 7, 300, 2, 900_000, 0, "模型测试", "", "模型测试"},
		{5, 7, 3600, 2, 999_999, 1, "next-hour", "req-3", "next"},
	}
	for _, row := range rows {
		if _, err := source.Exec(`INSERT INTO logs VALUES(?,?,?,?,?,?,?,?,?,?)`, row.id, row.user, row.at, row.kind, row.quota, row.token, row.tokenName, row.requestID, row.content, ""); err != nil {
			t.Fatal(err)
		}
	}
	events, err := fetchFinanceGiftBoundaryEvents(context.Background(), source, 0, []int64{7})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 2 || events[0].SourceLogID != 1 || events[0].Kind != "usage" || events[1].SourceLogID != 3 || events[1].Kind != "refund" {
		t.Fatalf("events=%+v", events)
	}
	for _, event := range events {
		if event.EvidenceHash == "" || event.SourceEpoch != "" || event.UserID != 7 {
			t.Fatalf("normalized event=%+v", event)
		}
	}
	selectList := strings.Split(mustFinanceGiftBoundarySQL(t, 1), "FROM logs")[0]
	for _, forbidden := range []string{"prompt", "content", "other", "token", "request_id", "api_key"} {
		if strings.Contains(strings.ToLower(selectList), forbidden) {
			t.Fatalf("gift boundary query retains forbidden request data: %s", forbidden)
		}
	}
}

func mustFinanceGiftBoundarySQL(t *testing.T, users int) string {
	t.Helper()
	query, err := financeGiftBoundarySQL(users)
	if err != nil {
		t.Fatal(err)
	}
	return query
}

func financeGiftBoundaryDB(t *testing.T, name string) *gorm.DB {
	t.Helper()
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&FinanceUserHourFact{}, &FinanceGiftBoundaryEvent{}, &FinanceGiftBoundaryState{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestReplaceFinanceGiftBoundaryUserHourIsVerifiedAndIdempotent(t *testing.T) {
	db := financeGiftBoundaryDB(t, "gift-boundary-publish")
	hour := int64(3600)
	aggregate := FinanceUserHourFact{HourTs: hour, UserID: 7, Requests: 2, RefundRecords: 1, ConsumeQuota: 500_000, RefundQuota: 100_000}
	if err := db.Create(&aggregate).Error; err != nil {
		t.Fatal(err)
	}
	events := []FinanceGiftBoundaryEvent{
		{SourceLogID: 10, HourTs: hour, UserID: 7, EventAt: hour + 10, Kind: "usage", Quota: 200_000},
		{SourceLogID: 11, HourTs: hour, UserID: 7, EventAt: hour + 20, Kind: "refund", Quota: 100_000},
		{SourceLogID: 12, HourTs: hour, UserID: 7, EventAt: hour + 30, Kind: "usage", Quota: 300_000},
	}
	for index := range events {
		events[index].EvidenceHash = financeGiftBoundaryEventHash(events[index])
	}
	first, err := replaceFinanceGiftBoundaryUserHour(context.Background(), db, hour, 7, 10_000, "source-v1", events)
	if err != nil {
		t.Fatal(err)
	}
	second, err := replaceFinanceGiftBoundaryUserHour(context.Background(), db, hour, 7, 10_100, "source-v1", events)
	if err != nil {
		t.Fatal(err)
	}
	if first.ContentHash == "" || first.ContentHash != second.ContentHash || second.Rows != 3 || second.Requests != 2 || second.RefundRecords != 1 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	var count int64
	if err := db.Model(&FinanceGiftBoundaryEvent{}).Where("source_epoch=? AND hour_ts=? AND user_id=?", "source-v1", hour, 7).Count(&count).Error; err != nil || count != 3 {
		t.Fatalf("count=%d err=%v", count, err)
	}
}

func TestReplaceFinanceGiftBoundaryUserHourMismatchPreservesPrior(t *testing.T) {
	db := financeGiftBoundaryDB(t, "gift-boundary-rollback")
	hour := int64(7200)
	if err := db.Create(&FinanceUserHourFact{HourTs: hour, UserID: 9, Requests: 1, ConsumeQuota: 250_000}).Error; err != nil {
		t.Fatal(err)
	}
	valid := FinanceGiftBoundaryEvent{SourceLogID: 20, HourTs: hour, UserID: 9, EventAt: hour + 1, Kind: "usage", Quota: 250_000}
	valid.EvidenceHash = financeGiftBoundaryEventHash(valid)
	if _, err := replaceFinanceGiftBoundaryUserHour(context.Background(), db, hour, 9, 20_000, "source-v1", []FinanceGiftBoundaryEvent{valid}); err != nil {
		t.Fatal(err)
	}
	mismatch := FinanceGiftBoundaryEvent{SourceLogID: 21, HourTs: hour, UserID: 9, EventAt: hour + 2, Kind: "usage", Quota: 249_999}
	mismatch.EvidenceHash = financeGiftBoundaryEventHash(mismatch)
	if _, err := replaceFinanceGiftBoundaryUserHour(context.Background(), db, hour, 9, 20_100, "source-v1", []FinanceGiftBoundaryEvent{mismatch}); err == nil {
		t.Fatal("detail that disagrees with the published hour must fail closed")
	}
	var stored FinanceGiftBoundaryEvent
	if err := db.First(&stored, "source_epoch=? AND source_log_id=?", "source-v1", 20).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Quota != 250_000 {
		t.Fatalf("prior publication changed: %+v", stored)
	}
}

func TestReplaceFinanceGiftBoundaryUserHourAllowsProvenZeroHour(t *testing.T) {
	db := financeGiftBoundaryDB(t, "gift-boundary-empty")
	state, err := replaceFinanceGiftBoundaryUserHour(context.Background(), db, 10_800, 11, 30_000, "source-v1", nil)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != "complete" || state.Rows != 0 || state.ContentHash == "" {
		t.Fatalf("state=%+v", state)
	}
}

func TestFetchFinanceGiftBoundaryEventsRejectsInvalidUsers(t *testing.T) {
	source := financeGiftBoundarySource(t)
	for _, users := range [][]int64{nil, {0}, {7, 7}} {
		if _, err := fetchFinanceGiftBoundaryEvents(context.Background(), source, 0, users); err == nil {
			t.Fatalf("users %v must fail closed", users)
		}
	}
}
