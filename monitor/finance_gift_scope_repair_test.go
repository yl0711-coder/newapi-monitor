package monitor

import (
	"context"
	"database/sql"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func giftScopeRepairFixture(t *testing.T) (*Monitor, *sql.DB, int64) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "gift-repair.example")
	hour, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	db, ctx := m.usageFactsStore(), context.Background()
	if _, err := replaceFinanceUserHourFacts(ctx, db, hour, "v1", []FinanceUserHourFact{
		{HourTs: hour, UserID: 7, Requests: 2, RefundRecords: 1, ConsumeQuota: 80_000_000, RefundQuota: 5_000_000},
		{HourTs: hour, UserID: 8, Requests: 1, ConsumeQuota: 100},
	}, hour+7200); err != nil {
		t.Fatal(err)
	}
	grant := FinanceCreditEvent{SourceLogID: 1, EventAt: hour + 10, TargetUserID: 7, Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000, EligibleTrial: true}
	grant.EvidenceHash = financeCreditEventHash(grant)
	if _, err := replaceFinanceCreditHour(ctx, db, hour, hour+7200, "v1", financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}); err != nil {
		t.Fatal(err)
	}
	events := []FinanceGiftBoundaryEvent{
		{SourceLogID: 2, HourTs: hour, UserID: 7, EventAt: hour + 20, Kind: "usage", Quota: 30_000_000},
		{SourceLogID: 3, HourTs: hour, UserID: 7, EventAt: hour + 30, Kind: "usage", Quota: 50_000_000},
		{SourceLogID: 4, HourTs: hour, UserID: 7, EventAt: hour + 40, Kind: "refund", Quota: 5_000_000},
	}
	for i := range events {
		events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
	}
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, hour, 7, hour+7200, "v1", events); err != nil {
		t.Fatal(err)
	}
	other := FinanceGiftBoundaryEvent{SourceLogID: 5, HourTs: hour, UserID: 8, EventAt: hour + 50, Kind: "usage", Quota: 100}
	other.EvidenceHash = financeGiftBoundaryEventHash(other)
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, hour, 8, hour+7200, "v1", []FinanceGiftBoundaryEvent{other}); err != nil {
		t.Fatal(err)
	}
	source := financeGiftBoundarySource(t)
	for i, e := range events {
		group := []string{"test", "business", "business"}[i]
		kind := 2
		if e.Kind == "refund" {
			kind = 6
		}
		if _, err := source.Exec("INSERT INTO logs(id,user_id,created_at,type,quota,token_id,token_name,request_id,content,other,`group`) VALUES(?,?,?,?,?,1,'customer','req','','',?)", e.SourceLogID, e.UserID, e.EventAt, kind, e.Quota, group); err != nil {
			t.Fatal(err)
		}
	}
	return m, source, hour
}

func TestFinanceGiftScopeRepairPreservesMoneyAndUnlocksOnlyVerifiedAllocation(t *testing.T) {
	m, source, hour := giftScopeRepairFixture(t)
	db, ctx := m.usageFactsStore(), context.Background()
	cursor := FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "v1", StartHourTs: hour, NextHourTs: hour + 3600, Status: "backfilling"}
	if err := db.Create(&cursor).Error; err != nil {
		t.Fatal(err)
	}
	var userFactsBefore []FinanceUserHourFact
	if err := db.Order("hour_ts,user_id").Find(&userFactsBefore).Error; err != nil {
		t.Fatal(err)
	}
	otherBefore, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 8)
	if err != nil {
		t.Fatal(err)
	}
	before, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+3600, map[string]bool{"test": false}, nil)
	if err != nil || before.Coverage.Complete || before.Coverage.ScopeUnknownEvents != 3 {
		t.Fatalf("before=%+v %v", before.Coverage, err)
	}
	fullBefore, err := m.financeReportSourceFingerprint(ctx, hour+3600, hour+7200)
	if err != nil {
		t.Fatal(err)
	}
	baseBefore, err := m.financeReportPeriodSourceFingerprint(ctx, hour+3600, hour+7200)
	if err != nil {
		t.Fatal(err)
	}
	repaired, err := repairFinanceGiftBoundaryScope(ctx, db, source, "v1", hour, 7, hour+8000)
	if err != nil || repaired.RowsUpdated != 3 {
		t.Fatalf("repair=%+v %v", repaired, err)
	}
	after, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+3600, map[string]bool{"test": false}, nil)
	// $60 test usage consumes the first $60 gift; $100 business usage uses
	// its remaining $40. The $10 business refund restores $10 of that gift.
	if err != nil || !after.Coverage.Complete || after.Allocation.PeriodGiftConsumptionMicroUSD != 30_000_000 {
		t.Fatalf("after=%+v %v", after, err)
	}
	internal, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+3600, map[string]bool{"test": false}, map[int64]bool{7: true})
	if err != nil || !internal.Coverage.Complete || internal.Allocation.PeriodGiftConsumptionMicroUSD != 0 {
		t.Fatalf("internal=%+v %v", internal, err)
	}
	otherAfter, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 8)
	if err != nil || !reflect.DeepEqual(otherBefore, otherAfter) {
		t.Fatal("changed another user's data", err)
	}
	state, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	retry, err := repairFinanceGiftBoundaryScope(ctx, db, nil, "v1", hour, 7, hour+9000)
	if err != nil || retry.RowsUpdated != 0 || retry.ContentHash != repaired.ContentHash {
		t.Fatalf("retry=%+v %v", retry, err)
	}
	stateAfter, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil || !reflect.DeepEqual(state, stateAfter) {
		t.Fatal("retry republished facts", err)
	}
	fullAfter, err := m.financeReportSourceFingerprint(ctx, hour+3600, hour+7200)
	if err != nil || fullAfter == fullBefore {
		t.Fatal("later gift-dependent report did not invalidate", err)
	}
	baseAfter, err := m.financeReportPeriodSourceFingerprint(ctx, hour+3600, hour+7200)
	if err != nil || baseBefore != baseAfter {
		t.Fatal("unrelated base period invalidated", err)
	}
	var cursorAfter FinanceFactSyncState
	if err := db.First(&cursorAfter, financeFactSyncStateID).Error; err != nil || cursorAfter != cursor {
		t.Fatal("main cursor changed", err)
	}
	var userFactsAfter []FinanceUserHourFact
	if err := db.Order("hour_ts,user_id").Find(&userFactsAfter).Error; err != nil || !reflect.DeepEqual(userFactsBefore, userFactsAfter) {
		t.Fatal("monetary aggregates changed", err)
	}
}

func TestFinanceGiftScopeRepairRejectsIncompleteOrChangedSource(t *testing.T) {
	for _, tc := range []struct{ name, sql string }{
		{"missing", "DELETE FROM logs WHERE id=4"},
		{"same_total_changed_identity", "UPDATE logs SET id=99 WHERE id=2"},
		{"same_total_changed_allocation", "UPDATE logs SET quota=quota+CASE id WHEN 2 THEN 1 WHEN 3 THEN -1 ELSE 0 END"},
		{"timestamp", "UPDATE logs SET created_at=created_at+1 WHERE id=2"},
		{"wrong_user", "UPDATE logs SET user_id=8 WHERE id=2"},
		{"source_unavailable", "DROP TABLE logs"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, source, hour := giftScopeRepairFixture(t)
			db, ctx := m.usageFactsStore(), context.Background()
			before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := source.Exec(tc.sql); err != nil {
				t.Fatal(err)
			}
			if _, err := repairFinanceGiftBoundaryScope(ctx, db, source, "v1", hour, 7, hour+8000); err == nil {
				t.Fatal("changed source accepted")
			}
			after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("failed repair changed old facts", err)
			}
		})
	}
}

type giftScopeHookSource struct {
	db   *sql.DB
	hook func()
}

func (s giftScopeHookSource) QueryContext(ctx context.Context, q string, args ...any) (*sql.Rows, error) {
	s.hook()
	return s.db.QueryContext(ctx, q, args...)
}

func TestFinanceGiftScopeRepairRejectsConcurrentPublication(t *testing.T) {
	m, source, hour := giftScopeRepairFixture(t)
	db, ctx := m.usageFactsStore(), context.Background()
	hook := giftScopeHookSource{source, func() {
		if err := db.Model(&FinanceGiftBoundaryState{}).Where("source_epoch=? AND hour_ts=? AND user_id=?", "v1", hour, 7).Update("updated_at", hour+7900).Error; err != nil {
			t.Fatal(err)
		}
	}}
	_, err := repairFinanceGiftBoundaryScope(ctx, db, hook, "v1", hour, 7, hour+8000)
	if err == nil || !strings.Contains(err.Error(), "target changed") {
		t.Fatal("concurrent write not detected", err)
	}
	state, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range state.Events {
		if e.GroupKnown {
			t.Fatal("published after concurrent change")
		}
	}
}

func TestFinanceGiftScopeRepairPublicationFailureIsAtomic(t *testing.T) {
	m, source, hour := giftScopeRepairFixture(t)
	db, ctx := m.usageFactsStore(), context.Background()
	before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TRIGGER reject_scope_repair BEFORE INSERT ON finance_gift_boundary_events WHEN NEW.group_known=1 BEGIN SELECT RAISE(ABORT,'test publication failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := repairFinanceGiftBoundaryScope(ctx, db, source, "v1", hour, 7, hour+8000); err == nil {
		t.Fatal("failure not returned")
	}
	after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failed publication lost old data", err)
	}
}

func TestFinanceGiftScopeRepairKnownGroupCannotBeReassigned(t *testing.T) {
	m, source, hour := giftScopeRepairFixture(t)
	db, ctx := m.usageFactsStore(), context.Background()
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	prior.Events[0].GroupKnown = true
	prior.Events[0].Group = "original"
	prior.Events[0].EvidenceHash = financeGiftBoundaryEventHash(prior.Events[0])
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, hour, 7, hour+7500, "v1", prior.Events); err != nil {
		t.Fatal(err)
	}
	before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repairFinanceGiftBoundaryScope(ctx, db, source, "v1", hour, 7, hour+8000); err == nil || !strings.Contains(err.Error(), "overwrite verified") {
		t.Fatal("known group overwritten", err)
	}
	after, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("changed known group", err)
	}
}

func TestFinanceGiftScopeRepairRejectsEpochAndAggregateMismatch(t *testing.T) {
	for _, column := range []string{"source_epoch", "consume_quota"} {
		t.Run(column, func(t *testing.T) {
			m, source, hour := giftScopeRepairFixture(t)
			db, ctx := m.usageFactsStore(), context.Background()
			before, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
			if err != nil {
				t.Fatal(err)
			}
			if column == "source_epoch" {
				if err := db.Model(&FinanceUserHourState{}).Where("hour_ts=?", hour).Update(column, "other-source").Error; err != nil {
					t.Fatal(err)
				}
			} else if err := db.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", hour, 7).Update(column, 1).Error; err != nil {
				t.Fatal(err)
			}
			if _, err := repairFinanceGiftBoundaryScope(ctx, db, source, "v1", hour, 7, hour+8000); err == nil {
				t.Fatal("mismatch accepted")
			}
			var events []FinanceGiftBoundaryEvent
			if err := db.Where("source_epoch=? AND hour_ts=? AND user_id=?", "v1", hour, 7).Order("event_at,source_log_id").Find(&events).Error; err != nil || !reflect.DeepEqual(events, before.Events) {
				t.Fatal("old events changed", err)
			}
		})
	}
}

// Optional acceptance against a downloaded, closed SQLite backup. It copies
// the file to a private test directory; it never changes the supplied backup,
// accesses a production source, or invents groups for real users.
func copyGiftScopeSnapshotForTest(t *testing.T) *gorm.DB {
	t.Helper()
	path := os.Getenv("MONITOR_GIFT_SCOPE_REPAIR_SNAPSHOT")
	if path == "" {
		t.Skip("set MONITOR_GIFT_SCOPE_REPAIR_SNAPSHOT to a closed local backup")
	}
	if info, err := os.Stat(path + "-wal"); err == nil && info.Size() != 0 {
		t.Fatal("use a closed SQLite backup, not a live WAL database")
	}
	input, err := os.Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer input.Close()
	copyPath := filepath.Join(t.TempDir(), "gift-repair-copy.db")
	output, err := os.OpenFile(copyPath, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0600)
	if err != nil {
		t.Fatal(err)
	}
	_, copyErr := io.Copy(output, input)
	closeErr := output.Close()
	if copyErr != nil || closeErr != nil {
		t.Fatal(copyErr, closeErr)
	}
	db, err := gorm.Open(sqlite.Open(copyPath), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	if err := db.AutoMigrate(&FinanceGiftBoundaryEvent{}); err != nil {
		t.Fatal(err)
	}
	return db
}

func TestFinanceGiftScopeRepairLocalSnapshotWithoutSource(t *testing.T) {
	db := copyGiftScopeSnapshotForTest(t)
	var target FinanceGiftBoundaryEvent
	if err := db.Where("COALESCE(group_known,0)=0 AND quota>0").Order("hour_ts,user_id,source_log_id").First(&target).Error; err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	before, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := repairFinanceGiftBoundaryScope(ctx, db, nil, target.SourceEpoch, target.HourTs, target.UserID, target.HourTs+7200)
	if err == nil || !strings.Contains(err.Error(), "source unavailable") || result.RowsUpdated != 0 {
		t.Fatalf("must keep real unknown scope: %+v %v", result, err)
	}
	after, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("real backup clone changed on missing source", err)
	}
	t.Logf("legacy snapshot accepted; checked %d events; no original group source, zero writes", result.RowsChecked)
}
