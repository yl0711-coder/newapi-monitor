package monitor

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Keep the previous full-scan implementation as an independent semantic oracle.
// It must not share the optimized query's channel-first selection logic.
const financeActivityReferenceQuery = `SELECT domain, MIN(hour_ts) first_ts FROM (
	SELECT LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain, s.hour_ts
	FROM stability_hour_samples s JOIN channel_snaps c ON c.id=s.channel_id
	WHERE s.traffic_class_version=? AND (s.success+s.anomaly+s.failed<>0 OR s.quota<>0 OR s.refund_quota<>0)
	UNION ALL
	SELECT LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain, t.hour_ts
	FROM channel_test_hour_samples t JOIN channel_snaps c ON c.id=t.channel_id
	WHERE t.traffic_class_version=? AND (t.requests<>0 OR t.quota<>0)
	UNION ALL
	SELECT LOWER(TRIM(domain)) domain, hour_ts FROM channel_upstream_usage_hours
	WHERE requests<>0 OR tokens<>0 OR quota<>0 OR cost_usd<>0
) activity WHERE hour_ts<? GROUP BY domain`

func financeActivityReference(t *testing.T, db *gorm.DB, to int64) map[string]int64 {
	t.Helper()
	var rows []struct {
		Domain  string
		FirstTs int64
	}
	if err := db.Raw(financeActivityReferenceQuery, stabilityTrafficClassificationVersion, stabilityTrafficClassificationVersion, to).Scan(&rows).Error; err != nil {
		t.Fatal(err)
	}
	result := make(map[string]int64)
	for _, row := range rows {
		domain := strings.ToLower(strings.TrimSpace(row.Domain))
		if domain != "" && row.FirstTs >= 0 {
			result[domain] = row.FirstTs
		}
	}
	return result
}

func TestFinanceActivityStartsPreserveEligibility(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	db := m.storeDB
	channels := []ChannelSnap{
		{ID: 1, BaseDomain: " Mixed.EXAMPLE ", Status: 1},
		{ID: 2, BaseDomain: "mixed.example", Status: 2, DeletedAt: 200},
		{ID: 3, BaseDomain: "test.example", Status: 3},
		{ID: 4, BaseDomain: "  "}, {ID: 5, BaseDomain: "future.example"},
		{ID: 6, BaseDomain: "negative.example"}, {ID: 7, BaseDomain: "refund.example"},
		{ID: 8, BaseDomain: "quota.example"}, {ID: 9, BaseDomain: "zero.example"},
		{ID: 10, BaseDomain: "null.example"}, {ID: 11, BaseDomain: "test-quota.example"},
		{ID: 12, BaseDomain: "inactive.example"},
		{ID: 13, BaseDomain: "epoch-zero.example"},
	}
	users := []StabilityHourSample{
		{ChannelID: 1, HourTs: 10, Success: 1, TrafficClassVersion: 5}, // Pre-v6 monetary facts are not compatible.
		{ChannelID: 1, HourTs: 20, Tokens: 1},                          // Tokens alone were not activity.
		{ChannelID: 1, HourTs: 300, Success: 1},
		{ChannelID: 2, HourTs: 50, Quota: 1}, // Disabled/deleted still has history.
		{ChannelID: 3, HourTs: 400, Anomaly: 1},
		{ChannelID: 4, HourTs: 90, Failed: 1},
		{ChannelID: 5, HourTs: 900, Success: 1}, // Exclusive right boundary.
		{ChannelID: 6, HourTs: -100, Success: 1}, {ChannelID: 6, HourTs: 10, Success: 1},
		{ChannelID: 7, HourTs: 100, RefundQuota: 1},
		{ChannelID: 8, HourTs: 110, Quota: -1},
		{ChannelID: 9, HourTs: 100, Success: 1, Anomaly: -1},
		{ChannelID: 10, HourTs: 60, Quota: 1},
		{ChannelID: 999, HourTs: 1, Success: 1}, // Orphan channel was not joined.
		{ChannelID: 13, HourTs: 0, Success: 1},  // A real zero hour is not NULL.
	}
	tests := []ChannelTestHourSample{
		{ChannelID: 3, HourTs: 10, Requests: 1, TrafficClassVersion: 5},
		{ChannelID: 3, HourTs: 20, Tokens: 1},
		{ChannelID: 3, HourTs: 120, Requests: 1},
		{ChannelID: 11, HourTs: 140, Quota: -1},
		{ChannelID: 999, HourTs: 1, Requests: 1},
	}
	bills := []ChannelUpstreamUsageHour{
		{Domain: " UPSTREAM.example ", HourTs: 75, CostUSD: -1},
		{Domain: "upstream.example", HourTs: 60, Tokens: 1},
		{Domain: "orphan-usage.example", HourTs: 80, Requests: 1},
		{Domain: "zero.example", HourTs: 1}, {Domain: "", HourTs: 20, CostUSD: 1},
	}
	for _, records := range []any{&channels, &users, &tests, &bills} {
		if err := db.Create(records).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec("UPDATE stability_hour_samples SET success=NULL,anomaly=NULL,failed=NULL WHERE channel_id=10").Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelSnap{}).Where("id=4").Update("base_domain", nil).Error; err != nil {
		t.Fatal(err)
	}
	for _, to := range []int64{-100, 0, 50, 51, 900, 901} {
		got, err := m.loadFinanceUpstreamActivityStarts(context.Background(), to)
		if err != nil || !reflect.DeepEqual(got, financeActivityReference(t, db, to)) {
			t.Fatalf("activity eligibility differs at to=%d: %v", to, err)
		}
	}
	got, err := m.loadFinanceUpstreamActivityStarts(context.Background(), 900)
	want := map[string]int64{
		"mixed.example": 50, "test.example": 120, "未配置/历史": 90,
		"refund.example": 100, "quota.example": 110, "null.example": 60,
		"test-quota.example": 140, "upstream.example": 60, "orphan-usage.example": 80,
		"epoch-zero.example": 0,
	}
	if err != nil || !reflect.DeepEqual(got, want) {
		t.Fatalf("activity semantics changed: got=%v want=%v err=%v", got, want, err)
	}
}

func TestFinanceActivityStartsUseExistingChannelHourIndexes(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	var plan []struct{ Detail string }
	if err := m.storeDB.Raw("EXPLAIN QUERY PLAN "+financeUpstreamActivityQuery, financeUpstreamActivityArgs(900)...).Scan(&plan).Error; err != nil {
		t.Fatal(err)
	}
	var details []string
	for _, step := range plan {
		details = append(details, step.Detail)
	}
	text := strings.Join(details, "\n")
	for _, search := range []string{"SEARCH s USING INDEX idx_stability_channel_hour", "SEARCH t USING INDEX idx_channel_test_channel_hour"} {
		if !strings.Contains(text, search) {
			t.Fatalf("activity lookup lost its indexed seek %q: %s", search, text)
		}
	}
	if strings.Contains(text, "USE TEMP B-TREE FOR ORDER BY") {
		t.Fatal("first-hour lookup sorts historical rows instead of walking its index")
	}
}

func TestFinanceActivityStartsFailClosedAndReleaseConnection(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	pool, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.loadFinanceUpstreamActivityStarts(ctx, 900); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled activity read did not fail: %v", err)
	}
	if err := m.storeDB.Exec("DROP TABLE channel_test_hour_samples").Error; err != nil {
		t.Fatal(err)
	}
	if got, err := m.loadFinanceUpstreamActivityStarts(context.Background(), 900); err == nil || got != nil {
		t.Fatal("missing source table was treated as an empty activity history")
	}
	writeCtx, writeCancel := context.WithTimeout(context.Background(), time.Second)
	defer writeCancel()
	if err := m.storeDB.WithContext(writeCtx).Create(&ChannelSnap{ID: 1}).Error; err != nil {
		t.Fatalf("failed read leaked the only connection: %v", err)
	}
}

func TestFinanceActivityStartsReadCommittedDuringMainWrite(t *testing.T) {
	m := newStabilityTestMonitor(t)
	t.Cleanup(m.Close)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, BaseDomain: "wal.example"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{ChannelID: 1, HourTs: 200, Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tx := m.storeDB.WithContext(ctx).Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Create(&StabilityHourSample{ChannelID: 1, HourTs: 100, Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	readCtx, readCancel := context.WithTimeout(ctx, time.Second)
	defer readCancel()
	got, err := m.loadFinanceUpstreamActivityStarts(readCtx, 300)
	if err != nil || got["wal.example"] != 200 {
		t.Fatalf("main read blocked on writer or saw uncommitted activity: %v %v", got, err)
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	got, err = m.loadFinanceUpstreamActivityStarts(ctx, 300)
	if err != nil || got["wal.example"] != 100 {
		t.Fatalf("committed historical activity remained hidden: %v %v", got, err)
	}
}
