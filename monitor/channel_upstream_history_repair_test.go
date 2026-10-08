package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestSub2HistoricalPrefixPreviewApplyAndCursorIsolation(t *testing.T) {
	dayTs := cstDayStart(time.Now().Add(-72 * time.Hour).Unix())
	day := time.Unix(dayTs, 0).In(cstLocation).Format("2006-01-02")
	earlierDay := time.Unix(dayTs-86400, 0).In(cstLocation).Format("2006-01-02")
	anchorDay := time.Unix(dayTs+2*86400, 0).In(cstLocation).Format("2006-01-02")
	var targetCostCents atomic.Int64
	targetCostCents.Store(125)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedDay := r.URL.Query().Get("start_date")
		if (r.URL.Path != "/api/v1/usage/dashboard/trend" && r.URL.Path != "/api/v1/usage/stats") || (requestedDay != day && requestedDay != earlierDay && requestedDay != anchorDay) || r.Header.Get("Authorization") != "Bearer history-access" {
			t.Errorf("unexpected historical request: %s", r.URL.String())
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		requests, tokens, cost := 3, 120, fmt.Sprintf("%.2f", float64(targetCostCents.Load())/100)
		if requestedDay == anchorDay {
			requests, tokens, cost = 9, 0, "9"
		}
		if r.URL.Path == "/api/v1/usage/stats" {
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
				"total_requests": requests, "total_tokens": tokens, "total_actual_cost": cost,
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
			"start_date": requestedDay, "end_date": requestedDay, "granularity": "hour",
			"trend": []any{map[string]any{"date": requestedDay + " 00:00", "requests": requests, "total_tokens": tokens, "actual_cost": cost}},
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	domain := server.Listener.Addr().String()
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderSub2API, BaseURL: server.URL, Account: "history-account", UserID: 7,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
		UsageDataUntil: dayTs + 3*86400, UsageBackfillCursor: dayTs + 3*86400, UsageBackfillDone: true,
	}
	if err := m.persistSyncedUpstreamAccount(t.Context(), &row, sub2APICredential{AccessToken: "history-access", ExpiresAt: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	firstStoredAt := dayTs + 2*86400
	for hour := firstStoredAt; hour < firstStoredAt+86400; hour += 3600 {
		bucket := ChannelUpstreamUsageHour{Domain: domain, HourTs: hour, BucketSeconds: 3600, Provider: upstreamProviderSub2API}
		if hour == firstStoredAt {
			bucket.Requests, bucket.CostUSD, bucket.Quota = 9, 9, 9
		}
		if err := m.storeDB.Create(&bucket).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", domain).
		Updates(map[string]any{"usage_status": upstreamStatusError, "usage_next_sync_at": time.Now().Add(time.Hour).Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.inspectSub2HistoricalDay(t.Context(), domain, day, "", "test-operator"); err == nil {
		t.Fatal("manual history preview bypassed account retry backoff")
	}
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", domain).
		Updates(map[string]any{"usage_status": upstreamStatusOK, "usage_next_sync_at": int64(0)}).Error; err != nil {
		t.Fatal(err)
	}
	preview, err := m.inspectSub2HistoricalDay(t.Context(), domain, day, "", "test-operator")
	if err != nil {
		t.Fatal(err)
	}
	if preview.Applied || !preview.AnchorMatched || preview.AnchorDay != anchorDay || preview.Buckets != 24 || preview.Requests != 3 || preview.Tokens != 120 || preview.CostUSD != 1.25 || len(preview.Digest) != 64 {
		t.Fatalf("invalid preview: %+v", preview)
	}
	var before int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, dayTs, dayTs+86400).Count(&before).Error; err != nil || before != 0 {
		t.Fatalf("preview wrote usage facts: count=%d err=%v", before, err)
	}
	if _, err := m.inspectSub2HistoricalDay(t.Context(), domain, day, "bad-digest", "test-operator"); !errors.Is(err, errUpstreamHistoricalDayConflict) {
		t.Fatalf("stale approval was accepted: %v", err)
	}
	targetCostCents.Store(200)
	if _, err := m.inspectSub2HistoricalDay(t.Context(), domain, day, preview.Digest, "test-operator"); !errors.Is(err, errUpstreamHistoricalDayConflict) {
		t.Fatalf("changed upstream amount was accepted under the old preview: %v", err)
	}
	targetCostCents.Store(125)
	applied, err := m.inspectSub2HistoricalDay(t.Context(), domain, day, preview.Digest, "test-operator")
	if err != nil {
		t.Fatal(err)
	}
	if !applied.Applied || applied.Digest != preview.Digest {
		t.Fatalf("unexpected apply result: %+v", applied)
	}
	var inserted []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, dayTs, dayTs+86400).Order("hour_ts ASC").Find(&inserted).Error; err != nil {
		t.Fatal(err)
	}
	if len(inserted) != 24 || inserted[0].CostUSD != 1.25 || inserted[0].Requests != 3 || inserted[23].HourTs != dayTs+23*3600 {
		t.Fatalf("historical day not published whole: %+v", inserted)
	}
	var receipt ChannelUpstreamHistoricalRepair
	if err := m.storeDB.First(&receipt, "domain = ? AND day_ts = ?", domain, dayTs).Error; err != nil {
		t.Fatal(err)
	}
	if receipt.Digest != preview.Digest || receipt.Actor != "test-operator" || receipt.Requests != 3 || receipt.CostUSD != 1.25 {
		t.Fatalf("repair receipt did not match published day: %+v", receipt)
	}
	var unchanged ChannelUpstreamAccount
	if err := m.storeDB.First(&unchanged, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.UsageDataUntil != row.UsageDataUntil || unchanged.UsageBackfillCursor != row.UsageBackfillCursor || !unchanged.UsageBackfillDone {
		t.Fatalf("repair changed normal sync watermarks: %+v", unchanged)
	}
	if _, err := m.inspectSub2HistoricalDay(t.Context(), domain, day, preview.Digest, "test-operator"); !errors.Is(err, errUpstreamHistoricalDayConflict) {
		t.Fatalf("repeat apply was not rejected: %v", err)
	}
	rollbackPreview, err := m.inspectSub2HistoricalDay(t.Context(), domain, earlierDay, "", "test-operator")
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER reject_historical_repair_receipt BEFORE INSERT ON channel_upstream_historical_repairs
		BEGIN SELECT RAISE(ABORT, 'injected audit failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.inspectSub2HistoricalDay(t.Context(), domain, earlierDay, rollbackPreview.Digest, "test-operator"); err == nil {
		t.Fatal("audit failure did not abort repair")
	}
	var leaked int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ? AND hour_ts >= ? AND hour_ts < ?", domain, dayTs-86400, dayTs).Count(&leaked).Error; err != nil || leaked != 0 {
		t.Fatalf("failed audit left a partial historical day: rows=%d err=%v", leaked, err)
	}
}

func TestSub2HistoricalPrefixRejectsEmptyAndExistingDay(t *testing.T) {
	dayTs := cstDayStart(time.Now().Add(-72 * time.Hour).Unix())
	day := time.Unix(dayTs, 0).In(cstLocation).Format("2006-01-02")
	anchorDay := time.Unix(dayTs+2*86400, 0).In(cstLocation).Format("2006-01-02")
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedDay := r.URL.Query().Get("start_date")
		trend := []any{}
		if requestedDay == anchorDay {
			trend = []any{map[string]any{"date": anchorDay + " 00:00", "requests": 1, "total_tokens": 1, "actual_cost": "1"}}
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
			"start_date": requestedDay, "end_date": requestedDay, "granularity": "hour", "trend": trend,
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL, Enabled: true, UsageSyncEnabled: true}
	if err := m.persistSyncedUpstreamAccount(t.Context(), &row, sub2APICredential{AccessToken: "history-access", ExpiresAt: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	for hour := dayTs + 2*86400; hour < dayTs+3*86400; hour += 3600 {
		bucket := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, BucketSeconds: 3600, Provider: row.Provider}
		if hour == dayTs+2*86400 {
			bucket.Requests, bucket.Tokens, bucket.CostUSD = 1, 1, 1
		}
		if err := m.storeDB.Create(&bucket).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.inspectSub2HistoricalDay(context.Background(), row.Domain, day, "", "test-operator"); err == nil {
		t.Fatal("empty remote history was treated as a complete zero-spend day")
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageArchive{Domain: row.Domain, HourTs: dayTs, BucketSeconds: 3600, Provider: row.Provider, AccountEpoch: "previous-account", ArchiveBatchID: "fixture-archive"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.inspectSub2HistoricalDay(context.Background(), row.Domain, day, "", "test-operator"); err == nil {
		t.Fatal("old account archive was ignored")
	}
	if _, err := m.inspectSub2HistoricalDay(context.Background(), row.Domain, time.Unix(dayTs+2*86400, 0).In(cstLocation).Format("2006-01-02"), "", "test-operator"); !errors.Is(err, errUpstreamHistoricalDayConflict) {
		t.Fatalf("existing live range was eligible for prefix repair: %v", err)
	}
}

func TestSub2HistoricalRepairEndpointDisabledAndAllowlisted(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	router := gin.New()
	router.POST("/preview", m.previewSub2HistoricalDayHandler)
	request := func() int {
		body := []byte(`{"domain":"taoken.ai","day":"2026-06-01"}`)
		recorder := httptest.NewRecorder()
		router.ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, "/preview", bytes.NewReader(body)))
		return recorder.Code
	}
	if status := request(); status != http.StatusServiceUnavailable {
		t.Fatalf("disabled repair endpoint status=%d", status)
	}
	m.cfg.UpstreamUsageHistoryRepairEnabled = true
	if status := request(); status != http.StatusForbidden {
		t.Fatalf("unlisted domain repair status=%d", status)
	}
}

func TestSub2HistoricalAnchorRequiresSameHourDistribution(t *testing.T) {
	stored := upstreamHistoricalDayView{Day: "2026-06-01", Adapter: upstreamUsageAdapterSub2Trend, Requests: 3, Tokens: 30, CostUSD: 1.5}
	fetched := stored
	a := []ChannelUpstreamUsageHour{{HourTs: 10, BucketSeconds: 3600, Requests: 1, Tokens: 10, CostUSD: 0.5}, {HourTs: 3610, BucketSeconds: 3600, Requests: 2, Tokens: 20, CostUSD: 1}}
	b := []ChannelUpstreamUsageHour{{HourTs: 10, BucketSeconds: 3600, Requests: 2, Tokens: 20, CostUSD: 1}, {HourTs: 3610, BucketSeconds: 3600, Requests: 1, Tokens: 10, CostUSD: 0.5}}
	if historicalSub2AnchorMatches(stored, fetched, a, b) {
		t.Fatal("matching day totals concealed changed hourly evidence")
	}
}

func TestSub2HistoricalTrendRequiresIndependentDayTotal(t *testing.T) {
	dayTs := cstDayStart(time.Now().Add(-72 * time.Hour).Unix())
	day := time.Unix(dayTs, 0).In(cstLocation).Format("2006-01-02")
	var statsAvailable atomic.Bool
	statsAvailable.Store(true)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestedDay := r.URL.Query().Get("start_date")
		if r.URL.Path == "/api/v1/usage/stats" {
			if !statsAvailable.Load() {
				http.NotFound(w, r)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
				"total_requests": 2, "total_tokens": 20, "total_actual_cost": "2",
			}})
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
			"start_date": requestedDay, "end_date": requestedDay, "granularity": "hour",
			"trend": []any{map[string]any{"date": requestedDay + " 00:00", "requests": 1, "total_tokens": 10, "actual_cost": "1"}},
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL, Enabled: true, UsageSyncEnabled: true}
	if err := m.persistSyncedUpstreamAccount(t.Context(), &row, sub2APICredential{AccessToken: "history-access", ExpiresAt: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	for hour := dayTs + 2*86400; hour < dayTs+3*86400; hour += 3600 {
		bucket := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: hour, BucketSeconds: 3600, Provider: row.Provider}
		if hour == dayTs+2*86400 {
			bucket.Requests, bucket.Tokens, bucket.CostUSD = 1, 10, 1
		}
		if err := m.storeDB.Create(&bucket).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.inspectSub2HistoricalDay(t.Context(), row.Domain, day, "", "test-operator"); err == nil {
		t.Fatal("incomplete hourly trend matched a different independent day total")
	}
	statsAvailable.Store(false)
	if _, err := m.inspectSub2HistoricalDay(t.Context(), row.Domain, day, "", "test-operator"); err == nil {
		t.Fatal("unavailable independent day total allowed historical preview")
	}
}
