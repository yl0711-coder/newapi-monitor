package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"testing"
	"time"
)

// No real upstream or production database is used by these bill regressions.
func sub2BillFixture(t *testing.T, stats map[string]any, statsStatus int) (*httptest.Server, *[]string, *sync.Mutex) {
	t.Helper()
	var paths []string
	mu := &sync.Mutex{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		paths = append(paths, r.URL.Path+"?"+r.URL.Query().Get("start_date"))
		mu.Unlock()
		if r.Method != http.MethodGet || r.Header.Get("Authorization") != "Bearer bill-test-access" ||
			r.URL.Query().Get("timezone") != "Asia/Shanghai" || r.URL.Query().Get("start_date") != r.URL.Query().Get("end_date") {
			http.Error(w, "unexpected request", http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/usage/dashboard/trend":
			day := r.URL.Query().Get("start_date")
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
				"start_date": day, "end_date": day, "granularity": "hour",
				"trend": []any{map[string]any{"date": day + " 00:00", "requests": 4, "total_tokens": 80, "actual_cost": 1.5}},
			}})
		case "/api/v1/usage/stats":
			if statsStatus != 0 {
				w.Header().Set("Retry-After", "120")
				w.WriteHeader(statsStatus)
				_, _ = w.Write([]byte(`{"message":"test upstream failure"}`))
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": stats})
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(server.Close)
	return server, &paths, mu
}

func TestSub2UsageClosedDayRequiresDailyAgreement(t *testing.T) {
	for _, tc := range []struct {
		name             string
		requests, tokens int64
		cost             float64
		status           int
		wantOK           bool
	}{
		{"matched_sparse", 4, 80, 1.5, 0, true},
		{"missing_cost", 4, 80, 3, 0, false},
		{"missing_requests", 5, 80, 1.5, 0, false},
		{"missing_tokens", 4, 81, 1.5, 0, false},
		{"day_total_unavailable", 4, 80, 1.5, 404, false},
		{"day_total_rate_limited", 4, 80, 1.5, 429, false},
		{"day_total_forbidden", 4, 80, 1.5, 403, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server, paths, mu := sub2BillFixture(t, map[string]any{"total_requests": tc.requests, "total_tokens": tc.tokens, "total_actual_cost": tc.cost}, tc.status)
			from := cstDayStart(time.Now().Unix()) - 86400
			row := ChannelUpstreamAccount{Domain: "sub2-bill.example", Provider: upstreamProviderSub2API, BaseURL: server.URL}
			cred := sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: time.Now().Add(time.Hour).Unix()}
			result, _, err := syncSub2APIUsage(context.Background(), newUpstreamHTTPClient(time.Second), row, cred, from, from+86400, newUpstreamUsageRequestPacer(2, 0), "")
			if (err == nil) != tc.wantOK {
				t.Fatalf("accepted=%v want=%v; err=%v", err == nil, tc.wantOK, err)
			}
			if tc.wantOK {
				if len(result.Hours) != 24 || result.Hours[0].CostUSD != 1.5 || result.Hours[1].Provisional {
					t.Fatalf("matched sparse day must include proven zero hours: %+v", result)
				}
			} else if len(result.Hours) != 0 {
				t.Fatal("failed verification returned publishable rows")
			}
			if tc.status == 429 {
				var httpErr *upstreamHTTPError
				if !errors.As(err, &httpErr) || httpErr.RetryAt <= time.Now().Unix() {
					t.Fatalf("lost upstream backoff: %v", err)
				}
			}
			mu.Lock()
			defer mu.Unlock()
			if len(*paths) != 2 {
				t.Fatalf("want exactly trend+stats, got %v", *paths)
			}
		})
	}
}

func TestSub2UsageOpenDayMissingHoursAreNotConfirmedZero(t *testing.T) {
	server, paths, mu := sub2BillFixture(t, nil, 500)
	from := cstDayStart(time.Now().Unix())
	row := ChannelUpstreamAccount{Domain: "sub2-bill.example", Provider: upstreamProviderSub2API, BaseURL: server.URL}
	cred := sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	result, _, err := syncSub2APIUsage(context.Background(), newUpstreamHTTPClient(time.Second), row, cred, from, from+3*3600, newUpstreamUsageRequestPacer(1, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hours) != 3 || result.Hours[0].Provisional || !result.Hours[1].Provisional || !result.Hours[2].Provisional {
		t.Fatalf("omitted hours were advertised as confirmed zero: %+v", result.Hours)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*paths) != 1 {
		t.Fatalf("open day must not compare moving totals: %v", *paths)
	}
}

func TestSub2UsageMissingHourDoesNotEraseStoredAmount(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := cstDayStart(time.Now().Unix())
	old := ChannelUpstreamUsageHour{Domain: "sub2-bill.example", Provider: upstreamProviderSub2API, HourTs: from, BucketSeconds: 1800, Requests: 9, Tokens: 900, Quota: 7, CostUSD: 7, FetchedAt: from + 1800}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	missing := ChannelUpstreamUsageHour{Domain: old.Domain, Provider: old.Provider, HourTs: from, BucketSeconds: 3600, Provisional: true}
	if err := m.persistUpstreamUsageWindow(context.Background(), old.Domain, from, from+3600, []ChannelUpstreamUsageHour{missing}, from+4000); err != nil {
		t.Fatal(err)
	}
	var got ChannelUpstreamUsageHour
	if err := m.storeDB.First(&got, "domain = ? AND hour_ts = ?", old.Domain, from).Error; err != nil {
		t.Fatal(err)
	}
	old.Provisional = true
	if !reflect.DeepEqual(got, old) {
		t.Fatalf("unconfirmed omission erased/refreshed known money: got=%+v want=%+v", got, old)
	}
}

func TestSub2UsageBackfillPrioritizesUnfinishedYesterday(t *testing.T) {
	server, paths, mu := sub2BillFixture(t, map[string]any{"total_requests": 4, "total_tokens": 80, "total_actual_cost": 1.5}, 0)
	m := newChannelUpstreamTestMonitor(t)
	now := time.Now().Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageDataUntil: today + 1, UsageBackfillCursor: today - 7*86400}
	cred := sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: now + 3600}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, cred); err != nil {
		t.Fatal(err)
	}
	old := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: row.Provider, HourTs: today - 3600, BucketSeconds: 2100, FetchedAt: today - 1500}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneHistory)
	if err != nil {
		t.Fatal(err)
	}
	if synced.UsageBackfillCursor != row.UsageBackfillCursor || synced.UsageBackfillDone || synced.UsageDataUntil != row.UsageDataUntil {
		t.Fatalf("recent repair skipped old history or moved tail: %+v", synced)
	}
	var got ChannelUpstreamUsageHour
	if err := m.storeDB.First(&got, "domain = ? AND hour_ts = ?", row.Domain, old.HourTs).Error; err != nil {
		t.Fatal(err)
	}
	if got.BucketSeconds != 3600 || got.Provisional || got.FetchedAt < today {
		t.Fatalf("yesterday still partial: %+v", got)
	}
	// A successful recent repair is durable; the next history turn must return
	// to the original cursor instead of polling yesterday over and over.
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).Update("usage_backfill_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	synced, err = m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneHistory)
	if err != nil {
		t.Fatal(err)
	}
	if synced.UsageBackfillCursor != row.UsageBackfillCursor+86400 {
		t.Fatalf("old history did not resume: %+v", synced)
	}
	mu.Lock()
	defer mu.Unlock()
	yesterday := time.Unix(today-86400, 0).In(cstLocation).Format("2006-01-02")
	older := time.Unix(row.UsageBackfillCursor, 0).In(cstLocation).Format("2006-01-02")
	want := []string{"/api/v1/usage/dashboard/trend?" + yesterday, "/api/v1/usage/stats?" + yesterday, "/api/v1/usage/dashboard/trend?" + older, "/api/v1/usage/stats?" + older}
	if !reflect.DeepEqual(*paths, want) {
		t.Fatalf("unexpected repeated/unbounded work: got=%v want=%v", *paths, want)
	}
}

func TestSub2UsageClosedMismatchPreservesPublishedRowsAndCursor(t *testing.T) {
	server, _, _ := sub2BillFixture(t, map[string]any{"total_requests": 4, "total_tokens": 80, "total_actual_cost": 10}, 0)
	m := newChannelUpstreamTestMonitor(t)
	now := time.Now().Unix()
	from := cstDayStart(now) - 86400
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageDataUntil: from + 86400, UsageBackfillCursor: from}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: now + 3600}); err != nil {
		t.Fatal(err)
	}
	old := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: row.Provider, HourTs: from, BucketSeconds: 3600, CostUSD: 9, Quota: 9, Requests: 9, Tokens: 900, FetchedAt: from + 3600}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneHistory)
	if err == nil {
		t.Fatal("mismatched day was published")
	}
	if synced.UsageBackfillCursor != from || synced.UsageBackfillDone || synced.UsageBackfillConsecutiveFails != 1 || synced.UsageBackfillNextSyncAt <= now || synced.UsageStatus != upstreamStatusOK || synced.UsageNextSyncAt != row.UsageNextSyncAt {
		t.Fatalf("failed history damaged schedules: %+v", synced)
	}
	var got []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", row.Domain).Find(&got).Error; err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], old) {
		t.Fatalf("failed proof changed published bills: %+v", got)
	}
}

func TestSub2UsageRecentRepairRespectsLaneAndBackoff(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := cstDayStart(time.Now().Unix()) + 12*3600
	base := ChannelUpstreamAccount{Domain: "sub2-backoff.example", Provider: upstreamProviderSub2API, UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 3600, UsageBackfillCursor: cstDayStart(now) - 7*86400}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: base.Domain, Provider: base.Provider, HourTs: cstDayStart(now) - 3600, BucketSeconds: 1800}).Error; err != nil {
		t.Fatal(err)
	}
	for _, mode := range []string{"tail_only", "history_backoff", "auth_paused", "tail_failed", "other_provider", "no_recent_rows"} {
		t.Run(mode, func(t *testing.T) {
			row, lane := base, upstreamUsageLaneHistory
			switch mode {
			case "tail_only":
				lane = upstreamUsageLaneTail
			case "history_backoff":
				row.UsageBackfillNextSyncAt = now + 3600
			case "auth_paused":
				row.UsageStatus = upstreamStatusReconnect
			case "tail_failed":
				row.UsageStatus = upstreamStatusError
			case "other_provider":
				row.Provider = upstreamProviderNewAPI
			case "no_recent_rows":
				row.Domain = "new-account.example"
			}
			original := planUpstreamUsageSyncForLane(row, now, 7, lane)
			got, err := m.prioritizeSub2RecentUsage(context.Background(), row, now, original)
			if err != nil {
				t.Fatal(err)
			}
			if got != original {
				t.Fatalf("recent repair bypassed %s: before=%+v after=%+v", mode, original, got)
			}
		})
	}
}

func TestSub2UsageVerifiedSparseAndZeroDaysStayUsable(t *testing.T) {
	for _, empty := range []bool{false, true} {
		t.Run(map[bool]string{false: "sparse", true: "zero"}[empty], func(t *testing.T) {
			day := cstDayStart(time.Now().Unix()) - 86400
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var requests, tokens int64
				cost := 0.0
				trend := []any{}
				if !empty {
					requests, tokens, cost = 4, 80, 1.5
					trend = append(trend, map[string]any{"date": r.URL.Query().Get("start_date") + " 00:00", "requests": requests, "total_tokens": tokens, "actual_cost": cost})
				}
				if r.URL.Path == "/api/v1/usage/dashboard/trend" {
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"start_date": r.URL.Query().Get("start_date"), "end_date": r.URL.Query().Get("end_date"), "granularity": "hour", "trend": trend}})
				} else if r.URL.Path == "/api/v1/usage/stats" {
					_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"total_requests": requests, "total_tokens": tokens, "total_actual_cost": cost}})
				} else {
					http.NotFound(w, r)
				}
			}))
			defer server.Close()
			row := ChannelUpstreamAccount{Domain: "sub2-projection.example", Provider: upstreamProviderSub2API, BaseURL: server.URL}
			result, _, err := syncSub2APIUsage(context.Background(), newUpstreamHTTPClient(time.Second), row, sub2APICredential{AccessToken: "test", ExpiresAt: time.Now().Add(time.Hour).Unix()}, day, day+86400, newUpstreamUsageRequestPacer(2, 0), "")
			if err != nil {
				t.Fatal(err)
			}
			accounts := map[string]ChannelUpstreamAccountView{row.Domain: {Provider: row.Provider, UsageSyncEnabled: true, UsageAdapter: result.Adapter}}
			scope := stabilityScope{FromTs: day, ToTs: day + 86400}
			actual, _, err := projectFinanceBillWindow(result.Hours, scope, day+2*86400, accounts, nil)
			if err != nil {
				t.Fatal(err)
			}
			want := 1.5
			if empty {
				want = 0
			}
			metric := actual[row.Domain]
			if !metric.Available || !metric.Complete || metric.Provisional || math.Abs(metric.CostUSD-want) > 1e-9 {
				t.Fatalf("verified day became unavailable/inaccurate: %+v", metric)
			}
			// The same amounts with one unproven omitted hour remain visible,
			// but neither module may call the bill complete.
			result.Hours[1].Provisional = true
			actual, _, err = projectFinanceBillWindow(result.Hours, scope, day+2*86400, accounts, nil)
			if err != nil {
				t.Fatal(err)
			}
			if metric = actual[row.Domain]; !metric.Available || metric.Complete || !metric.Provisional || math.Abs(metric.CostUSD-want) > 1e-9 {
				t.Fatalf("unproven hour hidden or promoted: %+v", metric)
			}
		})
	}
}

func TestSub2UsageMissingHourCannotRetainOverlappingDay(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	day := cstDayStart(time.Now().Unix())
	old := ChannelUpstreamUsageHour{Domain: "daily-sub2.example", Provider: upstreamProviderSub2API, HourTs: day, BucketSeconds: 7200, CostUSD: 10, Quota: 10, FetchedAt: day + 7200}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	newHours := []ChannelUpstreamUsageHour{{Domain: old.Domain, Provider: old.Provider, HourTs: day, BucketSeconds: 3600, Provisional: true}, {Domain: old.Domain, Provider: old.Provider, HourTs: day + 3600, BucketSeconds: 3600, CostUSD: 3, Quota: 3}}
	if err := m.persistUpstreamUsageWindow(context.Background(), old.Domain, day, day+7200, newHours, day+7300); err == nil {
		t.Fatal("daily total was retained alongside hourly values")
	}
	var got []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", old.Domain).Find(&got).Error; err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !reflect.DeepEqual(got[0], old) {
		t.Fatalf("incompatible partial replacement changed old day: %+v", got)
	}
}

func TestSub2UsageVerificationHonorsRequestBudget(t *testing.T) {
	server, paths, mu := sub2BillFixture(t, map[string]any{"total_requests": 4, "total_tokens": 80, "total_actual_cost": 1.5}, 0)
	from := cstDayStart(time.Now().Unix()) - 86400
	result, _, err := syncSub2APIUsage(context.Background(), newUpstreamHTTPClient(time.Second), ChannelUpstreamAccount{Domain: "budget.example", Provider: upstreamProviderSub2API, BaseURL: server.URL}, sub2APICredential{AccessToken: "bill-test-access", ExpiresAt: time.Now().Add(time.Hour).Unix()}, from, from+86400, newUpstreamUsageRequestPacer(1, 0), "")
	if err == nil || !upstreamUsageRunBudgetWasExhausted(err) || len(result.Hours) != 0 {
		t.Fatalf("unverified budget-exhausted day could be published: err=%v rows=%d", err, len(result.Hours))
	}
	mu.Lock()
	defer mu.Unlock()
	if len(*paths) != 1 {
		t.Fatalf("verification bypassed request budget: %v", *paths)
	}
}
