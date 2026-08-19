package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type upstreamUsageFixtureRow struct {
	ID        int64
	CreatedAt int64
}

func newUpstreamUsageFixtureServer(t *testing.T, initial []upstreamUsageFixtureRow, afterFirst func(*[]upstreamUsageFixtureRow)) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	rows := append([]upstreamUsageFixtureRow(nil), initial...)
	var mu sync.Mutex
	var firstPageOnce sync.Once
	var totalQueries atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/log/" || r.Header.Get("Authorization") != "Bearer usage-token" || r.Header.Get("New-Api-User") != "31" {
			http.Error(w, `{"message":"bad auth"}`, http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		from, fromErr := strconv.ParseInt(q.Get("start_timestamp"), 10, 64)
		endInclusive, endErr := strconv.ParseInt(q.Get("end_timestamp"), 10, 64)
		pageNumber, pageErr := strconv.Atoi(q.Get("p"))
		if fromErr != nil || endErr != nil || pageErr != nil || pageNumber <= 0 || q.Get("page_size") != "100" || q.Get("type") != "2" {
			http.Error(w, `{"message":"bad query"}`, http.StatusBadRequest)
			return
		}
		if q.Get("cursor") != "" || q.Get("before_id") != "" || q.Get("skip_total") != "" {
			http.Error(w, `{"message":"unexpected new protocol query"}`, http.StatusBadRequest)
			return
		}

		mu.Lock()
		filtered := make([]upstreamUsageFixtureRow, 0, len(rows))
		for _, row := range rows {
			if row.CreatedAt >= from && row.CreatedAt <= endInclusive {
				filtered = append(filtered, row)
			}
		}
		sort.Slice(filtered, func(i, j int) bool { return filtered[i].ID > filtered[j].ID })
		total := len(filtered)
		start := (pageNumber - 1) * upstreamUsagePageSize
		end := start + upstreamUsagePageSize
		if start > len(filtered) {
			start = len(filtered)
		}
		if end > len(filtered) {
			end = len(filtered)
		}
		filtered = filtered[start:end]
		totalQueries.Add(1)
		if pageNumber == 1 {
			firstPageOnce.Do(func() {
				if afterFirst != nil {
					afterFirst(&rows)
				}
			})
		}
		mu.Unlock()

		items := make([]map[string]any, 0, len(filtered))
		for index, row := range filtered {
			items = append(items, map[string]any{
				"id": start + index + 1, "created_at": row.CreatedAt, "quota": 500000,
				"prompt_tokens": 2, "completion_tokens": 1,
			})
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"total": total, "items": items},
		})
	}))
	t.Cleanup(server.Close)
	return server, &totalQueries
}

func usageFixtureRows(count int, from, span int64) []upstreamUsageFixtureRow {
	rows := make([]upstreamUsageFixtureRow, 0, count)
	for i := 1; i <= count; i++ {
		rows = append(rows, upstreamUsageFixtureRow{ID: int64(i), CreatedAt: from + int64(i-1)*span/int64(count)})
	}
	return rows
}

func TestPlanUpstreamUsageSyncKeepsTodayIndependentFromHistory(t *testing.T) {
	now := time.Date(2026, 8, 14, 16, 37, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		UsageDataUntil:      today + 10*3600 + 123,
		UsageBackfillCursor: today - 40*86400,
	}
	plan := planUpstreamUsageSync(row, now, 90)
	if plan.tailTo != now || plan.tailFrom != today+7*3600 {
		t.Fatalf("tail=[%d,%d), want [%d,%d)", plan.tailFrom, plan.tailTo, today+7*3600, now)
	}
	if plan.backfillFrom != row.UsageBackfillCursor || plan.backfillTo != row.UsageBackfillCursor+86400 {
		t.Fatalf("backfill=[%d,%d)", plan.backfillFrom, plan.backfillTo)
	}
	row.UsageBackfillNextSyncAt = now + 3600
	plan = planUpstreamUsageSync(row, now, 90)
	if plan.tailTo != now || plan.tailFrom != today+7*3600 {
		t.Fatalf("backfill retry suppressed tail: %+v", plan)
	}
	if plan.backfillFrom != 0 || plan.backfillTo != 0 {
		t.Fatalf("backfill ran before retry time: %+v", plan)
	}
}

func TestApplyUpstreamUsageBackfillFailureDoesNotChangeTailHealth(t *testing.T) {
	now := time.Date(2026, 8, 14, 16, 37, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{
		Domain: "example.com", UsageStatus: upstreamStatusOK,
		UsageLastSuccessAt: now - 60, UsageDataUntil: now - 30,
		UsageNextSyncAt: now + 1800,
	}
	applyUpstreamUsageBackfillResult(&row, errors.New("history timeout token-secret"), now, Settings{UpstreamUsageSyncMinutes: 30}, "token-secret")
	if row.UsageStatus != upstreamStatusOK || row.UsageLastSuccessAt != now-60 || row.UsageDataUntil != now-30 || row.UsageNextSyncAt != now+1800 {
		t.Fatalf("history failure changed tail state: %+v", row)
	}
	if row.UsageBackfillConsecutiveFails != 1 || row.UsageBackfillNextSyncAt <= now || strings.Contains(row.UsageBackfillLastError, "token-secret") {
		t.Fatalf("invalid history backoff state: %+v", row)
	}
}

func TestFetchNewAPIUsageWindowAccepts5000AndSplits5001(t *testing.T) {
	from := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC).Unix()
	for _, count := range []int{5000, 5001} {
		t.Run(strconv.Itoa(count), func(t *testing.T) {
			rows := usageFixtureRows(count, from, 4*3600)
			server, totalQueries := newUpstreamUsageFixtureServer(t, rows, nil)
			pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
			result, err := fetchNewAPIUsageWindowWithPacer(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
				Domain: "example.com", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
			}, newAPICredential{AccessToken: "usage-token"}, from, from+4*3600, pacer)
			if err != nil {
				t.Fatal(err)
			}
			var requests int64
			for _, hour := range result.Hours {
				requests += hour.Requests
			}
			if requests != int64(count) {
				t.Fatalf("requests=%d, want %d", requests, count)
			}
			wantTotalQueries := int64(51)
			if count == 5001 {
				wantTotalQueries = 54 // dense probe + two offset scans and their probes
			}
			if totalQueries.Load() != wantTotalQueries {
				t.Fatalf("total-bearing requests=%d, want %d", totalQueries.Load(), wantTotalQueries)
			}
			wantCalls := 51
			if count == 5001 {
				wantCalls = 54
			}
			if pacer.calls != wantCalls {
				t.Fatalf("HTTP calls=%d, want %d", pacer.calls, wantCalls)
			}
		})
	}
}

func TestUpstreamUsageRequestPacerBoundsAndSpacesBurst(t *testing.T) {
	pacer := newUpstreamUsageRequestPacer(2, 20*time.Millisecond)
	ctx := context.Background()
	if err := pacer.beforeRequest(ctx); err != nil {
		t.Fatal(err)
	}
	started := time.Now()
	if err := pacer.beforeRequest(ctx); err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed < 15*time.Millisecond {
		t.Fatalf("second request was not paced: %s", elapsed)
	}
	if err := pacer.beforeRequest(ctx); err == nil || !strings.Contains(err.Error(), "安全上限") {
		t.Fatalf("third request should hit the hard budget, err=%v", err)
	}
}

func TestFetchNewAPIUsageWindowRejectsConcurrentHeadInsert(t *testing.T) {
	from := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC).Unix()
	rows := usageFixtureRows(150, from, 3600)
	inserted := upstreamUsageFixtureRow{ID: 151, CreatedAt: from + 1800}
	server, _ := newUpstreamUsageFixtureServer(t, rows, func(rows *[]upstreamUsageFixtureRow) {
		*rows = append(*rows, inserted)
	})
	_, err := fetchNewAPIUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "example.com", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
	}, newAPICredential{AccessToken: "usage-token"}, from, from+3600)
	if err == nil || !strings.Contains(err.Error(), "total 变化") {
		t.Fatalf("concurrent insert should fail closed, err=%v", err)
	}
}

func TestFetchNewAPIUsageWindowRejectsSameTotalHeadMutation(t *testing.T) {
	from := time.Date(2026, 8, 10, 0, 0, 0, 0, time.UTC).Unix()
	rows := usageFixtureRows(150, from, 3600)
	server, _ := newUpstreamUsageFixtureServer(t, rows, func(rows *[]upstreamUsageFixtureRow) {
		for index := range *rows {
			if (*rows)[index].ID == 150 {
				(*rows)[index].CreatedAt++
				return
			}
		}
	})
	_, err := fetchNewAPIUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "example.com", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
	}, newAPICredential{AccessToken: "usage-token"}, from, from+3600)
	if err == nil || !strings.Contains(err.Error(), "首页已变化") {
		t.Fatalf("same-total head mutation should fail closed, err=%v", err)
	}
}
