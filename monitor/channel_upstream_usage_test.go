package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

type upstreamUsageFixtureRow struct {
	ID               int64
	CreatedAt        int64
	Group            string
	ModelName        string
	TokenID          int64
	Other            any
	PromptTokens     int64
	CompletionTokens int64
	Quota            int64
}

func newUpstreamUsageFixtureServer(t *testing.T, initial []upstreamUsageFixtureRow, afterFirst func(*[]upstreamUsageFixtureRow)) (*httptest.Server, *atomic.Int64) {
	t.Helper()
	rows := append([]upstreamUsageFixtureRow(nil), initial...)
	var mu sync.Mutex
	var firstPageOnce sync.Once
	var totalQueries atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/log/self" || r.Header.Get("Authorization") != "Bearer usage-token" || r.Header.Get("New-Api-User") != "31" {
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
			quota, prompt, completion := row.Quota, row.PromptTokens, row.CompletionTokens
			if quota == 0 {
				quota = 500000
			}
			if prompt == 0 && completion == 0 {
				prompt, completion = 2, 1
			}
			item := map[string]any{
				"id": start + index + 1, "created_at": row.CreatedAt, "quota": quota,
				"prompt_tokens": prompt, "completion_tokens": completion,
			}
			if row.Group != "" {
				item["group"] = row.Group
			}
			if row.ModelName != "" {
				item["model_name"] = row.ModelName
			}
			if row.TokenID != 0 {
				item["token_id"] = row.TokenID
			}
			if row.Other != nil {
				item["other"] = row.Other
			}
			items = append(items, item)
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

func TestPlanNewAPIUsageBackfillAdvancesOneClosedHour(t *testing.T) {
	now := time.Date(2026, 8, 14, 16, 37, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		Provider:                      upstreamProviderNewAPI,
		UsageStatus:                   upstreamStatusOK,
		UsageNextSyncAt:               now + 3600,
		UsageBackfillCursor:           today - 86400,
		UsageBackfillNextSyncAt:       now - 1,
		UsageBackfillLastAttemptAt:    now - 3600,
		UsageBackfillLastSuccessAt:    now - 3600,
		UsageBackfillConsecutiveFails: 0,
	}
	plan := planUpstreamUsageSync(row, now, 90)
	if plan.tailTo != 0 || plan.backfillFrom != row.UsageBackfillCursor || plan.backfillTo != row.UsageBackfillCursor+3600 {
		t.Fatalf("NewAPI history must advance exactly one closed hour: %+v", plan)
	}
}

func TestPlanUpstreamUsageSyncDoesNotBypassTailBackoff(t *testing.T) {
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		UsageStatus:                   upstreamStatusError,
		UsageNextSyncAt:               now + 1800,
		UsageBackfillCursor:           today - 7*86400,
		UsageBackfillNextSyncAt:       now - 1,
		UsageBackfillDone:             false,
		UsageConsecutiveFails:         1,
		UsageBackfillConsecutiveFails: 0,
	}
	plan := planUpstreamUsageSync(row, now, 90)
	if plan != (upstreamUsageSyncPlan{}) {
		t.Fatalf("history bypassed tail backoff: %+v", plan)
	}

	row.UsageStatus = upstreamStatusOK
	plan = planUpstreamUsageSync(row, now, 90)
	if plan.tailTo != 0 || plan.backfillFrom != row.UsageBackfillCursor || plan.backfillTo <= plan.backfillFrom {
		t.Fatalf("healthy history-only lane was not scheduled: %+v", plan)
	}
}

func TestTailFailureDefersHistoryWithoutChangingHistoryFailureCounter(t *testing.T) {
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Domain: "rate-limit.example", UsageBackfillNextSyncAt: now - 1}
	applyUpstreamUsageResult(&row, upstreamUsageResult{}, &upstreamHTTPError{Status: http.StatusTooManyRequests, RetryAt: now + 3600}, now, Settings{UpstreamUsageSyncMinutes: 30})
	coupleUpstreamUsageHistoryRetryToTail(&row)
	if row.UsageNextSyncAt < now+3600 || row.UsageBackfillNextSyncAt != row.UsageNextSyncAt {
		t.Fatalf("429 retry was not coupled to history deadline: %+v", row)
	}
	if row.UsageBackfillConsecutiveFails != 0 || row.UsageBackfillLastError != "" {
		t.Fatalf("tail failure polluted history health: %+v", row)
	}

	auth := ChannelUpstreamAccount{Domain: "auth.example"}
	applyUpstreamUsageResult(&auth, upstreamUsageResult{}, &upstreamAuthError{err: errors.New("bad credential")}, now, Settings{})
	coupleUpstreamUsageHistoryRetryToTail(&auth)
	if auth.UsageNextSyncAt != upstreamAccountIsolatedUntil || auth.UsageBackfillNextSyncAt != upstreamAccountIsolatedUntil {
		t.Fatalf("auth isolation did not cover both lanes: %+v", auth)
	}
}

func TestDecorateUpstreamUsageHealthIsFailClosedForGrayAndStaleData(t *testing.T) {
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: now - 10}
	view := upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, Settings{UpstreamUsageSyncMinutes: 30}, now)
	if view.UsageWorkerEnabled || view.UsageEffectiveStatus != "global_off" || !view.UsageFresh {
		t.Fatalf("gray-off view is misleading: %+v", view)
	}

	row.UsageLastAttemptAt = now - 10
	row.UsageLastSuccessAt = now - 10
	view = upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, Settings{UpstreamUsageSyncEnabled: true, UpstreamUsageSyncMinutes: 30}, now)
	if !view.UsageWorkerEnabled || view.UsageEffectiveStatus != upstreamStatusOK || !view.UsageFresh {
		t.Fatalf("fresh enabled view is not healthy: %+v", view)
	}

	row.UsageDataUntil = now - 2*3600
	view = upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, Settings{UpstreamUsageSyncEnabled: true, UpstreamUsageSyncMinutes: 30}, now)
	if view.UsageEffectiveStatus != "stale" || view.UsageFresh || view.UsageLagSeconds != 2*3600 {
		t.Fatalf("stale persisted success remained green: %+v", view)
	}
}

func TestDecorateUpstreamUsageHealthSeparatesFirstRunTailAndHistoryPhases(t *testing.T) {
	now := time.Date(2026, 8, 27, 18, 0, 0, 0, cstLocation).Unix()
	settings := Settings{UpstreamUsageSyncEnabled: true, UpstreamUsageSyncMinutes: 30}
	row := ChannelUpstreamAccount{
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusDisabled,
		UsageNextSyncAt: now + 10, UsageBackfillNextSyncAt: now + 20,
	}
	view := upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, settings, now)
	if view.UsageEffectiveStatus != "queued" || view.UsageTailPhase != "queued" || view.UsageHistoryPhase != "queued" {
		t.Fatalf("newly enabled account was not exposed as queued: %+v", view)
	}

	row.UsageStatus = upstreamStatusOK
	row.UsageLastAttemptAt = now - 30
	row.UsageLastSuccessAt = now - 30
	row.UsageDataUntil = now - 30
	row.UsageBackfillLastAttemptAt = now - 20
	row.UsageBackfillLastError = "upstream timeout"
	view = upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, settings, now)
	if view.UsageTailPhase != upstreamStatusOK || view.UsageHistoryPhase != "retry" {
		t.Fatalf("history retry polluted a healthy current-day Tail: %+v", view)
	}

	row.UsageBackfillLastError = ""
	row.UsageBackfillDone = true
	view = upstreamAccountView(row)
	decorateUpstreamUsageHealth(&view, row, settings, now)
	if view.UsageHistoryPhase != "complete" {
		t.Fatalf("completed history was not projected independently: %+v", view)
	}
}

func TestSyncChannelUpstreamUsageHandlerHonorsGlobalGraySwitch(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{UpstreamUsageSyncEnabled: false}}
	router := gin.New()
	router.POST("/sync", m.syncChannelUpstreamUsageHandler)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/sync", strings.NewReader(`{"domain":"example.com"}`))
	req.Header.Set("Content-Type", "application/json")
	router.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable || !strings.Contains(w.Body.String(), "灰度关闭") {
		t.Fatalf("gray switch handler status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestDueUsageSelectionSkipsBackedOffAccountAndDoesNotStarvePeer(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	rows := []ChannelUpstreamAccount{
		{Domain: "a-blocked.example", Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusError,
			UsageNextSyncAt: now + 1800, UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1},
		{Domain: "b-due.example", Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
			UsageNextSyncAt: now - 1, UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1},
		{Domain: "c-history.example", Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
			UsageNextSyncAt: now + 1800, UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1},
		{Domain: "c-pending-tail.example", Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusPending,
			UsageNextSyncAt: now + 15, UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1},
		{Domain: "d-account-disabled.example", Enabled: false, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
			UsageNextSyncAt: now - 1, UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1},
		{Domain: "e-usage-disabled.example", Enabled: true, UsageSyncEnabled: false, UsageStatus: upstreamStatusOK,
			UsageNextSyncAt: now - 1, UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1},
	}
	for index := range rows {
		if err := m.storeDB.Create(&rows[index]).Error; err != nil {
			t.Fatal(err)
		}
	}
	due, err := m.loadDueUpstreamUsageAccountsForLane(context.Background(), now, 10, upstreamUsageLaneTail)
	if err != nil {
		t.Fatal(err)
	}
	var domains []string
	for _, row := range due {
		domains = append(domains, row.Domain)
	}
	if strings.Contains(strings.Join(domains, ","), "a-blocked.example") {
		t.Fatalf("backed-off account was selected by history lane: %v", domains)
	}
	if len(domains) != 1 || domains[0] != "b-due.example" {
		t.Fatalf("tail lane selected non-tail work: %v", domains)
	}
	due, err = m.loadDueUpstreamUsageAccountsForLane(context.Background(), now, 10, upstreamUsageLaneHistory)
	if err != nil {
		t.Fatal(err)
	}
	domains = domains[:0]
	for _, row := range due {
		domains = append(domains, row.Domain)
	}
	if len(domains) != 2 || !containsString(domains, "b-due.example") || !containsString(domains, "c-history.example") {
		t.Fatalf("eligible history peers were starved or a disabled account was selected: %v", domains)
	}
}

func TestPlanUpstreamUsageSyncForLaneMasksOtherWork(t *testing.T) {
	now := time.Date(2026, 8, 23, 9, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{
		Domain: "both-due.example", Provider: upstreamProviderNewAPI,
		UsageNextSyncAt: now - 1, UsageBackfillCursor: cstDayStart(now) - 86400,
	}
	tail := planUpstreamUsageSyncForLane(row, now, 7, upstreamUsageLaneTail)
	if tail.tailTo == 0 || tail.backfillTo != 0 {
		t.Fatalf("tail lane plan leaked history work: %+v", tail)
	}
	history := planUpstreamUsageSyncForLane(row, now, 7, upstreamUsageLaneHistory)
	if history.tailTo != 0 || history.backfillTo == 0 {
		t.Fatalf("history lane plan leaked tail work: %+v", history)
	}
}

func TestUpstreamUsageFailureRetryPolicy(t *testing.T) {
	now := int64(1_800_000_000)
	settings := Settings{UpstreamUsageSyncMinutes: 20}
	tests := []struct {
		name     string
		err      error
		failures int
		want     time.Duration
	}{
		{name: "first timeout", err: context.DeadlineExceeded, failures: 1, want: 2 * time.Minute},
		{name: "second timeout", err: context.DeadlineExceeded, failures: 2, want: 5 * time.Minute},
		{name: "connection refused", err: errors.New("dial tcp: connection refused"), failures: 1, want: 2 * time.Minute},
		{name: "moving upstream page", err: errors.New("NewAPI 使用日志扫描期间 total 变化（334 -> 335）"), failures: 1, want: 2 * time.Minute},
		{name: "sqlite busy", err: errors.New("database is locked"), failures: 1, want: 10 * time.Second},
		{name: "sqlite busy repeated", err: errors.New("SQLITE_BUSY"), failures: 3, want: time.Minute},
		{name: "rate limit default", err: &upstreamHTTPError{Status: http.StatusTooManyRequests}, failures: 1, want: upstreamRetryAfterDefault},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got := upstreamUsageFailureRetryAt(settings, "retry.example", now, test.failures, test.err, "tail")
			if got != now+int64(test.want/time.Second) {
				t.Fatalf("retry_at=%d, want %d", got, now+int64(test.want/time.Second))
			}
		})
	}
}

func TestAICodeWithTailKeepsThirtyMinuteMinimum(t *testing.T) {
	now := int64(1_800_000_000)
	settings := Settings{UpstreamUsageSyncMinutes: 20}
	regular := nextUpstreamUsageSyncAtForProvider(settings, upstreamProviderNewAPI, "regular.example", now, 0)
	aicode := nextUpstreamUsageSyncAtForProvider(settings, upstreamProviderAICodeWith, "aicode.example", now, 0)
	if regular < now+20*60 || regular > now+20*60+45 {
		t.Fatalf("regular next sync outside 20-minute window: %d", regular-now)
	}
	if aicode < now+30*60 || aicode > now+30*60+45 {
		t.Fatalf("AICodeWith next sync outside 30-minute window: %d", aicode-now)
	}
}

func TestNewAPITailIncrementalModeRequiresStaleOrObservedDenseAccount(t *testing.T) {
	now := int64(1_800_000_000)
	if newAPITailNeedsIncrementalSync(ChannelUpstreamAccount{Provider: upstreamProviderNewAPI}, now) {
		t.Fatal("new low-volume account must keep the one-window request path")
	}
	if !newAPITailNeedsIncrementalSync(ChannelUpstreamAccount{
		Provider: upstreamProviderNewAPI, UsageDataUntil: now - int64(upstreamUsageTailOverlap/time.Second) - 1,
	}, now) {
		t.Fatal("stale watermark must use forward-first hourly recovery")
	}
	if !newAPITailNeedsIncrementalSync(ChannelUpstreamAccount{
		Provider: upstreamProviderNewAPI, UsageDataUntil: now - 60, UsageTailMode: upstreamUsageTailModeHourly,
	}, now) {
		t.Fatal("learned dense account lost its durable hourly strategy")
	}
	if !upstreamUsageRunBudgetWasExhausted(&upstreamUsageRunBudgetExhausted{max: upstreamUsageMaxRequestsPerRun}) {
		t.Fatal("request-budget exhaustion was not recognized")
	}
}

func TestHistoryFairnessRunsAfterFourHealthyTailBatchesButNeverBehindStaleTail(t *testing.T) {
	now := int64(1_800_000_000)
	healthyTail := ChannelUpstreamAccount{UsageNextSyncAt: now - int64((upstreamUsageTailOverdueGuard-time.Second)/time.Second)}
	if shouldRunUpstreamUsageHistoryFairness(now, healthyTail, upstreamUsageHistoryFairnessTailBatches-1) {
		t.Fatal("history ran before the four-tail fairness quota")
	}
	if !shouldRunUpstreamUsageHistoryFairness(now, healthyTail, upstreamUsageHistoryFairnessTailBatches) {
		t.Fatal("history remained starved after four healthy tail batches")
	}
	staleTail := ChannelUpstreamAccount{UsageNextSyncAt: now - int64((upstreamUsageTailOverdueGuard+time.Second)/time.Second)}
	if shouldRunUpstreamUsageHistoryFairness(now, staleTail, upstreamUsageHistoryFairnessTailBatches+100) {
		t.Fatal("history must not run while tail violates its freshness guard")
	}
	if shouldRunUpstreamUsageHistoryFairness(now, ChannelUpstreamAccount{}, upstreamUsageHistoryFairnessTailBatches) {
		t.Fatal("zero tail watermark must not spend a history fairness turn")
	}
}

func TestDueUsageRunnerSkipsBusyAccountWithoutSpendingConcurrencySlot(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/log/self" {
			http.NotFound(w, r)
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"total":0,"items":[]}}`))
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageBackfillDays = 7
	now := time.Now().Unix()
	busy := ChannelUpstreamAccount{
		Domain: "a-busy.example", Provider: upstreamProviderNewAPI, BaseURL: "https://a-busy.example",
		Enabled: true, UsageSyncEnabled: true, UsageNextSyncAt: now - 2,
	}
	if err := m.storeDB.Create(&busy).Error; err != nil {
		t.Fatal(err)
	}
	healthy := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL,
		Account: "31", UserID: 31, Enabled: true, UsageSyncEnabled: true,
		UsageNextSyncAt: now - 1, UsageBackfillDone: true,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &healthy, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	release, err := m.tryAcquireUpstreamAccountBackground(busy.Domain)
	if err != nil {
		t.Fatal(err)
	}
	defer release()

	m.runDueUpstreamUsageAccounts(context.Background(), []ChannelUpstreamAccount{busy, healthy}, upstreamUsageLaneTail, 1)
	if calls.Load() != 1 {
		t.Fatalf("healthy peer calls=%d, want 1 after busy candidate was skipped", calls.Load())
	}
	var stored ChannelUpstreamAccount
	if err := m.storeDB.First(&stored, "domain = ?", healthy.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if stored.UsageStatus != upstreamStatusOK || stored.UsageLastSuccessAt == 0 {
		t.Fatalf("healthy peer did not consume the available slot: %+v", stored)
	}
}

func TestAICodeWithHistoryFailureDoesNotPolluteTailKeyState(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := int64(1_800_000_000)
	state := AICodeWithKeySyncState{
		Domain: "aicode.example", SlotID: "slot-1", CredentialSetVersion: "v1",
		Status: upstreamStatusOK, LastSuccessAt: now - 30, NextSyncAt: now + 1800,
	}
	round := AICodeWithUsageRound{Domain: state.Domain, Kind: "backfill", RoundID: "history-round", CredentialSetVersion: state.CredentialSetVersion}
	err := m.recordAICodeWithKeyFailure(context.Background(), round, &state,
		&upstreamHTTPError{Status: http.StatusBadGateway, Message: "temporary"}, "secret", now)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != upstreamStatusOK || state.LastSuccessAt != now-30 || state.NextSyncAt != now+1800 || state.ConsecutiveFails != 0 {
		t.Fatalf("history failure polluted tail fields: %+v", state)
	}
	if state.BackfillConsecutiveFails != 1 || state.BackfillLastError == "" || state.BackfillNextSyncAt != now+120 {
		t.Fatalf("history failure was not recorded independently: %+v", state)
	}
}

func TestAICodeWithHistorySuccessDoesNotRewriteTailKeyWatermark(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := int64(1_800_000_000)
	state := AICodeWithKeySyncState{
		Domain: "aicode.example", SlotID: "slot-1", CredentialSetVersion: "v1",
		Status: upstreamStatusOK, LastAttemptAt: now - 60, LastSuccessAt: now - 60,
		NextSyncAt: now + 1800, TailRoundID: "tail-round",
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	round := AICodeWithUsageRound{
		Domain: state.Domain, Kind: "backfill", RoundID: "history-round", CredentialSetVersion: state.CredentialSetVersion,
		WindowFrom: now - 86400, WindowTo: now,
	}
	result := upstreamUsageResult{SourceKeyID: 7, Adapter: upstreamUsageAdapterAICodeWith}
	if err := m.stageAICodeWithKeyResult(context.Background(), round, &state, result, now); err != nil {
		t.Fatal(err)
	}
	if state.Status != upstreamStatusOK || state.LastAttemptAt != now-60 || state.LastSuccessAt != now-60 || state.NextSyncAt != now+1800 || state.TailRoundID != "tail-round" {
		t.Fatalf("history success rewrote tail watermark: %+v", state)
	}
	if state.BackfillRoundID != round.RoundID || state.BackfillLastSuccessAt != now || state.BackfillConsecutiveFails != 0 || state.BackfillLastError != "" {
		t.Fatalf("history success was not recorded independently: %+v", state)
	}
}

func TestScheduledUsageFailureCannotBeRetriedThroughHistoryLane(t *testing.T) {
	for _, tc := range []struct {
		name       string
		statusCode int
		wantStatus string
	}{
		{name: "rate_limit", statusCode: http.StatusTooManyRequests, wantStatus: upstreamStatusError},
		{name: "credential", statusCode: http.StatusUnauthorized, wantStatus: upstreamStatusReconnect},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				if tc.statusCode == http.StatusTooManyRequests {
					w.Header().Set("Retry-After", "3600")
				}
				http.Error(w, `{"message":"scheduled failure"}`, tc.statusCode)
			}))
			defer server.Close()

			m := newChannelUpstreamTestMonitor(t)
			m.cfg.UpstreamUsageSyncEnabled = true
			m.cfg.UpstreamUsageBackfillDays = 7
			now := time.Now().Unix()
			row := ChannelUpstreamAccount{
				Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL,
				Account: "31", UserID: 31, Enabled: true, UsageSyncEnabled: true,
				UsageStatus: upstreamStatusPending, UsageBackfillCursor: cstDayStart(now) - 7*86400,
			}
			if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
				t.Fatal(err)
			}
			m.syncDueUpstreamUsage(context.Background())
			if calls.Load() != 1 {
				t.Fatalf("first scheduled pass calls=%d, want 1", calls.Load())
			}
			var stored ChannelUpstreamAccount
			if err := m.storeDB.First(&stored, "domain = ?", row.Domain).Error; err != nil {
				t.Fatal(err)
			}
			if stored.UsageStatus != tc.wantStatus || stored.UsageNextSyncAt <= now || stored.UsageBackfillNextSyncAt != stored.UsageNextSyncAt {
				t.Fatalf("failure did not isolate both lanes: %+v", stored)
			}
			due, err := m.loadDueUpstreamUsageAccounts(context.Background(), time.Now().Unix(), 10)
			if err != nil {
				t.Fatal(err)
			}
			if len(due) != 0 {
				t.Fatalf("failed account remained due through history: %+v", due)
			}
			m.syncDueUpstreamUsage(context.Background())
			if calls.Load() != 1 {
				t.Fatalf("second scheduler pass bypassed backoff, calls=%d", calls.Load())
			}
		})
	}
}

func TestHistoryOnlyAuthFailureIsolatesTailAndHistory(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, `{"message":"credential expired"}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamUsageBackfillDays = 7
	now := time.Now().Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL,
		Account: "41", UserID: 41, Enabled: true, UsageSyncEnabled: true,
		UsageStatus: upstreamStatusOK, UsageDataUntil: now, UsageNextSyncAt: now + 3600,
		UsageBackfillCursor: today - 86400, UsageBackfillNextSyncAt: now - 1,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "history-token"}); err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamUsage(context.Background(), row.Domain)
	if err == nil {
		t.Fatal("history-only 401 unexpectedly succeeded")
	}
	if calls.Load() != 1 {
		t.Fatalf("history-only 401 calls=%d, want 1", calls.Load())
	}
	if synced.UsageStatus != upstreamStatusReconnect || synced.UsageNextSyncAt != upstreamAccountIsolatedUntil ||
		synced.UsageBackfillNextSyncAt != upstreamAccountIsolatedUntil {
		t.Fatalf("history auth failure did not isolate both lanes: %+v", synced)
	}
	view := m.channelUpstreamAccountView(synced)
	if view.UsageEffectiveStatus != upstreamStatusReconnect {
		t.Fatalf("history auth failure remained visually healthy: %+v", view)
	}
	due, loadErr := m.loadDueUpstreamUsageAccounts(context.Background(), time.Now().Unix(), 10)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if len(due) != 0 {
		t.Fatalf("auth-isolated history account remained scheduled: %+v", due)
	}
}

func TestPlanAICodeWithUsageSyncRefreshesTodayAndBackfills31Days(t *testing.T) {
	now := time.Date(2026, 8, 19, 11, 30, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		Provider:            upstreamProviderAICodeWith,
		UsageDataUntil:      today + 10*3600,
		UsageBackfillCursor: today - 70*86400,
	}
	plan := planUpstreamUsageSync(row, now, 90)
	if plan.tailFrom != today || plan.tailTo != now {
		t.Fatalf("AICodeWith tail must re-read today's cumulative bill: %+v", plan)
	}
	if plan.backfillFrom != row.UsageBackfillCursor || plan.backfillTo != row.UsageBackfillCursor+31*86400 {
		t.Fatalf("AICodeWith history must use one bounded 31-day request: %+v", plan)
	}
	row.UsageBackfillCursor = today - 10*86400
	plan = planUpstreamUsageSync(row, now, 90)
	if plan.backfillTo != today {
		t.Fatalf("last AICodeWith history batch must stop before the open day: %+v", plan)
	}
}

func TestPlanSub2APIUsageSyncRereadsOpenNaturalDay(t *testing.T) {
	now := time.Date(2026, 8, 19, 11, 30, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		Provider:            upstreamProviderSub2API,
		UsageDataUntil:      today + 10*3600,
		UsageBackfillCursor: today - 10*86400,
	}
	plan := planUpstreamUsageSync(row, now, 90)
	if plan.tailFrom != today || plan.tailTo != now {
		t.Fatalf("Sub2API tail must re-read today's aggregate: %+v", plan)
	}
	if plan.backfillFrom != row.UsageBackfillCursor || plan.backfillTo != row.UsageBackfillCursor+86400 {
		t.Fatalf("Sub2API history must advance one natural day: %+v", plan)
	}
}

func TestFetchSub2APIUsageTrendKeepsZeroAndPartialHours(t *testing.T) {
	from := time.Date(2026, 8, 19, 0, 0, 0, 0, cstLocation).Unix()
	to := from + 2*3600 + 900
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/usage/dashboard/trend" || r.Header.Get("Authorization") != "Bearer sub2-access" {
			http.Error(w, `{"message":"bad request"}`, http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		if q.Get("start_date") != "2026-08-19" || q.Get("end_date") != "2026-08-19" || q.Get("timezone") != "Asia/Shanghai" || q.Get("granularity") != "hour" {
			http.Error(w, `{"message":"bad range"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
			"start_date": "2026-08-19", "end_date": "2026-08-19", "granularity": "hour",
			"trend": []any{
				map[string]any{"date": "2026-08-19 00:00", "requests": 3, "total_tokens": 120, "actual_cost": 1.25},
				map[string]any{"date": "2026-08-19 02:00", "requests": 1, "total_tokens": 10, "actual_cost": 0.2},
			},
		}})
	}))
	defer server.Close()
	result, err := fetchSub2APIUsageTrend(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: server.URL,
	}, sub2APICredential{AccessToken: "sub2-access"}, from, to, newUpstreamUsageRequestPacer(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if result.Adapter != upstreamUsageAdapterSub2Trend || result.DataUntil != to || len(result.Hours) != 3 {
		t.Fatalf("Sub2API trend result=%+v", result)
	}
	if result.Hours[0].Requests != 3 || result.Hours[0].Tokens != 120 || math.Abs(result.Hours[0].CostUSD-1.25) > 1e-9 || result.Hours[0].BucketSeconds != 3600 {
		t.Fatalf("first bucket=%+v", result.Hours[0])
	}
	if result.Hours[1].Requests != 0 || result.Hours[1].CostUSD != 0 || result.Hours[1].BucketSeconds != 3600 {
		t.Fatalf("zero bucket=%+v", result.Hours[1])
	}
	if result.Hours[2].Requests != 1 || result.Hours[2].BucketSeconds != 900 {
		t.Fatalf("partial bucket=%+v", result.Hours[2])
	}
}

func TestFetchSub2APIUsageFallsBackToDailyStatsOnce(t *testing.T) {
	from := time.Date(2026, 8, 18, 0, 0, 0, 0, cstLocation).Unix()
	var trendCalls, statsCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/usage/dashboard/trend":
			trendCalls.Add(1)
			http.Error(w, `{"message":"not found"}`, http.StatusNotFound)
		case "/api/v1/usage/stats":
			statsCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"data":{"total_requests":8,"total_tokens":900,"total_actual_cost":"7.5"}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "legacy-sub2.example", Provider: upstreamProviderSub2API, BaseURL: server.URL}
	cred := sub2APICredential{AccessToken: "sub2-access"}
	pacer := newUpstreamUsageRequestPacer(4, 0)
	result, err := fetchSub2APIUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), row, cred, from, from+86400, pacer, "")
	if err != nil {
		t.Fatal(err)
	}
	if result.Adapter != upstreamUsageAdapterSub2Stats || len(result.Hours) != 1 || result.Hours[0].BucketSeconds != 86400 || result.Hours[0].Requests != 8 || math.Abs(result.Hours[0].CostUSD-7.5) > 1e-9 {
		t.Fatalf("fallback result=%+v", result)
	}
	// Once capability detection has been persisted, the caller passes the
	// selected adapter and no longer probes the missing trend route.
	if _, err := fetchSub2APIUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), row, cred, from, from+86400, pacer, result.Adapter); err != nil {
		t.Fatal(err)
	}
	if trendCalls.Load() != 1 || statsCalls.Load() != 2 {
		t.Fatalf("trend calls=%d stats calls=%d", trendCalls.Load(), statsCalls.Load())
	}
}

func TestFetchSub2APIUsageDoesNotFallbackOnRateLimit(t *testing.T) {
	from := time.Date(2026, 8, 18, 0, 0, 0, 0, cstLocation).Unix()
	var trendCalls, statsCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/v1/usage/dashboard/trend":
			trendCalls.Add(1)
			w.Header().Set("Retry-After", "120")
			http.Error(w, `{"message":"rate limited"}`, http.StatusTooManyRequests)
		case "/api/v1/usage/stats":
			statsCalls.Add(1)
			http.Error(w, `{"message":"must not be called"}`, http.StatusInternalServerError)
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	_, err := fetchSub2APIUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "limited-sub2.example", Provider: upstreamProviderSub2API, BaseURL: server.URL,
	}, sub2APICredential{AccessToken: "sub2-access"}, from, from+86400, newUpstreamUsageRequestPacer(4, 0), "")
	if err == nil {
		t.Fatal("429 must fail closed")
	}
	var statusErr *upstreamHTTPError
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusTooManyRequests || statusErr.RetryAt <= time.Now().Unix() {
		t.Fatalf("rate-limit error=%v", err)
	}
	if trendCalls.Load() != 1 || statsCalls.Load() != 0 {
		t.Fatalf("trend calls=%d stats calls=%d", trendCalls.Load(), statsCalls.Load())
	}
}

func TestSyncSub2APIUsageRefreshesRotatingCredential(t *testing.T) {
	from := time.Date(2026, 8, 19, 0, 0, 0, 0, cstLocation).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"access-new","refresh_token":"refresh-new","expires_in":3600}}`))
		case "/api/v1/usage/dashboard/trend":
			if r.Header.Get("Authorization") != "Bearer access-new" {
				http.Error(w, `{"message":"bad bearer"}`, http.StatusUnauthorized)
				return
			}
			_, _ = w.Write([]byte(`{"code":0,"data":{"start_date":"2026-08-19","end_date":"2026-08-19","granularity":"hour","trend":[]}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	result, updated, err := syncSub2APIUsage(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: server.URL,
	}, sub2APICredential{AccessToken: "access-old", RefreshToken: "refresh-old", ExpiresAt: time.Now().Add(-time.Minute).Unix()}, from, from+3600, newUpstreamUsageRequestPacer(3, 0), "")
	if err != nil {
		t.Fatal(err)
	}
	if updated.AccessToken != "access-new" || updated.RefreshToken != "refresh-new" || result.Adapter != upstreamUsageAdapterSub2Trend {
		t.Fatalf("result=%+v credential=%+v", result, updated)
	}
}

func TestFetchAICodeWithUsageWindowValidatesSummaryAndKeepsZeroDays(t *testing.T) {
	const apiKey = "sk-acw-usage-test-secret"
	from := time.Date(2026, 8, 1, 0, 0, 0, 0, cstLocation).Unix()
	to := from + 2*86400
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/api-keys/usage" || r.Header.Get("Authorization") != "Bearer "+apiKey {
			http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"bad key"}}`, http.StatusUnauthorized)
			return
		}
		q := r.URL.Query()
		if q.Get("start") != "2026-08-01" || q.Get("end") != "2026-08-02" || q.Get("group_by") != "day" {
			http.Error(w, `{"error":{"type":"VALIDATION_ERROR","message":"bad range"}}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"api_key_id":6,"period":{"start":"2026-08-01","end":"2026-08-02"},"group_by":"day","summary":{"cost":"2.7168","total_tokens":12852,"requests":11},"daily":[{"date":"2026-08-01","cost":"2.7168","total_tokens":12852,"requests":11}]}}`))
	}))
	defer server.Close()
	result, err := fetchAICodeWithUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "aicodewith.com", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1,
	}, apiKey, from, to, newUpstreamUsageRequestPacer(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hours) != 2 || result.DataUntil != to || result.SourceKeyID != 6 {
		t.Fatalf("unexpected AICodeWith result: %+v", result)
	}
	first, second := result.Hours[0], result.Hours[1]
	if first.HourTs != from || first.BucketSeconds != 86400 || first.Requests != 11 || first.Tokens != 12852 || math.Abs(first.CostUSD-2.7168) > 1e-12 {
		t.Fatalf("first day=%+v", first)
	}
	if second.HourTs != from+86400 || second.BucketSeconds != 86400 || second.Requests != 0 || second.Tokens != 0 || second.CostUSD != 0 {
		t.Fatalf("zero-consumption day was not represented explicitly: %+v", second)
	}
}

func TestFetchAICodeWithUsageWindowKeepsCNYLedgerAtContractOneToOne(t *testing.T) {
	const apiKey = "sk-acw-cny-usage-test"
	from := time.Date(2026, 8, 20, 0, 0, 0, 0, cstLocation).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"api_key_id":9,"period":{"start":"2026-08-20","end":"2026-08-20"},"group_by":"day","summary":{"cost":"70.0000","total_tokens":100,"requests":2},"daily":[{"date":"2026-08-20","cost":"70.0000","total_tokens":100,"requests":2}]}}`))
	}))
	defer server.Close()

	result, err := fetchAICodeWithUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "aicodewith.com", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1,
	}, apiKey, from, from+86400, newUpstreamUsageRequestPacer(2, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Hours) != 1 || result.Hours[0].Quota != 70 || math.Abs(result.Hours[0].CostUSD-70) > 1e-12 {
		t.Fatalf("unexpected 1:1 CNY usage: %+v", result)
	}
}

func TestFetchAICodeWithUsageWindowRejectsSummaryMismatch(t *testing.T) {
	from := time.Date(2026, 8, 19, 0, 0, 0, 0, cstLocation).Unix()
	to := from + 12*3600
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"data":{"api_key_id":6,"period":{"start":"2026-08-19","end":"2026-08-19"},"group_by":"day","summary":{"cost":"2.0000","total_tokens":10,"requests":2},"daily":[{"date":"2026-08-19","cost":"1.0000","total_tokens":10,"requests":2}]}}`))
	}))
	defer server.Close()
	_, err := fetchAICodeWithUsageWindow(context.Background(), newUpstreamHTTPClient(3*time.Second), ChannelUpstreamAccount{
		Domain: "aicodewith.com", Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}, "sk-acw-test", from, to, nil)
	if err == nil || !strings.Contains(err.Error(), "summary") {
		t.Fatalf("mismatched independent control total must fail closed: %v", err)
	}
}

func TestSyncAICodeWithUsageRejectsDuplicateRemoteKeyID(t *testing.T) {
	from := time.Date(2026, 8, 18, 0, 0, 0, 0, cstLocation).Unix()
	to := from + 86400
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/api-keys/usage" {
			http.NotFound(w, r)
			return
		}
		// Two different configured secrets resolving to the same remote key ID
		// must not be summed twice.
		_, _ = w.Write([]byte(`{"data":{"api_key_id":6,"period":{"start":"2026-08-18","end":"2026-08-18"},"group_by":"day","summary":{"cost":"1.25","total_tokens":20,"requests":2},"daily":[{"date":"2026-08-18","cost":"1.25","total_tokens":20,"requests":2}]}}`))
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: "aicodewith.com", Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
	}
	_, err := m.syncAICodeWithUsage(context.Background(), row, aiCodeWithCredential{
		APIKeys: []string{"sk-acw-duplicate-a", "sk-acw-duplicate-b"},
	}, from, to, make(map[string]*upstreamUsageRequestPacer))
	if err == nil || !strings.Contains(err.Error(), "同一个 api_key_id") {
		t.Fatalf("duplicate remote key ID must fail closed: %v", err)
	}
}

func TestSyncStoredAICodeWithUsagePersistsTailAndHistoryAtomically(t *testing.T) {
	apiKeys := []string{"sk-acw-stored-sync-a", "sk-acw-stored-sync-b"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/api-keys/usage" {
			http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"bad key"}}`, http.StatusUnauthorized)
			return
		}
		apiKey := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		metric := map[string]aiCodeWithUsageMetric{
			apiKeys[0]: {Cost: 1, Tokens: 10, Requests: 1},
			apiKeys[1]: {Cost: 2, Tokens: 20, Requests: 2},
		}[apiKey]
		if metric.Requests == 0 {
			http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"bad key"}}`, http.StatusUnauthorized)
			return
		}
		start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
		keyID := int64(1)
		if apiKey == apiKeys[1] {
			keyID = 2
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"api_key_id": keyID,
			"period":     map[string]any{"start": start, "end": end}, "group_by": "day",
			"summary": map[string]any{"cost": metric.Cost, "total_tokens": metric.Tokens, "requests": metric.Requests},
			"daily": []any{map[string]any{
				"date": start, "cost": metric.Cost, "total_tokens": metric.Tokens, "requests": metric.Requests,
			}},
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageBackfillDays = 1
	now := time.Now().Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
		Account: aiCodeWithKeyIdentity(apiKeys), Enabled: true, Status: upstreamStatusOK,
		UsageSyncEnabled: true, UsageStatus: upstreamStatusPending, UsageBackfillCursor: today - 86400,
	}
	// Domain is only the local primary key in this focused worker test; URL/host
	// binding is already enforced and covered by the save-handler tests.
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, aiCodeWithCredential{APIKeys: apiKeys}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	synced, err := m.syncStoredUpstreamUsage(ctx, row.Domain)
	if err != nil {
		t.Fatal(err)
	}
	if synced.UsageStatus != upstreamStatusOK || !synced.UsageBackfillDone || synced.UsageDataUntil < today {
		t.Fatalf("stored AICodeWith sync state=%+v", synced)
	}
	var buckets []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", row.Domain).Order("hour_ts ASC").Find(&buckets).Error; err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 2 || buckets[0].HourTs != today-86400 || buckets[0].BucketSeconds != 86400 || buckets[0].Requests != 3 || buckets[0].CostUSD != 3 ||
		buckets[1].HourTs != today || buckets[1].BucketSeconds <= 0 || buckets[1].BucketSeconds > 86400 || buckets[1].Requests != 3 || buckets[1].CostUSD != 3 {
		t.Fatalf("tail/history buckets=%+v", buckets)
	}
}

func TestAICodeWithAllKeysReconnectIsolatesAccountAndDoesNotStarvePeer(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		http.Error(w, `{"error":{"type":"UNAUTHORIZED","message":"expired"}}`, http.StatusUnauthorized)
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamUsageBackfillDays = 1
	now := time.Now().Unix()
	keys := []string{"sk-acw-expired-a", "sk-acw-expired-b", "sk-acw-expired-c", "sk-acw-expired-d"}
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
		Account: aiCodeWithKeyIdentity(keys), Enabled: true, Status: upstreamStatusOK,
		UsageSyncEnabled: true, UsageStatus: upstreamStatusPending,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, aiCodeWithCredential{APIKeys: keys}); err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamUsage(context.Background(), row.Domain)
	if err == nil {
		t.Fatal("all-key authentication failure unexpectedly succeeded")
	}
	if calls.Load() != int64(len(keys)) {
		t.Fatalf("calls=%d, want one bounded attempt per key (%d)", calls.Load(), len(keys))
	}
	if synced.UsageStatus != upstreamStatusReconnect || synced.UsageNextSyncAt != upstreamAccountIsolatedUntil ||
		synced.UsageBackfillNextSyncAt != upstreamAccountIsolatedUntil {
		t.Fatalf("all-key reconnect did not isolate the account: %+v", synced)
	}

	peer := ChannelUpstreamAccount{
		Domain: "peer.example", Provider: upstreamProviderNewAPI, BaseURL: "https://peer.example",
		Account: "52", UserID: 52, Enabled: true, UsageSyncEnabled: true,
		UsageStatus: upstreamStatusPending, UsageNextSyncAt: now - 1,
	}
	if err := m.storeDB.Create(&peer).Error; err != nil {
		t.Fatal(err)
	}
	due, loadErr := m.loadDueUpstreamUsageAccounts(context.Background(), time.Now().Unix(), 1)
	if loadErr != nil {
		t.Fatal(loadErr)
	}
	if len(due) != 1 || due[0].Domain != peer.Domain {
		t.Fatalf("isolated AICodeWith account starved due peer: %+v", due)
	}
}

func TestAICodeWithRoundWaitUsesEarliestRunnableKeyDeadline(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 8, 23, 12, 0, 0, 0, cstLocation).Unix()
	round := AICodeWithUsageRound{Domain: "wait.example", Kind: "tail", RoundID: "round-wait", CredentialSetVersion: "v1", TotalKeys: 4, Status: upstreamStatusPending}
	if err := m.storeDB.Create(&round).Error; err != nil {
		t.Fatal(err)
	}
	states := []AICodeWithKeySyncState{
		{Domain: round.Domain, SlotID: "completed", CredentialSetVersion: round.CredentialSetVersion, TailRoundID: round.RoundID},
		{Domain: round.Domain, SlotID: "isolated", CredentialSetVersion: round.CredentialSetVersion, NextSyncAt: upstreamAccountIsolatedUntil},
		{Domain: round.Domain, SlotID: "later", CredentialSetVersion: round.CredentialSetVersion, NextSyncAt: now + 120},
		{Domain: round.Domain, SlotID: "earlier", CredentialSetVersion: round.CredentialSetVersion, NextSyncAt: now + 60},
	}
	for i := range states {
		if err := m.storeDB.Create(&states[i]).Error; err != nil {
			t.Fatal(err)
		}
	}
	allIsolated, retryAt, err := m.aiCodeWithRoundWaitState(context.Background(), round.Domain, round.CredentialSetVersion, round.Kind, now)
	if err != nil {
		t.Fatal(err)
	}
	if allIsolated || retryAt != now+60 {
		t.Fatalf("wait state all_isolated=%v retry_at=%d, want false/%d", allIsolated, retryAt, now+60)
	}
}

func TestSyncStoredSub2APIUsagePublishesTailHistoryAdapterAndRotatedCredential(t *testing.T) {
	var refreshCalls, trendCalls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch r.URL.Path {
		case "/api/v1/auth/refresh":
			refreshCalls.Add(1)
			_, _ = w.Write([]byte(`{"code":0,"data":{"access_token":"access-rotated","refresh_token":"refresh-rotated","expires_in":3600}}`))
		case "/api/v1/usage/dashboard/trend":
			trendCalls.Add(1)
			if r.Header.Get("Authorization") != "Bearer access-rotated" {
				http.Error(w, `{"message":"bad bearer"}`, http.StatusUnauthorized)
				return
			}
			day := r.URL.Query().Get("start_date")
			if day == "" || day != r.URL.Query().Get("end_date") || r.URL.Query().Get("timezone") != "Asia/Shanghai" || r.URL.Query().Get("granularity") != "hour" {
				http.Error(w, `{"message":"bad range"}`, http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{
				"start_date": day, "end_date": day, "granularity": "hour",
				"trend": []any{map[string]any{"date": day + " 00:00", "requests": 4, "total_tokens": 80, "actual_cost": "1.5"}},
			}})
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageBackfillDays = 1
	now := time.Now().Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL,
		Account: "finance@example.com", Enabled: true, Status: upstreamStatusOK,
		UsageSyncEnabled: true, UsageStatus: upstreamStatusPending, UsageBackfillCursor: today - 86400,
	}
	oldCredential := sub2APICredential{AccessToken: "access-expired", RefreshToken: "refresh-old", ExpiresAt: now - 60}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, oldCredential); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	synced, err := m.syncStoredUpstreamUsage(ctx, row.Domain)
	if err != nil {
		t.Fatal(err)
	}
	if synced.UsageStatus != upstreamStatusOK || !synced.UsageBackfillDone || synced.UsageAdapter != upstreamUsageAdapterSub2Trend || synced.UsageDataUntil < today {
		t.Fatalf("stored Sub2API sync state=%+v", synced)
	}
	if refreshCalls.Load() != 1 || trendCalls.Load() != 2 {
		t.Fatalf("refresh calls=%d trend calls=%d, want 1/2", refreshCalls.Load(), trendCalls.Load())
	}
	var stored ChannelUpstreamAccount
	if err := m.storeDB.First(&stored, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	var rotated sub2APICredential
	if err := m.openUpstreamCredential(stored, &rotated); err != nil {
		t.Fatal(err)
	}
	if rotated.AccessToken != "access-rotated" || rotated.RefreshToken != "refresh-rotated" || rotated.ExpiresAt <= now {
		t.Fatalf("rotating credential was not durably persisted: %+v", rotated)
	}
	var buckets []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", row.Domain).Order("hour_ts ASC").Find(&buckets).Error; err != nil {
		t.Fatal(err)
	}
	if len(buckets) < 25 || buckets[0].HourTs != today-86400 || buckets[0].BucketSeconds != 3600 || buckets[0].Requests != 4 || buckets[0].CostUSD != 1.5 {
		t.Fatalf("Sub2API history/tail buckets=%+v", buckets)
	}
	var historyBuckets int
	var tailRequests int64
	for _, bucket := range buckets {
		if bucket.Provider != upstreamProviderSub2API || bucket.BucketSeconds <= 0 || bucket.BucketSeconds > 3600 {
			t.Fatalf("invalid persisted Sub2API bucket=%+v", bucket)
		}
		if bucket.HourTs >= today-86400 && bucket.HourTs < today {
			historyBuckets++
		}
		if bucket.HourTs >= today {
			tailRequests += bucket.Requests
		}
	}
	if historyBuckets != 24 || tailRequests != 4 {
		t.Fatalf("history buckets=%d tail requests=%d", historyBuckets, tailRequests)
	}
}

func TestAICodeWithSixtyFourKeysAdvanceInDurableBoundedTurns(t *testing.T) {
	const total = maxAICodeWithAPIKeys
	now := time.Date(2026, 8, 20, 8, 0, 0, 0, cstLocation).Unix()
	round := AICodeWithUsageRound{RoundID: "round-64", Kind: "tail", TotalKeys: total}
	states := make([]AICodeWithKeySyncState, total)
	for i := range states {
		states[i] = AICodeWithKeySyncState{SlotID: fmt.Sprintf("acw_%02d", i), Ordinal: i + 1}
	}
	turns := 0
	for {
		selected := selectAICodeWithKeyStatesForTurn(states, round, "tail", now, aiCodeWithKeysPerTurn)
		if len(selected) == 0 {
			break
		}
		if len(selected) > aiCodeWithKeysPerTurn {
			t.Fatalf("turn selected %d keys, budget=%d", len(selected), aiCodeWithKeysPerTurn)
		}
		for _, index := range selected {
			states[index].TailRoundID = round.RoundID // represents the committed per-key cursor
		}
		turns++
	}
	if turns != total/aiCodeWithKeysPerTurn {
		t.Fatalf("turns=%d, want %d", turns, total/aiCodeWithKeysPerTurn)
	}
	// A restart reloads the same persisted rows; completed keys are not selected
	// again and one failed/backed-off key cannot starve later keys.
	states[3].TailRoundID = ""
	states[3].NextSyncAt = now + 60
	states[40].TailRoundID = ""
	selected := selectAICodeWithKeyStatesForTurn(states, round, "tail", now, aiCodeWithKeysPerTurn)
	if len(selected) != 1 || selected[0] != 40 {
		t.Fatalf("restart selection=%v, want [40]", selected)
	}
}

func TestAICodeWithTailPublishesFrozenRoundWatermark(t *testing.T) {
	keys := []string{"sk-acw-freeze-1", "sk-acw-freeze-2", "sk-acw-freeze-3", "sk-acw-freeze-4", "sk-acw-freeze-5"}
	keyIDs := map[string]int64{}
	for i, key := range keys {
		keyIDs[key] = int64(i + 1)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		id := keyIDs[key]
		if id == 0 {
			http.Error(w, `{"message":"bad key"}`, http.StatusUnauthorized)
			return
		}
		start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"api_key_id": id, "period": map[string]any{"start": start, "end": end}, "group_by": "day",
			"summary": map[string]any{"cost": "1.00", "total_tokens": 10, "requests": 1},
			"daily":   []any{map[string]any{"date": start, "cost": "1.00", "total_tokens": 10, "requests": 1}},
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	normalized, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKeys: keys})
	if err != nil {
		t.Fatal(err)
	}
	version, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		t.Fatal(err)
	}
	firstNow := time.Date(2026, 8, 20, 10, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL}
	published, _, _, err := m.processAICodeWithRound(context.Background(), &row, normalized, version, "tail", cstDayStart(firstNow), firstNow, firstNow, aiCodeWithKeysPerTurn)
	if err != nil || published {
		t.Fatalf("first turn published=%v err=%v", published, err)
	}
	secondNow := firstNow + 10*60
	published, _, _, err = m.processAICodeWithRound(context.Background(), &row, normalized, version, "tail", cstDayStart(secondNow), secondNow, secondNow, aiCodeWithKeysPerTurn)
	if err != nil || !published {
		t.Fatalf("second turn published=%v err=%v", published, err)
	}
	if row.UsageDataUntil != firstNow {
		t.Fatalf("published watermark=%d want frozen window=%d (scheduler now=%d)", row.UsageDataUntil, firstNow, secondNow)
	}
}

func TestAICodeWithTransientFailureResumesWithoutRepeatingSuccessfulKeys(t *testing.T) {
	keys := []string{"sk-acw-resume-1", "sk-acw-resume-2", "sk-acw-resume-3", "sk-acw-resume-4"}
	keyIDs := map[string]int64{}
	calls := map[string]int{}
	var callsMu sync.Mutex
	requestNumber := 0
	firstSuccessfulKey := ""
	for i, key := range keys {
		keyIDs[key] = int64(i + 1)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		callsMu.Lock()
		calls[key]++
		requestNumber++
		currentRequest := requestNumber
		if currentRequest == 1 {
			firstSuccessfulKey = key
		}
		callsMu.Unlock()
		if currentRequest == 2 {
			http.Error(w, `{"message":"temporary upstream failure"}`, http.StatusBadGateway)
			return
		}
		id := keyIDs[key]
		start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"api_key_id": id, "period": map[string]any{"start": start, "end": end}, "group_by": "day",
			"summary": map[string]any{"cost": "1.00", "total_tokens": 10, "requests": 1},
			"daily":   []any{map[string]any{"date": start, "cost": "1.00", "total_tokens": 10, "requests": 1}},
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	normalized, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKeys: keys})
	if err != nil {
		t.Fatal(err)
	}
	version, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL}
	published, _, used, err := m.processAICodeWithRound(context.Background(), &row, normalized, version, "tail", cstDayStart(now), now, now, aiCodeWithKeysPerTurn)
	if err != nil || published || used != 2 {
		t.Fatalf("first turn published=%v used=%d err=%v", published, used, err)
	}
	var publicCount int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", row.Domain).Count(&publicCount).Error; err != nil {
		t.Fatal(err)
	}
	if publicCount != 0 {
		t.Fatalf("partial round leaked %d public usage rows", publicCount)
	}
	published, _, used, err = m.processAICodeWithRound(context.Background(), &row, normalized, version, "tail", cstDayStart(now), now, now+3*60, aiCodeWithKeysPerTurn)
	if err != nil || !published || used != 3 {
		t.Fatalf("recovery turn published=%v used=%d err=%v", published, used, err)
	}
	callsMu.Lock()
	defer callsMu.Unlock()
	if calls[firstSuccessfulKey] != 1 || requestNumber != 5 {
		t.Fatalf("unexpected retry calls=%v", calls)
	}
	retriedKeys := 0
	for _, count := range calls {
		if count == 2 {
			retriedKeys++
		} else if count != 1 {
			t.Fatalf("unexpected retry calls=%v", calls)
		}
	}
	if retriedKeys != 1 {
		t.Fatalf("expected exactly one failed key to be retried: %v", calls)
	}
	var buckets []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", row.Domain).Find(&buckets).Error; err != nil {
		t.Fatal(err)
	}
	var totalCost float64
	for _, bucket := range buckets {
		totalCost += bucket.CostUSD
	}
	if math.Abs(totalCost-4) > 1e-9 {
		t.Fatalf("published total cost=%v, want 4", totalCost)
	}
}

func TestAICodeWithFinalBackfillPublishesPerKeyCompletion(t *testing.T) {
	const key = "sk-acw-backfill-completion"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer "+key {
			http.Error(w, `{"message":"bad key"}`, http.StatusUnauthorized)
			return
		}
		start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"api_key_id": 77, "period": map[string]any{"start": start, "end": end}, "group_by": "day",
			"summary": map[string]any{"cost": "1.00", "total_tokens": 10, "requests": 1},
			"daily":   []any{map[string]any{"date": start, "cost": "1.00", "total_tokens": 10, "requests": 1}},
		}})
	}))
	defer server.Close()

	m := newChannelUpstreamTestMonitor(t)
	normalized, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKey: key})
	if err != nil {
		t.Fatal(err)
	}
	version, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL}

	published, _, _, err := m.processAICodeWithRound(context.Background(), &row, normalized, version, "backfill", today-2*86400, today-86400, now, aiCodeWithKeysPerTurn)
	if err != nil || !published {
		t.Fatalf("non-final backfill published=%v err=%v", published, err)
	}
	var state AICodeWithKeySyncState
	if err := m.storeDB.First(&state, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if state.BackfillDone {
		t.Fatalf("non-final backfill marked key complete: %+v", state)
	}

	published, _, _, err = m.processAICodeWithRound(context.Background(), &row, normalized, version, "backfill", today-86400, today, now+60, aiCodeWithKeysPerTurn)
	if err != nil || !published {
		t.Fatalf("final backfill published=%v err=%v", published, err)
	}
	if err := m.storeDB.First(&state, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if !state.BackfillDone || state.BackfillLastError != "" || state.BackfillNextSyncAt != 0 || state.BackfillConsecutiveFails != 0 {
		t.Fatalf("final backfill did not close per-key state: %+v", state)
	}
}

func TestAICodeWithSQLiteWriterLockDoesNotAdvancePublishedWatermarks(t *testing.T) {
	const key = "sk-acw-sqlite-lock"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
		_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
			"api_key_id": 91, "period": map[string]any{"start": start, "end": end}, "group_by": "day",
			"summary": map[string]any{"cost": "9.00", "total_tokens": 90, "requests": 9},
			"daily":   []any{map[string]any{"date": start, "cost": "9.00", "total_tokens": 90, "requests": 9}},
		}})
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	normalized, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKey: key})
	if err != nil {
		t.Fatal(err)
	}
	version, err := aiCodeWithCredentialSetVersion(normalized)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 20, 10, 0, 0, 0, cstLocation).Unix()
	oldWatermark := now - 3600
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: oldWatermark,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	state := AICodeWithKeySyncState{
		Domain: row.Domain, SlotID: normalized.Slots[0].SlotID, CredentialSetVersion: version,
		Ordinal: 1, Status: upstreamStatusOK, LastSuccessAt: oldWatermark,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	round := AICodeWithUsageRound{
		Domain: row.Domain, Kind: "tail", RoundID: "round-before-lock", CredentialSetVersion: version,
		WindowFrom: cstDayStart(now), WindowTo: now, TotalKeys: 1, Status: upstreamStatusPending, CreatedAt: now, UpdatedAt: now,
	}
	if err := m.storeDB.Create(&round).Error; err != nil {
		t.Fatal(err)
	}
	oldBucket := ChannelUpstreamUsageHour{Domain: row.Domain, HourTs: oldWatermark / 3600 * 3600, BucketSeconds: 3600, Provider: upstreamProviderAICodeWith, Requests: 3, CostUSD: 3}
	if err := m.storeDB.Create(&oldBucket).Error; err != nil {
		t.Fatal(err)
	}

	primarySQL, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	primarySQL.SetMaxOpenConns(1)
	if err := m.storeDB.Exec("PRAGMA busy_timeout=0").Error; err != nil {
		t.Fatal(err)
	}
	lockDB, err := gorm.Open(sqlite.Open(m.cfg.StorePath+"?_pragma=busy_timeout(0)&_pragma=journal_mode(WAL)"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	lockSQL, err := lockDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	defer lockSQL.Close()
	if err := lockDB.Exec("BEGIN EXCLUSIVE").Error; err != nil {
		t.Fatal(err)
	}
	locked := true
	defer func() {
		if locked {
			_ = lockDB.Exec("ROLLBACK").Error
		}
	}()

	published, _, _, syncErr := m.processAICodeWithRound(context.Background(), &row, normalized, version, "tail", round.WindowFrom, round.WindowTo, now, 1)
	if syncErr == nil || published || !isUpstreamUsageLocalStoreBusy(syncErr) {
		t.Fatalf("locked run published=%v err=%v", published, syncErr)
	}
	if err := lockDB.Exec("ROLLBACK").Error; err != nil {
		t.Fatal(err)
	}
	locked = false

	var storedRow ChannelUpstreamAccount
	if err := m.storeDB.First(&storedRow, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if storedRow.UsageDataUntil != oldWatermark || storedRow.UsageLastSuccessAt != 0 {
		t.Fatalf("account watermark advanced through failed transaction: %+v", storedRow)
	}
	var storedState AICodeWithKeySyncState
	if err := m.storeDB.First(&storedState, "domain = ? AND slot_id = ?", row.Domain, state.SlotID).Error; err != nil {
		t.Fatal(err)
	}
	if storedState.TailRoundID != "" || storedState.SourceKeyID != 0 || storedState.LastSuccessAt != oldWatermark {
		t.Fatalf("key watermark advanced through failed transaction: %+v", storedState)
	}
	var stageCount int64
	if err := m.storeDB.Model(&AICodeWithUsageStage{}).Where("domain = ?", row.Domain).Count(&stageCount).Error; err != nil {
		t.Fatal(err)
	}
	if stageCount != 0 {
		t.Fatalf("partial stage rows leaked after lock failure: %d", stageCount)
	}
	var buckets []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", row.Domain).Find(&buckets).Error; err != nil {
		t.Fatal(err)
	}
	if len(buckets) != 1 || buckets[0].HourTs != oldBucket.HourTs || buckets[0].Requests != oldBucket.Requests || buckets[0].CostUSD != oldBucket.CostUSD {
		t.Fatalf("published usage changed through failed transaction: %+v", buckets)
	}
}

func TestAICodeWithFourEightAndSixtyFourKeysPublishTailAndHistoryAcrossDurableTurns(t *testing.T) {
	for _, total := range []int{4, 8, maxAICodeWithAPIKeys} {
		t.Run(fmt.Sprintf("keys_%d", total), func(t *testing.T) {
			keys := make([]string, total)
			keyID := make(map[string]int64, total)
			calls := make(map[string]int, total)
			var callsMu sync.Mutex
			for i := range keys {
				keys[i] = fmt.Sprintf("sk-acw-scale-%02d", i)
				keyID[keys[i]] = int64(i + 1)
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
				id := keyID[key]
				if id == 0 {
					http.Error(w, `{"error":{"type":"UNAUTHORIZED"}}`, http.StatusUnauthorized)
					return
				}
				callsMu.Lock()
				calls[key]++
				callsMu.Unlock()
				start, end := r.URL.Query().Get("start"), r.URL.Query().Get("end")
				_ = json.NewEncoder(w).Encode(map[string]any{"data": map[string]any{
					"api_key_id": id, "period": map[string]any{"start": start, "end": end}, "group_by": "day",
					"summary": map[string]any{"cost": "1.00", "total_tokens": 10, "requests": 1},
					"daily":   []any{map[string]any{"date": start, "cost": "1.00", "total_tokens": 10, "requests": 1}},
				}})
			}))
			defer server.Close()
			m := newChannelUpstreamTestMonitor(t)
			normalized, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKeys: keys})
			if err != nil {
				t.Fatal(err)
			}
			version, err := aiCodeWithCredentialSetVersion(normalized)
			if err != nil {
				t.Fatal(err)
			}
			row := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL}
			now := time.Date(2026, 8, 20, 8, 0, 0, 0, cstLocation).Unix()
			windows := []struct {
				kind     string
				from, to int64
			}{{"tail", cstDayStart(now), now}, {"backfill", cstDayStart(now) - 7*86400, cstDayStart(now)}}
			for _, window := range windows {
				published := false
				for turn := 0; turn < total/aiCodeWithKeysPerTurn; turn++ {
					worker := m
					if turn >= total/(2*aiCodeWithKeysPerTurn) {
						// A new worker object has no in-process cursor. It must resume
						// solely from the SQLite key/round/stage records.
						worker = &Monitor{storeDB: m.storeDB, upstreamClient: m.upstreamClient, upstreamAICodeWithInterval: time.Millisecond}
					}
					var done int64
					var used int
					published, done, used, err = worker.processAICodeWithRound(context.Background(), &row, normalized, version, window.kind, window.from, window.to, now, aiCodeWithKeysPerTurn)
					if err != nil {
						t.Fatal(err)
					}
					if used != aiCodeWithKeysPerTurn {
						t.Fatalf("%s turn=%d used=%d", window.kind, turn, used)
					}
					if turn < total/aiCodeWithKeysPerTurn-1 && published {
						t.Fatalf("%s published a partial key set at %d/%d", window.kind, done, total)
					}
				}
				if !published {
					t.Fatalf("%s did not publish after all %d keys", window.kind, total)
				}
			}
			callsMu.Lock()
			defer callsMu.Unlock()
			for _, key := range keys {
				if calls[key] != 2 {
					t.Fatalf("key %s calls=%d want=2 (one tail + one history)", key, calls[key])
				}
			}
			var rounds, stages int64
			m.storeDB.Model(&AICodeWithUsageRound{}).Where("domain = ?", row.Domain).Count(&rounds)
			m.storeDB.Model(&AICodeWithUsageStage{}).Where("domain = ?", row.Domain).Count(&stages)
			if rounds != 0 || stages != 0 {
				t.Fatalf("published rounds left temporary state rounds=%d stages=%d", rounds, stages)
			}
		})
	}
}

func TestAICodeWithKeySlotsAppendAndRemoveWithoutReenteringSecrets(t *testing.T) {
	first, err := normalizeAICodeWithCredential(aiCodeWithCredential{APIKeys: []string{"sk-acw-existing-a", "sk-acw-existing-b"}})
	if err != nil {
		t.Fatal(err)
	}
	if len(first.Slots) != 2 || first.Slots[0].SlotID == first.Slots[1].SlotID {
		t.Fatalf("invalid initial slots: %+v", first.Slots)
	}
	var keptID, removedID string
	for _, slot := range first.Slots {
		switch slot.Secret {
		case "sk-acw-existing-a":
			removedID = slot.SlotID
		case "sk-acw-existing-b":
			keptID = slot.SlotID
		}
	}
	if keptID == "" || removedID == "" {
		t.Fatalf("initial secret-to-slot mapping missing: %+v", first.Slots)
	}
	changed, err := applyAICodeWithKeyChanges(first, []string{"sk-acw-new-c"}, []string{removedID})
	if err != nil {
		t.Fatal(err)
	}
	if len(changed.Slots) != 2 {
		t.Fatalf("changed slots=%+v", changed.Slots)
	}
	foundKept, foundNew := false, false
	for _, slot := range changed.Slots {
		foundKept = foundKept || slot.SlotID == keptID && slot.Secret == "sk-acw-existing-b"
		foundNew = foundNew || slot.Secret == "sk-acw-new-c"
	}
	if !foundKept || !foundNew {
		t.Fatalf("append/remove did not preserve opaque slot: %+v", changed.Slots)
	}
	if _, err := applyAICodeWithKeyChanges(changed, nil, []string{"acw_missing"}); err == nil {
		t.Fatal("unknown opaque slot deletion must fail closed")
	}
}

func TestAICodeWithKeySlotRenamePreservesIdentityAndSyncVersion(t *testing.T) {
	initial, err := normalizeAICodeWithCredential(aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{
		{SlotID: "acw_primary", Name: "旧名称", Secret: "sk-acw-existing-a"},
		{SlotID: "acw_backup", Secret: "sk-acw-existing-b"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	beforeVersion, err := aiCodeWithCredentialSetVersion(initial)
	if err != nil {
		t.Fatal(err)
	}
	changed, err := applyAICodeWithSlotChanges(initial, nil, []aicodeWithKeyRenameInput{{SlotID: "acw_primary", Name: "  Claude 主线路  "}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	afterVersion, err := aiCodeWithCredentialSetVersion(changed)
	if err != nil {
		t.Fatal(err)
	}
	if beforeVersion != afterVersion {
		t.Fatalf("rename changed credential-set version: before=%s after=%s", beforeVersion, afterVersion)
	}
	if len(changed.Slots) != 2 {
		t.Fatalf("renamed slots=%+v", changed.Slots)
	}
	byID := make(map[string]aiCodeWithKeyCredential, len(changed.Slots))
	for _, slot := range changed.Slots {
		byID[slot.SlotID] = slot
	}
	if byID["acw_primary"].Name != "Claude 主线路" || byID["acw_primary"].Secret != "sk-acw-existing-a" || byID["acw_backup"].Secret != "sk-acw-existing-b" {
		t.Fatalf("rename did not preserve slots and secrets: %+v", changed.Slots)
	}
	if _, err := applyAICodeWithSlotChanges(changed, nil, []aicodeWithKeyRenameInput{{SlotID: "acw_missing", Name: "未知"}}, nil); err == nil {
		t.Fatal("unknown slot rename must fail closed")
	}
	if _, err := normalizeAICodeWithKeyName("sk-acw-do-not-expose"); err == nil {
		t.Fatal("a key-shaped label must be rejected")
	}
}

func TestAICodeWithPerKeyFailureRedactsReflectedSecret(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	const secret = "sk-acw-reflected-secret"
	now := time.Now().Unix()
	state := AICodeWithKeySyncState{Domain: "aicodewith.com", SlotID: "acw_slot", CredentialSetVersion: "v1"}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	round := AICodeWithUsageRound{Domain: state.Domain, Kind: "tail", RoundID: "round-redact", CredentialSetVersion: "v1"}
	if err := m.recordAICodeWithKeyFailure(context.Background(), round, &state, fmt.Errorf("upstream rejected %s", secret), secret, now); err != nil {
		t.Fatal(err)
	}
	var stored AICodeWithKeySyncState
	if err := m.storeDB.First(&stored, "domain = ? AND slot_id = ?", state.Domain, state.SlotID).Error; err != nil {
		t.Fatal(err)
	}
	if strings.Contains(stored.LastError, secret) || stored.LastError == "" {
		t.Fatalf("stored per-key error leaked secret: %q", stored.LastError)
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

func TestNewAPITailPersistsCompleteHoursAndResumesAfterBudgetYield(t *testing.T) {
	from := time.Date(2026, 8, 12, 0, 0, 0, 0, cstLocation).Unix()
	to := from + 7*3600
	rows := usageFixtureRows(7700, from, to-from) // 1,100 rows and 12 requests per hour.
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: from,
	}
	credential := newAPICredential{AccessToken: "usage-token"}

	firstPacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	first, yielded, err := m.syncNewAPITailIncremental(context.Background(), row, credential, from, to, to, firstPacer)
	if err != nil || !yielded {
		t.Fatalf("first tail turn yielded=%v err=%v", yielded, err)
	}
	if first.DataUntil != from+5*3600 || firstPacer.calls != upstreamUsageMaxRequestsPerRun {
		t.Fatalf("first tail watermark=%d calls=%d", first.DataUntil, firstPacer.calls)
	}
	var published []ChannelUpstreamUsageHour
	if err := m.storeDB.Order("hour_ts ASC").Find(&published, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if len(published) != 5 {
		t.Fatalf("published complete hours=%d, want 5", len(published))
	}
	for _, hour := range published {
		if hour.Requests != 1100 || hour.BucketSeconds != 3600 {
			t.Fatalf("partial or corrupt hour published: %+v", hour)
		}
	}

	// A scheduler retry starts with the unpublished forward range, then uses
	// any remaining budget to refresh the configured three-hour overlap.
	row.UsageDataUntil = first.DataUntil
	secondPacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	second, yielded, err := m.syncNewAPITailIncremental(context.Background(), row, credential, first.DataUntil-3*3600, to, to+15, secondPacer)
	if err != nil || yielded || second.DataUntil != to {
		t.Fatalf("second tail turn watermark=%d yielded=%v err=%v", second.DataUntil, yielded, err)
	}
	if err := m.storeDB.Order("hour_ts ASC").Find(&published, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	var requests int64
	for _, hour := range published {
		requests += hour.Requests
	}
	if len(published) != 7 || requests != int64(len(rows)) {
		t.Fatalf("final published hours=%d requests=%d, want 7/%d", len(published), requests, len(rows))
	}
}

func TestNewAPIHistoryBackfillPersistsPageCheckpointAcrossBudgetYield(t *testing.T) {
	from := time.Date(2026, 8, 12, 8, 0, 0, 0, cstLocation).Unix()
	rows := usageFixtureRows(6500, from, 3600)
	server, totalQueries := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
	}
	credential := newAPICredential{AccessToken: "usage-token"}

	firstPacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	_, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, firstPacer)
	if err != nil || complete || progress == "" {
		t.Fatalf("first bounded turn complete=%v progress=%q err=%v", complete, progress, err)
	}
	if firstPacer.calls != upstreamUsageMaxRequestsPerRun {
		t.Fatalf("first turn calls=%d, want safety budget %d", firstPacer.calls, upstreamUsageMaxRequestsPerRun)
	}
	var checkpoint NewAPIUsageBackfillCheckpoint
	if err := m.storeDB.First(&checkpoint, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextPage != 61 || checkpoint.SourceRows != 6000 || checkpoint.Total != int64(len(rows)) {
		t.Fatalf("durable page checkpoint=%+v", checkpoint)
	}
	var published int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", row.Domain).Count(&published).Error; err != nil || published != 0 {
		t.Fatalf("partial hour became public: rows=%d err=%v", published, err)
	}

	secondPacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	result, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, secondPacer)
	if err != nil || !complete || progress != "" {
		t.Fatalf("resumed turn complete=%v progress=%q err=%v", complete, progress, err)
	}
	if secondPacer.calls != 6 { // one restart probe plus pages 61..65
		t.Fatalf("resume calls=%d, want 6", secondPacer.calls)
	}
	if len(result.Hours) != 1 || result.Hours[0].Requests != int64(len(rows)) || result.Hours[0].CostUSD != float64(len(rows)) {
		t.Fatalf("completed aggregate=%+v", result.Hours)
	}
	if err := m.persistNewAPIUsageBackfillWindow(context.Background(), row.Domain, from, from+3600, result.Hours, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillCheckpoint{}).Where("domain = ?", row.Domain).Count(&published).Error; err != nil || published != 0 {
		t.Fatalf("completed checkpoint was not cleared: rows=%d err=%v", published, err)
	}
	var hour ChannelUpstreamUsageHour
	if err := m.storeDB.First(&hour, "domain = ? AND hour_ts = ?", row.Domain, from).Error; err != nil {
		t.Fatal(err)
	}
	if hour.Requests != int64(len(rows)) || totalQueries.Load() != 66 {
		t.Fatalf("published hour=%+v total queries=%d", hour, totalQueries.Load())
	}
}

func TestNewAPIHistoryBackfillTimeoutFallsBackToDurableSegments(t *testing.T) {
	from := time.Date(2026, 8, 12, 8, 0, 0, 0, cstLocation).Unix()
	rows := usageFixtureRows(2400, from, 3600)
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31, BalanceUnit: 500000,
		UsageBackfillConsecutiveFails: 1,
		UsageBackfillLastError:        `连接上游失败: Client.Timeout exceeded while awaiting headers`,
	}
	credential := newAPICredential{AccessToken: "usage-token"}

	firstPacer := newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0)
	_, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, firstPacer)
	if err != nil || complete || !strings.Contains(progress, "慢查询降档") {
		t.Fatalf("first segmented turn complete=%v progress=%q err=%v", complete, progress, err)
	}
	if firstPacer.calls != upstreamUsageHistoryMaxRequestsPerRun {
		t.Fatalf("first segmented turn calls=%d, want %d", firstPacer.calls, upstreamUsageHistoryMaxRequestsPerRun)
	}
	var segments int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ? AND status = ?", row.Domain, from, newAPIBackfillSegmentComplete).Count(&segments).Error; err != nil {
		t.Fatal(err)
	}
	if segments != 6 {
		t.Fatalf("durable completed segments=%d, want 6", segments)
	}
	var publicRows int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", row.Domain).Count(&publicRows).Error; err != nil || publicRows != 0 {
		t.Fatalf("partial segmented hour became public: rows=%d err=%v", publicRows, err)
	}

	secondPacer := newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0)
	result, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, secondPacer)
	if err != nil || !complete || progress != "" {
		t.Fatalf("resumed segmented turn complete=%v progress=%q err=%v", complete, progress, err)
	}
	if len(result.Hours) != 1 || result.Hours[0].Requests != int64(len(rows)) || result.Hours[0].CostUSD != float64(len(rows)) {
		t.Fatalf("segmented aggregate=%+v", result.Hours)
	}
	if err := m.persistNewAPIUsageBackfillWindow(context.Background(), row.Domain, from, from+3600, result.Hours, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ?", row.Domain).Count(&segments).Error; err != nil || segments != 0 {
		t.Fatalf("completed segment state was not cleared: rows=%d err=%v", segments, err)
	}
	var checkpoints int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillCheckpoint{}).Where("domain = ?", row.Domain).Count(&checkpoints).Error; err != nil || checkpoints != 0 {
		t.Fatalf("completed page checkpoint was not cleared: rows=%d err=%v", checkpoints, err)
	}
	var hour ChannelUpstreamUsageHour
	if err := m.storeDB.First(&hour, "domain = ? AND hour_ts = ?", row.Domain, from).Error; err != nil {
		t.Fatal(err)
	}
	if hour.Requests != int64(len(rows)) {
		t.Fatalf("published segmented hour=%+v", hour)
	}
}

func TestNewAPIHistoryBackfillPersistsSegmentModeAcrossTimeoutAnd429(t *testing.T) {
	from := time.Date(2026, 8, 12, 8, 0, 0, 0, cstLocation).Unix()
	var mode atomic.Int32
	var fullHourCalls atomic.Int64
	var childCalls atomic.Int64
	transport := upstreamRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		start, _ := strconv.ParseInt(req.URL.Query().Get("start_timestamp"), 10, 64)
		end, _ := strconv.ParseInt(req.URL.Query().Get("end_timestamp"), 10, 64)
		if end-start+1 == 3600 {
			fullHourCalls.Add(1)
			return nil, context.DeadlineExceeded
		}
		childCalls.Add(1)
		if mode.Load() == 1 {
			header := make(http.Header)
			header.Set("Retry-After", "120")
			return &http.Response{
				StatusCode: http.StatusTooManyRequests, Status: "429 Too Many Requests",
				Header: header, Body: io.NopCloser(strings.NewReader(`{"message":"rate limited"}`)), Request: req,
			}, nil
		}
		if mode.Load() == 2 {
			return &http.Response{
				StatusCode: http.StatusServiceUnavailable, Status: "503 Service Unavailable",
				Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"message":"temporarily unavailable"}`)), Request: req,
			}, nil
		}
		return &http.Response{
			StatusCode: http.StatusOK, Status: "200 OK", Header: make(http.Header),
			Body: io.NopCloser(strings.NewReader(`{"success":true,"data":{"total":0,"items":[]}}`)), Request: req,
		}, nil
	})
	m := newChannelUpstreamTestMonitor(t)
	m.upstreamClient = &http.Client{Transport: transport}
	row := ChannelUpstreamAccount{
		Domain: "slow.example", Provider: upstreamProviderNewAPI, BaseURL: "https://slow.example",
		UserID: 31, BalanceUnit: 500000,
	}
	credential := newAPICredential{AccessToken: "usage-token"}

	_, progress, complete, err := m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(20, 0))
	if err != nil || complete || !strings.Contains(progress, "已持久降档") || fullHourCalls.Load() != 1 {
		t.Fatalf("timeout transition complete=%v progress=%q full_calls=%d err=%v", complete, progress, fullHourCalls.Load(), err)
	}
	var planned int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ?", row.Domain, from).Count(&planned).Error; err != nil || planned != 12 {
		t.Fatalf("persisted segment plan=%d err=%v", planned, err)
	}

	mode.Store(1)
	_, _, _, err = m.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(20, 0))
	var statusErr *upstreamHTTPError
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusTooManyRequests {
		t.Fatalf("segmented 429 err=%v", err)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ?", row.Domain, from).Count(&planned).Error; err != nil || planned != 12 {
		t.Fatalf("429 lost segment plan: rows=%d err=%v", planned, err)
	}

	mode.Store(2)
	if err := m.storeDB.Where("1 = 1").Delete(&UpstreamHostCircuit{}).Error; err != nil {
		t.Fatal(err)
	}
	m2 := &Monitor{storeDB: m.storeDB, cfg: m.cfg, upstreamClient: &http.Client{Transport: transport}}
	row.UsageBackfillConsecutiveFails = 1
	row.UsageBackfillLastError = "HTTP 429"
	_, _, _, err = m2.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(4, 0))
	if !errors.As(err, &statusErr) || statusErr.Status != http.StatusServiceUnavailable {
		t.Fatalf("segmented 503 err=%v", err)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ?", row.Domain, from).Count(&planned).Error; err != nil || planned != 12 {
		t.Fatalf("503 lost segment plan: rows=%d err=%v", planned, err)
	}
	if fullHourCalls.Load() != 1 {
		t.Fatalf("restart after 429 retried parent query: full=%d", fullHourCalls.Load())
	}

	mode.Store(3)
	if err := m.storeDB.Where("1 = 1").Delete(&UpstreamHostCircuit{}).Error; err != nil {
		t.Fatal(err)
	}
	m3 := &Monitor{storeDB: m.storeDB, cfg: m.cfg, upstreamClient: &http.Client{Transport: transport}}
	row.UsageBackfillConsecutiveFails = 2
	row.UsageBackfillLastError = "HTTP 503"
	_, progress, complete, err = m3.syncNewAPIUsageBackfillWindow(context.Background(), row, credential, from, from+3600, newUpstreamUsageRequestPacer(4, 0))
	if err != nil || complete || progress == "" {
		t.Fatalf("segmented resume complete=%v progress=%q err=%v", complete, progress, err)
	}
	if fullHourCalls.Load() != 1 || childCalls.Load() == 0 {
		t.Fatalf("mode fell back to parent query: full=%d child=%d", fullHourCalls.Load(), childCalls.Load())
	}
}

func TestNewAPIHistoryBackfillPublishRollsBackWhenSegmentCleanupFails(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Date(2026, 8, 12, 8, 0, 0, 0, cstLocation).Unix()
	domain := "publish-rollback.example"
	checkpoint := NewAPIUsageBackfillCheckpoint{
		Domain: domain, WindowFrom: from, WindowTo: from + 300,
		NextPage: 2, Total: 1, SourceRows: 1, UpdatedAt: time.Now().Unix(),
	}
	if err := m.storeDB.Create(&checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	segment := NewAPIUsageBackfillSegment{
		Domain: domain, HourFrom: from, SegmentFrom: from, SegmentTo: from + 300,
		Status: newAPIBackfillSegmentComplete, Total: 1, SourceRows: 1, Requests: 1, UpdatedAt: time.Now().Unix(),
	}
	if err := m.storeDB.Create(&segment).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_segment_cleanup BEFORE DELETE ON new_api_usage_backfill_segments BEGIN SELECT RAISE(ABORT, 'injected segment cleanup failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	hours := []ChannelUpstreamUsageHour{{Domain: domain, HourTs: from, BucketSeconds: 3600, Requests: 1, Provider: upstreamProviderNewAPI}}
	if err := m.persistNewAPIUsageBackfillWindow(context.Background(), domain, from, from+3600, hours, time.Now().Unix()); err == nil {
		t.Fatal("segment cleanup failure did not abort publish transaction")
	}
	var publicRows, checkpointRows, segmentRows int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", domain).Count(&publicRows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillCheckpoint{}).Where("domain = ?", domain).Count(&checkpointRows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ?", domain).Count(&segmentRows).Error; err != nil {
		t.Fatal(err)
	}
	if publicRows != 0 || checkpointRows != 1 || segmentRows != 1 {
		t.Fatalf("publish transaction was partially committed: public=%d checkpoints=%d segments=%d", publicRows, checkpointRows, segmentRows)
	}
}

func TestNewAPIHistoryBackfillSegmentSplitStateMachine(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Date(2026, 8, 12, 8, 0, 0, 0, cstLocation).Unix()
	account := ChannelUpstreamAccount{Domain: "split.example", UsageBackfillCursor: from}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	parent := NewAPIUsageBackfillSegment{
		Domain: account.Domain, HourFrom: from, SegmentFrom: from, SegmentTo: from + 300,
		Status: newAPIBackfillSegmentPending, UpdatedAt: time.Now().Unix(),
	}
	if err := m.storeDB.Create(&parent).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.splitNewAPIUsageBackfillSegment(context.Background(), parent); err != nil {
		t.Fatal(err)
	}
	var segments []NewAPIUsageBackfillSegment
	if err := m.storeDB.Where("domain = ? AND hour_from = ?", account.Domain, from).Order("segment_from ASC").Find(&segments).Error; err != nil {
		t.Fatal(err)
	}
	if len(segments) != 5 {
		t.Fatalf("300s split rows=%d, want 5", len(segments))
	}
	for i, segment := range segments {
		if segment.SegmentFrom != from+int64(i*60) || segment.SegmentTo != from+int64((i+1)*60) || segment.Status != newAPIBackfillSegmentPending {
			t.Fatalf("invalid 60s child[%d]=%+v", i, segment)
		}
	}
	if err := m.splitNewAPIUsageBackfillSegment(context.Background(), segments[0]); err != nil {
		t.Fatal(err)
	}
	segments = nil
	if err := m.storeDB.Where("domain = ? AND hour_from = ?", account.Domain, from).Order("segment_from ASC").Find(&segments).Error; err != nil {
		t.Fatal(err)
	}
	if len(segments) != 10 || segments[0].SegmentFrom != from || segments[5].SegmentTo != from+60 || segments[6].SegmentFrom != from+60 {
		t.Fatalf("60s split did not preserve continuous cover: %+v", segments)
	}
	if err := validateNewAPIUsageBackfillSegments(append(segments, NewAPIUsageBackfillSegment{
		Domain: account.Domain, HourFrom: from, SegmentFrom: from + 300, SegmentTo: from + 3600,
		Status: newAPIBackfillSegmentPending,
	}), from, from+3600, false); err != nil {
		t.Fatalf("split coverage invalid: %v", err)
	}
	if err := m.splitNewAPIUsageBackfillSegment(context.Background(), segments[0]); err == nil || !strings.Contains(err.Error(), "最小子窗口") {
		t.Fatalf("10s segment must fail closed, err=%v", err)
	}
	var remaining int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ? AND segment_from = ?", account.Domain, from, from).Count(&remaining).Error; err != nil || remaining != 1 {
		t.Fatalf("minimum segment was removed: rows=%d err=%v", remaining, err)
	}
	var publicRows int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", account.Domain).Count(&publicRows).Error; err != nil || publicRows != 0 {
		t.Fatalf("split state leaked to public facts: rows=%d err=%v", publicRows, err)
	}
	var reloaded ChannelUpstreamAccount
	if err := m.storeDB.First(&reloaded, "domain = ?", account.Domain).Error; err != nil || reloaded.UsageBackfillCursor != from {
		t.Fatalf("split moved account cursor: row=%+v err=%v", reloaded, err)
	}
}

func TestNewAPIHistoryOperationDeadlineYieldsWithoutSplitting(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Date(2026, 8, 12, 8, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{
		Domain: "budget.example", Provider: upstreamProviderNewAPI,
		BaseURL: "https://budget.example", UserID: 31, BalanceUnit: 500000,
	}
	if err := m.ensureNewAPIUsageBackfillSegmentPlan(context.Background(), row.Domain, from, from+3600); err != nil {
		t.Fatal(err)
	}
	m.upstreamClient = &http.Client{Transport: upstreamRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	deadlineCtx, deadlineCancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer deadlineCancel()
	_, progress, complete, err := m.syncNewAPIUsageBackfillWindowSegmented(deadlineCtx, row, newAPICredential{AccessToken: "usage-token"}, from, from+3600, newUpstreamUsageRequestPacer(20, 0))
	if err != nil || complete || !strings.Contains(progress, "时间预算已用完") {
		t.Fatalf("operation deadline complete=%v progress=%q err=%v", complete, progress, err)
	}
	var segments []NewAPIUsageBackfillSegment
	if err := m.storeDB.Where("domain = ? AND hour_from = ?", row.Domain, from).Order("segment_from ASC").Find(&segments).Error; err != nil {
		t.Fatal(err)
	}
	if len(segments) != 12 {
		t.Fatalf("operation stop changed topology: rows=%d", len(segments))
	}
	for _, segment := range segments {
		if segment.SegmentTo-segment.SegmentFrom != 300 {
			t.Fatalf("operation stop split a healthy child: %+v", segment)
		}
	}
}

func TestNewAPIHistoryWorkerPersistsBudgetYieldWithExpiredReadContext(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Now().Unix()
	from := cstDayStart(now) - 3600
	row := ChannelUpstreamAccount{
		Domain: "budget-worker.example", Provider: upstreamProviderNewAPI, BaseURL: "https://budget-worker.example",
		Account: "31", UserID: 31, Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
		UsageLastSuccessAt: now - 60, UsageDataUntil: now - 60, UsageNextSyncAt: now + 3600,
		UsageBackfillCursor: from, UsageBackfillNextSyncAt: 0,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	m.upstreamClient = &http.Client{Transport: upstreamRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		<-req.Context().Done()
		return nil, req.Context().Err()
	})}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	if _, err := m.syncStoredUpstreamUsageLaneBackground(ctx, row.Domain, upstreamUsageLaneHistory); err != nil {
		t.Fatalf("budget yield was returned as failure: %v", err)
	}
	var stored ChannelUpstreamAccount
	if err := m.storeDB.First(&stored, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if stored.UsageBackfillCursor != from || stored.UsageBackfillNextSyncAt <= now || !strings.Contains(stored.UsageBackfillProgress, "时间预算已用完") {
		t.Fatalf("budget yield was not durably scheduled: %+v", stored)
	}
	var segments int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ?", row.Domain).Count(&segments).Error; err != nil || segments != 0 {
		t.Fatalf("operation deadline incorrectly changed topology: rows=%d err=%v", segments, err)
	}
}

func TestBackfillBudgetYieldDoesNotBecomeFailureBackoff(t *testing.T) {
	now := time.Date(2026, 8, 25, 8, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{
		Domain: "dense.example", UsageBackfillConsecutiveFails: 3,
		UsageBackfillLastError: "old failure", UsageBackfillNextSyncAt: now + 3600,
	}
	applyUpstreamUsageBackfillYield(&row, "已安全保存 6000/6500 条", now)
	if row.UsageBackfillConsecutiveFails != 0 || row.UsageBackfillLastError != "" || row.UsageBackfillProgress == "" || row.UsageBackfillNextSyncAt != now+15 {
		t.Fatalf("healthy budget yield was classified as failure: %+v", row)
	}
}

func TestLegacyNewAPIBudgetFailureBecomesImmediatelyDue(t *testing.T) {
	now := time.Date(2026, 8, 25, 9, 0, 0, 0, cstLocation).Unix()
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: "legacy-budget.example", Provider: upstreamProviderNewAPI,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusError,
		UsageBackfillDone: false, UsageBackfillConsecutiveFails: 4,
		UsageBackfillLastError:  legacyNewAPIBackfillBudgetError(),
		UsageBackfillNextSyncAt: now + 3600, UsageNextSyncAt: now + 3600,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	due, err := m.loadDueUpstreamUsageAccounts(context.Background(), now, 1)
	if err != nil || len(due) != 1 || due[0].Domain != row.Domain {
		t.Fatalf("legacy budget row was not immediately due: rows=%+v err=%v", due, err)
	}
	normalizeLegacyNewAPIBackfillBudgetState(&due[0], now)
	if due[0].UsageBackfillConsecutiveFails != 0 || due[0].UsageBackfillLastError != "" ||
		due[0].UsageBackfillProgress != "等待断点续传" || due[0].UsageBackfillNextSyncAt != now {
		t.Fatalf("legacy budget row was not normalized: %+v", due[0])
	}
}

func TestSyncStoredNewAPIHistoryUsesBudgetAcrossHoursAndCompletes(t *testing.T) {
	now := time.Now().Unix()
	today := cstDayStart(now)
	from := today - 2*3600
	server, totalQueries := newUpstreamUsageFixtureServer(t, []upstreamUsageFixtureRow{
		{ID: 1, CreatedAt: from + 60},
		{ID: 2, CreatedAt: from + 3600 + 60},
	}, nil)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageBackfillDays = 1
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, Account: "31", UserID: 31, Enabled: true,
		UsageSyncEnabled: true, UsageStatus: upstreamStatusOK,
		UsageDataUntil: now, UsageNextSyncAt: now + 3600,
		UsageBackfillCursor: from, UsageBackfillNextSyncAt: now - 1,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	synced, err := m.syncStoredUpstreamUsage(context.Background(), row.Domain)
	if err != nil {
		t.Fatal(err)
	}
	if !synced.UsageBackfillDone || synced.UsageBackfillCursor != today || synced.UsageBackfillConsecutiveFails != 0 || synced.UsageBackfillLastError != "" || synced.UsageBackfillProgress != "" {
		t.Fatalf("multi-hour history did not complete cleanly: %+v", synced)
	}
	if totalQueries.Load() != 2 {
		t.Fatalf("queries=%d, want one page per sparse hour", totalQueries.Load())
	}
	var hours []ChannelUpstreamUsageHour
	if err := m.storeDB.Where("domain = ?", row.Domain).Order("hour_ts").Find(&hours).Error; err != nil {
		t.Fatal(err)
	}
	if len(hours) != 2 || hours[0].Requests != 1 || hours[1].Requests != 1 {
		t.Fatalf("published history hours=%+v", hours)
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
