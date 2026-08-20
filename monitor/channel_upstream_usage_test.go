package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
						worker = &Monitor{storeDB: m.storeDB, upstreamClient: m.upstreamClient}
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
