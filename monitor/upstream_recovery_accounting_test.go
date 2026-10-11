package monitor

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"reflect"
	"strconv"
	"testing"
	"time"
)

// Exercise the real recovery/worker/persistence path, not just a successful
// probe. Every upstream request is intercepted; all facts live in a temporary DB.
func TestUpstreamRecoveryHistoricalAccountingReplay(t *testing.T) {
	m, row, _ := newUpstreamRecoveryFixture(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	now := time.Now().Unix()
	end := cstDayStart(now)
	start := end - 3600
	row.UsageStatus, row.UsageLastError = upstreamStatusOK, ""
	row.UsageNextSyncAt, row.UsageDataUntil = now+3600, now
	row.UsageConsecutiveFails = 0
	row.UsageBackfillCursor, row.UsageBackfillDone = start, false
	if err := m.persistUpstreamAccount(ctx, &row); err != nil {
		t.Fatal(err)
	}
	partial := ChannelUpstreamUsageHour{Domain: row.Domain, Provider: row.Provider, HourTs: start,
		BucketSeconds: 900, Requests: 1, Tokens: 3, Quota: 500000, UnitPerUSD: 500000, CostUSD: 1, Provisional: true}
	if err := m.storeDB.Create(&partial).Error; err != nil {
		t.Fatal(err)
	}
	createChannelRechargeVersion(t, m, row.Domain, 1, start, 2.5, 8)
	accounts := map[string]ChannelUpstreamAccountView{row.Domain: {
		Configured: true, UsageSyncEnabled: true, Provider: row.Provider, UsageGranularity: "hour",
	}}
	scope := stabilityScope{FromTs: start, ToTs: end}
	status, quota := http.StatusOK, int64(2000000)
	setUpstreamRecoveryTransport(m, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/log/self" || r.URL.Host != "fixture.example" {
			t.Fatalf("unexpected upstream request: %s %s", r.Method, r.URL)
		}
		if status != http.StatusOK {
			return recoveryHTTPResponse(status, `{"message":"temporarily unavailable"}`), nil
		}
		q := r.URL.Query()
		from, err := strconv.ParseInt(q.Get("start_timestamp"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		to, err := strconv.ParseInt(q.Get("end_timestamp"), 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		items := []any{}
		if start+60 >= from && start+60 <= to {
			items = append(items, map[string]any{"id": 1, "created_at": start + 60, "type": 2,
				"quota": quota, "prompt_tokens": 20, "completion_tokens": 10})
		}
		body, err := json.Marshal(map[string]any{"success": true, "data": map[string]any{"total": len(items), "items": items}})
		if err != nil {
			t.Fatal(err)
		}
		return recoveryHTTPResponse(http.StatusOK, string(body)), nil
	}))
	result, err := m.recoverUpstreamTask(ctx, row.Domain, "usage_history")
	if err != nil || result.Status != "queued" {
		t.Fatalf("reactivation: %+v %v", result, err)
	}
	loadBucket := func() ChannelUpstreamUsageHour {
		t.Helper()
		var buckets []ChannelUpstreamUsageHour
		if err := m.storeDB.Where("domain=?", row.Domain).Find(&buckets).Error; err != nil || len(buckets) != 1 {
			t.Fatalf("unexpected bucket count: %d %v", len(buckets), err)
		}
		return buckets[0]
	}
	before := loadBucket()
	// A successful manual probe must not claim the historical bill is repaired.
	status = http.StatusServiceUnavailable
	failed, err := m.syncStoredUpstreamUsageLaneBackground(ctx, row.Domain, upstreamUsageLaneHistory)
	if err == nil || failed.UsageBackfillCursor != start || failed.UsageBackfillDone || failed.UsageBackfillConsecutiveFails <= row.UsageBackfillConsecutiveFails {
		t.Fatalf("failed catch-up advanced history or lost failure: %+v %v", failed, err)
	}
	if got := loadBucket(); !reflect.DeepEqual(got, before) {
		t.Fatal("failed historical request overwrote published facts")
	}
	if failed.UsageStatus != upstreamStatusOK || failed.UsageDataUntil != row.UsageDataUntil || failed.UsageNextSyncAt != row.UsageNextSyncAt {
		t.Fatal("history failure damaged the independent live-usage lane")
	}
	status = http.StatusOK
	for _, cost := range []int64{4, 4, 6} {
		quota = cost * 500000
		// First: retry becomes due. Second: simulate a crash after bucket commit
		// but before cursor commit. Third: replay a corrected upstream bill.
		if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", row.Domain).Updates(map[string]any{
			"usage_backfill_cursor": start, "usage_backfill_done": false, "usage_backfill_next_sync_at": 0,
		}).Error; err != nil {
			t.Fatal(err)
		}
		restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, upstreamClient: m.upstreamClient}
		synced, err := restarted.syncStoredUpstreamUsageLaneBackground(ctx, row.Domain, upstreamUsageLaneHistory)
		if err != nil || !synced.UsageBackfillDone || synced.UsageBackfillCursor != end || synced.UsageBackfillConsecutiveFails != 0 {
			t.Fatalf("catch-up did not finish: %+v %v", synced, err)
		}
		bucket := loadBucket()
		if bucket.Provisional || bucket.BucketSeconds != 3600 || bucket.Requests != 1 || bucket.Tokens != 30 || bucket.CostUSD != float64(cost) {
			t.Fatalf("replay duplicated or retained partial facts: %+v", bucket)
		}
		assertRecoveryAccountingAmounts(t, restarted, scope, now, accounts, row.Domain, float64(cost), float64(cost)*2.5/8)
	}
}

func assertRecoveryAccountingAmounts(t *testing.T, m *Monitor, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, domain string, raw, corrected float64) {
	t.Helper()
	channels, err := m.loadChannelUpstreamUsageWindow(context.Background(), scope, now, accounts, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	metrics, money, err := m.loadFinanceBillWindow(context.Background(), scope, now, accounts, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	channel := channels[domain]
	if !reflect.DeepEqual(channel, metrics[domain]) || !channel.Complete || !channel.AdjustedCostAvailable ||
		math.Abs(channel.CostUSD-raw) > 1e-9 || math.Abs(channel.AdjustedCostUSD-corrected) > 1e-9 {
		t.Fatalf("channel/finance bill projections disagree: channel=%+v finance=%+v", channel, metrics[domain])
	}
	if money[domain].Raw != economicsMoney(int64(math.Round(raw*1e6))) || money[domain].RechargeCorrected != economicsMoney(int64(math.Round(corrected*1e6))) {
		t.Fatalf("finance precision or recharge correction mismatch: %+v", money[domain])
	}
}

func TestUpstreamRecoveryBillPublicationRefreshesFinanceCache(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	ctx := context.Background()
	const domain = "hour.example"
	// dailyBillFixture seeds precomputed rows, not a synchronizable account.
	// Supply the unit evidence required when importing a previously absent hour.
	if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain=?", domain).Update("balance_unit", quotaPerUSD).Error; err != nil {
		t.Fatal(err)
	}
	for d := range accounts {
		createChannelRechargeVersion(t, m, d, 1, scope.FromTs, 2.5, 8)
	}
	// The missing first hour must remain incomplete until a bill is published.
	if err := m.storeDB.Where("domain=? AND hour_ts=?", domain, scope.FromTs).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		t.Fatal(err)
	}
	build := func() (financePeriodComponent, bool) {
		t.Helper()
		component, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "recovery-publication", accounts,
			channelFinanceSnapshot{}, financeInternalTestCostEvidence{Complete: true}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return component, hit
	}
	before, hit := build()
	if hit || before.UpstreamCoverage.Complete || before.Statement.UpstreamBilledCost != nil || before.Statement.KnownUpstreamBilledCost.MicroUSD != "32000000" {
		t.Fatal("missing hour was treated as a complete bill")
	}
	if _, hit := build(); !hit {
		t.Fatal("unchanged partial report did not reuse cache")
	}
	for _, tc := range []struct {
		name    string
		cost    float64
		wantHit bool
	}{
		{"backfill", 1, false},
		{"identical replay", 1, true},
		{"corrected bill", 4, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			bucket := ChannelUpstreamUsageHour{Domain: domain, Provider: upstreamProviderNewAPI, HourTs: scope.FromTs,
				BucketSeconds: 3600, UnitPerUSD: quotaPerUSD, Quota: tc.cost * quotaPerUSD, CostUSD: tc.cost}
			// Same fetched_at deliberately: cache correctness must follow content,
			// not rely on the timestamp changing between publications.
			if err := m.persistUpstreamUsageWindow(ctx, domain, scope.FromTs, scope.FromTs+3600, []ChannelUpstreamUsageHour{bucket}, scope.ToTs); err != nil {
				t.Fatal(err)
			}
			after, hit := build()
			wantRaw := economicsMoney(int64((32 + tc.cost) * 1e6))
			wantCorrected := economicsMoney(int64((32 + tc.cost) * 2.5 / 8 * 1e6))
			if hit != tc.wantHit || !after.UpstreamCoverage.Complete || after.Statement.UpstreamBilledCost == nil ||
				*after.Statement.UpstreamBilledCost != wantRaw || after.Statement.RawCorrectedUpstreamCost == nil || *after.Statement.RawCorrectedUpstreamCost != wantCorrected {
				t.Fatalf("publication/cache mismatch: hit=%t want=%t statement=%+v coverage=%+v", hit, tc.wantHit, after.Statement, after.UpstreamCoverage)
			}
			if after.Days[0].Statement.KnownUpstreamBilledCost != economicsMoney(int64((10+tc.cost)*1e6)) || after.Days[1].Statement.KnownUpstreamBilledCost.MicroUSD != "22000000" {
				t.Fatal("publication changed an unrelated day or reused stale daily amounts")
			}
			assertRecoveryAccountingAmounts(t, m, scope, scope.ToTs+86400, accounts, domain, tc.cost+2, (tc.cost+2)*2.5/8)
			if again, hit := build(); !hit || !reflect.DeepEqual(after, again) {
				t.Fatal("published report is not stable on repeated reads")
			}
		})
	}
	// Inject a local SQLite write failure after the replacement DELETE. Both
	// the old bucket and its valid cached report must survive the rollback.
	before, _ = build()
	if err := m.storeDB.Exec(`CREATE TRIGGER recovery_test_reject_bill BEFORE INSERT ON channel_upstream_usage_hours
		WHEN NEW.domain = 'hour.example' AND NEW.cost_usd = 99
		BEGIN SELECT RAISE(ABORT, 'test publication failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	bad := ChannelUpstreamUsageHour{Domain: domain, Provider: upstreamProviderNewAPI, HourTs: scope.FromTs,
		BucketSeconds: 3600, UnitPerUSD: quotaPerUSD, Quota: 99 * quotaPerUSD, CostUSD: 99}
	if err := m.persistUpstreamUsageWindow(ctx, domain, scope.FromTs, scope.FromTs+3600, []ChannelUpstreamUsageHour{bad}, scope.ToTs); err == nil {
		t.Fatal("injected publication failure was ignored")
	}
	if after, hit := build(); !hit || !reflect.DeepEqual(before, after) {
		t.Fatal("failed publication removed old facts or damaged cached report")
	}
}
