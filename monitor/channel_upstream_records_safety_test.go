package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestAICodeWithRecordOpenDayIsProvisionalAndExcludesUnclosedHour(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := recordTestDay()
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		if r.URL.Query().Get("group_by") != "record" {
			t.Error("open-day mutable summary must not masquerade as a control")
			w.WriteHeader(500)
			return
		}
		_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, 17, []map[string]any{recordTestItem(day, 1, 17, "0.1", 10), recordTestItem(day, 2, 18, "0.2", 20)}, false, ""))
	}))
	defer server.Close()
	m.upstreamClient = server.Client()
	row := ChannelUpstreamAccount{Domain: "s", Provider: upstreamProviderAICodeWith, BaseURL: server.URL}
	round := AICodeWithUsageRound{Domain: row.Domain, RoundID: "r", WindowFrom: day, WindowTo: day + 18*3600, RecordMode: true}
	result, ready, err := m.fetchAICodeWithRecordWindow(context.Background(), row, "test", round, "s", day+19*3600, intBudget(4))
	if err != nil || !ready || calls != 1 || len(result.Hours) != 18 || result.DataUntil != day+18*3600 {
		t.Fatalf("ready=%v result=%+v err=%v calls=%d", ready, result, err, calls)
	}
	for _, h := range result.Hours {
		if !h.Provisional {
			t.Fatal(h)
		}
	}
	last := result.Hours[17]
	if last.Requests != 1 || last.Tokens != 10 || last.CostUSD != 0.1 {
		t.Fatal(last)
	}
	if err := m.storeDB.Create(&result.Hours).Error; err != nil {
		t.Fatal(err)
	}
	accounts := map[string]ChannelUpstreamAccountView{"s": {Configured: true, Provider: upstreamProviderAICodeWith, UsageSyncEnabled: true, UsageAdapter: upstreamUsageAdapterAICodeWithRecord}}
	view, err := m.loadChannelUpstreamUsage(context.Background(), stabilityScope{FromTs: day + 17*3600, ToTs: day + 18*3600}, day+19*3600, accounts, channelFinanceSnapshot{})
	if err != nil || view["s"].Granularity != "hour" || !view["s"].Provisional || view["s"].Complete || view["s"].CostUSD != 0.1 {
		t.Fatalf("report=%+v err=%v", view, err)
	}
}

func TestAICodeWithRecordMigrationPreservesPublishedConversionEvidence(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := recordTestDay()
	old := ChannelUpstreamUsageHour{Domain: "s", HourTs: day, BucketSeconds: 86400, UnitPerUSD: 2, CostUSD: 12}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	buckets := map[int64]ChannelUpstreamUsageHour{}
	for i := int64(0); i < 24; i++ {
		ts := day + i*3600
		buckets[ts] = ChannelUpstreamUsageHour{HourTs: ts, Quota: 1, UnitPerUSD: 4, CostUSD: 0.25}
	}
	if err := preserveAICodeWithRecordUnits(m.storeDB, "s", AICodeWithUsageRound{WindowFrom: day, WindowTo: day + 86400}, buckets); err != nil {
		t.Fatal(err)
	}
	for _, bucket := range buckets {
		if bucket.UnitPerUSD != 2 || bucket.CostUSD != 0.5 {
			t.Fatal(bucket)
		}
	}
}

func TestAICodeWithRecordUnreconciledClosedDayExcludedFromBurnEstimate(t *testing.T) {
	m := newStabilityTestMonitor(t)
	day := recordTestDay()
	rows := []ChannelUpstreamUsageHour{
		{Domain: "s", Provider: upstreamProviderAICodeWith, HourTs: day, BucketSeconds: 3600, CostUSD: 1},
		{Domain: "s", Provider: upstreamProviderAICodeWith, HourTs: day + 3600, BucketSeconds: 3600, CostUSD: 99, Provisional: true},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), day+86400, upstreamBalancePolicy{Lookback: 1}, map[string]ChannelUpstreamAccountView{"s": {Provider: upstreamProviderAICodeWith}})
	if err != nil || estimates["s"].CompletedHours != 1 || estimates["s"].AverageDailyCostUSD != 24 {
		t.Fatal(estimates, err)
	}
}

func TestAICodeWithRecordChangedPageRestartsWithoutLosingPublishedData(t *testing.T) {
	for _, scenario := range []string{"cursor_cycle", "changed_duplicate", "no_progress", "expired_cursor"} {
		t.Run(scenario, func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			day := recordTestDay()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.URL.Query().Get("cursor") == "" {
					_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, 17, []map[string]any{recordTestItem(day, 1, 1, "0.1", 10)}, true, "next"))
					return
				}
				switch scenario {
				case "expired_cursor":
					w.WriteHeader(400)
					_, _ = w.Write([]byte(`{"error":"cursor expired"}`))
				case "cursor_cycle":
					_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, 17, []map[string]any{recordTestItem(day, 2, 2, "0.2", 20)}, true, "next"))
				case "changed_duplicate":
					_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, 17, []map[string]any{recordTestItem(day, 1, 1, "0.2", 10)}, false, ""))
				case "no_progress":
					_ = json.NewEncoder(w).Encode(recordTestEnvelope(day, 17, []map[string]any{recordTestItem(day, 1, 1, "0.1", 10)}, true, "another"))
				}
			}))
			defer server.Close()
			m.upstreamClient = server.Client()
			row := ChannelUpstreamAccount{Domain: "s", Provider: upstreamProviderAICodeWith, BaseURL: server.URL}
			round := AICodeWithUsageRound{Domain: "s", RoundID: "r", WindowFrom: day, WindowTo: day + 86400, RecordMode: true}
			if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "s", HourTs: day, CostUSD: 12}).Error; err != nil {
				t.Fatal(err)
			}
			_, ready, err := m.fetchAICodeWithRecordWindow(context.Background(), row, "test", round, "s", day+86400, intBudget(2))
			if err == nil || ready {
				t.Fatalf("ready=%v err=%v", ready, err)
			}
			var n int64
			if err := m.storeDB.Model(&AICodeWithRecordCheckpoint{}).Count(&n).Error; err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatal("checkpoint must restart after semantic drift")
			}
			if err := m.storeDB.Model(&AICodeWithRecordSeen{}).Count(&n).Error; err != nil {
				t.Fatal(err)
			}
			if n != 0 {
				t.Fatal("dedup checkpoint must restart together")
			}
			var old ChannelUpstreamUsageHour
			if err := m.storeDB.First(&old, "domain = ?", "s").Error; err != nil {
				t.Fatal(err)
			}
			if old.CostUSD != 12 {
				t.Fatal(old)
			}
		})
	}
}

func TestAICodeWithRecordAtomicPublicationFailureKeepsDailyAndCheckpoint(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.UpstreamAICodeWithRecordsEnabled = true
	m.upstreamAICodeWithInterval = time.Nanosecond
	fixture := &recordTestServer{}
	server := httptest.NewServer(http.HandlerFunc(fixture.serve))
	defer server.Close()
	m.upstreamClient = server.Client()
	day := recordTestDay()
	row := ChannelUpstreamAccount{Domain: "s", Provider: upstreamProviderAICodeWith, BaseURL: server.URL, BalanceUnit: 1}
	old := ChannelUpstreamUsageHour{Domain: "s", HourTs: day, BucketSeconds: 86400, CostUSD: 12}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER test_record_publish_abort BEFORE INSERT ON channel_upstream_usage_hours WHEN NEW.source_kind='aicodewith_record' BEGIN SELECT RAISE(FAIL,'test publication failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	cred, _ := normalizeAICodeWithCredential(aiCodeWithCredential{APIKey: "sk-acw-record-test-one"})
	version, _ := aiCodeWithCredentialSetVersion(cred)
	published, _, _, err := m.processAICodeWithRound(context.Background(), &row, cred, version, "backfill", day, day+86400, day+86400, 4)
	if published || err == nil {
		t.Fatalf("published=%v err=%v", published, err)
	}
	var kept ChannelUpstreamUsageHour
	if err := m.storeDB.First(&kept, "domain = ?", "s").Error; err != nil {
		t.Fatal(err)
	}
	if kept.CostUSD != 12 || kept.BucketSeconds != 86400 {
		t.Fatal(kept)
	}
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageArchive{}).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatal("archive must roll back with publication")
	}
	fixture.mu.Lock()
	calls := len(fixture.calls)
	fixture.mu.Unlock()
	if err := m.storeDB.Exec("DROP TRIGGER test_record_publish_abort").Error; err != nil {
		t.Fatal(err)
	}
	published, _, _, err = m.processAICodeWithRound(context.Background(), &row, cred, version, "backfill", day, day+86400, day+86460, 4)
	if !published || err != nil {
		t.Fatalf("retry published=%v err=%v", published, err)
	}
	fixture.mu.Lock()
	defer fixture.mu.Unlock()
	if len(fixture.calls) != calls {
		t.Fatal("local publication retry must not refetch upstream")
	}
}

func TestAICodeWithRecordRoundUnitChangeDropsOnlyUnpublishedStage(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.UpstreamAICodeWithRecordsEnabled = true
	day := recordTestDay()
	row := ChannelUpstreamAccount{Domain: "s", Provider: upstreamProviderAICodeWith, BaseURL: "https://aicodewith.ai", BalanceUnit: 1}
	first, err := m.ensureAICodeWithUsageRound(context.Background(), row, "v", "tail", day, day+3600, 1, day+7200)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&AICodeWithRecordCheckpoint{Domain: "s", RoundID: first.RoundID, SlotID: "s", Unit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	row.BalanceUnit = 2
	next, err := m.ensureAICodeWithUsageRound(context.Background(), row, "v", "tail", day, day+7200, 1, day+10800)
	if err != nil || next.RoundID == first.RoundID || next.UnitPerUSD != 2 {
		t.Fatal(next, err)
	}
	var n int64
	if err := m.storeDB.Model(&AICodeWithRecordCheckpoint{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal(n)
	}
}

func TestAICodeWithRecordGuardUsesEightSecondsWithoutSlowingOtherHosts(t *testing.T) {
	clock := &fakeUpstreamGuardClock{now: time.Unix(100, 0)}
	guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{Clock: clock, Jitter: func() time.Duration { return 0 }, MinInterval: time.Second})
	spring := guard.hostState("aicodewith.ai:443")
	other := guard.hostState("other.example:443")
	if err := guard.waitForStart(context.Background(), spring); err != nil {
		t.Fatal(err)
	}
	if err := guard.waitForStart(context.Background(), other); err != nil {
		t.Fatal(err)
	}
	if clock.Now().Unix() != 101 {
		t.Fatal("spring delay leaked into other provider")
	}
	if err := guard.waitForStart(context.Background(), spring); err != nil {
		t.Fatal(err)
	}
	if clock.Now().Unix() != 108 {
		t.Fatal("spring host was not paced at 8 seconds")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := guard.waitForStart(ctx, spring); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestAICodeWithRecordBudgetNeverStartsExtraRequest(t *testing.T) {
	p := intBudget(4)
	for i := 0; i < 4; i++ {
		if err := p.beforeRequest(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	err := p.beforeRequest(context.Background())
	if err == nil || p.calls != 4 || !strings.Contains(err.Error(), "4") {
		t.Fatal(err, p.calls)
	}
}
