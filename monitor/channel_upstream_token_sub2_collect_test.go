package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type sub2ShadowHTTPFixture struct {
	m                  *Monitor
	account            ChannelUpstreamAccount
	day                int64
	requests           atomic.Int64
	missing            atomic.Bool
	changed            atomic.Bool
	changeOnFinalProbe atomic.Bool
}

func newSub2ShadowHTTPFixture(t *testing.T) *sub2ShadowHTTPFixture {
	t.Helper()
	f := &sub2ShadowHTTPFixture{m: newTestMonitor(t), day: time.Date(2026, 8, 24, 0, 0, 0, 0, cstLocation).Unix()}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requestNumber := f.requests.Add(1)
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/usage" {
			t.Errorf("unexpected upstream request: %s %s", r.Method, r.URL.Path)
			w.WriteHeader(404)
			return
		}
		page, _ := strconv.Atoi(r.URL.Query().Get("page"))
		if page != 1 && page != 2 {
			t.Errorf("invalid page %d", page)
			w.WriteHeader(400)
			return
		}
		start, end := (page-1)*100, page*100
		if end > 150 {
			end = 150
		}
		items := make([]map[string]any, 0, end-start)
		for i := start; i < end; i++ {
			token := 574 + i%2
			if (f.changed.Load() || (f.changeOnFinalProbe.Load() && requestNumber == 3)) && i == 0 {
				token = 999
			}
			item := map[string]any{"id": i + 1, "api_key_id": token, "created_at": time.Unix(f.day+int64(i%2)*3600+int64(i), 0).In(cstLocation).Format(time.RFC3339),
				"model": "model-a", "input_tokens": 2, "output_tokens": 3, "actual_cost": "0.000001", "rate_multiplier": "0.15", "group": map[string]any{"name": "same-group", "rate_multiplier": 0.15}}
			if f.missing.Load() && i == 100 {
				delete(item, "api_key_id")
			}
			items = append(items, item)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"code": 0, "data": map[string]any{"items": items, "page": page, "page_size": 100, "pages": 2, "total": 150}})
	}))
	t.Cleanup(server.Close)
	f.account = ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderSub2API, BaseURL: server.URL, Account: "fixture-account", UsageAdapter: upstreamUsageAdapterSub2Trend}
	if err := f.m.storeDB.Create(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	f.m.cfg.ChannelCostClosureDomains = []string{f.account.Domain}
	f.m.cfg.ChannelCostHMACKey = strings.Repeat("unit-test-hmac-", 3)
	f.m.cfg.ChannelCostHMACKeyID = "test-v1"
	return f
}

func (f *sub2ShadowHTTPFixture) configure(t *testing.T, value string, revision int64) {
	t.Helper()
	if err := f.m.storeDB.Create(&ChannelUpstreamTokenConfig{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), ChannelID: 185, Revision: revision, Provider: f.account.Provider, TokenID: value, CreatedAt: f.day + 86400, CreatedBy: "fixture"}).Error; err != nil {
		t.Fatal(err)
	}
}

func (f *sub2ShadowHTTPFixture) fetch(budget int) (map[int64][]ChannelUpstreamPricingHourEvidence, bool, error) {
	rows, _, done, err := f.m.fetchSub2PricingDay(context.Background(), f.account, sub2APICredential{AccessToken: "synthetic-test-token"}, f.day, newUpstreamUsageRequestPacer(budget, 0), f.day+86400)
	return rows, done, err
}

func assertSub2ShadowLegacyTotal(t *testing.T, rows map[int64][]ChannelUpstreamPricingHourEvidence) {
	t.Helper()
	var charge, requests int64
	for _, group := range rows {
		for _, row := range group {
			charge += row.FinalQuota
			requests += row.EligibleRequests
		}
	}
	if len(rows) != 24 || charge != 150 || requests != 150 {
		t.Fatalf("legacy changed: hours=%d charge=%d requests=%d", len(rows), charge, requests)
	}
}

func TestSub2TokenCollectorReusesPagesAndPreservesLegacyExactly(t *testing.T) {
	f := newSub2ShadowHTTPFixture(t)
	baseline, done, err := f.fetch(10)
	if err != nil || !done {
		t.Fatal(err)
	}
	assertSub2ShadowLegacyTotal(t, baseline)
	beforeCalls := f.requests.Load()
	if beforeCalls != 3 {
		t.Fatalf("unexpected baseline requests: %d", beforeCalls)
	}
	if err := f.m.deletePricingPageCheckpoint(context.Background(), f.account.Domain, newAPIUpstreamAccountEpoch(f.account), f.day); err != nil {
		t.Fatal(err)
	}
	f.configure(t, "575", 1)
	f.m.cfg.ChannelCostClosureEnabled = true
	actual, done, err := f.fetch(10)
	if err != nil || !done || !reflect.DeepEqual(baseline, actual) {
		t.Fatal("shadow changed legacy output", err)
	}
	if f.requests.Load()-beforeCalls != 3 {
		t.Fatal("shadow added upstream calls")
	}
	cursor, err := f.m.loadSub2TokenScanCheckpoint(context.Background(), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
	if err != nil {
		t.Fatal(err)
	}
	cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
	if err != nil || cursor.Revision != 2 || len(cp.SeenRows) != 150 || reconcileSub2TokenShadow(cp, actual) != nil {
		t.Fatal("shadow lost conservation", err)
	}
	for _, model := range []any{&ChannelUpstreamCostHourEvidence{}, &ChannelCostSourceBinding{}, &ChannelEconomicsDirtyHour{}} {
		var count int64
		if err := f.m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("shadow activated %T", model)
		}
	}
}

func TestSub2TokenCollectorAbsentOrClearedConfigurationDoesNotWrite(t *testing.T) {
	for _, mode := range []string{"absent", "cleared", "disabled", "not-allowed", "read-only"} {
		t.Run(mode, func(t *testing.T) {
			f := newSub2ShadowHTTPFixture(t)
			f.m.cfg.ChannelCostClosureEnabled = true
			if mode != "absent" {
				f.configure(t, "575", 1)
			}
			switch mode {
			case "cleared":
				f.configure(t, "", 2)
			case "disabled":
				f.m.cfg.ChannelCostClosureEnabled = false
			case "not-allowed":
				f.m.cfg.ChannelCostClosureDomains = []string{"other.example"}
			case "read-only":
				f.m.cfg.LocalSnapshotOnly = true
			}
			if mode == "read-only" {
				if f.m.beginSub2TokenShadow(context.Background(), f.account, f.day, f.day+86400, ChannelUpstreamPricingPageCheckpoint{}, false) != nil {
					t.Fatal("readonly shadow active")
				}
			} else {
				rows, done, err := f.fetch(10)
				if err != nil || !done {
					t.Fatal(err)
				}
				assertSub2ShadowLegacyTotal(t, rows)
			}
			var count int64
			if err := f.m.storeDB.Model(&ChannelUpstreamTokenScanCheckpoint{}).Count(&count).Error; err != nil || count != 0 {
				t.Fatal("disabled shadow wrote cursor", err)
			}
		})
	}
}

func TestSub2TokenCollectorFailureNeverStopsLegacy(t *testing.T) {
	for _, mode := range []string{"missing-id", "write-failure", "key-missing"} {
		t.Run(mode, func(t *testing.T) {
			f := newSub2ShadowHTTPFixture(t)
			f.configure(t, "575", 1)
			f.m.cfg.ChannelCostClosureEnabled = true
			switch mode {
			case "missing-id":
				f.missing.Store(true)
			case "write-failure":
				if err := f.m.storeDB.Exec(`CREATE TRIGGER fail_token_shadow BEFORE INSERT ON channel_upstream_token_scan_checkpoints BEGIN SELECT RAISE(ABORT,'synthetic shadow failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			case "key-missing":
				f.m.cfg.ChannelCostHMACKey = ""
			}
			rows, done, err := f.fetch(10)
			if err != nil || !done {
				t.Fatal("shadow blocked legacy", err)
			}
			assertSub2ShadowLegacyTotal(t, rows)
			if f.requests.Load() != 3 {
				t.Fatal("failure added upstream retries")
			}
		})
	}
}

func TestSub2TokenCollectorBudgetResumeAndIdentityChange(t *testing.T) {
	for _, mode := range []string{"resume", "identity-change", "configured-mid-scan"} {
		t.Run(mode, func(t *testing.T) {
			f := newSub2ShadowHTTPFixture(t)
			f.m.cfg.ChannelCostClosureEnabled = true
			if mode != "configured-mid-scan" {
				f.configure(t, "575", 1)
			}
			_, done, err := f.fetch(1)
			if err != nil || done || f.requests.Load() != 1 {
				t.Fatal("budget did not yield", err)
			}
			if mode == "identity-change" {
				f.changed.Store(true)
			}
			if mode == "configured-mid-scan" {
				f.configure(t, "575", 1)
			}
			f.m = &Monitor{storeDB: f.m.storeDB, cfg: f.m.cfg}
			rows, done, err := f.fetch(3)
			if err != nil || !done {
				t.Fatal("legacy resume failed", err)
			}
			assertSub2ShadowLegacyTotal(t, rows)
			if f.requests.Load() != 3 {
				t.Fatal("resume added network calls")
			}
			cursor, err := f.m.loadSub2TokenScanCheckpoint(context.Background(), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "configured-mid-scan" {
				if cursor.Envelope != "" {
					t.Fatal("invented missing first-page identity")
				}
				return
			}
			cp, err := decodeSub2TokenCheckpoint([]byte(cursor.Envelope), f.account, f.day, []byte(f.m.cfg.ChannelCostHMACKey), f.m.cfg.ChannelCostHMACKeyID)
			if err != nil {
				t.Fatal(err)
			}
			if mode == "identity-change" {
				if len(cp.SeenRows) != 100 {
					t.Fatal("changed identity advanced shadow")
				}
			} else if len(cp.SeenRows) != 150 || reconcileSub2TokenShadow(cp, rows) != nil {
				t.Fatal("resume lost shadow evidence")
			}
		})
	}
}

func TestSub2TokenShadowReconcileRejectsSwappedHours(t *testing.T) {
	account, items, day, key := sub2TokenFixture(t)
	encoded, err := advanceSub2TokenCheckpoint(nil, account, sub2PricingPage{Items: items, Total: 3, Page: 1, PageSize: 100, Pages: 1}, day, day+86400, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	cp, err := decodeSub2TokenCheckpoint(encoded, account, day, key, "v1")
	if err != nil {
		t.Fatal(err)
	}
	pricing, err := buildSub2PricingEvidence(account, items, day+86400)
	if err != nil {
		t.Fatal(err)
	}
	legacy := make(map[int64][]ChannelUpstreamPricingHourEvidence)
	for hour := day; hour < day+86400; hour += 3600 {
		legacy[hour] = nil
	}
	for _, row := range pricing {
		legacy[row.HourTs] = append(legacy[row.HourTs], row)
	}
	if err := reconcileSub2TokenShadow(cp, legacy); err != nil {
		t.Fatal(err)
	}
	first, second := pricing[0].HourTs, pricing[1].HourTs
	legacy[first][0].FinalQuota++
	legacy[second][0].FinalQuota--
	if err := reconcileSub2TokenShadow(cp, legacy); err == nil {
		t.Fatal("matching daily total hid wrong hourly costs")
	}
}

func TestSub2TokenCollectorFailureStillCompletesLegacyTwoScanDispatcher(t *testing.T) {
	f := newSub2ShadowHTTPFixture(t)
	f.day = cstDayStart(time.Now().Add(-24 * time.Hour).Unix())
	f.configure(t, "575", 1)
	f.missing.Store(true)
	f.m.cfg.ChannelCostClosureEnabled = true
	f.m.cfg.UpstreamPricingLedgerEnabled = true
	f.m.cfg.UpstreamPricingLedgerDomains = []string{f.account.Domain}
	f.m.cfg.UpstreamCredentialSecret = "synthetic-dispatcher-secret"
	credential := sub2APICredential{AccessToken: "synthetic-token", RefreshToken: "synthetic-refresh", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	sealed, err := f.m.sealUpstreamCredential(f.account.Domain, f.account.Provider, credential)
	if err != nil {
		t.Fatal(err)
	}
	f.account.Enabled = true
	f.account.UsageSyncEnabled = true
	f.account.Credential = sealed
	f.account.CredentialVersion = upstreamCredentialVersion
	if err := f.m.storeDB.Save(&f.account).Error; err != nil {
		t.Fatal(err)
	}
	for hour := f.day; hour < f.day+86400; hour += 3600 {
		bucket := ChannelUpstreamUsageHour{Domain: f.account.Domain, HourTs: hour, BucketSeconds: 3600, Provider: f.account.Provider}
		if hour == f.day || hour == f.day+3600 {
			bucket.Requests = 75
			bucket.Tokens = 375
			bucket.CostUSD = 0.000075
			bucket.Quota = 0.000075
		}
		if err := f.m.storeDB.Create(&bucket).Error; err != nil {
			t.Fatal(err)
		}
	}
	state := ChannelUpstreamPricingSyncState{Domain: f.account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(f.account), SemanticsVersion: upstreamPricingSemanticsVersion,
		Status: "pending", BackfillStartHour: f.day, BackfillNextHour: f.day, BackfillTargetHour: f.day + 86400}
	if err := f.m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	first, err := f.m.syncStoredSub2Pricing(context.Background(), f.account.Domain)
	if err != nil || first.Status != "pending_verification" || first.BackfillNextHour != f.day {
		t.Fatalf("first legacy scan: status=%s err=%v", first.Status, err)
	}
	if err := f.m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain = ?", f.account.Domain).Update("backfill_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	second, err := f.m.syncStoredSub2Pricing(context.Background(), f.account.Domain)
	if err != nil || !second.BackfillDone || second.Status != "ok" || second.BackfillNextHour != f.day+86400 {
		t.Fatalf("shadow failure stopped legacy backfill: status=%s done=%v err=%v", second.Status, second.BackfillDone, err)
	}
	if f.requests.Load() != 6 {
		t.Fatalf("dispatcher added requests: %d", f.requests.Load())
	}
	var published int64
	if err := f.m.storeDB.Model(&ChannelUpstreamCostHourEvidence{}).Count(&published).Error; err != nil || published != 0 {
		t.Fatal("incomplete token evidence published", err)
	}
}
