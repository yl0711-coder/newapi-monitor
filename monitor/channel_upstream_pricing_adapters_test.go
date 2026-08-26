package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestSub2PricingEvidenceKeepsStandardEffectiveRateAndActualCost(t *testing.T) {
	raw := json.RawMessage(`{
		"id":91,"created_at":"2026-08-24T10:20:30+08:00","model":"gpt-5.5",
		"input_tokens":2,"output_tokens":3,"actual_cost":"0.123456",
		"rate_multiplier":"0.5","billing_mode":"token",
		"group":{"id":7,"name":"codex","rate_multiplier":"0.7"}
	}`)
	item, err := decodeSub2PricingUsageItem(raw)
	if err != nil {
		t.Fatal(err)
	}
	if item.GroupRatio.Text != "0.7" || item.EffectiveRatio.Text != "0.5" || item.ActualCostMicros != 123456 || item.EvidenceCapability != "effective_rate" {
		t.Fatalf("item=%+v", item)
	}
	account := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: "https://sub2.example", Account: "acct"}
	evidence, err := buildSub2PricingEvidence(account, []sub2PricingUsageItem{item}, item.CreatedAt+100)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].FinalQuota != 123456 || evidence[0].GroupRatio != "" || evidence[0].EffectiveRatio != "0.5" {
		t.Fatalf("evidence=%+v", evidence)
	}
	m := newTestMonitor(t)
	hour := item.CreatedAt - item.CreatedAt%3600
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: hour, BucketSeconds: 3600, Requests: 1, Tokens: 5, CostUSD: 0.123456, Quota: 0.123456, Provider: upstreamProviderSub2API}).Error; err != nil {
		t.Fatal(err)
	}
	state, err := pricingHourStateFromEvidence(account, hour, evidence, item.CreatedAt+100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, item.CreatedAt+100+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	var published ChannelUpstreamPricingHourState
	if err := m.storeDB.First(&published, "domain = ? AND hour_ts = ?", account.Domain, hour).Error; err != nil {
		t.Fatal(err)
	}
	if published.Status != "verified" || published.ReconcileStatus != "matched" || published.QuotaDelta != 0 {
		t.Fatalf("published=%+v", published)
	}
	var observed ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&observed, "domain = ? AND source_group = ? AND model_name = ?", account.Domain, "codex", "gpt-5.5").Error; err != nil {
		t.Fatal(err)
	}
	if observed.GroupRatio != "" || observed.EffectiveRatio != "0.5" || observed.EvidenceCapability != "effective_rate" {
		t.Fatalf("observed=%+v", observed)
	}
}

func TestSub2PricingAdapterCallsDocumentedUserUsageRoute(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, cstLocation).Unix()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/usage" || r.URL.Query().Get("start_date") != "2026-08-24" || r.URL.Query().Get("end_date") != "2026-08-24" || r.URL.Query().Get("sort_order") != "asc" {
			t.Errorf("unexpected request: %s", r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer sub2-token" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"success","data":{"items":[{
			"id":1,"created_at":"2026-08-24T10:00:00+08:00","model":"gpt-5.5",
			"input_tokens":1,"output_tokens":2,"cache_creation_tokens":3,"cache_read_tokens":4,
			"actual_cost":"0.25","rate_multiplier":"0.8","group":{"name":"codex","rate_multiplier":1}
		}],"total":1,"page":1,"page_size":100,"pages":1}}`))
	}))
	defer server.Close()
	page, err := fetchSub2PricingPage(context.Background(), server.Client(), ChannelUpstreamAccount{Domain: "sub2.example", BaseURL: server.URL}, sub2APICredential{AccessToken: "sub2-token"}, day, 1, newUpstreamUsageRequestPacer(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Items) != 1 || page.Items[0].PromptTokens != 8 || page.Items[0].CompletionTokens != 2 || page.Items[0].EvidenceCapability != "effective_rate" {
		t.Fatalf("page=%+v", page)
	}
}

func TestAICodeWithPricingAdapterCallsDetailsOncePerKey(t *testing.T) {
	day := time.Date(2026, 8, 24, 0, 0, 0, 0, cstLocation).Unix()
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.URL.Path != "/api/v1/usage/details" || r.URL.Query().Get("start") != "2026-08-24" || r.URL.Query().Get("end") != "2026-08-24" {
			t.Errorf("unexpected request: %s", r.URL.String())
		}
		if r.Header.Get("Authorization") != "Bearer spring-key" {
			t.Errorf("authorization=%q", r.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"data":{"details":[{"timestamp":"2026-08-24T10:00:00+08:00","channelDiscount":0.9,"totalCostCNY":2}],"total":1}}`))
	}))
	defer server.Close()
	items, err := fetchAICodeWithPricingDay(context.Background(), server.Client(), ChannelUpstreamAccount{Domain: "aicodewith.com", BaseURL: server.URL}, "spring-key", day, newUpstreamUsageRequestPacer(1, 0))
	if err != nil {
		t.Fatal(err)
	}
	if requests != 1 || len(items) != 1 || items[0].Discount.Text != "0.9" {
		t.Fatalf("requests=%d items=%+v", requests, items)
	}
}

func TestSub2PricingDispatcherCompletesTwoScanDayAndAdvancesCursor(t *testing.T) {
	day := cstDayStart(time.Now().Add(-24 * time.Hour).Unix())
	created := time.Unix(day+10*3600, 0).In(cstLocation).Format(time.RFC3339)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"code":0,"message":"success","data":{"items":[{
			"id":1,"created_at":%q,"model":"gpt-5.5","input_tokens":4,"output_tokens":6,
			"actual_cost":"0.25","rate_multiplier":"0.8","group":{"name":"codex","rate_multiplier":1}
		}],"total":1,"page":1,"page_size":100,"pages":1}}`, created)
	}))
	defer server.Close()
	m := newTestMonitor(t)
	m.cfg.UpstreamCredentialSecret = "sub2-pricing-test-secret"
	domain := server.Listener.Addr().String()
	credential := sub2APICredential{AccessToken: "sub2-token", RefreshToken: "refresh", ExpiresAt: time.Now().Add(time.Hour).Unix()}
	sealed, err := m.sealUpstreamCredential(domain, upstreamProviderSub2API, credential)
	if err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderSub2API, BaseURL: server.URL, Account: "acct", Credential: sealed, CredentialVersion: upstreamCredentialVersion, Enabled: true, UsageSyncEnabled: true, UsageAdapter: upstreamUsageAdapterSub2Trend}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	for hour := day; hour < day+86400; hour += 3600 {
		bucket := ChannelUpstreamUsageHour{Domain: domain, HourTs: hour, BucketSeconds: 3600, Provider: upstreamProviderSub2API}
		if hour == day+10*3600 {
			bucket.Requests, bucket.Tokens, bucket.CostUSD, bucket.Quota = 1, 10, 0.25, 0.25
		}
		if err := m.storeDB.Create(&bucket).Error; err != nil {
			t.Fatal(err)
		}
	}
	state := ChannelUpstreamPricingSyncState{Domain: domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion, Status: "pending", BackfillStartHour: day, BackfillNextHour: day, BackfillTargetHour: day + 86400}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	first, err := m.syncStoredUpstreamPricing(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != "pending_verification" || first.BackfillNextHour != day {
		t.Fatalf("first=%+v", first)
	}
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain = ?", domain).Update("backfill_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	second, err := m.syncStoredUpstreamPricing(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if !second.BackfillDone || second.BackfillNextHour != day+86400 || second.Status != "ok" {
		t.Fatalf("second=%+v", second)
	}
}

func TestAICodeWithPricingDispatcherUsesAllKeysBeforePublishing(t *testing.T) {
	day := cstDayStart(time.Now().Add(-24 * time.Hour).Unix())
	created := time.Unix(day+11*3600, 0).In(cstLocation).Format(time.RFC3339)
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"records":[{"timestamp":%q,"model":"gpt-5.5","channelDiscount":"0.9","totalCostCNY":"1"}],"total":1}}`, created)
	}))
	defer server.Close()
	m := newTestMonitor(t)
	m.cfg.UpstreamCredentialSecret = "aicodewith-pricing-test-secret"
	domain := server.Listener.Addr().String()
	credential := aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{
		{SlotID: "acw_one", Secret: "sk-acw-one"},
		{SlotID: "acw_two", Secret: "sk-acw-two"},
	}}
	sealed, err := m.sealUpstreamCredential(domain, upstreamProviderAICodeWith, credential)
	if err != nil {
		t.Fatal(err)
	}
	account := ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderAICodeWith, BaseURL: server.URL, Account: "keys", Credential: sealed, CredentialVersion: upstreamCredentialVersion, Enabled: true, UsageSyncEnabled: true}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	state := ChannelUpstreamPricingSyncState{Domain: domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion, Status: "pending", BackfillStartHour: day, BackfillNextHour: day, BackfillTargetHour: day + 86400}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	first, err := m.syncStoredUpstreamPricing(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 2 || first.Status != "pending_verification" {
		t.Fatalf("requests=%d first=%+v", requests, first)
	}
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain = ?", domain).Update("backfill_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	second, err := m.syncStoredUpstreamPricing(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if requests != 4 || !second.BackfillDone || second.Status != "ok" {
		t.Fatalf("requests=%d second=%+v", requests, second)
	}
	var observed ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&observed, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if observed.DiscountRatio != "0.9" || observed.EvidenceCapability != "discount_only" {
		t.Fatalf("observed=%+v", observed)
	}
}

func TestAICodeWithPricingCheckpointResumesFailedKeyWithoutRepeatingCompletedKey(t *testing.T) {
	day := cstDayStart(time.Now().Add(-24 * time.Hour).Unix())
	created := time.Unix(day+12*3600, 0).In(cstLocation).Format(time.RFC3339)
	oneCalls, twoCalls := 0, 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
		switch key {
		case "sk-acw-one":
			oneCalls++
		case "sk-acw-two":
			twoCalls++
			if twoCalls == 1 {
				w.Header().Set("Retry-After", "1")
				http.Error(w, `{"error":{"message":"rate limited"}}`, http.StatusTooManyRequests)
				return
			}
		default:
			t.Fatalf("unexpected key %q", key)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = fmt.Fprintf(w, `{"data":{"records":[{"timestamp":%q,"channelDiscount":1,"totalCostCNY":1}],"total":1}}`, created)
	}))
	defer server.Close()
	m := newTestMonitor(t)
	account := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderAICodeWith, BaseURL: server.URL, Account: "keys"}
	credential := aiCodeWithCredential{Slots: []aiCodeWithKeyCredential{{SlotID: "acw_one", Secret: "sk-acw-one"}, {SlotID: "acw_two", Secret: "sk-acw-two"}}}
	if _, _, _, err := m.fetchAICodeWithPricingDay(context.Background(), account, credential, day, time.Now().Unix()); err == nil {
		t.Fatal("rate-limited key must fail the current turn")
	}
	var checkpoint AICodeWithPricingCheckpoint
	if err := m.storeDB.First(&checkpoint, "domain = ? AND day_ts = ?", account.Domain, day).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextCredential != 1 || oneCalls != 1 || twoCalls != 1 {
		t.Fatalf("checkpoint=%+v calls=%d/%d", checkpoint, oneCalls, twoCalls)
	}
	// Simulate the persisted Retry-After/circuit deadline having elapsed; the
	// production scheduler waits for it instead of immediately retrying.
	if err := m.storeDB.Where("1 = 1").Delete(&UpstreamHostCircuit{}).Error; err != nil {
		t.Fatal(err)
	}
	byHour, _, complete, err := m.fetchAICodeWithPricingDay(context.Background(), account, credential, day, time.Now().Unix())
	if err != nil {
		t.Fatal(err)
	}
	if !complete || oneCalls != 1 || twoCalls != 2 || len(byHour[day+12*3600]) != 1 || byHour[day+12*3600][0].SourceRows != 2 {
		t.Fatalf("complete=%v calls=%d/%d evidence=%+v", complete, oneCalls, twoCalls, byHour[day+12*3600])
	}
	var published int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingHourEvidence{}).Where("domain = ?", account.Domain).Count(&published).Error; err != nil {
		t.Fatal(err)
	}
	if published != 0 {
		t.Fatalf("staging must not publish evidence, rows=%d", published)
	}
}

func TestAICodeWithPricingEvidenceIsExplicitlyDiscountOnly(t *testing.T) {
	body := []byte(`{"data":{"records":[{
		"timestamp":"2026-08-24T10:20:30+08:00","modelName":"gpt-5.5",
		"channelName":"spring-pool","channelDiscount":"0.82","totalCostCNY":"1.2345",
		"inputTokens":10,"outputTokens":2
	}],"total":1}}`)
	items, err := decodeAICodeWithPricingDetails(body)
	if err != nil {
		t.Fatal(err)
	}
	if len(items) != 1 || items[0].Discount.Text != "0.82" || items[0].CostMicros != 1234500 {
		t.Fatalf("items=%+v", items)
	}
	account := ChannelUpstreamAccount{Domain: "aicodewith.com", Provider: upstreamProviderAICodeWith, BaseURL: "https://aicodewith.com", Account: "keys"}
	evidence, err := buildAICodeWithPricingEvidence(account, items, items[0].CreatedAt+100)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 1 || evidence[0].EvidenceCapability != "discount_only" || evidence[0].DiscountRatio != "0.82" || evidence[0].EffectiveRatio != "" {
		t.Fatalf("evidence=%+v", evidence)
	}
	m := newTestMonitor(t)
	hour := items[0].CreatedAt - items[0].CreatedAt%3600
	state, err := pricingHourStateFromEvidence(account, hour, evidence, items[0].CreatedAt+100)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, items[0].CreatedAt+100+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	var observed ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&observed, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if observed.EvidenceCapability != "discount_only" || observed.DiscountRatio != "0.82" || observed.EffectiveRatio != "" {
		t.Fatalf("observed=%+v", observed)
	}
}

func TestAICodeWithPricingDetailsFailsClosedWhenPaginationIsRequired(t *testing.T) {
	body := []byte(`{"data":{"records":[{"timestamp":"2026-08-24T10:20:30+08:00","channelDiscount":1,"totalCostCNY":1}],"total":2}}`)
	if _, err := decodeAICodeWithPricingDetails(body); err == nil {
		t.Fatal("partial details must not be published")
	}
}
