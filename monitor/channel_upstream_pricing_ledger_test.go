package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestDecodeNewAPIPricingObservationFromContractFixture(t *testing.T) {
	fixture := json.RawMessage(`{
		"id":17,"created_at":1787623200,"quota":700000,
		"prompt_tokens":31731,"completion_tokens":377,
		"model_name":"gpt-5.5","group":"codex-1.4x","token_id":75,
		"other":"{\"model_ratio\":2.5,\"group_ratio\":1.4,\"user_group_ratio\":0.8,\"billing_mode\":\"token\",\"channel_id\":59}"
	}`)
	item, err := decodeNewAPIUsageItem(fixture)
	if err != nil {
		t.Fatal(err)
	}
	if item.CreatedAt != 1787623200 || item.QuotaExact != 700000 || !item.QuotaExactKnown {
		t.Fatalf("exact source fields lost: %+v", item)
	}
	if item.Pricing.GroupName != "codex-1.4x" || item.Pricing.ModelName != "gpt-5.5" || item.Pricing.TokenID != 75 {
		t.Fatalf("pricing dimensions lost: %+v", item.Pricing)
	}
	if item.Pricing.GroupRatio != "1.4" || item.Pricing.UserGroupRatio != "0.8" {
		t.Fatalf("ratios=%+v", item.Pricing)
	}
	if item.Pricing.EffectiveRatio != "0.8" || item.Pricing.EffectiveRatioSource != "user_group_ratio" {
		t.Fatalf("effective ratio=%+v", item.Pricing)
	}
}

func TestDecodeNewAPIPricingOtherCompatibilityMatrix(t *testing.T) {
	tests := []struct {
		name            string
		other           string
		groupState      string
		group           string
		userState       string
		user            string
		effective       string
		effectiveSource string
		otherValid      bool
	}{
		{"object numbers", `{"group_ratio":1.4000,"user_group_ratio":0.8}`, pricingRatioValid, "1.4", pricingRatioValid, "0.8", "0.8", "user_group_ratio", true},
		{"object strings", `{"group_ratio":"0.290000","user_group_ratio":"0"}`, pricingRatioValid, "0.29", pricingRatioValid, "0", "0.29", "group_ratio", true},
		{"empty object", `{}`, pricingRatioMissing, "", pricingRatioMissing, "", "", "unknown", true},
		{"null fields", `{"group_ratio":null,"user_group_ratio":null}`, pricingRatioNull, "", pricingRatioNull, "", "", "unknown", true},
		{"invalid user retains group", `{"group_ratio":1.2,"user_group_ratio":"bad"}`, pricingRatioValid, "1.2", pricingRatioInvalid, "", "1.2", "group_ratio", true},
		{"negative group invalid", `{"group_ratio":-1}`, pricingRatioInvalid, "", pricingRatioMissing, "", "", "unknown", true},
		{"broken other", `{broken`, pricingRatioInvalid, "", pricingRatioInvalid, "", "", "unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var other any = json.RawMessage(tt.other)
			if tt.name == "broken other" {
				other = tt.other
			}
			fixture, _ := json.Marshal(map[string]any{
				"created_at": 1000, "quota": 1, "prompt_tokens": 1, "completion_tokens": 2,
				"model_name": "m", "group": "g", "other": other,
			})
			item, err := decodeNewAPIUsageItem(fixture)
			if err != nil {
				t.Fatal(err)
			}
			got := item.Pricing
			if got.GroupRatioState != tt.groupState || got.GroupRatio != tt.group ||
				got.UserGroupRatioState != tt.userState || got.UserGroupRatio != tt.user ||
				got.EffectiveRatio != tt.effective || got.EffectiveRatioSource != tt.effectiveSource || got.OtherValid != tt.otherValid {
				t.Fatalf("pricing=%+v", got)
			}
		})
	}
}

func TestDecodeNewAPIPricingOtherStringAndMissing(t *testing.T) {
	encoded := json.RawMessage(`{"created_at":1000,"quota":"700000","prompt_tokens":"2","completion_tokens":1,"other":"{\"group_ratio\":\"1.2000\"}"}`)
	item, err := decodeNewAPIUsageItem(encoded)
	if err != nil {
		t.Fatal(err)
	}
	if item.QuotaExact != 700000 || !item.QuotaExactKnown || item.Pricing.GroupRatio != "1.2" {
		t.Fatalf("item=%+v", item)
	}

	missing, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1000,"quota":1}`))
	if err != nil {
		t.Fatal(err)
	}
	if !missing.Pricing.OtherValid || missing.Pricing.GroupRatioState != pricingRatioMissing || missing.Pricing.UserGroupRatioState != pricingRatioMissing {
		t.Fatalf("missing other=%+v", missing.Pricing)
	}
}

func TestPricingLedgerRejectsInexactFinalQuota(t *testing.T) {
	item, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1000,"quota":9007199254740993,"other":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if !item.QuotaExactKnown || item.QuotaExact != 9007199254740993 {
		t.Fatalf("large quota lost precision: %+v", item)
	}
	if _, err := pricingSourceContentHash([]newAPIPricingUsageItem{item}, 900, 1100); err != nil {
		t.Fatal(err)
	}

	fractional, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1000,"quota":1.5,"other":{}}`))
	if err != nil {
		t.Fatal(err)
	}
	if fractional.QuotaExactKnown {
		t.Fatal("fractional quota must not become an exact ledger value")
	}
	if _, err := pricingSourceContentHash([]newAPIPricingUsageItem{fractional}, 900, 1100); err == nil {
		t.Fatal("inexact quota must fail closed for the pricing ledger")
	}
}

func TestPricingLedgerHashCanonicalizesRatiosAndIgnoresJSONKeyOrder(t *testing.T) {
	left, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1000,"quota":7,"group":"g","model_name":"m","other":{"group_ratio":0.290000,"user_group_ratio":null}}`))
	if err != nil {
		t.Fatal(err)
	}
	right, err := decodeNewAPIUsageItem(json.RawMessage(`{"other":{"user_group_ratio":null,"group_ratio":"0.29"},"model_name":"m","group":"g","quota":"7","created_at":1000}`))
	if err != nil {
		t.Fatal(err)
	}
	if pricingDimensionHash(left.Pricing) != pricingDimensionHash(right.Pricing) {
		t.Fatalf("equivalent ratios produced different hashes: %+v %+v", left.Pricing, right.Pricing)
	}
	lh, err := pricingSourceContentHash([]newAPIPricingUsageItem{left}, 900, 1100)
	if err != nil {
		t.Fatal(err)
	}
	rh, err := pricingSourceContentHash([]newAPIPricingUsageItem{right}, 900, 1100)
	if err != nil {
		t.Fatal(err)
	}
	if lh != rh {
		t.Fatalf("equivalent records produced different content hashes: %s %s", lh, rh)
	}
}

func TestPricingLedgerNeverMultipliesFinalQuotaAgain(t *testing.T) {
	item, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1000,"quota":700000,"prompt_tokens":10,"completion_tokens":2,"other":{"group_ratio":1.4}}`))
	if err != nil {
		t.Fatal(err)
	}
	if item.QuotaExact != 700000 {
		t.Fatalf("final quota was transformed: %d", item.QuotaExact)
	}
}

func TestPricingLedgerFinalQuotaConservationAndAtomicReconcile(t *testing.T) {
	m := newTestMonitor(t)
	hour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 147426, BalanceUnit: 500000}
	items := make([]newAPIPricingUsageItem, 0, 4)
	for _, raw := range []string{
		`{"created_at":1787623201,"quota":700000,"prompt_tokens":10,"completion_tokens":2,"group":"codex-1.2x","model_name":"gpt-5.5","token_id":59,"other":{"group_ratio":1.2}}`,
		`{"created_at":1787623202,"quota":300000,"prompt_tokens":5,"completion_tokens":1,"group":"codex-1.2x","model_name":"gpt-5.5","token_id":59,"other":{"group_ratio":"1.2"}}`,
		`{"created_at":1787623203,"quota":100000,"prompt_tokens":3,"completion_tokens":1,"group":"codex-1.2x","model_name":"gpt-5.5","token_id":60,"other":{"group_ratio":1.2,"user_group_ratio":0.8}}`,
		`{"created_at":1787623204,"quota":0,"prompt_tokens":99,"completion_tokens":99,"group":"codex-1.2x","model_name":"gpt-5.5","other":{}}`,
	} {
		item, err := decodeNewAPIUsageItem(json.RawMessage(raw))
		if err != nil {
			t.Fatal(err)
		}
		items = append(items, item)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{
		Domain: account.Domain, HourTs: hour, BucketSeconds: 3600,
		Requests: 3, Tokens: 22, Quota: 1100000, CostUSD: 2.2, Provider: upstreamProviderNewAPI,
	}).Error; err != nil {
		t.Fatal(err)
	}
	evidence, state, err := buildNewAPIPricingHour(account, items, hour, hour+4000)
	if err != nil {
		t.Fatal(err)
	}
	if state.SourceRows != 4 || state.EligibleRequests != 3 || state.Tokens != 22 || state.FinalQuota != 1100000 {
		t.Fatalf("state=%+v", state)
	}
	var evidenceQuota, evidenceRequests int64
	for _, row := range evidence {
		evidenceQuota += row.FinalQuota
		evidenceRequests += row.EligibleRequests
	}
	if evidenceQuota != state.FinalQuota || evidenceRequests != state.EligibleRequests {
		t.Fatalf("evidence quota/requests=%d/%d state=%d/%d", evidenceQuota, evidenceRequests, state.FinalQuota, state.EligibleRequests)
	}
	if err := m.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, hour+4000); err != nil {
		t.Fatal(err)
	}
	var first ChannelUpstreamPricingHourState
	if err := m.storeDB.First(&first, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hour).Error; err != nil {
		t.Fatal(err)
	}
	if first.Status != "observed" || first.ReconcileStatus != "matched" || first.VerifiedScans != 1 {
		t.Fatalf("first=%+v", first)
	}
	if err := m.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, hour+5000); err != nil {
		t.Fatal(err)
	}
	var second ChannelUpstreamPricingHourState
	if err := m.storeDB.First(&second, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hour).Error; err != nil {
		t.Fatal(err)
	}
	if second.Status != "verified" || second.ReconcileStatus != "matched" || second.VerifiedScans != 2 || second.QuotaDelta != 0 {
		t.Fatalf("second=%+v", second)
	}
	var stored []ChannelUpstreamPricingHourEvidence
	if err := m.storeDB.Where("domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hour).Find(&stored).Error; err != nil {
		t.Fatal(err)
	}
	var storedQuota int64
	for _, row := range stored {
		storedQuota += row.FinalQuota
	}
	if storedQuota != 1100000 {
		t.Fatalf("rerun duplicated or transformed final quota: %d", storedQuota)
	}
}

func TestPricingTailIgnoresRefreshTimeButRequeuesContentChanges(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	closedThrough := int64(1787626800)
	hour := closedThrough - 3*3600
	account := ChannelUpstreamAccount{
		Domain: "4sapi.com", Provider: upstreamProviderNewAPI,
		BaseURL: "https://4sapi.com", UserID: 147426,
	}
	legacy := ChannelUpstreamUsageHour{
		Domain: account.Domain, HourTs: hour, BucketSeconds: 3600,
		Requests: 12, Tokens: 3456, Quota: 789000, CostUSD: 1.578,
		Provider: upstreamProviderNewAPI, FetchedAt: hour + 7200,
	}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	observed := ChannelUpstreamPricingHourState{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account),
		HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion,
		Status: "verified", ReconcileStatus: "matched",
		LegacyRequests: legacy.Requests, LegacyTokens: legacy.Tokens, LegacyQuota: 789000,
		LegacyFetchedAt: legacy.FetchedAt - 900,
	}
	if err := m.storeDB.Create(&observed).Error; err != nil {
		t.Fatal(err)
	}

	if dueHour, due, err := m.pricingTailHourDue(ctx, account, closedThrough); err != nil {
		t.Fatal(err)
	} else if due {
		t.Fatalf("refresh-time-only change requeued hour %d", dueHour)
	}

	for _, tc := range []struct {
		name   string
		column string
		value  any
	}{
		{name: "requests", column: "requests", value: legacy.Requests + 1},
		{name: "tokens", column: "tokens", value: legacy.Tokens + 1},
		{name: "quota", column: "quota", value: legacy.Quota + 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).
				Where("domain = ? AND hour_ts = ?", account.Domain, hour).
				Update(tc.column, tc.value).Error; err != nil {
				t.Fatal(err)
			}
			dueHour, due, err := m.pricingTailHourDue(ctx, account, closedThrough)
			if err != nil {
				t.Fatal(err)
			}
			if !due || dueHour != hour {
				t.Fatalf("content change did not requeue hour: due=%v hour=%d", due, dueHour)
			}
			if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).
				Where("domain = ? AND hour_ts = ?", account.Domain, hour).
				Updates(map[string]any{
					"requests": legacy.Requests,
					"tokens":   legacy.Tokens,
					"quota":    legacy.Quota,
				}).Error; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestPricingLedgerMismatchDoesNotPublishObservedCurrentState(t *testing.T) {
	m := newTestMonitor(t)
	hour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 147426}
	item, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1787623201,"quota":700000,"prompt_tokens":10,"completion_tokens":2,"group":"codex-1.2x","model_name":"gpt-5.5","other":{"group_ratio":1.2}}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: hour, Requests: 1, Tokens: 12, Quota: 699999}).Error; err != nil {
		t.Fatal(err)
	}
	evidence, state, err := buildNewAPIPricingHour(account, []newAPIPricingUsageItem{item}, hour, hour+4000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, hour+4000+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingObservedState{}).Where("domain = ?", account.Domain).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("mismatched hour published %d current pricing states", count)
	}
}

func TestPricingLedgerAccountEpochSeparatesIdentityChanges(t *testing.T) {
	first := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	second := first
	second.UserID = 2
	if newAPIUpstreamAccountEpoch(first) == newAPIUpstreamAccountEpoch(second) {
		t.Fatal("different upstream accounts share one pricing epoch")
	}
	rotated := first
	rotated.Credential = "different-secret"
	if newAPIUpstreamAccountEpoch(first) != newAPIUpstreamAccountEpoch(rotated) {
		t.Fatal("credential rotation must not reset pricing history")
	}
}

func TestPricingLedgerTablesAreIncludedInNormalStoreMigration(t *testing.T) {
	m := newTestMonitor(t)
	for _, model := range []any{
		&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{},
		&ChannelUpstreamPricingObservedState{}, &ChannelUpstreamPricingChangeEvent{},
		&ChannelUpstreamPricingSyncState{}, &ChannelUpstreamPricingPageCheckpoint{},
	} {
		if !m.storeDB.Migrator().HasTable(model) {
			t.Fatalf("pricing ledger table missing for %T", model)
		}
	}
	var checkpointColumns []struct {
		Name string `gorm:"column:name"`
		PK   int    `gorm:"column:pk"`
	}
	if err := m.storeDB.Raw("PRAGMA table_info(channel_upstream_pricing_page_checkpoints)").Scan(&checkpointColumns).Error; err != nil {
		t.Fatal(err)
	}
	pk := map[string]bool{}
	for _, column := range checkpointColumns {
		if column.PK > 0 {
			pk[column.Name] = true
		}
	}
	for _, name := range []string{"domain", "account_epoch", "semantics_version", "hour_ts"} {
		if !pk[name] {
			t.Fatalf("pricing checkpoint primary key missing %s: %+v", name, pk)
		}
	}
}

func TestPricingLedgerCheckpointRejectsPathologicalDimensionCardinality(t *testing.T) {
	m := newTestMonitor(t)
	evidence := make(map[string]*ChannelUpstreamPricingHourEvidence, upstreamPricingMaxCheckpointDimensions+1)
	for i := 0; i <= upstreamPricingMaxCheckpointDimensions; i++ {
		key := fmt.Sprintf("%064x", i)
		evidence[key] = &ChannelUpstreamPricingHourEvidence{DimensionHash: key}
	}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{Domain: "4sapi.com", AccountEpoch: "epoch", SemanticsVersion: upstreamPricingSemanticsVersion, HourTs: 3600}
	if err := m.savePricingPageCheckpoint(context.Background(), &checkpoint, evidence); err == nil {
		t.Fatal("pathological checkpoint cardinality was accepted")
	}
	var rows int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingPageCheckpoint{}).Count(&rows).Error; err != nil || rows != 0 {
		t.Fatalf("rejected checkpoint was persisted: rows=%d err=%v", rows, err)
	}
}

func TestPricingLedgerCheckpointRejectsOversizeBeforeDecodeAndAfterMarshal(t *testing.T) {
	m := newTestMonitor(t)
	oversize := ChannelUpstreamPricingPageCheckpoint{AggregatesJSON: strings.Repeat("x", upstreamPricingMaxCheckpointBytes+1)}
	if _, err := decodePricingCheckpointEvidence(oversize); err == nil {
		t.Fatal("oversize restored checkpoint reached JSON decoding")
	}

	evidence := make(map[string]*ChannelUpstreamPricingHourEvidence, 2000)
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("%064x", i)
		evidence[key] = &ChannelUpstreamPricingHourEvidence{
			DimensionHash: key, SourceGroup: strings.Repeat("g", 191), ModelName: strings.Repeat("m", 191),
			GroupRatio: strings.Repeat("1", 80), UserGroupRatio: strings.Repeat("2", 80), EffectiveRatio: strings.Repeat("3", 80),
		}
	}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{Domain: "4sapi.com", AccountEpoch: "epoch", SemanticsVersion: upstreamPricingSemanticsVersion, HourTs: 3600}
	if err := m.savePricingPageCheckpoint(context.Background(), &checkpoint, evidence); err == nil {
		t.Fatal("checkpoint below dimension limit but above byte limit was accepted")
	}

	rows := make([]ChannelUpstreamPricingHourEvidence, upstreamPricingMaxCheckpointDimensions+1)
	encoded, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) > upstreamPricingMaxCheckpointBytes {
		t.Fatalf("row-count fixture unexpectedly hit byte gate first: %d", len(encoded))
	}
	if _, err := decodePricingCheckpointEvidence(ChannelUpstreamPricingPageCheckpoint{AggregatesJSON: string(encoded)}); err == nil {
		t.Fatal("restored checkpoint above dimension limit was accepted")
	}
}

func newPricingLedgerFixtureServer(t *testing.T, status int, calls *atomic.Int64) *httptest.Server {
	t.Helper()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.URL.Path != "/api/log/self" || r.Header.Get("Authorization") != "Bearer usage-token" || r.Header.Get("New-Api-User") != "31" {
			http.Error(w, `{"message":"bad auth"}`, http.StatusUnauthorized)
			return
		}
		if status != http.StatusOK {
			if status == http.StatusTooManyRequests {
				w.Header().Set("Retry-After", "120")
			}
			http.Error(w, `{"message":"fixture failure"}`, status)
			return
		}
		from, err := strconv.ParseInt(r.URL.Query().Get("start_timestamp"), 10, 64)
		if err != nil {
			http.Error(w, `{"message":"bad range"}`, http.StatusBadRequest)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true, "data": map[string]any{
			"total": 1,
			"items": []map[string]any{{
				"id": 1, "created_at": from + 1, "quota": 500000,
				"prompt_tokens": 2, "completion_tokens": 1,
				"model_name": "gpt-5.5", "group": "codex-1.2x", "token_id": 59,
				"other": map[string]any{"group_ratio": 1.2, "billing_mode": "token"},
			}},
		}})
	}))
	t.Cleanup(server.Close)
	return server
}

func seedPricingFailureIsolation(t *testing.T, m *Monitor, server *httptest.Server) (ChannelUpstreamAccount, ChannelUpstreamUsageHour, int64) {
	t.Helper()
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingBackfillHoursPerRun = 1
	domain := server.Listener.Addr().String()
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	now := time.Now().Unix()
	closedThrough := now - now%3600
	hour := closedThrough - 3600
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: closedThrough,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	legacy := ChannelUpstreamUsageHour{Domain: domain, HourTs: hour, Requests: 7, Tokens: 21, Quota: 3500000, CostUSD: 7}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	state := newPricingSyncState(row, now, 1)
	state.LastSuccessAt = now - 600
	state.BackfillDone = true
	state.BackfillNextHour = state.BackfillTargetHour
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	return row, legacy, state.LastSuccessAt
}

func assertPricingFailureIsIsolated(t *testing.T, m *Monitor, row ChannelUpstreamAccount, legacy ChannelUpstreamUsageHour, oldSuccess int64) {
	t.Helper()
	var accountAfter ChannelUpstreamAccount
	if err := m.storeDB.First(&accountAfter, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if accountAfter.UsageStatus != upstreamStatusOK || accountAfter.UsageDataUntil != row.UsageDataUntil {
		t.Fatalf("pricing failure polluted existing usage health: %+v", accountAfter)
	}
	var pricingAfter ChannelUpstreamPricingSyncState
	if err := m.storeDB.First(&pricingAfter, "domain = ? AND account_epoch = ?", row.Domain, newAPIUpstreamAccountEpoch(row)).Error; err != nil {
		t.Fatal(err)
	}
	if pricingAfter.LastSuccessAt != oldSuccess || pricingAfter.Status != "error" || pricingAfter.ConsecutiveFailures < 1 {
		t.Fatalf("pricing failure changed success watermark or was not persisted: %+v", pricingAfter)
	}
	var legacyAfter ChannelUpstreamUsageHour
	if err := m.storeDB.First(&legacyAfter, "domain = ? AND hour_ts = ?", row.Domain, legacy.HourTs).Error; err != nil {
		t.Fatal(err)
	}
	if legacyAfter.Requests != legacy.Requests || legacyAfter.Tokens != legacy.Tokens || legacyAfter.Quota != legacy.Quota || legacyAfter.CostUSD != legacy.CostUSD {
		t.Fatalf("pricing failure changed existing aggregate: before=%+v after=%+v", legacy, legacyAfter)
	}
	var evidenceRows int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingHourEvidence{}).Where("domain = ?", row.Domain).Count(&evidenceRows).Error; err != nil || evidenceRows != 0 {
		t.Fatalf("failed scan published partial evidence: rows=%d err=%v", evidenceRows, err)
	}
}

func TestPricingLedgerDisabledSchedulerHasZeroSideEffects(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamPricingLedgerEnabled = false
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL,
		UserID: 31, Enabled: true, UsageSyncEnabled: true,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	m.syncDueUpstreamPricing(context.Background())
	if calls.Load() != 0 {
		t.Fatalf("disabled pricing ledger contacted upstream %d times", calls.Load())
	}
	var states int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("disabled pricing ledger wrote sync state: rows=%d err=%v", states, err)
	}
}

func TestPricingLedgerSub2StatsAccountIsNotEligibleOrScheduled(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamPricingLedgerEnabled = true
	domain := server.Listener.Addr().String()
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderSub2API, BaseURL: server.URL,
		Enabled: true, UsageSyncEnabled: true, UsageAdapter: upstreamUsageAdapterSub2Stats,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	views, err := m.loadChannelUpstreamViews(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if view := views[domain]; view.PricingLedgerEligible || view.PricingLedgerStatus != "not_selected" {
		t.Fatalf("daily-only Sub2 account advertised as pricing eligible: %+v", view)
	}
	m.syncDueUpstreamPricing(context.Background())
	if calls.Load() != 0 {
		t.Fatalf("daily-only Sub2 account contacted upstream %d times", calls.Load())
	}
	var states int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Where("domain = ?", domain).Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("daily-only Sub2 account created pricing state: rows=%d err=%v", states, err)
	}
}

func TestSyncStoredNewAPIPricingVerifiesWithoutChangingLegacyUsage(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingBackfillHoursPerRun = 1
	domain := server.Listener.Addr().String()
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	now := time.Now().Unix()
	closedThrough := now - now%3600
	hour := closedThrough - 3600
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, Account: "31", UserID: 31,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: closedThrough,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	legacy := ChannelUpstreamUsageHour{Domain: domain, HourTs: hour, BucketSeconds: 3600, Requests: 1, Tokens: 3, Quota: 500000, CostUSD: 1, Provider: upstreamProviderNewAPI}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	syncState := newPricingSyncState(row, now, 1)
	syncState.BackfillDone = true
	syncState.BackfillNextHour = syncState.BackfillTargetHour
	syncState.BackfillNextSyncAt = 0
	if err := m.storeDB.Create(&syncState).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.syncStoredNewAPIPricing(context.Background(), domain); err != nil {
		t.Fatal(err)
	}
	var first ChannelUpstreamPricingHourState
	if err := m.storeDB.First(&first, "domain = ? AND account_epoch = ? AND hour_ts = ?", domain, newAPIUpstreamAccountEpoch(row), hour).Error; err != nil {
		t.Fatal(err)
	}
	if first.Status != "observed" || first.ReconcileStatus != "matched" {
		t.Fatalf("first worker scan=%+v", first)
	}
	var firstSyncState ChannelUpstreamPricingSyncState
	if err := m.storeDB.First(&firstSyncState, "domain = ? AND account_epoch = ?", domain, newAPIUpstreamAccountEpoch(row)).Error; err != nil {
		t.Fatal(err)
	}
	if firstSyncState.Status != "pending_verification" || firstSyncState.TailThroughHour != 0 || firstSyncState.LastSuccessAt == 0 {
		t.Fatalf("first scan was incorrectly presented as complete: %+v", firstSyncState)
	}
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).
		Where("domain = ? AND account_epoch = ?", domain, newAPIUpstreamAccountEpoch(row)).
		Update("tail_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.syncStoredNewAPIPricing(context.Background(), domain); err != nil {
		t.Fatal(err)
	}
	var second ChannelUpstreamPricingHourState
	if err := m.storeDB.First(&second, "domain = ? AND account_epoch = ? AND hour_ts = ?", domain, newAPIUpstreamAccountEpoch(row), hour).Error; err != nil {
		t.Fatal(err)
	}
	if second.Status != "verified" || second.ReconcileStatus != "matched" || second.VerifiedScans != 2 {
		t.Fatalf("second worker scan=%+v", second)
	}
	var observed int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingObservedState{}).Where("domain = ?", domain).Count(&observed).Error; err != nil || observed != 1 {
		t.Fatalf("verified current pricing state rows=%d err=%v", observed, err)
	}
	var unchanged ChannelUpstreamUsageHour
	if err := m.storeDB.First(&unchanged, "domain = ? AND hour_ts = ?", domain, hour).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.Requests != legacy.Requests || unchanged.Tokens != legacy.Tokens || unchanged.Quota != legacy.Quota || unchanged.CostUSD != legacy.CostUSD {
		t.Fatalf("shadow ledger changed legacy usage: before=%+v after=%+v", legacy, unchanged)
	}
	views, err := m.loadChannelUpstreamViews(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	view := views[domain]
	if !view.PricingLedgerWorkerEnabled || !view.PricingLedgerEligible || view.PricingLedgerStatus != "ok" ||
		view.PricingVerifiedHours != 1 || view.PricingPendingHours != 0 || view.PricingMismatchHours != 0 ||
		view.PricingBackfillTotalHours != 24 || !view.PricingBackfillDone {
		t.Fatalf("pricing sync status view=%+v", view)
	}
	if calls.Load() != 2 {
		t.Fatalf("two single-page verification scans made %d requests", calls.Load())
	}
}

func TestPricingLedger429PreservesExistingUsageHealthAndAggregate(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusTooManyRequests, &calls)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingBackfillHoursPerRun = 1
	domain := server.Listener.Addr().String()
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	now := time.Now().Unix()
	closedThrough := now - now%3600
	hour := closedThrough - 3600
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: closedThrough,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	legacy := ChannelUpstreamUsageHour{Domain: domain, HourTs: hour, Requests: 7, Tokens: 21, Quota: 3500000, CostUSD: 7}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	state := newPricingSyncState(row, now, 1)
	state.LastSuccessAt = now - 600
	state.BackfillDone = true
	state.BackfillNextHour = state.BackfillTargetHour
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.syncStoredNewAPIPricing(context.Background(), domain); err == nil {
		t.Fatal("429 pricing scan unexpectedly succeeded")
	}
	var accountAfter ChannelUpstreamAccount
	if err := m.storeDB.First(&accountAfter, "domain = ?", domain).Error; err != nil {
		t.Fatal(err)
	}
	if accountAfter.UsageStatus != upstreamStatusOK || accountAfter.UsageDataUntil != closedThrough {
		t.Fatalf("pricing failure polluted existing usage health: %+v", accountAfter)
	}
	var pricingAfter ChannelUpstreamPricingSyncState
	if err := m.storeDB.First(&pricingAfter, "domain = ? AND account_epoch = ?", domain, newAPIUpstreamAccountEpoch(row)).Error; err != nil {
		t.Fatal(err)
	}
	if pricingAfter.LastSuccessAt != now-600 || pricingAfter.Status != "error" || pricingAfter.TailNextSyncAt < now+120 {
		t.Fatalf("failed request changed success watermark or ignored Retry-After: %+v", pricingAfter)
	}
	var legacyAfter ChannelUpstreamUsageHour
	if err := m.storeDB.First(&legacyAfter, "domain = ? AND hour_ts = ?", domain, hour).Error; err != nil {
		t.Fatal(err)
	}
	if legacyAfter.Requests != legacy.Requests || legacyAfter.Quota != legacy.Quota {
		t.Fatalf("pricing failure changed existing aggregate: %+v", legacyAfter)
	}
	var evidenceRows int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingHourEvidence{}).Where("domain = ?", domain).Count(&evidenceRows).Error; err != nil || evidenceRows != 0 {
		t.Fatalf("failed scan published partial evidence: rows=%d err=%v", evidenceRows, err)
	}
}

func TestPricingLedger401PreservesExistingUsageHealthAndAggregate(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusUnauthorized, &calls)
	m := newChannelUpstreamTestMonitor(t)
	row, legacy, oldSuccess := seedPricingFailureIsolation(t, m, server)
	if _, err := m.syncStoredNewAPIPricing(context.Background(), row.Domain); err == nil {
		t.Fatal("401 pricing scan unexpectedly succeeded")
	}
	assertPricingFailureIsIsolated(t, m, row, legacy, oldSuccess)
	if calls.Load() != 1 {
		t.Fatalf("401 scan made %d requests, want 1", calls.Load())
	}
}

func TestPricingLedgerTimeoutPreservesExistingUsageHealthAndAggregate(t *testing.T) {
	var calls atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		select {
		case <-r.Context().Done():
			return
		case <-time.After(500 * time.Millisecond):
			http.Error(w, `{"message":"unexpected late response"}`, http.StatusGatewayTimeout)
		}
	}))
	t.Cleanup(server.Close)
	m := newChannelUpstreamTestMonitor(t)
	guard := newUpstreamHostGuard(m.storeDB, upstreamHostGuardOptions{
		Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0,
	})
	m.upstreamClient = installUpstreamHostGuardForTest(newUpstreamHTTPClient(25*time.Millisecond), m.storeDB, guard)
	row, legacy, oldSuccess := seedPricingFailureIsolation(t, m, server)
	if _, err := m.syncStoredNewAPIPricing(context.Background(), row.Domain); err == nil {
		t.Fatal("timed-out pricing scan unexpectedly succeeded")
	}
	assertPricingFailureIsIsolated(t, m, row, legacy, oldSuccess)
	if calls.Load() != 1 {
		t.Fatalf("timeout scan made %d requests, want 1", calls.Load())
	}
}

func TestPricingLedgerMismatchBlocksCursorAndCurrentRate(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	row, legacy, _ := seedPricingFailureIsolation(t, m, server)
	// The fixture emits quota=500000, while this immutable legacy control total
	// deliberately differs. Two identical source reads must still not publish a
	// current rate or advance the verified tail cursor.
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).
		Where("domain = ? AND hour_ts = ?", row.Domain, legacy.HourTs).
		Update("quota", 600000).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.syncStoredNewAPIPricing(context.Background(), row.Domain); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).
		Where("domain = ? AND account_epoch = ?", row.Domain, newAPIUpstreamAccountEpoch(row)).
		Update("tail_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	beforeSecond := time.Now().Unix()
	if _, err := m.syncStoredNewAPIPricing(context.Background(), row.Domain); err != nil {
		t.Fatal(err)
	}
	var state ChannelUpstreamPricingSyncState
	if err := m.storeDB.First(&state, "domain = ? AND account_epoch = ?", row.Domain, newAPIUpstreamAccountEpoch(row)).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "reconcile_blocked" || state.TailThroughHour != 0 || state.MismatchHours != 1 || state.TailNextSyncAt < beforeSecond+pricingSyncIntervalSeconds(m.cfg)-1 {
		t.Fatalf("mismatch did not block cursor with normal backoff: %+v", state)
	}
	var currentRows int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingObservedState{}).Where("domain = ?", row.Domain).Count(&currentRows).Error; err != nil || currentRows != 0 {
		t.Fatalf("mismatched evidence published current rate: rows=%d err=%v", currentRows, err)
	}
	var unchanged ChannelUpstreamUsageHour
	if err := m.storeDB.First(&unchanged, "domain = ? AND hour_ts = ?", row.Domain, legacy.HourTs).Error; err != nil {
		t.Fatal(err)
	}
	if unchanged.Quota != 600000 {
		t.Fatalf("shadow mismatch changed legacy quota: %+v", unchanged)
	}
}

func TestPricingLedgerCorruptCheckpointIsDiscardedAndRecovers(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	hour := time.Now().Unix()
	hour = hour - hour%3600 - 3600
	account := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	first, err := fetchNewAPIPricingPage(context.Background(), m.channelUpstreamHTTPClient(), account, newAPICredential{AccessToken: "usage-token"}, hour, hour+3600, 1, newUpstreamUsageRequestPacer(5, 0))
	if err != nil {
		t.Fatal(err)
	}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion,
		HourTs: hour, NextPage: 2, Total: 1, SourceRows: 1,
		FirstPageFingerprint: fmt.Sprintf("%x", first.Fingerprint), AggregatesJSON: "{broken",
	}
	if err := m.storeDB.Create(&checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	if _, _, _, _, err := m.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(5, 0), time.Now().Unix()); err == nil {
		t.Fatal("corrupt checkpoint unexpectedly succeeded")
	}
	var checkpoints int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingPageCheckpoint{}).Where("domain = ?", account.Domain).Count(&checkpoints).Error; err != nil || checkpoints != 0 {
		t.Fatalf("corrupt checkpoint was not discarded: rows=%d err=%v", checkpoints, err)
	}
	evidence, state, _, complete, err := m.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(5, 0), time.Now().Unix())
	if err != nil || !complete || len(evidence) != 1 || state.SourceRows != 1 {
		t.Fatalf("fresh scan did not recover after corrupt checkpoint: complete=%v evidence=%d state=%+v err=%v", complete, len(evidence), state, err)
	}
}

func TestValidatePricingCheckpointEvidenceRejectsEnvelopeHashAndCounterCorruption(t *testing.T) {
	hour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 31}
	item, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":1787623201,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"group":"codex","model_name":"gpt-5.5","other":{"group_ratio":1.2}}`))
	if err != nil {
		t.Fatal(err)
	}
	evidence, _, err := buildNewAPIPricingHour(account, []newAPIPricingUsageItem{item}, hour, hour+4000)
	if err != nil || len(evidence) != 1 {
		t.Fatalf("build evidence len=%d err=%v", len(evidence), err)
	}
	checkpoint := ChannelUpstreamPricingPageCheckpoint{
		Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion,
		HourTs: hour, NextPage: 2, Total: 1, SourceRows: 1,
	}
	if err := validatePricingCheckpointEvidenceRow(checkpoint, evidence[0]); err != nil {
		t.Fatalf("valid checkpoint evidence rejected: %v", err)
	}
	tests := map[string]func(*ChannelUpstreamPricingHourEvidence){
		"domain":    func(row *ChannelUpstreamPricingHourEvidence) { row.Domain = "other.example" },
		"epoch":     func(row *ChannelUpstreamPricingHourEvidence) { row.AccountEpoch = "wrong" },
		"hour":      func(row *ChannelUpstreamPricingHourEvidence) { row.HourTs += 3600 },
		"hash":      func(row *ChannelUpstreamPricingHourEvidence) { row.DimensionHash = strings.Repeat("0", 64) },
		"counter":   func(row *ChannelUpstreamPricingHourEvidence) { row.SourceRows++ },
		"timestamp": func(row *ChannelUpstreamPricingHourEvidence) { row.LastSourceAt = row.HourTs + 3600 },
	}
	for name, mutate := range tests {
		t.Run(name, func(t *testing.T) {
			row := evidence[0]
			mutate(&row)
			if err := validatePricingCheckpointEvidenceRow(checkpoint, row); err == nil {
				t.Fatal("corrupt checkpoint evidence accepted")
			}
		})
	}
}

func TestPricingLedgerHistoricalMismatchDoesNotAdvanceBackfillCursor(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingBackfillHoursPerRun = 1
	domain := server.Listener.Addr().String()
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	now := time.Now().Unix()
	closedThrough := now - now%3600
	hour := closedThrough - 12*3600
	row := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: closedThrough,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: domain, HourTs: hour, Requests: 1, Tokens: 3, Quota: 600000}).Error; err != nil {
		t.Fatal(err)
	}
	state := newPricingSyncState(row, now, 1)
	state.BackfillStartHour, state.BackfillNextHour, state.BackfillTargetHour = hour, hour, hour+3600
	state.TailNextSyncAt = now + 3600
	state.BackfillNextSyncAt = 0
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.syncStoredNewAPIPricing(context.Background(), domain); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).
		Where("domain = ? AND account_epoch = ?", domain, newAPIUpstreamAccountEpoch(row)).
		Update("backfill_next_sync_at", 0).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.syncStoredNewAPIPricing(context.Background(), domain); err != nil {
		t.Fatal(err)
	}
	var after ChannelUpstreamPricingSyncState
	if err := m.storeDB.First(&after, "domain = ? AND account_epoch = ?", domain, newAPIUpstreamAccountEpoch(row)).Error; err != nil {
		t.Fatal(err)
	}
	if after.Status != "reconcile_blocked" || after.BackfillNextHour != hour || after.BackfillDone || after.MismatchHours != 1 {
		t.Fatalf("historical mismatch advanced backfill cursor: %+v", after)
	}
}

func TestLegacyNewAPIUsageDecoderIgnoresPricingPayload(t *testing.T) {
	item, err := decodeLegacyNewAPIUsageItem(json.RawMessage(`{"created_at":1787623201,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"other":"not-json","group":"oversized-pricing-dimension"}`))
	if err != nil {
		t.Fatal(err)
	}
	if item.CreatedAt != 1787623201 || item.Quota != 500000 || item.PromptTokens != 2 || item.CompletionTokens != 1 {
		t.Fatalf("legacy decoder result changed: %+v", item)
	}
	if fields := reflect.TypeOf(item).NumField(); fields != 4 {
		t.Fatalf("stable usage DTO grew shadow-ledger fields: fields=%d", fields)
	}
}

func TestPricingLedgerBackfillTargetExtendsAfterLongDowntime(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageSyncEnabled = true
	m.cfg.UpstreamPricingLedgerEnabled = true
	m.cfg.UpstreamPricingBackfillHoursPerRun = 1
	domain := server.Listener.Addr().String()
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	now := time.Now().Unix()
	closedThrough := now - now%3600
	firstMissing := closedThrough - 10*3600
	account := ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: closedThrough,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &account, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{
		Domain: domain, HourTs: firstMissing, BucketSeconds: 3600, Provider: upstreamProviderNewAPI,
		Requests: 1, Tokens: 3, Quota: 500000,
	}).Error; err != nil {
		t.Fatal(err)
	}
	state := newPricingSyncState(account, now, 1)
	state.BackfillStartHour = firstMissing
	state.BackfillNextHour = firstMissing
	state.BackfillTargetHour = firstMissing
	state.BackfillDone = true
	state.TailNextSyncAt = now + 3600
	state.BackfillNextSyncAt = 0
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	first, err := m.syncStoredNewAPIPricing(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if first.BackfillTargetHour != closedThrough || first.BackfillDone || first.BackfillNextHour != firstMissing || first.Status != "pending_verification" {
		t.Fatalf("long-downtime gap was not reopened: %+v", first)
	}
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).
		Where("domain = ? AND account_epoch = ?", domain, newAPIUpstreamAccountEpoch(account)).
		Updates(map[string]any{"backfill_next_sync_at": 0, "tail_next_sync_at": now + 3600}).Error; err != nil {
		t.Fatal(err)
	}
	second, err := m.syncStoredNewAPIPricing(context.Background(), domain)
	if err != nil {
		t.Fatal(err)
	}
	if second.BackfillNextHour != firstMissing+3600 || second.BackfillTargetHour != closedThrough || second.BackfillDone {
		t.Fatalf("continuous backfill did not advance into the old gap: %+v", second)
	}
}

func TestPricingLedgerTailHourCannotDeleteHistoricalCheckpoint(t *testing.T) {
	now := time.Now().Unix()
	closedThrough := now - now%3600
	historicalHour := closedThrough - 12*3600
	tailHour := closedThrough - 3600
	rows := usageFixtureRows(2500, historicalHour, 3600)
	rows = append(rows, upstreamUsageFixtureRow{ID: 3000, CreatedAt: tailHour + 1})
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	account := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	credential := newAPICredential{AccessToken: "usage-token"}

	_, _, progress, complete, err := m.fetchNewAPIPricingHour(context.Background(), account, credential, historicalHour, newUpstreamUsageRequestPacer(20, 0), now)
	if err != nil || complete || progress == "" {
		t.Fatalf("historical checkpoint setup complete=%v progress=%q err=%v", complete, progress, err)
	}
	var before ChannelUpstreamPricingPageCheckpoint
	if err := m.storeDB.First(&before, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), historicalHour).Error; err != nil {
		t.Fatal(err)
	}
	tailEvidence, tailState, _, complete, err := m.fetchNewAPIPricingHour(context.Background(), account, credential, tailHour, newUpstreamUsageRequestPacer(20, 0), now)
	if err != nil || !complete {
		t.Fatalf("tail hour scan complete=%v err=%v", complete, err)
	}
	if err := m.persistNewAPIPricingHour(context.Background(), account, tailHour, tailEvidence, tailState, now); err != nil {
		t.Fatal(err)
	}
	var after ChannelUpstreamPricingPageCheckpoint
	if err := m.storeDB.First(&after, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), historicalHour).Error; err != nil {
		t.Fatal(err)
	}
	if after.NextPage != before.NextPage || after.SourceRows != before.SourceRows || after.Total != before.Total || after.FirstPageFingerprint != before.FirstPageFingerprint {
		t.Fatalf("tail hour changed historical checkpoint: before=%+v after=%+v", before, after)
	}
}

func TestPricingLedgerYieldsToSameAccountCredentialOperation(t *testing.T) {
	var calls atomic.Int64
	server := newPricingLedgerFixtureServer(t, http.StatusOK, &calls)
	m := newChannelUpstreamTestMonitor(t)
	row, _, _ := seedPricingFailureIsolation(t, m, server)
	release, err := m.acquireUpstreamAccountAdmin(context.Background(), row.Domain)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	done := make(chan error, 1)
	go func() {
		_, err := m.syncStoredUpstreamPricing(context.Background(), row.Domain)
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, errUpstreamAccountBusy) {
			t.Fatalf("pricing worker did not yield to same-account credential operation: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("pricing shadow worker blocked instead of yielding immediately")
	}
}

func openRestartablePricingTestMonitor(t *testing.T, path string) *Monitor {
	t.Helper()
	m := &Monitor{cfg: Settings{SessionSecret: "pricing-restart-secret", UpstreamSyncTimeoutSec: 3}}
	if err := m.openStore(path); err != nil {
		t.Fatal(err)
	}
	m.upstreamCredentialPersistent = true
	guard := newUpstreamHostGuard(m.storeDB, upstreamHostGuardOptions{
		Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0,
	})
	m.upstreamClient = installUpstreamHostGuardForTest(newUpstreamHTTPClient(3*time.Second), m.storeDB, guard)
	return m
}

func richPricingFixtureRows(count int, from, span int64) []upstreamUsageFixtureRow {
	rows := usageFixtureRows(count, from, span)
	for i := range rows {
		rows[i].Group = []string{"codex-1.2x", "codex-1.4x", "claude-0.5x", "missing-rate", "invalid-rate"}[i%5]
		rows[i].ModelName = []string{"gpt-5.5", "gpt-5.4", "claude-sonnet-4", "unknown-model", "broken-model"}[i%5]
		rows[i].TokenID = int64(70 + i%5)
		switch i % 5 {
		case 0:
			rows[i].Other = map[string]any{"group_ratio": 1.2, "billing_mode": "token"}
		case 1:
			rows[i].Other = map[string]any{"group_ratio": "1.4000", "billing_mode": "token"}
		case 2:
			rows[i].Other = `{"group_ratio":0.5,"user_group_ratio":0.4,"billing_mode":"token"}`
		case 3:
			rows[i].Other = "{broken"
		case 4:
			// Deliberately omit other: missing evidence is retained as an
			// explicit audit state rather than silently assigned a rate.
		}
	}
	return rows
}

func TestPricingLedgerDenseHourResumesCheckpointAcrossRestart(t *testing.T) {
	hour := time.Now().Unix()
	hour = hour - hour%3600 - 3600
	rows := richPricingFixtureRows(6500, hour, 3600)
	server, calls := newUpstreamUsageFixtureServer(t, rows, nil)
	dbPath := t.TempDir() + "/pricing-restart.db"
	account := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, Account: "31", UserID: 31, BalanceUnit: 500000,
	}
	legacy := ChannelUpstreamUsageHour{
		Domain: account.Domain, HourTs: hour, BucketSeconds: 3600, Provider: upstreamProviderNewAPI,
		Requests: 6500, Tokens: 19500, Quota: 3250000000, CostUSD: 6500,
	}

	m1 := openRestartablePricingTestMonitor(t, dbPath)
	if err := m1.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	_, _, progress, complete, err := m1.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(60, 0), time.Now().Unix())
	if err != nil || complete || progress == "" {
		t.Fatalf("first dense pass complete=%v progress=%q err=%v", complete, progress, err)
	}
	var checkpoint ChannelUpstreamPricingPageCheckpoint
	if err := m1.storeDB.First(&checkpoint, "domain = ? AND account_epoch = ?", account.Domain, newAPIUpstreamAccountEpoch(account)).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextPage != 61 || checkpoint.SourceRows != 6000 || checkpoint.Total != 6500 {
		t.Fatalf("durable pricing checkpoint=%+v", checkpoint)
	}
	var premature int64
	if err := m1.storeDB.Model(&ChannelUpstreamPricingHourEvidence{}).Where("domain = ?", account.Domain).Count(&premature).Error; err != nil || premature != 0 {
		t.Fatalf("partial dense scan became public: rows=%d err=%v", premature, err)
	}
	m1.Close()

	m2 := openRestartablePricingTestMonitor(t, dbPath)
	evidence, state, progress, complete, err := m2.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(60, 0), time.Now().Unix())
	if err != nil || !complete || progress != "" {
		t.Fatalf("restart resume complete=%v progress=%q err=%v", complete, progress, err)
	}
	if state.SourceRows != 6500 || state.EligibleRequests != 6500 || state.Tokens != 19500 || state.FinalQuota != 3250000000 {
		t.Fatalf("resumed dense state=%+v", state)
	}
	if len(evidence) < 5 {
		t.Fatalf("rich pricing dimensions collapsed unexpectedly: %d", len(evidence))
	}
	var conserved int64
	for _, row := range evidence {
		conserved += row.FinalQuota
	}
	if conserved != 3250000000 {
		t.Fatalf("rich pricing quota not conserved: %d", conserved)
	}
	if err := m2.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	if err := m2.storeDB.Model(&ChannelUpstreamPricingPageCheckpoint{}).Where("domain = ?", account.Domain).Count(&premature).Error; err != nil || premature != 0 {
		t.Fatalf("completed checkpoint not cleared: rows=%d err=%v", premature, err)
	}

	// A second complete scan still follows the same bounded two-turn path. Only
	// after it matches the first scan may the hour become verified/current.
	_, _, progress, complete, err = m2.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(60, 0), time.Now().Unix())
	if err != nil || complete || progress == "" {
		t.Fatalf("second verification first turn complete=%v progress=%q err=%v", complete, progress, err)
	}
	evidence, state, progress, complete, err = m2.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(60, 0), time.Now().Unix())
	if err != nil || !complete || progress != "" {
		t.Fatalf("second verification resume complete=%v progress=%q err=%v", complete, progress, err)
	}
	if err := m2.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	var verified ChannelUpstreamPricingHourState
	if err := m2.storeDB.First(&verified, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hour).Error; err != nil {
		t.Fatal(err)
	}
	if verified.Status != "verified" || verified.ReconcileStatus != "matched" || verified.VerifiedScans != 2 {
		t.Fatalf("dense hour not verified after two complete scans: %+v", verified)
	}
	if calls.Load() != 132 {
		t.Fatalf("dense checkpoint requests=%d, want 132 for two bounded complete scans", calls.Load())
	}
	m2.Close()
}

func TestPricingLedgerProductionWorkerBudgetResumesAcrossRestart(t *testing.T) {
	hour := time.Now().Unix()
	hour = hour - hour%3600 - 3600
	rows := richPricingFixtureRows(6500, hour, 3600)
	server, calls := newUpstreamUsageFixtureServer(t, rows, nil)
	dbPath := t.TempDir() + "/pricing-worker-restart.db"
	account := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, Account: "31", UserID: 31, BalanceUnit: 500000,
		Enabled: true, UsageSyncEnabled: true, UsageStatus: upstreamStatusOK, UsageDataUntil: hour + 3600,
	}

	m1 := openRestartablePricingTestMonitor(t, dbPath)
	m1.cfg.UpstreamUsageSyncEnabled = true
	m1.cfg.UpstreamPricingLedgerEnabled = true
	m1.cfg.UpstreamPricingLedgerDomains = []string{account.Domain}
	m1.cfg.UpstreamPricingBackfillHoursPerRun = 1
	if err := m1.persistSyncedUpstreamAccount(context.Background(), &account, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	if err := m1.storeDB.Create(&ChannelUpstreamUsageHour{
		Domain: account.Domain, HourTs: hour, BucketSeconds: 3600, Provider: upstreamProviderNewAPI,
		Requests: 6500, Tokens: 19500, Quota: 3250000000, CostUSD: 6500,
	}).Error; err != nil {
		t.Fatal(err)
	}
	syncState := newPricingSyncState(account, time.Now().Unix(), 1)
	syncState.BackfillDone = true
	syncState.BackfillNextHour = syncState.BackfillTargetHour
	if err := m1.storeDB.Create(&syncState).Error; err != nil {
		t.Fatal(err)
	}
	first, err := m1.syncStoredNewAPIPricing(context.Background(), account.Domain)
	if err != nil || first.Status != "paging" || first.Progress == "" {
		t.Fatalf("first production-budget turn state=%+v err=%v", first, err)
	}
	var checkpoint ChannelUpstreamPricingPageCheckpoint
	if err := m1.storeDB.First(&checkpoint, "domain = ? AND account_epoch = ?", account.Domain, newAPIUpstreamAccountEpoch(account)).Error; err != nil {
		t.Fatal(err)
	}
	if checkpoint.NextPage != upstreamPricingMaxRequestsPerRun+1 || checkpoint.SourceRows != int64(upstreamPricingMaxRequestsPerRun*upstreamUsagePageSize) {
		t.Fatalf("production budget checkpoint=%+v", checkpoint)
	}
	m1.Close()

	m2 := openRestartablePricingTestMonitor(t, dbPath)
	m2.cfg.UpstreamUsageSyncEnabled = true
	m2.cfg.UpstreamPricingLedgerEnabled = true
	m2.cfg.UpstreamPricingLedgerDomains = []string{account.Domain}
	m2.cfg.UpstreamPricingBackfillHoursPerRun = 1
	forceDue := func() {
		if err := m2.storeDB.Model(&ChannelUpstreamPricingSyncState{}).
			Where("domain = ? AND account_epoch = ?", account.Domain, newAPIUpstreamAccountEpoch(account)).
			Update("tail_next_sync_at", 0).Error; err != nil {
			t.Fatal(err)
		}
	}
	var final ChannelUpstreamPricingSyncState
	for turn := 0; turn < 12; turn++ {
		forceDue()
		final, err = m2.syncStoredNewAPIPricing(context.Background(), account.Domain)
		if err != nil {
			t.Fatal(err)
		}
		if final.Status == "ok" {
			break
		}
		if final.Status != "paging" && final.Status != "pending_verification" {
			t.Fatalf("unexpected production worker state after turn %d: %+v", turn+2, final)
		}
	}
	if final.Status != "ok" || final.TailThroughHour != hour+3600 || final.VerifiedHours != 1 || final.PendingHours != 0 || final.MismatchHours != 0 {
		t.Fatalf("production worker did not finish verified hour: %+v", final)
	}
	var observed int64
	// All five dimensions remain in the immutable hourly evidence. Only the
	// three dimensions with valid, unambiguous rates may become current state;
	// the broken/missing `other` fixtures must fail closed.
	if err := m2.storeDB.Model(&ChannelUpstreamPricingObservedState{}).Where("domain = ?", account.Domain).Count(&observed).Error; err != nil || observed != 3 {
		t.Fatalf("production worker current state rows=%d err=%v", observed, err)
	}
	var evidenceDimensions int64
	if err := m2.storeDB.Model(&ChannelUpstreamPricingHourEvidence{}).Where("domain = ?", account.Domain).Count(&evidenceDimensions).Error; err != nil || evidenceDimensions != 5 {
		t.Fatalf("production worker evidence dimensions=%d err=%v", evidenceDimensions, err)
	}
	var remainingCheckpoints int64
	if err := m2.storeDB.Model(&ChannelUpstreamPricingPageCheckpoint{}).Where("domain = ?", account.Domain).Count(&remainingCheckpoints).Error; err != nil || remainingCheckpoints != 0 {
		t.Fatalf("production worker left checkpoint rows=%d err=%v", remainingCheckpoints, err)
	}
	if calls.Load() < 130 || calls.Load() > 140 {
		t.Fatalf("production worker request count=%d, expected bounded resumed scans", calls.Load())
	}
	m2.Close()
}

func TestPricingLedgerDenseCheckpointContinuesAfterPairedStoreRestore(t *testing.T) {
	hour := time.Now().Unix()
	hour = hour - hour%3600 - 3600
	rows := richPricingFixtureRows(6500, hour, 3600)
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	dir := t.TempDir()
	mainPath, factsPath := filepath.Join(dir, "main.db"), filepath.Join(dir, "facts.db")
	m1 := &Monitor{cfg: Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath,
		StoreBackupEnabled: true, StoreBackupDir: filepath.Join(dir, "backups"), StoreBackupRetention: 2,
		SessionSecret: "pricing-paired-restore-secret",
	}}
	if err := m1.openStore(mainPath); err != nil {
		t.Fatal(err)
	}
	guard := newUpstreamHostGuard(m1.storeDB, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
	m1.upstreamClient = installUpstreamHostGuardForTest(newUpstreamHTTPClient(3*time.Second), m1.storeDB, guard)
	account := ChannelUpstreamAccount{Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	legacy := ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: hour, Requests: 6500, Tokens: 19500, Quota: 3250000000}
	if err := m1.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	_, _, progress, complete, err := m1.fetchNewAPIPricingHour(context.Background(), account, newAPICredential{AccessToken: "usage-token"}, hour, newUpstreamUsageRequestPacer(20, 0), time.Now().Unix())
	if err != nil || complete || progress == "" {
		t.Fatalf("pre-backup dense checkpoint complete=%v progress=%q err=%v", complete, progress, err)
	}
	manifestPath, err := m1.createStoreBackupSet(context.Background(), time.Now(), true, true)
	if err != nil {
		t.Fatal(err)
	}
	m1.Close()

	restoreDir := filepath.Join(dir, "restored")
	if err := RestoreStoreBackupSet(context.Background(), manifestPath, restoreDir, "main.db", "facts.db"); err != nil {
		t.Fatal(err)
	}
	restoredDB, err := gorm.Open(sqlite.Open(filepath.Join(restoreDir, "main.db")), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	m2 := &Monitor{storeDB: restoredDB, cfg: Settings{UpstreamSyncTimeoutSec: 3}}
	restoredGuard := newUpstreamHostGuard(restoredDB, upstreamHostGuardOptions{Clock: realUpstreamGuardClock{}, Jitter: func() time.Duration { return 0 }, MinInterval: 0})
	m2.upstreamClient = installUpstreamHostGuardForTest(newUpstreamHTTPClient(3*time.Second), restoredDB, restoredGuard)
	credential := newAPICredential{AccessToken: "usage-token"}
	finishScan := func() ([]ChannelUpstreamPricingHourEvidence, ChannelUpstreamPricingHourState) {
		t.Helper()
		for turn := 0; turn < 8; turn++ {
			evidence, state, _, done, fetchErr := m2.fetchNewAPIPricingHour(context.Background(), account, credential, hour, newUpstreamUsageRequestPacer(20, 0), time.Now().Unix())
			if fetchErr != nil {
				t.Fatal(fetchErr)
			}
			if done {
				return evidence, state
			}
		}
		t.Fatal("restored checkpoint did not finish within bounded turns")
		return nil, ChannelUpstreamPricingHourState{}
	}
	firstEvidence, firstState := finishScan()
	if firstState.SourceRows != 6500 || firstState.FinalQuota != 3250000000 {
		t.Fatalf("restored scan totals=%+v", firstState)
	}
	if err := m2.persistNewAPIPricingHour(context.Background(), account, hour, firstEvidence, firstState, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	secondEvidence, secondState := finishScan()
	if err := m2.persistNewAPIPricingHour(context.Background(), account, hour, secondEvidence, secondState, time.Now().Unix()); err != nil {
		t.Fatal(err)
	}
	var verified ChannelUpstreamPricingHourState
	if err := restoredDB.First(&verified, "domain = ? AND account_epoch = ? AND hour_ts = ?", account.Domain, newAPIUpstreamAccountEpoch(account), hour).Error; err != nil {
		t.Fatal(err)
	}
	if verified.Status != "verified" || verified.ReconcileStatus != "matched" || verified.VerifiedScans != 2 {
		t.Fatalf("restored checkpoint did not publish a verified hour: %+v", verified)
	}
	if sqlDB, err := restoredDB.DB(); err == nil {
		_ = sqlDB.Close()
	}
}

func TestPricingLedgerAmbiguousHourDoesNotOverwriteConfirmedCurrentRate(t *testing.T) {
	m := newTestMonitor(t)
	baseHour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 31}
	makeItem := func(hour int64, ratio string) newAPIPricingUsageItem {
		item, err := decodeNewAPIUsageItem(json.RawMessage(`{"created_at":` + strconv.FormatInt(hour+1, 10) + `,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"group":"codex","model_name":"gpt-5.5","other":{"group_ratio":` + ratio + `}}`))
		if err != nil {
			t.Fatal(err)
		}
		return item
	}
	firstItems := []newAPIPricingUsageItem{makeItem(baseHour, "1.2")}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: baseHour, Requests: 1, Tokens: 3, Quota: 500000}).Error; err != nil {
		t.Fatal(err)
	}
	evidence, state, err := buildNewAPIPricingHour(account, firstItems, baseHour, baseHour+4000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.persistNewAPIPricingHour(context.Background(), account, baseHour, evidence, state, baseHour+4000+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	var before ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&before, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	secondHour := baseHour + 3600
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: secondHour, Requests: 2, Tokens: 6, Quota: 1000000}).Error; err != nil {
		t.Fatal(err)
	}
	ambiguousItems := []newAPIPricingUsageItem{makeItem(secondHour, "1.2"), makeItem(secondHour, "1.3")}
	evidence, state, err = buildNewAPIPricingHour(account, ambiguousItems, secondHour, secondHour+4000)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.persistNewAPIPricingHour(context.Background(), account, secondHour, evidence, state, secondHour+4000+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	var after ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&after, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if after.StateHash != before.StateHash || after.EffectiveRatio != "1.2" || after.LastObservedHour != before.LastObservedHour {
		t.Fatalf("ambiguous evidence overwrote confirmed current state: before=%+v after=%+v", before, after)
	}
	var events int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingChangeEvent{}).Where("domain = ?", account.Domain).Count(&events).Error; err != nil || events != 0 {
		t.Fatalf("ambiguous evidence emitted a false change event: rows=%d err=%v", events, err)
	}
}

func TestPricingLedgerUnknownRatioDoesNotPublishOrOverwriteCurrentRate(t *testing.T) {
	m := newTestMonitor(t)
	baseHour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 31}
	buildAndVerify := func(hour int64, raws ...string) {
		t.Helper()
		items := make([]newAPIPricingUsageItem, 0, len(raws))
		var quota, tokens int64
		for _, raw := range raws {
			item, err := decodeNewAPIUsageItem(json.RawMessage(raw))
			if err != nil {
				t.Fatal(err)
			}
			items = append(items, item)
			if item.QuotaExact > 0 {
				quota += item.QuotaExact
				tokens += item.PromptTokens + item.CompletionTokens
			}
		}
		if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: hour, Requests: int64(len(items)), Tokens: tokens, Quota: float64(quota)}).Error; err != nil {
			t.Fatal(err)
		}
		evidence, state, err := buildNewAPIPricingHour(account, items, hour, hour+4000)
		if err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 2; i++ {
			if err := m.persistNewAPIPricingHour(context.Background(), account, hour, evidence, state, hour+4000+int64(i)); err != nil {
				t.Fatal(err)
			}
		}
	}

	buildAndVerify(baseHour,
		`{"created_at":1787623201,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"group":"codex","model_name":"gpt-5.5","other":{}}`)
	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingObservedState{}).Where("domain = ?", account.Domain).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("unknown-only evidence published a current rate: rows=%d err=%v", count, err)
	}

	validHour := baseHour + 3600
	buildAndVerify(validHour,
		`{"created_at":1787626801,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"group":"codex","model_name":"gpt-5.5","other":{"group_ratio":1.2}}`)
	var confirmed ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&confirmed, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if confirmed.EffectiveRatio != "1.2" {
		t.Fatalf("confirmed rate=%+v", confirmed)
	}

	mixedHour := validHour + 3600
	buildAndVerify(mixedHour,
		`{"created_at":1787630401,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"group":"codex","model_name":"gpt-5.5","other":{"group_ratio":1.3}}`,
		`{"created_at":1787630402,"quota":500000,"prompt_tokens":2,"completion_tokens":1,"group":"codex","model_name":"gpt-5.5","other":{}}`)
	var after ChannelUpstreamPricingObservedState
	if err := m.storeDB.First(&after, "domain = ?", account.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if after.StateHash != confirmed.StateHash || after.EffectiveRatio != "1.2" || after.LastObservedHour != confirmed.LastObservedHour {
		t.Fatalf("mixed valid/unknown evidence overwrote confirmed rate: before=%+v after=%+v", confirmed, after)
	}
}

func TestPricingLedgerDueOrderingPreventsDomainStarvation(t *testing.T) {
	candidates := []upstreamPricingDueAccount{
		{Domain: "a.example", NextDueAt: 100, LastAttemptAt: 900},
		{Domain: "b.example", NextDueAt: 100, LastAttemptAt: 100},
		{Domain: "c.example", NextDueAt: 200, LastAttemptAt: 0},
	}
	sorted := sortUpstreamPricingDueAccounts(candidates)
	if sorted[0].Domain != "b.example" || sorted[1].Domain != "a.example" || sorted[2].Domain != "c.example" {
		t.Fatalf("due ordering=%+v", sorted)
	}
	// After b gets a turn, its due/attempt timestamps move forward and a must be
	// selected next; fixed alphabetical ordering would starve it forever.
	sorted[0].NextDueAt, sorted[0].LastAttemptAt = 300, 1000
	sorted = sortUpstreamPricingDueAccounts(sorted)
	if sorted[0].Domain != "a.example" {
		t.Fatalf("round-robin fairness lost after first account advanced: %+v", sorted)
	}
}
