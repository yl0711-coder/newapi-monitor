package monitor

import (
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestDecodeSub2APIFundItems(t *testing.T) {
	tests := []struct {
		name, raw, kind, direction string
		amount                     float64
		financial                  bool
	}{
		{"redeem balance", `{"id":11,"type":"balance","value":100,"status":"used","code":"must-not-persist","used_at":"2026-09-04T20:01:02+08:00"}`, upstreamFundKindRedemption, "credit", 100, true},
		{"administrator credit", `{"id":12,"type":"admin_balance","value":50,"status":"used","notes":"manual credit","used_at":"2026-09-04T20:02:02+08:00"}`, upstreamFundKindAdminAdd, "credit", 50, true},
		{"administrator debit", `{"id":13,"type":"admin_balance","value":-7.5,"status":"used","used_at":"2026-09-04T20:03:02+08:00"}`, upstreamFundKindAdminSub, "debit", 7.5, true},
		{"affiliate credit", `{"id":14,"type":"affiliate_balance","value":3,"status":"used","created_at":"2026-09-04T20:04:02+08:00"}`, upstreamFundKindAffiliateGrant, "credit", 3, true},
		{"concurrency is not money", `{"id":15,"type":"admin_concurrency","value":20,"status":"used","used_at":"2026-09-04T20:05:02+08:00"}`, upstreamFundKindUnknown, "info", 0, false},
		{"subscription is not money", `{"id":16,"type":"subscription","value":30,"status":"used","used_at":"2026-09-04T20:06:02+08:00"}`, upstreamFundKindUnknown, "info", 0, false},
		{"unused code is not credited", `{"id":17,"type":"balance","value":40,"status":"unused","used_at":"2026-09-04T20:07:02+08:00"}`, upstreamFundKindUnknown, "info", 0, false},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			item, err := decodeSub2APIFundItem(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if item.Financial != tc.financial {
				t.Fatalf("financial mismatch: %+v", item)
			}
			if !tc.financial {
				return
			}
			if item.Kind != tc.kind || item.Direction != tc.direction || !item.AmountKnown || item.AmountUSD != tc.amount || !item.UpstreamAmountKnown || item.UpstreamCurrency != "USD" {
				t.Fatalf("money record mismatch: %+v", item)
			}
			if strings.Contains(item.Raw, "must-not-persist") || strings.Contains(item.Raw, `"code"`) {
				t.Fatalf("redeem code leaked into persisted evidence: %s", item.Raw)
			}
		})
	}
}

func TestFetchSub2APIFundEventsUsesReadOnlyHistoryContract(t *testing.T) {
	var requests int
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/redeem/history" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		if r.Header.Get("Authorization") != "Bearer read-only-token" {
			t.Fatalf("authorization header mismatch")
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"message":"ok","data":[{"id":21,"type":"balance","value":25,"status":"used","code":"secret-code","used_at":"2026-09-04T20:01:02+08:00"},{"id":22,"type":"admin_balance","value":-2,"status":"used","used_at":"2026-09-04T20:02:02+08:00"},{"id":23,"type":"admin_concurrency","value":10,"status":"used","used_at":"2026-09-04T20:03:02+08:00"}]}`))
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: server.URL, UserID: 9}
	events, earliest, err := m.fetchSub2APIFundEvents(t.Context(), row, sub2APICredential{AccessToken: "read-only-token"}, 1788552300)
	if err != nil {
		t.Fatal(err)
	}
	wantEarliest, _ := time.Parse(time.RFC3339, "2026-09-04T20:01:02+08:00")
	if requests != 1 || len(events) != 2 || earliest != wantEarliest.Unix() {
		t.Fatalf("unexpected fetch result: requests=%d events=%d earliest=%d", requests, len(events), earliest)
	}
	for _, event := range events {
		if strings.Contains(event.RawJSON, "secret-code") || strings.Contains(event.RawJSON, `"code"`) {
			t.Fatalf("redeem code leaked into event: %s", event.RawJSON)
		}
	}
}

func TestSyncSub2UpstreamFundsPersistsRecentScope(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/redeem/history" {
			t.Fatalf("unexpected request: %s %s", r.Method, r.URL.Path)
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"code":0,"data":[{"id":31,"type":"admin_balance","value":8,"status":"used","used_at":"2026-09-04T20:02:02+08:00"},{"id":32,"type":"subscription","value":100,"status":"used","used_at":"2026-09-04T20:03:02+08:00"}]}`))
	}))
	defer server.Close()
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamFundsSyncEnabled = true
	m.cfg.UpstreamFundsDomains = []string{"sub2.example"}
	row := ChannelUpstreamAccount{
		Domain: "sub2.example", Provider: upstreamProviderSub2API, BaseURL: server.URL,
		UserID: 9, Enabled: true, UsageSyncEnabled: true,
	}
	if err := m.persistSyncedUpstreamAccount(t.Context(), &row, sub2APICredential{AccessToken: "valid-token", RefreshToken: "refresh-token", ExpiresAt: time.Now().Add(time.Hour).Unix()}); err != nil {
		t.Fatal(err)
	}
	state, err := m.syncOneUpstreamFunds(t.Context(), row.Domain, false)
	if err != nil {
		t.Fatal(err)
	}
	if state.Status != upstreamStatusOK || state.HistoryScope != "provider_recent" || state.BackfillDone || state.RowsTotal != 1 || state.LastSuccessAt == 0 || state.NextSyncAt <= state.LastSuccessAt {
		t.Fatalf("unexpected Sub2API fund state: %+v", state)
	}
	var events []ChannelUpstreamFundEvent
	if err := m.storeDB.Order("event_key").Find(&events, "domain = ?", row.Domain).Error; err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Kind != upstreamFundKindAdminAdd || events[0].AmountUSD != 8 {
		t.Fatalf("non-money history leaked or money record lost: %+v", events)
	}
}

func TestDecodeUpstreamFundTopupKeepsCreditedAndPaidSeparate(t *testing.T) {
	row := ChannelUpstreamAccount{BalanceUnit: 500000}
	raw := json.RawMessage(`{"id":1,"created_at":1788552000,"type":1,"quota":0,"content":"使用在线充值成功，充值金额: $100.00，支付金额: ￥720.00"}`)
	item, err := decodeUpstreamFundItem(row, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if !item.Financial || item.Kind != upstreamFundKindTopup || !item.AmountKnown || item.AmountUSD != 100 {
		t.Fatalf("credited amount decoded incorrectly: %+v", item)
	}
	if !item.PaidKnown || item.PaidAmount != 720 || item.PaidCurrency != "CNY" {
		t.Fatalf("paid amount must stay a separate native-currency field: %+v", item)
	}
}

func TestDecodeUpstreamFundRealProviderFormats(t *testing.T) {
	row := ChannelUpstreamAccount{BalanceUnit: 500000}
	tests := []struct {
		name, raw, kind, direction, currency string
		amount, before, after                float64
		usdKnown, paidKnown                  bool
	}{
		{
			name: "yen topup remains native", kind: upstreamFundKindTopup, direction: "credit", currency: "CNY", amount: 1000, paidKnown: true,
			raw: `{"id":1,"created_at":1788349054,"type":1,"quota":0,"content":"使用在线充值成功，充值金额: ¥1000.000000 额度，支付金额：1000.000000","other":""}`,
		},
		{
			name: "full width dollar topup", kind: upstreamFundKindTopup, direction: "credit", currency: "USD", amount: 200, usdKnown: true, paidKnown: true,
			raw: `{"id":2,"created_at":1787709367,"type":1,"quota":0,"content":"使用在线充值成功，充值金额: ＄200.000000 额度，支付金额：200.000000"}`,
		},
		{
			name: "redemption without amount label", kind: upstreamFundKindRedemption, direction: "credit", currency: "USD", amount: 5000, usdKnown: true,
			raw: `{"id":3,"created_at":1788015917,"type":1,"quota":0,"content":"通过兑换码充值 ＄5000.000000 额度，兑换码ID 850"}`,
		},
		{
			name: "administrator received", kind: upstreamFundKindManualTopup, direction: "credit", currency: "USD", amount: 500, usdKnown: true,
			raw: `{"id":4,"created_at":1788316230,"type":1,"quota":0,"content":"Received ＄500.000000 额度 from an administrator","other":"{\"op\":{\"action\":\"user.quota_received\",\"params\":{\"quota\":\"＄500.000000 额度\"}}}"}`,
		},
		{
			name: "yen administrator override", kind: upstreamFundKindAdminSet, direction: "credit", currency: "CNY", amount: 4999.995146, before: -0.225146, after: 4999.77,
			raw: `{"id":5,"created_at":1786590827,"type":3,"quota":0,"content":"管理员将用户额度从 ¥-0.225146 额度修改为 ¥4999.770000 额度"}`,
		},
		{
			name: "full width dollar administrator add", kind: upstreamFundKindAdminAdd, direction: "credit", currency: "USD", amount: 1000, usdKnown: true,
			raw: `{"id":6,"created_at":1786628118,"type":3,"quota":0,"content":"管理员为用户增加额度 ＄1000.000000 额度"}`,
		},
		{
			name: "signup grant", kind: upstreamFundKindSignupGrant, direction: "credit", currency: "USD", amount: 2, usdKnown: true,
			raw: `{"id":7,"created_at":1787649490,"type":4,"quota":0,"content":"新用户注册赠送 ＄2.000000 额度"}`,
		},
		{
			name: "checkin grant", kind: upstreamFundKindCheckinGrant, direction: "credit", currency: "USD", amount: 0.0513, usdKnown: true,
			raw: `{"id":8,"created_at":1787542875,"type":4,"quota":0,"content":"用户签到，获得额度 ＄0.051300 额度"}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var envelope struct {
				Type int `json:"type"`
			}
			if err := json.Unmarshal([]byte(tc.raw), &envelope); err != nil {
				t.Fatal(err)
			}
			item, err := decodeUpstreamFundItem(row, envelope.Type, json.RawMessage(tc.raw))
			if err != nil {
				t.Fatal(err)
			}
			if !item.Financial || item.Kind != tc.kind || item.Direction != tc.direction || !item.UpstreamAmountKnown || item.UpstreamCurrency != tc.currency || math.Abs(item.UpstreamAmount-tc.amount) > 1e-6 {
				t.Fatalf("real provider amount decoded incorrectly: %+v", item)
			}
			if item.AmountKnown != tc.usdKnown {
				t.Fatalf("USD normalization state mismatch: %+v", item)
			}
			if item.PaidKnown != tc.paidKnown {
				t.Fatalf("paid state mismatch: %+v", item)
			}
			if tc.before != 0 || tc.after != 0 {
				if !item.UpstreamBeforeKnown || !item.UpstreamAfterKnown || math.Abs(item.UpstreamBefore-tc.before) > 1e-6 || math.Abs(item.UpstreamAfter-tc.after) > 1e-6 {
					t.Fatalf("native before/after lost: %+v", item)
				}
			}
		})
	}
}

func TestDecodeUpstreamFundUnitlessAmountRemainsUnresolved(t *testing.T) {
	raw := json.RawMessage(`{"id":9,"created_at":1788552000,"type":1,"quota":0,"content":"充值金额: 100 额度"}`)
	item, err := decodeUpstreamFundItem(ChannelUpstreamAccount{BalanceUnit: 500000}, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if !item.Financial || !item.UpstreamAmountKnown || item.UpstreamAmount != 100 || item.UpstreamCurrency != "" || item.AmountKnown {
		t.Fatalf("unitless amount must remain auditable but unresolved: %+v", item)
	}
}

func TestDecodeUpstreamFundSingleAbsoluteAmountIsNotCredited(t *testing.T) {
	tests := []struct {
		name string
		raw  json.RawMessage
		kind string
	}{
		{"management override", json.RawMessage(`{"id":10,"created_at":1788552000,"type":3,"content":"管理员将用户额度设置为 $100.00 额度"}`), upstreamFundKindAdminSet},
		{"system override", json.RawMessage(`{"id":11,"created_at":1788552000,"type":4,"content":"系统将账户额度调整为 $100.00 额度"}`), upstreamFundKindSystemAdjust},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var envelope struct {
				Type int `json:"type"`
			}
			if err := json.Unmarshal(tc.raw, &envelope); err != nil {
				t.Fatal(err)
			}
			item, err := decodeUpstreamFundItem(ChannelUpstreamAccount{BalanceUnit: 500000}, envelope.Type, tc.raw)
			if err != nil {
				t.Fatal(err)
			}
			if !item.Financial || item.Kind != tc.kind || item.Direction != "info" || item.AmountKnown || item.UpstreamAmountKnown || !item.AfterKnown || !item.UpstreamAfterKnown || item.AfterUSD != 100 || item.UpstreamAfter != 100 {
				t.Fatalf("absolute amount must not enter credit totals: %+v", item)
			}
		})
	}
}

func TestDecodeUpstreamFundRejectsIgnoredTypeFilter(t *testing.T) {
	raw := json.RawMessage(`{"id":19,"created_at":1788552000,"type":4,"content":"新用户注册赠送 $2 额度"}`)
	if _, err := decodeUpstreamFundItem(ChannelUpstreamAccount{BalanceUnit: 500000}, 1, raw); err == nil {
		t.Fatal("fund parser accepted an item whose actual type differs from the requested type")
	}
}

func TestDecodeUpstreamFundIgnoresNonFinancialSystemLog(t *testing.T) {
	raw := json.RawMessage(`{"created_at":1788552000,"type":4,"content":"系统配置已更新"}`)
	item, err := decodeUpstreamFundItem(ChannelUpstreamAccount{}, 4, raw)
	if err != nil {
		t.Fatal(err)
	}
	if item.Financial {
		t.Fatalf("non-financial system log must not enter the ledger: %+v", item)
	}
}

func TestDecodeUpstreamFundStructuredAdminOperations(t *testing.T) {
	row := ChannelUpstreamAccount{BalanceUnit: 500000}
	tests := []struct {
		name, action, kind, direction string
		params                        string
		amount, before, after         float64
	}{
		{"add", "user.quota_add", upstreamFundKindAdminAdd, "credit", `{"quota":1000000}`, 2, 0, 0},
		{"subtract", "user.quota_subtract", upstreamFundKindAdminSub, "debit", `{"quota":250000}`, .5, 0, 0},
		{"override", "user.quota_override", upstreamFundKindAdminSet, "debit", `{"from":1500000,"to":500000}`, 2, 3, 1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			raw := json.RawMessage(`{"created_at":1788552000,"type":3,"content":"audit","other":{"op":{"action":"` + tc.action + `","params":` + tc.params + `}}}`)
			item, err := decodeUpstreamFundItem(row, 3, raw)
			if err != nil {
				t.Fatal(err)
			}
			if !item.Financial || item.Kind != tc.kind || item.Direction != tc.direction || item.Confidence != upstreamFundConfidenceStructured || !item.AmountKnown || math.Abs(item.AmountUSD-tc.amount) > 1e-9 {
				t.Fatalf("operation decoded incorrectly: %+v", item)
			}
			if tc.name == "override" && (!item.BeforeKnown || !item.AfterKnown || item.BeforeUSD != tc.before || item.AfterUSD != tc.after) {
				t.Fatalf("override before/after lost: %+v", item)
			}
		})
	}
}

func TestDecodeUpstreamFundIgnoresNonFinancialManagement(t *testing.T) {
	raw := json.RawMessage(`{"created_at":1788552000,"type":3,"content":"修改用户分组","other":{"op":{"action":"user.group_update","params":{"group":"vip"}}}}`)
	item, err := decodeUpstreamFundItem(ChannelUpstreamAccount{}, 3, raw)
	if err != nil {
		t.Fatal(err)
	}
	if item.Financial {
		t.Fatalf("non-financial audit must not enter the ledger: %+v", item)
	}
}

func TestNarrowedUpstreamFundWindowPreservesDirection(t *testing.T) {
	if from, to := narrowedUpstreamFundWindow(100, 200, 150, false); from != 100 || to != 150 {
		t.Fatalf("tail must retain the left half: [%d,%d)", from, to)
	}
	if from, to := narrowedUpstreamFundWindow(100, 200, 150, true); from != 150 || to != 200 {
		t.Fatalf("backfill must retain the boundary-adjacent right half: [%d,%d)", from, to)
	}
}

func TestPersistUpstreamFundEventKeepsLargestObservedMultiplicity(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&ChannelUpstreamFundEvent{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	base := ChannelUpstreamFundEvent{Domain: "up.example", AccountEpoch: "epoch", EventKey: "1-key", OccurredAt: 1, Kind: upstreamFundKindTopup, Direction: "credit", AmountUSD: 10, AmountKnown: true, ObservedCount: 2}
	if err := m.persistUpstreamFundEvents(t.Context(), []ChannelUpstreamFundEvent{base}); err != nil {
		t.Fatal(err)
	}
	base.ObservedCount = 1
	if err := m.persistUpstreamFundEvents(t.Context(), []ChannelUpstreamFundEvent{base}); err != nil {
		t.Fatal(err)
	}
	var got ChannelUpstreamFundEvent
	if err := db.First(&got, "domain = ? AND account_epoch = ? AND event_key = ?", base.Domain, base.AccountEpoch, base.EventKey).Error; err != nil {
		t.Fatal(err)
	}
	if got.ObservedCount != 2 {
		t.Fatalf("overlap rescan reduced event multiplicity: got=%d want=2", got.ObservedCount)
	}
}

func TestPersistUpstreamFundEventCannotRewriteHistoricalUnitEvidence(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&ChannelUpstreamFundEvent{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	old := ChannelUpstreamFundEvent{
		Domain: "funds.example", AccountEpoch: "epoch", EventKey: "1-key", Provider: upstreamProviderNewAPI,
		OccurredAt: 1, Kind: upstreamFundKindTopup, Direction: "credit", AmountUSD: 2, AmountKnown: true,
		UpstreamAmount: 2, UpstreamAmountKnown: true, UpstreamCurrency: "USD", UnitPerUSD: 500000, ObservedCount: 1,
	}
	if err := m.persistUpstreamFundEvents(t.Context(), []ChannelUpstreamFundEvent{old}); err != nil {
		t.Fatal(err)
	}
	incoming := old
	incoming.AmountUSD, incoming.UpstreamAmount, incoming.UnitPerUSD, incoming.ObservedCount = 1, 1, 1000000, 2
	if err := m.persistUpstreamFundEvents(t.Context(), []ChannelUpstreamFundEvent{incoming}); err != nil {
		t.Fatal(err)
	}
	var got ChannelUpstreamFundEvent
	if err := db.First(&got, "domain = ? AND account_epoch = ? AND event_key = ?", old.Domain, old.AccountEpoch, old.EventKey).Error; err != nil {
		t.Fatal(err)
	}
	if got.UnitPerUSD != old.UnitPerUSD || got.AmountUSD != old.AmountUSD || got.UpstreamAmount != old.UpstreamAmount || got.ObservedCount != 2 {
		t.Fatalf("overlap refresh rewrote historical monetary evidence: %+v", got)
	}
}

func TestDecodeHistoricalFundAfterUnitChangeFailsClosed(t *testing.T) {
	row := ChannelUpstreamAccount{
		Domain: "funds.example", Provider: upstreamProviderNewAPI, BalanceUnit: 1000000,
		BalanceUnitPrevious: 500000, BalanceUnitEffectiveAt: 7200,
	}
	raw := json.RawMessage(`{"id":1,"created_at":100,"type":1,"quota":1000000,"content":"topup"}`)
	item, err := decodeUpstreamFundItem(row, 1, raw)
	if err != nil {
		t.Fatal(err)
	}
	if !item.Financial || item.AmountKnown || item.UnitPerUSD != 0 || item.Raw == "" {
		t.Fatalf("unseen pre-change event must retain raw evidence without guessed USD: %+v", item)
	}
}

func TestReparseStoredUpstreamFundEventsUpgradesWithoutRefetch(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&ChannelUpstreamFundEvent{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	row := ChannelUpstreamAccount{Domain: "funds.example", Provider: upstreamProviderNewAPI, BaseURL: "https://funds.example", UserID: 7, BalanceUnit: 500000}
	epoch := newAPIUpstreamAccountEpoch(row)
	raw := `{"id":1,"created_at":1788349054,"type":1,"quota":0,"content":"使用在线充值成功，充值金额: ¥1000.000000 额度，支付金额：1000.000000"}`
	item, err := decodeUpstreamFundItem(row, 1, json.RawMessage(raw))
	if err != nil {
		t.Fatal(err)
	}
	old := makeChannelUpstreamFundEvent(row, epoch, item, 3, 1788552100)
	old.ParserVersion = 0
	old.UpstreamAmount, old.UpstreamAmountKnown, old.UpstreamCurrency = 0, false, ""
	if err := db.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.reparseStoredUpstreamFundEvents(t.Context(), row, 1788552200); err != nil {
		t.Fatal(err)
	}
	var got ChannelUpstreamFundEvent
	if err := db.First(&got, "domain = ? AND account_epoch = ? AND event_key = ?", row.Domain, epoch, old.EventKey).Error; err != nil {
		t.Fatal(err)
	}
	if got.ParserVersion != upstreamFundParserVersion || !got.UpstreamAmountKnown || got.UpstreamCurrency != "CNY" || got.UpstreamAmount != 1000 || got.AmountKnown || got.ObservedCount != 3 || got.FetchedAt != old.FetchedAt {
		t.Fatalf("stored event was not upgraded safely: %+v", got)
	}
}

func TestReparseStoredUpstreamFundEventsQuarantinesBadRowAndContinues(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&ChannelUpstreamFundEvent{}); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{storeDB: db}
	row := ChannelUpstreamAccount{Domain: "funds.example", Provider: upstreamProviderNewAPI, BaseURL: "https://funds.example", UserID: 7, BalanceUnit: 500000}
	epoch := newAPIUpstreamAccountEpoch(row)
	goodRaw := `{"id":12,"created_at":1788552000,"type":1,"quota":0,"content":"充值金额: $20.00"}`
	goodItem, err := decodeUpstreamFundItem(row, 1, json.RawMessage(goodRaw))
	if err != nil {
		t.Fatal(err)
	}
	goodEvent := makeChannelUpstreamFundEvent(row, epoch, goodItem, 1, 1788552100)
	goodEvent.ParserVersion = 0
	goodEvent.AmountUSD, goodEvent.AmountKnown = 0, false
	goodEvent.UpstreamAmount, goodEvent.UpstreamAmountKnown, goodEvent.UpstreamCurrency = 0, false, ""
	rows := []ChannelUpstreamFundEvent{
		{Domain: row.Domain, AccountEpoch: epoch, EventKey: "1-bad", OccurredAt: 1788552100, SourceType: 1, Kind: upstreamFundKindTopup, Direction: "credit", ParserVersion: 0, RawJSON: `{"broken":`, RawTruncated: true, ObservedCount: 1},
		goodEvent,
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.reparseStoredUpstreamFundEvents(t.Context(), row, 1788552200); err != nil {
		t.Fatalf("bad historical evidence must not block reparse or live tail: %v", err)
	}
	var got []ChannelUpstreamFundEvent
	if err := db.Order("occurred_at DESC").Find(&got, "domain = ? AND account_epoch = ?", row.Domain, epoch).Error; err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].ParserVersion != upstreamFundParserVersion || got[0].ReparseError == "" {
		t.Fatalf("bad row was not quarantined: %+v", got)
	}
	if got[1].ParserVersion != upstreamFundParserVersion || got[1].ReparseError != "" || !got[1].AmountKnown || got[1].AmountUSD != 20 {
		t.Fatalf("valid row after bad row was not upgraded: %+v", got[1])
	}
}

func TestUpstreamFundsSummaryIsNotLimitedByDetailPage(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamFundsSyncEnabled = true
	m.cfg.UpstreamFundsDomains = []string{"funds.example"}
	account := ChannelUpstreamAccount{
		Domain: "funds.example", Provider: upstreamProviderNewAPI, BaseURL: "https://funds.example",
		UserID: 7, Enabled: true, UsageSyncEnabled: true,
	}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	events := make([]ChannelUpstreamFundEvent, 0, 501)
	for i := 0; i < 501; i++ {
		event := ChannelUpstreamFundEvent{
			Domain: account.Domain, AccountEpoch: epoch, EventKey: "key-" + strconv.Itoa(i),
			OccurredAt: 1788552000 + int64(i), Kind: upstreamFundKindTopup, Direction: "credit",
			AmountUSD: 1, AmountKnown: true, ObservedCount: 1,
		}
		if i == 0 {
			event.PaidKnown, event.PaidCurrency, event.PaidAmount = true, "CNY", 7.2
		}
		events = append(events, event)
	}
	if err := m.storeDB.CreateInBatches(events, 100).Error; err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/channels/upstream/funds?domain=funds.example&from=1788551900&to=1788553000", nil)
	m.getChannelUpstreamFundsHandler(c)
	if recorder.Code != 200 {
		t.Fatalf("handler status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Limited    bool                    `json:"limited"`
		Summary    upstreamFundSummary     `json:"summary"`
		PaidTotals []upstreamFundPaidTotal `json:"paid_totals"`
		Events     []json.RawMessage       `json:"events"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Limited || len(response.Events) != 500 || response.Summary.CreditedUSD != 501 || response.Summary.EventOccurrences != 501 {
		t.Fatalf("summary was truncated with details: limited=%v rows=%d summary=%+v", response.Limited, len(response.Events), response.Summary)
	}
	if len(response.PaidTotals) != 1 || response.PaidTotals[0].Currency != "CNY" || response.PaidTotals[0].Amount != 7.2 {
		t.Fatalf("known paid currencies were not summarized separately: %+v", response.PaidTotals)
	}
}

func TestUpstreamFundsSummarySeparatesNativeCurrencyFromUnknown(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamFundsSyncEnabled = true
	m.cfg.UpstreamFundsDomains = []string{"native.example"}
	account := ChannelUpstreamAccount{Domain: "native.example", Provider: upstreamProviderNewAPI, BaseURL: "https://native.example", UserID: 7, Enabled: true, UsageSyncEnabled: true}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	events := []ChannelUpstreamFundEvent{
		{Domain: account.Domain, AccountEpoch: epoch, EventKey: "native", OccurredAt: 1788552000, Kind: upstreamFundKindTopup, Direction: "credit", UpstreamAmount: 1000, UpstreamAmountKnown: true, UpstreamCurrency: "CNY", ObservedCount: 2},
		{Domain: account.Domain, AccountEpoch: epoch, EventKey: "unknown", OccurredAt: 1788552001, Kind: upstreamFundKindAdminSet, Direction: "info", ObservedCount: 1},
		{Domain: account.Domain, AccountEpoch: epoch, EventKey: "unitless", OccurredAt: 1788552002, Kind: upstreamFundKindTopup, Direction: "credit", UpstreamAmount: 100, UpstreamAmountKnown: true, ObservedCount: 1},
		{Domain: account.Domain, AccountEpoch: epoch, EventKey: "quarantined", OccurredAt: 1788552003, Kind: upstreamFundKindTopup, Direction: "credit", AmountUSD: 999, AmountKnown: true, PaidAmount: 7, PaidKnown: true, PaidCurrency: "CNY", ReparseError: "invalid retained evidence", ObservedCount: 1},
	}
	if err := m.storeDB.Create(&events).Error; err != nil {
		t.Fatal(err)
	}
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest("GET", "/channels/upstream/funds?domain=native.example&from=1788551900&to=1788553000", nil)
	m.getChannelUpstreamFundsHandler(c)
	if recorder.Code != 200 {
		t.Fatalf("handler status=%d body=%s", recorder.Code, recorder.Body.String())
	}
	var response struct {
		Summary        upstreamFundSummary         `json:"summary"`
		UpstreamTotals []upstreamFundCurrencyTotal `json:"upstream_totals"`
	}
	if err := json.Unmarshal(recorder.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Summary.UnknownAmountEvents != 2 {
		t.Fatalf("native known amount must not count as unknown: %+v", response.Summary)
	}
	if response.Summary.CreditedUSD != 0 || response.Summary.QuarantinedEvents != 1 {
		t.Fatalf("quarantined derived amounts must fail closed: %+v", response.Summary)
	}
	if len(response.UpstreamTotals) != 1 || response.UpstreamTotals[0].Currency != "CNY" || response.UpstreamTotals[0].Credited != 2000 {
		t.Fatalf("native totals mismatch: %+v", response.UpstreamTotals)
	}
}

func TestUpstreamFundQueryCompletenessFailsClosed(t *testing.T) {
	state := UpstreamFundSyncState{CoverageFrom: 100, TailSyncedUntil: 200, BackfillDone: true}
	if !upstreamFundQueryComplete("supported", state, 100, 200) {
		t.Fatal("fully covered query was not accepted")
	}
	for name, candidate := range map[string]UpstreamFundSyncState{
		"history pending": {CoverageFrom: 100, TailSyncedUntil: 200},
		"left gap":        {CoverageFrom: 101, TailSyncedUntil: 200, BackfillDone: true},
		"tail gap":        {CoverageFrom: 100, TailSyncedUntil: 199, BackfillDone: true},
		"provider recent": {CoverageFrom: 100, TailSyncedUntil: 200, BackfillDone: true, HistoryScope: "provider_recent"},
	} {
		if upstreamFundQueryComplete("supported", candidate, 100, 200) {
			t.Fatalf("%s was incorrectly accepted as complete", name)
		}
	}
	if upstreamFundQueryComplete("global_off", state, 100, 200) {
		t.Fatal("disabled capability was incorrectly accepted as complete")
	}
}
