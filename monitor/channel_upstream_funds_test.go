package monitor

import (
	"encoding/json"
	"math"
	"net/http/httptest"
	"strconv"
	"testing"

	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

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
