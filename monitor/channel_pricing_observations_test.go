package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func pricingObservationEvidence(hour int64, ratio string) ChannelUpstreamPricingHourEvidence {
	return ChannelUpstreamPricingHourEvidence{
		HourTs: hour, Provider: upstreamProviderNewAPI, SourceGroup: "upstream-group", ModelName: "model",
		EligibleRequests: 10, OtherValid: true, EvidenceCapability: "full_rate", EffectiveRatioSource: "group_ratio", EffectiveRatio: ratio,
	}
}

func TestPricingObservationComparisonDoesNotInferOrApplyPricing(t *testing.T) {
	hour := int64(1789441200)
	evidence := pricingObservationEvidence(hour, "1")
	verified := ChannelUpstreamPricingHourState{Status: "verified", ReconcileStatus: "matched"}
	config := []channelPricingConfiguration{{ChannelID: 9, EffectiveMultiplier: "1"}}
	for _, tc := range []struct {
		name   string
		rows   []ChannelUpstreamPricingHourEvidence
		state  ChannelUpstreamPricingHourState
		config []channelPricingConfiguration
		want   string
	}{
		{"same", []ChannelUpstreamPricingHourEvidence{evidence}, verified, config, "same"},
		{"equivalent decimal", []ChannelUpstreamPricingHourEvidence{pricingObservationEvidence(hour, "1.000")}, verified, config, "same"},
		{"different", []ChannelUpstreamPricingHourEvidence{pricingObservationEvidence(hour, "1.2")}, verified, config, "different"},
		{"small difference is not rounded away", []ChannelUpstreamPricingHourEvidence{pricingObservationEvidence(hour, "1.0000000000001")}, verified, config, "different"},
		{"mixed", []ChannelUpstreamPricingHourEvidence{evidence, pricingObservationEvidence(hour, "1.2")}, verified, config, "mixed_rates"},
		{"missing config", []ChannelUpstreamPricingHourEvidence{evidence}, verified, nil, "not_configured"},
		{"conflicting config", []ChannelUpstreamPricingHourEvidence{evidence}, verified, append(append([]channelPricingConfiguration{}, config...), channelPricingConfiguration{ChannelID: 10, EffectiveMultiplier: "2"}), "configuration_conflict"},
		{"invalid config", []ChannelUpstreamPricingHourEvidence{evidence}, verified, []channelPricingConfiguration{{EffectiveMultiplier: ""}}, "configuration_conflict"},
		{"single scan", []ChannelUpstreamPricingHourEvidence{evidence}, ChannelUpstreamPricingHourState{Status: "observed", ReconcileStatus: "matched"}, config, "unverified"},
		{"reconcile mismatch", []ChannelUpstreamPricingHourEvidence{evidence}, ChannelUpstreamPricingHourState{Status: "verified", ReconcileStatus: "mismatch"}, config, "unverified"},
		{"invalid ratio alongside valid ratio", []ChannelUpstreamPricingHourEvidence{evidence, pricingObservationEvidence(hour, "bad")}, verified, config, "unverified"},
		{"exponent budget", []ChannelUpstreamPricingHourEvidence{pricingObservationEvidence(hour, "1e999999999")}, verified, config, "unverified"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			before, _ := json.Marshal(tc.rows)
			row, err := comparePricingObservation(tc.rows, tc.state, tc.config, hour+4000, true)
			if err != nil || row.Comparison != tc.want || row.HourTs != hour {
				t.Fatalf("row=%+v err=%v want=%s", row, err, tc.want)
			}
			after, _ := json.Marshal(tc.rows)
			if string(before) != string(after) {
				t.Fatal("comparison mutated source evidence")
			}
		})
	}
	for _, active := range []bool{true, false} {
		row, err := comparePricingObservation([]ChannelUpstreamPricingHourEvidence{evidence}, verified, config, hour+3600+int64(pricingObservationRecentAge/time.Second)+1, active)
		if err != nil || row.EvidenceStatus != "historical" {
			t.Fatalf("stale records advertised as current: %+v %v", row, err)
		}
	}
	row, _ := comparePricingObservation([]ChannelUpstreamPricingHourEvidence{evidence}, verified, config, hour+4000, false)
	if row.EvidenceStatus != "historical" {
		t.Fatal("stopped collector advertised as current")
	}
	unknown := evidence
	unknown.OtherValid = false
	row, _ = comparePricingObservation([]ChannelUpstreamPricingHourEvidence{evidence, unknown}, verified, config, hour+4000, true)
	if row.Comparison != "unverified" {
		t.Fatal("unknown evidence was discarded to obtain a comparison")
	}
	unknown = evidence
	unknown.EffectiveRatioSource = "unknown"
	row, _ = comparePricingObservation([]ChannelUpstreamPricingHourEvidence{unknown}, verified, config, hour+4000, true)
	if row.Comparison != "unverified" {
		t.Fatal("unknown ratio provenance was treated as reliable")
	}
	tooMany := evidence
	tooMany.EligibleRequests = math.MaxInt64
	if _, err := comparePricingObservation([]ChannelUpstreamPricingHourEvidence{tooMany, evidence}, verified, config, hour+4000, true); err == nil {
		t.Fatal("request counter overflow was accepted")
	}
}

func TestPricingObservationDiscountCannotDefineBaseRate(t *testing.T) {
	row := pricingObservationEvidence(3600, "")
	row.Provider, row.EvidenceCapability, row.DiscountRatioState, row.DiscountRatio = upstreamProviderAICodeWith, "discount_only", pricingRatioValid, "0.8"
	view, err := comparePricingObservation([]ChannelUpstreamPricingHourEvidence{row}, ChannelUpstreamPricingHourState{Status: "verified", ReconcileStatus: "source_verified"},
		[]channelPricingConfiguration{{EffectiveMultiplier: "0.8"}}, 7201, true)
	if err != nil || view.Comparison != "discount_only" || len(view.Rates) != 0 || len(view.Discounts) != 1 {
		t.Fatalf("discount was inflated into full-rate evidence: %+v %v", view, err)
	}
}

func newPricingObservationMonitor(t *testing.T) (*Monitor, ChannelUpstreamAccount, int64) {
	t.Helper()
	m := newChannelUpstreamTestMonitor(t)
	pool, _ := m.storeDB.DB()
	t.Cleanup(func() { _ = pool.Close() })
	account := ChannelUpstreamAccount{Domain: "pricing.example", Provider: upstreamProviderNewAPI, BaseURL: "https://pricing.example", Account: "private-account-name", UserID: 41, Enabled: true, UsageSyncEnabled: true, Credential: "private-ciphertext"}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	m.cfg.UpstreamPricingLedgerEnabled, m.cfg.UpstreamPricingLedgerDomains = true, []string{account.Domain}
	if err := m.storeDB.Create(&ChannelSnap{ID: 9, Name: "channel-nine", BaseDomain: account.Domain, Groups: "local-group"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelFinanceChannelCost{ChannelID: 9, Grp: "local-group", UpstreamGroupName: "upstream-group", Multiplier: 2, DiscountFactor: 0.5}).Error; err != nil {
		t.Fatal(err)
	}
	// A recharge correction must never be multiplied into this rate comparison.
	if err := m.storeDB.Create(&ChannelDomainCost{Domain: account.Domain, RechargePaid: 7, RechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	hour := time.Now().Unix()/3600*3600 - 3600
	return m, account, hour
}

func seedPricingObservation(t *testing.T, m *Monitor, account ChannelUpstreamAccount, hour int64, ratios ...string) {
	t.Helper()
	epoch := newAPIUpstreamAccountEpoch(account)
	if err := m.storeDB.Create(&ChannelUpstreamPricingHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, SemanticsVersion: upstreamPricingSemanticsVersion, Status: "verified", ReconcileStatus: "matched", VerifiedScans: 2}).Error; err != nil {
		t.Fatal(err)
	}
	for i, ratio := range ratios {
		row := pricingObservationEvidence(hour, ratio)
		row.Domain, row.AccountEpoch, row.SemanticsVersion, row.DimensionHash = account.Domain, epoch, upstreamPricingSemanticsVersion, fmt.Sprintf("%064d", i)
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func TestPricingObservationReaderUsesLatestDimensionAndCurrentAccountOnly(t *testing.T) {
	m, account, hour := newPricingObservationMonitor(t)
	seedPricingObservation(t, m, account, hour-3600, "1")
	seedPricingObservation(t, m, account, hour, "1", "1.2")
	oldAccount := account
	oldAccount.UserID++
	seedPricingObservation(t, m, oldAccount, hour, "99")
	seedPricingObservation(t, m, account, hour+7200, "99") // Unclosed/future hour.
	seedPricingObservation(t, m, account, hour-pricingObservationWindowHours*3600, "99")
	if err := m.storeDB.Create(&ChannelFinanceChannelCost{ChannelID: 9, Grp: "removed-group", UpstreamGroupName: "upstream-group", Multiplier: 99, DiscountFactor: 1}).Error; err != nil {
		t.Fatal(err)
	}
	view, err := m.loadChannelPricingObservations(context.Background(), account.Domain, hour+4000)
	if err != nil || len(view.Rows) != 1 || view.Rows[0].Comparison != "mixed_rates" || view.Rows[0].HourTs != hour || len(view.Rows[0].Rates) != 2 {
		t.Fatalf("wrong account/hour or hidden mixed evidence: %+v err=%v", view, err)
	}
	configs := view.Configurations["upstream-group"]
	if len(configs) != 1 || configs[0].EffectiveMultiplier != "1" {
		t.Fatalf("obsolete group or recharge ratio leaked into comparison: %+v", configs)
	}
	encoded, _ := json.Marshal(view)
	for _, secret := range []string{account.Account, account.Credential, "account_epoch", "raw_json"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("response included unrelated/private data %q", secret)
		}
	}
	var unchanged ChannelFinanceChannelCost
	if err := m.storeDB.First(&unchanged, "channel_id = 9 AND grp = 'local-group'").Error; err != nil || unchanged.Multiplier != 2 || unchanged.DiscountFactor != 0.5 {
		t.Fatal("reader changed current finance configuration")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.loadChannelPricingObservations(ctx, account.Domain, hour+4000); err == nil {
		t.Fatal("cancelled read did not stop")
	}
}

func TestPricingObservationReaderNeverPublishesTruncatedEvidence(t *testing.T) {
	m, account, hour := newPricingObservationMonitor(t)
	seedPricingObservation(t, m, account, hour, "1")
	rows := make([]ChannelUpstreamPricingHourEvidence, pricingObservationRowLimit)
	for i := range rows {
		rows[i] = pricingObservationEvidence(hour, "2")
		rows[i].Domain, rows[i].AccountEpoch, rows[i].SemanticsVersion = account.Domain, newAPIUpstreamAccountEpoch(account), upstreamPricingSemanticsVersion
		rows[i].DimensionHash = fmt.Sprintf("%064d", i+1)
	}
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	view, err := m.loadChannelPricingObservations(context.Background(), account.Domain, hour+4000)
	if err != nil || !view.Truncated || len(view.Rows) != 0 {
		t.Fatalf("truncated sample produced a supposedly authoritative comparison: %+v %v", view, err)
	}
}

func TestPricingObservationMissingEvidenceCannotMeanNoPricingChange(t *testing.T) {
	m, account, hour := newPricingObservationMonitor(t)
	view, err := m.loadChannelPricingObservations(context.Background(), account.Domain, hour+4000)
	if err != nil || len(view.Rows) != 0 || view.AsOfHour != 0 {
		t.Fatalf("missing evidence fabricated a pricing observation: %+v %v", view, err)
	}
	seedPricingObservation(t, m, account, hour, "1")
	m.cfg.LocalSnapshotOnly = true
	view, err = m.loadChannelPricingObservations(context.Background(), account.Domain, hour+4000)
	if err != nil || view.CollectionActive || view.Rows[0].EvidenceStatus != "historical" {
		t.Fatalf("offline snapshot falsely reported active collection: %+v %v", view, err)
	}
	var events []ChannelUpstreamPricingChangeEvent
	for i := 0; i < pricingObservationChangeLimit+1; i++ {
		events = append(events, ChannelUpstreamPricingChangeEvent{EventKey: fmt.Sprintf("event-%d", i), Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SemanticsVersion: upstreamPricingSemanticsVersion,
			SourceGroup: "upstream-group", ModelName: "model", PreviousRatio: "1", CurrentRatio: "1.2", FirstObservedHour: hour - int64(i)*3600})
	}
	if err := m.storeDB.Create(&events).Error; err != nil {
		t.Fatal(err)
	}
	view, err = m.loadChannelPricingObservations(context.Background(), account.Domain, hour+4000)
	if err != nil || !view.ChangesTruncated || len(view.Changes) != pricingObservationChangeLimit || view.Changes[0].HourTs != hour {
		t.Fatalf("history was unbounded or did not preserve latest order: %+v %v", view, err)
	}
}

func TestPricingObservationHTTPIsRootOnlyLocalAndNoStore(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m, account, hour := newPricingObservationMonitor(t)
	seedPricingObservation(t, m, account, hour, "1")
	router := gin.New()
	m.RegisterRoutes(router)
	for _, tc := range []struct {
		role   int
		query  string
		status int
	}{
		{0, "domain=pricing.example", 401},
		{roleAdmin, "domain=pricing.example", 403},
		{roleRoot, "domain=pricing.example", 200},
		{roleRoot, "domain=https%3A%2F%2Fpricing.example", 400},
		{roleRoot, "domain=absent.example", 404},
	} {
		req := httptest.NewRequest(http.MethodGet, "/channels/upstream/pricing-observations?"+tc.query, nil)
		req.Header.Set("Accept", "application/json")
		if tc.role != 0 {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("operator", tc.role, time.Now().Unix())})
		}
		response := httptest.NewRecorder()
		router.ServeHTTP(response, req)
		if response.Code != tc.status {
			t.Fatalf("role=%d query=%s status=%d body=%s", tc.role, tc.query, response.Code, response.Body.String())
		}
		if tc.status == 200 {
			if response.Header().Get("Cache-Control") != "no-store" || strings.Contains(response.Body.String(), "private-") {
				t.Fatal("private account data leaked or response can be cached")
			}
			var view channelPricingObservationView
			if err := json.Unmarshal(response.Body.Bytes(), &view); err != nil || len(view.Rows) != 1 || view.Rows[0].Comparison != "same" {
				t.Fatalf("HTTP contract invalid: %+v %v", view, err)
			}
		}
	}
}
