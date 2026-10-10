package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// Use the normal routes, signed sessions and feature gates, but never start
// collectors or construct an external client. All writes target test stores.
func historyFlowRouter(t *testing.T, m *Monitor, domain string) *gin.Engine {
	t.Helper()
	m.cfg.LocalSnapshotOnly, m.cfg.LocalAuthBypass = false, false
	m.cfg.StabilityEnabled, m.cfg.FinanceEnabled = true, true
	m.cfg.StabilityRetentionDays, m.cfg.FinanceStartDate = 365, "2026-05-01"
	m.cfg.ChannelCostClosureEnabled, m.cfg.ChannelEconomicsReportEnabled = true, true
	m.cfg.ChannelCostClosureDomains = []string{domain}
	m.cfg.UpstreamUsageSyncEnabled, m.cfg.UpstreamPricingLedgerEnabled = true, true
	m.cfg.UpstreamPricingLedgerDomains = []string{domain}
	m.cfg.ChannelCostHMACKey, m.cfg.ChannelCostHMACKeyID = "local-flow-test-only-0123456789abcdef", "local-flow"
	m.cfg.SessionSecret = "local-flow-session-only"
	if err := validateChannelCostClosureSettings(m.cfg); err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	router := gin.New()
	m.RegisterRoutes(router)
	return router
}

func historyFlowRequest(t *testing.T, m *Monitor, router http.Handler, method, path string, body any, want int) *httptest.ResponseRecorder {
	t.Helper()
	encoded := []byte{}
	if body != nil {
		var err error
		encoded, err = json.Marshal(body)
		if err != nil {
			t.Fatal(err)
		}
	}
	req := httptest.NewRequest(method, path, strings.NewReader(string(encoded)))
	req.Header.Set("Accept", "application/json")
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("local-history-test", roleRoot, time.Now().Unix())})
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)
	if recorder.Code != want {
		t.Fatalf("%s %s: status %d, want %d: %s", method, path, recorder.Code, want, recorder.Body.String())
	}
	return recorder
}

type historyFlowReports struct {
	channels           ChannelManagementReport
	economics          channelEconomicsReport
	finance            financeOperatingReport
	financeCacheStates []string
}

func readHistoryFlowReports(t *testing.T, m *Monitor, router http.Handler, domain, day string) historyFlowReports {
	t.Helper()
	var result historyFlowReports
	for _, route := range []struct {
		path string
		out  any
	}{
		{"/channels/report", &result.channels},
		{"/channels/economics", &result.economics},
		{"/finance/report", &result.finance},
	} {
		path := route.path + "?from=" + day + "&to=" + day + "&domain=" + domain
		res := historyFlowRequest(t, m, router, http.MethodGet, path, nil, http.StatusOK)
		if route.path == "/finance/report" {
			// Serving the last good result while rebuilding is intentional. Wait
			// for its replacement; do not force-refresh past the cache contract.
			deadline := time.Now().Add(30 * time.Second)
			for {
				state := res.Header().Get("X-Monitor-Finance-Cache")
				result.financeCacheStates = append(result.financeCacheStates, state)
				if !strings.Contains(state, "stale") {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("finance cache did not replace its stale report")
				}
				time.Sleep(50 * time.Millisecond)
				res = historyFlowRequest(t, m, router, http.MethodGet, path, nil, http.StatusOK)
			}
		}
		if err := json.Unmarshal(res.Body.Bytes(), route.out); err != nil {
			t.Fatal(route.path, err)
		}
	}
	if !result.channels.Enabled || !result.channels.Finance.CanEdit || !result.channels.CostClosure.Enabled ||
		!result.economics.Enabled || !result.finance.Enabled {
		t.Fatal("normal page capabilities or reports are disabled")
	}
	return result
}

func saveHistoryFlowBinding(t *testing.T, m *Monitor, router http.Handler, input channelCostHistoricalBindingInput) channelCostHistoricalBindingPlan {
	t.Helper()
	res := historyFlowRequest(t, m, router, http.MethodPost, "/channels/cost/historical-bindings/preview", input, http.StatusOK)
	var plan channelCostHistoricalBindingPlan
	if err := json.Unmarshal(res.Body.Bytes(), &plan); err != nil {
		t.Fatal(err)
	}
	input.ValidFrom, input.ValidTo = historyRangeHour(plan.Binding.ValidFrom), historyRangeHour(plan.Binding.ValidTo)
	historyFlowRequest(t, m, router, http.MethodPost, "/channels/cost/historical-bindings", input, http.StatusOK)
	historyFlowRequest(t, m, router, http.MethodPost, "/channels/cost/historical-bindings", input, http.StatusConflict)
	return plan
}

func TestHistoricalSharedFullReportFlow(t *testing.T) {
	m, account, source, hours := newHistoryFlowFixture(t)
	router := historyFlowRouter(t, m, account.Domain)
	for _, path := range []string{"/", "/channels/cost/sources?domain=" + account.Domain, "/channels/cost/proposals?domain=" + account.Domain} {
		historyFlowRequest(t, m, router, http.MethodGet, path, nil, http.StatusOK)
	}
	before := readHistoryFlowReports(t, m, router, account.Domain, "2026-08-11")
	for i, hour := range hours {
		mode, channel := "allocated", 59
		if i == 1 {
			mode, channel = "shared", 0
		}
		plan := saveHistoryFlowBinding(t, m, router, channelCostHistoricalBindingInput{
			Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SourceRef: source,
			AllocationMode: mode, LocalChannelID: channel,
			ValidFrom: historyRangeHour(hour), ValidTo: historyRangeHour(hour + 3600),
			Reason: "isolated synthetic acceptance only",
		})
		if plan.EvidenceHours != 1 || plan.WillQueueHours != 1 || plan.EvidenceBilledCost.MicroUSD != "1000000" {
			t.Fatal("preview disagrees with the finite test evidence", plan)
		}
	}
	if got := historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours"); got != int64(len(hours)) {
		t.Fatal("save/retry queued duplicate or extra hours", got)
	}
	// A failed publication must retain the last good reports and durable work.
	// Reopening SQLite before retry also tests recovery without in-memory state.
	publicationCount := historyRangeCount(t, m.storeDB, "channel_economics_hour_publications")
	manifestCount := historyRangeCount(t, m.storeDB, "channel_economics_hour_manifest_publications")
	const injectedFailure = "history flow injected publication failure"
	if err := m.storeDB.Exec(`CREATE TEMP TRIGGER fail_history_flow BEFORE INSERT ON channel_economics_hour_manifest_publications
		BEGIN SELECT RAISE(ABORT, 'history flow injected publication failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.publishOneDueChannelEconomicsHourOrdered(context.Background(), account, time.Now().Unix()+1, true); err == nil || !strings.Contains(err.Error(), injectedFailure) {
		t.Fatal("expected publication failure was not observed", err)
	}
	if err := m.storeDB.Exec("DROP TRIGGER fail_history_flow").Error; err != nil {
		t.Fatal(err)
	}
	if publicationCount != historyRangeCount(t, m.storeDB, "channel_economics_hour_publications") ||
		manifestCount != historyRangeCount(t, m.storeDB, "channel_economics_hour_manifest_publications") {
		t.Fatal("failed publication left partial immutable rows")
	}
	m = restartHistoryFlowMonitor(t, m)
	router = historyFlowRouter(t, m, account.Domain)
	var dirty ChannelEconomicsDirtyHour
	if err := m.storeDB.First(&dirty, "hour_ts = ?", hours[0]).Error; err != nil || dirty.Attempts != 1 ||
		!strings.Contains(dirty.LastError, injectedFailure) ||
		historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != int64(len(hours)) {
		t.Fatal("failed work did not survive reopening the store", dirty, err)
	}
	retained := readHistoryFlowReports(t, m, router, account.Domain, "2026-08-11")
	if !reflect.DeepEqual(before.channels.Summary, retained.channels.Summary) ||
		!reflect.DeepEqual(before.economics.Totals, retained.economics.Totals) ||
		!reflect.DeepEqual(before.finance.Statement, retained.finance.Statement) {
		t.Fatal("failure/restart changed the last good report amounts")
	}
	for range hours {
		if err := m.publishOneDueChannelEconomicsHourOrdered(context.Background(), account, time.Now().Unix()+120, true); err != nil {
			t.Fatal(err)
		}
	}
	after := readHistoryFlowReports(t, m, router, account.Domain, "2026-08-11")
	if historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 0 ||
		!reflect.DeepEqual(before.channels.Summary, after.channels.Summary) ||
		before.finance.Statement.KnownUpstreamBilledCost != after.finance.Statement.KnownUpstreamBilledCost ||
		before.finance.Statement.KnownRawCorrectedUpstreamCost != after.finance.Statement.KnownRawCorrectedUpstreamCost {
		t.Fatal("attribution duplicated/lost bills or left work queued")
	}
	for _, totals := range []channelEconomicsTotalsView{before.economics.Totals, after.economics.Totals} {
		if totals.KnownRevenue.MicroUSD != "6000000" || totals.KnownUpstreamCost.MicroUSD != "3000000" || totals.KnownCorrectedCost.MicroUSD != "1200000" || totals.Profit != nil {
			t.Fatal("cost/revenue conservation or shared profit gate failed", totals)
		}
	}
	if before.economics.Totals.PairedPublicationRows != 0 || after.economics.Totals.PairedPublicationRows != 2 ||
		after.finance.Statement.PairedUserConsumption.MicroUSD != "4000000" ||
		after.finance.Statement.PairedCorrectedCost.MicroUSD != "800000" ||
		after.finance.Statement.KnownContributionProfit.MicroUSD != "3200000" ||
		after.finance.Statement.ContributionProfit != nil {
		t.Fatal("normal finance report retained stale pairing or invented full profit", after.finance.Statement)
	}
	// Reopen the database again after recovery: durable reports and the warm
	// cache must agree, including the still-unallocated shared-hour cost.
	restarted := restartHistoryFlowMonitor(t, m)
	reopened := readHistoryFlowReports(t, restarted, historyFlowRouter(t, restarted, account.Domain), account.Domain, "2026-08-11")
	if !reflect.DeepEqual(after.finance.Statement, reopened.finance.Statement) || !reflect.DeepEqual(after.economics.Totals, reopened.economics.Totals) {
		t.Fatal("reopened and warm-cache reports disagree")
	}
	t.Logf("PASS signed-route save/retry, publication rollback, durable queue recovery, cache replacement and SQLite reopen; cache states: %v", after.financeCacheStates)
}
