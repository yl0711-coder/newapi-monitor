package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func historicalSharedBody(in channelCostHistoricalBindingInput) map[string]any {
	return map[string]any{"domain": in.Domain, "account_epoch": in.AccountEpoch, "source_ref": in.SourceRef,
		"allocation_mode": "shared", "local_channel_id": 0, "reason": "confirmed shared token; no channel allocation",
		"valid_from": 0, "valid_to": 10800}
}

func TestHistoricalSharedBindingPreviewAndFiniteSave(t *testing.T) {
	m, in := historyRangeFixture(t)
	body := historicalSharedBody(in)
	// A shared source does not require a surviving channel or infer one from
	// today's configuration. It also must not use channel 0 as a real channel.
	if err := m.storeDB.Where("id = ?", in.LocalChannelID).Delete(&ChannelSnap{}).Error; err != nil {
		t.Fatal(err)
	}
	response := previewChannelCostHistoricalBinding(t, m, body)
	var plan channelCostHistoricalBindingPlan
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &plan) != nil {
		t.Fatal("shared preview failed", response.Code, response.Body.String())
	}
	if plan.Binding.AllocationMode != "shared" || plan.Binding.LocalChannelID != 0 ||
		plan.Binding.ValidFrom != 3600 || plan.Binding.ValidTo != 10800 || plan.Binding.ValidTo == 0 ||
		plan.EvidenceHours != 2 || plan.EvidenceRequests != 5 || plan.EvidenceBilledCost.MicroUSD != "3000000" || plan.WillQueueHours != 1 {
		t.Fatal("shared preview changed evidence or selected scope", plan)
	}
	if plan.TemporalOverlapQuality != "shared" || plan.HasLocalActivity || len(plan.RiskWarnings) < 2 ||
		!strings.Contains(strings.Join(plan.RiskWarnings, " "), "内部测试") {
		t.Fatal("shared preview implies a verified channel or test deduction", plan)
	}
	if historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 0 || historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 0 {
		t.Fatal("preview wrote state")
	}
	body["valid_from"], body["valid_to"] = plan.Binding.ValidFrom, plan.Binding.ValidTo
	for attempt, want := range []int{http.StatusOK, http.StatusConflict} {
		if result := postChannelCostHistoricalBinding(t, m, body); result.Code != want {
			t.Fatal("save/retry mismatch", attempt, result.Code, result.Body.String())
		}
	}
	var binding ChannelCostSourceBinding
	if err := m.storeDB.First(&binding).Error; err != nil || binding.AllocationMode != "shared" || binding.LocalChannelID != 0 || binding.MappingSource != "manual_history" {
		t.Fatal("shared audit missing", binding, err)
	}
	var queued []int64
	if err := m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Pluck("hour_ts", &queued).Error; err != nil || len(queued) != 1 || queued[0] != 3600 {
		t.Fatal("unverified or out-of-scope hours queued", queued, err)
	}
	if historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 1 {
		t.Fatal("retry duplicated history")
	}
	if _, err := m.costSourceBindingAt(context.Background(), in.Domain, in.AccountEpoch, in.SourceRef, 10800); err == nil {
		t.Fatal("finite history leaked into the next hour")
	}
}

func TestHistoricalSharedBindingRejectsAmbiguousInputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		edit func(map[string]any)
	}{
		{"channel guess", func(b map[string]any) { b["local_channel_id"] = 59 }},
		{"negative channel", func(b map[string]any) { b["local_channel_id"] = -1 }},
		{"implicit lifetime", func(b map[string]any) { delete(b, "valid_from"); delete(b, "valid_to") }},
		{"open interval", func(b map[string]any) { delete(b, "valid_to") }},
		{"unknown mode", func(b map[string]any) { b["allocation_mode"] = "auto"; b["local_channel_id"] = 59 }},
		{"unallocated is not shared", func(b map[string]any) { b["allocation_mode"] = "unallocated"; b["local_channel_id"] = 59 }},
		{"missing audit reason", func(b map[string]any) { b["reason"] = " " }},
		{"missing source evidence", func(b map[string]any) { b["source_ref"] = strings.Repeat("d", 64) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, in := historyRangeFixture(t)
			body := historicalSharedBody(in)
			tc.edit(body)
			for _, call := range []func() int{
				func() int { return previewChannelCostHistoricalBinding(t, m, body).Code },
				func() int { return postChannelCostHistoricalBinding(t, m, body).Code },
			} {
				if status := call(); status != http.StatusBadRequest {
					t.Fatal("ambiguous request accepted", status)
				}
			}
			if historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 0 || historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 0 {
				t.Fatal("rejected input wrote state")
			}
		})
	}
}

func TestHistoricalSharedHistoryKeepsAdjacentExclusiveBindings(t *testing.T) {
	m, in := historyRangeFixture(t)
	previous := ChannelCostSourceBinding{Domain: in.Domain, AccountEpoch: in.AccountEpoch, SourceRef: in.SourceRef,
		Provider: upstreamProviderNewAPI, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "test-key",
		ValidFrom: 0, ValidTo: 3600, Status: "confirmed", AllocationMode: "allocated", LocalChannelID: 59, Reason: "earlier exclusive period", CreatedAt: 1}
	if err := m.saveCostSourceBinding(context.Background(), previous); err != nil {
		t.Fatal(err)
	}
	body := historicalSharedBody(in)
	body["valid_from"], body["valid_to"] = 3600, 7200
	if res := postChannelCostHistoricalBinding(t, m, body); res.Code != http.StatusOK {
		t.Fatal("shared period rejected next to earlier exclusive period", res.Code, res.Body.String())
	}
	in.ValidFrom, in.ValidTo = historyRangeHour(7200), historyRangeHour(10800)
	if res := postChannelCostHistoricalBinding(t, m, in); res.Code != http.StatusOK {
		t.Fatal("legacy exclusive request rejected next to shared period", res.Code, res.Body.String())
	}
	for _, tc := range []struct {
		hour    int64
		mode    string
		channel int
	}{{0, "allocated", 59}, {3600, "shared", 0}, {7200, "allocated", 59}} {
		got, err := m.costSourceBindingAt(context.Background(), in.Domain, in.AccountEpoch, in.SourceRef, tc.hour)
		if err != nil || got.AllocationMode != tc.mode || got.LocalChannelID != tc.channel {
			t.Fatal("historical periods crossed", got, err)
		}
		if tc.hour == 0 && got != previous {
			t.Fatal("previous audit was mutated", got)
		}
	}
	body["valid_from"], body["valid_to"] = 3600, 10800
	if res := postChannelCostHistoricalBinding(t, m, body); res.Code != http.StatusConflict {
		t.Fatal("shared entry may not override already confirmed exclusive history", res.Code)
	}
	if count := historyRangeCount(t, m.storeDB, "channel_cost_source_bindings"); count != 3 {
		t.Fatal("unexpected history count", count)
	}
}

func TestHistoricalSharedSourceListDoesNotImplyCurrentSharedOwnership(t *testing.T) {
	m, in := historyRangeFixture(t)
	account := ChannelUpstreamAccount{Domain: in.Domain, Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1}
	if err := m.storeDB.AutoMigrate(&ChannelUpstreamAccount{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	epoch := newAPIUpstreamAccountEpoch(account)
	for _, table := range []string{"channel_upstream_cost_hour_evidence", "channel_upstream_cost_hour_states"} {
		if err := m.storeDB.Table(table).Where("account_epoch = ?", in.AccountEpoch).Update("account_epoch", epoch).Error; err != nil {
			t.Fatal(err)
		}
	}
	in.AccountEpoch = epoch
	body := historicalSharedBody(in)
	if res := postChannelCostHistoricalBinding(t, m, body); res.Code != http.StatusOK {
		t.Fatal("shared save failed", res.Code, res.Body.String())
	}
	router := gin.New()
	router.GET("/sources", m.listChannelCostSourcesHandler)
	res := httptest.NewRecorder()
	router.ServeHTTP(res, httptest.NewRequest(http.MethodGet, "/sources?domain="+in.Domain, nil))
	var payload struct {
		Sources []channelCostSourceView `json:"sources"`
	}
	if res.Code != http.StatusOK || json.Unmarshal(res.Body.Bytes(), &payload) != nil || len(payload.Sources) != 1 {
		t.Fatal("source list failed", res.Code, res.Body.String())
	}
	view := payload.Sources[0]
	if view.HistoricalBindingCount != 1 || view.HistoricalSharedBindingCount != 1 || view.CurrentBinding != nil || view.AttributionState != "unattributable" {
		t.Fatal("historical limitation became a current mapping", view)
	}
}

func TestHistoricalSharedBindingPreservesSafetyGatesAndRollsBack(t *testing.T) {
	for _, mode := range []string{"snapshot", "disabled", "other domain", "queue failure", "bad unit", "mixed identity"} {
		t.Run(mode, func(t *testing.T) {
			m, in := historyRangeFixture(t)
			body := historicalSharedBody(in)
			want := http.StatusServiceUnavailable
			switch mode {
			case "snapshot":
				m.cfg.LocalSnapshotOnly = true
				if res := previewChannelCostHistoricalBinding(t, m, body); res.Code != http.StatusOK {
					t.Fatal("snapshot preview unavailable", res.Code)
				}
			case "disabled":
				m.cfg.ChannelCostClosureEnabled = false
			case "other domain":
				m.cfg.ChannelCostClosureDomains = []string{"other.example"}
			case "queue failure":
				want = http.StatusConflict
				if err := m.storeDB.Exec(`CREATE TRIGGER fail_shared_queue BEFORE INSERT ON channel_economics_dirty_hours BEGIN SELECT RAISE(ABORT, 'injected queue failure'); END`).Error; err != nil {
					t.Fatal(err)
				}
			case "bad unit", "mixed identity":
				want = http.StatusConflict
				column, value := "charge_units_per_usd", "invalid"
				if mode == "mixed identity" {
					column, value = "hmac_key_id", "different-key"
				}
				if err := m.storeDB.Model(&ChannelUpstreamCostHourEvidence{}).Where("hour_ts = ?", 7200).Update(column, value).Error; err != nil {
					t.Fatal(err)
				}
			}
			if res := postChannelCostHistoricalBinding(t, m, body); res.Code != want {
				t.Fatal("safety gate mismatch", res.Code, res.Body.String())
			}
			if historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 0 || historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 0 {
				t.Fatal("rejected/failed save left partial writes")
			}
		})
	}
}

func TestHistoricalSharedCostConservedWithoutChannelProfit(t *testing.T) {
	db, before := publishEconomicsCoverageFixture(t, economicsCoverageFixture{true, false, true, true, true, false})
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}}}
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 1, BalanceUnit: quotaPerUSD}
	in := channelCostHistoricalBindingInput{Domain: account.Domain, AccountEpoch: newAPIUpstreamAccountEpoch(account), SourceRef: strings.Repeat("a", 64)}
	body := historicalSharedBody(in)
	if res := postChannelCostHistoricalBinding(t, m, body); res.Code != http.StatusOK {
		t.Fatal("shared save failed", res.Code, res.Body.String())
	}
	// Simulate process restart: no in-memory attribution state survives.
	m = &Monitor{storeDB: db, cfg: m.cfg}
	for i := 0; i < 2; i++ {
		if err := m.publishChannelEconomicsHour(context.Background(), account, 3600, "shared_history", int64(10800+i)); err != nil {
			t.Fatal(err)
		}
	}
	var after []ChannelEconomicsHourPublication
	if err := db.Table("channel_economics_hour_publications p").Select("p.*").
		Joins("JOIN channel_economics_hour_current c ON c.publication_id = p.publication_id").Order("p.local_channel_id").Scan(&after).Error; err != nil {
		t.Fatal(err)
	}
	shared := publicationForChannel(t, after, 0)
	if shared.UpstreamCostMicroUSD != 1000000 || shared.CorrectedCostMicroUSD != 100000 || !shared.CorrectedCostKnown || shared.ProfitKnown {
		t.Fatal("shared cost lost or fictional profit published", shared)
	}
	channel := publicationForChannel(t, after, 59)
	if channel.UpstreamCostMicroUSD != 0 || channel.CorrectedCostKnown || channel.ProfitKnown || channel.RevenueMicroUSD != 2000000 {
		t.Fatal("shared evidence guessed a channel allocation", channel)
	}
	if len(before) != len(after) || historyRangeCount(t, db, "channel_economics_hour_publications") != int64(len(before)) {
		t.Fatal("unchanged costs republished or duplicated")
	}
	// Keep the test sensitive to the immutable per-channel publication data.
	for i := range before {
		if before[i].PublicationID != after[i].PublicationID {
			t.Fatal("shared labeling changed previously published amounts")
		}
	}
}
