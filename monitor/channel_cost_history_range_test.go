package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func historyRangeHour(value int64) *int64 { return &value }

func historyRangeCount(t *testing.T, db *gorm.DB, table string) int64 {
	t.Helper()
	var count int64
	if err := db.Table(table).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	return count
}

func historyRangeFixture(t *testing.T) (*Monitor, channelCostHistoricalBindingInput) {
	t.Helper()
	db := newChannelCostTestStore(t)
	if err := db.AutoMigrate(&ChannelSnap{}, &StabilityHourSample{}); err != nil {
		t.Fatal(err)
	}
	in := channelCostHistoricalBindingInput{Domain: "4sapi.com", AccountEpoch: strings.Repeat("a", 64),
		SourceRef: strings.Repeat("b", 64), LocalChannelID: 59, Reason: "isolated finite-range acceptance"}
	if err := db.Create(&ChannelSnap{ID: 59, Name: "history", BaseDomain: in.Domain}).Error; err != nil {
		t.Fatal(err)
	}
	for index, hour := range []int64{3600, 7200, (91*24 + 1) * 3600} {
		row := ChannelUpstreamCostHourEvidence{Domain: in.Domain, AccountEpoch: in.AccountEpoch, SourceRef: in.SourceRef,
			HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, DimensionHash: strings.Repeat("c", 64),
			Provider: upstreamProviderNewAPI, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "test-key",
			Requests: int64(index + 2), ChargeUnits: int64(index+1) * 500_000, ChargeUnitsPerUSD: "500000"}
		if err := db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		status := "verified"
		if index == 1 {
			status = "observed"
		}
		if err := db.Create(&ChannelUpstreamCostHourState{Domain: in.Domain, AccountEpoch: in.AccountEpoch, HourTs: hour,
			SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: row.Provider, Status: status, ReconcileStatus: "matched"}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&StabilityHourSample{ChannelID: 59, HourTs: hour, Success: int64(index + 2), ModelName: "model", Grp: "group", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		if index > 0 {
			if err := db.Create(&ChannelTestHourSample{ChannelID: 59, HourTs: hour, ModelName: "model", Grp: "test", Origin: "scheduled",
				TrafficClassVersion: stabilityTrafficClassificationVersion, Requests: int64(index + 2)}).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	return &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{in.Domain}}}, in
}

func TestHistoricalCostRangeRejectsInvalidBoundsBeforeReading(t *testing.T) {
	for _, bounds := range [][2]*int64{
		{historyRangeHour(3600), nil}, {nil, historyRangeHour(7200)},
		{historyRangeHour(-3600), historyRangeHour(7200)}, {historyRangeHour(3601), historyRangeHour(7200)},
		{historyRangeHour(3600), historyRangeHour(7201)}, {historyRangeHour(7200), historyRangeHour(3600)},
		{historyRangeHour(3600), historyRangeHour(3600)}, {historyRangeHour(0), historyRangeHour((90*24 + 1) * 3600)},
		{historyRangeHour(0), historyRangeHour(10800)},
	} {
		in := channelCostHistoricalBindingInput{ValidFrom: bounds[0], ValidTo: bounds[1]}
		if validateChannelCostHistoricalRange(in, 7200) == nil {
			t.Fatal("invalid explicit interval was accepted")
		}
	}
	for _, in := range []channelCostHistoricalBindingInput{{}, {ValidFrom: historyRangeHour(0), ValidTo: historyRangeHour(90 * 24 * 3600)}} {
		if err := validateChannelCostHistoricalRange(in, 90*24*3600); err != nil {
			t.Fatal("legacy request or exact maximum range rejected", err)
		}
	}
}

func TestHistoricalCostRangeFiltersAllEvidenceAndAllowsDisjointContinuation(t *testing.T) {
	m, in := historyRangeFixture(t)
	if response := previewChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusRequestEntityTooLarge {
		t.Fatal("unbounded historical span must retain its legacy safety limit", response.Code)
	}
	in.ValidFrom, in.ValidTo = historyRangeHour(0), historyRangeHour(10800)
	response := previewChannelCostHistoricalBinding(t, m, in)
	var plan channelCostHistoricalBindingPlan
	if response.Code != http.StatusOK || json.Unmarshal(response.Body.Bytes(), &plan) != nil {
		t.Fatal("bounded preview failed", response.Code, response.Body.String())
	}
	if plan.Binding.ValidFrom != 3600 || plan.Binding.ValidTo != 10800 || plan.EvidenceHours != 2 || plan.EvidenceRequests != 5 ||
		plan.EvidenceBilledCost.MicroUSD != "3000000" || plan.LocalActiveHours != 2 || plan.LocalRequests != 5 ||
		plan.LocalTestActiveHours != 1 || plan.LocalTestRequests != 3 || plan.WillQueueHours != 1 {
		t.Fatal("range filter omitted an evidence, activity or queue query", plan)
	}
	if historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 0 || historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 0 {
		t.Fatal("preview wrote state")
	}
	in.ValidFrom, in.ValidTo = &plan.Binding.ValidFrom, &plan.Binding.ValidTo
	if response := postChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusOK {
		t.Fatal("finite save failed", response.Code, response.Body.String())
	}
	if response := postChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusConflict {
		t.Fatal("overlapping range was not rejected")
	}
	next := (int64(91*24) + 1) * 3600
	in.ValidFrom, in.ValidTo = historyRangeHour(next), historyRangeHour(next+3600)
	if response := postChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusOK {
		t.Fatal("disjoint continuation incorrectly blocked by earlier binding", response.Code, response.Body.String())
	}
	var queued []int64
	if err := m.storeDB.Model(&ChannelEconomicsDirtyHour{}).Order("hour_ts").Pluck("hour_ts", &queued).Error; err != nil ||
		len(queued) != 2 || queued[0] != 3600 || queued[1] != next || historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 2 {
		t.Fatal("saved ranges enqueued unverified, absent or out-of-range hours", queued, err)
	}
}

func TestHistoricalCostRangeDoesNotExpandWhenLaterEvidenceArrives(t *testing.T) {
	m, in := historyRangeFixture(t)
	in.ValidFrom, in.ValidTo = historyRangeHour(0), historyRangeHour(7200)
	plan, status, err := m.planChannelCostHistoricalBinding(context.Background(), in, "test")
	if err != nil || status != http.StatusOK || plan.EvidenceHours != 1 {
		t.Fatal("finite preview failed", status, err)
	}
	in.ValidFrom, in.ValidTo = &plan.Binding.ValidFrom, &plan.Binding.ValidTo
	// An already present adjacent hour is outside this preview, regardless of
	// whether it becomes reconciled while the confirmation dialog is open.
	if err := m.storeDB.Model(&ChannelUpstreamCostHourState{}).Where("hour_ts=?", 7200).Update("status", "verified").Error; err != nil {
		t.Fatal(err)
	}
	if response := postChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusOK {
		t.Fatal("save failed", response.Code, response.Body.String())
	}
	var binding ChannelCostSourceBinding
	if err := m.storeDB.First(&binding).Error; err != nil || binding.ValidFrom != 3600 || binding.ValidTo != 7200 || historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 1 {
		t.Fatal("save silently widened the confirmed range", binding, err)
	}
}

func TestHistoricalCostRangeKeepsSnapshotAndMissingEvidenceGuards(t *testing.T) {
	m, in := historyRangeFixture(t)
	m.cfg.LocalSnapshotOnly = true
	in.ValidFrom, in.ValidTo = historyRangeHour(3600), historyRangeHour(7200)
	if response := previewChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusOK {
		t.Fatal("local finite preview rejected", response.Code)
	}
	if response := postChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusServiceUnavailable {
		t.Fatal("explicit range bypassed local snapshot write protection", response.Code)
	}
	in.ValidFrom, in.ValidTo = historyRangeHour(10800), historyRangeHour(14400)
	if response := previewChannelCostHistoricalBinding(t, m, in); response.Code != http.StatusBadRequest {
		t.Fatal("empty interval was treated as zero-cost evidence", response.Code)
	}
	if historyRangeCount(t, m.storeDB, "channel_cost_source_bindings") != 0 || historyRangeCount(t, m.storeDB, "channel_economics_dirty_hours") != 0 {
		t.Fatal("read-only preview or denied write changed state")
	}
}
