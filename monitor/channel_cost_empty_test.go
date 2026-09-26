package monitor

import (
	"context"
	"strings"
	"testing"
)

func TestChannelCostEmptyHourPublishesValidUnits(t *testing.T) {
	db := newChannelCostTestStore(t)
	m := &Monitor{storeDB: db, cfg: Settings{ChannelCostClosureEnabled: true, ChannelCostClosureDomains: []string{"4sapi.com"}, ChannelCostHMACKeyID: "review-key"}}
	hour := int64(1787623200 - 1787623200%3600)
	account := ChannelUpstreamAccount{Domain: "4sapi.com", Provider: upstreamProviderNewAPI, BaseURL: "https://4sapi.com", UserID: 42}
	epoch := newAPIUpstreamAccountEpoch(account)
	checkpoint := ChannelCostPageCheckpoint{Domain: account.Domain, AccountEpoch: epoch, SemanticsVersion: channelCostEvidenceSemanticsVersion, HourTs: hour, NextPage: 2, Total: 0, SourceRows: 0, AggregatesJSON: "[]"}
	if err := db.Create(&checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	pricing := ChannelUpstreamPricingHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour, Status: "verified", ReconcileStatus: "matched", FinalQuota: 0}
	if err := m.publishChannelCostHourFromCheckpoint(context.Background(), account, pricing, hour+3600); err != nil {
		t.Fatal(err)
	}
	var state ChannelUpstreamCostHourState
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	t.Logf("zero usage produces status=%s units=%q requests=%d", state.Status, state.ChargeUnitsPerUSD, state.Requests)
	if strings.TrimSpace(state.ChargeUnitsPerUSD) == "" {
		t.Fatal("empty cost checkpoint publishes a state rejected by next historical-unit validation")
	}
	if err := db.Create(&checkpoint).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.publishChannelCostHourFromCheckpoint(context.Background(), account, pricing, hour+3660); err != nil {
		t.Fatal(err)
	}
	if err := db.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if state.Status != "verified" || state.VerifiedScans != 2 {
		t.Fatalf("empty hour did not verify: %+v", state)
	}
	unit, err := channelCostFrozenUnit(state)
	if err != nil || unit != "" {
		t.Fatalf("empty observation must not freeze a unit for late arriving consumption: %q %v", unit, err)
	}
}

func TestChannelCostFrozenUnitRepairsOnlyEmptyLegacyState(t *testing.T) {
	base := ChannelUpstreamCostHourState{Status: "observed", ReconcileStatus: "matched"}
	if unit, err := channelCostFrozenUnit(base); err != nil || unit != "" {
		t.Fatalf("legacy zero hour rejected: %q %v", unit, err)
	}
	for _, field := range []string{"request", "row", "control", "evidence", "delta", "mismatch"} {
		t.Run(field, func(t *testing.T) {
			s := base
			switch field {
			case "request":
				s.Requests = 1
			case "row":
				s.EvidenceRows = 1
			case "control":
				s.ControlChargeUnits = 1
			case "evidence":
				s.EvidenceChargeUnits = 1
			case "delta":
				s.ReconcileDelta = 1
			case "mismatch":
				s.ReconcileStatus = "mismatch"
			}
			if _, err := channelCostFrozenUnit(s); err == nil {
				t.Fatal("invalid nonempty/unmatched state silently repaired")
			}
		})
	}
	base.Requests = 1
	base.ChargeUnitsPerUSD = "500000"
	if unit, err := channelCostFrozenUnit(base); err != nil || unit != "500000" {
		t.Fatalf("nonempty historical unit lost: %q %v", unit, err)
	}
}
