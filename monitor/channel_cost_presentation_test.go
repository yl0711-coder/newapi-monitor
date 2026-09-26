package monitor

import (
	"reflect"
	"testing"
)

func TestChannelInternalReasonsDoNotChangeMoneyOrPublication(t *testing.T) {
	metrics := ChannelUpstreamUsageMetrics{Available: true, CostUSD: 10, AdjustedCostAvailable: true, AdjustedCostUSD: 5}
	for _, tc := range []struct {
		name     string
		evidence financeInternalTestCostEvidence
		want     []string
	}{
		{"missing_source", financeInternalTestCostEvidence{}, []string{channelInternalSourceIncomplete}},
		{"mixed", financeInternalTestCostEvidence{SourceComplete: true, Events: []financeInternalTestCostEvent{{Domain: "a", State: "mixed"}}}, []string{channelInternalMixedUsage}},
		{"unknown_owner", financeInternalTestCostEvidence{SourceComplete: true, Events: []financeInternalTestCostEvent{{State: "unverified"}}}, []string{channelInternalPairingUnverified, channelInternalOwnershipUnknown}},
		{"other_domain", financeInternalTestCostEvidence{SourceComplete: true, Events: []financeInternalTestCostEvent{{Domain: "b", State: "mixed"}}}, nil},
		{"multiple", financeInternalTestCostEvidence{Events: []financeInternalTestCostEvent{{Domain: "a", State: "mixed"}, {Domain: "a", State: "mixed"}, {Domain: "a", State: "unverified"}}}, []string{channelInternalSourceIncomplete, channelInternalMixedUsage, channelInternalPairingUnverified}},
		{"inconsistent", financeInternalTestCostEvidence{SourceComplete: true, ByDomain: map[string]financeInternalTestCostFact{"a": {UpstreamCostMicroUSD: 20_000_000, CorrectedCostMicroUSD: 8_000_000}}}, []string{channelInternalAmountInconsistent}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := applyChannelInternalCostEvidence(metrics, tc.evidence, "a", 1)
			if !reflect.DeepEqual(got.InternalFilterReasons, tc.want) {
				t.Fatalf("reasons=%v want=%v", got.InternalFilterReasons, tc.want)
			}
			got.InternalFilterReasons = nil
			old := applyChannelInternalCostFilter(metrics, tc.evidence.ByDomain["a"], 1, financeInternalCostDomainComplete(tc.evidence, "a"))
			if !reflect.DeepEqual(got, old) {
				t.Fatalf("diagnostic changed money/availability: %+v vs %+v", got, old)
			}
			unconfigured := applyChannelInternalCostEvidence(metrics, tc.evidence, "a", 0)
			if len(unconfigured.InternalFilterReasons) != 0 || unconfigured.CostUSD != 10 || unconfigured.InternalFilterStatus != "not_configured" {
				t.Fatal("unconfigured filter became a fault")
			}
		})
	}
}
