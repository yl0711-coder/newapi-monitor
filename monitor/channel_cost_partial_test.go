package monitor

import (
	"math"
	"reflect"
	"testing"
)

func TestChannelPartialAdjustedCostDoesNotPublishCompleteCost(t *testing.T) {
	const start int64 = 1788192000
	const domain = "upstream.example"
	account := ChannelUpstreamAccountView{Provider: upstreamProviderSub2API, UsageSyncEnabled: true, UsageGranularity: "hour"}
	rows := []ChannelUpstreamUsageHour{
		{Domain: domain, Provider: account.Provider, HourTs: start, CostUSD: 100},
		{Domain: domain, Provider: account.Provider, HourTs: start + 3600, CostUSD: 20},
		{Domain: domain, Provider: account.Provider, HourTs: start + 7200, CostUSD: 10},
	}
	versions := []channelRechargeVersion{
		{Version: 1, EffectiveAt: start + 3600, Paid: 2.5, Credit: 8, Valid: true},
		{Version: 2, EffectiveAt: start + 7200, Paid: 7, Credit: 1, Valid: true},
	}
	before := append([]ChannelUpstreamUsageHour(nil), rows...)
	got, err := projectChannelUpstreamUsageWindow(rows, stabilityScope{FromTs: start, ToTs: start + 10800}, start+14400,
		map[string]ChannelUpstreamAccountView{domain: account}, map[string][]channelRechargeVersion{domain: versions})
	if err != nil {
		t.Fatal(err)
	}
	m := got[domain]
	if !m.Complete || m.CostUSD != 130 || m.AdjustedCostAvailable || m.AdjustedCostUSD != 0 || m.AdjustedCostStatus != upstreamAdjustedCostMissingHistory {
		t.Fatalf("full publication contract changed: %+v", m)
	}
	want := ChannelAdjustedCostBreakdown{KnownCostUSD: 76.25, UnresolvedBillUSD: 100, KnownBuckets: 2, UnresolvedBuckets: 1}
	if m.AdjustedCostBreakdown == nil || *m.AdjustedCostBreakdown != want {
		t.Fatalf("partial cost: %+v", m.AdjustedCostBreakdown)
	}
	// Presentation metadata must not enable internal-adjusted/business publication.
	net := applyChannelInternalCostEvidence(m, financeInternalTestCostEvidence{SourceComplete: true}, domain, 1)
	if net.BusinessAdjustedCostAvailable || net.BusinessAdjustedCostUSD != 0 {
		t.Fatalf("subtotal promoted to net cost: %+v", net)
	}
	if !reflect.DeepEqual(rows, before) {
		t.Fatal("projection mutated bill facts")
	}
}

func TestChannelPartialAdjustedCostBoundaries(t *testing.T) {
	const start int64 = 1788192000
	const domain = "upstream.example"
	base := []ChannelUpstreamUsageHour{
		{Domain: domain, Provider: upstreamProviderSub2API, HourTs: start, CostUSD: 100},
		{Domain: domain, Provider: upstreamProviderSub2API, HourTs: start + 3600, CostUSD: 20},
	}
	for _, tc := range []struct {
		name               string
		mutate             func([]ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour
		versions           []channelRechargeVersion
		want               *ChannelAdjustedCostBreakdown
		complete, adjusted bool
	}{
		{"complete", nil, []channelRechargeVersion{{EffectiveAt: start, Paid: 1, Credit: 10, Valid: true}}, &ChannelAdjustedCostBreakdown{KnownCostUSD: 12, KnownBuckets: 2}, true, true},
		{"all_unknown", nil, nil, &ChannelAdjustedCostBreakdown{UnresolvedBillUSD: 120, UnresolvedBuckets: 2}, true, false},
		{"mid_bucket_change", nil, []channelRechargeVersion{{EffectiveAt: start, Paid: 1, Credit: 10, Valid: true}, {EffectiveAt: start + 1800, Paid: 1, Credit: 5, Valid: true}}, &ChannelAdjustedCostBreakdown{KnownCostUSD: 4, UnresolvedBillUSD: 100, KnownBuckets: 1, UnresolvedBuckets: 1}, true, false},
		{"zero_is_known_without_ratio", func(r []ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour { r[0].CostUSD = 0; return r }, nil, &ChannelAdjustedCostBreakdown{UnresolvedBillUSD: 20, KnownBuckets: 1, UnresolvedBuckets: 1}, true, false},
		{"provisional_not_complete", func(r []ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour { r[0].Provisional = true; return r }, []channelRechargeVersion{{EffectiveAt: start + 3600, Paid: 1, Credit: 10, Valid: true}}, &ChannelAdjustedCostBreakdown{KnownCostUSD: 2, UnresolvedBillUSD: 100, KnownBuckets: 1, UnresolvedBuckets: 1}, false, false},
		{"overlap_discards_subtotal", func(r []ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour { r[0].BucketSeconds = 7200; return r }, nil, nil, false, false},
		{"invalid_discards_subtotal", func(r []ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour { r[1].CostUSD = math.NaN(); return r }, nil, nil, false, false},
		{"no_rows_unknown_not_zero", func(r []ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour { return nil }, nil, nil, false, false},
		{"subscription_not_cash", func(r []ChannelUpstreamUsageHour) []ChannelUpstreamUsageHour {
			for i := range r {
				r[i].Provider = upstreamProviderOpenOx
			}
			return r
		}, nil, nil, true, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rows := append([]ChannelUpstreamUsageHour(nil), base...)
			if tc.mutate != nil {
				rows = tc.mutate(rows)
			}
			provider := upstreamProviderSub2API
			if len(rows) > 0 {
				provider = rows[0].Provider
			}
			result, err := projectChannelUpstreamUsageWindow(rows, stabilityScope{FromTs: start, ToTs: start + 7200}, start+10800,
				map[string]ChannelUpstreamAccountView{domain: {Provider: provider, UsageSyncEnabled: true, UsageGranularity: "hour"}}, map[string][]channelRechargeVersion{domain: tc.versions})
			if err != nil {
				t.Fatal(err)
			}
			got := result[domain]
			if !reflect.DeepEqual(got.AdjustedCostBreakdown, tc.want) || got.Complete != tc.complete || got.AdjustedCostAvailable != tc.adjusted {
				t.Fatalf("got=%+v breakdown=%+v want=%+v", got, got.AdjustedCostBreakdown, tc.want)
			}
		})
	}
}
