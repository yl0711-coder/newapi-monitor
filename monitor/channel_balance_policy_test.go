package monitor

import (
	"encoding/json"
	"testing"
)

func TestUpstreamBalanceSummaryExcludesOnlyOwnedDomain(t *testing.T) {
	for _, tc := range []struct {
		domain   string
		excluded bool
	}{
		{"modelapi.link", true}, {"https://MODELAPI.link.:443/v1", true}, {"api.modelapi.link", true},
		{"modelapi.link.example.com", false}, {"notmodelapi.link", false}, {"4sapi.com", false},
	} {
		t.Run(tc.domain, func(t *testing.T) {
			view := upstreamAccountView(ChannelUpstreamAccount{Domain: tc.domain, BalanceKnown: true, BalanceUSD: 123, Enabled: true})
			payload, err := json.Marshal(view)
			if err != nil {
				t.Fatal(err)
			}
			var fields map[string]any
			if err := json.Unmarshal(payload, &fields); err != nil {
				t.Fatal(err)
			}
			if got := fields["exclude_from_balance_summary"] == true; got != tc.excluded {
				t.Errorf("excluded=%v want %v", got, tc.excluded)
			}
			if view.BalanceUSD == nil || *view.BalanceUSD != 123 || !view.Configured {
				t.Fatal("exclusion must preserve account/detail balance")
			}
		})
	}
}
