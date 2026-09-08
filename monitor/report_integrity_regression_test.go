package monitor

import (
	"context"
	"testing"
	"time"
)

func TestEmptyUpstreamWindowKeepsExpectedCoverage(t *testing.T) {
	m := newStabilityTestMonitor(t)
	start := time.Date(2026, 9, 6, 0, 0, 0, 0, cstLocation).Unix()
	accounts := map[string]ChannelUpstreamAccountView{"hourly.example": {
		Configured: true, Provider: upstreamProviderTokenForce, UsageSyncEnabled: true,
	}}
	rows, err := m.loadChannelUpstreamUsage(context.Background(), stabilityScope{FromTs: start, ToTs: start + 86400}, start+2*86400, accounts, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	got := rows["hourly.example"]
	if got.Available || got.Complete || got.ExpectedHours != 24 || got.CompletedHours != 0 || got.Granularity != "hour" {
		t.Fatalf("missing bill must be 0/24 unknown, not an empty 0/0 window: %+v", got)
	}
}

func TestStabilityComparisonRequiresBothPeriodsFullyFinalized(t *testing.T) {
	for _, scenario := range []string{"missing_current_hour", "provisional_tail", "complete"} {
		t.Run(scenario, func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			start := time.Date(2026, 9, 6, 0, 0, 0, 0, cstLocation).Unix()
			for _, hour := range []int64{start - 86400, start - 86400 + 3600, start, start + 3600} {
				if scenario == "missing_current_hour" && hour == start+3600 {
					continue
				}
				if err := m.replaceStabilityHourTraffic(hour,
					[]StabilityHourSample{{HourTs: hour, ChannelID: 1, ModelName: "model", Grp: "group", Success: 9, Failed: 1}}, nil,
					StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 10}); err != nil {
					t.Fatal(err)
				}
			}
			now := start + 5*3600
			if scenario == "provisional_tail" {
				now = start + 2*3600 + 1800
			}
			r, err := m.buildStabilityReport(context.Background(), stabilityScope{FromTs: start, ToTs: start + 7200}, now)
			if err != nil {
				t.Fatal(err)
			}
			want := scenario == "complete"
			if r.Meta.ComparisonAvailable != want || (r.DeltaPP != nil) != want {
				t.Fatalf("comparison availability=%v delta=%v coverage=%+v", r.Meta.ComparisonAvailable, r.DeltaPP, r.Meta.DataCoverage)
			}
			if !want {
				for _, g := range r.Groups {
					if g.DeltaPP != nil {
						t.Fatal("partial current period leaked group comparison")
					}
				}
			}
		})
	}
}
