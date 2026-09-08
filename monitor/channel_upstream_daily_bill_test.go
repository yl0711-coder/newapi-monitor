package monitor

import (
	"context"
	"math"
	"testing"
	"time"
)

func TestNaturalDayBillingScope(t *testing.T) {
	day := time.Date(2026, 9, 7, 0, 0, 0, 0, cstLocation).Unix()
	now := day + 18*3600 + 19*60
	for _, tc := range []struct {
		name                       string
		from, to, wantFrom, wantTo int64
	}{
		{"yesterday", day - 86400, day, day - 86400, day},
		{"today", day, day + 18*3600, day, now},
		{"rolling24", day - 6*3600, day + 18*3600, day - 86400, now},
		{"historicalHours", day - 30*3600, day - 26*3600, day - 2*86400, day - 86400},
		{"midnightExclusive", day - 2*86400, day - 86400, day - 2*86400, day - 86400},
		{"empty", day, day, 0, 0},
		{"future", day + 86400, day + 2*86400, 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := naturalDayBillingScope(stabilityScope{FromTs: tc.from, ToTs: tc.to}, now)
			if got.FromTs != tc.wantFrom || got.ToTs != tc.wantTo || got.RangeHours != 0 {
				t.Fatalf("scope=%+v", got)
			}
		})
	}
}

func TestNaturalDayBillSurvivesEveryTailRefreshWithoutLeakingIntoHourlyTotals(t *testing.T) {
	m := newStabilityTestMonitor(t)
	ctx := context.Background()
	day := time.Date(2026, 9, 7, 0, 0, 0, 0, cstLocation).Unix()
	accounts := map[string]ChannelUpstreamAccountView{
		"aicodewith.com": {Configured: true, Provider: upstreamProviderAICodeWith, UsageSyncEnabled: true},
		"sub2.example":   {Configured: true, Provider: upstreamProviderSub2API, UsageSyncEnabled: true, UsageGranularity: "day"},
	}
	for domain, account := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, day-86400, 1, 2)
		row := ChannelUpstreamUsageHour{Domain: domain, HourTs: day - 86400, BucketSeconds: 86400, CostUSD: 397.8902, Provider: account.Provider}
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	// Includes both sides of an hour boundary and the exact production 18:17
	// sample. Each successful refresh must remain visible at its true scope.
	for _, tail := range []int64{17*3600 + 59*60, 18*3600 + 17*60, 18*3600 + 45*60, 19 * 3600} {
		now := day + tail + 120
		to := (day + tail) / 3600 * 3600
		scope := stabilityScope{FromTs: to - 86400, ToTs: to}
		for domain, account := range accounts {
			row := ChannelUpstreamUsageHour{Domain: domain, HourTs: day, BucketSeconds: tail, CostUSD: 372.4261, FetchedAt: day + tail, Provider: account.Provider}
			if err := m.storeDB.Save(&row).Error; err != nil {
				t.Fatal(err)
			}
		}
		exact, err := m.loadChannelUpstreamUsage(ctx, scope, now, accounts, channelFinanceSnapshot{})
		if err != nil {
			t.Fatal(err)
		}
		bills, err := m.loadChannelUpstreamNaturalDayBills(ctx, scope, now, accounts, channelFinanceSnapshot{})
		if err != nil {
			t.Fatal(err)
		}
		for domain := range accounts {
			if exact[domain].CostUSD != 0 || exact[domain].AdjustedCostAvailable || exact[domain].IntegrityStatus != upstreamUsageIntegrityWindowMismatch {
				t.Fatalf("%s daily bill leaked into hourly total: %+v", domain, exact[domain])
			}
			bill := bills[domain]
			if bill == nil || bill.FromTs != day-86400 || bill.ToTs != now {
				t.Fatalf("scope: %+v", bill)
			}
			u := bill.Usage
			if !u.Available || u.IntegrityStatus != upstreamUsageIntegrityComplete || math.Abs(u.CostUSD-770.3163) > 1e-8 || !u.AdjustedCostAvailable || math.Abs(u.AdjustedCostUSD-385.15815) > 1e-8 || u.DataUntil != day+tail {
				t.Fatalf("tail=%d %s lost bill or correction: %+v", tail, domain, u)
			}
		}
	}
}

func TestNaturalDayBillPreservesMissingAndInvalidEvidence(t *testing.T) {
	day := time.Date(2026, 9, 7, 0, 0, 0, 0, cstLocation).Unix()
	for _, tc := range []struct {
		name                          string
		cost                          float64
		missing, overlap, ratioChange bool
		wantIntegrity                 string
	}{
		{name: "zeroIsKnown", cost: 0, wantIntegrity: upstreamUsageIntegrityComplete},
		{name: "missingIsNotZero", missing: true},
		{name: "negative", cost: -1, wantIntegrity: upstreamUsageIntegrityInvalidAmount},
		{name: "overlap", cost: 10, overlap: true, wantIntegrity: upstreamUsageIntegrityOverlap},
		{name: "middayRatio", cost: 10, ratioChange: true, wantIntegrity: upstreamUsageIntegrityComplete},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			accounts := map[string]ChannelUpstreamAccountView{"daily.example": {Configured: true, Provider: upstreamProviderAICodeWith, UsageSyncEnabled: true}}
			createChannelRechargeVersion(t, m, "daily.example", 1, day-86400, 1, 2)
			if !tc.missing {
				row := ChannelUpstreamUsageHour{Domain: "daily.example", HourTs: day - 86400, BucketSeconds: 86400, CostUSD: tc.cost, Provider: upstreamProviderAICodeWith}
				if err := m.storeDB.Create(&row).Error; err != nil {
					t.Fatal(err)
				}
				if tc.overlap {
					row.HourTs += 3600
					row.BucketSeconds = 3600
					if err := m.storeDB.Create(&row).Error; err != nil {
						t.Fatal(err)
					}
				}
			}
			if tc.ratioChange {
				createChannelRechargeVersion(t, m, "daily.example", 2, day-12*3600, 1, 3)
			}
			bills, err := m.loadChannelUpstreamNaturalDayBills(context.Background(), stabilityScope{FromTs: day - 12*3600, ToTs: day}, day, accounts, channelFinanceSnapshot{})
			if err != nil {
				t.Fatal(err)
			}
			u := bills["daily.example"].Usage
			if tc.missing {
				if u.Available || u.Complete || u.ExpectedHours != 24 {
					t.Fatalf("missing published: %+v", u)
				}
				return
			}
			if u.IntegrityStatus != tc.wantIntegrity {
				t.Fatalf("integrity: %+v", u)
			}
			if tc.ratioChange && (u.AdjustedCostAvailable || u.AdjustedCostStatus != upstreamAdjustedCostBucketAmbiguous || u.CostUSD != 10) {
				t.Fatalf("ratio evidence: %+v", u)
			}
			if tc.wantIntegrity != upstreamUsageIntegrityComplete && (u.CostUSD != 0 || u.AdjustedCostAvailable) {
				t.Fatalf("bad bill published: %+v", u)
			}
		})
	}
}
