package monitor

import (
	"context"
	"testing"
)

func dailyBillFixture(t *testing.T) (*Monitor, stabilityScope, map[string]ChannelUpstreamAccountView) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "hour.example")
	day, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: day, ToTs: day + 2*86400}
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, BaseDomain: "hour.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: day, ChannelID: 1, Success: 1, Quota: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	accounts := map[string]ChannelUpstreamAccountView{
		"hour.example": {Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI, UsageGranularity: "hour"},
		"day.example":  {Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderAICodeWith, UsageGranularity: "day"},
	}
	for d := int64(0); d < 2; d++ {
		for h := int64(0); h < 24; h++ {
			cost := 0.0
			if h == 0 {
				cost = float64(d + 1)
			}
			row := ChannelUpstreamUsageHour{Domain: "hour.example", HourTs: day + d*86400 + h*3600, BucketSeconds: 3600, CostUSD: cost, Quota: cost * quotaPerUSD, UnitPerUSD: quotaPerUSD, Provider: upstreamProviderNewAPI}
			if err := m.storeDB.Create(&row).Error; err != nil {
				t.Fatal(err)
			}
		}
		row := ChannelUpstreamUsageHour{Domain: "day.example", HourTs: day + d*86400, BucketSeconds: 86400, CostUSD: 10 * float64(d+1), Provider: upstreamProviderAICodeWith}
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	return m, scope, accounts
}

func TestFinanceDailyBillsUseAccountsWithoutPairing(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	ctx := context.Background()
	component, _, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "daily-test", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if component.Statement.UpstreamBilledCost == nil || component.Statement.UpstreamBilledCost.MicroUSD != "33000000" {
		t.Fatalf("month bill: %+v", component.Statement.UpstreamBilledCost)
	}
	var sum int64
	for i, day := range component.Days {
		if day.Statement.UpstreamBilledCost == nil {
			t.Fatal("account bill hidden because no channel pairing exists")
		}
		value, ok := financeMoneyInt64(*day.Statement.UpstreamBilledCost)
		if !ok || value != int64(i+1)*11_000_000 {
			t.Fatalf("day bill %+v", day.Statement.UpstreamBilledCost)
		}
		sum += value
		if day.Statement.ContributionProfit != nil || day.Statement.RawCorrectedUpstreamCost != nil {
			t.Fatal("raw bill fabricated corrected cost/profit")
		}
	}
	if sum != 33_000_000 {
		t.Fatal("daily bills differ from period")
	}
}

func TestFinanceDailyBillsCoverageBoundaries(t *testing.T) {
	for _, mode := range []string{"missing_hour", "provisional", "invalid_amount", "overlap", "unconfigured", "partial_day", "zero", "late_account"} {
		t.Run(mode, func(t *testing.T) {
			m, scope, accounts := dailyBillFixture(t)
			query := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", "hour.example", scope.FromTs)
			switch mode {
			case "missing_hour":
				if err := query.Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
					t.Fatal(err)
				}
			case "provisional":
				if err := query.Update("provisional", true).Error; err != nil {
					t.Fatal(err)
				}
			case "invalid_amount":
				if err := query.Update("cost_usd", 10).Error; err != nil {
					t.Fatal(err)
				}
			case "overlap":
				if err := query.Update("bucket_seconds", 7200).Error; err != nil {
					t.Fatal(err)
				}
			case "unconfigured":
				if err := m.storeDB.Create(&StabilityHourSample{HourTs: scope.FromTs, ChannelID: 999, Success: 1, Quota: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
					t.Fatal(err)
				}
			case "partial_day":
				scope.FromTs += 3600
				scope.ToTs -= 3600
			case "zero":
				if err := query.Updates(map[string]any{"cost_usd": 0, "quota": 0}).Error; err != nil {
					t.Fatal(err)
				}
			case "late_account":
				if err := m.storeDB.Where("domain=? AND hour_ts<?", "day.example", scope.FromTs+86400).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
					t.Fatal(err)
				}
			}
			bills, err := m.loadFinanceDailyBills(context.Background(), scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, nil)
			if err != nil {
				t.Fatal(err)
			}
			first := bills[cstDayStart(scope.FromTs)]
			if mode == "zero" || mode == "late_account" {
				if first.Exact == nil {
					t.Fatal("zero/unused account treated as missing")
				}
			} else if first.Exact != nil || first.Coverage.Complete {
				t.Fatal("incomplete account bill published exact")
			}
			if mode == "partial_day" && first.Known.MicroUSD != "0" {
				t.Fatal("daily bucket was apportioned into partial day")
			}
			if mode == "unconfigured" && first.Coverage.UnconfiguredDomains != 1 {
				t.Fatal("unresolved channel disappeared")
			}
		})
	}
}

func TestFinanceDailyBillsBucketPrecision(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	// Both hourly and daily sources contain a sub-micro-dollar fraction.
	// One bucket is the canonical rounding unit, independent of query range.
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("cost_usd>0").Updates(map[string]any{"cost_usd": 0.0000006, "quota": 0.0000006 * quotaPerUSD}).Error; err != nil {
		t.Fatal(err)
	}
	build := func(window stabilityScope) financePeriodComponent {
		t.Helper()
		component, _, err := m.buildFinancePeriodComponent(context.Background(), window, scope.ToTs+86400, "precision", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return component
	}
	whole := build(scope)
	if whole.Statement.KnownUpstreamBilledCost.MicroUSD != "4" {
		t.Fatalf("expected four individually rounded buckets, got %+v", whole.Statement.KnownUpstreamBilledCost)
	}
	for _, day := range whole.Days {
		if day.Statement.KnownUpstreamBilledCost.MicroUSD != "2" {
			t.Fatalf("daily precision: %+v", day.Statement.KnownUpstreamBilledCost)
		}
		part := build(stabilityScope{FromTs: day.From, ToTs: day.To})
		if part.Statement.KnownUpstreamBilledCost.MicroUSD != day.Statement.KnownUpstreamBilledCost.MicroUSD {
			t.Fatal("changing the query range changes bucket rounding")
		}
	}
}
