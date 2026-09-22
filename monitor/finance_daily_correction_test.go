package monitor

import (
	"context"
	"testing"
)

func TestFinanceDailyRechargeCorrectionVersions(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for domain := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 2)
		createChannelRechargeVersion(t, m, domain, 2, scope.FromTs+86400, 1, 4)
	}
	component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "daily-correction", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// Day 1: 11/2. Day 2: 22/4. A new ratio must not reprice day 1.
	if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "11000000" {
		t.Fatalf("period correction: %+v", component.Statement.KnownRawCorrectedUpstreamCost)
	}
	for _, day := range component.Days {
		value := day.RechargeCorrection
		if !value.Complete || value.Cost == nil || value.KnownCost.MicroUSD != "5500000" {
			t.Fatalf("daily correction: %+v", value)
		}
		if day.Statement.PairedCorrectedCost.MicroUSD != "" || day.Statement.ContributionProfit != nil {
			t.Fatal("account correction fabricated channel pairing/profit")
		}
	}
	_, cached, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "daily-correction", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil || !cached {
		t.Fatalf("unchanged component should be cached: cached=%v err=%v", cached, err)
	}
	// A newly recorded historical version must invalidate the old component,
	// not reprice the earlier day or retain a stale day/month combination.
	createChannelRechargeVersion(t, m, "hour.example", 3, scope.FromTs+86400, 1, 8)
	updated, cached, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "daily-correction", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil || cached {
		t.Fatalf("historical version should invalidate cache: cached=%v err=%v", cached, err)
	}
	if updated.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "10750000" || updated.Days[0].RechargeCorrection.KnownCost.MicroUSD != "5500000" || updated.Days[1].RechargeCorrection.KnownCost.MicroUSD != "5250000" {
		t.Fatal("historical correction did not refresh consistent day/month amounts")
	}
}

func TestFinanceDailyRechargeCorrectionBoundaries(t *testing.T) {
	for _, mode := range []string{"missing_history", "inside_day", "same_ratio", "invalid_version", "provisional", "unconfigured", "zero", "current_without_date"} {
		t.Run(mode, func(t *testing.T) {
			m, scope, accounts := dailyBillFixture(t)
			finance := channelFinanceSnapshot{}
			createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs, 1, 2)
			if mode != "missing_history" && mode != "current_without_date" {
				createChannelRechargeVersion(t, m, "day.example", 1, scope.FromTs, 1, 2)
			}
			switch mode {
			case "missing_history":
				createChannelRechargeVersion(t, m, "day.example", 1, scope.FromTs+86400, 1, 2)
			case "inside_day":
				createChannelRechargeVersion(t, m, "day.example", 2, scope.FromTs+3600, 1, 4)
			case "same_ratio":
				createChannelRechargeVersion(t, m, "day.example", 2, scope.FromTs+3600, 2, 4)
			case "invalid_version":
				createChannelRechargeVersion(t, m, "day.example", 2, scope.FromTs+3600, 0, 0)
			case "provisional":
				if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", "day.example", scope.FromTs).Update("provisional", true).Error; err != nil {
					t.Fatal(err)
				}
			case "unconfigured":
				if err := m.storeDB.Create(&StabilityHourSample{HourTs: scope.FromTs, ChannelID: 999, Success: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
					t.Fatal(err)
				}
			case "zero":
				if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("hour_ts>=?", scope.FromTs).Updates(map[string]any{"cost_usd": 0, "quota": 0}).Error; err != nil {
					t.Fatal(err)
				}
			case "current_without_date":
				finance.domainCosts = map[string]ChannelDomainCost{"day.example": {RechargePaid: 1, RechargeCredit: 2}}
			}
			bills, err := m.loadFinanceDailyBills(context.Background(), scope, scope.ToTs+86400, accounts, finance, map[string]bool{"day.example": true, "hour.example": true})
			if err != nil {
				t.Fatal(err)
			}
			first := bills[scope.FromTs].RechargeCorrection
			if mode == "same_ratio" || mode == "zero" {
				if first.Cost == nil || !first.Complete {
					t.Fatalf("legitimate correction rejected: %+v", first)
				}
			} else if first.Cost != nil || first.Complete {
				t.Fatalf("uncertain correction marked complete: %+v", first)
			}
			if mode == "missing_history" || mode == "inside_day" || mode == "invalid_version" || mode == "current_without_date" {
				if first.KnownCost.MicroUSD != "500000" || first.AvailableDomains != 1 {
					t.Fatalf("ambiguous daily bucket should not contribute: %+v", first)
				}
			}
			if mode == "zero" && first.KnownCost.MicroUSD != "0" {
				t.Fatal("true zero missing")
			}
		})
	}
}

func TestFinanceDailyRechargeCorrectionUsesPeriodSources(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs, 1, 2)
	// Only day 2 has evidence for this domain: the whole-period recharge
	// source is unavailable, so its daily drilldown must not inflate the sum.
	createChannelRechargeVersion(t, m, "day.example", 1, scope.FromTs+86400, 1, 2)
	component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "period-sources", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "1500000" {
		t.Fatal("partial recharge source entered the period")
	}
	var sum int64
	for _, day := range component.Days {
		value, ok := financeMoneyInt64(day.RechargeCorrection.KnownCost)
		if !ok || day.RechargeCorrection.Cost != nil || day.RechargeCorrection.AvailableDomains != 1 {
			t.Fatal("daily source scope differs from parent period")
		}
		sum += value
	}
	if sum != 1_500_000 {
		t.Fatal("daily recharge cost includes extra domains")
	}
}

func TestFinanceRechargeCorrectionRoundsOriginalBucketOnce(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for domain := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 2)
	}
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("cost_usd>0").Updates(map[string]any{"cost_usd": 0.0000006, "quota": 0.0000006 * quotaPerUSD}).Error; err != nil {
		t.Fatal(err)
	}
	component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "correction-rounding", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	// 0.6 micro * 1/2 = 0.3 micro -> 0 for each bucket. Do not multiply
	// the already rounded raw 1 micro by 1/2, which would invent 1 micro.
	if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "0" {
		t.Fatal("period correction rounded the aggregate or rounded twice")
	}
	for _, day := range component.Days {
		if day.RechargeCorrection.KnownCost.MicroUSD != "0" || day.RechargeCorrection.Cost == nil {
			t.Fatal("daily correction rounded twice or hid true zero")
		}
	}
}
