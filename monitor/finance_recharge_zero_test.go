package monitor

import (
	"context"
	"testing"
)

func TestFinanceRechargeObservedZeroDoesNotInventHistoricalTerms(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	ctx := context.Background()
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("1=1").
		Updates(map[string]any{"cost_usd": 0, "quota": 0}).Error; err != nil {
		t.Fatal(err)
	}
	rows, err := m.loadChannelUpstreamUsageRows(ctx, scope)
	if err != nil {
		t.Fatal(err)
	}
	for _, history := range []map[string][]channelRechargeVersion{
		nil,
		{"hour.example": {{Version: 1, EffectiveAt: scope.FromTs, Valid: false}},
			"day.example": {{Version: 1, EffectiveAt: scope.FromTs, Paid: 1, Credit: 1, Valid: true},
				{Version: 2, EffectiveAt: scope.FromTs + 3600, Paid: 2.5, Credit: 8, Valid: true}}},
	} {
		metrics, money, err := projectFinanceBillWindow(rows, scope, scope.ToTs+3600, accounts, history)
		if err != nil {
			t.Fatal(err)
		}
		for domain, metric := range metrics {
			if !metric.Complete || !metric.AdjustedCostAvailable || metric.AdjustedCostUSD != 0 ||
				metric.AdjustedCostStatus != upstreamAdjustedCostComplete || metric.RechargeRatio != 0 ||
				money[domain].Raw.MicroUSD != "0" || money[domain].RechargeCorrected.MicroUSD != "0" {
				t.Fatalf("observed zero was blocked or assigned invented terms: %s %+v %+v", domain, metric, money[domain])
			}
		}
		plan, err := inspectFinanceRechargeGaps(ctx, &financeBillInputs{scope: scope, accounts: accounts, rows: rows, versions: history}, scope.ToTs+3600)
		if err != nil || len(plan.Domains) != 0 || len(plan.Skipped) != 0 {
			t.Fatalf("zero bill requested unnecessary history repair: %+v %v", plan, err)
		}
	}
	var versions int64
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Count(&versions).Error; err != nil || versions != 0 {
		t.Fatal("zero projection wrote a synthetic version", versions, err)
	}
}

func TestFinanceRechargeZeroDoesNotHideUnknownNonzeroOrInvalidSources(t *testing.T) {
	for _, scenario := range []string{"missing_bucket", "provisional", "nonzero_below_micro", "unit_evidence_nonzero", "invalid_amount"} {
		t.Run(scenario, func(t *testing.T) {
			m, scope, accounts := dailyBillFixture(t)
			ctx := context.Background()
			if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("1=1").
				Updates(map[string]any{"cost_usd": 0, "quota": 0}).Error; err != nil {
				t.Fatal(err)
			}
			query := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", "hour.example", scope.FromTs)
			var err error
			switch scenario {
			case "missing_bucket":
				err = query.Delete(&ChannelUpstreamUsageHour{}).Error
			case "provisional":
				err = query.Update("provisional", true).Error
			case "nonzero_below_micro":
				err = query.Updates(map[string]any{"cost_usd": 0.0000001, "quota": 0.0000001 * quotaPerUSD}).Error
			case "unit_evidence_nonzero":
				err = query.Updates(map[string]any{"quota": 1, "unit_per_usd": 1e12}).Error
			case "invalid_amount":
				err = query.Update("cost_usd", -1).Error
			}
			if err != nil {
				t.Fatal(err)
			}
			metrics, _, err := m.loadFinanceBillWindow(ctx, scope, scope.ToTs+3600, accounts, channelFinanceSnapshot{})
			if err != nil {
				t.Fatal(err)
			}
			metric := metrics["hour.example"]
			if scenario == "missing_bucket" || scenario == "provisional" {
				if metric.Complete {
					t.Fatal("absence or provisional zero became a complete source", metric)
				}
			} else if metric.AdjustedCostAvailable {
				t.Fatal("unverified or nonzero source became exact zero", metric)
			}
		})
	}
}
