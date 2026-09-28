package monitor

import (
	"context"
	"reflect"
	"testing"
)

func TestFinancePartialCorrectionBucketEvidence(t *testing.T) {
	start, _ := financeStartHour("2026-05-01")
	scope := stabilityScope{FromTs: start, ToTs: start + 7200}
	accounts := map[string]ChannelUpstreamAccountView{"a": {Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI, UsageGranularity: "hour"}}
	for _, mode := range []string{"missing", "partial", "zero", "complete", "ambiguous", "invalid", "overlap", "overflow"} {
		t.Run(mode, func(t *testing.T) {
			rows := []ChannelUpstreamUsageHour{
				{Domain: "a", Provider: upstreamProviderNewAPI, HourTs: start, BucketSeconds: 3600, CostUSD: 1},
				{Domain: "a", Provider: upstreamProviderNewAPI, HourTs: start + 3600, BucketSeconds: 3600, CostUSD: 0.0000012},
			}
			versions := map[string][]channelRechargeVersion{"a": {{EffectiveAt: start + 3600, Paid: 1, Credit: 2, Valid: true}}}
			want, complete := "1", false // Round the accepted bucket only once.
			switch mode {
			case "missing":
				versions = nil
				want = ""
			case "zero":
				rows[1].CostUSD, want = 0, "0"
			case "complete":
				versions["a"][0].EffectiveAt = start
				want, complete = "500001", true
			case "ambiguous":
				versions["a"] = []channelRechargeVersion{{EffectiveAt: start, Paid: 1, Credit: 1, Valid: true}, {EffectiveAt: start + 1800, Paid: 1, Credit: 2, Valid: true}}
			case "invalid":
				rows[0].CostUSD, want = -1, ""
			case "overlap":
				rows[0].BucketSeconds, want = 7200, ""
			case "overflow":
				rows[1].CostUSD = 1e12
				versions["a"][0].Paid = 100
			}
			metrics, amounts, err := projectFinanceBillWindow(rows, scope, scope.ToTs, accounts, versions)
			if mode == "overflow" {
				if err == nil {
					t.Fatal("partial corrected overflow accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			original, err := projectChannelUpstreamUsageWindow(rows, scope, scope.ToTs, accounts, versions)
			if err != nil || !reflect.DeepEqual(original, metrics) {
				t.Fatal("changed channel management metric semantics", err)
			}
			if amounts["a"].RechargeCorrected.MicroUSD != want || metrics["a"].AdjustedCostAvailable != complete {
				t.Fatalf("amount=%+v metric=%+v", amounts["a"], metrics["a"])
			}
		})
	}
}

func TestFinancePartialCorrectionPeriodDailyAndCache(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	t.Cleanup(m.Close)
	// First day's cost cannot use a version created in its middle. Second
	// day's cost is covered. Both hourly and natural-day buckets are exercised.
	for _, domain := range []string{"hour.example", "day.example"} {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs+12*3600, 1, 2)
	}
	build := func() (financePeriodComponent, bool) {
		t.Helper()
		v, hit, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "partial-correction", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return v, hit
	}
	first, _ := build()
	if first.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "11000000" || first.Statement.RawCorrectedUpstreamCost != nil || first.Statement.ContributionProfit != nil {
		t.Fatalf("partial cost published as exact/profit: %+v", first.Statement)
	}
	var sum int64
	for _, day := range first.Days {
		if day.CostReconciliation.Status != "reconciled" {
			t.Fatalf("daily reconciliation failed: %+v", day.CostReconciliation)
		}
		value, ok := financeMoneyInt64(day.CostReconciliation.KnownTotal)
		if !ok {
			t.Fatal("missing daily known amount")
		}
		sum += value
	}
	if sum != 11_000_000 {
		t.Fatalf("daily sum=%d", sum)
	}
	if first.Days[0].RechargeCorrection.Cost != nil || first.Days[1].RechargeCorrection.Cost == nil {
		t.Fatal("daily completeness lost")
	}
	for i := range first.CostDetails {
		applyFinanceClosureReadiness(&first.CostDetails[i])
		if first.CostDetails[i].ClosureReadiness != "finance_history_missing" {
			t.Fatalf("misleading repair action: %+v", first.CostDetails[i])
		}
	}
	_, hit := build()
	if !hit {
		t.Fatal("unchanged month did not reuse cache")
	}
	// Local fixture only: model reviewed evidence extending the effective
	// boundary, without re-downloading bills or changing their amounts.
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain IN ?", []string{"hour.example", "day.example"}).Update("effective_at", scope.FromTs).Error; err != nil {
		t.Fatal(err)
	}
	second, hit := build()
	if hit {
		t.Fatal("historical term change reused stale component")
	}
	if second.Statement.RawCorrectedUpstreamCost == nil || second.Statement.RawCorrectedUpstreamCost.MicroUSD != "16500000" {
		t.Fatalf("historical evidence not reflected: %+v", second.Statement)
	}
	if second.Statement.ContributionProfit != nil {
		t.Fatal("cost coverage fabricated paired profit")
	}
}

func TestFinancePartialCorrectionMergeAndInternalExclusion(t *testing.T) {
	partial := financeCostDetailView{Domain: "a", Status: "incomplete", KnownBilledCost: economicsMoney(20), KnownCorrectedCost: economicsMoney(10), CorrectionSource: "充值版本", RechargeHistoryStatus: upstreamAdjustedCostMissingHistory}
	full := financeCostDetailView{Domain: "a", KnownBilledCost: economicsMoney(40), BilledCost: financeMoneyPointer(economicsMoney(40)), KnownCorrectedCost: economicsMoney(20), CorrectedCost: financeMoneyPointer(economicsMoney(20)), CorrectionSource: "充值版本"}
	coverage, details, err := mergeFinanceCostDetails([][]financeCostDetailView{{partial}, {full}}, map[string]ChannelUpstreamAccountView{"a": {Configured: true, UsageSyncEnabled: true}})
	if err != nil {
		t.Fatal(err)
	}
	if len(details) != 1 || details[0].KnownCorrectedCost.MicroUSD != "30" || details[0].CorrectedCost != nil || coverage.CorrectedCompleteDomains != 0 {
		t.Fatalf("merge=%+v", details)
	}
	applyFinanceClosureReadiness(&details[0])
	if details[0].ClosureReadiness != "finance_history_missing" {
		t.Fatal("lost history gap after month merge")
	}
	partial.RechargeHistoryStatus = upstreamAdjustedCostBucketAmbiguous
	applyFinanceClosureReadiness(&partial)
	if partial.ClosureReadiness != "correction_ambiguous" {
		t.Fatal("ambiguous bucket treated as missing configuration")
	}
	sources := financePeriodDeductionSources([]financeCostDetailView{partial})
	if sources["a"].Included {
		t.Fatal("partial cost allowed unproven internal-test deduction")
	}
}

func TestFinancePartialCorrectionDoesNotDoubleCountLedger(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	t.Cleanup(m.Close)
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs+86400, 1, 2)
	pub := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs, 1, 0, 2_000_000, 3_000_000, 0)
	insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
	component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "ledger-precedence", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	for _, detail := range component.CostDetails {
		if detail.Domain != "hour.example" {
			continue
		}
		if detail.KnownCorrectedCost.MicroUSD != "3000000" || detail.CorrectionSource != "经济事实账" || detail.RechargeHistoryStatus != "" {
			t.Fatalf("partial account bill replaced/added to ledger or mislabelled its gaps: %+v", detail)
		}
	}
	if component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "3000000" {
		t.Fatal("double-counted cost")
	}
	for _, day := range component.Days {
		if day.CostReconciliation.Status == "source_mismatch" {
			t.Fatal("daily ledger selection differs from parent")
		}
	}
}
