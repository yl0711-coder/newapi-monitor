package monitor

import (
	"context"
	"fmt"
	"testing"
)

func TestFinanceInternalExclusionClearsNonpositiveRevenueMargin(t *testing.T) {
	for _, tc := range []struct {
		name      string
		excluded  int64
		remaining int64
		margin    string
	}{
		{"zero_revenue", 3_000_000, 0, ""},
		{"net_refund", 4_000_000, -1_000_000, ""},
		{"positive_revenue", 1_000_000, 2_000_000, "60.00"},
	} {
		for _, exact := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/exact=%t", tc.name, exact), func(t *testing.T) {
				oldMargin := "66.67"
				statement := financeStatementView{
					PairedUserConsumption: economicsMoney(3_000_000), PairedCorrectedCost: economicsMoney(1_000_000),
					KnownContributionProfit: economicsMoney(2_000_000), PairedContributionMargin: &oldMargin,
				}
				if exact {
					statement.ContributionProfit = financeMoneyPointer(statement.KnownContributionProfit)
					statement.ContributionMargin = &oldMargin
				}
				fact := financeInternalTestCostFact{Rows: 1, RevenueMicroUSD: tc.excluded, CorrectedCostMicroUSD: 200_000, ProfitMicroUSD: tc.excluded - 200_000}
				if err := subtractFinanceInternalTestFromContribution(&statement, fact, 2); err != nil {
					t.Fatal(err)
				}
				if statement.PairedUserConsumption != economicsMoney(tc.remaining) || statement.PairedCorrectedCost != economicsMoney(800_000) ||
					statement.KnownContributionProfit != economicsMoney(tc.remaining-800_000) {
					t.Fatal("margin projection changed the contribution amounts")
				}
				if tc.margin == "" {
					if statement.PairedContributionMargin != nil || statement.ContributionMargin != nil {
						t.Fatal("nonpositive remaining revenue retained a stale percentage")
					}
				} else if statement.PairedContributionMargin == nil || *statement.PairedContributionMargin != tc.margin ||
					(exact && (statement.ContributionMargin == nil || *statement.ContributionMargin != tc.margin)) {
					t.Fatal("positive-revenue margin was not recomputed")
				}
				if !exact && (statement.ContributionProfit != nil || statement.ContributionMargin != nil) {
					t.Fatal("partial contribution was promoted to exact")
				}
			})
		}
	}
}

func TestFinancePeriodInternalCostCannotDeductOtherDomain(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for domain := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 1)
	}
	fact := financeInternalTestCostFact{Rows: 1, CorrectedCostMicroUSD: 4_000_000}
	evidence := financeInternalTestCostEvidence{Total: fact, ByDomain: map[string]financeInternalTestCostFact{"missing.example": fact}, Complete: true}
	statement, _, _, _, err := m.buildFinancePeriod(context.Background(), scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, evidence, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statement.KnownRawCorrectedUpstreamCost.MicroUSD != "33000000" || statement.KnownInternalTestUpstreamCost.MicroUSD != "4000000" {
		t.Fatal("raw costs must remain visible")
	}
	if statement.KnownCorrectedUpstreamCost.MicroUSD != "" || statement.CorrectedUpstreamCost != nil || statement.RawCorrectedUpstreamCost != nil || statement.InternalCostDeductionStatus != "cost_source_missing" {
		t.Fatalf("internal cost outside available sources was deducted: %+v", statement.KnownCorrectedUpstreamCost)
	}
}

func TestFinanceInternalCostDeductionBoundaries(t *testing.T) {
	fact := financeInternalTestCostFact{Rows: 1, CorrectedCostMicroUSD: 4_000_000}
	for _, mode := range []string{"matched", "missing_domain", "incomplete_source", "exceeds_domain", "exceeds_total", "missing_gross", "missing_breakdown", "mismatched_total", "zero", "no_internal", "negative", "repeat"} {
		t.Run(mode, func(t *testing.T) {
			raw := economicsMoney(10_000_000)
			evidence := financeInternalTestCostEvidence{Total: fact, ByDomain: map[string]financeInternalTestCostFact{"a": fact}}
			sources := map[string]financeDeductionSource{"a": {Cost: raw, Included: true}}
			switch mode {
			case "missing_domain":
				delete(sources, "a")
			case "incomplete_source":
				sources["a"] = financeDeductionSource{Cost: raw, Included: false}
			case "exceeds_domain":
				sources["a"] = financeDeductionSource{Cost: economicsMoney(3_000_000), Included: true}
			case "exceeds_total":
				raw = economicsMoney(3_000_000)
			case "missing_gross":
				raw = channelEconomicsMoneyView{}
			case "missing_breakdown":
				evidence.ByDomain = nil
			case "mismatched_total":
				evidence.Total = financeInternalTestCostFact{}
			case "zero":
				evidence.Total.CorrectedCostMicroUSD = 0
				evidence.ByDomain["a"] = evidence.Total
			case "no_internal":
				evidence = financeInternalTestCostEvidence{}
			case "negative":
				evidence.Total.CorrectedCostMicroUSD = -1
				evidence.ByDomain["a"] = evidence.Total
			}
			got, status := financeInternalCostDeduction(raw, evidence, sources)
			switch mode {
			case "matched", "repeat":
				if got.MicroUSD != "6000000" || status != "" {
					t.Fatalf("valid deduction: %+v %s", got, status)
				}
				again, nextStatus := financeInternalCostDeduction(raw, evidence, sources)
				if again != got || nextStatus != status || raw.MicroUSD != "10000000" {
					t.Fatal("projection mutated gross cost or deducted twice")
				}
			case "zero", "no_internal":
				if got != raw || status != "" {
					t.Fatal("no-cost deduction changed gross amount")
				}
			default:
				if got.MicroUSD != "" || status == "" {
					t.Fatalf("unproven deduction published: %+v %s", got, status)
				}
			}
		})
	}
}

func TestFinanceDailyInternalCostMissingGrossPreservesReport(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	fact := financeInternalTestCostFact{Rows: 1, CorrectedCostMicroUSD: 4_000_000}
	evidence := financeInternalTestCostEvidence{SourceComplete: true, SourceScope: scope,
		Events: []financeInternalTestCostEvent{{HourTs: scope.FromTs, Domain: "missing.example", State: "strict", Fact: fact}}}
	days, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, evidence, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatalf("missing cost must not fail the entire report: %v", err)
	}
	if len(days) != 2 || days[0].Statement.InternalCostDeductionStatus != "cost_source_missing" || days[0].Statement.KnownInternalTestUpstreamCost.MicroUSD != "4000000" || days[0].Statement.KnownCorrectedUpstreamCost.MicroUSD != "" {
		t.Fatalf("unsafe daily deduction: %+v", days)
	}
}

func TestFinanceInternalCostDeductionAggregateKeepsGap(t *testing.T) {
	periods := []financePeriodView{
		{Statement: financeStatementView{KnownRawCorrectedUpstreamCost: economicsMoney(10_000_000), KnownCorrectedUpstreamCost: economicsMoney(6_000_000)}, UpstreamCoverage: financeUpstreamCoverageView{CorrectedDomains: 1}},
		{Statement: financeStatementView{KnownRawCorrectedUpstreamCost: economicsMoney(3_000_000), InternalCostDeductionStatus: "cost_source_missing"}, UpstreamCoverage: financeUpstreamCoverageView{CorrectedDomains: 1}},
	}
	got, err := aggregateFinancePeriods(periods, StabilityDataCoverage{}, financeUpstreamCoverageView{})
	if err != nil {
		t.Fatal(err)
	}
	if got.KnownRawCorrectedUpstreamCost.MicroUSD != "13000000" || got.KnownCorrectedUpstreamCost.MicroUSD != "" || got.InternalCostDeductionStatus == "" {
		t.Fatalf("aggregation concealed deduction gap: %+v", got)
	}
}

func TestFinancePeriodInternalCostRejectsDifferentPricingBasis(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for domain := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 1)
	}
	createChannelRechargeVersion(t, m, "hour.example", 2, scope.FromTs, 2, 1)
	// The bill now uses a newly effective ratio, while the internal ledger
	// still carries an older corrected amount. Domain and amount bounds alone
	// cannot establish that these two amounts may be subtracted.
	fact := financeInternalTestCostFact{Rows: 1, UpstreamCostMicroUSD: 1_000_000, CorrectedCostMicroUSD: 1_000_000}
	evidence := financeInternalTestCostEvidence{Total: fact, ByDomain: map[string]financeInternalTestCostFact{"hour.example": fact}, Complete: true,
		Events: []financeInternalTestCostEvent{{FinanceVersion: 1, HourTs: scope.FromTs, Domain: "hour.example", State: "strict", Fact: fact}}}
	statement, _, _, _, err := m.buildFinancePeriod(context.Background(), scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, evidence, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if statement.KnownRawCorrectedUpstreamCost.MicroUSD != "36000000" || statement.KnownInternalTestUpstreamCost.MicroUSD != "1000000" {
		t.Fatal("independently known amounts must be preserved")
	}
	if statement.KnownCorrectedUpstreamCost.MicroUSD != "" || statement.InternalCostDeductionStatus != "cost_pricing_basis_mismatch" {
		t.Fatalf("different pricing basis published net cost: %s", statement.KnownCorrectedUpstreamCost.MicroUSD)
	}
}
