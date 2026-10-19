package monitor

import (
	"context"
	"testing"
)

// A failed, uncharged request still is not proof that an unidentified upstream
// charged zero. Complete configured-account bills cannot certify that scope.
func TestFinanceUnknownFailedTrafficDoesNotBecomeZeroCost(t *testing.T) {
	for _, requests := range []int64{1, 49} {
		m, scope, accounts := dailyBillFixture(t)
		users := map[string]financeDomainUserFact{
			"hour.example":            {Domain: "hour.example", Requests: 1, ConsumeQuota: 1},
			financeUnattributedDomain: {Domain: financeUnattributedDomain, Requests: requests},
		}
		known, exact, _, _, coverage, details, err := m.loadFinanceUpstreamFacts(context.Background(), scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, users)
		if err != nil {
			t.Fatal(err)
		}
		if known.MicroUSD != "33000000" || exact != nil || coverage.CompleteDomains != 2 || coverage.UnconfiguredDomains != 1 || coverage.UnconfiguredUserRequests != requests || coverage.UnconfiguredUserConsumption.MicroUSD != "0" {
			t.Fatalf("zero customer charge changed known bills or certified unknown upstream cost: %+v %+v", known, coverage)
		}
		found := false
		for _, detail := range details {
			if detail.Domain == financeUnattributedDomain {
				found = true
				if detail.Status != "not_configured" || detail.BilledCost != nil || detail.CorrectedCost != nil {
					t.Fatal("unknown cost became zero", detail)
				}
			}
		}
		if !found {
			t.Fatal("unknown scope hidden")
		}
		// An incomplete month cannot reuse a previously known overall profit.
		s := financeStatementView{OperatingRevenue: financeMoneyPointer(economicsMoney(100_000_000)), KnownRawCorrectedUpstreamCost: known, OperatingProfit: financeMoneyPointer(economicsMoney(62_000_000))}
		applyFinanceCURCost(&s, financeCURCostView{Loaded: true, IncludedDays: 2, KnownCost: economicsMoney(5_000_000), ExactCost: financeMoneyPointer(economicsMoney(5_000_000))})
		if s.OperatingProfit != nil || s.KnownOperatingProfit.MicroUSD != "" {
			t.Fatal("unknown upstream expense retained a definitive profit")
		}
	}
}
