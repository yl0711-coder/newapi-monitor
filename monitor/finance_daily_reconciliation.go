package monitor

// Reconciled means only that the known, parent-selected cost sources add up.
// It does not certify complete costs, customer attribution or profit.
type financeDailyCostReconciliation struct {
	KnownTotal channelEconomicsMoneyView `json:"known_total"`
	Status     string                    `json:"status"`
}

func applyFinanceDailyCostReconciliation(days []financeDailyView, bills map[int64]financeDailyBill, details []financeCostDetailView, parent channelEconomicsMoneyView) {
	values, matched := reconcileFinanceDailyCosts(days, bills, details, parent)
	for index := range days {
		view := financeDailyCostReconciliation{Status: "unavailable"}
		if !matched {
			view.Status = "source_mismatch"
		} else if values[index].MicroUSD != "" {
			view.Status = "reconciled"
			view.KnownTotal = values[index]
		}
		days[index].CostReconciliation = view
	}
}

// Validate each supplier independently so offsetting errors cannot pass.
// No extra database reads and no redistribution of differences across days.
func reconcileFinanceDailyCosts(days []financeDailyView, bills map[int64]financeDailyBill, details []financeCostDetailView, parent channelEconomicsMoneyView) ([]channelEconomicsMoneyView, bool) {
	values := make([]channelEconomicsMoneyView, len(days))
	dayTotals := make([]int64, len(days))
	dayKnown := make([]bool, len(days))
	seen := map[string]bool{}
	var expectedTotal int64
	knownSources := 0
	for _, detail := range details {
		if seen[detail.Domain] {
			return nil, false
		}
		seen[detail.Domain] = true
		if detail.KnownCorrectedCost.MicroUSD == "" {
			continue // Missing costs are not manufactured into zero.
		}
		expected, valid := financeMoneyInt64(detail.KnownCorrectedCost)
		if !valid || expected < 0 || addEconomicsInt64(&expectedTotal, expected) != nil {
			return nil, false
		}
		knownSources++
		var actual int64
		foundSource := false
		for index, day := range days {
			var amount int64
			var found bool
			switch detail.CorrectionSource {
			case "充值版本":
				amount, found = bills[cstDayStart(day.From)].CorrectionByDomain[detail.Domain]
			case "经济事实账":
				amount, found = day.ledgerSourceCosts[detail.Domain]
			default:
				return nil, false
			}
			if !found {
				continue
			}
			foundSource = true
			dayKnown[index] = true
			if amount < 0 || addEconomicsInt64(&actual, amount) != nil || addEconomicsInt64(&dayTotals[index], amount) != nil {
				return nil, false
			}
		}
		if !foundSource || actual != expected {
			return nil, false
		}
	}
	if knownSources == 0 {
		return values, parent.MicroUSD == ""
	}
	parentTotal, valid := financeMoneyInt64(parent)
	if !valid || parentTotal != expectedTotal {
		return nil, false
	}
	for index := range days {
		if dayKnown[index] {
			values[index] = economicsMoney(dayTotals[index])
		}
	}
	return values, true
}
