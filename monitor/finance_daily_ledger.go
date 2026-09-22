package monitor

import "fmt"

// A component of the parent's gross corrected cost, not a paired customer
// cost. Parent source selection prevents double counting recharge bills.
type financeDailyLedgerCorrection struct {
	KnownCost        channelEconomicsMoneyView  `json:"known_cost"`
	Cost             *channelEconomicsMoneyView `json:"cost"`
	RelevantDomains  int                        `json:"relevant_domains"`
	AvailableDomains int                        `json:"available_domains"`
	Status           string                     `json:"status"`
}

func applyFinanceDailyLedgerCorrection(days []financeDailyView, details []financeCostDetailView) error {
	selected := make(map[string]financeCostDetailView)
	allExact := true
	for _, detail := range details {
		if detail.CorrectionSource == "经济事实账" {
			selected[detail.Domain] = detail
			allExact = allExact && detail.CorrectedCost != nil
		}
	}
	byDomain := make(map[string]int64, len(selected))
	projected := make([]financeDailyLedgerCorrection, len(days))
	for index, day := range days {
		view := financeDailyLedgerCorrection{RelevantDomains: len(selected), Status: "not_required"}
		var total int64
		for domain := range selected {
			value, found := day.ledgerSourceCosts[domain]
			if !found {
				continue
			}
			if value < 0 {
				return fmt.Errorf("每日账本回退成本不得为负数")
			}
			if err := addEconomicsInt64(&total, value); err != nil {
				return err
			}
			domainTotal := byDomain[domain]
			if err := addEconomicsInt64(&domainTotal, value); err != nil {
				return err
			}
			byDomain[domain] = domainTotal
			view.AvailableDomains++
		}
		if len(selected) > 0 {
			view.Status = "incomplete"
		}
		if view.AvailableDomains > 0 {
			view.KnownCost = economicsMoney(total)
			// Conservative: a day cannot certify more than its parent's chosen
			// ledger scope. Unknown pairing is never converted into exact cost.
			if allExact && view.AvailableDomains == len(selected) {
				view.Cost = financeMoneyPointer(view.KnownCost)
				view.Status = "complete"
			}
		}
		projected[index] = view
	}
	// Compare each supplier, not only the overall sum: offsetting differences
	// must not make an inconsistent source projection look reconciled.
	matched := true
	for domain, detail := range selected {
		expected, known := financeMoneyInt64(detail.KnownCorrectedCost)
		if !known || expected < 0 || expected != byDomain[domain] {
			matched = false
		}
	}
	for index := range days {
		if !matched {
			projected[index].KnownCost = channelEconomicsMoneyView{}
			projected[index].Cost = nil
			projected[index].Status = "source_mismatch"
		}
		days[index].LedgerCorrection = projected[index]
	}
	return nil
}
