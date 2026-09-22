package monitor

// A strict internal ledger fact is not automatically included in a partial
// account bill. Prove the source envelope before subtracting across domains.
type financeDeductionSource struct {
	Cost          channelEconomicsMoneyView
	Included      bool
	PricingStatus string
}

func financePeriodDeductionSources(details []financeCostDetailView) map[string]financeDeductionSource {
	sources := make(map[string]financeDeductionSource, len(details))
	for _, detail := range details {
		// A partial account bill does not prove that the internal ledger's
		// hours were collected. Ledger fallback shares the immutable facts;
		// recharge correction must first cover this domain's whole window.
		sources[detail.Domain] = financeDeductionSource{Cost: detail.KnownCorrectedCost,
			Included: detail.CorrectionSource == "经济事实账" || (detail.CorrectionSource == "充值版本" && detail.CorrectedCost != nil)}
	}
	return sources
}

func financeInternalCostDeduction(raw channelEconomicsMoneyView, evidence financeInternalTestCostEvidence, sources map[string]financeDeductionSource) (channelEconomicsMoneyView, string) {
	blocked := func(reason string) (channelEconomicsMoneyView, string) { return channelEconomicsMoneyView{}, reason }
	if evidence.Total.Rows == 0 && evidence.Total.CorrectedCostMicroUSD == 0 {
		for _, fact := range evidence.ByDomain {
			if fact.Rows != 0 || fact.CorrectedCostMicroUSD != 0 {
				return blocked("cost_exclusion_evidence_mismatch")
			}
		}
		return raw, ""
	}
	amount, ok := financeMoneyInt64(raw)
	if !ok || amount < 0 {
		return blocked("cost_source_missing")
	}
	var excluded, rows int64
	for domain, fact := range evidence.ByDomain {
		if fact.Rows == 0 && fact.CorrectedCostMicroUSD == 0 {
			continue
		}
		if domain == "" || fact.Rows <= 0 || fact.CorrectedCostMicroUSD < 0 {
			return blocked("cost_exclusion_evidence_mismatch")
		}
		source, found := sources[domain]
		value, known := financeMoneyInt64(source.Cost)
		if !found || !known || value < 0 {
			return blocked("cost_source_missing")
		}
		if !source.Included {
			return blocked("cost_source_incomplete")
		}
		if source.PricingStatus != "" {
			return blocked(source.PricingStatus)
		}
		if fact.CorrectedCostMicroUSD > value {
			return blocked("cost_exclusion_exceeds_source")
		}
		if addEconomicsInt64(&excluded, fact.CorrectedCostMicroUSD) != nil || addEconomicsInt64(&rows, fact.Rows) != nil {
			return blocked("cost_exclusion_evidence_mismatch")
		}
	}
	if rows != evidence.Total.Rows || excluded != evidence.Total.CorrectedCostMicroUSD || rows <= 0 {
		return blocked("cost_exclusion_evidence_mismatch")
	}
	if excluded > amount {
		return blocked("cost_exclusion_exceeds_source")
	}
	return economicsMoney(amount - excluded), ""
}
