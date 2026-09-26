package monitor

import (
	"context"
	"fmt"
	"math"
	"math/big"
)

// Account bills are corrected at query time; internal costs are immutable
// publications. Compare their effective pricing before mixing those sources.
// Ledger-to-ledger subtraction already shares the published amounts.
func (m *Monitor) loadFinancePeriodDeductionSources(ctx context.Context, details []financeCostDetailView, evidence financeInternalTestCostEvidence) (map[string]financeDeductionSource, error) {
	sources := financePeriodDeductionSources(details)
	accounts := make(map[string]ChannelUpstreamAccountView)
	for _, detail := range details {
		if detail.CorrectionSource == "充值版本" && sources[detail.Domain].Included && evidence.ByDomain[detail.Domain].Rows > 0 {
			accounts[detail.Domain] = ChannelUpstreamAccountView{Configured: true, UsageSyncEnabled: true}
		}
	}
	if len(accounts) == 0 {
		return sources, nil
	}
	// One local batch query, not one query per event. Do not use the mutable
	// legacy fallback: it cannot prove a publication's historical version.
	versions, err := m.loadChannelRechargeVersions(ctx, accounts, channelFinanceSnapshot{})
	if err != nil {
		return nil, fmt.Errorf("核对内部测试成本历史计价: %w", err)
	}
	events := make(map[string][]financeInternalTestCostEvent, len(accounts))
	for _, event := range evidence.Events {
		if _, needed := accounts[event.Domain]; needed && event.State == "strict" {
			events[event.Domain] = append(events[event.Domain], event)
		}
	}
	for domain := range accounts {
		source := sources[domain]
		source.PricingStatus = financeRechargeDeductionStatus(events[domain], evidence.ByDomain[domain], versions[domain])
		sources[domain] = source
	}
	return sources, nil
}

func financeRechargeRatio(paid, credit float64) *big.Rat {
	p, err := nonnegativeFloatRat(paid)
	if err != nil || p.Sign() <= 0 {
		return nil
	}
	c, err := nonnegativeFloatRat(credit)
	if err != nil || c.Sign() <= 0 {
		return nil
	}
	return p.Quo(p, c)
}

func financeRechargeDeductionStatus(events []financeInternalTestCostEvent, expected financeInternalTestCostFact, versions []channelRechargeVersion) string {
	byVersion := make(map[int64]channelRechargeVersion, len(versions))
	for _, version := range versions {
		byVersion[version.Version] = version
	}
	var rows, raw, corrected int64
	for _, event := range events {
		if event.Fact.Rows != 1 || event.HourTs < 0 || event.HourTs%3600 != 0 || event.HourTs > math.MaxInt64-3600 {
			return "cost_exclusion_evidence_mismatch"
		}
		historical, found := byVersion[event.FinanceVersion]
		if event.FinanceVersion <= 0 || !found || !historical.Valid || historical.EffectiveAt > event.HourTs {
			return "cost_pricing_history_missing"
		}
		paid, credit, status := rechargeTermsForBucket(versions, event.HourTs, event.HourTs+3600)
		if status != upstreamAdjustedCostComplete {
			return "cost_pricing_window_incomplete"
		}
		oldRatio, billRatio := financeRechargeRatio(historical.Paid, historical.Credit), financeRechargeRatio(paid, credit)
		if oldRatio == nil || billRatio == nil || oldRatio.Cmp(billRatio) != 0 {
			return "cost_pricing_basis_mismatch"
		}
		for _, version := range versions {
			if version.EffectiveAt > event.HourTs && version.EffectiveAt < event.HourTs+3600 {
				ratio := financeRechargeRatio(version.Paid, version.Credit)
				if !version.Valid || ratio == nil || ratio.Cmp(billRatio) != 0 {
					return "cost_pricing_window_incomplete"
				}
			}
		}
		// Reproduce the publication's integer rounding, not float arithmetic on
		// the aggregate. Matching version numbers alone does not prove amounts.
		amount, err := correctedCostMicroUSD(event.Fact.UpstreamCostMicroUSD, historical.Paid, historical.Credit)
		if err != nil || amount != event.Fact.CorrectedCostMicroUSD {
			return "cost_pricing_amount_mismatch"
		}
		if addEconomicsInt64(&rows, event.Fact.Rows) != nil || addEconomicsInt64(&raw, event.Fact.UpstreamCostMicroUSD) != nil || addEconomicsInt64(&corrected, amount) != nil {
			return "cost_exclusion_evidence_mismatch"
		}
	}
	if rows <= 0 || rows != expected.Rows || raw != expected.UpstreamCostMicroUSD || corrected != expected.CorrectedCostMicroUSD {
		return "cost_exclusion_evidence_mismatch"
	}
	return ""
}
