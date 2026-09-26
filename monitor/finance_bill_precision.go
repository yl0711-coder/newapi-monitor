package monitor

import (
	"context"
	"fmt"
)

// Finance bills round each accepted source bucket once, then sum integer
// micro-USD. Query boundaries and day/month grouping cannot change precision.
// Recharge correction is applied to the original cost BEFORE rounding; raw
// bill rounding must not introduce a second rounding into the corrected cost.
type financeAccountBillAmounts struct {
	Raw               channelEconomicsMoneyView
	RechargeCorrected channelEconomicsMoneyView
}

type financeBillSum struct {
	micro int64
	err   error
}

func (a *financeBillSum) addUSD(usd float64) {
	if a.err != nil {
		return
	}
	money, err := financeMoneyFromUSD(usd)
	a.err = err
	if err != nil {
		return
	}
	value, ok := financeMoneyInt64(money)
	if !ok {
		a.err = fmt.Errorf("无效的账单微美元金额")
		return
	}
	a.err = addEconomicsInt64(&a.micro, value)
}

func (m *Monitor) loadFinanceBillWindow(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot) (map[string]ChannelUpstreamUsageMetrics, map[string]financeAccountBillAmounts, error) {
	rows, err := m.loadChannelUpstreamUsageRows(ctx, scope)
	if err != nil {
		return nil, nil, err
	}
	versions, err := m.loadChannelRechargeVersions(ctx, accounts, finance)
	if err != nil {
		return nil, nil, err
	}
	return projectFinanceBillWindow(rows, scope, now, accounts, versions)
}

func projectFinanceBillWindow(rows []ChannelUpstreamUsageHour, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, versions map[string][]channelRechargeVersion) (map[string]ChannelUpstreamUsageMetrics, map[string]financeAccountBillAmounts, error) {
	type amount struct {
		raw, corrected financeBillSum
	}
	totals := map[string]amount{}
	metrics, err := projectUpstreamUsageBuckets(rows, scope, now, accounts, versions, func(row ChannelUpstreamUsageHour) {
		a := totals[row.Domain]
		a.raw.addUSD(row.CostUSD)
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		paid, credit, status := rechargeTermsForBucket(versions[row.Domain], row.HourTs, row.HourTs+seconds)
		if status == upstreamAdjustedCostComplete {
			if cost, _, ok := adjustedUpstreamUsageCost(row.CostUSD, ChannelDomainCost{RechargePaid: paid, RechargeCredit: credit}, true); ok {
				a.corrected.addUSD(cost)
			}
		}
		totals[row.Domain] = a
	})
	if err != nil {
		return nil, nil, err
	}
	money := map[string]financeAccountBillAmounts{}
	for domain, metric := range metrics {
		if !metric.Available || metric.IntegrityStatus != upstreamUsageIntegrityComplete {
			continue
		}
		a, found := totals[domain]
		if !found {
			continue
		}
		if a.raw.err != nil {
			return nil, nil, fmt.Errorf("上游 %s 账单精度计算失败: %w", domain, a.raw.err)
		}
		value := financeAccountBillAmounts{Raw: economicsMoney(a.raw.micro)}
		if metric.AdjustedCostAvailable {
			if a.corrected.err != nil {
				return nil, nil, fmt.Errorf("上游 %s 修正账单精度计算失败: %w", domain, a.corrected.err)
			}
			value.RechargeCorrected = economicsMoney(a.corrected.micro)
		}
		money[domain] = value
	}
	return metrics, money, nil
}
