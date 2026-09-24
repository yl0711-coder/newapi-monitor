package monitor

import (
	"context"
	"fmt"
)

// Account bills and channel-paired costs are distinct facts. Daily raw bills
// use the same bucket checks as month/channel totals, never a paired subset.
type financeDailyBillCoverage struct {
	RelevantDomains     int  `json:"relevant_domains"`
	AvailableDomains    int  `json:"available_domains"`
	CompleteDomains     int  `json:"complete_domains"`
	UnconfiguredDomains int  `json:"unconfigured_domains"`
	Complete            bool `json:"complete"`
}

type financeDailyBill struct {
	Known              channelEconomicsMoneyView
	Exact              *channelEconomicsMoneyView
	Coverage           financeDailyBillCoverage
	RechargeCorrection financeDailyRechargeCorrection
	CorrectionByDomain map[string]int64
}

// Account recharge correction includes internal traffic and is deliberately
// separate from channel-paired customer cost and economics-ledger fallback.
type financeDailyRechargeCorrection struct {
	KnownCost        channelEconomicsMoneyView  `json:"known_cost"`
	Cost             *channelEconomicsMoneyView `json:"cost"`
	RelevantDomains  int                        `json:"relevant_domains"`
	AvailableDomains int                        `json:"available_domains"`
	CompleteDomains  int                        `json:"complete_domains"`
	Complete         bool                       `json:"complete"`
}

func (m *Monitor) loadFinanceDailyBills(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot, correctionDomains map[string]bool) (map[int64]financeDailyBill, error) {
	return m.loadFinanceDailyBillsWithInputs(ctx, scope, now, accounts, finance, correctionDomains, nil)
}

func (m *Monitor) loadFinanceDailyBillsWithInputs(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView, finance channelFinanceSnapshot, correctionDomains map[string]bool, shared *financeBillInputs) (map[int64]financeDailyBill, error) {
	inputs, err := m.financePeriodBills(ctx, scope, accounts, finance, shared)
	if err != nil {
		return nil, err
	}
	unavailable, err := m.loadFinanceDailyUnconfiguredActivity(ctx, scope, accounts)
	if err != nil {
		return nil, err
	}
	return projectFinanceDailyBills(inputs.rows, scope, now, inputs.accounts, unavailable, inputs.versions, correctionDomains)
}

func (m *Monitor) loadFinanceDailyUnconfiguredActivity(ctx context.Context, scope stabilityScope, accounts map[string]ChannelUpstreamAccountView) (map[int64]map[string]bool, error) {
	// Include non-customer traffic: a raw account bill includes test spending.
	// A missing/disabled source is unknown cost, not zero. LEFT JOIN preserves
	// traffic whose historical channel can no longer be resolved.
	var activity []struct {
		DayTs  int64
		Domain string
	}
	err := m.storeDB.WithContext(ctx).Raw(`SELECT DISTINCT ((s.hour_ts+28800)/86400)*86400-28800 day_ts,
		LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain
		FROM stability_hour_samples s LEFT JOIN channel_snaps c ON c.id=s.channel_id
		WHERE s.hour_ts>=? AND s.hour_ts<? AND s.traffic_class_version=?
		AND (s.success+s.anomaly+s.failed<>0 OR s.quota<>0 OR s.refund_quota<>0)
		UNION
		SELECT DISTINCT ((t.hour_ts+28800)/86400)*86400-28800 day_ts,
		LOWER(COALESCE(NULLIF(TRIM(c.base_domain),''),'未配置/历史')) domain
		FROM channel_test_hour_samples t LEFT JOIN channel_snaps c ON c.id=t.channel_id
		WHERE t.hour_ts>=? AND t.hour_ts<? AND t.traffic_class_version=? AND (t.requests<>0 OR t.quota<>0)`,
		scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion, scope.FromTs, scope.ToTs, stabilityTrafficClassificationVersion).Scan(&activity).Error
	if err != nil {
		return nil, fmt.Errorf("读取每日账单覆盖范围: %w", err)
	}
	unavailable := map[int64]map[string]bool{}
	for _, row := range activity {
		account, ok := accounts[row.Domain]
		if ok && account.Configured && account.UsageSyncEnabled {
			continue
		}
		if unavailable[row.DayTs] == nil {
			unavailable[row.DayTs] = map[string]bool{}
		}
		unavailable[row.DayTs][row.Domain] = true
	}
	return unavailable, nil
}

func projectFinanceDailyBills(rows []ChannelUpstreamUsageHour, scope stabilityScope, now int64, relevant map[string]ChannelUpstreamAccountView, unavailable map[int64]map[string]bool, versions map[string][]channelRechargeVersion, correctionDomains map[string]bool) (map[int64]financeDailyBill, error) {
	result := map[int64]financeDailyBill{}
	for day := cstDayStart(scope.FromTs); day < scope.ToTs; day += 86400 {
		window := stabilityScope{FromTs: max(day, scope.FromTs), ToTs: min(day+86400, scope.ToTs)}
		dayAccounts := map[string]ChannelUpstreamAccountView{}
		for domain, account := range relevant {
			if account.Configured && account.FinanceRequiredFrom < window.ToTs {
				dayAccounts[domain] = account
			}
		}
		// Filter in memory, maintaining domain/hour order. Do not query once per
		// day or split daily buckets into fabricated hourly charges.
		dayRows := make([]ChannelUpstreamUsageHour, 0)
		for _, row := range rows {
			seconds := row.BucketSeconds
			if seconds <= 0 {
				seconds = 3600
			}
			if row.HourTs < window.ToTs && row.HourTs+seconds > window.FromTs {
				dayRows = append(dayRows, row)
			}
		}
		metrics, amounts, err := projectFinanceBillWindow(dayRows, window, now, dayAccounts, versions)
		if err != nil {
			return nil, err
		}
		bill := financeDailyBill{Coverage: financeDailyBillCoverage{RelevantDomains: len(dayAccounts), UnconfiguredDomains: len(unavailable[day])}, CorrectionByDomain: map[string]int64{}}
		bill.RechargeCorrection.RelevantDomains = len(dayAccounts)
		var total, correctedTotal int64
		for domain, metric := range metrics {
			if !metric.Available || (metric.IntegrityStatus != "" && metric.IntegrityStatus != upstreamUsageIntegrityComplete) {
				continue
			}
			money := amounts[domain].Raw
			value, ok := financeMoneyInt64(money)
			if !ok {
				return nil, fmt.Errorf("每日账单金额无效")
			}
			if err := addEconomicsInt64(&total, value); err != nil {
				return nil, err
			}
			bill.Coverage.AvailableDomains++
			if metric.Complete {
				bill.Coverage.CompleteDomains++
			}
			// The daily drilldown follows the parent period's chosen source.
			// Otherwise a domain with a historical gap may contribute on some
			// days although the monthly statement excludes its recharge source.
			if metric.AdjustedCostAvailable && correctionDomains[domain] {
				corrected, ok := financeMoneyInt64(amounts[domain].RechargeCorrected)
				if !ok {
					return nil, fmt.Errorf("每日修正账单金额无效")
				}
				if err := addEconomicsInt64(&correctedTotal, corrected); err != nil {
					return nil, err
				}
				bill.RechargeCorrection.AvailableDomains++
				bill.CorrectionByDomain[domain] = corrected
				if metric.Complete {
					bill.RechargeCorrection.CompleteDomains++
				}
			}
		}
		if bill.Coverage.AvailableDomains > 0 {
			bill.Known = economicsMoney(total)
		}
		bill.Coverage.Complete = bill.Coverage.RelevantDomains > 0 && bill.Coverage.UnconfiguredDomains == 0 && bill.Coverage.CompleteDomains == bill.Coverage.RelevantDomains
		if bill.Coverage.Complete {
			bill.Exact = financeMoneyPointer(bill.Known)
		}
		if bill.RechargeCorrection.AvailableDomains > 0 {
			bill.RechargeCorrection.KnownCost = economicsMoney(correctedTotal)
		}
		bill.RechargeCorrection.Complete = bill.Coverage.Complete && bill.RechargeCorrection.CompleteDomains == bill.Coverage.RelevantDomains
		if bill.RechargeCorrection.Complete {
			bill.RechargeCorrection.Cost = financeMoneyPointer(bill.RechargeCorrection.KnownCost)
		}
		result[day] = bill
	}
	return result, nil
}
