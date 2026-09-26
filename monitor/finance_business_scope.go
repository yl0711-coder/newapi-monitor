package monitor

import (
	"context"
	"fmt"
	"sort"
	"strings"
)

// financeCostExclusions joins disjoint exclusions: configured internal users
// in included groups, plus ALL users in excluded groups. Synthetic rows only
// enter cost attribution; they are never persisted as user facts. Keep the
// immutable ledger gross to avoid rewriting history when policies change.
func (m *Monitor) financeCostExclusions(ctx context.Context, scope stabilityScope, configured financeConfiguredInternalEvidence) (financeConfiguredInternalEvidence, error) {
	policies, err := loadChannelBusinessGroupPolicies(ctx, m.storeDB)
	if err != nil {
		return configured, err
	}
	if !channelBusinessGroupsExcluded(policies) {
		return configured, nil
	}
	result := configured
	result.Rows = make([]FinanceInternalAccountHourFact, 0, len(configured.Rows))
	for _, row := range configured.Rows {
		if channelBusinessGroupIncluded(policies, row.Grp) {
			result.Rows = append(result.Rows, row)
		}
	}
	groups := []string{}
	for group, included := range policies {
		if !included {
			groups = append(groups, strings.TrimSpace(group))
		}
	}
	sort.Strings(groups)
	var rows []FinanceInternalAccountHourFact
	err = m.storeDB.WithContext(ctx).Raw(`SELECT hour_ts,channel_id,grp,
		COALESCE(SUM(success+anomaly+failed),0) requests,
		COALESCE(SUM(refund_records),0) refund_records,
		COALESCE(SUM(quota),0) consume_quota,COALESCE(SUM(refund_quota),0) refund_quota
		FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<?
		AND traffic_class_version IN ? AND TRIM(grp) IN ?
		GROUP BY hour_ts,channel_id,grp LIMIT ?`, scope.FromTs, scope.ToTs,
		accountingTrafficVersions(), groups, maxChannelEconomicsReportRows+1).Scan(&rows).Error
	if err != nil {
		return configured, fmt.Errorf("读取非业务分组成本排除依据: %w", err)
	}
	if len(rows) > maxChannelEconomicsReportRows {
		return configured, fmt.Errorf("非业务分组依据超过安全上限，请缩小区间")
	}
	result.Rows = append(result.Rows, rows...)
	return result, nil
}
