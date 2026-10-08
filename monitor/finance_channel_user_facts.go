package monitor

import (
	"context"
	"fmt"
)

type financeChannelUserFactRow struct {
	ChannelID    int
	Grp          string
	HourTs       int64
	Requests     int64
	ConsumeQuota int64
	RefundQuota  int64
}

// Keep the channel-first aggregate fast path. Only channels with a domain
// transition inside the selected interval need the finer hourly grouping.
func (m *Monitor) loadFinanceChannelUserFacts(ctx context.Context, scope stabilityScope) ([]financeChannelUserFactRow, financeChannelDomains, error) {
	domains, err := loadFinanceChannelDomains(ctx, m.storeDB)
	if err != nil {
		return nil, domains, fmt.Errorf("读取渠道历史域名归属: %w", err)
	}
	varying := domains.varyingChannels(scope.FromTs, scope.ToTs)
	var rows []financeChannelUserFactRow
	query := `SELECT channel_id,grp,? hour_ts,
		COALESCE(SUM(success+anomaly+failed),0) requests,
		COALESCE(SUM(quota),0) consume_quota,COALESCE(SUM(refund_quota),0) refund_quota
		FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version IN ?`
	args := []any{scope.FromTs, scope.FromTs, scope.ToTs, accountingTrafficVersions()}
	if len(varying) > 0 {
		query += " AND channel_id NOT IN ?"
		args = append(args, varying)
	}
	if err := m.storeDB.WithContext(ctx).Raw(query+" GROUP BY channel_id,grp", args...).Scan(&rows).Error; err != nil {
		return nil, domains, fmt.Errorf("读取全站用户用量事实: %w", err)
	}
	if len(varying) > 0 {
		var hourly []financeChannelUserFactRow
		if err := m.storeDB.WithContext(ctx).Raw(`SELECT channel_id,grp,hour_ts,
			COALESCE(SUM(success+anomaly+failed),0) requests,
			COALESCE(SUM(quota),0) consume_quota,COALESCE(SUM(refund_quota),0) refund_quota
			FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version IN ? AND channel_id IN ?
			GROUP BY channel_id,grp,hour_ts`, scope.FromTs, scope.ToTs, accountingTrafficVersions(), varying).Scan(&hourly).Error; err != nil {
			return nil, domains, fmt.Errorf("读取变更归属渠道的小时用量: %w", err)
		}
		rows = append(rows, hourly...)
	}
	return rows, domains, nil
}
