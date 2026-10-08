package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
)

// Stream the small, local account-bucket ledger in primary-key order. Interval
// sums lose same-total corrections across domains/days and conversion evidence.
// Include the overlapping natural-day bucket, exactly as the bill reader does;
// never read request details, contact providers, or retain the rows in memory.
func (m *Monitor) hashFinanceUpstreamBillBuckets(ctx context.Context, out io.Writer, from, to int64) error {
	rows, err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,hour_ts,
		COALESCE(bucket_seconds,0),COALESCE(requests,0),COALESCE(tokens,0),COALESCE(quota,0),COALESCE(cost_usd,0),
		COALESCE(unit_per_usd,0),COALESCE(source_cost_units,0),COALESCE(source_kind,''),COALESCE(provisional,0),
		COALESCE(provider,''),COALESCE(fetched_at,0)
		FROM channel_upstream_usage_hours WHERE hour_ts>=? AND hour_ts<? ORDER BY domain,hour_ts`, cstDayStart(from), to).Rows()
	if err != nil {
		return fmt.Errorf("读取上游账单桶版本: %w", err)
	}
	defer rows.Close()
	if _, err := io.WriteString(out, "upstream-bill-buckets-v1\n"); err != nil {
		return err
	}
	encoder := json.NewEncoder(out)
	for rows.Next() {
		if err := ctx.Err(); err != nil {
			return err
		}
		var row ChannelUpstreamUsageHour
		if err := rows.Scan(&row.Domain, &row.HourTs, &row.BucketSeconds, &row.Requests, &row.Tokens, &row.Quota, &row.CostUSD,
			&row.UnitPerUSD, &row.SourceCostUnits, &row.SourceKind, &row.Provisional, &row.Provider, &row.FetchedAt); err != nil {
			return fmt.Errorf("读取上游账单桶内容: %w", err)
		}
		if err := encoder.Encode(row); err != nil {
			return err
		}
	}
	return rows.Err()
}
