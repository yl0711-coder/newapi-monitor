package monitor

import (
	"context"
	"database/sql"
	"strings"
)

// Broad SUM is the newly measured optimization. Channel/model/group filters
// already used a primary span: retain that plan but fix its split snapshots.
// Selective name/token/user-ID SUMs keep native plans, not blanket hints.
func usageLogsUseQuotaSpan(f usageLogFilter) bool {
	return usageLogsBoundedSpanWindow(f) && f.Username == ""
}

func usageLogsBoundedSpanWindow(f usageLogFilter) bool {
	return !usageLogsHasDiagnosticFilter(f) && f.UserID == 0 && f.TokenName == "" && f.Start > 0 && f.End >= f.Start && f.End-f.Start <= 26*3600
}

const usageLogsQuotaSpanMaxRows = int64(1_000_000)

// One SELECT shares a consistent-read snapshot for bounds and SUM, without
// separate transaction round trips. Never assume IDs and time are monotonic.
// CASE skips the primary scan if the span is sparse/large; NULL requests the
// unchanged original query, not a zero amount. No new index is required.
func usageLogsQuotaSpanSQL(f usageLogFilter, where *usageLogWhere) (string, []any) {
	return usageLogsAggregateSpanSQL(f, where, "COALESCE(SUM(quota),0)")
}

func usageLogsAggregateSpanSQL(f usageLogFilter, where *usageLogWhere, expression string) (string, []any) {
	query := usageLogsSelect + `CASE
 WHEN bounds.row_count=0 THEN 0
 WHEN bounds.row_count<=? AND bounds.low_id>=0 AND bounds.high_id>=bounds.low_id
      AND bounds.high_id-bounds.low_id<=2*bounds.row_count-1
 THEN (SELECT ` + expression + ` FROM logs FORCE INDEX (PRIMARY)` + where.sql() + `
       AND id BETWEEN bounds.low_id AND bounds.high_id)
 ELSE NULL END AS quota
 FROM (SELECT MIN(id) AS low_id, MAX(id) AS high_id, COUNT(*) AS row_count
       FROM logs FORCE INDEX (idx_created_at_type)
       WHERE created_at>=? AND created_at<=?) AS bounds`
	args := []any{usageLogsQuotaSpanMaxRows}
	args = append(args, where.args...)
	args = append(args, f.Start, f.End)
	return query, args
}

func (m *Monitor) queryUsageLogsQuota(ctx context.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere) (int64, error) {
	if !m.usageLogsMySQL() || !usageLogsUseQuotaSpan(f) {
		return m.queryUsageLogsQuotaLegacy(ctx, db, f, where)
	}
	planning := f
	planning.Type = 0 // stat deliberately ignores the page's log-type selector.
	probe, err := usageLogsProbeAggregate(ctx, db, planning, where, "COALESCE(SUM(quota),0)")
	if err != nil {
		return 0, err
	}
	if probe.exact {
		return probe.value, nil
	}
	plan := probe.plan
	if plan == usageLogPlanNative {
		return m.queryUsageLogsQuotaLegacy(ctx, db, f, where)
	}
	if plan == usageLogPlanTime {
		return queryUsageLogsQuotaSum(ctx, db, "logs FORCE INDEX (idx_created_at_type)", where)
	}
	query, args := usageLogsQuotaSpanSQL(f, where)
	var quota sql.NullInt64
	if err := db.QueryRowContext(ctx, query, args...).Scan(&quota); err != nil {
		// Do not automatically repeat a failed/timed-out expensive SUM.
		return 0, err
	}
	if quota.Valid {
		return quota.Int64, nil
	}
	return m.queryUsageLogsQuotaLegacy(ctx, db, f, where)
}

func (m *Monitor) queryUsageLogsQuotaLegacy(ctx context.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere) (int64, error) {
	return queryUsageLogsQuotaSum(ctx, db, "logs", where)
}

// Time/type-only COUNT is already index-covered: do not add a bounds scan.
// Exact request/token selectors keep their own selective indexes as well.
func usageLogsUseCountSpan(f usageLogFilter) bool {
	if !usageLogsBoundedSpanWindow(f) || f.RequestID != "" || f.UpstreamRequestID != "" {
		return false
	}
	if f.Username == "" {
		// Previously optimized combinations keep primary spans, now within one
		// snapshot. Time/type-only COUNT stays on its covering native index.
		return f.Channel != 0 || f.ModelName != "" || f.Group != ""
	}
	// Newly added dispatch is limited to the measured exact-name + channel
	// combination. Do not extrapolate to name-only or wildcard plans.
	return !strings.Contains(f.Username, "%") && f.Channel != 0 && f.ModelName == "" && f.Group == ""
}

func (m *Monitor) queryUsageLogsCount(ctx context.Context, db usageLogsQuerier, f usageLogFilter, where *usageLogWhere) (int64, error) {
	if m.usageLogsMySQL() && usageLogsUseCountSpan(f) {
		probe, err := usageLogsProbeAggregate(ctx, db, f, where, "COUNT(*)")
		if err != nil {
			return 0, err
		}
		if probe.exact {
			return probe.value, nil
		}
		plan := probe.plan
		if plan == usageLogPlanTime {
			var count int64
			err := db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs FORCE INDEX (idx_created_at_type)"+where.sql(), where.args...).Scan(&count)
			return count, err
		}
		if plan == usageLogPlanNative {
			var count int64
			err := db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&count)
			return count, err
		}
		query, args := usageLogsAggregateSpanSQL(f, where, "COUNT(*)")
		var count sql.NullInt64
		if err := db.QueryRowContext(ctx, query, args...).Scan(&count); err != nil {
			return 0, err
		}
		if count.Valid {
			return count.Int64, nil
		}
	}
	var count int64
	err := db.QueryRowContext(ctx, usageLogsSelect+"COUNT(*) FROM logs"+where.sql(), where.args...).Scan(&count)
	return count, err
}

func queryUsageLogsQuotaSum(ctx context.Context, db usageLogsQuerier, source string, where *usageLogWhere) (int64, error) {
	var quota int64
	err := db.QueryRowContext(ctx, usageLogsSelect+"COALESCE(SUM(quota),0) FROM "+source+where.sql(), where.args...).Scan(&quota)
	return quota, err
}
