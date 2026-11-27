package monitor

import (
	"context"
	"database/sql"
	"errors"
	"strconv"
	"strings"
)

// A single source read either computes an exact small aggregate, or returns
// bounded planning metadata. A planning sample is NEVER an aggregate result.
type usageLogAggregateProbe struct {
	exact bool
	value int64
	plan  int
}

func usageLogsAggregateProbeSQL(f usageLogFilter, w *usageLogWhere, expression string) (string, []any) {
	var small []string
	var args []any
	for _, s := range []struct {
		column  string
		value   any
		enabled bool
	}{
		{"username", f.Username, f.Username != ""},
		{"channel_id", f.Channel, f.Channel != 0},
		{"model_name", f.ModelName, f.ModelName != ""},
		{"`group`", f.Group, f.Group != ""},
	} {
		if s.enabled {
			small = append(small, "NOT EXISTS(SELECT 1 FROM logs WHERE "+s.column+"=? LIMIT 256,1)")
			args = append(args, s.value)
		}
	}
	if len(small) == 0 {
		small = []string{"1=0"}
	}
	// A tiny ASCII integer protocol avoids JSON/float conversion (>2^53).
	exact := func(source string) string {
		return "CONCAT('exact|',CAST((SELECT " + expression + " FROM " + source + w.sql() + ") AS CHAR))"
	}
	q := usageLogsSelect + "CASE WHEN (" + strings.Join(small, " OR ") + ") THEN " + exact("logs") + `
 WHEN NOT EXISTS(SELECT 1 FROM logs FORCE INDEX (idx_created_at_type)
      WHERE created_at>=? AND created_at<=? LIMIT 63,1)
 THEN ` + exact("logs FORCE INDEX (idx_created_at_type)") + `
 ELSE CONCAT('plan|',
 (SELECT CONCAT(COUNT(*),'|',COALESCE(MIN(id),0),'|',COALESCE(MAX(id),0)) FROM
    (SELECT id FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at>=? AND created_at<=?
     ORDER BY created_at,type,id LIMIT 64) AS probe),'|',
 (SELECT CONCAT(id,'|',COALESCE(created_at,0)) FROM logs FORCE INDEX (PRIMARY) ORDER BY id LIMIT 1),'|',
 (SELECT CONCAT(id,'|',COALESCE(created_at,0)) FROM logs FORCE INDEX (PRIMARY) ORDER BY id DESC LIMIT 1))
 END AS usage_aggregate_probe`
	args = append(args, w.args...)
	args = append(args, f.Start, f.End)
	args = append(args, w.args...)
	args = append(args, f.Start, f.End)
	return q, args
}

func decodeUsageLogsAggregateProbe(f usageLogFilter, raw []byte) (usageLogAggregateProbe, error) {
	bad := errors.New("invalid usage-log aggregate probe")
	if len(raw) == 0 || len(raw) > 4096 {
		return usageLogAggregateProbe{}, bad
	}
	parts := strings.Split(string(raw), "|")
	if len(parts) != 2 && len(parts) != 8 {
		return usageLogAggregateProbe{}, bad
	}
	values := make([]int64, len(parts)-1)
	for i, s := range parts[1:] {
		n, err := strconv.ParseInt(s, 10, 64)
		if err != nil {
			return usageLogAggregateProbe{}, bad
		}
		values[i] = n
	}
	switch parts[0] {
	case "exact":
		if len(parts) != 2 {
			return usageLogAggregateProbe{}, bad
		}
		return usageLogAggregateProbe{exact: true, value: values[0]}, nil
	case "plan":
		if len(parts) != 8 || values[0] != 64 {
			return usageLogAggregateProbe{}, bad
		}
		// Reaching this arm proves every active selector has >=257 rows in the
		// SAME statement snapshot. Those thresholds select plans, not totals.
		// Sample MIN/MAX remain planning-only; final spans compute their full
		// window bounds inside the same SELECT that computes the aggregate.
		s := []usageLogPlanningSummary{{5, 64, values[1], values[2], 0}, {6, 1, values[3], values[3], values[4]}, {7, 1, values[5], values[5], values[6]}}
		for kind, enabled := range []bool{f.Username != "", f.Channel != 0, f.ModelName != "", f.Group != ""} {
			if enabled {
				s = append(s, usageLogPlanningSummary{kind + 1, 257, 0, 0, 0})
			}
		}
		return usageLogAggregateProbe{plan: usageLogsSummaryDecision(f, s)}, nil
	default:
		return usageLogAggregateProbe{}, bad
	}
}

func usageLogsProbeAggregate(ctx context.Context, db usageLogsQuerier, f usageLogFilter, w *usageLogWhere, expression string) (usageLogAggregateProbe, error) {
	if strings.Contains(f.ModelName, "%") || strings.Contains(f.Username, "%") {
		return usageLogAggregateProbe{plan: usageLogPlanNative}, nil
	}
	q, args := usageLogsAggregateProbeSQL(f, w, expression)
	rows, err := db.QueryContext(ctx, q, args...)
	if err != nil {
		return usageLogAggregateProbe{}, err
	}
	defer rows.Close()
	if !rows.Next() {
		if err = rows.Err(); err != nil {
			return usageLogAggregateProbe{}, err
		}
		return usageLogAggregateProbe{}, sql.ErrNoRows
	}
	var raw []byte
	if err = rows.Scan(&raw); err != nil {
		return usageLogAggregateProbe{}, err
	}
	if rows.Next() {
		return usageLogAggregateProbe{}, errors.New("multiple usage-log aggregate probes")
	}
	if err = rows.Err(); err != nil {
		return usageLogAggregateProbe{}, err
	}
	if err = rows.Close(); err != nil {
		return usageLogAggregateProbe{}, err
	}
	if err = ctx.Err(); err != nil {
		return usageLogAggregateProbe{}, err
	}
	return decodeUsageLogsAggregateProbe(f, raw)
}
