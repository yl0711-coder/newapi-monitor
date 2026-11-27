package monitor

import (
	"context"
	"database/sql"
	"strings"
)

// Candidate only: finish native/time plans inside the planning SELECT. A NULL
// asks for the unchanged, separately protected full-bound span. No sample is
// ever a result or a final ID boundary.
func usageLogsInlinePlanCandidate(f usageLogFilter, w *usageLogWhere, expr string) (string, []any) {
	var small []string
	var args []any
	for _, s := range []struct {
		col string
		v   any
		on  bool
	}{{"username", f.Username, f.Username != ""}, {"channel_id", f.Channel, f.Channel != 0}, {"model_name", f.ModelName, f.ModelName != ""}, {"`group`", f.Group, f.Group != ""}} {
		if s.on {
			small = append(small, "NOT EXISTS(SELECT 1 FROM logs WHERE "+s.col+"=? LIMIT 256,1)")
			args = append(args, s.v)
		}
	}
	if len(small) == 0 {
		small = []string{"1=0"}
	}
	agg := func(source string) string {
		args = append(args, w.args...)
		return "(SELECT " + expr + " FROM " + source + w.sql() + ")"
	}
	nativeSmall := agg("logs")
	nativeBad := agg("logs")
	timeResult := agg("logs FORCE INDEX (idx_created_at_type)")
	otherType := "0=1"
	if f.Type != 0 && f.Type != 2 {
		otherType = "1=1"
	}
	broad := "0=1"
	if f.Username == "" && f.Channel == 0 && f.ModelName == "" && f.Group == "" {
		broad = "1=1"
	}
	args = append(args, f.Start, f.End, f.Start, f.End, f.Start, f.End, usageLogsQuotaSpanMaxRows)
	nativeFinal := agg("logs")
	q := usageLogsSelect + "CASE WHEN " + strings.Join(small, " OR ") + " THEN " + nativeSmall + `
 ELSE (SELECT CASE
 WHEN first_row.id IS NULL OR last_row.id IS NULL OR first_row.id<0 OR last_row.id<0
      OR first_row.at<=0 OR last_row.at<=0 OR sample.low_id<0 OR sample.high_id<sample.low_id THEN ` + nativeBad + `
 WHEN sample.n<64 OR CAST(sample.high_id AS DECIMAL(65,0))-CAST(sample.low_id AS DECIMAL(65,0))>2*CAST(sample.n AS DECIMAL(65,0))-1 OR ` + otherType + ` THEN ` + timeResult + `
 WHEN (` + broad + ` AND first_row.at BETWEEN ? AND ? AND last_row.at BETWEEN ? AND ?)
   OR (last_row.at BETWEEN ? AND ? AND last_row.id>=sample.low_id
       AND CAST(last_row.id AS DECIMAL(65,0))-CAST(sample.low_id AS DECIMAL(65,0))>=?) THEN ` + nativeFinal + `
 ELSE NULL END
 FROM (SELECT COUNT(*) AS n,COALESCE(MIN(id),0) AS low_id,COALESCE(MAX(id),0) AS high_id FROM
       (SELECT id FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at>=? AND created_at<=? ORDER BY created_at,type,id LIMIT 64) AS probe) AS sample
 LEFT JOIN (SELECT id,COALESCE(created_at,0) AS at FROM logs FORCE INDEX (PRIMARY) ORDER BY id LIMIT 1) AS first_row ON 1=1
 LEFT JOIN (SELECT id,COALESCE(created_at,0) AS at FROM logs FORCE INDEX (PRIMARY) ORDER BY id DESC LIMIT 1) AS last_row ON 1=1)
 END AS usage_inline_probe`
	args = append(args, f.Start, f.End)
	return q, args
}

func usageLogsReadInlineCandidate(ctx context.Context, db usageLogsQuerier, f usageLogFilter, w *usageLogWhere, expr string) (int64, error) {
	q, a := usageLogsInlinePlanCandidate(f, w, expr)
	var n sql.NullInt64
	if e := db.QueryRowContext(ctx, q, a...).Scan(&n); e != nil {
		return 0, e
	}
	if n.Valid {
		return n.Int64, nil
	}
	q, a = usageLogsAggregateSpanSQL(f, w, expr)
	if e := db.QueryRowContext(ctx, q, a...).Scan(&n); e != nil {
		return 0, e
	}
	if n.Valid {
		return n.Int64, nil
	}
	var result int64
	e := db.QueryRowContext(ctx, usageLogsSelect+expr+" FROM logs"+w.sql(), w.args...).Scan(&result)
	return result, e
}
