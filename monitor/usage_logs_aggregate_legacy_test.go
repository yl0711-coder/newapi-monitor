package monitor

import (
	"fmt"
	"strings"
)

// Frozen old planning SQL, retained ONLY as a performance comparison oracle.
// Production uses usageLogsAggregateProbeSQL and never sends this UNION.
func usageLogsPlanningSummarySQL(f usageLogFilter) (string, []any) {
	var branches []string
	var args []any
	for _, s := range []struct {
		kind    int
		column  string
		value   any
		enabled bool
	}{
		{1, "username", f.Username, f.Username != "" && !strings.Contains(f.Username, "%")},
		{2, "channel_id", f.Channel, f.Channel != 0},
		{3, "model_name", f.ModelName, f.ModelName != "" && !strings.Contains(f.ModelName, "%")},
		{4, "`group`", f.Group, f.Group != ""},
	} {
		if s.enabled {
			branches = append(branches, fmt.Sprintf("(SELECT 'audit_plan_summary',%d,COUNT(*),COALESCE(MIN(id),0),COALESCE(MAX(id),0),0 FROM (SELECT id FROM logs WHERE %s=? LIMIT 257) AS probe)", s.kind, s.column))
			args = append(args, s.value)
		}
	}
	branches = append(branches, `(SELECT 'audit_plan_summary',5,COUNT(*),COALESCE(MIN(id),0),COALESCE(MAX(id),0),0 FROM
 (SELECT id FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at>=? AND created_at<=? ORDER BY created_at,type,id LIMIT 64) AS probe)`,
		`(SELECT 'audit_plan_summary',6,1,id,id,COALESCE(created_at,0) FROM logs FORCE INDEX (PRIMARY) ORDER BY id LIMIT 1)`,
		`(SELECT 'audit_plan_summary',7,1,id,id,COALESCE(created_at,0) FROM logs FORCE INDEX (PRIMARY) ORDER BY id DESC LIMIT 1)`)
	args = append(args, f.Start, f.End)
	return strings.Replace(strings.Join(branches, " UNION ALL "), "(SELECT ", "("+usageLogsSelect, 1), args
}
