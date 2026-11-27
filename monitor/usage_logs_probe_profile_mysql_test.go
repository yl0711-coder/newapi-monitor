package monitor

import (
	"os"
	"testing"
)

// Diagnostic only: requires the task-owned matrix fixture already prepared in
// the same isolated container. Never opens a configured production database.
func TestUsageLogsProbeProfileMySQL(t *testing.T) {
	if os.Getenv("MONITOR_SQL_PROBE_PROFILE") != "1" {
		t.Skip("opt-in diagnostic; requires prepared task fixture")
	}
	run := isolatedSQLOptimizationRunner(t)
	f := usageLogFilter{Start: 100, End: 20099, Username: "rare", Channel: 7}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	q, a := usageLogsAggregateProbeSQL(f, w, "COUNT(*)")
	out := run("USE sql_usage_perf;SET profiling=1;" + usageLogsSyntheticBind(t, q, a) + ";SHOW PROFILE FOR QUERY 1;")
	if err := os.WriteFile("../docs/sql-audit-20261126/usage-fused-probe-profile-20261127.txt", []byte(out+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	t.Log(out)
}
