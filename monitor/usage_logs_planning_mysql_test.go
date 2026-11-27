package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

func TestUsageLogsPlanningFiltersAndBudgetMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,
username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,` + "`group`" + ` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_logs_model_name(model_name),INDEX idx_logs_group(` + "`group`" + `),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/10),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),201,
'raw_name','m','t',7,'g',CONCAT('r',n),CONCAT('u',n) FROM seq WHERE n<=1000;
INSERT INTO logs VALUES(0,90,2,0,202,'old','old','old',9,'old','prefix','prefix'),(2000,300,2,0,202,'old','old','old',9,'old','tail','tail');ANALYZE TABLE logs;`)
	type result struct {
		Filter usageLogFilter
		Scope  usageLogWhereScope
		Route  []string
		Value  string
		Plans  []json.RawMessage
	}
	var results []result
	for _, f := range []usageLogFilter{
		{Channel: 7}, {ModelName: "m"}, {Group: "g"}, {Channel: 7, ModelName: "m", Group: "g"},
		{Username: "raw_name", Channel: 7}, {Username: "missing", Channel: 7}, {Username: "raw%", Channel: 7},
		{ModelName: "mo%", Channel: 7}, {Type: 5, Channel: 7}, {UserID: 201, Channel: 7}, {TokenName: "t", Channel: 7},
		{RequestID: "r20", Channel: 7}, {UpstreamRequestID: "u20", Channel: 7},
	} {
		f.Start, f.End = 100, 199
		for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
			w, err := buildUsageLogWhere(f, scope)
			if err != nil {
				t.Fatal(err)
			}
			expr := "COUNT(*)"
			if scope == usageLogWhereStat {
				expr = "COALESCE(SUM(quota),0)"
			}
			q := usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs"+w.sql(), w.args)
			value := run("USE sql_usage_perf;" + q + ";")
			r := result{Filter: f, Scope: scope, Route: usagePerfCheckActualDispatch(t, run, f, scope, value), Value: value}
			planning := f
			if scope == usageLogWhereStat {
				planning.Type = 0
			}
			p, pa := usageLogsAggregateProbeSQL(planning, w, expr)
			for _, planned := range []string{usageLogsSyntheticBind(t, p, pa), usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs FORCE INDEX (idx_created_at_type)"+w.sql(), w.args)} {
				plan := json.RawMessage(run("USE sql_usage_perf;EXPLAIN FORMAT=JSON " + planned + ";"))
				if !json.Valid(plan) {
					t.Fatal("invalid planner/forced-time plan")
				}
				r.Plans = append(r.Plans, plan)
			}
			results = append(results, r)
		}
	}
	// Make only the last source-edge scalar expensive. The top SELECT hint
	// must cover the entire plan branch, not just the first selector.
	f := usageLogFilter{Start: 100, End: 199, Channel: 7}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	q, a := usageLogsAggregateProbeSQL(f, w, "COUNT(*)")
	q = strings.Replace(q, "MAX_EXECUTION_TIME(18000)", "MAX_EXECUTION_TIME(10)", 1)
	old := "COALESCE(created_at,0)) FROM logs"
	last := strings.LastIndex(q, old)
	if last < 0 {
		t.Fatal("last branch template changed")
	}
	q = q[:last] + strings.Replace(q[last:], old, "COALESCE(created_at,0)+(SELECT COUNT(*) FROM logs AS budget_a CROSS JOIN logs AS budget_b CROSS JOIN logs AS budget_c WHERE budget_a.id+budget_b.id+budget_c.id>=0)) FROM logs", 1)
	exactF := usageLogFilter{Start: 100, End: 199, Channel: 999}
	exactW, _ := buildUsageLogWhere(exactF, usageLogWhereList)
	exactQ, exactA := usageLogsAggregateProbeSQL(exactF, exactW, "COUNT(*)+(SELECT COUNT(*) FROM logs AS budget_a CROSS JOIN logs AS budget_b CROSS JOIN logs AS budget_c WHERE budget_a.id+budget_b.id+budget_c.id>=0)")
	exactQ = strings.Replace(exactQ, "MAX_EXECUTION_TIME(18000)", "MAX_EXECUTION_TIME(10)", 1)
	for _, c := range []struct {
		name, q string
		a       []any
	}{{"plan", q, a}, {"exact", exactQ, exactA}} {
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", os.Getenv("MONITOR_SQL_OPT_CONTAINER"), "mysql", "--protocol=socket", "-uroot", "--batch", "--skip-column-names")
		cmd.Stdin = strings.NewReader("USE sql_usage_perf;" + usageLogsSyntheticBind(t, c.q, c.a) + ";SHOW WARNINGS;")
		var stderr, stdout bytes.Buffer
		cmd.Stderr = &stderr
		cmd.Stdout = &stdout
		err := cmd.Run()
		ctxErr := ctx.Err()
		cancel()
		if err == nil || ctxErr != nil || !strings.Contains(stderr.String(), "ERROR 3024") {
			t.Fatalf("%s probe not source-timed-out: err=%v ctx=%v stderr=%s stdout=%s", c.name, err, ctxErr, stderr.String(), stdout.String())
		}
	}
	raw, err := json.MarshalIndent(struct {
		Warning                 string
		Results                 []result
		WholeProbeSourceTimeout string
	}{"Synthetic actual MySQL dispatch and EXPLAIN. No production data, p95 or write durability claim.", results, "3024 for both plan-scalar and exact aggregate non-separable synthetic joins under top SELECT hint"}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/usage-fused-filters-budget-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d actual dispatch/exact-result cases; planner/time EXPLAIN each; exact and plan source budgets enforced", len(results))
}
