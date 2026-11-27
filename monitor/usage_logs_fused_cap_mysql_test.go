package monitor

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestUsageLogsFusedMillionCapMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	// Narrow memory fixture: do not rerun unrelated experimental index DDL.
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,username VARCHAR(16),channel_id INT,
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_created_at_type(created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/20),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),'heavy',IF(MOD(n,2)=0,7,9) FROM seq;
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n+1000000,50100+FLOOR((n-1)/20),IF(MOD(n,11)=0,5,2),MOD(n,7)-3,'heavy',IF(MOD(n,2)=0,7,9) FROM seq WHERE n<=100000;
INSERT INTO logs VALUES(0,90,2,0,'edge',9),(2000000,80000,2,0,'edge',9);ANALYZE TABLE logs;`)
	type result struct {
		Name  string
		Scope usageLogWhereScope
		Value string
		Route []string
	}
	var results []result
	for _, c := range []struct {
		name  string
		end   int64
		route string
	}{{"at_million", 50099, "probe,span"}, {"over_million", 60099, "probe,span,native"}, {"early_native", 80000, "probe,native"}} {
		for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
			f := usageLogFilter{Start: 100, End: c.end, Channel: 7}
			w, e := buildUsageLogWhere(f, scope)
			if e != nil {
				t.Fatal(e)
			}
			expr := "COUNT(*)"
			if scope == usageLogWhereStat {
				expr = "COALESCE(SUM(quota),0)"
			}
			value := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs"+w.sql(), w.args) + ";")
			route := usagePerfCheckActualDispatch(t, run, f, scope, value)
			if strings.Join(route, ",") != c.route {
				t.Fatalf("wrong cap route %s %v", c.name, route)
			}
			results = append(results, result{c.name, scope, value, route})
		}
	}
	raw, e := json.MarshalIndent(results, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("../docs/sql-audit-20261126/usage-fused-million-cap-20261127.json", append(raw, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	t.Logf("%d real 1.1m-row cap/exact fallback cases", len(results))
}
