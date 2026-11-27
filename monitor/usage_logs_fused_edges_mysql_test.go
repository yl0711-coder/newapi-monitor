package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestUsageLogsFusedProbeEdgesMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,username VARCHAR(80),channel_id INT,model_name VARCHAR(80),` + "`group`" + ` VARCHAR(80),token_name VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_logs_model_name(model_name),INDEX idx_logs_group(` + "`group`" + `),INDEX idx_created_at_type(created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/5),IF(MOD(n,11)=0,5,2),IF(n=1,9007199254740993,IF(MOD(n,17)=0,NULL,MOD(n,7)-3)),201,'heavy',7,'m','g','t','r','u' FROM seq WHERE n<=1000;
INSERT INTO logs VALUES(0,90,2,0,202,'edge',9,'edge','edge','t','r','u'),(2000000,400,2,0,202,'edge',9,'edge','edge','t','r','u');`)
	type result struct {
		N     int
		Scope usageLogWhereScope
		Type  int
		Exact bool
		Value string
		Route []string
	}
	var results []result
	for _, n := range []int{0, 1, 63, 64, 256, 257} {
		run(fmt.Sprintf("USE sql_usage_perf;UPDATE logs SET username=IF(id>0 AND id<=%d,'selected','heavy') WHERE id>0 AND id<2000000;", n))
		for _, typ := range []int{0, 2, 5} {
			for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
				f := usageLogFilter{Start: 100, End: 350, Username: "selected", Channel: 7, Type: typ}
				w, e := buildUsageLogWhere(f, scope)
				if e != nil {
					t.Fatal(e)
				}
				expr := "COUNT(*)"
				planning := f
				if scope == usageLogWhereStat {
					expr = "COALESCE(SUM(quota),0)"
					planning.Type = 0
				}
				want := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs"+w.sql(), w.args) + ";")
				q, a := usageLogsAggregateProbeSQL(planning, w, expr)
				p, e := decodeUsageLogsAggregateProbe(planning, []byte(run("USE sql_usage_perf;"+usageLogsSyntheticBind(t, q, a)+";")))
				if e != nil {
					t.Fatal(e)
				}
				if p.exact != (n <= 256) {
					t.Fatalf("wrong selector threshold %d %+v", n, p)
				}
				if p.exact && strconv.FormatInt(p.value, 10) != want {
					t.Fatal("exact amount/count lost precision", p.value, want)
				}
				// Stat's username scope deliberately stays native in production.
				route := usagePerfCheckActualDispatch(t, run, f, scope, want)
				results = append(results, result{n, scope, typ, p.exact, want, route})
			}
		}
	}
	// Insert AFTER a planning-only snapshot, before the actual span statement.
	// Its high ID must be included by fresh full bounds or exact fallback.
	run("USE sql_usage_perf;UPDATE logs SET username='heavy' WHERE id>0 AND id<2000000;")
	var route []string
	injected := false
	wrapped := func(q string) string {
		out := run(q)
		if !injected && strings.Contains(q, "AS usage_aggregate_probe") {
			injected = true
			run("USE sql_usage_perf;INSERT INTO logs VALUES(1000000,150,2,19,201,'heavy',7,'m','g','t','late','late');")
		}
		return out
	}
	name := fmt.Sprintf("fused-insert-mysql-%d", usageQuotaDriverID.Add(1))
	sql.Register(name, usagePerfSocketMySQLDriver{t, wrapped, &route})
	db, e := sql.Open(name, "")
	if e != nil {
		t.Fatal(e)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	f := usageLogFilter{Start: 100, End: 220, Username: "heavy", Channel: 7}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	m := &Monitor{prodDB: db}
	got, e := m.queryUsageLogsCount(context.Background(), db, f, w)
	if e != nil {
		t.Fatal(e)
	}
	want := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, "SELECT COUNT(*) FROM logs"+w.sql(), w.args) + ";")
	if !injected || strconv.FormatInt(got, 10) != want || strings.Join(route, ",") != "probe,span,native" || db.Stats().InUse != 0 {
		t.Fatalf("concurrent insert lost/route leak: %d %s %v", got, want, route)
	}
	raw, e := json.MarshalIndent(struct {
		Results         []result
		ConcurrentRoute []string
		ConcurrentCount string
	}{results, route, want}, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	if e = os.WriteFile("../docs/sql-audit-20261126/usage-fused-edges-20261127.json", append(raw, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	t.Logf("%d MySQL threshold/type/amount cases, concurrent insert route %v", len(results), route)
}
