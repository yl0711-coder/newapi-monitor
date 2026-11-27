package monitor

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

// All DDL and writes below target only the task-owned network-none tmpfs
// container. This is evidence for a separate index decision, NOT online DDL.
func TestUsageLogsIndexAndMillionCapMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,
username VARCHAR(16),channel_id INT,INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_created_at_type(created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/20),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),
IF(MOD(n,10000)=0,'rare','heavy'),IF(MOD(n,2)=0,7,9) FROM seq WHERE n<=200000;ANALYZE TABLE logs;`)
	type observation struct {
		Stage, Name, Result string
		US                  []int64
		Plan                json.RawMessage
	}
	var observations []observation
	var pages []string
	queries := []struct{ name, q string }{
		{"heavy_count", `SELECT COUNT(*) FROM logs WHERE username='heavy' AND channel_id=7 AND created_at>=100 AND created_at<=10099`},
		{"rare_count", `SELECT COUNT(*) FROM logs WHERE username='rare' AND channel_id=7 AND created_at>=100 AND created_at<=10099`},
		{"broad_sum", `SELECT COALESCE(SUM(quota),0) FROM logs WHERE type=2 AND created_at>=100 AND created_at<=10099`},
		{"channel_sum", `SELECT COALESCE(SUM(quota),0) FROM logs WHERE type=2 AND channel_id=7 AND created_at>=100 AND created_at<=10099`},
	}
	want := map[string]string{}
	for _, stage := range []string{"existing_indexes", "isolated_compound_and_cover"} {
		if stage != "existing_indexes" {
			run(`USE sql_usage_perf;ALTER TABLE logs ADD INDEX audit_username_channel_time_type(username,channel_id,created_at,type),ADD INDEX audit_time_type_quota(created_at,type,quota);ANALYZE TABLE logs;`)
		}
		for _, p := range queries {
			o := observation{Stage: stage, Name: p.name}
			for n := 0; n < 4; n++ {
				value, us := timedSyntheticSQL(t, run, "sql_usage_perf", p.q)
				o.US = append(o.US, us)
				o.Result = value
				if stage == "existing_indexes" {
					want[p.name] = value
				}
				if value != want[p.name] {
					t.Fatal("index changed exact result", stage, p.name)
				}
			}
			o.Plan = json.RawMessage(run("USE sql_usage_perf;EXPLAIN FORMAT=JSON " + p.q + ";"))
			if !json.Valid(o.Plan) {
				t.Fatal("invalid plan")
			}
			observations = append(observations, o)
			if stage != "existing_indexes" {
				index := "audit_time_type_quota"
				if p.name == "heavy_count" || p.name == "rare_count" {
					index = "audit_username_channel_time_type"
				}
				forced := strings.Replace(p.q, "FROM logs WHERE", "FROM logs FORCE INDEX ("+index+") WHERE", 1)
				fo := observation{Stage: stage, Name: p.name + "_forced"}
				for n := 0; n < 4; n++ {
					value, us := timedSyntheticSQL(t, run, "sql_usage_perf", forced)
					if value != want[p.name] {
						t.Fatal("forced index changed result")
					}
					fo.Result = value
					fo.US = append(fo.US, us)
				}
				fo.Plan = json.RawMessage(run("USE sql_usage_perf;EXPLAIN FORMAT=JSON " + forced + ";"))
				if !json.Valid(fo.Plan) {
					t.Fatal("invalid forced plan")
				}
				observations = append(observations, fo)
			}
		}
		// Transactional rolled-back insert latency is only an index-maintenance
		// signal in memory, not fsync/commit durability or production write cost.
		o := observation{Stage: stage, Name: "1000_insert_then_rollback"}
		for n := 0; n < 3; n++ {
			_, us := timedSyntheticSQL(t, run, "sql_usage_perf", `START TRANSACTION;INSERT INTO logs SELECT id+300000,created_at,type,quota,username,channel_id FROM logs WHERE id BETWEEN 1 AND 1000;ROLLBACK;SELECT 'rolled_back'`)
			o.US = append(o.US, us)
		}
		observations = append(observations, o)
		pages = append(pages, stage+"\n"+run(`SELECT index_name,stat_value FROM mysql.innodb_index_stats WHERE database_name='sql_usage_perf' AND table_name='logs' AND stat_name='size' ORDER BY index_name;`))
	}
	// Reuse the same narrow table, removing only the experimental indexes.
	// A real >1,000,000-row range exercises the actual, unchanged production cap.
	run(`USE sql_usage_perf;ALTER TABLE logs DROP INDEX audit_username_channel_time_type,DROP INDEX audit_time_type_quota;
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n+200000,100+FLOOR((n+199999)/20),IF(MOD(n+200000,11)=0,5,2),IF(MOD(n+200000,97)=0,NULL,MOD(n+200000,7)-3),'heavy',IF(MOD(n,2)=0,7,9) FROM seq WHERE n<=900000;
INSERT INTO logs VALUES(0,90,2,0,'prefix',9),(2000000,80000,2,0,'tail',9);ANALYZE TABLE logs;`)
	type capResult struct {
		Name, Operation, Value string
		Route                  []string
	}
	var caps []capResult
	for _, window := range []struct {
		name string
		end  int64
	}{{"at_million", 50099}, {"over_million", 60099}} {
		f := usageLogFilter{Start: 100, End: window.end, Channel: 7}
		for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
			w, err := buildUsageLogWhere(f, scope)
			if err != nil {
				t.Fatal(err)
			}
			expr := "COUNT(*)"
			if scope == usageLogWhereStat {
				expr = "COALESCE(SUM(quota),0)"
			}
			q, a := usageLogsAggregateSpanSQL(f, w, expr)
			span := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, q, a) + ";")
			value := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs"+w.sql(), w.args) + ";")
			if window.name == "over_million" && span != "NULL" {
				t.Fatal("real cap not exercised")
			}
			if window.name == "at_million" && span != value {
				t.Fatal("exact cap result changed")
			}
			route := usagePerfCheckActualDispatch(t, run, f, scope, value)
			wantRoute := "probe,span"
			if window.name == "over_million" {
				wantRoute += ",native"
			}
			if strings.Join(route, ",") != wantRoute {
				t.Fatalf("wrong real cap route %v want %s", route, wantRoute)
			}
			caps = append(caps, capResult{window.name, expr, value, route})
		}
	}
	run("USE sql_usage_perf;DELETE FROM logs WHERE id=2000000;")
	f := usageLogFilter{Start: 100, End: 60099, Channel: 7}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	value := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, usageLogsSelect+"COUNT(*) FROM logs"+w.sql(), w.args) + ";")
	route := usagePerfCheckActualDispatch(t, run, f, usageLogWhereList, value)
	if strings.Join(route, ",") != "probe,native" {
		t.Fatalf("cap hint not exercised: %v", route)
	}
	caps = append(caps, capResult{"over_million_early_native", "COUNT(*)", value, route})
	raw, err := json.MarshalIndent(struct {
		Warning      string
		Observations []observation
		IndexPages   []string
		Caps         []capResult
	}{"Synthetic isolated MySQL only; index pages are logical allocation (16KiB/page), not production size. Inserts rolled back, not durability benchmarks. No online DDL.", observations, pages, caps}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/usage-index-and-real-cap-forced-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("index alternatives measured; exact cap/fallback routes=%v", caps)
}
