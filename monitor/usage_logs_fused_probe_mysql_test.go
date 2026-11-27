package monitor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func TestUsageLogsFusedProbeMatrixMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,
username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,` + "`group`" + ` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),padding VARCHAR(32),
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_logs_model_name(model_name),INDEX idx_logs_group(` + "`group`" + `),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/5),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),201,
IF(MOD(n,10000)=0,'rare','heavy'),'m','t',IF(MOD(n,2)=0,7,9),'g',CONCAT('r',n),CONCAT('u',n),REPEAT('p',32) FROM seq WHERE n<=100000;ANALYZE TABLE logs;`)
	type sample struct {
		Filter     usageLogFilter
		Scope      usageLogWhereScope
		Variant    string
		US         []int64
		Handlers   []map[string]int64
		Statements int
		Plans      []json.RawMessage
		Analyze    []string
		Route      []string
		Value      string
	}
	var results []sample
	for _, tc := range []struct {
		f            usageLogFilter
		distribution string
	}{
		{usageLogFilter{Start: 18100, End: 20099, Username: "heavy", Channel: 7}, ""},
		{usageLogFilter{Start: 100, End: 20099, Username: "heavy", Channel: 7}, ""},
		{usageLogFilter{Start: 100, End: 20099, Username: "rare", Channel: 7}, ""},
		{usageLogFilter{Start: 100, End: 20099, Username: "missing", Channel: 7}, ""},
		{usageLogFilter{Start: 100, End: 20099, Channel: 999}, ""},
		{usageLogFilter{Start: 18100, End: 20099, Channel: 7, ModelName: "m", Group: "g"}, ""},
		{usageLogFilter{Start: 18100, End: 20099}, ""},
		{usageLogFilter{Start: 100, End: 20099}, ""},
		{usageLogFilter{Start: 20100, End: 20200, Channel: 7}, ""},
		{usageLogFilter{Start: 100, End: 102, Channel: 7}, "sparse"},
		{usageLogFilter{Start: 100, End: 1099, Username: "heavy", Channel: 7}, "shuffle"},
	} {
		if tc.distribution == "sparse" {
			run("USE sql_usage_perf;UPDATE logs SET created_at=IF(MOD(id,100)=0,101,90);ANALYZE TABLE logs;")
		}
		if tc.distribution == "shuffle" {
			run("USE sql_usage_perf;UPDATE logs SET created_at=100+MOD(id*7919,20000);ANALYZE TABLE logs;")
		}
		f := tc.f
		scope, expr := usageLogWhereList, "COUNT(*)"
		if f.Username == "" {
			scope, expr = usageLogWhereStat, "COALESCE(SUM(quota),0)"
		}
		w, err := buildUsageLogWhere(f, scope)
		if err != nil {
			t.Fatal(err)
		}
		native := usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs"+w.sql(), w.args)
		want := run("USE sql_usage_perf;" + native + ";")
		follow := func(plan int) []string {
			if plan == usageLogPlanNative {
				return []string{native}
			}
			if plan == usageLogPlanTime {
				return []string{usageLogsSyntheticBind(t, usageLogsSelect+expr+" FROM logs FORCE INDEX (idx_created_at_type)"+w.sql(), w.args)}
			}
			q, a := usageLogsAggregateSpanSQL(f, w, expr)
			q = usageLogsSyntheticBind(t, q, a)
			if run("USE sql_usage_perf;"+q+";") == "NULL" {
				return []string{q, native}
			}
			return []string{q}
		}
		old, oa := usageLogsPlanningSummarySQL(f)
		old = usageLogsSyntheticBind(t, old, oa)
		_, _, _, plan := usagePerfSample(t, run, old+";SELECT 0", f)
		legacy := append([]string{old}, follow(plan)...)
		q, a := usageLogsAggregateProbeSQL(f, w, expr)
		q = usageLogsSyntheticBind(t, q, a)
		probe, err := decodeUsageLogsAggregateProbe(f, []byte(run("USE sql_usage_perf;"+q+";")))
		if err != nil {
			t.Fatal(err)
		}
		fused := []string{q}
		if !probe.exact {
			fused = append(fused, follow(probe.plan)...)
		}
		scripts := [][]string{{native}, legacy, fused}
		start := len(results)
		for i, v := range []string{"native", "old-planning", "fused"} {
			r := sample{Filter: f, Scope: scope, Variant: v, Statements: len(scripts[i]), Value: want}
			for _, statement := range scripts[i] {
				p := json.RawMessage(run("USE sql_usage_perf;EXPLAIN FORMAT=JSON " + statement + ";"))
				if !json.Valid(p) {
					t.Fatal("invalid plan")
				}
				r.Plans = append(r.Plans, p)
				r.Analyze = append(r.Analyze, run("USE sql_usage_perf;EXPLAIN ANALYZE "+statement+";"))
			}
			results = append(results, r)
		}
		for round := 0; round < 7; round++ {
			for j := range scripts {
				i := j
				if round%2 != 0 {
					i = len(scripts) - 1 - j
				}
				v, us, h, _ := usagePerfSample(t, run, strings.Join(scripts[i], ";"), f)
				lines := strings.Split(v, "\n")
				if i == 2 {
					p, e := decodeUsageLogsAggregateProbe(f, []byte(lines[0]))
					if e != nil {
						t.Fatal(e)
					}
					if p.exact {
						v = strconv.FormatInt(p.value, 10)
					} else {
						v = strings.Join(lines[1:], "\n")
					}
				}
				if strings.TrimPrefix(v, "NULL\n") != want {
					t.Fatalf("%s changed exact result: %q != %q", results[start+i].Variant, v, want)
				}
				r := &results[start+i]
				r.US = append(r.US, us)
				r.Handlers = append(r.Handlers, h)
			}
		}
		// Observe the fused probe and actual final query, not a hand-built route.
		results[start+2].Route = usagePerfCheckActualDispatch(t, run, f, scope, want)
	}
	hashes := map[string]string{}
	for _, p := range []string{"usage_logs_aggregate_probe.go", "usage_logs_aggregate_plan.go", "usage_logs_quota.go", "usage_logs_fused_probe_mysql_test.go"} {
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		hashes[p] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	raw, err := json.MarshalIndent(struct {
		Warning      string
		Results      []sample
		SourceSHA256 map[string]string
	}{"100000 synthetic rows; seven alternating full-path samples; every statement EXPLAIN+ANALYZE. Server-session wall including CLI delivery/gaps, not HTTP/p95 or physical IO. Actual Go route verified separately; immutable fixture, no cross-statement transaction claim.", results, hashes}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/usage-fused-probe-final-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d variants, %d exact comparisons", len(results), len(results)*7)
}
