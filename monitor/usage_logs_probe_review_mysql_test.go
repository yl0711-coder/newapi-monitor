package monitor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"testing"
)

// Test-only alternative: the selector probe needs a threshold, not MIN/MAX or
// an exact count. OFFSET is inside EXISTS; it never truncates the final result.
func usagePlanningExistsCandidate(t *testing.T, f usageLogFilter) (string, []any) {
	t.Helper()
	q, args := usageLogsPlanningSummarySQL(f)
	for _, column := range []string{"username", "channel_id", "model_name", "`group`"} {
		old := "COUNT(*),COALESCE(MIN(id),0),COALESCE(MAX(id),0),0 FROM (SELECT id FROM logs WHERE " + column + "=? LIMIT 257) AS probe"
		next := "IF(EXISTS(SELECT 1 FROM logs WHERE " + column + "=? LIMIT 256,1),257,0),0,0,0"
		q = strings.ReplaceAll(q, old, next)
	}
	return q, args
}

func TestUsageLogsProbeReviewMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,
username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,` + "`group`" + ` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),padding VARCHAR(32),
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_logs_model_name(model_name),INDEX idx_logs_group(` + "`group`" + `),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/5),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),201,
IF(MOD(n,10000)=0,'rare','heavy'),'m','t',IF(MOD(n,2)=0,7,9),'g',CONCAT('r',n),CONCAT('u',n),REPEAT('p',32) FROM seq WHERE n<=100000;ANALYZE TABLE logs;`)
	var results []usagePerfResult
	var dispatch []struct {
		Filter usageLogFilter
		Scope  usageLogWhereScope
		Route  []string
		Value  string
	}
	for _, f := range []usageLogFilter{
		{Start: 18100, End: 20099, Username: "heavy", Channel: 7},
		{Start: 100, End: 20099, Username: "heavy", Channel: 7},
		{Start: 100, End: 20099, Username: "rare", Channel: 7},
		{Start: 100, End: 20099, Username: "missing", Channel: 7},
		{Start: 100, End: 20099, Channel: 999},
		{Start: 18100, End: 20099, Channel: 7, ModelName: "m", Group: "g"},
		{Start: 18100, End: 20099},
		{Start: 100, End: 20099},
	} {
		scope, expression := usageLogWhereList, "COUNT(*)"
		if f.Username == "" {
			scope, expression = usageLogWhereStat, "COALESCE(SUM(quota),0)"
		}
		w, err := buildUsageLogWhere(f, scope)
		if err != nil {
			t.Fatal(err)
		}
		native := usageLogsSyntheticBind(t, usageLogsSelect+expression+" FROM logs"+w.sql(), w.args)
		want := run("USE sql_usage_perf;" + native + ";")
		dispatch = append(dispatch, struct {
			Filter usageLogFilter
			Scope  usageLogWhereScope
			Route  []string
			Value  string
		}{f, scope, usagePerfCheckActualDispatch(t, run, f, scope, want), want})
		variants := []string{"native", "current", "exists"}
		if f.Username != "" {
			variants = append(variants, "scalar-exact", "bounded-exact")
		}
		start := len(results)
		var scripts []string
		for _, variant := range variants {
			r := usagePerfResult{Scenario: fmt.Sprintf("%+v", f), Operation: expression, Variant: variant}
			q := native
			if variant != "native" {
				p, a := usageLogsPlanningSummarySQL(f)
				if variant == "exists" {
					p, a = usagePlanningExistsCandidate(t, f)
				}
				p = usageLogsSyntheticBind(t, p, a)
				_, _, _, plan := usagePerfSample(t, run, p+";SELECT 0", f)
				final := native
				if plan == usageLogPlanTime {
					final = usageLogsSyntheticBind(t, usageLogsSelect+expression+" FROM logs FORCE INDEX(idx_created_at_type)"+w.sql(), w.args)
				} else if plan == usageLogPlanSpan {
					s, a := usageLogsAggregateSpanSQL(f, w, expression)
					final = usageLogsSyntheticBind(t, s, a)
					v := run("USE sql_usage_perf;" + final + ";")
					if v == "NULL" {
						final += ";" + native
					}
				}
				q = p + ";" + final
				if variant == "scalar-exact" || variant == "bounded-exact" {
					gate := usageLogsSelect + `CASE WHEN EXISTS(SELECT 1 FROM logs WHERE username=? LIMIT 256,1) THEN NULL ELSE (SELECT COUNT(*) FROM logs` + w.sql() + `) END`
					a := append([]any{f.Username}, w.args...)
					if variant == "bounded-exact" {
						gate = usageLogsSelect + `CASE WHEN COUNT(*)<=256 THEN COALESCE(SUM(CASE WHEN created_at>=? AND created_at<=? AND channel_id=? THEN 1 ELSE 0 END),0) ELSE NULL END FROM (SELECT created_at,channel_id FROM logs WHERE username=? LIMIT 257) AS exact_probe`
						a = []any{f.Start, f.End, f.Channel, f.Username}
					}
					gate = usageLogsSyntheticBind(t, gate, a)
					v := run("USE sql_usage_perf;" + gate + ";")
					if v == "NULL" {
						q = gate + ";" + q
					} else {
						q = gate
					}
				}
			}
			first := strings.Split(q, ";")[0]
			r.Plan = json.RawMessage(run("USE sql_usage_perf;EXPLAIN FORMAT=JSON " + first + ";"))
			if !json.Valid(r.Plan) {
				t.Fatal("invalid actual candidate plan")
			}
			r.Analyze = run("USE sql_usage_perf;EXPLAIN ANALYZE " + first + ";")
			results = append(results, r)
			scripts = append(scripts, q)
		}
		for round := 0; round < 7; round++ {
			for j := range scripts {
				i := j
				if round%2 != 0 {
					i = len(scripts) - 1 - j
				}
				v, us, h, _ := usagePerfSample(t, run, scripts[i], f)
				if strings.TrimPrefix(v, "NULL\n") != want {
					t.Fatalf("%s changed exact result: %q vs %q", variants[i], v, want)
				}
				r := &results[start+i]
				r.Values = append(r.Values, v)
				r.US = append(r.US, us)
				r.Handlers = append(r.Handlers, h)
			}
		}
	}
	hashes := map[string]string{}
	for _, p := range []string{"usage_logs_quota.go", "usage_logs_aggregate_plan.go", "usage_logs_probe_review_mysql_test.go"} {
		b, err := os.ReadFile(p)
		if err != nil {
			t.Fatal(err)
		}
		hashes[p] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	raw, err := json.MarshalIndent(struct {
		Warning      string
		Results      []usagePerfResult
		Dispatch     any
		SourceSHA256 map[string]string
	}{"100000 synthetic rows. Complete path including probe/final/fallback. Immutable fixture; dispatch choice computed before timing. Seven alternating order samples. Server-session wall, not HTTP/p95 or physical IO. Actual Go dispatch separately verifies results/routes, not latency. Plan/Analyze cover the FIRST statement of each script, not all fallback statements; existing span plans are separate evidence. Bounded-exact only covers these type-free test cases, not adopted production code.", results, dispatch, hashes}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/usage-probe-review-experiment-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
