package monitor

// Candidates remain test-only until semantics, plans and the complete fallback
// path are measured. No current-user lookup or approximate result is allowed.
import (
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

func usageCountSelectiveProbeCandidate(t *testing.T, f usageLogFilter, w *usageLogWhere) (string, []any) {
	t.Helper()
	span, args := usageLogsAggregateSpanSQL(f, w, "COUNT(*)")
	span = strings.TrimPrefix(span, usageLogsSelect)
	// An uncorrelated aggregate-derived table can be evaluated by MySQL even
	// when its surrounding CASE arm is not chosen. Gate the source scan too.
	bound := "WHERE created_at>=? AND created_at<=?) AS bounds"
	if strings.Count(span, bound) != 1 {
		t.Fatal("boundary template changed")
	}
	span = strings.Replace(span, bound, "WHERE created_at>=? AND created_at<=? AND gate.n>256) AS bounds", 1)
	// The small username probe is planning evidence only: either final branch
	// still computes the exact original predicate, without a result row limit.
	q := usageLogsSelect + `CASE WHEN gate.n<=256 THEN
 (SELECT COUNT(*) FROM logs` + w.sql() + `)
 ELSE (SELECT ` + span + `) END AS quota
 FROM (SELECT COUNT(*) AS n FROM
 (SELECT id FROM logs FORCE INDEX(idx_logs_username) WHERE username=? LIMIT 257) AS planning_probe) AS gate`
	all := append([]any{}, w.args...)
	all = append(all, args...)
	return q, append(all, f.Username)
}

func usageSpanInlineFallbackCandidate(t *testing.T, f usageLogFilter, w *usageLogWhere, expression string) (string, []any) {
	t.Helper()
	q, args := usageLogsAggregateSpanSQL(f, w, expression)
	old := "ELSE NULL END AS quota"
	if strings.Count(q, old) != 1 {
		t.Fatal("span fallback template changed")
	}
	q = strings.Replace(q, old, "ELSE (SELECT "+expression+" FROM logs"+w.sql()+") END AS quota", 1)
	a := append([]any{}, args[:len(args)-2]...)
	a = append(a, w.args...)
	a = append(a, args[len(args)-2:]...)
	return q, a
}

// An exact short result or planning-only NULL, never the truncated probe count.
// Keep a possible range aggregate in a subsequent statement: an uncorrelated
// derived table precomputes even in an unused CASE arm, while correlating it
// prevents the constant primary-range optimization in the measured engine.
func usageCountSmallNameCandidate(f usageLogFilter, w *usageLogWhere) (string, []any) {
	q := usageLogsSelect + `CASE WHEN gate.n<=256 THEN
 (SELECT COUNT(*) FROM logs` + w.sql() + `)
 ELSE NULL END AS planned_count
 FROM (SELECT COUNT(*) AS n FROM
 (SELECT id FROM logs WHERE username=? LIMIT 257) AS planning_probe) AS gate`
	return q, append(append([]any{}, w.args...), f.Username)
}

func usageAggregatePlanningCandidate(f usageLogFilter, w *usageLogWhere, expression string) (string, []any) {
	var conditions, from []string
	var fromArgs []any
	for _, s := range []struct {
		column  string
		value   any
		enabled bool
	}{
		{"username", f.Username, f.Username != "" && !strings.Contains(f.Username, "%")},
		{"channel_id", f.Channel, f.Channel != 0},
		{"model_name", f.ModelName, f.ModelName != "" && !strings.Contains(f.ModelName, "%")},
		{"`group`", f.Group, f.Group != ""},
	} {
		if !s.enabled {
			continue
		}
		alias := "selector_" + strconv.Itoa(len(from))
		from = append(from, "(SELECT COUNT(*) AS n FROM (SELECT id FROM logs WHERE "+s.column+"=? LIMIT 257) AS planning_keys) AS "+alias)
		fromArgs = append(fromArgs, s.value)
		conditions = append(conditions, alias+".n<=256")
	}
	from = append(from, `(SELECT MIN(id) AS low_id,MAX(id) AS high_id,COUNT(*) AS n FROM
 (SELECT id FROM logs FORCE INDEX(idx_created_at_type) WHERE created_at>=? AND created_at<=?
 ORDER BY created_at,type,id LIMIT 64) AS time_keys) AS window_probe`,
		`(SELECT (SELECT created_at FROM logs FORCE INDEX(PRIMARY) ORDER BY id LIMIT 1) AS first_at,
 (SELECT created_at FROM logs FORCE INDEX(PRIMARY) ORDER BY id DESC LIMIT 1) AS last_at,
 (SELECT id FROM logs FORCE INDEX(PRIMARY) ORDER BY id DESC LIMIT 1) AS last_id) AS edges`)
	fromArgs = append(fromArgs, f.Start, f.End)
	conditions = append(conditions, "window_probe.n<64", "window_probe.low_id<0", "CAST(window_probe.high_id AS DECIMAL(65,0))-CAST(window_probe.low_id AS DECIMAL(65,0))>2*window_probe.n-1",
		"(edges.first_at>=? AND edges.first_at<=? AND edges.last_at>=? AND edges.last_at<=?)",
		"(edges.last_at>=? AND edges.last_at<=? AND CAST(edges.last_id AS DECIMAL(65,0))-CAST(window_probe.low_id AS DECIMAL(65,0))>=?)")
	args := []any{f.Start, f.End, f.Start, f.End, f.Start, f.End, usageLogsQuotaSpanMaxRows}
	args = append(args, w.args...)
	args = append(args, fromArgs...)
	return usageLogsSelect + "CASE WHEN " + strings.Join(conditions, " OR ") + " THEN (SELECT " + expression + " FROM logs" + w.sql() + ") ELSE NULL END AS planned_aggregate FROM " + strings.Join(from, " CROSS JOIN "), args
}

type usagePerfResult struct {
	Scenario  string
	Operation string
	Variant   string
	Values    []string
	US        []int64
	Handlers  []map[string]int64
	Plan      json.RawMessage
	Analyze   string   `json:",omitempty"`
	AppRoute  []string `json:",omitempty"`
}

func usagePerfSample(t *testing.T, run func(string) string, q string, f usageLogFilter) (string, int64, map[string]int64, int) {
	t.Helper()
	out := run("USE sql_usage_perf; SET @audit_started=NOW(6);" + q + `;SELECT 'audit_elapsed_us',TIMESTAMPDIFF(MICROSECOND,@audit_started,NOW(6)); SHOW SESSION STATUS WHERE Variable_name LIKE 'Handler_read%';`)
	var values []string
	var us int64 = -1
	h := map[string]int64{}
	var keys []usageLogPlanningKey
	var summaries []usageLogPlanningSummary
	for _, line := range strings.Split(out, "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) == 6 && parts[0] == "audit_plan_summary" {
			values := make([]int64, 5)
			for n := range values {
				v, err := strconv.ParseInt(parts[n+1], 10, 64)
				if err != nil {
					t.Fatal(err)
				}
				values[n] = v
			}
			summaries = append(summaries, usageLogPlanningSummary{int(values[0]), int(values[1]), values[2], values[3], values[4]})
		} else if len(parts) == 4 && parts[0] == "audit_plan_key" {
			kind, err := strconv.Atoi(parts[1])
			if err != nil {
				t.Fatal(err)
			}
			id, err := strconv.ParseInt(parts[2], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			at, err := strconv.ParseInt(parts[3], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			keys = append(keys, usageLogPlanningKey{kind, id, at})
		} else if len(parts) == 2 && parts[0] == "audit_elapsed_us" {
			var err error
			us, err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		} else if len(parts) == 2 && strings.HasPrefix(parts[0], "Handler_read") {
			var err error
			h[parts[0]], err = strconv.ParseInt(parts[1], 10, 64)
			if err != nil {
				t.Fatal(err)
			}
		} else if us < 0 {
			values = append(values, line)
		} else {
			t.Fatal("unexpected measurement output")
		}
	}
	if us < 0 || len(values) == 0 {
		t.Fatal("missing scalar result or timing")
	}
	plan := usageLogsPlanningKeysDecision(f, keys)
	if len(summaries) > 0 {
		plan = usageLogsSummaryDecision(f, summaries)
	}
	return strings.Join(values, "\n"), us, h, plan
}

func TestUsageLogsPerformanceMatrixMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,
username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,
` + "`group`" + ` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),padding VARCHAR(128),
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/5),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),
IF(MOD(n,10000)=0,202,201),IF(MOD(n,10000)=0,'rare','heavy'),'m','t',IF(MOD(n,2)=0,7,9),'g',CONCAT('r',n),CONCAT('u',n),REPEAT('p',128) FROM seq WHERE n<=200000;ANALYZE TABLE logs;`)
	var results []usagePerfResult
	defer func() {
		raw, err := json.MarshalIndent(struct {
			Warning string
			Results []usagePerfResult
		}{"Synthetic 200000-row MySQL matrix. Handler_read counters are logical handler operations, not physical I/O or Rows_examined. Complete fallback included. Not production or p95. Candidate SQL is test-only.", results}, "", "  ")
		if err != nil {
			t.Error(err)
			return
		}
		if err = os.WriteFile("../docs/sql-audit-20261126/usage-performance-matrix-compact-20261127.json", append(raw, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}()
	for _, scenario := range []struct {
		name         string
		f            usageLogFilter
		distribution string
	}{
		{"dense-short-heavy", usageLogFilter{Start: 38100, End: 40099, Username: "heavy", Channel: 7}, ""},
		{"dense-wide-heavy", usageLogFilter{Start: 100, End: 40099, Username: "heavy", Channel: 7}, ""},
		{"dense-wide-rare", usageLogFilter{Start: 100, End: 40099, Username: "rare", Channel: 7}, ""},
		{"dense-wide-missing", usageLogFilter{Start: 100, End: 40099, Username: "absent", Channel: 7}, ""},
		{"dense-short-broad", usageLogFilter{Start: 38100, End: 40099}, ""},
		{"dense-wide-broad", usageLogFilter{Start: 100, End: 40099}, ""},
		{"sparse-time-heavy", usageLogFilter{Start: 100, End: 102, Username: "heavy", Channel: 7}, "sparse"},
		{"sparse-time-broad", usageLogFilter{Start: 100, End: 102}, "sparse"},
		{"shuffled-time-heavy", usageLogFilter{Start: 100, End: 1099, Username: "heavy", Channel: 7}, "shuffle"},
		{"shuffled-time-broad", usageLogFilter{Start: 100, End: 1099}, "shuffle"},
	} {
		switch scenario.distribution {
		case "sparse":
			run("USE sql_usage_perf;UPDATE logs SET created_at=IF(MOD(id,100)=0,101,90);ANALYZE TABLE logs;")
		case "shuffle":
			run("USE sql_usage_perf;UPDATE logs SET created_at=100+MOD(id*37,200000);ANALYZE TABLE logs;")
		}
		for _, expression := range []string{"COUNT(*)", "COALESCE(SUM(quota),0)"} {
			scope := usageLogWhereList
			if expression != "COUNT(*)" {
				scope = usageLogWhereStat
			}
			f := scenario.f
			w, err := buildUsageLogWhere(f, scope)
			if err != nil {
				t.Fatal(err)
			}
			native := usageLogsSyntheticBind(t, usageLogsSelect+expression+" FROM logs"+w.sql(), w.args)
			q, a := usageLogsAggregateSpanSQL(f, w, expression)
			span := usageLogsSyntheticBind(t, q, a)
			inline, args := usageSpanInlineFallbackCandidate(t, f, w, expression)
			inline = usageLogsSyntheticBind(t, inline, args)
			queries := []struct{ name, q string }{{"native", native}, {"current-span", span}, {"force-time", usageLogsSyntheticBind(t, usageLogsSelect+expression+" FROM logs FORCE INDEX(idx_created_at_type)"+w.sql(), w.args)}, {"inline-fallback", inline}}
			planned, pa := usageAggregatePlanningCandidate(f, w, expression)
			queries = append(queries, struct{ name, q string }{"planned", usageLogsSyntheticBind(t, planned, pa)})
			keyQuery, ka := usageLogsPlanningSummarySQL(f)
			queries = append(queries, struct{ name, q string }{"planning-keys", usageLogsSyntheticBind(t, keyQuery, ka)})
			// Earlier rejected rewrites remain in prior evidence, not every run.
			filtered := queries[:0]
			for _, v := range queries {
				if v.name == "native" || v.name == "current-span" || v.name == "planning-keys" {
					filtered = append(filtered, v)
				}
			}
			queries = filtered
			if expression == "COUNT(*)" && f.Username != "" {
				q, a = usageCountSmallNameCandidate(f, w)
				queries = append(queries, struct{ name, q string }{"small-name-probe", usageLogsSyntheticBind(t, q, a)})
			}
			// SUM production keeps native for username; the forced-span SUM here
			// is a diagnostic counterfactual, not a claim that the app dispatches it.
			want := run("USE sql_usage_perf;" + native + ";")
			start := len(results)
			for _, v := range queries {
				results = append(results, usagePerfResult{Scenario: scenario.name, Operation: expression, Variant: v.name})
			}
			for round := 0; round < 4; round++ {
				for n := range queries {
					i := n
					if round%2 != 0 {
						i = len(queries) - 1 - n
					}
					v := queries[i]
					script := v.q
					// Every fallback path is timed in the same client session. We
					// know this synthetic distribution, not a production shortcut.
					if v.name == "current-span" && scenario.distribution != "" {
						script += ";" + native
					}
					if v.name == "small-name-probe" && f.Username == "heavy" {
						script += ";" + span
						if scenario.distribution != "" {
							script += ";" + native
						}
					}
					if v.name == "planned" && strings.HasPrefix(scenario.name, "dense-short-") {
						script += ";" + span
					}
					wantSpan := strings.HasPrefix(scenario.name, "dense-short-") || scenario.name == "dense-wide-heavy"
					if v.name == "planning-keys" {
						if wantSpan {
							script += ";" + span
						} else if scenario.distribution != "" {
							script += ";" + usageLogsSyntheticBind(t, usageLogsSelect+expression+" FROM logs FORCE INDEX (idx_created_at_type)"+w.sql(), w.args)
						} else {
							script += ";" + native
						}
					}
					value, us, handlers, plan := usagePerfSample(t, run, script, f)
					if v.name == "planning-keys" && (plan != usageLogPlanSpan) == wantSpan {
						t.Fatal("planning key decision differs from measured route", scenario.name)
					}
					lines := strings.Split(value, "\n")
					prefixOK := true
					for _, s := range lines[:len(lines)-1] {
						prefixOK = prefixOK && s == "NULL"
					}
					if lines[len(lines)-1] != want || !prefixOK {
						t.Fatalf("%s %s %s changed exact result: %q vs %q", scenario.name, expression, v.name, value, want)
					}
					r := &results[start+i]
					r.Values = append(r.Values, value)
					r.US = append(r.US, us)
					r.Handlers = append(r.Handlers, handlers)
				}
			}
			for i, v := range queries {
				r := &results[start+i]
				r.Plan = json.RawMessage(run("USE sql_usage_perf;EXPLAIN FORMAT=JSON " + v.q + ";"))
				if !json.Valid(r.Plan) {
					t.Fatal("invalid actual MySQL plan")
				}
				if v.name == "planning-keys" {
					r.AppRoute = usagePerfCheckActualDispatch(t, run, f, scope, want)
					warnings := run("USE sql_usage_perf;" + v.q + ";SHOW WARNINGS;")
					if strings.Contains(warnings, "Warning") || strings.Contains(warnings, "ignored") || strings.Contains(warnings, "not supported") {
						t.Fatal("planning query hint ignored", warnings)
					}
				}
				if expression == "COUNT(*)" && (v.name == "native" || v.name == "small-name-probe" || v.name == "planned") {
					r.Analyze = run("USE sql_usage_perf;EXPLAIN ANALYZE " + v.q + ";")
				}
				t.Logf("%s %s %s us=%v handler-first=%v", scenario.name, expression, v.name, r.US, r.Handlers[0])
			}
		}
	}
}

// The actual Go dispatcher executes every statement through the owned MySQL
// socket container. CLI overhead is deliberately not a latency measurement.
type usagePerfSocketMySQLDriver struct {
	t     *testing.T
	run   func(string) string
	route *[]string
}
type usagePerfSocketConn struct{ d usagePerfSocketMySQLDriver }

func (d usagePerfSocketMySQLDriver) Open(string) (driver.Conn, error) {
	return usagePerfSocketConn{d}, nil
}
func (c usagePerfSocketConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("synthetic prepare unsupported")
}
func (c usagePerfSocketConn) Close() error { return nil }
func (c usagePerfSocketConn) Begin() (driver.Tx, error) {
	return nil, errors.New("synthetic transaction unsupported")
}
func (c usagePerfSocketConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	args := make([]any, len(a))
	for n := range a {
		args[n] = a[n].Value
	}
	route := "native"
	if strings.Contains(q, "AS usage_aggregate_probe") {
		route = "probe"
	} else if strings.Contains(q, "'audit_plan_summary'") {
		route = "keys"
	} else if strings.Contains(q, "bounds.row_count") {
		route = "span"
	} else if strings.Contains(q, "FORCE INDEX (idx_created_at_type)") {
		route = "time"
	}
	*c.d.route = append(*c.d.route, route)
	out := c.d.run("USE sql_usage_perf;" + usageLogsSyntheticBind(c.d.t, q, args) + ";")
	cols := []string{"value"}
	if route == "keys" {
		cols = []string{"marker", "kind", "n", "low", "high", "at"}
	}
	r := &explainCatalogRows{columns: cols}
	if out == "" {
		return r, nil
	}
	for _, line := range strings.Split(out, "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != len(cols) {
			return nil, fmt.Errorf("unexpected synthetic row width %d", len(parts))
		}
		v := make([]driver.Value, len(parts))
		for n, s := range parts {
			if s != "NULL" {
				v[n] = s
			}
		}
		r.values = append(r.values, v)
	}
	return r, nil
}
func usagePerfCheckActualDispatch(t *testing.T, run func(string) string, f usageLogFilter, scope usageLogWhereScope, want string) []string {
	t.Helper()
	var route []string
	name := fmt.Sprintf("perf-socket-mysql-%d", usageQuotaDriverID.Add(1))
	sql.Register(name, usagePerfSocketMySQLDriver{t, run, &route})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	m := &Monitor{prodDB: db}
	w, err := buildUsageLogWhere(f, scope)
	if err != nil {
		t.Fatal(err)
	}
	var got int64
	if scope == usageLogWhereStat {
		got, err = m.queryUsageLogsQuota(context.Background(), db, f, w)
	} else {
		got, err = m.queryUsageLogsCount(context.Background(), db, f, w)
	}
	if err != nil || strconv.FormatInt(got, 10) != want {
		t.Fatalf("real dispatcher result=%d want=%s error=%v", got, want, err)
	}
	if db.Stats().InUse != 0 {
		t.Fatal("MySQL dispatcher connection leaked")
	}
	eligible := scope == usageLogWhereList && usageLogsUseCountSpan(f) || scope == usageLogWhereStat && usageLogsUseQuotaSpan(f)
	eligible = eligible && !strings.Contains(f.Username, "%") && !strings.Contains(f.ModelName, "%")
	if eligible && (len(route) < 1 || route[0] != "probe") {
		t.Fatalf("actual MySQL planning path not exercised: %v", route)
	}
	return route
}
