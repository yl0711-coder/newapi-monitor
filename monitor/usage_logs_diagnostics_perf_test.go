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

type diagnosticPerfSample struct {
	ElapsedUS    int64
	HandlerReads map[string]int64
}

// Server-session timing and handler-counter deltas, not Go/HTTP latency or
// exact rows_examined. SHOW SESSION STATUS is used with performance_schema off.
func measureDiagnosticSQL(t *testing.T, run func(string) string, database, query string) (string, diagnosticPerfSample) {
	t.Helper()
	status := "SHOW SESSION STATUS WHERE Variable_name IN ('Handler_read_key','Handler_read_next','Handler_read_prev','Handler_read_rnd','Handler_read_rnd_next');"
	raw := run("USE " + database + ";SELECT 'perf_before';" + status + "SELECT 'perf_result';SET @perf_start=NOW(6);" + query + ";SET @perf_us=TIMESTAMPDIFF(MICROSECOND,@perf_start,NOW(6));SELECT 'perf_after',@perf_us;" + status)
	s := diagnosticPerfSample{HandlerReads: map[string]int64{}}
	before := map[string]int64{}
	var output []string
	part := ""
	for _, line := range strings.Split(raw, "\n") {
		if line == "perf_before" || line == "perf_result" {
			part = line
			continue
		}
		if strings.HasPrefix(line, "perf_after\t") {
			part = "perf_after"
			var err error
			s.ElapsedUS, err = strconv.ParseInt(strings.TrimPrefix(line, "perf_after\t"), 10, 64)
			if err != nil {
				t.Fatal(err)
			}
			continue
		}
		if part == "perf_result" {
			output = append(output, line)
			continue
		}
		fields := strings.Split(line, "\t")
		if len(fields) != 2 || !strings.HasPrefix(fields[0], "Handler_read_") {
			t.Fatal("unexpected synthetic timing/status output", line)
		}
		n, err := strconv.ParseInt(fields[1], 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		if part == "perf_before" {
			before[fields[0]] = n
		} else {
			s.HandlerReads[fields[0]] = n - before[fields[0]]
		}
	}
	if part != "perf_after" || len(s.HandlerReads) != 5 {
		t.Fatal("incomplete synthetic timing/status markers")
	}
	return strings.Join(output, "\n"), s
}

func TestUsageLogsDiagnosticPerformanceMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	const database = "usage_diagnostics_perf"
	const start = int64(100000)
	const end = start + 86400 - 1
	const maxRows = 200000
	run("DROP DATABASE IF EXISTS " + database + ";CREATE DATABASE " + database + ";USE " + database + ";" +
		"CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,content TEXT,other TEXT,username VARCHAR(80),channel_id INT,user_id BIGINT,token_name VARCHAR(80),model_name VARCHAR(80),quota BIGINT,prompt_tokens BIGINT,completion_tokens BIGINT,use_time INT,is_stream INT,token_id BIGINT,`group` VARCHAR(80),ip VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80)," +
		"INDEX idx_created_at_type(created_at,type),INDEX idx_created_at_id_rc26(created_at,id),INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_user_created_type(user_id,created_at,type),INDEX idx_request_id(request_id),INDEX idx_upstream_request_id(upstream_request_id));")
	type perfCase struct {
		name string
		f    usageLogFilter
	}
	type result struct {
		Phase, Case, Kind string
		Rows, Matches     int
		AutoCount         bool
		Samples           []diagnosticPerfSample
		Plan              json.RawMessage
	}
	var results []result
	sources := map[string]string{}
	for _, name := range []string{"usage_logs_diagnostics.go", "usage_logs_diagnostic_batch.go", "newapi_usage_logs.go", "usage_logs_quota.go", "usage_logs_diagnostics_perf_test.go"} {
		raw, err := os.ReadFile(name)
		if err != nil {
			t.Fatal(err)
		}
		sources[name] = fmt.Sprintf("%x", sha256.Sum256(raw))
	}
	persist := func() {
		name := "usage-log-diagnostics-performance-20261127.json"
		if os.Getenv("MONITOR_SQL_DIAGNOSTIC_BATCH_CANDIDATE") == "1" {
			name = "usage-log-diagnostics-batch-candidate-20261127.json"
		}
		sqlScaleWrite(t, name, map[string]any{
			"synthetic_only": true, "production_latency_proven": false, "source_sha256": sources,
			"notes":   "Current page/count SQL; 2 samples each, 1 CPU/1280MiB container, 64MiB InnoDB buffer. Existing source time/id index reproduced, not a proposed production index. Server-session wall time incl result delivery, excludes docker/HTTP. Handler counters are not exact rows_examined. Warm/mixed cache, no production data or schema changes. COUNT measured separately even if UI infers exact small total from page. No universal latency threshold inferred.",
			"results": results,
		})
	}
	for _, phase := range []struct {
		name  string
		rows  int
		first int
	}{{"50k", 50000, 1}, {"200k", maxRows, 50001}, {"200k-wide-old-records", maxRows, 0}} {
		if phase.first > 0 {
			for first := phase.first; first <= phase.rows; first += 10000 {
				last := first + 9999
				if last > phase.rows {
					last = phase.rows
				}
				run("USE " + database + "; INSERT INTO logs(id,created_at,type,content,other,username,channel_id,user_id,token_name,model_name,quota,prompt_tokens,completion_tokens,use_time,is_stream,token_id,`group`,ip,request_id,upstream_request_id) " + syntheticDigitsCTE() + fmt.Sprintf(
					"SELECT n,%d+FLOOR((n-1)*86400/%d),IF(MOD(n,100)=0,5,2),CASE WHEN n=1 THEN 'rare-diagnostic-marker' WHEN MOD(n,100)=0 THEN 'status_code=429 rate_limit_exceeded' ELSE REPEAT('normal synthetic text ',4) END,IF(MOD(n,10007)=0,'{bad',JSON_OBJECT('frt',600,'model_ratio',2,'completion_ratio',5,'padding',REPEAT('s',512),'admin_info',IF(MOD(n,200)=1,JSON_OBJECT('reject_reason','claude_stop_reason=refusal'),JSON_OBJECT()))),IF(MOD(n,1000)=0,'rare-user','heavy-user'),7+MOD(n,10),31,'t','claude-demo',50000,1200,240,3,1,5,'g','127.0.0.1',CONCAT('diag-',n),CONCAT('up-',n) FROM seq WHERE n BETWEEN %d AND %d;", start, maxRows, first, last))
			}
			run("USE " + database + ";ANALYZE TABLE logs;")
		} else {
			run("USE " + database + ";UPDATE logs SET other=JSON_SET(other,'$.padding',REPEAT('w',4096)) WHERE id<=4000 AND JSON_VALID(other);ANALYZE TABLE logs;")
		}
		cases := []perfCase{
			{"baseline", usageLogFilter{}},
			{"missing-global", usageLogFilter{Message: "does-not-exist"}},
			{"rare-oldest-global", usageLogFilter{Message: "rare-diagnostic-marker"}},
			{"errors-text", usageLogFilter{Type: 5, Message: "rate_limit_exceeded"}},
			{"rejected-global", usageLogFilter{DiagnosticScope: "rejected"}},
			{"issues-global", usageLogFilter{DiagnosticScope: "issues"}},
			{"refusal-text-global", usageLogFilter{Message: "refusal"}},
			{"missing-hour", usageLogFilter{Message: "does-not-exist", End: start + 3599}},
			{"missing-rare-customer", usageLogFilter{Message: "does-not-exist", Username: "rare-user"}},
			{"missing-heavy-customer", usageLogFilter{Message: "does-not-exist", Username: "heavy-user"}},
			{"refusal-channel", usageLogFilter{Message: "refusal", Channel: 8}},
			{"exact-request", usageLogFilter{Message: "rare-diagnostic-marker", RequestID: "diag-1"}},
		}
		if phase.first == 0 {
			cases = []perfCase{cases[0], cases[1], cases[2], cases[4], cases[6]}
		}
		for _, c := range cases {
			f := c.f
			f.Start = start
			if f.End == 0 {
				f.End = end
			}
			// Independent synthetic oracle; do not derive expected answers from
			// the SQL under test or infer historical usernames via current users.
			var expected []int
			keysInRange := 0
			for n := phase.rows; n >= 1; n-- {
				at := start + int64((n-1)*86400/maxRows)
				if at <= f.End {
					keysInRange++
				}
				isError, reason := n%100 == 0, n%200 == 1 && n%10007 != 0
				if at > f.End || f.Type == 5 && !isError || f.Username == "rare-user" && n%1000 != 0 || f.Username == "heavy-user" && n%1000 == 0 || f.Channel != 0 && 7+n%10 != f.Channel || f.RequestID != "" && n != 1 {
					continue
				}
				if f.DiagnosticScope == "rejected" && !reason || f.DiagnosticScope == "issues" && !reason && !isError {
					continue
				}
				if f.Message == "does-not-exist" || f.Message == "rare-diagnostic-marker" && n != 1 || f.Message == "rate_limit_exceeded" && !isError || f.Message == "refusal" && !reason {
					continue
				}
				expected = append(expected, n)
			}
			w, err := buildUsageLogWhere(f, usageLogWhereList, true)
			if err != nil {
				t.Fatal(err)
			}
			page, args := usageLogsPageSQL(w, "logs", 1, 20, nil, true)
			if f.RequestID != "" {
				page, args = usageLogsIDPageSQL(w, nil, 1, 20)
			}
			queries := []struct{ name, sql string }{{"list", usageLogsSyntheticBind(t, page, args)}}
			if f.RequestID == "" {
				queries = append(queries, struct{ name, sql string }{"count", usageLogsSyntheticBind(t, usageLogsSelect+"COUNT(*) FROM logs"+w.sql(), w.args)})
			}
			if os.Getenv("MONITOR_SQL_DIAGNOSTIC_BATCH_CANDIDATE") == "1" && usageLogsHasDiagnosticFilter(f) && f.RequestID == "" {
				for _, cap := range []int{1000, 5000, 10000} {
					q, args := usageLogsDiagnosticBatchSQL(f, w, nil, 20, cap, true)
					queries = append(queries, struct{ name, sql string }{fmt.Sprintf("batch-%d", cap), usageLogsSyntheticBind(t, q, args)})
				}
			}
			for _, query := range queries {
				r := result{Phase: phase.name, Case: c.name, Kind: query.name, Rows: phase.rows, Matches: len(expected), AutoCount: len(expected) > 20 && f.RequestID == ""}
				r.Plan = json.RawMessage(run("USE " + database + ";EXPLAIN FORMAT=JSON " + query.sql + ";"))
				for sample := 0; sample < 2; sample++ {
					output, metric := measureDiagnosticSQL(t, run, database, query.sql)
					if strings.HasPrefix(query.name, "batch-") {
						cap, _ := strconv.Atoi(strings.TrimPrefix(query.name, "batch-"))
						var got []int
						for _, line := range strings.Split(output, "\n") {
							fields := strings.Split(line, "\t")
							if len(fields) != 25 {
								t.Fatal("batch column count", len(fields))
							}
							n, _ := strconv.Atoi(fields[0])
							if n != min(cap+1, keysInRange) {
								t.Fatal("candidate keys not bounded", n, cap)
							}
							if fields[5] != "NULL" {
								id, e := strconv.Atoi(fields[5])
								if e != nil {
									t.Fatal(e)
								}
								got = append(got, id)
							}
						}
						want := []int{}
						for _, id := range expected {
							if id > keysInRange-cap && len(want) < 20 {
								want = append(want, id)
							}
						}
						if fmt.Sprint(got) != fmt.Sprint(want) {
							t.Fatal("bounded candidate differs", c.name, cap, got, want)
						}
					} else if query.name == "count" {
						if output != fmt.Sprint(len(expected)) {
							t.Fatal("count differs", phase.name, c.name, output, len(expected))
						}
					} else {
						var ids []string
						if output != "" {
							for _, line := range strings.Split(output, "\n") {
								ids = append(ids, strings.SplitN(line, "\t", 2)[0])
							}
						}
						n := len(expected)
						if n > 20 {
							n = 20
						}
						if len(ids) != n {
							t.Fatal("wrong page length", c.name, len(ids), n)
						}
						for i, id := range ids {
							if id != fmt.Sprint(expected[i]) {
								t.Fatal("wrong page ID", c.name, id, expected[i])
							}
						}
					}
					r.Samples = append(r.Samples, metric)
				}
				results = append(results, r)
				t.Logf("%s %s %s: matches=%d elapsed_us=%d/%d", phase.name, c.name, query.name, len(expected), r.Samples[0].ElapsedUS, r.Samples[1].ElapsedUS)
			}
		}
		persist()
	}
}
