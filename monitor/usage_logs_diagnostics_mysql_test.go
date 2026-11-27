package monitor

import (
	"encoding/hex"
	"fmt"
	"strings"
	"testing"
)

// Opt-in, synthetic only. The shared runner rejects network-enabled containers
// and containers without this task's ownership label. This does not benchmark
// the production replica or prove that an unindexed text search is cheap.
func TestUsageLogsDiagnosticMySQL(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	const database = "usage_diagnostics_synthetic"
	run("DROP DATABASE IF EXISTS " + database + "; CREATE DATABASE " + database + "; USE " + database + ";" +
		"CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,content TEXT,other TEXT,username VARCHAR(80),channel_id INT," +
		"user_id BIGINT,token_name VARCHAR(80),model_name VARCHAR(80),quota BIGINT,prompt_tokens BIGINT,completion_tokens BIGINT,use_time INT,is_stream INT,token_id BIGINT,`group` VARCHAR(80),ip VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),INDEX idx_created_at_type(created_at,type));")
	// Hex literals keep malformed JSON and backslashes byte-identical to the
	// SQLite/HTTP fixture regardless of the MySQL session's escape mode.
	for i, row := range diagnosticFixture() {
		run(fmt.Sprintf("USE %s; INSERT INTO logs(id,created_at,type,content,other,username,channel_id) VALUES(%d,%d,%d,CONVERT(X'%s' USING utf8mb4),CONVERT(X'%s' USING utf8mb4),'fixture',7);", database, i+1, 100+i/2, row.typ, hex.EncodeToString([]byte(row.content)), hex.EncodeToString([]byte(row.other))))
	}
	type result struct {
		Name, IDs, Count, Plan string
		ElapsedUS              int64
	}
	var results []result
	for _, tc := range []struct{ name, scope, message, ids string }{
		{"issues", "issues", "", "13,12,11,5,3,2"},
		{"rejected", "rejected", "", "13,12,11,5,2"},
		{"error-text", "", "invalid value", "3"},
		{"refusal-text", "issues", "refusal", "2"},
		{"literal-wildcards", "", "50%_!", "12"},
		{"parameterized-quote", "", "' OR 1=1 --", ""},
		{"no-match", "", "missing-text", ""},
	} {
		f := usageLogFilter{Start: 99, End: 120, DiagnosticScope: tc.scope, Message: tc.message}
		w, err := buildUsageLogWhere(f, usageLogWhereList, true)
		if err != nil {
			t.Fatal(err)
		}
		where := usageLogsSyntheticBind(t, w.sql(), w.args)
		q := "SELECT COALESCE(GROUP_CONCAT(id ORDER BY created_at DESC,id DESC),'') FROM logs" + where
		got := run("USE " + database + ";" + q + ";")
		if got != tc.ids {
			t.Fatalf("%s: MySQL IDs %q, want %q", tc.name, got, tc.ids)
		}
		count := run("USE " + database + "; SELECT COUNT(*) FROM logs" + where + ";")
		wantCount := 0
		if tc.ids != "" {
			wantCount = len(strings.Split(tc.ids, ","))
		}
		if count != fmt.Sprint(wantCount) {
			t.Fatal(tc.name, count)
		}
		for _, deferred := range []bool{false, true} {
			pageSQL, args := usageLogsPageSQL(w, "logs", 1, 100, nil, deferred)
			page := run("USE " + database + ";" + usageLogsSyntheticBind(t, pageSQL, args) + ";")
			var ids []string
			if page != "" {
				for _, line := range strings.Split(page, "\n") {
					ids = append(ids, strings.SplitN(line, "\t", 2)[0])
				}
			}
			if strings.Join(ids, ",") != tc.ids {
				t.Fatal("derived page differs from count/filter", tc.name, deferred, ids)
			}
		}
		results = append(results, result{Name: tc.name, IDs: got, Count: count})
	}
	// Ten thousand irrelevant rows lie outside the requested time window.
	// Verify the real builder retains an indexed time range, not a table-wide
	// JSON pass. Selective customer/channel indexes can further narrow it on
	// production, but this experiment deliberately doesn't add such indexes.
	run("USE " + database + "; INSERT INTO logs(id,created_at,type,content,other,username,channel_id) " + syntheticDigitsCTE() + "SELECT n+1000,10000+n,2,'ok','{}','other',9 FROM seq WHERE n<=10000; ANALYZE TABLE logs;")
	for _, scope := range []string{"issues", "rejected"} {
		w, err := buildUsageLogWhere(usageLogFilter{Start: 99, End: 120, DiagnosticScope: scope}, usageLogWhereList, true)
		if err != nil {
			t.Fatal(err)
		}
		q := usageLogsSyntheticBind(t, "SELECT COUNT(*) FROM logs"+w.sql(), w.args)
		plan := run("USE " + database + ";EXPLAIN FORMAT=JSON " + q + ";")
		if !strings.Contains(plan, `"key": "idx_created_at_type"`) || !strings.Contains(plan, `"access_type": "range"`) {
			t.Fatal("bounded diagnostics lost indexed time access", plan)
		}
		count, us := timedSyntheticSQL(t, run, database, q)
		results = append(results, result{Name: scope + "-time-plan", Count: count, Plan: plan, ElapsedUS: us})
	}
	sqlScaleWrite(t, "usage-log-diagnostics-mysql-20261127.json", map[string]any{
		"synthetic_only": true, "dialect": "MySQL", "fixture_rows": 14, "out_of_window_rows": 10000,
		"production_latency_proven": false, "results": results,
	})
}
