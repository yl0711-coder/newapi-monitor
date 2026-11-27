package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

func isolatedSQLOptimizationRunner(t *testing.T) func(string) string {
	t.Helper()
	name := os.Getenv("MONITOR_SQL_OPT_CONTAINER")
	if name == "" {
		t.Skip("requires task-owned network-isolated synthetic MySQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	t.Cleanup(cancel)
	meta, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "ai.task"}} {{.HostConfig.NetworkMode}}`, name).CombinedOutput()
	if err != nil || strings.TrimSpace(string(meta)) != "monitor-sql-opt-20261126 none" {
		t.Fatal("refusing non-owned or network-enabled source")
	}
	return func(q string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", name, "mysql", "--protocol=socket", "-uroot", "--batch", "--raw", "--skip-column-names")
		cmd.Stdin = strings.NewReader(q)
		var out, diagnostic bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diagnostic
		if err := cmd.Run(); err != nil {
			t.Fatalf("synthetic SQL failed: %v %s", err, diagnostic.String())
		}
		if out.Len() > 8<<20 {
			t.Fatal("synthetic output exceeded 8MiB")
		}
		return strings.TrimSpace(out.String())
	}
}

// One client session per sample, with the same result delivered before the
// elapsed marker. This is server-session wall time (including local delivery),
// not production latency or pure CPU time. Reverse sample order to expose noise.
func timedSyntheticSQL(t *testing.T, run func(string) string, database, query string) (string, int64) {
	t.Helper()
	out := run("USE " + database + "; SET @audit_started=NOW(6);" + query + ";SELECT 'audit_elapsed_us',TIMESTAMPDIFF(MICROSECOND,@audit_started,NOW(6));")
	lines := strings.Split(out, "\n")
	tail := strings.Split(lines[len(lines)-1], "\t")
	if len(tail) != 2 || tail[0] != "audit_elapsed_us" {
		t.Fatal("missing synthetic timing marker")
	}
	us, err := strconv.ParseInt(tail[1], 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return strings.Join(lines[:len(lines)-1], "\n"), us
}

func syntheticDigitsCTE() string {
	return "WITH digits(n) AS (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9), seq(n) AS (SELECT 1+a.n+10*b.n+100*c.n+1000*d.n+10000*e.n+100000*f.n FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e CROSS JOIN digits f) "
}

func TestSQLFundingSortMySQLSemanticsAndPlan(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	const db = "sql_funding_sort_synthetic"
	run("DROP DATABASE IF EXISTS " + db + ";CREATE DATABASE " + db + ";USE " + db + ";CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,content TEXT,other TEXT,INDEX idx_user_created_type(user_id,created_at,type),INDEX idx_created_at_type(created_at,type));INSERT INTO logs " + syntheticDigitsCTE() + "SELECT n,201+MOD(n,4),100+MOD(n*37,50000),CASE WHEN MOD(n,11)=0 THEN 3 WHEN MOD(n,17)=0 THEN 1 ELSE 2 END,REPEAT('synthetic',16),'{}' FROM seq WHERE n<=50000;")
	type result struct {
		Name                        string
		Rows                        int
		SHA256                      string
		OriginalUS, CandidateUS     []int64
		OriginalPlan, CandidatePlan json.RawMessage
	}
	var results []result
	for _, p := range []struct {
		name, old, next string
		args            []any
	}{
		{"member", oldSimpleMemberFundingSQL, simpleMemberFundingSQL, []any{int64(201), int64(100)}},
		{"admin", oldSimpleAdminFundingSQL, simpleAdminFundingSQL, []any{int64(100), int64(50100)}},
	} {
		old := usageLogsSyntheticBind(t, p.old, p.args)
		next := usageLogsSyntheticBind(t, p.next, p.args)
		r := result{Name: p.name}
		var expected string
		for _, candidate := range []bool{false, true, true, false} {
			q := old
			if candidate {
				q = next
			}
			rows, us := timedSyntheticSQL(t, run, db, q)
			list := strings.Split(rows, "\n")
			sort.Strings(list)
			normalized := strings.Join(list, "\n")
			if expected == "" {
				expected = normalized
				r.Rows = len(list)
			} else if normalized != expected {
				t.Fatal("removing filesort changed MySQL row set")
			}
			if candidate {
				r.CandidateUS = append(r.CandidateUS, us)
			} else {
				r.OriginalUS = append(r.OriginalUS, us)
			}
		}
		r.SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(expected)))
		for _, candidate := range []bool{false, true} {
			q := old
			if candidate {
				q = next
			}
			plan := run("USE " + db + ";EXPLAIN FORMAT=JSON " + q + ";")
			if !json.Valid([]byte(plan)) {
				t.Fatal("invalid MySQL plan")
			}
			if candidate {
				if strings.Contains(plan, `"using_filesort": true`) {
					t.Fatal("candidate still sorts source rows")
				}
				r.CandidatePlan = json.RawMessage(plan)
			} else {
				if !strings.Contains(plan, `"using_filesort": true`) {
					t.Fatal("fixture did not reproduce original filesort")
				}
				r.OriginalPlan = json.RawMessage(plan)
			}
		}
		t.Logf("%s rows=%d original_us=%v candidate_us=%v; synthetic, not production latency", r.Name, r.Rows, r.OriginalUS, r.CandidateUS)
		results = append(results, r)
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/funding-sort-mysql.json", append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}

// Experiment only. LIMIT makes the boundary probe cap+1 bounded, but also
// introduces a materialized ID table. Do not ship without representative
// dense, overflow and source-plan evidence; the production builder is unchanged.
func boundedUsageSpanExperiment(t *testing.T, f usageLogFilter, w *usageLogWhere, cap int64) (string, []any) {
	t.Helper()
	q, a := usageLogsAggregateSpanSQL(f, w, "COALESCE(SUM(quota),0)")
	a[0] = cap
	old := "FROM logs FORCE INDEX (idx_created_at_type)\n       WHERE created_at>=? AND created_at<=?) AS bounds"
	next := fmt.Sprintf("FROM (SELECT id FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at>=? AND created_at<=? LIMIT %d) AS probe_rows) AS bounds", cap+1)
	if strings.Count(q, old) != 1 {
		t.Fatal("original boundary template changed")
	}
	return strings.Replace(q, old, next, 1), a
}

func TestSQLBoundedSpanMySQLExperiment(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	const db = "sql_span_probe_synthetic"
	run("DROP DATABASE IF EXISTS " + db + ";CREATE DATABASE " + db + ";USE " + db + ";CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,INDEX idx_created_at_type(created_at,type));INSERT INTO logs " + syntheticDigitsCTE() + "SELECT n,100+n,2,n FROM seq WHERE n<=200000;")
	type result struct {
		Name                    string
		Rows, ProbeCap          int64
		OriginalUS, CandidateUS []int64
		Value                   string
	}
	var results []result
	for _, c := range []struct {
		name string
		rows int64
	}{{"dense", 20000}, {"overflow", 200000}} {
		f := usageLogFilter{Start: 101, End: 100 + c.rows}
		w, err := buildUsageLogWhere(f, usageLogWhereStat)
		if err != nil {
			t.Fatal(err)
		}
		old, a := usageLogsQuotaSpanSQL(f, w)
		a[0] = int64(20000)
		old = usageLogsSyntheticBind(t, old, a)
		next, args := boundedUsageSpanExperiment(t, f, w, 20000)
		next = usageLogsSyntheticBind(t, next, args)
		native := usageLogsSyntheticBind(t, "SELECT COALESCE(SUM(quota),0) FROM logs"+w.sql(), w.args)
		// Compare the complete path, including the exact fallback when over cap.
		if c.rows > 20000 {
			old += ";" + native
			next += ";" + native
		}
		r := result{Name: c.name, Rows: c.rows, ProbeCap: 20000}
		for _, candidate := range []bool{false, true, true, false} {
			q := old
			if candidate {
				q = next
			}
			value, us := timedSyntheticSQL(t, run, db, q)
			want := strconv.FormatInt(c.rows*(c.rows+1)/2, 10)
			if c.rows > 20000 {
				want = "NULL\n" + want
			}
			if value != want {
				t.Fatalf("probe changed exact result/fallback: got %q want %q", value, want)
			}
			r.Value = value
			if candidate {
				r.CandidateUS = append(r.CandidateUS, us)
			} else {
				r.OriginalUS = append(r.OriginalUS, us)
			}
		}
		t.Logf("experiment %s original_us=%v candidate_us=%v; production probe unchanged", r.Name, r.OriginalUS, r.CandidateUS)
		results = append(results, r)
	}
	b, err := json.MarshalIndent(results, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../docs/sql-audit-20261126/bounded-probe-experiment.json", append(b, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
}
