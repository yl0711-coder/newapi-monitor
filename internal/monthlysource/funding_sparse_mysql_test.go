package monthlysource

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Compare the actual builder, never copied business predicates. No candidate
// here is adopted automatically: member/time distributions choose winners.
func TestSQLAuditFundingSparseMySQL(t *testing.T) {
	container := os.Getenv("MONITOR_SQL_OPT_CONTAINER")
	if container == "" {
		t.Skip("requires task-owned isolated synthetic MySQL")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 120*time.Second)
	defer cancel()
	meta, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "ai.task"}} {{.HostConfig.NetworkMode}}`, container).CombinedOutput()
	if err != nil || strings.TrimSpace(string(meta)) != "monitor-sql-opt-20261126 none" {
		t.Fatal("refusing non-owned/network-enabled container")
	}
	run := func(q string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container, "mysql", "--protocol=socket", "-uroot", "--batch", "--raw", "--skip-column-names")
		cmd.Stdin = strings.NewReader(q)
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("synthetic SQL failed: %v %s", err, out)
		}
		if len(out) > 2<<20 {
			t.Fatal("output budget")
		}
		return strings.TrimSpace(string(out))
	}
	bind := func(q string, a []any) string {
		t.Helper()
		for _, v := range a {
			n, ok := v.(int64)
			if !ok {
				t.Fatal("non-synthetic numeric argument")
			}
			q = strings.Replace(q, "?", strconv.FormatInt(n, 10), 1)
		}
		if strings.Contains(q, "?") {
			t.Fatal("unbound query")
		}
		return q
	}
	run(`DROP DATABASE IF EXISTS sql_usage_perf;DROP DATABASE IF EXISTS sql_funding_sparse;CREATE DATABASE sql_funding_sparse;USE sql_funding_sparse;
CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,created_at BIGINT,type INT,content TEXT,other TEXT,INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type));
INSERT INTO logs WITH digits(n) AS (SELECT 0 UNION ALL SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3 UNION ALL SELECT 4 UNION ALL SELECT 5 UNION ALL SELECT 6 UNION ALL SELECT 7 UNION ALL SELECT 8 UNION ALL SELECT 9),seq(n) AS (SELECT 1+a.n+10*b.n+100*c.n+1000*d.n+10000*e.n+100000*f.n FROM digits a CROSS JOIN digits b CROSS JOIN digits c CROSS JOIN digits d CROSS JOIN digits e CROSS JOIN digits f)
SELECT n,201+MOD(n,200),1790784000+FLOOR((n-1)/1000)*86400+MOD(n,1000),CASE WHEN MOD(n,10000)=0 THEN 3 WHEN MOD(n,997)=0 THEN 1 WHEN MOD(n,991)=0 THEN 4 ELSE 2 END,'synthetic','{}' FROM seq WHERE n<=200000;ANALYZE TABLE logs;`)
	type result struct {
		Distribution string
		Members      int
		Resume       bool
		Variant      string
		Rows         int
		SHA256       string
		US           []int64
		Plan         json.RawMessage
		Analyze      string
	}
	var results []result
	for _, distribution := range []string{"200_historical_days_sparse", "one_day_sparse"} {
		from := int64(1790784000 + 199*86400)
		if distribution == "one_day_sparse" {
			run("USE sql_funding_sparse;UPDATE logs SET created_at=1790784000+MOD(id,80000);ANALYZE TABLE logs;")
			from = 1790784000
		}
		for _, members := range []int{1, 200} {
			ids := make([]int64, members)
			for n := range ids {
				ids[n] = int64(201 + n)
			}
			for _, resume := range []bool{false, true} {
				after := FundingCursor{}
				if resume {
					after = FundingCursor{At: from + 400, ID: 199400}
				}
				q, a := fundingQuery(FundingScan{Version: 2, UserIDs: ids, From: from, To: from + 86400}, 300000, after)
				candidate := strings.Replace(q, "FORCE INDEX(idx_user_created_type)", "FORCE INDEX(idx_created_at_type)", 1)
				if candidate == q {
					t.Fatal("builder changed")
				}
				want := ""
				start := len(results)
				for _, variant := range []string{"existing", "time_driver_candidate"} {
					results = append(results, result{Distribution: distribution, Members: members, Resume: resume, Variant: variant})
				}
				for round := 0; round < 4; round++ {
					for n := 0; n < 2; n++ {
						i := n
						if round%2 != 0 {
							i = 1 - n
						}
						sql := q
						if i == 1 {
							sql = candidate
						}
						sql = bind(sql, a)
						out := run("USE sql_funding_sparse;SET @audit_started=NOW(6);" + sql + ";SELECT 'elapsed',TIMESTAMPDIFF(MICROSECOND,@audit_started,NOW(6));")
						lines := strings.Split(out, "\n")
						tail := strings.Split(lines[len(lines)-1], "\t")
						if len(tail) != 2 || tail[0] != "elapsed" {
							t.Fatal("timing marker")
						}
						us, err := strconv.ParseInt(tail[1], 10, 64)
						if err != nil {
							t.Fatal(err)
						}
						rows := strings.Join(lines[:len(lines)-1], "\n")
						if round == 0 && n == 0 {
							want = rows
						}
						if rows != want {
							t.Fatal("candidate changed exact ordered page")
						}
						r := &results[start+i]
						r.Rows = len(lines) - 1
						r.SHA256 = fmt.Sprintf("%x", sha256.Sum256([]byte(rows)))
						r.US = append(r.US, us)
					}
				}
				for i := 0; i < 2; i++ {
					sql := q
					if i == 1 {
						sql = candidate
					}
					sql = bind(sql, a)
					r := &results[start+i]
					r.Plan = json.RawMessage(run("USE sql_funding_sparse;EXPLAIN FORMAT=JSON " + sql + ";"))
					if !json.Valid(r.Plan) {
						t.Fatal("invalid plan")
					}
					r.Analyze = run("USE sql_funding_sparse;EXPLAIN ANALYZE " + sql + ";")
				}
			}
		}
	}
	source, err := os.ReadFile("funding_capture.go")
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.MarshalIndent(struct {
		Warning, SourceSHA256 string
		Results               []result
	}{"Actual v2 builder, 200000 synthetic rows. Same ordered page required; no source index or production SQL change. Not source transaction wall time/p95/durable IO.", fmt.Sprintf("%x", sha256.Sum256(source)), results}, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile("../../docs/sql-audit-20261126/funding-sparse-mysql-20261127.json", append(raw, '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("%d sparse funding variants executed with EXPLAIN and EXPLAIN ANALYZE", len(results))
}
