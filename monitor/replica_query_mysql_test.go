package monitor

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/exec"
	"sort"
	"strings"
	"testing"
	"time"
)

// An explicit disposable container, no source credentials or network. Compare
// aggregate results on MySQL itself; SQLite equivalence cannot prove JSON type
// and collation semantics. This is not a production performance benchmark.
func TestReplicaMySQLSamplingSemantics(t *testing.T) {
	container := os.Getenv("MONITOR_REPLICA_SQL_MYSQL_CONTAINER")
	if container == "" {
		t.Skip("requires explicitly selected isolated synthetic MySQL container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	benchmark := os.Getenv("MONITOR_REPLICA_SQL_BENCH") == "1"
	metadata, err := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "ai.task"}} {{.HostConfig.NetworkMode}}`, container).CombinedOutput()
	if err != nil || strings.TrimSpace(string(metadata)) != "monitor-replica-sql-20261126 none" {
		t.Fatal("refusing non-owned or network-enabled MySQL test container")
	}
	run := func(script string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container, "mysql", "--protocol=socket", "-uroot", "--batch", "--skip-column-names")
		cmd.Stdin = strings.NewReader(script)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("synthetic MySQL failed: %v %s", err, stderr.String())
		}
		return strings.TrimSpace(stdout.String())
	}
	run("DROP DATABASE IF EXISTS replica_sampling_synthetic; CREATE DATABASE replica_sampling_synthetic; USE replica_sampling_synthetic;" +
		"CREATE TABLE logs(id BIGINT PRIMARY KEY,user_id BIGINT,username VARCHAR(80),channel_id INT,created_at BIGINT,type INT,model_name VARCHAR(80),quota BIGINT,prompt_tokens BIGINT,completion_tokens BIGINT,use_time BIGINT,`group` VARCHAR(80),is_stream INT,content TEXT,token_name VARCHAR(80),token_id BIGINT,request_id VARCHAR(80),other LONGTEXT,INDEX idx_created_at_type(created_at,type));")
	var insert strings.Builder
	insert.WriteString("USE replica_sampling_synthetic; INSERT INTO logs VALUES ")
	documents := []string{
		`{"frt":500,"request_path":"/v1/chat/completions"}`,
		`{"frt":"1500","request_path":"/v1/messages","stream_status":{"end_reason":"client_gone"}}`,
		`{"frt":0}`, `{"frt":-1}`, `{"frt":null}`, `{}`, `null`, `"text"`, `not-json`, ``,
	}
	rowCount := 64
	if benchmark {
		rowCount = 20_000 // synthetic only; no source or snapshot import
	}
	for i := 1; i <= rowCount; i++ {
		if i > 1 {
			insert.WriteString(",")
		}
		typ, uid, tokenID := 2, 31+i%3, 1
		content, tokenName, requestID := "", fmt.Sprintf("tok%d", i%3), fmt.Sprintf("r%d", i)
		if i%5 == 0 {
			typ, content = 5, "status_code=429 rate limited"
		}
		if i%7 == 0 {
			typ = 6
		}
		if i%17 == 0 {
			typ, content, tokenName = 2, "模型测试", "模型测试"
		}
		if i%19 == 0 {
			typ, uid, tokenID, tokenName, requestID = 5, 1, 0, "", ""
		}
		createdAt, document := 100+i*5, documents[i%len(documents)]
		if benchmark {
			createdAt = 100 + i%600
			if strings.HasSuffix(document, "}") {
				padding := `"padding":"` + strings.Repeat("x", 512) + `"}`
				if document == "{}" {
					document = "{" + padding
				} else {
					document = strings.TrimSuffix(document, "}") + "," + padding
				}
			}
		}
		row := usageLogsSyntheticBind(t, "(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)", []any{
			i, uid, "synthetic", 7 + i%2, createdAt, typ, "gpt-test", i * 100,
			10, i % 3, i % 10, "g", i % 2, content, tokenName, tokenID, requestID, document,
		})
		insert.WriteString(row)
	}
	insert.WriteString(";")
	run(insert.String())
	normalized := func(q string) string {
		t.Helper()
		q = usageLogsSyntheticBind(t, q, []any{int64(100), int64(700)})
		rows := strings.Split(run("USE replica_sampling_synthetic;"+q+";"), "\n")
		sort.Strings(rows)
		return strings.Join(rows, "\n")
	}
	for _, pair := range []struct{ name, plain, optimized string }{
		{"token", sampleTokenAggregateSQL(), sampleTokenSQL()},
		{"FRT", frtRecomputeWindowSQL(), replicaFRTParseOnceCandidateSQL()},
		{"minute", sampleWindowAggregateSQL(false, false), sampleWindowSQL()},
		{"user minute", sampleWindowAggregateSQL(true, false), sampleWindowUserSQL()},
		{"health", sampleWindowAggregateSQL(true, true), sampleCustomerHealthWindowSQL()},
	} {
		if benchmark && pair.name != "token" && pair.name != "FRT" {
			continue // unchanged minute projections need no repeated large run
		}
		t.Run(pair.name, func(t *testing.T) {
			started := time.Now()
			plain := normalized(pair.plain)
			plainFirst := time.Since(started)
			started = time.Now()
			optimized := normalized(pair.optimized)
			onceFirst := time.Since(started)
			if plain == "" || plain != optimized {
				t.Fatal("MySQL plain and parse-once results differ")
			}
			if benchmark {
				started = time.Now()
				optimizedAgain := normalized(pair.optimized)
				onceSecond := time.Since(started)
				started = time.Now()
				plainAgain := normalized(pair.plain)
				plainSecond := time.Since(started)
				if plainAgain != plain || optimizedAgain != plain {
					t.Fatal("reversed-order performance samples changed results")
				}
				t.Logf("synthetic rows=%d; client times incl Docker CLI; original_ms=[%d,%d] candidate_ms=[%d,%d]; orders=original/candidate,candidate/original; not production latency",
					rowCount, plainFirst.Milliseconds(), plainSecond.Milliseconds(), onceFirst.Milliseconds(), onceSecond.Milliseconds())
			}
			query := usageLogsSyntheticBind(t, pair.optimized, []any{int64(100), int64(700)})
			plan := run("USE replica_sampling_synthetic; EXPLAIN FORMAT=JSON " + query + ";")
			if !strings.Contains(plan, "materialized_from_subquery") {
				t.Fatal("MySQL merged the parse-once table back into the outer aggregation")
			}
		})
	}
	// Budget-only edits must not alter the row set or ordering of finance reads.
	boundary, _ := financeGiftBoundarySQL(1)
	for name, q := range map[string]string{"credit": financeCreditHourSQL(), "gift": boundary} {
		args := []any{int64(100), int64(700)}
		if name == "gift" {
			args = append(args, int64(31))
		}
		q = usageLogsSyntheticBind(t, q, args)
		plain := strings.Replace(q, " /*+ MAX_EXECUTION_TIME(8000) */", "", 1)
		if run("USE replica_sampling_synthetic;"+q+";") != run("USE replica_sampling_synthetic;"+plain+";") {
			t.Fatalf("%s budget changed source rows", name)
		}
	}
	for _, tc := range []struct{ query, order string }{
		{usageFactHistoryDetailSQL("FLOOR((created_at+28800)/86400)", "user_id > 0", channelTestLogPredicateSQL()), "day_idx, user_id, channel_id, `group`, model_name, token_id"},
		{usageFactHistoryControlSQL("FLOOR((created_at+28800)/86400)", "user_id > 0", channelTestLogPredicateSQL()), "day_idx, user_id"},
	} {
		query := strings.Replace(tc.query, "LIMIT ?", "LIMIT 10000", 1)
		ordered := strings.Replace(query, "LIMIT 10000", "ORDER BY "+tc.order+" LIMIT 10000", 1)
		if normalized(query) != normalized(ordered) {
			t.Fatal("MySQL history groups changed after removing redundant sort")
		}
	}
	if benchmark {
		t.Log("synthetic performance: 2 JSON aggregates with reversed order, 2 finance and 2 history equivalence pairs passed")
	} else {
		t.Log("MySQL: 5 JSON aggregate, 2 finance row/order and 2 history sorting pairs passed")
	}
}
