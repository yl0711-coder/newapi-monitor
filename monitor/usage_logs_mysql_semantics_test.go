package monitor

import (
	"bytes"
	"context"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Opt in only against a disposable, network-isolated container carrying our
// task label. No source DSN or real data is used. This proves MySQL semantics,
// not production latency, durable storage, or the production reference COUNT.
func TestUsageLogsMySQLAggregateSemantics(t *testing.T) {
	container := os.Getenv("MONITOR_USAGE_SQL_MYSQL_CONTAINER")
	if container == "" {
		t.Skip("requires explicitly selected isolated synthetic MySQL container")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	check := exec.CommandContext(ctx, "docker", "inspect", "--format", `{{index .Config.Labels "ai.task"}} {{.HostConfig.NetworkMode}}`, container)
	metadata, err := check.CombinedOutput()
	if err != nil || strings.TrimSpace(string(metadata)) != "monitor-usage-sql-20261126 none" {
		t.Fatalf("refusing non-isolated/non-owned MySQL container: %v", err)
	}
	run := func(script string) string {
		t.Helper()
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", container, "mysql", "--protocol=socket", "-uroot", "--batch", "--skip-column-names")
		cmd.Stdin = strings.NewReader(script)
		var stdout, stderr bytes.Buffer
		cmd.Stdout, cmd.Stderr = &stdout, &stderr
		if err := cmd.Run(); err != nil {
			t.Fatalf("synthetic MySQL query failed: %v %s", err, stderr.String())
		}
		return strings.TrimSpace(stdout.String())
	}
	// This database exists only inside the test-owned tmpfs container.
	run("DROP DATABASE IF EXISTS usage_logs_synthetic; CREATE DATABASE usage_logs_synthetic; USE usage_logs_synthetic;" +
		"CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,`group` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),INDEX idx_created_at_type(created_at,type));" +
		"INSERT INTO logs VALUES (1,90,2,1,31,'old','m','t',7,'g','r1','s1')," +
		"(2,100,2,10,31,'raw_name','m','t',7,'g','r2','s2')," +
		"(3,99,2,100,31,'raw_name','m','t',7,'g','r3','s3')," +
		"(4,150,5,900,31,'interim','m','t',7,'g','r4','s4')," +
		"(5,175,2,-30,40,'raw_name','other','t',9,'g','r5','s5')," +
		"(6,210,2,700,31,'raw_name','m','t',7,'g','r6','s6')," +
		"(7,200,2,40,31,'raw_name','m','t',7,'g','r7','s7');")
	filters := []usageLogFilter{
		{Start: 100, End: 200},
		{Start: 100, End: 200, Username: "raw_name", Channel: 7},
		{Start: 100, End: 200, Username: "%raw_name%", Channel: 7},
		{Start: 100, End: 200, Username: "missing", Channel: 7},
		{Start: 100, End: 200, Username: "raw_name", Group: "g", ModelName: "m", Type: 5},
		{Start: 100, End: 200, RequestID: "r7", UpstreamRequestID: "s7"},
		{Start: 100, End: 200, TokenName: "t"},
		{Start: 100, End: 200, UserID: 31},
		{Start: 500, End: 600, Channel: 7},
	}
	for i, f := range filters {
		for _, scope := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
			w, err := buildUsageLogWhere(f, scope)
			if err != nil {
				t.Fatal(err)
			}
			for _, expression := range []string{"COUNT(*)", "COALESCE(SUM(quota),0)"} {
				q, args := usageLogsAggregateSpanSQL(f, w, expression)
				candidate := usageLogsSyntheticBind(t, q, args)
				original := usageLogsSyntheticBind(t, "SELECT "+expression+" FROM logs"+w.sql(), w.args)
				got := strings.Split(run("USE usage_logs_synthetic;"+candidate+";"+original+";"), "\n")
				if len(got) != 2 || got[0] != got[1] {
					t.Fatalf("case %d scope %d %s differs: %v", i, scope, expression, got)
				}
			}
		}
	}
	f := usageLogFilter{Start: 100, End: 200, Username: "raw_name", Channel: 7}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	for _, expression := range []string{"COUNT(*)", "COALESCE(SUM(quota),0)"} {
		q, args := usageLogsAggregateSpanSQL(f, w, expression)
		args[0] = int64(1) // cap must request fallback, not return an incorrect zero.
		if got := run("USE usage_logs_synthetic;" + usageLogsSyntheticBind(t, q, args) + ";"); got != "NULL" {
			t.Fatalf("over cap should request fallback: %s", got)
		}
	}
	run("USE usage_logs_synthetic; UPDATE logs SET id=id+9223372036853000000;")
	for _, expression := range []string{"COUNT(*)", "COALESCE(SUM(quota),0)"} {
		q, args := usageLogsAggregateSpanSQL(f, w, expression)
		candidate := usageLogsSyntheticBind(t, q, args)
		original := usageLogsSyntheticBind(t, "SELECT "+expression+" FROM logs"+w.sql(), w.args)
		got := strings.Split(run("USE usage_logs_synthetic;"+candidate+";"+original+";"), "\n")
		if len(got) != 2 || got[0] != got[1] {
			t.Fatalf("large id %s differs: %v", expression, got)
		}
	}
	run("USE usage_logs_synthetic; INSERT INTO logs VALUES (1000000,150,2,5,31,'raw_name','m','t',7,'g','r8','s8');")
	q, args := usageLogsAggregateSpanSQL(f, w, "COUNT(*)")
	if got := run("USE usage_logs_synthetic;" + usageLogsSyntheticBind(t, q, args) + ";"); got != "NULL" {
		t.Fatalf("sparse span should request fallback: %s", got)
	}
	t.Log("38 candidate/native MySQL pairs and 3 NULL-fallback cases passed")
}

// Only synthetic values reach this CLI helper. Production always uses driver
// placeholders; this is deliberately not a reusable production SQL formatter.
func usageLogsSyntheticBind(t *testing.T, q string, args []any) string {
	t.Helper()
	for _, arg := range args {
		var value string
		switch v := arg.(type) {
		case string:
			if strings.ContainsAny(v, "\\\x00") {
				t.Fatal("unsupported synthetic SQL value")
			}
			value = "'" + strings.ReplaceAll(v, "'", "''") + "'"
		case int:
			value = strconv.Itoa(v)
		case int64:
			value = strconv.FormatInt(v, 10)
		default:
			t.Fatalf("unsupported synthetic argument %T", arg)
		}
		q = strings.Replace(q, "?", value, 1)
	}
	if strings.Contains(q, "?") {
		t.Fatal("unbound synthetic query")
	}
	return q
}
