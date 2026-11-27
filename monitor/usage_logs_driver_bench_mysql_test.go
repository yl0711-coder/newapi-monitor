package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"
)

type usageBenchStep struct {
	SQL  string
	Args []any
	Want *string
}
type usageBenchVariant struct {
	Name  string
	Steps []usageBenchStep
}
type usageBenchCase struct {
	Name     string
	Variants []usageBenchVariant
}
type usageTraceMySQLDriver struct {
	inner driver.Driver
	steps *[]usageBenchStep
}
type usageTraceMySQLConn struct {
	driver.Conn
	steps *[]usageBenchStep
}

func (d usageTraceMySQLDriver) Open(s string) (driver.Conn, error) {
	c, e := d.inner.Open(s)
	if e != nil {
		return nil, e
	}
	return usageTraceMySQLConn{c, d.steps}, nil
}
func (c usageTraceMySQLConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	s := usageBenchStep{SQL: q}
	for _, v := range a {
		s.Args = append(s.Args, v.Value)
	}
	*c.steps = append(*c.steps, s)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, a)
}

func TestUsageLogsPersistentDriverMySQL(t *testing.T) {
	if os.Getenv("MONITOR_SQL_BENCH_TOOL") != "/tmp/usage-sql-bench" {
		t.Skip("requires task-owned Linux replay tool")
	}
	run := isolatedSQLOptimizationRunner(t)
	if out, e := exec.Command("docker", "exec", os.Getenv("MONITOR_SQL_OPT_CONTAINER"), "test", "-x", "/tmp/usage-sql-bench").CombinedOutput(); e != nil {
		t.Fatalf("benchmark tool unavailable before fixture: %v %s", e, out)
	}
	run(`DROP DATABASE IF EXISTS sql_usage_perf;CREATE DATABASE sql_usage_perf;USE sql_usage_perf;
CREATE TABLE logs(id BIGINT PRIMARY KEY,created_at BIGINT,type INT,quota BIGINT,user_id BIGINT,username VARCHAR(80),model_name VARCHAR(80),token_name VARCHAR(80),channel_id INT,` + "`group`" + ` VARCHAR(80),request_id VARCHAR(80),upstream_request_id VARCHAR(80),padding VARCHAR(32),
INDEX idx_logs_username(username),INDEX idx_logs_channel_id(channel_id),INDEX idx_logs_model_name(model_name),INDEX idx_logs_group(` + "`group`" + `),INDEX idx_created_at_type(created_at,type),INDEX idx_user_created_type(user_id,created_at,type));
INSERT INTO logs ` + syntheticDigitsCTE() + `SELECT n,100+FLOOR((n-1)/5),IF(MOD(n,11)=0,5,2),IF(MOD(n,97)=0,NULL,MOD(n,7)-3),201,IF(MOD(n,10000)=0,'rare','heavy'),'m','t',IF(MOD(n,2)=0,7,9),'g',CONCAT('r',n),CONCAT('u',n),REPEAT('p',32) FROM seq WHERE n<=100000;ANALYZE TABLE logs;`)
	type phase struct {
		Name string
		Data json.RawMessage
	}
	var phases []phase
	for _, p := range []string{"dense", "sparse", "shuffle"} {
		if p == "sparse" {
			run("USE sql_usage_perf;UPDATE logs SET created_at=IF(MOD(id,100)=0,101,90);ANALYZE TABLE logs;")
		}
		if p == "shuffle" {
			run("USE sql_usage_perf;UPDATE logs SET created_at=100+MOD(id*7919,20000);ANALYZE TABLE logs;")
		}
		var cases []usageBenchCase
		for i, f := range []usageLogFilter{{Start: 18100, End: 20099, Username: "heavy", Channel: 7}, {Start: 100, End: 20099, Username: "heavy", Channel: 7}, {Start: 100, End: 20099, Username: "rare", Channel: 7}, {Start: 100, End: 20099, Username: "missing", Channel: 7}, {Start: 100, End: 20099, Channel: 999}, {Start: 18100, End: 20099, Channel: 7, ModelName: "m", Group: "g"}, {Start: 18100, End: 20099}, {Start: 100, End: 20099}, {Start: 20100, End: 20200, Channel: 7}} {
			if p != "dense" {
				f.Start, f.End = 100, 102
				if p == "shuffle" {
					f.End = 1099
				}
				if i != 0 && i != 6 {
					continue
				}
			}
			scope, expr := usageLogWhereList, "COUNT(*)"
			if f.Username == "" {
				scope, expr = usageLogWhereStat, "COALESCE(SUM(quota),0)"
			}
			w, e := buildUsageLogWhere(f, scope)
			if e != nil {
				t.Fatal(e)
			}
			q := usageLogsSelect + expr + " FROM logs" + w.sql()
			want := run("USE sql_usage_perf;" + usageLogsSyntheticBind(t, q, w.args) + ";")
			bc := usageBenchCase{Name: fmt.Sprintf("%s-%d", p, i), Variants: []usageBenchVariant{{"native", []usageBenchStep{{q, w.args, &want}}}}}
			var steps []usageBenchStep
			var route []string
			capture := func(q string) string {
				out := run(q)
				i := len(steps) - 1
				if i < 0 {
					t.Fatal("capture without query")
				}
				if out != "NULL" {
					value := out
					steps[i].Want = &value
				}
				return out
			}
			name := fmt.Sprintf("trace-mysql-%d", usageQuotaDriverID.Add(1))
			sql.Register(name, usageTraceMySQLDriver{usagePerfSocketMySQLDriver{t, capture, &route}, &steps})
			db, e := sql.Open(name, "")
			if e != nil {
				t.Fatal(e)
			}
			db.SetMaxOpenConns(1)
			m := &Monitor{prodDB: db}
			var got int64
			if scope == usageLogWhereStat {
				got, e = m.queryUsageLogsQuota(context.Background(), db, f, w)
			} else {
				got, e = m.queryUsageLogsCount(context.Background(), db, f, w)
			}
			if e != nil || fmt.Sprint(got) != want {
				t.Fatalf("capture changed value %d %s %v", got, want, e)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("capture leaked connection")
			}
			db.Close()
			bc.Variants = append(bc.Variants, usageBenchVariant{"current", steps})
			if os.Getenv("MONITOR_SQL_BENCH_CANDIDATE") == "1" {
				steps = nil
				name := fmt.Sprintf("inline-trace-mysql-%d", usageQuotaDriverID.Add(1))
				sql.Register(name, usageTraceMySQLDriver{usagePerfSocketMySQLDriver{t, capture, &route}, &steps})
				candidateDB, e := sql.Open(name, "")
				if e != nil {
					t.Fatal(e)
				}
				candidateDB.SetMaxOpenConns(1)
				planning := f
				if scope == usageLogWhereStat {
					planning.Type = 0
				}
				got, e := usageLogsReadInlineCandidate(context.Background(), candidateDB, planning, w, expr)
				if e != nil || fmt.Sprint(got) != want {
					t.Fatalf("inline candidate changed value %v %d %s", e, got, want)
				}
				if candidateDB.Stats().InUse != 0 {
					t.Fatal("candidate leaked connection")
				}
				candidateDB.Close()
				bc.Variants = append(bc.Variants, usageBenchVariant{"proposed", steps})
			}
			cases = append(cases, bc)
		}
		in, e := json.Marshal(struct{ Cases []usageBenchCase }{cases})
		if e != nil {
			t.Fatal(e)
		}
		ctx, cancel := context.WithTimeout(context.Background(), 55*time.Second)
		cmd := exec.CommandContext(ctx, "docker", "exec", "-i", os.Getenv("MONITOR_SQL_OPT_CONTAINER"), "/tmp/usage-sql-bench")
		cmd.Stdin = bytes.NewReader(in)
		var out, diag bytes.Buffer
		cmd.Stdout = &out
		cmd.Stderr = &diag
		e = cmd.Run()
		cancel()
		if e != nil {
			t.Fatalf("driver bench stopped: %v %s", e, diag.String())
		}
		if out.Len() > 1<<20 || !json.Valid(out.Bytes()) {
			t.Fatal("invalid/budgeted driver report")
		}
		phases = append(phases, phase{p, json.RawMessage(out.Bytes())})
	}
	hashes := map[string]string{}
	for _, p := range []string{"usage_logs_aggregate_probe.go", "usage_logs_aggregate_plan.go", "usage_logs_quota.go", "usage_logs_driver_bench_mysql_test.go", "usage_logs_inline_candidate_test.go", "../dev/usage-sql-bench/main.go"} {
		b, e := os.ReadFile(p)
		if e != nil {
			t.Fatal(e)
		}
		hashes[p] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	raw, e := json.MarshalIndent(struct {
		Phases       []phase
		SourceSHA256 map[string]string
	}{phases, hashes}, "", "  ")
	if e != nil {
		t.Fatal(e)
	}
	output := "../docs/sql-audit-20261126/usage-persistent-driver-before-20261127.json"
	if strings.TrimSpace(os.Getenv("MONITOR_SQL_BENCH_AFTER")) == "1" {
		output = "../docs/sql-audit-20261126/usage-persistent-driver-after-20261127.json"
	}
	if os.Getenv("MONITOR_SQL_BENCH_CANDIDATE") == "1" {
		output = "../docs/sql-audit-20261126/usage-persistent-driver-candidate-20261127.json"
	}
	if e = os.WriteFile(output, append(raw, '\n'), 0600); e != nil {
		t.Fatal(e)
	}
	t.Logf("persistent driver: %d phases, captured actual SQL and args", len(phases))
}
