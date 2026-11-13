// Bounded, sequential, read-only arrival audit. Never prints credentials or
// individual requests. Two runs with the same --to reveal late arrivals by minute.
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	_ "modernc.org/sqlite"
)

const aggregate = `SELECT (created_at DIV 60)*60 AS minute_ts, COUNT(*) AS requests, MAX(id) AS last_id, MAX(use_time) AS longest_seconds FROM logs WHERE created_at >= ? AND created_at < ? AND type = 2 GROUP BY minute_ts ORDER BY minute_ts`

type minute struct {
	Minute   int64         `json:"minute_ts"`
	Requests int64         `json:"requests"`
	LastID   int64         `json:"last_id"`
	Longest  sql.NullInt64 `json:"longest_request_seconds"`
}

func main() {
	var rawTo, out, envFile string
	var local bool
	flag.StringVar(&rawTo, "to", "", "fixed UTC right boundary")
	flag.StringVar(&out, "out", "", "evidence path")
	flag.StringVar(&envFile, "env", "", "optional env file; reads only NEWAPI_LOG_DSN")
	flag.BoolVar(&local, "local-tunnel", false, "use localhost for the existing host.docker.internal tunnel")
	flag.Parse()
	to, err := time.Parse(time.RFC3339, rawTo)
	if err != nil || out == "" {
		fail("--to RFC3339 and --out required")
	}
	if to.After(time.Now().Add(time.Minute)) || to.Before(time.Now().Add(-time.Hour)) {
		fail("audit boundary must be within the past hour")
	}
	from := to.Add(-20 * time.Minute)
	// Validate the equivalent range/group plan on a local indexed log snapshot
	// before opening any source connection. The production EXPLAIN must also
	// use a bounded index range; an ALL scan is rejected before aggregation.
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		fail("local audit DB unavailable")
	}
	db.SetMaxOpenConns(1)
	_, err = db.Exec(`CREATE TABLE logs(id INTEGER,created_at INTEGER,type INTEGER,use_time INTEGER); CREATE INDEX audit_log_time ON logs(created_at); INSERT INTO logs VALUES(1,1,2,3)`)
	if err != nil {
		fail("local schema failed")
	}
	plan, err := db.Query("EXPLAIN QUERY PLAN "+strings.ReplaceAll(aggregate, " DIV 60", " / 60"), from.Unix(), to.Unix())
	if err != nil {
		fail("local query plan failed")
	}
	localPlan := []string{}
	for plan.Next() {
		var id, parent, unused int
		var detail string
		if plan.Scan(&id, &parent, &unused, &detail) != nil {
			fail("local plan scan failed")
		}
		localPlan = append(localPlan, detail)
	}
	plan.Close()
	db.Close()
	if !strings.Contains(strings.Join(localPlan, " "), "USING INDEX audit_log_time") {
		fail("local plan is not a bounded range")
	}
	dsn := os.Getenv("NEWAPI_LOG_DSN")
	if dsn == "" && envFile != "" {
		b, e := os.ReadFile(envFile)
		if e != nil {
			fail("env file unavailable")
		}
		for _, line := range strings.Split(string(b), "\n") {
			k, v, ok := strings.Cut(strings.TrimSpace(line), "=")
			if ok && k == "NEWAPI_LOG_DSN" {
				dsn = strings.Trim(strings.TrimSpace(v), "\"'")
				break
			}
		}
	}
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		fail("invalid read-only source DSN")
	}
	if (cfg.User != "nexus_ro" && cfg.User != "monitor_ro") || cfg.DBName != "nexusapi" {
		fail("source must be allow-listed read-only nexusapi account")
	}
	if local {
		cfg.Addr = strings.Replace(cfg.Addr, "host.docker.internal:", "127.0.0.1:", 1)
	}
	cfg.Timeout = 5 * time.Second
	cfg.ReadTimeout = 10 * time.Second
	source, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		fail("source unavailable")
	}
	defer source.Close()
	source.SetMaxOpenConns(1)
	source.SetMaxIdleConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()
	var identity, database string
	if source.QueryRowContext(ctx, "SELECT CURRENT_USER(), DATABASE()").Scan(&identity, &database) != nil || !strings.HasPrefix(identity, cfg.User+"@") || database != "nexusapi" {
		fail("source identity mismatch")
	}
	explain, err := source.QueryContext(ctx, "EXPLAIN "+aggregate, from.Unix(), to.Unix())
	if err != nil {
		fail("production plan unavailable")
	}
	cols, _ := explain.Columns()
	sourcePlan := []map[string]string{}
	bounded := false
	for explain.Next() {
		vals := make([]sql.NullString, len(cols))
		ptr := make([]any, len(cols))
		for i := range vals {
			ptr[i] = &vals[i]
		}
		if explain.Scan(ptr...) != nil {
			fail("source plan invalid")
		}
		row := map[string]string{}
		for i, c := range cols {
			row[c] = vals[i].String
		}
		if row["table"] == "logs" && row["type"] == "range" {
			bounded = true
		}
		sourcePlan = append(sourcePlan, row)
	}
	explain.Close()
	if !bounded {
		fail("refusing non-range source plan")
	}
	started := time.Now()
	rows, err := source.QueryContext(ctx, strings.Replace(aggregate, "SELECT ", "SELECT /*+ MAX_EXECUTION_TIME(3000) */ ", 1), from.Unix(), to.Unix())
	if err != nil {
		fail("bounded source aggregation failed")
	}
	minutes := []minute{}
	for rows.Next() {
		var m minute
		if rows.Scan(&m.Minute, &m.Requests, &m.LastID, &m.Longest) != nil {
			fail("source row invalid")
		}
		minutes = append(minutes, m)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		fail("source aggregation incomplete")
	}
	result := map[string]any{"source": "production.logs", "account": cfg.User, "from": from.UTC().Format(time.RFC3339), "to": to.UTC().Format(time.RFC3339), "observed_at": time.Now().UTC().Format(time.RFC3339), "query_ms": time.Since(started).Milliseconds(), "sql": aggregate, "local_plan": localPlan, "source_plan": sourcePlan, "minutes": minutes, "arrival_semantics": "created_at is request time, not ingestion time; same fixed-window comparison brackets arrivals only"}
	b, err := json.MarshalIndent(result, "", "  ")
	if err != nil {
		fail("evidence encoding failed")
	}
	if os.MkdirAll(filepath.Dir(out), 0700) != nil || os.WriteFile(out, b, 0600) != nil {
		fail("cannot save evidence")
	}
	fmt.Printf("saved %d minute aggregates; %d ms\n", len(minutes), time.Since(started).Milliseconds())
}
func fail(s string) { fmt.Fprintln(os.Stderr, s); os.Exit(1) }
