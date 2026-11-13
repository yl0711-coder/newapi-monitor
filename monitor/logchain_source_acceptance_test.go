package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/go-sql-driver/mysql"
)

// Explicit, single-query acceptance against the configured read-only source.
// EXPLAIN runs before the application read; request contents stay out of the
// evidence. An ordinary test run never connects to a production database.
func TestLogChainSourceReadOnlyAcceptance(t *testing.T) {
	dsn := os.Getenv("MONITOR_LOGCHAIN_AUDIT_DSN")
	if dsn == "" {
		t.Skip("explicit read-only source audit not configured")
	}
	parsed, err := mysql.ParseDSN(dsn)
	if err != nil || parsed.DBName != "nexusapi" || (parsed.User != "nexus_ro" && parsed.User != "monitor_ro") {
		t.Fatal("read-only source identity rejected")
	}
	from, e1 := strconv.ParseInt(os.Getenv("MONITOR_LOGCHAIN_AUDIT_FROM"), 10, 64)
	to, e2 := strconv.ParseInt(os.Getenv("MONITOR_LOGCHAIN_AUDIT_TO"), 10, 64)
	out := os.Getenv("MONITOR_LOGCHAIN_AUDIT_OUT")
	if e1 != nil || e2 != nil || from <= 0 || to <= from || to-from > 86400 || out == "" {
		t.Fatal("fixed window up to one day and evidence path required")
	}
	scope := logChainScope{FromTs: from, ToTs: to, Limit: 100}
	if os.Getenv("MONITOR_LOGCHAIN_AUDIT_ALL") != "true" {
		scope.Anomaly = anomalyErrAnom
	}
	m := &Monitor{cfg: Settings{ProdDSN: dsn, LogChainOnlySource: true,
		sourceLifecycleConfigured: true, sourceProbe: func(context.Context, *sql.DB) error { return nil }}}
	if err := m.initializeSource(); err != nil {
		t.Fatal("audit initialization failed")
	}
	defer m.prodDB.Close()
	m.prodDB.SetMaxOpenConns(1)
	ctx, cancel := context.WithTimeout(context.Background(), logChainGateTimeout)
	defer cancel()
	var identity, database string
	if err := m.prodDB.QueryRowContext(ctx, "SELECT CURRENT_USER(),DATABASE()").Scan(&identity, &database); err != nil || database != parsed.DBName || !strings.HasPrefix(identity, parsed.User+"@") {
		t.Fatal("source identity verification failed")
	}
	q, args := m.logChainPageQuery(scope, nil)
	evidence := map[string]any{"at": time.Now().UTC(), "from": from, "to": to, "limit": scope.Limit,
		"anomaly": scope.Anomaly, "sql": q, "args": args, "source_concurrency": 1, "read_only": true}
	defer func() {
		b, _ := json.MarshalIndent(evidence, "", "  ")
		if err := os.WriteFile(out, append(b, '\n'), 0600); err != nil {
			t.Error("cannot save audit evidence")
		}
	}()
	planRows, err := m.prodDB.QueryContext(ctx, "EXPLAIN "+q, args...)
	if err != nil {
		t.Fatal("source explain unavailable")
	}
	cols, _ := planRows.Columns()
	var plan []map[string]string
	bounded := true
	for planRows.Next() {
		values := make([]sql.NullString, len(cols))
		ptr := make([]any, len(cols))
		for i := range values {
			ptr[i] = &values[i]
		}
		if err := planRows.Scan(ptr...); err != nil {
			t.Fatal("source plan decode failed")
		}
		row := map[string]string{}
		for i, name := range cols {
			row[name] = values[i].String
		}
		if row["table"] == "logs" && (row["type"] == "ALL" || row["type"] == "index" || row["key"] == "") {
			bounded = false
		}
		plan = append(plan, row)
	}
	err = planRows.Err()
	planRows.Close()
	evidence["plan"] = plan
	if err != nil || !bounded || len(plan) == 0 {
		t.Fatal("refusing unbounded or incomplete source plan")
	}
	if os.Getenv("MONITOR_LOGCHAIN_AUDIT_PLAN_ONLY") == "true" {
		return
	}
	started := time.Now()
	rows, more, err := m.queryLogChain(context.Background(), scope, nil)
	evidence["elapsed_ms"], evidence["rows"], evidence["has_more"] = time.Since(started).Milliseconds(), len(rows), more
	evidence["source_budget"] = m.usageSourceStats()
	if err != nil {
		evidence["error"] = err.Error()
		t.Fatal(err)
	}
	if len(rows) > scope.Limit {
		t.Fatal("source page exceeded bound")
	}
	for i := 1; i < len(rows); i++ {
		if rows[i].CreatedAt > rows[i-1].CreatedAt || (rows[i].CreatedAt == rows[i-1].CreatedAt && rows[i].ID >= rows[i-1].ID) {
			t.Fatal("source page ordering or uniqueness failed")
		}
	}
	b, _ := json.Marshal(rows)
	evidence["row_sha256"] = fmt.Sprintf("%x", sha256.Sum256(b))
}
