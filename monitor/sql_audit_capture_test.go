package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/callbacks"
)

type capturedSQLPlan struct {
	Fixture     string   `json:"fixture"`
	Store       string   `json:"store"`
	File        string   `json:"file"`
	Line        int      `json:"line"`
	Function    string   `json:"function"`
	Fingerprint string   `json:"sql_sha256"`
	Template    string   `json:"redacted_template"`
	Parameters  int      `json:"parameters"`
	Shape       []string `json:"binding_shape"`
	Status      string   `json:"status"`
	Plan        []string `json:"plan,omitempty"`
	ErrorClass  string   `json:"error_class,omitempty"`
}

type sqlPlanCapture struct {
	mu        sync.Mutex
	seen      map[string]bool
	items     []capturedSQLPlan
	onlyFiles map[string]bool
}

var activeSQLPlanCapture *sqlPlanCapture
var auditStringLiteral = regexp.MustCompile(`'(?:''|[^'])*'`)

func TestMain(m *testing.M) {
	path := os.Getenv("MONITOR_SQL_AUDIT_REPORT")
	if path == "" {
		os.Exit(m.Run())
	}
	abs, err := filepath.Abs(path)
	root, rootErr := filepath.Abs("../docs/sql-audit-20261126")
	if err != nil || rootErr != nil || !strings.HasPrefix(abs, root+string(os.PathSeparator)) {
		fmt.Fprintln(os.Stderr, "SQL audit output must be inside the task report directory")
		os.Exit(2)
	}
	// Audit mode is for synthetic local tests, never an invitation to use real
	// snapshot, MySQL, AWS, export or browser acceptance environment flags.
	for _, key := range []string{"NEWAPI_LOG_DSN", "NEWAPI_BASE_URL", "MONITOR_USAGE_LOGS_BROWSER", "MONITOR_REPLICA_SQL_MYSQL_CONTAINER", "MONITOR_USAGE_SQL_MYSQL_CONTAINER", "MONITOR_FINANCE_ACCEPTANCE_MAIN_SNAPSHOT", "MONITOR_FINANCE_ACCEPTANCE_FACTS_SNAPSHOT"} {
		if os.Getenv(key) != "" {
			fmt.Fprintln(os.Stderr, "SQL audit rejects external acceptance configuration")
			os.Exit(2)
		}
	}
	for _, value := range os.Environ() {
		key, setting, _ := strings.Cut(value, "=")
		if sqlAuditBlockedTokenSetting(key, setting) {
			fmt.Fprintln(os.Stderr, "SQL audit rejects external or optional Token acceptance configuration")
			os.Exit(2)
		}
	}
	files := map[string]bool{}
	sourceHashes := map[string]string{}
	for _, file := range strings.Split(os.Getenv("MONITOR_SQL_AUDIT_FILES"), ",") {
		file = strings.TrimSpace(file)
		if file == "" {
			continue
		}
		if !strings.HasPrefix(file, "monitor/") || filepath.Clean(file) != file || strings.Contains(strings.TrimPrefix(file, "monitor/"), "/") || !strings.HasSuffix(file, ".go") || strings.HasSuffix(file, "_test.go") {
			fmt.Fprintln(os.Stderr, "invalid SQL audit source scope")
			os.Exit(2)
		}
		body, e := os.ReadFile("../" + file)
		if e != nil {
			fmt.Fprintln(os.Stderr, "unreadable SQL audit source scope")
			os.Exit(2)
		}
		sum := sha256.Sum256(body)
		sourceHashes[file] = hex.EncodeToString(sum[:])
		files[file] = true
	}
	activeSQLPlanCapture = &sqlPlanCapture{seen: map[string]bool{}, items: []capturedSQLPlan{}, onlyFiles: files}
	code := m.Run()
	for file, want := range sourceHashes {
		body, e := os.ReadFile("../" + file)
		sum := sha256.Sum256(body)
		if e != nil || hex.EncodeToString(sum[:]) != want {
			fmt.Fprintln(os.Stderr, "SQL audit source changed during tests")
			code = 2
		}
	}
	c := activeSQLPlanCapture
	c.mu.Lock()
	sort.Slice(c.items, func(i, j int) bool {
		a, b := c.items[i], c.items[j]
		if a.File != b.File {
			return a.File < b.File
		}
		if a.Line != b.Line {
			return a.Line < b.Line
		}
		return a.Fingerprint < b.Fingerprint
	})
	counts := map[string]int{}
	for _, item := range c.items {
		counts[item.Status]++
	}
	data, marshalErr := json.MarshalIndent(struct {
		Warning      string            `json:"warning"`
		TestExit     int               `json:"test_exit"`
		SourceHashes map[string]string `json:"source_sha256,omitempty"`
		Counts       map[string]int    `json:"counts"`
		Entries      []capturedSQLPlan `json:"entries"`
	}{"Synthetic fixture plans only; store labels distinguish SQLite from explicitly instrumented isolated MySQL. Untriggered branches and uninstrumented direct database/sql calls remain uncovered; full scans are review flags, not automatic bugs. Bind values are not stored. Scoped source hashes checked before/after tests, not schema/image/whole-program attestations.", code, sourceHashes, counts, c.items}, "", "  ")
	c.mu.Unlock()
	if marshalErr != nil || os.MkdirAll(root, 0700) != nil || os.WriteFile(abs, append(data, '\n'), 0600) != nil {
		fmt.Fprintln(os.Stderr, "failed to retain SQL audit evidence")
		os.Exit(2)
	}
	fmt.Printf("SQL_AUDIT records=%d states=%v report=%s\n", len(c.items), counts, abs)
	os.Exit(code)
}

// Optional Token tests also accept _DB, _READONLY_FILE, _CAPTURE and replay
// inputs, not only names containing SNAPSHOT. Audit batches are synthetic-only;
// reject all inherited opt-ins, including unexpectedly expensive scale runs.
func sqlAuditBlockedTokenSetting(key, value string) bool {
	return strings.HasPrefix(key, "TOKEN_") && value != ""
}

func TestSQLAuditExternalTokenInputsAreBlocked(t *testing.T) {
	for _, key := range []string{"TOKEN_BACKLOG_SNAPSHOT", "TOKEN_CALIBRATION_DB", "TOKEN_REFERENCE_READONLY_FILE", "TOKEN_ACQUISITION_CAPTURE", "TOKEN_RC48_REPLAY_INPUT", "TOKEN_QUEUE_SCALE"} {
		if !sqlAuditBlockedTokenSetting(key, "configured") || sqlAuditBlockedTokenSetting(key, "") {
			t.Fatalf("unsafe external Token audit setting: %s", key)
		}
	}
	if sqlAuditBlockedTokenSetting("MONITOR_SQL_AUDIT_REPORT", "local") {
		t.Fatal("audit's own output configuration blocked")
	}
}

// Only existing synthetic fixture handles are instrumented; the application
// code, connection count, database files and schema remain unchanged.
func attachSQLAuditFixture(t *testing.T, m *Monitor) {
	if activeSQLPlanCapture == nil {
		return
	}
	// Deterministic ordering: a legacy shared handle is labeled main; a
	// physically independent Token/evidence fixture retains its own identity.
	for _, store := range []struct {
		label string
		db    *gorm.DB
	}{{"main", m.storeDB}, {"facts", m.usageFactsDB}, {"token", m.tokenDB}, {"token-reader", m.tokenReadDB}, {"evidence", m.nginxEvidenceDB}} {
		label, db := store.label, store.db
		if db == nil || db.Dialector.Name() != "sqlite" {
			continue
		}
		name := "audit:explain"
		if db.Callback().Query().Get(name) != nil {
			continue
		}
		capture := func(tx *gorm.DB) { activeSQLPlanCapture.capture(t.Name(), label, tx) }
		read := func(tx *gorm.DB) {
			if tx.Error == nil && tx.Statement.SQL.Len() == 0 {
				callbacks.BuildQuerySQL(tx)
			}
			capture(tx)
		}
		for _, err := range []error{
			db.Callback().Query().Before("gorm:query").Register(name, read),
			db.Callback().Row().Before("gorm:row").Register(name, read),
			db.Callback().Raw().Before("gorm:raw").Register(name, capture),
			db.Callback().Create().After("gorm:create").Before("gorm:commit_or_rollback_transaction").Register(name, capture),
			db.Callback().Update().After("gorm:update").Before("gorm:commit_or_rollback_transaction").Register(name, capture),
			db.Callback().Delete().After("gorm:delete").Before("gorm:commit_or_rollback_transaction").Register(name, capture),
		} {
			if err != nil {
				t.Fatal(err)
			}
		}
	}
}

func sqlAuditCaller() (string, int, string) {
	pcs := make([]uintptr, 30)
	n := runtime.Callers(3, pcs)
	frames := runtime.CallersFrames(pcs[:n])
	for {
		f, more := frames.Next()
		if strings.Contains(f.File, "/monitor/") && !strings.HasSuffix(f.File, "sql_audit_capture_test.go") {
			// A test visitor can issue fault-injection SQL while an application
			// function is above it on the stack. Do not misattribute that SQL to
			// the older production frame and "fix" a test-only mutation.
			if strings.HasSuffix(f.File, "_test.go") {
				return "", 0, ""
			}
			return "monitor/" + filepath.Base(f.File), f.Line, f.Function
		}
		if !more {
			break
		}
	}
	return "", 0, ""
}

func (c *sqlPlanCapture) capture(fixture, store string, db *gorm.DB) {
	if db.Statement == nil || db.Statement.SQL.Len() == 0 {
		return
	}
	file, line, fn := sqlAuditCaller()
	if file == "" {
		return
	} // fixture-only setup is not application coverage
	if len(c.onlyFiles) > 0 && !c.onlyFiles[file] {
		return
	}
	query := db.Statement.SQL.String()
	sum := sha256.Sum256([]byte(query))
	fingerprint := hex.EncodeToString(sum[:])
	item := capturedSQLPlan{Fixture: fixture, Store: store, File: file, Line: line, Function: fn, Fingerprint: fingerprint, Template: auditStringLiteral.ReplaceAllString(query, "'<redacted>'"), Parameters: len(db.Statement.Vars), Status: "unreviewed"}
	for _, v := range db.Statement.Vars {
		shape := fmt.Sprintf("%T", v)
		if s, ok := v.(string); ok {
			switch {
			case s == "":
				shape = "string:empty"
			case strings.HasPrefix(s, "%"):
				shape = "string:leading_wildcard"
			case strings.Contains(s, "%"):
				shape = "string:contains_wildcard"
			default:
				shape = "string:literal"
			}
		}
		item.Shape = append(item.Shape, shape)
	}
	key := fmt.Sprintf("%s|%s|%s:%d|%s|%s", fixture, store, file, line, fingerprint, strings.Join(item.Shape, ","))
	c.mu.Lock()
	if c.seen[key] {
		c.mu.Unlock()
		return
	}
	c.seen[key] = true
	c.mu.Unlock()
	finish := func() { c.mu.Lock(); c.items = append(c.items, item); c.mu.Unlock() }
	if db.DryRun {
		item.Status = "dry_run_template_not_explained"
		finish()
		return
	}
	if db.Error != nil {
		item.Status = "original_query_failed"
		finish()
		return
	}
	if !sqlAuditPlanStatement(query) {
		item.Status = "not_a_query_plan_statement"
		finish()
		return
	}
	ctx := db.Statement.Context
	if ctx == nil {
		ctx = context.Background()
	}
	if deadline, ok := ctx.Deadline(); ctx.Err() != nil || (ok && time.Until(deadline) < 750*time.Millisecond) {
		item.Status = "not_run_short_or_cancelled_budget"
		finish()
		return
	}
	qctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	rows, err := db.Statement.ConnPool.QueryContext(qctx, "EXPLAIN QUERY PLAN "+query, db.Statement.Vars...)
	if err != nil {
		item.Status = "explain_error"
		item.ErrorClass = fmt.Sprintf("%T", err)
		finish()
		return
	}
	defer rows.Close()
	for rows.Next() {
		var id, parent, unused int
		var detail string
		if err = rows.Scan(&id, &parent, &unused, &detail); err != nil {
			break
		}
		item.Plan = append(item.Plan, detail)
		if len(item.Plan) > 200 {
			item.Status = "plan_row_limit"
			finish()
			return
		}
	}
	if err == nil {
		err = rows.Err()
	}
	if err != nil {
		item.Status = "explain_error"
		item.ErrorClass = fmt.Sprintf("%T", err)
	} else {
		item.Status = "explained"
	}
	finish()
}

func sqlAuditPlanStatement(query string) bool {
	f := strings.Fields(query)
	if len(f) == 0 {
		return false
	}
	switch strings.ToUpper(f[0]) {
	case "SELECT", "WITH", "INSERT", "UPDATE", "DELETE", "REPLACE":
		return true
	}
	return false
}

func TestSQLAuditPlanStatementWhitespace(t *testing.T) {
	for _, q := range []string{"SELECT 1", "SELECT\n1", " select\t1", "WITH\nx AS (SELECT 1) SELECT * FROM x", "UPDATE\nt SET x=1"} {
		if !sqlAuditPlanStatement(q) {
			t.Fatal("valid query skipped", q)
		}
	}
	for _, q := range []string{"", "SELECTED 1", "PRAGMA table_info(t)", "DROP TABLE t"} {
		if sqlAuditPlanStatement(q) {
			t.Fatal("non-plan classified", q)
		}
	}
}
