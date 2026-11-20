package monitor

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"sort"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	mysql "github.com/go-sql-driver/mysql"
)

var usageLogsMySQLPilotPaths = []string{
	"meta", "user/31", "user/99", "list?request_id=req-238",
	"list?upstream_request_id=up-22&p=3&ps=2",
	"list?start_timestamp=1794500000&end_timestamp=1794550000&channel=1",
	"list?start_timestamp=1794500000&end_timestamp=1794550000&username=stable&channel=2",
	"list?start_timestamp=1794500000&end_timestamp=1794550000&username=renamed&defer_total=1",
	"list?group=codex&page_size=5&p=2&before_created_at=1794540000&before_id=2000&defer_total=1",
	"count?start_timestamp=1794500000&end_timestamp=1794550000&channel=1",
	"count?start_timestamp=1794500000&end_timestamp=1794550000&username=stable&channel=2",
	"stat?start_timestamp=1794500000&end_timestamp=1794550000&model_name=fixture_a",
	"stat?start_timestamp=1794500000&end_timestamp=1794550000&skip_quota=1",
}

// Local-only opt-in. Run on an isolated fixture with no other traffic.
// The account and schema must be the synthetic fixtures; never use live data.
func TestUsageLogsIsolatedMySQLParity(t *testing.T) {
	db, _ := openUsageLogsPilotFixture(t)
	gin.SetMode(gin.TestMode)
	m := &Monitor{prodDB: db}
	engine := gin.New()
	m.RegisterRoutes(engine)
	session := m.signSession("pilot-test-admin", roleAdmin, time.Now().Unix())
	proof := sha256.New()
	for _, route := range usageLogsMySQLPilotPaths {
		_, _ = proof.Write(usageLogsPilotResponse(t, engine, session, route))
	}
	before := usageLogsPilotMySQLSelects(t, db)
	var nanos []int64
	for range 25 {
		for _, route := range usageLogsMySQLPilotPaths {
			started := time.Now()
			usageLogsPilotResponse(t, engine, session, route)
			nanos = append(nanos, time.Since(started).Nanoseconds())
		}
	}
	selects := usageLogsPilotMySQLSelects(t, db) - before
	if selects <= 0 {
		t.Fatal("fixture did not observe real MySQL selects")
	}
	sort.Slice(nanos, func(i, j int) bool { return nanos[i] < nanos[j] })
	t.Logf("PARITY hash=%x requests=%d SQL_selects=%d p50_ms=%.3f p95_ms=%.3f", proof.Sum(nil),
		len(nanos), selects, float64(nanos[len(nanos)/2])/1e6, float64(nanos[len(nanos)*95/100])/1e6)
}

func openUsageLogsPilotFixture(t *testing.T) (*sql.DB, *mysql.Config) {
	t.Helper()
	addr := os.Getenv("MONITOR_USAGE_LOGS_MYSQL_FIXTURE")
	if addr == "" {
		t.Skip("enable only for the disposable dev/fixtures/usage-logs-mysql.sql database")
	}
	host, port, err := net.SplitHostPort(addr)
	ip := net.ParseIP(host)
	if err != nil || port == "" || ip == nil || (!ip.IsPrivate() && !ip.IsLoopback()) {
		t.Fatal("fixture must be on an isolated private or loopback address")
	}
	cfg := mysql.NewConfig()
	cfg.Net, cfg.Addr, cfg.DBName = "tcp", addr, "usage_logs_fixture"
	cfg.User, cfg.Passwd = "pilot_reader", "fixture-read-only"
	cfg.Timeout, cfg.ReadTimeout = 2*time.Second, 5*time.Second
	cfg.InterpolateParams = true
	db, err := sql.Open("mysql", cfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(4)
	db.SetMaxIdleConns(2)
	var fixtureRows int64
	if err := db.QueryRow("SELECT COUNT(*) FROM logs").Scan(&fixtureRows); err != nil || fixtureRows != 30000 {
		t.Fatalf("fixture is invalid: rows=%d err=%v", fixtureRows, err)
	}
	return db, cfg
}

// A lock makes the SELECT stay in MySQL until we cancel it. Keep the lock
// while verifying the statement is gone; releasing it would hide a leak.
func TestUsageLogsIsolatedMySQLCancellation(t *testing.T) {
	reader, cfg := openUsageLogsPilotFixture(t)
	bcfg := cfg.Clone()
	bcfg.User, bcfg.Passwd = "pilot_blocker", "fixture-table-lock"
	blocker, err := sql.Open("mysql", bcfg.FormatDSN())
	if err != nil {
		t.Fatal(err)
	}
	defer blocker.Close()
	fence, end := context.WithTimeout(context.Background(), 9*time.Second)
	defer end()
	lock, err := blocker.Conn(fence)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if _, err := lock.ExecContext(fence, "LOCK TABLES logs WRITE"); err != nil {
		t.Fatal(err)
	}
	defer func() { _, _ = lock.ExecContext(fence, "UNLOCK TABLES") }()
	m := &Monitor{prodDB: reader}
	m.cfg.ProdDSN = cfg.FormatDSN()
	ctx, cancel := context.WithCancel(fence)
	defer cancel()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/usage-logs/count", nil).WithContext(ctx)
	responded := make(chan int, 1)
	go func() {
		m.serveUsageLogsCount(c)
		responded <- c.Writer.Status()
	}()
	waitUsageLogsPilotStatements(fence, t, reader, 1)
	cancel()
	select {
	case status := <-responded:
		if status != statusClientClosedRequest {
			t.Fatalf("cancelled MySQL query returned HTTP %d", status)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("HTTP response waited for the blocked MySQL query")
	}
	waitUsageLogsPilotStatements(fence, t, reader, 0)
	if _, err := lock.ExecContext(fence, "UNLOCK TABLES"); err != nil {
		t.Fatal(err)
	}
	w = usageLogsGET(t, m, m.serveUsageLogsCount, "/usage-logs/count?channel=1", nil)
	var data struct {
		Success bool
		Data    struct{ Total int64 }
	}
	if json.Unmarshal(w.Body.Bytes(), &data) != nil || !data.Success || data.Data.Total != 15000 {
		t.Fatalf("query did not recover after cancellation: %s", w.Body.String())
	}
}

func waitUsageLogsPilotStatements(ctx context.Context, t *testing.T, db *sql.DB, want int) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		var n int
		err := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM information_schema.PROCESSLIST "+
			"WHERE ID <> CONNECTION_ID() AND INFO LIKE 'SELECT /*+ MAX_EXECUTION_TIME(18000) */ COUNT(*) FROM logs%'").Scan(&n)
		if err != nil {
			t.Fatal(err)
		}
		if n == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("MySQL SELECT count did not reach %d while the test table lock was held", want)
}

func usageLogsPilotResponse(t *testing.T, httpHandler http.Handler, session, route string) []byte {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, "/usage-logs/"+route, nil)
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: session})
	w := httptest.NewRecorder()
	httpHandler.ServeHTTP(w, req)
	dec := json.NewDecoder(bytes.NewReader(w.Body.Bytes()))
	dec.UseNumber()
	var result map[string]any
	if err := dec.Decode(&result); err != nil || w.Code != 200 || result["success"] != (route != "user/99") {
		t.Fatalf("%s returned status=%d, error=%v", route, w.Code, err)
	}
	canon, err := json.Marshal(result)
	if err != nil {
		t.Fatal(err)
	}
	return canon
}

func usageLogsPilotMySQLSelects(t *testing.T, db *sql.DB) int64 {
	t.Helper()
	var metric string
	var count int64
	if err := db.QueryRow("SHOW GLOBAL STATUS LIKE 'Com_select'").Scan(&metric, &count); err != nil || metric != "Com_select" {
		t.Fatalf("invalid MySQL status response: %v", err)
	}
	return count
}
