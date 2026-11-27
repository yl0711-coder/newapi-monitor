package monthlysource

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"database/sql/driver"
	"encoding/hex"
	"strings"
	"testing"
	"time"
)

// Local diagnostic: forward to the real SQLite driver, counting only source
// SELECT invocations and delivered byte/string values. No source connection,
// business rules, schema or runtime instrumentation is changed.
type sqlAuditReadCounter struct {
	queries, bytes, heads, batchedHeads int
	rewriteRows                         func(string, driver.Rows) (driver.Rows, error)
	observeQuery                        func(context.Context, driver.Conn, string, []driver.NamedValue) error
}
type sqlAuditCounterConnector struct {
	base  driver.Driver
	count *sqlAuditReadCounter
}

func (c sqlAuditCounterConnector) Driver() driver.Driver { return c.base }

func (c sqlAuditCounterConnector) Connect(context.Context) (driver.Conn, error) {
	conn, err := c.base.Open(":memory:")
	if err != nil {
		return nil, err
	}
	return &sqlAuditCountingConn{Conn: conn, count: c.count}, nil
}

type sqlAuditCountingConn struct {
	driver.Conn
	count *sqlAuditReadCounter
}

func (c *sqlAuditCountingConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}
func (c *sqlAuditCountingConn) ExecContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, args)
}
func (c *sqlAuditCountingConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if c.count.observeQuery != nil {
		if err := c.count.observeQuery(ctx, c.Conn, q, args); err != nil {
			return nil, err
		}
	}
	c.count.queries++
	if strings.Contains(q, "FROM portal_monthly_funding_pages WHERE scan_id=?") {
		c.count.heads++
	}
	if strings.Contains(q, "WITH requested(ordinal,scan_id)") {
		c.count.batchedHeads++
	}
	rows, err := c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
	if err != nil {
		return nil, err
	}
	if c.count.rewriteRows != nil {
		rows, err = c.count.rewriteRows(q, rows)
		if err != nil {
			return nil, err
		}
	}
	return &sqlAuditCountingRows{Rows: rows, count: c.count}, nil
}

type sqlAuditCountingRows struct {
	driver.Rows
	count *sqlAuditReadCounter
}

func (r *sqlAuditCountingRows) Next(values []driver.Value) error {
	if err := r.Rows.Next(values); err != nil {
		return err
	}
	for _, v := range values {
		switch v := v.(type) {
		case string:
			r.count.bytes += len(v)
		case []byte:
			r.count.bytes += len(v)
		}
	}
	return nil
}

func TestSQLAuditFundingMonthInspectionWork(t *testing.T) {
	base, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	count := &sqlAuditReadCounter{}
	db := sql.OpenDB(sqlAuditCounterConnector{base: base.Driver(), count: count})
	_ = base.Close()
	db.SetMaxOpenConns(1)
	defer db.Close()
	if err := InitFundingPageStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	if err := InitFundingMonthCaptureStore(t.Context(), db); err != nil {
		t.Fatal(err)
	}
	memberSum := sha256.Sum256([]byte("201,202,"))
	scope := OrganizationMonthScope{Version: 1, Mode: OrganizationMonthScopeMode, GroupID: 1, AuthVersion: 1,
		MemberFingerprint: hex.EncodeToString(memberSum[:16]), MemberManifest: strings.Repeat("c", 64),
		Funding: FundingMonthRequest{Month: "2026-10", SourceIdentity: strings.Repeat("a", 64), Rules: fundingTestRules(), UserIDs: []int64{201, 202}}}
	key := digest([]byte("sql-audit-inspection-only"))
	plan, err := EnsureFundingMonthCapture(t.Context(), db, key, scope)
	if err != nil || len(plan.Scans) != 62 {
		t.Fatal("October diagnostic plan", err)
	}
	started := time.Now().Add(-time.Hour).UnixMilli()
	saved := 0
	for _, completed := range []int{0, 31, 62} {
		for saved < completed {
			p := FundingPage{Version: 1, Scan: plan.Scans[saved], EndReached: true, Rows: []FundingLog{},
				StartedAtMS: started + int64(saved*1000), CompletedAtMS: started + int64(saved*1000) + 1}
			if _, err := SaveFundingPage(t.Context(), db, p); err != nil {
				t.Fatal(err)
			}
			saved++
		}
		*count = sqlAuditReadCounter{}
		p, err := legacyFundingMonthInspect(t.Context(), db, key, scope, time.Now())
		if err != nil || p.FinishedScans != completed || count.queries != 63 || count.heads != 62 {
			t.Fatalf("unexpected actual inspection work: completed=%d progress=%+v counters=%+v err=%v", completed, p, count, err)
		}
		oldBytes := count.bytes
		*count = sqlAuditReadCounter{}
		got, err := InspectFundingMonthCapture(t.Context(), db, key, scope)
		if err != nil || got != p || count.queries != 2 || count.batchedHeads != 1 || count.bytes != oldBytes {
			t.Fatalf("batch changed progress/bytes or did not remove repeated calls: progress=%+v counters=%+v err=%v", got, count, err)
		}
		t.Logf("October retained heads=%d: old SQL=63, batch SQL=%d, same delivered text/blob bytes=%d; synthetic empty pages, not production latency", completed, count.queries, count.bytes)
	}
}
