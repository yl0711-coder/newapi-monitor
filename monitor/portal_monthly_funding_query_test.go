package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	sqlite "github.com/glebarez/go-sqlite"
)

// Independent pre-change SQL, not a baseline rebuilt from the new constants.
const oldSimpleMemberFundingSQL = "SELECT /*+ MAX_EXECUTION_TIME(15000) */ id, user_id, created_at, type, COALESCE(content,''), COALESCE(other,'') FROM logs FORCE INDEX (idx_user_created_type) WHERE user_id = ? AND created_at >= ? AND type IN (1,3,4) ORDER BY id"
const oldSimpleAdminFundingSQL = "SELECT /*+ MAX_EXECUTION_TIME(15000) */ id, user_id, created_at, type, COALESCE(content,''), COALESCE(other,'') FROM logs FORCE INDEX (idx_created_at_type) WHERE created_at >= ? AND created_at < ? AND type = 3 ORDER BY id"

func TestSimpleFundingQueriesOnlyRemoveRedundantSort(t *testing.T) {
	for _, pair := range [][2]string{{oldSimpleMemberFundingSQL, simpleMemberFundingSQL}, {oldSimpleAdminFundingSQL, simpleAdminFundingSQL}} {
		if pair[1] != strings.TrimSuffix(pair[0], " ORDER BY id") || strings.Contains(pair[1], "LIMIT") {
			t.Fatal("funding SQL changed more than its redundant sort")
		}
	}
}

// Translate FORCE INDEX only for this in-memory SQLite result test. MySQL
// optimizer behavior is verified separately, never inferred from SQLite.
type simpleFundingAuditDriver struct {
	old, fail bool
	funding   *atomic.Int64
}
type simpleFundingAuditConn struct {
	driver.Conn
	d *simpleFundingAuditDriver
}

var simpleFundingAuditID atomic.Int64

func (d *simpleFundingAuditDriver) Open(name string) (driver.Conn, error) {
	c, e := (&sqlite.Driver{}).Open(name)
	if e != nil {
		return nil, e
	}
	return &simpleFundingAuditConn{c, d}, nil
}
func (c *simpleFundingAuditConn) BeginTx(ctx context.Context, o driver.TxOptions) (driver.Tx, error) {
	return c.Conn.(driver.ConnBeginTx).BeginTx(ctx, o)
}
func (c *simpleFundingAuditConn) ExecContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Result, error) {
	return c.Conn.(driver.ExecerContext).ExecContext(ctx, q, a)
}
func (c *simpleFundingAuditConn) QueryContext(ctx context.Context, q string, a []driver.NamedValue) (driver.Rows, error) {
	if q == simpleMemberFundingSQL || q == simpleAdminFundingSQL {
		c.d.funding.Add(1)
		if c.d.fail {
			return nil, errors.New("synthetic funding source failure")
		}
		if c.d.old {
			if q == simpleMemberFundingSQL {
				q = oldSimpleMemberFundingSQL
			} else {
				q = oldSimpleAdminFundingSQL
			}
		}
	}
	q = strings.NewReplacer(" FORCE INDEX (idx_user_created_type)", "", " FORCE INDEX (idx_created_at_type)", "").Replace(q)
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, a)
}

func newSimpleFundingAuditDB(t *testing.T, old, fail bool) (*sql.DB, *atomic.Int64) {
	t.Helper()
	calls := &atomic.Int64{}
	name := fmt.Sprintf("simple-funding-audit-%d", simpleFundingAuditID.Add(1))
	sql.Register(name, &simpleFundingAuditDriver{old, fail, calls})
	db, e := sql.Open(name, ":memory:")
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = db.Close() })
	db.SetMaxOpenConns(1)
	for _, q := range []string{
		"PRAGMA reverse_unordered_selects=ON",
		"CREATE TABLE users(id INTEGER PRIMARY KEY,quota INTEGER,created_at INTEGER)",
		"CREATE TABLE logs(id INTEGER PRIMARY KEY,user_id INTEGER,created_at INTEGER,type INTEGER,quota INTEGER,content TEXT,other TEXT)",
		"CREATE INDEX idx_user_created_type ON logs(user_id,created_at,type)",
		"CREATE INDEX idx_created_at_type ON logs(created_at,type)",
		"CREATE TABLE user_subscriptions(id INTEGER,user_id INTEGER,plan_id INTEGER,start_time INTEGER,end_time INTEGER,status TEXT,source TEXT)",
	} {
		if _, e = db.Exec(q); e != nil {
			t.Fatal(e)
		}
	}
	return db, calls
}

func TestSimpleFundingSnapshotUnorderedRowsKeepMoneyAndIdentity(t *testing.T) {
	from := simpleWeekStart(time.Now().Unix()) - 2*simpleWeek
	tail := from + simpleWeek
	var snapshots []simpleSnapshot
	for _, old := range []bool{true, false} {
		db, calls := newSimpleFundingAuditDB(t, old, false)
		for _, uid := range []int64{201, 202} {
			if _, e := db.Exec("INSERT INTO users VALUES(?,?,?)", uid, 8000000, from); e != nil {
				t.Fatal(e)
			}
		}
		// Deliberately non-monotonic time/ID order; type=3 under an operator,
		// duplicates across member/admin reads, outside members, unknown rows,
		// before-start rows and consumption/refunds all retain their old meaning.
		rows := []struct {
			id, uid, at, typ, q int64
			content, other      string
		}{
			{90, 201, from + 50, 1, 0, "使用在线充值成功，充值金额: ＄2.000000 额度，支付金额：2.000000", "{}"},
			{10, 201, from + 100, 4, 0, "New user sign-up bonus ＄1.000000", `{"content_parts":[{"key":"New user sign-up bonus {{quota}}","params":{"quota":"＄1.000000"}}]}`},
			{80, 1, from + 200, 3, 0, "Increased user quota by ＄3.000000 额度", `{"admin_info":{"admin_id":1},"op":{"action":"user.quota_add","params":{"target_user_id":202,"quota":"＄3.000000 额度"}}}`},
			{30, 201, from + 250, 3, 0, "管理员增加用户额度 ＄4.000000 额度", "{}"},
			{20, 201, from + 300, 1, 0, "unknown funding", "{}"},
			{70, 999, from + 400, 1, 0, "管理员增加用户额度 ＄9.000000 额度", "{}"},
			{1, 201, from - 1, 1, 0, "管理员增加用户额度 ＄9.000000 额度", "{}"},
			{40, 201, tail, 2, 200, "", "{}"}, {50, 201, tail + 1, 6, 30, "", "{}"},
		}
		for _, r := range rows {
			if _, e := db.Exec("INSERT INTO logs VALUES(?,?,?,?,?,?,?)", r.id, r.uid, r.at, r.typ, r.q, r.content, r.other); e != nil {
				t.Fatal(e)
			}
		}
		// Prove that the fixture really changes source traversal order; a pair
		// which happened to return the same order would not test the collector.
		read, e := db.Query(simpleMemberFundingSQL, int64(201), from)
		if e != nil {
			t.Fatal(e)
		}
		var sourceIDs []int64
		for read.Next() {
			var id, uid, at, typ int64
			var content, other string
			if e = read.Scan(&id, &uid, &at, &typ, &content, &other); e != nil {
				t.Fatal(e)
			}
			sourceIDs = append(sourceIDs, id)
		}
		e = read.Err()
		_ = read.Close()
		if e != nil {
			t.Fatal(e)
		}
		ascending := []int64{10, 20, 30, 90}
		if reflect.DeepEqual(sourceIDs, ascending) != old {
			t.Fatalf("fixture did not vary source order: old=%t ids=%v", old, sourceIDs)
		}
		s := newSimpleReplicaSource(db)
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		got, e := s.Snapshot(ctx, []int64{201, 202}, from, tail)
		cancel()
		if e != nil {
			t.Fatal(e)
		}
		if calls.Load() < 3 {
			t.Fatal("actual source funding reads were not exercised")
		}
		if len(got.Events) != 4 || len(got.Review) != 1 || got.Review[0] != 20 || got.TailNet[201] != 170 {
			t.Fatalf("unexpected monetary fixture: events=%v review=%v tail=%v", got.Events, got.Review, got.TailNet)
		}
		for i := 1; i < len(got.Events); i++ {
			if got.Events[i-1].ID >= got.Events[i].ID {
				t.Fatal("canonical event order or deduplication changed")
			}
		}
		found := false
		for _, e := range got.Events {
			if e.ID == 80 {
				found = e.UserID == 202 && e.Delta == 1500000
			}
		}
		if !found {
			t.Fatal("operator-owned adjustment lost its true beneficiary")
		}
		var total int64
		for _, event := range got.Events {
			total += event.Delta
		}
		if total != 5000000 || got.BalanceAtTail(201) != 8000170 || got.BalanceAtTail(202) != 8000000 {
			t.Fatal("money or implied boundary balance changed")
		}
		got.CapturedAt = 0
		snapshots = append(snapshots, got)
		// Reuse closed-week rows through the same collector; output must still
		// match a cold read, without doubling any member/admin overlap.
		ctx, cancel = context.WithTimeout(context.Background(), 3*time.Second)
		cached, e := s.Snapshot(ctx, []int64{201, 202}, from, tail)
		cancel()
		if e != nil {
			t.Fatal(e)
		}
		cached.CapturedAt = 0
		if !reflect.DeepEqual(got, cached) {
			t.Fatal("warm cache changed snapshot values")
		}
	}
	if !reflect.DeepEqual(snapshots[0], snapshots[1]) {
		t.Fatalf("removing source sort changed final snapshot: old=%+v new=%+v", snapshots[0], snapshots[1])
	}
}

func TestSimpleFundingSnapshotFailureDoesNotReturnSuccess(t *testing.T) {
	db, _ := newSimpleFundingAuditDB(t, false, true)
	from := simpleWeekStart(time.Now().Unix()) - simpleWeek
	if _, e := db.Exec("INSERT INTO users VALUES(201,8000000,?)", from); e != nil {
		t.Fatal(e)
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, e := newSimpleReplicaSource(db).Snapshot(ctx, []int64{201}, from, from+3600); e == nil {
		t.Fatal("failed funding source became a successful zero snapshot")
	}
	var count int
	if e := db.QueryRowContext(ctx, "SELECT COUNT(*) FROM users").Scan(&count); e != nil || count != 1 {
		t.Fatalf("failed snapshot leaked its only connection: %v", e)
	}
}
