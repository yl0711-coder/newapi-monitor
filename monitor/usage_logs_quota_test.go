package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"math"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"

	glebarezsqlite "github.com/glebarez/go-sqlite"
)

// SQLite 3.x in the existing test driver predates CONCAT. Only the synthetic
// MySQL adapter uses this deterministic test function; production SQL remains
// validated independently on MySQL. Match MySQL NULL propagation exactly.
func init() {
	glebarezsqlite.MustRegisterDeterministicScalarFunction("concat", -1, func(_ *glebarezsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
		var b strings.Builder
		for _, v := range args {
			if v == nil {
				return nil, nil
			}
			switch v := v.(type) {
			case []byte:
				b.Write(v)
			default:
				fmt.Fprint(&b, v)
			}
		}
		return b.String(), nil
	})
}

func usageTestSQLiteDriver(t *testing.T) driver.Driver {
	t.Helper()
	db, err := sql.Open("sqlite", ":memory:")
	if err != nil {
		t.Fatal(err)
	}
	d := db.Driver()
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}
	return d
}

func TestUsageLogsQuotaSpanOnlyBroadShortRange(t *testing.T) {
	cases := []struct {
		name string
		f    usageLogFilter
		want bool
	}{
		{"broad", usageLogFilter{Start: 100, End: 200}, true},
		{"ignored type and IDs", usageLogFilter{Start: 100, End: 200, Type: 5, RequestID: "r", UpstreamRequestID: "u"}, true},
		{"26h", usageLogFilter{Start: 100, End: 100 + 26*3600}, true},
		{"wide", usageLogFilter{Start: 100, End: 101 + 26*3600}, false},
		{"open", usageLogFilter{Start: 100}, false}, {"inverted", usageLogFilter{Start: 200, End: 100}, false},
		{"username", usageLogFilter{Start: 100, End: 200, Username: "u"}, false},
		{"user", usageLogFilter{Start: 100, End: 200, UserID: 31}, false},
		{"token", usageLogFilter{Start: 100, End: 200, TokenName: "t"}, false},
		{"model", usageLogFilter{Start: 100, End: 200, ModelName: "m"}, true},
		{"channel", usageLogFilter{Start: 100, End: 200, Channel: 7}, true},
		{"group", usageLogFilter{Start: 100, End: 200, Group: "g"}, true},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := usageLogsUseQuotaSpan(c.f); got != c.want {
				t.Fatalf("%v vs %v", got, c.want)
			}
		})
	}
}

// Execute the actual MySQL dispatch path while only translating FORCE INDEX
// for in-memory SQLite. This is not MySQL timing evidence.
type usageQuotaMySQLDriver struct {
	inner driver.Driver
	calls *usageQuotaCalls
}
type usageQuotaCalls struct {
	span, legacy, planning atomic.Int64
	probe                  atomic.Int64
	fail                   atomic.Bool
}

func (d *usageQuotaMySQLDriver) Open(name string) (driver.Conn, error) {
	c, e := d.inner.Open(name)
	if e != nil {
		return nil, e
	}
	return &usageQuotaMySQLConn{Conn: c, calls: d.calls}, nil
}

type usageQuotaMySQLConn struct {
	driver.Conn
	calls *usageQuotaCalls
}

func (c *usageQuotaMySQLConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "AS usage_aggregate_probe") {
		c.calls.probe.Add(1)
		if c.calls.fail.Load() {
			return nil, context.DeadlineExceeded
		}
	} else if strings.Contains(q, "'audit_plan_summary'") {
		c.calls.planning.Add(1)
		if c.calls.fail.Load() {
			return nil, context.DeadlineExceeded
		}
	} else if strings.Contains(q, "ELSE NULL END AS quota") {
		c.calls.span.Add(1)
		if c.calls.fail.Load() {
			return nil, context.DeadlineExceeded
		}
	} else if strings.Contains(q, "COALESCE(SUM(quota),0)") || strings.Contains(q, "COUNT(*) FROM logs") {
		c.calls.legacy.Add(1)
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, sqliteQuotaSpanSQL(q), args)
}

var usageQuotaDriverID atomic.Int64

func TestUsageLogsCountDispatchFallbackAndFailure(t *testing.T) {
	for _, name := range []string{"dense", "sparse", "empty", "timeout", "cancelled", "covering_native"} {
		t.Run(name, func(t *testing.T) {
			calls := &usageQuotaCalls{}
			driverName := fmt.Sprintf("count-mysql-simulation-%d", usageQuotaDriverID.Add(1))
			sql.Register(driverName, &usageQuotaMySQLDriver{inner: usageTestSQLiteDriver(t), calls: calls})
			db, err := sql.Open(driverName, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			if _, err := db.Exec("CREATE TABLE logs(id INTEGER PRIMARY KEY,created_at INTEGER,type INTEGER,username TEXT,channel_id INTEGER)"); err != nil {
				t.Fatal(err)
			}
			if name != "empty" {
				if _, err := db.Exec("INSERT INTO logs VALUES(1,100,2,'u',7),(2,150,5,'other',7),(3,200,2,'u',7)"); err != nil {
					t.Fatal(err)
				}
			}
			if name == "sparse" {
				if _, err := db.Exec("INSERT INTO logs VALUES(1000000,175,2,'u',7)"); err != nil {
					t.Fatal(err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if name == "timeout" {
				calls.fail.Store(true)
			}
			if name == "cancelled" {
				cancel()
			}
			f := usageLogFilter{Start: 100, End: 200, Username: "u", Channel: 7}
			if name == "covering_native" {
				f.Username, f.Channel = "", 0
			}
			w, _ := buildUsageLogWhere(f, usageLogWhereList)
			m := &Monitor{prodDB: db}
			got, err := m.queryUsageLogsCount(ctx, db, f, w)
			if name == "timeout" || name == "cancelled" {
				if (!errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)) || calls.legacy.Load() != 0 {
					t.Fatalf("failed COUNT must stop without fallback: %v calls=%d", err, calls.legacy.Load())
				}
				return
			}
			want, span, legacy := int64(2), int64(0), int64(0)
			switch name {
			case "empty":
				want = 0
			case "sparse":
				want, legacy = 3, 0
			case "covering_native":
				want, span, legacy = 3, 0, 1
			}
			if err != nil || got != want || calls.span.Load() != span || calls.legacy.Load() != legacy {
				t.Fatalf("count=%d want=%d span=%d legacy=%d err=%v", got, want, calls.span.Load(), calls.legacy.Load(), err)
			}
		})
	}
}

func TestUsageLogsQuotaDispatchFallbackAndFailure(t *testing.T) {
	for _, name := range []string{"dense", "sparse", "empty", "timeout", "cancelled"} {
		t.Run(name, func(t *testing.T) {
			calls := &usageQuotaCalls{}
			driverName := fmt.Sprintf("quota-mysql-simulation-%d", usageQuotaDriverID.Add(1))
			sql.Register(driverName, &usageQuotaMySQLDriver{inner: usageTestSQLiteDriver(t), calls: calls})
			db, e := sql.Open(driverName, ":memory:")
			if e != nil {
				t.Fatal(e)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			db.SetMaxIdleConns(1)
			if _, e := db.Exec("CREATE TABLE logs(id INTEGER PRIMARY KEY,created_at INTEGER,type INTEGER,quota INTEGER)"); e != nil {
				t.Fatal(e)
			}
			if name != "empty" {
				if _, e := db.Exec("INSERT INTO logs VALUES(1,100,2,10),(2,150,5,99),(3,200,2,30)"); e != nil {
					t.Fatal(e)
				}
			}
			if name == "sparse" {
				if _, e := db.Exec("INSERT INTO logs VALUES(1000000,175,2,4)"); e != nil {
					t.Fatal(e)
				}
			}
			if name == "timeout" {
				calls.fail.Store(true)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if name == "cancelled" {
				cancel()
			}
			m := &Monitor{prodDB: db}
			f := usageLogFilter{Start: 100, End: 200}
			where, _ := buildUsageLogWhere(f, usageLogWhereStat)
			got, e := m.queryUsageLogsQuota(ctx, db, f, where)
			if name == "timeout" || name == "cancelled" {
				if !errors.Is(e, context.DeadlineExceeded) && !errors.Is(e, context.Canceled) {
					t.Fatalf("missing query failure: %v", e)
				}
				if calls.legacy.Load() != 0 {
					t.Fatal("failed SUM was automatically repeated")
				}
				return
			}
			want := int64(40)
			if name == "empty" {
				want = 0
			}
			if name == "sparse" {
				want = 44
			}
			if e != nil || got != want || calls.span.Load() != 0 || calls.probe.Load() != 1 || calls.planning.Load() != 0 {
				t.Fatalf("got=%d want=%d err=%v span=%d", got, want, e, calls.span.Load())
			}
			legacy := int64(0)
			if calls.legacy.Load() != legacy {
				t.Fatal("unexpected legacy query/fallback count")
			}
		})
	}
}

// SQLite proves predicates/CASE behavior, not MySQL performance or plans.
func sqliteQuotaSpanSQL(s string) string {
	s = strings.NewReplacer(" FORCE INDEX (PRIMARY)", "", " FORCE INDEX (idx_created_at_type)", "").Replace(s)
	if strings.Contains(s, "'audit_plan_summary'") {
		s = "SELECT * FROM " + s
		s = strings.ReplaceAll(s, ") UNION ALL (", ") UNION ALL SELECT * FROM (")
	}
	return s
}

func TestUsageLogsQuotaSpanSingleStatementEqualsOriginal(t *testing.T) {
	for _, name := range []string{"dense", "empty", "nonmonotonic", "sparse", "over_cap", "negative_quota", "large_id"} {
		t.Run(name, func(t *testing.T) {
			db := newUsageLogsFakeProd(t)
			data := []usageLogRow{{id: 10, createdAt: 100, typ: 2, quota: 10}, {id: 11, createdAt: 150, typ: 5, quota: 900}, {id: 12, createdAt: 200, typ: 2, quota: 30}, {id: 13, createdAt: 210, typ: 2, quota: 700}}
			switch name {
			case "empty":
				data = nil
			case "nonmonotonic":
				data = append(data, usageLogRow{id: 9, createdAt: 175, typ: 2, quota: 4}, usageLogRow{id: 14, createdAt: 180, typ: 6, quota: 999})
				data[1].createdAt = 90
			case "sparse":
				data = append(data, usageLogRow{id: 1000000, createdAt: 175, typ: 2, quota: 4})
			case "negative_quota":
				data[0].quota = -10
			case "large_id":
				for i := range data {
					data[i].id += math.MaxInt64 - 100
				}
			}
			insertUsageLogs(t, db, data...)
			f := usageLogFilter{Start: 100, End: 200, Type: 5, RequestID: "ignored"}
			where, e := buildUsageLogWhere(f, usageLogWhereStat)
			if e != nil {
				t.Fatal(e)
			}
			original := append([]any(nil), where.args...)
			query, args := usageLogsQuotaSpanSQL(f, where)
			if name == "over_cap" {
				args[0] = int64(2)
			}
			var got sql.NullInt64
			var want int64
			if e := db.QueryRow(sqliteQuotaSpanSQL(query), args...).Scan(&got); e != nil {
				t.Fatal(e)
			}
			if e := db.QueryRow("SELECT COALESCE(SUM(quota),0) FROM logs"+where.sql(), where.args...).Scan(&want); e != nil {
				t.Fatal(e)
			}
			if name == "sparse" || name == "over_cap" {
				if got.Valid {
					t.Fatal("unsafe span must request fallback, not return zero")
				}
			} else if !got.Valid || got.Int64 != want {
				t.Fatalf("got=%+v want=%d", got, want)
			}
			if !reflect.DeepEqual(original, where.args) || strings.Contains(where.sql(), "BETWEEN") {
				t.Fatal("original where mutated")
			}
			if strings.Count(query, "MAX_EXECUTION_TIME") != 1 || !strings.Contains(query, "MAX_EXECUTION_TIME(18000)") {
				t.Fatal("top-level timeout missing")
			}
		})
	}
}

func TestUsageLogsQuotaStatementSnapshotSeesConcurrentInsertOnNextRead(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	if _, e := db.Exec("PRAGMA journal_mode=WAL"); e != nil {
		t.Fatal(e)
	}
	insertUsageLogs(t, db, usageLogRow{id: 1, createdAt: 150, typ: 2, quota: 10})
	tx, e := db.BeginTx(context.Background(), nil)
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = tx.Rollback() }()
	var snapshotCount int
	if e := tx.QueryRow("SELECT COUNT(*) FROM logs").Scan(&snapshotCount); e != nil {
		t.Fatal(e)
	}
	insertUsageLogs(t, db, usageLogRow{id: 2, createdAt: 150, typ: 2, quota: 20})
	f := usageLogFilter{Start: 100, End: 200}
	w, _ := buildUsageLogWhere(f, usageLogWhereStat)
	q, args := usageLogsQuotaSpanSQL(f, w)
	var got int64
	if e := tx.QueryRow(sqliteQuotaSpanSQL(q), args...).Scan(&got); e != nil {
		t.Fatal(e)
	}
	if got != 10 {
		t.Fatalf("snapshot changed: %d", got)
	}
	if e := tx.Rollback(); e != nil {
		t.Fatal(e)
	}
	if e := db.QueryRow(sqliteQuotaSpanSQL(q), args...).Scan(&got); e != nil || got != 30 {
		t.Fatalf("next fresh query missed consumption: %d %v", got, e)
	}
}
