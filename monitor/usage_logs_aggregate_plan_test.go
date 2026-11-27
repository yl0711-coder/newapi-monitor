package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"testing"
)

// Expand synthetic keys into the same bounded summaries returned by MySQL;
// production never transfers individual planning keys to Go.
func usageLogsPlanningKeysDecision(f usageLogFilter, keys []usageLogPlanningKey) int {
	var summaries [8]usageLogPlanningSummary
	for _, k := range keys {
		if k.kind < 1 || k.kind > 7 || k.id < 0 {
			return usageLogPlanNative
		}
		s := &summaries[k.kind]
		if s.n == 0 || k.id < s.low {
			s.low = k.id
		}
		if s.n == 0 || k.id > s.high {
			s.high = k.id
		}
		s.kind = k.kind
		s.n++
		s.at = k.at
	}
	var out []usageLogPlanningSummary
	for _, s := range summaries {
		if s.n > 0 {
			out = append(out, s)
		}
	}
	return usageLogsSummaryDecision(f, out)
}

func TestUsageLogsPlanningDecisionBoundaries(t *testing.T) {
	f := usageLogFilter{Start: 100, End: 200, Username: "historical_name", Channel: 7}
	dense := func(selectors int) []usageLogPlanningKey {
		keys := []usageLogPlanningKey{{6, 1, 90}, {7, 1000, 210}}
		for kind := 1; kind <= 2; kind++ {
			for n := 0; n < selectors; n++ {
				keys = append(keys, usageLogPlanningKey{kind, int64(10 + n), 0})
			}
		}
		for n := 0; n < 64; n++ {
			keys = append(keys, usageLogPlanningKey{5, int64(100 + n), 100})
		}
		return keys
	}
	for _, tc := range []struct {
		name string
		edit func(*usageLogFilter, *[]usageLogPlanningKey)
		want int
	}{
		{"dense", nil, usageLogPlanSpan},
		{"256 is native", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { *k = dense(256) }, usageLogPlanNative},
		{"missing selector", func(_ *usageLogFilter, k *[]usageLogPlanningKey) {
			(*k)[2].kind = 2
			(*k)[3].kind = 2
			*k = append((*k)[:2], (*k)[259:]...)
		}, usageLogPlanNative},
		{"only 63 time keys", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { *k = (*k)[:len(*k)-1] }, usageLogPlanTime},
		{"sparse sample", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { (*k)[len(*k)-1].id = 10000 }, usageLogPlanTime},
		{"other log type", func(f *usageLogFilter, _ *[]usageLogPlanningKey) { f.Type = 5 }, usageLogPlanTime},
		{"whole source broad", func(f *usageLogFilter, k *[]usageLogPlanningKey) {
			f.Username, f.Channel = "", 0
			(*k)[0].at, (*k)[1].at = 100, 200
		}, usageLogPlanNative},
		{"whole source selective", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { (*k)[0].at, (*k)[1].at = 100, 200 }, usageLogPlanSpan},
		{"likely cap", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { (*k)[1] = usageLogPlanningKey{7, 1000100, 200} }, usageLogPlanNative},
		{"cap but tail outside", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { (*k)[1] = usageLogPlanningKey{7, 1000100, 201} }, usageLogPlanSpan},
		{"large signed IDs", func(_ *usageLogFilter, k *[]usageLogPlanningKey) {
			for n := range *k {
				(*k)[n].id += math.MaxInt64 - 2000
			}
		}, usageLogPlanSpan},
		{"negative ID", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { (*k)[3].id = -1 }, usageLogPlanNative},
		{"missing edges", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { *k = (*k)[2:] }, usageLogPlanNative},
		{"invalid kind", func(_ *usageLogFilter, k *[]usageLogPlanningKey) { (*k)[3].kind = 8 }, usageLogPlanNative},
		{"wildcard", func(f *usageLogFilter, _ *[]usageLogPlanningKey) { f.Username = "%name%" }, usageLogPlanNative},
	} {
		t.Run(tc.name, func(t *testing.T) {
			next, keys := f, dense(257)
			if tc.edit != nil {
				tc.edit(&next, &keys)
			}
			if got := usageLogsPlanningKeysDecision(next, keys); got != tc.want {
				t.Fatalf("got %d want %d", got, tc.want)
			}
		})
	}
}

// Use the real dispatcher with enough rows to exercise span and the late
// outlier fallback. SQLite only translates MySQL syntax; it is not plan proof.
func TestUsageLogsPlanningDispatchExactAndFallback(t *testing.T) {
	for _, name := range []string{"dense", "late_outlier", "sparse", "empty_time", "rare", "type_ignored_by_stat"} {
		t.Run(name, func(t *testing.T) {
			calls := &usageQuotaCalls{}
			driverName := fmt.Sprintf("planning-mysql-%d", usageQuotaDriverID.Add(1))
			sql.Register(driverName, &usageQuotaMySQLDriver{inner: usageTestSQLiteDriver(t), calls: calls})
			db, err := sql.Open(driverName, ":memory:")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			_, err = db.Exec(`CREATE TABLE logs(id INTEGER PRIMARY KEY,created_at INTEGER,type INTEGER,quota INTEGER,username TEXT,channel_id INTEGER);
WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n<600)
INSERT INTO logs SELECT n,100+n/5,CASE WHEN n%11=0 THEN 5 ELSE 2 END,CASE WHEN n%17=0 THEN NULL ELSE n%7-3 END,'historical_name',7 FROM seq;
INSERT INTO logs VALUES(0,90,2,100,'old',9),(2000000,300,2,100,'old',9);`)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "late_outlier":
				_, err = db.Exec("INSERT INTO logs VALUES(1000000,210,2,-900,'historical_name',7)")
			case "sparse":
				_, err = db.Exec("UPDATE logs SET created_at=CASE WHEN id%10=0 THEN 150 ELSE 90 END WHERE id>0 AND id<2000000")
			case "rare":
				_, err = db.Exec("UPDATE logs SET username='other' WHERE id>20")
			}
			if err != nil {
				t.Fatal(err)
			}
			f := usageLogFilter{Start: 100, End: 220, Username: "historical_name", Channel: 7}
			if name == "empty_time" {
				f.Start, f.End = 250, 260
			}
			if name == "type_ignored_by_stat" {
				f.Type = 5
			}
			m := &Monitor{prodDB: db}
			for _, op := range []usageLogWhereScope{usageLogWhereList, usageLogWhereStat} {
				calls.span.Store(0)
				calls.legacy.Store(0)
				calls.planning.Store(0)
				calls.probe.Store(0)
				current := f
				if op == usageLogWhereStat {
					current.Username = ""
				}
				w, err := buildUsageLogWhere(current, op)
				if err != nil {
					t.Fatal(err)
				}
				var want, got int64
				expr := "COUNT(*)"
				if op == usageLogWhereStat {
					expr = "COALESCE(SUM(quota),0)"
				}
				if err = db.QueryRow("SELECT "+expr+" FROM logs"+w.sql(), w.args...).Scan(&want); err != nil {
					t.Fatal(err)
				}
				calls.legacy.Store(0)
				if op == usageLogWhereStat {
					got, err = m.queryUsageLogsQuota(context.Background(), db, current, w)
				} else {
					got, err = m.queryUsageLogsCount(context.Background(), db, current, w)
				}
				if err != nil || got != want {
					t.Fatalf("op=%v got=%d want=%d err=%v", op, got, want, err)
				}
				if calls.probe.Load() != 1 || calls.planning.Load() != 0 {
					t.Fatal("planner not exercised")
				}
				if name == "dense" || name == "late_outlier" || name == "type_ignored_by_stat" && op == usageLogWhereStat {
					if calls.span.Load() != 1 {
						t.Fatal("expected actual span")
					}
					wantFallback := int64(0)
					if name == "late_outlier" {
						wantFallback = 1
					}
					if calls.legacy.Load() != wantFallback {
						t.Fatalf("fallback got %d want %d", calls.legacy.Load(), wantFallback)
					}
				}
				if db.Stats().InUse != 0 {
					t.Fatal("planning rows leaked connection")
				}
			}
		})
	}
}

// Inject malformed streams independently of SQLite to ensure no heavy query
// starts after failed metadata. Do not silently retry a failed source read.
type usagePlanningFaultMySQLDriver struct{ mode string }
type usagePlanningFaultConn struct{ mode string }
type usagePlanningFaultRows struct {
	mode string
	n    int
}

func (d usagePlanningFaultMySQLDriver) Open(string) (driver.Conn, error) {
	return usagePlanningFaultConn(d), nil
}
func (c usagePlanningFaultConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepare")
}
func (c usagePlanningFaultConn) Close() error { return nil }
func (c usagePlanningFaultConn) Begin() (driver.Tx, error) {
	return nil, errors.New("unexpected transaction")
}
func (c usagePlanningFaultConn) QueryContext(_ context.Context, q string, _ []driver.NamedValue) (driver.Rows, error) {
	if !strings.Contains(q, "AS usage_aggregate_probe") {
		return nil, errors.New("heavy query must not start")
	}
	return &usagePlanningFaultRows{mode: c.mode}, nil
}
func (r *usagePlanningFaultRows) Columns() []string {
	return []string{"probe"}
}
func (r *usagePlanningFaultRows) Close() error {
	if r.mode == "close" {
		return errors.New("injected close")
	}
	return nil
}
func (r *usagePlanningFaultRows) Next(v []driver.Value) error {
	r.n++
	if r.mode == "empty" {
		return io.EOF
	}
	if r.n > 1 && r.mode == "close" {
		return io.EOF
	}
	if r.n > 1 && r.mode == "cancel" {
		return context.Canceled
	}
	if r.n > 1 && r.mode == "midstream" {
		return errors.New("injected midstream")
	}
	if r.n > 1 && r.mode != "duplicate_summary" {
		return io.EOF
	}
	v[0] = "plan|64|1|64|1|90|1000|210"
	if r.mode == "null" {
		v[0] = nil
	}
	if r.mode == "oversize" {
		v[0] = strings.Repeat("x", 4097)
	}
	if r.mode == "marker" {
		v[0] = "wrong"
	}
	if r.mode == "scan" {
		v[0] = struct{}{}
	}
	if r.mode == "bad_kind" {
		v[0] = "unknown|0"
	}
	if r.mode == "bad_count" {
		v[0] = "plan|65|1|64|1|90|1000|210"
	}
	if r.mode == "negative_count" {
		v[0] = "plan|-1|1|64|1|90|1000|210"
	}
	if r.mode == "bad_edge" {
		v[0] = "plan|64|1|64|1|90|1000"
	}
	return nil
}
func TestUsageLogsPlanningStreamFailuresStop(t *testing.T) {
	for _, mode := range []string{"midstream", "close", "marker", "scan", "duplicate_summary", "cancel", "bad_kind", "bad_count", "negative_count", "bad_edge", "empty", "null", "oversize"} {
		t.Run(mode, func(t *testing.T) {
			name := fmt.Sprintf("fault-mysql-%d", usageQuotaDriverID.Add(1))
			sql.Register(name, usagePlanningFaultMySQLDriver{mode})
			db, err := sql.Open(name, "")
			if err != nil {
				t.Fatal(err)
			}
			defer db.Close()
			db.SetMaxOpenConns(1)
			m := &Monitor{prodDB: db}
			f := usageLogFilter{Start: 100, End: 200, Username: "u", Channel: 7}
			w, _ := buildUsageLogWhere(f, usageLogWhereList)
			_, err = m.queryUsageLogsCount(context.Background(), db, f, w)
			if err == nil || strings.Contains(err.Error(), "heavy query must not start") {
				t.Fatalf("unsafe error %v", err)
			}
			if db.Stats().InUse != 0 {
				t.Fatal("connection leaked")
			}
		})
	}
}
