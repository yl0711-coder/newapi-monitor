package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"
)

func TestSourceAggregateSlicesPlan(t *testing.T) {
	h := int64(1794787200) // an hour boundary
	cases := []struct {
		name     string
		from, to int64
		want     [][2]int64
	}{
		{"realtime lookback stays one statement", h, h + 240, [][2]int64{{h, h + 240}}},
		{"finalize slice stays one statement", h, h + 600, [][2]int64{{h, h + 600}}},
		{"customer-health window stays one statement", h, h + 1260, [][2]int64{{h, h + 1260}}},
		{"threshold is inclusive", h, h + sourceAggregateMaxWindowSec, [][2]int64{{h, h + sourceAggregateMaxWindowSec}}},
		{"unaligned start is never split", h + 7, h + 3607, [][2]int64{{h + 7, h + 3607}}},
		{"hour becomes six slices", h, h + 3600, [][2]int64{{h, h + 600}, {h + 600, h + 1200}, {h + 1200, h + 1800}, {h + 1800, h + 2400}, {h + 2400, h + 3000}, {h + 3000, h + 3600}}},
		{"ragged tail keeps the exact end", h, h + 1860, [][2]int64{{h, h + 600}, {h + 600, h + 1200}, {h + 1200, h + 1800}, {h + 1800, h + 1860}}},
	}
	for _, c := range cases {
		if got := sourceAggregateSlices(c.from, c.to); !reflect.DeepEqual(got, c.want) {
			t.Fatalf("%s: got %v want %v", c.name, got, c.want)
		}
		got := sourceAggregateSlices(c.from, c.to)
		for i, w := range got {
			if w[0]%60 != 0 && len(got) > 1 {
				t.Fatalf("%s: slice %d starts off a minute boundary", c.name, i)
			}
			if i > 0 && got[i-1][1] != w[0] {
				t.Fatalf("%s: slices must be contiguous", c.name)
			}
		}
	}
}

// An hour aggregated in ten-minute slices must leave the local store in the
// same state as the single statement it replaces.
func TestSlicedSourceWindowMatchesSingleStatement(t *testing.T) {
	for _, governance := range []bool{false, true} {
		t.Run(fmt.Sprintf("governance=%v", governance), func(t *testing.T) { slicedMatchesSingle(t, governance) })
	}
}

func slicedMatchesSingle(t *testing.T, governance bool) {
	sliced, end := modelFRTFixture(t, false)
	single, _ := modelFRTFixture(t, false)
	sliced.cfg.GroupGovernanceEnabled, single.cfg.GroupGovernanceEnabled = governance, governance
	source := modelFRTSQLiteSource(t)
	hour := end - end%3600 - 3600
	insertParseOnceFuzzLogs(t, source, hour+3600, 3600, sampleWindowAggregateSQL(true, false))
	sliced.prodDB, single.prodDB = source, source

	ctx := context.Background()
	n1, err := sliced.sampleRangeWithPriorityOptions(ctx, hour, hour+3600, true, true, true)
	if err != nil {
		t.Fatal(err)
	}
	n2, err := single.sampleRangeSlice(ctx, hour, hour+3600, true, true, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if n1 != n2 || n1 < 50 {
		t.Fatalf("returned rows: sliced=%d single=%d", n1, n2)
	}
	models := []any{&MetricSample{}, &CapacityUserMinuteSample{}, &GroupGovernanceMinuteCoverage{}}
	if governance {
		models = append(models, &GroupGovernanceErrorMinute{})
	}
	for _, model := range models {
		a, b := storeSnapshot(t, sliced.storeDB, model), storeSnapshot(t, single.storeDB, model)
		if len(a) == 0 {
			t.Fatalf("%T: nothing written", model)
		}
		if strings.Join(a, "\n") != strings.Join(b, "\n") {
			t.Fatalf("%T differs: sliced %d rows vs single %d rows\nfirst sliced: %s\nfirst single: %s", model, len(a), len(b), safeIndex(a, 0), safeIndex(b, 0))
		}
	}
}

// A failed slice must fail the whole window without publishing the
// range-level coverage proof, like a failed single statement.
func TestSlicedSourceWindowFailureDoesNotPublishCoverage(t *testing.T) {
	m, end := modelFRTFixture(t, false)
	base := modelFRTSQLiteSource(t)
	hour := end - end%3600 - 3600
	insertParseOnceFuzzLogs(t, base, hour+3600, 3600, sampleWindowAggregateSQL(true, false))
	var path string
	var id int
	var name string
	if err := base.QueryRow("PRAGMA database_list").Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	var aggregates atomic.Int32
	driverName := fmt.Sprintf("slice-fail-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, failingAggregateDriver{inner: modelFRTSQLDriver{base.Driver()}, failOn: 3, seen: &aggregates})
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { db.Close() })
	m.prodDB = db
	if err := m.storeDB.Where("1=1").Delete(&GroupGovernanceMinuteCoverage{}).Error; err != nil {
		t.Fatal(err)
	}

	n, err := m.sampleRangeWithPriorityOptions(context.Background(), hour, hour+3600, true, true, true)
	if err == nil || n != 0 {
		t.Fatalf("sliced window with a failing slice = (%d, %v), want (0, error)", n, err)
	}
	if got := aggregates.Load(); got != 3 {
		t.Fatalf("aggregation statements issued = %d, want stop at the failing third slice", got)
	}
	var proofs int64
	if err := m.storeDB.Model(&GroupGovernanceMinuteCoverage{}).Count(&proofs).Error; err != nil {
		t.Fatal(err)
	}
	if proofs != 0 {
		t.Fatal("coverage proof must not be published when a slice failed")
	}
}

type failingAggregateDriver struct {
	inner  driver.Driver
	failOn int32
	seen   *atomic.Int32
}
type failingAggregateConn struct {
	driver.Conn
	d failingAggregateDriver
}

func (d failingAggregateDriver) Open(name string) (driver.Conn, error) {
	c, err := d.inner.Open(name)
	if err != nil {
		return nil, err
	}
	return failingAggregateConn{c, d}, nil
}

func (c failingAggregateConn) QueryContext(ctx context.Context, q string, args []driver.NamedValue) (driver.Rows, error) {
	if strings.Contains(q, "AS bucket") && c.d.seen.Add(1) == c.d.failOn {
		return nil, errors.New("injected slice failure")
	}
	return c.Conn.(driver.QueryerContext).QueryContext(ctx, q, args)
}

// storeSnapshot renders every row of a table without volatile timestamps.
func storeSnapshot(t *testing.T, db *gorm.DB, model any) []string {
	t.Helper()
	stmt := &gorm.Statement{DB: db}
	if err := stmt.Parse(model); err != nil {
		t.Fatal(err)
	}
	rows, err := db.Raw("SELECT * FROM " + stmt.Schema.Table).Rows()
	if err != nil {
		t.Fatal(err)
	}
	defer rows.Close()
	cols, _ := rows.Columns()
	var out []string
	for rows.Next() {
		vals := make([]sql.NullString, len(cols))
		ptrs := make([]any, len(cols))
		for i := range vals {
			ptrs[i] = &vals[i]
		}
		if err := rows.Scan(ptrs...); err != nil {
			t.Fatal(err)
		}
		var parts []string
		for i, c := range cols {
			if c == "id" || strings.HasSuffix(c, "updated_at") || strings.HasSuffix(c, "captured_at") || strings.HasSuffix(c, "created_at") || strings.HasSuffix(c, "sampled_at") {
				continue
			}
			parts = append(parts, c+"="+vals[i].String)
		}
		out = append(out, strings.Join(parts, "|"))
	}
	sort.Strings(out)
	return out
}
