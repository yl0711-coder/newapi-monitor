package monitor

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestCustomerHealthProjectionFixedWindow(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	source := newFakeProdDB(t)
	var id int
	var name, path string
	if err := source.QueryRow("PRAGMA database_list").Scan(&id, &name, &path); err != nil {
		t.Fatal(err)
	}
	driverName := fmt.Sprintf("health-projection-%d", usageCountingDriverID.Add(1))
	sql.Register(driverName, modelFRTSQLDriver{source.Driver()})
	db, err := sql.Open(driverName, path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	m.prodDB = db
	// Includes multiple dimensions, failures, refunds, channel-test exclusion,
	// nonstream and invalid FRT, and values above float64 exact integer range.
	for i, other := range []string{`{"frt":100}`, `{"frt":501}`, `{"frt":1001}`, `{"frt":2001}`, `{"frt":5001}`, `{"frt":10001}`, `{"frt":"9007199254740993"}`, `{"frt":null}`, `not-json`} {
		typ := 2
		if i == 7 {
			typ = 5
		}
		if i == 8 {
			typ = 6
		}
		_, err := db.Exec("INSERT INTO logs(id,user_id,username,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`group`,is_stream,other) VALUES(?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
			i+1, 1+i%2, fmt.Sprintf("fixture-%d", i%2), 1+i%3, end-60+int64(i), typ, fmt.Sprintf("m%d", i%2), 11+i, 20+i, i%3, 1, fmt.Sprintf("g%d", i%2), 1, other)
		if err != nil {
			t.Fatal(err)
		}
	}
	read := func(q string) ([]string, []map[string]string) {
		t.Helper()
		start := time.Now()
		r, err := db.Query(q+" ORDER BY bucket,channel_id,model_name,grp,user_id", end-60, end)
		if err != nil {
			t.Fatal(err)
		}
		defer r.Close()
		cols, _ := r.Columns()
		out := []map[string]string{}
		for r.Next() {
			values := make([]sql.NullString, len(cols))
			args := make([]any, len(cols))
			for i := range values {
				args[i] = &values[i]
			}
			if err := r.Scan(args...); err != nil {
				t.Fatal(err)
			}
			row := map[string]string{}
			for i, c := range cols {
				row[c] = values[i].String
			}
			out = append(out, row)
		}
		if err := r.Err(); err != nil {
			t.Fatal(err)
		}
		t.Logf("columns=%d groups=%d elapsed=%s", len(cols), len(out), time.Since(start))
		return cols, out
	}
	oldCols, old := read(sampleWindowUserSQL())
	cols, got := read(sampleCustomerHealthWindowSQL())
	if len(oldCols) != 43 || len(cols) != 24 || len(old) != len(got) {
		t.Fatal(len(oldCols), len(cols), len(old), len(got))
	}
	for i, row := range got {
		for c, v := range row {
			if old[i][c] != v {
				t.Fatalf("group %d column %s: %s != %s", i, c, v, old[i][c])
			}
		}
	}
	if strings.Contains(sampleCustomerHealthWindowSQL(), "AS completion_tokens") || !strings.Contains(sampleCustomerHealthWindowSQL(), "prompt_tokens+completion_tokens") {
		t.Fatal("completion token dependency changed")
	}
	if _, err := m.sampleRangeWithPriorityOptions(context.Background(), end-60, end, false, true, true); err != nil {
		t.Fatal(err)
	}
	var before, after []CapacityUserMinuteSample
	if err := m.storeDB.Order("bucket_ts,user_id,channel_id,model_name,grp").Find(&before).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.sampleCustomerHealthRangeLow(context.Background(), end-60, end); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Order("bucket_ts,user_id,channel_id,model_name,grp").Find(&after).Error; err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, after) {
		t.Fatalf("retained metrics or FRT metadata changed: before=%+v after=%+v", before, after)
	}
	for _, r := range after {
		if r.FRTSumVersion != frtSumSemanticsVersion || r.FRTSumObserved != r.TtftObserved {
			t.Fatal(r)
		}
	}
	if dir := os.Getenv("CUSTOMER_HEALTH_AUDIT_SQL_DIR"); dir != "" {
		for file, q := range map[string]string{"customer-health-43.sql": sampleWindowUserSQL(), "customer-health-24.sql": sampleCustomerHealthWindowSQL()} {
			if err := os.WriteFile(filepath.Join(dir, file), []byte(q), 0600); err != nil {
				t.Fatal(err)
			}
		}
	}
}
