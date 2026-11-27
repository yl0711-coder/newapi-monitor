package monitor

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
)

// Reuses only the same task-owned deterministic synthetic fixture. No copy or
// production source. Run after the diagnostic performance fixture has finished.
func TestUsageLogsDiagnosticBatchMySQLRetainedFixture(t *testing.T) {
	run := isolatedSQLOptimizationRunner(t)
	const db = "usage_diagnostics_perf"
	if run("USE "+db+";SELECT COUNT(*) FROM logs;") != "200000" {
		t.Fatal("requires the task's 200k synthetic fixture")
	}
	type result struct {
		Case, Kind string
		Samples    []diagnosticPerfSample
		Plan       json.RawMessage
	}
	var results []result
	for _, tc := range []struct {
		name string
		f    usageLogFilter
	}{
		{"missing", usageLogFilter{Message: "does-not-exist"}},
		{"rare-oldest", usageLogFilter{Message: "rare-diagnostic-marker"}},
		{"rejected", usageLogFilter{DiagnosticScope: "rejected"}},
		{"error-text", usageLogFilter{Type: 5, Message: "rate_limit_exceeded"}},
		{"rare-customer", usageLogFilter{Username: "rare-user", Message: "does-not-exist"}},
	} {
		f := tc.f
		f.Start, f.End = 100000, 186399
		w, err := buildUsageLogWhere(f, usageLogWhereList, true)
		if err != nil {
			t.Fatal(err)
		}
		old, a := usageLogsPageSQL(w, "logs", 1, 20, nil, true)
		for _, cap := range []int{0, 1000, 5000, 10000} {
			q, args := old, a
			kind := "original"
			if cap > 0 {
				q, args = usageLogsDiagnosticBatchSQL(f, w, nil, 20, cap, true)
				kind = fmt.Sprintf("batch-%d", cap)
			}
			query := usageLogsSyntheticBind(t, q, args)
			r := result{Case: tc.name, Kind: kind}
			r.Plan = json.RawMessage(run("USE " + db + ";EXPLAIN FORMAT=JSON " + query + ";"))
			for i := 0; i < 3; i++ {
				_, s := measureDiagnosticSQL(t, run, db, query)
				r.Samples = append(r.Samples, s)
				if cap > 0 && s.HandlerReads["Handler_read_prev"] > int64(cap+1) {
					t.Fatal("metadata budget exceeded", s)
				}
				if cap > 0 && s.HandlerReads["Handler_read_next"] > int64(5*(cap+1)) {
					t.Fatal("unexpected full-window metadata scan", s)
				}
			}
			results = append(results, r)
			t.Logf("%s %s elapsed_us=%d/%d/%d", tc.name, kind, r.Samples[0].ElapsedUS, r.Samples[1].ElapsedUS, r.Samples[2].ElapsedUS)
		}
	}
	type traversal struct {
		Case               string
		Batches, Rows      int
		Advanced, ServerUS int64
	}
	var traversals []traversal
	for _, tc := range []struct {
		name string
		f    usageLogFilter
		want int
	}{
		{"missing", usageLogFilter{Message: "does-not-exist"}, 0},
		{"rare-oldest", usageLogFilter{Message: "rare-diagnostic-marker"}, 1},
		{"rejected", usageLogFilter{DiagnosticScope: "rejected"}, 1000},
	} {
		f := tc.f
		f.Start, f.End = 100000, 186399
		w, _ := buildUsageLogWhere(f, usageLogWhereList, true)
		var cursor *usageLogCursor
		seen := map[int64]bool{}
		tr := traversal{Case: tc.name}
		for step := 0; step < 100; step++ {
			q, a := usageLogsDiagnosticBatchSQL(f, w, cursor, 20, 5000, true)
			raw, us := timedSyntheticSQL(t, run, db, usageLogsSyntheticBind(t, q, a))
			tr.ServerUS += us
			tr.Batches++
			var count, tailAt, tailID, matches, lastOrdinal int64
			var last *usageLogCursor
			for _, line := range strings.Split(raw, "\n") {
				v := strings.Split(line, "\t")
				if len(v) < 6 {
					t.Fatal("batch result shape")
				}
				integer := func(i int) int64 {
					n, e := strconv.ParseInt(v[i], 10, 64)
					if e != nil {
						t.Fatal(e)
					}
					return n
				}
				count, tailAt, tailID, matches, lastOrdinal = integer(0), integer(1), integer(2), integer(3), integer(4)
				if v[5] == "NULL" {
					continue
				}
				id, at := integer(5), integer(7)
				if seen[id] {
					t.Fatal("duplicate result across batches", id)
				}
				seen[id] = true
				last = &usageLogCursor{CreatedAt: at, ID: id}
			}
			var next *usageLogCursor
			if matches > 20 {
				next = last
				tr.Advanced += lastOrdinal
			} else {
				tr.Advanced += min(count, 5000)
				if count > 5000 {
					next = &usageLogCursor{CreatedAt: tailAt, ID: tailID}
				}
			}
			if next == nil {
				break
			}
			if cursor != nil && (next.CreatedAt > cursor.CreatedAt || next.CreatedAt == cursor.CreatedAt && next.ID >= cursor.ID) {
				t.Fatal("non-progressing checkpoint")
			}
			cursor = next
			if step == 99 {
				t.Fatal("did not finish")
			}
		}
		tr.Rows = len(seen)
		if tr.Rows != tc.want || tr.Advanced != 200000 {
			t.Fatal("traversal incomplete", tr)
		}
		traversals = append(traversals, tr)
	}
	sources := map[string]string{}
	for _, name := range []string{"usage_logs_diagnostic_batch.go", "newapi_usage_logs.go", "usage_logs_diagnostics.go"} {
		b, e := os.ReadFile(name)
		if e != nil {
			t.Fatal(e)
		}
		sources[name] = fmt.Sprintf("%x", sha256.Sum256(b))
	}
	sqlScaleWrite(t, "usage-log-batch-forced-time-index-20261127.json", map[string]any{"synthetic_only": true, "fixture_reused": true, "sources": sources, "results": results, "traversals": traversals})
}
