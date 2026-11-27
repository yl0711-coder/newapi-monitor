package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// Exercise the HTTP work unit and client-like assembly separately from the raw
// SQL batch. Empty gaps must not turn into short pages or false next-page links.
func TestUsageLogsDiagnosticPageCompleteTraversal(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	for i := 1; i <= 53; i++ {
		content := "normal"
		if i%7 == 0 || i == 53 {
			content = "match"
		}
		if _, err := db.Exec("INSERT INTO logs(id,created_at,type,content,other) VALUES(?,?,2,?,'{}')", i, 100+i/3, content); err != nil {
			t.Fatal(err)
		}
	}
	f := usageLogFilter{Start: 99, End: 120, Message: "match"}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	for _, cap := range []int{1, 3, 7, 100} {
		for _, max := range []int{1, 2, 8} {
			for _, size := range []int{1, 2, 4, 8, 20} {
				t.Run(fmt.Sprintf("cap%d-batches%d-page%d", cap, max, size), func(t *testing.T) {
					var cursor *usageLogCursor
					var got []int64
					for page := 1; page < 20; page++ {
						items, more := []usageLogItem{}, false
						for request := 0; request < 100; request++ {
							remaining := size - len(items)
							probe := remaining == 0
							if probe {
								remaining = 1
							}
							r, err := queryUsageLogsDiagnosticPage(context.Background(), db, f, w, cursor, page, remaining, cap, max, false)
							if err != nil {
								t.Fatal(err)
							}
							if r.Scanned > int64(cap*max) || len(r.Items) > remaining {
								t.Fatal("budget", r)
							}
							if probe && len(r.Items) > 0 {
								more = true
								break
							}
							items = append(items, r.Items...)
							if r.HasMore {
								more = true
								cursor = r.Next
								break
							}
							if !r.ScanPending {
								break
							}
							if r.ScanCursor == nil {
								t.Fatal("missing checkpoint")
							}
							cursor = r.ScanCursor
							if request == 99 {
								t.Fatal("did not terminate")
							}
						}
						if more && len(items) != size {
							t.Fatalf("short intermediate page: %d", len(items))
						}
						for _, it := range items {
							got = append(got, it.ID)
						}
						if !more {
							break
						}
					}
					want := []int64{53, 49, 42, 35, 28, 21, 14, 7}
					if !reflect.DeepEqual(got, want) {
						t.Fatalf("got %v want %v", got, want)
					}
				})
			}
		}
	}
}

type diagnosticFailQuerier struct {
	db            *sql.DB
	calls, failAt int
	cancel        context.CancelFunc
}

func (q *diagnosticFailQuerier) QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row {
	return q.db.QueryRowContext(ctx, query, args...)
}

func (q *diagnosticFailQuerier) QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error) {
	q.calls++
	if q.calls == q.failAt {
		if q.cancel != nil {
			q.cancel()
			return nil, ctx.Err()
		}
		return nil, errors.New("injected source failure")
	}
	return q.db.QueryContext(ctx, query, args...)
}

func TestUsageLogsDiagnosticPageFailureDoesNotPublishPartialSuccess(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	for i := 1; i <= 9; i++ {
		if _, err := db.Exec("INSERT INTO logs(id,created_at,type,content,other) VALUES(?,100,2,'normal','{}')", i); err != nil {
			t.Fatal(err)
		}
	}
	f := usageLogFilter{Start: 99, End: 101, Message: "missing"}
	w, _ := buildUsageLogWhere(f, usageLogWhereList)
	for _, cancelled := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		q := &diagnosticFailQuerier{db: db, failAt: 2}
		if cancelled {
			q.cancel = cancel
		}
		r, err := queryUsageLogsDiagnosticPage(ctx, q, f, w, nil, 1, 2, 3, 8, false)
		cancel()
		if err == nil || q.calls != 2 || r.HasMore || !r.TotalPending {
			t.Fatal("failure hidden", r, err, q.calls)
		}
		if cancelled && !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	}
	// An unpublished failed HTTP unit is retried from its original anchor.
	r, err := queryUsageLogsDiagnosticPage(context.Background(), db, f, w, nil, 1, 2, 3, 8, false)
	if err != nil || r.ScanPending || !r.TotalPending || r.Total != nil || r.Scanned != 9 {
		t.Fatal(r, err)
	}
}

func TestUsageLogsDiagnosticBatchCompleteTraversal(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	for i, r := range diagnosticFixture() {
		// Deliberately break ID/time monotonicity and create timestamp ties.
		if _, err := db.Exec("INSERT INTO logs(id,created_at,type,content,other,username,channel_id) VALUES(?,?,?,?,?,'fixture',7)", i+1, 100+(i*7)%5, r.typ, r.content, r.other); err != nil {
			t.Fatal(err)
		}
	}
	for _, f := range []usageLogFilter{
		{DiagnosticScope: "issues"}, {DiagnosticScope: "rejected"}, {Message: "no-match"}, {Message: "refusal"},
		{Message: "invalid value", Type: 5}, {Message: "50%_!"}, {Message: "refusal", Username: "fixture", Channel: 7},
		{DiagnosticScope: "rejected", Type: 2}, {Message: "refusal", Username: "absent"},
	} {
		f.Start, f.End = 99, 120
		w, err := buildUsageLogWhere(f, usageLogWhereList)
		if err != nil {
			t.Fatal(err)
		}
		rows, err := db.Query("SELECT id FROM logs"+w.sql()+" ORDER BY created_at DESC,id DESC", w.args...)
		if err != nil {
			t.Fatal(err)
		}
		want := []int64{}
		for rows.Next() {
			var id int64
			if err := rows.Scan(&id); err != nil {
				t.Fatal(err)
			}
			want = append(want, id)
		}
		rows.Close()
		for _, cap := range []int{1, 3, 5, 100} {
			for _, size := range []int{1, 2, 10} {
				got := []int64{}
				var cursor *usageLogCursor
				for step := 0; step < 40; step++ {
					r, err := queryUsageLogsDiagnosticBatch(context.Background(), db, f, w, cursor, 1, size, cap, false)
					if err != nil {
						t.Fatal(err)
					}
					if r.Scanned > int64(cap) || r.Scanned < 0 || len(r.Items) > size {
						t.Fatal("budget violated", r)
					}
					for _, it := range r.Items {
						got = append(got, it.ID)
					}
					var next *usageLogCursor
					if r.HasMore {
						next = r.Next
					} else if r.ScanPending {
						next = r.ScanCursor
					} else {
						break
					}
					if next == nil || r.Scanned == 0 || cursor != nil && (next.CreatedAt > cursor.CreatedAt || next.CreatedAt == cursor.CreatedAt && next.ID >= cursor.ID) {
						t.Fatal("cursor did not advance", cursor, r)
					}
					cursor = next
				}
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("%+v cap=%d size=%d got=%v want=%v", f, cap, size, got, want)
				}
			}
		}
	}
}

func TestUsageLogsDiagnosticBatchEmptyAndBoundaries(t *testing.T) {
	for _, n := range []int{0, 3, 4} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			db := newUsageLogsFakeProd(t)
			for i := 1; i <= n; i++ {
				if _, err := db.Exec("INSERT INTO logs(id,created_at,type,content,other) VALUES(?,100,2,'normal','{}')", i); err != nil {
					t.Fatal(err)
				}
			}
			f := usageLogFilter{Start: 99, End: 101, Message: "missing"}
			w, _ := buildUsageLogWhere(f, usageLogWhereList)
			r, err := queryUsageLogsDiagnosticBatch(context.Background(), db, f, w, nil, 1, 2, 3, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(r.Items) != 0 || r.HasMore || r.ScanPending != (n > 3) {
				t.Fatal(r)
			}
			if n <= 3 && (r.TotalPending || r.Total != 0) {
				t.Fatal("complete empty range was not known", r)
			}
			if n > 3 && (!r.TotalPending || r.Total != nil || r.ScanCursor == nil) {
				t.Fatal("partial empty range falsely complete", r)
			}
		})
	}
}

func TestUsageLogsDiagnosticBatchHTTPAndValidation(t *testing.T) {
	db := newUsageLogsFakeProd(t)
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	for i := 1; i <= 5001; i++ {
		if _, err := tx.Exec("INSERT INTO logs(id,created_at,type,content,other) VALUES(?,?,2,'normal','{}')", i, 100+i/2); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{prodDB: db}
	get := func(q url.Values) usageLogsDiagnosticBatch {
		t.Helper()
		q.Set("start_timestamp", "99")
		q.Set("end_timestamp", "3000")
		q.Set("message", "absent")
		q.Set("defer_total", "1")
		q.Set("bounded_search", "1")
		w := usageLogsGET(t, m, m.serveUsageLogsList, "/usage-logs/list?"+q.Encode(), nil)
		var reply struct {
			Success bool
			Data    usageLogsDiagnosticBatch
		}
		if json.Unmarshal(w.Body.Bytes(), &reply) != nil || !reply.Success {
			t.Fatal(w.Body.String())
		}
		return reply.Data
	}
	r := get(url.Values{})
	if !r.Bounded || r.ScanPending || r.HasMore || r.Scanned != 5001 || len(r.Items) != 0 || !r.TotalPending || r.Total != nil {
		t.Fatal(r)
	}
	for _, query := range []string{
		"bounded_search=1&defer_total=1",                                                    // not a diagnostic query
		"bounded_search=1&message=x&start_timestamp=99&end_timestamp=120",                   // no defer
		"bounded_search=1&defer_total=1&message=x&start_timestamp=99&end_timestamp=120&p=2", // no anchor
		"bounded_search=1&defer_total=1&message=x&start_timestamp=99&end_timestamp=120&before_created_at=121&before_id=1",
		"bounded_search=1&defer_total=1&message=x&start_timestamp=99&end_timestamp=120&request_id=r",
	} {
		missing := &Monitor{}
		w := usageLogsGET(t, missing, missing.serveUsageLogsList, "/usage-logs/list?"+query, nil)
		if !strings.Contains(w.Body.String(), `"success":false`) || strings.Contains(w.Body.String(), "未连接") {
			t.Fatal("invalid mode reached source", query, w.Body.String())
		}
	}
}
