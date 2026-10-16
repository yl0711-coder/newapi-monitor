package monitor

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
	"time"
)

// Local-only capacity acceptance. This is indexed SQLite with synthetic rows,
// NOT a production MySQL benchmark or evidence about a particular customer.
// Capture the response to disk, not httptest.ResponseRecorder's in-memory body.
type portalCapacityWriter struct {
	header http.Header
	status int
	file   *os.File
	bytes  int64
}

func (w *portalCapacityWriter) Header() http.Header  { return w.header }
func (w *portalCapacityWriter) WriteHeader(code int) { w.status = code }
func (w *portalCapacityWriter) Write(p []byte) (int, error) {
	if w.status == 0 {
		w.status = 200
	}
	n, err := w.file.Write(p)
	w.bytes += int64(n)
	return n, err
}

func TestPortalExportLocalCapacity(t *testing.T) {
	if os.Getenv("USAGE_EXPORT_CAPACITY") != "1" {
		t.Skip("opt-in local capacity acceptance")
	}
	for _, n := range []int{1000, 10000, 50000} {
		for _, format := range []string{"logs", "bill"} {
			t.Run(fmt.Sprintf("%s/%d", format, n), func(t *testing.T) {
				m, handler, cookie, _ := billHTTPFixture(t)
				// This database is newFakeProdDB(t), never a configured source DB.
				if _, err := m.prodDB.Exec("DELETE FROM logs"); err != nil {
					t.Fatal(err)
				}
				if _, err := m.prodDB.Exec("CREATE INDEX capacity_user_time ON logs(user_id,created_at,type,id)"); err != nil {
					t.Fatal(err)
				}
				tx, err := m.prodDB.Begin()
				if err != nil {
					t.Fatal(err)
				}
				defer func() { _ = tx.Rollback() }()
				stmt, err := tx.Prepare("INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,model_name,other,request_id) VALUES(?,101,?,2,1212,0,202,'capacity','claude-opus-5-5',?,?)")
				if err != nil {
					t.Fatal(err)
				}
				defer stmt.Close()
				row := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 1212)
				metadata, err := json.Marshal(map[string]any{
					"billing_mode": "tiered_expr", "expr_b64": "dGllcigiYmFzZSIsIHAgKiA0ICsgY3IgKiAwLjIgKyBjYyAqIDUgKyBjYzFoICogOCArIGMgKiAyMCk=",
					"matched_tier": "base", "billing_source": "wallet", "usage_semantic": "anthropic", "group_ratio": 0.6,
					"cache_tokens": 0, "cache_creation_tokens": 237815, "cache_creation_tokens_5m": 0, "cache_creation_tokens_1h": 0, "request_path": "/v1/messages",
				})
				if err != nil {
					t.Fatal(err)
				}
				for i := 1; i <= n; i++ {
					other := string(metadata)
					if i%4 == 0 {
						other = ""
					} // Old logs lacking optional metadata.
					if _, err := stmt.Exec(i, row.CreatedAt+int64(i%3600), other, fmt.Sprintf("capacity-%d", i)); err != nil {
						t.Fatal(err)
					}
				}
				if err := tx.Commit(); err != nil {
					t.Fatal(err)
				}
				q := "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13"
				path := "/api/logs/export/v1?ticket="
				if format == "bill" {
					q += "&format=bill-v1"
					path = "/api/logs/bill/v1?ticket="
				}
				runtime.GC()
				var before, after runtime.MemStats
				runtime.ReadMemStats(&before)
				started := time.Now()
				prep := billGet(handler, cookie, q)
				var reply struct {
					Ticket string `json:"ticket"`
					Total  int    `json:"total"`
				}
				if err := json.Unmarshal(prep.Body.Bytes(), &reply); err != nil || prep.Code != 200 || reply.Total != n || reply.Ticket == "" {
					t.Fatalf("preflight: %d %s", prep.Code, prep.Body.String())
				}
				prepared := time.Since(started)
				f, err := os.Create(filepath.Join(t.TempDir(), "response.csv"))
				if err != nil {
					t.Fatal(err)
				}
				defer f.Close()
				writer := &portalCapacityWriter{header: make(http.Header), file: f}
				req := httptest.NewRequest("GET", path+reply.Ticket, nil)
				req.AddCookie(cookie)
				handler.ServeHTTP(writer, req)
				elapsed := time.Since(started)
				runtime.ReadMemStats(&after)
				if writer.status != 200 || !strings.Contains(writer.header.Get("Content-Type"), "text/csv") {
					t.Fatalf("download status %d", writer.status)
				}
				if _, err = f.Seek(3, io.SeekStart); err != nil {
					t.Fatal(err)
				} // UTF-8 BOM
				reader := csv.NewReader(f)
				header, err := reader.Read()
				if err != nil {
					t.Fatal(err)
				}
				amountColumn, idColumn := 9, 11
				if format == "bill" {
					amountColumn, idColumn = 13, 5
					if len(header) != 15 {
						t.Fatal("wrong bill width")
					}
				}
				seen := make(map[int]bool, n)
				sum := new(big.Rat)
				count := 0
				for {
					record, err := reader.Read()
					if errors.Is(err, io.EOF) {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					id, err := strconv.Atoi(strings.TrimPrefix(record[idColumn], "capacity-"))
					if err != nil || id < 1 || id > n || seen[id] {
						t.Fatal("missing/duplicate/unexpected row")
					}
					seen[id] = true
					amount, ok := new(big.Rat).SetString(strings.TrimPrefix(record[amountColumn], "¥"))
					if !ok {
						t.Fatal("invalid charge")
					}
					sum.Add(sum, amount)
					count++
				}
				want := big.NewRat(int64(n)*1212, 500000)
				if count != n || sum.Cmp(want) != 0 {
					t.Fatalf("amount/count mismatch %d %s", count, sum)
				}
				t.Logf("synthetic rows=%d prepare=%s total=%s csv_bytes=%d allocated_bytes=%d amount=%s", n, prepared, elapsed, writer.bytes, after.TotalAlloc-before.TotalAlloc, sum.FloatString(6))
			})
		}
	}
}
