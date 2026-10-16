package monitor

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func portalLargeFixture(t *testing.T, n, contentBytes int) (*Monitor, http.Handler, *http.Cookie, int64) {
	t.Helper()
	m, handler, cookie, gid := billHTTPFixture(t)
	// newFakeProdDB(t) only. Never use a configured source for capacity tests.
	if _, err := m.prodDB.Exec("DELETE FROM logs"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("CREATE INDEX large_user_time ON logs(user_id,created_at,type,id)"); err != nil {
		t.Fatal(err)
	}
	_, err := m.prodDB.Exec(`WITH RECURSIVE seq(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM seq WHERE n < ?)
	INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,username,model_name,request_id,other,content)
	SELECT n,101,?,2,123,10,5,'large-local','unknown-model','large-'||n,'{}',? FROM seq`, n, time.Date(2026, 10, 13, 12, 0, 0, 0, usageCST).Unix(), strings.Repeat("x", contentBytes))
	if err != nil {
		t.Fatal(err)
	}
	return m, handler, cookie, gid
}

func portalLargeResetPrepare(m *Monitor, gid int64) {
	// Isolated tests only: avoid waiting five seconds between different cases.
	m.exportLim.mu.Lock()
	delete(m.exportLim.prepare, gid)
	m.exportLim.mu.Unlock()
}

func portalLargePrepare(t *testing.T, handler http.Handler, cookie *http.Cookie, format string) string {
	t.Helper()
	path := "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13"
	if format == "bill" {
		path += "&format=bill-v1"
	}
	w := billGet(handler, cookie, path)
	var out struct {
		Ticket string `json:"ticket"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil || w.Code != 200 || out.Ticket == "" {
		t.Fatalf("prepare %d %s", w.Code, w.Body.String())
	}
	return out.Ticket
}

func portalLargeDownload(t *testing.T, handler http.Handler, cookie *http.Cookie, path string) (*portalCapacityWriter, time.Duration) {
	t.Helper()
	f, err := os.Create(filepath.Join(t.TempDir(), "download.csv"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = f.Close() })
	w := &portalCapacityWriter{header: make(http.Header), file: f}
	r := httptest.NewRequest("GET", path, nil)
	r.AddCookie(cookie)
	start := time.Now()
	handler.ServeHTTP(w, r)
	return w, time.Since(start)
}

func portalLargeNoStaleFiles(t *testing.T, m *Monitor) {
	t.Helper()
	dir, err := m.portalExportTempDir()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if os.IsNotExist(err) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), portalExportTempFilePrefix) {
			t.Fatal("partial export file retained")
		}
	}
}

func TestPortalExportLocalOverscale(t *testing.T) {
	if os.Getenv("USAGE_EXPORT_LARGE") != "1" {
		t.Skip("opt-in local large dataset test")
	}
	for _, n := range []int{100000, 500000, 1000000} {
		t.Run(fmt.Sprint(n), func(t *testing.T) {
			m, handler, cookie, gid := portalLargeFixture(t, n, 0)
			start := time.Now()
			bill := billGet(handler, cookie, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&format=bill-v1&confirm=1")
			billDuration := time.Since(start)
			if bill.Code != 413 || strings.Contains(bill.Body.String(), `"ticket"`) {
				t.Fatal("overscale bill not refused before download")
			}
			portalLargeResetPrepare(m, gid)
			start = time.Now()
			ticket := portalLargePrepare(t, handler, cookie, "logs")
			prepareDuration := time.Since(start)
			unconfirmed := billGet(handler, cookie, "/api/logs/export/v1?ticket="+ticket)
			if unconfirmed.Code != 200 || !strings.Contains(unconfirmed.Body.String(), `"need_confirm":true`) || strings.Contains(unconfirmed.Header().Get("Content-Type"), "text/csv") {
				t.Fatal("ordinary truncation did not require confirmation")
			}
			w, duration := portalLargeDownload(t, handler, cookie, "/api/logs/export/v1?ticket="+ticket+"&confirm=1")
			if w.status != 200 {
				t.Fatalf("download %d", w.status)
			}
			if _, err := w.file.Seek(3, io.SeekStart); err != nil {
				t.Fatal(err)
			}
			reader := csv.NewReader(w.file)
			if _, err := reader.Read(); err != nil {
				t.Fatal(err)
			}
			count := 0
			for {
				row, err := reader.Read()
				if err == io.EOF {
					break
				}
				if err != nil {
					t.Fatal(err)
				}
				if row[11] != fmt.Sprintf("large-%d", n-count) || row[9] != "0.000246" {
					t.Fatalf("row %d mismatch", count)
				}
				count++
			}
			if count != portalExportCap {
				t.Fatalf("capped rows=%d", count)
			}
			portalLargeNoStaleFiles(t, m)
			t.Logf("source_rows=%d bill_refusal=%s logs_prepare=%s confirmed_50000_download=%s bytes=%d", n, billDuration, prepareDuration, duration, w.bytes)
		})
	}
}

func TestPortalExportLocalWideRows(t *testing.T) {
	if os.Getenv("USAGE_EXPORT_LARGE") != "1" {
		t.Skip("opt-in local wide row test")
	}
	for _, width := range []int{2048, 4096} {
		t.Run(fmt.Sprint(width), func(t *testing.T) {
			m, handler, cookie, gid := portalLargeFixture(t, 50000, width)
			ticket := portalLargePrepare(t, handler, cookie, "logs")
			w, duration := portalLargeDownload(t, handler, cookie, "/api/logs/export/v1?ticket="+ticket)
			want := 200
			if width == 4096 {
				want = 413
			}
			if w.status != want {
				t.Fatalf("width=%d code=%d expected=%d", width, w.status, want)
			}
			if want != 200 && (strings.Contains(w.header.Get("Content-Type"), "text/csv") || w.header.Get("Content-Disposition") != "") {
				t.Fatal("partial CSV exposed")
			}
			portalLargeNoStaleFiles(t, m)
			t.Logf("logs rows=50000 content_bytes=%d status=%d response_bytes=%d time=%s", width, w.status, w.bytes, duration)
			if want == 413 {
				// Failure must release both process capacity and the account quota.
				portalLargeResetPrepare(m, gid)
				ticket = portalLargePrepare(t, handler, cookie, "bill")
				bill, elapsed := portalLargeDownload(t, handler, cookie, "/api/logs/bill/v1?ticket="+ticket)
				if bill.status != 200 {
					t.Fatalf("failed export blocked bill retry %d", bill.status)
				}
				if _, err := bill.file.Seek(3, io.SeekStart); err != nil {
					t.Fatal(err)
				}
				reader := csv.NewReader(bill.file)
				if _, err := reader.Read(); err != nil {
					t.Fatal(err)
				}
				count := 0
				for {
					row, err := reader.Read()
					if err == io.EOF {
						break
					}
					if err != nil {
						t.Fatal(err)
					}
					if strings.TrimPrefix(row[13], "¥") != "0.000246" {
						t.Fatal("bill charge changed")
					}
					count++
				}
				if count != 50000 {
					t.Fatal("bill count changed")
				}
				portalLargeNoStaleFiles(t, m)
				t.Logf("bill after refused logs rows=%d bytes=%d time=%s", count, bill.bytes, elapsed)
			}
		})
	}
}

type portalBlockedDownload struct {
	header  http.Header
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func (w *portalBlockedDownload) Header() http.Header { return w.header }
func (w *portalBlockedDownload) WriteHeader(int)     {}
func (w *portalBlockedDownload) Write([]byte) (int, error) {
	w.once.Do(func() { close(w.started) })
	<-w.release
	return 0, io.ErrClosedPipe
}

func TestPortalExportSlowDownloadIsolationAndRecovery(t *testing.T) {
	m, handler, cookie, _ := billHTTPFixture(t)
	_, otherHandler, otherCookie, _ := billHTTPFixture(t)
	ticket := portalLargePrepare(t, handler, cookie, "bill")
	otherTicket := portalLargePrepare(t, otherHandler, otherCookie, "bill")
	w := &portalBlockedDownload{header: make(http.Header), started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan struct{})
	var release sync.Once
	defer func() { release.Do(func() { close(w.release) }); <-done }()
	go func() {
		defer close(done)
		r := httptest.NewRequest("GET", "/api/logs/bill/v1?ticket="+ticket, nil)
		r.AddCookie(cookie)
		handler.ServeHTTP(w, r)
	}()
	select {
	case <-w.started:
	case <-time.After(5 * time.Second):
		t.Fatal("download never started")
	}
	start := time.Now()
	page := billGet(handler, cookie, "/api/logs?from=2026-10-13&to=2026-10-13")
	if page.Code != 200 {
		t.Fatalf("slow download blocked log page %d", page.Code)
	}
	pageTime := time.Since(start)
	other := billGet(otherHandler, otherCookie, "/api/logs/bill/v1?ticket="+otherTicket)
	if other.Code != 429 {
		t.Fatalf("global generation limit not respected: %d", other.Code)
	}
	release.Do(func() { close(w.release) })
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("broken download did not exit")
	}
	portalLargeNoStaleFiles(t, m)
	if retry := billGet(otherHandler, otherCookie, "/api/logs/bill/v1?ticket="+otherTicket); retry.Code != 200 {
		t.Fatalf("other customer retry failed %d", retry.Code)
	}
	if retry := billGet(handler, cookie, "/api/logs/bill/v1?ticket="+ticket); retry.Code != 200 {
		t.Fatalf("interrupted customer retry failed %d", retry.Code)
	}
	t.Logf("log page during blocked download=%s; other export=429; retry after disconnect=PASS", pageTime)
}
