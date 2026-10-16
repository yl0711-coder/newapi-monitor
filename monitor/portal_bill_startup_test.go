package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/csv"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestPortalBillStartupSettingsExplicitOptIn(t *testing.T) {
	const key = "MONITOR_PORTAL_BILL_TASKS_ENABLED"
	for _, value := range []string{"", "false", "true", "TRUE", "1", "typo"} {
		t.Setenv(key, value)
		if got := LoadSettings().PortalBillTasksEnabled; got != (value == "true") {
			t.Fatalf("unexpected activation for %q", value)
		}
	}
}

func TestPortalBillStartupDisabledDoesNotCreateWorker(t *testing.T) {
	m, h, ck, _ := billHTTPFixture(t)
	if m.portalBillService != nil {
		t.Fatal("default created background worker")
	}
	for _, path := range []string{"/api/logs/bill/tasks", "/api/logs/bill/tasks/" + strings.Repeat("a", 32)} {
		w := billTaskRequest(h, ck, "POST", path, `{}`)
		if w.Code != http.StatusNotFound {
			t.Fatalf("default exposed task API: %d", w.Code)
		}
	}
	parent, err := m.portalExportTempDir()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(parent, "bill-tasks-v1")); !os.IsNotExist(err) {
		t.Fatal("default created task staging")
	}
}

func TestPortalBillStartupCleanupRejectsSymlink(t *testing.T) {
	out := t.TempDir()
	if err := os.Chmod(out, 0750); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "export")
	if err := os.Symlink(out, link); err != nil {
		t.Fatal(err)
	}
	if err := preparePortalExportTempDir(link); err == nil {
		t.Fatal("cleanup followed substituted directory")
	}
	info, err := os.Stat(out)
	if err != nil || info.Mode().Perm() != 0750 {
		t.Fatal("outside permissions changed")
	}
}

func TestPortalBillStartupEnabledAndFailureIsolation(t *testing.T) {
	for _, broken := range []bool{false, true} {
		t.Run(strconv.FormatBool(broken), func(t *testing.T) {
			m, old, ck, _ := billHTTPFixture(t)
			oldTicket := billPrepare(t, old, ck)
			m.exportLim = &exportLimiter{last: map[int64]int64{}}
			if broken {
				parent, _ := m.portalExportTempDir()
				outside := t.TempDir()
				if err := os.Symlink(outside, filepath.Join(parent, "bill-tasks-v1")); err != nil {
					t.Fatal(err)
				}
			}
			m.cfg.PortalBillTasksEnabled = true
			h := gin.New()
			m.RegisterPortalRoutes(h)
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				m.stopPortalBillTasks(ctx)
			})
			w := billGet(h, ck, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13&format=bill-v1")
			if broken {
				if w.Code != 503 || strings.Contains(w.Body.String(), `"ticket"`) || m.portalBillService != nil {
					t.Fatalf("failed init silently fell back: %d %s", w.Code, w.Body.String())
				}
				w = billTaskRequest(h, ck, "POST", "/api/logs/bill/tasks", `{"ticket":"`+oldTicket+`"}`)
				if w.Code != 503 || w.Header().Get("Cache-Control") != "no-store" {
					t.Fatal("failed init task route not explicit")
				}
			} else if w.Code != 200 || !strings.Contains(w.Body.String(), `"bill_task_contract":1`) || m.portalBillService == nil {
				t.Fatalf("enabled service not advertised: %d %s", w.Code, w.Body.String())
			}
			w = billGet(h, ck, "/api/logs/bill/v1?ticket="+oldTicket)
			if w.Code != 409 || strings.Contains(w.Header().Get("Content-Type"), "text/csv") {
				t.Fatal("stale page bypassed snapshot mode")
			}
			for _, path := range []string{"/login", "/", "/api/logs?from=2026-10-13&to=2026-10-13"} {
				if got := billGet(h, ck, path); got.Code != 200 {
					t.Fatalf("unrelated portal route %s affected: %d", path, got.Code)
				}
			}
			// Isolate the existing preflight rate limiter, not the background mode.
			m.exportLim = &exportLimiter{last: map[int64]int64{}}
			w = billGet(h, ck, "/api/logs/export/prepare?from=2026-10-13&to=2026-10-13")
			var prep struct {
				Ticket string `json:"ticket"`
			}
			if json.Unmarshal(w.Body.Bytes(), &prep) != nil || w.Code != 200 || prep.Ticket == "" || strings.Contains(w.Body.String(), "bill_task_contract") {
				t.Fatal("ordinary CSV preflight affected")
			}
			w = billGet(h, ck, "/api/logs/export/v1?ticket="+prep.Ticket)
			if w.Code != 200 || !strings.Contains(w.Header().Get("Content-Type"), "text/csv") {
				t.Fatalf("ordinary CSV download affected: %d %s", w.Code, w.Body.String())
			}
		})
	}
}

// Full normal registration -> real HTTP preflight -> job -> read-only MySQL
// snapshot -> verified private file -> HTTP download. All data is synthetic and
// opt-in fixture only permits the local Unix socket, never an arbitrary DSN.
func TestPortalBillStartupMySQLHTTPDownload(t *testing.T) {
	portalBillStartupMySQLHTTPDownload(t, 2100)
}

func TestPortalBillStartupMySQLHTTPDownloadLarge(t *testing.T) {
	if os.Getenv("BILL_SNAPSHOT_LARGE") != "1" {
		t.Skip("opt-in isolated 50k HTTP run")
	}
	portalBillStartupMySQLHTTPDownload(t, 50000)
}

func portalBillStartupMySQLHTTPDownload(t *testing.T, count int) {
	t.Helper()
	source, _ := billSnapshotMySQLFixture(t, count)
	m, _, ck, _ := billHTTPFixture(t)
	m.prodDB = source.prodDB
	m.cfg.PortalBillTasksEnabled = true
	h := gin.New()
	m.RegisterPortalRoutes(h)
	t.Cleanup(func() { _ = m.portalBillService.Close() })
	server := httptest.NewServer(h)
	defer server.Close()
	client := &http.Client{Timeout: 10 * time.Second}
	type responseMetadata struct {
		StatusCode    int
		ContentLength int64
	}
	do := func(method, path, body, origin string) (responseMetadata, []byte) {
		t.Helper()
		req, err := http.NewRequest(method, server.URL+path, strings.NewReader(body))
		if err != nil {
			t.Fatal(err)
		}
		req.Host = "usage.example.invalid" // HTTPS edge preserving public Host.
		req.AddCookie(ck)
		req.Header.Set("Origin", origin)
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Usage-Bill-Request", "1")
		req.Header.Set("X-Forwarded-Host", "attacker.example.invalid")
		res, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer res.Body.Close()
		b, err := io.ReadAll(res.Body)
		if err != nil {
			t.Fatal(err)
		}
		return responseMetadata{StatusCode: res.StatusCode, ContentLength: res.ContentLength}, b
	}
	const origin = "https://usage.example.invalid"
	res, b := do("GET", "/api/logs/export/prepare?from=2023-11-15&to=2023-11-15&format=bill-v1", "", origin)
	var prep struct {
		Ticket   string `json:"ticket"`
		Total    int64  `json:"total"`
		Contract int    `json:"bill_task_contract"`
	}
	if json.Unmarshal(b, &prep) != nil || res.StatusCode != 200 || prep.Total != int64(count) || prep.Contract != 1 {
		t.Fatalf("preflight: %d %s", res.StatusCode, b)
	}
	res, _ = do("POST", "/api/logs/bill/tasks", `{"ticket":"`+prep.Ticket+`"}`, "https://attacker.example.invalid")
	if res.StatusCode != 403 {
		t.Fatal("forwarded host bypassed Origin check")
	}
	res, b = do("POST", "/api/logs/bill/tasks", `{"ticket":"`+prep.Ticket+`"}`, origin)
	var out struct {
		Task struct {
			ID    string `json:"id"`
			State string `json:"state"`
			SHA   string `json:"sha256"`
			Bytes int64  `json:"bytes"`
		} `json:"task"`
	}
	if json.Unmarshal(b, &out) != nil || res.StatusCode != 202 || len(out.Task.ID) != 32 {
		t.Fatalf("create: %d %s", res.StatusCode, b)
	}
	id := out.Task.ID
	deadline := time.Now().Add(10 * time.Second)
	for out.Task.State != "ready" && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
		res, b = do("GET", "/api/logs/bill/tasks/"+id, "", origin)
		if json.Unmarshal(b, &out) != nil || res.StatusCode != 200 || out.Task.State == "failed" {
			t.Fatalf("status: %d %s", res.StatusCode, b)
		}
	}
	if out.Task.State != "ready" {
		t.Fatal("task did not complete")
	}
	res, b = do("GET", "/api/logs/bill/tasks/"+id+"/download", "", origin)
	digest := sha256.Sum256(b)
	if res.StatusCode != 200 || res.ContentLength != int64(len(b)) || int64(len(b)) != out.Task.Bytes || hex.EncodeToString(digest[:]) != out.Task.SHA {
		t.Fatal("download length/hash mismatch")
	}
	rows, err := csv.NewReader(strings.NewReader(strings.TrimPrefix(string(b), "\ufeff"))).ReadAll()
	if err != nil || len(rows) != count+1 {
		t.Fatalf("download rows %d: %v", len(rows), err)
	}
	for i, row := range rows[1:] {
		wantID := count - i
		wantCharge := strconv.FormatFloat(float64(wantID%31)/500000, 'f', 6, 64)
		if len(row) != 15 || row[5] != "local-"+strconv.Itoa(wantID) || strings.TrimPrefix(row[13], "¥") != wantCharge {
			t.Fatalf("row %d identity/charge mismatch", i)
		}
	}
	if source.prodDB.Stats().InUse != 0 {
		t.Fatal("download retained source connection")
	}
	t.Logf("registered opt-in service: HTTP CSV verified %d rows, %d bytes, all IDs/charges/SHA256/Content-Length matched", count, len(b))
}
