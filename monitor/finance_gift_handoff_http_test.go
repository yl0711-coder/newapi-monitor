//go:build unix

package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

const giftPreviewURL = "/finance/gift-handoff/preview"

func giftPreviewHTTPFixture(t *testing.T) (*Monitor, *gin.Engine, string) {
	t.Helper()
	job, digest, backup, plan := giftHandoffPreviewFixture(t)
	return giftPreviewHTTPMonitor(t, job, digest, backup, plan.Targets[0].SourceEpoch)
}

func giftPreviewHTTPMonitor(t *testing.T, job, digest, backup, epoch string) (*Monitor, *gin.Engine, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "receiver.db")
	if _, err := giftLocalCopyBackup(backup, path); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
		t.Fatal(err)
	}
	m := &Monitor{usageFactsDB: db, cfg: Settings{FinanceEnabled: true, FinanceFactsReadIsolationEnabled: true,
		FinanceGiftHandoffPreviewDir: job, FinanceGiftHandoffPreviewSHA256: digest, UsageFactsHistorySourceEpoch: epoch,
		UsageFactsStorePath: path, StorePath: filepath.Join(t.TempDir(), "unused-main.db"), SessionSecret: "local-handoff-http-test"}}
	t.Cleanup(func() {
		if reader := m.financeFactsReadDB.Load(); reader != nil {
			pool, _ := reader.DB()
			pool.Close()
		}
		closeDB()
	})
	if m.financeFactsReadStore() == db {
		t.Fatal("missing independent reader")
	}
	r := gin.New()
	m.RegisterRoutes(r)
	return m, r, `{"confirmation":"` + digest + `"}`
}

func giftPreviewRequest(m *Monitor, r http.Handler, url, body string, role int) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, url, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if role >= 0 {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("acceptance", role, time.Now().Unix())})
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestFinanceGiftHTTPPreviewAuthAndDefaultOff(t *testing.T) {
	m := &Monitor{cfg: Settings{SessionSecret: "local-only"}}
	r := gin.New()
	m.RegisterRoutes(r)
	for _, tc := range []struct{ role, code int }{{-1, 401}, {1, 403}, {roleAdmin, 403}, {roleRoot, 404}} {
		w := giftPreviewRequest(m, r, giftPreviewURL, `{}`, tc.role)
		if w.Code != tc.code || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
			t.Fatal(tc, w.Code, w.Header())
		}
	}
	for _, path := range []string{"/finance/gift-handoff/run", "/finance/gift-handoff/apply"} {
		if w := giftPreviewRequest(m, r, path, `{}`, roleRoot); w.Code != 404 {
			t.Fatal("execution route unexpectedly exists", path, w.Code)
		}
	}
}

func TestFinanceGiftHTTPPreviewReadonlyAndCooldown(t *testing.T) {
	m, r, body := giftPreviewHTTPFixture(t)
	source := filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "usage-facts.db")
	audit := filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "audit.jsonl")
	beforeSource, beforeAudit, beforeReceiver := giftLocalFileHash(t, source), giftLocalFileHash(t, audit), giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
	w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot)
	if w.Header().Get("X-Finance-Gift-Cooldown-Seconds") != "10" {
		t.Fatal("preview omitted cooldown hint", w.Header())
	}
	var response struct {
		Mode      string                    `json:"mode"`
		Execution bool                      `json:"execution_enabled"`
		Preview   FinanceGiftHandoffPreview `json:"preview"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if w.Code != 200 || response.Execution || response.Mode != "current_facts_preview_only" || response.Preview.Status != "ready" || response.Preview.RowsToUpdate != 3 || response.Preview.ReceiverSHA256 != "" {
		t.Fatal(w.Code, w.Body.String())
	}
	if beforeSource != giftLocalFileHash(t, source) || beforeAudit != giftLocalFileHash(t, audit) || beforeReceiver != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) {
		t.Fatal("preview changed inputs")
	}
	if m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("preview migrated execution ledger")
	}
	if w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot); w.Code != 429 || w.Header().Get("Retry-After") == "" {
		t.Fatal("cooldown not enforced", w.Code)
	}
}

func TestFinanceGiftHTTPPreviewRejectsPathsAndInvalidPayloads(t *testing.T) {
	m, r, body := giftPreviewHTTPFixture(t)
	for _, value := range []string{`{}`, `{"confirmation":"wrong"}`, body + `{}`, strings.Repeat(" ", 1025), strings.TrimSuffix(body, "}") + `,"receiver":"/private/live.db"}`} {
		if w := giftPreviewRequest(m, r, giftPreviewURL, value, roleRoot); w.Code != 400 {
			t.Fatal("bad body accepted", w.Code)
		}
	}
	if w := giftPreviewRequest(m, r, giftPreviewURL+"?job=/private/live", body, roleRoot); w.Code != 400 {
		t.Fatal(w.Code)
	}
	// Requests rejected before evidence reads do not consume a valid preview slot.
	if w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot); w.Code != 200 {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestFinanceGiftHTTPPreviewDoesNotQueueOrUseWriterFallback(t *testing.T) {
	m, r, body := giftPreviewHTTPFixture(t)
	m.financeGiftPreviewMu.Lock()
	w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot)
	m.financeGiftPreviewMu.Unlock()
	if w.Code != 429 {
		t.Fatal("concurrent request queued", w.Code)
	}
	// Simulate a legacy shared main/facts deployment: no fallback to writable pool.
	m.storeDB = m.usageFactsDB
	if w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot); w.Code != 503 {
		t.Fatal("writer fallback accepted", w.Code)
	}
}

func TestFinanceGiftHTTPPreviewSeesOnlyCommittedFacts(t *testing.T) {
	m, r, body := giftPreviewHTTPFixture(t)
	tx := m.usageFactsDB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	if err := tx.Exec("UPDATE finance_user_hour_facts SET consume_quota=consume_quota+1").Error; err != nil {
		t.Fatal(err)
	}
	w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot)
	if w.Code != 200 || !strings.Contains(w.Body.String(), `"status":"ready"`) {
		t.Fatal("preview blocked on writer or saw uncommitted changes", w.Code, w.Body.String())
	}
	if err := tx.Commit().Error; err != nil {
		t.Fatal(err)
	}
	m.financeGiftPreviewMu.Lock()
	m.financeGiftPreviewNextAt = time.Time{}
	m.financeGiftPreviewMu.Unlock()
	w = giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot)
	if w.Code != 200 || !strings.Contains(w.Body.String(), "receiver_aggregate_mismatch") || !strings.Contains(w.Body.String(), `"status":"blocked"`) {
		t.Fatal("new committed conflict ignored", w.Code, w.Body.String())
	}
}

func TestFinanceGiftHTTPPreviewRejectsUntrustedOrBusySource(t *testing.T) {
	for _, scenario := range []string{"epoch", "evidence", "lock", "digest"} {
		t.Run(scenario, func(t *testing.T) {
			m, r, body := giftPreviewHTTPFixture(t)
			switch scenario {
			case "epoch":
				m.cfg.UsageFactsHistorySourceEpoch = "different-source"
			case "evidence":
				if err := os.WriteFile(filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, giftLocalEvidenceName(0)), []byte("invalid"), 0600); err != nil {
					t.Fatal(err)
				}
			case "lock":
				lock, err := giftLocalLock(m.cfg.FinanceGiftHandoffPreviewDir)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			case "digest":
				m.cfg.FinanceGiftHandoffPreviewSHA256 = strings.Repeat("b", 64)
				body = `{"confirmation":"` + m.cfg.FinanceGiftHandoffPreviewSHA256 + `"}`
			}
			before := giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
			w := giftPreviewRequest(m, r, giftPreviewURL, body, roleRoot)
			if w.Code != 409 || strings.Contains(w.Body.String(), m.cfg.FinanceGiftHandoffPreviewDir) || strings.Contains(w.Body.String(), "SELECT") {
				t.Fatal(w.Code, w.Body.String())
			}
			if before != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) {
				t.Fatal("failed preview changed facts")
			}
		})
	}
}
