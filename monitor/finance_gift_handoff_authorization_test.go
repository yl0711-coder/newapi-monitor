//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

const giftAuthorizationURL = "/finance/gift-handoff/authorizations"
const giftAuthorizationNonce = "0123456789abcdef0123456789abcdef"

type giftAuthorizationResponse struct {
	TaskID        string                          `json:"task_id"`
	Status        string                          `json:"status"`
	Execution     bool                            `json:"execution_enabled"`
	Revalidate    bool                            `json:"requires_revalidation"`
	Authorization financeGiftAuthorizationPayload `json:"authorization"`
	RevokedAt     int64                           `json:"revoked_at"`
	RevokedBy     string                          `json:"revoked_by"`
}

func giftAuthorizationFixture(t *testing.T) (*Monitor, *gin.Engine, string) {
	t.Helper()
	m, r, _ := giftPreviewHTTPFixture(t)
	attachGiftAuthorizationStore(t, m)
	body := `{"confirmation":"` + m.cfg.FinanceGiftHandoffPreviewSHA256 + `","request_id":"` + giftAuthorizationNonce + `"}`
	return m, r, body
}

func attachGiftAuthorizationStore(t *testing.T, m *Monitor) {
	t.Helper()
	if err := giftLocalWriteNew(m.cfg.StorePath, nil); err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(m.cfg.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeDB)
	if err := db.AutoMigrate(&financeGiftHandoffAuthorization{}); err != nil {
		t.Fatal(err)
	}
	m.storeDB = db
	m.cfg.FinanceGiftHandoffApprovalEnabled = true
}

func giftAuthorizationRequestHTTP(m *Monitor, r http.Handler, method, path, body, actor string, role int) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	if role >= 0 {
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession(actor, role, time.Now().Unix())})
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func giftAuthorizationDecode(t *testing.T, w *httptest.ResponseRecorder) giftAuthorizationResponse {
	t.Helper()
	var response giftAuthorizationResponse
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatalf("HTTP %d %s", w.Code, w.Body.String())
	}
	if response.Execution || !response.Revalidate || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatal("unsafe response", response, w.Header())
	}
	return response
}

func TestFinanceGiftAuthorizationPersistenceAndRevocation(t *testing.T) {
	m, r, body := giftAuthorizationFixture(t)
	paths := []string{m.cfg.UsageFactsStorePath, filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "usage-facts.db"), filepath.Join(m.cfg.FinanceGiftHandoffPreviewDir, "audit.jsonl")}
	hashes := make([]string, len(paths))
	for i, p := range paths {
		hashes[i] = giftLocalFileHash(t, p)
	}
	response := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot)
	if response.Header().Get("X-Finance-Gift-Cooldown-Seconds") != "10" {
		t.Fatal("authorization omitted cooldown hint", response.Header())
	}
	first := giftAuthorizationDecode(t, response)
	if first.Status != "awaiting_execution" || first.Authorization.Actor != "owner" || first.Authorization.MaxHours != 2 || len(first.Authorization.Targets) != 1 || first.Authorization.Targets[0].RowsToUpdate != 3 {
		t.Fatal(first)
	}
	second := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot))
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if string(a) != string(b) {
		t.Fatal("retry changed approval")
	}
	// Reopen the persisted main store and reconstruct Monitor/router. No in-memory approval cache.
	db, closeDB, err := giftLocalDatabase(m.cfg.StorePath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	restarted := &Monitor{cfg: m.cfg, storeDB: db, usageFactsDB: m.usageFactsDB}
	r2 := gin.New()
	restarted.RegisterRoutes(r2)
	url := giftAuthorizationURL + "/" + first.TaskID
	status := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(restarted, r2, "GET", url, "", "owner", roleRoot))
	if status.TaskID != first.TaskID || status.Authorization.ExpiresAt != first.Authorization.ExpiresAt {
		t.Fatal("approval not durable", status)
	}
	revoked := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(restarted, r2, "POST", url+"/revoke", "{}", "second-root", roleRoot))
	if revoked.Status != "revoked" || revoked.RevokedBy != "second-root" || revoked.RevokedAt <= 0 {
		t.Fatal(revoked)
	}
	repeat := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot))
	if repeat.Status != "revoked" || repeat.RevokedAt != revoked.RevokedAt {
		t.Fatal("retry resurrected authorization", repeat)
	}
	for i, p := range paths {
		if hashes[i] != giftLocalFileHash(t, p) {
			t.Fatal("approval changed facts/source", p)
		}
	}
	if m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffAuthorization{}) || m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("approval added facts schema")
	}
	var count int64
	if err := m.storeDB.Model(&financeGiftHandoffAuthorization{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatal(count, err)
	}
}

func TestFinanceGiftAuthorizationAuthDefaultAndInput(t *testing.T) {
	m, r, body := giftAuthorizationFixture(t)
	for _, tc := range []struct{ role, code int }{{-1, 401}, {1, 403}, {roleAdmin, 403}} {
		for _, route := range []struct{ method, path string }{{"POST", giftAuthorizationURL}, {"GET", giftAuthorizationURL + "/" + strings.Repeat("a", 64)}, {"POST", giftAuthorizationURL + "/" + strings.Repeat("a", 64) + "/revoke"}} {
			w := giftAuthorizationRequestHTTP(m, r, route.method, route.path, body, "user", tc.role)
			if w.Code != tc.code {
				t.Fatal(route, tc, w.Code)
			}
		}
	}
	m.cfg.FinanceGiftHandoffApprovalEnabled = false
	if w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot); w.Code != 404 {
		t.Fatal(w.Code)
	}
	m.cfg.FinanceGiftHandoffApprovalEnabled = true
	for _, invalid := range []string{`{}`, body + `{}`, strings.Replace(body, giftAuthorizationNonce, "../bad", 1), strings.Replace(body, `}`, `,"path":"/live.db"}`, 1), strings.Replace(body, `}`, `,"max_hours":0}`, 1), strings.Replace(body, `}`, `,"max_hours":11}`, 1), strings.Repeat("a", 1025)} {
		if w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, invalid, "owner", roleRoot); w.Code != 400 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	req := httptest.NewRequest("POST", giftAuthorizationURL, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "https://attacker.invalid")
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("owner", roleRoot, time.Now().Unix())})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != 403 {
		t.Fatal("cross-origin accepted", w.Code)
	}
	first := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot))
	for _, tc := range []struct{ body, actor string }{{body, "other"}, {strings.Replace(body, `}`, `,"max_hours":1}`, 1), "owner"}} {
		if w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, tc.body, tc.actor, roleRoot); w.Code != 409 {
			t.Fatal("idempotency collision accepted", first, w.Code)
		}
	}
	for _, path := range []string{"/finance/gift-handoff/run", "/finance/gift-handoff/apply"} {
		if w := giftAuthorizationRequestHTTP(m, r, "POST", path, body, "owner", roleRoot); w.Code != 404 {
			t.Fatal("write route exists", path)
		}
	}
}

func TestFinanceGiftAuthorizationFailsClosed(t *testing.T) {
	for _, scenario := range []string{"epoch", "aggregate", "readonly-fallback", "missing-table", "busy-gate"} {
		t.Run(scenario, func(t *testing.T) {
			m, r, body := giftAuthorizationFixture(t)
			want := 409
			switch scenario {
			case "epoch":
				m.cfg.UsageFactsHistorySourceEpoch = "other"
			case "aggregate":
				if err := m.usageFactsDB.Model(&FinanceUserHourFact{}).Where("1=1").Update("consume_quota", -1).Error; err != nil {
					t.Fatal(err)
				}
			case "readonly-fallback":
				m.cfg.FinanceFactsReadIsolationEnabled = false
				want = 503
			case "missing-table":
				if err := m.storeDB.Migrator().DropTable(&financeGiftHandoffAuthorization{}); err != nil {
					t.Fatal(err)
				}
				want = 503
			case "busy-gate":
				m.financeGiftPreviewMu.Lock()
				defer m.financeGiftPreviewMu.Unlock()
				want = 429
			}
			before := giftLocalFileHash(t, m.cfg.UsageFactsStorePath)
			w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot)
			if w.Code != want || strings.Contains(w.Body.String(), m.cfg.StorePath) || strings.Contains(w.Body.String(), "SELECT") {
				t.Fatal(scenario, w.Code, w.Body.String())
			}
			if before != giftLocalFileHash(t, m.cfg.UsageFactsStorePath) {
				t.Fatal("failed authorization changed facts")
			}
		})
	}
}

func TestFinanceGiftAuthorizationExpiryConfigurationAndCorruption(t *testing.T) {
	m, r, body := giftAuthorizationFixture(t)
	first := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot))
	row, p, err := readFinanceGiftAuthorization(context.Background(), m.storeDB, first.TaskID)
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct {
		now  int64
		cfg  Settings
		want string
	}{{p.ExpiresAt, m.cfg, "expired"}, {p.ApprovedAt, m.cfg, "awaiting_execution"}} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		giftAuthorizationReply(c, row, p, tc.cfg, tc.now)
		if !strings.Contains(w.Body.String(), `"status":"`+tc.want+`"`) {
			t.Fatal(w.Body.String())
		}
	}
	m.cfg.UsageFactsStorePath = filepath.Join(t.TempDir(), "another.db")
	if got := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "GET", giftAuthorizationURL+"/"+first.TaskID, "", "owner", roleRoot)); got.Status != "configuration_changed" {
		t.Fatal(got)
	}
	if err := m.storeDB.Model(&financeGiftHandoffAuthorization{}).Where("id=?", first.TaskID).Update("payload", "{}").Error; err != nil {
		t.Fatal(err)
	}
	if w := giftAuthorizationRequestHTTP(m, r, "GET", giftAuthorizationURL+"/"+first.TaskID, "", "owner", roleRoot); w.Code != 503 {
		t.Fatal("corrupt approval accepted", w.Code)
	}
}

func TestFinanceGiftAuthorizationSchemaOptIn(t *testing.T) {
	for _, enabled := range []bool{false, true} {
		m := &Monitor{cfg: Settings{FinanceGiftHandoffApprovalEnabled: enabled, LocalSnapshotOnly: true}}
		if err := m.openStore(filepath.Join(t.TempDir(), "monitor.db")); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { m.Close() })
		if m.storeDB.Migrator().HasTable(&financeGiftHandoffAuthorization{}) != enabled {
			t.Fatal("schema opt-in not honored", enabled)
		}
	}
}

func TestFinanceGiftAuthorizationPoolWaitBounded(t *testing.T) {
	m, _, _ := giftAuthorizationFixture(t)
	pool, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	pool.SetMaxOpenConns(1)
	tx := m.storeDB.Begin()
	if tx.Error != nil {
		t.Fatal(tx.Error)
	}
	defer tx.Rollback()
	started := time.Now()
	_, _, err = readFinanceGiftAuthorization(context.Background(), m.storeDB, strings.Repeat("a", 64))
	if !errors.Is(err, context.DeadlineExceeded) || time.Since(started) > 4*time.Second {
		t.Fatal("approval query wait unbounded", time.Since(started), err)
	}
}

func TestFinanceGiftAuthorizationConcurrentIdempotency(t *testing.T) {
	m, r, body := giftAuthorizationFixture(t)
	first := giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL, body, "owner", roleRoot))
	errorsCh := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() {
			_, _, err := saveFinanceGiftAuthorization(context.Background(), m.storeDB, first.TaskID, first.Authorization)
			errorsCh <- err
		}()
	}
	for i := 0; i < 2; i++ {
		if err := <-errorsCh; err != nil {
			t.Fatal(err)
		}
	}
	p := first.Authorization
	p.MaxHours = 1
	if _, _, err := saveFinanceGiftAuthorization(context.Background(), m.storeDB, first.TaskID, p); !errors.Is(err, errGiftAuthorizationConflict) {
		t.Fatal("different scope overwrote existing approval", err)
	}
	row, stored, err := readFinanceGiftAuthorization(context.Background(), m.storeDB, first.TaskID)
	if err != nil || stored.MaxHours != 2 || row.RevokedAt != 0 {
		t.Fatal(stored, err)
	}
}
