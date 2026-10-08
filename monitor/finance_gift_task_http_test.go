//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func giftTaskFixture(t *testing.T) (*Monitor, *gin.Engine, giftAuthorizationResponse) {
	return giftTaskFixtureForMode(t, false)
}

func giftTaskFixtureForMode(t *testing.T, live bool) (*Monitor, *gin.Engine, giftAuthorizationResponse) {
	t.Helper()
	fixture := giftAuthorizedLocalFixture
	if live {
		fixture = giftLiveFixture
	}
	m, r, a := fixture(t)
	m.cfg.FinanceGiftHandoffLocalExecutionEnabled = !live
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if !m.financeGiftTask.shutdown(ctx) {
			t.Error("test worker did not stop")
		}
	})
	return m, r, a
}

func TestFinanceGiftTaskHTTPRejectsStaleAndCrossSiteRequests(t *testing.T) {
	testFinanceGiftTaskHTTPRejectsStaleAndCrossSiteRequests(t, false)
}

func testFinanceGiftTaskHTTPRejectsStaleAndCrossSiteRequests(t *testing.T, live bool) {
	t.Helper()
	m, r, a := giftTaskFixtureForMode(t, live)
	v := giftHTTPControl(t, m, r)
	v.RequestID = strings.Repeat("a", 32)
	path := giftAuthorizationURL + "/" + a.TaskID + "/start"
	for _, mode := range []string{"origin", "fetch_site", "extra_field", "old_process", "old_revision", "bad_nonce"} {
		body := giftTaskBody(v)
		expected := 400
		copy := v
		switch mode {
		case "old_process":
			copy.Instance = "previous-process"
			body = giftTaskBody(copy)
			expected = 409
		case "old_revision":
			copy.Revision++
			body = giftTaskBody(copy)
			expected = 409
		case "bad_nonce":
			copy.RequestID = "bad"
			body = giftTaskBody(copy)
		case "extra_field":
			body = strings.TrimSuffix(body, "}") + `,"max_hours":100}`
		}
		req := httptest.NewRequest("POST", path, strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("Accept", "application/json")
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("local-root", roleRoot, time.Now().Unix())})
		if mode == "origin" {
			req.Header.Set("Origin", "https://other.invalid")
			expected = 403
		}
		if mode == "fetch_site" {
			req.Header.Set("Sec-Fetch-Site", "cross-site")
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != expected {
			t.Fatal(mode, w.Code, w.Body.String())
		}
		if got := giftHTTPControl(t, m, r); got.Status != "idle" {
			t.Fatal("rejected request started worker", got)
		}
	}
}

func giftHTTPControl(t *testing.T, m *Monitor, r *gin.Engine) financeGiftTaskView {
	t.Helper()
	path := "/finance/gift-handoff/local/control"
	if m.cfg.FinanceGiftHandoffLiveExecutionEnabled {
		path = "/finance/gift-handoff/live/control"
	}
	w := giftAuthorizationRequestHTTP(m, r, "GET", path, "", "local-root", roleRoot)
	var response struct {
		Control financeGiftTaskView `json:"control"`
	}
	if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &response) != nil {
		t.Fatal(w.Code, w.Body.String())
	}
	return response.Control
}

func giftTaskBody(v financeGiftTaskView) string {
	b, _ := json.Marshal(financeGiftTaskRequest{Instance: v.Instance, Revision: v.Revision, RequestID: v.RequestID})
	return string(b)
}

func TestFinanceGiftTaskHTTPPreviewCooldownHint(t *testing.T) {
	m, r, _ := giftTaskFixture(t)
	check := func(minimum, maximum int) {
		t.Helper()
		w := giftAuthorizationRequestHTTP(m, r, "GET", "/finance/gift-handoff/local/control", "", "local-root", roleRoot)
		var result struct {
			Seconds int `json:"preview_retry_after_seconds"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &result) != nil || result.Seconds < minimum || result.Seconds > maximum {
			t.Fatal("invalid cooldown hint", w.Code, w.Body.String())
		}
	}
	m.financeGiftPreviewMu.Lock()
	m.financeGiftPreviewNextAt = time.Now().Add(3500 * time.Millisecond)
	m.financeGiftPreviewMu.Unlock()
	check(3, 4)
	m.financeGiftPreviewMu.Lock()
	check(10, 10) // Must not wait behind a running preview.
	m.financeGiftPreviewNextAt = time.Now().Add(-time.Second)
	m.financeGiftPreviewMu.Unlock()
	check(0, 0)
}

func TestFinanceGiftTaskHTTPStartStopAndRetry(t *testing.T) {
	testFinanceGiftTaskHTTPStartStopAndRetry(t, false)
}

func testFinanceGiftTaskHTTPStartStopAndRetry(t *testing.T, live bool) {
	t.Helper()
	m, r, a := giftTaskFixtureForMode(t, live)
	v := giftHTTPControl(t, m, r)
	v.RequestID = strings.Repeat("a", 32)
	path := giftAuthorizationURL + "/" + a.TaskID
	before := giftBoundaryFingerprint(t, m.usageFactsDB)
	for i := 0; i < 2; i++ {
		w := giftAuthorizationRequestHTTP(m, r, "POST", path+"/start", giftTaskBody(v), "local-root", roleRoot)
		if w.Code != 202 {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	current := giftHTTPControl(t, m, r)
	if current.Status != "running" || current.RequestID != v.RequestID {
		t.Fatal(current)
	}
	w := giftAuthorizationRequestHTTP(m, r, "POST", path+"/stop", giftTaskBody(current), "local-root", roleRoot)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := giftControlWait(t, &m.financeGiftTask); got.Status != "stopped" {
		t.Fatal(got)
	}
	if giftBoundaryFingerprint(t, m.usageFactsDB) != before {
		t.Fatal("stop before cooldown changed facts")
	}
	w = giftAuthorizationRequestHTTP(m, r, "POST", path+"/start", giftTaskBody(v), "local-root", roleRoot)
	if w.Code != 202 || giftHTTPControl(t, m, r).Status != "stopped" {
		t.Fatal("retry restarted stopped job", w.Code)
	}
}

func TestFinanceGiftTaskHTTPCompletedAndClose(t *testing.T) {
	testFinanceGiftTaskHTTPCompletedAndClose(t, false)
}

func testFinanceGiftTaskHTTPCompletedAndClose(t *testing.T, live bool) {
	t.Helper()
	m, r, a := giftTaskFixtureForMode(t, live)
	// Complete only the approved subset, then verify HTTP resume is a no-op.
	run := m.runFinanceGiftAuthorizedLocal
	if live {
		run = m.runFinanceGiftAuthorizedLive
	}
	if _, err := run(context.Background(), a.TaskID, handoffNoWait); err != nil {
		t.Fatal(err)
	}
	before := giftBoundaryFingerprint(t, m.usageFactsDB)
	v := giftHTTPControl(t, m, r)
	v.RequestID = strings.Repeat("a", 32)
	w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/start", giftTaskBody(v), "local-root", roleRoot)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := giftControlWait(t, &m.financeGiftTask); got.Status != "complete" {
		t.Fatal(got)
	}
	if giftBoundaryFingerprint(t, m.usageFactsDB) != before {
		t.Fatal("completed HTTP replay changed facts")
	}
	// Close must cancel/join the worker before closing either database.
	v, _ = m.financeGiftTask.snapshot()
	v.RequestID = strings.Repeat("b", 32)
	v.TaskID = a.TaskID
	readBeforeExit := make(chan error, 1)
	if _, err := m.financeGiftTask.start(v, func(ctx context.Context) (string, error) {
		<-ctx.Done()
		var n int64
		readBeforeExit <- m.usageFactsDB.Model(&financeGiftHandoffCommit{}).Count(&n).Error
		return "", ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	m.Close()
	if err := <-readBeforeExit; err != nil {
		t.Fatal("database closed before worker exited", err)
	}
	if _, err := m.financeGiftTask.start(v, func(context.Context) (string, error) { return "complete", nil }); err == nil {
		t.Fatal("started after Close")
	}
}

func TestFinanceGiftTaskHTTPGates(t *testing.T) {
	m, r, a := giftTaskFixture(t)
	page := giftAuthorizationRequestHTTP(m, r, "GET", "/finance/gift-handoff/local", "", "local-root", roleRoot)
	if page.Code != 200 || !strings.Contains(page.Body.String(), "仅操作隔离副本") || page.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatal(page.Code)
	}
	paths := []string{"/finance/gift-handoff/local", "/finance/gift-handoff/local/control", giftAuthorizationURL + "/" + a.TaskID + "/start", giftAuthorizationURL + "/" + a.TaskID + "/stop"}
	for _, role := range []int{-1, roleAdmin} {
		for i, path := range paths {
			method := "GET"
			if i >= 2 {
				method = "POST"
			}
			w := giftAuthorizationRequestHTTP(m, r, method, path, "{}", "viewer", role)
			if w.Code != 401 && w.Code != 403 {
				t.Fatal(w.Code, path)
			}
		}
	}
	for _, mode := range []string{"disabled", "nonlocal", "dsn", "api"} {
		cfg := m.cfg
		code := 503
		switch mode {
		case "disabled":
			m.cfg.FinanceGiftHandoffLocalExecutionEnabled = false
			code = 404
		case "nonlocal":
			m.cfg.LocalSnapshotOnly = false
		case "dsn":
			m.cfg.ProdDSN = "must-not-connect"
		case "api":
			m.cfg.NewAPIBaseURL = "https://must-not-connect.invalid"
		}
		for i, path := range paths {
			method := "GET"
			if i >= 2 {
				method = "POST"
			}
			w := giftAuthorizationRequestHTTP(m, r, method, path, "{}", "local-root", roleRoot)
			if w.Code != code {
				t.Fatal(mode, w.Code, path)
			}
		}
		m.cfg = cfg
	}
	reader := m.financeFactsReadDB.Load()
	m.financeFactsReadDB.Store(nil)
	defer m.financeFactsReadDB.Store(reader)
	for i, path := range paths {
		method := "GET"
		if i >= 2 {
			method = "POST"
		}
		w := giftAuthorizationRequestHTTP(m, r, method, path, "{}", "local-root", roleRoot)
		if w.Code != 503 {
			t.Fatal("accepted reader fallback", path, w.Code)
		}
	}
}
