//go:build unix

package monitor

import (
	"context"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestFinanceGiftLiveHTTPRejectsStaleAndCrossSiteRequests(t *testing.T) {
	testFinanceGiftTaskHTTPRejectsStaleAndCrossSiteRequests(t, true)
}

func TestFinanceGiftLiveHTTPStartStopAndRetry(t *testing.T) {
	testFinanceGiftTaskHTTPStartStopAndRetry(t, true)
}

func TestFinanceGiftLiveHTTPCompletedAndClose(t *testing.T) {
	testFinanceGiftTaskHTTPCompletedAndClose(t, true)
}

func TestFinanceGiftLiveHTTPGatesAndReadonlyPage(t *testing.T) {
	m, r, a := giftTaskFixtureForMode(t, true)
	before := giftLiveFacts(t, m)
	page := giftAuthorizationRequestHTTP(m, r, "GET", "/finance/gift-handoff/live", "", "root", roleRoot)
	if page.Code != 200 || !strings.Contains(page.Body.String(), "将增量修改当前 Monitor 事实库") ||
		!strings.Contains(page.Body.String(), "const executionMode='live_finite_handoff';") ||
		strings.Contains(page.Body.String(), "仅操作隔离副本，不连接生产。") || page.Header().Get("X-Frame-Options") != "DENY" {
		t.Fatal("incorrect execution page", page.Code)
	}
	if got := giftHTTPControl(t, m, r); got.Status != "idle" {
		t.Fatal(got)
	}
	progress := giftGetProgress(t, m, r, a.TaskID, 503)
	if progress.ErrorCode != "ledger_missing" || progress.Progress != nil {
		t.Fatal(progress)
	}
	for _, path := range []string{"/finance/gift-handoff/local", "/finance/gift-handoff/local/control"} {
		if w := giftAuthorizationRequestHTTP(m, r, "GET", path, "", "root", roleRoot); w.Code != 404 {
			t.Fatal(path, w.Code)
		}
	}
	paths := []struct{ method, path string }{
		{"GET", "/finance/gift-handoff/live"}, {"GET", "/finance/gift-handoff/live/control"},
		{"GET", giftAuthorizationURL + "/" + a.TaskID + "/progress"},
		{"POST", giftAuthorizationURL + "/" + a.TaskID + "/start"}, {"POST", giftAuthorizationURL + "/" + a.TaskID + "/stop"},
	}
	for _, role := range []int{-1, roleAdmin} {
		for _, p := range paths {
			w := giftAuthorizationRequestHTTP(m, r, p.method, p.path, "{}", "viewer", role)
			if w.Code != 401 && w.Code != 403 {
				t.Fatal(p.path, w.Code)
			}
		}
	}
	for _, mode := range []string{"disabled", "local", "approval-off", "mixed", "bypass"} {
		cfg := m.cfg
		code := 503
		switch mode {
		case "disabled":
			m.cfg.FinanceGiftHandoffLiveExecutionEnabled = false
			code = 404
		case "local":
			m.cfg.LocalSnapshotOnly = true
		case "approval-off":
			m.cfg.FinanceGiftHandoffApprovalEnabled = false
			code = 503
		case "mixed":
			m.cfg.FinanceGiftHandoffLocalExecutionEnabled = true
		case "bypass":
			m.cfg.LocalAuthBypass = true
		}
		for _, p := range paths {
			want := code
			if mode == "approval-off" && strings.HasSuffix(p.path, "/progress") {
				want = 404
			}
			w := giftAuthorizationRequestHTTP(m, r, p.method, p.path, "{}", "root", roleRoot)
			if w.Code != want {
				t.Fatal(mode, p.path, w.Code, w.Body.String())
			}
		}
		m.cfg = cfg
	}
	if before != giftLiveFacts(t, m) || m.usageFactsDB.Migrator().HasTable(&financeGiftHandoffCommit{}) {
		t.Fatal("read/rejected request mutated facts")
	}
}

func TestFinanceGiftLiveHTTPRestartAndHistoricalProgress(t *testing.T) {
	m, r, a := giftTaskFixtureForMode(t, true)
	if _, err := m.runFinanceGiftAuthorizedLive(context.Background(), a.TaskID, handoffNoWait); err != nil {
		t.Fatal(err)
	}
	before := giftGetProgress(t, m, r, a.TaskID, 200)
	old := giftHTTPControl(t, m, r)
	old.RequestID = strings.Repeat("a", 32)
	restarted := &Monitor{cfg: m.cfg, storeDB: m.storeDB, usageFactsDB: m.usageFactsDB}
	restarted.financeFactsReadDB.Store(m.financeFactsReadDB.Load())
	other := gin.New()
	restarted.RegisterRoutes(other)
	control := giftHTTPControl(t, restarted, other)
	if control.Status != "idle" || control.Instance == old.Instance {
		t.Fatal("unexpected auto-resume", control)
	}
	w := giftAuthorizationRequestHTTP(restarted, other, "POST", giftAuthorizationURL+"/"+a.TaskID+"/start", giftTaskBody(old), "root", roleRoot)
	if w.Code != 409 {
		t.Fatal("old process request admitted", w.Code)
	}
	after := giftGetProgress(t, restarted, other, a.TaskID, 200)
	if *after.Progress != *before.Progress {
		t.Fatal("restart lost receipts", after)
	}
	giftAuthorizationDecode(t, giftAuthorizationRequestHTTP(restarted, other, "POST", giftAuthorizationURL+"/"+a.TaskID+"/revoke", "{}", "root", roleRoot))
	revoked := giftGetProgress(t, restarted, other, a.TaskID, 200)
	if revoked.AuthorizationStatus != "revoked" || *revoked.Progress != *before.Progress {
		t.Fatal(revoked)
	}
	control.RequestID = strings.Repeat("b", 32)
	w = giftAuthorizationRequestHTTP(restarted, other, "POST", giftAuthorizationURL+"/"+a.TaskID+"/start", giftTaskBody(control), "root", roleRoot)
	if w.Code != 409 {
		t.Fatal("revoked authorization started", w.Code)
	}
}

func TestFinanceGiftLiveHTTPChangedFactsFailWithoutOverwrite(t *testing.T) {
	m, r, a := giftTaskFixtureForMode(t, true)
	target := a.Authorization.Targets[0]
	if err := m.usageFactsDB.Exec("UPDATE finance_user_hour_facts SET consume_quota=consume_quota+1 WHERE hour_ts=? AND user_id=?", target.HourTs, target.UserID).Error; err != nil {
		t.Fatal(err)
	}
	before := giftLiveFacts(t, m)
	money := giftMixedMonetarySnapshot(t, m.usageFactsDB)
	v := giftHTTPControl(t, m, r)
	v.RequestID = strings.Repeat("a", 32)
	w := giftAuthorizationRequestHTTP(m, r, "POST", giftAuthorizationURL+"/"+a.TaskID+"/start", giftTaskBody(v), "root", roleRoot)
	if w.Code != 202 {
		t.Fatal(w.Code, w.Body.String())
	}
	if got := giftControlWait(t, &m.financeGiftTask); got.Status != "failed" {
		t.Fatal(got)
	}
	if before != giftLiveFacts(t, m) || money != giftMixedMonetarySnapshot(t, m.usageFactsDB) {
		t.Fatal("stale evidence overwrote current facts")
	}
	progress := giftGetProgress(t, m, r, a.TaskID, 200)
	if progress.Progress.CommittedTargets != 0 {
		t.Fatal(progress)
	}
}
