package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestReportCacheHeadersCoverRegisteredRoutesAndAuthFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	// Enable usage reads using an isolated SQLite fixture, never a live source.
	m.prodDB = newFakeProdDB(t)
	m.cfg.NewAPIBaseURL = "https://example.invalid"
	m.cfg.SessionSecret = "local-report-header-test"
	m.cfg.FinanceStartDate = "2026-05-01"
	r := gin.New()
	m.RegisterRoutes(r)
	request := func(t *testing.T, path string, role, wantStatus int) {
		t.Helper()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", "application/json")
		if role > 0 {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("fixture", role, time.Now().Unix())})
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != wantStatus {
			t.Fatalf("%s role=%d: status=%d want=%d body=%s", path, role, w.Code, wantStatus, w.Body.String())
		}
		if !strings.Contains(w.Header().Get("Cache-Control"), "no-store") || w.Header().Get("Pragma") != "no-cache" {
			t.Fatalf("%s role=%d: cache headers missing: %v", path, role, w.Header())
		}
	}
	for _, path := range []string{
		"/usage/users", "/usage/groups", "/usage/followups", "/usage/followups/log", "/usage/settings", "/usage/matrix", "/usage/stats",
		"/channels/report", "/channels/data-status", "/channels/economics", "/finance/report", "/finance/internal-accounts",
		"/stability/report", "/stability/detail", "/stability/problems",
	} {
		t.Run(path, func(t *testing.T) {
			request(t, path, 0, http.StatusUnauthorized)
			request(t, path, 1, http.StatusForbidden)
		})
	}
	for _, path := range []string{
		"/usage/matrix?from=bad&to=bad", "/usage/stats?user_id=-1",
		"/channels/report?from=bad&to=bad", "/finance/report?from=bad&to=bad", "/stability/detail?group=",
	} {
		request(t, path, roleAdmin, http.StatusBadRequest)
	}
	request(t, "/usage/groups", roleAdmin, http.StatusOK)
	m.cfg.StabilityEnabled = false
	request(t, "/stability/report", roleAdmin, http.StatusOK)
	request(t, "/channels/report", roleAdmin, http.StatusOK)
	request(t, "/finance/report?from=2026-09-01&to=2026-09-02", roleAdmin, http.StatusOK)
}
