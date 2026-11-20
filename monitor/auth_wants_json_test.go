package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// A script request that follows the login redirect gets the login page with
// 200 and can mistake an expired session for success. Only top-level page
// navigations may be redirected.
func TestRequireRoleRedirectsOnlyPageNavigations(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{}
	r := gin.New()
	r.Any("/x", m.requireRole(roleRoot), func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	cases := []struct {
		name, method, accept, dest string
		want                       int
	}{
		{"page navigation", http.MethodGet, "text/html", "document", http.StatusFound},
		{"old browser navigation", http.MethodGet, "", "", http.StatusFound},
		{"fetch GET without Accept", http.MethodGet, "", "empty", http.StatusUnauthorized},
		{"fetch GET with JSON Accept", http.MethodGet, "application/json", "", http.StatusUnauthorized},
		{"POST without Accept", http.MethodPost, "", "", http.StatusUnauthorized},
		{"PUT without Accept", http.MethodPut, "*/*", "", http.StatusUnauthorized},
		{"DELETE without Accept", http.MethodDelete, "", "", http.StatusUnauthorized},
		{"HEAD navigation", http.MethodHead, "", "", http.StatusFound},
	}
	for _, tc := range cases {
		req := httptest.NewRequest(tc.method, "/x", strings.NewReader("{}"))
		if tc.accept != "" {
			req.Header.Set("Accept", tc.accept)
		}
		if tc.dest != "" {
			req.Header.Set("Sec-Fetch-Dest", tc.dest)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		if w.Code != tc.want {
			t.Errorf("%s: status %d, want %d", tc.name, w.Code, tc.want)
		}
		if tc.want == http.StatusUnauthorized && tc.method != http.MethodHead && !strings.Contains(w.Body.String(), "login_required") {
			t.Errorf("%s: JSON 401 body missing login_required: %s", tc.name, w.Body.String())
		}
	}
}

func TestRequireRoleForbiddenWriteGetsJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret-for-role-check-0123456789"}}
	r := gin.New()
	r.POST("/x", m.requireRole(roleRoot), func(c *gin.Context) { c.String(http.StatusOK, "ok") })
	req := httptest.NewRequest(http.MethodPost, "/x", strings.NewReader("{}"))
	req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("viewer", roleAdmin, time.Now().Unix())})
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden || !strings.Contains(w.Header().Get("Content-Type"), "json") {
		t.Fatalf("forbidden write: status %d content-type %q", w.Code, w.Header().Get("Content-Type"))
	}
}
