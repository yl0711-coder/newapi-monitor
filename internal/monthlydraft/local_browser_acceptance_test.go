package monthlydraft

// Test-only composition root. It cannot be compiled into a production binary.
// Every database is freshly generated; no source paths or online credentials
// may be supplied. Approval controls are private local files, never web routes.
import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

type localGenerationAcceptance struct {
	f        generationFixture
	g        *Generation
	auth     *gorm.DB
	router   *gin.Engine
	server   *httptest.Server
	password string
	runtime  *LocalRuntime
	monitor  *monitor.Monitor
}

func newLocalGenerationAcceptance(t *testing.T, reconstructed ...bool) *localGenerationAcceptance {
	t.Helper()
	gin.SetMode(gin.TestMode)
	auth, ro, path := usageAuthorityFixture(t)
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	m, err := monitor.New(monitor.Settings{StorePath: path, LocalSnapshotOnly: true, AlertsDisabled: true, PortalMonthlyStatementEnabled: true, SessionSecret: hex.EncodeToString(secret)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	password := "SyntheticLocalOnly!2026"
	hash, err := bcrypt.GenerateFromPassword([]byte(password), bcrypt.MinCost)
	if err != nil {
		t.Fatal(err)
	}
	if err := auth.Model(&monitor.CustomerGroup{}).Where("id IN ?", []int64{1, 2}).Update("portal_pw_admin", string(hash)).Error; err != nil {
		t.Fatal(err)
	}
	var f generationFixture
	var scopePath string
	if len(reconstructed) > 1 {
		t.Fatal("ambiguous synthetic acceptance fixture")
	}
	if len(reconstructed) == 1 && reconstructed[0] {
		f, scopePath = reconstructedGenerationFixture(t)
		if err := InitGenerationSelections(context.Background(), f.receiver); err != nil {
			t.Fatal(err)
		}
	} else {
		f = selectionFixture(t)
	}
	owner, _, err := LoadUsageGenerationOwner(context.Background(), ro, 1, 1)
	if err != nil {
		t.Fatal(err)
	}
	f.plan.Owner = owner
	if scopePath != "" {
		rewriteReconstructionScopeFixture(t, &f, scopePath, func(doc *RetainedAutomaticScope) { doc.Owner = owner })
	}
	selector, err := GenerationSelector(f.receiver)
	if err != nil {
		t.Fatal(err)
	}
	guard, err := NewUsageGenerationGuard(ro, selector)
	if err != nil {
		t.Fatal(err)
	}
	g, err := NewGeneration(f.receiver, f.funding, f.root, guard)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = g.Close() })
	stageFixture(t, f, "")
	runtime, err := NewControlledLocalRuntime(g, ro, GenerationRollout{Enabled: true, Targets: []GenerationTarget{{GroupID: 1, Month: "2026-09"}}})
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.Use(gin.Recovery())
	if err := r.SetTrustedProxies(nil); err != nil {
		t.Fatal(err)
	}
	srv := httptest.NewUnstartedServer(r)
	host := srv.Listener.Addr().String()
	r.Use(func(c *gin.Context) {
		if c.Request.Host != host {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		c.Header("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; connect-src 'self'; frame-ancestors 'none'; form-action 'self'")
		c.Header("Cache-Control", "private, no-store")
		// This fixture has only monthly source data. Do not enter unrelated
		// overview/log handlers, which expect a separately configured source DB.
		if c.Request.Method == "GET" {
			switch c.Request.URL.Path {
			case "/", "/login", "/logout", "/monthly-generation-local",
				"/echarts.js", "/flatpickr.js", "/flatpickr.css", "/range-picker.js",
				"/react.js", "/react-dom.js", "/semi-ui.js", "/semi-ui.css",
				"/api/monthly-statements", "/api/monthly-statement", "/api/monthly-statement/download",
				"/api/monthly-statement/generation":
			default:
				c.AbortWithStatusJSON(http.StatusServiceUnavailable, gin.H{"error": "本机合成环境仅验收月度账单"})
				return
			}
		}
		// No account administration or source configuration mutations.
		if c.Request.Method != "GET" && !(c.Request.Method == "POST" && (c.Request.URL.Path == "/login" || c.Request.URL.Path == "/api/monthly-statement/generation")) {
			c.AbortWithStatus(http.StatusForbidden)
			return
		}
		c.Next()
	})
	m.RegisterPortalRoutes(r)
	if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, runtime); err != nil {
		t.Fatal(err)
	}
	if err := runtime.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := runtime.Stop(ctx); err != nil {
			t.Error("local runtime failed to drain", err)
			// Do not advance to database cleanup while calls still own it.
			// The enclosing go test timeout remains the final failure bound.
			_ = runtime.Stop(context.Background())
		}
	})
	srv.Config.ReadHeaderTimeout = 5 * time.Second
	srv.Start()
	t.Cleanup(func() {
		// A browser may leave speculative connections open. Stop only this
		// synthetic listener's clients before waiting for HTTP shutdown.
		srv.CloseClientConnections()
		srv.Close()
	})
	return &localGenerationAcceptance{f: f, g: g, auth: auth, router: r, server: srv, password: password, runtime: runtime, monitor: m}
}

func (a *localGenerationAcceptance) request(t *testing.T, method, path, body string, cookies []*http.Cookie) *http.Response {
	t.Helper()
	req, err := http.NewRequest(method, a.server.URL+path, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", a.server.URL)
	req.Header.Set("X-Usage-Bill-Request", "1")
	req.Header.Set("X-Monthly-Statistics", "nullable-v1")
	req.Header.Set("X-Monthly-Display", "cents-chain-v1")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	client := &http.Client{Timeout: 10 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	res, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func (a *localGenerationAcceptance) login(t *testing.T, email string) []*http.Cookie {
	t.Helper()
	res := a.request(t, "POST", "/login", `{"email":"`+email+`","password":"`+a.password+`"}`, nil)
	defer res.Body.Close()
	if res.StatusCode != 200 || len(res.Cookies()) == 0 {
		t.Fatal("local fixture login failed", res.StatusCode)
	}
	return res.Cookies()
}

func TestUsageGenerationLocalSocketSmoke(t *testing.T) {
	a := newLocalGenerationAcceptance(t)
	owner := a.login(t, "owner@example.invalid")
	for _, path := range []string{"/monthly-generation-local", "/echarts.js", "/react.js", "/range-picker.js"} {
		res := a.request(t, "GET", path, "", owner)
		body, err := io.ReadAll(io.LimitReader(res.Body, 8<<20))
		_ = res.Body.Close()
		if err != nil || res.StatusCode != 200 || len(body) == 0 {
			t.Fatal("local page or asset unavailable", path, res.StatusCode, err)
		}
		if !strings.Contains(res.Header.Get("Content-Security-Policy"), "connect-src 'self'") {
			t.Fatal("network isolation header absent")
		}
	}
	res := a.request(t, "POST", "/api/password", `{}`, owner)
	_ = res.Body.Close()
	if res.StatusCode != 403 {
		t.Fatal("unrelated mutation allowed")
	}
	for _, path := range []string{"/api/overview", "/api/logs/export", "/api/logs/export/prepare", "/api/user"} {
		res = a.request(t, "GET", path, "", owner)
		body, err := io.ReadAll(res.Body)
		_ = res.Body.Close()
		if err != nil || res.StatusCode != 503 || !strings.Contains(string(body), "本机合成环境仅验收月度账单") {
			t.Fatal("unrelated source handler was not isolated", path, res.StatusCode, err)
		}
	}
	res = a.request(t, "POST", "/api/monthly-statement/generation", `{"month":"2026-09"}`, owner)
	_ = res.Body.Close()
	if res.StatusCode != 202 {
		t.Fatal("admission failed", res.StatusCode)
	}
	job, err := a.g.tasks.Find(context.Background(), generationRequest(a.f.plan, mustGenerationPlan(t, a.f)))
	if err != nil {
		t.Fatal(err)
	}
	waitLocalDraft(t, a.g, job)
	res = a.request(t, "GET", "/api/monthly-statement/generation?month=2026-09", "", owner)
	body, err := io.ReadAll(res.Body)
	_ = res.Body.Close()
	if err != nil || res.StatusCode != 200 || !strings.Contains(string(body), `"state":"pending_verification"`) {
		t.Fatal("socket did not observe real worker", err, res.StatusCode, string(body))
	}
	res = a.request(t, "GET", "/api/monthly-statement?month=2026-09", "", owner)
	_ = res.Body.Close()
	if res.StatusCode != 404 {
		t.Fatal("draft auto-published")
	}
	bad, err := http.NewRequest("GET", a.server.URL+"/login", nil)
	if err != nil {
		t.Fatal(err)
	}
	bad.Host = "foreign.invalid"
	res, err = a.server.Client().Do(bad)
	if err != nil {
		t.Fatal(err)
	}
	_ = res.Body.Close()
	if res.StatusCode != 403 {
		t.Fatal("foreign Host allowed")
	}
}

func mustGenerationPlan(t *testing.T, f generationFixture) []byte {
	t.Helper()
	_, raw, _, err := canonicalGeneration(f.plan, f.raw, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// Opt-in browser session, automatically ends after 45 minutes. Control files
// contain a random test-only token and reside in a newly-created 0700 directory.
func TestUsageGenerationLocalBrowserServer(t *testing.T) {
	mode := os.Getenv("MONTHLY_GENERATION_BROWSER_SERVER")
	if mode != "synthetic" && mode != "synthetic-reconstruction" {
		t.Skip("explicit synthetic browser session only")
	}
	a := newLocalGenerationAcceptance(t, mode == "synthetic-reconstruction")
	dir, err := os.MkdirTemp("", "monthly-generation-browser-")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	secret := make([]byte, 32)
	if _, err := rand.Read(secret); err != nil {
		t.Fatal(err)
	}
	token := hex.EncodeToString(secret)
	manifest := map[string]string{"url": a.server.URL + "/login", "generation_url": a.server.URL + "/monthly-generation-local", "email": "owner@example.invalid", "password": a.password, "control_token": token}
	raw, err := json.Marshal(manifest)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "session.json"), raw, 0600); err != nil {
		t.Fatal(err)
	}
	t.Logf("LOCAL_BROWSER_URL=%s/login LOCAL_CONTROL_DIR=%s (synthetic only; 45 minute lifetime)", a.server.URL, dir)
	deadline := time.NewTimer(45 * time.Minute)
	defer deadline.Stop()
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	approved := false
	for {
		select {
		case <-deadline.C:
			return
		case <-tick.C:
			for _, action := range []string{"stop", "review"} {
				if action == "review" && approved {
					continue
				}
				command, err := os.ReadFile(filepath.Join(dir, action+".request"))
				if err != nil {
					continue
				}
				if string(command) != token {
					continue
				}
				if action == "stop" {
					return
				}
				job, err := a.g.tasks.Find(context.Background(), generationRequest(a.f.plan, mustGenerationPlan(t, a.f)))
				if err != nil {
					t.Fatal(err)
				}
				// The test helper performs explicit synthetic review and exact
				// source/page/XLSX assertions, not a new automatic release policy.
				own, foreign := a.login(t, "owner@example.invalid"), a.login(t, "other@example.invalid")
				request := func(method, path, body string, cookies []*http.Cookie) *httptest.ResponseRecorder {
					req := httptest.NewRequest(method, a.server.URL+path, strings.NewReader(body))
					req.Header.Set("Content-Type", "application/json")
					req.Header.Set("Origin", a.server.URL)
					req.Header.Set("X-Usage-Bill-Request", "1")
					req.Header.Set("X-Monthly-Statistics", "nullable-v1")
					req.Header.Set("X-Monthly-Display", "cents-chain-v1")
					for _, c := range cookies {
						req.AddCookie(c)
					}
					w := httptest.NewRecorder()
					a.router.ServeHTTP(w, req)
					return w
				}
				if a.f.plan.AutomaticScopeSourceID == "" {
					testGenerationReviewedPublication(t, a.g, a.f, job, a.auth, request, own, foreign)
				} else {
					load, err := NewRetainedAutomaticScopeLoader(a.g, a.f.plan.AutomaticScopeSourceID)
					if err != nil {
						t.Fatal(err)
					}
					metadata, err := NewObservedAutomaticContext(a.g, load)
					if err != nil {
						t.Fatal(err)
					}
					meta, err := metadata(context.Background(), job.Request)
					if err != nil {
						t.Fatal(err)
					}
					testGenerationReviewedPublication(t, a.g, a.f, job, a.auth, request, own, foreign, meta.BundleContext)
				}
				approved = true
				if err := os.WriteFile(filepath.Join(dir, "review.passed"), []byte("synthetic review and source/page/XLSX assertions passed\n"), 0600); err != nil {
					t.Fatal(err)
				}
			}
		}
	}
}
