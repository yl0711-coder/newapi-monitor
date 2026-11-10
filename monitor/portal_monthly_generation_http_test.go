package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

type monthlyGenerationTestService struct {
	call func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error)
}

func (s monthlyGenerationTestService) SubmitMonthly(ctx context.Context, gid, version int64, month string) (MonthlyGenerationView, error) {
	return s.call(ctx, gid, version, month, true)
}

func (s monthlyGenerationTestService) ReadMonthly(ctx context.Context, gid, version int64, month string) (MonthlyGenerationView, error) {
	return s.call(ctx, gid, version, month, false)
}

func monthlyGenerationHTTPFixture(t *testing.T) (*Monitor, *gin.Engine, CustomerGroup, *http.Cookie) {
	t.Helper()
	m, _, r := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.LocalSnapshotOnly, m.cfg.PortalMonthlyStatementEnabled = true, true
	g := CustomerGroup{Name: "synthetic monthly org", PortalEmail: "local@example.invalid", PortalAuthVer: 2}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 201, GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	return m, r, g, &http.Cookie{Name: portalSessionCookie, Value: m.signPortalSession(g.ID, g.PortalAuthVer, time.Now().Unix())}
}

func generationHTTPDo(r *gin.Engine, method, suffix, body string, cookie *http.Cookie, mutate func(*http.Request)) *httptest.ResponseRecorder {
	req := httptest.NewRequest(method, "http://127.0.0.1/api/monthly-statement/generation"+suffix, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Origin", "http://127.0.0.1")
	req.Header.Set("X-Usage-Bill-Request", "1")
	if cookie != nil {
		req.AddCookie(cookie)
	}
	if mutate != nil {
		mutate(req)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestMonthlyGenerationPreviewIsExplicitLocalAndNeverChangesNormalPage(t *testing.T) {
	m, r, _, cookie := monthlyGenerationHTTPFixture(t)
	read := func(path string, authenticated bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest("GET", path, nil)
		if authenticated {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	before := read("/", true)
	if w := read("/monthly-generation-local", true); w.Code != http.StatusNotFound {
		t.Fatal("preview mounted by default", w.Code)
	}
	service := monthlyGenerationTestService{func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error) {
		t.Fatal("page rendering must not submit or query generation")
		return MonthlyGenerationView{}, nil
	}}
	if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, service); err != nil {
		t.Fatal(err)
	}
	if w := read("/monthly-generation-local", false); w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
		t.Fatal("preview authentication missing", w.Code)
	}
	w := read("/monthly-generation-local", true)
	if w.Code != http.StatusOK || w.Header().Get("Cache-Control") != "private, no-store" || !strings.Contains(w.Body.String(), portalMonthlyGenerationLocalJS) || !strings.Contains(w.Body.String(), `<a data-v="monthly">月度账单</a>`) {
		t.Fatal("local preview not rendered correctly", w.Code)
	}
	if strings.Contains(portalMonthlyGenerationLocalJS, "</script>") {
		t.Fatal("inline preview script terminates HTML script element")
	}
	after := read("/", true)
	if before.Code != http.StatusOK || after.Body.String() != before.Body.String() || strings.Contains(after.Body.String(), "monthlyGenerationLocal") {
		t.Fatal("ordinary page changed by local preview registration")
	}
}

func TestMonthlyGenerationProductionRegistrationIsExplicitAndIsolated(t *testing.T) {
	m, _, g, cookie := monthlyGenerationHTTPFixture(t)
	m.cfg.LocalSnapshotOnly = false // This route is no longer the local preview.
	ordinary := gin.New()
	m.RegisterPortalRoutes(ordinary)
	read := func(r *gin.Engine, path string, authenticated bool) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		if authenticated {
			req.AddCookie(cookie)
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	before := read(ordinary, "/", true)
	calls := 0
	service := monthlyGenerationTestService{func(_ context.Context, gid, version int64, month string, _ bool) (MonthlyGenerationView, error) {
		calls++
		if gid != g.ID || version != g.PortalAuthVer || month != "2026-09" {
			t.Fatal("incorrect authenticated scope", gid, version, month)
		}
		return MonthlyGenerationView{Month: month, State: "queued"}, nil
	}}
	r := gin.New()
	if err := m.RegisterPortalRoutesWithGeneration(r, service); err != nil {
		t.Fatal(err)
	}
	w := read(r, "/", true)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "生成月度账单") || strings.Contains(w.Body.String(), "生成账单 · 本机验收") || calls != 0 {
		t.Fatal("customer root not correctly composed", w.Code, calls)
	}
	if w := read(r, "/", false); w.Code != http.StatusFound || w.Header().Get("Location") != "/login" {
		t.Fatal("customer root lost authentication", w.Code)
	}
	if w := read(r, "/monthly-generation-local", true); w.Code != http.StatusNotFound {
		t.Fatal("local preview exposed", w.Code)
	}
	if w := generationHTTPDo(r, "POST", "", `{"month":"2026-09"}`, nil, nil); w.Code != 401 || calls != 0 {
		t.Fatal("unauthenticated generation accepted", w.Code, calls)
	}
	if w := generationHTTPDo(r, "POST", "", `{"month":"2026-09","group_id":999}`, cookie, nil); w.Code != 400 || calls != 0 {
		t.Fatal("client scope accepted", w.Code, calls)
	}
	if w := generationHTTPDo(r, "POST", "", `{"month":"2026-09"}`, cookie, nil); w.Code != 202 || calls != 1 {
		t.Fatal("authenticated generation failed", w.Code, calls)
	}
	// Turning the feature off is a new engine, not mutation of a live router.
	off := gin.New()
	m.RegisterPortalRoutes(off)
	after := read(off, "/", true)
	if before.Code != 200 || after.Body.String() != before.Body.String() {
		t.Fatal("disabled root changed")
	}
	if w := generationHTTPDo(off, "POST", "", `{"month":"2026-09"}`, cookie, nil); w.Code != 404 || calls != 1 {
		t.Fatal("disabled engine exposed generation", w.Code, calls)
	}
	wantRoutes := map[string]bool{}
	for _, route := range ordinary.Routes() {
		wantRoutes[route.Method+" "+route.Path] = true
	}
	wantRoutes["GET /api/monthly-statement/generation"] = true
	wantRoutes["POST /api/monthly-statement/generation"] = true
	for _, route := range r.Routes() {
		key := route.Method + " " + route.Path
		if !wantRoutes[key] {
			t.Fatal("unexpected route exposed", key)
		}
		delete(wantRoutes, key)
	}
	if len(wantRoutes) != 0 {
		t.Fatal("existing routes missing", wantRoutes)
	}
	if m.RegisterPortalRoutesWithGeneration(r, service) == nil || m.RegisterPortalRoutesWithGeneration(nil, service) == nil || m.RegisterPortalRoutesWithGeneration(gin.New(), nil) == nil {
		t.Fatal("invalid registration accepted")
	}
	m.cfg.PortalMonthlyStatementEnabled = false
	if m.RegisterPortalRoutesWithGeneration(gin.New(), service) == nil {
		t.Fatal("disabled statements accepted")
	}
}

func TestMonthlyGenerationHTTPExplicitLocalGateAndAuthenticatedScope(t *testing.T) {
	m, r, g, cookie := monthlyGenerationHTTPFixture(t)
	calls := 0
	service := monthlyGenerationTestService{func(_ context.Context, gid, version int64, month string, submit bool) (MonthlyGenerationView, error) {
		calls++
		if gid != g.ID || version != 2 || month != "2026-09" {
			t.Fatal("untrusted scope reached service", gid, version, month)
		}
		return MonthlyGenerationView{Month: month, State: "queued"}, nil
	}}
	if w := generationHTTPDo(r, "GET", "?month=2026-09", "", cookie, nil); w.Code != 404 {
		t.Fatal("generation mounted by default", w.Code)
	}
	m.cfg.LocalSnapshotOnly = false
	if m.RegisterLocalPortalMonthlyGenerationRoutes(r, service) == nil {
		t.Fatal("non-local generation allowed")
	}
	m.cfg.LocalSnapshotOnly = true
	if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, service); err != nil {
		t.Fatal(err)
	}
	if m.RegisterLocalPortalMonthlyGenerationRoutes(r, service) == nil {
		t.Fatal("duplicate route registration accepted")
	}
	for _, cookie := range []*http.Cookie{nil, {Name: portalSessionCookie, Value: m.signPortalSession(g.ID, 1, time.Now().Unix())}} {
		w := generationHTTPDo(r, "POST", "", `{"month":"2026-09"}`, cookie, nil)
		if w.Code != 401 || calls != 0 || w.Header().Get("Cache-Control") != "private, no-store" {
			t.Fatal("unauthorized submission reached service", w.Code, calls)
		}
	}
	for _, method := range []string{"GET", "POST"} {
		suffix, body, want := "?month=2026-09", "", 200
		if method == "POST" {
			suffix, body, want = "", `{"month":"2026-09"}`, 202
		}
		w := generationHTTPDo(r, method, suffix, body, cookie, func(req *http.Request) { req.Header.Set("X-Group-ID", "999") })
		var out map[string]any
		if w.Code != want || json.Unmarshal(w.Body.Bytes(), &out) != nil || len(out) != 3 || out["state"] != "queued" || out["month"] != "2026-09" {
			t.Fatal("unsafe or wrong HTTP result", w.Code, w.Body.String())
		}
	}
	if calls != 2 {
		t.Fatal("unexpected repeated invocation", calls)
	}
}

func TestMonthlyGenerationHTTPRejectsClientScopeAndCSRF(t *testing.T) {
	m, r, _, cookie := monthlyGenerationHTTPFixture(t)
	calls := 0
	if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, monthlyGenerationTestService{func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error) {
		calls++
		return MonthlyGenerationView{}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, body := range []string{`{}`, `null`, `[]`, `{"month":null}`, `{"month":"2026-09","group_id":2}`, `{"month":"2026-09","month":"2026-08"}`, `{"Month":"2026-09"}`, `{"month":"2026-09"} {}`, `{"month":"2999-09"}`, `{"month":"2019-09"}`, `{"month":"2026-9"}`, `{"month":"` + strings.Repeat("x", 600) + `"}`} {
		if w := generationHTTPDo(r, "POST", "", body, cookie, nil); w.Code != 400 {
			t.Fatal("invalid body accepted", w.Code)
		}
	}
	for _, suffix := range []string{"", "?month=2026-09&group_id=2", "?month=2026-09&month=2026-09", "?month=2026-09;group_id=2", "?month=2026-09&amount=1"} {
		if w := generationHTTPDo(r, "GET", suffix, "", cookie, nil); w.Code != 400 {
			t.Fatal("invalid query accepted", suffix, w.Code)
		}
	}
	for _, mutate := range []func(*http.Request){
		func(r *http.Request) { r.Header.Del("Origin") },
		func(r *http.Request) { r.Header.Set("Origin", "https://other.invalid") },
		func(r *http.Request) { r.Header.Del("X-Usage-Bill-Request") },
		func(r *http.Request) { r.Header.Set("Sec-Fetch-Site", "cross-site") },
	} {
		if w := generationHTTPDo(r, "POST", "", `{"month":"2026-09"}`, cookie, mutate); w.Code != 403 {
			t.Fatal("CSRF accepted", w.Code)
		}
	}
	if w := generationHTTPDo(r, "POST", "?month=2026-09", `{"month":"2026-09"}`, cookie, nil); w.Code != 400 {
		t.Fatal("mixed parameter sources accepted")
	}
	if w := generationHTTPDo(r, "POST", "", `{"month":"2026-09"}`, cookie, func(r *http.Request) { r.Header.Set("Content-Type", "text/plain") }); w.Code != 400 {
		t.Fatal("non JSON accepted")
	}
	if calls != 0 {
		t.Fatal("invalid requests reached service", calls)
	}
}

func TestMonthlyGenerationHTTPRecheckDoesNotReplaySubmission(t *testing.T) {
	for _, change := range []string{"members", "session", "cancel"} {
		t.Run(change, func(t *testing.T) {
			m, r, g, cookie := monthlyGenerationHTTPFixture(t)
			calls := 0
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			service := monthlyGenerationTestService{func(_ context.Context, _, _ int64, month string, _ bool) (MonthlyGenerationView, error) {
				calls++
				var err error
				switch change {
				case "members":
					err = m.storeDB.Create(&TrackedUser{UserID: 202, GroupID: g.ID}).Error
				case "session":
					err = m.storeDB.Model(&CustomerGroup{}).Where("id = ?", g.ID).Update("portal_auth_ver", 3).Error
				case "cancel":
					cancel()
				}
				if err != nil {
					t.Fatal(err)
				}
				return MonthlyGenerationView{Month: month, State: "queued"}, nil
			}}
			if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, service); err != nil {
				t.Fatal(err)
			}
			w := generationHTTPDo(r, "POST", "", `{"month":"2026-09"}`, cookie, func(r *http.Request) { *r = *r.WithContext(ctx) })
			if w.Code < 400 || calls != 1 || strings.Contains(w.Body.String(), `"state"`) {
				t.Fatal("changed authority leaked or replayed submission", w.Code, calls, w.Body.String())
			}
		})
	}
}

func TestMonthlyGenerationHTTPAllowlistAndExistingBillsUnaffected(t *testing.T) {
	f := newMonthlyFunctionalFixture(t)
	cookie := &http.Cookie{Name: portalSessionCookie, Value: f.m.signPortalSession(f.a.ID, f.a.PortalAuthVer, time.Now().Unix())}
	path := "/api/monthly-statement?month=" + f.latest
	before := portalDo(f.r, "GET", path, "", cookie)
	if before.Code != 200 {
		t.Fatal(before.Code, before.Body.String())
	}
	view, failure := MonthlyGenerationView{Month: f.latest, State: "pending_verification"}, error(nil)
	if err := f.m.RegisterLocalPortalMonthlyGenerationRoutes(f.r, monthlyGenerationTestService{func(context.Context, int64, int64, string, bool) (MonthlyGenerationView, error) { return view, failure }}); err != nil {
		t.Fatal(err)
	}
	w := generationHTTPDo(f.r, "GET", "?month="+f.latest, "", cookie, nil)
	if w.Code != 200 || strings.Contains(w.Body.String(), "download") || !strings.Contains(w.Body.String(), `"state":"available"`) {
		t.Fatal("saved bill not recognized", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "已生成并保存") || strings.Contains(w.Body.String(), "待核验后可查看") {
		t.Fatal("task status contradicted published bill", w.Body.String())
	}
	after := portalDo(f.r, "GET", path, "", cookie)
	if before.Body.String() != after.Body.String() {
		t.Fatal("generation status changed existing published bill")
	}
	// Use a genuinely unissued month to test untrusted service results. An
	// existing valid publication intentionally bypasses the source/task service.
	missing := time.Now().In(usageCST).AddDate(0, -8, 0).Format("2006-01")
	for _, bad := range []MonthlyGenerationView{{Month: f.latest, State: "published"}, {Month: "2026-01", State: "queued"}, {Month: f.latest, State: "<script>secret</script>"}} {
		view = bad
		if w := generationHTTPDo(f.r, "GET", "?month="+missing, "", cookie, nil); w.Code != 503 || strings.Contains(w.Body.String(), "secret") {
			t.Fatal("invalid service result escaped", w.Code)
		}
	}
	for _, err := range []error{errors.New("secret:mysql://private/1.819570"), MonthlyGenerationFailure("secret"), MonthlyGenerationFailure("needs_review")} {
		failure = err
		w := generationHTTPDo(f.r, "GET", "?month="+missing, "", cookie, nil)
		if w.Code < 400 || strings.Contains(w.Body.String(), "secret") || strings.Contains(w.Body.String(), "1.819570") {
			t.Fatal("raw error leaked", w.Body.String())
		}
	}
}

func TestMonthlyGenerationHTTPCustomerStatusCopyAndPayloadStayLimited(t *testing.T) {
	m, r, _, cookie := monthlyGenerationHTTPFixture(t)
	state := "not_requested"
	if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, monthlyGenerationTestService{func(_ context.Context, _, _ int64, month string, _ bool) (MonthlyGenerationView, error) {
		return MonthlyGenerationView{Month: month, State: state}, nil
	}}); err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ state, message string }{
		{"not_requested", "该月尚未提交生成申请。"},
		{"processing", "账单正在生成，请稍后查看。"},
		{"pending_verification", "请在账单列表查看生成结果；如需帮助请联系客服。"},
		{"needs_preparation", "该月账单暂未生成，请稍后查看或联系客服。"},
		{"needs_review", "该月账单暂未生成，请联系客服协助处理。"},
	} {
		state = tc.state
		w := generationHTTPDo(r, "GET", "?month=2026-09", "", cookie, nil)
		var body map[string]string
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &body) != nil || len(body) != 3 || body["month"] != "2026-09" || body["state"] != state || body["message"] != tc.message {
			t.Fatal("customer state, message or minimal response changed", w.Code, w.Body.String())
		}
		for _, internal := range []string{"生成任务", "尚未发布", "未发布的账单", "重新准备", "source", "quota", "sql", "task_id"} {
			if strings.Contains(w.Body.String(), internal) {
				t.Fatal("internal workflow escaped into customer progress", internal)
			}
		}
	}
}

func TestMonthlyGenerationHTTPBoundedConcurrentCalls(t *testing.T) {
	m, r, _, cookie := monthlyGenerationHTTPFixture(t)
	entered, release := make(chan struct{}, 4), make(chan struct{})
	var calls atomic.Int64
	service := monthlyGenerationTestService{func(ctx context.Context, _, _ int64, month string, _ bool) (MonthlyGenerationView, error) {
		calls.Add(1)
		entered <- struct{}{}
		select {
		case <-release:
			return MonthlyGenerationView{Month: month, State: "not_requested"}, nil
		case <-ctx.Done():
			return MonthlyGenerationView{}, ctx.Err()
		}
	}}
	if err := m.RegisterLocalPortalMonthlyGenerationRoutes(r, service); err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() { defer wg.Done(); generationHTTPDo(r, "GET", "?month=2026-09", "", cookie, nil) }()
	}
	defer func() { close(release); wg.Wait() }()
	for range 4 {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			t.Fatal("requests failed to enter bounded service")
		}
	}
	w := generationHTTPDo(r, "GET", "?month=2026-09", "", cookie, nil)
	if w.Code != 429 || calls.Load() != 4 || w.Header().Get("Retry-After") != "5" {
		t.Fatal("concurrency bound bypassed", w.Code, calls.Load())
	}
}
