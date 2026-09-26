package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// mockNewAPI:登录置 session=用户名 cookie;/self 据 cookie 返回角色。
func mockNewAPI() *httptest.Server {
	roles := map[string]int{"root": 100, "admin": 10, "user": 1}
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		var in struct{ Username, Password string }
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in.Password != "good" {
			_ = json.NewEncoder(w).Encode(map[string]any{"success": false, "message": "用户名或密码错误"})
			return
		}
		http.SetCookie(w, &http.Cookie{Name: "session", Value: in.Username, Path: "/"})
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		ck, _ := r.Cookie("session")
		u := ""
		if ck != nil {
			u = ck.Value
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true,
			"data": map[string]any{"username": u, "role": roles[u]}})
	})
	return httptest.NewServer(mux)
}

// 端到端:POST /login 拿 cookie → 用 cookie 访问受限页。
func TestLoginFlow(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := mockNewAPI()
	defer srv.Close()
	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL, SessionSecret: "e2e-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	m.RegisterRoutes(r)

	login := func(user string) (*http.Cookie, int) {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"`+user+`","password":"good"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		for _, ck := range w.Result().Cookies() {
			if ck.Name == sessionCookie {
				return ck, w.Code
			}
		}
		return nil, w.Code
	}

	// 普通用户:登录被拒(403),无 cookie
	if ck, code := login("user"); ck != nil || code != 403 {
		t.Errorf("普通用户应 403 无 cookie,实际 code=%d ck=%v", code, ck)
	}
	// 超管:登录成功拿 cookie,带 cookie 访问 /alert → 200
	ck, code := login("root")
	if ck == nil || code != 200 {
		t.Fatalf("超管登录失败 code=%d", code)
	}
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/alert", nil)
	req.AddCookie(ck)
	r.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Errorf("超管带 cookie 访问 /alert 应 200,实际 %d", w.Code)
	}
}

// 管理端登录按来源 IP 限制失败次数，既保护自身也避免被用作对主站的爆破转发器；
// 登录请求体独立限制为 64KiB，不能借全局批量上报上限绕过。
func TestAdminLoginGuards(t *testing.T) {
	gin.SetMode(gin.TestMode)
	srv := mockNewAPI()
	defer srv.Close()
	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL, SessionSecret: "guard-secret"}, chNames: map[string]string{}}
	r := gin.New()
	m.RegisterRoutes(r)

	big := `{"username":"` + strings.Repeat("a", maxLoginRequestBody) + `","password":"x"}`
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(big))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("超大登录请求应 413，得 %d", w.Code)
	}

	for i := 0; i < portalLoginMaxFails; i++ {
		w = httptest.NewRecorder()
		req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"root","password":"bad"}`))
		req.Header.Set("Content-Type", "application/json")
		r.ServeHTTP(w, req)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("第 %d 次失败登录应 401，得 %d", i+1, w.Code)
		}
	}
	w = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"root","password":"bad"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusTooManyRequests {
		t.Fatalf("达到失败上限后应 429，得 %d", w.Code)
	}
}

func TestNewapiAuth(t *testing.T) {
	srv := mockNewAPI()
	defer srv.Close()
	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL}}

	if role, name, err := m.newapiAuth("root", "good"); err != nil || role != 100 || name != "root" {
		t.Errorf("root 登录: role=%d name=%q err=%v", role, name, err)
	}
	if role, _, err := m.newapiAuth("admin", "good"); err != nil || role != 10 {
		t.Errorf("admin 登录: role=%d err=%v", role, err)
	}
	if _, _, err := m.newapiAuth("admin", "bad"); err == nil || !strings.Contains(err.Error(), "密码") {
		t.Errorf("错误密码应失败: err=%v", err)
	}
}

func TestNewapiAuthRC26NestedUser(t *testing.T) {
	selfCalled := false
	type requestSnapshot struct {
		Method        string
		Path          string
		Authorization string
		UserAgent     string
	}
	revoked := make(chan requestSnapshot, 1)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		if got := r.UserAgent(); got != newAPIAuthUserAgent {
			t.Errorf("login User-Agent=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"access_token": "rc26-access-token",
				"session":      map[string]any{"sid": "rc26-session-id"},
				"user": map[string]any{
					"username":     "root",
					"display_name": "RC26 管理员",
					"role":         100,
				},
			},
		})
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		selfCalled = true
		w.WriteHeader(http.StatusUnauthorized)
	})
	mux.HandleFunc("/api/user/sessions/rc26-session-id", func(w http.ResponseWriter, r *http.Request) {
		revoked <- requestSnapshot{
			Method:        r.Method,
			Path:          r.URL.Path,
			Authorization: r.Header.Get("Authorization"),
			UserAgent:     r.UserAgent(),
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL}}
	role, name, err := m.newapiAuth("root", "good")
	if err != nil || role != 100 || name != "RC26 管理员" {
		t.Fatalf("RC26 登录: role=%d name=%q err=%v", role, name, err)
	}
	if selfCalled {
		t.Fatal("RC26 登录响应已带用户角色，不应再请求 /api/user/self")
	}
	select {
	case got := <-revoked:
		if got.Method != http.MethodDelete || got.Path != "/api/user/sessions/rc26-session-id" {
			t.Fatalf("临时会话回收请求=%s %s", got.Method, got.Path)
		}
		if got.Authorization != "Bearer rc26-access-token" {
			t.Fatalf("临时会话回收 Authorization=%q", got.Authorization)
		}
		if got.UserAgent != newAPIAuthUserAgent {
			t.Fatalf("临时会话回收 User-Agent=%q", got.UserAgent)
		}
	default:
		t.Fatal("RC26 登录成功后未回收本次临时会话")
	}
}

func TestNewapiAuthBearerFallback(t *testing.T) {
	var revokeCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"access_token": "fallback-access-token",
				"session":      map[string]any{"sid": "fallback-session-id"},
			},
		})
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer fallback-access-token" {
			t.Fatalf("Authorization=%q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"username": "admin", "role": 10},
		})
	})
	mux.HandleFunc("/api/user/sessions/fallback-session-id", func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer fallback-access-token" {
			t.Errorf("revoke Authorization=%q", got)
		}
		revokeCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL}}
	role, name, err := m.newapiAuth("admin", "good")
	if err != nil || role != 10 || name != "admin" {
		t.Fatalf("Bearer fallback: role=%d name=%q err=%v", role, name, err)
	}
	if got := revokeCalls.Load(); got != 1 {
		t.Fatalf("fallback 会话回收次数=%d", got)
	}
}

func TestNewapiAuthRC26CleanupFailureDoesNotRejectValidLogin(t *testing.T) {
	var revokeCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"access_token": "valid-access-token",
				"session":      map[string]any{"sid": "temporary-session-id"},
				"user":         map[string]any{"username": "root", "role": 100},
			},
		})
	})
	mux.HandleFunc("/api/user/sessions/temporary-session-id", func(w http.ResponseWriter, _ *http.Request) {
		revokeCalls.Add(1)
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL}}
	role, name, err := m.newapiAuth("root", "good")
	if err != nil || role != 100 || name != "root" {
		t.Fatalf("回收失败不应否定已校验的登录: role=%d name=%q err=%v", role, name, err)
	}
	if got := revokeCalls.Load(); got != 1 {
		t.Fatalf("回收失败时不应自动重试，调用次数=%d", got)
	}
}

func TestRevokeTemporaryNewAPISessionDoesNotExposeSIDOnTransportError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	base := srv.URL
	srv.Close()

	const sid = "secret-session-id-must-not-be-logged"
	err := revokeTemporaryNewAPISession(&http.Client{Timeout: time.Second}, base, "secret-token", sid)
	if err == nil {
		t.Fatal("已关闭的服务端应返回传输错误")
	}
	if strings.Contains(err.Error(), sid) || strings.Contains(err.Error(), "secret-token") {
		t.Fatalf("回收错误泄露了认证材料: %q", err)
	}
}

func TestLoginFlowRC26UnauthorizedRoleStillRevokesTemporarySession(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var revokeCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"access_token": "ordinary-user-token",
				"session":      map[string]any{"sid": "ordinary-user-session"},
				"user":         map[string]any{"username": "ordinary", "role": 1},
			},
		})
	})
	mux.HandleFunc("/api/user/sessions/ordinary-user-session", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodDelete || r.Header.Get("Authorization") != "Bearer ordinary-user-token" {
			http.Error(w, "invalid revoke", http.StatusBadRequest)
			return
		}
		revokeCalls.Add(1)
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL, SessionSecret: "unauthorized-secret"}, chNames: map[string]string{}}
	r := gin.New()
	m.RegisterRoutes(r)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/login", strings.NewReader(`{"username":"ordinary","password":"good"}`))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusForbidden {
		t.Fatalf("普通用户登录 Monitor 应返回 403，实际=%d", w.Code)
	}
	if got := revokeCalls.Load(); got != 1 {
		t.Fatalf("无权用户的 NewAPI 临时会话也必须回收，实际=%d", got)
	}
	for _, cookie := range w.Result().Cookies() {
		if cookie.Name == sessionCookie && cookie.Value != "" {
			t.Fatal("无权用户不应获得 Monitor 会话")
		}
	}
}

func TestNewapiAuthRC4DoesNotCallSessionEndpoint(t *testing.T) {
	var sessionCalls atomic.Int32
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, r *http.Request) {
		http.SetCookie(w, &http.Cookie{Name: "session", Value: "root", Path: "/"})
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})
	mux.HandleFunc("/api/user/self", func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data":    map[string]any{"username": "root", "role": 100},
		})
	})
	mux.HandleFunc("/api/user/sessions/", func(w http.ResponseWriter, _ *http.Request) {
		sessionCalls.Add(1)
		http.Error(w, "unexpected", http.StatusInternalServerError)
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL}}
	role, name, err := m.newapiAuth("root", "good")
	if err != nil || role != 100 || name != "root" {
		t.Fatalf("RC4 登录: role=%d name=%q err=%v", role, name, err)
	}
	if got := sessionCalls.Load(); got != 0 {
		t.Fatalf("RC4 不应调用会话回收接口，实际=%d", got)
	}
}

func TestNewapiAuthRC26ConcurrentSessionsAreRevokedExactly(t *testing.T) {
	const workers = 24
	var issued atomic.Int64
	var mu sync.Mutex
	revoked := make(map[string]int, workers)
	mux := http.NewServeMux()
	mux.HandleFunc("/api/user/login", func(w http.ResponseWriter, _ *http.Request) {
		id := issued.Add(1)
		sid := fmt.Sprintf("session-%d", id)
		token := fmt.Sprintf("token-%d", id)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"success": true,
			"data": map[string]any{
				"access_token": token,
				"session":      map[string]any{"sid": sid},
				"user":         map[string]any{"username": "root", "role": 100},
			},
		})
	})
	mux.HandleFunc("/api/user/sessions/", func(w http.ResponseWriter, r *http.Request) {
		sid := strings.TrimPrefix(r.URL.Path, "/api/user/sessions/")
		wantToken := "Bearer " + strings.Replace(sid, "session-", "token-", 1)
		if r.Method != http.MethodDelete || r.Header.Get("Authorization") != wantToken {
			http.Error(w, "mismatched session", http.StatusBadRequest)
			return
		}
		mu.Lock()
		revoked[sid]++
		mu.Unlock()
		_ = json.NewEncoder(w).Encode(map[string]any{"success": true})
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	m := &Monitor{cfg: Settings{NewAPIBaseURL: srv.URL}}
	errCh := make(chan error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			role, _, err := m.newapiAuth("root", "good")
			if err != nil {
				errCh <- fmt.Errorf("并发登录失败: %w", err)
				return
			}
			if role != 100 {
				errCh <- fmt.Errorf("并发登录角色=%d，期望=100", role)
			}
		}()
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Error(err)
	}
	if got := issued.Load(); got != workers {
		t.Fatalf("创建会话数=%d，期望=%d", got, workers)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(revoked) != workers {
		t.Fatalf("精确回收会话数=%d，期望=%d", len(revoked), workers)
	}
	for sid, count := range revoked {
		if count != 1 {
			t.Errorf("会话 %s 回收次数=%d", sid, count)
		}
	}
}

func TestSession(t *testing.T) {
	m := &Monitor{cfg: Settings{SessionSecret: "secret-a"}}
	now := time.Now().Unix()
	tok := m.signSession("张三", roleRoot, now)

	if name, role, ok := m.verifySession(tok, now); !ok || name != "张三" || role != roleRoot {
		t.Fatalf("正常会话校验失败: %q %d %v", name, role, ok)
	}
	// 篡改 → 失败
	if _, _, ok := m.verifySession(tok+"x", now); ok {
		t.Error("篡改的会话不应通过")
	}
	// 换密钥 → 失败
	m.cfg.SessionSecret = "secret-b"
	if _, _, ok := m.verifySession(tok, now); ok {
		t.Error("换密钥后旧会话不应通过")
	}
	m.cfg.SessionSecret = "secret-a"
	// 过期 → 失败
	if _, _, ok := m.verifySession(tok, now+int64(sessionTTL.Seconds())+10); ok {
		t.Error("过期会话不应通过")
	}
}

// 角色门禁:未登录→跳转/401;管理员→可看监控但不可进配置;超管→可进配置。
func TestRoleGating(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{SessionSecret: "secret-gate"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	m.RegisterRoutes(r)
	now := time.Now().Unix()

	do := func(path, role string) int {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", "application/json")
		switch role {
		case "admin":
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("a", roleAdmin, now)})
		case "root":
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("r", roleRoot, now)})
		}
		r.ServeHTTP(w, req)
		return w.Code
	}

	// 未登录:/data 401(带 Accept json),/alert 也 401
	if c := do("/data", "none"); c != 401 {
		t.Errorf("未登录 /data 应 401,实际 %d", c)
	}
	if c := do("/stability/report", "none"); c != 401 {
		t.Errorf("未登录 /stability/report 应 401,实际 %d", c)
	}
	if c := do("/stability/problems", "none"); c != 401 {
		t.Errorf("未登录 /stability/problems 应 401,实际 %d", c)
	}
	if c := do("/channels/report", "none"); c != 401 {
		t.Errorf("未登录 /channels/report 应 401,实际 %d", c)
	}
	if c := do("/channels/economics?hours=24", "none"); c != 401 {
		t.Errorf("未登录 /channels/economics 应 401,实际 %d", c)
	}
	if c := do("/capacity/report", "none"); c != 401 {
		t.Errorf("未登录 /capacity/report 应 401,实际 %d", c)
	}
	if c := do("/sync/overview", "none"); c != 401 {
		t.Errorf("未登录 /sync/overview 应 401,实际 %d", c)
	}
	// 管理员:/data 可(200),/alert 不可(403)
	if c := do("/data", "admin"); c != 200 {
		t.Errorf("管理员 /data 应 200,实际 %d", c)
	}
	if c := do("/stability/report", "admin"); c != 200 {
		t.Errorf("管理员 /stability/report 应 200,实际 %d", c)
	}
	if c := do("/stability/problems", "admin"); c != 200 {
		t.Errorf("管理员 /stability/problems 应 200,实际 %d", c)
	}
	if c := do("/channels/report", "admin"); c != 200 {
		t.Errorf("管理员 /channels/report 应 200,实际 %d", c)
	}
	if c := do("/channels/economics?hours=24", "admin"); c != 200 {
		t.Errorf("管理员 /channels/economics 应 200,实际 %d", c)
	}
	if c := do("/capacity/report", "admin"); c != 200 {
		t.Errorf("管理员 /capacity/report 应 200,实际 %d", c)
	}
	if c := do("/sync/overview", "admin"); c != 200 {
		t.Errorf("管理员 /sync/overview 应 200,实际 %d", c)
	}
	if c := do("/sync/workloads?domain=usage", "admin"); c != 200 {
		t.Errorf("管理员 /sync/workloads 应 200,实际 %d", c)
	}
	if c := do("/alert", "admin"); c != 403 {
		t.Errorf("管理员 /alert 应 403,实际 %d", c)
	}
	// 超管:/alert 可(200)
	if c := do("/alert", "root"); c != 200 {
		t.Errorf("超管 /alert 应 200,实际 %d", c)
	}
}

func TestLocalSnapshotAuthBypassNeedsNoCookie(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{LocalSnapshotOnly: true, LocalAuthBypass: true, AlertsDisabled: true}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	m.RegisterRoutes(r)

	for _, path := range []string{"/", "/data", "/channels/report", "/alert"} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", "application/json")
		r.ServeHTTP(w, req)
		if w.Code == http.StatusFound || w.Code == http.StatusUnauthorized || w.Code == http.StatusForbidden {
			t.Fatalf("本地快照免登录访问 %s 被鉴权拦截: %d", path, w.Code)
		}
	}
}
