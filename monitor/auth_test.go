package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
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
	// 管理员:/data 可(200),/alert 不可(403)
	if c := do("/data", "admin"); c != 200 {
		t.Errorf("管理员 /data 应 200,实际 %d", c)
	}
	if c := do("/alert", "admin"); c != 403 {
		t.Errorf("管理员 /alert 应 403,实际 %d", c)
	}
	// 超管:/alert 可(200)
	if c := do("/alert", "root"); c != 200 {
		t.Errorf("超管 /alert 应 200,实际 %d", c)
	}
}
