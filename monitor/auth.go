package monitor

import (
	"bytes"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/cookiejar"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

// auth.go:登录鉴权与角色分权。复用 new-api 用户身份——账密提交给 new-api 验证,
// 取回角色后由监控签发自己的会话。全程不改 new-api、不写主站,只调其只读接口。
//
// new-api 角色:1=普通用户 10=管理员 100=超级管理员(root)。
// 规则:仅 >=10 可登录;100 可改配置,10 仅看监控,<10 提示无权限。

const (
	roleAdmin = 10
	roleRoot  = 100
)

const sessionCookie = "newapi_monitor_session"
const sessionTTL = 12 * time.Hour

func randomSecret() string {
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "insecure-default-change-me"
	}
	return hex.EncodeToString(b)
}

// newapiAuth 用 new-api 账号密码验证身份,返回角色与显示名。
// ⚠️ 红线:账密只在内网转发给 new-api,绝不记录/打印。
func (m *Monitor) newapiAuth(username, password string) (role int, name string, err error) {
	base := strings.TrimRight(m.cfg.NewAPIBaseURL, "/")
	if base == "" {
		return 0, "", fmt.Errorf("未配置主站地址(MONITOR_NEWAPI_BASE_URL)")
	}
	jar, _ := cookiejar.New(nil)
	cl := &http.Client{Timeout: 10 * time.Second, Jar: jar}

	// 1) 登录(new-api 校验账密并下发会话 cookie)
	payload, _ := json.Marshal(map[string]string{"username": username, "password": password})
	resp, err := cl.Post(base+"/api/user/login", "application/json", bytes.NewReader(payload))
	if err != nil {
		return 0, "", fmt.Errorf("连接主站失败: %w", err)
	}
	var lr struct {
		Success bool   `json:"success"`
		Message string `json:"message"`
		Data    struct {
			Username    string `json:"username"`
			DisplayName string `json:"display_name"`
			Role        int    `json:"role"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&lr)
	resp.Body.Close()
	if !lr.Success {
		if lr.Message == "" {
			lr.Message = "用户名或密码错误"
		}
		return 0, "", fmt.Errorf("%s", lr.Message)
	}

	// new-api 登录响应已直接带回用户信息(含 role),优先用它——避免再打 /api/user/self。
	// /self 依赖会话 cookie:内网明文下 Secure cookie 不回发、经 CloudFront 时 Set-Cookie 可能被剥离,均会 401。
	if lr.Data.Role > 0 {
		n := lr.Data.DisplayName
		if n == "" {
			n = lr.Data.Username
		}
		if n == "" {
			n = username
		}
		return lr.Data.Role, n, nil
	}

	// 2) 兜底:登录响应未带角色时,再用会话取自身信息(含 role)
	req, _ := http.NewRequest(http.MethodGet, base+"/api/user/self", nil)
	resp2, err := cl.Do(req)
	if err != nil {
		return 0, "", fmt.Errorf("获取用户信息失败: %w", err)
	}
	defer resp2.Body.Close()
	var sr struct {
		Success bool `json:"success"`
		Data    struct {
			Username    string `json:"username"`
			DisplayName string `json:"display_name"`
			Role        int    `json:"role"`
		} `json:"data"`
	}
	_ = json.NewDecoder(resp2.Body).Decode(&sr)
	if !sr.Success {
		return 0, "", fmt.Errorf("获取用户信息失败")
	}
	n := sr.Data.DisplayName
	if n == "" {
		n = sr.Data.Username
	}
	if n == "" {
		n = username
	}
	return sr.Data.Role, n, nil
}

// ---- 监控自己的会话(签名 cookie)----

// signSession 生成会话令牌:base64(payload).hex(hmac)。payload = name|role|issuedUnix。
func (m *Monitor) signSession(name string, role int, nowUnix int64) string {
	p := fmt.Sprintf("%s|%d|%d", strings.ReplaceAll(name, "|", "/"), role, nowUnix)
	enc := base64.RawURLEncoding.EncodeToString([]byte(p))
	mac := hmac.New(sha256.New, []byte(m.cfg.SessionSecret))
	mac.Write([]byte(enc))
	return enc + "." + hex.EncodeToString(mac.Sum(nil))
}

// verifySession 校验并解析会话,返回 name/role。
func (m *Monitor) verifySession(token string, nowUnix int64) (name string, role int, ok bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return "", 0, false
	}
	mac := hmac.New(sha256.New, []byte(m.cfg.SessionSecret))
	mac.Write([]byte(parts[0]))
	if !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(parts[1])) {
		return "", 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return "", 0, false
	}
	f := strings.Split(string(raw), "|")
	if len(f) != 3 {
		return "", 0, false
	}
	role, _ = strconv.Atoi(f[1])
	issued, _ := strconv.ParseInt(f[2], 10, 64)
	if nowUnix-issued > int64(sessionTTL.Seconds()) {
		return "", 0, false // 过期
	}
	return f[0], role, true
}

func (m *Monitor) currentUser(c *gin.Context) (name string, role int, ok bool) {
	ck, err := c.Cookie(sessionCookie)
	if err != nil || ck == "" {
		return "", 0, false
	}
	return m.verifySession(ck, time.Now().Unix())
}

// requireRole 中间件:未登录→页面跳登录/接口 401;角色不足→403 无权限。
func (m *Monitor) requireRole(min int) gin.HandlerFunc {
	return func(c *gin.Context) {
		name, role, ok := m.currentUser(c)
		wantsJSON := strings.Contains(c.GetHeader("Accept"), "json")
		if !ok {
			if wantsJSON {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "未登录", "login_required": true})
				return
			}
			c.Redirect(http.StatusFound, "/login")
			c.Abort()
			return
		}
		if role < min {
			if wantsJSON {
				c.AbortWithStatusJSON(http.StatusForbidden, gin.H{"error": "无权限"})
				return
			}
			c.Data(http.StatusForbidden, "text/html; charset=utf-8", []byte(noPermHTML))
			c.Abort()
			return
		}
		c.Set("uname", name)
		c.Set("urole", role)
		c.Next()
	}
}

// ---- 处理器 ----

func (m *Monitor) loginPage(c *gin.Context) {
	if _, _, ok := m.currentUser(c); ok {
		c.Redirect(http.StatusFound, "/")
		return
	}
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(loginHTML))
}

func (m *Monitor) loginSubmit(c *gin.Context) {
	var in struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.Username == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请输入用户名和密码"})
		return
	}
	role, name, err := m.newapiAuth(in.Username, in.Password)
	if err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": err.Error()})
		return
	}
	if role < roleAdmin {
		c.JSON(http.StatusForbidden, gin.H{"error": "该账号无权限访问监控(需管理员及以上)"})
		return
	}
	tok := m.signSession(name, role, time.Now().Unix())
	secure := c.Request.TLS != nil || c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetCookie(sessionCookie, tok, int(sessionTTL.Seconds()), "/", "", secure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true, "name": name, "role": role})
}

func logout(c *gin.Context) {
	c.SetCookie(sessionCookie, "", -1, "/", "", false, true)
	if strings.Contains(c.GetHeader("Accept"), "json") {
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	c.Redirect(http.StatusFound, "/login")
}

// me 返回当前登录者(页面据此显示用户名、决定是否显示「报警设置」入口)。
func me(c *gin.Context) {
	name := c.GetString("uname")
	role := c.GetInt("urole")
	c.JSON(http.StatusOK, gin.H{"name": name, "role": role, "is_root": role >= roleRoot})
}

const noPermHTML = `<!DOCTYPE html><html lang="zh-CN"><head><meta charset="UTF-8">
<title>无权限</title><style>body{background:#0f1117;color:#e2e8f0;font-family:-apple-system,sans-serif;
display:flex;align-items:center;justify-content:center;height:100vh;margin:0}
.box{text-align:center}h1{font-size:48px;margin:0}p{color:#94a3b8}a{color:#6366f1}</style></head>
<body><div class="box"><h1>🚫</h1><p>您的账号无权访问此页面</p><p><a href="/logout">退出登录</a></p></div></body></html>`
