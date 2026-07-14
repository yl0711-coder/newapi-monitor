package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newPortalTestMonitor(t *testing.T) (*Monitor, *gin.Engine, *gin.Engine) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	m := &Monitor{cfg: Settings{SessionSecret: "portal-test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(t.TempDir() + "/t.db"); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	admin := gin.New()
	m.RegisterRoutes(admin)
	portal := gin.New()
	m.RegisterPortalRoutes(portal)
	return m, admin, portal
}

func portalDo(r *gin.Engine, method, path, body string, cookies ...*http.Cookie) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	req := httptest.NewRequest(method, path, strings.NewReader(body))
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	req.Header.Set("Accept", "application/json")
	for _, c := range cookies {
		req.AddCookie(c)
	}
	r.ServeHTTP(w, req)
	return w
}

func portalCookie(w *httptest.ResponseRecorder) *http.Cookie {
	for _, c := range w.Result().Cookies() {
		if c.Name == portalSessionCookie {
			return c
		}
	}
	return nil
}

// 双密码:我方配置密码与客户自改密码都能登录;改密码后我方密码仍有效;错误密码拒绝。
func TestPortalDualPassword(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	h, _ := hashPassword("admin-pass-123")
	g := CustomerGroup{Name: "Acme", Stage: "active", PortalEmail: "acme@x.com", PortalPwAdmin: h}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}

	// 我方配置密码 → 登录成功
	w := portalDo(portal, "POST", "/login", `{"email":"acme@x.com","password":"admin-pass-123"}`)
	if w.Code != 200 || portalCookie(w) == nil {
		t.Fatalf("我方配置密码应能登录 = %d %s", w.Code, w.Body.String())
	}
	ck := portalCookie(w)

	// 错误密码 → 401
	if w := portalDo(portal, "POST", "/login", `{"email":"acme@x.com","password":"wrong"}`); w.Code != 401 {
		t.Fatalf("错误密码应 401 = %d", w.Code)
	}

	// 客户自改密码
	w = portalDo(portal, "POST", "/api/password", `{"old":"admin-pass-123","new":"customer-pass-456"}`, ck)
	if w.Code != 200 {
		t.Fatalf("改密码应成功 = %d %s", w.Code, w.Body.String())
	}
	// 新密码可登录
	if w := portalDo(portal, "POST", "/login", `{"email":"acme@x.com","password":"customer-pass-456"}`); w.Code != 200 {
		t.Fatalf("客户自改密码应能登录 = %d", w.Code)
	}
	// 我方配置密码【仍然有效】(双密码并存的核心约定)
	if w := portalDo(portal, "POST", "/login", `{"email":"acme@x.com","password":"admin-pass-123"}`); w.Code != 200 {
		t.Fatalf("我方配置密码必须始终有效 = %d", w.Code)
	}
	// 库里只有哈希,绝无明文
	var g2 CustomerGroup
	m.storeDB.First(&g2, g.ID)
	if strings.Contains(g2.PortalPwAdmin, "admin-pass") || strings.Contains(g2.PortalPwUser, "customer-pass") {
		t.Fatal("密码疑似明文入库")
	}
}

// 越权隔离:A 组会话查 B 组成员明细必须 404;管理端会话在客户端口无效。
func TestPortalScopeIsolation(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	ha, _ := hashPassword("password-aaa")
	ga := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: ha}
	gb := CustomerGroup{Name: "B公司"}
	m.storeDB.Create(&ga)
	m.storeDB.Create(&gb)
	m.storeDB.Create(&TrackedUser{UserID: 101, Username: "a-user", GroupID: ga.ID})
	m.storeDB.Create(&TrackedUser{UserID: 202, Username: "b-user", GroupID: gb.ID})

	w := portalDo(portal, "POST", "/login", `{"email":"a@x.com","password":"password-aaa"}`)
	ck := portalCookie(w)
	if ck == nil {
		t.Fatal("登录失败")
	}
	// 查他组成员 → 404(不暴露存在性)
	if w := portalDo(portal, "GET", "/api/user?uid=202", "", ck); w.Code != 404 {
		t.Fatalf("跨组查成员应 404 = %d", w.Code)
	}
	// 无会话 → 401
	if w := portalDo(portal, "GET", "/api/overview", ""); w.Code != 401 {
		t.Fatalf("无会话应 401 = %d", w.Code)
	}
	// 管理端会话 cookie 在客户端口无效(独立密钥域)
	adminTok := m.signSession("root", roleRoot, time.Now().Unix())
	if w := portalDo(portal, "GET", "/api/overview", "", &http.Cookie{Name: portalSessionCookie, Value: adminTok}); w.Code != 401 {
		t.Fatalf("管理端会话不应被客户端接受 = %d", w.Code)
	}
	// 账号被关闭后,旧会话立即失效
	m.storeDB.Model(&CustomerGroup{}).Where("id = ?", ga.ID).Update("portal_email", "")
	if w := portalDo(portal, "GET", "/api/overview", "", ck); w.Code != 401 {
		t.Fatalf("关闭账号后旧会话应失效 = %d", w.Code)
	}
}

// 写穿透预热隔离:管理端「全组」矩阵切片灌各组缓存时,A 组缓存只含 A 的成员/格,
// 绝不混入 B 组任何数据,且内部备注(Note)被剥除。这是所有组数据同时流经的唯一位置,切错即串组。
func TestPortalWarmFromMatrixIsolation(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	m.portalCache = newTTLCache()
	ga := CustomerGroup{Name: "A公司"}
	gb := CustomerGroup{Name: "B公司"}
	m.storeDB.Create(&ga)
	m.storeDB.Create(&gb)
	tracked := []TrackedUser{
		{UserID: 101, GroupID: ga.ID},
		{UserID: 102, GroupID: ga.ID},
		{UserID: 202, GroupID: gb.ID},
	}
	mx := &UsageMatrix{
		From: "2026-07-01", To: "2026-07-02", Days: []string{"2026-07-01"},
		Users: []UsageMatrixUser{
			{UserID: 101, Username: "a1", Note: "内部备注-A1", GroupID: ga.ID},
			{UserID: 102, Username: "a2", Note: "机密", GroupID: ga.ID},
			{UserID: 202, Username: "b1", Note: "内部备注-B1", GroupID: gb.ID},
		},
		Cells: []UsageMatrixCell{
			{UserID: 101, Date: "2026-07-01", CostUSD: 1},
			{UserID: 202, Date: "2026-07-01", CostUSD: 9},
		},
	}
	m.portalWarmFromMatrix(mx, tracked, 1751328000, 1751414400)

	raw, err := m.portalCache.Do(portalOverviewKey(ga.ID, 1751328000, 1751414400), portalCacheTTL,
		func() (any, error) { t.Fatal("A 组缓存未命中,预热未写入"); return nil, nil })
	if err != nil {
		t.Fatal(err)
	}
	p := raw.(*portalOverviewPayload)
	for _, u := range p.Users {
		if u.UserID == 202 {
			t.Fatalf("A 组缓存混入了 B 组成员 202")
		}
		if u.Note != "" {
			t.Fatalf("A 组缓存未剥除内部备注: uid=%d note=%q", u.UserID, u.Note)
		}
	}
	if len(p.Users) != 2 {
		t.Fatalf("A 组应恰好 2 名成员, 实得 %d", len(p.Users))
	}
	for _, cell := range p.Cells {
		if cell.UserID == 202 {
			t.Fatalf("A 组缓存混入了 B 组消费格 202")
		}
	}
}

// 登录限流:同 IP+邮箱连续失败达上限后 429。
func TestPortalLoginRateLimit(t *testing.T) {
	_, _, portal := newPortalTestMonitor(t)
	var last int
	for i := 0; i < portalLoginMaxFails+2; i++ {
		w := portalDo(portal, "POST", "/login", `{"email":"nobody@x.com","password":"bad"}`)
		last = w.Code
	}
	if last != http.StatusTooManyRequests {
		t.Fatalf("连续失败后应 429 = %d", last)
	}
}

// 管理端接口:开通校验(邮箱唯一/首次必须设密码/关闭清空)。
func TestSetGroupPortalValidation(t *testing.T) {
	m, admin, _ := newPortalTestMonitor(t)
	rootCk := &http.Cookie{Name: sessionCookie, Value: m.signSession("root", roleRoot, time.Now().Unix())}
	g1 := CustomerGroup{Name: "G1"}
	g2 := CustomerGroup{Name: "G2"}
	m.storeDB.Create(&g1)
	m.storeDB.Create(&g2)

	post := func(body string) *httptest.ResponseRecorder {
		return portalDo(admin, "POST", "/usage/groups/portal", body, rootCk)
	}
	// 首次开通不带密码 → 400
	if w := post(`{"id":` + itoa(g1.ID) + `,"email":"c@x.com"}`); w.Code != 400 {
		t.Fatalf("首次开通必须设密码 = %d %s", w.Code, w.Body.String())
	}
	// 正常开通
	if w := post(`{"id":` + itoa(g1.ID) + `,"email":"c@x.com","password":"12345678"}`); w.Code != 200 {
		t.Fatalf("开通失败 = %d %s", w.Code, w.Body.String())
	}
	// 邮箱跨组唯一
	if w := post(`{"id":` + itoa(g2.ID) + `,"email":"c@x.com","password":"12345678"}`); w.Code != 400 {
		t.Fatalf("邮箱跨组应唯一 = %d", w.Code)
	}
	// 关闭
	if w := post(`{"id":` + itoa(g1.ID) + `,"clear":true}`); w.Code != 200 {
		t.Fatalf("关闭失败 = %d", w.Code)
	}
	var g CustomerGroup
	m.storeDB.First(&g, g1.ID)
	if g.PortalEmail != "" || g.PortalPwAdmin != "" {
		t.Fatal("关闭后应清空账号")
	}
}

func itoa(v int64) string { b, _ := json.Marshal(v); return string(b) }

// 缓存:singleflight——同键并发只执行一次 fill;TTL 内命中不再执行。
func TestTTLCacheSingleflight(t *testing.T) {
	c := newTTLCache()
	var calls atomic.Int32
	fill := func() (any, error) {
		calls.Add(1)
		time.Sleep(50 * time.Millisecond)
		return "v", nil
	}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			v, err := c.Do("k", time.Second, fill)
			if err != nil || v != "v" {
				t.Errorf("Do = %v %v", v, err)
			}
		}()
	}
	wg.Wait()
	if calls.Load() != 1 {
		t.Fatalf("20 并发应只真正查询 1 次,实际 %d", calls.Load())
	}
	// TTL 内再取:仍不查询
	if _, err := c.Do("k", time.Second, fill); err != nil || calls.Load() != 1 {
		t.Fatalf("TTL 内应命中缓存,calls=%d err=%v", calls.Load(), err)
	}
}
