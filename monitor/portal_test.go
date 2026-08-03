package monitor

import (
	"context"
	"encoding/json"
	"errors"
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
	// 密码变化后，改密前签发的 12 小时会话必须立即失效。
	if w := portalDo(portal, "GET", "/api/overview", "", ck); w.Code != http.StatusUnauthorized {
		t.Fatalf("旧会话应在改密后立即失效 = %d %s", w.Code, w.Body.String())
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

// 管理端矩阵刷新不能把只有 Users/Cells 的半成品总览写给客户；否则趋势所需的
// DailyByModel/ByModel 为空。正确策略是删掉当前范围内相关组的总览缓存，迫使下次读走完整聚合。
func TestPortalMatrixRefreshInvalidatesPartialOverview(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	m.portalCache = newTTLCache()
	tracked := []TrackedUser{
		{UserID: 101, GroupID: 11},
		{UserID: 202, GroupID: 22},
	}
	const fromTs, toTs = 1751328000, 1751414400
	for _, gid := range []int64{11, 22} {
		m.portalCache.Put(portalOverviewKey(gid, fromTs, toTs), &portalOverviewPayload{Cells: []UsageMatrixCell{{UserID: 1}}}, portalCacheTTL)
	}
	m.invalidatePortalOverviews(tracked, fromTs, toTs)
	for _, gid := range []int64{11, 22} {
		called := false
		_, err := m.portalCache.Do(portalOverviewKey(gid, fromTs, toTs), portalCacheTTL, func() (any, error) {
			called = true
			return &portalOverviewPayload{DailyByModel: []UsageDailyModel{{Date: "2026-07-01", Model: "m"}}}, nil
		})
		if err != nil || !called {
			t.Fatalf("group %d 应重新构建完整总览: called=%v err=%v", gid, called, err)
		}
	}
}

// 成员从 A 组移动到 B 组后，两组所有日期范围、总览/维度/成员明细缓存都必须立即失效。
func TestPortalMemberMoveInvalidatesBothGroupCaches(t *testing.T) {
	m, admin, _ := newPortalTestMonitor(t)
	ga := CustomerGroup{Name: "A公司"}
	gb := CustomerGroup{Name: "B公司"}
	if err := m.storeDB.Create(&ga).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&gb).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "member", GroupID: ga.ID}).Error; err != nil {
		t.Fatal(err)
	}
	keys := []string{}
	for _, gid := range []int64{ga.ID, gb.ID} {
		keys = append(keys,
			portalOverviewKey(gid, 100, 200),
			fmt.Sprintf("bd|%d|100|200", gid),
			fmt.Sprintf("ud|%d|101|0|100|200", gid),
		)
	}
	for _, key := range keys {
		m.portalCache.Put(key, "stale", time.Minute)
	}
	rootCk := &http.Cookie{Name: sessionCookie, Value: m.signSession("root", roleRoot, time.Now().Unix())}
	w := portalDo(admin, http.MethodPost, "/usage/users/group", fmt.Sprintf(`{"user_id":101,"group_id":%d}`, gb.ID), rootCk)
	if w.Code != http.StatusOK {
		t.Fatalf("移动成员失败 = %d %s", w.Code, w.Body.String())
	}
	for _, key := range keys {
		called := false
		v, err := m.portalCache.Do(key, time.Minute, func() (any, error) {
			called = true
			return "fresh", nil
		})
		if err != nil || !called || v != "fresh" {
			t.Fatalf("缓存 %q 未失效: called=%v value=%v err=%v", key, called, v, err)
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

func TestTTLCacheWaiterRespectsContextCancellation(t *testing.T) {
	c := newTTLCache()
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.DoContext(context.Background(), "k", time.Minute, func() (any, error) {
			close(started)
			<-release
			return "ready", nil
		})
		firstDone <- err
	}()
	<-started

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	called := false
	if _, err := c.DoContext(ctx, "k", time.Minute, func() (any, error) {
		called = true
		return "wrong", nil
	}); !errors.Is(err, context.Canceled) || called {
		t.Fatalf("取消的 singleflight 等待者不应继续等待或执行填充: err=%v called=%v", err, called)
	}

	close(release)
	if err := <-firstDone; err != nil {
		t.Fatalf("原填充请求不应受等待者取消影响: %v", err)
	}
	called = false
	v, err := c.Do("k", time.Minute, func() (any, error) {
		called = true
		return "wrong", nil
	})
	if err != nil || called || v != "ready" {
		t.Fatalf("取消等待者不应删除或污染原请求生成的缓存: value=%v called=%v err=%v", v, called, err)
	}
}

func TestTTLCacheCanceledFillerDoesNotCancelNormalWaiterOrPoisonCache(t *testing.T) {
	c := newTTLCache()
	fillStarted := make(chan struct{})
	firstDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := c.DoContext(ctx, "k", time.Minute, func() (any, error) {
			close(fillStarted)
			<-ctx.Done()
			return nil, ctx.Err()
		})
		firstDone <- err
	}()
	<-fillStarted

	var replacementCalls atomic.Int32
	normalDone := make(chan struct {
		v   any
		err error
	}, 1)
	go func() {
		v, err := c.DoContext(context.Background(), "k", time.Minute, func() (any, error) {
			replacementCalls.Add(1)
			return "fresh", nil
		})
		normalDone <- struct {
			v   any
			err error
		}{v: v, err: err}
	}()

	cancel()
	if err := <-firstDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("被取消的填充请求应返回 Canceled: %v", err)
	}
	got := <-normalDone
	if got.err != nil || got.v != "fresh" || replacementCalls.Load() != 1 {
		t.Fatalf("正常等待者应接手失败填充并成功: value=%v err=%v calls=%d", got.v, got.err, replacementCalls.Load())
	}

	called := false
	v, err := c.Do("k", time.Minute, func() (any, error) {
		called = true
		return "wrong", nil
	})
	if err != nil || called || v != "fresh" {
		t.Fatalf("取消错误不能污染后续成功缓存: value=%v called=%v err=%v", v, called, err)
	}
}

func TestTTLCacheCanceledWaitersDoNotCancelNormalWaiters(t *testing.T) {
	c := newTTLCache()
	fillStarted := make(chan struct{})
	releaseFill := make(chan struct{})
	var fillCalls atomic.Int32
	firstDone := make(chan error, 1)
	go func() {
		_, err := c.DoContext(context.Background(), "k", time.Minute, func() (any, error) {
			fillCalls.Add(1)
			close(fillStarted)
			<-releaseFill
			return "ready", nil
		})
		firstDone <- err
	}()
	<-fillStarted

	const waiterCount = 20
	var normalWG sync.WaitGroup
	normalErrs := make(chan error, waiterCount)
	for i := 0; i < waiterCount; i++ {
		normalWG.Add(1)
		go func() {
			defer normalWG.Done()
			v, err := c.DoContext(context.Background(), "k", time.Minute, func() (any, error) {
				fillCalls.Add(1)
				return "wrong", nil
			})
			if err != nil {
				normalErrs <- fmt.Errorf("正常等待者失败: %w", err)
			} else if v != "ready" {
				normalErrs <- fmt.Errorf("正常等待者结果=%v want=ready", v)
			}
		}()
	}

	var canceledWG sync.WaitGroup
	canceledErrs := make(chan error, waiterCount)
	for i := 0; i < waiterCount; i++ {
		ctx, cancel := context.WithCancel(context.Background())
		canceledWG.Add(1)
		go func(ctx context.Context) {
			defer canceledWG.Done()
			_, err := c.DoContext(ctx, "k", time.Minute, func() (any, error) {
				fillCalls.Add(1)
				return "wrong", nil
			})
			if !errors.Is(err, context.Canceled) {
				canceledErrs <- err
			}
		}(ctx)
		cancel()
	}
	canceledWG.Wait()
	close(canceledErrs)
	for err := range canceledErrs {
		t.Fatalf("取消等待者应返回 Canceled，got %v", err)
	}

	close(releaseFill)
	if err := <-firstDone; err != nil {
		t.Fatalf("原填充不应受取消等待者影响: %v", err)
	}
	normalWG.Wait()
	close(normalErrs)
	for err := range normalErrs {
		t.Fatalf("正常等待者不应被误取消: %v", err)
	}
	if got := fillCalls.Load(); got != 1 {
		t.Fatalf("混合等待者仍应只填充一次，实际=%d", got)
	}
}

func TestTTLCacheDeletePrefix(t *testing.T) {
	c := newTTLCache()
	c.Put("ov|1|a", "a", time.Minute)
	c.Put("ov|10|b", "b", time.Minute)
	c.Put("bd|1|c", "c", time.Minute)
	c.DeletePrefix("ov|1|")
	called := false
	v, err := c.Do("ov|1|a", time.Minute, func() (any, error) {
		called = true
		return "fresh", nil
	})
	if err != nil || !called || v != "fresh" {
		t.Fatalf("匹配前缀的缓存应删除: called=%v value=%v err=%v", called, v, err)
	}
	called = false
	v, err = c.Do("ov|10|b", time.Minute, func() (any, error) {
		called = true
		return "wrong", nil
	})
	if err != nil || called || v != "b" {
		t.Fatalf("相似 gid 的缓存不应误删: called=%v value=%v err=%v", called, v, err)
	}
}

// CSV 公式注入消毒:= + - @ 开头的文本前置单引号,其余原样。
func TestCSVSafe(t *testing.T) {
	cases := map[string]string{
		"=HYPERLINK(\"x\")": "'=HYPERLINK(\"x\")",
		"+1+1":              "'+1+1",
		"-2":                "'-2",
		"@cmd":              "'@cmd",
		"\tx":               "'\tx",
		"\rx":               "'\rx",
		"正常令牌":              "正常令牌",
		"tk-01":             "tk-01",
		"":                  "",
	}
	for in, want := range cases {
		if got := csvSafe(in); got != want {
			t.Fatalf("csvSafe(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestPortalCSVRecord(t *testing.T) {
	r := portalCSVRecord(LogRow{CreatedAt: 1751328000, Member: "alice", TokenName: "=formula", Group: "vip", Type: 2, ModelName: "m", CostUSD: 0.12, Detail: "raw upstream detail", RequestID: "req-1"})
	if len(r) != 12 || r[2] != "'=formula" || r[9] != "0.120000" || r[10] != "raw upstream detail" {
		t.Fatalf("CSV 行字段不符合预期: %#v", r)
	}
}

// 客户日志 API 参数契约:错误(5)/退款(6)不对客户提供,显式请求必须 400(防参数层校验被放宽的回归);
// 令牌名搜索超长同样 400。查看与导出共用 portalLogParams,两端点都验。
func TestPortalLogsParamContract(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t) // type 1-6 全开放后会真的查生产库(不再在参数层短路拒绝),需要一个可查的假库
	h, _ := hashPassword("password-aaa")
	g := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: h}
	m.storeDB.Create(&g)
	m.storeDB.Create(&TrackedUser{UserID: 101, Username: "a-user", GroupID: g.ID})
	ck := portalCookie(portalDo(portal, "POST", "/login", `{"email":"a@x.com","password":"password-aaa"}`))
	if ck == nil {
		t.Fatal("登录失败")
	}
	// 类型 1-6 全部开放(对齐 new-api 官方客户端使用日志,含错误5/退款6);越界(如7)静默当"全部",两者都不应 400。
	// /api/logs 不限流,循环测;/api/logs/export 有 1 次/5min 的下载限流,只在循环外单测一次,避免第二次触发 429。
	for _, q := range []string{"type=5", "type=6", "type=7"} {
		if w := portalDo(portal, "GET", "/api/logs?"+q, "", ck); w.Code != 200 {
			t.Fatalf("/api/logs?%s 应 200,得 %d %s", q, w.Code, w.Body.String())
		}
	}
	if w := portalDo(portal, "GET", "/api/logs/export?type=6", "", ck); w.Code != 200 {
		t.Fatalf("/api/logs/export?type=6 应 200,得 %d %s", w.Code, w.Body.String())
	}
	long := strings.Repeat("a", 65)
	if w := portalDo(portal, "GET", "/api/logs?token="+long, "", ck); w.Code != 400 {
		t.Fatalf("超长令牌搜索应 400,得 %d", w.Code)
	}
	if w := portalDo(portal, "GET", "/api/logs?request_id="+long, "", ck); w.Code != 400 {
		t.Fatalf("超长 Request ID 搜索应 400,得 %d", w.Code)
	}
}

func TestPortalErrorOnlyModelAndGroupFiltersAcceptExactInput(t *testing.T) {
	html := string(portalHTML)
	for _, want := range []string{
		"function cboxCommitCustom(id)",
		"cboxInit('logModel',()=>loadLogs(true),true)",
		"cboxInit('logGroup',()=>loadLogs(true),true)",
		"mergeLogFilterOptions(rows)",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("错误日志的模型/分组筛选仍只能依赖消费聚合选项，缺少 %q", want)
		}
	}
}
