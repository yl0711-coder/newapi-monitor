package monitor

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type portalFailAfterWriter struct {
	dst       io.Writer
	remaining int
}

func (w *portalFailAfterWriter) Write(payload []byte) (int, error) {
	if w.remaining <= 0 {
		return 0, syscall.ENOSPC
	}
	if len(payload) <= w.remaining {
		n, err := w.dst.Write(payload)
		w.remaining -= n
		return n, err
	}
	n, err := w.dst.Write(payload[:w.remaining])
	w.remaining -= n
	if err != nil {
		return n, err
	}
	return n, syscall.ENOSPC
}

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

func TestCustomerGroupStageFeatureRemovedWithoutDroppingLegacyColumns(t *testing.T) {
	m, admin, _ := newPortalTestMonitor(t)
	root := &http.Cookie{Name: sessionCookie, Value: m.signSession("root", roleRoot, time.Now().Unix())}
	w := portalDo(admin, http.MethodPost, "/usage/groups", `{"name":"无状态客户","note":"只保留客户资料","stage":"trial","trial_end":9999999999}`, root)
	if w.Code != http.StatusOK {
		t.Fatalf("create group: %d %s", w.Code, w.Body.String())
	}
	var stored CustomerGroup
	if err := m.storeDB.Where("name = ?", "无状态客户").First(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if stored.Stage != "active" || stored.TrialEnd != 0 {
		t.Fatalf("legacy columns must stay inert: %+v", stored)
	}
	listed := portalDo(admin, http.MethodGet, "/usage/groups", "", root)
	if listed.Code != http.StatusOK || strings.Contains(listed.Body.String(), `"stage"`) || strings.Contains(listed.Body.String(), `"trial_end"`) {
		t.Fatalf("removed status fields leaked from list API: %d %s", listed.Code, listed.Body.String())
	}
	if old := portalDo(admin, http.MethodPost, "/usage/groups/stage", `{"id":1,"stage":"trial"}`, root); old.Code != http.StatusNotFound {
		t.Fatalf("removed stage endpoint should be 404, got %d %s", old.Code, old.Body.String())
	}
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

func TestPortalPartialMemberIsHiddenFromOverviewDetailLogsAndExport(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	enableUsageFactsForTest(m)

	hash, _ := hashPassword("partial-member-password")
	group := CustomerGroup{Name: "Partial Gate", PortalEmail: "partial@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	for _, member := range []TrackedUser{
		{UserID: 101, Username: "serving-member", Email: "serving@example.test", GroupID: group.ID},
		{UserID: 202, Username: "partial-secret-member", Email: "partial-secret@example.test", GroupID: group.ID},
	} {
		if err := m.storeDB.Create(&member).Error; err != nil {
			t.Fatal(err)
		}
	}
	day := time.Date(2026, 8, 2, 0, 0, 0, 0, usageCST).Unix()
	for _, fact := range []UsageDailyFact{
		{DateTs: day, UserID: 101, ChannelID: 1, Grp: "g", ModelName: "serving-model", Requests: 1, ConsumeQuota: 100},
		// 刻意预置 partial 用户的候选 facts：Portal 必须依然看不见。
		{DateTs: day, UserID: 202, ChannelID: 1, Grp: "g", ModelName: "partial-secret-model", Requests: 9, ConsumeQuota: 900},
	} {
		if err := m.usageFactsStore().Create(&fact).Error; err != nil {
			t.Fatal(err)
		}
	}
	seedPublishedUsageFactsForTest(t, m, []int64{101}, day, day+usageFactDaySeconds)
	if err := m.usageFactsStore().Model(&UsageFactPublishedMember{}).Where("user_id = ?", 101).
		Update("tracked_revision", 1).Error; err != nil {
		t.Fatal(err)
	}
	for _, statement := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'serving-member','serving@example.test',1000,100),(202,'partial-secret-member','partial-secret@example.test',2000,200)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,username,content) VALUES (1,101,%d,2,'serving-log-model',100,'serving-member','serving-detail'),(2,202,%d,2,'partial-secret-log-model',900,'partial-secret-member','partial-secret-detail')", day+12*3600, day+13*3600),
	} {
		if _, err := m.prodDB.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"partial@test.local","password":"partial-member-password"}`)
	cookie := portalCookie(login)
	if cookie == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	for _, assertion := range []struct {
		path       string
		wantStatus int
		want       string
		forbidden  string
	}{
		{"/api/overview?from=2026-08-02&to=2026-08-02", http.StatusOK, "serving-member", "partial-secret"},
		{"/api/user?uid=202&from=2026-08-02&to=2026-08-02", http.StatusNotFound, "not found", "partial-secret"},
		{"/api/logs?member=202&from=2026-08-02&to=2026-08-02", http.StatusNotFound, "not found", "partial-secret"},
		{"/api/logs?from=2026-08-02&to=2026-08-02", http.StatusOK, "serving-log-model", "partial-secret"},
	} {
		response := portalDo(portal, http.MethodGet, assertion.path, "", cookie)
		if response.Code != assertion.wantStatus || !strings.Contains(response.Body.String(), assertion.want) || strings.Contains(response.Body.String(), assertion.forbidden) {
			t.Fatalf("Portal partial 隔离失败 path=%s status=%d body=%s", assertion.path, response.Code, response.Body.String())
		}
	}

	prepared := portalDo(portal, http.MethodGet, "/api/logs/export/prepare?from=2026-08-02&to=2026-08-02", "", cookie)
	var ticket struct {
		Ticket string `json:"ticket"`
		Total  int64  `json:"total"`
	}
	if prepared.Code != http.StatusOK || json.Unmarshal(prepared.Body.Bytes(), &ticket) != nil || ticket.Ticket == "" || ticket.Total != 1 {
		t.Fatalf("导出预检未严格使用 serving 名单: %d %s", prepared.Code, prepared.Body.String())
	}
	download := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+ticket.Ticket, "", cookie)
	if download.Code != http.StatusOK || !strings.Contains(download.Body.String(), "serving-log-model") || strings.Contains(download.Body.String(), "partial-secret") {
		t.Fatalf("CSV 暴露 partial 成员: %d %s", download.Code, download.Body.String())
	}
}

func TestPortalOverviewDiscardsInFlightResponseAfterMemberRemoval(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	hash, _ := hashPassword("inflight-password")
	group := CustomerGroup{Name: "Inflight Gate", PortalEmail: "inflight@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 303, Username: "inflight-secret-member", GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (303,'inflight-secret-member','inflight@example.test',1000,100)"); err != nil {
		t.Fatal(err)
	}
	cookie := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"inflight@test.local","password":"inflight-password"}`))
	if cookie == nil {
		t.Fatal("登录失败")
	}

	blocked := make(chan struct{})
	release := make(chan struct{})
	var groupQueries atomic.Int64
	callbackName := "test:block_portal_final_auth"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "customer_groups" && groupQueries.Add(1) == 4 {
			close(blocked)
			<-release
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(callbackName) })

	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responseCh <- portalDo(portal, http.MethodGet, "/api/overview?from=2026-08-02&to=2026-08-02", "", cookie)
	}()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("未在 Portal 返回前命中末端权限复核 barrier")
	}
	if _, err := m.removeUsageMember(context.Background(), 303, usageMemberMutationMeta{RequestID: "inflight-remove"}); err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case response := <-responseCh:
		if response.Code != http.StatusOK || strings.Contains(response.Body.String(), "inflight-secret") {
			t.Fatalf("成员删除后不得发送在途旧响应: %d %s", response.Code, response.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Portal 权限复核重试死锁")
	}
}

func TestPortalOverviewDiscardsInFlightResponseAfterAuthVersionChange(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	hash, _ := hashPassword("inflight-auth-password")
	group := CustomerGroup{Name: "Inflight Auth", PortalEmail: "inflight-auth@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 304, Username: "auth-secret-member", GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (304,'auth-secret-member','auth@example.test',1000,100)"); err != nil {
		t.Fatal(err)
	}
	cookie := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"inflight-auth@test.local","password":"inflight-auth-password"}`))
	if cookie == nil {
		t.Fatal("登录失败")
	}

	blocked := make(chan struct{})
	release := make(chan struct{})
	var groupQueries atomic.Int64
	callbackName := "test:block_portal_final_auth_version"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if tx.Statement != nil && tx.Statement.Table == "customer_groups" && groupQueries.Add(1) == 4 {
			close(blocked)
			<-release
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(callbackName) })

	responseCh := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		responseCh <- portalDo(portal, http.MethodGet, "/api/overview?from=2026-08-02&to=2026-08-02", "", cookie)
	}()
	select {
	case <-blocked:
	case <-time.After(3 * time.Second):
		t.Fatal("未命中 Portal AuthVer 末端复核 barrier")
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", group.ID).
		Update("portal_auth_ver", gorm.Expr("portal_auth_ver + 1")).Error; err != nil {
		t.Fatal(err)
	}
	close(release)
	select {
	case response := <-responseCh:
		if response.Code != http.StatusUnauthorized || strings.Contains(response.Body.String(), "auth-secret") {
			t.Fatalf("AuthVer 改变后不得发送在途旧响应: %d %s", response.Code, response.Body.String())
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Portal AuthVer 末端复核死锁")
	}
}

func TestPortalAggregateGuardRejectsOldResultAcrossServingGenerationABA(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	hash, _ := hashPassword("serving-generation-password")
	group := CustomerGroup{Name: "Serving Generation Gate", PortalEmail: "serving-generation@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 305, Username: "generation-secret-old", GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}

	router := gin.New()
	var calls atomic.Int64
	guarded := m.portalAggregateAuthorizationGuard(func(c *gin.Context) {
		if calls.Add(1) == 1 {
			// Model an audit revoke followed by a fast repair/re-publish ABA: the
			// final member fingerprint is unchanged, but the old aggregate must not
			// be committed after ServingGeneration advances.
			c.JSON(http.StatusOK, gin.H{"value": "generation-secret-old"})
			if err := m.usageFactsStore().Model(&UsageFactSyncState{}).Where("id = 1").Updates(map[string]any{
				"serving_generation": int64(1), "published_fingerprint": "aba-publication-v2",
			}).Error; err != nil {
				t.Error(err)
			}
			m.usageFactsServingRevision.Store(1)
			return
		}
		c.JSON(http.StatusOK, gin.H{"value": "generation-safe-new"})
	})
	router.GET("/guarded", func(c *gin.Context) {
		c.Set("portalGID", group.ID)
		c.Set("portalAuthVer", group.PortalAuthVer)
		guarded(c)
	})

	response := portalDo(router, http.MethodGet, "/guarded", "")
	if response.Code != http.StatusOK || !strings.Contains(response.Body.String(), "generation-safe-new") ||
		strings.Contains(response.Body.String(), "generation-secret-old") || calls.Load() != 2 {
		t.Fatalf("serving-generation ABA leaked buffered old response: status=%d calls=%d body=%s",
			response.Code, calls.Load(), response.Body.String())
	}
}

func TestUsageCacheStatsRequireAdminSessionAndContainNoBusinessData(t *testing.T) {
	m, admin, _ := newPortalTestMonitor(t)
	if w := portalDo(admin, http.MethodGet, "/usage/cache-stats", ""); w.Code != http.StatusUnauthorized {
		t.Fatalf("未登录访问缓存指标应返回 401，得到 %d %s", w.Code, w.Body.String())
	}
	ck := &http.Cookie{Name: sessionCookie, Value: m.signSession("admin", roleAdmin, time.Now().Unix())}
	w := portalDo(admin, http.MethodGet, "/usage/cache-stats", "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("管理员读取缓存指标失败: %d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	cache, ok := body["cache"].(map[string]any)
	if !ok || cache["requests"] == nil || cache["remote_configured"] == nil {
		t.Fatalf("缓存指标字段不完整: %s", w.Body.String())
	}
	for _, forbidden := range []string{"key", "prefix", "user", "group", "model", "payload"} {
		if _, exists := cache[forbidden]; exists {
			t.Fatalf("缓存指标不应暴露业务字段 %q: %s", forbidden, w.Body.String())
		}
	}
}

// 端到端验证：overview 首次分别填充 matrix 和 stats 两个独立原子结果，breakdown 复用 stats；
// 聚合命中缓存后，用户名/邮箱/余额仍由 users 主键查询实时组装，Redis 中不出现这些资料。
func TestPortalAggregateCacheKeepsLiveUserFieldsOutOfRedis(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	remote := newMemoryByteCacheStore()
	m.usageCache = newUsageResultCacheForTest(remote, 32, 1<<20)

	hash, _ := hashPassword("cache-test-password")
	g := CustomerGroup{Name: "Cache Test", PortalEmail: "cache@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, GroupID: g.ID, Username: "old-name", Email: "old@example.test"}).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	seed := []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-name','live@example.test',500000,1000000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (1,101,%d,2,'gpt-test',250000,10,5,'test-group')", createdAt),
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"cache@test.local","password":"cache-test-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	path := "/api/overview?from=2026-08-02&to=2026-08-02"
	first := portalDo(portal, http.MethodGet, path, "", ck)
	if first.Code != http.StatusOK {
		t.Fatalf("首次总览失败: %d %s", first.Code, first.Body.String())
	}
	var firstResp struct {
		Data portalOverviewPayload `json:"data"`
	}
	if err := json.Unmarshal(first.Body.Bytes(), &firstResp); err != nil {
		t.Fatal(err)
	}
	if len(firstResp.Data.Users) != 1 || firstResp.Data.Users[0].Username != "live-name" || firstResp.Data.Users[0].BalanceQuota == nil || *firstResp.Data.Users[0].BalanceQuota != 500000 {
		t.Fatalf("实时用户字段错误: %+v", firstResp.Data.Users)
	}
	if got := m.usageCache.fills.Load(); got != 2 {
		t.Fatalf("overview 应分别填充 matrix 与 stats 两个原子聚合结果，实际 %d", got)
	}

	// 同范围 breakdown 必须复用 overview 已缓存的 stats，不新增源聚合。
	breakdown := portalDo(portal, http.MethodGet, "/api/breakdown?from=2026-08-02&to=2026-08-02", "", ck)
	if breakdown.Code != http.StatusOK || m.usageCache.fills.Load() != 2 {
		t.Fatalf("breakdown 未复用 stats: code=%d fills=%d body=%s", breakdown.Code, m.usageCache.fills.Load(), breakdown.Body.String())
	}

	// 聚合仍命中缓存，但余额必须立即反映 users 表新值。
	if _, err := m.prodDB.Exec("UPDATE users SET quota=750000 WHERE id=101"); err != nil {
		t.Fatal(err)
	}
	second := portalDo(portal, http.MethodGet, path, "", ck)
	var secondResp struct {
		Data portalOverviewPayload `json:"data"`
	}
	if err := json.Unmarshal(second.Body.Bytes(), &secondResp); err != nil {
		t.Fatal(err)
	}
	if len(secondResp.Data.Users) != 1 || secondResp.Data.Users[0].BalanceQuota == nil || *secondResp.Data.Users[0].BalanceQuota != 750000 || m.usageCache.fills.Load() != 2 {
		t.Fatalf("缓存命中后实时余额错误: users=%+v fills=%d", secondResp.Data.Users, m.usageCache.fills.Load())
	}

	remote.mu.Lock()
	defer remote.mu.Unlock()
	for key, item := range remote.items {
		text := string(item.value)
		if strings.Contains(text, "live@example.test") || strings.Contains(text, "live-name") {
			t.Fatalf("Redis 聚合键 %q 泄漏用户资料: %s", key, text)
		}
	}
}

// 趋势统计属于总览的可选数据域。即使其来源暂时不可读，客户仍应能看到成员、余额和
// 每日矩阵；不能把一个可选图表故障放大成整页“查询失败”。
func TestPortalOverviewKeepsMatrixWhenTrendStatsFail(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	hash, _ := hashPassword("trend-failure-password")
	g := CustomerGroup{Name: "趋势降级测试", PortalEmail: "trend@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 101, GroupID: g.ID, Username: "已缓存成员", Email: "old@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-member','live@example.test',900000,100000)"); err != nil {
		t.Fatal(err)
	}

	from := time.Date(2026, 8, 2, 0, 0, 0, 0, usageCST).Unix()
	to := time.Date(2026, 8, 3, 0, 0, 0, 0, usageCST).Unix()
	memberFP := portalMemberFingerprint([]TrackedUser{member})
	mxKey := m.usageFactCacheKey(portalGroupAggregateKey("matrix", g.ID, memberFP, from, to))
	seeded := &UsageMatrix{
		From: "2026-08-02", To: "2026-08-02", Days: []string{"2026-08-02"},
		Cells: []UsageMatrixCell{{UserID: 101, Date: "2026-08-02", UsageBilling: UsageBilling{Requests: 3, ConsumeQuota: 250000}}},
	}
	if err := m.usageCache.DoJSON(context.Background(), mxKey, time.Minute, &UsageMatrix{}, func() (any, error) {
		return seeded, nil
	}); err != nil {
		t.Fatal(err)
	}
	// matrix 已缓存后仅破坏 stats 所需的来源表，刻意制造可选趋势域失败。
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"trend@test.local","password":"trend-failure-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	w := portalDo(portal, http.MethodGet, "/api/overview?from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("趋势失败不得拖垮总览: status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Data portalOverviewPayload `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.TrendAvailable || body.Data.TrendMessage == "" || len(body.Data.Users) != 1 || body.Data.Users[0].Username != "live-member" || len(body.Data.Cells) != 1 || body.Data.Cells[0].Requests != 3 {
		t.Fatalf("趋势降级时主数据载荷不正确: %+v", body.Data)
	}
}

// 单用户页的令牌列表属于可选数据域。令牌聚合来源临时失败时，只要核心汇总已有
// 有效缓存，客户仍必须看到汇总、余额与累计消耗；不能把局部故障放大成整页 500。
func TestPortalUserDetailKeepsStatsWhenTokenListFails(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(newMemoryByteCacheStore(), 32, 1<<20)

	hash, _ := hashPassword("token-list-failure-password")
	g := CustomerGroup{Name: "令牌明细降级测试", PortalEmail: "token-list@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 101, GroupID: g.ID, Username: "成员快照", Email: "member@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-member','live@example.test',900000,100000)"); err != nil {
		t.Fatal(err)
	}

	from := time.Date(2026, 8, 2, 0, 0, 0, 0, usageCST).Unix()
	to := time.Date(2026, 8, 3, 0, 0, 0, 0, usageCST).Unix()
	memberFP := portalMemberFingerprint([]TrackedUser{member})
	statsKey := m.usageFactCacheKey(portalUserAggregateKey("stats", g.ID, memberFP, member.UserID, 0, from, to))
	if err := m.usageCache.DoJSON(context.Background(), statsKey, time.Minute, &UsageStats{}, func() (any, error) {
		return &UsageStats{Summary: UsageDim{UsageBilling: UsageBilling{Requests: 7, ConsumeQuota: 350000}}}, nil
	}); err != nil {
		t.Fatal(err)
	}
	// 主统计已在缓存，故意破坏仅用于令牌明细的 logs 来源表。
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"token-list@test.local","password":"token-list-failure-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	w := portalDo(portal, http.MethodGet, "/api/user?uid=101&from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("令牌明细失败不得拖垮成员汇总: status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Data struct {
			Stats              UsageStats `json:"stats"`
			TokenDataAvailable bool       `json:"token_data_available"`
			TokenDataMessage   string     `json:"token_data_message"`
			BalanceQuota       *int64     `json:"balance_quota"`
			TotalUsedQuota     *int64     `json:"total_used_quota"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Stats.Summary.Requests != 7 || body.Data.TokenDataAvailable || body.Data.TokenDataMessage == "" ||
		body.Data.BalanceQuota == nil || *body.Data.BalanceQuota != 900000 ||
		body.Data.TotalUsedQuota == nil || *body.Data.TotalUsedQuota != 100000 {
		t.Fatalf("令牌明细降级载荷不正确: %+v", body.Data)
	}
}

// 门户总览的核心是成员每日用量矩阵。它和趋势、令牌明细一样有独立数据域，但矩阵
// 在源库短暂异常时允许展示受限窗口内的最近成功值，必须带明确 stale 标记，不能让
// 客户看到空白页或误以为这是实时数据。
func TestPortalOverviewReturnsExplicitStaleMatrixOnSourceFailure(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(nil, 32, 1<<20)

	hash, _ := hashPassword("stale-overview-password")
	g := CustomerGroup{Name: "陈旧矩阵测试", PortalEmail: "stale-overview@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 101, GroupID: g.ID, Username: "成员快照", Email: "member@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	for _, q := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-member','live@example.test',900000,100000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (1,101,%d,2,'gpt-test',250000,10,5,'test-group')", createdAt),
	} {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"stale-overview@test.local","password":"stale-overview-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	path := "/api/overview?from=2026-08-02&to=2026-08-02"
	first := portalDo(portal, http.MethodGet, path, "", ck)
	if first.Code != http.StatusOK {
		t.Fatalf("首次总览失败: %d %s", first.Code, first.Body.String())
	}

	tracked := []TrackedUser{member}
	fromTs, toTs, err := parseUsageRange("2026-08-02", "2026-08-02", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	key := m.usageFactCacheKey(portalGroupAggregateKey("matrix", g.ID, portalMemberFingerprint(tracked), fromTs, toTs))
	fullKey := m.usageCache.fullKey(key)
	data, ok := m.usageCache.local.GetStale(fullKey, time.Now())
	if !ok {
		t.Fatal("首次成功的门户矩阵应在本机缓存中")
	}
	m.usageCache.local.PutWithStale(fullKey, data, time.Millisecond, time.Minute, time.Now().Add(-10*time.Millisecond))
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}

	w := portalDo(portal, http.MethodGet, path, "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("源故障时门户总览应保留核心矩阵: %d %s", w.Code, w.Body.String())
	}
	var body struct {
		Data portalOverviewPayload `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if !body.Data.MatrixStale || body.Data.MatrixMessage == "" || len(body.Data.Users) != 1 ||
		body.Data.Users[0].BalanceQuota == nil || *body.Data.Users[0].BalanceQuota != 900000 ||
		len(body.Data.Cells) != 1 || body.Data.Cells[0].Requests != 1 {
		t.Fatalf("门户陈旧矩阵载荷不正确: %+v", body.Data)
	}
}

// 趋势统计与按维度排行共用 stats 聚合。该聚合已有受限窗口内的最近成功结果时，
// 来源短暂失败应回退到该结果并明确标记 stale，而不是清空趋势或让下游 breakdown 500。
func TestPortalStatsFallbackKeepsTrendAndBreakdownAvailable(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(nil, 32, 1<<20)

	hash, _ := hashPassword("stale-stats-password")
	g := CustomerGroup{Name: "陈旧统计测试", PortalEmail: "stale-stats@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 101, GroupID: g.ID, Username: "成员快照", Email: "member@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	for _, q := range []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-member','live@example.test',900000,100000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`) VALUES (1,101,%d,2,'gpt-test',250000,10,5,'test-group')", createdAt),
	} {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"stale-stats@test.local","password":"stale-stats-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	path := "/api/overview?from=2026-08-02&to=2026-08-02"
	if first := portalDo(portal, http.MethodGet, path, "", ck); first.Code != http.StatusOK {
		t.Fatalf("首次总览失败: %d %s", first.Code, first.Body.String())
	}

	fromTs, toTs, err := parseUsageRange("2026-08-02", "2026-08-02", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	statsKey := m.usageFactCacheKey(portalGroupAggregateKey("stats", g.ID, portalMemberFingerprint([]TrackedUser{member}), fromTs, toTs))
	fullKey := m.usageCache.fullKey(statsKey)
	data, ok := m.usageCache.local.GetStale(fullKey, time.Now())
	if !ok {
		t.Fatal("首次成功的门户 stats 应在本机缓存中")
	}
	m.usageCache.local.PutWithStale(fullKey, data, time.Millisecond, time.Minute, time.Now().Add(-10*time.Millisecond))
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}

	w := portalDo(portal, http.MethodGet, path, "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("stats 回退时门户总览仍应成功: %d %s", w.Code, w.Body.String())
	}
	var overview struct {
		Data portalOverviewPayload `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &overview); err != nil {
		t.Fatal(err)
	}
	if !overview.Data.TrendAvailable || !overview.Data.TrendStale || overview.Data.TrendMessage == "" || len(overview.Data.DailyByModel) != 1 || len(overview.Data.ByModel) != 1 {
		t.Fatalf("陈旧 stats 的趋势降级载荷错误: %+v", overview.Data)
	}

	breakdown := portalDo(portal, http.MethodGet, "/api/breakdown?from=2026-08-02&to=2026-08-02", "", ck)
	if breakdown.Code != http.StatusOK {
		t.Fatalf("stats 回退时 breakdown 仍应成功: %d %s", breakdown.Code, breakdown.Body.String())
	}
	var breakdownBody struct {
		Data portalBreakdownPayload `json:"data"`
	}
	if err := json.Unmarshal(breakdown.Body.Bytes(), &breakdownBody); err != nil {
		t.Fatal(err)
	}
	if !breakdownBody.Data.Available || !breakdownBody.Data.Stale || breakdownBody.Data.Message == "" || len(breakdownBody.Data.ByGroup) != 1 || len(breakdownBody.Data.ByModel) != 1 {
		t.Fatalf("陈旧 stats 的 breakdown 载荷错误: %+v", breakdownBody.Data)
	}
}

// 当 stats 从未成功过且来源不可用时，按分组/按模型只能局部不可用；接口仍必须
// 成功返回结构化状态，客户端可继续查看总览、余额、每日矩阵和使用日志。
func TestPortalBreakdownReturnsStructuredUnavailableInsteadOf500(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(nil, 32, 1<<20)

	hash, _ := hashPassword("breakdown-unavailable-password")
	g := CustomerGroup{Name: "细分统计降级测试", PortalEmail: "breakdown-unavailable@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, GroupID: g.ID, Username: "成员快照"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"breakdown-unavailable@test.local","password":"breakdown-unavailable-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	w := portalDo(portal, http.MethodGet, "/api/breakdown?from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("细分统计不可用时不应返回 500: %d %s", w.Code, w.Body.String())
	}
	var body struct {
		Data portalBreakdownPayload `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.Available || body.Data.Message == "" || len(body.Data.ByGroup) != 0 || len(body.Data.ByModel) != 0 {
		t.Fatalf("细分统计不可用的结构化载荷错误: %+v", body.Data)
	}
}

// 门户与管理端共享“已有日期先展示”的事实窗口：近 30 天尚未补齐时，矩阵、趋势
// 和范围汇总都只读取已发布的完整自然日，并向客户明确提示正在补全。
func TestPortalOverviewShowsPublishedOverlapWhileBackfillContinues(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	enableUsageFactsForTest(m)
	m.usageCache = newUsageResultCacheForTest(nil, 32, 4<<20)

	g := CustomerGroup{Name: "渐进展示客户"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, GroupID: g.ID, Username: "member", AddedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	publishedFrom := time.Date(2026, 8, 7, 13, 0, 0, 0, usageCST).Unix()
	publishedThrough := time.Date(2026, 8, 14, 13, 0, 0, 0, usageCST).Unix()
	seedPublishedUsageFactsForTest(t, m, []int64{101}, publishedFrom, publishedThrough)
	if err := m.storeDB.Create(&UsageUserSnapshot{UserID: 101, Username: "member", Exists: true, CapturedAt: time.Now().Unix()}).Error; err != nil {
		t.Fatal(err)
	}
	availableDay := time.Date(2026, 8, 8, 0, 0, 0, 0, usageCST).Unix()
	if err := m.storeDB.Create(&UsageDailyFact{
		DateTs: availableDay, UserID: 101, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1,
		Requests: 3, PromptTokens: 20, CompletionTokens: 5, ConsumeQuota: 1_000_000,
	}).Error; err != nil {
		t.Fatal(err)
	}
	fromTs, toTs, err := parseUsageRange("2026-07-16", "2026-08-14", time.Now())
	if err != nil {
		t.Fatal(err)
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/overview", nil)
	counts.reset()
	p, err := m.buildPortalOverview(c, g.ID, fromTs, toTs)
	if err != nil {
		t.Fatal(err)
	}
	if !p.MatrixAvailable || !p.TrendAvailable || !p.RangePartial || p.RangeMessage == "" ||
		p.RequestedFrom != "2026-07-16" || p.RequestedTo != "2026-08-14" ||
		p.From != "2026-08-08" || p.To != "2026-08-14" || len(p.Days) != 7 || len(p.Cells) != 1 ||
		len(p.Users) != 1 || p.Users[0].TotalUSD != 2 || len(p.DailyByModel) != 1 {
		t.Fatalf("门户渐进总览载荷错误: %+v", p)
	}
	if got := counts.logs.Load(); got != 0 {
		t.Fatalf("门户渐进展示不得扫描生产 logs，实际=%d", got)
	}
}

// 门户总览没有任何旧矩阵可回退时，日志聚合失败仍必须保留成员、余额和累计消耗。
// 这与趋势、令牌列表一样属于独立数据域，不能让客户面对整页“查询失败”。
func TestPortalOverviewKeepsProfilesWhenMatrixUnavailable(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(nil, 32, 1<<20)

	hash, _ := hashPassword("matrix-unavailable-password")
	g := CustomerGroup{Name: "矩阵不可用降级测试", PortalEmail: "matrix-unavailable@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	member := TrackedUser{UserID: 101, GroupID: g.ID, Username: "成员快照", Email: "member@example.test"}
	if err := m.storeDB.Create(&member).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-member','live@example.test',900000,100000)"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}
	counts.reset()

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"matrix-unavailable@test.local","password":"matrix-unavailable-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	w := portalDo(portal, http.MethodGet, "/api/overview?from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("矩阵不可用不得拖垮门户总览: status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Data portalOverviewPayload `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.MatrixAvailable || body.Data.MatrixMessage == "" || body.Data.TrendAvailable || body.Data.TrendMessage == "" || len(body.Data.Cells) != 0 || len(body.Data.Users) != 1 ||
		body.Data.Users[0].BalanceQuota == nil || *body.Data.Users[0].BalanceQuota != 900000 ||
		body.Data.Users[0].TotalUsedQuota == nil || *body.Data.Users[0].TotalUsedQuota != 100000 ||
		body.Data.Users[0].TotalUSD != 0 {
		t.Fatalf("门户矩阵不可用时的降级载荷错误: %+v", body.Data)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("矩阵失败后不应继续触发趋势 logs 聚合，实际 logs 查询 %d 次", got)
	}
}

// 门户成员详情的范围统计不可用时，客户仍要能够看到自己当前余额与累计消耗；
// 日志聚合失败不能被令牌明细再次放大为第二次来源扫描。
func TestPortalUserDetailKeepsLiveFieldsWhenStatsUnavailable(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	m.usageDayExpr = usageDayExprSQLite
	m.usageCache = newUsageResultCacheForTest(nil, 32, 1<<20)

	hash, _ := hashPassword("detail-stats-unavailable-password")
	g := CustomerGroup{Name: "成员详情统计降级", PortalEmail: "detail-stats-unavailable@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, GroupID: g.ID, Username: "成员快照", Email: "member@example.test"}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-member','live@example.test',900000,100000)"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.prodDB.Exec("DROP TABLE logs"); err != nil {
		t.Fatal(err)
	}
	counts.reset()

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"detail-stats-unavailable@test.local","password":"detail-stats-unavailable-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	w := portalDo(portal, http.MethodGet, "/api/user?uid=101&from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusOK {
		t.Fatalf("成员范围统计失败不得拖垮门户详情: status=%d body=%s", w.Code, w.Body.String())
	}
	var body struct {
		Data struct {
			StatsAvailable     bool         `json:"stats_available"`
			StatsMessage       string       `json:"stats_message"`
			BalanceQuota       *int64       `json:"balance_quota"`
			TotalUsedQuota     *int64       `json:"total_used_quota"`
			ByToken            []TokenUsage `json:"by_token"`
			TokenDataAvailable *bool        `json:"token_data_available"`
			TokenDataMessage   string       `json:"token_data_message"`
		} `json:"data"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if body.Data.StatsAvailable || body.Data.StatsMessage == "" ||
		body.Data.BalanceQuota == nil || *body.Data.BalanceQuota != 900000 ||
		body.Data.TotalUsedQuota == nil || *body.Data.TotalUsedQuota != 100000 ||
		body.Data.TokenDataAvailable == nil || *body.Data.TokenDataAvailable || body.Data.TokenDataMessage == "" || len(body.Data.ByToken) != 0 {
		t.Fatalf("门户成员统计不可用时的降级载荷错误: %+v", body.Data)
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("成员详情失败后不应重复扫描 logs，实际 %d 次", got)
	}
}

// 成员详情与令牌详情的日志聚合可以缓存，但用户身份/余额及单令牌元数据必须每次实时读取。
// 同时验证 Redis 载荷不包含用户名、邮箱或完整令牌 key。
func TestPortalUserAndTokenDetailCacheKeepsLiveFieldsOutOfRedis(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	remote := newMemoryByteCacheStore()
	m.usageCache = newUsageResultCacheForTest(remote, 32, 1<<20)

	hash, _ := hashPassword("detail-cache-password")
	g := CustomerGroup{Name: "Detail Cache", PortalEmail: "detail@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, GroupID: g.ID, Username: "snapshot-name", Email: "snapshot@example.test"}).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	seed := []string{
		"INSERT INTO users (id,username,email,quota,used_quota) VALUES (101,'live-name-v1','live-v1@example.test',500000,1000000)",
		"INSERT INTO tokens (id,user_id,name,`key`,`group`,used_quota) VALUES (9001,101,'token-v1','abcdefghijklmnop','vip',750000)",
		fmt.Sprintf("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,token_id,token_name) VALUES (1,101,%d,2,'gpt-test',250000,10,5,'vip',9001,'token-v1')", createdAt),
	}
	for _, q := range seed {
		if _, err := m.prodDB.Exec(q); err != nil {
			t.Fatal(err)
		}
	}

	login := portalDo(portal, http.MethodPost, "/login", `{"email":"detail@test.local","password":"detail-cache-password"}`)
	ck := portalCookie(login)
	if ck == nil {
		t.Fatalf("登录失败: %d %s", login.Code, login.Body.String())
	}
	memberPath := "/api/user?uid=101&from=2026-08-02&to=2026-08-02"
	type memberDetailResponse struct {
		Data struct {
			Stats          UsageStats   `json:"stats"`
			ByToken        []TokenUsage `json:"by_token"`
			BalanceQuota   *int64       `json:"balance_quota"`
			TotalUsedQuota *int64       `json:"total_used_quota"`
		} `json:"data"`
	}
	readMember := func() memberDetailResponse {
		w := portalDo(portal, http.MethodGet, memberPath, "", ck)
		if w.Code != http.StatusOK {
			t.Fatalf("成员详情失败: %d %s", w.Code, w.Body.String())
		}
		var out memberDetailResponse
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	first := readMember()
	if first.Data.Stats.Summary.Requests != 1 || len(first.Data.ByToken) != 1 {
		t.Fatalf("成员聚合错误: %+v", first.Data)
	}
	tok := first.Data.ByToken[0]
	if tok.Owner != "live-name-v1" || tok.MaskedKey == "abcdefghijklmnop" || tok.MaskedKey == "" ||
		first.Data.BalanceQuota == nil || *first.Data.BalanceQuota != 500000 ||
		first.Data.TotalUsedQuota == nil || *first.Data.TotalUsedQuota != 1000000 {
		t.Fatalf("成员实时字段或令牌脱敏错误: token=%+v balance=%v used=%v", tok, first.Data.BalanceQuota, first.Data.TotalUsedQuota)
	}
	if got := m.usageCache.fills.Load(); got != 2 {
		t.Fatalf("成员详情首次应只填充 stats+tokens，实际 %d", got)
	}

	// 聚合保持命中，但用户名、邮箱、余额和累计消耗必须立即取到 users 表新值。
	if _, err := m.prodDB.Exec("UPDATE users SET username='live-name-v2',email='live-v2@example.test',quota=800000,used_quota=1200000 WHERE id=101"); err != nil {
		t.Fatal(err)
	}
	second := readMember()
	if second.Data.ByToken[0].Owner != "live-name-v2" ||
		second.Data.BalanceQuota == nil || *second.Data.BalanceQuota != 800000 ||
		second.Data.TotalUsedQuota == nil || *second.Data.TotalUsedQuota != 1200000 ||
		m.usageCache.fills.Load() != 2 {
		t.Fatalf("缓存命中后实时字段错误: data=%+v fills=%d", second.Data, m.usageCache.fills.Load())
	}

	// 令牌详情只缓存日志统计；名称、脱敏 key、分组和累计消耗由 tokenMetaOf 实时读取。
	tokenPath := memberPath + "&token_id=9001"
	type tokenDetailResponse struct {
		Data struct {
			Stats UsageStats  `json:"stats"`
			Token *TokenUsage `json:"token"`
		} `json:"data"`
	}
	readToken := func() tokenDetailResponse {
		w := portalDo(portal, http.MethodGet, tokenPath, "", ck)
		if w.Code != http.StatusOK {
			t.Fatalf("令牌详情失败: %d %s", w.Code, w.Body.String())
		}
		var out tokenDetailResponse
		if err := json.Unmarshal(w.Body.Bytes(), &out); err != nil {
			t.Fatal(err)
		}
		return out
	}
	tokenFirst := readToken()
	if tokenFirst.Data.Stats.Summary.Requests != 1 || tokenFirst.Data.Token == nil || tokenFirst.Data.Token.Name != "token-v1" {
		t.Fatalf("令牌详情首次结果错误: %+v", tokenFirst.Data)
	}
	if _, err := m.prodDB.Exec("UPDATE tokens SET name='token-v2',`key`='qrstuvwxyzabcdef',`group`='vip-v2',used_quota=900000 WHERE id=9001"); err != nil {
		t.Fatal(err)
	}
	tokenSecond := readToken()
	if tokenSecond.Data.Token == nil || tokenSecond.Data.Token.Name != "token-v2" ||
		tokenSecond.Data.Token.Group != "vip-v2" || tokenSecond.Data.Token.MaskedKey == "qrstuvwxyzabcdef" ||
		tokenSecond.Data.Token.TotalCostQuota == nil || *tokenSecond.Data.Token.TotalCostQuota != 900000 ||
		m.usageCache.fills.Load() != 3 {
		t.Fatalf("令牌统计命中后实时元数据错误: data=%+v fills=%d", tokenSecond.Data, m.usageCache.fills.Load())
	}
	memberAfterTokenUpdate := readMember()
	if len(memberAfterTokenUpdate.Data.ByToken) != 1 || memberAfterTokenUpdate.Data.ByToken[0].Name != "token-v2" ||
		memberAfterTokenUpdate.Data.ByToken[0].Group != "vip-v2" ||
		memberAfterTokenUpdate.Data.ByToken[0].TotalCostQuota == nil || *memberAfterTokenUpdate.Data.ByToken[0].TotalCostQuota != 900000 ||
		m.usageCache.fills.Load() != 3 {
		t.Fatalf("成员令牌列表也必须在聚合命中时补回实时元数据: data=%+v fills=%d", memberAfterTokenUpdate.Data, m.usageCache.fills.Load())
	}

	// 主站删除用户后，客户端历史聚合和令牌仍可读；展示名沿用最近自愈快照，金额降级为 null。
	if _, err := m.prodDB.Exec("DELETE FROM users WHERE id=101"); err != nil {
		t.Fatal(err)
	}
	deletedMember := readMember()
	if len(deletedMember.Data.ByToken) != 1 || deletedMember.Data.ByToken[0].Owner != "live-name-v2" ||
		deletedMember.Data.BalanceQuota != nil || deletedMember.Data.TotalUsedQuota != nil ||
		m.usageCache.fills.Load() != 3 {
		t.Fatalf("主站用户删除后的 Portal 降级语义错误: data=%+v fills=%d", deletedMember.Data, m.usageCache.fills.Load())
	}

	remote.mu.Lock()
	defer remote.mu.Unlock()
	for key, item := range remote.items {
		text := string(item.value)
		for _, secret := range []string{
			"live-name-v1", "live-name-v2", "live-v1@example.test", "live-v2@example.test",
			"abcdefghijklmnop", "qrstuvwxyzabcdef",
		} {
			if strings.Contains(text, secret) {
				t.Fatalf("Redis 聚合键 %q 泄漏实时或敏感字段 %q: %s", key, secret, text)
			}
		}
	}
}

// 管理端矩阵刷新后，只精确删除相同成员集合/日期范围的 matrix/stats 原子聚合键。
// 这既保证客户下次读到完整趋势，也不会对 Redis 使用 KEYS/SCAN。
func TestPortalMatrixRefreshInvalidatesExactAggregates(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	remote := newMemoryByteCacheStore()
	m.usageCache = newUsageResultCacheForTest(remote, 32, 1<<20)
	tracked := []TrackedUser{
		{UserID: 101, GroupID: 11},
		{UserID: 202, GroupID: 22},
	}
	const fromTs, toTs = 1751328000, 1751414400
	for _, u := range tracked {
		fp := portalMemberFingerprint([]TrackedUser{u})
		for _, kind := range []string{"matrix", "stats"} {
			key := portalGroupAggregateKey(kind, u.GroupID, fp, fromTs, toTs)
			if kind == "matrix" {
				var out UsageMatrix
				if err := m.usageCache.DoJSON(context.Background(), key, usageAggregateLiveTTL, &out, func() (any, error) {
					return &UsageMatrix{Cells: []UsageMatrixCell{{UserID: u.UserID, UsageBilling: UsageBilling{Requests: 99}}}}, nil
				}); err != nil {
					t.Fatal(err)
				}
				continue
			}
			var out UsageStats
			if err := m.usageCache.DoJSON(context.Background(), key, usageAggregateLiveTTL, &out, func() (any, error) {
				return &UsageStats{Summary: UsageDim{UsageBilling: UsageBilling{Requests: 99}}}, nil
			}); err != nil {
				t.Fatal(err)
			}
		}
	}
	m.invalidatePortalAggregates(tracked, fromTs, toTs)
	for _, u := range tracked {
		fp := portalMemberFingerprint([]TrackedUser{u})
		for _, kind := range []string{"matrix", "stats"} {
			called := false
			key := portalGroupAggregateKey(kind, u.GroupID, fp, fromTs, toTs)
			if kind == "matrix" {
				var out UsageMatrix
				err := m.usageCache.DoJSON(context.Background(), key, usageAggregateLiveTTL, &out, func() (any, error) {
					called = true
					return &UsageMatrix{Cells: []UsageMatrixCell{{UserID: u.UserID, UsageBilling: UsageBilling{Requests: 1}}}}, nil
				})
				if err != nil || !called || len(out.Cells) != 1 || out.Cells[0].Requests != 1 {
					t.Fatalf("group=%d kind=%s 应重新聚合: called=%v out=%+v err=%v", u.GroupID, kind, called, out, err)
				}
				continue
			}
			var out UsageStats
			err := m.usageCache.DoJSON(context.Background(), key, usageAggregateLiveTTL, &out, func() (any, error) {
				called = true
				return &UsageStats{Summary: UsageDim{UsageBilling: UsageBilling{Requests: 1}}}, nil
			})
			if err != nil || !called || out.Summary.Requests != 1 {
				t.Fatalf("group=%d kind=%s 应重新聚合: called=%v requests=%d err=%v", u.GroupID, kind, called, out.Summary.Requests, err)
			}
		}
	}
}

// 成员从 A 组移动到 B 组后，两组成员指纹都会变化；旧 Redis 键即使尚未 TTL 过期，
// 新请求也不可能命中它，因此无需做危险的前缀扫描删除。
func TestPortalMemberMoveChangesBothGroupCacheScopes(t *testing.T) {
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
	beforeA := []TrackedUser{{UserID: 101, GroupID: ga.ID}}
	beforeB := []TrackedUser{}
	oldA := portalGroupAggregateKey("matrix", ga.ID, portalMemberFingerprint(beforeA), 100, 200)
	oldB := portalGroupAggregateKey("matrix", gb.ID, portalMemberFingerprint(beforeB), 100, 200)
	rootCk := &http.Cookie{Name: sessionCookie, Value: m.signSession("root", roleRoot, time.Now().Unix())}
	w := portalDo(admin, http.MethodPost, "/usage/users/group", fmt.Sprintf(`{"user_id":101,"group_id":%d}`, gb.ID), rootCk)
	if w.Code != http.StatusOK {
		t.Fatalf("移动成员失败 = %d %s", w.Code, w.Body.String())
	}
	var afterA, afterB []TrackedUser
	if err := m.storeDB.Where("group_id = ?", ga.ID).Find(&afterA).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("group_id = ?", gb.ID).Find(&afterB).Error; err != nil {
		t.Fatal(err)
	}
	newA := portalGroupAggregateKey("matrix", ga.ID, portalMemberFingerprint(afterA), 100, 200)
	newB := portalGroupAggregateKey("matrix", gb.ID, portalMemberFingerprint(afterB), 100, 200)
	if oldA == newA || oldB == newB {
		t.Fatalf("移动成员后两组缓存域都必须变化: A %q -> %q, B %q -> %q", oldA, newA, oldB, newB)
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

func cachedString(ctx context.Context, c *usageResultCache, key string, ttl time.Duration, fill func() (any, error)) (string, error) {
	var out string
	err := c.DoJSON(ctx, key, ttl, &out, fill)
	return out, err
}

// 缓存:singleflight——同键并发只执行一次 fill;TTL 内命中不再执行。
func TestTTLCacheSingleflight(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
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
			v, err := cachedString(context.Background(), c, "k", time.Second, fill)
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
	if _, err := cachedString(context.Background(), c, "k", time.Second, fill); err != nil || calls.Load() != 1 {
		t.Fatalf("TTL 内应命中缓存,calls=%d err=%v", calls.Load(), err)
	}
}

func TestTTLCacheWaiterRespectsContextCancellation(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)
	go func() {
		_, err := cachedString(context.Background(), c, "k", time.Minute, func() (any, error) {
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
	if _, err := cachedString(ctx, c, "k", time.Minute, func() (any, error) {
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
	v, err := cachedString(context.Background(), c, "k", time.Minute, func() (any, error) {
		called = true
		return "wrong", nil
	})
	if err != nil || called || v != "ready" {
		t.Fatalf("取消等待者不应删除或污染原请求生成的缓存: value=%v called=%v err=%v", v, called, err)
	}
}

func TestTTLCacheCanceledFillerDoesNotCancelNormalWaiterOrPoisonCache(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	fillStarted := make(chan struct{})
	firstDone := make(chan error, 1)
	ctx, cancel := context.WithCancel(context.Background())
	go func() {
		_, err := cachedString(ctx, c, "k", time.Minute, func() (any, error) {
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
		v, err := cachedString(context.Background(), c, "k", time.Minute, func() (any, error) {
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
	v, err := cachedString(context.Background(), c, "k", time.Minute, func() (any, error) {
		called = true
		return "wrong", nil
	})
	if err != nil || called || v != "fresh" {
		t.Fatalf("取消错误不能污染后续成功缓存: value=%v called=%v err=%v", v, called, err)
	}
}

func TestTTLCacheCanceledWaitersDoNotCancelNormalWaiters(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	fillStarted := make(chan struct{})
	releaseFill := make(chan struct{})
	var fillCalls atomic.Int32
	firstDone := make(chan error, 1)
	go func() {
		_, err := cachedString(context.Background(), c, "k", time.Minute, func() (any, error) {
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
			v, err := cachedString(context.Background(), c, "k", time.Minute, func() (any, error) {
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
			_, err := cachedString(ctx, c, "k", time.Minute, func() (any, error) {
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

func TestUsageResultCacheDeletesOnlyExactKey(t *testing.T) {
	c := newUsageResultCacheForTest(nil, 32, 1<<20)
	for key, value := range map[string]string{"ov|1|a": "a", "ov|10|b": "b", "bd|1|c": "c"} {
		v := value
		if _, err := cachedString(context.Background(), c, key, time.Minute, func() (any, error) { return v, nil }); err != nil {
			t.Fatal(err)
		}
	}
	c.Delete(context.Background(), "ov|1|a")
	called := false
	v, err := cachedString(context.Background(), c, "ov|1|a", time.Minute, func() (any, error) {
		called = true
		return "fresh", nil
	})
	if err != nil || !called || v != "fresh" {
		t.Fatalf("匹配前缀的缓存应删除: called=%v value=%v err=%v", called, v, err)
	}
	called = false
	v, err = cachedString(context.Background(), c, "ov|10|b", time.Minute, func() (any, error) {
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

func TestPortalExportLimitedWriterRejectsWholeOverflowChunk(t *testing.T) {
	var dst bytes.Buffer
	limited := &portalExportLimitedWriter{dst: &dst, max: 5}
	if n, err := limited.Write([]byte("1234")); err != nil || n != 4 {
		t.Fatalf("限额内写入失败: n=%d err=%v", n, err)
	}
	if n, err := limited.Write([]byte("56")); !errors.Is(err, errPortalExportTooLarge) || n != 0 {
		t.Fatalf("超限 chunk 应在写入前整块拒绝: n=%d err=%v", n, err)
	}
	if got := dst.String(); got != "1234" {
		t.Fatalf("超限 chunk 不得局部落盘: %q", got)
	}
}

func TestPortalExportDefaultCapacityAccepts50000RowsWith2KiBDetails(t *testing.T) {
	limited := &portalExportLimitedWriter{dst: io.Discard, max: portalExportMaxBytes}
	writer := csv.NewWriter(limited)
	if _, err := limited.Write([]byte("\xEF\xBB\xBF")); err != nil {
		t.Fatal(err)
	}
	header := []string{"时间", "成员", "令牌", "分组", "类型", "模型", "用时(秒)", "输入tokens", "输出tokens", "费用(美元)", "详情", "Request ID"}
	if err := writer.Write(header); err != nil {
		t.Fatal(err)
	}
	record := portalCSVRecord(LogRow{
		CreatedAt: time.Now().Unix(), Member: "capacity-user", TokenName: "capacity-token", Group: "capacity-group",
		Type: 2, ModelName: "capacity-model", CostUSD: 1.25, Detail: strings.Repeat("d", 2<<10), RequestID: "capacity-request",
	})
	for i := 0; i < portalExportCap; i++ {
		if err := writer.Write(record); err != nil {
			t.Fatalf("50k 容量在第 %d 行意外超限: %v", i+1, err)
		}
	}
	writer.Flush()
	if err := writer.Error(); err != nil {
		t.Fatal(err)
	}
	if limited.written >= portalExportMaxBytes {
		t.Fatalf("50k×2KiB 容量预算越界: bytes=%d max=%d", limited.written, portalExportMaxBytes)
	}
}

func TestPortalExportStorageLeaseSerializesAndReleases(t *testing.T) {
	m := newTestMonitor(t)
	first, firstLease, err := m.createPortalExportTempFile(1024)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		_ = first.Close()
		_ = os.Remove(first.Name())
		firstLease.release()
	}()
	const contenders = 8
	errorsCh := make(chan error, contenders)
	var wait sync.WaitGroup
	for i := 0; i < contenders; i++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			file, lease, err := m.createPortalExportTempFile(1024)
			if err == nil {
				_ = file.Close()
				_ = os.Remove(file.Name())
				lease.release()
			}
			errorsCh <- err
		}()
	}
	wait.Wait()
	close(errorsCh)
	for err := range errorsCh {
		if !errors.Is(err, errPortalExportStorageBusy) {
			t.Fatalf("全局配额已占用时所有客户导出必须在创建文件前被拒绝: %v", err)
		}
	}
	firstLease.release()
	second, secondLease, err := m.createPortalExportTempFile(1024)
	if err != nil {
		t.Fatalf("前一个导出释放后应可重试: %v", err)
	}
	name := second.Name()
	_ = second.Close()
	_ = os.Remove(name)
	secondLease.release()
}

func TestPortalExportTempFilesUseDataDirPermissionsAndCleanup(t *testing.T) {
	m := newTestMonitor(t)
	dir, err := m.portalExportTempDir()
	if err != nil {
		t.Fatal(err)
	}
	if filepath.Dir(dir) != filepath.Dir(m.cfg.StorePath) {
		t.Fatalf("导出临时目录必须与持久化数据库同卷: dir=%s store=%s", dir, m.cfg.StorePath)
	}
	if err := preparePortalExportTempDir(dir); err != nil {
		t.Fatal(err)
	}
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("导出目录权限错误: info=%v err=%v", info, err)
	}
	oldPath := filepath.Join(dir, portalExportTempFilePrefix+"old.csv")
	recentPath := filepath.Join(dir, portalExportTempFilePrefix+"recent.csv")
	unrelatedPath := filepath.Join(dir, "keep.db")
	for _, path := range []string{oldPath, recentPath, unrelatedPath} {
		if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	now := time.Now()
	old := now.Add(-portalExportStaleFileAge - time.Minute)
	if err := os.Chtimes(oldPath, old, old); err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupPortalExportTempFiles(now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(oldPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("超时残留文件未清理: %v", err)
	}
	for _, path := range []string{recentPath, unrelatedPath} {
		if _, err := os.Stat(path); err != nil {
			t.Fatalf("新导出/无关数据不应被清理 path=%s err=%v", path, err)
		}
	}
}

func TestPortalExportOversizeReturns413WithoutPartialCSV(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	hash, _ := hashPassword("export-limit-password")
	group := CustomerGroup{Name: "Export Limit", PortalEmail: "export-limit@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 404, Username: "export-user", GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,username,content) VALUES (1,404,?,2,'export-model',100,'export-user',?)", day, strings.Repeat("x", 4096)); err != nil {
		t.Fatal(err)
	}
	cookie := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"export-limit@test.local","password":"export-limit-password"}`))
	if cookie == nil {
		t.Fatal("登录失败")
	}

	limitedRouter := gin.New()
	limitedRouter.GET("/api/logs/export", m.requirePortal(true), func(c *gin.Context) {
		m.portalLogsExportWithLimit(c, 512)
	})
	response := portalDo(limitedRouter, http.MethodGet, "/api/logs/export?from=2026-08-02&to=2026-08-02", "", cookie)
	if response.Code != http.StatusRequestEntityTooLarge || strings.Contains(response.Header().Get("Content-Type"), "text/csv") || response.Header().Get("Content-Disposition") != "" {
		t.Fatalf("超限导出必须在响应前返回 413 JSON: %d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	if strings.Contains(response.Body.String(), "export-model") || strings.HasPrefix(response.Body.String(), "\xEF\xBB\xBF") {
		t.Fatalf("超限导出不得发送 CSV 前缀: %q", response.Body.String())
	}
	dir, err := m.portalExportTempDir()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), portalExportTempFilePrefix) {
			t.Fatalf("超限后临时文件未清理: %s", entry.Name())
		}
	}
}

func TestPortalExportMidWriteENOSPCReturns507AndCleansPartialFile(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	hash, _ := hashPassword("export-enospc-password")
	group := CustomerGroup{Name: "Export ENOSPC", PortalEmail: "export-enospc@test.local", PortalPwAdmin: hash}
	if err := m.storeDB.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 405, Username: "enospc-user", GroupID: group.ID}).Error; err != nil {
		t.Fatal(err)
	}
	day := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,username,content) VALUES (1,405,?,2,'enospc-model',100,'enospc-user',?)", day, strings.Repeat("z", 2048)); err != nil {
		t.Fatal(err)
	}
	cookie := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"export-enospc@test.local","password":"export-enospc-password"}`))
	if cookie == nil {
		t.Fatal("登录失败")
	}

	router := gin.New()
	router.GET("/api/logs/export", m.requirePortal(true), func(c *gin.Context) {
		m.portalLogsExportWithWriter(c, portalExportMaxBytes, func(dst io.Writer) io.Writer {
			return &portalFailAfterWriter{dst: dst, remaining: 256}
		})
	})
	response := portalDo(router, http.MethodGet, "/api/logs/export?from=2026-08-02&to=2026-08-02", "", cookie)
	if response.Code != http.StatusInsufficientStorage || strings.Contains(response.Header().Get("Content-Type"), "text/csv") || response.Header().Get("Content-Disposition") != "" {
		t.Fatalf("落盘中途 ENOSPC 必须在下载前返回 507 JSON: %d headers=%v body=%s", response.Code, response.Header(), response.Body.String())
	}
	if strings.Contains(response.Body.String(), "enospc-model") || strings.HasPrefix(response.Body.String(), "\xEF\xBB\xBF") {
		t.Fatalf("ENOSPC 不得发送部分 CSV: %q", response.Body.String())
	}
	dir, err := m.portalExportTempDir()
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), portalExportTempFilePrefix) {
			t.Fatalf("ENOSPC 后部分临时文件未清理: %s", entry.Name())
		}
	}
}

func TestPortalExportInsufficientStorageResponseIs507(t *testing.T) {
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	portalExportStorageError(c, fmt.Errorf("wrapped: %w", errPortalExportInsufficientStorage))
	if w.Code != http.StatusInsufficientStorage || strings.Contains(w.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("存储水位阻断应返回 507 JSON: %d %s", w.Code, w.Body.String())
	}
}

func TestPortalLogMemberStoreFailureReturnsServerError(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	h, _ := hashPassword("password-aaa")
	g := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: h}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	ck := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"a@x.com","password":"password-aaa"}`))
	if ck == nil {
		t.Fatal("登录失败")
	}
	if err := m.storeDB.Migrator().DropTable(&TrackedUser{}); err != nil {
		t.Fatal(err)
	}
	w := portalDo(portal, http.MethodGet, "/api/logs?from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("本地名单库失败应 fail-closed 返回 503，得到 %d %s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "暂不可用") || strings.Contains(strings.ToLower(w.Body.String()), "tracked_users") {
		t.Fatalf("应返回通用错误且不泄漏表结构: %s", w.Body.String())
	}
}

func TestPortalLogsFirstPageDoesNotRunFullCount(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	g := CustomerGroup{Name: "游标分页测试"}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, GroupID: g.ID, Username: "member"}).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	if _, err := prodDB.Exec("INSERT INTO logs(id,user_id,created_at,type,model_name,prompt_tokens,completion_tokens,use_time,quota,is_stream) VALUES(1,101,?,2,'model-a',10,5,1,5000,0)", createdAt); err != nil {
		t.Fatal(err)
	}
	if rows, err := m.queryGroupLogs(context.Background(), []int64{101}, createdAt-3600, createdAt+3600, 0, 0, "", "", "", "", "", 0, portalLogPageSize+1); err != nil || len(rows) != 1 {
		t.Fatalf("日志 LIMIT 查询前置校验失败: rows=%+v err=%v", rows, err)
	}
	counts.reset()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Set("portalGID", g.ID)
	c.Request = httptest.NewRequest(http.MethodGet, "/api/logs?from=2026-08-02&to=2026-08-02", nil)
	m.portalLogs(c)
	if w.Code != http.StatusOK {
		t.Fatalf("日志首页失败: %d %s", w.Code, w.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatal(err)
	}
	if _, exists := body["total"]; exists {
		t.Fatalf("日志首页不应为总页数追加全量 COUNT: %s", w.Body.String())
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("日志首页只应执行一条 LIMIT 查询，实际 logs 查询 %d", got)
	}
}

func TestPortalExportPrepareTicketAndNativeDownload(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	h, _ := hashPassword("password-aaa")
	g := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: h}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "alice", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,username,content) VALUES (1,101,?,2,'gpt-test',250000,10,5,'test-group','alice','ok')", createdAt); err != nil {
		t.Fatal(err)
	}
	ck := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"a@x.com","password":"password-aaa"}`))
	if ck == nil {
		t.Fatal("登录失败")
	}
	prep := portalDo(portal, http.MethodGet, "/api/logs/export/prepare?from=2026-08-02&to=2026-08-02", "", ck)
	if prep.Code != http.StatusOK {
		t.Fatalf("导出预检失败: %d %s", prep.Code, prep.Body.String())
	}
	var p struct {
		OK          bool   `json:"ok"`
		Ticket      string `json:"ticket"`
		Total       int64  `json:"total"`
		NeedConfirm bool   `json:"need_confirm"`
	}
	if err := json.Unmarshal(prep.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if !p.OK || p.Ticket == "" || p.Total != 1 || p.NeedConfirm {
		t.Fatalf("预检结果错误: %+v", p)
	}
	// 预检完成后新写入的日志不应混进这次文件；否则预检的总数/5 万行判断会漂移。
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,username,content) VALUES (2,101,?,2,'after-snapshot',250000,10,5,'test-group','alice','new')", createdAt+1); err != nil {
		t.Fatal(err)
	}

	// 篡改票据必须在任何数据库导出或限流占位前被拒绝。
	bad := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+p.Ticket+"x", "", ck)
	if bad.Code != http.StatusBadRequest {
		t.Fatalf("篡改票据应 400: %d %s", bad.Code, bad.Body.String())
	}

	dl := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+p.Ticket, "", ck)
	if dl.Code != http.StatusOK || !strings.Contains(dl.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("凭票下载失败: %d %s", dl.Code, dl.Body.String())
	}
	if body := dl.Body.String(); !strings.Contains(body, "alice") || !strings.Contains(body, "gpt-test") || strings.Contains(body, "after-snapshot") {
		t.Fatalf("CSV 快照内容错误: %s", body)
	}

	// 票据绑定成员指纹；管理员移动成员后，旧票据不能跨越新的权限范围继续下载。
	if err := m.storeDB.Model(&TrackedUser{}).Where("user_id = ?", 101).Update("group_id", 0).Error; err != nil {
		t.Fatal(err)
	}
	stale := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+p.Ticket, "", ck)
	if stale.Code != http.StatusConflict {
		t.Fatalf("成员变化后旧票据应 409: %d %s", stale.Code, stale.Body.String())
	}
}

// 空结果同样必须固定预检时刻的快照边界。否则 startCursor=0 会被查询层解释为
// “不限制 ID”，导致预检之后新到的日志意外出现在本应为空的 CSV 中。
func TestPortalExportEmptySnapshotExcludesLaterRows(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	h, _ := hashPassword("password-aaa")
	g := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: h}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "alice", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	ck := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"a@x.com","password":"password-aaa"}`))
	if ck == nil {
		t.Fatal("登录失败")
	}

	prep := portalDo(portal, http.MethodGet, "/api/logs/export/prepare?from=2026-08-02&to=2026-08-02", "", ck)
	if prep.Code != http.StatusOK {
		t.Fatalf("空结果导出预检失败: %d %s", prep.Code, prep.Body.String())
	}
	var p struct {
		OK     bool   `json:"ok"`
		Ticket string `json:"ticket"`
		Total  int64  `json:"total"`
	}
	if err := json.Unmarshal(prep.Body.Bytes(), &p); err != nil {
		t.Fatal(err)
	}
	if !p.OK || p.Ticket == "" || p.Total != 0 {
		t.Fatalf("空结果预检应签发 total=0 的票据: %+v", p)
	}
	claim, ok := m.verifyPortalExportClaim(p.Ticket, time.Now().Unix())
	if !ok || claim.StartCursor != 1 {
		t.Fatalf("空结果票据必须携带明确快照边界 cursor=1: ok=%v claim=%+v", ok, claim)
	}

	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,username,content) VALUES (1,101,?,2,'after-empty-snapshot',250000,10,5,'test-group','alice','new')", createdAt); err != nil {
		t.Fatal(err)
	}
	dl := portalDo(portal, http.MethodGet, "/api/logs/export?ticket="+p.Ticket, "", ck)
	if dl.Code != http.StatusOK || !strings.Contains(dl.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("空结果凭票下载失败: %d %s", dl.Code, dl.Body.String())
	}
	body := dl.Body.String()
	if strings.Contains(body, "after-empty-snapshot") || strings.Contains(body, "alice") {
		t.Fatalf("预检后新日志不应混入空快照 CSV: %s", body)
	}
	if lines := strings.Count(strings.TrimSpace(strings.TrimPrefix(body, "\xEF\xBB\xBF")), "\n"); lines != 0 {
		t.Fatalf("空快照 CSV 应只有表头，得到额外数据行: %s", body)
	}
}

// 旧页面可能在发布后继续调用不带 ticket 的导出地址。该兼容路径也必须先取得
// COUNT+MaxID 快照；用 MaxInt64 边界验证它确实经过快照溢出保护，而不是退回普通 COUNT。
func TestPortalLegacyExportTakesSnapshot(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	m.usageDayExpr = usageDayExprSQLite
	h, _ := hashPassword("password-aaa")
	g := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: h}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "alice", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	createdAt := time.Date(2026, 8, 2, 12, 0, 0, 0, usageCST).Unix()
	maxID := int64(^uint64(0) >> 1)
	if _, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,`group`,username,content) VALUES (?,101,?,2,'gpt-test',250000,10,5,'test-group','alice','ok')", maxID, createdAt); err != nil {
		t.Fatal(err)
	}
	ck := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"a@x.com","password":"password-aaa"}`))
	if ck == nil {
		t.Fatal("登录失败")
	}
	w := portalDo(portal, http.MethodGet, "/api/logs/export?from=2026-08-02&to=2026-08-02", "", ck)
	if w.Code != http.StatusInternalServerError || !strings.Contains(w.Body.String(), "查询失败") {
		t.Fatalf("旧导出路径必须执行快照边界保护: %d %s", w.Code, w.Body.String())
	}
	if strings.Contains(w.Header().Get("Content-Type"), "text/csv") {
		t.Fatalf("快照校验失败前不应开始 CSV 响应: %s", w.Header().Get("Content-Type"))
	}
}

func TestPortalExportClaimExpiresAtBoundary(t *testing.T) {
	m := &Monitor{cfg: Settings{SessionSecret: "test-export-session-secret"}}
	now := time.Now().Unix()
	claim := portalExportClaim{
		GID: 1, MemberFP: "members", FromTs: 10, ToTs: 20,
		Total: 1, StartCursor: 2, ExpiresAt: now + 1,
	}
	ticket, err := m.signPortalExportClaim(claim)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := m.verifyPortalExportClaim(ticket, now); !ok {
		t.Fatal("有效期内票据应通过")
	}
	if _, ok := m.verifyPortalExportClaim(ticket, claim.ExpiresAt); ok {
		t.Fatal("到达 expires_at 边界的票据必须立即失效")
	}
}

func TestExportPrepareLimiterBlocksConcurrentCountPreflight(t *testing.T) {
	l := &exportLimiter{last: map[int64]int64{}}
	now := time.Now().Unix()
	if !l.allowPrepare(7, now, int64(portalExportWindow.Seconds()), int64(portalExportPrepWindow.Seconds())) {
		t.Fatal("首次导出预检应放行")
	}
	if l.allowPrepare(7, now, int64(portalExportWindow.Seconds()), int64(portalExportPrepWindow.Seconds())) {
		t.Fatal("同组并发/连点预检不应重复执行 COUNT")
	}
	if !l.allowPrepare(8, now, int64(portalExportWindow.Seconds()), int64(portalExportPrepWindow.Seconds())) {
		t.Fatal("一个客户组的预检不应阻塞其他组")
	}
	if !l.allowPrepare(7, now+int64(portalExportPrepWindow.Seconds()), int64(portalExportWindow.Seconds()), int64(portalExportPrepWindow.Seconds())) {
		t.Fatal("短预检窗口结束后应允许重试")
	}
}

func TestPortalExportUsesNativeDownloadWithoutBlobAccumulation(t *testing.T) {
	for _, required := range []string{
		"/api/logs/export/prepare?${logQuery()}",
		"function beginNativeExport(ticket,confirm)",
		"new URLSearchParams({ticket})",
	} {
		if !strings.Contains(portalHTML, required) {
			t.Fatalf("原生 CSV 下载缺少 %q", required)
		}
	}
	for _, forbidden := range []string{"const chunks=[]", "new Blob(chunks", "r.body.getReader()"} {
		if strings.Contains(portalHTML, forbidden) {
			t.Fatalf("CSV 仍在浏览器内存累计: %q", forbidden)
		}
	}
}

// 客户日志 API 参数契约:与 new-api 普通用户日志一致，类型 1-6 均可筛选；
// 超出范围的类型按“全部”处理。令牌名搜索超长仍应 400。查看与导出共用 portalLogParams,两端点都验。
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

// token_name/content 包含搜索不能使用普通 B-tree 前缀索引。宽日期、
// 过短关键词和带模糊条件的宽导出必须在取得来源查询槽位之前拒绝。
func TestPortalFuzzyLogSearchBudgetsRejectBeforeSourceQuery(t *testing.T) {
	m, _, portal := newPortalTestMonitor(t)
	prodDB, counts := newCountingFakeProdDB(t)
	m.prodDB = prodDB
	h, _ := hashPassword("password-aaa")
	g := CustomerGroup{Name: "A公司", PortalEmail: "a@x.com", PortalPwAdmin: h}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&TrackedUser{UserID: 101, Username: "alice", GroupID: g.ID}).Error; err != nil {
		t.Fatal(err)
	}
	ck := portalCookie(portalDo(portal, http.MethodPost, "/login", `{"email":"a@x.com","password":"password-aaa"}`))
	if ck == nil {
		t.Fatal("登录失败")
	}
	cases := []string{
		"/api/logs?from=2026-01-01&to=2026-02-01&token=ab",                // 32 天 token 包含扫描
		"/api/logs?from=2026-01-01&to=2026-01-08&detail_kw=abc",           // 8 天 content 包含扫描
		"/api/logs?from=2026-01-01&to=2026-01-01&token=a",                 // 过短 token
		"/api/logs?from=2026-01-01&to=2026-01-01&detail_kw=ab",            // 过短 content
		"/api/logs/export/prepare?from=2026-01-01&to=2026-01-08&token=ab", // 导出 token 最多 7 天
		"/api/logs/export?from=2026-01-01&to=2026-01-02&detail_kw=abc",    // 导出 content 最多 1 天
	}
	for _, path := range cases {
		counts.reset()
		w := portalDo(portal, http.MethodGet, path, "", ck)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("%s 应在来源 SQL 前返回 400，得到 %d %s", path, w.Code, w.Body.String())
		}
		if got := counts.logs.Load(); got != 0 {
			t.Fatalf("%s 被拒绝后仍查询了来源 logs %d 次", path, got)
		}
	}

	counts.reset()
	allowed := portalDo(portal, http.MethodGet, "/api/logs?from=2026-01-01&to=2026-01-01&detail_kw=abc", "", ck)
	if allowed.Code != http.StatusOK {
		t.Fatalf("单日合法详情搜索应允许: %d %s", allowed.Code, allowed.Body.String())
	}
	if got := counts.logs.Load(); got != 1 {
		t.Fatalf("合法搜索应仅执行一条 LIMIT 查询，实际 %d", got)
	}
}

func TestPortalErrorOnlyModelAndGroupFiltersAcceptExactInput(t *testing.T) {
	html := portalHTML
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

func TestPortalRangeSwitchIsAtomicAndPickerCannotBeCleared(t *testing.T) {
	html := portalHTML
	for _, want := range []string{
		"function cancelPortalDependentLoads()",
		"if(breakdownAbort)breakdownAbort.abort()",
		"if(detailAbort)detailAbort.abort()",
		"if(logAbort)logAbort.abort()",
		"cancelPortalDependentLoads();",
		"restoreLastLoadedRange()",
		"showClear:false",
	} {
		if !strings.Contains(html, want) {
			t.Fatalf("Portal 日期切换缺少一致性保护 %q", want)
		}
	}
	if strings.Contains(html, "showClear:true") || strings.Contains(html, "onClear:") {
		t.Fatal("Portal 日期控件仍允许清空为无效范围")
	}
}

// 图表属于可选展示层：图表库加载/重排失败时，必须由页面原生节点给出当前卡片的提示，
// 不能把一个局部错误表现成整页空白或影响其他用量区块。
func TestPortalChartsHaveNativeScopedFallbacks(t *testing.T) {
	for _, required := range []string{
		`id="trendChartFallback"`,
		`id="memberBarFallback"`,
		`id="groupPieFallback"`,
		`id="modelPieFallback"`,
		`id="dailyChartFallback"`,
		`id="dModelPieFallback"`,
		`function showChartFallback(id,message)`,
		`function setChartOption(ch,id,option)`,
		`showChartFallback(id,'图表暂不可用')`,
	} {
		if !strings.Contains(portalHTML, required) {
			t.Fatalf("图表局部降级缺少 %q", required)
		}
	}
	if strings.Contains(portalHTML, `graphic:[{type:'text'`) {
		t.Fatal("图表空态仍依赖 ECharts graphic，浏览器重排异常时可能留下空白卡片")
	}
}

// 用户列应根据当前成员名称和邮箱自适应：有空间时不提前截断；窄屏才由矩阵容器横向滚动。
func TestPortalMatrixUserColumnIsAdaptive(t *testing.T) {
	for _, required := range []string{
		"--mx-user-w:216px",
		"--mx-user-text-w",
		"function sizePortalMatrixUserColumn(users,dayCount)",
		"portalMatrixTextWidth(u.username",
		"const maxWidth=wrapWidth<1200?216:232",
		"sizePortalMatrixUserColumn(us,matrixAvailable?days.length:0)",
		"sizePortalMatrixUserColumn(portalMatrixUsers",
	} {
		if !strings.Contains(portalHTML, required) {
			t.Fatalf("Portal 用户列缺少自适应显示保护: %q", required)
		}
	}
	for _, forbidden := range []string{".mxwrap.mx-compact table", "--portal-mx-table-width", "classList.toggle('mx-compact'"} {
		if strings.Contains(portalHTML, forbidden) {
			t.Fatalf("Portal 用户列修复不应改变整表宽度模式: %q", forbidden)
		}
	}
}

func TestPortalMatrixSummaryFallbackHasIndependentColumnWidths(t *testing.T) {
	for _, required := range []string{
		".mxwrap table.mx-summary-only",
		"--mx-summary-user-w:clamp(260px,24vw,360px)",
		"table.mx-summary-only td[colspan]",
		"matrixTable.classList.toggle('mx-summary-only',!matrixAvailable)",
	} {
		if !strings.Contains(portalHTML, required) {
			t.Fatalf("Portal 三列降级表缺少独立列宽保护: %q", required)
		}
	}
}
