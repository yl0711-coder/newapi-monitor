package monitor

// portal.go:客户端「用量报表」——给客户看自己分组用量的独立页面。
//
// 隔离铁律(与对外 API 平台同一条):
//   - 独立监听端口(MONITOR_PORTAL_ADDR,默认关):客户域名只指到这个端口,
//     该端口上【不存在】任何管理端路由/页面/资源——物理隔离,零 monitor 痕迹;
//   - 会话独立:自己的 cookie 名 + 独立 HMAC 密钥域,管理端会话在这里无效,反之亦然;
//   - 数据强隔离:group_id 只从会话取,服务端强制只查该组成员;客户传什么参数都越不了权。
//
// 账号模型:一组一账号(portal_email),双密码并存——我方配置密码(PortalPwAdmin,永久有效)
// 和客户自改密码(PortalPwUser)任一匹配即可登录;均只存 bcrypt 哈希,后台不可见只能重置。
//
// 容量设计:昂贵聚合结果走 Redis(可选)+有界本机应急缓存+
// singleflight(cache.go)。Redis 不可用时自动降级；缓存键带服务端成员指纹，成员变化不会串组。

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/csv"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
	"gorm.io/gorm"
)

//go:embed portal.html
var portalHTML string

//go:embed portal_login.html
var portalLoginHTML string

const (
	portalSessionCookie  = "report_session" // 中性命名,不带 monitor 字样
	portalSessionTTL     = 12 * time.Hour
	portalLoginWindow    = 10 * time.Minute // 登录限流窗口
	portalLogPageSize    = 50               // 日志查看每页条数
	portalExportCap      = 50000            // 单次 CSV 导出封顶行数(超出弹确认导最新这么多)
	portalExportPageSize = 500              // CSV 分页读取，避免 5 万行及详情同时堆入内存
	portalExportWindow   = 5 * time.Minute  // 导出限流:每组织账号该窗口内 1 次(仅计成功下载)
	portalLoginMaxFails  = 8                // 窗口内最多失败次数(按来源 IP)
	loginLimiterMaxKeys  = 4096             // 防大量伪造来源把限流表撑大
)

// ---- 密码(bcrypt) ----

func hashPassword(pw string) (string, error) {
	b, err := bcrypt.GenerateFromPassword([]byte(pw), bcrypt.DefaultCost)
	return string(b), err
}

func checkPassword(hash, pw string) bool {
	if hash == "" || pw == "" {
		return false
	}
	return bcrypt.CompareHashAndPassword([]byte(hash), []byte(pw)) == nil
}

// ---- 管理端:配置分组客户账号(仅超管;路由挂在管理端口) ----

// setGroupPortal POST /usage/groups/portal:开通/更新分组的客户端账号。
// {id, email, password?, clear?, reset_user_pw?}
//   - clear=true:关闭账号(清邮箱+双密码)
//   - password 留空=不改我方配置密码(但首次开通必须设);reset_user_pw=true 清客户自改密码
func (m *Monitor) setGroupPortal(c *gin.Context) {
	var in struct {
		ID          int64  `json:"id"`
		Email       string `json:"email"`
		Password    string `json:"password"`
		Clear       bool   `json:"clear"`
		ResetUserPw bool   `json:"reset_user_pw"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.ID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "id required"})
		return
	}
	var g CustomerGroup
	if err := m.storeDB.First(&g, in.ID).Error; err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "分组不存在"})
		return
	}
	if in.Clear {
		if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).
			Updates(map[string]any{"portal_email": "", "portal_pw_admin": "", "portal_pw_user": "", "portal_auth_ver": gorm.Expr("portal_auth_ver + 1")}).Error; err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusOK, gin.H{"ok": true})
		return
	}
	// 登录账号:用户名/邮箱都行,不校验格式(用户要求);仅去空格+统一小写(登录不区分大小写)
	email := strings.ToLower(strings.TrimSpace(in.Email))
	if email == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请填写登录账号"})
		return
	}
	// 账号跨组唯一(登录按账号找组,撞了就乱)
	var dup int64
	m.storeDB.Model(&CustomerGroup{}).Where("portal_email = ? AND id <> ?", email, in.ID).Count(&dup)
	if dup > 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "该账号已被其他分组使用"})
		return
	}
	upd := map[string]any{"portal_email": email}
	if in.Password != "" {
		if len(in.Password) < 8 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "密码至少 8 位"})
			return
		}
		h, err := hashPassword(in.Password)
		if err != nil {
			c.JSON(http.StatusInternalServerError, gin.H{"error": "密码处理失败"})
			return
		}
		upd["portal_pw_admin"] = h
	} else if g.PortalPwAdmin == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "首次开通必须设置密码"})
		return
	}
	if in.ResetUserPw {
		upd["portal_pw_user"] = ""
	}
	if email != g.PortalEmail || in.Password != "" || in.ResetUserPw {
		upd["portal_auth_ver"] = gorm.Expr("portal_auth_ver + 1")
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).Updates(upd).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---- 客户端会话(独立密钥域,与管理端互不相认) ----

func (m *Monitor) portalMACKey() []byte { return []byte(m.cfg.SessionSecret + "|portal") }

func (m *Monitor) signPortalSession(gid, authVer, nowUnix int64) string {
	p := fmt.Sprintf("%d|%d|%d", gid, authVer, nowUnix)
	enc := base64.RawURLEncoding.EncodeToString([]byte(p))
	mac := hmac.New(sha256.New, m.portalMACKey())
	mac.Write([]byte(enc))
	return enc + "." + hex.EncodeToString(mac.Sum(nil))
}

func (m *Monitor) verifyPortalSession(token string, nowUnix int64) (gid, authVer int64, ok bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return 0, 0, false
	}
	mac := hmac.New(sha256.New, m.portalMACKey())
	mac.Write([]byte(parts[0]))
	if !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(parts[1])) {
		return 0, 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, 0, false
	}
	f := strings.Split(string(raw), "|")
	if len(f) != 2 && len(f) != 3 {
		return 0, 0, false
	}
	gid, _ = strconv.ParseInt(f[0], 10, 64)
	var issued int64
	if len(f) == 2 {
		// 升级前会话没有版本字段，等价于版本 0；这样部署本身不强制全员掉线。
		// 一旦账号或密码发生变化，库内版本递增，旧会话仍会立即失效。
		authVer = 0
		issued, _ = strconv.ParseInt(f[1], 10, 64)
	} else {
		authVer, _ = strconv.ParseInt(f[1], 10, 64)
		issued, _ = strconv.ParseInt(f[2], 10, 64)
	}
	if gid <= 0 || nowUnix-issued > int64(portalSessionTTL.Seconds()) {
		return 0, 0, false
	}
	return gid, authVer, true
}

// ---- 登录限流(来源 IP,窗口内失败次数封顶) ----

type portalLimiter struct {
	mu sync.Mutex
	m  map[string][]int64
}

func (l *portalLimiter) tooMany(key string, now int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	ts, exists := l.m[key]
	if !exists {
		return false
	}
	cut := now - int64(portalLoginWindow.Seconds())
	kept := ts[:0]
	for _, t := range ts {
		if t > cut {
			kept = append(kept, t)
		}
	}
	if len(kept) == 0 {
		delete(l.m, key)
		return false
	}
	l.m[key] = kept
	return len(kept) >= portalLoginMaxFails
}

func (l *portalLimiter) fail(key string, now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, exists := l.m[key]; !exists && len(l.m) >= loginLimiterMaxKeys {
		l.pruneLocked(now)
		if len(l.m) >= loginLimiterMaxKeys {
			return // 入口受保护优先，宁可不记录新的来源也不能让限流表无界增长。
		}
	}
	l.m[key] = append(l.m[key], now)
}

// prune 清掉窗口内已无失败记录的键,防止攻击者用大量不同 IP/邮箱刷 /login 使 map 无界增长。
func (l *portalLimiter) prune(now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.pruneLocked(now)
}

func (l *portalLimiter) pruneLocked(now int64) {
	cut := now - int64(portalLoginWindow.Seconds())
	for k, ts := range l.m {
		kept := ts[:0]
		for _, t := range ts {
			if t > cut {
				kept = append(kept, t)
			}
		}
		if len(kept) == 0 {
			delete(l.m, k)
		} else {
			l.m[k] = kept
		}
	}
}

// ---- 导出限流(每组织账号 gid:窗口内最多 1 次,仅计成功下载) ----
// 原子预占语义:reserve 在检查通过的同一把锁内立即写入占位,并发请求不可能同时通过
// (check-then-act 分离会被并发绕过);探测/失败路径用 rollback 退回,保住"仅计成功下载"。

type exportLimiter struct {
	mu   sync.Mutex
	last map[int64]int64 // gid -> 最近一次占位(成功导出/在途预占)unix
}

// reserve 窗口内无占用则原子占位并返回旧值(供回退);否则 ok=false。
func (l *exportLimiter) reserve(gid, now, window int64) (prev int64, ok bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if now-l.last[gid] < window {
		return 0, false
	}
	prev = l.last[gid]
	l.last[gid] = now
	return prev, true
}

// rollback 撤销 reserve 的占位(探测/出错未真正下载时调用);仅当占位仍是自己写的才回退。
func (l *exportLimiter) rollback(gid, prev, reservedAt int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.last[gid] == reservedAt {
		l.last[gid] = prev
	}
}

func (l *exportLimiter) prune(now, window int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
	for k, t := range l.last {
		if now-t > window*2 {
			delete(l.last, k)
		}
	}
}

// ---- 路由注册(独立引擎,挂到独立端口) ----

func (m *Monitor) RegisterPortalRoutes(r *gin.Engine) {
	r.Use(requestBodyLimit(maxJSONRequestBody))
	if m.usageCache == nil {
		m.usageCache = newUsageResultCache(m.cfg)
	}
	if m.portalLim == nil {
		m.portalLim = &portalLimiter{m: map[string][]int64{}}
	}
	if m.exportLim == nil {
		m.exportLim = &exportLimiter{last: map[int64]int64{}}
	}
	// 限流表 GC:低频粗扫,防长期运行/被刷时缓慢增长。结果缓存自身有容量硬上限，
	// Redis 键又全部带 TTL，不需要后台全表扫描。
	go func() {
		t := time.NewTicker(10 * time.Minute)
		for range t.C {
			m.portalLim.prune(time.Now().Unix())
			m.exportLim.prune(time.Now().Unix(), int64(portalExportWindow.Seconds()))
		}
	}()

	r.GET("/echarts.js", func(c *gin.Context) { // 图表库自服务,不走 CDN(客户域名零外链)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", echartsJS)
	})
	r.GET("/flatpickr.js", func(c *gin.Context) { // 日期范围选择器,与管理端同一控件、自服务
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", flatpickrJS)
	})
	r.GET("/flatpickr.css", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "text/css; charset=utf-8", flatpickrCSS)
	})
	r.GET("/range-picker.js", func(c *gin.Context) { // 与管理端同源的中转站风格范围选择器
		// 这是会随 Monitor 功能调整的适配层，不做永久不可变缓存。
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", rangePickerJS)
	})
	r.GET("/react.js", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", reactJS)
	})
	r.GET("/react-dom.js", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", reactDOMJS)
	})
	r.GET("/semi-ui.js", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", semiUIJS)
	})
	r.GET("/semi-ui.css", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "text/css; charset=utf-8", semiUICSS)
	})
	r.GET("/login", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, portalLoginHTML)
	})
	r.POST("/login", m.portalLogin)
	r.GET("/logout", func(c *gin.Context) {
		c.SetCookie(portalSessionCookie, "", -1, "/", "", false, true)
		c.Redirect(http.StatusFound, "/login")
	})

	page := r.Group("/", m.requirePortal(false))
	page.GET("/", func(c *gin.Context) {
		c.Header("Content-Type", "text/html; charset=utf-8")
		c.String(http.StatusOK, portalHTML)
	})
	api := r.Group("/api", m.requirePortal(true))
	api.GET("/overview", m.portalOverview)
	api.GET("/breakdown", m.portalBreakdown) // 整组按分组/按模型汇总
	api.GET("/user", m.portalUserDetail)
	api.GET("/logs", m.portalLogs)              // 使用日志:游标分页查看
	api.GET("/logs/export", m.portalLogsExport) // 使用日志:CSV 导出(超5万确认/限流)
	api.POST("/password", m.portalChangePassword)
}

// requirePortal 客户会话门:apiMode=true 未登录回 401 JSON,否则 302 到 /login。
func (m *Monitor) requirePortal(apiMode bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok, _ := c.Cookie(portalSessionCookie)
		gid, authVer, ok := m.verifyPortalSession(tok, time.Now().Unix())
		if !ok {
			if apiMode {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			} else {
				c.Redirect(http.StatusFound, "/login")
				c.Abort()
			}
			return
		}
		// 会话有效但账号可能已被关闭:每次核一遍(代价=本地 sqlite 主键查,可忽略)
		var g CustomerGroup
		if err := m.storeDB.First(&g, gid).Error; err != nil || g.PortalEmail == "" || g.PortalAuthVer != authVer {
			if apiMode {
				c.AbortWithStatusJSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
			} else {
				c.Redirect(http.StatusFound, "/login")
				c.Abort()
			}
			return
		}
		c.Set("portalGID", gid)
		c.Set("portalGroupName", g.Name)
		c.Next()
	}
}

func (m *Monitor) portalLogin(c *gin.Context) {
	if !limitBodyForLogin(c) {
		return
	}
	var in struct {
		Email    string `json:"email"`
		Password string `json:"password"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请输入账号和密码"})
		return
	}
	email := strings.ToLower(strings.TrimSpace(in.Email))
	now := time.Now().Unix()
	// 按来源 IP 限制，而不是 IP+账号；否则攻击者不断换账号即可绕过限流。
	// Gin 仅接受 MONITOR_TRUSTED_PROXIES 指定反代提供的转发头，避免来源 IP 被伪造。
	limKey := c.ClientIP()
	if m.portalLim.tooMany(limKey, now) {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "尝试次数过多,请稍后再试"})
		return
	}
	var g CustomerGroup
	err := m.storeDB.Where("portal_email = ? AND portal_email <> ''", email).First(&g).Error
	// 双密码:我方配置密码 / 客户自改密码,任一匹配即可
	if err != nil || (!checkPassword(g.PortalPwAdmin, in.Password) && !checkPassword(g.PortalPwUser, in.Password)) {
		m.portalLim.fail(limKey, now)
		c.JSON(http.StatusUnauthorized, gin.H{"error": "账号或密码错误"})
		return
	}
	tok := m.signPortalSession(g.ID, g.PortalAuthVer, now)
	secure := c.Request.TLS != nil || c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(portalSessionCookie, tok, int(portalSessionTTL.Seconds()), "/", "", secure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true, "group_name": g.Name})
}

// portalChangePassword POST /api/password {old,new}:客户自改密码(写 PortalPwUser;我方配置密码始终有效)。
func (m *Monitor) portalChangePassword(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	var in struct {
		Old string `json:"old"`
		New string `json:"new"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || len(in.New) < 8 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "新密码至少 8 位"})
		return
	}
	var g CustomerGroup
	if err := m.storeDB.First(&g, gid).Error; err != nil {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	if !checkPassword(g.PortalPwAdmin, in.Old) && !checkPassword(g.PortalPwUser, in.Old) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "当前密码不正确"})
		return
	}
	h, err := hashPassword(in.New)
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "密码处理失败"})
		return
	}
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", gid).Updates(map[string]any{
		"portal_pw_user":  h,
		"portal_auth_ver": gorm.Expr("portal_auth_ver + 1"),
	}).Error; err != nil {
		slog.Warn("客户改密码写库失败", "gid", gid, "err", err) // 细节进日志,不回显库内部结构给客户
		c.JSON(http.StatusInternalServerError, gin.H{"error": "保存失败,请稍后重试"})
		return
	}
	secure := c.Request.TLS != nil || c.GetHeader("X-Forwarded-Proto") == "https"
	c.SetSameSite(http.SameSiteLaxMode)
	c.SetCookie(portalSessionCookie, "", -1, "/", "", secure, true)
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---- 组隔离数据接口(gid 只从会话取) ----

type portalOverviewPayload struct {
	GroupName        string            `json:"group_name"`
	From             string            `json:"from"`
	To               string            `json:"to"`
	Days             []string          `json:"days"`
	Users            []UsageMatrixUser `json:"users"` // 复用矩阵行结构(note/group 字段对本组无泄露风险,前端不展示 note)
	Cells            []UsageMatrixCell `json:"cells"`
	DailyByModel     []UsageDailyModel `json:"daily_by_model"` // 供首页每日消费趋势按模型堆叠展示
	ByModel          []UsageDim        `json:"by_model"`       // 供堆叠图确定 top-N 模型
	ByModelTruncated bool              `json:"by_model_truncated"`
}

// portalMemberFingerprint 只使用服务端名单中的 user_id，排序后取 SHA-256 前 128 位。
// 它不是鉴权凭据，只用于让成员增删/移动后自然换键，避免 Redis 中旧权限域被重新命中。
func portalMemberFingerprint(tracked []TrackedUser) string {
	ids := idsOf(tracked)
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	h := sha256.New()
	for _, id := range ids {
		_, _ = h.Write([]byte(strconv.FormatInt(id, 10)))
		_, _ = h.Write([]byte{','})
	}
	return hex.EncodeToString(h.Sum(nil)[:16])
}

func portalGroupAggregateKey(kind string, gid int64, memberFP string, fromTs, toTs int64) string {
	return fmt.Sprintf("agg:%s:portal:g:%d:m:%s:r:%d:%d:%s", usageAggregateKeyVersion, gid, memberFP, fromTs, toTs, kind)
}

func portalUserAggregateKey(kind string, gid int64, memberFP string, uid, tokenID, fromTs, toTs int64) string {
	return fmt.Sprintf("agg:%s:portal:g:%d:m:%s:r:%d:%d:u:%d:t:%d:%s", usageAggregateKeyVersion, gid, memberFP, fromTs, toTs, uid, tokenID, kind)
}

func (m *Monitor) portalOverview(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	val, err := m.buildPortalOverview(c, gid, fromTs, toTs)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": val})
}

func (m *Monitor) buildPortalOverview(c *gin.Context, gid, fromTs, toTs int64) (*portalOverviewPayload, error) {
	var g CustomerGroup
	if err := m.storeDB.First(&g, gid).Error; err != nil {
		return nil, err
	}
	var tracked []TrackedUser
	if err := m.storeDB.Where("group_id = ?", gid).Order("added_at").Find(&tracked).Error; err != nil {
		return nil, err
	}
	p := &portalOverviewPayload{GroupName: g.Name}
	if len(tracked) == 0 {
		mx, _ := m.computeUsageMatrix(c.Request.Context(), nil, fromTs, toTs)
		p.From, p.To, p.Days = mx.From, mx.To, mx.Days
		p.Users, p.Cells = []UsageMatrixUser{}, []UsageMatrixCell{}
		return p, nil
	}
	tracked, balances, usedTotals := m.refreshTrackedLabels(c.Request.Context(), tracked)
	if err := c.Request.Context().Err(); err != nil {
		return nil, err
	}
	ids := idsOf(tracked)
	memberFP := portalMemberFingerprint(tracked)
	cacheTTL := usageAggregateTTL(toTs, time.Now())
	mx := &UsageMatrix{}
	err := m.usageCache.DoJSON(
		c.Request.Context(),
		portalGroupAggregateKey("matrix", gid, memberFP, fromTs, toTs),
		cacheTTL,
		mx,
		func() (any, error) {
			result, err := m.computeUsageMatrix(c.Request.Context(), ids, fromTs, toTs)
			if result != nil {
				// 防御未来 computeUsageMatrix 改动：Redis 只允许稀疏消费格，不保存用户资料列。
				result.Users = nil
			}
			return result, err
		},
	)
	if err != nil {
		return nil, err
	}
	totals := map[int64]UsageBilling{}
	for _, cell := range mx.Cells {
		t := totals[cell.UserID]
		t.add(cell.UsageBilling)
		totals[cell.UserID] = t
	}
	for _, u := range tracked {
		t := totals[u.UserID]
		mu := UsageMatrixUser{UserID: u.UserID, Username: u.Username, Email: u.Email,
			ConsumeQuota: t.ConsumeQuota, RefundQuota: t.RefundQuota, NetQuota: t.NetQuota,
			TotalUSD: t.CostUSD, RefundUSD: t.RefundUSD, NetUSD: t.NetUSD}
		if b, ok := balances[u.UserID]; ok {
			bq := b
			bv := float64(bq) / quotaPerUSD
			mu.BalanceQuota, mu.BalanceUSD = &bq, &bv
		}
		if uq, ok := usedTotals[u.UserID]; ok {
			usedQ := uq
			uv := float64(usedQ) / quotaPerUSD
			mu.TotalUsedQuota, mu.TotalUsedUSD = &usedQ, &uv
		}
		p.Users = append(p.Users, mu)
	}
	sortPortalUsers(p.Users)
	p.From, p.To, p.Days, p.Cells = mx.From, mx.To, mx.Days, mx.Cells
	// 供首页每日趋势图按模型堆叠展示:查询同范围内按日×模型的聚合(复用 computeUsageStats 内部逻辑)
	st := &UsageStats{}
	err = m.usageCache.DoJSON(
		c.Request.Context(),
		portalGroupAggregateKey("stats", gid, memberFP, fromTs, toTs),
		cacheTTL,
		st,
		func() (any, error) { return m.computeUsageStats(c.Request.Context(), ids, fromTs, toTs, 0) },
	)
	if err != nil {
		return nil, err
	}
	p.DailyByModel = st.DailyByModel
	p.ByModel = st.ByModel
	p.ByModelTruncated = st.ByModelTruncated
	return p, nil
}

// sortPortalUsers 与管理端同规则:累计总消耗降序,稳定。
func sortPortalUsers(users []UsageMatrixUser) {
	usedOf := func(u UsageMatrixUser) int64 {
		if u.TotalUsedQuota != nil {
			return *u.TotalUsedQuota
		}
		return 0
	}
	sort.SliceStable(users, func(i, j int) bool {
		ui, uj := usedOf(users[i]), usedOf(users[j])
		if ui != uj {
			return ui > uj
		}
		return users[i].Username < users[j].Username
	})
}

// portalBreakdown GET /api/breakdown:整组(公司)按分组 + 按模型的汇总。
// 与 overview 复用完全相同的 stats 聚合键，避免同一范围冷启动重复扫描日志。
func (m *Monitor) portalBreakdown(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var tracked []TrackedUser
	if err := m.storeDB.Where("group_id = ?", gid).Find(&tracked).Error; err != nil {
		slog.Warn("查询客户组成员失败", "gid", gid, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	ids := idsOf(tracked)
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{
			"by_group": []UsageDim{}, "by_model": []UsageDim{},
			"by_group_truncated": false, "by_model_truncated": false,
		}})
		return
	}
	st := &UsageStats{}
	cacheTTL := usageAggregateTTL(toTs, time.Now())
	err = m.usageCache.DoJSON(
		c.Request.Context(),
		portalGroupAggregateKey("stats", gid, portalMemberFingerprint(tracked), fromTs, toTs),
		cacheTTL,
		st,
		func() (any, error) { return m.computeUsageStats(c.Request.Context(), ids, fromTs, toTs, 0) },
	)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{
		"by_group": st.ByGroup, "by_model": st.ByModel,
		"by_group_truncated": st.ByGroupTruncated, "by_model_truncated": st.ByModelTruncated,
	}})
}

func (m *Monitor) portalUserDetail(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	uid, _ := strconv.ParseInt(c.Query("uid"), 10, 64)
	if uid <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "uid required"})
		return
	}
	// 越权闸:uid 必须是本组成员。顺路取得完整成员集合，服务端成员指纹进入缓存键。
	var tracked []TrackedUser
	if err := m.storeDB.Where("group_id = ?", gid).Order("user_id").Find(&tracked).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	found := false
	var member TrackedUser
	for _, u := range tracked {
		if u.UserID == uid {
			found = true
			member = u
			break
		}
	}
	if !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	// 令牌下钻(可选):聚合强制 uid+token_id 双条件,别组令牌只会查出空,不做归属探测响应差异
	var tokenID int64
	if t := strings.TrimSpace(c.Query("token_id")); t != "" {
		tokenID, _ = strconv.ParseInt(t, 10, 64)
		if tokenID <= 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "token_id 不合法"})
			return
		}
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	memberFP := portalMemberFingerprint(tracked)
	cacheTTL := usageAggregateTTL(toTs, time.Now())
	st := &UsageStats{}
	err = m.usageCache.DoJSON(
		c.Request.Context(),
		portalUserAggregateKey("stats", gid, memberFP, uid, tokenID, fromTs, toTs),
		cacheTTL,
		st,
		func() (any, error) {
			return m.computeUsageStats(c.Request.Context(), []int64{uid}, fromTs, toTs, tokenID)
		},
	)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	if tokenID > 0 { // 令牌元数据保持实时查询；缓存中只有日志聚合数字。
		c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{
			"stats": st, "token": m.tokenMetaOf(c.Request.Context(), uid, tokenID),
		}})
		return
	}

	var tokenAggregates []tokenUsageAggregate
	err = m.usageCache.DoJSON(
		c.Request.Context(),
		portalUserAggregateKey("tokens", gid, memberFP, uid, 0, fromTs, toTs),
		cacheTTL,
		&tokenAggregates,
		func() (any, error) {
			return m.computeUserTokenAggregates(c.Request.Context(), uid, fromTs, toTs)
		},
	)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	// owner 最终仍由下方的 users 实时查询校准；先传本地快照只是避免
	// hydrateUserTokenUsage 内再发起一次必然会被后续结果覆盖的 users 查询。
	ownerSnapshot := trackedLabel(member)
	toks, err := m.hydrateUserTokenUsage(c.Request.Context(), uid, tokenAggregates, &ownerSnapshot)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	// 用户名/邮箱/余额/累计总消耗保持实时：一个 users 主键查询同时取回四项，
	// 不进入 Redis。失败时沿用本地脱敏展示名，金额显示 —，不阻断历史统计。
	fresh, balances, usedTotals := m.refreshTrackedLabels(c.Request.Context(), []TrackedUser{member})
	if err := c.Request.Context().Err(); err != nil {
		return
	}
	owner := trackedLabel(member)
	if len(fresh) == 1 {
		owner = trackedLabel(fresh[0])
	}
	for i := range toks {
		toks[i].Owner = owner
	}
	var balanceQ, usedQ *int64
	if v, ok := balances[uid]; ok {
		q := v
		balanceQ = &q
	}
	if v, ok := usedTotals[uid]; ok {
		q := v
		usedQ = &q
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": gin.H{
		"stats":            st,
		"by_token":         toks, // key 始终在 hydrateUserTokenUsage 的实时元数据阶段脱敏
		"balance_quota":    balanceQ,
		"balance_usd":      quotaUSDPtr(balanceQ),
		"total_used_quota": usedQ,
		"total_used_usd":   quotaUSDPtr(usedQ),
	}})
}

// ---- 使用日志(逐条明细):查看 + CSV 导出 ----

// errMemberNotInGroup:成员筛选值不属本组(越权探测)。对外统一装作 404,不暴露"组里有没有这个人"。
var errMemberNotInGroup = errors.New("member not in group")

// portalLogParamsError 统一处理 portalLogParams 的错误响应:越权探测→404,其余参数错→400。
func portalLogParamsError(c *gin.Context, err error) {
	if errors.Is(err, errMemberNotInGroup) {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
}

// portalLogParams 解析并校验日志相关的公共参数(组隔离:成员必属本组)。
func (m *Monitor) portalLogParams(c *gin.Context) (gid int64, ids []int64, memberUID, fromTs, toTs int64, logType int, model, group, tokenName, detailKw, requestID string, err error) {
	gid = c.GetInt64("portalGID")
	fromTs, toTs, err = parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		return
	}
	var tracked []TrackedUser
	m.storeDB.Where("group_id = ?", gid).Find(&tracked)
	ids = idsOf(tracked)
	memberUID, _ = strconv.ParseInt(c.Query("member"), 10, 64)
	if memberUID > 0 { // 成员筛选值必须属本组(越权闸)
		in := false
		for _, id := range ids {
			if id == memberUID {
				in = true
				break
			}
		}
		if !in {
			err = errMemberNotInGroup
			return
		}
	}
	if t, e := strconv.Atoi(c.Query("type")); e == nil && t >= 1 && t <= 6 {
		logType = t // 具体类型(1-6 全部开放,同 new-api 官方客户端使用日志);其余(0/越界)= 全部
	}
	model = strings.TrimSpace(c.Query("model"))
	group = strings.TrimSpace(c.Query("group"))
	tokenName = strings.TrimSpace(c.Query("token"))
	if len(tokenName) > 64 { // 令牌名搜索限长:防超长 LIKE 模式拖慢生产库查询
		err = fmt.Errorf("令牌名搜索最长 64 字符")
		return
	}
	detailKw = strings.TrimSpace(c.Query("detail_kw"))
	if len(detailKw) > 64 { // 详情关键字搜索同限长,同一防御理由
		err = fmt.Errorf("详情关键字搜索最长 64 字符")
		return
	}
	requestID = strings.TrimSpace(c.Query("request_id"))
	if len(requestID) > 64 { // request_id 精确匹配,同一限长防御(new-api request_id 列本身是 varchar(64))
		err = fmt.Errorf("Request ID 最长 64 字符")
		return
	}
	return
}

// portalLogs GET /api/logs:游标分页看本组消费日志(时间倒序)。
func (m *Monitor) portalLogs(c *gin.Context) {
	gid, ids, memberUID, fromTs, toTs, logType, model, group, tokenName, detailKw, requestID, err := m.portalLogParams(c)
	if err != nil {
		portalLogParamsError(c, err)
		return
	}
	_ = gid
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "rows": []LogRow{}, "has_more": false})
		return
	}
	beforeID, _ := strconv.ParseInt(c.Query("cursor"), 10, 64)
	rows, err := m.queryGroupLogs(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID, beforeID, portalLogPageSize+1)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	hasMore := len(rows) > portalLogPageSize
	if hasMore {
		rows = rows[:portalLogPageSize]
	}
	var next int64
	if len(rows) > 0 {
		next = rows[len(rows)-1].ID
	}
	resp := gin.H{"ok": true, "rows": rows, "has_more": hasMore, "next_cursor": next}
	if beforeID == 0 { // 仅首页(筛选变更时)数一次总条数,前端据此算总页数并在翻页时复用
		if total, err := m.countGroupLogs(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID); err == nil {
			resp["total"] = total
		}
	}
	c.JSON(http.StatusOK, resp)
}

// csvSafe 防 CSV 公式注入:文本以 = + - @ 制表符或回车开头时前置单引号,Excel/WPS 打开时按文本处理。
// 令牌名/详情等值最终来自用户可控输入(如用户把令牌起名 =HYPERLINK(...)),导出前必须消毒。
func csvSafe(s string) string {
	if s == "" {
		return s
	}
	switch s[0] {
	case '=', '+', '-', '@', '\t', '\r':
		return "'" + s
	}
	return s
}

// portalLogsExport GET /api/logs/export:CSV 导出。顺序:限流(1次/5min,仅计成功下载)→ COUNT 探测
// (超 5 万条且未确认→need_confirm,不拉行)→ 拉行(封顶 5 万)→ CSV。超一年由 parseUsageRange 拒。
func (m *Monitor) portalLogsExport(c *gin.Context) {
	gid, ids, memberUID, fromTs, toTs, logType, model, group, tokenName, detailKw, requestID, err := m.portalLogParams(c)
	if err != nil {
		portalLogParamsError(c, err)
		return
	}
	if len(ids) == 0 {
		c.JSON(http.StatusOK, gin.H{"error": "本组暂无成员日志"})
		return
	}
	// 限流前置 + 原子预占:窗口内已有占用直接拒,重查询根本不执行;并发请求只有一个能占到位。
	// 探测(need_confirm)/查询失败走 rollback 不计次,仅成功下载保留占位(defer 统一处理)。
	now := time.Now().Unix()
	prev, ok := m.exportLim.reserve(gid, now, int64(portalExportWindow.Seconds()))
	if !ok {
		c.JSON(http.StatusTooManyRequests, gin.H{"error": "导出过于频繁,请 5 分钟后再试"})
		return
	}
	downloaded := false
	defer func() {
		if !downloaded {
			m.exportLim.rollback(gid, prev, now)
		}
	}()
	// COUNT 只取数量、不拉 5 万整行；其执行计划仍取决于生产库索引，受统一查询闸门和 15 秒上限约束。
	total, err := m.countGroupLogs(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID)
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	confirm := c.Query("confirm") == "1"
	if total > portalExportCap && !confirm { // 超上限、未确认:让前端弹确认框(不消耗限流、不拉行)
		c.JSON(http.StatusOK, gin.H{"need_confirm": true, "cap": portalExportCap})
		return
	}
	fname := fmt.Sprintf("usage-logs-%s_%s.csv",
		time.Unix(fromTs, 0).In(usageCST).Format("20060102"),
		time.Unix(toTs-1, 0).In(usageCST).Format("20060102"))
	c.Header("Content-Disposition", "attachment; filename=\""+fname+"\"")
	c.Header("Content-Type", "text/csv; charset=utf-8")
	c.Status(http.StatusOK)
	// 直接写响应，内存中只保留一页(最多 500 行)。保留 5 万行总量上限，
	// 它仍用于约束生产库查询和下载时间；流式写出只解决内存随导出量增长的问题。
	_, _ = c.Writer.Write([]byte("\xEF\xBB\xBF")) // Excel UTF-8 BOM
	w := csv.NewWriter(c.Writer)
	if err := w.Write([]string{"时间", "成员", "令牌", "分组", "类型", "模型", "用时(秒)", "输入tokens", "输出tokens", "费用(美元)", "详情", "Request ID"}); err != nil {
		slog.Warn("客户端 CSV 表头写入失败", "gid", gid, "err", err)
		return
	}
	w.Flush()
	if err := w.Error(); err != nil {
		slog.Warn("客户端 CSV 表头刷新失败", "gid", gid, "err", err)
		return
	}
	downloaded = true // 已开始向客户端写出，保留预占，避免中断后无限重复占用生产库。

	var beforeID int64
	remaining := portalExportCap
	for remaining > 0 {
		limit := portalExportPageSize
		if remaining < limit {
			limit = remaining
		}
		rows, qerr := m.queryGroupLogs(c.Request.Context(), ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID, beforeID, limit)
		if qerr != nil {
			if isCanceledUsageRequest(c, qerr) {
				return
			}
			// 响应已开始，不能再写 JSON；日志记录原因，客户端下载到的文件仍是有效的 CSV 前缀。
			slog.Warn("客户端 CSV 分页查询失败", "gid", gid, "err", qerr)
			return
		}
		if len(rows) == 0 {
			break
		}
		for _, row := range rows {
			if err := w.Write(portalCSVRecord(row)); err != nil {
				slog.Warn("客户端 CSV 写入失败", "gid", gid, "err", err)
				return
			}
		}
		w.Flush()
		if err := w.Error(); err != nil {
			slog.Warn("客户端 CSV 刷新失败", "gid", gid, "err", err)
			return
		}
		remaining -= len(rows)
		beforeID = rows[len(rows)-1].ID
		if len(rows) < limit {
			break
		}
	}
}

// portalCSVRecord 把允许给客户导出的字段转成一行。详情保留原始内容；csvSafe
// 防 Excel/WPS 把用户可控文本当作公式执行。
func portalCSVRecord(r LogRow) []string {
	cost := ""
	if r.Type == 2 {
		cost = strconv.FormatFloat(r.CostUSD, 'f', 6, 64)
	}
	return []string{
		time.Unix(r.CreatedAt, 0).In(usageCST).Format("2006-01-02 15:04:05"),
		csvSafe(r.Member), csvSafe(r.TokenName), csvSafe(r.Group), logTypeName(r.Type), csvSafe(r.ModelName),
		strconv.FormatInt(r.UseTime, 10), strconv.FormatInt(r.PromptTokens, 10), strconv.FormatInt(r.CompletionTokens, 10),
		cost, csvSafe(r.Detail), csvSafe(r.RequestID),
	}
}

// invalidatePortalAggregates 管理端手动刷新矩阵后，精确删除同范围内各客户组的
// matrix/stats 两个聚合键。删除失败不影响响应；旧结果仍受自身有界 TTL 约束。
func (m *Monitor) invalidatePortalAggregates(tracked []TrackedUser, fromTs, toTs int64) {
	if m.usageCache == nil {
		return
	}
	byGroup := make(map[int64][]TrackedUser)
	for _, u := range tracked {
		if u.GroupID > 0 {
			byGroup[u.GroupID] = append(byGroup[u.GroupID], u)
		}
	}
	keys := make([]string, 0, len(byGroup)*2)
	for gid, members := range byGroup {
		memberFP := portalMemberFingerprint(members)
		keys = append(keys,
			portalGroupAggregateKey("matrix", gid, memberFP, fromTs, toTs),
			portalGroupAggregateKey("stats", gid, memberFP, fromTs, toTs),
		)
	}
	m.usageCache.Delete(context.Background(), keys...)
}

// invalidatePortalUserAggregates 在管理端主动刷新某用户/令牌后，精确删除该客户组的
// 对应下钻聚合。指纹必须使用整个客户组的当前成员，不能只用被查的单个用户。
func (m *Monitor) invalidatePortalUserAggregates(tracked []TrackedUser, uid, tokenID, fromTs, toTs int64) {
	if m.usageCache == nil || uid <= 0 {
		return
	}
	var gid int64
	for _, u := range tracked {
		if u.UserID == uid {
			gid = u.GroupID
			break
		}
	}
	if gid <= 0 {
		return // 未分组用户没有 Usage Portal 权限域。
	}
	members := make([]TrackedUser, 0)
	for _, u := range tracked {
		if u.GroupID == gid {
			members = append(members, u)
		}
	}
	memberFP := portalMemberFingerprint(members)
	keys := []string{portalUserAggregateKey("stats", gid, memberFP, uid, tokenID, fromTs, toTs)}
	if tokenID == 0 {
		keys = append(keys, portalUserAggregateKey("tokens", gid, memberFP, uid, 0, fromTs, toTs))
	}
	m.usageCache.Delete(context.Background(), keys...)
}
