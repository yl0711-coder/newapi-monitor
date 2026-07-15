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
// 容量设计(100-200 人同时看没事):组级 TTL 缓存 + singleflight(cache.go),
// 生产库压力 ≤ 每组每 TTL 一条小查询;管理端刷新时按组切片写穿透预热(portalWarmFromMatrix)。

import (
	"crypto/hmac"
	"crypto/sha256"
	_ "embed"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"golang.org/x/crypto/bcrypt"
)

//go:embed portal.html
var portalHTML string

//go:embed portal_login.html
var portalLoginHTML string

const (
	portalSessionCookie = "report_session" // 中性命名,不带 monitor 字样
	portalSessionTTL    = 12 * time.Hour
	portalCacheTTL      = 60 * time.Second // 组级数据缓存;客户看到的数据最多滞后 60s
	portalLoginWindow   = 10 * time.Minute // 登录限流窗口
	portalLoginMaxFails = 8                // 窗口内最多失败次数(按 IP+邮箱)
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
			Updates(map[string]any{"portal_email": "", "portal_pw_admin": "", "portal_pw_user": ""}).Error; err != nil {
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
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", in.ID).Updates(upd).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---- 客户端会话(独立密钥域,与管理端互不相认) ----

func (m *Monitor) portalMACKey() []byte { return []byte(m.cfg.SessionSecret + "|portal") }

func (m *Monitor) signPortalSession(gid int64, nowUnix int64) string {
	p := fmt.Sprintf("%d|%d", gid, nowUnix)
	enc := base64.RawURLEncoding.EncodeToString([]byte(p))
	mac := hmac.New(sha256.New, m.portalMACKey())
	mac.Write([]byte(enc))
	return enc + "." + hex.EncodeToString(mac.Sum(nil))
}

func (m *Monitor) verifyPortalSession(token string, nowUnix int64) (gid int64, ok bool) {
	parts := strings.SplitN(token, ".", 2)
	if len(parts) != 2 {
		return 0, false
	}
	mac := hmac.New(sha256.New, m.portalMACKey())
	mac.Write([]byte(parts[0]))
	if !hmac.Equal([]byte(hex.EncodeToString(mac.Sum(nil))), []byte(parts[1])) {
		return 0, false
	}
	raw, err := base64.RawURLEncoding.DecodeString(parts[0])
	if err != nil {
		return 0, false
	}
	f := strings.Split(string(raw), "|")
	if len(f) != 2 {
		return 0, false
	}
	gid, _ = strconv.ParseInt(f[0], 10, 64)
	issued, _ := strconv.ParseInt(f[1], 10, 64)
	if gid <= 0 || nowUnix-issued > int64(portalSessionTTL.Seconds()) {
		return 0, false
	}
	return gid, true
}

// ---- 登录限流(IP+邮箱,窗口内失败次数封顶) ----

type portalLimiter struct {
	mu sync.Mutex
	m  map[string][]int64
}

func (l *portalLimiter) tooMany(key string, now int64) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	cut := now - int64(portalLoginWindow.Seconds())
	kept := l.m[key][:0]
	for _, t := range l.m[key] {
		if t > cut {
			kept = append(kept, t)
		}
	}
	l.m[key] = kept
	return len(kept) >= portalLoginMaxFails
}

func (l *portalLimiter) fail(key string, now int64) {
	l.mu.Lock()
	l.m[key] = append(l.m[key], now)
	l.mu.Unlock()
}

// prune 清掉窗口内已无失败记录的键,防止攻击者用大量不同 IP/邮箱刷 /login 使 map 无界增长。
func (l *portalLimiter) prune(now int64) {
	l.mu.Lock()
	defer l.mu.Unlock()
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

// ---- 路由注册(独立引擎,挂到独立端口) ----

func (m *Monitor) RegisterPortalRoutes(r *gin.Engine) {
	if m.portalCache == nil {
		m.portalCache = newTTLCache()
	}
	if m.portalLim == nil {
		m.portalLim = &portalLimiter{m: map[string][]int64{}}
	}
	// 缓存 + 限流表 GC:低频粗扫,防长期运行/被刷时缓慢增长
	go func() {
		t := time.NewTicker(10 * time.Minute)
		for range t.C {
			m.portalCache.gc()
			m.portalLim.prune(time.Now().Unix())
		}
	}()

	r.GET("/echarts.js", func(c *gin.Context) { // 图表库自服务,不走 CDN(客户域名零外链)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", echartsJS)
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
	api.POST("/password", m.portalChangePassword)
}

// requirePortal 客户会话门:apiMode=true 未登录回 401 JSON,否则 302 到 /login。
func (m *Monitor) requirePortal(apiMode bool) gin.HandlerFunc {
	return func(c *gin.Context) {
		tok, _ := c.Cookie(portalSessionCookie)
		gid, ok := m.verifyPortalSession(tok, time.Now().Unix())
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
		if err := m.storeDB.First(&g, gid).Error; err != nil || g.PortalEmail == "" {
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
	limKey := c.ClientIP() + "|" + email
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
	tok := m.signPortalSession(g.ID, now)
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
	if err := m.storeDB.Model(&CustomerGroup{}).Where("id = ?", gid).Update("portal_pw_user", h).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

// ---- 组隔离数据接口(gid 只从会话取) ----

type portalOverviewPayload struct {
	GroupName string            `json:"group_name"`
	From      string            `json:"from"`
	To        string            `json:"to"`
	Days      []string          `json:"days"`
	Users     []UsageMatrixUser `json:"users"` // 复用矩阵行结构(note/group 字段对本组无泄露风险,前端不展示 note)
	Cells     []UsageMatrixCell `json:"cells"`
}

func portalOverviewKey(gid, fromTs, toTs int64) string {
	return fmt.Sprintf("ov|%d|%d|%d", gid, fromTs, toTs)
}

func (m *Monitor) portalOverview(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	val, err := m.portalCache.Do(portalOverviewKey(gid, fromTs, toTs), portalCacheTTL, func() (any, error) {
		return m.buildPortalOverview(c, gid, fromTs, toTs)
	})
	if err != nil {
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
	mx, err := m.computeUsageMatrix(c.Request.Context(), idsOf(tracked), fromTs, toTs)
	if err != nil {
		return nil, err
	}
	totals := map[int64]float64{}
	for _, cell := range mx.Cells {
		totals[cell.UserID] += cell.CostUSD
	}
	for _, u := range tracked {
		mu := UsageMatrixUser{UserID: u.UserID, Username: u.Username, Email: u.Email, TotalUSD: totals[u.UserID]}
		if b, ok := balances[u.UserID]; ok {
			bv := b
			mu.BalanceUSD = &bv
		}
		if uq, ok := usedTotals[u.UserID]; ok {
			uv := uq
			mu.TotalUsedUSD = &uv
		}
		p.Users = append(p.Users, mu)
	}
	sortPortalUsers(p.Users)
	p.From, p.To, p.Days, p.Cells = mx.From, mx.To, mx.Days, mx.Cells
	return p, nil
}

// sortPortalUsers 与管理端同规则:累计总消耗降序,稳定。
func sortPortalUsers(users []UsageMatrixUser) {
	usedOf := func(u UsageMatrixUser) float64 {
		if u.TotalUsedUSD != nil {
			return *u.TotalUsedUSD
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

// portalBreakdown GET /api/breakdown:整组(公司)按分组 + 按模型的汇总,独立缓存(不走写穿透)。
func (m *Monitor) portalBreakdown(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	var tracked []TrackedUser
	m.storeDB.Where("group_id = ?", gid).Find(&tracked)
	ids := idsOf(tracked)
	key := fmt.Sprintf("bd|%d|%d|%d", gid, fromTs, toTs)
	val, err := m.portalCache.Do(key, portalCacheTTL, func() (any, error) {
		if len(ids) == 0 {
			return gin.H{"by_group": []UsageDim{}, "by_model": []UsageDim{}}, nil
		}
		st, err := m.computeUsageStats(c.Request.Context(), ids, fromTs, toTs)
		if err != nil {
			return nil, err
		}
		return gin.H{"by_group": st.ByGroup, "by_model": st.ByModel}, nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": val})
}

func (m *Monitor) portalUserDetail(c *gin.Context) {
	gid := c.GetInt64("portalGID")
	uid, _ := strconv.ParseInt(c.Query("uid"), 10, 64)
	if uid <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "uid required"})
		return
	}
	// 越权闸:uid 必须是本组成员
	var cnt int64
	m.storeDB.Model(&TrackedUser{}).Where("group_id = ? AND user_id = ?", gid, uid).Count(&cnt)
	if cnt == 0 {
		c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		return
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	key := fmt.Sprintf("ud|%d|%d|%d|%d", gid, uid, fromTs, toTs)
	val, err := m.portalCache.Do(key, portalCacheTTL, func() (any, error) {
		st, err := m.computeUsageStats(c.Request.Context(), []int64{uid}, fromTs, toTs)
		if err != nil {
			return nil, err
		}
		toks, err := m.computeUserTokenUsage(c.Request.Context(), uid, fromTs, toTs)
		if err != nil {
			return nil, err
		}
		return gin.H{
			"stats":          st,   // 含每日/按分组/按模型(客户可看自己的分组与模型用量)
			"by_token":       toks, // 该用户各令牌用量,key 已脱敏
			"balance_usd":    m.userBalanceUSD(c.Request.Context(), uid),
			"total_used_usd": m.userUsedUSD(c.Request.Context(), uid),
		}, nil
	})
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "查询失败,请稍后重试"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "data": val})
}

// ---- 写穿透预热:管理端矩阵查询完成后按组切片灌缓存(管理端自身不读缓存) ----

func (m *Monitor) portalWarmFromMatrix(mx *UsageMatrix, tracked []TrackedUser, fromTs, toTs int64) {
	if m.portalCache == nil || mx == nil {
		return
	}
	gm := m.groupNameMap()
	byGid := map[int64]*portalOverviewPayload{}
	uidGid := map[int64]int64{}
	for _, u := range tracked {
		uidGid[u.UserID] = u.GroupID
	}
	for _, u := range mx.Users {
		gid := uidGid[u.UserID]
		if gid <= 0 {
			continue
		}
		p := byGid[gid]
		if p == nil {
			p = &portalOverviewPayload{GroupName: gm[gid], From: mx.From, To: mx.To, Days: mx.Days}
			byGid[gid] = p
		}
		cu := u
		cu.GroupID, cu.GroupName, cu.Note = 0, "", "" // 客户端载荷不带内部备注
		p.Users = append(p.Users, cu)
	}
	for _, cell := range mx.Cells {
		gid := uidGid[cell.UserID]
		if p := byGid[gid]; p != nil {
			p.Cells = append(p.Cells, cell)
		}
	}
	for gid, p := range byGid {
		sortPortalUsers(p.Users)
		if p.Cells == nil {
			p.Cells = []UsageMatrixCell{}
		}
		m.portalCache.Put(portalOverviewKey(gid, fromTs, toTs), p, portalCacheTTL)
	}
}
