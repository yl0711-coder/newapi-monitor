package monitor

import (
	"crypto/subtle"
	_ "embed"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"

	"github.com/yl0711-coder/newapi-monitor/monitor/public"
)

// server.go:监控自带的 HTTP 层。页面是自包含的静态 HTML(内嵌),数据走 /data JSON。

//go:embed page.html
var pageHTML string

//go:embed alert.html
var alertPageHTML string

//go:embed login.html
var loginHTML string

//go:embed echarts.min.js
var echartsJS []byte // 内嵌 ECharts(Apache 2.0),自服务、不走 CDN,保持自包含

//go:embed flatpickr.min.js
var flatpickrJS []byte // 内嵌 flatpickr v4.6.13+zh 语言包(MIT),用量页日期范围选择器

//go:embed flatpickr.min.css
var flatpickrCSS []byte // flatpickr 暗色主题

var allowedWindows = map[int]bool{15: true, 30: true, 60: true, 180: true, 360: true, 720: true, 1440: true}

func parseWindow(c *gin.Context) int {
	w, _ := strconv.Atoi(c.DefaultQuery("window", "60"))
	if !allowedWindows[w] {
		w = 60
	}
	return w
}

// RegisterRoutes 把监控的页面与数据接口挂到给定的 gin 引擎上。
// 鉴权:登录复用 new-api 身份;>=管理员可看监控,仅超级管理员可改配置。
func (m *Monitor) RegisterRoutes(r *gin.Engine) {
	// 公开:登录/登出/健康检查/站点名
	r.GET("/health", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"status": "ok"}) })
	r.GET("/echarts.js", func(c *gin.Context) { // 公开:内嵌 ECharts,自服务、版本固定可长期缓存
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", echartsJS)
	})
	r.GET("/flatpickr.js", func(c *gin.Context) { // 公开:内嵌 flatpickr(同 echarts,自服务)
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", flatpickrJS)
	})
	r.GET("/flatpickr.css", func(c *gin.Context) {
		c.Header("Cache-Control", "public, max-age=31536000, immutable")
		c.Data(http.StatusOK, "text/css; charset=utf-8", flatpickrCSS)
	})
	r.GET("/api/brand", m.brandHandler)                // 公开:站点名,供前端设置页面标题
	r.POST("/internal/rejections", m.ingestRejections) // 机器对机器:接收采集器推送的前置拒绝(token 鉴权)
	r.POST("/internal/host", m.ingestHost)             // 机器对机器:接收各节点主机 agent 推送的 OS 内存/磁盘(token 鉴权)
	r.GET("/login", m.loginPage)
	r.POST("/login", m.loginSubmit)
	r.GET("/logout", logout)
	r.POST("/logout", logout)

	// 需登录(管理员及以上):看监控
	view := r.Group("/", m.requireRole(roleAdmin))
	{
		view.GET("/", m.servePage)
		view.GET("/monitor", m.servePage)
		view.GET("/data", m.serveData)
		view.GET("/monitor/data", m.serveData)
		view.GET("/trend/long", m.serveLongTrend)
		view.GET("/infra", m.serveInfra)              // 服务端健康监控(实例/DB/LB)快照
		view.GET("/infra/series", m.serveInfraSeries) // 按需取某资源某些指标的近 N 小时序列(展开图用)
		view.GET("/usage/users", m.listTrackedUsers)  // 用户用量:被盯名单(含分组)
		view.GET("/usage/groups", m.listGroups)       // 用户用量:客户分组列表
		view.GET("/usage/matrix", m.serveUsageMatrix) // 用户用量:列表页矩阵(前端渲染 行=用户×列=日期,格=当日费用)
		view.GET("/usage/stats", m.serveUsageStats)   // 用户用量:单用户详情聚合(每日/分组/模型/费用)
		view.GET("/me", me)
	}

	// 仅超级管理员:报警配置(看 + 改)
	root := r.Group("/alert", m.requireRole(roleRoot))
	{
		root.GET("", func(c *gin.Context) { c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(alertPageHTML)) })
		root.GET("/config", m.getAlertConfig)
		root.POST("/config", m.saveAlertConfigHandler)
		root.POST("/test", m.testAlertHandler)
		root.POST("/smtp/sync", m.syncSMTPHandler) // 「使用主站配置」:从 new-api 同步 SMTP
	}

	// 仅超级管理员:用户用量名单增删(看名单/看统计在上面 view 组,管理员即可)
	rootUsage := r.Group("/usage", m.requireRole(roleRoot))
	{
		rootUsage.POST("/users", m.addTrackedUser)
		rootUsage.POST("/users/delete", m.deleteTrackedUser)
		rootUsage.POST("/users/group", m.setUserGroup)  // 改用户归属分组
		rootUsage.POST("/users/note", m.setUserNote)    // 改用户备注
		rootUsage.POST("/groups", m.createGroup)        // 客户分组:新建
		rootUsage.POST("/groups/update", m.updateGroup) // 客户分组:编辑
		rootUsage.POST("/groups/delete", m.deleteGroup) // 客户分组:解散(成员回未分组)
	}
}

// ingestRejections 接收各节点 newapi-reject-collector 推来的前置拒绝计数(token 鉴权)。
// 未配置 MONITOR_INGEST_TOKEN 则接口关闭(503),不接受任何推送。
func (m *Monitor) ingestRejections(c *gin.Context) {
	want := m.cfg.IngestToken
	if want == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ingest disabled"})
		return
	}
	if subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+want)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	var in struct {
		Node    string `json:"node"`
		Samples []struct {
			BucketTs int64  `json:"bucket_ts"`
			Reason   string `json:"reason"`
			Model    string `json:"model"`
			Group    string `json:"group"`
			Count    int64  `json:"count"`
		} `json:"samples"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if len(in.Samples) > 5000 { // 防异常大包
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "too many samples"})
		return
	}
	node := clip(in.Node, 64)
	rows := make([]RejectionSample, 0, len(in.Samples))
	for _, s := range in.Samples {
		if s.Model == "" || s.Reason == "" || s.Count <= 0 || s.BucketTs <= 0 {
			continue // 丢弃残缺项
		}
		rows = append(rows, RejectionSample{
			BucketTs: s.BucketTs / 60 * 60,
			Node:     node,
			Reason:   clip(s.Reason, 64),
			Model:    clip(s.Model, 128),
			Grp:      clip(s.Group, 64),
			Count:    s.Count,
		})
	}
	if err := m.upsertRejections(rows); err != nil {
		slog.Warn("被拒请求入库失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "stored": len(rows)})
}

// serveInfra 返回服务端健康监控(实例/DB/LB)快照;未启用则 enabled:false。
func (m *Monitor) serveInfra(c *gin.Context) {
	if !m.InfraEnabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "snapshot": m.computeInfraSnapshot(time.Now().Unix())})
}

// serveInfraSeries 按需返回某资源(resource)若干指标(metrics 逗号分隔)近 N 小时(hours,默认6,封顶24)的时序。
// 展开实例/切换指标组时前端才拉,避免快照一次性塞满所有图。结果:{series:{metric:[{ts,value}]}}。
func (m *Monitor) serveInfraSeries(c *gin.Context) {
	if !m.InfraEnabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	resource := strings.TrimSpace(c.Query("resource"))
	if resource == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "resource required"})
		return
	}
	hours := 6
	if v, err := strconv.Atoi(c.Query("hours")); err == nil && v > 0 {
		hours = v
	}
	if hours > 24 {
		hours = 24
	}
	since := time.Now().Unix() - int64(hours)*3600
	series := map[string][]InfraPoint{}
	for _, met := range strings.Split(c.Query("metrics"), ",") {
		met = strings.TrimSpace(met)
		if met == "" {
			continue
		}
		series[met] = m.storeInfraSeries(resource, met, since)
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "series": series})
}

// ingestHost 接收各节点主机 agent 推来的 OS 指标(内存/磁盘/load),写 infra_samples(rtype=host)。
// 复用 MONITOR_INGEST_TOKEN 鉴权;未配置则接口关闭(503)。只接非敏感数值,不含任何密钥/业务数据。
func (m *Monitor) ingestHost(c *gin.Context) {
	want := m.cfg.IngestToken
	if want == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ingest disabled"})
		return
	}
	if subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+want)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return
	}
	// 指标用指针:agent 某项采集失败会省略该字段,这里就不写——避免「缺失=0」被算成异常(如可用 0=已用 100%)。
	var in struct {
		Node            string   `json:"node"`
		MemTotalMB      *float64 `json:"mem_total_mb"`
		MemAvailMB      *float64 `json:"mem_avail_mb"`
		SwapUsedMB      *float64 `json:"swap_used_mb"`
		DiskUsedPct     *float64 `json:"disk_used_pct"`
		Load1           *float64 `json:"load1"`
		Load5           *float64 `json:"load5"`
		Load15          *float64 `json:"load15"`
		ContainersUp    *float64 `json:"containers_up"`
		ContainersTotal *float64 `json:"containers_total"`
		Ts              int64    `json:"ts"`
	}
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	node := clip(in.Node, 128)
	if node == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "node required"})
		return
	}
	ts := in.Ts
	if ts <= 0 {
		ts = time.Now().Unix()
	}
	bucket := ts / 60 * 60
	var rows []InfraSample
	addP := func(metric string, p *float64) {
		if p != nil {
			rows = append(rows, InfraSample{BucketTs: bucket, Resource: node, RType: "host", Metric: metric, Value: *p})
		}
	}
	addP("mem_avail_mb", in.MemAvailMB)
	addP("swap_mb", in.SwapUsedMB) // 与 DB 的 swap_mb 统一键名(均为「已用 Swap MB」)
	addP("disk_used_pct", in.DiskUsedPct)
	addP("load1", in.Load1)
	addP("load5", in.Load5)
	addP("load15", in.Load15)
	if in.MemTotalMB != nil && *in.MemTotalMB > 0 {
		addP("mem_total_mb", in.MemTotalMB)
	}
	if in.ContainersTotal != nil {
		addP("containers_total", in.ContainersTotal)
		addP("containers_up", in.ContainersUp)
	}
	if len(rows) == 0 {
		c.JSON(http.StatusOK, gin.H{"ok": true, "stored": 0})
		return
	}
	if err := m.upsertInfra(rows); err != nil {
		slog.Warn("主机指标入库失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "stored": len(rows)})
}

// clip 截断字符串到 n 字节,防御异常长输入。
func clip(s string, n int) string {
	if len(s) > n {
		return s[:n]
	}
	return s
}

// syncSMTPHandler 从主站 options 表同步 SMTP 配置(凭证存库,不回显)。
func (m *Monitor) syncSMTPHandler(c *gin.Context) {
	cfg, err := m.syncSMTPFromMainSite()
	if err != nil {
		c.JSON(http.StatusBadGateway, gin.H{"error": err.Error()})
		return
	}
	cfg.SMTPPassword = ""
	c.JSON(http.StatusOK, gin.H{"ok": true, "config": cfg, "smtp_password_set": true})
}

func (m *Monitor) getAlertConfig(c *gin.Context) {
	m.ensureSMTPDefault() // 首次未配置 SMTP 时默认从主站同步一次
	cfg := m.loadAlertConfig()
	hasPw := cfg.SMTPPassword != ""
	cfg.SMTPPassword = "" // 不回显明文
	c.JSON(http.StatusOK, gin.H{"config": cfg, "smtp_password_set": hasPw})
}

func (m *Monitor) saveAlertConfigHandler(c *gin.Context) {
	var in AlertConfig
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if in.SMTPPassword == "" { // 留空 = 保留原密码
		in.SMTPPassword = m.loadAlertConfig().SMTPPassword
	}
	if err := m.saveAlertConfig(in); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (m *Monitor) testAlertHandler(c *gin.Context) {
	cfg := m.loadAlertConfig()
	if cfg.SMTPHost == "" || cfg.Recipients == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请先保存 SMTP 服务器和收件人,再发测试邮件"})
		return
	}
	body := "这是一封 new-api 上游监控的【报警测试邮件】。\n收到此邮件说明 SMTP 与收件人配置正确。\n\n时间:" + time.Now().Format("2006-01-02 15:04:05")
	if err := sendMail(cfg, "[new-api监控] 测试邮件", body); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true})
}

func (m *Monitor) servePage(c *gin.Context) {
	c.Data(http.StatusOK, "text/html; charset=utf-8", []byte(pageHTML))
}

// RegisterPublicBoard 挂载对外公开看板(无鉴权:/status 页面 + /public/status JSON)。
// 看板是独立 public 包,只拿到本地采样库与少量配置,绝不触及内部结构与生产库。
// 站点名与 logo 在此【部署时同步一次】:从主站 new-api 取 system_name + logo;取不到则用 env 兜底。
func (m *Monitor) RegisterPublicBoard(r *gin.Engine) {
	name, logo := m.fetchBrand()
	if name == "" {
		name = m.cfg.SiteName // 主站不可达时用 MONITOR_SITE_NAME 兜底;再空则前端显通用名
	}
	public.Register(r, m.storeDB, public.Config{
		NewAPIBaseURL: m.cfg.NewAPIBaseURL,
		SiteName:      name,
		Logo:          logo,
	})
}

// serveLongTrend 返回小时级长期序列(默认近 30 天),供长期趋势图按需拉取(不进 30s 轮询)。
func (m *Monitor) serveLongTrend(c *gin.Context) {
	days, _ := strconv.Atoi(c.DefaultQuery("days", "30"))
	if days < 1 || days > 365 {
		days = 30
	}
	since := time.Now().Unix() - int64(days)*86400
	c.JSON(http.StatusOK, gin.H{"series": m.storeHourSeries(since)})
}

func (m *Monitor) serveData(c *gin.Context) {
	if !m.Enabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	snap, err := m.GetSnapshot(parseWindow(c), time.Now().Unix())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"enabled": true, "error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"enabled": true, "snapshot": snap})
}
