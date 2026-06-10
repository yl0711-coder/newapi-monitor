package monitor

import (
	"crypto/subtle"
	_ "embed"
	"log/slog"
	"net/http"
	"strconv"
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
	r.GET("/api/brand", m.brandHandler)                // 公开:站点名,供前端设置页面标题
	r.POST("/internal/rejections", m.ingestRejections) // 机器对机器:接收采集器推送的前置拒绝(token 鉴权)
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
