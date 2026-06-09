package monitor

import (
	_ "embed"
	"net/http"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
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
	r.GET("/api/brand", m.brandHandler) // 公开:站点名,供前端设置页面标题
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
