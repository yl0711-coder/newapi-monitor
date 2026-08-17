package monitor

import (
	"context"
	"crypto/subtle"
	_ "embed"
	"errors"
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
var flatpickrJS []byte // 内嵌 flatpickr v4.6.13+zh 语言包(MIT),保留旧静态资源兼容

//go:embed flatpickr.min.css
var flatpickrCSS []byte // flatpickr 旧主题兼容资源

//go:embed range_picker.js
var rangePickerJS []byte // 把真实 Semi DatePicker 挂到零构建页面的轻量适配层

//go:embed react.production.min.js
var reactJS []byte // React 18.2.0（MIT），Semi UI 运行时依赖

//go:embed react-dom.production.min.js
var reactDOMJS []byte // ReactDOM 18.2.0（MIT），Semi UI 运行时依赖

//go:embed semi-ui.min.js
var semiUIJS []byte // Semi UI 2.72.2（MIT），与 NewAPI 日期控件相同的组件实现

//go:embed semi-ui.min.css
var semiUICSS []byte // Semi UI 2.72.2 原始组件样式

//go:embed stability.css
var stabilityCSS []byte // Monitor 管理端新框架与稳定性报表样式；不被 Usage Portal 引用

//go:embed stability.js
var stabilityJS []byte // 稳定性报表交互；页面请求只访问 /stability/* 本地汇总接口

//go:embed channel_management.js
var channelManagementJS []byte // 渠道管理交互；只访问 Monitor 本地渠道汇总接口

var allowedWindows = map[int]bool{15: true, 30: true, 60: true, 180: true, 360: true, 720: true, 1440: true}

const maxJSONRequestBody = 4 << 20   // 4 MiB:足以覆盖节点批量上报，同时拒绝异常大请求体
const maxLoginRequestBody = 64 << 10 // 登录只需要账号密码，64 KiB 已远大于正常请求

// requestBodyLimit 在进入业务处理前限制所有带 body 的请求，避免攻击者用大 JSON
// 长时间占用内存。MaxBytesReader 同时覆盖没有 Content-Length 的 chunked 请求。
func requestBodyLimit(maxBytes int64) gin.HandlerFunc {
	return func(c *gin.Context) {
		if c.Request.ContentLength > maxBytes {
			c.AbortWithStatusJSON(http.StatusRequestEntityTooLarge, gin.H{"error": "请求体过大"})
			return
		}
		c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxBytes)
		c.Next()
	}
}

// limitBodyForLogin 给登录再收紧一层上限；全局 4MiB 是为了节点批量上报保留的。
func limitBodyForLogin(c *gin.Context) bool {
	if c.Request.ContentLength > maxLoginRequestBody {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "登录请求过大"})
		return false
	}
	c.Request.Body = http.MaxBytesReader(c.Writer, c.Request.Body, maxLoginRequestBody)
	return true
}

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
	r.Use(requestBodyLimit(maxJSONRequestBody))
	if m.adminLim == nil {
		m.adminLim = &portalLimiter{m: map[string][]int64{}}
	}
	// 公开健康端点：/live 只证明进程存活，供容器防重启风暴；
	// /ready 只读后台原子状态，绝不因为探活增加生产库 QPS。
	// /health 保留为 /live 的向后兼容别名。
	r.GET("/live", m.serveLive)
	r.GET("/ready", m.serveReady)
	r.GET("/health", m.serveLive)
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
	r.GET("/range-picker.js", func(c *gin.Context) {
		// 适配层会随 Monitor 功能调整，不能像固定版本的第三方资源一样
		// 永久缓存，否则升级后浏览器会继续执行旧的日期控件逻辑。
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
	r.GET("/stability.css", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "text/css; charset=utf-8", stabilityCSS)
	})
	r.GET("/stability.js", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", stabilityJS)
	})
	r.GET("/channel-management.js", func(c *gin.Context) {
		c.Header("Cache-Control", "no-cache")
		c.Data(http.StatusOK, "application/javascript; charset=utf-8", channelManagementJS)
	})
	r.GET("/api/brand", m.brandHandler)                // 公开:站点名,供前端设置页面标题
	r.POST("/internal/rejections", m.ingestRejections) // 机器对机器:接收采集器推送的前置拒绝(token 鉴权)
	r.POST("/internal/host", m.ingestHost)             // 机器对机器:接收各节点主机 agent 推送的 OS 内存/磁盘(token 鉴权)
	r.POST("/internal/nginx", m.ingestNginx)           // 机器对机器:接收已脱敏的 Nginx 分钟聚合(token 鉴权,默认关闭)
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
		view.GET("/stability/report", m.serveStabilityReport)                              // 历史稳定性:只读 Monitor 本地 SQLite
		view.GET("/stability/detail", m.serveStabilityDetail)                              // 单分组详情:按需加载渠道时间条/模型
		view.GET("/stability/problems", m.serveStabilityProblems)                          // 原始错误签名:只读本地问题样本
		view.GET("/stability/health", m.serveStabilityHealth)                              // 采集新鲜度/覆盖/积压:不查生产库
		view.GET("/stability/edge", m.serveNginxEdge)                                      // Nginx 入口层:只读本地脱敏分钟汇总
		view.GET("/channels/report", m.serveChannelManagementReport)                       // 渠道管理:主域名→厂商→渠道→服务分组的本地汇总
		view.GET("/infra", m.serveInfra)                                                   // 服务端健康监控(实例/DB/LB)快照
		view.GET("/infra/series", m.serveInfraSeries)                                      // 按需取某资源某些指标的近 N 小时序列(展开图用)
		view.GET("/usage/users", m.listTrackedUsers)                                       // 用户用量:被盯名单(含分组)
		view.GET("/usage/groups", m.listGroups)                                            // 用户用量:客户分组列表
		view.GET("/usage/followups", m.usageAggregateAuthorizationGuard(m.serveFollowUps)) // 用户用量:待跟进清单
		view.GET("/usage/followups/log", m.listFollowLogs)                                 // 用户用量:某客户跟进记录
		view.GET("/usage/settings", m.getUsageSettings)                                    // 用户用量:跟进阈值(读)
		view.GET("/usage/matrix", m.usageAggregateAuthorizationGuard(m.serveUsageMatrix))  // 用户用量:列表页矩阵(前端渲染 行=用户×列=日期,格=当日费用)
		view.GET("/usage/stats", m.usageAggregateAuthorizationGuard(m.serveUsageStats))    // 用户用量:单用户详情聚合(每日/分组/模型/费用)
		view.GET("/usage/cache-stats", m.serveUsageCacheStats)                             // 用户用量缓存:无敏感信息的运维计数
		view.GET("/usage/facts-status", m.serveUsageFactsStatus)                           // 用户用量本地事实层:覆盖率/同步状态(只读 Monitor SQLite)
		view.GET("/usage/facts-history", m.serveUsageFactHistoryStatus)                    // 全历史逐成员阶段/水位/失败原因(只读本地)
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

	// 仅超级管理员:用当前判据重算历史桶(判据变更后一次性执行)。
	// 做成接口而非启动参数:不必重启、可重跑、可只补一段;放启动流程会每次重启都压一遍生产库。
	r.POST("/admin/backfill", m.requireRole(roleRoot), m.backfillHandler)
	r.POST("/admin/stability/backfill", m.requireRole(roleRoot), m.startStabilityBackfillHandler)
	r.POST("/admin/stability/backfill/retry", m.requireRole(roleRoot), m.retryStabilityBackfillHandler)
	r.GET("/admin/stability/backfill", m.requireRole(roleRoot), m.stabilityBackfillStatusHandler)
	r.POST("/admin/store/backup", m.requireRole(roleRoot), m.triggerStoreBackupHandler)

	// 仅超级管理员:用户用量名单增删(看名单/看统计在上面 view 组,管理员即可)
	rootUsage := r.Group("/usage", m.requireRole(roleRoot))
	{
		rootUsage.POST("/users", m.addTrackedUser)
		rootUsage.POST("/users/delete", m.deleteTrackedUser)
		rootUsage.POST("/users/group", m.setUserGroup)                    // 改用户归属分组
		rootUsage.POST("/users/note", m.setUserNote)                      // 改用户备注
		rootUsage.POST("/groups", m.createGroup)                          // 客户分组:新建
		rootUsage.POST("/groups/update", m.updateGroup)                   // 客户分组:编辑
		rootUsage.POST("/groups/delete", m.deleteGroup)                   // 客户分组:解散(成员回未分组)
		rootUsage.POST("/groups/portal", m.setGroupPortal)                // 客户分组:客户端账号(开通/更新/重置/关闭)
		rootUsage.POST("/followups/log", m.addFollowLog)                  // 跟进记录:追加
		rootUsage.POST("/settings", m.saveUsageSettings)                  // 跟进阈值:保存
		rootUsage.POST("/facts-repair", m.requestUsageFactsRepairHandler) // 历史晚到/旧库 proof 受控补数
		rootUsage.POST("/facts-history/retry", m.retryUsageFactHistoryHandler)
		rootUsage.POST("/facts-history/repair", m.requestUsageFactHistoryDayRepairHandler)
	}

	// 仅超级管理员:维护渠道毛利率的本地计价配置。接口只写 Monitor SQLite，
	// 不读取或改写 NewAPI 的渠道、倍率与充值配置。
	rootChannels := r.Group("/channels", m.requireRole(roleRoot))
	{
		rootChannels.POST("/finance", m.saveChannelFinanceHandler)
		rootChannels.POST("/finance/site", m.saveChannelFinanceSiteHandler)
		rootChannels.POST("/finance/site-groups/sync", m.syncWebsiteGroupCatalogHandler)
		rootChannels.POST("/finance/domain", m.saveChannelFinanceDomainHandler)
		rootChannels.POST("/finance/channel", m.saveChannelFinanceChannelHandler)
		rootChannels.POST("/finance/domain-rates", m.saveChannelFinanceDomainRatesHandler)
		rootChannels.GET("/upstream", m.getChannelUpstreamHandler)
		rootChannels.POST("/upstream", m.saveChannelUpstreamHandler)
		rootChannels.POST("/upstream/sync", m.syncChannelUpstreamHandler)
		rootChannels.POST("/upstream/usage-sync", m.syncChannelUpstreamUsageHandler)
	}
}

func (m *Monitor) triggerStoreBackupHandler(c *gin.Context) {
	if !m.cfg.StoreBackupEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地 SQLite 备份未启用"})
		return
	}
	if !m.triggerManualStoreBackup() {
		c.JSON(http.StatusConflict, gin.H{"error": "本地 SQLite 备份正在进行或无可备份文件"})
		return
	}
	c.JSON(http.StatusAccepted, gin.H{"ok": true, "running": true})
}

// serveUsageCacheStats 只向管理端会话暴露缓存运行计数。接口不主动访问 Redis，
// 不返回缓存键或任何客户资料，可用于上线后核对命中率、回源次数和降级情况。
func (m *Monitor) serveUsageCacheStats(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"cache":             m.usageCache.Stats(time.Now()),
		"source_budget":     m.usageSourceStats(),
		"facts_read_budget": m.usageFactsReadBudgetStats(),
	})
}

// serveUsageFactsStatus 返回本地事实层的可切读状态和 Monitor SQLite 的备份状态。
// 该接口不会因查看状态而访问 NewAPI 的 logs/users/tokens；用于两阶段上线时确认
// 历史小时和当前关注客户名单均已同步，再由运维显式打开
// MONITOR_USAGE_FACTS_READ_ENABLED。store 字段只读内存原子状态，不触发备份或检查。
func (m *Monitor) serveUsageFactsStatus(c *gin.Context) {
	status, err := m.usageFactsStatus(c.Request.Context(), time.Now())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error":       "读取本地用量事实状态失败",
			"store":       m.storeReliabilityStatus(),
			"facts_store": m.usageFactsStoreReliabilityStatus(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{
		"facts":       status,
		"store":       m.storeReliabilityStatus(),
		"facts_store": m.usageFactsStoreReliabilityStatus(),
	})
}

func (m *Monitor) serveUsageFactHistoryStatus(c *gin.Context) {
	progress, err := m.usageFactHistoryProgress(c.Request.Context(), time.Now())
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "读取全历史事实进度失败"})
		return
	}
	c.JSON(http.StatusOK, progress)
}

func (m *Monitor) retryUsageFactHistoryHandler(c *gin.Context) {
	var in struct {
		UserID int64  `json:"user_id"`
		JobID  string `json:"job_id"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id required"})
		return
	}
	job, err := m.retryUsageFactHistoryJobTarget(c.Request.Context(), in.UserID, in.JobID, time.Now())
	if err != nil {
		switch {
		case errors.Is(err, errUsageFactHistoryJobNotFound):
			c.JSON(http.StatusNotFound, gin.H{"error": err.Error()})
		case errors.Is(err, errUsageFactHistoryRetryConflict):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "重试全历史事实任务失败"})
		}
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"ok": true, "user_id": in.UserID, "job_id": job.ID, "stage": job.Kind,
		"status": job.Status, "next_hour": job.NextHour, "verify_next_hour": job.VerifyNextHour,
	})
}

func (m *Monitor) requestUsageFactHistoryDayRepairHandler(c *gin.Context) {
	var in struct {
		UserID    int64  `json:"user_id"`
		Day       string `json:"day"`
		Reason    string `json:"reason"`
		RequestID string `json:"request_id"`
		Confirm   string `json:"confirm"`
	}
	if err := c.ShouldBindJSON(&in); err != nil || in.UserID <= 0 || strings.TrimSpace(in.Day) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "user_id/day required"})
		return
	}
	if strings.TrimSpace(in.Confirm) != "REPAIR_FULL_HISTORY_DAY" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "请明确确认单成员单日全历史修复"})
		return
	}
	if strings.TrimSpace(in.RequestID) == "" && strings.TrimSpace(c.GetHeader("Idempotency-Key")) == "" &&
		strings.TrimSpace(c.GetHeader("X-Idempotency-Key")) == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "request_id/Idempotency-Key required"})
		return
	}
	meta, err := usageMemberMutationMetaFromGin(c, in.RequestID, in.Reason)
	if err != nil || strings.TrimSpace(meta.Reason) == "" {
		if err == nil {
			err = errors.New("请填写修复原因")
		}
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	dayText := strings.TrimSpace(in.Day)
	day, err := time.ParseInLocation("2006-01-02", dayText, usageCST)
	if err != nil || day.Format("2006-01-02") != dayText {
		c.JSON(http.StatusBadRequest, gin.H{"error": "day 必须是 CST YYYY-MM-DD"})
		return
	}
	job, err := m.requestUsageFactHistoryDayRepair(
		c.Request.Context(), in.UserID, day.Unix(), meta.Reason, meta.RequestID, meta.Actor, time.Now(),
	)
	if err != nil {
		switch {
		case errors.Is(err, errUsageFactHistoryRepairRequestConflict), errors.Is(err, errUsageMemberControlIntegrity):
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
		case errors.Is(err, errUsageFactHistoryManualRepairInvalid):
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		default:
			c.JSON(http.StatusInternalServerError, gin.H{"error": "创建全历史精确修复失败"})
		}
		return
	}
	c.JSON(http.StatusAccepted, gin.H{
		"ok": true, "request_id": meta.RequestID, "user_id": in.UserID, "day": dayText,
		"job_id": job.ID, "stage": job.Kind, "status": job.Status, "next_hour": job.NextHour,
	})
}

// checkIngest 校验节点推送接口的 Bearer token(MONITOR_INGEST_TOKEN)。
// 未配置则接口关闭(503);不匹配 401(常数时间比较)。所有 ingest 端点共用这一道闸,
// 返回 false 时响应已写好,调用方直接 return。
func (m *Monitor) checkIngest(c *gin.Context) bool {
	want := m.cfg.IngestToken
	if want == "" {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "ingest disabled"})
		return false
	}
	if subtle.ConstantTimeCompare([]byte(c.GetHeader("Authorization")), []byte("Bearer "+want)) != 1 {
		c.JSON(http.StatusUnauthorized, gin.H{"error": "unauthorized"})
		return false
	}
	return true
}

// ingestRejections 接收各节点 newapi-reject-collector 推来的前置拒绝计数(token 鉴权)。
// 未配置 MONITOR_INGEST_TOKEN 则接口关闭(503),不接受任何推送。
func (m *Monitor) ingestRejections(c *gin.Context) {
	if !m.checkIngest(c) {
		return
	}
	var in struct {
		Node    string `json:"node"`
		BatchID string `json:"batch_id"`
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
	node := clip(strings.TrimSpace(in.Node), 64)
	batchID := strings.TrimSpace(in.BatchID)
	if node == "" {
		c.JSON(http.StatusBadRequest, gin.H{"error": "node required"})
		return
	}
	if !validIngestBatchID(batchID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "valid batch_id required"})
		return
	}
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
	duplicate, err := m.ingestRejectionBatch(node, batchID, rows, time.Now().Unix())
	if errors.Is(err, errRejectionBatchConflict) {
		c.JSON(http.StatusConflict, gin.H{"error": "batch_id payload conflict"})
		return
	}
	if err != nil {
		slog.Warn("被拒请求入库失败", "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
		return
	}
	stored := len(rows)
	if duplicate {
		stored = 0
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "stored": stored, "duplicate": duplicate})
}

func validIngestBatchID(id string) bool {
	if len(id) < 8 || len(id) > 64 {
		return false
	}
	for _, r := range id {
		if (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("._:-", r) {
			continue
		}
		return false
	}
	return true
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
	if m.infraExcluded(resource) {
		c.JSON(http.StatusNotFound, gin.H{"error": "resource is not monitored"})
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
// 复用 MONITOR_INGEST_TOKEN 鉴权;未配置则接口关闭(503)。只接非敏感数值和
// 显式白名单内的容器 name/state/health/restart_count，不含密钥、配置或业务数据。
type hostContainerInput struct {
	Name         string `json:"name"`
	State        string `json:"state"`
	Health       string `json:"health"`
	RestartCount int    `json:"restart_count"`
}

func (m *Monitor) ingestHost(c *gin.Context) {
	if !m.checkIngest(c) {
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
		// 指针用于区分旧版 agent 的“字段不存在”和新版 agent 的“明确空列表”。
		// 前者保留已有快照，后者表示白名单为空并清空该节点的容器明细。
		Containers *[]hostContainerInput `json:"containers"`
		Ts         int64                 `json:"ts"`
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
	receivedAt := time.Now().Unix()
	ts := in.Ts
	if ts <= 0 {
		ts = receivedAt
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
	var snapshots []HostContainerSnapshot
	if in.Containers != nil {
		if len(*in.Containers) > 64 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "too many containers"})
			return
		}
		seen := make(map[string]struct{}, len(*in.Containers))
		snapshots = make([]HostContainerSnapshot, 0, len(*in.Containers))
		for _, item := range *in.Containers {
			name := clip(strings.TrimSpace(strings.TrimPrefix(item.Name, "/")), 128)
			if name == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "container name required"})
				return
			}
			if _, exists := seen[name]; exists {
				c.JSON(http.StatusBadRequest, gin.H{"error": "duplicate container name"})
				return
			}
			seen[name] = struct{}{}
			restarts := item.RestartCount
			if restarts < 0 {
				restarts = 0
			}
			snapshots = append(snapshots, HostContainerSnapshot{
				Node: node, Name: name, State: safeContainerState(item.State),
				Health: safeContainerHealth(item.Health), RestartCount: restarts, LastSeen: receivedAt,
			})
		}
	}
	// 所有输入通过校验后再写入，避免错误的容器明细请求留下半套指标。
	if len(rows) > 0 {
		if err := m.upsertInfra(rows); err != nil {
			slog.Warn("主机指标入库失败", "err", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
			return
		}
	}
	containerStored := 0
	if in.Containers != nil {
		if err := m.replaceHostContainerSnapshots(node, snapshots); err != nil {
			slog.Warn("主机容器明细入库失败", "node", node, "err", err)
			c.JSON(http.StatusInternalServerError, gin.H{"error": "container snapshot store failed"})
			return
		}
		containerStored = len(snapshots)
	}
	if len(rows) == 0 && in.Containers == nil {
		c.JSON(http.StatusOK, gin.H{"ok": true, "stored": 0, "containers_stored": 0})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "stored": len(rows), "containers_stored": containerStored})
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

// backfillHandler 触发历史回填。同步执行(约 168 片 × 0.5s ≈ 数分钟),完成后返回统计。
// 超时上限给足:回填按小时切片、片间有间隔,不能被请求超时半途掐断留下半新半旧的数据。
func (m *Monitor) backfillHandler(c *gin.Context) {
	hours, _ := strconv.Atoi(c.Query("hours"))
	if hours <= 0 {
		hours = m.cfg.RetentionDays * 24 // 默认补满分钟级留存
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 30*time.Minute)
	defer cancel()
	res, err := m.BackfillHours(ctx, hours)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	c.JSON(http.StatusOK, res)
}

func (m *Monitor) testAlertHandler(c *gin.Context) {
	if m.cfg.AlertsDisabled { // 断路器:本地/测试实例连手动测试邮件也不放行
		c.JSON(http.StatusForbidden, gin.H{"error": "本实例已通过 MONITOR_ALERTS_DISABLED 关闭全部报警发信"})
		return
	}
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
