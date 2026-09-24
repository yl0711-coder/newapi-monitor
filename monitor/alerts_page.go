package monitor

// alerts_page.go：「问题预警」——统计未到达任何渠道的请求。
//
// 这类请求（令牌配错、请求不存在的模型等）从未到达渠道，不进渠道稳定率。
// 页面读取本地 rejection_samples 分钟事实；事实可由 CloudWatch 直采或兼容
// 的旁路采集器写入，页面刷新不直接访问 AWS/生产数据库。

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// alertsNoDataNote：0 条不等于「没有问题」。前置拒绝事实来自本地分钟
// rejection_samples，可能由 CloudWatch 直采或兼容的旁路采集器写入；
// 对应来源未运行、失败或尚未追平时，表同样可能为空。
const alertsNoDataNote = "无数据。前置拒绝来自本地分钟事实（CloudWatch 直采或兼容采集器），请先确认对应采集水位与采集器状态——0 条不代表没有问题。"

// alertsCoverageNote 在直采尚未覆盖完请求日期时给出 fail-closed 提示。
// 问题预警可以同时包含历史旁路采集器和 CloudWatch 直采事实；即使已经有
// 几行结果，也不能把尚未追平的窗口展示成完整统计。日期完全早于直采的
// 保留起点时不提示，避免把历史兼容来源误报成当前缺口。
func (m *Monitor) alertsCoverageNote(scope stabilityScope, now time.Time) string {
	if m == nil || !m.cfg.CloudWatchPreRouteEnabled {
		return ""
	}
	targetFrom, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	if target <= 0 || scope.ToTs <= 0 {
		return ""
	}
	coverageFrom := m.cloudWatchPreRouteFrom.Load()
	// Before the durable cursor is restored, use the configured lookback start
	// as the conservative lower bound. A date wholly before that bound is not a
	// missing CloudWatch window; it is outside this direct lane's responsibility
	// and may legitimately be served by the compatibility collector.
	if coverageFrom <= 0 {
		coverageFrom = targetFrom
	}
	// No direct-lane interval intersects this range when it ends before the
	// retained lookback starts, or when it begins after the current closed
	// target.  A range that crosses coverageFrom is only partly owned by the
	// direct lane and must be called out as dependent on the compatibility
	// collector rather than silently presented as complete.
	if scope.ToTs <= coverageFrom || scope.FromTs >= target {
		return ""
	}
	requiredThrough := scope.ToTs
	if requiredThrough > target {
		requiredThrough = target
	}
	lastSuccess := m.cloudWatchPreRouteLastSuccess.Load()
	lastFailure := m.cloudWatchPreRouteLastFailure.Load()
	through := m.cloudWatchPreRouteThrough.Load()
	// This page is an incident/evidence view, so any unpublished closed
	// minute must be called out.  The readiness endpoint intentionally keeps a
	// 20-minute noise budget for normal polling jitter, but silently accepting
	// that gap here can make a real recent rejection look absent.
	coverageIncomplete := through <= 0 || through < requiredThrough
	legacyPrefixRisk := scope.FromTs < coverageFrom && scope.ToTs > coverageFrom
	// A later failed poll must not taint a historical range whose requested end
	// is already behind the durable watermark. It matters only when the failure
	// leaves part of this particular range unpublished.
	failureAffectsScope := lastFailure > lastSuccess && through < requiredThrough
	if coverageIncomplete || failureAffectsScope || legacyPrefixRisk {
		return "CloudWatch 前置拒绝直采覆盖范围不完整或最近采集失败，当前结果可能只覆盖已发布窗口或兼容采集器已覆盖的部分；请同时核对采集水位与兼容采集器状态。"
	}
	return ""
}

// AlertRejectRow 是一条按分钟聚合的前置拒绝明细，供表格逐行展示。
//
// ★ 没有令牌与渠道两列，那不是漏做 ★
// 令牌：new-api 的拒绝日志里根本不写令牌名（实测 8 个真实样本文件，
// 只有额度数字如 "token remain quota"，没有名称）。
// 渠道：前置拒绝的定义就是从未到达任何渠道，填任何值都是编的。
type AlertRejectRow struct {
	// Ts 是分钟桶起点。采集侧按分钟聚合，所以精度到分钟而非秒。
	Ts     int64  `json:"ts"`
	Reason string `json:"reason"`
	Model  string `json:"model"`
	Grp    string `json:"grp"`
	Count  int64  `json:"count"`
	// UserID 0 = 未鉴权(无效令牌)或采集器未上报该字段。
	UserID int64 `json:"user_id"`
	// Username 取自本地用户名缓存；缓存里没有就是空串，
	// 前端必须显示 ID 本身，不能显示空白。
	Username string `json:"username,omitempty"`
}

// AlertRejectStats 是查询结果的统计汇总,一次查出避免重复扫表。
// user_id=0 不能一律解释成未鉴权：invalid_token 才是明确未鉴权；
// 其他原因的真实日志没有 user 段，只能说「客户未知（日志未提供用户 ID）」。
type AlertRejectStats struct {
	RowTotal                  int64 // 当前筛选范围的聚合行数
	Total                     int64 // 全时段被拒总次数
	UnauthCount               int64 // user_id=0 且 reason=invalid_token
	UnknownCustomerCount      int64 // user_id=0 且 reason!=invalid_token
	UnknownUserQuotaCount     int64 // 客户未知：用户额度不足
	UnknownPreConsumeCount    int64 // 客户未知：预扣费失败
	UnknownTokenQuotaCount    int64 // 客户未知：令牌额度不足
	UnknownQuotaAccountCount  int64 // 客户未知：CloudWatch 账户或额度不足
	UnknownCustomerOtherCount int64 // 客户未知：除上述额度类外的新原因
}

// AlertsResponse 是问题预警接口的返回。
type AlertsResponse struct {
	Enabled bool   `json:"enabled"`
	From    string `json:"from"`
	To      string `json:"to"`
	Total   int64  `json:"total"`
	// CoverageNote 说明这批数据靠旁路采集器，以及直采水位是否完整；
	// 空结果也必须明确提示「未采集」或覆盖风险，不能说成「没有问题」。
	CoverageNote string `json:"coverage_note,omitempty"`
	// Rows 是逐分钟明细。RowsTruncated 为真表示超出上限被截断，
	// 页面必须说明「还有更多」，不能让人以为这就是全部。
	Rows          []AlertRejectRow `json:"rows,omitempty"`
	RowsTruncated bool             `json:"rows_truncated,omitempty"` // 兼容旧前端；值与 HasMore 相同
	RowTotal      int64            `json:"row_total"`
	HasMore       bool             `json:"has_more"`
	NextCursor    string           `json:"next_cursor,omitempty"`
	ReasonOptions []string         `json:"reason_options,omitempty"`
	// 身份缺失分两档：invalid_token 明确是未鉴权；其他 reason 是日志没提供用户 ID。
	UnauthCount               int64 `json:"unauth_count,omitempty"`
	UnknownCustomerCount      int64 `json:"unknown_customer_count,omitempty"`
	UnknownUserQuotaCount     int64 `json:"unknown_user_quota_count,omitempty"`
	UnknownPreConsumeCount    int64 `json:"unknown_pre_consume_count,omitempty"`
	UnknownTokenQuotaCount    int64 `json:"unknown_token_quota_count,omitempty"`
	UnknownQuotaAccountCount  int64 `json:"unknown_quota_account_count,omitempty"`
	UnknownCustomerOtherCount int64 `json:"unknown_customer_other_count,omitempty"`
}

// 分页、筛选和统计查询见 alerts_query.go。

func (m *Monitor) getRejectAlertsHandler(c *gin.Context) {
	if !m.cfg.StabilityEnabled {
		c.JSON(200, AlertsResponse{Enabled: false})
		return
	}
	if m.storeDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地事实库未就绪，无法读取问题预警"})
		return
	}
	scope, err := stabilityRange(c, time.Now(), m.cfg.stabilityQueryDays())
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	filter, err := parseAlertRejectFilter(c)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	// Today's range ends at the current second.  A continuation cursor is a
	// snapshot of the first request, so allow the live upper bound to advance
	// and then query that cursor snapshot consistently for the remaining pages.
	if err := validateAlertRejectCursorForContinuation(scope, filter); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if cursor := filter.Cursor; cursor != nil {
		scope.FromTs, scope.ToTs = cursor.FromTs, cursor.ToTs
	}
	rows, hasMore, nextCursor, stats, err := m.queryRejectPage(scope, filter)
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	reasons, err := m.queryRejectReasonOptions(scope, filter)
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	resp := AlertsResponse{
		Enabled:                   true,
		Total:                     stats.Total,
		Rows:                      rows,
		RowsTruncated:             hasMore,
		RowTotal:                  stats.RowTotal,
		HasMore:                   hasMore,
		NextCursor:                nextCursor,
		ReasonOptions:             reasons,
		UnauthCount:               stats.UnauthCount,
		UnknownCustomerCount:      stats.UnknownCustomerCount,
		UnknownUserQuotaCount:     stats.UnknownUserQuotaCount,
		UnknownPreConsumeCount:    stats.UnknownPreConsumeCount,
		UnknownTokenQuotaCount:    stats.UnknownTokenQuotaCount,
		UnknownQuotaAccountCount:  stats.UnknownQuotaAccountCount,
		UnknownCustomerOtherCount: stats.UnknownCustomerOtherCount,
	}
	if note := m.alertsCoverageNote(scope, time.Now()); note != "" {
		resp.CoverageNote = note
	} else if len(rows) == 0 && filter.Reason == "" && filter.UserID == nil {
		resp.CoverageNote = alertsNoDataNote
	}
	resp.From = time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02")
	resp.To = time.Unix(scope.ToTs-1, 0).In(cstLocation).Format("2006-01-02")
	c.JSON(200, resp)
}
