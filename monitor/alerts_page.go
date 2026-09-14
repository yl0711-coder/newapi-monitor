package monitor

// alerts_page.go：「问题预警」——统计未到达任何渠道的请求。
//
// 这类请求（令牌配错、请求不存在的模型等）从未到达渠道，不进渠道稳定率。
// 数据源是 stability_reject_hours，由旁路采集器读 Nginx 文件日志推来。

import (
	"time"

	"github.com/gin-gonic/gin"
)

// alertsNoDataNote：0 条不等于「没有问题」。这类数据靠旁路采集器推送，
// 采集器没跑时表就是空的。把两者说成一回事会让人误以为一切正常。
const alertsNoDataNote = "无数据。这类请求靠旁路采集器读 Nginx 文件日志推送，请先确认采集器在运行——0 条不代表没有问题。"

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
	UnknownCustomerOtherCount int64 // 客户未知：除上述三种外的新原因
}

// AlertsResponse 是问题预警接口的返回。
type AlertsResponse struct {
	Enabled bool   `json:"enabled"`
	From    string `json:"from"`
	To      string `json:"to"`
	Total   int64  `json:"total"`
	// CoverageNote 说明这批数据靠旁路采集器；采集器没跑时这里为空且 total=0，
	// 页面必须把这种情况说成「未采集」，不能说成「没有问题」。
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
	UnknownCustomerOtherCount int64 `json:"unknown_customer_other_count,omitempty"`
}

// 分页、筛选和统计查询见 alerts_query.go。

func (m *Monitor) getRejectAlertsHandler(c *gin.Context) {
	if !m.cfg.StabilityEnabled {
		c.JSON(200, AlertsResponse{Enabled: false})
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
	if err := validateAlertRejectCursor(scope, filter); err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
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
		UnknownCustomerOtherCount: stats.UnknownCustomerOtherCount,
	}
	if len(rows) == 0 && filter.Reason == "" && filter.UserID == nil {
		resp.CoverageNote = alertsNoDataNote
	}
	resp.From = time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02")
	resp.To = time.Unix(scope.ToTs-1, 0).In(cstLocation).Format("2006-01-02")
	c.JSON(200, resp)
}
