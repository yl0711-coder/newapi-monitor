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

// AlertsCoverage describes the evidence watermark for the local rejection
// facts.  The count returned by this page is always a count of rows currently
// present in rejection_samples; it is not a final total unless Complete is
// true.  Keeping this state in the response prevents callers from mistaking a
// partially caught-up CloudWatch lane for a zero/complete result.
type AlertsCoverage struct {
	Complete bool   `json:"coverage_complete"`
	Source   string `json:"source"` // cloudwatch, collector, mixed, or unknown
	Through  int64  `json:"through_ts"`
	Target   int64  `json:"target_ts"`
	Note     string `json:"coverage_note,omitempty"`
}

const alertsIncompleteCoverageNote = "问题预警数据不完整：本地 rejection_samples 尚未证明连续覆盖当前范围；页面中的数量只能作为已采集部分，不能当作最终总数。"

// The collector cursor can predate the fact-retention boundary. Its original
// start must not certify a historical interval whose minute rows were pruned.
func (m *Monitor) retainedRejectionCoverageFrom(from, now int64) int64 {
	days := m.cfg.RetentionDays
	if days <= 0 {
		days = 7
	}
	return max(from, metricMinuteRetentionCutoff(now, days))
}

// alertsCoverage computes a fail-closed watermark for the requested range.
// Legacy/旁路 collector rows do not carry a durable range watermark, so a
// range that relies on them is explicitly incomplete rather than silently
// claiming that a partial total is complete.  CloudWatch direct rows are
// complete only when the whole requested interval lies inside its [from,
// through) watermark and does not extend beyond the current closed target.
func (m *Monitor) alertsCoverage(scope stabilityScope, now time.Time) AlertsCoverage {
	coverage := AlertsCoverage{Source: "unknown"}
	if scope.ToTs <= scope.FromTs || scope.ToTs <= 0 {
		return coverage
	}
	if m == nil || !m.cfg.CloudWatchPreRouteEnabled || m.storeDB == nil || !cloudWatchPreRouteCoverageTableAvailable(m.storeDB) {
		coverage.Source = "collector"
		if m != nil && m.cfg.CloudWatchPreRouteEnabled {
			coverage.Note = alertsIncompleteCoverageNote + " CloudWatch 前置拒绝采集水位尚未就绪。"
		}
		return coverage
	}
	targetFrom, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	coverage.Target = target
	coverage.Through = m.cloudWatchPreRouteThrough.Load()
	coverageFrom := m.cloudWatchPreRouteFrom.Load()
	if coverageFrom <= 0 {
		coverageFrom = targetFrom
	}
	coverageFrom = m.retainedRejectionCoverageFrom(coverageFrom, now.Unix())
	// A range wholly before the direct lane is owned by the compatibility
	// collector.  Its source is explicit, but without a collector cursor it is
	// not safe to call that range complete.
	if scope.ToTs <= coverageFrom || scope.FromTs >= target {
		coverage.Source = "collector"
		return coverage
	}
	coverage.Source = "cloudwatch"
	coverage.Note = alertsIncompleteCoverageNote
	if scope.FromTs < coverageFrom || scope.ToTs > target {
		coverage.Source = "mixed"
	}
	// During migration the same minute can contain direct CloudWatch rows and
	// legacy collector rows.  The overlap query can suppress a proven duplicate
	// for user_id>0, but it cannot prove that user_id=0 rows describe the same
	// request.  Therefore any legacy row in the direct lane makes the requested
	// range mixed/incomplete; never publish a complete CloudWatch result based
	// only on the direct watermark.
	if coverage.Source == "cloudwatch" {
		legacy, err := m.alertsHasLegacyRows(scope.FromTs, scope.ToTs)
		if err != nil {
			coverage.Source = "mixed"
			coverage.Note = alertsIncompleteCoverageNote + " 无法确认旧采集器与 CloudWatch 的来源边界。"
			return coverage
		}
		if legacy {
			coverage.Source = "mixed"
			coverage.Note = alertsIncompleteCoverageNote + " 当前区间同时包含 CloudWatch 直采和旧采集器记录，来源尚未统一。"
			return coverage
		}
	}
	requiredThrough := scope.ToTs
	if requiredThrough > target {
		requiredThrough = target
	}
	// A date that starts before the direct lane also depends on the legacy
	// prefix; it is therefore incomplete even when the direct suffix caught up.
	if scope.FromTs >= coverageFrom && scope.ToTs <= target && coverage.Through >= scope.ToTs {
		coverage.Complete = true
		coverage.Note = ""
		return coverage
	}
	// Keep the failure check explicit: a failed poll must not be hidden just
	// because the in-memory through value was restored after a restart.
	if m.cloudWatchPreRouteLastFailure.Load() > m.cloudWatchPreRouteLastSuccess.Load() && coverage.Through < requiredThrough {
		coverage.Note = alertsIncompleteCoverageNote + " CloudWatch 前置拒绝采集最近一次失败，需先追平水位。"
	}
	return coverage
}

func (m *Monitor) alertsHasLegacyRows(from, to int64) (bool, error) {
	if m == nil || m.storeDB == nil || to <= from {
		return false, nil
	}
	var count int64
	err := m.storeDB.Model(&RejectionSample{}).
		Where("bucket_ts >= ? AND bucket_ts < ? AND node <> ? AND count > 0", from, to, cloudWatchPreRouteNode).
		Count(&count).Error
	return count > 0, err
}

// alertsCoverageNote is retained for existing callers/tests; the structured
// coverage fields above are the authoritative API contract.
func (m *Monitor) alertsCoverageNote(scope stabilityScope, now time.Time) string {
	return m.alertsCoverage(scope, now).Note
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
	// Total is a partial/local count when CoverageComplete is false.  Clients
	// must not present it as the final number until the watermark catches up.
	CoverageComplete bool   `json:"coverage_complete"`
	Source           string `json:"source"`
	ThroughTs        int64  `json:"through_ts"`
	TargetTs         int64  `json:"target_ts"`
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
	coverage := m.alertsCoverage(scope, time.Now())
	reasons, err := m.queryRejectReasonOptions(scope, filter)
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	resp := AlertsResponse{
		Enabled:                   true,
		Total:                     stats.Total,
		CoverageComplete:          coverage.Complete,
		Source:                    coverage.Source,
		ThroughTs:                 coverage.Through,
		TargetTs:                  coverage.Target,
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
	if coverage.Note != "" {
		resp.CoverageNote = coverage.Note
	} else if len(rows) == 0 && filter.Reason == "" && filter.UserID == nil {
		resp.CoverageNote = alertsNoDataNote
	}
	resp.From = time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02")
	resp.To = time.Unix(scope.ToTs-1, 0).In(cstLocation).Format("2006-01-02")
	c.JSON(200, resp)
}
