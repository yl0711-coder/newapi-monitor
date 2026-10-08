package monitor

// model_statistics.go 提供独立的“模型统计”页面数据。
//
// 这不是客户维护的稳定性报表：它回答的是“各分组实际请求过哪些模型、
// 请求了多少次”。已经进入渠道的请求复用客户维护的用户分钟事实，选渠道前就因没有可用
// 渠道而失败的请求来自 rejection_samples；两类请求在 NewAPI 的数据边界
// 上互斥。两条拒绝采集来源按可确认的客户维度去重；身份缺失时保守保留
// 两边事实，避免把真实请求静默吞掉。

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const modelStatisticsQueryTimeout = 5 * time.Second

type modelStatisticsWindow struct {
	Key     string
	Label   string
	Seconds int64
}

var modelStatisticsWindows = map[string]modelStatisticsWindow{
	"24h": {Key: "24h", Label: "近24小时", Seconds: 24 * 3600},
	"3d":  {Key: "3d", Label: "近3天", Seconds: 3 * 86400},
	"7d":  {Key: "7d", Label: "近7天", Seconds: 7 * 86400},
}

type modelStatisticsRow struct {
	Group             string `gorm:"column:grp"`
	Model             string `gorm:"column:model"`
	UserID            int64  `gorm:"column:user_id"`
	Username          string `gorm:"column:username"`
	ChannelID         int    `gorm:"column:channel_id"`
	RoutedRequests    int64  `gorm:"column:routed_requests"`
	UnavailableRoutes int64  `gorm:"column:unavailable_routes"`
	Ttft500           int64  `gorm:"column:ttft_500"`
	Ttft1k            int64  `gorm:"column:ttft_1k"`
	Ttft2k            int64  `gorm:"column:ttft_2k"`
	Ttft5k            int64  `gorm:"column:ttft_5k"`
	Ttft10k           int64  `gorm:"column:ttft_10k"`
	TtftInf           int64  `gorm:"column:ttft_inf"`
	TtftMaxMs         int    `gorm:"column:ttft_max_ms"`
	TtftObserved      int64  `gorm:"column:ttft_observed"`
	TtftOver3s        int64  `gorm:"column:ttft_over_3s"`
}

type modelStatisticsAggregate struct {
	routed, unavailable int64
	customers           map[int64]int64
	ttft                modelStatisticsTTFTAggregate
	channels            map[int]*modelStatisticsChannelAggregate
}

// A negative key is reserved internally for requests rejected before channel
// selection. Channel ID 0 is kept for routed logs whose channel is unknown;
// merging those two facts would make the drill-down disagree with the
// unavailable-channel total.
const modelStatisticsUnavailableChannelKey = -1

// modelStatisticsTTFTAggregate retains the bounded histogram written by the
// sampler. TtftOver3s is an exact source counter; it must not be inferred from
// the 2-5 second histogram bucket.
type modelStatisticsTTFTAggregate struct {
	ttft500, ttft1k, ttft2k, ttft5k, ttft10k, ttftInf int64
	maxMs, observed, over3s                           int64
	invalid                                           bool
}

type modelStatisticsChannelAggregate struct {
	requests int64
	ttft     modelStatisticsTTFTAggregate
}

type ModelStatisticsModel struct {
	Model             string  `json:"model"`
	Requests          int64   `json:"requests"`
	RoutedRequests    int64   `json:"routed_requests"`
	UnavailableRoutes int64   `json:"unavailable_channel_requests"`
	TTFTObserved      int64   `json:"ttft_observed"`
	TTFTP50Ms         float64 `json:"ttft_p50_ms"`
	TTFTP95Ms         float64 `json:"ttft_p95_ms"`
	TTFTP99Ms         float64 `json:"ttft_p99_ms"`
	TTFTMaxMs         int64   `json:"ttft_max_ms"`
	TTFTOver3s        int64   `json:"ttft_over_3s"`
	TTFTOver3sPct     float64 `json:"ttft_over_3s_pct"`
	// FRT is the canonical public name. The ttft_* fields above remain as
	// deprecated aliases for existing clients and are populated identically.
	FRTObserved  int64                  `json:"frt_observed"`
	FRTP50Ms     float64                `json:"frt_p50_ms"`
	FRTP95Ms     float64                `json:"frt_p95_ms"`
	FRTP99Ms     float64                `json:"frt_p99_ms"`
	FRTMaxMs     int64                  `json:"frt_max_ms"`
	FRTOver3s    int64                  `json:"frt_over_3s"`
	FRTOver3sPct float64                `json:"frt_over_3s_pct"`
	Groups       []ModelStatisticsGroup `json:"groups"`
}

type ModelStatisticsGroup struct {
	Group             string                    `json:"group"`
	Requests          int64                     `json:"requests"`
	RoutedRequests    int64                     `json:"routed_requests"`
	UnavailableRoutes int64                     `json:"unavailable_channel_requests"`
	TTFTObserved      int64                     `json:"ttft_observed"`
	TTFTP50Ms         float64                   `json:"ttft_p50_ms"`
	TTFTP95Ms         float64                   `json:"ttft_p95_ms"`
	TTFTP99Ms         float64                   `json:"ttft_p99_ms"`
	TTFTMaxMs         int64                     `json:"ttft_max_ms"`
	TTFTOver3s        int64                     `json:"ttft_over_3s"`
	TTFTOver3sPct     float64                   `json:"ttft_over_3s_pct"`
	FRTObserved       int64                     `json:"frt_observed"`
	FRTP50Ms          float64                   `json:"frt_p50_ms"`
	FRTP95Ms          float64                   `json:"frt_p95_ms"`
	FRTP99Ms          float64                   `json:"frt_p99_ms"`
	FRTMaxMs          int64                     `json:"frt_max_ms"`
	FRTOver3s         int64                     `json:"frt_over_3s"`
	FRTOver3sPct      float64                   `json:"frt_over_3s_pct"`
	Channels          []ModelStatisticsChannel  `json:"channels"`
	Customers         []ModelStatisticsCustomer `json:"customers"`
}

// ModelStatisticsChannel is one channel row under a model and service group.
// ChannelID -1 is a reserved, structured sentinel for requests rejected before
// channel selection. ChannelID 0 is kept for routed rows whose source channel
// is unknown. ChannelKind/IsUnavailable make the distinction explicit for API
// consumers; callers must not use the localized ChannelName as an identity key.
type ModelStatisticsChannel struct {
	ChannelID     int     `json:"channel_id"`
	ChannelKind   string  `json:"channel_kind"`
	IsUnavailable bool    `json:"is_unavailable"`
	ChannelName   string  `json:"channel_name"`
	Requests      int64   `json:"requests"`
	TTFTObserved  int64   `json:"ttft_observed"`
	TTFTP50Ms     float64 `json:"ttft_p50_ms"`
	TTFTP95Ms     float64 `json:"ttft_p95_ms"`
	TTFTP99Ms     float64 `json:"ttft_p99_ms"`
	TTFTMaxMs     int64   `json:"ttft_max_ms"`
	TTFTOver3s    int64   `json:"ttft_over_3s"`
	TTFTOver3sPct float64 `json:"ttft_over_3s_pct"`
	FRTObserved   int64   `json:"frt_observed"`
	FRTP50Ms      float64 `json:"frt_p50_ms"`
	FRTP95Ms      float64 `json:"frt_p95_ms"`
	FRTP99Ms      float64 `json:"frt_p99_ms"`
	FRTMaxMs      int64   `json:"frt_max_ms"`
	FRTOver3s     int64   `json:"frt_over_3s"`
	FRTOver3sPct  float64 `json:"frt_over_3s_pct"`
}

// ModelStatisticsCustomer 是某个“模型＋服务分组”下的一位请求客户。
// CustomerID 对应 NewAPI user_id；名字来自本地全量用户目录，目录未同步到时
// 保留 ID 并明确显示未知，不因展示字段缺失而丢掉请求计数。
type ModelStatisticsCustomer struct {
	CustomerID   int64   `json:"customer_id"`
	CustomerName string  `json:"customer_name"`
	Requests     int64   `json:"requests"`
	SharePct     float64 `json:"share_pct"`
}

type ModelStatisticsSource struct {
	RoutedFact      string `json:"routed_fact"`
	UnavailableFact string `json:"unavailable_channel_fact"`
	// FactsComplete requires BOTH source lanes to prove the entire window.
	// A normally pending live tail is exposed separately. Counts are still
	// returned while it is false so the page remains useful during catch-up,
	// but callers must not present them as a complete ranking.
	FactsComplete bool `json:"facts_complete"`
	// RequestsComplete covers request counts from both source lanes. TTFTComplete
	// (and canonical FRTComplete) is separate because historical request rows
	// may predate the FRT fields.
	RequestsComplete    bool                    `json:"requests_complete"`
	TTFTComplete        bool                    `json:"ttft_complete"`
	FRTComplete         bool                    `json:"frt_complete"`
	CoverageFromTs      int64                   `json:"coverage_from_ts"`
	CoverageThroughTs   int64                   `json:"coverage_through_ts"`
	CoverageStatus      string                  `json:"coverage_status"`
	RoutedCoverage      ModelStatisticsCoverage `json:"routed_coverage"`
	UnavailableCoverage ModelStatisticsCoverage `json:"unavailable_coverage"`
	TTFTCoverage        ModelStatisticsCoverage `json:"ttft_coverage"`
	FRTCoverage         ModelStatisticsCoverage `json:"frt_coverage"`
	// False because unknown user_id=0 rows are intentionally retained from
	// both sources when their request identity cannot be proven equal.
	RequestsAreUnique bool   `json:"requests_are_unique"`
	Note              string `json:"note"`
}

// modelStatisticsFactsCoverage checks the source watermark that produced the
// routed user-minute rows.  A non-empty table is not proof of a complete
// window: a stalled sampler can leave plausible-looking partial rankings.
// Keep this check local-only and conservative.
func (m *Monitor) modelStatisticsFactsCoverage(ctx context.Context, from, to int64) (complete bool, coveredFrom, coveredThrough int64, note string) {
	if m == nil || m.storeDB == nil || (!m.cfg.CapacityEnabled && !m.cfg.CustomerHealthSourceEnabled) {
		return false, 0, 0, "用户分钟事实采集未开启，当前排名可能不完整"
	}
	if m.cfg.CustomerHealthSourceEnabled {
		proof, err := m.customerHealthHistoricalCoverage(ctx, from, to)
		if err != nil {
			return false, 0, 0, "客户维护独立采集尚未建立连续水位，当前排名可能不完整"
		}
		coveredFrom, coveredThrough = proof.RequestFromTs, proof.RequestThroughTs
		complete = proof.RequestsComplete
		if complete {
			return true, coveredFrom, min(coveredThrough, to), "已由客户维护独立采集连续覆盖所选窗口"
		}
		return false, coveredFrom, minPositive(coveredThrough, to), "客户维护独立采集仍在追赶所选窗口，当前排名可能不完整"
	}
	complete, coveredFrom, coveredThrough = m.capacityWindowComplete(ctx, from, to, true), 0, 0
	var state MetricFinalizeState
	if err := m.storeDB.WithContext(ctx).First(&state, 1).Error; err == nil {
		coveredFrom, coveredThrough = state.CoverageFromTs, min(state.NextTs, to)
	}
	if complete {
		return true, coveredFrom, coveredThrough, "已由分钟事实定稿水位连续覆盖所选窗口"
	}
	return false, coveredFrom, coveredThrough, "分钟事实定稿水位未连续覆盖所选窗口，当前排名可能不完整"
}

func minPositive(value, upper int64) int64 {
	if value <= 0 {
		return 0
	}
	if upper > 0 && value > upper {
		return upper
	}
	return value
}

type ModelStatisticsReport struct {
	Window      string                 `json:"window"`
	WindowLabel string                 `json:"window_label"`
	FromTs      int64                  `json:"from_ts"`
	ToTs        int64                  `json:"to_ts"`
	TargetTs    int64                  `json:"target_ts"`
	LagSeconds  int64                  `json:"lag_seconds"`
	GeneratedAt int64                  `json:"generated_at"`
	GroupCount  int                    `json:"group_count"`
	Models      []ModelStatisticsModel `json:"models"`
	Source      ModelStatisticsSource  `json:"source"`
}

func modelStatisticsWindowFor(raw string) (modelStatisticsWindow, error) {
	key := strings.ToLower(strings.TrimSpace(raw))
	if key == "" {
		key = "24h"
	}
	window, ok := modelStatisticsWindows[key]
	if !ok {
		return modelStatisticsWindow{}, fmt.Errorf("window 必须是 24h、3d 或 7d")
	}
	return window, nil
}

func modelStatisticsRetentionDays(cfg Settings) int {
	if cfg.RetentionDays <= 0 {
		return 7
	}
	return cfg.RetentionDays
}

// modelStatisticsWindowTarget is the theoretical closed-minute target, not
// proof that a collector has committed that minute. The actual query end is
// also bounded by persisted cursors in modelStatisticsAvailableWindowEnd.
func modelStatisticsWindowTarget(m *Monitor, now time.Time) int64 {
	if now.IsZero() {
		now = time.Now()
	}
	targets := make([]int64, 0, 2)
	if m == nil || !m.cfg.CustomerHealthSourceEnabled {
		targets = append(targets, metricFinalizeTarget(now.Unix()))
	} else {
		// Do not clamp to today's midnight: during the first two minutes,
		// yesterday's last minutes are still inside the finalization delay.
		targets = append(targets, customerHealthSourceFinalizedThrough(now))
	}
	if m != nil && m.cfg.CloudWatchPreRouteEnabled {
		_, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
		targets = append(targets, target)
	}
	var end int64
	for _, target := range targets {
		if target <= 0 {
			continue
		}
		if end == 0 || target < end {
			end = target
		}
	}
	return end / 60 * 60
}

// buildModelStatisticsReport 只读取 Monitor 本地 SQLite，不触发生产库查询。
// to 同时受理论定稿目标与持久采集水位约束；窗口宽度不变。
func (m *Monitor) buildModelStatisticsReport(ctx context.Context, windowKey string, now time.Time) (ModelStatisticsReport, error) {
	window, err := modelStatisticsWindowFor(windowKey)
	if err != nil {
		return ModelStatisticsReport{}, err
	}
	if m == nil {
		return ModelStatisticsReport{}, fmt.Errorf("Monitor 未初始化")
	}
	retentionDays := modelStatisticsRetentionDays(m.cfg)
	if window.Seconds > int64(retentionDays)*86400 {
		return ModelStatisticsReport{}, fmt.Errorf("%s 数据超出当前分钟事实留存 %d 天", window.Label, retentionDays)
	}
	if m.storeDB == nil {
		return ModelStatisticsReport{}, fmt.Errorf("本地事实库未就绪")
	}
	// A normal polling delay must not mix an uncollected tail into an otherwise
	// complete window. Shift the whole window to the common committed end,
	// then validate all historical coverage and row semantics as before.
	target := modelStatisticsWindowTarget(m, now)
	to, err := m.modelStatisticsAvailableWindowEnd(ctx, target)
	if err != nil {
		return ModelStatisticsReport{}, fmt.Errorf("读取模型统计采集水位失败: %w", err)
	}
	if to <= 0 {
		return ModelStatisticsReport{}, fmt.Errorf("模型统计定稿水位尚未建立")
	}
	from := to - window.Seconds
	if from <= 0 || to <= from {
		return ModelStatisticsReport{}, fmt.Errorf("模型统计时间窗口无效")
	}

	var rows []modelStatisticsRow
	// Keep the overlap rule identical to the problem-alert query: a legacy
	// rejection is removed only when a CloudWatch direct row with the same
	// minute/model/group/user and canonical reason is actually present.  A
	// cursor by itself is only a coverage claim; it must not make an
	// uncorrelated legacy row disappear (for example user_id=0 versus a direct
	// row attributed to user 7).  user_id=0 on both sides is still unknown
	// identity, so it is deliberately fail-open and keeps the legacy row.
	legacyReason := alertRejectCanonicalReasonSQLForColumn("rejected.reason")
	directReason := alertRejectCanonicalReasonSQLForColumn("direct.reason")
	coverageWhere := ""
	coverageArgs := []any(nil)
	cloudWatchCoverageEnabled := m.cfg.CloudWatchPreRouteEnabled && cloudWatchPreRouteCoverageTableAvailable(m.storeDB)
	if cloudWatchCoverageEnabled {
		coverageWhere = `
		  AND (
		       rejected.node = ?
		       OR rejected.user_id <= 0
		       OR NOT EXISTS (
		           SELECT 1
		           FROM cloud_watch_pre_route_cursors AS coverage
		           WHERE coverage.id = ?
		             AND coverage.semantics_version = ?
		             AND rejected.bucket_ts >= coverage.coverage_from_ts
		             AND rejected.bucket_ts < coverage.through_ts
		             AND EXISTS (
		                 SELECT 1
		                 FROM rejection_samples AS direct
                         WHERE direct.node = ?
                           AND direct.bucket_ts = rejected.bucket_ts
                           AND direct.model = rejected.model
                           AND direct.grp = rejected.grp
                           AND direct.user_id = rejected.user_id
                           AND direct.count > 0
                           AND ` + directReason + ` = ` + legacyReason + `
		             )
		       )
		  )`
		coverageArgs = []any{cloudWatchPreRouteNode, cloudWatchPreRouteCursorID, cloudWatchPreRouteVersion, cloudWatchPreRouteNode}
	} else {
		// The direct lane may have been disabled after publishing facts.  Do
		// not use its stale cursor as an authority in that mode (or while an
		// older database is missing the cursor table), but still
		// suppress an explicit, fully matching direct duplicate.  Unmatched
		// legacy rows remain visible, and user_id=0 stays fail-open because it
		// is not a stable identity key.  This branch deliberately references
		// only rejection_samples, so an older local database without the
		// cursor table can still serve model statistics while the lane is off.
		coverageWhere = `
		  AND (
		       rejected.node = ?
		       OR rejected.user_id <= 0
		       OR NOT EXISTS (
		           SELECT 1
		           FROM rejection_samples AS direct
		           WHERE direct.node = ?
		             AND direct.bucket_ts = rejected.bucket_ts
		             AND direct.model = rejected.model
		             AND direct.grp = rejected.grp
		             AND direct.user_id = rejected.user_id
		             AND direct.count > 0
		             AND ` + directReason + ` = ` + legacyReason + `
		       )
		  )`
		coverageArgs = []any{cloudWatchPreRouteNode, cloudWatchPreRouteNode}
	}
	query := `
		SELECT grp, model_name AS model, user_id, channel_id, MAX(username) AS username,
		       SUM(CASE WHEN success + anomaly + failed > 0 THEN success + anomaly + failed ELSE 0 END) AS routed_requests,
		       0 AS unavailable_routes,
		       COALESCE(SUM(ttft_500),0) AS ttft_500, COALESCE(SUM(ttft_1k),0) AS ttft_1k,
		       COALESCE(SUM(ttft_2k),0) AS ttft_2k, COALESCE(SUM(ttft_5k),0) AS ttft_5k,
		       COALESCE(SUM(ttft_10k),0) AS ttft_10k, COALESCE(SUM(ttft_inf),0) AS ttft_inf,
		       COALESCE(MAX(ttft_max_ms),0) AS ttft_max_ms,
		       COALESCE(SUM(ttft_observed),0) AS ttft_observed,
		       COALESCE(SUM(ttft_over_3s),0) AS ttft_over_3s
		FROM capacity_user_minute_samples
		WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ? AND user_id >= 0
		GROUP BY grp, model_name, user_id, channel_id
		HAVING SUM(CASE WHEN success + anomaly + failed > 0 THEN success + anomaly + failed ELSE 0 END) > 0
		UNION ALL
		SELECT grp, model, user_id, 0 AS channel_id, '' AS username,
		       0 AS routed_requests,
		       SUM(CASE WHEN count > 0 THEN count ELSE 0 END) AS unavailable_routes,
		       0 AS ttft_500, 0 AS ttft_1k, 0 AS ttft_2k, 0 AS ttft_5k,
		       0 AS ttft_10k, 0 AS ttft_inf, 0 AS ttft_max_ms,
		       0 AS ttft_observed, 0 AS ttft_over_3s
		FROM rejection_samples AS rejected
		WHERE rejected.bucket_ts >= ? AND rejected.bucket_ts < ?
		  AND rejected.user_id >= 0
		  AND (` + legacyReason + `) = 'route_no_channel'
		` + coverageWhere + `
		GROUP BY grp, model, user_id
		HAVING SUM(CASE WHEN count > 0 THEN count ELSE 0 END) > 0`
	args := []any{from, to, stabilityTrafficClassificationVersion, from, to}
	args = append(args, coverageArgs...)
	if err := m.storeDB.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return ModelStatisticsReport{}, err
	}
	routedCoverage := m.modelStatisticsRoutedCoverage(ctx, from, to, now)
	rejectionCoverage := m.modelStatisticsRejectionCoverage(ctx, from, to, now)
	coverageStatus := modelStatisticsCombinedCoverage(routedCoverage, rejectionCoverage)

	groups := make(map[string]map[string]*modelStatisticsAggregate)
	usernames := make(map[int64]string)
	for _, row := range rows {
		group := strings.TrimSpace(row.Group)
		model := strings.TrimSpace(row.Model)
		models := groups[group]
		if models == nil {
			models = make(map[string]*modelStatisticsAggregate)
			groups[group] = models
		}
		item := models[model]
		if item == nil {
			item = &modelStatisticsAggregate{customers: make(map[int64]int64), channels: make(map[int]*modelStatisticsChannelAggregate)}
			models[model] = item
		}
		routed := maxNonNegative(row.RoutedRequests)
		unavailable := maxNonNegative(row.UnavailableRoutes)
		item.routed += routed
		item.unavailable += unavailable
		item.customers[row.UserID] += routed + unavailable
		if routed > 0 {
			mergeModelStatisticsTTFT(&item.ttft, row)
			channel := item.channels[row.ChannelID]
			if channel == nil {
				channel = &modelStatisticsChannelAggregate{}
				item.channels[row.ChannelID] = channel
			}
			channel.requests += routed
			mergeModelStatisticsTTFT(&channel.ttft, row)
		}
		if unavailable > 0 {
			// Keep pre-route requests visible in the same hierarchy. They have
			// no channel and therefore no meaningful TTFT denominator.
			channel := item.channels[modelStatisticsUnavailableChannelKey]
			if channel == nil {
				channel = &modelStatisticsChannelAggregate{}
				item.channels[modelStatisticsUnavailableChannelKey] = channel
			}
			channel.requests += unavailable
		}
		if name := strings.TrimSpace(row.Username); row.UserID > 0 && name != "" {
			usernames[row.UserID] = name
		}
	}
	userIDs := make([]int64, 0, len(usernames))
	seenUserIDs := make(map[int64]struct{})
	for _, models := range groups {
		for _, item := range models {
			for userID := range item.customers {
				if userID <= 0 {
					continue
				}
				if _, ok := seenUserIDs[userID]; ok {
					continue
				}
				seenUserIDs[userID] = struct{}{}
				userIDs = append(userIDs, userID)
			}
		}
	}
	// user_directory_entries 是后台同步的全量展示缓存，能覆盖只有前置拒绝、
	// 从未进入渠道的客户；查询失败或尚未同步时仍保留上面的日志内用户名。
	for userID, username := range lookupUserNames(m.storeDB.WithContext(ctx), userIDs) {
		usernames[userID] = username
	}
	if err := ctx.Err(); err != nil {
		return ModelStatisticsReport{}, err
	}
	channelNames := modelStatisticsChannelNames(m.storeDB.WithContext(ctx))
	if err := ctx.Err(); err != nil {
		return ModelStatisticsReport{}, err
	}

	sourceNote := "无可用渠道请求不会进入 NewAPI logs/metric_samples；已知客户维度按直采优先去重，普通前置拒绝不计入模型需求。user_id=0 无法证明跨来源是同一请求，为避免漏报会保守保留，可能重复计数（数量取决于未知身份重叠）。"
	sourceNote += " 首字耗时只统计采集到有效 FRT 的请求；旧的用户分钟事实没有原始 FRT 时显示为—，不会把缺失样本当成快速请求。"
	ttftCoverage := routedCoverage
	if routedCoverage.TTFTComplete {
		ttftCoverage.Status = modelCoverageComplete
		ttftCoverage.Note = "FRT：已路由事实的首个数据事件延迟覆盖已确认"
	} else {
		ttftCoverage.Status = modelCoverageIncomplete
		ttftCoverage.Note = "FRT：覆盖尚未确认，历史缺失样本不计入快速请求"
	}
	sourceNote += " " + routedCoverage.Note + " " + rejectionCoverage.Note + " " + ttftCoverage.Note
	report := ModelStatisticsReport{
		Window: window.Key, WindowLabel: window.Label, FromTs: from, ToTs: to,
		TargetTs: target, LagSeconds: max(int64(0), target-to), GeneratedAt: now.Unix(),
		GroupCount: len(groups), Models: make([]ModelStatisticsModel, 0),
		Source: ModelStatisticsSource{
			RoutedFact:       "capacity_user_minute_samples：复用客户维护采集的已进入渠道用户请求（成功、交付异常、错误均计一次）",
			UnavailableFact:  "rejection_samples：选渠道前无可用渠道的模型请求",
			FactsComplete:    coverageStatus == modelCoverageComplete,
			RequestsComplete: routedCoverage.RequestsComplete && rejectionCoverage.RequestsComplete,
			TTFTComplete:     routedCoverage.TTFTComplete && rejectionCoverage.TTFTComplete,
			CoverageFromTs:   routedCoverage.FromTs, CoverageThroughTs: routedCoverage.ThroughTs,
			CoverageStatus: coverageStatus, RoutedCoverage: routedCoverage, UnavailableCoverage: rejectionCoverage, TTFTCoverage: ttftCoverage,
			// Known positive user IDs are reconciled across the direct and
			// legacy rejection lanes.  user_id=0 is deliberately fail-open:
			// two rows with no identity cannot be proven to describe the same
			// request, so the report may conservatively retain both.
			RequestsAreUnique: false,
			Note:              sourceNote,
		},
	}
	modelGroups := make(map[string][]ModelStatisticsGroup)
	modelTTFT := make(map[string]modelStatisticsTTFTAggregate)
	for groupName, models := range groups {
		for modelName, item := range models {
			requests := item.routed + item.unavailable
			ttftObserved, ttftP50, ttftP95, ttftP99, ttftMax, ttftOver3s, ttftOver3sPct := modelStatisticsTTFTViewDetailed(item.ttft)
			modelAggregate := modelTTFT[modelName]
			mergeModelStatisticsTTFTAggregate(&modelAggregate, item.ttft)
			modelTTFT[modelName] = modelAggregate
			modelGroups[modelName] = append(modelGroups[modelName], ModelStatisticsGroup{
				Group: modelStatisticsDimensionLabel(groupName, "分组"), Requests: requests,
				RoutedRequests: item.routed, UnavailableRoutes: item.unavailable,
				TTFTObserved: ttftObserved, TTFTP50Ms: ttftP50, TTFTP95Ms: ttftP95, TTFTP99Ms: ttftP99,
				TTFTMaxMs: ttftMax, TTFTOver3s: ttftOver3s, TTFTOver3sPct: ttftOver3sPct,
				Channels:  modelStatisticsChannels(item.channels, channelNames),
				Customers: modelStatisticsCustomers(item.customers, usernames, requests),
			})
		}
	}
	for modelName, groupRows := range modelGroups {
		model := ModelStatisticsModel{Model: modelStatisticsDimensionLabel(modelName, "模型"), Groups: groupRows}
		for _, group := range groupRows {
			model.Requests += group.Requests
			model.RoutedRequests += group.RoutedRequests
			model.UnavailableRoutes += group.UnavailableRoutes
		}
		model.TTFTObserved, model.TTFTP50Ms, model.TTFTP95Ms, model.TTFTP99Ms, model.TTFTMaxMs, model.TTFTOver3s, model.TTFTOver3sPct = modelStatisticsTTFTViewDetailed(modelTTFT[modelName])
		sortModelStatisticsGroups(model.Groups)
		report.Models = append(report.Models, model)
	}
	sortModelStatisticsModels(report.Models)
	syncModelStatisticsFRTAliases(&report)
	return report, nil
}

// syncModelStatisticsFRTAliases publishes the canonical FRT vocabulary while
// retaining the historical ttft_* JSON keys for existing clients. The source
// remains other.frt (first data event), never a verified model-token TTFT.
func syncModelStatisticsFRTAliases(report *ModelStatisticsReport) {
	if report == nil {
		return
	}
	s := &report.Source
	s.FRTComplete = s.TTFTComplete
	s.FRTCoverage = s.TTFTCoverage
	syncCoverageFRTAliases(&s.FRTCoverage)
	syncCoverageFRTAliases(&s.RoutedCoverage)
	syncCoverageFRTAliases(&s.UnavailableCoverage)
	syncCoverageFRTAliases(&s.TTFTCoverage)
	for mi := range report.Models {
		model := &report.Models[mi]
		model.FRTObserved, model.FRTP50Ms, model.FRTP95Ms, model.FRTP99Ms = model.TTFTObserved, model.TTFTP50Ms, model.TTFTP95Ms, model.TTFTP99Ms
		model.FRTMaxMs, model.FRTOver3s, model.FRTOver3sPct = model.TTFTMaxMs, model.TTFTOver3s, model.TTFTOver3sPct
		for gi := range model.Groups {
			group := &model.Groups[gi]
			group.FRTObserved, group.FRTP50Ms, group.FRTP95Ms, group.FRTP99Ms = group.TTFTObserved, group.TTFTP50Ms, group.TTFTP95Ms, group.TTFTP99Ms
			group.FRTMaxMs, group.FRTOver3s, group.FRTOver3sPct = group.TTFTMaxMs, group.TTFTOver3s, group.TTFTOver3sPct
			for ci := range group.Channels {
				channel := &group.Channels[ci]
				channel.FRTObserved, channel.FRTP50Ms, channel.FRTP95Ms, channel.FRTP99Ms = channel.TTFTObserved, channel.TTFTP50Ms, channel.TTFTP95Ms, channel.TTFTP99Ms
				channel.FRTMaxMs, channel.FRTOver3s, channel.FRTOver3sPct = channel.TTFTMaxMs, channel.TTFTOver3s, channel.TTFTOver3sPct
			}
		}
	}
}

func syncCoverageFRTAliases(c *ModelStatisticsCoverage) {
	if c == nil {
		return
	}
	c.FRTComplete = c.TTFTComplete
	c.FRTFromTs, c.FRTThroughTs = c.TTFTFromTs, c.TTFTThroughTs
}

func mergeModelStatisticsTTFT(dst *modelStatisticsTTFTAggregate, row modelStatisticsRow) {
	if dst == nil {
		return
	}
	if row.Ttft500 < 0 || row.Ttft1k < 0 || row.Ttft2k < 0 || row.Ttft5k < 0 || row.Ttft10k < 0 || row.TtftInf < 0 || row.TtftObserved < 0 || row.TtftOver3s < 0 || row.TtftMaxMs < 0 {
		dst.invalid = true
	}
	dst.ttft500 += row.Ttft500
	dst.ttft1k += row.Ttft1k
	dst.ttft2k += row.Ttft2k
	dst.ttft5k += row.Ttft5k
	dst.ttft10k += row.Ttft10k
	dst.ttftInf += row.TtftInf
	dst.observed += row.TtftObserved
	dst.over3s += row.TtftOver3s
	if row.TtftMaxMs > 0 && int64(row.TtftMaxMs) > dst.maxMs {
		dst.maxMs = int64(row.TtftMaxMs)
	}
}

func mergeModelStatisticsTTFTAggregate(dst *modelStatisticsTTFTAggregate, src modelStatisticsTTFTAggregate) {
	if dst == nil {
		return
	}
	dst.ttft500 += src.ttft500
	dst.ttft1k += src.ttft1k
	dst.ttft2k += src.ttft2k
	dst.ttft5k += src.ttft5k
	dst.ttft10k += src.ttft10k
	dst.ttftInf += src.ttftInf
	dst.observed += src.observed
	dst.over3s += src.over3s
	dst.invalid = dst.invalid || src.invalid
	if src.maxMs > dst.maxMs {
		dst.maxMs = src.maxMs
	}
}

func modelStatisticsTTFTViewDetailed(a modelStatisticsTTFTAggregate) (observed int64, p50, p95, p99 float64, maxMs, over3s int64, over3sPct float64) {
	if a.invalid {
		return
	}
	view := computeTTFTMetricView(
		[6]int64{a.ttft500, a.ttft1k, a.ttft2k, a.ttft5k, a.ttft10k, a.ttftInf},
		a.observed, a.over3s, a.maxMs,
	)
	return view.Observed, view.P50Ms, view.P95Ms, view.P99Ms, view.MaxMs, view.Over3s, view.Over3sPct
}

// modelStatisticsTTFTView keeps the pre-P99 helper contract used by older
// tests/callers while the report itself publishes the additional percentile.
func modelStatisticsTTFTView(a modelStatisticsTTFTAggregate) (observed int64, p50, p95 float64, maxMs, over3s int64, over3sPct float64) {
	observed, p50, p95, _, maxMs, over3s, over3sPct = modelStatisticsTTFTViewDetailed(a)
	return
}

func modelStatisticsChannelNames(db *gorm.DB) map[int]string {
	names := map[int]string{}
	if db == nil {
		return names
	}
	var rows []ChannelSnap
	tx := db.Select("id, name").Find(&rows)
	if tx.Error != nil {
		warnReadErr("modelStatisticsChannelNames", tx)
		return names
	}
	for _, row := range rows {
		if row.ID <= 0 {
			continue
		}
		if name := strings.TrimSpace(row.Name); name != "" {
			names[row.ID] = name
		} else {
			names[row.ID] = fmt.Sprintf("渠道 #%d", row.ID)
		}
	}
	return names
}

func modelStatisticsChannels(aggregates map[int]*modelStatisticsChannelAggregate, names map[int]string) []ModelStatisticsChannel {
	channels := make([]ModelStatisticsChannel, 0, len(aggregates))
	for id, aggregate := range aggregates {
		if aggregate == nil || aggregate.requests <= 0 {
			continue
		}
		observed, p50, p95, p99, maxMs, over3s, over3sPct := modelStatisticsTTFTViewDetailed(aggregate.ttft)
		outputID := id
		kind := "routed"
		unavailable := false
		name := names[id]
		if id == modelStatisticsUnavailableChannelKey {
			kind = "unavailable"
			unavailable = true
			name = "无可用渠道"
		} else if id == 0 {
			kind = "routed_unknown"
			if name == "" {
				name = "未标注渠道"
			}
		} else if name == "" {
			name = fmt.Sprintf("渠道 #%d", id)
		}
		channels = append(channels, ModelStatisticsChannel{
			ChannelID: outputID, ChannelKind: kind, IsUnavailable: unavailable, ChannelName: name, Requests: aggregate.requests,
			TTFTObserved: observed, TTFTP50Ms: p50, TTFTP95Ms: p95, TTFTP99Ms: p99, TTFTMaxMs: maxMs,
			TTFTOver3s: over3s, TTFTOver3sPct: over3sPct,
		})
	}
	sort.SliceStable(channels, func(i, j int) bool {
		if channels[i].Requests != channels[j].Requests {
			return channels[i].Requests > channels[j].Requests
		}
		if channels[i].ChannelID != channels[j].ChannelID {
			return channels[i].ChannelID < channels[j].ChannelID
		}
		if channels[i].ChannelKind != channels[j].ChannelKind {
			return channels[i].ChannelKind < channels[j].ChannelKind
		}
		return channels[i].ChannelName < channels[j].ChannelName
	})
	return channels
}

func modelStatisticsCustomers(counts map[int64]int64, usernames map[int64]string, total int64) []ModelStatisticsCustomer {
	out := make([]ModelStatisticsCustomer, 0, len(counts))
	for userID, requests := range counts {
		if requests <= 0 {
			continue
		}
		name := strings.TrimSpace(usernames[userID])
		if name == "" {
			name = "未知客户"
		}
		share := 0.0
		if total > 0 {
			share = float64(requests) * 100 / float64(total)
		}
		out = append(out, ModelStatisticsCustomer{
			CustomerID: userID, CustomerName: name, Requests: requests, SharePct: share,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Requests != out[j].Requests {
			return out[i].Requests > out[j].Requests
		}
		if out[i].CustomerID != out[j].CustomerID {
			return out[i].CustomerID < out[j].CustomerID
		}
		return out[i].CustomerName < out[j].CustomerName
	})
	return out
}

func modelStatisticsDimensionLabel(value, kind string) string {
	if value != "" {
		return value
	}
	return "未标注" + kind
}

func maxNonNegative(value int64) int64 {
	if value < 0 {
		return 0
	}
	return value
}

func sortModelStatisticsModels(rows []ModelStatisticsModel) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Requests != rows[j].Requests {
			return rows[i].Requests > rows[j].Requests
		}
		return rows[i].Model < rows[j].Model
	})
}

func sortModelStatisticsGroups(rows []ModelStatisticsGroup) {
	sort.SliceStable(rows, func(i, j int) bool {
		if rows[i].Requests != rows[j].Requests {
			return rows[i].Requests > rows[j].Requests
		}
		return rows[i].Group < rows[j].Group
	})
}

// serveModelStatisticsReport GET /model-statistics/report
func (m *Monitor) serveModelStatisticsReport(c *gin.Context) {
	if m.storeDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地事实库未就绪，无法统计模型"})
		return
	}
	window := c.DefaultQuery("window", "24h")
	if _, err := modelStatisticsWindowFor(window); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), modelStatisticsQueryTimeout)
	defer cancel()
	report, err := m.buildModelStatisticsReport(ctx, window, time.Now())
	if err != nil {
		if strings.Contains(err.Error(), "超出当前分钟事实留存") {
			c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
			return
		}
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "模型统计本地事实暂时无法读取"})
		return
	}
	c.Header("Cache-Control", "private, no-store")
	c.JSON(http.StatusOK, report)
}
