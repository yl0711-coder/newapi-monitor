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
	RoutedRequests    int64  `gorm:"column:routed_requests"`
	UnavailableRoutes int64  `gorm:"column:unavailable_routes"`
}

type modelStatisticsAggregate struct {
	routed, unavailable int64
	customers           map[int64]int64
}

type ModelStatisticsModel struct {
	Model             string                 `json:"model"`
	Requests          int64                  `json:"requests"`
	RoutedRequests    int64                  `json:"routed_requests"`
	UnavailableRoutes int64                  `json:"unavailable_channel_requests"`
	Groups            []ModelStatisticsGroup `json:"groups"`
}

type ModelStatisticsGroup struct {
	Group             string                    `json:"group"`
	Requests          int64                     `json:"requests"`
	RoutedRequests    int64                     `json:"routed_requests"`
	UnavailableRoutes int64                     `json:"unavailable_channel_requests"`
	Customers         []ModelStatisticsCustomer `json:"customers"`
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
	// FactsComplete is true only when the local user-minute fact lane has
	// proved continuous coverage for the requested window.  Counts are still
	// returned while it is false so the page remains useful during catch-up,
	// but callers must not present them as a complete ranking.
	FactsComplete     bool  `json:"facts_complete"`
	CoverageFromTs    int64 `json:"coverage_from_ts"`
	CoverageThroughTs int64 `json:"coverage_through_ts"`
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
	if m == nil || m.storeDB == nil || !m.cfg.CapacityEnabled {
		return false, 0, 0, "用户分钟事实采集未开启，当前排名可能不完整"
	}
	if m.cfg.CustomerHealthSourceEnabled {
		var state CustomerHealthSourceCursor
		if err := m.storeDB.WithContext(ctx).First(&state, 1).Error; err != nil {
			return false, 0, 0, "客户维护独立采集尚未建立连续水位，当前排名可能不完整"
		}
		coveredFrom, coveredThrough = state.DayTs, state.ThroughTs
		complete = state.SemanticsVersion == customerHealthStabilityPolicyVersion &&
			coveredFrom > 0 && coveredFrom <= from && coveredThrough >= to
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

// buildModelStatisticsReport 只读取 Monitor 本地 SQLite，不触发生产库查询。
// to 取最近一个已闭合分钟，避免正在写入的当前分钟被展示成稳定数字。
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
	to := now.Unix() / 60 * 60
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
		SELECT grp, model_name AS model, user_id, MAX(username) AS username,
		       SUM(CASE WHEN success + anomaly + failed > 0 THEN success + anomaly + failed ELSE 0 END) AS routed_requests,
		       0 AS unavailable_routes
		FROM capacity_user_minute_samples
		WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ? AND user_id >= 0
		GROUP BY grp, model_name, user_id
		HAVING SUM(CASE WHEN success + anomaly + failed > 0 THEN success + anomaly + failed ELSE 0 END) > 0
		UNION ALL
		SELECT grp, model, user_id, '' AS username,
		       0 AS routed_requests,
		       SUM(CASE WHEN count > 0 THEN count ELSE 0 END) AS unavailable_routes
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
	factsComplete, coveredFrom, coveredThrough, factsNote := m.modelStatisticsFactsCoverage(ctx, from, to)

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
			item = &modelStatisticsAggregate{customers: make(map[int64]int64)}
			models[model] = item
		}
		routed := maxNonNegative(row.RoutedRequests)
		unavailable := maxNonNegative(row.UnavailableRoutes)
		item.routed += routed
		item.unavailable += unavailable
		item.customers[row.UserID] += routed + unavailable
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
	for userID, username := range lookupUserNames(m.storeDB, userIDs) {
		usernames[userID] = username
	}

	sourceNote := "无可用渠道请求不会进入 NewAPI logs/metric_samples；已知客户维度按直采优先去重，普通前置拒绝不计入模型需求。user_id=0 无法证明跨来源是同一请求，为避免漏报会保守保留，可能重复计数（数量取决于未知身份重叠）。"
	sourceNote += " " + factsNote
	if coverageNote := m.alertsCoverageNote(stabilityScope{FromTs: from, ToTs: to}, now); coverageNote != "" {
		sourceNote += " " + coverageNote
	}
	report := ModelStatisticsReport{
		Window: window.Key, WindowLabel: window.Label, FromTs: from, ToTs: to, GeneratedAt: now.Unix(),
		GroupCount: len(groups), Models: make([]ModelStatisticsModel, 0),
		Source: ModelStatisticsSource{
			RoutedFact:      "capacity_user_minute_samples：复用客户维护采集的已进入渠道用户请求（成功、交付异常、错误均计一次）",
			UnavailableFact: "rejection_samples：选渠道前无可用渠道的模型请求",
			FactsComplete:   factsComplete, CoverageFromTs: coveredFrom, CoverageThroughTs: coveredThrough,
			// Known positive user IDs are reconciled across the direct and
			// legacy rejection lanes.  user_id=0 is deliberately fail-open:
			// two rows with no identity cannot be proven to describe the same
			// request, so the report may conservatively retain both.
			RequestsAreUnique: false,
			Note:              sourceNote,
		},
	}
	modelGroups := make(map[string][]ModelStatisticsGroup)
	for groupName, models := range groups {
		for modelName, item := range models {
			requests := item.routed + item.unavailable
			modelGroups[modelName] = append(modelGroups[modelName], ModelStatisticsGroup{
				Group: modelStatisticsDimensionLabel(groupName, "分组"), Requests: requests,
				RoutedRequests: item.routed, UnavailableRoutes: item.unavailable,
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
		sortModelStatisticsGroups(model.Groups)
		report.Models = append(report.Models, model)
	}
	sortModelStatisticsModels(report.Models)
	return report, nil
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
