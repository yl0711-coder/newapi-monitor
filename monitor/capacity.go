package monitor

// capacity.go 是容量规划的旁路读取层。它只读 Monitor SQLite 中已有的
// 业务分钟事实、Nginx 脱敏分钟事实和资源采样；不连接生产 MySQL，
// 不改变任何采样/稳定性/用量口径。任一可选数据源缺失时只局部降级。

import (
	"context"
	"encoding/base64"
	"fmt"
	"math"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const capacityQueryTimeout = 5 * time.Second
const capacityEmptyDimensionFilter = "\x00capacity-empty"

type capacitySource struct {
	Available           bool   `json:"available"`
	Configured          bool   `json:"configured"`
	Watermark           int64  `json:"watermark"`
	AgeSec              *int64 `json:"age_sec"`
	FilteredWatermark   int64  `json:"filtered_watermark,omitempty"`
	FilteredAgeSec      *int64 `json:"filtered_age_sec,omitempty"`
	Rows                int64  `json:"rows"`
	DimensionsAvailable *bool  `json:"dimensions_available,omitempty"`
	Note                string `json:"note,omitempty"`
}

type capacityMeta struct {
	FromTs        int64                     `json:"from_ts"`
	ToTs          int64                     `json:"to_ts"`
	BucketSeconds int64                     `json:"bucket_seconds"`
	GeneratedAt   int64                     `json:"generated_at"`
	Filters       map[string]any            `json:"filters"`
	Sources       map[string]capacitySource `json:"sources"`
}

type capacityPoint struct {
	Ts                   int64    `json:"ts"`
	DurationMinutes      int64    `json:"duration_minutes"`
	ObservedTrafficMins  int64    `json:"observed_traffic_minutes"`
	LoggedRequests       int64    `json:"logged_requests"`
	RejectedRequests     int64    `json:"rejected_requests"`
	BusinessRequests     int64    `json:"business_requests"`
	Tokens               int64    `json:"tokens"`
	LoggedRPM            *float64 `json:"logged_rpm"`
	RejectedRPM          *float64 `json:"rejected_rpm"`
	BusinessRPM          *float64 `json:"business_rpm"`
	TPM                  *float64 `json:"tpm"`
	EstimatedConcurrency *float64 `json:"estimated_concurrency"`
	StabilityPct         *float64 `json:"stability_pct"`
	P95Seconds           *float64 `json:"p95_seconds"`
}

type capacitySummary struct {
	CurrentBusinessRPM *float64 `json:"current_business_rpm"`
	PeakBusinessRPM    *float64 `json:"peak_business_rpm"`
	CurrentTPM         *float64 `json:"current_tpm"`
	PeakTPM            *float64 `json:"peak_tpm"`
	PeakConcurrency    *float64 `json:"peak_concurrency"`
	StabilityPct       *float64 `json:"stability_pct"`
	LoggedRequests     int64    `json:"logged_requests"`
	RejectedRequests   int64    `json:"rejected_requests"`
	BusinessRequests   int64    `json:"business_requests"`
	Tokens             int64    `json:"tokens"`
	PeakAt             int64    `json:"peak_at"`
	CurrentAt          int64    `json:"current_at"`
}

type capacityBreakdown struct {
	Key              string   `json:"key"`
	Label            string   `json:"label"`
	Requests         int64    `json:"requests"`
	RejectedRequests int64    `json:"rejected_requests"`
	Tokens           int64    `json:"tokens"`
	AverageRPM       float64  `json:"average_rpm"`
	AverageTPM       float64  `json:"average_tpm"`
	StabilityPct     *float64 `json:"stability_pct"`
	StabilityScope   string   `json:"stability_scope"`
}

type capacityOption struct {
	Key   string `json:"key"`
	Label string `json:"label"`
}

type capacityIngressPoint struct {
	Ts                        int64   `json:"ts"`
	Node                      string  `json:"node"`
	Requests                  int64   `json:"requests"`
	RPM                       float64 `json:"rpm"`
	Error5xx                  int64   `json:"error_5xx"`
	UpstreamError5xx          int64   `json:"upstream_error_5xx"`
	AverageLatency            float64 `json:"average_latency_ms"`
	AverageUpstreamLatency    float64 `json:"average_upstream_latency_ms"`
	MaxLatency                float64 `json:"max_latency_ms"`
	EstimatedInflight         float64 `json:"estimated_inflight"`
	EstimatedUpstreamInflight float64 `json:"estimated_upstream_inflight"`
}

type capacityInfraPoint struct {
	Ts       int64   `json:"ts"`
	Resource string  `json:"resource"`
	RType    string  `json:"rtype"`
	Metric   string  `json:"metric"`
	Value    float64 `json:"value"`
}

type capacityEconomics struct {
	Available bool   `json:"available"`
	Note      string `json:"note"`
}

type capacityReport struct {
	Enabled    bool                           `json:"enabled"`
	Meta       capacityMeta                   `json:"meta"`
	Summary    capacitySummary                `json:"summary"`
	Series     []capacityPoint                `json:"series"`
	Breakdowns map[string][]capacityBreakdown `json:"breakdowns"`
	Options    map[string][]capacityOption    `json:"options"`
	Ingress    []capacityIngressPoint         `json:"ingress"`
	Infra      []capacityInfraPoint           `json:"infra"`
	Components []InfraResource                `json:"components"`
	Economics  capacityEconomics              `json:"economics"`
}

type capacityMinuteRow struct {
	Ts         int64 `gorm:"column:ts"`
	Success    int64
	Anomaly    int64
	Failed     int64
	Tokens     int64
	SumUseTime int64 `gorm:"column:sum_use_time"`
	Lat1       int64 `gorm:"column:lat_1"`
	Lat2       int64 `gorm:"column:lat_2"`
	Lat5       int64 `gorm:"column:lat_5"`
	Lat10      int64 `gorm:"column:lat_10"`
	Lat30      int64 `gorm:"column:lat_30"`
	Lat60      int64 `gorm:"column:lat_60"`
	LatInf     int64 `gorm:"column:lat_inf"`
}

type capacityDimensionRow struct {
	ChannelID int    `gorm:"column:channel_id"`
	ModelName string `gorm:"column:model_name"`
	Grp       string `gorm:"column:grp"`
	Success   int64
	Anomaly   int64
	Failed    int64
	Tokens    int64
}

type capacityRejectionRow struct {
	Ts    int64 `gorm:"column:ts"`
	Count int64
}

type capacityRejectionDimensionRow struct {
	ModelName string `gorm:"column:model_name"`
	Grp       string `gorm:"column:grp"`
	Count     int64
}

type capacityIngressRow struct {
	Ts                int64  `gorm:"column:ts"`
	Node              string `gorm:"column:node"`
	Count             int64
	Error5xx          int64 `gorm:"column:error_5xx"`
	UpstreamError5xx  int64 `gorm:"column:upstream_error_5xx"`
	RequestTimeSumMS  int64 `gorm:"column:request_time_sum_ms"`
	RequestTimeMaxMS  int64 `gorm:"column:request_time_max_ms"`
	UpstreamTimeSumMS int64 `gorm:"column:upstream_time_sum_ms"`
	UpstreamTimeCount int64 `gorm:"column:upstream_time_count"`
}

type capacityInfraRow struct {
	Ts       int64   `gorm:"column:ts"`
	Resource string  `gorm:"column:resource"`
	RType    string  `gorm:"column:rtype"`
	Metric   string  `gorm:"column:metric"`
	Value    float64 `gorm:"column:value"`
}

func capacityBucketSeconds(hours int) int64 {
	switch {
	case hours <= 6:
		return 60
	case hours <= 24:
		return 300
	default:
		return 900
	}
}

func capacityHours(raw string) int {
	h, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		return 24
	}
	for _, allowed := range []int{1, 6, 24, 168} {
		if h == allowed {
			return h
		}
	}
	return 24
}

func capacityAge(now, watermark int64) *int64 {
	if watermark <= 0 {
		return nil
	}
	age := now - watermark - 60
	if age < 0 {
		age = 0
	}
	return &age
}

func capacityFloatPtr(v float64) *float64 { return &v }

// 下拉框 key 对原始值做可逆编码，使空字符串与“未筛选”严格区分，
// 也避免展示名“未标记”与真实同名分组发生碰撞。
func capacityDimensionKey(raw string) string {
	return "v:" + base64.RawURLEncoding.EncodeToString([]byte(raw))
}

func capacityDimensionValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	if !strings.HasPrefix(raw, "v:") {
		return raw, nil // 兼容旧链接中的直接值。
	}
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimPrefix(raw, "v:"))
	if err != nil {
		return "", fmt.Errorf("invalid dimension filter")
	}
	if len(decoded) == 0 {
		return capacityEmptyDimensionFilter, nil
	}
	return string(decoded), nil
}

func capacityRawDimensionValue(value string) string {
	if value == capacityEmptyDimensionFilter {
		return ""
	}
	return value
}

func capacityActualChannelID(channelID int) int {
	if channelID == -1 {
		return 0
	}
	return channelID
}

func (m *Monitor) serveCapacityReport(c *gin.Context) {
	if !m.cfg.CapacityEnabled {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	if m.storeDB == nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"enabled": true, "error": "local store unavailable"})
		return
	}

	hours := capacityHours(c.Query("hours"))
	channelID := 0
	if raw := strings.TrimSpace(c.Query("channel")); raw != "" {
		parsed, err := strconv.Atoi(raw)
		if err != nil || parsed < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"enabled": true, "error": "channel 必须是非负整数"})
			return
		}
		channelID = parsed
		if parsed == 0 {
			channelID = -1 // -1 是内部筛选哨兵，真实查询值仍为 channel_id=0。
		}
	}
	groupRaw, modelRaw := strings.TrimSpace(c.Query("group")), strings.TrimSpace(c.Query("model"))
	group, err := capacityDimensionValue(groupRaw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"enabled": true, "error": "group 筛选值无效"})
		return
	}
	model, err := capacityDimensionValue(modelRaw)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"enabled": true, "error": "model 筛选值无效"})
		return
	}
	now := time.Now().Unix()
	to := now / 60 * 60 // 当前分钟还在写入，永不进报表。
	from := to - int64(hours)*3600
	bucket := capacityBucketSeconds(hours)

	ctx, cancel := context.WithTimeout(c.Request.Context(), capacityQueryTimeout)
	defer cancel()
	report, err := m.buildCapacityReport(ctx, from, to, bucket, channelID, group, model, now)
	if err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{"enabled": true, "error": "本地容量事实暂时无法读取"})
		return
	}
	c.JSON(http.StatusOK, report)
}

func (m *Monitor) buildCapacityReport(ctx context.Context, from, to, bucket int64, channelID int, group, model string, now int64) (capacityReport, error) {
	report := capacityReport{Enabled: true, Breakdowns: map[string][]capacityBreakdown{}, Options: map[string][]capacityOption{},
		Economics: capacityEconomics{Available: false, Note: "待上游成本与利润模块完成后，再评估当前及扩容成本覆盖。"}}
	report.Meta = capacityMeta{FromTs: from, ToTs: to, BucketSeconds: bucket, GeneratedAt: now,
		Filters: map[string]any{"channel": capacityActualChannelID(channelID), "group": capacityRawDimensionValue(group), "model": capacityRawDimensionValue(model)}, Sources: map[string]capacitySource{}}

	minuteRows, dimRows, metricSource, err := m.readCapacityMetrics(ctx, from, to, channelID, group, model, now)
	if err != nil {
		return report, err // 业务分钟事实是本页唯一必需源。
	}
	report.Meta.Sources["business_log"] = metricSource

	rejections, rejectionSource := m.readCapacityRejections(ctx, from, to, channelID, group, model, now)
	report.Meta.Sources["pre_route_rejection"] = rejectionSource

	report.Series, report.Summary = aggregateCapacitySeries(minuteRows, rejections, from, to, bucket)
	var rejectionDims []capacityRejectionDimensionRow
	rejectionsIncluded := channelID == 0 && rejectionSource.Available
	if rejectionsIncluded {
		dimensionsAvailable := true
		rejectionSource.DimensionsAvailable = &dimensionsAvailable
		report.Meta.Sources["pre_route_rejection"] = rejectionSource
		if rows, err := m.readCapacityRejectionDimensions(ctx, from, to); err == nil {
			rejectionDims = rows
		} else {
			rejectionsIncluded = false
			dimensionsAvailable = false
			rejectionSource.DimensionsAvailable = &dimensionsAvailable
			rejectionSource.Note += " 前置拒绝分钟汇总仍可用，但维度归因不可用；排名回退为仅日志口径。"
			report.Meta.Sources["pre_route_rejection"] = rejectionSource
		}
	}
	report.Breakdowns, report.Options = m.capacityBreakdowns(dimRows, rejectionDims, to-from, channelID, group, model, rejectionsIncluded)

	ingress, ingressSource := m.readCapacityIngress(ctx, from, to, bucket, now)
	report.Ingress = ingress
	report.Meta.Sources["nginx_ingress"] = ingressSource

	infra, infraSource := m.readCapacityInfra(ctx, from, to, bucket, now)
	report.Infra = infra
	report.Meta.Sources["infrastructure"] = infraSource
	if m.InfraEnabled() {
		snap := m.computeInfraSnapshot(now)
		report.Components = append(report.Components, snap.Instances...)
		if snap.Database != nil {
			report.Components = append(report.Components, *snap.Database)
		}
		if snap.LB != nil {
			report.Components = append(report.Components, *snap.LB)
		}
	}
	return report, nil
}

func (m *Monitor) readCapacityMetrics(ctx context.Context, from, to int64, channelID int, group, model string, now int64) ([]capacityMinuteRow, []capacityDimensionRow, capacitySource, error) {
	baseWhere := "bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?"
	baseArgs := []any{from, to, userTrafficClassificationVersion}
	where := baseWhere
	args := append([]any{}, baseArgs...)
	if channelID > 0 {
		channelID = capacityActualChannelID(channelID)
		where += " AND channel_id = ?"
		args = append(args, channelID)
	} else if channelID == -1 {
		where += " AND channel_id = 0"
	}
	if group != "" {
		where += " AND grp = ?"
		args = append(args, capacityRawDimensionValue(group))
	}
	if model != "" {
		where += " AND model_name = ?"
		args = append(args, capacityRawDimensionValue(model))
	}
	var rows []capacityMinuteRow
	minuteSQL := `SELECT bucket_ts ts, SUM(success) success, SUM(anomaly) anomaly, SUM(failed) failed,
		SUM(tokens) tokens, SUM(sum_use_time) sum_use_time,
		SUM(lat_1) lat_1, SUM(lat_2) lat_2, SUM(lat_5) lat_5, SUM(lat_10) lat_10,
		SUM(lat_30) lat_30, SUM(lat_60) lat_60, SUM(lat_inf) lat_inf
		FROM metric_samples WHERE ` + where + ` GROUP BY bucket_ts ORDER BY bucket_ts`
	if err := m.storeDB.WithContext(ctx).Raw(minuteSQL, args...).Scan(&rows).Error; err != nil {
		return nil, nil, capacitySource{}, err
	}
	var dims []capacityDimensionRow
	dimSQL := `SELECT channel_id, model_name, grp, SUM(success) success, SUM(anomaly) anomaly,
		SUM(failed) failed, SUM(tokens) tokens FROM metric_samples WHERE ` + baseWhere + `
		GROUP BY channel_id, model_name, grp`
	if err := m.storeDB.WithContext(ctx).Raw(dimSQL, baseArgs...).Scan(&dims).Error; err != nil {
		return nil, nil, capacitySource{}, err
	}
	var filteredWatermark int64
	if len(rows) > 0 {
		filteredWatermark = rows[len(rows)-1].Ts
	}
	var globalWatermark int64
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(MAX(bucket_ts),0) FROM metric_samples
		WHERE traffic_class_version = ? AND bucket_ts < ?`, userTrafficClassificationVersion, to).Scan(&globalWatermark).Error; err != nil {
		return nil, nil, capacitySource{}, err
	}
	source := capacitySource{Available: globalWatermark > 0, Configured: true, Watermark: globalWatermark,
		AgeSec: capacityAge(now, globalWatermark), FilteredWatermark: filteredWatermark, FilteredAgeSec: capacityAge(now, filteredWatermark), Rows: int64(len(rows)),
		Note: "Rows 为当前筛选的已观测分钟数；已排除渠道内部测试，空闲分钟不伪造为采样数据。"}
	return rows, dims, source, nil
}

func (m *Monitor) readCapacityRejectionDimensions(ctx context.Context, from, to int64) ([]capacityRejectionDimensionRow, error) {
	var rows []capacityRejectionDimensionRow
	err := m.storeDB.WithContext(ctx).Raw(`SELECT model model_name, grp, SUM(count) count
		FROM rejection_samples WHERE bucket_ts >= ? AND bucket_ts < ? GROUP BY model, grp`, from, to).Scan(&rows).Error
	return rows, err
}

func (m *Monitor) readCapacityRejections(ctx context.Context, from, to int64, channelID int, group, model string, now int64) (map[int64]int64, capacitySource) {
	configured := strings.TrimSpace(m.cfg.IngestToken) != ""
	if channelID != 0 { // 前置拒绝没有渠道维度，不能伪归到某渠道。
		return map[int64]int64{}, capacitySource{Configured: configured, Note: "按渠道筛选时前置拒绝不参与计算：拒绝发生在选渠道之前。"}
	}
	where := "bucket_ts >= ? AND bucket_ts < ?"
	args := []any{from, to}
	if group != "" {
		where += " AND grp = ?"
		args = append(args, capacityRawDimensionValue(group))
	}
	if model != "" {
		where += " AND model = ?"
		args = append(args, capacityRawDimensionValue(model))
	}
	var rows []capacityRejectionRow
	err := m.storeDB.WithContext(ctx).Raw(`SELECT bucket_ts ts, SUM(count) count FROM rejection_samples WHERE `+where+` GROUP BY bucket_ts`, args...).Scan(&rows).Error
	out := map[int64]int64{}
	var watermark int64
	var count int64
	if err == nil {
		for _, row := range rows {
			out[row.Ts] += row.Count
			count += row.Count
			if row.Ts > watermark {
				watermark = row.Ts
			}
		}
	}
	var ingest struct{ Last int64 }
	_ = m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(MAX(received_at),0) last FROM rejection_ingest_batches`).Scan(&ingest).Error
	if ingest.Last > watermark {
		watermark = ingest.Last / 60 * 60
	}
	note := "独立显示路由前拒绝，不冒充已进入 NewAPI 日志的请求。"
	if err != nil {
		note = "前置拒绝本地事实不可用；不影响业务日志容量数据。"
	}
	return out, capacitySource{Available: err == nil && watermark > 0, Configured: configured,
		Watermark: watermark, AgeSec: capacityAge(now, watermark), Rows: count, Note: note}
}

func aggregateCapacitySeries(rows []capacityMinuteRow, rejects map[int64]int64, from, to, bucket int64) ([]capacityPoint, capacitySummary) {
	byMinute := make(map[int64]capacityMinuteRow, len(rows))
	for _, row := range rows {
		byMinute[row.Ts] = row
	}
	points := map[int64]*capacityPoint{}
	runtimeByBucket := map[int64]int64{}
	successByBucket := map[int64]int64{}
	histByBucket := map[int64][7]int64{}
	var summary capacitySummary
	var totalSuccess int64
	var latestTs int64
	for _, row := range rows {
		key := row.Ts / bucket * bucket
		p := points[key]
		if p == nil {
			mins := bucket / 60
			if key < from {
				mins -= (from - key + 59) / 60
			}
			if key+bucket > to {
				mins -= (key + bucket - to + 59) / 60
			}
			if mins < 1 {
				mins = 1
			}
			p = &capacityPoint{Ts: key, DurationMinutes: mins}
			points[key] = p
		}
		logged := row.Success + row.Anomaly + row.Failed
		rejected := rejects[row.Ts]
		p.ObservedTrafficMins++
		p.LoggedRequests += logged
		p.RejectedRequests += rejected
		p.BusinessRequests += logged + rejected
		p.Tokens += row.Tokens
		runtimeByBucket[key] += row.SumUseTime
		successByBucket[key] += row.Success
		hist := histByBucket[key]
		hist[0] += row.Lat1
		hist[1] += row.Lat2
		hist[2] += row.Lat5
		hist[3] += row.Lat10
		hist[4] += row.Lat30
		hist[5] += row.Lat60
		hist[6] += row.LatInf
		histByBucket[key] = hist
		totalSuccess += row.Success
		summary.LoggedRequests += logged
		summary.RejectedRequests += rejected
		summary.BusinessRequests += logged + rejected
		summary.Tokens += row.Tokens

		minuteRPM := float64(logged + rejected)
		minuteTPM := float64(row.Tokens)
		minuteConcurrency := float64(row.SumUseTime) / 60
		if summary.PeakBusinessRPM == nil || minuteRPM > *summary.PeakBusinessRPM {
			summary.PeakBusinessRPM, summary.PeakAt = capacityFloatPtr(minuteRPM), row.Ts
		}
		if summary.PeakTPM == nil || minuteTPM > *summary.PeakTPM {
			summary.PeakTPM = capacityFloatPtr(minuteTPM)
		}
		if summary.PeakConcurrency == nil || minuteConcurrency > *summary.PeakConcurrency {
			summary.PeakConcurrency = capacityFloatPtr(minuteConcurrency)
		}
		if row.Ts >= latestTs {
			latestTs = row.Ts
			summary.CurrentAt = row.Ts
			summary.CurrentBusinessRPM = capacityFloatPtr(minuteRPM)
			summary.CurrentTPM = capacityFloatPtr(minuteTPM)
		}
	}
	// 拒绝分钟可能没有日志行；单独保留计数，但不伪造该分钟的完整业务 RPM。
	for ts, n := range rejects {
		if _, ok := byMinute[ts]; ok {
			continue
		}
		key := ts / bucket * bucket
		p := points[key]
		if p == nil {
			p = &capacityPoint{Ts: key, DurationMinutes: bucket / 60}
			points[key] = p
		}
		p.RejectedRequests += n
		p.BusinessRequests += n
		summary.RejectedRequests += n
		summary.BusinessRequests += n
		minuteRPM := float64(n)
		if summary.PeakBusinessRPM == nil || minuteRPM > *summary.PeakBusinessRPM {
			summary.PeakBusinessRPM, summary.PeakAt = capacityFloatPtr(minuteRPM), ts
		}
		if ts > latestTs {
			latestTs = ts
			summary.CurrentAt = ts
			summary.CurrentBusinessRPM = capacityFloatPtr(minuteRPM)
			summary.CurrentTPM = nil // 该分钟只有路由前拒绝，不伪造 token 事实。
		}
	}
	keys := make([]int64, 0, len(points))
	for key := range points {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool { return keys[i] < keys[j] })
	out := make([]capacityPoint, 0, len(keys))
	for _, key := range keys {
		p := points[key]
		mins := float64(p.DurationMinutes)
		if p.ObservedTrafficMins > 0 {
			p.LoggedRPM = capacityFloatPtr(float64(p.LoggedRequests) / mins)
			p.RejectedRPM = capacityFloatPtr(float64(p.RejectedRequests) / mins)
			p.BusinessRPM = capacityFloatPtr(float64(p.BusinessRequests) / mins)
			p.TPM = capacityFloatPtr(float64(p.Tokens) / mins)
			// 粗粒度并发是窗口内累计处理秒数 / 窗口秒数。
			p.EstimatedConcurrency = capacityFloatPtr(float64(runtimeByBucket[key]) / (mins * 60))
			if p.BusinessRequests > 0 {
				p.StabilityPct = capacityFloatPtr(float64(successByBucket[key]) * 100 / float64(p.BusinessRequests))
			}
			if v, ok := histogramP95(histByBucket[key]); ok {
				p.P95Seconds = capacityFloatPtr(v)
			}
		} else if p.RejectedRequests > 0 {
			// 路由前拒绝本身是已观测的业务请求；只有日志 RPM/TPM
			// 保持 null，避免把未进入日志误画成 0 token 请求。
			p.RejectedRPM = capacityFloatPtr(float64(p.RejectedRequests) / mins)
			p.BusinessRPM = capacityFloatPtr(float64(p.BusinessRequests) / mins)
			p.StabilityPct = capacityFloatPtr(0)
		}
		out = append(out, *p)
	}
	if summary.BusinessRequests > 0 {
		summary.StabilityPct = capacityFloatPtr(float64(totalSuccess) * 100 / float64(summary.BusinessRequests))
	}
	return out, summary
}

func histogramP95(hist [7]int64) (float64, bool) {
	var total int64
	for _, n := range hist {
		total += n
	}
	if total == 0 {
		return 0, false
	}
	target := int64(math.Ceil(float64(total) * .95))
	limits := [...]float64{1, 2, 5, 10, 30, 60, 60}
	var cumulative int64
	for i, n := range hist {
		cumulative += n
		if cumulative >= target {
			return limits[i], true
		}
	}
	return 60, true
}

func (m *Monitor) capacityBreakdowns(rows []capacityDimensionRow, rejectionRows []capacityRejectionDimensionRow, durationSec int64, channelID int, group, model string, rejectionsIncluded bool) (map[string][]capacityBreakdown, map[string][]capacityOption) {
	type agg struct{ success, anomaly, failed, rejected, tokens int64 }
	groups, models, channels := map[string]*agg{}, map[string]*agg{}, map[string]*agg{}
	optionSets := map[string]map[string]bool{"groups": {}, "models": {}, "channels": {}}
	labelFor := func(key string) string {
		if strings.TrimSpace(key) == "" {
			return "未标记"
		}
		return key
	}
	add := func(dst map[string]*agg, key string, row capacityDimensionRow) {
		a := dst[key]
		if a == nil {
			a = &agg{}
			dst[key] = a
		}
		a.success += row.Success
		a.anomaly += row.Anomaly
		a.failed += row.Failed
		a.tokens += row.Tokens
	}
	for _, row := range rows {
		groupKey, modelKey, channelKey := row.Grp, row.ModelName, strconv.Itoa(row.ChannelID)
		optionSets["groups"][groupKey] = true
		optionSets["models"][modelKey] = true
		optionSets["channels"][channelKey] = true
		if (channelID != 0 && row.ChannelID != capacityActualChannelID(channelID)) ||
			(group != "" && row.Grp != capacityRawDimensionValue(group)) ||
			(model != "" && row.ModelName != capacityRawDimensionValue(model)) {
			continue
		}
		add(groups, row.Grp, row)
		add(models, row.ModelName, row)
		add(channels, channelKey, row)
	}
	if rejectionsIncluded {
		for _, row := range rejectionRows {
			groupKey, modelKey := row.Grp, row.ModelName
			optionSets["groups"][groupKey] = true
			optionSets["models"][modelKey] = true
			if (group != "" && row.Grp != capacityRawDimensionValue(group)) ||
				(model != "" && row.ModelName != capacityRawDimensionValue(model)) {
				continue
			}
			for _, item := range []struct {
				dst map[string]*agg
				key string
			}{{groups, groupKey}, {models, modelKey}} {
				a := item.dst[item.key]
				if a == nil {
					a = &agg{}
					item.dst[item.key] = a
				}
				a.rejected += row.Count
			}
		}
	}
	channelNames := map[string]string{"0": "未分配渠道"}
	var snaps []ChannelSnap
	if m.storeDB != nil {
		_ = m.storeDB.Select("id,name").Find(&snaps).Error
	}
	for _, snap := range snaps {
		channelNames[strconv.Itoa(snap.ID)] = fmt.Sprintf("#%d %s", snap.ID, snap.Name)
	}
	convert := func(src map[string]*agg, labels map[string]string, scope string) []capacityBreakdown {
		mins := math.Max(1, float64(durationSec)/60)
		out := make([]capacityBreakdown, 0, len(src))
		for key, a := range src {
			requests := a.success + a.anomaly + a.failed + a.rejected
			label := key
			if labels == nil {
				label = labelFor(key)
			}
			if labels != nil && labels[key] != "" {
				label = labels[key]
			}
			item := capacityBreakdown{Key: key, Label: label, Requests: requests, RejectedRequests: a.rejected, Tokens: a.tokens,
				AverageRPM: float64(requests) / mins, AverageTPM: float64(a.tokens) / mins, StabilityScope: scope}
			if requests > 0 {
				item.StabilityPct = capacityFloatPtr(float64(a.success) * 100 / float64(requests))
			}
			out = append(out, item)
		}
		sort.Slice(out, func(i, j int) bool { return out[i].Requests > out[j].Requests })
		if len(out) > 20 {
			out = out[:20]
		}
		return out
	}
	dimensionScope := "log_only"
	if rejectionsIncluded {
		dimensionScope = "log_plus_pre_route"
	}
	breakdowns := map[string][]capacityBreakdown{
		"groups": convert(groups, nil, dimensionScope), "models": convert(models, nil, dimensionScope), "channels": convert(channels, channelNames, "log_only"),
	}
	options := map[string][]capacityOption{}
	for dimension, set := range optionSets {
		for key := range set {
			label, optionKey := labelFor(key), key
			if dimension == "channels" && channelNames[key] != "" {
				label = channelNames[key]
			} else if dimension != "channels" {
				optionKey = capacityDimensionKey(key)
			}
			options[dimension] = append(options[dimension], capacityOption{Key: optionKey, Label: label})
		}
		sort.Slice(options[dimension], func(i, j int) bool { return options[dimension][i].Label < options[dimension][j].Label })
	}
	return breakdowns, options
}

func (m *Monitor) readCapacityIngress(ctx context.Context, from, to, bucket, now int64) ([]capacityIngressPoint, capacitySource) {
	var rows []capacityIngressRow
	err := m.storeDB.WithContext(ctx).Raw(`SELECT (bucket_ts / ?) * ? ts, node, SUM(count) count,
		SUM(CASE WHEN status >= 500 THEN count ELSE 0 END) error_5xx,
		SUM(CASE WHEN upstream_status >= 500 THEN count ELSE 0 END) upstream_error_5xx,
		SUM(request_time_sum_ms) request_time_sum_ms, MAX(request_time_max_ms) request_time_max_ms,
		SUM(upstream_time_sum_ms) upstream_time_sum_ms, SUM(upstream_time_count) upstream_time_count
		FROM nginx_minute_samples WHERE bucket_ts >= ? AND bucket_ts < ? AND method='POST'
		AND route IN ('/v1/chat/completions','/v1/responses','/v1/messages','/v1/*')
		GROUP BY ts,node ORDER BY ts,node`, bucket, bucket, from, to).Scan(&rows).Error
	var watermark, total int64
	out := make([]capacityIngressPoint, 0, len(rows))
	if err == nil {
		for _, row := range rows {
			mins := float64(bucket / 60)
			if mins < 1 {
				mins = 1
			}
			avg := float64(0)
			avgUpstream := float64(0)
			if row.Count > 0 {
				avg = float64(row.RequestTimeSumMS) / float64(row.Count)
			}
			if row.UpstreamTimeCount > 0 {
				avgUpstream = float64(row.UpstreamTimeSumMS) / float64(row.UpstreamTimeCount)
			}
			windowMS := float64(bucket * 1000)
			out = append(out, capacityIngressPoint{Ts: row.Ts, Node: row.Node, Requests: row.Count, RPM: float64(row.Count) / mins,
				Error5xx: row.Error5xx, UpstreamError5xx: row.UpstreamError5xx, AverageLatency: avg, AverageUpstreamLatency: avgUpstream,
				MaxLatency: float64(row.RequestTimeMaxMS), EstimatedInflight: float64(row.RequestTimeSumMS) / windowMS,
				EstimatedUpstreamInflight: float64(row.UpstreamTimeSumMS) / windowMS})
			total += row.Count
		}
		_ = m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(MAX(bucket_ts),0) FROM nginx_minute_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? AND method='POST'
			AND route IN ('/v1/chat/completions','/v1/responses','/v1/messages','/v1/*')`, from, to).Scan(&watermark).Error
	}
	configured := m.cfg.NginxEnabled
	note := "入口 RPM 只包含 POST 推理路由；与日志 RPM 是两套独立证据。"
	if err != nil {
		note = "Nginx 本地事实不可用；业务 RPM/TPM 仍可用。"
	}
	return out, capacitySource{Available: err == nil && watermark > 0, Configured: configured, Watermark: watermark, AgeSec: capacityAge(now, watermark), Rows: total, Note: note}
}

func (m *Monitor) readCapacityInfra(ctx context.Context, from, to, bucket, now int64) ([]capacityInfraPoint, capacitySource) {
	m.infraAggregateMu.Lock()
	defer m.infraAggregateMu.Unlock()

	// 页面首版只返回用于判断容量瓶颈的四条时序；内存/存储/容器等
	// 当前值由 components 快照提供。这个白名单把 7 天响应体限制在可预期范围。
	allowed := []string{"cpu", "connections", "disk_queue", "resp_ms"}
	marks := strings.TrimRight(strings.Repeat("?,", len(allowed)), ",")
	args := []any{bucket, bucket, from, to}
	for _, v := range allowed {
		args = append(args, v)
	}
	var rows []capacityInfraRow
	err := m.storeDB.WithContext(ctx).Raw(`SELECT (bucket_ts / ?) * ? ts, resource, rtype, metric, AVG(value) value
		FROM infra_samples WHERE bucket_ts >= ? AND bucket_ts < ? AND metric IN (`+marks+`)
		GROUP BY ts,resource,rtype,metric ORDER BY ts,resource,metric`, args...).Scan(&rows).Error
	var watermark int64
	out := make([]capacityInfraPoint, 0, len(rows))
	if err == nil {
		for _, row := range rows {
			out = append(out, capacityInfraPoint(row))
		}
		watermarkArgs := []any{from, to}
		for _, v := range allowed {
			watermarkArgs = append(watermarkArgs, v)
		}
		_ = m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(MAX(bucket_ts),0) FROM infra_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? AND metric IN (`+marks+`)`, watermarkArgs...).Scan(&watermark).Error
	}
	note := "资源曲线与流量同轴用于相关性观察，不声称因果；日志事实暂无服务节点维度。"
	if err != nil {
		note = "基础设施本地事实不可用；不影响业务 RPM/TPM。"
	}
	return out, capacitySource{Available: err == nil && watermark > 0, Configured: m.InfraEnabled(), Watermark: watermark, AgeSec: capacityAge(now, watermark), Rows: int64(len(rows)), Note: note}
}
