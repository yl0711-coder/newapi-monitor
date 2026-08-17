package monitor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

var cstLocation = time.FixedZone("CST", 8*60*60)

// newAPIChannelTypeNames mirrors NewAPI constant.ChannelTypeNames. Keeping the
// numeric mapping here avoids importing the whole gateway module into Monitor.
// Unknown future types remain explicitly unlabelled instead of being guessed
// from the channel name.
var newAPIChannelTypeNames = map[int]string{
	0: "未标记", 1: "OpenAI", 2: "Midjourney", 3: "Azure", 4: "Ollama",
	5: "MidjourneyPlus", 6: "OpenAIMax", 7: "OhMyGPT", 8: "Custom",
	9: "AILS", 10: "AIProxy", 11: "PaLM", 12: "API2GPT", 13: "AIGC2D",
	14: "Anthropic", 15: "Baidu", 16: "Zhipu", 17: "Ali", 18: "Xunfei",
	19: "360", 20: "OpenRouter", 21: "AIProxyLibrary", 22: "FastGPT",
	23: "Tencent", 24: "Gemini", 25: "Moonshot", 26: "ZhipuV4",
	27: "Perplexity", 31: "LingYiWanWu", 33: "AWS", 34: "Cohere",
	35: "MiniMax", 36: "SunoAPI", 37: "Dify", 38: "Jina", 39: "Cloudflare",
	40: "SiliconFlow", 41: "VertexAI", 42: "Mistral", 43: "DeepSeek",
	44: "MokaAI", 45: "VolcEngine", 46: "BaiduV2", 47: "Xinference",
	48: "xAI", 49: "Coze", 50: "Kling", 51: "Jimeng", 52: "Vidu",
	53: "Submodel", 54: "DoubaoVideo", 55: "Sora", 56: "Replicate", 57: "Codex",
}

func newAPIChannelTypeName(channelType int) string {
	if name := newAPIChannelTypeNames[channelType]; name != "" {
		return name
	}
	return "未标记"
}

type stabilityScope struct {
	FromTs     int64
	ToTs       int64
	RangeHours int
	Group      string
	ChannelID  int
	Model      string
	Vendor     string
}

type StabilityMetrics struct {
	Requests       int64    `json:"requests"`
	Success        int64    `json:"success"`
	Anomaly        int64    `json:"anomaly"`
	Failed         int64    `json:"failed"`
	Rejected       int64    `json:"rejected"`
	Problems       int64    `json:"problems"`
	Stability      *float64 `json:"stability"`
	ProblemRate    *float64 `json:"problem_rate"`
	Tokens         int64    `json:"tokens"`
	CostUSD        float64  `json:"cost_usd"`
	AvgLatencySec  *float64 `json:"avg_latency_sec"`
	MaxLatencySec  int      `json:"max_latency_sec"`
	AnomalyBilled  int64    `json:"anomaly_billed"`
	AnomalyFree    int64    `json:"anomaly_free"`
	AnomalyStream  int64    `json:"anomaly_stream"`
	AnomalyCostUSD float64  `json:"anomaly_cost_usd"`
	Err4xx         int64    `json:"err_4xx"`
	Err5xx         int64    `json:"err_5xx"`
	ErrTimeout     int64    `json:"err_timeout"`
	ErrOther       int64    `json:"err_other"`
	Health         string   `json:"health"`
}

type stabilityCounts struct {
	Success, Anomaly, Failed, Rejected                      int64
	AnomalyBilled, AnomalyFree, AnomalyStream, AnomalyQuota int64
	SumUseTime, Tokens, Quota                               int64
	MaxUseTime                                              int
	Err4xx, Err5xx, ErrTimeout, ErrOther                    int64
}

func (c *stabilityCounts) add(o stabilityCounts) {
	c.Success += o.Success
	c.Anomaly += o.Anomaly
	c.Failed += o.Failed
	c.Rejected += o.Rejected
	c.AnomalyBilled += o.AnomalyBilled
	c.AnomalyFree += o.AnomalyFree
	c.AnomalyStream += o.AnomalyStream
	c.AnomalyQuota += o.AnomalyQuota
	c.SumUseTime += o.SumUseTime
	c.Tokens += o.Tokens
	c.Quota += o.Quota
	if o.MaxUseTime > c.MaxUseTime {
		c.MaxUseTime = o.MaxUseTime
	}
	c.Err4xx += o.Err4xx
	c.Err5xx += o.Err5xx
	c.ErrTimeout += o.ErrTimeout
	c.ErrOther += o.ErrOther
}

func floatPtr(v float64) *float64 { return &v }

func (c stabilityCounts) metrics() StabilityMetrics {
	requests := c.Success + c.Anomaly + c.Failed + c.Rejected
	problems := c.Anomaly + c.Failed + c.Rejected
	m := StabilityMetrics{
		Requests: requests, Success: c.Success, Anomaly: c.Anomaly, Failed: c.Failed,
		Rejected: c.Rejected, Problems: problems, Tokens: c.Tokens,
		CostUSD: float64(c.Quota) / quotaPerUSD, MaxLatencySec: c.MaxUseTime,
		AnomalyBilled: c.AnomalyBilled, AnomalyFree: c.AnomalyFree, AnomalyStream: c.AnomalyStream,
		AnomalyCostUSD: float64(c.AnomalyQuota) / quotaPerUSD,
		Err4xx:         c.Err4xx, Err5xx: c.Err5xx, ErrTimeout: c.ErrTimeout, ErrOther: c.ErrOther,
	}
	if requests > 0 {
		m.Stability = floatPtr(rate(c.Success, requests))
		m.ProblemRate = floatPtr(rate(problems, requests))
		m.Health = health(requests, *m.Stability)
	} else {
		m.Health = "nosample"
	}
	// use_time 来自 type=2，错误/前置拒绝没有可比较的完整耗时。
	if delivered := c.Success + c.Anomaly; delivered > 0 {
		m.AvgLatencySec = floatPtr(float64(c.SumUseTime) / float64(delivered))
	}
	return m
}

type StabilityDay struct {
	Date string `json:"date"`
	StabilityMetrics
}

// StabilityStripPoint 是分组/渠道状态窄条的紧凑数据。它故意不带费用、耗时
// 等大字段，避免 90 个时间桶 × 多个分组/渠道时放大 JSON 和浏览器内存。
type StabilityStripPoint struct {
	Ts        int64    `json:"ts"`
	Requests  int64    `json:"requests"`
	Problems  int64    `json:"problems"`
	Stability *float64 `json:"stability"`
}

type StabilityModel struct {
	Name     string   `json:"name"`
	SharePct float64  `json:"share_pct"`
	DeltaPP  *float64 `json:"delta_pp"`
	StabilityMetrics
}

type StabilityChannel struct {
	ID              int                   `json:"id"`
	Name            string                `json:"name"`
	Vendor          string                `json:"vendor"`
	Status          int                   `json:"status"`
	Current         bool                  `json:"current"`
	SharePct        float64               `json:"share_pct"`
	ProblemSharePct float64               `json:"problem_share_pct"`
	DeltaPP         *float64              `json:"delta_pp"`
	Models          []StabilityModel      `json:"models"`
	ModelCount      int                   `json:"model_count"`
	Daily           []StabilityDay        `json:"daily"`
	Timeline        []StabilityStripPoint `json:"timeline"`
	StabilityMetrics
}

type StabilityGroup struct {
	Name       string                `json:"name"`
	Vendor     string                `json:"vendor"`
	SharePct   float64               `json:"share_pct"`
	DeltaPP    *float64              `json:"delta_pp"`
	Channels   []StabilityChannel    `json:"channels"`
	Models     []StabilityModel      `json:"models"`
	ModelCount int                   `json:"model_count"`
	Daily      []StabilityDay        `json:"daily"`
	Timeline   []StabilityStripPoint `json:"timeline"`
	StabilityMetrics
}

type StabilityFilterChannel struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Vendor string `json:"vendor"`
}

type StabilityFilters struct {
	Vendors  []string                 `json:"vendors"`
	Groups   []string                 `json:"groups"`
	Channels []StabilityFilterChannel `json:"channels"`
	Models   []string                 `json:"models"`
}

type StabilitySourceStatus struct {
	NewAPILastTs              int64                             `json:"newapi_last_ts"`
	NewAPIDataAgeSec          int64                             `json:"newapi_data_age_sec"`
	ProblemLastTs             int64                             `json:"problem_last_ts"`
	ProblemCoverageTo         int64                             `json:"problem_coverage_to"`
	ProblemCoverageLagSec     int64                             `json:"problem_coverage_lag_sec"`
	ProblemPendingMinutes     int64                             `json:"problem_pending_minutes"`
	ProblemSamplerLastSuccess int64                             `json:"problem_sampler_last_success"`
	ProblemSamplerLastFailure int64                             `json:"problem_sampler_last_failure"`
	ProblemMigration          stabilityProblemMigrationProgress `json:"problem_migration"`
	NginxEnabled              bool                              `json:"nginx_enabled"`
	NginxStatus               string                            `json:"nginx_status"`
	NginxConnected            bool                              `json:"nginx_connected"`
	NginxHealthySources       int                               `json:"nginx_healthy_sources"`
	NginxSourceCount          int                               `json:"nginx_source_count"`
	NginxLastTs               int64                             `json:"nginx_last_ts"`
	RequestIDCoverage         *float64                          `json:"request_id_coverage"`
}

type StabilityReportMeta struct {
	From                string                `json:"from"`
	To                  string                `json:"to"`
	GeneratedAt         int64                 `json:"generated_at"`
	FirstDataTs         int64                 `json:"first_data_ts"`
	LastDataTs          int64                 `json:"last_data_ts"`
	RetentionDays       int                   `json:"retention_days"`
	RowsTruncated       bool                  `json:"rows_truncated"`
	ComparisonAvailable bool                  `json:"comparison_available"`
	ComparisonCoverage  StabilityDataCoverage `json:"comparison_coverage"`
	TimelineBucketSec   int64                 `json:"timeline_bucket_sec"`
	DataCoverage        StabilityDataCoverage `json:"data_coverage"`
	Sources             StabilitySourceStatus `json:"sources"`
}

type StabilityRankItem struct {
	ID       int      `json:"id,omitempty"`
	Name     string   `json:"name"`
	Vendor   string   `json:"vendor,omitempty"`
	SharePct float64  `json:"share_pct"`
	DeltaPP  *float64 `json:"delta_pp"`
	StabilityMetrics
}

type StabilityRankings struct {
	Groups   []StabilityRankItem `json:"groups"`
	Channels []StabilityRankItem `json:"channels"`
	Models   []StabilityRankItem `json:"models"`
}

type StabilityReport struct {
	Enabled  bool                `json:"enabled"`
	Meta     StabilityReportMeta `json:"meta"`
	Filters  StabilityFilters    `json:"filters"`
	Summary  StabilityMetrics    `json:"summary"`
	Previous StabilityMetrics    `json:"previous"`
	DeltaPP  *float64            `json:"delta_pp"`
	Groups   []StabilityGroup    `json:"groups"`
	Rankings StabilityRankings   `json:"rankings"`
}

type stabilityDimRow struct {
	Grp, ModelName, ChannelName, Vendor                     string
	ChannelID                                               int
	Status                                                  int
	Current                                                 bool
	Success, Anomaly, Failed                                int64
	AnomalyBilled, AnomalyFree, AnomalyStream, AnomalyQuota int64
	SumUseTime, Tokens, Quota                               int64
	MaxUseTime                                              int
	Err4xx, Err5xx, ErrTimeout, ErrOther                    int64
}

func (r stabilityDimRow) counts() stabilityCounts {
	return stabilityCounts{
		Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed,
		AnomalyBilled: r.AnomalyBilled, AnomalyFree: r.AnomalyFree,
		AnomalyStream: r.AnomalyStream, AnomalyQuota: r.AnomalyQuota,
		SumUseTime: r.SumUseTime, MaxUseTime: r.MaxUseTime, Tokens: r.Tokens, Quota: r.Quota,
		Err4xx: r.Err4xx, Err5xx: r.Err5xx, ErrTimeout: r.ErrTimeout, ErrOther: r.ErrOther,
	}
}

type stabilityRejectRow struct {
	Grp, Model string
	Count      int64
}

type stabilityDailyRow struct {
	Day, Grp                                                string
	ChannelID                                               int
	Success, Anomaly, Failed                                int64
	AnomalyBilled, AnomalyFree, AnomalyStream, AnomalyQuota int64
	SumUseTime, Tokens, Quota                               int64
	MaxUseTime                                              int
	Err4xx, Err5xx, ErrTimeout, ErrOther                    int64
}

type stabilityTimelineRow struct {
	BucketTs                                                int64
	Grp                                                     string
	ChannelID                                               int
	Success, Anomaly, Failed                                int64
	AnomalyBilled, AnomalyFree, AnomalyStream, AnomalyQuota int64
	SumUseTime, Tokens, Quota                               int64
	MaxUseTime                                              int
	Err4xx, Err5xx, ErrTimeout, ErrOther                    int64
}

func (r stabilityTimelineRow) counts() stabilityCounts {
	return stabilityCounts{
		Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed,
		AnomalyBilled: r.AnomalyBilled, AnomalyFree: r.AnomalyFree,
		AnomalyStream: r.AnomalyStream, AnomalyQuota: r.AnomalyQuota,
		SumUseTime: r.SumUseTime, MaxUseTime: r.MaxUseTime, Tokens: r.Tokens, Quota: r.Quota,
		Err4xx: r.Err4xx, Err5xx: r.Err5xx, ErrTimeout: r.ErrTimeout, ErrOther: r.ErrOther,
	}
}

func (r stabilityDailyRow) counts() stabilityCounts {
	return stabilityCounts{
		Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed,
		AnomalyBilled: r.AnomalyBilled, AnomalyFree: r.AnomalyFree,
		AnomalyStream: r.AnomalyStream, AnomalyQuota: r.AnomalyQuota,
		SumUseTime: r.SumUseTime, MaxUseTime: r.MaxUseTime, Tokens: r.Tokens, Quota: r.Quota,
		Err4xx: r.Err4xx, Err5xx: r.Err5xx, ErrTimeout: r.ErrTimeout, ErrOther: r.ErrOther,
	}
}

const stabilityAggColumns = `
	COALESCE(SUM(sh.success),0) AS success,
	COALESCE(SUM(sh.anomaly),0) AS anomaly,
	COALESCE(SUM(sh.failed),0) AS failed,
	COALESCE(SUM(sh.anomaly_billed),0) AS anomaly_billed,
	COALESCE(SUM(sh.anomaly_free),0) AS anomaly_free,
	COALESCE(SUM(sh.anomaly_stream),0) AS anomaly_stream,
	COALESCE(SUM(sh.anomaly_quota),0) AS anomaly_quota,
	COALESCE(SUM(sh.sum_use_time),0) AS sum_use_time,
	COALESCE(MAX(sh.max_use_time),0) AS max_use_time,
	COALESCE(SUM(sh.tokens),0) AS tokens,
	COALESCE(SUM(sh.quota),0) AS quota,
	COALESCE(SUM(sh.err_4xx),0) AS err_4xx,
	COALESCE(SUM(sh.err_5xx),0) AS err_5xx,
	COALESCE(SUM(sh.err_timeout),0) AS err_timeout,
	COALESCE(SUM(sh.err_other),0) AS err_other`

func (s stabilityScope) sqlWhere(alias string) (string, []any) {
	where := " WHERE " + alias + ".hour_ts >= ? AND " + alias + ".hour_ts < ? AND " + alias + ".traffic_class_version = ?"
	args := []any{s.FromTs, s.ToTs, userTrafficClassificationVersion}
	if s.Group != "" {
		where += " AND " + alias + ".grp = ?"
		args = append(args, s.Group)
	}
	if s.ChannelID > 0 {
		where += " AND " + alias + ".channel_id = ?"
		args = append(args, s.ChannelID)
	}
	if s.Model != "" {
		where += " AND " + alias + ".model_name = ?"
		args = append(args, s.Model)
	}
	if s.Vendor != "" {
		where += " AND COALESCE(cs.vendor,'') = ?"
		args = append(args, s.Vendor)
	}
	return where, args
}

func (m *Monitor) queryStabilityDims(ctx context.Context, scope stabilityScope, limit int) ([]stabilityDimRow, bool, error) {
	where, args := scope.sqlWhere("sh")
	q := `SELECT sh.grp, sh.channel_id, sh.model_name,
		COALESCE(cs.name,'') AS channel_name, COALESCE(cs.vendor,'') AS vendor, COALESCE(cs.status,0) AS status,
		CASE WHEN cs.id IS NOT NULL AND COALESCE(cs.deleted_at,0)=0 THEN 1 ELSE 0 END AS current,` +
		stabilityAggColumns + `
		FROM stability_hour_samples sh LEFT JOIN channel_snaps cs ON cs.id = sh.channel_id` + where + `
		GROUP BY sh.grp, sh.channel_id, sh.model_name, cs.name, cs.vendor, cs.status
		ORDER BY (SUM(sh.success)+SUM(sh.anomaly)+SUM(sh.failed)) DESC LIMIT ?`
	args = append(args, limit+1)
	var rows []stabilityDimRow
	if err := m.storeDB.WithContext(ctx).Raw(q, args...).Scan(&rows).Error; err != nil {
		return nil, false, err
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	return rows, truncated, nil
}

func (m *Monitor) queryStabilityRejects(ctx context.Context, scope stabilityScope) ([]stabilityRejectRow, error) {
	// 前置拒绝没有真实渠道和厂商，选择这两类筛选时必须排除，不能虚构归属。
	if scope.ChannelID > 0 || scope.Vendor != "" {
		return nil, nil
	}
	where := " WHERE hour_ts >= ? AND hour_ts < ?"
	args := []any{scope.FromTs, scope.ToTs}
	if scope.Group != "" {
		where += " AND grp = ?"
		args = append(args, scope.Group)
	}
	if scope.Model != "" {
		where += " AND model = ?"
		args = append(args, scope.Model)
	}
	var rows []stabilityRejectRow
	err := m.storeDB.WithContext(ctx).Raw(`SELECT grp, model, COALESCE(SUM(count),0) AS count FROM stability_reject_hours`+
		where+` GROUP BY grp, model`, args...).Scan(&rows).Error
	return rows, err
}

func (m *Monitor) queryStabilityDaily(ctx context.Context, scope stabilityScope, includeChannels bool, limit int) ([]stabilityDailyRow, bool, error) {
	where, args := scope.sqlWhere("sh")
	dimensions := "sh.grp, sh.channel_id,"
	groupBy := "day, sh.grp, sh.channel_id"
	orderBy := "day, sh.grp, sh.channel_id"
	if !includeChannels {
		dimensions = "sh.grp, 0 AS channel_id,"
		groupBy = "day, sh.grp"
		orderBy = "day, sh.grp"
	}
	q := `SELECT strftime('%Y-%m-%d', sh.hour_ts, 'unixepoch', '+8 hours') AS day,
		` + dimensions + stabilityAggColumns + `
		FROM stability_hour_samples sh LEFT JOIN channel_snaps cs ON cs.id = sh.channel_id` + where + `
		GROUP BY ` + groupBy + ` ORDER BY ` + orderBy + ` LIMIT ?`
	args = append(args, limit+1)
	var rows []stabilityDailyRow
	if err := m.storeDB.WithContext(ctx).Raw(q, args...).Scan(&rows).Error; err != nil {
		return nil, false, err
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	return rows, truncated, nil
}

func (m *Monitor) queryStabilityRejectDaily(ctx context.Context, scope stabilityScope) (map[string]map[string]int64, error) {
	out := map[string]map[string]int64{}
	if scope.ChannelID > 0 || scope.Vendor != "" {
		return out, nil
	}
	where := " WHERE hour_ts >= ? AND hour_ts < ?"
	args := []any{scope.FromTs, scope.ToTs}
	if scope.Group != "" {
		where += " AND grp = ?"
		args = append(args, scope.Group)
	}
	if scope.Model != "" {
		where += " AND model = ?"
		args = append(args, scope.Model)
	}
	var rows []struct {
		Day, Grp string
		Count    int64
	}
	err := m.storeDB.WithContext(ctx).Raw(`SELECT strftime('%Y-%m-%d', hour_ts, 'unixepoch', '+8 hours') AS day,
		grp, COALESCE(SUM(count),0) AS count FROM stability_reject_hours`+where+` GROUP BY day, grp`, args...).Scan(&rows).Error
	for _, r := range rows {
		if out[r.Grp] == nil {
			out[r.Grp] = map[string]int64{}
		}
		out[r.Grp][r.Day] += r.Count
	}
	return out, err
}

func stabilityTimelineBucketSec(scope stabilityScope) int64 {
	hours := (scope.ToTs - scope.FromTs + 3599) / 3600
	if hours < 1 {
		hours = 1
	}
	// 目标约 90 个窄条；源表本身是小时粒度，因此最小一小时。
	stepHours := (hours + 89) / 90
	if stepHours < 1 {
		stepHours = 1
	}
	return stepHours * 3600
}

func stabilityBucketStart(ts, step int64) int64 {
	// 加 8 小时后分桶再减回，使大于一小时的桶在东八区自然日内对齐。
	const cstOffset = int64(8 * 3600)
	return ((ts+cstOffset)/step)*step - cstOffset
}

func stabilityBucketKeys(scope stabilityScope, step int64) []int64 {
	start := stabilityBucketStart(scope.FromTs, step)
	out := make([]int64, 0, 96)
	for ts := start; ts < scope.ToTs; ts += step {
		out = append(out, ts)
	}
	return out
}

func buildTimeline(keys []int64, rows map[int64]stabilityCounts) []StabilityStripPoint {
	out := make([]StabilityStripPoint, 0, len(keys))
	for _, ts := range keys {
		m := rows[ts].metrics()
		out = append(out, StabilityStripPoint{Ts: ts, Requests: m.Requests, Problems: m.Problems, Stability: m.Stability})
	}
	return out
}

func (m *Monitor) queryStabilityTimeline(ctx context.Context, scope stabilityScope, step int64, includeChannels bool, limit int) ([]stabilityTimelineRow, bool, error) {
	where, args := scope.sqlWhere("sh")
	dimensions := "sh.grp, sh.channel_id,"
	groupBy := "bucket_ts, sh.grp, sh.channel_id"
	orderBy := "bucket_ts, sh.grp, sh.channel_id"
	if !includeChannels {
		dimensions = "sh.grp, 0 AS channel_id,"
		groupBy = "bucket_ts, sh.grp"
		orderBy = "bucket_ts, sh.grp"
	}
	q := `SELECT (((sh.hour_ts + 28800) / ?) * ? - 28800) AS bucket_ts,
		` + dimensions + stabilityAggColumns + `
		FROM stability_hour_samples sh LEFT JOIN channel_snaps cs ON cs.id = sh.channel_id` + where + `
		GROUP BY ` + groupBy + ` ORDER BY ` + orderBy + ` LIMIT ?`
	qArgs := []any{step, step}
	qArgs = append(qArgs, args...)
	qArgs = append(qArgs, limit+1)
	var rows []stabilityTimelineRow
	if err := m.storeDB.WithContext(ctx).Raw(q, qArgs...).Scan(&rows).Error; err != nil {
		return nil, false, err
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	return rows, truncated, nil
}

func (m *Monitor) queryStabilityRejectTimeline(ctx context.Context, scope stabilityScope, step int64) (map[string]map[int64]int64, error) {
	out := map[string]map[int64]int64{}
	if scope.ChannelID > 0 || scope.Vendor != "" {
		return out, nil
	}
	where := " WHERE hour_ts >= ? AND hour_ts < ?"
	args := []any{step, step, scope.FromTs, scope.ToTs}
	if scope.Group != "" {
		where += " AND grp = ?"
		args = append(args, scope.Group)
	}
	if scope.Model != "" {
		where += " AND model = ?"
		args = append(args, scope.Model)
	}
	var rows []struct {
		BucketTs int64
		Grp      string
		Count    int64
	}
	err := m.storeDB.WithContext(ctx).Raw(`SELECT (((hour_ts + 28800) / ?) * ? - 28800) AS bucket_ts,
		grp, COALESCE(SUM(count),0) AS count FROM stability_reject_hours`+where+`
		GROUP BY bucket_ts, grp`, args...).Scan(&rows).Error
	for _, r := range rows {
		if out[r.Grp] == nil {
			out[r.Grp] = map[int64]int64{}
		}
		out[r.Grp][r.BucketTs] += r.Count
	}
	return out, err
}

type stabilityChannelBuild struct {
	ID           int
	Name, Vendor string
	Status       int
	Current      bool
	Counts       stabilityCounts
	Models       map[string]*stabilityCounts
}

type stabilityGroupBuild struct {
	Name     string
	Counts   stabilityCounts
	Vendors  map[string]bool
	Channels map[int]*stabilityChannelBuild
	Models   map[string]*stabilityCounts
}

func deltaPP(current, previous StabilityMetrics) *float64 {
	if current.Stability == nil || previous.Stability == nil {
		return nil
	}
	return floatPtr(*current.Stability - *previous.Stability)
}

func vendorLabel(set map[string]bool) string {
	delete(set, "")
	if len(set) == 0 {
		return "未标记"
	}
	if len(set) == 1 {
		for v := range set {
			return v
		}
	}
	return "多个厂商"
}

func dateKeys(fromTs, toTs int64) []string {
	start := time.Unix(fromTs, 0).In(cstLocation)
	start = time.Date(start.Year(), start.Month(), start.Day(), 0, 0, 0, 0, cstLocation)
	end := time.Unix(toTs-1, 0).In(cstLocation)
	end = time.Date(end.Year(), end.Month(), end.Day(), 0, 0, 0, 0, cstLocation)
	var out []string
	for d := start; !d.After(end); d = d.AddDate(0, 0, 1) {
		out = append(out, d.Format("2006-01-02"))
	}
	return out
}

func buildDaily(keys []string, rows map[string]stabilityCounts) []StabilityDay {
	out := make([]StabilityDay, 0, len(keys))
	for _, day := range keys {
		out = append(out, StabilityDay{Date: day, StabilityMetrics: rows[day].metrics()})
	}
	return out
}

func sortModels(rows []StabilityModel) {
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].Requests == rows[j].Requests {
			return rows[i].Name < rows[j].Name
		}
		return rows[i].Requests > rows[j].Requests
	})
}

func (m *Monitor) buildStabilityReport(ctx context.Context, scope stabilityScope, now int64) (*StabilityReport, error) {
	return m.buildStabilityReportWithDetails(ctx, scope, now, true)
}

// buildStabilityReportWithDetails 的轻量模式仍返回完整总览、分组趋势、渠道摘要和排行，
// 但不为每个渠道预生成每日/时间条/模型明细。详情按分组单独加载，避免 90 天首屏
// 随“分组×渠道×时间桶”膨胀到数 MB 并占用 256 MiB 容器的大量堆内存。
func (m *Monitor) buildStabilityReportWithDetails(ctx context.Context, scope stabilityScope, now int64, includeNested bool) (*StabilityReport, error) {
	const maxDimRows = 2000
	const maxDailyRows = 30000
	const maxTimelineRows = 30000
	rows, truncated, err := m.queryStabilityDims(ctx, scope, maxDimRows)
	if err != nil {
		return nil, err
	}
	if truncated {
		return nil, fmt.Errorf("当前范围超过 %d 个分组/渠道/模型组合，请缩小日期或增加筛选；为保证准确性未返回部分数据", maxDimRows)
	}
	rejects, err := m.queryStabilityRejects(ctx, scope)
	if err != nil {
		return nil, err
	}

	// 对比窗口按东八区自然日整体前移。默认范围包含“今天截至当前时刻”，
	// 不能直接用已过秒数作为周期长度，否则上周起点会落在中午而非同一时刻。
	periodDays := len(dateKeys(scope.FromTs, scope.ToTs))
	if periodDays < 1 {
		periodDays = 1
	}
	comparisonShift := int64(periodDays) * 86400
	previousScope := scope
	previousScope.FromTs = scope.FromTs - comparisonShift
	previousScope.ToTs = scope.ToTs - comparisonShift
	prevRows, prevTruncated, err := m.queryStabilityDims(ctx, previousScope, maxDimRows)
	if err != nil {
		return nil, err
	}
	if prevTruncated {
		return nil, fmt.Errorf("上一对比周期超过 %d 个分组/渠道/模型组合，请缩小日期或增加筛选", maxDimRows)
	}
	prevRejects, err := m.queryStabilityRejects(ctx, previousScope)
	if err != nil {
		return nil, err
	}

	groups := map[string]*stabilityGroupBuild{}
	modelTotals := map[string]*stabilityCounts{}
	channelTotals := map[int]*stabilityChannelBuild{}
	var total stabilityCounts
	ensureGroup := func(name string) *stabilityGroupBuild {
		g := groups[name]
		if g == nil {
			g = &stabilityGroupBuild{Name: name, Vendors: map[string]bool{}, Channels: map[int]*stabilityChannelBuild{}, Models: map[string]*stabilityCounts{}}
			groups[name] = g
		}
		return g
	}
	for _, row := range rows {
		c := row.counts()
		total.add(c)
		g := ensureGroup(row.Grp)
		g.Counts.add(c)
		g.Vendors[row.Vendor] = true
		ch := g.Channels[row.ChannelID]
		if ch == nil {
			name := row.ChannelName
			if name == "" {
				name = "渠道 #" + strconv.Itoa(row.ChannelID)
			}
			ch = &stabilityChannelBuild{ID: row.ChannelID, Name: name, Vendor: row.Vendor, Status: row.Status, Current: row.Current, Models: map[string]*stabilityCounts{}}
			g.Channels[row.ChannelID] = ch
		}
		ch.Counts.add(c)
		allCh := channelTotals[row.ChannelID]
		if allCh == nil {
			allCh = &stabilityChannelBuild{ID: row.ChannelID, Name: ch.Name, Vendor: row.Vendor, Status: row.Status, Current: row.Current, Models: map[string]*stabilityCounts{}}
			channelTotals[row.ChannelID] = allCh
		}
		allCh.Counts.add(c)
		mc := ch.Models[row.ModelName]
		if mc == nil {
			mc = &stabilityCounts{}
			ch.Models[row.ModelName] = mc
		}
		mc.add(c)
		gm := g.Models[row.ModelName]
		if gm == nil {
			gm = &stabilityCounts{}
			g.Models[row.ModelName] = gm
		}
		gm.add(c)
		mt := modelTotals[row.ModelName]
		if mt == nil {
			mt = &stabilityCounts{}
			modelTotals[row.ModelName] = mt
		}
		mt.add(c)
	}
	for _, row := range rejects {
		c := stabilityCounts{Rejected: row.Count}
		total.add(c)
		g := ensureGroup(row.Grp)
		g.Counts.add(c)
		gm := g.Models[row.Model]
		if gm == nil {
			gm = &stabilityCounts{}
			g.Models[row.Model] = gm
		}
		gm.add(c)
		mt := modelTotals[row.Model]
		if mt == nil {
			mt = &stabilityCounts{}
			modelTotals[row.Model] = mt
		}
		mt.add(c)
	}

	prevGroup := map[string]stabilityCounts{}
	prevChannel := map[string]stabilityCounts{}
	prevChannelGlobal := map[int]stabilityCounts{}
	prevModel := map[string]stabilityCounts{}
	prevGroupModel := map[string]stabilityCounts{}
	prevChannelModel := map[string]stabilityCounts{}
	var prevTotal stabilityCounts
	for _, row := range prevRows {
		c := row.counts()
		prevTotal.add(c)
		v := prevGroup[row.Grp]
		v.add(c)
		prevGroup[row.Grp] = v
		ck := row.Grp + "\x00" + strconv.Itoa(row.ChannelID)
		v = prevChannel[ck]
		v.add(c)
		prevChannel[ck] = v
		v = prevChannelGlobal[row.ChannelID]
		v.add(c)
		prevChannelGlobal[row.ChannelID] = v
		v = prevModel[row.ModelName]
		v.add(c)
		prevModel[row.ModelName] = v
		gmk := row.Grp + "\x00" + row.ModelName
		v = prevGroupModel[gmk]
		v.add(c)
		prevGroupModel[gmk] = v
		cmk := ck + "\x00" + row.ModelName
		v = prevChannelModel[cmk]
		v.add(c)
		prevChannelModel[cmk] = v
	}
	for _, row := range prevRejects {
		c := stabilityCounts{Rejected: row.Count}
		prevTotal.add(c)
		v := prevGroup[row.Grp]
		v.add(c)
		prevGroup[row.Grp] = v
		v = prevModel[row.Model]
		v.add(c)
		prevModel[row.Model] = v
		gmk := row.Grp + "\x00" + row.Model
		v = prevGroupModel[gmk]
		v.add(c)
		prevGroupModel[gmk] = v
	}

	dailyRows, dailyTruncated, err := m.queryStabilityDaily(ctx, scope, includeNested, maxDailyRows)
	if err != nil {
		return nil, err
	}
	if dailyTruncated {
		return nil, fmt.Errorf("当前范围超过 %d 条每日维度记录，请缩小日期或增加筛选", maxDailyRows)
	}
	rejectDaily, err := m.queryStabilityRejectDaily(ctx, scope)
	if err != nil {
		return nil, err
	}
	timelineStep := stabilityTimelineBucketSec(scope)
	timelineRows, timelineTruncated, err := m.queryStabilityTimeline(ctx, scope, timelineStep, includeNested, maxTimelineRows)
	if err != nil {
		return nil, err
	}
	if timelineTruncated {
		return nil, fmt.Errorf("当前范围超过 %d 条稳定性时间桶记录，请增加厂商/分组筛选", maxTimelineRows)
	}
	rejectTimeline, err := m.queryStabilityRejectTimeline(ctx, scope, timelineStep)
	if err != nil {
		return nil, err
	}
	groupDaily := map[string]map[string]stabilityCounts{}
	channelDaily := map[string]map[string]stabilityCounts{}
	for _, row := range dailyRows {
		if groupDaily[row.Grp] == nil {
			groupDaily[row.Grp] = map[string]stabilityCounts{}
		}
		c := groupDaily[row.Grp][row.Day]
		c.add(row.counts())
		groupDaily[row.Grp][row.Day] = c
		if includeNested {
			ck := row.Grp + "\x00" + strconv.Itoa(row.ChannelID)
			if channelDaily[ck] == nil {
				channelDaily[ck] = map[string]stabilityCounts{}
			}
			c = channelDaily[ck][row.Day]
			c.add(row.counts())
			channelDaily[ck][row.Day] = c
		}
	}
	for grp, byDay := range rejectDaily {
		if groupDaily[grp] == nil {
			groupDaily[grp] = map[string]stabilityCounts{}
		}
		for day, n := range byDay {
			c := groupDaily[grp][day]
			c.Rejected += n
			groupDaily[grp][day] = c
		}
	}
	groupTimeline := map[string]map[int64]stabilityCounts{}
	channelTimeline := map[string]map[int64]stabilityCounts{}
	for _, row := range timelineRows {
		if groupTimeline[row.Grp] == nil {
			groupTimeline[row.Grp] = map[int64]stabilityCounts{}
		}
		counts := groupTimeline[row.Grp][row.BucketTs]
		counts.add(row.counts())
		groupTimeline[row.Grp][row.BucketTs] = counts
		if includeNested {
			channelKey := row.Grp + "\x00" + strconv.Itoa(row.ChannelID)
			if channelTimeline[channelKey] == nil {
				channelTimeline[channelKey] = map[int64]stabilityCounts{}
			}
			counts = channelTimeline[channelKey][row.BucketTs]
			counts.add(row.counts())
			channelTimeline[channelKey][row.BucketTs] = counts
		}
	}
	for group, byBucket := range rejectTimeline {
		if groupTimeline[group] == nil {
			groupTimeline[group] = map[int64]stabilityCounts{}
		}
		for bucket, count := range byBucket {
			counts := groupTimeline[group][bucket]
			counts.Rejected += count
			groupTimeline[group][bucket] = counts
		}
	}

	keys := dateKeys(scope.FromTs, scope.ToTs)
	timelineKeys := stabilityBucketKeys(scope, timelineStep)
	totalMetrics, prevMetrics := total.metrics(), prevTotal.metrics()
	resultGroups := make([]StabilityGroup, 0, len(groups))
	for _, gb := range groups {
		gm := gb.Counts.metrics()
		g := StabilityGroup{Name: gb.Name, Vendor: vendorLabel(gb.Vendors), StabilityMetrics: gm, ModelCount: len(gb.Models),
			DeltaPP: deltaPP(gm, prevGroup[gb.Name].metrics()), Daily: buildDaily(keys, groupDaily[gb.Name]),
			Timeline: buildTimeline(timelineKeys, groupTimeline[gb.Name])}
		if totalMetrics.Requests > 0 {
			g.SharePct = float64(g.Requests) / float64(totalMetrics.Requests) * 100
		}
		for _, cb := range gb.Channels {
			cm := cb.Counts.metrics()
			channelKey := gb.Name + "\x00" + strconv.Itoa(cb.ID)
			ch := StabilityChannel{ID: cb.ID, Name: cb.Name, Vendor: cb.Vendor, Status: cb.Status, Current: cb.Current, StabilityMetrics: cm, ModelCount: len(cb.Models),
				DeltaPP: deltaPP(cm, prevChannel[channelKey].metrics())}
			if includeNested {
				ch.Daily = buildDaily(keys, channelDaily[channelKey])
				ch.Timeline = buildTimeline(timelineKeys, channelTimeline[channelKey])
			}
			if g.Requests > 0 {
				ch.SharePct = float64(ch.Requests) / float64(g.Requests) * 100
			}
			if g.Problems > 0 {
				ch.ProblemSharePct = float64(ch.Problems) / float64(g.Problems) * 100
			}
			if includeNested {
				for name, counts := range cb.Models {
					mm := counts.metrics()
					model := StabilityModel{Name: name, StabilityMetrics: mm,
						DeltaPP: deltaPP(mm, prevChannelModel[gb.Name+"\x00"+strconv.Itoa(cb.ID)+"\x00"+name].metrics())}
					if ch.Requests > 0 {
						model.SharePct = float64(model.Requests) / float64(ch.Requests) * 100
					}
					ch.Models = append(ch.Models, model)
				}
				sortModels(ch.Models)
			}
			g.Channels = append(g.Channels, ch)
		}
		sort.Slice(g.Channels, func(i, j int) bool {
			if g.Channels[i].Requests == g.Channels[j].Requests {
				return g.Channels[i].ID < g.Channels[j].ID
			}
			return g.Channels[i].Requests > g.Channels[j].Requests
		})
		if includeNested {
			for name, counts := range gb.Models {
				mm := counts.metrics()
				model := StabilityModel{Name: name, StabilityMetrics: mm, DeltaPP: deltaPP(mm, prevGroupModel[gb.Name+"\x00"+name].metrics())}
				if g.Requests > 0 {
					model.SharePct = float64(model.Requests) / float64(g.Requests) * 100
				}
				g.Models = append(g.Models, model)
			}
			sortModels(g.Models)
		}
		resultGroups = append(resultGroups, g)
	}
	sort.Slice(resultGroups, func(i, j int) bool {
		if resultGroups[i].Requests == resultGroups[j].Requests {
			return resultGroups[i].Name < resultGroups[j].Name
		}
		return resultGroups[i].Requests > resultGroups[j].Requests
	})

	modelRanking := make([]StabilityRankItem, 0, len(modelTotals))
	for name, counts := range modelTotals {
		mm := counts.metrics()
		r := StabilityRankItem{Name: name, StabilityMetrics: mm, DeltaPP: deltaPP(mm, prevModel[name].metrics())}
		if totalMetrics.Requests > 0 {
			r.SharePct = float64(r.Requests) / float64(totalMetrics.Requests) * 100
		}
		modelRanking = append(modelRanking, r)
	}
	sort.Slice(modelRanking, func(i, j int) bool {
		if modelRanking[i].Requests == modelRanking[j].Requests {
			return modelRanking[i].Name < modelRanking[j].Name
		}
		return modelRanking[i].Requests > modelRanking[j].Requests
	})
	channelRanking := make([]StabilityRankItem, 0, len(channelTotals))
	for _, cb := range channelTotals {
		cm := cb.Counts.metrics()
		ch := StabilityRankItem{ID: cb.ID, Name: cb.Name, Vendor: cb.Vendor,
			StabilityMetrics: cm, DeltaPP: deltaPP(cm, prevChannelGlobal[cb.ID].metrics())}
		if totalMetrics.Requests > 0 {
			ch.SharePct = float64(ch.Requests) / float64(totalMetrics.Requests) * 100
		}
		channelRanking = append(channelRanking, ch)
	}
	sort.Slice(channelRanking, func(i, j int) bool {
		if channelRanking[i].Requests == channelRanking[j].Requests {
			return channelRanking[i].ID < channelRanking[j].ID
		}
		return channelRanking[i].Requests > channelRanking[j].Requests
	})
	groupRanking := make([]StabilityRankItem, 0, len(resultGroups))
	for _, g := range resultGroups {
		groupRanking = append(groupRanking, StabilityRankItem{Name: g.Name, SharePct: g.SharePct, DeltaPP: g.DeltaPP, StabilityMetrics: g.StabilityMetrics})
	}

	queryDays := m.cfg.stabilityQueryDays()
	comparisonCoverage := m.stabilityDataCoverage(ctx, previousScope.FromTs, previousScope.ToTs, now)
	meta := StabilityReportMeta{From: time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02"), To: time.Unix(scope.ToTs-1, 0).In(cstLocation).Format("2006-01-02"), GeneratedAt: now, RetentionDays: queryDays, RowsTruncated: false, ComparisonAvailable: comparisonCoverage.Complete, ComparisonCoverage: comparisonCoverage, TimelineBucketSec: timelineStep}
	meta.DataCoverage = m.stabilityDataCoverage(ctx, scope.FromTs, scope.ToTs, now)
	var coverage struct{ Min, Max int64 }
	warnReadErr("stability coverage", m.storeDB.WithContext(ctx).Raw(
		"SELECT COALESCE(MIN(hour_ts),0) min, COALESCE(MAX(hour_ts),0) max FROM stability_hour_samples WHERE traffic_class_version = ?",
		userTrafficClassificationVersion).Scan(&coverage))
	meta.FirstDataTs, meta.LastDataTs = coverage.Min, coverage.Max
	if lastBucket := m.storeFreshness(); lastBucket > 0 {
		meta.Sources.NewAPILastTs = lastBucket + 60
		meta.Sources.NewAPIDataAgeSec = now - meta.Sources.NewAPILastTs
		if meta.Sources.NewAPIDataAgeSec < 0 {
			meta.Sources.NewAPIDataAgeSec = 0
		}
	}
	var problemLast struct{ Max int64 }
	warnReadErr("stability problem freshness", m.storeDB.WithContext(ctx).Raw(
		"SELECT COALESCE(MAX(last_ts),0) max FROM stability_problem_samples WHERE source='newapi' AND traffic_class_version=?",
		userTrafficClassificationVersion).Scan(&problemLast))
	meta.Sources.ProblemLastTs = problemLast.Max
	var problemLive StabilityProblemLiveCursor
	if err := m.storeDB.WithContext(ctx).First(&problemLive, "id = ? AND traffic_class_version = ?", 1, userTrafficClassificationVersion).Error; err == nil {
		meta.Sources.ProblemCoverageTo = problemLive.NextTs
		if problemLive.TargetThroughTs > problemLive.NextTs {
			meta.Sources.ProblemPendingMinutes = (problemLive.TargetThroughTs - problemLive.NextTs + 59) / 60
		}
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		slog.Warn("读取稳定性问题实时水位失败", "err", err)
	}
	meta.Sources.ProblemSamplerLastSuccess = m.problemLastSuccess.Load()
	meta.Sources.ProblemSamplerLastFailure = m.problemLastFailure.Load()
	meta.Sources.ProblemMigration = m.stabilityProblemMigrationProgress()
	if meta.Sources.NewAPILastTs > meta.Sources.ProblemCoverageTo && meta.Sources.ProblemCoverageTo > 0 {
		meta.Sources.ProblemCoverageLagSec = meta.Sources.NewAPILastTs - meta.Sources.ProblemCoverageTo
	}
	meta.Sources.NginxEnabled = m.cfg.NginxEnabled
	meta.Sources.NginxConnected, meta.Sources.NginxHealthySources, meta.Sources.NginxSourceCount,
		meta.Sources.NginxLastTs, meta.Sources.RequestIDCoverage = m.nginxSourceSummary(ctx, now)
	meta.Sources.NginxStatus = "disabled"
	if meta.Sources.NginxEnabled {
		meta.Sources.NginxStatus = "degraded"
		if meta.Sources.NginxConnected {
			meta.Sources.NginxStatus = "ok"
		}
	}

	return &StabilityReport{Enabled: true, Meta: meta, Filters: buildStabilityFilters(rows), Summary: totalMetrics, Previous: prevMetrics, DeltaPP: deltaPP(totalMetrics, prevMetrics), Groups: resultGroups, Rankings: StabilityRankings{Groups: groupRanking, Channels: channelRanking, Models: modelRanking}}, nil
}

func buildStabilityFilters(rows []stabilityDimRow) StabilityFilters {
	vendors, groups, models := map[string]bool{}, map[string]bool{}, map[string]bool{}
	channels := map[int]StabilityFilterChannel{}
	for _, r := range rows {
		if r.Vendor != "" {
			vendors[r.Vendor] = true
		}
		if r.Grp != "" {
			groups[r.Grp] = true
		}
		if r.ModelName != "" {
			models[r.ModelName] = true
		}
		name := r.ChannelName
		if name == "" {
			name = "渠道 #" + strconv.Itoa(r.ChannelID)
		}
		channels[r.ChannelID] = StabilityFilterChannel{ID: r.ChannelID, Name: name, Vendor: r.Vendor}
	}
	out := StabilityFilters{}
	for v := range vendors {
		out.Vendors = append(out.Vendors, v)
	}
	for v := range groups {
		out.Groups = append(out.Groups, v)
	}
	for v := range models {
		out.Models = append(out.Models, v)
	}
	for _, v := range channels {
		out.Channels = append(out.Channels, v)
	}
	sort.Strings(out.Vendors)
	sort.Strings(out.Groups)
	sort.Strings(out.Models)
	sort.Slice(out.Channels, func(i, j int) bool { return out.Channels[i].ID < out.Channels[j].ID })
	return out
}

func stabilityRange(c *gin.Context, now time.Time, maxDays int) (stabilityScope, error) {
	if maxDays <= 0 {
		maxDays = 90
	}
	now = now.In(cstLocation)
	days, _ := strconv.Atoi(c.DefaultQuery("days", "7"))
	if days < 1 {
		days = 7
	}
	if days > maxDays {
		days = maxDays
	}
	start := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, cstLocation).AddDate(0, 0, -days+1)
	end := now
	fromText, toText := strings.TrimSpace(c.Query("from")), strings.TrimSpace(c.Query("to"))
	if fromText != "" || toText != "" {
		if fromText == "" || toText == "" {
			return stabilityScope{}, fmt.Errorf("from 和 to 必须同时提供")
		}
		from, err := time.ParseInLocation("2006-01-02", fromText, cstLocation)
		if err != nil {
			return stabilityScope{}, fmt.Errorf("from 日期格式应为 YYYY-MM-DD")
		}
		to, err := time.ParseInLocation("2006-01-02", toText, cstLocation)
		if err != nil {
			return stabilityScope{}, fmt.Errorf("to 日期格式应为 YYYY-MM-DD")
		}
		to = to.AddDate(0, 0, 1)
		if to.After(now) {
			to = now
		}
		if !to.After(from) {
			return stabilityScope{}, fmt.Errorf("结束日期必须晚于开始日期")
		}
		if to.Sub(from) > time.Duration(maxDays)*24*time.Hour {
			return stabilityScope{}, fmt.Errorf("查询范围不能超过 %d 天", maxDays)
		}
		start, end = from, to
	}
	channelID, _ := strconv.Atoi(c.Query("channel"))
	if channelID < 0 {
		channelID = 0
	}
	return stabilityScope{FromTs: start.Unix(), ToTs: end.Unix(), Group: clip(strings.TrimSpace(c.Query("group")), 64), ChannelID: channelID, Model: clip(strings.TrimSpace(c.Query("model")), 128), Vendor: clip(strings.TrimSpace(c.Query("vendor")), 128)}, nil
}

func (m *Monitor) serveStabilityReport(c *gin.Context) {
	if !m.cfg.StabilityEnabled {
		c.JSON(200, gin.H{"enabled": false})
		return
	}
	maxDays := m.cfg.stabilityQueryDays()
	scope, err := stabilityRange(c, time.Now(), maxDays)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
	defer cancel()
	report, err := m.buildStabilityReportWithDetails(ctx, scope, time.Now().Unix(), false)
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	c.JSON(200, report)
}

func (m *Monitor) serveStabilityDetail(c *gin.Context) {
	if !m.cfg.StabilityEnabled {
		c.JSON(200, gin.H{"enabled": false})
		return
	}
	maxDays := m.cfg.stabilityQueryDays()
	scope, err := stabilityRange(c, time.Now(), maxDays)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	if scope.Group == "" {
		c.JSON(400, gin.H{"error": "必须指定服务分组"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
	defer cancel()
	report, err := m.buildStabilityReportWithDetails(ctx, scope, time.Now().Unix(), true)
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	if len(report.Groups) == 0 {
		c.JSON(404, gin.H{"error": "当前范围没有该服务分组数据"})
		return
	}
	c.JSON(200, gin.H{"enabled": true, "group": report.Groups[0]})
}

// 前端切换日期/筛选时会主动 AbortController 取消旧请求。499 只记录“客户端已取消”，
// 不再把正常交互写成服务端 500；服务端自身查询超时仍明确返回 504。
func writeStabilityReadError(c *gin.Context, err error) {
	if errors.Is(err, context.Canceled) {
		c.AbortWithStatus(499)
		return
	}
	if errors.Is(err, context.DeadlineExceeded) {
		c.JSON(http.StatusGatewayTimeout, gin.H{"error": "查询超时，请缩小日期范围或增加筛选后重试"})
		return
	}
	c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
}
