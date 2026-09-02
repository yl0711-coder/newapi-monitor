package monitor

import (
	"context"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

type StabilityProblem struct {
	Source        string   `json:"source"`
	SignatureHash string   `json:"signature_hash"`
	Code          string   `json:"code"`
	Message       string   `json:"message"`
	Truncated     bool     `json:"truncated"`
	Count         int64    `json:"count"`
	SharePct      float64  `json:"share_pct"`
	FirstTs       int64    `json:"first_ts"`
	LastTs        int64    `json:"last_ts"`
	Groups        []string `json:"groups"`
	ChannelIDs    []int    `json:"channel_ids"`
	Models        []string `json:"models"`
	// 建议知识库尚未经过业务确认。接口明确给状态，不在运行时根据关键词生成结论。
	AdviceStatus string `json:"advice_status"`
}

type StabilityProblemsResponse struct {
	Enabled          bool               `json:"enabled"`
	From             string             `json:"from"`
	To               string             `json:"to"`
	GeneratedAt      int64              `json:"generated_at"`
	CoverageFrom     int64              `json:"coverage_from"`
	CoverageTo       int64              `json:"coverage_to"`
	PendingMinutes   int64              `json:"pending_minutes"`
	UncoveredMinutes int64              `json:"uncovered_minutes"`
	CoverageComplete bool               `json:"coverage_complete"`
	CapturedTotal    int64              `json:"captured_total"`
	Truncated        bool               `json:"truncated"`
	Problems         []StabilityProblem `json:"problems"`
}

func splitDistinct(s string) []string {
	if s == "" {
		return nil
	}
	set := map[string]bool{}
	for _, v := range strings.Split(s, ",") {
		if v = strings.TrimSpace(v); v != "" {
			set[v] = true
		}
	}
	out := make([]string, 0, len(set))
	for v := range set {
		out = append(out, v)
	}
	sort.Strings(out)
	return out
}

func splitDistinctInts(s string) []int {
	var out []int
	for _, v := range splitDistinct(s) {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			out = append(out, n)
		}
	}
	sort.Ints(out)
	return out
}

func (m *Monitor) queryStabilityProblems(ctx context.Context, scope stabilityScope, limit int) (*StabilityProblemsResponse, error) {
	db := m.storeDB.WithContext(ctx)
	where := " WHERE p.bucket_ts >= ? AND p.bucket_ts < ? AND p.traffic_class_version = ?"
	args := []any{scope.FromTs, scope.ToTs, userTrafficClassificationVersion}
	if scope.Group != "" {
		where += " AND p.grp = ?"
		args = append(args, scope.Group)
	}
	if scope.ChannelID > 0 {
		where += " AND p.channel_id = ?"
		args = append(args, scope.ChannelID)
	}
	if scope.Model != "" {
		where += " AND p.model_name = ?"
		args = append(args, scope.Model)
	}
	if scope.Vendor != "" {
		where += " AND COALESCE(cs.vendor,'') = ?"
		args = append(args, scope.Vendor)
	}
	type problemRow struct {
		Source, SignatureHash, Code, Message string
		Truncated                            bool
		Count, FirstTs, LastTs               int64
		Groups, Channels, Models             string
	}
	q := `SELECT p.source, p.signature_hash, p.code, p.message, p.truncated,
		COALESCE(SUM(p.count),0) AS count, MIN(p.first_ts) AS first_ts, MAX(p.last_ts) AS last_ts,
		GROUP_CONCAT(DISTINCT p.grp) AS groups,
		GROUP_CONCAT(DISTINCT p.channel_id) AS channels,
		GROUP_CONCAT(DISTINCT p.model_name) AS models
		FROM stability_problem_samples p LEFT JOIN channel_snaps cs ON cs.id=p.channel_id` + where + `
		GROUP BY p.source,p.signature_hash,p.code,p.message,p.truncated
		ORDER BY count DESC,last_ts DESC LIMIT ?`
	whereArgs := append([]any(nil), args...)
	args = append(args, limit+1)
	var rows []problemRow
	if err := db.Raw(q, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	truncated := len(rows) > limit
	if truncated {
		rows = rows[:limit]
	}
	out := make([]StabilityProblem, 0, len(rows)+8)
	for _, r := range rows {
		message, redactionTruncated := stabilityProblemText(r.Message)
		out = append(out, StabilityProblem{Source: r.Source, SignatureHash: r.SignatureHash, Code: r.Code, Message: message, Truncated: r.Truncated || redactionTruncated, Count: r.Count, FirstTs: r.FirstTs, LastTs: r.LastTs, Groups: splitDistinct(r.Groups), ChannelIDs: splitDistinctInts(r.Channels), Models: splitDistinct(r.Models), AdviceStatus: "knowledge_base_pending_review"})
	}

	sort.Slice(out, func(i, j int) bool {
		if out[i].Count == out[j].Count {
			return out[i].LastTs > out[j].LastTs
		}
		return out[i].Count > out[j].Count
	})
	if len(out) > limit {
		out = out[:limit]
		truncated = true
	}
	// CapturedTotal / SharePct 必须以当前筛选下的全部已采集错误为分母，
	// 不能只统计 Top N，否则限制行数后比例会被放大。
	var rawTotal struct{ Count int64 }
	if err := db.Raw(`SELECT COALESCE(SUM(p.count),0) count FROM stability_problem_samples p
		LEFT JOIN channel_snaps cs ON cs.id=p.channel_id`+where, whereArgs...).Scan(&rawTotal).Error; err != nil {
		return nil, err
	}
	total := rawTotal.Count
	if total > 0 {
		for i := range out {
			out[i].SharePct = float64(out[i].Count) / float64(total) * 100
		}
	}
	// 覆盖度必须按本次查询范围计算。“当前无积压”不等于
	// “历史范围已全量采集”，特别是功能上线前的日期不能被误报为完整。
	now := time.Now().Unix()
	coverageFrom := scope.FromTs / 60 * 60
	coverageTargetTo := scope.ToTs / 60 * 60
	finalizedTo := (now - stabilityProblemFinalizeDelaySec) / 60 * 60
	if coverageTargetTo > finalizedTo {
		coverageTargetTo = finalizedTo
	}
	if coverageTargetTo < coverageFrom {
		coverageTargetTo = coverageFrom
	}
	var coverage struct{ Min, Max, Total, Complete, Pending int64 }
	warnReadErr("stability problem coverage", db.Raw(`SELECT
		COALESCE(MIN(CASE WHEN complete THEN bucket_ts END),0) min,
		COALESCE(MAX(CASE WHEN complete THEN bucket_ts END),0) max,
		COUNT(*) total,
		COALESCE(SUM(CASE WHEN complete THEN 1 ELSE 0 END),0) complete,
		COALESCE(SUM(CASE WHEN complete THEN 0 ELSE 1 END),0) pending
		FROM stability_problem_ingest_states WHERE bucket_ts>=? AND bucket_ts<? AND traffic_class_version=?`,
		coverageFrom, coverageTargetTo, userTrafficClassificationVersion).Scan(&coverage))
	expectedMinutes := (coverageTargetTo - coverageFrom) / 60
	uncoveredMinutes := expectedMinutes - coverage.Total
	if uncoveredMinutes < 0 {
		uncoveredMinutes = 0
	}
	coverageTo := int64(0)
	if coverage.Max > 0 {
		coverageTo = coverage.Max + 60
	}
	coverageComplete := expectedMinutes > 0 && coverage.Pending == 0 && coverage.Complete == expectedMinutes
	return &StabilityProblemsResponse{Enabled: true, From: time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02"), To: time.Unix(scope.ToTs-1, 0).In(cstLocation).Format("2006-01-02"), GeneratedAt: now, CoverageFrom: coverage.Min, CoverageTo: coverageTo, PendingMinutes: coverage.Pending, UncoveredMinutes: uncoveredMinutes, CoverageComplete: coverageComplete, CapturedTotal: total, Truncated: truncated, Problems: out}, nil
}

func (m *Monitor) serveStabilityProblems(c *gin.Context) {
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
	limit, _ := strconv.Atoi(c.DefaultQuery("limit", "50"))
	if limit < 1 {
		limit = 50
	}
	if limit > 100 {
		limit = 100
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 8*time.Second)
	defer cancel()
	result, err := m.queryStabilityProblems(ctx, scope, limit)
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	c.JSON(200, result)
}
