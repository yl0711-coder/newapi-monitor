package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"regexp"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// nginx.go 只处理节点侧已经脱敏并聚合的 Nginx 入口事实。这里不接收原始日志，
// 也不存 IP、query、Header、Cookie、Key、请求体、响应体或 Request ID 原值。

var nginxNodeNamePattern = regexp.MustCompile(`^[A-Za-z0-9][A-Za-z0-9._-]{0,63}$`)

// validateNginxSettings 在进程启动前拦住“页面显示已启用，但接收口永远不可用”
// 的半配置状态。默认关闭时不增加任何新启动条件。
func validateNginxSettings(s Settings) error {
	if !s.NginxEnabled {
		return nil
	}
	if strings.TrimSpace(s.IngestToken) == "" {
		return fmt.Errorf("MONITOR_NGINX_ENABLED=true 时必须配置 MONITOR_INGEST_TOKEN")
	}
	if len(s.NginxAllowedNodes) == 0 {
		return fmt.Errorf("MONITOR_NGINX_ENABLED=true 时必须配置 MONITOR_NGINX_ALLOWED_NODES")
	}
	if len(s.NginxAllowedNodes) > 32 {
		return fmt.Errorf("MONITOR_NGINX_ALLOWED_NODES 最多允许 32 个节点")
	}
	seen := make(map[string]struct{}, len(s.NginxAllowedNodes))
	for _, raw := range s.NginxAllowedNodes {
		node := strings.TrimSpace(raw)
		if !nginxNodeNamePattern.MatchString(node) {
			return fmt.Errorf("MONITOR_NGINX_ALLOWED_NODES 包含无效节点名")
		}
		if _, exists := seen[node]; exists {
			return fmt.Errorf("MONITOR_NGINX_ALLOWED_NODES 包含重复节点")
		}
		seen[node] = struct{}{}
	}
	return nil
}

type nginxIngestSample struct {
	BucketTs          int64  `json:"bucket_ts"`
	Route             string `json:"route"`
	Method            string `json:"method"`
	Status            int    `json:"status"`
	UpstreamStatus    int    `json:"upstream_status"`
	Count             int64  `json:"count"`
	RequestTimeSumMS  int64  `json:"request_time_sum_ms"`
	RequestTimeMaxMS  int64  `json:"request_time_max_ms"`
	UpstreamTimeSumMS int64  `json:"upstream_time_sum_ms"`
	UpstreamTimeCount int64  `json:"upstream_time_count"`
	BytesSent         int64  `json:"bytes_sent"`
	RequestIDPresent  int64  `json:"request_id_present"`
}

type nginxIngestRequest struct {
	Node                      string              `json:"node"`
	BatchID                   string              `json:"batch_id"`
	Samples                   []nginxIngestSample `json:"samples"`
	BacklogBytes              int64               `json:"backlog_bytes"`
	BacklogKnown              bool                `json:"backlog_known"`
	CursorDiscontinuities     int64               `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt int64               `json:"last_cursor_discontinuity_at"`
	DiscardedLines            int64               `json:"discarded_lines"`
	LastDiscardedAt           int64               `json:"last_discarded_at"`
}

func normalizeNginxRoute(path string) string {
	path = strings.TrimSpace(strings.SplitN(path, "?", 2)[0])
	switch path {
	case "/v1/chat/completions", "/v1/responses", "/v1/messages", "/v1/models", "/api/status":
		return path
	}
	if strings.HasPrefix(path, "/v1/") {
		return "/v1/*"
	}
	if strings.HasPrefix(path, "/api/") {
		return "/api/*"
	}
	return "/other"
}

func normalizeNginxMethod(method string) string {
	method = strings.ToUpper(strings.TrimSpace(method))
	switch method {
	case "GET", "POST", "PUT", "PATCH", "DELETE", "HEAD", "OPTIONS":
		return method
	default:
		return "OTHER"
	}
}

func safeBatchID(value string) bool {
	if len(value) < 8 || len(value) > 64 {
		return false
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' {
			continue
		}
		return false
	}
	return true
}

func (m *Monitor) nginxNodeAllowed(node string) bool {
	if len(m.cfg.NginxAllowedNodes) == 0 {
		return false
	}
	for _, allowed := range m.cfg.NginxAllowedNodes {
		if node == allowed {
			return true
		}
	}
	return false
}

func (m *Monitor) nginxRetentionDays() int {
	days := m.cfg.NginxRetentionDays
	if days < 1 || days > 90 {
		return 7
	}
	return days
}

func validateNginxSample(sample nginxIngestSample, now int64, retentionDays int) (NginxMinuteSample, error) {
	if retentionDays < 1 {
		retentionDays = 7
	}
	bucket := sample.BucketTs / 60 * 60
	oldest := now - int64(retentionDays+1)*86400
	if bucket <= 0 || bucket < oldest || bucket > now+300 {
		return NginxMinuteSample{}, fmt.Errorf("bucket_ts outside retention window")
	}
	// HTTP status 是入口事实的核心维度，不存在“未知状态也计入请求数”
	// 的合法口径。若放行 0，持有 ingest token 的异常客户端可以稀释
	// 2xx/4xx/5xx 占比，使报表总请求与状态码之和不守恒。
	if sample.Status < 100 || sample.Status > 599 {
		return NginxMinuteSample{}, fmt.Errorf("invalid status")
	}
	if sample.UpstreamStatus != 0 && (sample.UpstreamStatus < 100 || sample.UpstreamStatus > 599) {
		return NginxMinuteSample{}, fmt.Errorf("invalid upstream_status")
	}
	if sample.Count <= 0 || sample.Count > 10_000_000 {
		return NginxMinuteSample{}, fmt.Errorf("invalid count")
	}
	if sample.RequestTimeSumMS < 0 || sample.RequestTimeMaxMS < 0 || sample.RequestTimeMaxMS > 86_400_000 ||
		sample.UpstreamTimeSumMS < 0 || sample.UpstreamTimeCount < 0 || sample.UpstreamTimeCount > sample.Count ||
		sample.BytesSent < 0 || sample.BytesSent > sample.Count*(16<<30) ||
		sample.RequestIDPresent < 0 || sample.RequestIDPresent > sample.Count {
		return NginxMinuteSample{}, fmt.Errorf("invalid aggregate values")
	}
	if sample.RequestTimeSumMS > sample.Count*86_400_000 || sample.UpstreamTimeSumMS > sample.Count*86_400_000 {
		return NginxMinuteSample{}, fmt.Errorf("aggregate duration too large")
	}
	if sample.RequestTimeMaxMS > sample.RequestTimeSumMS {
		return NginxMinuteSample{}, fmt.Errorf("request_time_max_ms exceeds sum")
	}
	// 0 ms 上游响应是合法值：此时 count=1,sum=0。只拒绝“无样本却有总和”。
	if sample.UpstreamTimeCount == 0 && sample.UpstreamTimeSumMS != 0 {
		return NginxMinuteSample{}, fmt.Errorf("upstream time sum without samples")
	}
	if sample.UpstreamTimeSumMS > sample.UpstreamTimeCount*86_400_000 {
		return NginxMinuteSample{}, fmt.Errorf("upstream duration exceeds sample count")
	}
	return NginxMinuteSample{
		BucketTs: bucket, Route: normalizeNginxRoute(sample.Route), Method: normalizeNginxMethod(sample.Method),
		Status: sample.Status, UpstreamStatus: sample.UpstreamStatus, Count: sample.Count,
		RequestTimeSumMS: sample.RequestTimeSumMS, RequestTimeMaxMS: sample.RequestTimeMaxMS,
		UpstreamTimeSumMS: sample.UpstreamTimeSumMS, UpstreamTimeCount: sample.UpstreamTimeCount,
		BytesSent: sample.BytesSent, RequestIDPresent: sample.RequestIDPresent,
	}, nil
}

func mergeNginxSample(dst *NginxMinuteSample, src NginxMinuteSample) error {
	add := func(target *int64, value int64) error {
		if value < 0 || *target > math.MaxInt64-value {
			return fmt.Errorf("aggregate overflow")
		}
		*target += value
		return nil
	}
	for _, pair := range []struct {
		target *int64
		value  int64
	}{
		{&dst.Count, src.Count}, {&dst.RequestTimeSumMS, src.RequestTimeSumMS},
		{&dst.UpstreamTimeSumMS, src.UpstreamTimeSumMS}, {&dst.UpstreamTimeCount, src.UpstreamTimeCount},
		{&dst.BytesSent, src.BytesSent}, {&dst.RequestIDPresent, src.RequestIDPresent},
	} {
		if err := add(pair.target, pair.value); err != nil {
			return err
		}
	}
	if src.RequestTimeMaxMS > dst.RequestTimeMaxMS {
		dst.RequestTimeMaxMS = src.RequestTimeMaxMS
	}
	// 单条样本的上限不能被同 key 多行合并绕过。
	if dst.Count > 10_000_000 || dst.RequestTimeMaxMS > dst.RequestTimeSumMS ||
		dst.UpstreamTimeCount > dst.Count || (dst.UpstreamTimeCount == 0 && dst.UpstreamTimeSumMS != 0) ||
		dst.UpstreamTimeSumMS > dst.UpstreamTimeCount*86_400_000 || dst.BytesSent > dst.Count*(16<<30) ||
		dst.RequestIDPresent > dst.Count {
		return fmt.Errorf("merged aggregate exceeds limits")
	}
	return nil
}

func (m *Monitor) ingestNginx(c *gin.Context) {
	if !m.cfg.NginxEnabled {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx ingest disabled"})
		return
	}
	if !m.checkIngest(c) {
		return
	}
	var in nginxIngestRequest
	if err := c.ShouldBindJSON(&in); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid json"})
		return
	}
	in.Node = strings.TrimSpace(in.Node)
	// 节点名是采集状态与样本的归属键，必须完整校验后再和白名单比较。
	// 不能先截断：否则“合法白名单名 + 任意后缀”会被截成同一个节点名。
	if !nginxNodeNamePattern.MatchString(in.Node) || !m.nginxNodeAllowed(in.Node) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "node not allowed"})
		return
	}
	if !safeBatchID(in.BatchID) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid batch_id"})
		return
	}
	if len(in.Samples) > 2000 {
		c.JSON(http.StatusRequestEntityTooLarge, gin.H{"error": "too many samples"})
		return
	}
	now := time.Now().Unix()
	if in.BacklogBytes < 0 || in.BacklogBytes > 1<<50 || (!in.BacklogKnown && in.BacklogBytes != 0) ||
		in.CursorDiscontinuities < 0 || in.CursorDiscontinuities > 1_000_000_000 ||
		in.LastCursorDiscontinuityAt < 0 || in.LastCursorDiscontinuityAt > now+300 ||
		(in.CursorDiscontinuities == 0) != (in.LastCursorDiscontinuityAt == 0) ||
		in.DiscardedLines < 0 || in.DiscardedLines > 1_000_000_000_000 || in.LastDiscardedAt < 0 || in.LastDiscardedAt > now+300 ||
		(in.DiscardedLines == 0) != (in.LastDiscardedAt == 0) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid collector telemetry"})
		return
	}
	merged := make(map[string]NginxMinuteSample, len(in.Samples))
	var firstTs, lastTs, acceptedCount int64
	for i, raw := range in.Samples {
		row, err := validateNginxSample(raw, now, m.nginxRetentionDays())
		if err != nil {
			c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid sample %d: %v", i, err)})
			return
		}
		row.Node = in.Node
		key := fmt.Sprintf("%d\x00%s\x00%s\x00%d\x00%d", row.BucketTs, row.Route, row.Method, row.Status, row.UpstreamStatus)
		if current, ok := merged[key]; ok {
			if err := mergeNginxSample(&current, row); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": fmt.Sprintf("invalid sample %d: %v", i, err)})
				return
			}
			merged[key] = current
		} else {
			merged[key] = row
		}
		if firstTs == 0 || row.BucketTs < firstTs {
			firstTs = row.BucketTs
		}
		if row.BucketTs > lastTs {
			lastTs = row.BucketTs
		}
		acceptedCount += row.Count
	}
	rows := make([]NginxMinuteSample, 0, len(merged))
	for _, row := range merged {
		rows = append(rows, row)
	}
	sort.Slice(rows, func(i, j int) bool {
		if rows[i].BucketTs != rows[j].BucketTs {
			return rows[i].BucketTs < rows[j].BucketTs
		}
		return rows[i].Route < rows[j].Route
	})
	duplicate := false
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		batch := NginxIngestBatch{Node: in.Node, BatchID: in.BatchID, FirstTs: firstTs, LastTs: lastTs, Rows: len(rows), ReceivedAt: now}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			duplicate = true
			return nil
		}
		if len(rows) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns: []clause.Column{{Name: "bucket_ts"}, {Name: "node"}, {Name: "route"}, {Name: "method"}, {Name: "status"}, {Name: "upstream_status"}},
				DoUpdates: clause.Assignments(map[string]any{
					"count":                gorm.Expr("count + excluded.count"),
					"request_time_sum_ms":  gorm.Expr("request_time_sum_ms + excluded.request_time_sum_ms"),
					"request_time_max_ms":  gorm.Expr("MAX(request_time_max_ms, excluded.request_time_max_ms)"),
					"upstream_time_sum_ms": gorm.Expr("upstream_time_sum_ms + excluded.upstream_time_sum_ms"),
					"upstream_time_count":  gorm.Expr("upstream_time_count + excluded.upstream_time_count"),
					"bytes_sent":           gorm.Expr("bytes_sent + excluded.bytes_sent"),
					"request_id_present":   gorm.Expr("request_id_present + excluded.request_id_present"),
				}),
			}).CreateInBatches(rows, 200).Error; err != nil {
				return err
			}
		}
		state := NginxSourceState{
			Node: in.Node, LastEventTs: lastTs, LastIngestTs: now, LastBatchID: in.BatchID,
			AcceptedRows: int64(len(rows)), AcceptedCount: acceptedCount,
			BacklogBytes: in.BacklogBytes, BacklogKnown: in.BacklogKnown,
			CursorDiscontinuities: in.CursorDiscontinuities, LastCursorDiscontinuityAt: in.LastCursorDiscontinuityAt,
			DiscardedLines: in.DiscardedLines, LastDiscardedAt: in.LastDiscardedAt,
		}
		return tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "node"}},
			DoUpdates: clause.Assignments(map[string]any{
				"last_event_ts":  gorm.Expr("MAX(last_event_ts, excluded.last_event_ts)"),
				"last_ingest_ts": now,
				"last_batch_id":  in.BatchID,
				"accepted_rows":  gorm.Expr("accepted_rows + excluded.accepted_rows"),
				"accepted_count": gorm.Expr("accepted_count + excluded.accepted_count"),
				"backlog_bytes":  in.BacklogBytes,
				"backlog_known":  in.BacklogKnown,
				"cursor_discontinuities": gorm.Expr(
					"MAX(COALESCE(cursor_discontinuities, 0), excluded.cursor_discontinuities)"),
				"last_cursor_discontinuity_at": gorm.Expr(
					"MAX(COALESCE(last_cursor_discontinuity_at, 0), excluded.last_cursor_discontinuity_at)"),
				"discarded_lines":   gorm.Expr("MAX(COALESCE(discarded_lines, 0), excluded.discarded_lines)"),
				"last_discarded_at": gorm.Expr("MAX(COALESCE(last_discarded_at, 0), excluded.last_discarded_at)"),
			}),
		}).Create(&state).Error
	})
	if err != nil {
		slog.Warn("Nginx 聚合入库失败", "node", in.Node, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"ok": true, "duplicate": duplicate, "stored": len(rows)})
}

func (m *Monitor) startNginxMaintenance(ctx context.Context) {
	prune := func() {
		days := m.nginxRetentionDays()
		cutoff := time.Now().Unix()/60*60 - int64(days)*86400
		if err := m.storeDB.Where("bucket_ts < ?", cutoff).Delete(&NginxMinuteSample{}).Error; err != nil {
			slog.Warn("清理 Nginx 聚合失败", "err", err)
		}
		if err := m.storeDB.Where("received_at < ?", cutoff).Delete(&NginxIngestBatch{}).Error; err != nil {
			slog.Warn("清理 Nginx 幂等批次失败", "err", err)
		}
	}
	prune()
	go func() {
		ticker := time.NewTicker(time.Hour)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				prune()
			}
		}
	}()
}

type NginxEdgeSummary struct {
	Requests          int64   `json:"requests"`
	Status2xx         int64   `json:"status_2xx"`
	Status3xx         int64   `json:"status_3xx"`
	Status4xx         int64   `json:"status_4xx"`
	Status5xx         int64   `json:"status_5xx"`
	AvgRequestMS      float64 `json:"avg_request_ms"`
	MaxRequestMS      int64   `json:"max_request_ms"`
	AvgUpstreamMS     float64 `json:"avg_upstream_ms"`
	BytesSent         int64   `json:"bytes_sent"`
	RequestIDCoverage float64 `json:"request_id_coverage"`
}

type NginxEdgeBreakdown struct {
	Name      string  `json:"name"`
	Requests  int64   `json:"requests"`
	Status4xx int64   `json:"status_4xx"`
	Status5xx int64   `json:"status_5xx"`
	AvgMS     float64 `json:"avg_ms"`
}

type NginxEdgeDay struct {
	Date string `json:"date"`
	NginxEdgeSummary
}

type NginxEdgeSource struct {
	Node                      string   `json:"node"`
	LastEventTs               int64    `json:"last_event_ts"`
	LastIngestTs              int64    `json:"last_ingest_ts"`
	AgeSec                    int64    `json:"age_sec"`
	EventAgeSec               int64    `json:"event_age_sec"`
	Status                    string   `json:"status"`
	HealthReasons             []string `json:"health_reasons,omitempty"`
	BacklogBytes              int64    `json:"backlog_bytes"`
	BacklogKnown              bool     `json:"backlog_known"`
	CursorDiscontinuities     int64    `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt int64    `json:"last_cursor_discontinuity_at"`
	DiscardedLines            int64    `json:"discarded_lines"`
	LastDiscardedAt           int64    `json:"last_discarded_at"`
}

const (
	nginxRecentDataLossWindowSec = int64(15 * 60)
	nginxEventLagWarnSec         = int64(3 * 60)
	nginxBacklogWarnBytes        = int64(16 << 20)
)

type NginxEdgeReport struct {
	Enabled       bool                 `json:"enabled"`
	RetentionDays int                  `json:"retention_days,omitempty"`
	From          string               `json:"from,omitempty"`
	To            string               `json:"to,omitempty"`
	GeneratedAt   int64                `json:"generated_at"`
	Summary       NginxEdgeSummary     `json:"summary"`
	Daily         []NginxEdgeDay       `json:"daily"`
	Routes        []NginxEdgeBreakdown `json:"routes"`
	Nodes         []NginxEdgeBreakdown `json:"nodes"`
	Sources       []NginxEdgeSource    `json:"sources"`
}

type NginxEdgeAggregate struct {
	Requests, Status2xx, Status3xx, Status4xx, Status5xx int64
	RequestTimeSumMS, MaxRequestMS                       int64
	UpstreamTimeSumMS, UpstreamTimeCount                 int64
	BytesSent, RequestIDPresent                          int64
}

func (row NginxEdgeAggregate) summary() NginxEdgeSummary {
	out := NginxEdgeSummary{Requests: row.Requests, Status2xx: row.Status2xx, Status3xx: row.Status3xx, Status4xx: row.Status4xx, Status5xx: row.Status5xx, MaxRequestMS: row.MaxRequestMS, BytesSent: row.BytesSent}
	if row.Requests > 0 {
		out.AvgRequestMS = float64(row.RequestTimeSumMS) / float64(row.Requests)
		out.RequestIDCoverage = float64(row.RequestIDPresent) / float64(row.Requests) * 100
	}
	if row.UpstreamTimeCount > 0 {
		out.AvgUpstreamMS = float64(row.UpstreamTimeSumMS) / float64(row.UpstreamTimeCount)
	}
	return out
}

const nginxAggregateColumns = `COALESCE(SUM(count),0) requests,
	COALESCE(SUM(CASE WHEN status BETWEEN 200 AND 299 THEN count ELSE 0 END),0) status2xx,
	COALESCE(SUM(CASE WHEN status BETWEEN 300 AND 399 THEN count ELSE 0 END),0) status3xx,
	COALESCE(SUM(CASE WHEN status BETWEEN 400 AND 499 THEN count ELSE 0 END),0) status4xx,
	COALESCE(SUM(CASE WHEN status BETWEEN 500 AND 599 THEN count ELSE 0 END),0) status5xx,
	COALESCE(SUM(request_time_sum_ms),0) request_time_sum_ms,
	COALESCE(MAX(request_time_max_ms),0) max_request_ms,
	COALESCE(SUM(upstream_time_sum_ms),0) upstream_time_sum_ms,
	COALESCE(SUM(upstream_time_count),0) upstream_time_count,
	COALESCE(SUM(bytes_sent),0) bytes_sent,
	COALESCE(SUM(request_id_present),0) request_id_present`

func (m *Monitor) nginxSources(ctx context.Context, now int64) []NginxEdgeSource {
	if len(m.cfg.NginxAllowedNodes) == 0 {
		return nil
	}
	var states []NginxSourceState
	warnReadErr("nginx source states", m.storeDB.WithContext(ctx).Where("node IN ?", m.cfg.NginxAllowedNodes).Order("node").Find(&states))
	byNode := make(map[string]NginxSourceState, len(states))
	for _, state := range states {
		byNode[state.Node] = state
	}
	out := make([]NginxEdgeSource, 0, len(m.cfg.NginxAllowedNodes))
	for _, node := range m.cfg.NginxAllowedNodes {
		state, exists := byNode[node]
		if !exists {
			out = append(out, NginxEdgeSource{Node: node, AgeSec: -1, EventAgeSec: -1, Status: "bad", HealthReasons: []string{"source_missing"}})
			continue
		}
		age := now - state.LastIngestTs
		if age < 0 {
			age = 0
		}
		eventAge := now - state.LastEventTs
		if eventAge < 0 {
			eventAge = 0
		}
		status := "ok"
		reasons := make([]string, 0, 3)
		if age > 180 {
			status = "warn"
			reasons = append(reasons, "heartbeat_stale")
		}
		if age > 900 {
			status = "bad"
		}
		if state.BacklogKnown && state.BacklogBytes >= nginxBacklogWarnBytes {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "backlog_large")
		}
		if state.BacklogKnown && state.BacklogBytes > 0 && state.LastEventTs > 0 && eventAge > nginxEventLagWarnSec {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "event_lag_with_backlog")
		}
		if state.LastCursorDiscontinuityAt > 0 && now-state.LastCursorDiscontinuityAt <= nginxRecentDataLossWindowSec {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "recent_cursor_discontinuity")
		}
		if state.LastDiscardedAt > 0 && now-state.LastDiscardedAt <= nginxRecentDataLossWindowSec {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "recent_discarded_lines")
		}
		out = append(out, NginxEdgeSource{
			Node: state.Node, LastEventTs: state.LastEventTs, LastIngestTs: state.LastIngestTs, AgeSec: age,
			EventAgeSec: eventAge, Status: status, HealthReasons: reasons,
			BacklogBytes: state.BacklogBytes, BacklogKnown: state.BacklogKnown,
			CursorDiscontinuities: state.CursorDiscontinuities, LastCursorDiscontinuityAt: state.LastCursorDiscontinuityAt,
			DiscardedLines: state.DiscardedLines, LastDiscardedAt: state.LastDiscardedAt,
		})
	}
	return out
}

func (m *Monitor) nginxSourceSummary(ctx context.Context, now int64) (connected bool, healthy, total int, lastTs int64, requestIDCoverage *float64) {
	if !m.cfg.NginxEnabled {
		return false, 0, 0, 0, nil
	}
	sources := m.nginxSources(ctx, now)
	total = len(sources)
	connected = total > 0
	for _, source := range sources {
		if source.Status == "ok" {
			healthy++
		} else {
			connected = false
		}
		if source.LastEventTs > lastTs {
			lastTs = source.LastEventTs
		}
	}
	var row struct{ Requests, Present int64 }
	warnReadErr("nginx request id coverage", m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(SUM(count),0) requests, COALESCE(SUM(request_id_present),0) present FROM nginx_minute_samples WHERE bucket_ts >= ?`, now-86400).Scan(&row))
	if row.Requests > 0 {
		v := float64(row.Present) / float64(row.Requests) * 100
		requestIDCoverage = &v
	}
	return connected, healthy, total, lastTs, requestIDCoverage
}

func (m *Monitor) serveNginxEdge(c *gin.Context) {
	now := time.Now()
	retentionDays := m.nginxRetentionDays()
	if !m.cfg.NginxEnabled {
		c.JSON(http.StatusOK, NginxEdgeReport{Enabled: false, RetentionDays: retentionDays, GeneratedAt: now.Unix()})
		return
	}
	scope, err := stabilityRange(c, now, retentionDays)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 5*time.Second)
	defer cancel()
	report := NginxEdgeReport{Enabled: true, RetentionDays: retentionDays, GeneratedAt: now.Unix(), From: time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02"), To: time.Unix(scope.ToTs-1, 0).In(cstLocation).Format("2006-01-02")}
	whereArgs := []any{scope.FromTs, nginxQueryToTs(scope, now.Unix())}
	var total NginxEdgeAggregate
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT `+nginxAggregateColumns+` FROM nginx_minute_samples WHERE bucket_ts >= ? AND bucket_ts < ?`, whereArgs...).Scan(&total).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取入口聚合失败"})
		return
	}
	report.Summary = total.summary()
	var daily []struct {
		Date string
		NginxEdgeAggregate
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT strftime('%Y-%m-%d', bucket_ts, 'unixepoch', '+8 hours') date, `+nginxAggregateColumns+` FROM nginx_minute_samples WHERE bucket_ts >= ? AND bucket_ts < ? GROUP BY date ORDER BY date`, whereArgs...).Scan(&daily).Error; err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取入口日趋势失败"})
		return
	}
	for _, row := range daily {
		report.Daily = append(report.Daily, NginxEdgeDay{Date: row.Date, NginxEdgeSummary: row.summary()})
	}
	queryBreakdown := func(column string) ([]NginxEdgeBreakdown, error) {
		var rows []struct {
			Name                           string
			Requests, Status4xx, Status5xx int64
			SumMS                          int64
		}
		q := `SELECT ` + column + ` name, COALESCE(SUM(count),0) requests,
			COALESCE(SUM(CASE WHEN status BETWEEN 400 AND 499 THEN count ELSE 0 END),0) status4xx,
			COALESCE(SUM(CASE WHEN status BETWEEN 500 AND 599 THEN count ELSE 0 END),0) status5xx,
			COALESCE(SUM(request_time_sum_ms),0) sum_ms FROM nginx_minute_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? GROUP BY ` + column + ` ORDER BY requests DESC LIMIT 50`
		if err := m.storeDB.WithContext(ctx).Raw(q, whereArgs...).Scan(&rows).Error; err != nil {
			return nil, err
		}
		out := make([]NginxEdgeBreakdown, 0, len(rows))
		for _, row := range rows {
			item := NginxEdgeBreakdown{Name: row.Name, Requests: row.Requests, Status4xx: row.Status4xx, Status5xx: row.Status5xx}
			if row.Requests > 0 {
				item.AvgMS = float64(row.SumMS) / float64(row.Requests)
			}
			out = append(out, item)
		}
		return out, nil
	}
	if report.Routes, err = queryBreakdown("route"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取入口路径失败"})
		return
	}
	if report.Nodes, err = queryBreakdown("node"); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": "读取入口节点失败"})
		return
	}
	report.Sources = m.nginxSources(ctx, now.Unix())
	c.JSON(http.StatusOK, report)
}

// nginxQueryToTs keeps the report's half-open interval stable at an exact
// minute boundary. Nginx samples are minute buckets: when now is HH:MM:00,
// bucket_ts equals scope.ToTs and would otherwise disappear for one second.
func nginxQueryToTs(scope stabilityScope, nowUnix int64) int64 {
	if scope.ToTs == nowUnix {
		return scope.ToTs + 1
	}
	return scope.ToTs
}
