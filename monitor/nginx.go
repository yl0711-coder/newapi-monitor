package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

var errNginxBatchConflict = errors.New("nginx batch id reused with different payload")

// nginxBatchPayloadHash hashes only the validated, merged aggregate rows. Collector
// telemetry is intentionally excluded because backlog may grow while the same source
// batch is retried. The server computes this value; callers cannot choose it.
func nginxBatchPayloadHash(rows []NginxMinuteSample) string {
	canonical := append([]NginxMinuteSample(nil), rows...)
	sort.Slice(canonical, func(i, j int) bool {
		a, b := canonical[i], canonical[j]
		if a.BucketTs != b.BucketTs {
			return a.BucketTs < b.BucketTs
		}
		if a.Node != b.Node {
			return a.Node < b.Node
		}
		if a.Route != b.Route {
			return a.Route < b.Route
		}
		if a.Method != b.Method {
			return a.Method < b.Method
		}
		if a.Status != b.Status {
			return a.Status < b.Status
		}
		return a.UpstreamStatus < b.UpstreamStatus
	})
	payload, _ := json.Marshal(canonical)
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// validateNginxSettings 在进程启动前拦住“页面显示已启用，但接收口永远不可用”
// 的半配置状态。默认关闭时不增加任何新启动条件。
func validateNginxSettings(s Settings) error {
	if s.NginxErrorEnabled && !s.NginxEnabled {
		return fmt.Errorf("启用 Nginx error 聚合时必须同时开启 MONITOR_NGINX_ENABLED")
	}
	if s.NginxSourceV2Enabled && !s.NginxEnabled {
		return fmt.Errorf("启用 Nginx source v2 时必须同时开启 MONITOR_NGINX_ENABLED")
	}
	if s.NginxSourceV2CutoverEnabled && !s.NginxSourceV2Enabled {
		return fmt.Errorf("Nginx source v2 cutover 只能在 source v2 schema/prepare 已启用时开放")
	}
	if !s.NginxSourceV2Enabled && (len(s.NginxSourceV2AllowedNodes) > 0 || len(s.NginxSourceV2AllowedLanes) > 0) {
		return fmt.Errorf("Nginx source v2 白名单只能在 v2 总开关开启时配置")
	}
	evidenceMode := nginxEvidenceMode(s.NginxEvidenceMode)
	if strings.TrimSpace(s.NginxEvidenceMode) != "" && evidenceMode == "off" && !strings.EqualFold(strings.TrimSpace(s.NginxEvidenceMode), "off") {
		return fmt.Errorf("MONITOR_NGINX_EVIDENCE_MODE 只允许 off、pilot 或 verified")
	}
	if evidenceMode != "off" {
		if !s.NginxEnabled {
			return fmt.Errorf("启用 Nginx evidence 时必须同时开启 MONITOR_NGINX_ENABLED")
		}
		if strings.TrimSpace(s.NginxEvidenceStorePath) == "" {
			return fmt.Errorf("启用 Nginx evidence 时必须显式配置独立证据库路径")
		}
		if s.NginxEvidenceRetentionHours < 24 || s.NginxEvidenceRetentionHours > 24*31 {
			return fmt.Errorf("MONITOR_NGINX_EVIDENCE_RETENTION_HOURS 必须在 24～744 小时之间")
		}
		if s.NginxEvidenceMaxMiB < 64 || s.NginxEvidenceMaxMiB > 2048 {
			return fmt.Errorf("MONITOR_NGINX_EVIDENCE_MAX_MIB 必须在 64～2048 之间")
		}
		if len(s.NginxEvidenceHMACKey) < 32 || !nginxEvidenceKeyIDPattern.MatchString(s.NginxEvidenceHMACKeyID) {
			return fmt.Errorf("Nginx evidence HMAC 密钥至少 32 字节，且 key id 必须合法")
		}
		if s.NginxEvidenceHMACKey == s.IngestToken || s.NginxEvidenceHMACKey == s.SessionSecret || s.NginxEvidenceHMACKey == s.UpstreamCredentialSecret {
			return fmt.Errorf("Nginx evidence HMAC 密钥必须独立于登录、ingest 和上游凭据")
		}
		prevKey, prevID := s.NginxEvidencePreviousHMACKey, s.NginxEvidencePreviousHMACKeyID
		if (prevKey == "") != (prevID == "") || prevKey != "" && (len(prevKey) < 32 || !nginxEvidenceKeyIDPattern.MatchString(prevID) || prevID == s.NginxEvidenceHMACKeyID || prevKey == s.NginxEvidenceHMACKey) {
			return fmt.Errorf("Nginx evidence 上一把 HMAC 密钥和 key id 必须成对、合法且不同于当前密钥")
		}
	}
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
	if s.NginxSourceV2Enabled {
		if len(s.NginxSourceV2AllowedNodes) == 0 {
			return fmt.Errorf("启用 Nginx source v2 时必须显式配置逐节点白名单")
		}
		v2Seen := make(map[string]struct{}, len(s.NginxSourceV2AllowedNodes))
		for _, raw := range s.NginxSourceV2AllowedNodes {
			node := strings.TrimSpace(raw)
			if _, ok := seen[node]; !ok {
				return fmt.Errorf("Nginx source v2 节点必须属于 MONITOR_NGINX_ALLOWED_NODES")
			}
			if _, exists := v2Seen[node]; exists {
				return fmt.Errorf("MONITOR_NGINX_SOURCE_V2_ALLOWED_NODES 包含重复节点")
			}
			v2Seen[node] = struct{}{}
		}
		if len(s.NginxSourceV2AllowedLanes) == 0 {
			return fmt.Errorf("启用 Nginx source v2 时必须显式配置逐 lane 白名单")
		}
		laneSeen := make(map[string]struct{}, len(s.NginxSourceV2AllowedLanes))
		for _, lane := range s.NginxSourceV2AllowedLanes {
			canonical := strings.TrimSpace(lane)
			parts := strings.Split(canonical, ":")
			if len(parts) != 2 || (parts[1] != "access" && parts[1] != "error") {
				return fmt.Errorf("MONITOR_NGINX_SOURCE_V2_ALLOWED_LANES 只允许 node:access 或 node:error")
			}
			if _, ok := v2Seen[parts[0]]; !ok {
				return fmt.Errorf("Nginx source v2 lane 节点必须属于逐节点白名单")
			}
			if _, exists := laneSeen[canonical]; exists {
				return fmt.Errorf("MONITOR_NGINX_SOURCE_V2_ALLOWED_LANES 包含重复 lane")
			}
			laneSeen[canonical] = struct{}{}
		}
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
	LatencyCount      int64  `json:"latency_count"`
	Latency0To1s      int64  `json:"latency_0_1s"`
	Latency1To5s      int64  `json:"latency_1_5s"`
	Latency5To15s     int64  `json:"latency_5_15s"`
	Latency15To30s    int64  `json:"latency_15_30s"`
	Latency30To60s    int64  `json:"latency_30_60s"`
	LatencyOver60s    int64  `json:"latency_over_60s"`
}

type nginxIngestRequest struct {
	Node                      string                 `json:"node"`
	BatchID                   string                 `json:"batch_id"`
	Samples                   []nginxIngestSample    `json:"samples"`
	BacklogBytes              int64                  `json:"backlog_bytes"`
	BacklogKnown              bool                   `json:"backlog_known"`
	CursorDiscontinuities     int64                  `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt int64                  `json:"last_cursor_discontinuity_at"`
	DiscardedLines            int64                  `json:"discarded_lines"`
	LastDiscardedAt           int64                  `json:"last_discarded_at"`
	EvidencePersistFailures   int64                  `json:"evidence_persist_failures"`
	EvidenceDroppedEvents     int64                  `json:"evidence_dropped_events"`
	SourceBoundary            *nginxSourceBoundaryV1 `json:"source_boundary,omitempty"`
	SourceRangeV2             *nginxSourceRangeV2    `json:"source_range_v2,omitempty"`
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

func (m *Monitor) nginxSourceV2NodeAllowed(node string) bool {
	if !m.cfg.NginxSourceV2Enabled || (!m.cfg.NginxSourceV2CutoverEnabled && !m.nginxSourceV2Active.Load()) {
		return false
	}
	for _, allowed := range m.cfg.NginxSourceV2AllowedNodes {
		if node == allowed {
			return true
		}
	}
	return false
}

func (m *Monitor) nginxSourceV2LaneAllowed(node, kind string) bool {
	if !m.nginxSourceV2NodeAllowed(node) || (kind != "access" && kind != "error") {
		return false
	}
	want := node + ":" + kind
	for _, allowed := range m.cfg.NginxSourceV2AllowedLanes {
		if strings.TrimSpace(allowed) == want {
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
	latencySum := sample.Latency0To1s + sample.Latency1To5s + sample.Latency5To15s + sample.Latency15To30s + sample.Latency30To60s + sample.LatencyOver60s
	if sample.LatencyCount < 0 || sample.LatencyCount > sample.Count || latencySum < 0 ||
		(sample.LatencyCount == 0 && latencySum != 0) || (sample.LatencyCount > 0 && (latencySum != sample.LatencyCount || sample.LatencyCount != sample.Count)) {
		return NginxMinuteSample{}, fmt.Errorf("invalid latency histogram")
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
		LatencyCount: sample.LatencyCount, Latency0To1s: sample.Latency0To1s, Latency1To5s: sample.Latency1To5s,
		Latency5To15s: sample.Latency5To15s, Latency15To30s: sample.Latency15To30s,
		Latency30To60s: sample.Latency30To60s, LatencyOver60s: sample.LatencyOver60s,
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
		{&dst.LatencyCount, src.LatencyCount}, {&dst.Latency0To1s, src.Latency0To1s}, {&dst.Latency1To5s, src.Latency1To5s},
		{&dst.Latency5To15s, src.Latency5To15s}, {&dst.Latency15To30s, src.Latency15To30s},
		{&dst.Latency30To60s, src.Latency30To60s}, {&dst.LatencyOver60s, src.LatencyOver60s},
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
	latencySum := dst.Latency0To1s + dst.Latency1To5s + dst.Latency5To15s + dst.Latency15To30s + dst.Latency30To60s + dst.LatencyOver60s
	if dst.LatencyCount > dst.Count || latencySum != dst.LatencyCount || (dst.LatencyCount > 0 && dst.LatencyCount != dst.Count) {
		return fmt.Errorf("merged latency histogram is not conserved")
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
	if err := validateNginxSourceBoundaryV1(in.SourceBoundary); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	if in.SourceBoundary != nil && in.SourceRangeV2 != nil || in.SourceRangeV2 != nil && !validNginxSourceRangeV2(*in.SourceRangeV2, "access") {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid or ambiguous source range"})
		return
	}
	if in.SourceRangeV2 != nil && !m.nginxSourceV2LaneAllowed(in.Node, "access") {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx source v2 is not enabled for this node"})
		return
	}
	if in.SourceBoundary != nil && in.SourceBoundary.Checkpoint && len(in.Samples) != 0 {
		c.JSON(http.StatusBadRequest, gin.H{"error": "source checkpoint must not contain samples"})
		return
	}
	if in.SourceBoundary != nil && !m.nginxSourceV2SchemaReady.Load() {
		c.JSON(http.StatusServiceUnavailable, gin.H{"error": "nginx source continuity preparation is not enabled"})
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
	if in.EvidencePersistFailures < 0 || in.EvidencePersistFailures > 1 || in.EvidenceDroppedEvents < 0 || in.EvidenceDroppedEvents > 2000 ||
		(in.EvidencePersistFailures == 0 && in.EvidenceDroppedEvents != 0) {
		c.JSON(http.StatusBadRequest, gin.H{"error": "invalid evidence failure telemetry"})
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
	payloadHash := nginxBatchPayloadHash(rows)
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		sourceDuplicate := false
		if in.SourceRangeV2 != nil {
			var err error
			sourceDuplicate, err = applyNginxSourceRangeV2(tx, in.Node, "access", in.BatchID, payloadHash, *in.SourceRangeV2, now)
			if err != nil {
				return err
			}
		} else if m.cfg.NginxSourceV2Enabled || m.nginxSourceV2SchemaReady.Load() {
			allowed, err := legacyNginxSourceAllowed(tx, in.Node, "access")
			if err != nil {
				return err
			}
			if !allowed {
				return errNginxLegacyAfterV2
			}
		}
		batch := NginxIngestBatch{Node: in.Node, BatchID: in.BatchID, PayloadHash: payloadHash, FirstTs: firstTs, LastTs: lastTs, Rows: len(rows), ReceivedAt: now}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			var existing NginxIngestBatch
			if err := tx.First(&existing, "node = ? AND batch_id = ?", in.Node, in.BatchID).Error; err != nil {
				return err
			}
			// Rows written by older releases have an empty hash. Treat them as an
			// already accepted legacy batch rather than inventing a payload claim.
			// New rows are always hashed and conflicting reuse is rejected.
			if existing.PayloadHash != "" && existing.PayloadHash != payloadHash {
				return errNginxBatchConflict
			}
			if in.SourceRangeV2 != nil && !sourceDuplicate {
				return errNginxSourceConflict
			}
			if in.SourceBoundary != nil {
				if existing.PayloadHash == "" {
					return errNginxBatchConflict
				}
				if err := applyNginxSourceBoundaryV1(tx, in.Node, "access", in.BatchID, payloadHash, *in.SourceBoundary, true, now); err != nil {
					return err
				}
			}
			duplicate = true
			return nil
		}
		if sourceDuplicate {
			return errNginxSourceConflict
		}
		if in.SourceBoundary != nil {
			if err := applyNginxSourceBoundaryV1(tx, in.Node, "access", in.BatchID, payloadHash, *in.SourceBoundary, false, now); err != nil {
				return err
			}
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
					"latency_count":        gorm.Expr("latency_count + excluded.latency_count"),
					"latency0_to1s":        gorm.Expr("latency0_to1s + excluded.latency0_to1s"),
					"latency1_to5s":        gorm.Expr("latency1_to5s + excluded.latency1_to5s"),
					"latency5_to15s":       gorm.Expr("latency5_to15s + excluded.latency5_to15s"),
					"latency15_to30s":      gorm.Expr("latency15_to30s + excluded.latency15_to30s"),
					"latency30_to60s":      gorm.Expr("latency30_to60s + excluded.latency30_to60s"),
					"latency_over60s":      gorm.Expr("latency_over60s + excluded.latency_over60s"),
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
			EvidencePersistFailures: in.EvidencePersistFailures, EvidenceDroppedEvents: in.EvidenceDroppedEvents,
		}
		if in.EvidencePersistFailures > 0 {
			state.LastEvidencePersistFailureAt = now
		}
		updates := map[string]any{
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
			"discarded_lines":                  gorm.Expr("MAX(COALESCE(discarded_lines, 0), excluded.discarded_lines)"),
			"last_discarded_at":                gorm.Expr("MAX(COALESCE(last_discarded_at, 0), excluded.last_discarded_at)"),
			"evidence_persist_failures":        gorm.Expr("COALESCE(evidence_persist_failures, 0) + excluded.evidence_persist_failures"),
			"evidence_dropped_events":          gorm.Expr("COALESCE(evidence_dropped_events, 0) + excluded.evidence_dropped_events"),
			"last_evidence_persist_failure_at": gorm.Expr("MAX(COALESCE(last_evidence_persist_failure_at, 0), excluded.last_evidence_persist_failure_at)"),
		}
		return tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "node"}},
			DoUpdates: clause.Assignments(updates),
		}).Create(&state).Error
	})
	if err != nil {
		if errors.Is(err, errNginxLegacyAfterV2) {
			c.JSON(http.StatusConflict, gin.H{"error": "collector protocol v2 is active; legacy ingest is closed"})
			return
		}
		if errors.Is(err, errNginxBatchConflict) {
			c.JSON(http.StatusConflict, gin.H{"error": "batch id conflict"})
			return
		}
		if errors.Is(err, errNginxSourceConflict) || errors.Is(err, errNginxSourceGap) || errors.Is(err, errNginxSourceOverlap) || errors.Is(err, errNginxSourceEpoch) || errors.Is(err, errNginxSourceUnregistered) {
			c.JSON(http.StatusConflict, gin.H{"error": err.Error()})
			return
		}
		slog.Warn("Nginx 聚合入库失败", "node", in.Node, "err", err)
		c.JSON(http.StatusInternalServerError, gin.H{"error": "store failed"})
		return
	}
	response := gin.H{"ok": true, "duplicate": duplicate, "stored": len(rows)}
	if ack := nginxSourceBoundaryAckForV1(in.SourceBoundary, in.BatchID); ack != nil {
		response["source_boundary_ack"] = ack
	}
	if ack := nginxSourceCommitAckForV2(in.SourceRangeV2, in.BatchID); ack != nil {
		response["source_ack_v2"] = ack
	}
	c.JSON(http.StatusOK, response)
}

func (m *Monitor) startNginxMaintenance(ctx context.Context) {
	prune := func() {
		now := time.Now()
		days := m.nginxRetentionDays()
		cutoff := now.Unix()/60*60 - int64(days)*86400
		if err := m.storeDB.Where("bucket_ts < ?", cutoff).Delete(&NginxMinuteSample{}).Error; err != nil {
			slog.Warn("清理 Nginx 聚合失败", "err", err)
		}
		if err := m.storeDB.Where("received_at < ?", cutoff).Delete(&NginxIngestBatch{}).Error; err != nil {
			slog.Warn("清理 Nginx 幂等批次失败", "err", err)
		}
		if err := m.storeDB.Where("bucket_ts < ?", cutoff).Delete(&NginxErrorMinuteSample{}).Error; err != nil {
			slog.Warn("清理 Nginx error 聚合失败", "err", err)
		}
		if err := m.storeDB.Where("received_at < ?", cutoff).Delete(&NginxErrorIngestBatch{}).Error; err != nil {
			slog.Warn("清理 Nginx error 幂等批次失败", "err", err)
		}
		if err := m.pruneNginxSourceBoundaryV1BatchesOnce(ctx, cutoff); err != nil {
			slog.Warn("清理 Nginx v1 source 边界幂等批次失败", "err", err)
		}
		if err := m.pruneNginxSourceV2CommitsOnce(ctx, now); err != nil {
			slog.Warn("清理 Nginx source v2 提交证据失败", "err", err)
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

func (m *Monitor) pruneNginxSourceBoundaryV1BatchesOnce(ctx context.Context, cutoff int64) error {
	if m.storeDB == nil || !m.nginxSourceV2SchemaReady.Load() {
		return nil
	}
	return m.storeDB.WithContext(ctx).Exec(`DELETE FROM nginx_source_boundary_batch_v1 WHERE rowid IN (
		SELECT rowid FROM nginx_source_boundary_batch_v1 WHERE received_at < ? ORDER BY received_at LIMIT 10000
	)`, cutoff).Error
}

// pruneNginxSourceV2CommitsOnce bounds the permanent data/evidence join.
// The row is retained for at least the aggregate window and for the complete
// evidence acceptance window (retention + 24h late-delivery allowance). One
// small chunk per maintenance pass avoids a long SQLite writer lock.
func (m *Monitor) pruneNginxSourceV2CommitsOnce(ctx context.Context, now time.Time) error {
	if m.storeDB == nil || !m.nginxSourceV2SchemaReady.Load() {
		return nil
	}
	hours := m.nginxRetentionDays() * 24
	evidenceHours := m.cfg.NginxEvidenceRetentionHours
	if evidenceHours < 24 || evidenceHours > 24*31 {
		evidenceHours = 168
	}
	if evidenceHours > hours {
		hours = evidenceHours
	}
	cutoff := now.Add(-time.Duration(hours+24) * time.Hour).Unix()
	return m.storeDB.WithContext(ctx).Exec(`DELETE FROM nginx_source_commit_v2 WHERE rowid IN (
		SELECT rowid FROM nginx_source_commit_v2 WHERE received_at < ? ORDER BY received_at LIMIT 10000
	)`, cutoff).Error
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
	LatencyCoverage   float64 `json:"latency_coverage"`
	P95RequestMS      int64   `json:"p95_request_ms"`
	P99RequestMS      int64   `json:"p99_request_ms"`
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
	Node                         string   `json:"node"`
	LastEventTs                  int64    `json:"last_event_ts"`
	LastIngestTs                 int64    `json:"last_ingest_ts"`
	AgeSec                       int64    `json:"age_sec"`
	EventAgeSec                  int64    `json:"event_age_sec"`
	Status                       string   `json:"status"`
	HealthReasons                []string `json:"health_reasons,omitempty"`
	BacklogBytes                 int64    `json:"backlog_bytes"`
	BacklogKnown                 bool     `json:"backlog_known"`
	CursorDiscontinuities        int64    `json:"cursor_discontinuities"`
	LastCursorDiscontinuityAt    int64    `json:"last_cursor_discontinuity_at"`
	DiscardedLines               int64    `json:"discarded_lines"`
	LastDiscardedAt              int64    `json:"last_discarded_at"`
	EvidencePersistFailures      int64    `json:"evidence_persist_failures"`
	EvidenceDroppedEvents        int64    `json:"evidence_dropped_events"`
	LastEvidencePersistFailureAt int64    `json:"last_evidence_persist_failure_at"`
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
	Errors        []NginxErrorSummary  `json:"errors,omitempty"`
	ErrorSources  []NginxErrorSource   `json:"error_sources,omitempty"`
}

type NginxEdgeAggregate struct {
	Requests, Status2xx, Status3xx, Status4xx, Status5xx int64
	RequestTimeSumMS, MaxRequestMS                       int64
	UpstreamTimeSumMS, UpstreamTimeCount                 int64
	BytesSent, RequestIDPresent                          int64
	LatencyCount, Latency0To1s, Latency1To5s             int64
	Latency5To15s, Latency15To30s, Latency30To60s        int64
	LatencyOver60s                                       int64
}

func approximateLatencyPercentile(total int64, percentile float64, buckets ...int64) int64 {
	if total <= 0 || len(buckets) != 6 {
		return 0
	}
	target := int64(math.Ceil(float64(total) * percentile))
	var cumulative int64
	upper := []int64{1000, 5000, 15000, 30000, 60000, 0}
	for i, count := range buckets {
		cumulative += count
		if cumulative >= target {
			return upper[i]
		}
	}
	return 0
}

func (row NginxEdgeAggregate) summary() NginxEdgeSummary {
	out := NginxEdgeSummary{Requests: row.Requests, Status2xx: row.Status2xx, Status3xx: row.Status3xx, Status4xx: row.Status4xx, Status5xx: row.Status5xx, MaxRequestMS: row.MaxRequestMS, BytesSent: row.BytesSent}
	if row.Requests > 0 {
		out.AvgRequestMS = float64(row.RequestTimeSumMS) / float64(row.Requests)
		out.RequestIDCoverage = float64(row.RequestIDPresent) / float64(row.Requests) * 100
	}
	if row.LatencyCount > 0 {
		out.LatencyCoverage = float64(row.LatencyCount) / float64(row.Requests) * 100
		out.P95RequestMS = approximateLatencyPercentile(row.LatencyCount, .95, row.Latency0To1s, row.Latency1To5s, row.Latency5To15s, row.Latency15To30s, row.Latency30To60s, row.LatencyOver60s)
		out.P99RequestMS = approximateLatencyPercentile(row.LatencyCount, .99, row.Latency0To1s, row.Latency1To5s, row.Latency5To15s, row.Latency15To30s, row.Latency30To60s, row.LatencyOver60s)
		if out.P95RequestMS == 0 {
			out.P95RequestMS = row.MaxRequestMS
		}
		if out.P99RequestMS == 0 {
			out.P99RequestMS = row.MaxRequestMS
		}
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
	COALESCE(SUM(request_id_present),0) request_id_present,
	COALESCE(SUM(latency_count),0) latency_count,
	COALESCE(SUM(latency0_to1s),0) latency0_to1s,
	COALESCE(SUM(latency1_to5s),0) latency1_to5s,
	COALESCE(SUM(latency5_to15s),0) latency5_to15s,
	COALESCE(SUM(latency15_to30s),0) latency15_to30s,
	COALESCE(SUM(latency30_to60s),0) latency30_to60s,
	COALESCE(SUM(latency_over60s),0) latency_over60s`

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
		if !state.BacklogKnown {
			status = "warn"
			reasons = append(reasons, "log_or_backlog_unreadable")
		}
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
		if state.LastEvidencePersistFailureAt > 0 && now-state.LastEvidencePersistFailureAt <= nginxRecentDataLossWindowSec {
			if status != "bad" {
				status = "warn"
			}
			reasons = append(reasons, "recent_evidence_persist_failure")
		}
		out = append(out, NginxEdgeSource{
			Node: state.Node, LastEventTs: state.LastEventTs, LastIngestTs: state.LastIngestTs, AgeSec: age,
			EventAgeSec: eventAge, Status: status, HealthReasons: reasons,
			BacklogBytes: state.BacklogBytes, BacklogKnown: state.BacklogKnown,
			CursorDiscontinuities: state.CursorDiscontinuities, LastCursorDiscontinuityAt: state.LastCursorDiscontinuityAt,
			DiscardedLines: state.DiscardedLines, LastDiscardedAt: state.LastDiscardedAt,
			EvidencePersistFailures: state.EvidencePersistFailures, EvidenceDroppedEvents: state.EvidenceDroppedEvents,
			LastEvidencePersistFailureAt: state.LastEvidencePersistFailureAt,
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
	queryToTs := nginxQueryToTs(scope, now.Unix())
	whereArgs := []any{scope.FromTs, queryToTs}
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
	if m.cfg.NginxErrorEnabled {
		report.Errors = m.nginxErrorSummary(ctx, scope.FromTs, queryToTs)
		report.ErrorSources = m.nginxErrorSources(ctx, now.Unix())
	}
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
