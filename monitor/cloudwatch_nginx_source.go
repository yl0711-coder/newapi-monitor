package monitor

// CloudWatch Nginx 连续采集。
//
// 这条 lane 只读取固定的结构化 access/标准 error.log 查询，把完整窗口聚合
// 到现有分钟事实表。原始日志、IP、Header、请求/响应正文和 Request ID 原值
// 均不落盘。水位只在 access/error 同一窗口原子发布成功后推进。

import (
	"github.com/yl0711-coder/newapi-monitor/internal/metering"

	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	cloudWatchNginxCursorID      = uint(1)
	cloudWatchNginxVersion       = 1
	cloudWatchNginxNode          = "cloudwatch-direct"
	cloudWatchNginxFinalizeDelay = 2 * time.Minute
	cloudWatchNginxReplay        = 20 * time.Minute
	cloudWatchNginxWindow        = time.Hour
	cloudWatchNginxRepairHorizon = 72 * time.Hour
	cloudWatchNginxRepairEvery   = 20 * time.Minute
	// 请求级入口证据单独补扫整个 evidence retention 窗口。它不能复用
	// 分钟聚合的 72 小时修复水位：evidence 是后来启用时，主聚合水位可能
	// 已经 caught_up，若共用水位就永远不会补回旧请求。
	// Keep the evidence backfill chunk within the fixed CloudWatch query
	// validator's maximum range.  A larger chunk fails before AWS is queried
	// (and would leave the evidence cursor permanently stuck).
	cloudWatchNginxEvidenceChunkWindow = cloudWatchLogsMaxWindow
	cloudWatchNginxCatchupDelay        = 2 * time.Second
	cloudWatchNginxQueryBudget         = 2 * time.Minute
)

// CloudWatchNginxCursor 是 access/error 共用的连续覆盖水位。ThroughTs 之前
// 的窗口均已完整查询、解析并原子发布；失败时 NextTs 保持不动。
type CloudWatchNginxCursor struct {
	ID                uint   `gorm:"primaryKey;autoIncrement:false"`
	CoverageFromTs    int64  `gorm:"column:coverage_from_ts"`
	NextTs            int64  `gorm:"column:next_ts;index"`
	ThroughTs         int64  `gorm:"column:through_ts"`
	TargetThroughTs   int64  `gorm:"column:target_through_ts"`
	SemanticsVersion  int    `gorm:"column:semantics_version;index"`
	Status            string `gorm:"size:24;index"`
	LastSuccessAt     int64  `gorm:"column:last_success_at"`
	LastFailureAt     int64  `gorm:"column:last_failure_at"`
	RepairNextTs      int64  `gorm:"column:repair_next_ts;index"`
	LastRepairAt      int64  `gorm:"column:last_repair_at"`
	LastError         string `gorm:"size:64;column:last_error"`
	ReadWindowSeconds int64  `gorm:"not null;default:0" json:"-"`
	// Historical repair keeps its own read width; a slow old hour must not
	// throttle the primary lane or restart at the same full hour forever.
	RepairReadWindowSeconds int64 `gorm:"not null;default:0" json:"-"`
	ReplayNextTs            int64
	ReplayThroughTs         int64
	ReplayLastAt            int64
	ReplayReadWindowSeconds int64 `gorm:"not null;default:0" json:"-"`
	// 请求级 HMAC evidence 的独立补扫水位。它与分钟聚合的 NextTs/RepairNextTs
	// 分开，允许在主聚合已追平后把 evidence retention 内的历史补回来。
	EvidenceCoverageFromTs int64  `gorm:"column:evidence_coverage_from_ts"`
	EvidenceNextTs         int64  `gorm:"column:evidence_next_ts;index"`
	EvidenceThroughTs      int64  `gorm:"column:evidence_through_ts"`
	EvidenceStatus         string `gorm:"size:24;column:evidence_status;index"`
	EvidenceLastSuccessAt  int64  `gorm:"column:evidence_last_success_at"`
	EvidenceLastFailureAt  int64  `gorm:"column:evidence_last_failure_at"`
	EvidenceLastError      string `gorm:"size:64;column:evidence_last_error"`
	// Execution timeouts narrow only the next read, never the covered interval.
	// Keep the successful smaller chunk across retries and restarts. An unknown
	// legacy timeout is not enough evidence to select a smaller range.
	EvidenceReadWindowSeconds int64  `gorm:"not null;default:0" json:"-"`
	EvidenceStoreID           string `gorm:"size:64;column:evidence_store_id"`
	EvidenceGapFromTs         int64  `gorm:"column:evidence_gap_from_ts"`
	EvidenceGapToTs           int64  `gorm:"column:evidence_gap_to_ts"`
	EvidenceGapDetectedAt     int64  `gorm:"column:evidence_gap_detected_at"`
	EvidenceGapReason         string `gorm:"size:64;column:evidence_gap_reason"`
	UpdatedAt                 int64  `gorm:"column:updated_at"`
}

func cloudWatchNginxRange(now time.Time, lookbackHours int) (int64, int64) {
	if now.IsZero() {
		now = time.Now()
	}
	through := now.Add(-cloudWatchNginxFinalizeDelay).Unix() / 60 * 60
	from := through - int64(lookbackHours)*3600
	if from < 0 {
		from = 0
	}
	return from / 60 * 60, through
}

// Read width is a retry checkpoint, not a coverage proof. Never move the left
// boundary on timeout. Recent replay has a separate durable cursor.
func cloudWatchNginxNextWindow(state CloudWatchNginxCursor, target int64) (int64, int64) {
	from := state.NextTs
	width := state.ReadWindowSeconds
	if width <= 0 || width > int64(cloudWatchNginxWindow/time.Second) {
		width = int64(cloudWatchNginxWindow / time.Second)
	}
	return from, min(target, from+width)
}

func cloudWatchNginxEvidenceRange(now time.Time, retentionHours int) (int64, int64) {
	if now.IsZero() {
		now = time.Now()
	}
	if retentionHours < 24 {
		retentionHours = 24
	}
	if retentionHours > 744 {
		retentionHours = 744
	}
	through := now.Add(-cloudWatchNginxFinalizeDelay).Unix() / 60 * 60
	// Start at the next retained whole minute, not two minutes before the
	// retention cutoff (the finalized target already includes a delay).
	from := (now.UnixMilli() - int64(retentionHours)*3_600_000 + 59_999) / 60_000 * 60
	if from < 0 {
		from = 0
	}
	return from / 60 * 60, through
}

func cloudWatchNginxEvidenceWindow(state CloudWatchNginxCursor, target int64) (int64, int64, bool) {
	if state.EvidenceStatus == "disabled" {
		return 0, 0, false
	}
	from := state.EvidenceNextTs
	if from < state.EvidenceCoverageFromTs {
		from = state.EvidenceCoverageFromTs
	}
	if from >= target {
		return 0, 0, false
	}
	width := state.EvidenceReadWindowSeconds
	if width <= 0 || width > int64(cloudWatchNginxEvidenceChunkWindow/time.Second) {
		width = int64(cloudWatchNginxEvidenceChunkWindow / time.Second)
	}
	to := from + width
	if to > target {
		to = target
	}
	return from, to, to > from
}

func narrowCloudWatchNginxEvidenceRead(state *CloudWatchNginxCursor, attemptedSeconds int64, err error) {
	if state == nil || cloudWatchNginxTimeoutStage(err) != "execution" {
		return
	}
	// Retry the same left boundary with fewer minutes. Do not increase the
	// two-minute query budget, issue an extra query here, or publish coverage.
	state.EvidenceReadWindowSeconds = max(int64(60), min(int64(15*60), attemptedSeconds/2/60*60))
}

// cloudWatchNginxRepairBounds returns only fully closed UTC hours. The moving
// 72-hour range is intentionally independent from the real-time replay window:
// it catches logs that reach CloudWatch after the normal 20-minute replay.
func cloudWatchNginxRepairBounds(state CloudWatchNginxCursor, target int64) (int64, int64) {
	end := target / int64(time.Hour/time.Second) * int64(time.Hour/time.Second)
	start := end - int64(cloudWatchNginxRepairHorizon/time.Second)
	if start < state.CoverageFromTs {
		start = (state.CoverageFromTs + int64(time.Hour/time.Second) - 1) /
			int64(time.Hour/time.Second) * int64(time.Hour/time.Second)
	}
	if start < 0 {
		start = 0
	}
	// The rolling repair horizon selects new cycles; it is not a successful
	// checkpoint. Keep an already scheduled interval across clock movement and
	// restarts, so an hour of repeated timeouts cannot silently age past it.
	if state.RepairNextTs > 0 && state.RepairNextTs%60 == 0 && state.RepairNextTs >= state.CoverageFromTs && state.RepairNextTs < start {
		start = state.RepairNextTs
	}
	return start, end
}

func cloudWatchNginxRepairWindow(state CloudWatchNginxCursor, target int64) (int64, int64, bool) {
	start, end := cloudWatchNginxRepairBounds(state, target)
	if end <= start {
		return 0, 0, false
	}
	from := state.RepairNextTs
	if from < start || from >= end || from%60 != 0 {
		from = start
	}
	width := state.RepairReadWindowSeconds
	if width < 60 || width > int64(cloudWatchNginxWindow/time.Second) {
		width = int64(cloudWatchNginxWindow / time.Second)
	}
	// Complete the scheduled UTC hour in contiguous minute-sized pieces.
	// Its last piece can be shorter than the learned transport width.
	to := min(end, from+width/60*60, (from/3600+1)*3600)
	return from, to, to > from
}

func (m *Monitor) loadCloudWatchNginxCursor(now time.Time) (CloudWatchNginxCursor, error) {
	from, target := cloudWatchNginxRange(now, m.cfg.CloudWatchNginxLookbackHours)
	evidenceFrom, evidenceTarget := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	var state CloudWatchNginxCursor
	err := m.storeDB.First(&state, "id = ?", cloudWatchNginxCursorID).Error
	if err == nil && state.SemanticsVersion == cloudWatchNginxVersion {
		// Older versions shared NextTs with split replays. Preserve that exact
		// unfinished replay while restoring the independently proven frontier.
		if state.NextTs > 0 && state.NextTs >= state.CoverageFromTs && state.NextTs < state.ThroughTs && state.ReplayThroughTs <= state.ReplayNextTs {
			state.ReplayNextTs, state.ReplayThroughTs = state.NextTs, state.ThroughTs
			state.ReplayReadWindowSeconds = state.ReadWindowSeconds
			state.NextTs = state.ThroughTs
		}
		state.TargetThroughTs = target
		if state.CoverageFromTs <= 0 {
			state.CoverageFromTs = from
		}
		if state.NextTs < state.CoverageFromTs {
			state.NextTs = state.CoverageFromTs
		}
		if state.ThroughTs < state.CoverageFromTs {
			state.ThroughTs = state.CoverageFromTs
		}
		if err := m.reconcileCloudWatchNginxEvidence(&state, evidenceFrom, evidenceTarget, now); err != nil {
			return CloudWatchNginxCursor{}, err
		}
		repairStart, repairEnd := cloudWatchNginxRepairBounds(state, target)
		if repairEnd > repairStart && (state.RepairNextTs < repairStart || state.RepairNextTs >= repairEnd ||
			state.RepairNextTs%60 != 0) {
			state.RepairNextTs = repairStart
		}
		state.UpdatedAt = now.Unix()
		if saveErr := m.storeDB.Save(&state).Error; saveErr != nil {
			return CloudWatchNginxCursor{}, saveErr
		}
		m.refreshCloudWatchNginxEvidenceRecovery(now)
		return state, nil
	}
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return CloudWatchNginxCursor{}, err
	}
	state = CloudWatchNginxCursor{
		ID: cloudWatchNginxCursorID, CoverageFromTs: from, NextTs: from,
		ThroughTs: from, TargetThroughTs: target, SemanticsVersion: cloudWatchNginxVersion,
		Status: "running", UpdatedAt: now.Unix(),
	}
	if err := m.reconcileCloudWatchNginxEvidence(&state, evidenceFrom, evidenceTarget, now); err != nil {
		return CloudWatchNginxCursor{}, err
	}
	state.RepairNextTs, _ = cloudWatchNginxRepairBounds(state, target)
	// 语义升级只重置 CloudWatch 自己的分钟事实与状态；旧采集器历史保留，
	// 既可回看，也可用于独立验收。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("node = ?", cloudWatchNginxNode).Delete(&NginxMinuteSample{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node = ?", cloudWatchNginxNode).Delete(&NginxErrorMinuteSample{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node = ?", cloudWatchNginxNode).Delete(&NginxSourceState{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node = ?", cloudWatchNginxNode).Delete(&NginxErrorSourceState{}).Error; err != nil {
			return err
		}
		return tx.Save(&state).Error
	}); err != nil {
		return CloudWatchNginxCursor{}, err
	}
	m.refreshCloudWatchNginxEvidenceRecovery(now)
	return state, nil
}

func (m *Monitor) recordCloudWatchNginxFailure(state *CloudWatchNginxCursor, cause error, now int64) {
	m.cloudWatchNginxLastFailure.Store(now)
	if state == nil || m.storeDB == nil {
		return
	}
	state.Status = "degraded"
	state.LastFailureAt = now
	state.LastError = cloudWatchPreRouteErrorCode(cause)
	state.UpdatedAt = now
	_ = m.storeDB.Save(state).Error
}

func (m *Monitor) queryCloudWatchNginxRange(ctx context.Context, parser *cloudWatchEvidenceParser,
	from, to time.Time, queries *int, depth int) ([]cloudWatchStructuredEvidence, error) {
	continuousParser := *parser
	continuousParser.nginxContinuousIdentity = true
	return m.queryCloudWatchCompleteEvidenceRange(ctx, &continuousParser, cwQueryWorkerNginxContinuous,
		cloudWatchLogsNginxLimit, from, to, queries, depth)
}

func cloudWatchNginxLatency(sample *NginxMinuteSample, requestMS int64) {
	sample.LatencyCount = 1
	switch {
	case requestMS <= 1000:
		sample.Latency0To1s = 1
	case requestMS <= 5000:
		sample.Latency1To5s = 1
	case requestMS <= 15000:
		sample.Latency5To15s = 1
	case requestMS <= 30000:
		sample.Latency15To30s = 1
	case requestMS <= 60000:
		sample.Latency30To60s = 1
	default:
		sample.LatencyOver60s = 1
	}
}

func cloudWatchNginxSamples(evidence []cloudWatchStructuredEvidence, from, to int64) ([]NginxMinuteSample, []NginxErrorMinuteSample, int64, int64, error) {
	accessByKey := make(map[string]NginxMinuteSample)
	errorByKey := make(map[string]NginxErrorMinuteSample)
	seen := make(map[string]struct{}, len(evidence))
	var lastAccess, lastError int64
	for _, item := range evidence {
		second := item.EventMS / 1000
		if second < from || second >= to {
			continue
		}
		if item.EventRef == "" {
			return nil, nil, 0, 0, fmt.Errorf("cloudwatch nginx event identity missing")
		}
		if _, exists := seen[item.EventRef]; exists {
			continue
		}
		seen[item.EventRef] = struct{}{}
		bucket := second / 60 * 60
		switch item.Kind {
		case cwEvidenceNginxAccess:
			if item.Status == nil || item.RequestMS == nil || *item.Status < 100 || *item.Status > 599 || *item.RequestMS < 0 {
				return nil, nil, 0, 0, fmt.Errorf("cloudwatch nginx access missing required fields")
			}
			row := NginxMinuteSample{
				BucketTs: bucket, Node: cloudWatchNginxNode, Route: normalizeNginxRoute(item.Route),
				Method: normalizeNginxMethod(item.Method), Status: *item.Status,
				UpstreamStatus: shadowLastUpstreamStatus(item.UpstreamStatuses), Count: 1,
				RequestTimeSumMS: *item.RequestMS, RequestTimeMaxMS: *item.RequestMS,
			}
			if item.UpstreamMS != nil {
				row.UpstreamTimeSumMS, row.UpstreamTimeCount = *item.UpstreamMS, 1
			}
			if item.BytesSent != nil {
				row.BytesSent = *item.BytesSent
			}
			if item.OneAPIIDHMAC != "" {
				row.RequestIDPresent = 1
			}
			cloudWatchNginxLatency(&row, *item.RequestMS)
			key := fmt.Sprintf("%d\x00%s\x00%s\x00%d\x00%d", row.BucketTs, row.Route, row.Method, row.Status, row.UpstreamStatus)
			if current, ok := accessByKey[key]; ok {
				if err := mergeNginxSample(&current, row); err != nil {
					return nil, nil, 0, 0, err
				}
				accessByKey[key] = current
			} else {
				accessByKey[key] = row
			}
			if second > lastAccess {
				lastAccess = second
			}
		case cwEvidenceNginxError:
			category, severity := strings.TrimSpace(item.Category), strings.TrimSpace(item.Severity)
			if !nginxErrorCategories[category] || !nginxErrorSeverities[severity] {
				return nil, nil, 0, 0, fmt.Errorf("cloudwatch nginx error classification invalid")
			}
			key := fmt.Sprintf("%d\x00%s\x00%s", bucket, category, severity)
			row := errorByKey[key]
			row.BucketTs, row.Node, row.Category, row.Severity = bucket, cloudWatchNginxNode, category, severity
			row.Count++
			if row.Count > 10_000_000 {
				return nil, nil, 0, 0, fmt.Errorf("cloudwatch nginx error aggregate exceeds limit")
			}
			errorByKey[key] = row
			if second > lastError {
				lastError = second
			}
		default:
			return nil, nil, 0, 0, fmt.Errorf("cloudwatch nginx query returned unsupported evidence")
		}
	}
	access := make([]NginxMinuteSample, 0, len(accessByKey))
	for _, row := range accessByKey {
		access = append(access, row)
	}
	sort.Slice(access, func(i, j int) bool {
		if access[i].BucketTs != access[j].BucketTs {
			return access[i].BucketTs < access[j].BucketTs
		}
		return fmt.Sprintf("%s\x00%s\x00%d\x00%d", access[i].Route, access[i].Method, access[i].Status, access[i].UpstreamStatus) <
			fmt.Sprintf("%s\x00%s\x00%d\x00%d", access[j].Route, access[j].Method, access[j].Status, access[j].UpstreamStatus)
	})
	errorRows := make([]NginxErrorMinuteSample, 0, len(errorByKey))
	for _, row := range errorByKey {
		errorRows = append(errorRows, row)
	}
	sort.Slice(errorRows, func(i, j int) bool {
		if errorRows[i].BucketTs != errorRows[j].BucketTs {
			return errorRows[i].BucketTs < errorRows[j].BucketTs
		}
		return errorRows[i].Category+"\x00"+errorRows[i].Severity < errorRows[j].Category+"\x00"+errorRows[j].Severity
	})
	return access, errorRows, lastAccess, lastError, nil
}

func (m *Monitor) publishCloudWatchNginxWindowMode(ctx context.Context, state *CloudWatchNginxCursor,
	from, to int64, access []NginxMinuteSample, errorRows []NginxErrorMinuteSample, lastAccess, lastError, target int64,
	repair bool, recoveryWidth ...int64) error {
	return m.publishCloudWatchNginxWindowLane(ctx, state, from, to, access, errorRows, lastAccess, lastError, target, repair, false, recoveryWidth...)
}

func (m *Monitor) publishCloudWatchNginxWindowLane(ctx context.Context, state *CloudWatchNginxCursor,
	from, to int64, access []NginxMinuteSample, errorRows []NginxErrorMinuteSample, lastAccess, lastError, target int64,
	repair, replay bool, recoveryWidth ...int64) error {
	finishedAt := time.Now().Unix()
	next := *state
	next.TargetThroughTs = target
	if replay {
		next.ReplayNextTs = to
		if to >= next.ReplayThroughTs {
			next.ReplayLastAt = finishedAt
		}
		if len(recoveryWidth) > 0 && recoveryWidth[0] > 0 {
			next.ReplayReadWindowSeconds = recoveryWidth[0]
		}
	} else if repair {
		if len(recoveryWidth) > 0 && recoveryWidth[0] > 0 {
			next.RepairReadWindowSeconds = recoveryWidth[0]
		}
		repairStart, repairEnd := cloudWatchNginxRepairBounds(next, target)
		next.RepairNextTs = to
		if next.RepairNextTs >= repairEnd {
			next.RepairNextTs = repairStart
		}
		// The existing twenty-minute pause applies after a whole repaired
		// hour. A split prefix must resume after the primary lane's next turn.
		if to%3600 == 0 {
			next.LastRepairAt = finishedAt
		}
	} else {
		if to > next.ThroughTs {
			next.ThroughTs = to
		}
		next.NextTs = to
		next.Status = "running"
		if next.ThroughTs >= target && to >= target {
			next.Status = "caught_up"
		}
		// Grow during catch-up only after the reader has observed sustained
		// fast, unsplit successes. Publish the width with this exact prefix;
		// it is a retry hint and never coverage for the next unread window.
		if len(recoveryWidth) > 0 && recoveryWidth[0] > next.ReadWindowSeconds &&
			next.ReadWindowSeconds > 0 && to-from == next.ReadWindowSeconds {
			next.ReadWindowSeconds = recoveryWidth[0]
		}
		next.LastSuccessAt = finishedAt
	}
	next.LastError = ""
	next.UpdatedAt = finishedAt
	batchPrefix := "cw"
	if repair {
		batchPrefix = "cw-repair"
	}
	batchID := fmt.Sprintf("%s-%d-%d", batchPrefix, from, to)
	accessCount, errorCount := int64(0), int64(0)
	for _, row := range access {
		accessCount += row.Count
	}
	for _, row := range errorRows {
		errorCount += row.Count
	}
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("node = ? AND bucket_ts >= ? AND bucket_ts < ?", cloudWatchNginxNode, from, to).
			Delete(&NginxMinuteSample{}).Error; err != nil {
			return err
		}
		if err := tx.Where("node = ? AND bucket_ts >= ? AND bucket_ts < ?", cloudWatchNginxNode, from, to).
			Delete(&NginxErrorMinuteSample{}).Error; err != nil {
			return err
		}
		if len(access) > 0 {
			if err := tx.CreateInBatches(access, 200).Error; err != nil {
				return err
			}
		}
		if len(errorRows) > 0 {
			if err := tx.CreateInBatches(errorRows, 200).Error; err != nil {
				return err
			}
		}
		accessState := NginxSourceState{Node: cloudWatchNginxNode, LastEventTs: lastAccess,
			LastIngestTs: finishedAt, LastBatchID: batchID, AcceptedRows: int64(len(access)),
			AcceptedCount: accessCount, BacklogKnown: true}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}}, DoUpdates: clause.Assignments(map[string]any{
			"last_event_ts": gorm.Expr("MAX(last_event_ts, excluded.last_event_ts)"), "last_ingest_ts": finishedAt,
			"last_batch_id": batchID, "accepted_rows": int64(len(access)), "accepted_count": accessCount,
			"backlog_bytes": 0, "backlog_known": true,
		})}).Create(&accessState).Error; err != nil {
			return err
		}
		errorState := NginxErrorSourceState{Node: cloudWatchNginxNode, LastEventTs: lastError,
			LastIngestTs: finishedAt, LastBatchID: batchID, AcceptedRows: int64(len(errorRows)),
			AcceptedCount: errorCount, BacklogKnown: true}
		if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "node"}}, DoUpdates: clause.Assignments(map[string]any{
			"last_event_ts": gorm.Expr("MAX(last_event_ts, excluded.last_event_ts)"), "last_ingest_ts": finishedAt,
			"last_batch_id": batchID, "accepted_rows": int64(len(errorRows)), "accepted_count": errorCount,
			"backlog_bytes": 0, "backlog_known": true,
		})}).Create(&errorState).Error; err != nil {
			return err
		}
		return tx.Save(&next).Error
	})
	if err != nil {
		return err
	}
	*state = next
	return nil
}

// persistCloudWatchNginxRequestEvidence keeps the request-level, HMAC-only
// portion of the structured access log in the same short-lived evidence store
// used by the old collector.  Minute aggregates are sufficient for capacity
// charts, but they cannot answer the customer question "did this exact
// request reach Nginx, and what did Nginx see upstream?".  Persisting the
// already-redacted HMAC IDs closes that gap without storing raw IDs or log
// lines.  It is deliberately done before the cursor transaction: if this
// optional store is unavailable, the CloudWatch watermark must not advance
// past a window whose request-level evidence was lost.
func (m *Monitor) persistCloudWatchNginxRequestEvidence(ctx context.Context, evidence []cloudWatchStructuredEvidence, from, to int64, batchID string) error {
	return m.persistCloudWatchNginxEvidenceMode(ctx, evidence, from, to, batchID, false)
}

func (m *Monitor) persistCloudWatchNginxEvidenceMode(ctx context.Context, evidence []cloudWatchStructuredEvidence, from, to int64, batchID string, recent bool) error {
	if m == nil || m.nginxEvidenceDB == nil || nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" {
		return nil
	}
	rows := make([]NginxRequestEvidence, 0, len(evidence))
	// A Logs Insights range can return the same structured event more than once
	// when a window is split at the result limit or replayed at a cursor
	// boundary.  EventRef is the stable event identity and EventID is the
	// SQLite primary key, so let the first occurrence win before attempting the
	// batch insert.  Without this guard one duplicate aborts the whole window
	// transaction and leaves the watermark stuck forever.
	seen := make(map[string]struct{}, len(evidence))
	for _, item := range evidence {
		if item.Kind != cwEvidenceNginxAccess || item.EventMS < from*1000 || item.EventMS >= to*1000 ||
			item.EventRef == "" || item.OneAPIIDHMAC == "" || item.Status == nil || item.RequestMS == nil ||
			item.Method != "POST" || !isNginxInferenceRoute(item.Route) {
			continue
		}
		if _, exists := seen[item.EventRef]; exists {
			continue
		}
		seen[item.EventRef] = struct{}{}
		if !metering.ValidLength(item.RequestLength, item.RequestLengthState, item.RequestLengthVersion) {
			return fmt.Errorf("cloudwatch nginx request length metadata invalid")
		}
		statuses := append([]int(nil), item.UpstreamStatuses...)
		upstreamStatus := 0
		if len(statuses) > 0 {
			upstreamStatus = statuses[len(statuses)-1]
		}
		// EventRef and both IDs are produced by the parser's bounded HMAC
		// functions.  Re-check the shape here so a future parser change cannot
		// write malformed rows that the verified lookup would silently ignore.
		if !nginxEvidenceHex64Pattern.MatchString(item.EventRef) || !nginxEvidenceHex64Pattern.MatchString(item.OneAPIIDHMAC) ||
			item.NginxIDHMAC != "" && !nginxEvidenceHex64Pattern.MatchString(item.NginxIDHMAC) {
			return fmt.Errorf("cloudwatch nginx request evidence identity invalid")
		}
		statusesJSON, err := json.Marshal(statuses)
		if err != nil {
			return err
		}
		row := NginxRequestEvidence{
			EventID: item.EventRef, EventMS: item.EventMS, Node: cloudWatchNginxNode,
			Route: item.Route, Method: item.Method, Status: *item.Status,
			UpstreamStatus: upstreamStatus, UpstreamAttempts: len(statuses), UpstreamStatuses: string(statusesJSON),
			RequestMS: valueOrZero(item.RequestMS), UpstreamMS: valueOrZero(item.UpstreamMS), UpstreamPresent: len(statuses) > 0,
			ConnectMS: valueOrZero(item.ConnectMS), HeaderMS: valueOrZero(item.HeaderMS), BytesSent: valueOrZero(item.BytesSent),
			RequestLength: item.RequestLength, RequestLengthState: item.RequestLengthState, RequestLengthVersion: item.RequestLengthVersion,
			Completion: item.Completion, NginxIDHMAC: item.NginxIDHMAC, OneAPIIDHMAC: item.OneAPIIDHMAC, OneAPIRequestID: item.OneAPIRequestID,
			HMACKeyID: item.HMACKeyID, BatchID: batchID, ReceivedAt: time.Now().Unix(),
		}
		if !validNginxEvidenceEvent(nginxEvidenceEvent{EventID: row.EventID, EventMS: row.EventMS, Route: row.Route, Method: row.Method,
			Status: row.Status, UpstreamStatus: row.UpstreamStatus, UpstreamAttempts: row.UpstreamAttempts, UpstreamStatuses: statuses,
			RequestMS: row.RequestMS, UpstreamMS: row.UpstreamMS, UpstreamPresent: row.UpstreamPresent, ConnectMS: row.ConnectMS,
			HeaderMS: row.HeaderMS, BytesSent: row.BytesSent, RequestLength: row.RequestLength, RequestLengthState: row.RequestLengthState, RequestLengthVersion: row.RequestLengthVersion, Completion: row.Completion, NginxIDHMAC: row.NginxIDHMAC, OneAPIIDHMAC: row.OneAPIIDHMAC},
			time.Now().UnixMilli(), m.cfg.NginxEvidenceRetentionHours) {
			continue
		}
		rows = append(rows, row)
	}
	if recent {
		// Append/update immutable exact events. A provisional observation never
		// deletes prior evidence or touches the historical continuity proof.
		for start := 0; start < len(rows); start += cloudWatchNginxEvidencePersistBatchSize {
			end := min(start+cloudWatchNginxEvidencePersistBatchSize, len(rows))
			if err := m.nginxEvidenceDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				fields := []string{"route", "method", "status", "upstream_status", "request_ms", "request_length", "request_length_state", "request_length_version", "oneapi_id_hmac", "oneapi_request_id", "hmac_key_id", "completion"}
				changed := make([]string, 0, len(fields))
				for _, field := range fields {
					changed = append(changed, "nginx_request_evidences."+field+" IS NOT excluded."+field)
				}
				return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "event_id"}}, DoUpdates: clause.AssignmentColumns(append(fields, "received_at", "batch_id")), Where: clause.Where{Exprs: []clause.Expression{clause.Expr{SQL: strings.Join(changed, " OR ")}}}}).CreateInBatches(rows[start:end], end-start).Error
			}); err != nil {
				return err
			}
		}
		m.signalTokenAssociationWake()
		return nil
	}
	// A replay/repair window is a replacement, not an increment.  Remove
	// only this source's rows; old collector evidence remains untouched.  The
	// delete is deliberately its own short transaction: a high-volume window
	// must not keep the old rows, the new rows, and all SQLite transient pages
	// in one transaction.  If a later insert chunk fails, the cursor is left at
	// its old value and the next retry repeats this delete before rebuilding the
	// complete window.
	if err := ctx.Err(); err != nil {
		return err
	}
	// The next transaction may invalidate an already covered interval. Publish
	// unknown before touching it so readiness cannot reuse a stale proof while
	// the bounded replacement transactions are in progress.
	m.cloudWatchNginxEvidenceRecoveryCache.Store(nil)
	if err := m.nginxEvidenceDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := beginCloudWatchNginxEvidenceReplace(tx, from, to); err != nil {
			return err
		}
		return tx.Where("node = ? AND event_ms >= ? AND event_ms < ?", cloudWatchNginxNode, from*1000, to*1000).
			Delete(&NginxRequestEvidence{}).Error
	}); err != nil {
		return fmt.Errorf("delete cloudwatch nginx evidence window: %w", err)
	}
	for start := 0; start < len(rows); start += cloudWatchNginxEvidencePersistBatchSize {
		if err := ctx.Err(); err != nil {
			return err
		}
		end := start + cloudWatchNginxEvidencePersistBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		chunk := rows[start:end]
		// Use a batch size equal to this already-bounded chunk.  GORM then
		// executes the insert inside this transaction without opening another
		// nested savepoint, which is important when SQLite is under pressure.
		if err := m.nginxEvidenceDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			return tx.CreateInBatches(chunk, len(chunk)).Error
		}); err != nil {
			return fmt.Errorf("insert cloudwatch nginx evidence batch [%d,%d): %w", start, end, err)
		}
	}
	err := m.nginxEvidenceDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		return completeCloudWatchNginxEvidenceReplace(tx, from, to)
	})
	if err == nil {
		m.refreshCloudWatchNginxEvidenceRecovery(time.Now())
	}
	return err
}

func (m *Monitor) publishCloudWatchNginxEvidenceCursor(ctx context.Context, state *CloudWatchNginxCursor, from, to, target int64) error {
	if state == nil || state.EvidenceStatus == "disabled" || to <= from {
		return nil
	}
	next := *state
	next.EvidenceNextTs = to
	next.EvidenceThroughTs = to
	next.EvidenceStatus = "running"
	next.EvidenceLastError = ""
	next.EvidenceLastSuccessAt = time.Now().Unix()
	var proof CloudWatchNginxEvidenceCheckpoint
	if m.nginxEvidenceDB == nil {
		return fmt.Errorf("cloudwatch nginx evidence store unavailable")
	}
	if err := m.nginxEvidenceDB.WithContext(ctx).First(&proof, "id = ?", cloudWatchNginxCursorID).Error; err != nil {
		return err
	}
	retentionFrom, _ := cloudWatchNginxEvidenceRange(time.Now(), m.cfg.NginxEvidenceRetentionHours)
	if !cloudWatchNginxEvidenceProofMatches(next, proof, retentionFrom) {
		return fmt.Errorf("cloudwatch nginx evidence coverage proof mismatch")
	}
	if to >= target {
		next.EvidenceStatus = "caught_up"
	}
	if next.EvidenceGapToTs > to {
		next.EvidenceStatus = "recovering"
	}
	next.UpdatedAt = time.Now().Unix()
	if err := m.storeDB.WithContext(ctx).Save(&next).Error; err != nil {
		return err
	}
	*state = next
	m.refreshCloudWatchNginxEvidenceRecovery(time.Now())
	return nil
}

func (m *Monitor) recordCloudWatchNginxEvidenceFailure(state *CloudWatchNginxCursor, cause error, now int64) {
	if state == nil || state.EvidenceStatus == "disabled" {
		return
	}
	state.EvidenceStatus = "degraded"
	state.EvidenceLastFailureAt = now
	state.EvidenceLastError = cloudWatchPreRouteErrorCode(cause)
	state.UpdatedAt = now
	_ = m.storeDB.Save(state).Error
}

// cloudWatchNginxErrorDetail is intended for operational logs only. It keeps
// concrete local failures (for example a SQLite constraint) diagnosable while
// refusing to emit raw CloudWatch messages, request logs, or credentials.
func cloudWatchNginxErrorDetail(err error) string {
	if err == nil {
		return ""
	}
	var logsErr *cloudWatchLogsError
	if errors.As(err, &logsErr) {
		detail := fmt.Sprintf("%s operation=%s", logsErr.Kind, logsErr.Operation)
		if logsErr.cause != nil {
			detail += fmt.Sprintf(" cause=%s", cloudWatchLogsErrorKindOf(logsErr.cause))
		}
		return detail
	}
	message := strings.Join(strings.Fields(err.Error()), " ")
	lower := strings.ToLower(message)
	for _, marker := range []string{"authorization", "bearer ", "access key", "secret", "password", "credential", "token=", "x-amz-"} {
		if strings.Contains(lower, marker) {
			return fmt.Sprintf("%T (detail redacted)", err)
		}
	}
	runes := []rune(message)
	if len(runes) > 256 {
		message = string(runes[:256])
	}
	return message
}

func (m *Monitor) runCloudWatchNginxEvidenceWindow(ctx context.Context, state *CloudWatchNginxCursor, from, to, target int64) (int, error) {
	started := time.Now()
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		return 0, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, cloudWatchNginxQueryBudget)
	defer cancel()
	queries := 0
	evidence, err := m.queryCloudWatchNginxRange(queryCtx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
	if err != nil {
		m.cloudWatchNginxEvidenceFastWindows.Store(0)
		return queries, err
	}
	if err := m.persistCloudWatchNginxRequestEvidence(ctx, evidence, from, to,
		fmt.Sprintf("cw-evidence-%d-%d", from, to)); err != nil {
		return queries, fmt.Errorf("request evidence persist: %w", err)
	}
	next := *state
	next.EvidenceReadWindowSeconds = cloudWatchNginxRecoveredWidth(state.EvidenceReadWindowSeconds, to-from, queries, time.Since(started), &m.cloudWatchNginxEvidenceFastWindows, int64(cloudWatchNginxEvidenceChunkWindow/time.Second))
	if err := m.publishCloudWatchNginxEvidenceCursor(ctx, &next, from, to, target); err != nil {
		return queries, fmt.Errorf("request evidence cursor publish: %w", err)
	}
	*state = next
	m.cloudWatchNginxEvidenceFrom.Store(state.EvidenceCoverageFromTs)
	m.cloudWatchNginxEvidenceThrough.Store(state.EvidenceThroughTs)
	m.cloudWatchNginxEvidenceLastSuccess.Store(state.EvidenceLastSuccessAt)
	return queries, nil
}

// processCloudWatchNginxEvidenceChunk consumes one bounded historical evidence
// chunk and records its outcome.  Once the minute lane is caught up, callers
// use this ahead of the normal 20-minute replay; otherwise a replay every poll
// would leave one two-hour evidence chunk waiting five minutes and make a
// seven-day initial backfill take many hours.
func (m *Monitor) processCloudWatchNginxEvidenceChunk(ctx context.Context, state *CloudWatchNginxCursor, target int64) (bool, error) {
	if state == nil {
		return false, nil
	}
	from, to, ok := cloudWatchNginxEvidenceWindow(*state, target)
	if !ok {
		return false, nil
	}
	queries, err := m.runCloudWatchNginxEvidenceWindow(ctx, state, from, to, target)
	if err != nil {
		narrowCloudWatchNginxEvidenceRead(state, to-from, err)
		m.recordCloudWatchNginxEvidenceFailure(state, err, time.Now().Unix())
		m.cloudWatchNginxEvidenceLastFailure.Store(state.EvidenceLastFailureAt)
		slog.Warn("CloudWatch Nginx 请求级证据补扫失败(保留水位重试)",
			"from", from, "to", to, "queries", queries, "class", cloudWatchPreRouteErrorCode(err), "detail", cloudWatchNginxErrorDetail(err))
		return true, err
	}
	slog.Info("CloudWatch Nginx 请求级证据补扫完成", "from", from, "to", to,
		"through", state.EvidenceThroughTs, "target", target, "queries", queries)
	return true, nil
}

func valueOrZero(v *int64) int64 {
	if v == nil {
		return 0
	}
	return *v
}

func (m *Monitor) publishCloudWatchNginxWindow(ctx context.Context, state *CloudWatchNginxCursor,
	from, to int64, access []NginxMinuteSample, errorRows []NginxErrorMinuteSample, lastAccess, lastError, target int64) error {
	return m.publishCloudWatchNginxWindowMode(ctx, state, from, to, access, errorRows, lastAccess, lastError, target, false)
}

func (m *Monitor) publishCloudWatchNginxRepairWindow(ctx context.Context, state *CloudWatchNginxCursor,
	from, to int64, access []NginxMinuteSample, errorRows []NginxErrorMinuteSample, lastAccess, lastError, target int64) error {
	return m.publishCloudWatchNginxWindowMode(ctx, state, from, to, access, errorRows, lastAccess, lastError, target, true)
}

func (m *Monitor) runCloudWatchNginxWindowMode(ctx context.Context, state *CloudWatchNginxCursor,
	from, to, target int64, repair bool) (queryCount int, runErr error) {
	return m.runCloudWatchNginxWindowLane(ctx, state, from, to, target, repair, false)
}

func (m *Monitor) runCloudWatchNginxWindowLane(ctx context.Context, state *CloudWatchNginxCursor,
	from, to, target int64, repair, replay bool) (queryCount int, runErr error) {
	started := time.Now()
	width, fast := state.ReadWindowSeconds, &m.cloudWatchNginxFastWindows
	if repair {
		width, fast = state.RepairReadWindowSeconds, &m.cloudWatchNginxRepairFastWindows
	}
	if replay {
		width, fast = state.ReplayReadWindowSeconds, &m.cloudWatchNginxReplayFastWindows
	}
	defer func() {
		if runErr != nil {
			fast.Store(0)
		}
	}()
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		return 0, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, cloudWatchNginxQueryBudget)
	defer cancel()
	queries := 0
	evidence, err := m.queryCloudWatchNginxRange(queryCtx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
	if err != nil {
		if cloudWatchNginxTimeoutStage(err) == "execution" {
			width := max(int64(60), min(int64(15*60), (to-from)/2/60*60))
			if replay {
				state.ReplayReadWindowSeconds = width
				state.ReplayNextTs = from
			} else if repair {
				state.RepairReadWindowSeconds = width
				state.RepairNextTs = from
			} else {
				state.ReadWindowSeconds = width
				state.NextTs = from
			}
		}
		return queries, err
	}
	access, errorRows, lastAccess, lastError, err := cloudWatchNginxSamples(evidence, from, to)
	if err != nil {
		return queries, err
	}
	batchPrefix := "cw"
	if repair {
		batchPrefix = "cw-repair"
	}
	if err := m.persistCloudWatchNginxRequestEvidence(ctx, evidence, from, to,
		fmt.Sprintf("%s-%d-%d", batchPrefix, from, to)); err != nil {
		return queries, fmt.Errorf("request evidence persist: %w", err)
	}
	recoveryWidth := cloudWatchNginxRecoveredWidth(width, to-from, queries, time.Since(started), fast, int64(cloudWatchNginxWindow/time.Second))
	if err := m.publishCloudWatchNginxWindowLane(ctx, state, from, to, access, errorRows, lastAccess, lastError, target, repair, replay, recoveryWidth); err != nil {
		return queries, fmt.Errorf("minute aggregate publish: %w", err)
	}
	// The primary catch-up windows already contain the same request-level
	// evidence. Advance the independent evidence watermark only when this exact
	// window is contiguous; replay and repair windows must not jump it forward.
	if !repair && !replay && state.EvidenceStatus != "disabled" && state.EvidenceNextTs == from {
		if err := m.publishCloudWatchNginxEvidenceCursor(ctx, state, from, to, target); err != nil {
			return queries, err
		}
	}
	m.cloudWatchNginxFrom.Store(state.CoverageFromTs)
	m.cloudWatchNginxThrough.Store(state.ThroughTs)
	m.cloudWatchNginxLastSuccess.Store(state.LastSuccessAt)
	m.cloudWatchNginxEvidenceFrom.Store(state.EvidenceCoverageFromTs)
	m.cloudWatchNginxEvidenceThrough.Store(state.EvidenceThroughTs)
	m.cloudWatchNginxEvidenceLastSuccess.Store(state.EvidenceLastSuccessAt)
	return queries, nil
}

func (m *Monitor) runCloudWatchNginxWindow(ctx context.Context, state *CloudWatchNginxCursor,
	from, to, target int64) (int, error) {
	return m.runCloudWatchNginxWindowMode(ctx, state, from, to, target, false)
}

func (m *Monitor) runCloudWatchNginxRepairWindow(ctx context.Context, state *CloudWatchNginxCursor,
	from, to, target int64) (int, error) {
	return m.runCloudWatchNginxWindowMode(ctx, state, from, to, target, true)
}

func (m *Monitor) startCloudWatchNginx(ctx context.Context) {
	if m == nil || !m.cfg.CloudWatchNginxEnabled || m.storeDB == nil || m.cloudWatchLogs == nil || !m.cloudWatchLogs.enabled {
		return
	}
	go func() {
		m.cloudWatchNginxRunning.Store(true)
		defer m.cloudWatchNginxRunning.Store(false)
		state, err := m.loadCloudWatchNginxCursor(time.Now())
		if err != nil {
			m.recordCloudWatchNginxFailure(nil, err, time.Now().Unix())
			slog.Error("初始化 CloudWatch Nginx 采集水位失败", "class", cloudWatchPreRouteErrorCode(err), "detail", cloudWatchNginxErrorDetail(err))
			return
		}
		m.cloudWatchNginxFrom.Store(state.CoverageFromTs)
		m.cloudWatchNginxThrough.Store(state.ThroughTs)
		m.cloudWatchNginxLastSuccess.Store(state.LastSuccessAt)
		m.cloudWatchNginxLastFailure.Store(state.LastFailureAt)
		m.cloudWatchNginxEvidenceFrom.Store(state.EvidenceCoverageFromTs)
		m.cloudWatchNginxEvidenceThrough.Store(state.EvidenceThroughTs)
		m.cloudWatchNginxEvidenceLastSuccess.Store(state.EvidenceLastSuccessAt)
		m.cloudWatchNginxEvidenceLastFailure.Store(state.EvidenceLastFailureAt)
		poll := time.Duration(m.cfg.CloudWatchNginxPollSeconds) * time.Second
		schedule := cloudWatchNginxSchedule{}
		for {
			if ctx.Err() != nil {
				return
			}
			now := time.Now()
			historyReady := true
			if err := m.refreshCloudWatchNginxEvidenceCursor(ctx, &state, now); err != nil {
				m.recordCloudWatchNginxEvidenceFailure(&state, err, now.Unix())
				historyReady = false
			}
			_, target := cloudWatchNginxRange(now, m.cfg.CloudWatchNginxLookbackHours)
			state.TargetThroughTs = target
			if m.cfg.TokenFactsEnabled && state.EvidenceStatus != "disabled" && m.nginxEvidenceDB != nil {
				w, found, e := selectCloudWatchNginxRecentWindow(m.nginxEvidenceDB.WithContext(ctx), now.Unix())
				if e == nil && found {
					schedule.recentFrom, schedule.recentTo = w.StartAt, w.EndAt
				} else {
					schedule.recentFrom, schedule.recentTo = 0, 0
				}
				if e != nil {
					slog.Warn("CloudWatch Nginx recent scheduling deferred", "detail", cloudWatchNginxErrorDetail(e))
				}
			}
			lane, from, to, replayEnd, ok := schedule.choose(state, target, now, poll)
			if !historyReady {
				lane, from, to, replayEnd = nginxRecent, schedule.recentFrom, schedule.recentTo, 0
				ok = to > from && !now.Before(schedule.due[nginxRecent])
			}
			if !ok {
				if !waitSourceLifecycle(ctx, schedule.idleDelay(now, poll)) {
					return
				}
				continue
			}
			if m.cfg.TokenFactsEnabled && state.EvidenceStatus != "disabled" && lane != nginxRecent {
				// Historical work keeps its left boundary and proof; a small turn
				// lets the recent lane run between remote queries without extra concurrency.
				to = min(to, from+60)
			}
			// Persist a newly scheduled replay before reading. Its unfinished
			// suffix survives cancellation/restart without changing NextTs.
			if lane == nginxReplay && state.ReplayThroughTs <= state.ReplayNextTs {
				next := state
				next.ReplayNextTs, next.ReplayThroughTs = from, replayEnd
				if err := m.storeDB.WithContext(ctx).Save(&next).Error; err != nil {
					m.recordCloudWatchNginxFailure(&state, err, now.Unix())
					schedule.due[lane] = now.Add(poll)
					continue
				}
				state = next
			}
			var queries int
			var runErr error
			workCtx, finishTurn := context.WithCancel(ctx)
			if m.cfg.TokenFactsEnabled && state.EvidenceStatus != "disabled" {
				finishTurn()
				workCtx, finishTurn = context.WithTimeout(ctx, 20*time.Second)
			}
			if lane == nginxRecent {
				queries, runErr = m.runCloudWatchNginxRecentWindow(workCtx, from, to)
			} else if lane == nginxEvidence {
				queries, runErr = m.runCloudWatchNginxEvidenceWindow(workCtx, &state, from, to, target)
				if runErr != nil && workCtx.Err() == nil {
					narrowCloudWatchNginxEvidenceRead(&state, to-from, runErr)
					m.recordCloudWatchNginxEvidenceFailure(&state, runErr, time.Now().Unix())
				}
			} else {
				queries, runErr = m.runCloudWatchNginxWindowLane(workCtx, &state, from, to, target, lane == nginxRepair, lane == nginxReplay)
				if runErr != nil && workCtx.Err() == nil {
					m.recordCloudWatchNginxFailure(&state, runErr, time.Now().Unix())
				}
			}
			yielded := runErr != nil && workCtx.Err() != nil && ctx.Err() == nil
			finishTurn()
			if ctx.Err() != nil {
				return
			}
			if yielded {
				schedule.due[lane] = time.Now().Add(cloudWatchNginxCatchupDelay)
				slog.Info("CloudWatch Nginx 窗口延期(保留水位重试)", "lane", lane.String(), "from", from, "to", to, "queries", queries, "state", "deferred")
			} else if runErr != nil {
				schedule.due[lane] = time.Now().Add(cloudWatchNginxRetryDelay(runErr, poll))
				if lane == nginxRecent {
					schedule.due[lane] = time.Now().Add(cloudWatchNginxCatchupDelay)
				}
				slog.Warn("CloudWatch Nginx 窗口失败(保留水位重试)", "lane", lane.String(), "from", from, "to", to, "queries", queries,
					"stage", cloudWatchNginxTimeoutStage(runErr), "class", cloudWatchPreRouteErrorCode(runErr), "detail", cloudWatchNginxErrorDetail(runErr))
			} else {
				schedule.due[lane] = time.Now().Add(cloudWatchNginxCatchupDelay)
				slog.Info("CloudWatch Nginx 完成窗口", "lane", lane.String(), "from", from, "to", to, "through", state.ThroughTs,
					"evidence_through", state.EvidenceThroughTs, "target", target, "queries", queries)
			}
			if !waitSourceLifecycle(ctx, cloudWatchNginxCatchupDelay) {
				return
			}
		}
	}()
}
