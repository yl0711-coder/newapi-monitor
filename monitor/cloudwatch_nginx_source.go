package monitor

// CloudWatch Nginx 连续采集。
//
// 这条 lane 只读取固定的结构化 access/标准 error.log 查询，把完整窗口聚合
// 到现有分钟事实表。原始日志、IP、Header、请求/响应正文和 Request ID 原值
// 均不落盘。水位只在 access/error 同一窗口原子发布成功后推进。

import (
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
	ID               uint   `gorm:"primaryKey;autoIncrement:false"`
	CoverageFromTs   int64  `gorm:"column:coverage_from_ts"`
	NextTs           int64  `gorm:"column:next_ts;index"`
	ThroughTs        int64  `gorm:"column:through_ts"`
	TargetThroughTs  int64  `gorm:"column:target_through_ts"`
	SemanticsVersion int    `gorm:"column:semantics_version;index"`
	Status           string `gorm:"size:24;index"`
	LastSuccessAt    int64  `gorm:"column:last_success_at"`
	LastFailureAt    int64  `gorm:"column:last_failure_at"`
	RepairNextTs     int64  `gorm:"column:repair_next_ts;index"`
	LastRepairAt     int64  `gorm:"column:last_repair_at"`
	LastError        string `gorm:"size:64;column:last_error"`
	// 请求级 HMAC evidence 的独立补扫水位。它与分钟聚合的 NextTs/RepairNextTs
	// 分开，允许在主聚合已追平后把 evidence retention 内的历史补回来。
	EvidenceCoverageFromTs int64  `gorm:"column:evidence_coverage_from_ts"`
	EvidenceNextTs         int64  `gorm:"column:evidence_next_ts;index"`
	EvidenceThroughTs      int64  `gorm:"column:evidence_through_ts"`
	EvidenceStatus         string `gorm:"size:24;column:evidence_status;index"`
	EvidenceLastSuccessAt  int64  `gorm:"column:evidence_last_success_at"`
	EvidenceLastFailureAt  int64  `gorm:"column:evidence_last_failure_at"`
	EvidenceLastError      string `gorm:"size:64;column:evidence_last_error"`
	UpdatedAt              int64  `gorm:"column:updated_at"`
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
	from := through - int64(retentionHours)*3600
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
	to := from + int64(cloudWatchNginxEvidenceChunkWindow/time.Second)
	if to > target {
		to = target
	}
	return from, to, to > from
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
	return start, end
}

func cloudWatchNginxRepairWindow(state CloudWatchNginxCursor, target int64) (int64, int64, bool) {
	start, end := cloudWatchNginxRepairBounds(state, target)
	if end <= start {
		return 0, 0, false
	}
	from := state.RepairNextTs
	if from < start || from >= end || from%int64(time.Hour/time.Second) != 0 {
		from = start
	}
	to := from + int64(cloudWatchNginxWindow/time.Second)
	if to > end {
		to = end
	}
	return from, to, to > from
}

func (m *Monitor) loadCloudWatchNginxCursor(now time.Time) (CloudWatchNginxCursor, error) {
	from, target := cloudWatchNginxRange(now, m.cfg.CloudWatchNginxLookbackHours)
	evidenceFrom, evidenceTarget := cloudWatchNginxEvidenceRange(now, m.cfg.NginxEvidenceRetentionHours)
	var state CloudWatchNginxCursor
	err := m.storeDB.First(&state, "id = ?", cloudWatchNginxCursorID).Error
	if err == nil && state.SemanticsVersion == cloudWatchNginxVersion {
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
		if nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" || m.nginxEvidenceDB == nil {
			state.EvidenceStatus = "disabled"
		} else {
			if state.EvidenceCoverageFromTs <= 0 || state.EvidenceCoverageFromTs > evidenceTarget {
				state.EvidenceCoverageFromTs = evidenceFrom
			}
			if state.EvidenceCoverageFromTs < evidenceFrom {
				state.EvidenceCoverageFromTs = evidenceFrom
			}
			if state.EvidenceNextTs < state.EvidenceCoverageFromTs || state.EvidenceNextTs == 0 {
				state.EvidenceNextTs = state.EvidenceCoverageFromTs
			}
			if state.EvidenceThroughTs < state.EvidenceCoverageFromTs {
				state.EvidenceThroughTs = state.EvidenceCoverageFromTs
			}
			if state.EvidenceNextTs >= evidenceTarget {
				state.EvidenceStatus = "caught_up"
			} else if state.EvidenceStatus == "" || state.EvidenceStatus == "disabled" {
				state.EvidenceStatus = "running"
			}
		}
		repairStart, repairEnd := cloudWatchNginxRepairBounds(state, target)
		if repairEnd > repairStart && (state.RepairNextTs < repairStart || state.RepairNextTs >= repairEnd ||
			state.RepairNextTs%int64(time.Hour/time.Second) != 0) {
			state.RepairNextTs = repairStart
		}
		state.UpdatedAt = now.Unix()
		if saveErr := m.storeDB.Save(&state).Error; saveErr != nil {
			return CloudWatchNginxCursor{}, saveErr
		}
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
	if nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" || m.nginxEvidenceDB == nil {
		state.EvidenceStatus = "disabled"
	} else {
		state.EvidenceCoverageFromTs, state.EvidenceNextTs, state.EvidenceThroughTs = evidenceFrom, evidenceFrom, evidenceFrom
		state.EvidenceStatus = "running"
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
	return m.queryCloudWatchCompleteEvidenceRange(ctx, parser, cwQueryWorkerNginxContinuous,
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
	repair bool) error {
	finishedAt := time.Now().Unix()
	next := *state
	next.TargetThroughTs = target
	if repair {
		repairStart, repairEnd := cloudWatchNginxRepairBounds(next, target)
		next.RepairNextTs = to
		if next.RepairNextTs >= repairEnd {
			next.RepairNextTs = repairStart
		}
		next.LastRepairAt = finishedAt
	} else {
		if to > next.ThroughTs {
			next.ThroughTs = to
		}
		next.NextTs = to
		next.Status = "running"
		if next.ThroughTs >= target {
			next.Status = "caught_up"
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
			Completion: item.Completion, NginxIDHMAC: item.NginxIDHMAC, OneAPIIDHMAC: item.OneAPIIDHMAC,
			HMACKeyID: item.HMACKeyID, BatchID: batchID, ReceivedAt: time.Now().Unix(),
		}
		if !validNginxEvidenceEvent(nginxEvidenceEvent{EventID: row.EventID, EventMS: row.EventMS, Route: row.Route, Method: row.Method,
			Status: row.Status, UpstreamStatus: row.UpstreamStatus, UpstreamAttempts: row.UpstreamAttempts, UpstreamStatuses: statuses,
			RequestMS: row.RequestMS, UpstreamMS: row.UpstreamMS, UpstreamPresent: row.UpstreamPresent, ConnectMS: row.ConnectMS,
			HeaderMS: row.HeaderMS, BytesSent: row.BytesSent, Completion: row.Completion, NginxIDHMAC: row.NginxIDHMAC, OneAPIIDHMAC: row.OneAPIIDHMAC},
			time.Now().UnixMilli(), m.cfg.NginxEvidenceRetentionHours) {
			continue
		}
		rows = append(rows, row)
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
	if err := m.nginxEvidenceDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
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
	return ctx.Err()
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
	if to >= target {
		next.EvidenceStatus = "caught_up"
	}
	next.UpdatedAt = time.Now().Unix()
	if err := m.storeDB.WithContext(ctx).Save(&next).Error; err != nil {
		return err
	}
	*state = next
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
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		return 0, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, cloudWatchNginxQueryBudget)
	defer cancel()
	queries := 0
	evidence, err := m.queryCloudWatchNginxRange(queryCtx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
	if err != nil {
		return queries, err
	}
	if err := m.persistCloudWatchNginxRequestEvidence(ctx, evidence, from, to,
		fmt.Sprintf("cw-evidence-%d-%d", from, to)); err != nil {
		return queries, fmt.Errorf("request evidence persist: %w", err)
	}
	if err := m.publishCloudWatchNginxEvidenceCursor(ctx, state, from, to, target); err != nil {
		return queries, fmt.Errorf("request evidence cursor publish: %w", err)
	}
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
	from, to, target int64, repair bool) (int, error) {
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		return 0, err
	}
	queryCtx, cancel := context.WithTimeout(ctx, cloudWatchNginxQueryBudget)
	defer cancel()
	queries := 0
	evidence, err := m.queryCloudWatchNginxRange(queryCtx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
	if err != nil {
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
	if err := m.publishCloudWatchNginxWindowMode(ctx, state, from, to, access, errorRows, lastAccess, lastError, target, repair); err != nil {
		return queries, fmt.Errorf("minute aggregate publish: %w", err)
	}
	// The primary catch-up windows already contain the same request-level
	// evidence. Advance the independent evidence watermark only when this exact
	// window is contiguous; replay and repair windows must not jump it forward.
	if !repair && state.EvidenceStatus != "disabled" && state.EvidenceNextTs == from {
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
		for {
			if ctx.Err() != nil {
				return
			}
			_, target := cloudWatchNginxRange(time.Now(), m.cfg.CloudWatchNginxLookbackHours)
			state.TargetThroughTs = target
			// Once the primary minute lane is caught up, drain the independent
			// request-evidence backlog before replaying the same recent minutes.
			// This prevents the five-minute poll interval from throttling the
			// initial historical evidence backfill.
			if state.ThroughTs >= target {
				if processed, evidenceErr := m.processCloudWatchNginxEvidenceChunk(ctx, &state, target); processed {
					delay := cloudWatchNginxCatchupDelay
					if evidenceErr != nil {
						delay = poll
					}
					if !waitSourceLifecycle(ctx, delay) {
						return
					}
					continue
				}
			}
			from := state.NextTs
			if state.Status == "caught_up" {
				from = state.ThroughTs - int64(cloudWatchNginxReplay/time.Second)
				if from < state.CoverageFromTs {
					from = state.CoverageFromTs
				}
			}
			to := from + int64(cloudWatchNginxWindow/time.Second)
			if to > target {
				to = target
			}
			if to <= from {
				// The minute lane is already caught up.  Continue the independent
				// request-evidence backfill even when the old main cursor was
				// caught_up before evidence persistence was enabled.
				if processed, evidenceErr := m.processCloudWatchNginxEvidenceChunk(ctx, &state, target); processed {
					delay := cloudWatchNginxCatchupDelay
					if evidenceErr != nil {
						delay = poll
					}
					if !waitSourceLifecycle(ctx, delay) {
						return
					}
					continue
				}
				if !waitSourceLifecycle(ctx, poll) {
					return
				}
				continue
			}
			queries, runErr := m.runCloudWatchNginxWindow(ctx, &state, from, to, target)
			if runErr != nil {
				m.recordCloudWatchNginxFailure(&state, runErr, time.Now().Unix())
				slog.Warn("CloudWatch Nginx 持续采集失败(保留水位重试)",
					"from", from, "to", to, "queries", queries, "class", cloudWatchPreRouteErrorCode(runErr), "detail", cloudWatchNginxErrorDetail(runErr))
				if !waitSourceLifecycle(ctx, poll) {
					return
				}
				continue
			}
			// If this primary window did not advance the independent cursor (for
			// example, the main lane was already caught up before evidence was
			// enabled), consume one bounded evidence chunk before the optional
			// late-log repair.  The chunk is replacement-based and fail-closed.
			if state.ThroughTs >= target {
				if processed, evidenceErr := m.processCloudWatchNginxEvidenceChunk(ctx, &state, target); processed {
					if evidenceErr != nil {
						if !waitSourceLifecycle(ctx, poll) {
							return
						}
					} else if !waitSourceLifecycle(ctx, cloudWatchNginxCatchupDelay) {
						return
					}
					continue
				}
			}
			slog.Info("CloudWatch Nginx 持续采集完成窗口", "from", from, "to", to,
				"through", state.ThroughTs, "target", target, "queries", queries)
			if state.ThroughTs >= target && (state.LastRepairAt == 0 ||
				time.Now().Unix()-state.LastRepairAt >= int64(cloudWatchNginxRepairEvery/time.Second)) {
				repairFrom, repairTo, ok := cloudWatchNginxRepairWindow(state, target)
				if ok {
					repairQueries, repairErr := m.runCloudWatchNginxRepairWindow(ctx, &state, repairFrom, repairTo, target)
					if repairErr != nil {
						m.recordCloudWatchNginxFailure(&state, repairErr, time.Now().Unix())
						slog.Warn("CloudWatch Nginx 历史补扫失败(保留修复水位重试)",
							"from", repairFrom, "to", repairTo, "queries", repairQueries,
							"class", cloudWatchPreRouteErrorCode(repairErr), "detail", cloudWatchNginxErrorDetail(repairErr))
					} else {
						slog.Info("CloudWatch Nginx 历史补扫完成窗口", "from", repairFrom, "to", repairTo,
							"next", state.RepairNextTs, "queries", repairQueries)
					}
				}
			}
			delay := cloudWatchNginxCatchupDelay
			if state.ThroughTs >= target {
				delay = poll
			}
			if !waitSourceLifecycle(ctx, delay) {
				return
			}
		}
	}()
}
