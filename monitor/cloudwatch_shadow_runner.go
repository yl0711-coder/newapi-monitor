package monitor

// cloudwatch_shadow_runner.go wires the phase-4 comparison foundation to a
// bounded, dormant-by-default background job.  The job is deliberately gated
// by CloudWatchShadowNginxContractReady: until production nginx/ access logs
// are structured, a zero-row CloudWatch result is a blind spot, not a match.

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	cloudWatchShadowQueryLimit     = cloudWatchLogsHardLimit
	cloudWatchShadowQueryBudget    = 2 * time.Minute
	cloudWatchShadowMaxQueriesTurn = 64
	cloudWatchShadowMaxSplitDepth  = 12
)

type cloudWatchShadowLaneResult struct {
	status     string
	evidence   []cloudWatchStructuredEvidence
	queries    int
	bytes      int64
	blindSpots []string
}

func appendShadowBlindSpot(dst []string, value string) []string {
	value = strings.TrimSpace(value)
	if value == "" {
		return dst
	}
	for _, existing := range dst {
		if existing == value {
			return dst
		}
	}
	return append(dst, value)
}

func cloudWatchShadowQueryBlindSpot(prefix string, err error) string {
	if err == nil {
		return prefix
	}
	// Only the stable error class is recorded.  AWS request IDs, log group
	// names and SDK messages must not enter the durable blind-spot string.
	return fmt.Sprintf("%s_%s", prefix, cloudWatchLogsErrorKindOf(err))
}

func cloudWatchShadowLaneStatus(queryErr error, truncated bool, parseFailed uint64, evidenceCount, oldCount int64, legacyPresent, nginx bool) string {
	if queryErr != nil {
		return cloudWatchShadowStatusFailed
	}
	if nginx && parseFailed > 0 && evidenceCount == 0 {
		return cloudWatchShadowStatusBlockedLogContract
	}
	// A non-empty CloudWatch window with no corresponding legacy rows is not a
	// zero-delta comparison.  It means this Monitor instance has no old-side
	// baseline (for example, the 8204 read-only stack has no collector sink).
	// Fail closed so a complete CloudWatch query cannot manufacture differences
	// against an absent collector.
	if evidenceCount > 0 && !legacyPresent {
		return cloudWatchShadowStatusBlockedLegacy
	}
	if truncated || parseFailed > 0 || (evidenceCount == 0 && oldCount > 0) {
		return cloudWatchShadowStatusPartial
	}
	return cloudWatchShadowStatusComplete
}

// queryCloudWatchShadowRange keeps every individual Insights query at the
// closed 500-row limit. A busy window is bisected until both halves are known
// complete; unresolved truncation is returned explicitly and is never treated
// as a zero-difference window.
func (m *Monitor) queryCloudWatchShadowRange(ctx context.Context, parser *cloudWatchEvidenceParser,
	kind cloudWatchFixedQueryKind, from, to time.Time, queries *int, bytes *int64, depth int) ([]cloudWatchStructuredEvidence, uint64, bool, error) {
	if !from.Before(to) {
		return nil, 0, false, nil
	}
	if depth > cloudWatchShadowMaxSplitDepth || *queries >= cloudWatchShadowMaxQueriesTurn {
		return nil, 0, true, nil
	}
	*queries++
	query, err := m.cloudWatchLogs.insights(ctx, cloudWatchInsightsRequest{
		Kind: kind, From: from, To: to, Limit: cloudWatchShadowQueryLimit,
	})
	if query.BytesScanned <= ^uint64(0)>>1 {
		*bytes += int64(query.BytesScanned)
	}
	if err != nil {
		return nil, 0, false, err
	}
	if query.Truncated {
		if to.Sub(from) <= time.Second {
			return nil, 0, true, nil
		}
		mid := from.Add(to.Sub(from) / 2).Truncate(time.Second)
		if !mid.After(from) || !mid.Before(to) {
			return nil, 0, true, nil
		}
		left, leftFailed, leftTruncated, err := m.queryCloudWatchShadowRange(ctx, parser, kind, from, mid, queries, bytes, depth+1)
		if err != nil || leftTruncated {
			return left, leftFailed, leftTruncated, err
		}
		right, rightFailed, rightTruncated, err := m.queryCloudWatchShadowRange(ctx, parser, kind, mid, to, queries, bytes, depth+1)
		return append(left, right...), leftFailed + rightFailed, rightTruncated, err
	}
	batch := m.cloudWatchLogs.parseInsightsEvidence(parser, query)
	return batch.Evidence, batch.ParseFailed, false, nil
}

func (m *Monitor) runCloudWatchShadowLane(ctx context.Context, parser *cloudWatchEvidenceParser, kind cloudWatchFixedQueryKind, from, to time.Time, oldCount int64, legacyPresent, nginx bool) cloudWatchShadowLaneResult {
	result := cloudWatchShadowLaneResult{status: cloudWatchShadowStatusFailed}
	if m == nil || m.cloudWatchLogs == nil {
		result.blindSpots = appendShadowBlindSpot(result.blindSpots, cloudWatchShadowQueryBlindSpot("cloudwatch_unavailable", nil))
		return result
	}
	evidence, parseFailed, truncated, err := m.queryCloudWatchShadowRange(ctx, parser, kind, from, to, &result.queries, &result.bytes, 0)
	if err != nil {
		result.blindSpots = appendShadowBlindSpot(result.blindSpots, cloudWatchShadowQueryBlindSpot(string(kind), err))
		result.status = cloudWatchShadowLaneStatus(err, false, 0, 0, oldCount, legacyPresent, nginx)
		return result
	}
	// Keep only the lane's expected evidence.  This protects the closed
	// comparison set if a future fixed query is broadened accidentally.
	for _, item := range evidence {
		if nginx && item.Kind == cwEvidenceNginxAccess {
			result.evidence = append(result.evidence, item)
		}
		if !nginx && item.Kind == cwEvidenceNewAPIError && isShadowRejectionCategory(item.Category) {
			result.evidence = append(result.evidence, item)
		}
	}
	if truncated {
		result.blindSpots = appendShadowBlindSpot(result.blindSpots, fmt.Sprintf("%s_truncated", kind))
	}
	if parseFailed > 0 {
		if nginx && len(result.evidence) == 0 {
			result.blindSpots = appendShadowBlindSpot(result.blindSpots, "nginx_structured_parse_failed")
		} else {
			result.blindSpots = appendShadowBlindSpot(result.blindSpots, fmt.Sprintf("%s_parse_failed", kind))
		}
	}
	result.status = cloudWatchShadowLaneStatus(nil, truncated, parseFailed, int64(len(result.evidence)), oldCount, legacyPresent, nginx)
	if result.status == cloudWatchShadowStatusBlockedLegacy {
		result.blindSpots = appendShadowBlindSpot(result.blindSpots, fmt.Sprintf("%s_legacy_baseline_missing", kind))
	}
	return result
}

func (m *Monitor) runCloudWatchShadowComparison(ctx context.Context, now time.Time) error {
	if m == nil || !m.cfg.CloudWatchShadowEnabled {
		return nil
	}
	if !m.cfg.CloudWatchShadowNginxContractReady {
		return fmt.Errorf("Nginx access 结构化日志投递尚未通过 Shadow 准入")
	}
	if m.storeDB == nil || m.cloudWatchLogs == nil || !m.cloudWatchLogs.enabled {
		return fmt.Errorf("Shadow 对账依赖的本地库或 CloudWatch Logs 不可用")
	}
	if now.IsZero() {
		now = time.Now()
	}
	to := now.UTC().Truncate(time.Minute)
	from := to.Add(-time.Duration(m.cfg.CloudWatchShadowLookbackMinutes) * time.Minute)
	if from.Unix() <= 0 || !from.Before(to) {
		return fmt.Errorf("Shadow 对账窗口不合法")
	}
	startedAt := time.Now().Unix()

	var oldNginx []NginxMinuteSample
	var oldReject []RejectionSample
	qctx, cancel := context.WithTimeout(ctx, cloudWatchShadowQueryBudget)
	defer cancel()
	if err := m.storeDB.WithContext(qctx).Where("bucket_ts >= ? AND bucket_ts < ? AND node <> ?", from.Unix(), to.Unix(), cloudWatchNginxNode).Find(&oldNginx).Error; err != nil {
		return fmt.Errorf("读取旧 Nginx 对账样本: %w", err)
	}
	if err := m.storeDB.WithContext(qctx).Where("bucket_ts >= ? AND bucket_ts < ?", from.Unix(), to.Unix()).Find(&oldReject).Error; err != nil {
		return fmt.Errorf("读取旧拒绝对账样本: %w", err)
	}
	oldNginxCount, oldRejectCount := int64(0), int64(0)
	for _, row := range oldNginx {
		if row.Count > 0 {
			oldNginxCount += row.Count
		}
	}
	for _, row := range oldReject {
		if isShadowRejectionCategory(row.Reason) && row.Count > 0 {
			oldRejectCount += row.Count
		}
	}
	parser, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		return fmt.Errorf("构造 CloudWatch Shadow 解析器: %w", err)
	}
	nginxResult := m.runCloudWatchShadowLane(qctx, parser, cwQueryWorkerShadowNginx, from, to, oldNginxCount, oldNginxCount > 0, true)
	rejectResult := m.runCloudWatchShadowLane(qctx, parser, cwQueryWorkerShadowReject, from, to, oldRejectCount, oldRejectCount > 0, false)

	finishedAt := time.Now().Unix()
	run := CloudWatchShadowReconciliationRun{
		ID: cloudWatchShadowRunID(time.Now()), WindowFrom: from.Unix(), WindowTo: to.Unix(),
		Status: cloudWatchShadowStatusPartial, CreatedAtUnix: startedAt, StartedAtUnix: startedAt, FinishedAtUnix: finishedAt,
		QueryCount: nginxResult.queries + rejectResult.queries, BytesScanned: nginxResult.bytes + rejectResult.bytes,
		NginxStatus: nginxResult.status, RejectionStatus: rejectResult.status,
		NginxOldCount: oldNginxCount, NginxNewCount: int64(len(nginxResult.evidence)),
		RejectOldCount: oldRejectCount, RejectNewCount: int64(len(rejectResult.evidence)),
	}
	blindSpots := append([]string{}, nginxResult.blindSpots...)
	for _, spot := range rejectResult.blindSpots {
		blindSpots = appendShadowBlindSpot(blindSpots, spot)
	}
	if len(blindSpots) > 0 {
		run.BlindSpots = strings.Join(blindSpots, ",")
		if len(run.BlindSpots) > 2048 {
			run.BlindSpots = run.BlindSpots[:2048]
		}
	}
	var diffs []CloudWatchShadowReconciliationDiff
	if nginxResult.status == cloudWatchShadowStatusComplete {
		diffs = append(diffs, compareShadowNginx(oldNginx, nginxResult.evidence, from.Unix(), to.Unix(), m.cfg.CloudWatchEvidenceHMACKey, finishedAt)...)
	}
	if rejectResult.status == cloudWatchShadowStatusComplete {
		diffs = append(diffs, compareShadowRejections(oldReject, rejectResult.evidence, from.Unix(), to.Unix(), m.cfg.CloudWatchEvidenceHMACKey, finishedAt)...)
	}
	if nginxResult.status == cloudWatchShadowStatusComplete && rejectResult.status == cloudWatchShadowStatusComplete {
		run.Status = cloudWatchShadowStatusComplete
	} else if nginxResult.status == cloudWatchShadowStatusFailed && rejectResult.status == cloudWatchShadowStatusFailed {
		run.Status = cloudWatchShadowStatusFailed
	}
	// AWS 查询的工作上下文可能已经接近截止；发布审计结果和清理
	// 旧记录使用独立的小窗口，避免“查询超时”连带丢掉本轮失败留痕。
	persistCtx, persistCancel := context.WithTimeout(ctx, 5*time.Second)
	defer persistCancel()
	if err := m.persistCloudWatchShadowComparison(persistCtx, run, diffs); err != nil {
		return err
	}
	if err := m.pruneCloudWatchShadow(persistCtx, now); err != nil {
		return err
	}
	if run.Status != cloudWatchShadowStatusComplete {
		return fmt.Errorf("Shadow 对账未完整覆盖: %s/%s", run.NginxStatus, run.RejectionStatus)
	}
	return nil
}

func (m *Monitor) pruneCloudWatchShadow(ctx context.Context, now time.Time) error {
	if m == nil || m.storeDB == nil || m.cfg.CloudWatchShadowRetentionDays < 1 {
		return nil
	}
	cutoff := now.Unix() - int64(m.cfg.CloudWatchShadowRetentionDays)*86400
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("created_at_unix < ?", cutoff).Delete(&CloudWatchShadowReconciliationDiff{}).Error; err != nil {
			return fmt.Errorf("清理 Shadow 差异: %w", err)
		}
		if err := tx.Where("created_at_unix < ?", cutoff).Delete(&CloudWatchShadowReconciliationRun{}).Error; err != nil {
			return fmt.Errorf("清理 Shadow 运行: %w", err)
		}
		return nil
	})
}

func (m *Monitor) startCloudWatchShadow(ctx context.Context) {
	if m == nil || !m.cfg.CloudWatchShadowEnabled || !m.cfg.CloudWatchShadowNginxContractReady {
		return
	}
	interval := time.Duration(m.cfg.CloudWatchShadowIntervalMinutes) * time.Minute
	if interval <= 0 {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-m.shutdownSignal():
				return
			case at := <-ticker.C:
				if !m.shadowRunInProgress.CompareAndSwap(false, true) {
					continue
				}
				err := m.runCloudWatchShadowComparison(ctx, at)
				m.shadowRunInProgress.Store(false)
				if err != nil {
					slog.Warn("CloudWatch Shadow 对账未完成", "err", err)
				}
			}
		}
	}()
}
