package monitor

// cloudwatch_shadow_reconciliation.go contains the phase-4 Shadow foundation.
//
// The Shadow lane is deliberately separate from the customer-facing
// investigation path.  It compares bounded, normalized aggregates from the
// legacy collectors with structured CloudWatch evidence and persists only
// anonymous run/difference summaries.  It does not start a scheduler or read
// AWS by itself; the production 7-day clock is opened only by the later
// runner after the CloudWatch Nginx access-log delivery contract is ready.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strings"
	"time"

	"gorm.io/gorm"
)

const (
	cloudWatchShadowLaneNginxAccess = "nginx_access"
	cloudWatchShadowLaneRejection   = "newapi_rejection"

	cloudWatchShadowStatusPrepared           = "prepared"
	cloudWatchShadowStatusRunning            = "running"
	cloudWatchShadowStatusComplete           = "complete"
	cloudWatchShadowStatusPartial            = "partial"
	cloudWatchShadowStatusFailed             = "failed"
	cloudWatchShadowStatusBlockedLogContract = "blocked_log_contract"
	cloudWatchShadowStatusBlockedLegacy      = "blocked_legacy_baseline"
)

// CloudWatchShadowReconciliationRun is the durable, low-cardinality header
// for one bounded comparison window.  It intentionally contains no raw
// Request ID, customer IP, log body, model, group or user identity.
type CloudWatchShadowReconciliationRun struct {
	ID             string `gorm:"primaryKey;size:64"`
	WindowFrom     int64  `gorm:"index:idx_cw_shadow_run_window,priority:1;not null"`
	WindowTo       int64  `gorm:"index:idx_cw_shadow_run_window,priority:2;not null"`
	Status         string `gorm:"size:24;index;not null"`
	CreatedAtUnix  int64  `gorm:"index;not null"`
	StartedAtUnix  int64  `gorm:"not null"`
	FinishedAtUnix int64  `gorm:"not null"`
	QueryCount     int    `gorm:"not null"`
	BytesScanned   int64  `gorm:"not null"`

	NginxStatus     string `gorm:"size:24;not null"`
	RejectionStatus string `gorm:"size:24;not null"`
	NginxOldCount   int64  `gorm:"not null"`
	NginxNewCount   int64  `gorm:"not null"`
	RejectOldCount  int64  `gorm:"not null"`
	RejectNewCount  int64  `gorm:"not null"`
	DifferenceCount int    `gorm:"not null"`
	BlindSpots      string `gorm:"size:2048;not null"`
}

// CloudWatchShadowReconciliationDiff stores only a closed metric set and an
// HMAC of the comparison dimension.  DimensionHMAC lets the operator group
// repeated differences without putting a route/model/group/user value into
// the durable audit store.
type CloudWatchShadowReconciliationDiff struct {
	ID               uint   `gorm:"primaryKey"`
	RunID            string `gorm:"size:64;index:idx_cw_shadow_diff_run,priority:1;not null"`
	Lane             string `gorm:"size:24;index:idx_cw_shadow_diff_run,priority:2;not null"`
	BucketTs         int64  `gorm:"index:idx_cw_shadow_diff_bucket;not null"`
	DimensionClass   string `gorm:"size:48;not null"`
	DimensionHMAC    string `gorm:"size:64;index;not null"`
	OldCount         int64  `gorm:"not null"`
	NewCount         int64  `gorm:"not null"`
	Delta            int64  `gorm:"not null"`
	OldRequestSumMS  int64  `gorm:"not null"`
	NewRequestSumMS  int64  `gorm:"not null"`
	OldRequestMaxMS  int64  `gorm:"not null"`
	NewRequestMaxMS  int64  `gorm:"not null"`
	OldUpstreamSumMS int64  `gorm:"not null"`
	NewUpstreamSumMS int64  `gorm:"not null"`
	OldRequestIDs    int64  `gorm:"not null"`
	NewRequestIDs    int64  `gorm:"not null"`
	DifferenceMask   string `gorm:"size:160;not null"`
	CreatedAtUnix    int64  `gorm:"index;not null"`
}

type shadowNginxDimension struct {
	BucketTs       int64
	Route          string
	Method         string
	Status         int
	UpstreamStatus int
}

type shadowNginxAggregate struct {
	OldCount         int64
	NewCount         int64
	OldRequestSumMS  int64
	NewRequestSumMS  int64
	OldRequestMaxMS  int64
	NewRequestMaxMS  int64
	OldUpstreamSumMS int64
	NewUpstreamSumMS int64
	OldRequestIDs    int64
	NewRequestIDs    int64
}

type shadowRejectionDimension struct {
	BucketTs int64
	Reason   string
	Model    string
	Group    string
	UserID   int64
}

type shadowRejectionAggregate struct {
	OldCount int64
	NewCount int64
}

func shadowMinute(ts int64) int64 {
	if ts <= 0 {
		return 0
	}
	return ts - ts%60
}

func shadowEventInWindow(ts, from, to int64) bool {
	return ts >= from && ts < to
}

func shadowRoute(value string) string {
	value = strings.TrimSpace(value)
	if value == "" {
		return ""
	}
	return cwRoute(value)
}

func shadowLastUpstreamStatus(values []int) int {
	if len(values) == 0 {
		return 0
	}
	return values[len(values)-1]
}

func aggregateShadowNginxLegacy(rows []NginxMinuteSample, from, to int64) map[shadowNginxDimension]shadowNginxAggregate {
	out := make(map[shadowNginxDimension]shadowNginxAggregate)
	for _, row := range rows {
		bucket := shadowMinute(row.BucketTs)
		if bucket == 0 || !shadowEventInWindow(bucket, from, to) {
			continue
		}
		key := shadowNginxDimension{BucketTs: bucket, Route: shadowRoute(row.Route), Method: strings.ToUpper(strings.TrimSpace(row.Method)), Status: row.Status, UpstreamStatus: row.UpstreamStatus}
		item := out[key]
		item.OldCount += row.Count
		item.OldRequestSumMS += row.RequestTimeSumMS
		if row.RequestTimeMaxMS > item.OldRequestMaxMS {
			item.OldRequestMaxMS = row.RequestTimeMaxMS
		}
		item.OldUpstreamSumMS += row.UpstreamTimeSumMS
		item.OldRequestIDs += row.RequestIDPresent
		out[key] = item
	}
	return out
}

func aggregateShadowNginxCloudWatch(evidence []cloudWatchStructuredEvidence, from, to int64) map[shadowNginxDimension]shadowNginxAggregate {
	out := make(map[shadowNginxDimension]shadowNginxAggregate)
	for _, item := range evidence {
		if item.Kind != cwEvidenceNginxAccess || !shadowEventInWindow(item.EventMS/1000, from, to) || item.Status == nil || item.RequestMS == nil {
			continue
		}
		key := shadowNginxDimension{BucketTs: shadowMinute(item.EventMS / 1000), Route: shadowRoute(item.Route), Method: strings.ToUpper(strings.TrimSpace(item.Method)), Status: *item.Status, UpstreamStatus: shadowLastUpstreamStatus(item.UpstreamStatuses)}
		row := out[key]
		row.NewCount++
		row.NewRequestSumMS += *item.RequestMS
		if *item.RequestMS > row.NewRequestMaxMS {
			row.NewRequestMaxMS = *item.RequestMS
		}
		if item.UpstreamMS != nil {
			row.NewUpstreamSumMS += *item.UpstreamMS
		}
		if item.OneAPIIDHMAC != "" {
			row.NewRequestIDs++
		}
		out[key] = row
	}
	return out
}

func shadowNginxDimensionKey(key shadowNginxDimension) string {
	return fmt.Sprintf("%d\x00%s\x00%s\x00%d\x00%d", key.BucketTs, key.Route, key.Method, key.Status, key.UpstreamStatus)
}

func shadowDimensionHMAC(secret, lane, canonical string) string {
	if strings.TrimSpace(secret) == "" {
		// This fallback is used only by pure unit tests.  Production persistence
		// rejects an empty CloudWatch evidence key before this value is stored.
		h := sha256.Sum256([]byte(lane + "\x00" + canonical))
		return hex.EncodeToString(h[:])
	}
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte("cloudwatch-shadow-dimension\x00"))
	_, _ = mac.Write([]byte(lane))
	_, _ = mac.Write([]byte{0})
	_, _ = mac.Write([]byte(canonical))
	return hex.EncodeToString(mac.Sum(nil))
}

func shadowAppendMask(parts []string, value string) []string {
	for _, existing := range parts {
		if existing == value {
			return parts
		}
	}
	return append(parts, value)
}

func shadowNginxDiff(key shadowNginxDimension, agg shadowNginxAggregate, secret string, now int64) (CloudWatchShadowReconciliationDiff, bool) {
	mask := make([]string, 0, 6)
	if agg.OldCount != agg.NewCount {
		mask = shadowAppendMask(mask, "count")
	}
	if agg.OldRequestSumMS != agg.NewRequestSumMS {
		mask = shadowAppendMask(mask, "request_sum_ms")
	}
	if agg.OldRequestMaxMS != agg.NewRequestMaxMS {
		mask = shadowAppendMask(mask, "request_max_ms")
	}
	if agg.OldUpstreamSumMS != agg.NewUpstreamSumMS {
		mask = shadowAppendMask(mask, "upstream_sum_ms")
	}
	if agg.OldRequestIDs != agg.NewRequestIDs {
		mask = shadowAppendMask(mask, "request_id_coverage")
	}
	if len(mask) == 0 {
		return CloudWatchShadowReconciliationDiff{}, false
	}
	canonical := shadowNginxDimensionKey(key)
	return CloudWatchShadowReconciliationDiff{
		Lane: cloudWatchShadowLaneNginxAccess, BucketTs: key.BucketTs,
		DimensionClass: "minute_route_method_status_upstream",
		DimensionHMAC:  shadowDimensionHMAC(secret, cloudWatchShadowLaneNginxAccess, canonical),
		OldCount:       agg.OldCount, NewCount: agg.NewCount, Delta: agg.NewCount - agg.OldCount,
		OldRequestSumMS: agg.OldRequestSumMS, NewRequestSumMS: agg.NewRequestSumMS,
		OldRequestMaxMS: agg.OldRequestMaxMS, NewRequestMaxMS: agg.NewRequestMaxMS,
		OldUpstreamSumMS: agg.OldUpstreamSumMS, NewUpstreamSumMS: agg.NewUpstreamSumMS,
		OldRequestIDs: agg.OldRequestIDs, NewRequestIDs: agg.NewRequestIDs,
		DifferenceMask: strings.Join(mask, ","), CreatedAtUnix: now,
	}, true
}

// compareShadowNginx compares the old minute aggregates with the structured
// CloudWatch events. Missing dimensions are intentional differences; they are
// never silently treated as zero coverage.
func compareShadowNginx(oldRows []NginxMinuteSample, newEvidence []cloudWatchStructuredEvidence, from, to int64, secret string, now int64) []CloudWatchShadowReconciliationDiff {
	oldAgg := aggregateShadowNginxLegacy(oldRows, from, to)
	newAgg := aggregateShadowNginxCloudWatch(newEvidence, from, to)
	keys := make(map[shadowNginxDimension]struct{}, len(oldAgg)+len(newAgg))
	for key := range oldAgg {
		keys[key] = struct{}{}
	}
	for key := range newAgg {
		keys[key] = struct{}{}
	}
	ordered := make([]shadowNginxDimension, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool { return shadowNginxDimensionKey(ordered[i]) < shadowNginxDimensionKey(ordered[j]) })
	result := make([]CloudWatchShadowReconciliationDiff, 0)
	for _, key := range ordered {
		agg := oldAgg[key]
		newPart := newAgg[key]
		agg.NewCount = newPart.NewCount
		agg.NewRequestSumMS = newPart.NewRequestSumMS
		agg.NewRequestMaxMS = newPart.NewRequestMaxMS
		agg.NewUpstreamSumMS = newPart.NewUpstreamSumMS
		agg.NewRequestIDs = newPart.NewRequestIDs
		if diff, ok := shadowNginxDiff(key, agg, secret, now); ok {
			result = append(result, diff)
		}
	}
	return result
}

func canonicalShadowRejectionReason(value string) string {
	lower := strings.ToLower(strings.TrimSpace(value))
	lower = strings.NewReplacer("-", "_", " ", "_", ".", "_").Replace(lower)
	switch lower {
	case "no_channel", "no_available_channel", "route_no_channel", "noavailablechannel":
		return "route_no_channel"
	case "invalid_token", "token_invalid":
		return "invalid_token"
	case "token_disabled", "disabled_token":
		return "token_disabled"
	case "model_forbidden", "token_model_forbidden":
		return "model_forbidden"
	case "model_not_found", "unknown_model":
		return "model_not_found"
	case "quota_account", "quota_insufficient", "insufficient_quota", "user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed":
		return "quota_account"
	case "rate_limited", "rate_limit", "too_many_requests":
		return "rate_limited"
	default:
		return lower
	}
}

func isShadowRejectionCategory(category string) bool {
	switch canonicalShadowRejectionReason(category) {
	case "route_no_channel", "invalid_token", "token_disabled", "model_forbidden", "model_not_found", "quota_account", "rate_limited":
		return true
	default:
		return false
	}
}

func aggregateShadowRejectionsLegacy(rows []RejectionSample, from, to int64) map[shadowRejectionDimension]shadowRejectionAggregate {
	out := make(map[shadowRejectionDimension]shadowRejectionAggregate)
	for _, row := range rows {
		// RejectionSample 是历史旁路表，旧采集器可能曾写入未知 reason。
		// Shadow 只对账闭集的前置拒绝类别；普通 NewAPI 错误必须和新侧
		// 一样排除，否则会被错误地当成 CloudWatch 漏采。
		if !isShadowRejectionCategory(row.Reason) {
			continue
		}
		bucket := shadowMinute(row.BucketTs)
		if bucket == 0 || !shadowEventInWindow(bucket, from, to) {
			continue
		}
		key := shadowRejectionDimension{BucketTs: bucket, Reason: canonicalShadowRejectionReason(row.Reason), Model: strings.TrimSpace(row.Model), Group: strings.TrimSpace(row.Grp), UserID: row.UserID}
		item := out[key]
		item.OldCount += row.Count
		out[key] = item
	}
	return out
}

func aggregateShadowRejectionsCloudWatch(evidence []cloudWatchStructuredEvidence, from, to int64) map[shadowRejectionDimension]shadowRejectionAggregate {
	out := make(map[shadowRejectionDimension]shadowRejectionAggregate)
	for _, item := range evidence {
		if item.Kind != cwEvidenceNewAPIError || !isShadowRejectionCategory(item.Category) || !shadowEventInWindow(item.EventMS/1000, from, to) {
			continue
		}
		userID := int64(0)
		if item.UserID != nil {
			userID = *item.UserID
		}
		key := shadowRejectionDimension{BucketTs: shadowMinute(item.EventMS / 1000), Reason: canonicalShadowRejectionReason(item.Category), Model: strings.TrimSpace(item.Model), Group: strings.TrimSpace(item.Group), UserID: userID}
		row := out[key]
		row.NewCount++
		out[key] = row
	}
	return out
}

func shadowRejectionDimensionKey(key shadowRejectionDimension) string {
	return fmt.Sprintf("%d\x00%s\x00%s\x00%s\x00%d", key.BucketTs, key.Reason, key.Model, key.Group, key.UserID)
}

func shadowRejectionDiff(key shadowRejectionDimension, agg shadowRejectionAggregate, secret string, now int64) (CloudWatchShadowReconciliationDiff, bool) {
	if agg.OldCount == agg.NewCount {
		return CloudWatchShadowReconciliationDiff{}, false
	}
	return CloudWatchShadowReconciliationDiff{
		Lane: cloudWatchShadowLaneRejection, BucketTs: key.BucketTs,
		DimensionClass: "minute_reason_model_group_user",
		DimensionHMAC:  shadowDimensionHMAC(secret, cloudWatchShadowLaneRejection, shadowRejectionDimensionKey(key)),
		OldCount:       agg.OldCount, NewCount: agg.NewCount, Delta: agg.NewCount - agg.OldCount,
		DifferenceMask: "count", CreatedAtUnix: now,
	}, true
}

func compareShadowRejections(oldRows []RejectionSample, newEvidence []cloudWatchStructuredEvidence, from, to int64, secret string, now int64) []CloudWatchShadowReconciliationDiff {
	oldAgg := aggregateShadowRejectionsLegacy(oldRows, from, to)
	newAgg := aggregateShadowRejectionsCloudWatch(newEvidence, from, to)
	keys := make(map[shadowRejectionDimension]struct{}, len(oldAgg)+len(newAgg))
	for key := range oldAgg {
		keys[key] = struct{}{}
	}
	for key := range newAgg {
		keys[key] = struct{}{}
	}
	ordered := make([]shadowRejectionDimension, 0, len(keys))
	for key := range keys {
		ordered = append(ordered, key)
	}
	sort.Slice(ordered, func(i, j int) bool {
		return shadowRejectionDimensionKey(ordered[i]) < shadowRejectionDimensionKey(ordered[j])
	})
	result := make([]CloudWatchShadowReconciliationDiff, 0)
	for _, key := range ordered {
		agg := oldAgg[key]
		agg.NewCount = newAgg[key].NewCount
		if diff, ok := shadowRejectionDiff(key, agg, secret, now); ok {
			result = append(result, diff)
		}
	}
	return result
}

func cloudWatchShadowRunID(now time.Time) string {
	return fmt.Sprintf("shadow_%d", now.UnixNano())
}

// persistCloudWatchShadowComparison atomically publishes a run header and its
// differences.  A half-written run is never exposed as a completed run, and
// the legacy collector tables are read-only inputs to this transaction.
func (m *Monitor) persistCloudWatchShadowComparison(ctx context.Context, run CloudWatchShadowReconciliationRun, diffs []CloudWatchShadowReconciliationDiff) error {
	if m == nil || m.storeDB == nil {
		return fmt.Errorf("shadow 对账库不可用")
	}
	if strings.TrimSpace(run.ID) == "" || run.WindowFrom <= 0 || run.WindowTo <= run.WindowFrom {
		return fmt.Errorf("shadow 对账窗口或运行 ID 不合法")
	}
	if len(run.ID) > 64 || strings.IndexFunc(run.ID, func(r rune) bool { return r < 0x21 || r == 0x7f }) >= 0 {
		return fmt.Errorf("shadow 运行 ID 不合法")
	}
	if len(m.cfg.CloudWatchEvidenceHMACKey) < 32 || !cwValidKeyID(m.cfg.CloudWatchEvidenceHMACKeyID) {
		return fmt.Errorf("shadow 对账未配置合规的 CloudWatch 证据 HMAC 密钥")
	}
	switch run.Status {
	case cloudWatchShadowStatusPrepared, cloudWatchShadowStatusRunning, cloudWatchShadowStatusComplete, cloudWatchShadowStatusPartial, cloudWatchShadowStatusFailed:
	default:
		return fmt.Errorf("shadow 运行状态不受支持: %q", run.Status)
	}
	if run.QueryCount < 0 || run.BytesScanned < 0 {
		return fmt.Errorf("shadow 查询成本不能为负数")
	}
	if run.StartedAtUnix < 0 || run.FinishedAtUnix < 0 || (run.FinishedAtUnix > 0 && run.StartedAtUnix > 0 && run.FinishedAtUnix < run.StartedAtUnix) {
		return fmt.Errorf("shadow 运行时间戳不合法")
	}
	for lane, status := range map[string]string{cloudWatchShadowLaneNginxAccess: run.NginxStatus, cloudWatchShadowLaneRejection: run.RejectionStatus} {
		switch status {
		case cloudWatchShadowStatusPrepared, cloudWatchShadowStatusRunning, cloudWatchShadowStatusComplete, cloudWatchShadowStatusPartial, cloudWatchShadowStatusFailed, cloudWatchShadowStatusBlockedLogContract, cloudWatchShadowStatusBlockedLegacy:
		default:
			return fmt.Errorf("shadow %s 泳道状态不受支持: %q", lane, status)
		}
	}
	if run.Status == cloudWatchShadowStatusComplete && (run.NginxStatus != cloudWatchShadowStatusComplete || run.RejectionStatus != cloudWatchShadowStatusComplete) {
		return fmt.Errorf("shadow 完成状态与泳道状态不一致")
	}
	if len(run.BlindSpots) > 2048 || strings.IndexFunc(run.BlindSpots, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
		return fmt.Errorf("shadow blind spot 不合法")
	}
	if run.CreatedAtUnix == 0 {
		run.CreatedAtUnix = time.Now().Unix()
	}
	if run.FinishedAtUnix == 0 && run.Status == cloudWatchShadowStatusComplete {
		run.FinishedAtUnix = run.CreatedAtUnix
	}
	run.DifferenceCount = len(diffs)
	for i := range diffs {
		diffs[i].RunID = run.ID
		if diffs[i].CreatedAtUnix == 0 {
			diffs[i].CreatedAtUnix = run.CreatedAtUnix
		}
		if diffs[i].Lane != cloudWatchShadowLaneNginxAccess && diffs[i].Lane != cloudWatchShadowLaneRejection {
			return fmt.Errorf("shadow 差异泳道不受支持: %q", diffs[i].Lane)
		}
		if len(diffs[i].DimensionHMAC) != sha256.Size*2 {
			return fmt.Errorf("shadow 差异维度引用不是 SHA-256 HMAC")
		}
		if _, err := hex.DecodeString(diffs[i].DimensionHMAC); err != nil {
			return fmt.Errorf("shadow 差异维度引用不是十六进制 HMAC")
		}
		if diffs[i].BucketTs < run.WindowFrom || diffs[i].BucketTs >= run.WindowTo || diffs[i].OldCount < 0 || diffs[i].NewCount < 0 || diffs[i].Delta != diffs[i].NewCount-diffs[i].OldCount || diffs[i].OldRequestSumMS < 0 || diffs[i].NewRequestSumMS < 0 || diffs[i].OldRequestMaxMS < 0 || diffs[i].NewRequestMaxMS < 0 || diffs[i].OldUpstreamSumMS < 0 || diffs[i].NewUpstreamSumMS < 0 || diffs[i].OldRequestIDs < 0 || diffs[i].NewRequestIDs < 0 {
			return fmt.Errorf("shadow 差异指标或时间桶不合法")
		}
		if strings.TrimSpace(diffs[i].DifferenceMask) == "" || strings.IndexFunc(diffs[i].DifferenceMask, func(r rune) bool { return r < 0x20 || r == 0x7f }) >= 0 {
			return fmt.Errorf("shadow 差异掩码不合法")
		}
		for _, part := range strings.Split(diffs[i].DifferenceMask, ",") {
			switch strings.TrimSpace(part) {
			case "count", "request_sum_ms", "request_max_ms", "upstream_sum_ms", "request_id_coverage":
			default:
				return fmt.Errorf("shadow 差异掩码包含未知指标: %q", part)
			}
		}
	}
	return m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Create(&run).Error; err != nil {
			return fmt.Errorf("写入 shadow 对账运行: %w", err)
		}
		for start := 0; start < len(diffs); start += 200 {
			end := start + 200
			if end > len(diffs) {
				end = len(diffs)
			}
			if err := tx.CreateInBatches(diffs[start:end], 200).Error; err != nil {
				return fmt.Errorf("写入 shadow 对账差异: %w", err)
			}
		}
		return nil
	})
}
