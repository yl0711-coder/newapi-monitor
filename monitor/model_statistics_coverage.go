package monitor

import (
	"context"
	"time"

	"gorm.io/gorm"
)

const (
	modelCoverageComplete    = "complete"
	modelCoveragePending     = "pending"
	modelCoverageIncomplete  = "incomplete"
	modelCoverageUnavailable = "unavailable"
)

// modelStatisticsAvailableWindowEnd bounds the theoretical target by durable
// request/FRT/rejection cursors. This selects a window, not a coverage verdict:
// the existing full-window checks still reject missing days, old semantics,
// inconsistent projections and mixed rejection sources. Never use MAX(row.ts)
// or a left-to-right history proof here: a historical hole must not move the
// right boundary backwards and disappear from the queried interval.
func (m *Monitor) modelStatisticsAvailableWindowEnd(ctx context.Context, target int64) (int64, error) {
	end := target
	if m == nil || m.storeDB == nil || target <= 0 {
		return end, nil
	}
	bound := func(from, through int64) {
		if from > 0 && through >= from && from%60 == 0 && through%60 == 0 {
			end = min(end, through)
		}
	}
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if m.cfg.CustomerHealthSourceEnabled {
			// Read the newest tail-day certificate, not the first historical
			// prefix. A same-day live cursor overrides the ledger even after a
			// replay reset, just as customerHealthHistoricalCoverage does.
			var day CustomerHealthDayCoverage
			if err := tx.Where("day_ts <= ?", customerHealthDayStart(target)).
				Order("day_ts DESC").Limit(1).Find(&day).Error; err != nil {
				return err
			}
			var live CustomerHealthSourceCursor
			if err := tx.Where("id = ?", 1).Limit(1).Find(&live).Error; err != nil {
				return err
			}
			if live.DayTs > 0 && live.DayTs <= target && live.DayTs >= day.DayTs {
				day = CustomerHealthDayCoverage{
					DayTs: live.DayTs, ThroughTs: live.ThroughTs, SemanticsVersion: live.SemanticsVersion,
					TTFTSemanticsVersion: live.TTFTSemanticsVersion, TTFTCoverageFromTs: live.TTFTCoverageFromTs,
					TTFTCoverageThroughTs: live.TTFTCoverageThroughTs,
				}
			}
			if day.DayTs > 0 && customerHealthDayStart(day.DayTs) == day.DayTs {
				if day.SemanticsVersion == customerHealthStabilityPolicyVersion && day.ThroughTs <= day.DayTs+86400 {
					bound(day.DayTs, day.ThroughTs)
				}
				if day.TTFTSemanticsVersion == ttftCoverageSemanticsVersion && day.TTFTCoverageFromTs == day.DayTs &&
					day.TTFTCoverageThroughTs <= day.DayTs+86400 {
					bound(day.TTFTCoverageFromTs, day.TTFTCoverageThroughTs)
				}
			}
		} else if m.cfg.CapacityEnabled {
			var state MetricFinalizeState
			if err := tx.Where("id = ?", 1).Limit(1).Find(&state).Error; err != nil {
				return err
			}
			if state.SemanticsVersion == stabilityTrafficClassificationVersion {
				bound(state.CoverageFromTs, state.NextTs)
			}
			if state.TTFTSemanticsVersion == ttftCoverageSemanticsVersion {
				bound(state.TTFTCoverageFromTs, state.TTFTCoverageThroughTs)
			}
		}
		// Older read-only snapshots may have legacy rejection facts but no
		// direct-source cursor table. Preserve those partial reports; absence
		// of a table is not a watermark or a coverage certificate.
		if m.cfg.CloudWatchPreRouteEnabled && cloudWatchPreRouteCoverageTableAvailable(tx) {
			var state CloudWatchPreRouteCursor
			if err := tx.Where("id = ?", cloudWatchPreRouteCursorID).Limit(1).Find(&state).Error; err != nil {
				return err
			}
			if state.SemanticsVersion == cloudWatchPreRouteVersion {
				bound(state.CoverageFromTs, state.ThroughTs)
			}
		}
		return nil
	})
	return end, err
}

// Coverage describes proof, not whether any requests happened. The report
// window is already bounded by the closed-minute finalization watermark, so a
// normal unfinalized wall-clock tail is not a lost hour or a false TTFT gap.
type ModelStatisticsCoverage struct {
	Status string `json:"status"`
	// Source identifies the evidence lane.  A mixed value is deliberately
	// incomplete: direct CloudWatch rows and legacy collector rows cannot be
	// treated as one complete population while unknown user_id=0 identities
	// remain present in both lanes.
	Source string `json:"source"`
	// RequestsComplete and TTFTComplete are intentionally separate.  A
	// request-fact watermark may be complete while historical rows still carry
	// the pre-TTFT schema (zero-valued TTFT columns).
	RequestsComplete  bool   `json:"requests_complete"`
	TTFTComplete      bool   `json:"ttft_complete"`
	FRTComplete       bool   `json:"frt_complete"`
	FromTs            int64  `json:"from_ts"`
	ThroughTs         int64  `json:"through_ts"`
	TTFTFromTs        int64  `json:"ttft_from_ts"`
	TTFTThroughTs     int64  `json:"ttft_through_ts"`
	FRTFromTs         int64  `json:"frt_from_ts"`
	FRTThroughTs      int64  `json:"frt_through_ts"`
	ExpectedThroughTs int64  `json:"expected_through_ts"`
	Note              string `json:"note"`
}

// modelStatisticsTTFTRowsComplete rejects historical rows created before the
// exact TTFT projection existed.  Those rows have version 0 and must never be
// counted as a fast request merely because all TTFT counters are zero.
func (m *Monitor) modelStatisticsTTFTRowsComplete(ctx context.Context, from, to int64) bool {
	if m == nil || m.storeDB == nil || from >= to {
		return false
	}
	var old int64
	err := m.storeDB.WithContext(ctx).Raw(`SELECT COUNT(*) FROM capacity_user_minute_samples
		WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
		  AND (((success + anomaly + failed > 0 OR tokens <> 0 OR quota <> 0 OR refund_quota <> 0)
		        AND COALESCE(ttft_semantics_version,0) <> ?)
		       OR `+ttftInvalidRowPredicate("capacity_user_minute_samples")+`)`, from, to,
		stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion).Scan(&old).Error
	return err == nil && old == 0
}

func (m *Monitor) modelStatisticsRoutedCoverage(ctx context.Context, from, to int64, now time.Time) ModelStatisticsCoverage {
	result := ModelStatisticsCoverage{Status: modelCoverageUnavailable, ExpectedThroughTs: min(to, metricFinalizeTarget(now.Unix()))}
	if m == nil || m.storeDB == nil || (!m.cfg.CapacityEnabled && !m.cfg.CustomerHealthSourceEnabled) {
		result.Note = "已路由请求：用户分钟事实采集未开启，覆盖不完整"
		return result
	}
	if m.cfg.CustomerHealthSourceEnabled {
		_, target := customerHealthSourceRange(now)
		result.ExpectedThroughTs = min(to, target)
	}
	complete, fromProof, throughProof, note := m.modelStatisticsFactsCoverage(ctx, from, to)
	result.FromTs, result.ThroughTs, result.Note = fromProof, throughProof, "已路由请求："+note
	result.RequestsComplete = complete
	result.TTFTFromTs, result.TTFTThroughTs = fromProof, throughProof
	result.TTFTComplete = complete && m.modelStatisticsTTFTRowsComplete(ctx, from, to)
	if !m.cfg.CustomerHealthSourceEnabled {
		result.TTFTComplete = result.TTFTComplete && capacityTTFTProjectionComplete(ctx, m.storeDB, from, to)
	}
	if m.cfg.CustomerHealthSourceEnabled {
		proof, err := m.customerHealthHistoricalCoverage(ctx, from, to)
		result.TTFTFromTs, result.TTFTThroughTs = proof.FRTFromTs, proof.FRTThroughTs
		result.TTFTComplete = result.TTFTComplete && err == nil && proof.FRTComplete
	} else {
		var state MetricFinalizeState
		if err := m.storeDB.WithContext(ctx).First(&state, 1).Error; err == nil &&
			state.TTFTSemanticsVersion == ttftCoverageSemanticsVersion {
			result.TTFTFromTs, result.TTFTThroughTs = state.TTFTCoverageFromTs, min(state.TTFTCoverageThroughTs, to)
			result.TTFTComplete = result.TTFTComplete && state.TTFTCoverageFromTs > 0 &&
				state.TTFTCoverageFromTs <= from && state.TTFTCoverageThroughTs >= to
		} else {
			result.TTFTComplete = false
		}
	}
	if !result.TTFTComplete {
		result.Note += "；FRT 覆盖尚未确认，历史缺失样本不计入快速请求"
	}
	if complete {
		result.Status = modelCoverageComplete
		result.RequestsComplete = true
		return result
	}
	result.Status = modelCoverageIncomplete
	if m.cfg.CustomerHealthSourceEnabled && fromProof > from {
		result.Note = "已路由请求：独立采集正在补齐历史区间；所选窗口尚未连续覆盖，统计未完成"
		return result
	}
	if throughProof < to && result.ExpectedThroughTs > from && result.ExpectedThroughTs < to {
		closedComplete, _, _, _ := m.modelStatisticsFactsCoverage(ctx, from, result.ExpectedThroughTs)
		if closedComplete {
			result.Status = modelCoveragePending
			result.Note = "已路由请求：历史区间覆盖已核验，实时尾段等待定稿，已采集请求仍计入展示"
		}
	}
	return result
}

func (m *Monitor) modelStatisticsRejectionCoverage(ctx context.Context, from, to int64, now time.Time) ModelStatisticsCoverage {
	result := ModelStatisticsCoverage{Status: modelCoverageUnavailable, Source: "collector", TTFTComplete: true, Note: "无可用渠道请求：尚无连续覆盖证明，不能根据零条记录认定没有拒绝请求"}
	if m == nil || m.storeDB == nil || !m.cfg.CloudWatchPreRouteEnabled {
		return result
	}
	result.Source = "cloudwatch"
	_, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	result.ExpectedThroughTs = min(to, target)
	var cursor CloudWatchPreRouteCursor
	if err := m.storeDB.WithContext(ctx).First(&cursor, cloudWatchPreRouteCursorID).Error; err != nil {
		return result
	}
	retainedFrom := m.retainedRejectionCoverageFrom(cursor.CoverageFromTs, now.Unix())
	result.FromTs, result.ThroughTs = retainedFrom, minPositive(cursor.ThroughTs, to)
	result.Status = modelCoverageIncomplete
	result.Note = "无可用渠道请求：CloudWatch 前置拒绝覆盖范围不完整，所选窗口尚未全部核验"
	if cursor.SemanticsVersion != cloudWatchPreRouteVersion || cursor.CoverageFromTs <= 0 || retainedFrom > from {
		if retainedFrom > 0 && from < retainedFrom {
			result.Source = "mixed"
		}
		return result
	}
	if to > target {
		result.Source = "mixed"
		return result
	}
	if legacy, err := m.alertsHasLegacyRows(from, to); err != nil {
		result.Source = "mixed"
		result.Note = "无可用渠道请求：无法确认 CloudWatch 与旧采集器来源边界，覆盖不完整"
		return result
	} else if legacy {
		result.Source = "mixed"
		result.Note = "无可用渠道请求：CloudWatch 与旧采集器记录混合（user_id=0 无法去重），覆盖不完整"
		return result
	}
	if cursor.ThroughTs >= to {
		result.Status, result.Note = modelCoverageComplete, "无可用渠道请求：连续覆盖已核验"
		result.RequestsComplete = true
	} else if result.ExpectedThroughTs > from && cursor.ThroughTs >= result.ExpectedThroughTs {
		result.Status, result.Note = modelCoveragePending, "无可用渠道请求：历史区间覆盖已核验，实时尾段等待定稿"
	}
	return result
}

func modelStatisticsCombinedCoverage(routed, rejected ModelStatisticsCoverage) string {
	requestsComplete := routed.Status == modelCoverageComplete && rejected.Status == modelCoverageComplete
	ttftComplete := routed.TTFTComplete && rejected.TTFTComplete
	if requestsComplete && ttftComplete {
		return modelCoverageComplete
	}
	if requestsComplete && !ttftComplete {
		return modelCoverageIncomplete
	}
	closed := func(status string) bool { return status == modelCoverageComplete || status == modelCoveragePending }
	if closed(routed.Status) && closed(rejected.Status) && ttftComplete {
		return modelCoveragePending
	}
	return modelCoverageIncomplete
}
