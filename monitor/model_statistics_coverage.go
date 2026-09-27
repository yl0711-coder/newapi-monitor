package monitor

import (
	"context"
	"time"
)

const (
	modelCoverageComplete    = "complete"
	modelCoveragePending     = "pending"
	modelCoverageIncomplete  = "incomplete"
	modelCoverageUnavailable = "unavailable"
)

// Coverage describes proof, not whether any requests happened. The observed
// report window stays unchanged; a normal unfinalized tail is not a lost hour.
type ModelStatisticsCoverage struct {
	Status            string `json:"status"`
	FromTs            int64  `json:"from_ts"`
	ThroughTs         int64  `json:"through_ts"`
	ExpectedThroughTs int64  `json:"expected_through_ts"`
	Note              string `json:"note"`
}

func (m *Monitor) modelStatisticsRoutedCoverage(ctx context.Context, from, to int64, now time.Time) ModelStatisticsCoverage {
	result := ModelStatisticsCoverage{Status: modelCoverageUnavailable, ExpectedThroughTs: min(to, metricFinalizeTarget(now.Unix()))}
	if m == nil || m.storeDB == nil || !m.cfg.CapacityEnabled {
		result.Note = "已路由请求：用户分钟事实采集未开启，覆盖不完整"
		return result
	}
	if m.cfg.CustomerHealthSourceEnabled {
		_, target := customerHealthSourceRange(now)
		result.ExpectedThroughTs = min(to, target)
	}
	complete, fromProof, throughProof, note := m.modelStatisticsFactsCoverage(ctx, from, to)
	result.FromTs, result.ThroughTs, result.Note = fromProof, throughProof, "已路由请求："+note
	if complete {
		result.Status = modelCoverageComplete
		return result
	}
	result.Status = modelCoverageIncomplete
	if m.cfg.CustomerHealthSourceEnabled && fromProof > from {
		result.Note = "已路由请求：独立采集水位仅证明当前日；所选历史区间覆盖未确认，不能把历史记录视为完整统计"
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
	result := ModelStatisticsCoverage{Status: modelCoverageUnavailable, Note: "无可用渠道请求：尚无连续覆盖证明，不能根据零条记录认定没有拒绝请求"}
	if m == nil || m.storeDB == nil || !m.cfg.CloudWatchPreRouteEnabled {
		return result
	}
	_, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	result.ExpectedThroughTs = min(to, target)
	var cursor CloudWatchPreRouteCursor
	if err := m.storeDB.WithContext(ctx).First(&cursor, cloudWatchPreRouteCursorID).Error; err != nil {
		return result
	}
	result.FromTs, result.ThroughTs = cursor.CoverageFromTs, minPositive(cursor.ThroughTs, to)
	result.Status = modelCoverageIncomplete
	result.Note = "无可用渠道请求：CloudWatch 前置拒绝覆盖范围不完整，所选窗口尚未全部核验"
	if cursor.SemanticsVersion != cloudWatchPreRouteVersion || cursor.CoverageFromTs <= 0 || cursor.CoverageFromTs > from {
		return result
	}
	if cursor.ThroughTs >= to {
		result.Status, result.Note = modelCoverageComplete, "无可用渠道请求：连续覆盖已核验"
	} else if result.ExpectedThroughTs > from && cursor.ThroughTs >= result.ExpectedThroughTs {
		result.Status, result.Note = modelCoveragePending, "无可用渠道请求：历史区间覆盖已核验，实时尾段等待定稿"
	}
	return result
}

func modelStatisticsCombinedCoverage(routed, rejected ModelStatisticsCoverage) string {
	if routed.Status == modelCoverageComplete && rejected.Status == modelCoverageComplete {
		return modelCoverageComplete
	}
	closed := func(status string) bool { return status == modelCoverageComplete || status == modelCoveragePending }
	if closed(routed.Status) && closed(rejected.Status) {
		return modelCoveragePending
	}
	return modelCoverageIncomplete
}
