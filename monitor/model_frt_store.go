package monitor

import (
	"context"
	"strconv"
	"strings"
)

func (m *Monitor) modelFRTRows(ctx context.Context, from, to int64, filter ModelFRTFilter) ([]modelFRTRow, error) {
	// Keep the old page's per-user HAVING population, then merge to leaf keys in
	// SQLite. Validate each original row before SUM: corrupt rows cannot cancel.
	columns := []string{"ttft_500", "ttft_1k", "ttft_2k", "ttft_5k", "ttft_10k", "ttft_inf", "ttft_observed", "ttft_over_3s"}
	inner, outer := []string{}, []string{}
	for _, column := range columns {
		sum := "COALESCE(SUM(" + column + "),0) AS " + column
		inner, outer = append(inner, sum), append(outer, sum)
	}
	nulls := []string{"ttft_max_ms IS NULL"}
	for _, column := range columns {
		nulls = append(nulls, column+" IS NULL")
	}
	invalid := ttftInvalidRowPredicate("capacity_user_minute_samples") + " OR " + strings.Join(nulls, " OR ")
	where := ""
	args := []any{ttftCoverageSemanticsVersion, from, to, stabilityTrafficClassificationVersion}
	if filter.ChannelID != nil {
		where += " AND channel_id = ?"
		args = append(args, *filter.ChannelID)
	}
	if filter.Group != nil {
		where += " AND TRIM(grp) = ?"
		args = append(args, *filter.Group)
	}
	if filter.Model != nil {
		where += " AND TRIM(model_name) = ?"
		args = append(args, *filter.Model)
	}
	args = append(args, modelFRTMaxLeaves+1)
	// Validate before SUM so negative/missing rows cannot cancel. Empty historical
	// rows add no samples; history with valid samples but no sum blocks only mean.
	version := strconv.Itoa(frtSumSemanticsVersion)
	meanUnsupported := "ttft_observed > 0 AND (COALESCE(frt_sum_version,0) <> " + version + " OR frt_sum_ms IS NULL)"
	meanInvalid := "ttft_observed > 0 AND frt_sum_version = " + version + " AND frt_sum_ms IS NOT NULL AND (typeof(frt_sum_ms) <> 'integer' OR frt_sum_ms <= 0 OR frt_sum_observed IS NULL OR frt_sum_observed <> ttft_observed OR frt_sum_ms < ttft_max_ms OR frt_sum_ms / ttft_observed > ttft_max_ms OR (frt_sum_ms / ttft_observed = ttft_max_ms AND frt_sum_ms % ttft_observed > 0))"
	meanValid := "ttft_observed > 0 AND NOT (" + meanUnsupported + ") AND NOT (" + meanInvalid + ")"
	meanProjection := `SUM(CASE WHEN (` + meanValid + `) THEN frt_sum_ms ELSE 0 END) AS frt_sum_ms,
   SUM(CASE WHEN (` + meanValid + `) THEN frt_sum_observed ELSE 0 END) AS frt_sum_observed,
   SUM(CASE WHEN (` + meanUnsupported + `) THEN 1 ELSE 0 END) AS mean_unsupported_rows,
   SUM(CASE WHEN (` + meanInvalid + `) THEN 1 ELSE 0 END) AS mean_invalid_rows,`
	// A read-only legacy snapshot cannot be migrated. Keep its histogram usable
	// while declaring the mean unsupported; this only inspects local schema.
	var sumColumns int64
	if err := m.storeDB.WithContext(ctx).Raw("SELECT COUNT(*) FROM pragma_table_info('capacity_user_minute_samples') WHERE name IN ('frt_sum_ms','frt_sum_observed','frt_sum_version')").Scan(&sumColumns).Error; err != nil {
		return nil, err
	}
	if sumColumns != 3 {
		meanProjection = "0 AS frt_sum_ms, 0 AS frt_sum_observed, SUM(CASE WHEN ttft_observed > 0 THEN 1 ELSE 0 END) AS mean_unsupported_rows, 0 AS mean_invalid_rows,"
	}
	query := `SELECT channel_id, grp, model, ` + strings.Join(outer, ",") + `,
  MAX(ttft_max_ms) AS ttft_max_ms,
  SUM(frt_sum_ms) AS frt_sum_ms, SUM(frt_sum_observed) AS frt_sum_observed,
  SUM(mean_unsupported_rows) AS mean_unsupported_rows, SUM(mean_invalid_rows) AS mean_invalid_rows,
  SUM(historical_rows) AS historical_rows, SUM(invalid_rows) AS invalid_rows
  FROM (SELECT channel_id, grp, model_name AS model, ` + strings.Join(inner, ",") + `,
   COALESCE(MAX(ttft_max_ms),0) AS ttft_max_ms,
` + meanProjection + `
   SUM(CASE WHEN COALESCE(ttft_semantics_version,0) <> ? THEN 1 ELSE 0 END) AS historical_rows,
   SUM(CASE WHEN (` + invalid + `) THEN 1 ELSE 0 END) AS invalid_rows
   FROM capacity_user_minute_samples
   WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ? AND user_id >= 0 ` + where + `
   GROUP BY grp, model_name, user_id, channel_id
   HAVING SUM(CASE WHEN success+anomaly+failed > 0 THEN success+anomaly+failed ELSE 0 END) > 0
  ) GROUP BY channel_id, grp, model LIMIT ?`
	var rows []modelFRTRow
	if err := m.storeDB.WithContext(ctx).Raw(query, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	if len(rows) > modelFRTMaxLeaves {
		return nil, errModelFRTLimit
	}
	return rows, nil
}

func (m *Monitor) modelFRTCoverage(ctx context.Context, from, to int64) (ModelFRTCoverage, error) {
	result := ModelFRTCoverage{Status: modelCoverageUnavailable, Note: "用户分钟事实采集未开启；已存样本不代表所选窗口完整。"}
	if !m.cfg.CustomerHealthSourceEnabled && !m.cfg.CapacityEnabled {
		return result, nil
	}
	result.Status = modelCoverageIncomplete
	var requestComplete, frtComplete bool
	if m.cfg.CustomerHealthSourceEnabled {
		proof, err := m.customerHealthHistoricalCoverage(ctx, from, to)
		if err != nil {
			return result, err
		}
		requestComplete, frtComplete = proof.RequestsComplete, proof.FRTComplete
		result.From, result.Through = modelFRTTime(proof.FRTFromTs), modelFRTTime(proof.FRTThroughTs)
	} else {
		var state MetricFinalizeState
		if err := m.storeDB.WithContext(ctx).Where("id = ?", 1).Limit(1).Find(&state).Error; err != nil {
			return result, err
		}
		var err error
		requestComplete, err = m.capacityWindowCoverage(ctx, from, to, true)
		if err != nil {
			return result, err
		}
		frtComplete, err = capacityTTFTProjectionCoverage(ctx, m.storeDB, from, to)
		if err != nil {
			return result, err
		}
		frtComplete = frtComplete && state.TTFTSemanticsVersion == ttftCoverageSemanticsVersion && state.TTFTCoverageFromTs > 0 && state.TTFTCoverageFromTs <= from && state.TTFTCoverageThroughTs >= to
		if state.TTFTSemanticsVersion == ttftCoverageSemanticsVersion {
			result.From, result.Through = modelFRTTime(state.TTFTCoverageFromTs), modelFRTTime(min(state.TTFTCoverageThroughTs, to))
		}
	}
	rowsComplete, err := m.modelStatisticsTTFTRowsCoverage(ctx, from, to)
	if err != nil {
		return result, err
	}
	result.Complete = requestComplete && frtComplete && rowsComplete
	result.Note = "窗口存在采集缺口、未支持的历史数据或无效投影；数值仅代表已采集的有效 FRT 样本，不能据此确认全窗耗时。"
	if result.Complete {
		result.Status = modelCoverageComplete
		result.Note = "所选窗口的本地请求与 FRT 覆盖已核验；耗时仅统计有效 FRT 样本，不表示每个请求都有有效流式 FRT。"
	}
	return result, nil
}
