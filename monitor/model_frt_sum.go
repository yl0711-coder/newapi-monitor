package monitor

import "math"

// This version is independent of histogram coverage; introducing a mean must
// not trigger an automatic historical source replay.
const frtSumSemanticsVersion = 1

// frtSourceExpr 是 FRT（首个 data 事件延迟，毫秒）在生产日志里的唯一取值口径。
// 采集（sampleWindowSQLWithUser）与总和回算（frtRecomputeWindowSQL）必须共用它：
// 两处各写一份迟早漂移，而漂移后回算出的均值无法与采集结果对账。
// 它不是严格的首个有效模型 token；只有流式请求才有可比较的 FRT。
const frtSourceExpr = "(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)"

func mergeMetricFRTSum(dst *MetricSample, src MetricSample) {
	if dst.TtftObserved == 0 && dst.FRTSumVersion == 0 {
		zero := int64(0)
		dst.FRTSumMs, dst.FRTSumVersion = &zero, frtSumSemanticsVersion
	}
	if src.TtftObserved == 0 {
		return
	}
	if dst.FRTSumMs == nil || src.FRTSumMs == nil || src.FRTSumVersion != frtSumSemanticsVersion || src.FRTSumObserved != src.TtftObserved || src.FRTSumObserved < 0 || *src.FRTSumMs <= 0 || *dst.FRTSumMs > math.MaxInt64-*src.FRTSumMs || dst.FRTSumObserved > math.MaxInt64-src.FRTSumObserved {
		dst.FRTSumMs = nil
		return
	}
	sum := *dst.FRTSumMs + *src.FRTSumMs
	dst.FRTSumMs = &sum
	dst.FRTSumObserved += src.FRTSumObserved
}

// filterModelFRTChannelsByMean 按渠道真实平均值严格大于阈值过滤。
//
// 三条口径，不得放宽：
//  1. 严格大于：等于阈值不算命中。
//  2. 命中渠道整棵子树原样保留，分组和模型不按各自均值再筛一遍——
//     用户问的是"哪些渠道慢",慢渠道下面的快模型仍是排查所需的上下文。
//  3. 平均值不可计算的渠道不参与筛选：既不保留也不算未命中,单独计数返回,
//     由页面明确提示。把它们留下会伪装成命中,静默丢掉会被读成"不存在"。
func filterModelFRTChannelsByMean(channels []ModelFRTChannel, threshold int64) ([]ModelFRTChannel, int64) {
	kept, excluded := make([]ModelFRTChannel, 0, len(channels)), int64(0)
	for _, channel := range channels {
		mean := channel.FRT.MeanMs
		if mean == nil || math.IsNaN(*mean) {
			excluded++
			continue
		}
		if *mean > float64(threshold) {
			kept = append(kept, channel)
		}
	}
	return kept, excluded
}

// Deterministic ordering at every level; unavailable means always sort last.
func modelFRTMeanLess(a, b ModelFRTMetrics, tie bool) bool {
	if a.MeanMs == nil || b.MeanMs == nil {
		if a.MeanMs != nil {
			return true
		}
		if b.MeanMs != nil {
			return false
		}
		return tie
	}
	if *a.MeanMs != *b.MeanMs {
		return *a.MeanMs > *b.MeanMs
	}
	return tie
}
