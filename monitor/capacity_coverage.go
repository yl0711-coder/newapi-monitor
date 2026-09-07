package monitor

import "context"

// Coverage is proven by the completed scan interval, never by a MAX(timestamp).
// The user projection was introduced later: reconcile it against minute facts
// before reusing their coverage. All reads stay within Monitor's local store.
func (m *Monitor) capacityWindowComplete(ctx context.Context, from, to int64, users bool) bool {
	if from >= to {
		return false
	}
	var state MetricFinalizeState
	if err := m.storeDB.WithContext(ctx).First(&state, 1).Error; err != nil ||
		state.SemanticsVersion != stabilityTrafficClassificationVersion ||
		state.CoverageFromTs <= 0 || state.CoverageFromTs > from || state.NextTs < to {
		return false
	}
	if !users {
		return true
	}
	var mismatches int64
	err := m.storeDB.WithContext(ctx).Raw(`SELECT COUNT(*) FROM (
		SELECT bucket_ts FROM (
			SELECT bucket_ts, channel_id, model_name, grp, success, anomaly, failed, tokens FROM metric_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
			UNION ALL
			SELECT bucket_ts, channel_id, model_name, grp, -success, -anomaly, -failed, -tokens FROM capacity_user_minute_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
		) GROUP BY bucket_ts, channel_id, model_name, grp
		HAVING SUM(success) != 0 OR SUM(anomaly) != 0 OR SUM(failed) != 0 OR SUM(tokens) != 0
	)`, from, to, stabilityTrafficClassificationVersion,
		from, to, stabilityTrafficClassificationVersion).Scan(&mismatches).Error
	return err == nil && mismatches == 0
}

// Explicit null buckets prevent the chart connecting two isolated samples as
// though the intervening time were continuously observed. Never extrapolate.
func capacitySeriesWithGaps(points []capacityPoint, from, to, bucket int64) []capacityPoint {
	if bucket <= 0 || from >= to {
		return points
	}
	byTime := make(map[int64]capacityPoint, len(points))
	for _, point := range points {
		byTime[point.Ts] = point
	}
	result := make([]capacityPoint, 0, (to-from)/bucket+1)
	for ts := from / bucket * bucket; ts < to; ts += bucket {
		point, ok := byTime[ts]
		if !ok {
			point = capacityPoint{Ts: ts, DurationMinutes: (min(ts+bucket, to) - max(ts, from)) / 60}
		}
		result = append(result, point)
	}
	return result
}
