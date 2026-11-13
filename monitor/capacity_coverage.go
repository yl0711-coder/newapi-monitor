package monitor

import (
	"context"
	"log/slog"

	"gorm.io/gorm"
)

// Coverage is proven by the completed scan interval, never by a MAX(timestamp).
// The user projection was introduced later: reconcile it against minute facts
// before reusing their coverage. All reads stay within Monitor's local store.
func (m *Monitor) capacityWindowComplete(ctx context.Context, from, to int64, users bool) bool {
	complete, _ := m.capacityWindowCoverage(ctx, from, to, users)
	return complete
}

func (m *Monitor) capacityWindowCoverage(ctx context.Context, from, to int64, users bool) (bool, error) {
	if from >= to {
		return false, nil
	}
	var state MetricFinalizeState
	if err := m.storeDB.WithContext(ctx).Where("id = ?", 1).Limit(1).Find(&state).Error; err != nil {
		return false, err
	}
	if state.SemanticsVersion != stabilityTrafficClassificationVersion ||
		state.CoverageFromTs <= 0 || state.CoverageFromTs > from || state.NextTs < to {
		return false, nil
	}
	if !users {
		return true, nil
	}
	if ctx.Value(reportProjectionCacheKey{}) != nil {
		result, err := capacityProjectionCoverage(ctx, m.storeDB, from, to)
		return err == nil && result.RequestErrors == 0, err
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
	return err == nil && mismatches == 0, err
}

func capacityTTFTRowsComplete(ctx context.Context, db *gorm.DB, from, to int64) bool {
	if db == nil || from >= to {
		return false
	}
	var old int64
	err := db.WithContext(ctx).Raw(`SELECT COUNT(*) FROM (
		SELECT bucket_ts FROM metric_samples
		WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
		  AND (success + anomaly + failed > 0 OR tokens <> 0)
		  AND (COALESCE(ttft_semantics_version,0) <> ? OR `+ttftInvalidRowPredicate("metric_samples")+`)
		UNION ALL
		SELECT bucket_ts FROM capacity_user_minute_samples
		WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
		  AND (success + anomaly + failed > 0 OR tokens <> 0 OR quota <> 0 OR refund_quota <> 0)
		  AND (COALESCE(ttft_semantics_version,0) <> ? OR `+ttftInvalidRowPredicate("capacity_user_minute_samples")+`)
	) AS legacy_ttft`, from, to, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion,
		from, to, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion).Scan(&old).Error
	return err == nil && old == 0
}

// ttftInvalidRowPredicate is the SQL counterpart of computeTTFTMetricView.
// A current-semantics row must still carry a self-consistent projection: the
// histogram accounts for observed samples, over_3s is a subset of observed,
// max belongs to the highest non-zero bucket, and a zero-observation row has
// no orphaned max/over-3s values.  Returning false for an invalid row would
// let a damaged projection masquerade as complete coverage.
func ttftInvalidRowPredicate(table string) string {
	return `
		` + table + `.ttft_500 < 0 OR ` + table + `.ttft_1k < 0 OR ` + table + `.ttft_2k < 0 OR
		` + table + `.ttft_5k < 0 OR ` + table + `.ttft_10k < 0 OR ` + table + `.ttft_inf < 0 OR
		` + table + `.ttft_observed < 0 OR ` + table + `.ttft_over_3s < 0 OR ` + table + `.ttft_max_ms < 0 OR
		` + table + `.ttft_over_3s > ` + table + `.ttft_observed OR
		` + table + `.ttft_over_3s < (` + table + `.ttft_10k + ` + table + `.ttft_inf) OR
		` + table + `.ttft_over_3s > (` + table + `.ttft_5k + ` + table + `.ttft_10k + ` + table + `.ttft_inf) OR
		(` + table + `.ttft_over_3s > 0 AND ` + table + `.ttft_max_ms <= 3000) OR
		(` + table + `.ttft_over_3s = 0 AND ` + table + `.ttft_max_ms > 3000) OR
		(` + table + `.ttft_500 + ` + table + `.ttft_1k + ` + table + `.ttft_2k + ` + table + `.ttft_5k + ` + table + `.ttft_10k + ` + table + `.ttft_inf) <> ` + table + `.ttft_observed OR
		(` + table + `.ttft_observed = 0 AND (` + table + `.ttft_max_ms <> 0 OR ` + table + `.ttft_over_3s <> 0)) OR
		(` + table + `.ttft_observed > 0 AND ` + table + `.ttft_max_ms <= 0) OR
		(` + table + `.ttft_inf > 0 AND ` + table + `.ttft_max_ms <= 10000) OR
		(` + table + `.ttft_inf = 0 AND ` + table + `.ttft_10k > 0 AND (` + table + `.ttft_max_ms <= 5000 OR ` + table + `.ttft_max_ms > 10000)) OR
		(` + table + `.ttft_inf = 0 AND ` + table + `.ttft_10k = 0 AND ` + table + `.ttft_5k > 0 AND (` + table + `.ttft_max_ms <= 2000 OR ` + table + `.ttft_max_ms > 5000)) OR
		(` + table + `.ttft_inf = 0 AND ` + table + `.ttft_10k = 0 AND ` + table + `.ttft_5k = 0 AND ` + table + `.ttft_2k > 0 AND (` + table + `.ttft_max_ms <= 1000 OR ` + table + `.ttft_max_ms > 2000)) OR
		(` + table + `.ttft_inf = 0 AND ` + table + `.ttft_10k = 0 AND ` + table + `.ttft_5k = 0 AND ` + table + `.ttft_2k = 0 AND ` + table + `.ttft_1k > 0 AND (` + table + `.ttft_max_ms <= 500 OR ` + table + `.ttft_max_ms > 1000)) OR
		(` + table + `.ttft_inf = 0 AND ` + table + `.ttft_10k = 0 AND ` + table + `.ttft_5k = 0 AND ` + table + `.ttft_2k = 0 AND ` + table + `.ttft_1k = 0 AND ` + table + `.ttft_500 > 0 AND (` + table + `.ttft_max_ms <= 0 OR ` + table + `.ttft_max_ms > 500)) OR
		(` + table + `.ttft_inf = 0 AND ` + table + `.ttft_10k = 0 AND ` + table + `.ttft_5k = 0 AND ` + table + `.ttft_2k = 0 AND ` + table + `.ttft_1k = 0 AND ` + table + `.ttft_500 = 0 AND ` + table + `.ttft_max_ms <> 0)`
}

// capacityTTFTProjectionComplete verifies that the user-minute projection and
// the aggregate minute facts agree on every exact TTFT counter. Keeping this
// separate from capacityWindowComplete lets callers report request coverage
// and TTFT coverage independently.
func capacityTTFTProjectionComplete(ctx context.Context, db *gorm.DB, from, to int64) bool {
	complete, err := capacityTTFTProjectionCoverage(ctx, db, from, to)
	if err != nil {
		slog.Warn("FRT projection coverage query failed", "err", err)
	}
	return complete
}

func capacityTTFTProjectionCoverage(ctx context.Context, db *gorm.DB, from, to int64) (bool, error) {
	if db == nil || from >= to {
		return false, nil
	}
	if ctx.Value(reportProjectionCacheKey{}) != nil {
		result, err := capacityProjectionCoverage(ctx, db, from, to)
		return err == nil && result.FRTErrors == 0, err
	}
	// Keep the reconciliation inside SQLite: transferring every minute/key to
	// Go grows with the full historical window, and string-joined keys are not
	// collision-safe. Check each source row before aggregation, so contradictory
	// users cannot cancel one another into a seemingly valid aggregate.
	var mismatches int64
	err := db.WithContext(ctx).Raw(`SELECT COUNT(*) FROM (
		SELECT bucket_ts, channel_id, model_name, grp
		FROM (
			SELECT bucket_ts, channel_id, model_name, grp,
				ttft_500, ttft_1k, ttft_2k, ttft_5k, ttft_10k, ttft_inf,
				ttft_observed, ttft_over_3s,
				ttft_max_ms AS metric_max, 0 AS user_max, 1 AS metric_row, 0 AS user_row,
				CASE WHEN `+ttftInvalidRowPredicate("metric_samples")+` THEN 1 ELSE 0 END AS invalid_row
			FROM metric_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
			UNION ALL
			SELECT bucket_ts, channel_id, model_name, grp,
				-ttft_500, -ttft_1k, -ttft_2k, -ttft_5k, -ttft_10k, -ttft_inf,
				-ttft_observed, -ttft_over_3s,
				0 AS metric_max, ttft_max_ms AS user_max, 0 AS metric_row, 1 AS user_row,
				CASE WHEN `+ttftInvalidRowPredicate("capacity_user_minute_samples")+` THEN 1 ELSE 0 END AS invalid_row
			FROM capacity_user_minute_samples
			WHERE bucket_ts >= ? AND bucket_ts < ? AND traffic_class_version = ?
		) AS projections
		GROUP BY bucket_ts, channel_id, model_name, grp
		HAVING SUM(metric_row) = 0 OR SUM(user_row) = 0 OR MAX(invalid_row) <> 0 OR
			SUM(ttft_500) <> 0 OR SUM(ttft_1k) <> 0 OR SUM(ttft_2k) <> 0 OR
			SUM(ttft_5k) <> 0 OR SUM(ttft_10k) <> 0 OR SUM(ttft_inf) <> 0 OR
			SUM(ttft_observed) <> 0 OR SUM(ttft_over_3s) <> 0 OR MAX(metric_max) <> MAX(user_max)
	) AS mismatches`, from, to, stabilityTrafficClassificationVersion,
		from, to, stabilityTrafficClassificationVersion).Scan(&mismatches).Error
	return err == nil && mismatches == 0, err
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
