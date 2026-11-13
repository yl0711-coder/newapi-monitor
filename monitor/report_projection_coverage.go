package monitor

import (
	"context"
	"sync"

	"gorm.io/gorm"
)

type reportProjectionKey struct{ from, to int64 }
type reportProjectionResult struct{ RequestErrors, FRTErrors int64 }
type reportProjectionCache struct {
	mu   sync.Mutex
	rows map[reportProjectionKey]reportProjectionResult
}
type reportProjectionCacheKey struct{}

// Installed only within a single report read transaction. No cross-refresh
// cache: a late source correction is visible on the next request.
func withReportProjectionCache(ctx context.Context) context.Context {
	return context.WithValue(ctx, reportProjectionCacheKey{}, &reportProjectionCache{rows: map[reportProjectionKey]reportProjectionResult{}})
}

func capacityProjectionCoverage(ctx context.Context, db *gorm.DB, from, to int64) (reportProjectionResult, error) {
	cache, _ := ctx.Value(reportProjectionCacheKey{}).(*reportProjectionCache)
	key := reportProjectionKey{from, to}
	if cache != nil {
		cache.mu.Lock()
		defer cache.mu.Unlock()
		if r, ok := cache.rows[key]; ok {
			return r, nil
		}
	}
	// Requests and FRT reconcile the same keys. Group once, retaining separate
	// verdicts and validating every original row before sums can cancel errors.
	var result reportProjectionResult
	err := db.WithContext(ctx).Raw(`SELECT COALESCE(SUM(request_error),0) AS request_errors, COALESCE(SUM(frt_error),0) AS frt_errors FROM (
	 SELECT CASE WHEN SUM(success)<>0 OR SUM(anomaly)<>0 OR SUM(failed)<>0 OR SUM(tokens)<>0 THEN 1 ELSE 0 END AS request_error,
	 CASE WHEN SUM(metric_row)=0 OR SUM(user_row)=0 OR MAX(invalid_row)<>0 OR
	 SUM(ttft_500)<>0 OR SUM(ttft_1k)<>0 OR SUM(ttft_2k)<>0 OR SUM(ttft_5k)<>0 OR SUM(ttft_10k)<>0 OR SUM(ttft_inf)<>0 OR
	 SUM(ttft_observed)<>0 OR SUM(ttft_over_3s)<>0 OR MAX(metric_max)<>MAX(user_max) THEN 1 ELSE 0 END AS frt_error
	 FROM (
	 SELECT bucket_ts,channel_id,model_name,grp,success,anomaly,failed,tokens,
	 ttft_500,ttft_1k,ttft_2k,ttft_5k,ttft_10k,ttft_inf,ttft_observed,ttft_over_3s,
	 ttft_max_ms AS metric_max,0 AS user_max,1 AS metric_row,0 AS user_row,
	 CASE WHEN (`+ttftInvalidRowPredicate("metric_samples")+`) THEN 1 ELSE 0 END AS invalid_row
	 FROM metric_samples WHERE bucket_ts>=? AND bucket_ts<? AND traffic_class_version=?
	 UNION ALL
	 SELECT bucket_ts,channel_id,model_name,grp,-success,-anomaly,-failed,-tokens,
	 -ttft_500,-ttft_1k,-ttft_2k,-ttft_5k,-ttft_10k,-ttft_inf,-ttft_observed,-ttft_over_3s,
	 0 AS metric_max,ttft_max_ms AS user_max,0 AS metric_row,1 AS user_row,
	 CASE WHEN (`+ttftInvalidRowPredicate("capacity_user_minute_samples")+`) THEN 1 ELSE 0 END AS invalid_row
	 FROM capacity_user_minute_samples WHERE bucket_ts>=? AND bucket_ts<? AND traffic_class_version=?
	 ) GROUP BY bucket_ts,channel_id,model_name,grp
	)`, from, to, stabilityTrafficClassificationVersion, from, to, stabilityTrafficClassificationVersion).Scan(&result).Error
	if err == nil && cache != nil {
		cache.rows[key] = result
	}
	return result, err
}
