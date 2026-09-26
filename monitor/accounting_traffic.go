package monitor

import (
	"context"
	"errors"
	"log/slog"

	"gorm.io/gorm"
)

// Delivery v7 only reclassifies short client cancellations between success
// and anomaly. Compared with v6, source membership, total requests, tokens,
// consumption and refunds are unchanged. These two versions are compatible
// for accounting, but NOT for success/anomaly rates. Do not widen this list
// automatically when delivery classification changes again.
func accountingTrafficVersions() []int { return []int{6, 7} }

func accountingTrafficVersionSupported(version int) bool {
	return version == 6 || version == 7
}

// The writer atomically replaces an hour and its receipt. Version is not part
// of the hour fact primary key, so accepting v6 and v7 cannot double count a
// rewritten fact. Unknown versions and incomplete receipts remain excluded.
// A positive minute from EITHER compatible version contradicts a zero hour.
func accountingCompleteHourPredicateSQL(alias string) string {
	return alias + `.status = 'complete' AND ` + alias + `.traffic_class_version IN ? AND NOT (` +
		alias + `.requests = 0 AND EXISTS (` +
		`SELECT 1 FROM metric_samples ms INDEXED BY idx_metric_bucket WHERE ms.bucket_ts >= ` + alias + `.hour_ts ` +
		`AND ms.bucket_ts < ` + alias + `.hour_ts + 3600 AND ms.traffic_class_version IN ? ` +
		`AND (ms.success + ms.anomaly + ms.failed) > 0))`
}

// Accounting coverage is independent from delivery-policy migration. Keep the
// existing response shape and live-tail semantics, without the stability
// report's arbitrary legacy fallback or relabelling old facts as delivery v7.
func (m *Monitor) accountingDataCoverage(ctx context.Context, fromTs, toTs, now int64) StabilityDataCoverage {
	fromTs = fromTs / 3600 * 3600
	requestedTo, finalizedTo := toTs, finalizedStabilityHourTo(now)
	toTs = min(toTs, finalizedTo) / 3600 * 3600
	result := StabilityDataCoverage{FromTs: fromTs, ToTs: toTs}
	if requestedTo > toTs {
		result.RequestedToTs = requestedTo
		result.ProvisionalSeconds = requestedTo - max(fromTs, toTs)
	}
	if toTs <= fromTs {
		result.Complete, result.EffectiveComplete = true, true
		result.Percent, result.EffectivePercent = 100, 100
		result.LatestHourPending = result.ProvisionalSeconds > 0
		if result.LatestHourPending {
			result.PendingHourTs = max(fromTs, toTs)
		}
		return result
	}
	result.ExpectedHours = (toTs - fromTs) / 3600
	result.MissingHours, result.EffectiveMissingHours = result.ExpectedHours, result.ExpectedHours
	query := `SELECT COUNT(*) FROM stability_hour_ingest_states hs WHERE hs.hour_ts>=? AND hs.hour_ts<? AND ` +
		accountingCompleteHourPredicateSQL("hs")
	if err := m.storeDB.WithContext(ctx).Raw(query, fromTs, toTs, accountingTrafficVersions(), accountingTrafficVersions()).
		Scan(&result.CompletedHours).Error; err != nil {
		slog.Warn("读取核算用量小时覆盖台账失败", "err", err)
		return result
	}
	result.MissingHours = max(int64(0), result.ExpectedHours-result.CompletedHours)
	result.Percent = float64(result.CompletedHours) / float64(result.ExpectedHours) * 100
	result.Complete = result.MissingHours == 0
	result.EffectiveHours, result.EffectiveMissingHours = result.CompletedHours, result.MissingHours
	result.EffectivePercent, result.EffectiveComplete = result.Percent, result.Complete
	if result.Complete && result.ProvisionalSeconds > 0 {
		result.LatestHourPending, result.PendingHourTs = true, toTs
	}
	if result.MissingHours == 1 && toTs == finalizedTo {
		var latest StabilityHourIngestState
		err := m.storeDB.WithContext(ctx).First(&latest, "hour_ts=?", toTs-3600).Error
		if errors.Is(err, gorm.ErrRecordNotFound) || (err == nil && (latest.Status == "queued" || latest.Status == "running")) {
			result.LatestHourPending, result.PendingHourTs = true, toTs-3600
		} else if err != nil {
			slog.Warn("读取最新核算用量小时状态失败", "err", err)
		}
	}
	return result
}
