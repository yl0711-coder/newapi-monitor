package monitor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const stabilityFRTRepairTimeout = 2 * time.Second

var errStabilityFRTMismatch = errors.New("稳定性 FRT 分钟证据与已封口小时不一致")

// Reuse the independent replay's certified prefix. Absence of minute rows is
// not proof of zero traffic: both that prefix and the sealed hour's controls
// must agree. Only FRT columns change; accounting facts and timestamps stay put.
// The cursor is advisory, so restart, rollback and retry need no schema change.
func (m *Monitor) repairOneStabilityFRTFromMinutes(ctx context.Context, now int64) error {
	ctx, cancel := context.WithTimeout(ctx, stabilityFRTRepairTimeout)
	defer cancel()
	var visited int64
	err := retryStabilityLocalWrite(ctx, func(attemptCtx context.Context) error {
		return m.storeDB.WithContext(attemptCtx).Transaction(func(tx *gorm.DB) error {
			var proof MetricFinalizeState
			if err := tx.First(&proof, "id = ?", 1).Error; err != nil {
				if errors.Is(err, gorm.ErrRecordNotFound) {
					return nil
				}
				return err
			}
			if proof.SemanticsVersion != stabilityTrafficClassificationVersion || proof.TTFTSemanticsVersion != ttftCoverageSemanticsVersion || proof.TTFTCoverageFromTs <= 0 {
				return nil
			}
			from := (proof.TTFTCoverageFromTs + 3599) / 3600 * 3600
			to := min(proof.TTFTCoverageThroughTs, proof.NextTs, proof.TargetThroughTs, finalizedStabilityHourTo(now)) / 3600 * 3600
			if from >= to {
				return nil
			}
			find := func(after int64) *gorm.DB {
				return tx.Where("hour_ts >= ? AND hour_ts < ? AND hour_ts > ? AND status = ? AND traffic_class_version = ? AND COALESCE(ttft_semantics_version,0) <> ?", from, to, after, "complete", stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion).Order("hour_ts")
			}
			var receipt StabilityHourIngestState
			err := find(m.stabilityFRTRepairAfter.Load()).First(&receipt).Error
			if errors.Is(err, gorm.ErrRecordNotFound) {
				err = find(0).First(&receipt).Error
			}
			if errors.Is(err, gorm.ErrRecordNotFound) {
				return nil
			}
			if err != nil {
				return err
			}
			visited = receipt.HourTs
			return repairStabilityFRTHour(tx, receipt)
		})
	})
	// Rotate even on a control mismatch so one unrepairable hour cannot starve
	// others. The next sweep retries it, without querying NewAPI or certifying it.
	if visited > 0 && (err == nil || errors.Is(err, errStabilityFRTMismatch)) {
		m.stabilityFRTRepairAfter.Store(visited)
	}
	return err
}

func repairStabilityFRTHour(tx *gorm.DB, receipt StabilityHourIngestState) error {
	var old []StabilityHourSample
	if err := tx.Where("hour_ts = ?", receipt.HourTs).Order("channel_id,model_name,grp").Limit(maxStabilityRowsPerHour + 1).Find(&old).Error; err != nil {
		return err
	}
	if len(old) > maxStabilityRowsPerHour || int64(len(old)) != receipt.Rows {
		return fmt.Errorf("%w: hour=%d row controls", errStabilityFRTMismatch, receipt.HourTs)
	}
	var invalid int64
	// Check individual rows before aggregation: two corrupt minute counters
	// must not cancel each other into a plausible hourly histogram.
	if err := tx.Model(&MetricSample{}).Where("bucket_ts >= ? AND bucket_ts < ? AND (COALESCE(traffic_class_version,0) <> ? OR COALESCE(ttft_semantics_version,0) <> ? OR ttft_observed > success+anomaly OR ("+ttftInvalidRowPredicate("metric_samples")+"))", receipt.HourTs, receipt.HourTs+3600, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion).Count(&invalid).Error; err != nil {
		return err
	}
	if invalid != 0 {
		return fmt.Errorf("%w: hour=%d invalid or legacy minutes", errStabilityFRTMismatch, receipt.HourTs)
	}
	var projected []StabilityHourSample
	query := `SELECT ? AS hour_ts, sh.channel_id, sh.model_name, sh.grp, ? AS traffic_class_version, ? AS ttft_semantics_version,
		COALESCE(SUM(sh.refund_records),0) AS refund_records, COALESCE(SUM(sh.refund_quota),0) AS refund_quota,` + stabilityAggColumns + `
		FROM metric_samples sh WHERE sh.bucket_ts >= ? AND sh.bucket_ts < ?
		GROUP BY sh.channel_id,sh.model_name,sh.grp ORDER BY sh.channel_id,sh.model_name,sh.grp LIMIT ?`
	if err := tx.Raw(query, receipt.HourTs, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion, receipt.HourTs, receipt.HourTs+3600, maxStabilityRowsPerHour+1).Scan(&projected).Error; err != nil {
		return err
	}
	if len(old) != len(projected) {
		return fmt.Errorf("%w: hour=%d dimensions", errStabilityFRTMismatch, receipt.HourTs)
	}
	requests, tokens, quota := stabilityHourTotals(old)
	if requests != receipt.Requests || tokens != receipt.Tokens || quota != receipt.Quota {
		return fmt.Errorf("%w: hour=%d totals", errStabilityFRTMismatch, receipt.HourTs)
	}
	for i, row := range projected {
		if stabilityWithoutFRT(old[i]) != stabilityWithoutFRT(row) || !validStabilityFRTProjection(row) {
			return fmt.Errorf("%w: hour=%d channel=%d", errStabilityFRTMismatch, receipt.HourTs, row.ChannelID)
		}
	}
	if len(projected) > 0 {
		if err := tx.Clauses(clause.OnConflict{
			Columns:   []clause.Column{{Name: "hour_ts"}, {Name: "channel_id"}, {Name: "model_name"}, {Name: "grp"}},
			DoUpdates: clause.AssignmentColumns([]string{"ttft_500", "ttft_1k", "ttft_2k", "ttft_5k", "ttft_10k", "ttft_inf", "ttft_max_ms", "ttft_observed", "ttft_over_3s", "ttft_semantics_version"}),
		}).CreateInBatches(projected, 100).Error; err != nil {
			return err
		}
	}
	return tx.Model(&StabilityHourIngestState{}).Where("hour_ts = ?", receipt.HourTs).UpdateColumn("ttft_semantics_version", ttftCoverageSemanticsVersion).Error
}

func stabilityWithoutFRT(row StabilityHourSample) StabilityHourSample {
	row.Ttft500, row.Ttft1k, row.Ttft2k, row.Ttft5k, row.Ttft10k, row.TtftInf = 0, 0, 0, 0, 0, 0
	row.TtftObserved, row.TtftOver3s, row.TtftMaxMs, row.TTFTSemanticsVersion = 0, 0, 0, 0
	return row
}

func validStabilityFRTProjection(row StabilityHourSample) bool {
	if row.TtftObserved < 0 || row.TtftObserved > row.Success+row.Anomaly {
		return false
	}
	hist := [6]int64{row.Ttft500, row.Ttft1k, row.Ttft2k, row.Ttft5k, row.Ttft10k, row.TtftInf}
	if row.TtftObserved == 0 {
		return hist == [6]int64{} && row.TtftOver3s == 0 && row.TtftMaxMs == 0
	}
	return computeTTFTMetricView(hist, row.TtftObserved, row.TtftOver3s, int64(row.TtftMaxMs)).HistValid
}
