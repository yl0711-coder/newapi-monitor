package monitor

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// Completeness requires both proven coverage and a usable, single-version sample.
// Process completion and the original engine decision are independent of it.
func incidentDecisionHasCompleteData(d incidentWindowDecision) bool {
	usable := d.WindowState == incidentWindowStateNormal ||
		d.WindowState == incidentWindowStateAnomaly || d.WindowState == incidentWindowStateNoSample
	return d.DataComplete && d.MixedRows == 0 && usable
}

// A revision is one local transaction: audit, contribution, latest measured facts,
// source retry state, and any required watermark rollback succeed or fail together.
// Original snapshots and B2 observations never enter this write path.
func (m *Monitor) applyIncidentFinalizedRevision(ctx context.Context, ingest IncidentIngestConfig,
	original IncidentIngestWindowState, d incidentWindowDecision, kind string, found bool, now int64) error {

	changed := incidentWindowInputsChanged(original, d, found)
	row := original
	incidentApplyMeasured(&row, d, now)
	row.DataComplete = incidentDecisionHasCompleteData(d)
	if row.DataComplete {
		if !original.DataComplete {
			row.GapRecheckClosed = true
			row.NextSourceCheckAt, row.NextRetryAt = 0, 0
		}
	} else {
		// A new gap reopens source review even if an earlier gap was backfilled.
		// Its seven-day budget is independent of late-review attempts/horizon.
		row.GapRecheckClosed, row.MixedVersionClosed = false, false
		row.SourceCheckAttempts++
		row.NextSourceCheckAt = incidentSourceCheckBackoff(row.SourceCheckAttempts, ingest.PollSeconds, now)
		row.NextRetryAt = 0
	}
	if kind == "gap_backfilled_anomalous" {
		row.ScenarioPending = true
		row.PendingReason = incidentPendingReasonGapBackfilledAnomalous
	}
	row.UpdatedAt = now
	return m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		if changed {
			if err := m.recordIncidentRevisionTx(tx, original, d, kind, found, now); err != nil {
				return err
			}
			if err := m.writeIncidentImpactWindows(tx, row.IncidentID, row.SegmentID, d, now); err != nil {
				return err
			}
		}
		if err := m.saveIncidentWindowStateTx(tx, row); err != nil {
			return err
		}
		if !row.DataComplete {
			// A crash before the end-of-turn recomputation must not leave a saved
			// complete watermark beyond a newly discovered incomplete window.
			if err := tx.Model(&IncidentIngestCursor{}).
				Where("id = ? AND complete_through_ts > ?", incidentIngestCursorID, row.WindowFromTs).
				Update("complete_through_ts", row.WindowFromTs).Error; err != nil {
				return fmt.Errorf("rollback incident completeness watermark: %w", err)
			}
		}
		return nil
	})
}
