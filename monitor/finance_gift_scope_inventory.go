//go:build unix

package monitor

import (
	"context"

	"gorm.io/gorm"
)

// FinanceGiftScopeMonthGap is a raw, offline inventory, not evidence that an
// original source is available or that these hours are safe to repair. Monetary
// rows count nonzero quota records, NOT gift amounts or business-filtered rows.
type FinanceGiftScopeMonthGap struct {
	Month                 string `json:"month"`
	UnknownRows           int64  `json:"unknown_rows"`
	MonetaryRows          int64  `json:"unknown_nonzero_quota_rows"`
	UserHours             int64  `json:"unknown_user_hours"`
	FirstHourTs           int64  `json:"first_hour_ts"`
	LastHourTs            int64  `json:"last_hour_ts"`
	DeclaredWholeHourRows int64  `json:"declared_whole_hour_rows"`
	MissingStates         int64  `json:"user_hours_missing_state"`
	InvalidStates         int64  `json:"user_hours_invalid_state_metadata"`
	OversizeHours         int64  `json:"user_hours_over_row_limit"`
}

// Call only from the existing closed-backup inspector with its deadline and
// file guards. This adds no live report query, worker, or source connection.
func inspectFinanceGiftScopeMonths(ctx context.Context, db *gorm.DB, epoch string) ([]FinanceGiftScopeMonthGap, error) {
	months := make([]FinanceGiftScopeMonthGap, 0)
	// Report dates use Beijing time. Whole-hour sizes come from publication
	// metadata and include already-known/zero-quota rows; raw hashes and monetary
	// aggregates are still verified by the existing bounded candidate planner.
	err := db.WithContext(ctx).Raw(`SELECT
strftime('%Y-%m',e.hour_ts,'unixepoch','+8 hours') AS month,
SUM(e.n) AS unknown_rows, SUM(e.monetary) AS monetary_rows,
COUNT(*) AS user_hours, MIN(e.hour_ts) AS first_hour_ts, MAX(e.hour_ts) AS last_hour_ts,
SUM(CASE WHEN s.status='complete' AND s.rows>=e.n AND s.rows<=? THEN s.rows ELSE 0 END) AS declared_whole_hour_rows,
SUM(CASE WHEN s.source_epoch IS NULL THEN 1 ELSE 0 END) AS missing_states,
SUM(CASE WHEN s.source_epoch IS NOT NULL AND
  (COALESCE(s.status,'')<>'complete' OR COALESCE(s.rows,-1)<e.n OR s.rows>?) THEN 1 ELSE 0 END) AS invalid_states,
SUM(CASE WHEN s.rows>? THEN 1 ELSE 0 END) AS oversize_hours
FROM (SELECT source_epoch,hour_ts,user_id,COUNT(*) AS n,
SUM(CASE WHEN quota<>0 THEN 1 ELSE 0 END) AS monetary
FROM finance_gift_boundary_events WHERE source_epoch=? AND COALESCE(group_known,0)=0
GROUP BY source_epoch,hour_ts,user_id) e
LEFT JOIN finance_gift_boundary_states s
ON s.source_epoch=e.source_epoch AND s.hour_ts=e.hour_ts AND s.user_id=e.user_id
GROUP BY month ORDER BY month`, financeGiftBoundaryMaxRows, financeGiftBoundaryMaxRows, financeGiftLocalMaxRows, epoch).Scan(&months).Error
	if err != nil {
		return nil, err // Do not return partial inventory after cancellation/failure.
	}
	return months, nil
}
