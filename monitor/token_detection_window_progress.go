package monitor

import "gorm.io/gorm"

// These are scheduling reservations, never collection/detection completeness
// watermarks. All older identities remain eligible in the historical lane.
// Persist the reservation with a successful decision transaction so a moving
// source seal or worker restart cannot abandon a partly processed recent window.
type tokenDetectionWindowProgress struct {
	Fresh, Correction                                        int64
	HistoryScanAt, HistoryScanID, RecentScanAt, RecentScanID int64
	ScanPolicyHash                                           string
	AdvanceScans, ScanMore                                   bool
	AdvanceRecentScan                                        bool
	RecentDiscovery                                          []tokenDetectionScanRow
}

func tokenDetectionRetainedWindows(db *gorm.DB, epoch, hash string, now int64) (tokenDetectionWindowProgress, error) {
	var state TokenDetectionState
	if err := db.Where("source_epoch=?", epoch).Limit(1).Find(&state).Error; err != nil {
		return tokenDetectionWindowProgress{}, err
	}
	initial, _ := tokenDetectionCarryWindow(now)
	from := func(saved int64) int64 {
		if saved > 0 && saved <= now {
			return saved
		}
		return initial
	}
	var fresh struct{ CreatedAt int64 }
	if err := db.Table(tokenReportPendingMembers+" AS f INDEXED BY idx_token_detection_pending_fresh_v1").
		Select("f.created_at").Where("f.source_epoch=? AND f.retry_present=0 AND f.head_present=0 AND f.created_at>=? AND f.created_at<=?", epoch, from(state.FreshWindowAt), now).
		Order("f.created_at,f.log_id").Limit(1).Scan(&fresh).Error; err != nil {
		return tokenDetectionWindowProgress{}, err
	}
	var corrected struct{ StartAt int64 }
	if err := db.Table(tokenReportPendingGroups+" AS g INDEXED BY idx_token_detection_changed_group_v1").
		Select("g.start_at").Where("g.source_epoch=? AND g.base_pending=1 AND g.retry_present=0 AND g.head_present=1 AND g.start_at>=? AND g.start_at<=?", epoch, from(state.CorrectionWindowAt), now).
		Order("g.start_at").Limit(1).Scan(&corrected).Error; err != nil {
		return tokenDetectionWindowProgress{}, err
	}
	return tokenDetectionWindowProgress{Fresh: fresh.CreatedAt / 300 * 300, Correction: corrected.StartAt}, nil
}
