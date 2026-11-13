package monitor

import "gorm.io/gorm"

func tokenDetectionRecentCorrections(tx *gorm.DB, epoch, hash string, now int64, limit, cadence int, oldest []TokenMeteringFact, retained ...int64) ([]TokenMeteringFact, error) {
	if len(oldest) == 0 {
		return oldest, nil
	}
	var firstRecent struct{ StartAt int64 }
	// Find the first affected recent window through compact groups. Continue
	// forward through the bounded three-hour tail, rather than jumping to the newest
	// affected window and abandoning the previous window's unfinished suffix.
	recentFrom := (now - 3*3600) / 300 * 300
	if len(retained) > 0 && retained[0] > 0 {
		recentFrom = min(recentFrom, retained[0])
	}
	err := tx.Table(tokenReportPendingGroups+" AS g INDEXED BY idx_token_detection_changed_group_v1").
		Select("g.start_at").
		Where("g.source_epoch=? AND g.base_pending=1 AND g.retry_present=0 AND g.head_present=1 AND g.start_at>=? AND g.start_at<=?", epoch, recentFrom, now).
		Order("g.start_at").Limit(1).Scan(&firstRecent).Error
	if err != nil || firstRecent.StartAt == 0 {
		return oldest, err
	}
	// A single ascending prefix can still hide the newest sealed window when
	// several older windows became stale together (for example after a delayed
	// association or a coverage proof). Keep both ends of the bounded recent
	// range in one indexed statement, just as first-evaluation work does. A
	// bounded reservation for the preceding sealed window prevents its remaining
	// corrections from disappearing between those ends when a new window arrives.
	// All three seeks share the same limit and overlapping identities are removed.
	recentHead := max(1, limit/4)
	recentTail := max(0, limit-recentHead)
	if limit < 32 {
		recentHead, recentTail = limit, 0
	}
	carry := min(tokenDetectionMaxCommitRows, recentTail)
	if len(retained) > 0 && retained[0] > 0 {
		carry = max(carry, recentTail*2/3)
	}
	recentTail -= carry
	carryFrom, carryTo := tokenDetectionCarryWindow(now)
	if len(retained) > 0 && retained[0] > 0 {
		carryFrom, carryTo = retained[0], retained[0]+tokenMeteringWindowSeconds
	}
	from := max(firstRecent.StartAt, now-3*3600)
	stale := `f.base_pending=1`
	memberTable := tokenReportPendingMembers + " AS f INDEXED BY idx_token_detection_changed_identity_v1"
	sql := `WITH recent_head AS (
		SELECT f.source_epoch,f.log_id,f.created_at FROM ` + memberTable + `
		WHERE f.source_epoch=? AND f.retry_present=0 AND f.head_present=1 AND f.created_at>=? AND f.created_at<=? AND ` + stale + `
		ORDER BY f.created_at,f.log_id LIMIT ?
	), recent_tail AS (
		SELECT f.source_epoch,f.log_id,f.created_at FROM ` + memberTable + `
		WHERE f.source_epoch=? AND f.retry_present=0 AND f.head_present=1 AND f.created_at>=? AND f.created_at<=? AND ` + stale + `
		ORDER BY f.created_at DESC,f.log_id DESC LIMIT ?
	), recent_carry AS (
		SELECT f.source_epoch,f.log_id,f.created_at FROM ` + memberTable + `
		WHERE f.source_epoch=? AND f.retry_present=0 AND f.head_present=1 AND f.created_at>=? AND f.created_at<? AND ` + stale + `
		ORDER BY f.created_at,f.log_id LIMIT ?
	)
	SELECT source_epoch,log_id,created_at,0 AS queue_lane FROM recent_head
	UNION ALL
	SELECT source_epoch,log_id,created_at,1 AS queue_lane FROM recent_tail
	UNION ALL
	SELECT source_epoch,log_id,created_at,2 AS queue_lane FROM recent_carry
	ORDER BY queue_lane,created_at,log_id`
	args := []any{epoch, from, now, recentHead, epoch, from, now, recentTail, epoch, carryFrom, carryTo, carry}
	var rows []tokenDetectionRecentEndJob
	if err = tx.Raw(sql, args...).Scan(&rows).Error; err != nil {
		return nil, err
	}
	recent := tokenDetectionMergeRecentEnds(rows)
	return tokenDetectionPreferRecent(oldest, recent, now, limit, cadence), nil
}
