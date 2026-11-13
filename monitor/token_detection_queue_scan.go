package monitor

import (
	"sort"

	"gorm.io/gorm"
)

// Each read materializes at most 512 compact identities before consulting
// mutable head/window/reference state. In particular an empty repair result
// cannot turn into a scan of all retained facts or all retained groups.
const tokenDetectionScanRows = 512

type tokenDetectionScanRow struct {
	SourceEpoch                                           string
	LogID, CreatedAt, NextAt                              int64
	Changed, HeadPresent, RetryPresent, RetryChanged, Due bool
}

func tokenDetectionScanPrefix(db *gorm.DB, epoch, hash string, now, at, id int64, recent bool) ([]tokenDetectionScanRow, error) {
	from := int64(0)
	if recent {
		from = now - 900
	}
	query := `WITH prefix AS MATERIALIZED (
	 SELECT source_epoch,log_id,created_at,next_at,retry_present,head_present,base_pending,policy_hash,
	 reference_bucket,reference_generation,source_complete,start_at,fact_revision,link_revision
	 FROM token_report_pending_members_v1 INDEXED BY idx_token_detection_discovery_v1
	 WHERE source_epoch=? AND created_at>=? AND created_at<=? AND (created_at,log_id)>(?,?) ORDER BY created_at,log_id LIMIT 512)
	 SELECT f.source_epoch,f.log_id,f.created_at,COALESCE(f.next_at,0) AS next_at,f.retry_present,f.head_present,
	 (f.base_pending=1 OR COALESCE(c.detection_state,'')='' OR COALESCE(f.policy_hash,'')!=? OR COALESCE(f.reference_generation,-1)!=COALESCE(b.generation,0)
	 OR COALESCE(f.source_complete,-1)!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END) AS changed,
	 (r.log_id IS NOT NULL AND (r.fact_revision!=f.fact_revision OR r.link_revision!=f.link_revision OR COALESCE(r.policy_hash,'')!=?)) AS retry_changed,
	 (COALESCE(f.next_at,0)>0 AND f.next_at<=?) AS due
	 FROM prefix f LEFT JOIN ` + tokenDetectionHeadTable(db) + ` ON c.source_epoch=f.source_epoch AND c.log_id=f.log_id
	 LEFT JOIN token_detection_buckets b ON b.id=f.reference_bucket
	 LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=f.start_at
	 LEFT JOIN token_detection_retries r ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id
	 ORDER BY f.created_at,f.log_id`
	var rows []tokenDetectionScanRow
	err := db.Raw(query, epoch, from, now, at, id, hash, hash, now).Scan(&rows).Error
	return rows, err
}

func tokenDetectionBoundedQueueJobs(db *gorm.DB, epoch, hash string, now int64, limit, cadence int, progress *tokenDetectionWindowProgress) ([]TokenMeteringFact, error) {
	latestProgress := tokenDetectionWindowProgress{}
	latest, err := tokenDetectionLatestJobs(db, epoch, hash, now, limit, cadence, &latestProgress)
	if progress != nil {
		*progress = latestProgress
	}
	if err != nil {
		return latest, err
	}
	ready, err := tokenPendingProjectionReady(db)
	if err != nil || !ready {
		return latest, err
	}
	// Historical reservations remain active once their projection is ready.
	// Recent jobs lead each bounded prefix; spare capacity and one in sixteen
	// later positions still serve the original four lanes.
	historicalProgress := tokenDetectionWindowProgress{}
	history, err := tokenDetectionHistoryQueueJobs(db, epoch, hash, now, limit, cadence, &historicalProgress)
	if err != nil {
		return latest, err
	}
	if limit == 1 && len(history) > 0 && now/int64(max(cadence, 1))%4 == 2 {
		// Even a single committed identity per turn retains an occasional
		// historical opportunity. Keep discovery cursors until their full
		// selected prefix can be processed in a later turn.
		if progress != nil {
			progress.AdvanceScans, progress.AdvanceRecentScan = false, false
		}
		return history[:1], nil
	}
	result := tokenDetectionPriorityJobs(latest, history, limit)
	seen := map[int64]bool{}
	for _, job := range result {
		seen[job.LogID] = true
	}
	if progress != nil {
		*progress = historicalProgress
		progress.RecentDiscovery = latestProgress.RecentDiscovery
		progress.AdvanceRecentScan = latestProgress.AdvanceRecentScan
		progress.RecentScanAt, progress.RecentScanID = latestProgress.RecentScanAt, latestProgress.RecentScanID
		progress.ScanMore = progress.ScanMore || latestProgress.ScanMore
		for _, job := range latest {
			if !seen[job.LogID] {
				progress.AdvanceRecentScan = false
			}
		}
		for _, job := range history {
			if !seen[job.LogID] {
				progress.AdvanceScans = false
			}
		}
	}
	return result, nil
}

// Publish only the examined prefix whose changed identities have committed or
// received durable retries. A 512-row discovery page must not require all 512
// jobs to fit into a 250-row round before its cursor can move.
func tokenDetectionRecentCommittedPrefix(rows []tokenDetectionScanRow, visited []TokenMeteringFact) (int64, int64, bool) {
	type identity struct {
		epoch string
		id    int64
	}
	done := make(map[identity]bool, len(visited))
	for _, job := range visited {
		done[identity{job.SourceEpoch, job.LogID}] = true
	}
	var at, id int64
	advanced := false
	for _, row := range rows {
		needsWork := (!row.RetryPresent && row.HeadPresent && row.Changed) || row.RetryChanged
		if needsWork && !done[identity{row.SourceEpoch, row.LogID}] {
			return at, id, advanced
		}
		at, id, advanced = row.CreatedAt, row.LogID, true
	}
	if len(rows) < tokenDetectionScanRows {
		return 0, 0, true
	}
	return at, id, advanced
}

func tokenDetectionHistoryQueueJobs(db *gorm.DB, epoch, hash string, now int64, limit, cadence int, progress *tokenDetectionWindowProgress) ([]TokenMeteringFact, error) {
	lanes := make([][]TokenMeteringFact, 4)
	var state TokenDetectionState
	if err := db.Where("source_epoch=?", epoch).Limit(1).Find(&state).Error; err != nil {
		return nil, err
	}
	if state.ScanPolicyHash != hash {
		state.HistoryScanAt, state.HistoryScanID, state.RecentScanAt, state.RecentScanID = 0, 0, 0, 0
	}
	var err error
	lanes[0], err = tokenDetectionFreshJobs(db, epoch, now, limit, cadence, true, state.FreshWindowAt)
	if err != nil {
		return nil, err
	}
	windows, err := tokenDetectionRetainedWindows(db, epoch, hash, now)
	if err != nil {
		return nil, err
	}
	if progress != nil {
		*progress = windows
		progress.ScanPolicyHash = hash
	}
	var scanned []tokenDetectionScanRow
	for _, recent := range []bool{false, true} {
		at, id := state.HistoryScanAt, state.HistoryScanID
		if recent {
			at, id = state.RecentScanAt, state.RecentScanID
		}
		rows, e := tokenDetectionScanPrefix(db, epoch, hash, now, at, id, recent)
		if e != nil {
			return nil, e
		}
		scanned = append(scanned, rows...)
		var endAt, endID int64
		if len(rows) == tokenDetectionScanRows {
			last := rows[len(rows)-1]
			endAt, endID = last.CreatedAt, last.LogID
		}
		if progress != nil {
			if recent {
				progress.RecentScanAt, progress.RecentScanID = endAt, endID
			} else {
				progress.HistoryScanAt, progress.HistoryScanID = endAt, endID
			}
			progress.ScanMore = progress.ScanMore || len(rows) == tokenDetectionScanRows
		}
	}
	// Immediate link/fact changes have their own partial index. They do not
	// wait for the historical discovery cursor to wrap around.
	var changed []TokenMeteringFact
	if err := db.Table(tokenReportPendingMembers+" INDEXED BY idx_token_detection_changed_identity_v1").
		Select("source_epoch,log_id,created_at").Where("source_epoch=? AND base_pending=1 AND head_present=1 AND retry_present=0", epoch).
		Order("created_at,log_id").Limit(limit).Scan(&changed).Error; err != nil {
		return nil, err
	}
	lanes[1] = append(lanes[1], changed...)
	for _, row := range scanned {
		if (!row.RetryPresent && row.HeadPresent && row.Changed) || row.RetryChanged {
			lanes[1] = append(lanes[1], TokenMeteringFact{SourceEpoch: row.SourceEpoch, LogID: row.LogID, CreatedAt: row.CreatedAt})
		}
	}
	sort.SliceStable(lanes[1], func(i, j int) bool {
		a, b := lanes[1][i], lanes[1][j]
		if a.CreatedAt == b.CreatedAt {
			return a.LogID < b.LogID
		}
		return a.CreatedAt < b.CreatedAt
	})
	if len(lanes[1]) >= 32 {
		lanes[1], err = tokenDetectionRecentCorrections(db, epoch, hash, now, limit, cadence, lanes[1], windows.Correction)
		if err != nil {
			return nil, err
		}
	}
	// Scheduled candidates are bounded before any validity test. Stale due
	// heads are legitimate repair jobs; rejecting them first could scan forever.
	if err := db.Raw(`WITH due AS MATERIALIZED (SELECT source_epoch,log_id,next_at,fact_revision,link_revision,policy_hash FROM token_detection_retries
	 INDEXED BY idx_token_detection_retry_schedule_v1 WHERE source_epoch=? AND next_at<=? ORDER BY next_at,log_id LIMIT ?)
	 SELECT f.source_epoch,f.log_id FROM due r JOIN token_report_pending_members_v1 f ON f.source_epoch=r.source_epoch AND f.log_id=r.log_id
	 ORDER BY r.next_at,f.created_at,f.log_id`, epoch, now, limit).Scan(&lanes[2]).Error; err != nil {
		return nil, err
	}
	if err := db.Table(tokenReportPendingMembers+" INDEXED BY idx_token_detection_pending_due_v1").Select("source_epoch,log_id").
		Where("source_epoch=? AND retry_present=0 AND head_present=1 AND next_at>0 AND next_at<=?", epoch, now).
		Order("next_at,created_at,log_id").Limit(limit).Scan(&lanes[3]).Error; err != nil {
		return nil, err
	}
	jobs := tokenDetectionInterleaveJobs(lanes, now, limit, cadence)
	if progress != nil {
		selected := map[int64]bool{}
		for _, job := range jobs {
			selected[job.LogID] = true
		}
		progress.AdvanceScans = true
		for _, row := range scanned {
			if ((!row.RetryPresent && row.HeadPresent && row.Changed) || row.RetryChanged) && !selected[row.LogID] {
				progress.AdvanceScans = false
				break
			}
		}
	}
	return jobs, nil
}
