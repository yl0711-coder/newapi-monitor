package monitor

import (
	"context"
	"time"

	"gorm.io/gorm"
)

// Derived historical rebuilds yield to already materialized recent work.
// Cleanup keeps its independent cadence and all migration cursors are retained.
func (m *Monitor) tokenLatestWorkPending(parent context.Context, now int64) bool {
	if !m.cfg.TokenDetectionEnabled {
		return false
	}
	ctx, cancel := context.WithTimeout(parent, 250*time.Millisecond)
	defer cancel()
	pending := false
	err := m.tokenBackgroundRead(ctx, func(tx *gorm.DB) error {
		if !tx.Migrator().HasIndex(tokenReportPendingMembers, "idx_token_detection_pending_fresh_v1") {
			return nil
		}
		for stage := 0; stage < 2; stage++ {
			from := now - 900
			index, where := "idx_token_detection_pending_fresh_v1", "retry_present=0 AND head_present=0"
			if stage == 1 {
				index, where = "idx_token_detection_changed_identity_v1", "retry_present=0 AND head_present=1 AND base_pending=1"
				from = now - 3*3600
			}
			var id int64
			if err := tx.Table(tokenReportPendingMembers+" INDEXED BY "+index).Select("log_id").Where("source_epoch=? AND created_at>=? AND created_at<=? AND "+where, m.cfg.TokenFactsSourceEpoch, from, now).Limit(1).Scan(&id).Error; err != nil {
				return err
			}
			if id > 0 {
				pending = true
				break
			}
		}
		return nil
	})
	return pending || err != nil
}

// Seek already materialized recent identities before historical discovery.
// Migration readiness remains unchanged. Preparation re-reads each fact/head;
// the existing per-job lease and revision validation fence every publication.
func tokenDetectionLatestJobs(tx *gorm.DB, epoch, hash string, now int64, limit, cadence int, progress *tokenDetectionWindowProgress) ([]TokenMeteringFact, error) {
	if limit <= 0 {
		return nil, nil
	}
	lanes := make([][]TokenMeteringFact, 4)
	urgent := make([][]TokenMeteringFact, 4)
	// The outer recent range also contains restart backlog. Giving its oldest
	// prefix half of every fresh/correction lane lets a moving tail repeatedly
	// abandon the middle. Drain new arrivals chronologically in a short range;
	// the existing older probes keep a bounded share below, including during
	// projection rebuild. No cursor or readiness marker is advanced here.
	urgentFrom := now - 180
	var state TokenDetectionState
	if err := tx.Where("source_epoch=?", epoch).Limit(1).Find(&state).Error; err != nil {
		return nil, err
	}
	at, id := state.RecentScanAt, state.RecentScanID
	if state.ScanPolicyHash != hash {
		at, id = 0, 0
	}
	// Reuse the existing durable recent discovery cursor for reference, policy
	// and source-window revisions. A bounded prefix is consulted even when
	// fresh work is busy. Keep its fifteen-minute range: a reference, sealed
	// source window or due review may change after the urgent first three
	// minutes. Priority affects selection, not which dependencies remain visible.
	scanned, err := tokenDetectionScanPrefix(tx, epoch, hash, now, at, id, true)
	if err != nil {
		return nil, err
	}
	for stage := 0; stage < 2; stage++ {
		from := now - 900
		index, predicate := "idx_token_detection_pending_fresh_v1", "retry_present=0 AND head_present=0"
		if stage == 1 {
			index, predicate = "idx_token_detection_changed_identity_v1", "retry_present=0 AND head_present=1 AND base_pending=1"
			// Retain the existing three-hour correction horizon: delayed exact
			// evidence must not fall behind the historical queue after 15 minutes.
			from = now - 3*3600
		}
		query := func(start int64, order string, count int) ([]TokenMeteringFact, error) {
			var rows []TokenMeteringFact
			err := tx.Table(tokenReportPendingMembers+" INDEXED BY "+index).Select("source_epoch,log_id,created_at").
				Where("source_epoch=? AND created_at>=? AND created_at<=? AND "+predicate, epoch, start, now).
				Order(order).Limit(count).Scan(&rows).Error
			return rows, err
		}
		urgent[stage], err = query(urgentFrom, "created_at,log_id", limit)
		if err != nil {
			return nil, err
		}
		tail, err := query(from, "created_at DESC,log_id DESC", max(1, limit/2))
		if err != nil {
			return nil, err
		}
		head, err := query(from, "created_at,log_id", max(1, limit/2))
		if err != nil {
			return nil, err
		}
		// Both ends get early commits under a short writer budget.
		for i := 0; i < max(len(tail), len(head)); i++ {
			if i < len(head) {
				lanes[stage] = append(lanes[stage], head[i])
			}
			if i < len(tail) {
				lanes[stage] = append(lanes[stage], tail[i])
			}
		}
	}
	// If either live three-minute lane is idle, first evaluations or association
	// corrections in the rest of the recent window inherit that lane's budget.
	// Keep older three-hour corrections in their existing background lane;
	// otherwise a short round can spend half its correction share on that
	// backlog while a ten-minute-old request waits for its second decision.
	for stage := 0; stage < 2; stage++ {
		if len(urgent[stage]) == 0 {
			older := make([]TokenMeteringFact, 0, len(lanes[stage]))
			for _, job := range lanes[stage] {
				if job.CreatedAt >= now-900 {
					urgent[stage] = append(urgent[stage], job)
				} else {
					older = append(older, job)
				}
			}
			lanes[stage] = older
		}
	}
	hasPrimaryWork := len(urgent[0])+len(urgent[1]) > 0
	for _, row := range scanned {
		target := lanes
		if row.CreatedAt >= urgentFrom {
			target = urgent
		}
		if (!row.RetryPresent && row.HeadPresent && row.Changed) || row.RetryChanged {
			// Reserve dependency repair opportunities alongside primary work,
			// but do not promote only repairs above all recent first decisions.
			if hasPrimaryWork {
				target = urgent
			}
			target[3] = append(target[3], TokenMeteringFact{SourceEpoch: row.SourceEpoch, LogID: row.LogID, CreatedAt: row.CreatedAt})
		} else if row.HeadPresent && !row.RetryPresent && row.Due {
			target[3] = append(target[3], TokenMeteringFact{SourceEpoch: row.SourceEpoch, LogID: row.LogID, CreatedAt: row.CreatedAt})
		}
	}
	// Equality on BOTH prefix columns is required for the event-time seek.
	// Bound each prefix before consulting mutable retry due times.
	for _, head := range []int{0, 1} {
		var retries []TokenMeteringFact
		err := tx.Raw(`WITH recent AS MATERIALIZED (
	 SELECT source_epoch,log_id,created_at FROM token_report_pending_members_v1 INDEXED BY idx_token_detection_pending_fresh_v1
	 WHERE source_epoch=? AND retry_present=1 AND head_present=? AND created_at>=? AND created_at<=?
	 ORDER BY created_at DESC,log_id DESC LIMIT ?)
	 SELECT f.source_epoch,f.log_id,f.created_at FROM recent f JOIN token_detection_retries r
	 ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id WHERE r.next_at<=? ORDER BY f.created_at DESC,f.log_id DESC`, epoch, head, now-900, now, limit, now).Scan(&retries).Error
		if err != nil {
			return nil, err
		}
		for _, row := range retries {
			if row.CreatedAt >= urgentFrom {
				urgent[2] = append(urgent[2], row)
			} else {
				lanes[2] = append(lanes[2], row)
			}
		}
	}
	// Rotate only the two primary lanes; retry cannot take the whole first
	// short transaction ahead of both a new fact and a late-evidence correction.
	jobs := tokenDetectionPriorityJobs(
		tokenDetectionInterleaveJobs(urgent, now, limit, max(cadence, 1), true),
		tokenDetectionInterleaveJobs(lanes, now, limit, max(cadence, 1), true), limit)
	if progress != nil {
		progress.RecentDiscovery = scanned
		progress.ScanPolicyHash, progress.AdvanceRecentScan = hash, true
		progress.ScanMore = len(scanned) == tokenDetectionScanRows
		if progress.ScanMore {
			last := scanned[len(scanned)-1]
			progress.RecentScanAt, progress.RecentScanID = last.CreatedAt, last.LogID
		}
		selected := map[int64]bool{}
		for _, job := range jobs {
			selected[job.LogID] = true
		}
		for _, row := range scanned {
			if ((!row.RetryPresent && row.HeadPresent && row.Changed) || row.RetryChanged) && !selected[row.LogID] {
				progress.AdvanceRecentScan = false
				break
			}
		}
	}
	return jobs, nil
}

// Both lists share the existing row cap, leases and commit path. Background
// work retains one opportunity per sixteen selected identities while current
// arrivals are busy, and immediately uses every unused primary opportunity.
// Count unique jobs, so overlapping probes cannot erase the reserved share.
func tokenDetectionPriorityJobs(primary, background []TokenMeteringFact, limit int) []TokenMeteringFact {
	result := make([]TokenMeteringFact, 0, max(0, limit))
	seen := make(map[struct {
		epoch string
		id    int64
	}]bool)
	lanes, positions := [][]TokenMeteringFact{primary, background}, [2]int{}
	add := func(lane int) bool {
		for positions[lane] < len(lanes[lane]) {
			job := lanes[lane][positions[lane]]
			positions[lane]++
			key := struct {
				epoch string
				id    int64
			}{job.SourceEpoch, job.LogID}
			if !seen[key] {
				seen[key] = true
				result = append(result, job)
				return true
			}
		}
		return false
	}
	for len(result) < limit {
		lane := 0
		if len(result)%16 == 15 {
			lane = 1
		}
		if !add(lane) && !add(1-lane) {
			break
		}
	}
	return result
}
