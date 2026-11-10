package monitor

import (
	"slices"

	"gorm.io/gorm"
)

// A multi-day fresh backlog must not hide recently collected windows. Within
// the recent range, preserve an older prefix while spending most opportunities
// on the new sealed tail. New facts may require both an initial decision and
// an association/coverage correction before they become current.
// Recent probes are bounded index seeks; no full backlog count is needed.
func tokenDetectionFreshJobs(db *gorm.DB, epoch string, now int64, limit, cadence int, indexed bool, retained ...int64) ([]TokenMeteringFact, error) {
	query := func(order string, count int) *gorm.DB {
		var q *gorm.DB
		if indexed {
			q = db.Table(tokenReportPendingMembers+" AS f INDEXED BY idx_token_detection_pending_fresh_v1").
				Where("f.source_epoch=? AND f.retry_present=0 AND f.head_present=0", epoch)
		} else {
			q = db.Table("token_metering_facts AS f").Joins(tokenDetectionHeadJoin(db)).
				Joins("LEFT JOIN token_detection_retries r ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id").
				Where("f.source_epoch=? AND (c.log_id IS NULL OR COALESCE(c.detection_state,'')='') AND r.log_id IS NULL", epoch)
		}
		return q.Select("f.source_epoch,f.log_id,f.created_at").Order(order).Limit(count)
	}
	var oldest, recent []TokenMeteringFact
	if err := query("f.created_at,f.log_id", max(limit, 32)).Scan(&oldest).Error; err != nil {
		return nil, err
	}
	identity := func(rows []TokenMeteringFact) []TokenMeteringFact {
		for i := range rows {
			rows[i].CreatedAt = 0 // Work items carry identity, not a stale fact copy.
		}
		return rows
	}
	if len(oldest) < 32 {
		return identity(oldest[:min(limit, len(oldest))]), nil
	}
	// A sealed tail can be several minutes behind the detector clock while the
	// oldest pending row is still recent. Probe both ends of a bounded three-hour
	// tail. A single ascending prefix is insufficient when historical facts
	// occupy that whole interval: newly sealed facts then remain headless until
	// every older row in the interval has been evaluated. Split the bounded
	// sample so the historical prefix and the newest sealed suffix each retain
	// a deterministic share of the same row budget. Both branches are index
	// seeks; the merge is identity-only and deduplicates their overlap.
	recentHead := max(1, limit/4)
	recentTail := max(0, limit-recentHead)
	if limit < 32 {
		// Keep small compatibility/report probes strictly chronological. The
		// split is for bounded worker batches; tiny probes must retain the old
		// identity/order oracle used by callers and tests.
		recentHead, recentTail = limit, 0
	}
	if err := tokenDetectionFreshRecentEnds(db, epoch, now, recentHead, recentTail, indexed, &recent, retained...); err != nil {
		return nil, err
	}
	return identity(tokenDetectionPreferRecent(oldest, recent, now, limit, cadence)), nil
}

func tokenDetectionFreshRecentEnds(db *gorm.DB, epoch string, now int64, head, tail int, indexed bool, out *[]TokenMeteringFact, retained ...int64) error {
	if head <= 0 && tail <= 0 {
		*out = nil
		return nil
	}
	from, to := now-3*3600, now
	table, where := "", ""
	if indexed {
		table = tokenReportPendingMembers + " AS f INDEXED BY idx_token_detection_pending_fresh_v1"
		where = "f.source_epoch=? AND f.retry_present=0 AND f.head_present=0"
	} else {
		table = "token_metering_facts AS f" + " " + tokenDetectionHeadJoin(db) + " LEFT JOIN token_detection_retries r ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id"
		where = "f.source_epoch=? AND (c.log_id IS NULL OR COALESCE(c.detection_state,'')='') AND r.log_id IS NULL"
	}
	// Finish a bounded prefix of the preceding sealed window before newer
	// arrivals occupy the descending probe. The three-hour head may still be
	// historical work, so neither of the original two ends includes that suffix.
	// Take its slots from the tail; the selected row budget does not increase.
	carry := min(tokenDetectionMaxCommitRows, tail)
	if len(retained) > 0 && retained[0] > 0 {
		carry = max(carry, tail*2/3)
	}
	tail -= carry
	carryFrom, carryTo := tokenDetectionCarryWindow(now)
	if len(retained) > 0 && retained[0] > 0 {
		carryFrom, carryTo = retained[0], retained[0]+tokenMeteringWindowSeconds
	}
	sql := `WITH recent_head AS (
		SELECT f.source_epoch,f.log_id,f.created_at FROM ` + table + `
		WHERE ` + where + ` AND f.created_at>=? AND f.created_at<=?
		ORDER BY f.created_at,f.log_id LIMIT ?
	), recent_tail AS (
		SELECT f.source_epoch,f.log_id,f.created_at FROM ` + table + `
		WHERE ` + where + ` AND f.created_at>=? AND f.created_at<=?
		ORDER BY f.created_at DESC,f.log_id DESC LIMIT ?
	), recent_carry AS (
		SELECT f.source_epoch,f.log_id,f.created_at FROM ` + table + `
		WHERE ` + where + ` AND f.created_at>=? AND f.created_at<?
		ORDER BY f.created_at,f.log_id LIMIT ?
	)
	SELECT source_epoch,log_id,created_at,0 AS queue_lane FROM recent_head
	UNION ALL
	SELECT source_epoch,log_id,created_at,1 AS queue_lane FROM recent_tail
	UNION ALL
	SELECT source_epoch,log_id,created_at,2 AS queue_lane FROM recent_carry
	ORDER BY queue_lane,created_at,log_id`
	args := []any{epoch, from, to, head, epoch, from, to, tail, epoch, carryFrom, carryTo, carry}
	var rows []tokenDetectionRecentEndJob
	if err := db.Raw(sql, args...).Scan(&rows).Error; err != nil {
		return err
	}
	*out = tokenDetectionMergeRecentEnds(rows)
	return nil
}

// The newest sealed window gets the descending tail. Reserve its predecessor
// independently, so a moving boundary cannot defer a small unfinished suffix
// until all of the older three-hour backlog has been processed.
func tokenDetectionCarryWindow(now int64) (int64, int64) {
	// This is the historical carry lane. The independent latest identity
	// seeks serve open buckets without moving an unfinished sealed reservation.
	target := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	return target - 2*tokenMeteringWindowSeconds, target - tokenMeteringWindowSeconds
}

type tokenDetectionRecentEndJob struct {
	SourceEpoch string
	LogID       int64
	CreatedAt   int64
	QueueLane   int
}

// A bounded round often commits only the first few jobs of its selected batch.
// Sorting both probes together by event time put the whole older prefix before
// the newest suffix again. Interleave the two ordered probes so either end gets
// work even when the round ends before consuming its full row allowance. A
// one-to-one split lets the old three-hour prefix consume half of both the
// first-evaluation and correction lanes while new arrivals need both stages.
// First finish the reserved preceding-window prefix, then give the newest probe
// three slots per older slot. All probes share the row budget; the independent
// global historical reservation remains in tokenDetectionPreferRecent.
func tokenDetectionMergeRecentEnds(rows []tokenDetectionRecentEndJob) []TokenMeteringFact {
	var ends [3][]TokenMeteringFact
	for _, row := range rows {
		ends[row.QueueLane] = append(ends[row.QueueLane], TokenMeteringFact{SourceEpoch: row.SourceEpoch, LogID: row.LogID, CreatedAt: row.CreatedAt})
	}
	// The SQL merge orders each bounded result by ascending identity. Restore
	// the tail probe's descending direction: when fewer than half a batch are
	// new, the tail also contains older rows and those must not lead it again.
	slices.Reverse(ends[1])
	seen := make(map[int64]bool, len(rows))
	merged := make([]TokenMeteringFact, 0, len(rows))
	positions := [3]int{}
	for positions[0] < len(ends[0]) || positions[1] < len(ends[1]) || positions[2] < len(ends[2]) {
		// A large retained window may need many short rounds. Interleave its
		// reservation with both recent ends; putting the whole reservation first
		// would again hide new sealed traffic in rounds that commit fewer rows.
		for _, lane := range []int{2, 2, 2, 1, 2, 2, 2, 0, 2, 2, 2, 1, 2, 2, 2, 1} {
			if positions[lane] < len(ends[lane]) {
				job := ends[lane][positions[lane]]
				positions[lane]++
				if !seen[job.LogID] {
					seen[job.LogID] = true
					merged = append(merged, job)
				}
			}
		}
	}
	return merged
}

// Reserve an opportunity for the historical prefix, then consume both recent
// probes in their fair order. Unused recent capacity immediately serves history.
// A fixed half-share for history leaves recent work growing even when its
// arrivals fit in the existing total budget. Rotation also covers tiny turns.
func tokenDetectionPreferRecent(oldest, recent []TokenMeteringFact, now int64, limit, cadence int) []TokenMeteringFact {
	ends := [][]TokenMeteringFact{oldest, recent}
	first := int(now / int64(cadence) % 2)
	seen := map[int64]bool{}
	jobs := make([]TokenMeteringFact, 0, limit)
	add := func(job TokenMeteringFact) {
		if len(jobs) < limit && !seen[job.LogID] {
			seen[job.LogID] = true
			jobs = append(jobs, job)
		}
	}
	for offset := 0; offset < 2; offset++ {
		if rows := ends[(first+offset)%2]; len(rows) > 0 {
			add(rows[0])
		}
	}
	for _, rows := range [][]TokenMeteringFact{recent, oldest} {
		for _, job := range rows {
			add(job)
			if len(jobs) == limit {
				return jobs
			}
		}
	}
	return jobs
}
