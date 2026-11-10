package monitor

import "gorm.io/gorm"

// Both recent probes seek their own stage of the same bounded event-time range.
// A repeated check of an older recent request must not hide an unchecked one.
func initTokenMeteringLinkIndex(db *gorm.DB) error {
	if err := initTokenMeteringWindowIndexes(db); err != nil {
		return err
	}
	if err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_token_link_recent_stage_v1 ON token_metering_facts
		(source_epoch,(COALESCE(link_checked_at,0)>0),created_at,log_id,link_next_at)
		WHERE link_next_at>0`).Error; err != nil {
		return err
	}
	// Retention seeks a bounded keyset and evaluates the protection predicates
	// against the same identity columns. Keep this covering prefix separate
	// from the recent association lane so cleanup cannot force a table scan.
	return db.Exec(`CREATE INDEX IF NOT EXISTS idx_token_fact_cleanup_v1 ON token_metering_facts
		(source_epoch,created_at,log_id,link_state,link_next_at,revision,link_revision,stored_bytes)`).Error
}

func initTokenMeteringWindowIndexes(db *gorm.DB) error {
	for _, sql := range []string{
		`CREATE TABLE IF NOT EXISTS token_metering_source_repairs_v1(source_epoch TEXT NOT NULL,start_at INTEGER NOT NULL,requested_at INTEGER NOT NULL,PRIMARY KEY(source_epoch,start_at))`,
		`CREATE INDEX IF NOT EXISTS idx_token_window_incomplete_v1 ON token_metering_windows(source_epoch,start_at) WHERE COALESCE(done,0)=0 OR COALESCE(source_complete,0)=0 OR replay=1`,
		`CREATE INDEX IF NOT EXISTS idx_token_window_lane_pending_v1 ON token_metering_windows(source_epoch,lane,start_at,end_at,next_at) WHERE done=0 OR replay=1`,
		`CREATE INDEX IF NOT EXISTS idx_token_link_due_identity_v1 ON token_metering_facts(source_epoch,link_next_at,log_id) WHERE link_next_at>0`,
	} {
		if err := db.Exec(sql).Error; err != nil {
			return err
		}
	}
	return initTokenMeteringEvidenceCursor(db)
}

// Keep the existing row, HMAC and time budgets. Reserve an opportunity for
// oldest due work and recent rechecks, then finish recent first associations.
// Spare first-association capacity is shared by the other two lanes.
func tokenMeteringLinkJobs(db *gorm.DB, epoch string, now int64, limit, cadence int) ([]TokenMeteringFact, error) {
	if limit <= 0 {
		return nil, nil
	}
	var oldest []TokenMeteringFact
	if err := db.Where("source_epoch=? AND link_next_at>0 AND link_next_at<=?", epoch, now).Order("link_next_at,source_epoch,log_id").Limit(limit).Find(&oldest).Error; err != nil {
		return nil, err
	}
	index := "idx_token_detection_work_fact" // Read-only legacy stores retain a bounded-hour fallback.
	if db.Migrator().HasIndex(&TokenMeteringFact{}, "idx_token_link_recent_stage_v1") {
		index = "idx_token_link_recent_stage_v1"
	}
	lanes := [][]TokenMeteringFact{oldest, nil, nil}
	for stage := 0; stage < 2; stage++ {
		from := now - 900
		if stage == 1 {
			// Preserve the existing recent recheck horizon for delayed evidence.
			from = now - 3600
		}
		if err := db.Table("token_metering_facts INDEXED BY "+index).
			Where("source_epoch=? AND (COALESCE(link_checked_at,0)>0)=? AND created_at>=? AND created_at<=? AND link_next_at>0 AND link_next_at<=?", epoch, stage, from, now, now).
			Order("created_at DESC,log_id DESC").Limit(limit).Find(&lanes[stage+1]).Error; err != nil {
			return nil, err
		}
	}
	type identity struct {
		epoch string
		id    int64
	}
	seen, result := map[identity]bool{}, make([]TokenMeteringFact, 0, limit)
	add := func(f TokenMeteringFact) {
		key := identity{f.SourceEpoch, f.LogID}
		if len(result) < limit && !seen[key] {
			seen[key] = true
			result = append(result, f)
		}
	}
	positions := [3]int{}
	take := func(lane int) bool {
		for positions[lane] < len(lanes[lane]) && len(result) < limit {
			before := len(result)
			add(lanes[lane][positions[lane]])
			positions[lane]++
			if len(result) > before {
				return true
			}
		}
		return false
	}
	if limit < 3 {
		// Tiny committed prefixes rotate across the same three lanes.
		first := int(now / int64(max(cadence, 1)) % 3)
		for offset := 0; offset < 3; offset++ {
			take((first + offset) % 3)
		}
		return result, nil
	}
	// Reserve a correction share without allowing repeated checks to create a
	// first-association backlog at 40 arrivals per 50-row turn. Historical work
	// gets one early commit plus unused capacity; every query remains bounded.
	rechecks := max(1, limit/5-1)
	quota, used := [3]int{1, limit - 1 - rechecks, rechecks}, [3]int{}
	first := 1 + int(now/int64(max(cadence, 1))%2)
	for _, lane := range []int{first, 3 - first, 0} {
		if take(lane) {
			used[lane]++
		}
	}
	for len(result) < limit {
		advanced := false
		for _, lane := range []int{1, 1, 1, 1, 2} {
			if used[lane] < quota[lane] && take(lane) {
				used[lane]++
				advanced = true
			}
		}
		if !advanced {
			break
		}
	}
	for len(result) < limit {
		advanced := false
		for _, lane := range []int{1, 2, 0} {
			advanced = take(lane) || advanced
		}
		if !advanced {
			break
		}
	}
	return result, nil
}
