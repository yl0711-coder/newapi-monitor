package monitor

import "gorm.io/gorm"

type tokenMeteringLane uint8

const (
	tokenMeteringLaneForward tokenMeteringLane = iota
	tokenMeteringLaneRecent
	tokenMeteringLaneReplay
)
const (
	tokenMeteringLaneRealtimeTail   = "realtime_tail"
	tokenMeteringLaneForwardCatchup = "forward_catchup"
	tokenMeteringLaneReplayRepair   = "replay_repair"
)

func tokenMeteringLaneName(lane tokenMeteringLane) string {
	switch lane {
	case tokenMeteringLaneRecent:
		return tokenMeteringLaneRealtimeTail
	case tokenMeteringLaneReplay:
		return tokenMeteringLaneReplayRepair
	default:
		return tokenMeteringLaneForwardCatchup
	}
}
func tokenMeteringWindowLane(w TokenMeteringWindow, scanThrough int64) string {
	if w.Lane != "" {
		return w.Lane
	}
	if w.Replay {
		return tokenMeteringLaneReplayRepair
	}
	if w.StartAt > scanThrough {
		return tokenMeteringLaneRealtimeTail
	}
	return tokenMeteringLaneForwardCatchup
}
func tokenMeteringNeedsRecent(scan, now int64) bool { return now > 0 }

// Reserve the first source turn for committed new requests even when history
// is caught up. All lanes retain their own budget and the same four-read cap.
func tokenMeteringRoundLanes(scan, now int64, rounds uint64) []tokenMeteringLane {
	return []tokenMeteringLane{tokenMeteringLaneRecent, tokenMeteringLaneRecent, tokenMeteringLaneForward, tokenMeteringLaneReplay}
}

// The five bucket horizon includes the open tail, overlap and a newly sealable
// bucket. A backed-off bucket stays durable but cannot block a newer one. The
// lane alternates the newest bucket with the least recently serviced overlap;
// bounded partial pages and restart use the same authoritative window cursor.
func tokenMeteringRecentWindow(tx *gorm.DB, state TokenMeteringState, now int64) (TokenMeteringWindow, bool, error) {
	tail := now / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	floor := max(state.StartAt, tail-tokenMeteringLagSeconds-2*tokenMeteringWindowSeconds)
	var rows []TokenMeteringWindow
	if err := tx.Where("source_epoch=? AND start_at>=? AND start_at<=?", state.SourceEpoch, floor, tail).
		Order("start_at DESC").Limit(8).Find(&rows).Error; err != nil {
		return TokenMeteringWindow{}, false, err
	}
	byStart := map[int64]TokenMeteringWindow{}
	for _, row := range rows {
		byStart[row.StartAt] = row
	}
	var lane TokenMeteringLaneState
	if err := tx.Where("source_epoch=? AND lane=?", state.SourceEpoch, tokenMeteringLaneRealtimeTail).Limit(1).Find(&lane).Error; err != nil {
		return TokenMeteringWindow{}, false, err
	}
	if lane.WindowStart == tail {
		if repair, ok, err := tokenMeteringRecentSourceRepair(tx, state, now); ok || err != nil {
			return repair, ok, err
		}
	}
	var selected TokenMeteringWindow
	found := false
	for start := tail; start >= floor; start -= tokenMeteringWindowSeconds {
		if start <= state.ScanThrough {
			// The forward lane owns this prefix; recent reservations serve the
			// committed tail beyond it without stealing its retry/page budget.
			continue
		}
		w, exists := byStart[start]
		if !exists {
			w = TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: start, EndAt: start + tokenMeteringWindowSeconds, CursorAt: start}
		}
		if w.Done && !w.Replay || w.NextAt > now {
			continue
		}
		if start == tail && lane.WindowStart != tail {
			return w, true, nil
		}
		if !found || selected.StartAt == tail || w.StartAt != tail && w.UpdatedAt < selected.UpdatedAt {
			selected, found = w, true
		}
	}
	return selected, found, nil
}

// Called only in the lease-fenced reservation transaction. An unfinished
// prefix never rewinds. After open EOF, overlap catches late inserts; the
// first seal pass rereads the whole bounded bucket before granting coverage.
func tokenMeteringResumeWindow(w *TokenMeteringWindow, now int64) {
	sealed := (now - tokenMeteringLagSeconds) / tokenMeteringWindowSeconds * tokenMeteringWindowSeconds
	if w.EndAt <= sealed {
		if !w.SealStarted && w.Lane == tokenMeteringLaneRealtimeTail {
			w.CursorAt, w.CursorID = w.StartAt, 0
		}
		w.SealStarted = true
	} else if w.OpenObservedAt > 0 && w.NextAt > 0 && w.NextAt <= now {
		w.CursorAt, w.CursorID = max(w.StartAt, w.CursorAt-tokenMeteringTailOverlap), 0
		w.OpenObservedAt = 0
	}
}
