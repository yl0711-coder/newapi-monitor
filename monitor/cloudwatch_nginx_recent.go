package monitor

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

// Minute-anchored observations live with their evidence. They do not certify the
// historical continuous cursor, and a failed minute never hides a newer one.
type CloudWatchNginxRecentWindow struct {
	StartAt        int64  `gorm:"primaryKey;autoIncrement:false" json:"from"`
	EndAt          int64  `json:"through"`
	NextAt         int64  `json:"next_at"`
	Attempts       int    `json:"attempts"`
	ObservedAt     int64  `json:"observed_at"`
	LastSuccessAt  int64  `json:"last_success_at"`
	SuccessThrough int64  `json:"success_through"`
	LastError      string `gorm:"size:64" json:"last_error"`
	LastState      string `gorm:"size:32" json:"last_state"`
}

func selectCloudWatchNginxRecentWindow(db *gorm.DB, now int64) (CloudWatchNginxRecentWindow, bool, error) {
	// One bounded query includes the current and two preceding minutes. This
	// rereads recently completed requests without waiting for the older overlap
	// windows to rotate. Identity upserts retain unique facts and late enrichment.
	tail := now/60*60 - 120
	var rows []CloudWatchNginxRecentWindow
	if err := db.Where("start_at>=? AND start_at<=?", tail-19*60, tail).Order("start_at DESC").Limit(20).Find(&rows).Error; err != nil {
		return CloudWatchNginxRecentWindow{}, false, err
	}
	byStart := map[int64]CloudWatchNginxRecentWindow{}
	for _, w := range rows {
		byStart[w.StartAt] = w
	}
	var selected CloudWatchNginxRecentWindow
	found := false
	for start := tail; start >= tail-19*60; start -= 60 {
		w, exists := byStart[start]
		if !exists {
			w = CloudWatchNginxRecentWindow{StartAt: start, EndAt: start + 180}
		}
		w.EndAt = min(start+180, now)
		if w.EndAt <= start {
			continue
		}
		if w.NextAt > now {
			continue
		}
		if start == tail {
			return w, true, nil
		}
		if !found || w.ObservedAt < selected.ObservedAt {
			selected, found = w, true
		}
	}
	return selected, found, nil
}

func (m *Monitor) runCloudWatchNginxRecentWindow(parent context.Context, from, to int64) (int, error) {
	ctx, cancel := context.WithTimeout(parent, 20*time.Second)
	defer cancel()
	var w CloudWatchNginxRecentWindow
	if err := m.nginxEvidenceDB.WithContext(ctx).Where("start_at=?", from).Limit(1).Find(&w).Error; err != nil {
		return 0, err
	}
	w.StartAt, w.EndAt = from, to
	// Checkpoint the attempt before the remote read. Cancellation/restart must
	// not manufacture a successful observation or discard the failed interval.
	w.ObservedAt = time.Now().Unix()
	w.NextAt = w.ObservedAt + 15
	w.LastState = "reading"
	if err := m.nginxEvidenceDB.WithContext(ctx).Save(&w).Error; err != nil {
		return 0, err
	}
	parser, err := m.cloudWatchEvidenceParser(false)
	queries := 0
	if err == nil {
		var evidence []cloudWatchStructuredEvidence
		evidence, err = m.queryCloudWatchNginxRange(ctx, parser, time.Unix(from, 0).UTC(), time.Unix(to, 0).UTC(), &queries, 0)
		if err == nil {
			err = m.persistCloudWatchNginxEvidenceMode(ctx, evidence, from, to, fmt.Sprintf("cw-recent-%d-%d", from, to), true)
		}
	}
	w.ObservedAt = time.Now().Unix()
	if err == nil {
		w.LastSuccessAt = w.ObservedAt
		w.SuccessThrough = to
		w.Attempts = 0
		w.LastError = ""
		w.LastState = "observed"
		w.NextAt = w.ObservedAt + 15
	} else if ctx.Err() != nil {
		// A bounded turn exhausted its allowance. Preserve the exact interval
		// and prior proof; yielding is neither source failure nor completion.
		w.LastState = "deferred"
		w.NextAt = w.ObservedAt + 15
	} else {
		w.Attempts++
		w.LastError = cloudWatchPreRouteErrorCode(err)
		w.LastState = "source_failed"
		w.NextAt = w.ObservedAt + min(int64(300), int64(15)<<min(w.Attempts, 4))
	}
	// A bounded finish independent of the consumed remote budget preserves the
	// retry state. Only successful persistence can publish this minute's proof.
	finish, cancelFinish := context.WithTimeout(context.WithoutCancel(parent), time.Second)
	defer cancelFinish()
	if e := m.nginxEvidenceDB.WithContext(finish).Save(&w).Error; e != nil {
		return queries, e
	}
	return queries, err
}

func (m *Monitor) tokenRecentEntryObservations(parent context.Context, now int64) map[string]any {
	out := map[string]any{"available": false, "scope": "per_minute_observations_not_historical_continuity"}
	if m.nginxEvidenceDB == nil {
		return out
	}
	ctx, cancel := context.WithTimeout(parent, 250*time.Millisecond)
	defer cancel()
	db := m.nginxEvidenceDB.WithContext(ctx)
	if !db.Migrator().HasTable(&CloudWatchNginxRecentWindow{}) {
		return out
	}
	var rows []CloudWatchNginxRecentWindow
	if err := db.Where("start_at>=? AND start_at<=?", now/60*60-21*60, now).Order("start_at DESC").Limit(20).Find(&rows).Error; err != nil {
		out["reason"] = "local_recent_entry_observations_unavailable"
		return out
	}
	out["available"], out["windows"] = true, rows
	return out
}
