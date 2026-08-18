package monitor

// The recent bridge is the service-oriented counterpart to deep history.
// Live owns the newest continuous cursor and cold owns the immutable source
// floor. The bridge fills the normal seven-complete-day UI window plus today's
// finalized hours between them, then
// atomically moves the published left edge backwards. A partial bridge is
// never published, so missing hours cannot be rendered as zero.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const usageFactRecentServiceDays = 7

func usageFactRecentServiceFrom(target int64) int64 {
	// The UI's default seven-day report ends at yesterday.  At any point today
	// it therefore needs [today-7d, today), while the live lane separately owns
	// today's finalized hours.  Using days-1 here would cover only six complete
	// days plus today and would still omit the oldest date after a green sync.
	return usageFactDayStart(target) - int64(usageFactRecentServiceDays)*usageFactDaySeconds
}

func (m *Monitor) prepareUsageFactRawRecentTargets(ctx context.Context, target int64, now time.Time) error {
	if target <= 0 {
		return errors.New("近期用量桥接目标水位无效")
	}
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	desiredDefault := usageFactRecentServiceFrom(target)
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var states []UsageFactMemberState
		if err := tx.Where("active = ?", true).Find(&states).Error; err != nil {
			return err
		}
		for _, state := range states {
			if !usageFactRawLiveEligible(state, epoch) || state.LiveFromHour == nil || state.LiveThroughHour == nil ||
				state.LiveTargetHour == nil || *state.LiveThroughHour < *state.LiveTargetHour || state.LiveStatus != "ready" {
				continue
			}
			desired := desiredDefault
			if state.SourceFloorHour != nil && desired < *state.SourceFloorHour {
				desired = *state.SourceFloorHour
			}
			liveFrom := *state.LiveFromHour
			if desired >= liveFrom {
				if state.RecentStatus != "ready" || state.RecentFromHour == nil || *state.RecentFromHour != liveFrom {
					if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ? AND active = ? AND tracked_revision = ?", state.UserID, true, state.TrackedRevision).
						Updates(map[string]any{"recent_from_hour": liveFrom, "recent_through_hour": liveFrom, "recent_target_hour": liveFrom,
							"recent_status": "ready", "recent_attempts": 0, "recent_next_retry_at": 0, "recent_last_error": "", "updated_at": now.Unix()}).Error; err != nil {
						return err
					}
				}
				continue
			}
			// A source-wide no-history proof already covers the whole registered
			// range. Expanding its read boundary is local-only; the live activity
			// probe will revoke that signature before committing the first event.
			if state.SourceHistoryStatus == "no_history" {
				if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ? AND active = ? AND tracked_revision = ?", state.UserID, true, state.TrackedRevision).
					Updates(map[string]any{"live_from_hour": desired, "recent_from_hour": desired, "recent_through_hour": liveFrom,
						"recent_target_hour": liveFrom, "recent_status": "ready", "recent_attempts": 0,
						"recent_next_retry_at": 0, "recent_last_error": "", "updated_at": now.Unix()}).Error; err != nil {
					return err
				}
				continue
			}

			from, through, bridgeTarget := desired, desired, liveFrom
			if state.RecentFromHour != nil && state.RecentThroughHour != nil && state.RecentTargetHour != nil &&
				*state.RecentFromHour == desired && *state.RecentTargetHour == liveFrom {
				through = *state.RecentThroughHour
			}
			// Cold coverage is continuous from SourceFloorHour. Reuse it as a
			// proof instead of querying any hour twice.
			if state.CoverageThroughHour != nil && *state.CoverageThroughHour > through {
				through = min(*state.CoverageThroughHour, bridgeTarget)
			}
			status := "catching_up"
			updates := map[string]any{"recent_from_hour": from, "recent_through_hour": through, "recent_target_hour": bridgeTarget,
				"recent_status": status, "updated_at": now.Unix()}
			if state.RecentFromHour == nil || *state.RecentFromHour != from || state.RecentTargetHour == nil || *state.RecentTargetHour != bridgeTarget {
				updates["recent_attempts"], updates["recent_next_retry_at"] = 0, 0
				updates["recent_span_hours"], updates["recent_last_error"] = usageFactRawShardDefaultHours, ""
			}
			if through >= bridgeTarget {
				status = "ready"
				updates["recent_status"] = status
				updates["live_from_hour"] = from
				updates["recent_attempts"], updates["recent_next_retry_at"], updates["recent_last_error"] = 0, 0, ""
			}
			if err := tx.Model(&UsageFactMemberState{}).Where("user_id = ? AND active = ? AND tracked_revision = ?", state.UserID, true, state.TrackedRevision).
				Updates(updates).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Monitor) nextUsageFactRawRecentMember(ctx context.Context, now time.Time) (UsageFactMemberState, bool, error) {
	var state UsageFactMemberState
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("active = ? AND recent_through_hour IS NOT NULL AND recent_target_hour IS NOT NULL AND recent_through_hour < recent_target_hour AND recent_next_retry_at <= ?", true, now.Unix()).
			Order("recent_last_served_seq, recent_last_served_at, recent_through_hour, user_id").First(&state).Error; err != nil {
			return err
		}
		var maxSeq int64
		if err := tx.Model(&UsageFactMemberState{}).Select("COALESCE(MAX(recent_last_served_seq), 0)").Scan(&maxSeq).Error; err != nil {
			return err
		}
		result := tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ? AND recent_through_hour = ?", state.UserID, true, state.TrackedRevision, *state.RecentThroughHour).
			Updates(map[string]any{"recent_last_served_seq": maxSeq + 1, "recent_last_served_at": now.Unix(), "updated_at": now.Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: recent scheduling cursor changed user_id=%d", errUsageMemberControlIntegrity, state.UserID)
		}
		state.RecentLastServedSeq, state.RecentLastServedAt = maxSeq+1, now.Unix()
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return UsageFactMemberState{}, false, nil
	}
	return state, err == nil, err
}

func (m *Monitor) recordUsageFactRawRecentFailure(ctx context.Context, state UsageFactMemberState, cause error, now time.Time) error {
	if usageFactHistoryFailureIsSourceGlobal(cause) {
		return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND tracked_revision = ?", state.UserID, state.TrackedRevision).
			Updates(map[string]any{"recent_last_served_at": now.Unix(), "recent_last_error": truncateUsageFactError(cause.Error()), "updated_at": now.Unix()}).Error
	}
	attempts := state.RecentAttempts + 1
	status := "retry"
	if attempts >= 5 {
		status = "paused"
	}
	return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
		Where("user_id = ? AND tracked_revision = ?", state.UserID, state.TrackedRevision).
		Updates(map[string]any{"recent_status": status, "recent_attempts": attempts,
			"recent_next_retry_at": usageFactHistoryRetryAt(now, attempts), "recent_last_served_at": now.Unix(),
			"recent_last_failure_at": now.Unix(), "recent_last_error": truncateUsageFactError(cause.Error()), "updated_at": now.Unix()}).Error
}

func usageFactRawRecentShardRange(db *gorm.DB, state UsageFactMemberState, from int64) (int64, int, error) {
	if db == nil || state.RecentTargetHour == nil || from <= 0 || *state.RecentTargetHour <= from {
		return 0, 0, errors.New("近期用量桥接没有可执行分片")
	}
	var existing UsageFactPageIngestState
	err := db.First(&existing, "user_id = ? AND hour_ts = ?", state.UserID, from).Error
	if err == nil {
		through := usageFactRawPageStateThrough(existing)
		if existing.SourceEpoch != state.SourceEpoch || through <= from || through > *state.RecentTargetHour ||
			through-from > usageFactRawShardDefaultHours*usageFactHourSeconds {
			return 0, 0, errors.New("近期用量桥接持久分片范围无效")
		}
		return through, int((through - from) / usageFactHourSeconds), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, 0, err
	}
	span := normalizedUsageFactRawSpanHours(state.RecentSpanHours)
	remaining := int((*state.RecentTargetHour - from) / usageFactHourSeconds)
	if remaining < span {
		span = remaining
	}
	if span < 1 {
		span = 1
	}
	return from + int64(span)*usageFactHourSeconds, span, nil
}

func (m *Monitor) syncOneUsageFactRawRecentMember(ctx context.Context, state UsageFactMemberState, now time.Time) error {
	if state.RecentThroughHour == nil || state.RecentTargetHour == nil || *state.RecentThroughHour >= *state.RecentTargetHour {
		return nil
	}
	// Cold may have advanced after this scheduler ticket was issued. Consume
	// its continuous proof locally and yield without starting a duplicate read.
	var latest UsageFactMemberState
	if err := m.usageFactsStore().WithContext(ctx).First(&latest, "user_id = ? AND active = ? AND tracked_revision = ?", state.UserID, true, state.TrackedRevision).Error; err != nil {
		return err
	}
	from := *latest.RecentThroughHour
	if latest.CoverageThroughHour != nil && *latest.CoverageThroughHour > from {
		next := min(*latest.CoverageThroughHour, *latest.RecentTargetHour)
		return m.advanceUsageFactRawRecentCursor(ctx, latest, from, next, latest.RecentSpanHours, now)
	}
	through, span, err := usageFactRawRecentShardRange(m.usageFactsStore().WithContext(ctx), latest, from)
	if err != nil {
		return err
	}
	complete, err := importUsageFactRawShardPages(ctx, m.usageFactsStore(), m.usageFactRawPageSource(true), latest.UserID, from, through,
		latest.SourceEpoch, usageFactRawPagesPerTurn)
	if errors.Is(err, errUsageFactRawShardDense) && span > 1 {
		return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND tracked_revision = ? AND recent_through_hour = ?", latest.UserID, latest.TrackedRevision, from).
			Updates(map[string]any{"recent_span_hours": smallerUsageFactRawSpanHours(span), "recent_status": "catching_up",
				"recent_last_served_at": now.Unix(), "updated_at": now.Unix()}).Error
	}
	if err != nil {
		return err
	}
	if !complete {
		return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND tracked_revision = ? AND recent_through_hour = ?", latest.UserID, latest.TrackedRevision, from).
			Updates(map[string]any{"recent_status": "importing", "recent_last_served_at": now.Unix(), "updated_at": now.Unix()}).Error
	}
	var page UsageFactPageIngestState
	if err := m.usageFactsStore().WithContext(ctx).First(&page, "user_id = ? AND hour_ts = ?", latest.UserID, from).Error; err != nil {
		return err
	}
	nextSpan := adaptiveUsageFactRawSpanHours(span, page.SourceRows, page.Pages)
	if err := m.commitUsageFactRawPageShardProof(ctx, latest.UserID, from, through, latest.TrackedRevision); err != nil {
		return err
	}
	if err := m.finalizeUsageFactRecentTouchedDays(ctx, latest.UserID, from, through); err != nil {
		return err
	}
	return m.advanceUsageFactRawRecentCursor(ctx, latest, from, through, nextSpan, now)
}

// finalizeUsageFactRecentTouchedDays deliberately checks the end of every CST
// day touched by a bridge shard, even when the bridge itself ends before
// 23:00.  Live may already own the later hours of that day.  In that ordering,
// finalizing only when the bridge imports hour 23 would leave an older daily
// proof stale after the bridge replaces an earlier hour.  The non-strict
// finalizer is local-only in raw-page mode and simply yields until all 24
// independent hourly proofs exist.
func (m *Monitor) finalizeUsageFactRecentTouchedDays(ctx context.Context, userID, from, through int64) error {
	if userID <= 0 || from <= 0 || through <= from {
		return errors.New("近期用量日终签收范围无效")
	}
	lastDay := usageFactDayStart(through - 1)
	for day := usageFactDayStart(from); day <= lastDay; day += usageFactDaySeconds {
		if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, day+23*usageFactHourSeconds, []int64{userID}, false); err != nil {
			return err
		}
	}
	return nil
}

func (m *Monitor) advanceUsageFactRawRecentCursor(ctx context.Context, state UsageFactMemberState, expected, next int64, nextSpan int, now time.Time) error {
	if state.RecentFromHour == nil || state.RecentTargetHour == nil || next < expected || next > *state.RecentTargetHour {
		return errors.New("近期用量桥接推进范围无效")
	}
	status := "catching_up"
	updates := map[string]any{"recent_through_hour": next, "recent_status": status, "recent_attempts": 0,
		"recent_next_retry_at": 0, "recent_span_hours": normalizedUsageFactRawSpanHours(nextSpan),
		"recent_last_served_at": now.Unix(), "recent_last_success_at": now.Unix(), "recent_last_failure_at": int64(0),
		"recent_last_error": "", "updated_at": now.Unix()}
	if next >= *state.RecentTargetHour {
		status = "ready"
		updates["recent_status"] = status
		updates["live_from_hour"] = *state.RecentFromHour
	}
	result := m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
		Where("user_id = ? AND active = ? AND tracked_revision = ? AND recent_through_hour = ?", state.UserID, true, state.TrackedRevision, expected).
		Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: recent cursor changed user_id=%d", errUsageMemberControlIntegrity, state.UserID)
	}
	return nil
}

func (m *Monitor) syncNextUsageFactRawRecentBridge(ctx context.Context, now time.Time) (bool, error) {
	state, ok, err := m.nextUsageFactRawRecentMember(ctx, now)
	if err != nil || !ok {
		return false, err
	}
	if err := m.syncOneUsageFactRawRecentMember(ctx, state, now); err != nil {
		if errors.Is(err, errUsageFactRawPageSuperseded) {
			return true, nil
		}
		if persistErr := m.recordUsageFactRawRecentFailure(context.Background(), state, err, now); persistErr != nil {
			return true, errors.Join(err, persistErr)
		}
		return true, err
	}
	var latest UsageFactMemberState
	if err := m.usageFactsStore().WithContext(ctx).First(&latest, "user_id = ?", state.UserID).Error; err != nil {
		return true, err
	}
	if latest.RecentStatus == "ready" && latest.RecentThroughHour != nil && latest.RecentTargetHour != nil &&
		*latest.RecentThroughHour >= *latest.RecentTargetHour {
		// Service-window completion is a publication event, not merely cold
		// progress. Publish immediately so an always-nonempty archive queue cannot
		// keep a newly ready member hidden until the worker happens to become idle.
		if _, err := m.publishUsageFactFullHistorySnapshot(ctx, now); err != nil {
			return true, err
		}
	}
	return true, nil
}
