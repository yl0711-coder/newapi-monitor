package monitor

// The live usage lane is deliberately separate from cold-history coverage.
// It keeps a durable continuous cursor per member and uses the same bounded
// raw-page transaction as cold import. A hot historical shard can therefore
// consume source time, but it can never own or falsely advance live freshness.

import (
	"context"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const (
	// A member may be published as soon as this recent, source-controlled
	// service window is continuous. Deep history then expands the left edge in
	// the cold lane. This prevents a large archive from blocking today's usage.
	usageFactRawLiveInitialLookbackHours = 24
	usageFactRawLiveCycleBudget          = 4 * time.Minute
)

func usageFactRawLiveEligible(state UsageFactMemberState, epoch string) bool {
	return state.Active && state.TrackedRevision > 0 && state.SourceEpoch == epoch &&
		state.ClassificationVersion == userTrafficClassificationVersion &&
		state.QuerySemanticsVersion == usageFactQuerySemanticsVersion &&
		(state.SourceHistoryStatus == "complete_hot" || state.SourceHistoryStatus == "no_history")
}

func (m *Monitor) prepareUsageFactRawLiveTargets(ctx context.Context, target int64, now time.Time) error {
	if target <= 0 {
		return errors.New("实时用量目标水位无效")
	}
	epoch := m.cfg.UsageFactsHistorySourceEpoch
	return m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var states []UsageFactMemberState
		if err := tx.Where("active = ?", true).Find(&states).Error; err != nil {
			return err
		}
		for _, state := range states {
			if !usageFactRawLiveEligible(state, epoch) {
				continue
			}
			updates := map[string]any{"live_target_hour": target, "updated_at": now.Unix()}
			through := int64(0)
			if state.LiveThroughHour != nil {
				through = *state.LiveThroughHour
			}
			if through <= 0 {
				through = target - usageFactRawLiveInitialLookbackHours*usageFactHourSeconds
				if state.SourceFloorHour != nil && through < *state.SourceFloorHour {
					through = *state.SourceFloorHour
				}
				if through > target {
					through = target
				}
				updates["live_from_hour"] = through
				updates["live_through_hour"] = through
				updates["live_attempts"] = 0
				updates["live_span_hours"] = usageFactRawShardDefaultHours
				updates["live_next_retry_at"] = 0
				updates["live_last_error"] = ""
			}
			if through >= target {
				updates["live_status"] = "ready"
			} else {
				updates["live_status"] = "catching_up"
			}
			if err := tx.Model(&UsageFactMemberState{}).
				Where("user_id = ? AND active = ? AND tracked_revision = ?", state.UserID, true, state.TrackedRevision).
				Updates(updates).Error; err != nil {
				return err
			}
		}
		return nil
	})
}

func (m *Monitor) nextUsageFactRawLiveMember(ctx context.Context, now time.Time) (UsageFactMemberState, bool, error) {
	var state UsageFactMemberState
	err := m.usageFactsStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		err := tx.Where("active = ? AND live_through_hour IS NOT NULL AND live_target_hour IS NOT NULL AND live_through_hour < live_target_hour AND live_next_retry_at <= ?", true, now.Unix()).
			Order("live_last_served_seq, live_last_served_at, live_through_hour, user_id").First(&state).Error
		if err != nil {
			return err
		}
		var maxServedSeq int64
		if err := tx.Model(&UsageFactMemberState{}).Select("COALESCE(MAX(live_last_served_seq), 0)").Scan(&maxServedSeq).Error; err != nil {
			return err
		}
		result := tx.Model(&UsageFactMemberState{}).
			Where("user_id = ? AND active = ? AND tracked_revision = ? AND live_through_hour = ?", state.UserID, true, state.TrackedRevision, *state.LiveThroughHour).
			Updates(map[string]any{"live_last_served_seq": maxServedSeq + 1, "live_last_served_at": now.Unix(), "updated_at": now.Unix()})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return fmt.Errorf("%w: live scheduling cursor changed user_id=%d", errUsageMemberControlIntegrity, state.UserID)
		}
		state.LiveLastServedSeq = maxServedSeq + 1
		state.LiveLastServedAt = now.Unix()
		return nil
	})
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return UsageFactMemberState{}, false, nil
	}
	return state, err == nil, err
}

func (m *Monitor) recordUsageFactRawLiveFailure(ctx context.Context, state UsageFactMemberState, cause error, now time.Time) error {
	if usageFactHistoryFailureIsSourceGlobal(cause) {
		return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND tracked_revision = ?", state.UserID, state.TrackedRevision).
			Updates(map[string]any{"live_last_served_at": now.Unix(), "live_last_error": truncateUsageFactError(cause.Error()), "updated_at": now.Unix()}).Error
	}
	attempts := state.LiveAttempts + 1
	status := "retry"
	if attempts >= 5 {
		status = "paused"
	}
	return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
		Where("user_id = ? AND tracked_revision = ?", state.UserID, state.TrackedRevision).
		Updates(map[string]any{
			"live_status": status, "live_attempts": attempts,
			"live_next_retry_at":  usageFactHistoryRetryAt(now, attempts),
			"live_last_served_at": now.Unix(), "live_last_failure_at": now.Unix(),
			"live_last_error": truncateUsageFactError(cause.Error()), "updated_at": now.Unix(),
		}).Error
}

// usageFactRawLiveShardRange gives an already-durable page checkpoint
// ownership of its exact range. This is essential during upgrades and after a
// cold/live handoff: an older one-hour checkpoint at the live cursor must be
// resumed as one hour instead of being reinterpreted as the lane's current
// 24/6/3-hour preference. Once that checkpoint completes, density-based
// adaptation selects the next range normally.
func usageFactRawLiveShardRange(db *gorm.DB, state UsageFactMemberState, hour int64) (int64, int, error) {
	if db == nil || state.LiveTargetHour == nil || hour <= 0 || *state.LiveTargetHour <= hour {
		return 0, 0, errors.New("实时用量没有可执行分片")
	}
	var existing UsageFactPageIngestState
	err := db.First(&existing, "user_id = ? AND hour_ts = ?", state.UserID, hour).Error
	if err == nil {
		through := usageFactRawPageStateThrough(existing)
		if existing.SourceEpoch != state.SourceEpoch || through <= hour || through > *state.LiveTargetHour ||
			through-hour > usageFactRawShardDefaultHours*usageFactHourSeconds {
			return 0, 0, errors.New("实时用量持久分片范围无效")
		}
		return through, int((through - hour) / usageFactHourSeconds), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return 0, 0, err
	}
	spanHours := normalizedUsageFactRawSpanHours(state.LiveSpanHours)
	remainingHours := int((*state.LiveTargetHour - hour) / usageFactHourSeconds)
	if remainingHours < spanHours {
		spanHours = remainingHours
	}
	if spanHours < 1 {
		spanHours = 1
	}
	return hour + int64(spanHours)*usageFactHourSeconds, spanHours, nil
}

func (m *Monitor) syncOneUsageFactRawLiveMember(ctx context.Context, state UsageFactMemberState, now time.Time) error {
	if state.LiveThroughHour == nil || state.LiveTargetHour == nil || *state.LiveThroughHour >= *state.LiveTargetHour {
		return nil
	}
	hour := *state.LiveThroughHour
	if hour <= 0 || hour%usageFactHourSeconds != 0 {
		return errors.New("实时用量成员水位无效")
	}
	through, spanHours, err := usageFactRawLiveShardRange(m.usageFactsStore().WithContext(ctx), state, hour)
	if err != nil {
		return err
	}
	source := m.usageFactRawPageSource(false)
	var pageSource usageFactRawPageSource = source
	if state.SourceHistoryStatus == "no_history" {
		var checkpoint UsageFactPageIngestState
		checkpointErr := m.usageFactsStore().WithContext(ctx).First(&checkpoint, "user_id = ? AND hour_ts = ?", state.UserID, hour).Error
		if checkpointErr != nil && !errors.Is(checkpointErr, gorm.ErrRecordNotFound) {
			return checkpointErr
		}
		if checkpointErr == nil && (checkpoint.SourceRows > 0 || checkpoint.CursorID > 0) {
			var job UsageFactJob
			if err := m.usageFactsStore().WithContext(ctx).First(&job, "id = ?", usageFactHistoryJobID(state.UserID, state.TrackedRevision)).Error; err != nil {
				return err
			}
			return m.invalidateNoHistoryAfterRawActivity(ctx, job, now)
		}
		first, err := source.FetchUsageFactRawPage(ctx, state.UserID, hour, through, 0, 0, 0, usageFactRawPageSize)
		if err != nil {
			return err
		}
		if len(first) > 0 {
			var job UsageFactJob
			if err := m.usageFactsStore().WithContext(ctx).First(&job, "id = ?", usageFactHistoryJobID(state.UserID, state.TrackedRevision)).Error; err != nil {
				return err
			}
			return m.invalidateNoHistoryAfterRawActivity(ctx, job, now)
		}
		pageSource = &usageFactPrefetchedRawPageSource{base: source, userID: state.UserID, fromTs: hour, throughTs: through, page: first}
	}
	complete, err := importUsageFactRawShardPages(ctx, m.usageFactsStore(), pageSource, state.UserID, hour, through, state.SourceEpoch, usageFactRawPagesPerTurn)
	if errors.Is(err, errUsageFactRawShardDense) && spanHours > 1 {
		nextSpan := smallerUsageFactRawSpanHours(spanHours)
		return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND tracked_revision = ? AND live_through_hour = ?", state.UserID, state.TrackedRevision, hour).
			Updates(map[string]any{"live_span_hours": nextSpan, "live_status": "catching_up", "live_last_served_at": now.Unix(), "updated_at": now.Unix()}).Error
	}
	if err != nil {
		return err
	}
	if !complete {
		return m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
			Where("user_id = ? AND tracked_revision = ? AND live_through_hour = ?", state.UserID, state.TrackedRevision, hour).
			Updates(map[string]any{"live_status": "importing", "live_last_served_at": now.Unix(), "updated_at": now.Unix()}).Error
	}
	var completedPage UsageFactPageIngestState
	if err := m.usageFactsStore().WithContext(ctx).First(&completedPage, "user_id = ? AND hour_ts = ?", state.UserID, hour).Error; err != nil {
		return err
	}
	nextSpanHours := adaptiveUsageFactRawSpanHours(spanHours, completedPage.SourceRows, completedPage.Pages)
	if err := m.commitUsageFactRawPageShardProof(ctx, state.UserID, hour, through, state.TrackedRevision); err != nil {
		return err
	}
	for completedHour := hour; completedHour < through; completedHour += usageFactHourSeconds {
		if err := m.maybeFinalizeUsageFactHistoryDayFromHours(ctx, completedHour, []int64{state.UserID}, false); err != nil {
			return err
		}
	}
	next := through
	status := "catching_up"
	if next >= *state.LiveTargetHour {
		status = "ready"
	}
	result := m.usageFactsStore().WithContext(ctx).Model(&UsageFactMemberState{}).
		Where("user_id = ? AND active = ? AND tracked_revision = ? AND live_through_hour = ?", state.UserID, true, state.TrackedRevision, hour).
		Updates(map[string]any{
			"live_through_hour": next, "live_status": status, "live_attempts": 0, "live_next_retry_at": 0,
			"live_span_hours":     nextSpanHours,
			"live_last_served_at": now.Unix(), "live_last_success_at": now.Unix(), "live_last_failure_at": int64(0),
			"live_last_error": "", "updated_at": now.Unix(),
		})
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("%w: live cursor changed user_id=%d", errUsageMemberControlIntegrity, state.UserID)
	}
	return nil
}

func (m *Monitor) syncUsageFactsRawLiveTail(ctx context.Context, now time.Time) error {
	target := m.usageFactFinalizedHour(now)
	if err := m.prepareUsageFactRawLiveTargets(ctx, target, now); err != nil {
		return err
	}
	deadline := time.Now().Add(usageFactRawLiveCycleBudget)
	var joined error
	for ctx.Err() == nil && time.Now().Before(deadline) {
		state, ok, err := m.nextUsageFactRawLiveMember(ctx, time.Now())
		if err != nil {
			return errors.Join(joined, err)
		}
		if !ok {
			break
		}
		turnNow := time.Now()
		if err := m.syncOneUsageFactRawLiveMember(ctx, state, turnNow); err != nil {
			joined = errors.Join(joined, err)
			if persistErr := m.recordUsageFactRawLiveFailure(context.Background(), state, err, turnNow); persistErr != nil {
				joined = errors.Join(joined, persistErr)
			}
		}
	}
	// The live loop is the owner of the current service target. Only after its
	// durable cursor is ready do we create/refresh a separate recent bridge;
	// the cold worker then consumes that bridge at low source duty.
	if err := m.prepareUsageFactRawRecentTargets(ctx, target, time.Now()); err != nil {
		joined = errors.Join(joined, err)
	}
	return joined
}
