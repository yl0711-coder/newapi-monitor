package monitor

import (
	"context"
	"errors"
	"fmt"
	"time"

	mysqlDriver "github.com/go-sql-driver/mysql"
)

const (
	usageFactBulkCircuitClosed   = "closed"
	usageFactBulkCircuitOpen     = "open"
	usageFactBulkCircuitHalfOpen = "half_open"

	usageFactBulkTimeoutPause = 15 * time.Minute
	usageFactBulkHardPause    = time.Hour
	usageFactBulkTripCount    = 3
	usageFactBulkProbeCount   = 3
)

func normalizedUsageFactBulkCircuitState(state string) string {
	switch state {
	case usageFactBulkCircuitOpen, usageFactBulkCircuitHalfOpen:
		return state
	default:
		return usageFactBulkCircuitClosed
	}
}

func usageFactHistoryBulkTimeout(err error) bool {
	if err == nil || errors.Is(err, context.Canceled) {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	var my *mysqlDriver.MySQLError
	return errors.As(err, &my) && my.Number == 3024
}

func applyUsageFactBulkFailure(state *UsageFactSyncState, cause error, now time.Time) {
	state.HistoryBulkFailureStreak++
	state.HistoryBulkSlowStreak = 0
	state.HistoryBulkHalfOpenSuccesses = 0
	pause := usageFactBulkTimeoutPause
	if state.HistoryBulkFailureStreak >= usageFactBulkTripCount {
		pause = usageFactBulkHardPause
	}
	state.HistoryBulkCircuitState = usageFactBulkCircuitOpen
	state.HistoryBulkOpenedUntil = now.Add(pause).Unix()
	state.HistoryBulkLastQueryAt = now.Unix()
	if cause != nil {
		state.HistoryBulkLastError = truncateUsageFactError(cause.Error())
	}
}

func applyUsageFactBulkSuccess(state *UsageFactSyncState, result usageFactHistoryRange, now time.Time) {
	perQuery := time.Duration(0)
	if result.SourceQueries > 0 {
		perQuery = result.QueryDuration / time.Duration(result.SourceQueries)
	}
	state.HistoryBulkLastQueryMS = perQuery.Milliseconds()
	state.HistoryBulkLastQueryAt = now.Unix()
	state.HistoryBulkLastError = ""
	if perQuery > 2*time.Second {
		state.HistoryBulkFailureStreak = 0
		state.HistoryBulkSlowStreak++
		state.HistoryBulkHalfOpenSuccesses = 0
		if state.HistoryBulkSlowStreak >= usageFactBulkTripCount {
			state.HistoryBulkCircuitState = usageFactBulkCircuitOpen
			state.HistoryBulkOpenedUntil = now.Add(usageFactBulkHardPause).Unix()
		} else if normalizedUsageFactBulkCircuitState(state.HistoryBulkCircuitState) == usageFactBulkCircuitHalfOpen {
			state.HistoryBulkCircuitState = usageFactBulkCircuitOpen
			state.HistoryBulkOpenedUntil = now.Add(usageFactBulkTimeoutPause).Unix()
		}
		return
	}
	state.HistoryBulkSlowStreak = 0
	state.HistoryBulkFailureStreak = 0
	if normalizedUsageFactBulkCircuitState(state.HistoryBulkCircuitState) == usageFactBulkCircuitHalfOpen {
		state.HistoryBulkHalfOpenSuccesses++
		if state.HistoryBulkHalfOpenSuccesses < usageFactBulkProbeCount {
			state.HistoryBulkCircuitState = usageFactBulkCircuitHalfOpen
			state.HistoryBulkOpenedUntil = 0
			return
		}
	}
	state.HistoryBulkCircuitState = usageFactBulkCircuitClosed
	state.HistoryBulkOpenedUntil = 0
	state.HistoryBulkHalfOpenSuccesses = 0
}

func usageFactBulkCircuitUpdates(state UsageFactSyncState) map[string]any {
	return map[string]any{
		"history_bulk_circuit_state":       state.HistoryBulkCircuitState,
		"history_bulk_opened_until":        state.HistoryBulkOpenedUntil,
		"history_bulk_slow_streak":         state.HistoryBulkSlowStreak,
		"history_bulk_failure_streak":      state.HistoryBulkFailureStreak,
		"history_bulk_half_open_successes": state.HistoryBulkHalfOpenSuccesses,
		"history_bulk_last_query_ms":       state.HistoryBulkLastQueryMS,
		"history_bulk_last_query_at":       state.HistoryBulkLastQueryAt,
		"history_bulk_last_error":          state.HistoryBulkLastError,
	}
}

func (m *Monitor) mutateUsageFactBulkCircuit(ctx context.Context, mutate func(*UsageFactSyncState)) error {
	if _, hasDeadline := ctx.Deadline(); !hasDeadline {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
	}
	m.usageFactsSyncMu.Lock()
	defer m.usageFactsSyncMu.Unlock()
	var state UsageFactSyncState
	if err := m.usageFactsStore().WithContext(ctx).First(&state, 1).Error; err != nil {
		return err
	}
	mutate(&state)
	return m.usageFactsStore().WithContext(ctx).Model(&UsageFactSyncState{}).Where("id = ?", 1).
		Updates(usageFactBulkCircuitUpdates(state)).Error
}

func (m *Monitor) usageFactHistoryBulkCircuitAllowed(ctx context.Context, now time.Time) (bool, bool, error) {
	var state UsageFactSyncState
	if err := m.usageFactsStore().WithContext(ctx).First(&state, 1).Error; err != nil {
		return false, false, err
	}
	switch normalizedUsageFactBulkCircuitState(state.HistoryBulkCircuitState) {
	case usageFactBulkCircuitOpen:
		if now.Unix() < state.HistoryBulkOpenedUntil {
			return false, false, nil
		}
		if err := m.mutateUsageFactBulkCircuit(ctx, func(current *UsageFactSyncState) {
			if normalizedUsageFactBulkCircuitState(current.HistoryBulkCircuitState) == usageFactBulkCircuitOpen &&
				now.Unix() >= current.HistoryBulkOpenedUntil {
				current.HistoryBulkCircuitState = usageFactBulkCircuitHalfOpen
				current.HistoryBulkOpenedUntil = 0
				current.HistoryBulkHalfOpenSuccesses = 0
			}
		}); err != nil {
			return false, false, err
		}
		return true, true, nil
	case usageFactBulkCircuitHalfOpen:
		return true, true, nil
	default:
		return true, false, nil
	}
}

func (m *Monitor) recordUsageFactHistoryBulkFailure(ctx context.Context, cause error, now time.Time) error {
	if !usageFactHistoryBulkTimeout(cause) {
		return nil
	}
	if err := m.mutateUsageFactBulkCircuit(ctx, func(state *UsageFactSyncState) {
		applyUsageFactBulkFailure(state, cause, now)
	}); err != nil {
		return fmt.Errorf("persist full-history bulk circuit failure: %w", err)
	}
	return nil
}

func (m *Monitor) recordUsageFactHistoryBulkSuccess(ctx context.Context, result usageFactHistoryRange, now time.Time) error {
	if err := m.mutateUsageFactBulkCircuit(ctx, func(state *UsageFactSyncState) {
		applyUsageFactBulkSuccess(state, result, now)
	}); err != nil {
		return fmt.Errorf("persist full-history bulk circuit success: %w", err)
	}
	return nil
}
