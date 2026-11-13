package monitor

import (
	"context"
	"errors"
	"net/http"
	"time"

	"gorm.io/gorm"
)

// Existing per-task counters and schedules persist the incident budget. No
// separate in-memory retry loop can reset it on restart. After the initial
// attempt and five retries, only three scheduled probe rounds are permitted.
const upstreamRecoveryProbeThreshold = 6
const upstreamRecoveryFailureLimit = 9

var upstreamRecoveryProbeDelays = [...]time.Duration{30 * time.Minute, 2 * time.Hour, 6 * time.Hour}

// Only a proven credential failure isolates an account until an administrator
// acts. Every other failure keeps being retried at a low, bounded rate after
// its incident budget, so a supplier that recovers (a slow log API, a passing
// outage, a 4xx from a temporarily changed endpoint) resumes by itself.
const (
	upstreamTransientFailureRecheck = 6 * time.Hour
	upstreamClientErrorRecheck      = 12 * time.Hour
)

func upstreamProvenAuthenticationFailure(err error) bool {
	var httpErr *upstreamHTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Status == http.StatusUnauthorized
	}
	var auth *upstreamAuthError
	return errors.As(err, &auth)
}

func upstreamNeedsRecoveryProbe(status string, failures int) bool {
	return status == upstreamStatusError && failures >= upstreamRecoveryProbeThreshold && failures < upstreamRecoveryFailureLimit
}

// Apply after the lane's ordinary retry calculation, so provider-specific
// intervals and valid Retry-After values remain lower bounds. Local scheduling
// yields and database contention do not consume the upstream incident budget.
func boundUpstreamFailure(err error, now int64, failures *int, status *string, next *int64) {
	var circuit *upstreamCircuitOpenError
	if isUpstreamUsageLocalStoreBusy(err) || isUpstreamLocalQueueTimeout(err) || errors.As(err, &circuit) {
		*failures = max(0, *failures-1)
		return
	}
	var httpErr *upstreamHTTPError
	var auth *upstreamAuthError
	forbidden := false
	if errors.As(err, &httpErr) {
		if httpErr.Status == http.StatusForbidden {
			// A 403 does not prove credential expiry. Keep the REAL failure
			// count, enforce a conservative cooldown, then use the finite budget.
			forbidden = true
		} else if httpErr.Status == http.StatusUnauthorized {
			*status, *next = upstreamStatusReconnect, upstreamAccountIsolatedUntil
			return
		} else if httpErr.Status >= 400 && httpErr.Status < 500 && httpErr.Status != 408 && httpErr.Status != 425 && httpErr.Status != 429 {
			// One 4xx is not proof the account is unusable; recheck rarely.
			*status, *next = upstreamStatusError, now+int64(upstreamClientErrorRecheck/time.Second)
			if at := upstreamRetryAt(err); at > *next {
				*next = at
			}
			return
		}
	} else if errors.As(err, &auth) {
		*status, *next = upstreamStatusReconnect, upstreamAccountIsolatedUntil
		return
	}
	*status = upstreamStatusError
	if *failures >= upstreamRecoveryFailureLimit {
		recheck := upstreamTransientFailureRecheck
		if forbidden {
			recheck = upstreamClientErrorRecheck
		}
		*next = now + int64(recheck/time.Second)
		if at := upstreamRetryAt(err); at > *next {
			*next = at
		}
		return
	}
	if *failures >= upstreamRecoveryProbeThreshold {
		retryAt := now + int64(upstreamRecoveryProbeDelays[*failures-upstreamRecoveryProbeThreshold]/time.Second)
		if *next < upstreamAccountIsolatedUntil && *next > retryAt {
			retryAt = *next
		}
		if at := upstreamRetryAt(err); at > retryAt {
			retryAt = at
		}
		*next = retryAt
	} else if *next == upstreamAccountIsolatedUntil {
		// Preserve temporary errors even when an older adapter wrapped them in
		// upstreamAuthError. HTTP status wins over the wrapper's broad label.
		*next = now + int64(upstreamUsageRetryDelay(upstreamUsageTransientRetryDelays[:], *failures)/time.Second)
		if at := upstreamRetryAt(err); at > *next {
			*next = at
		}
	}
	if forbidden && *next < now+int64(upstreamRecoveryProbeDelays[0]/time.Second) {
		*next = now + int64(upstreamRecoveryProbeDelays[0]/time.Second)
	}
}

func upstreamProbeBeforeRetry(ctx context.Context, m *Monitor, row ChannelUpstreamAccount, task, status string, failures int) error {
	if !upstreamNeedsRecoveryProbe(status, failures) {
		return nil
	}
	err := m.probeUpstreamRecovery(ctx, row, task)
	// An expired access token is not an upstream outage. Let the ordinary
	// bounded synchronization own refresh + durable credential persistence.
	// The incident counter is still reset only after a real successful commit.
	if errors.Is(err, errUpstreamProbeRefreshRequired) {
		return nil
	}
	return err
}

// upstreamPauseRetryAt reports outcomes that are not upstream failures: the
// local store was busy, or the host circuit is open after an earlier failure.
// The account keeps its last status and error and is simply retried later, so
// the console shows only problems that need attention.
func upstreamPauseRetryAt(err error, now int64) (int64, bool) {
	if isUpstreamUsageLocalStoreBusy(err) || isUpstreamLocalQueueTimeout(err) {
		return now + int64(upstreamUsageLocalStoreRetryDelays[0]/time.Second), true
	}
	var circuit *upstreamCircuitOpenError
	if errors.As(err, &circuit) {
		if circuit.RetryAt > now {
			return circuit.RetryAt, true
		}
		return now + 60, true
	}
	return 0, false
}

// upstreamSyncYieldReason names outcomes that postponed a sync without an
// upstream failure, for logs. They keep the account's status and failure count.
func upstreamSyncYieldReason(err error) (string, bool) {
	var circuit *upstreamCircuitOpenError
	var stored *upstreamStoredSyncError
	switch {
	case errors.As(err, &stored) && stored.yield != "":
		return stored.yield, true
	case isUpstreamLocalQueueTimeout(err):
		return "本地排队等待请求名额超时，未请求上游", true
	case isUpstreamUsageLocalStoreBusy(err):
		return "本地数据库繁忙", true
	case errors.As(err, &circuit):
		return "该上游主机处于熔断冷却期", true
	}
	return "", false
}

// releaseNonCredentialUpstreamIsolation reschedules tasks that an earlier
// policy paused indefinitely for a reason other than a credential failure
// (a single 4xx or an exhausted incident budget). Under the current policy
// those are retried slowly; leaving them paused would keep a recovered
// supplier unsynchronised until someone noticed. Credential isolation
// (reconnect) is untouched. Only scheduling columns change; facts, cursors,
// errors and failure counts are kept, so the next failure resumes the slow
// recheck instead of a fresh burst.
func (m *Monitor) releaseNonCredentialUpstreamIsolation(ctx context.Context, now int64) (int64, error) {
	if m == nil || m.storeDB == nil {
		return 0, nil
	}
	isolated := upstreamAccountIsolatedUntil
	type release struct {
		model  any
		column string
		where  string
		args   []any
	}
	releases := []release{
		{&ChannelUpstreamAccount{}, "next_sync_at", "status = ? AND next_sync_at = ?", []any{upstreamStatusError, isolated}},
		{&ChannelUpstreamAccount{}, "usage_next_sync_at", "usage_status = ? AND usage_next_sync_at = ?", []any{upstreamStatusError, isolated}},
		{&ChannelUpstreamAccount{}, "usage_backfill_next_sync_at", "usage_status <> ? AND usage_backfill_done = ? AND usage_backfill_next_sync_at = ?", []any{upstreamStatusReconnect, false, isolated}},
		// Older images recorded a NewAPI tail round that ran out of its request
		// budget on one dense hour as a failure with exponential backoff. The live
		// lane now resumes such hours with a durable page cursor, so reschedule
		// those accounts now instead of waiting hours for the inherited backoff.
		{&ChannelUpstreamAccount{}, "usage_next_sync_at", "provider = ? AND usage_status = ? AND usage_last_error = ? AND usage_next_sync_at > ? AND usage_next_sync_at <> ?", []any{upstreamProviderNewAPI, upstreamStatusError, legacyNewAPIBackfillBudgetError(), now, isolated}},
		{&UpstreamErrorLogSyncState{}, "next_sync_at", "status = ? AND next_sync_at = ?", []any{upstreamStatusError, isolated}},
		{&UpstreamFundSyncState{}, "next_sync_at", "status = ? AND next_sync_at = ?", []any{upstreamStatusError, isolated}},
		{&AICodeWithKeySyncState{}, "next_sync_at", "status = ? AND next_sync_at = ?", []any{upstreamStatusError, isolated}},
		{&AICodeWithKeySyncState{}, "backfill_next_sync_at", "status <> ? AND backfill_done = ? AND backfill_next_sync_at = ?", []any{upstreamStatusReconnect, false, isolated}},
	}
	var released int64
	err := m.queuedWriteStore().WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for _, r := range releases {
			if !tx.Migrator().HasTable(r.model) {
				continue
			}
			result := tx.Model(r.model).Where(r.where, r.args...).UpdateColumn(r.column, now)
			if result.Error != nil {
				return result.Error
			}
			released += result.RowsAffected
		}
		return nil
	})
	return released, err
}
