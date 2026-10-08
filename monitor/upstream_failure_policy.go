package monitor

import (
	"context"
	"errors"
	"net/http"
	"time"
)

// Existing per-task counters and schedules persist the incident budget. No
// separate in-memory retry loop can reset it on restart. After the initial
// attempt and five retries, only three scheduled probe rounds are permitted.
const upstreamRecoveryProbeThreshold = 6
const upstreamRecoveryFailureLimit = 9

var upstreamRecoveryProbeDelays = [...]time.Duration{30 * time.Minute, 2 * time.Hour, 6 * time.Hour}

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
	if isUpstreamUsageLocalStoreBusy(err) || errors.As(err, &circuit) {
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
			*status, *next = upstreamStatusError, upstreamAccountIsolatedUntil
			return
		}
	} else if errors.As(err, &auth) {
		*status, *next = upstreamStatusReconnect, upstreamAccountIsolatedUntil
		return
	}
	*status = upstreamStatusError
	if *failures >= upstreamRecoveryFailureLimit {
		*next = upstreamAccountIsolatedUntil
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
