package monitor

import (
	"context"
	"errors"
	"net/http"
	"testing"
	"time"
)

func TestUpstreamFailurePolicyFiniteBudget(t *testing.T) {
	now := int64(1_800_000_000)
	row := ChannelUpstreamAccount{Domain: "fixture.example", BalanceKnown: true, BalanceUSD: 71, LastSuccessAt: now - 500}
	for attempt := 1; attempt <= upstreamRecoveryFailureLimit; attempt++ {
		applyUpstreamSyncResult(&row, upstreamBalanceResult{}, &upstreamHTTPError{Status: 503}, now, Settings{})
		if row.ConsecutiveFails != attempt {
			t.Fatalf("failures=%d want %d", row.ConsecutiveFails, attempt)
		}
		if row.BalanceUSD != 71 || row.LastSuccessAt != 1_800_000_000-500 {
			t.Fatal("failure changed published balance")
		}
		if attempt < upstreamRecoveryProbeThreshold {
			if upstreamNeedsRecoveryProbe(row.Status, row.ConsecutiveFails) {
				t.Fatal("probe too early")
			}
		} else if attempt < upstreamRecoveryFailureLimit {
			if !upstreamNeedsRecoveryProbe(row.Status, row.ConsecutiveFails) {
				t.Fatal("bulk retry instead of probe")
			}
			want := now + int64(upstreamRecoveryProbeDelays[attempt-upstreamRecoveryProbeThreshold]/time.Second)
			if row.NextSyncAt < want || row.NextSyncAt == upstreamAccountIsolatedUntil {
				t.Fatalf("retryAt=%d below probe minimum %d or permanently paused too soon", row.NextSyncAt, want)
			}
		} else if row.NextSyncAt != upstreamAccountIsolatedUntil {
			t.Fatal("exhausted budget keeps retrying")
		}
		now = row.NextSyncAt
	}
}

func TestUpstreamFailurePolicyPermissionAndLocalIsolation(t *testing.T) {
	now := int64(1_800_000_000)
	row := ChannelUpstreamAccount{UsageStatus: upstreamStatusOK, UsageNextSyncAt: now + 600, UsageDataUntil: now - 20, UsageBackfillCursor: now - 86400}
	applyUpstreamUsageBackfillResult(&row, &upstreamAuthError{err: &upstreamHTTPError{Status: http.StatusForbidden}}, now, Settings{})
	if row.UsageStatus != upstreamStatusOK || row.UsageNextSyncAt != now+600 || row.UsageDataUntil != now-20 || row.UsageBackfillCursor != now-86400 {
		t.Fatal("history-only 403 affected live consumption or cursor")
	}
	if row.UsageBackfillConsecutiveFails != 1 || row.UsageBackfillNextSyncAt < now+1800 || row.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil {
		t.Fatal("403 lost the actual failure count or bounded cooldown")
	}
	old := row
	applyUpstreamUsageResult(&row, upstreamUsageResult{}, context.Canceled, now, Settings{})
	if row != old {
		t.Fatal("cancellation changed upstream state")
	}
	applyUpstreamUsageResult(&row, upstreamUsageResult{}, errors.New("database is locked"), now, Settings{})
	if row.UsageConsecutiveFails != old.UsageConsecutiveFails {
		t.Fatal("SQLite busy consumed upstream retry budget")
	}
}

func TestUpstreamFailurePolicyRetryAfterIsLowerBound(t *testing.T) {
	now := int64(1_800_000_000)
	for _, failures := range []int{1, 6, 7, 8} {
		next, status := now+10, upstreamStatusError
		count := failures
		boundUpstreamFailure(&upstreamHTTPError{Status: 429, RetryAt: now + 86400}, now, &count, &status, &next)
		// Ordinary lane calculation already applies Retry-After; probe stages
		// must also retain that lower bound themselves.
		if failures >= 6 && next < now+86400 {
			t.Fatal("probe shortened rate-limit cooldown")
		}
	}
}
