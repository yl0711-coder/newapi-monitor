package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

func tokenForceHistoryFakeSync(t *testing.T, requestsPerWindow int, failAt map[int64]error) (func(context.Context, int64, int64, *upstreamUsageRequestPacer) (upstreamUsageResult, error), *[]int64) {
	t.Helper()
	var windows []int64
	return func(ctx context.Context, from, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
		windows = append(windows, from)
		for i := 0; i < requestsPerWindow; i++ {
			if err := pacer.beforeRequest(ctx); err != nil {
				return upstreamUsageResult{}, err
			}
		}
		if err := failAt[from]; err != nil {
			return upstreamUsageResult{}, err
		}
		return upstreamUsageResult{Hours: []ChannelUpstreamUsageHour{{
			Domain: "hainahn.test", HourTs: from, BucketSeconds: 3600, Requests: 1, CostUSD: 1, Provider: upstreamProviderTokenForce, UnitPerUSD: tokenForceLedgerUnit,
		}}, DataUntil: to}, nil
	}, &windows
}

func noTokenForceHistorySecrets() []string { return nil }

func tokenForceHistoryRow(domain string, cursor int64) ChannelUpstreamAccount {
	return ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderTokenForce, UsageBackfillCursor: cursor, UsageBackfillConsecutiveFails: 2, UsageBackfillLastError: "old"}
}

func countTokenForceHistoryHours(t *testing.T, m *Monitor, domain string) int64 {
	t.Helper()
	var n int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", domain).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	return n
}

// One round imports as many closed hours as the request budget allows and
// resumes shortly, instead of one hour per half-hour round.
func TestTokenForceHistoryImportsManyHoursPerRound(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 11, 12, 7, 30, 0, 0, cstLocation).Unix()
	from := time.Date(2026, 11, 6, 21, 0, 0, 0, cstLocation).Unix()
	row := tokenForceHistoryRow("hainahn.test", from)
	plan := upstreamUsageSyncPlan{backfillFrom: from, backfillTo: from + tokenForceBackfillWindowSeconds}
	syncUsage, windows := tokenForceHistoryFakeSync(t, 2, nil)
	retryAt := int64(0)
	pacer := newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0)
	if err := m.syncTokenForceHistoryWindows(context.Background(), &row, plan, now, pacer, syncUsage, &retryAt, noTokenForceHistorySecrets); err != nil {
		t.Fatal(err)
	}
	// 20 requests, 2 per hour, a new hour needs at least 3 left: 9 hours.
	if len(*windows) != 9 || row.UsageBackfillCursor != from+9*3600 || row.UsageBackfillDone {
		t.Fatalf("windows=%d cursor=%s done=%v", len(*windows), time.Unix(row.UsageBackfillCursor, 0).In(cstLocation), row.UsageBackfillDone)
	}
	if n := countTokenForceHistoryHours(t, m, row.Domain); n != 9 {
		t.Fatalf("committed hours=%d", n)
	}
	if row.UsageBackfillNextSyncAt != now+15 || row.UsageBackfillConsecutiveFails != 0 || row.UsageBackfillLastError != "" ||
		row.UsageBackfillLastSuccessAt != now || !strings.Contains(row.UsageBackfillProgress, "本轮补入 9 小时") {
		t.Fatalf("yield state=%+v", row)
	}
}

// Running out of budget inside a window after progress keeps every committed
// hour and is a healthy yield, not a failure.
func TestTokenForceHistoryBudgetStopMidWindowIsYield(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 11, 12, 7, 30, 0, 0, cstLocation).Unix()
	from := time.Date(2026, 11, 10, 0, 0, 0, 0, cstLocation).Unix()
	row := tokenForceHistoryRow("hainahn.test", from)
	plan := upstreamUsageSyncPlan{backfillFrom: from, backfillTo: from + 3600}
	syncUsage, windows := tokenForceHistoryFakeSync(t, 4, nil)
	retryAt := int64(0)
	pacer := newUpstreamUsageRequestPacer(11, 0)
	if err := m.syncTokenForceHistoryWindows(context.Background(), &row, plan, now, pacer, syncUsage, &retryAt, noTokenForceHistorySecrets); err != nil {
		t.Fatal(err)
	}
	// 4 + 4 requests commit two hours; 3 remain, so a third window starts and
	// runs out inside it.
	if len(*windows) != 3 || row.UsageBackfillCursor != from+2*3600 || row.UsageBackfillNextSyncAt != now+15 || row.UsageBackfillConsecutiveFails != 0 {
		t.Fatalf("windows=%d row=%+v", len(*windows), row)
	}
	if n := countTokenForceHistoryHours(t, m, row.Domain); n != 2 {
		t.Fatalf("committed hours=%d", n)
	}
}

func TestTokenForceHistoryFailureKeepsCommittedProgress(t *testing.T) {
	now := time.Date(2026, 11, 12, 7, 30, 0, 0, cstLocation).Unix()
	from := time.Date(2026, 11, 10, 0, 0, 0, 0, cstLocation).Unix()
	upstreamErr := errors.New("TokenForce 使用明细首页数量异常")
	for _, tc := range []struct {
		name       string
		failAt     int64
		wantCursor int64
	}{
		{"first window", from, from},
		{"after progress", from + 2*3600, from + 2*3600},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := newChannelUpstreamTestMonitor(t)
			row := tokenForceHistoryRow("hainahn.test", from)
			plan := upstreamUsageSyncPlan{backfillFrom: from, backfillTo: from + 3600}
			syncUsage, _ := tokenForceHistoryFakeSync(t, 2, map[int64]error{tc.failAt: upstreamErr})
			retryAt := int64(0)
			err := m.syncTokenForceHistoryWindows(context.Background(), &row, plan, now, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0), syncUsage, &retryAt, noTokenForceHistorySecrets)
			if !errors.Is(err, upstreamErr) || row.UsageBackfillCursor != tc.wantCursor || row.UsageBackfillDone ||
				row.UsageBackfillConsecutiveFails != 3 || row.UsageBackfillLastError == "" || row.UsageBackfillNextSyncAt <= now+15 {
				t.Fatalf("err=%v row=%+v", err, row)
			}
			if n := countTokenForceHistoryHours(t, m, row.Domain); n != (tc.wantCursor-from)/3600 {
				t.Fatalf("committed hours=%d", n)
			}
		})
	}
}

func TestTokenForceHistoryFinishesAtToday(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 11, 12, 7, 30, 0, 0, cstLocation).Unix()
	from := cstDayStart(now) - 3*3600
	row := tokenForceHistoryRow("hainahn.test", from)
	plan := upstreamUsageSyncPlan{backfillFrom: from, backfillTo: from + 3600}
	syncUsage, windows := tokenForceHistoryFakeSync(t, 2, nil)
	retryAt := int64(0)
	if err := m.syncTokenForceHistoryWindows(context.Background(), &row, plan, now, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0), syncUsage, &retryAt, noTokenForceHistorySecrets); err != nil {
		t.Fatal(err)
	}
	if len(*windows) != 3 || !row.UsageBackfillDone || row.UsageBackfillCursor != cstDayStart(now) || row.UsageBackfillNextSyncAt != 0 || row.UsageBackfillProgress != "" {
		t.Fatalf("windows=%d row=%+v", len(*windows), row)
	}
}

// A credential rotated inside the loop must still be redacted from a later
// window's stored error: the redaction list is read when the error is stored.
func TestTokenForceHistoryRedactsCredentialRotatedMidRun(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 11, 12, 7, 30, 0, 0, cstLocation).Unix()
	from := time.Date(2026, 11, 10, 0, 0, 0, 0, cstLocation).Unix()
	row := tokenForceHistoryRow("hainahn.test", from)
	plan := upstreamUsageSyncPlan{backfillFrom: from, backfillTo: from + 3600}
	secrets := []string{"old-access-token-value"}
	const rotated = "rotated-access-token-value"
	syncUsage := func(ctx context.Context, cursor, to int64, pacer *upstreamUsageRequestPacer) (upstreamUsageResult, error) {
		if err := pacer.beforeRequest(ctx); err != nil {
			return upstreamUsageResult{}, err
		}
		if cursor == from {
			secrets = []string{rotated}
			return upstreamUsageResult{Hours: []ChannelUpstreamUsageHour{{Domain: "hainahn.test", HourTs: cursor, BucketSeconds: 3600, Provider: upstreamProviderTokenForce, UnitPerUSD: tokenForceLedgerUnit}}, DataUntil: to}, nil
		}
		return upstreamUsageResult{}, errors.New("upstream echoed Bearer " + rotated)
	}
	retryAt := int64(0)
	err := m.syncTokenForceHistoryWindows(context.Background(), &row, plan, now, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0), syncUsage, &retryAt, func() []string { return secrets })
	if err == nil || row.UsageBackfillCursor != from+3600 || row.UsageBackfillLastError == "" || strings.Contains(row.UsageBackfillLastError, rotated) {
		t.Fatalf("err=%v cursor=%d stored=%q", err, row.UsageBackfillCursor, row.UsageBackfillLastError)
	}
}

// The history operation deadline after committed progress is a yield, not a
// failure; the failing window is retried on the next run.
func TestTokenForceHistoryDeadlineAfterProgressIsYield(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Date(2026, 11, 12, 7, 30, 0, 0, cstLocation).Unix()
	from := time.Date(2026, 11, 10, 0, 0, 0, 0, cstLocation).Unix()
	row := tokenForceHistoryRow("hainahn.test", from)
	plan := upstreamUsageSyncPlan{backfillFrom: from, backfillTo: from + 3600}
	syncUsage, _ := tokenForceHistoryFakeSync(t, 1, map[int64]error{from + 2*3600: context.DeadlineExceeded})
	retryAt := int64(0)
	if err := m.syncTokenForceHistoryWindows(context.Background(), &row, plan, now, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0), syncUsage, &retryAt, noTokenForceHistorySecrets); err != nil {
		t.Fatal(err)
	}
	if row.UsageBackfillCursor != from+2*3600 || row.UsageBackfillNextSyncAt != now+15 || row.UsageBackfillConsecutiveFails != 0 || row.UsageBackfillLastError != "" {
		t.Fatalf("row=%+v", row)
	}
}
