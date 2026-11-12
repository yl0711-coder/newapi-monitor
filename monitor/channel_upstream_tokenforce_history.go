package monitor

import (
	"context"
	"errors"
	"fmt"
	"time"
)

// tokenForceHistoryMinRequestsPerWindow is the request budget a one-hour
// TokenForce history window needs at minimum: the main read, the late-finish
// slice and one widening read. A further window starts only when this much
// budget is left, so a round never opens a window it cannot normally finish.
const tokenForceHistoryMinRequestsPerWindow = 3

// syncTokenForceHistoryWindows imports closed one-hour history windows in
// order until the run's request budget or operation deadline is used up.
// A typical hour needs two or three requests, so importing only one hour per
// history round (every half hour) caught up far slower than the budget
// allows (hainahn.com, 2026-11). Every window is committed before the cursor
// moves past it; a budget or deadline stop after progress is a healthy yield
// that resumes shortly, and any other error keeps the committed progress and
// enters the normal history backoff.
//
// secrets is read at the moment an error is stored: syncUsage may rotate the
// TokenForce credential inside the loop, and the stored error must be
// redacted against the newest tokens, not the ones from before the run.
func (m *Monitor) syncTokenForceHistoryWindows(ctx context.Context, row *ChannelUpstreamAccount, plan upstreamUsageSyncPlan, now int64, pacer *upstreamUsageRequestPacer,
	syncUsage func(context.Context, int64, int64, *upstreamUsageRequestPacer) (upstreamUsageResult, error), operationRetryAt *int64, secrets func() []string) error {
	today := cstDayStart(now)
	cursor := plan.backfillFrom
	completed := 0
	for cursor < today {
		if completed > 0 && pacer != nil && pacer.maxRequests > 0 && pacer.maxRequests-pacer.calls < tokenForceHistoryMinRequestsPerWindow {
			break
		}
		windowTo := cursor + tokenForceBackfillWindowSeconds
		history, err := syncUsage(ctx, cursor, windowTo, pacer)
		if err == nil {
			err = m.persistUpstreamUsageWindow(ctx, row.Domain, cursor, windowTo, history.Hours, now)
		}
		if errors.Is(err, context.Canceled) {
			return err
		}
		if err != nil {
			if completed > 0 && (upstreamUsageRunBudgetWasExhausted(err) || errors.Is(err, context.DeadlineExceeded)) {
				break
			}
			if retryAt := upstreamRetryAt(err); retryAt > *operationRetryAt {
				*operationRetryAt = retryAt
			}
			applyUpstreamUsageBackfillResult(row, err, now, m.cfg, secrets()...)
			return err
		}
		completed++
		cursor = windowTo
		row.UsageBackfillCursor = cursor
	}
	row.UsageBackfillDone = cursor >= today
	if row.UsageBackfillDone {
		applyUpstreamUsageBackfillResult(row, nil, now, m.cfg)
		row.UsageBackfillNextSyncAt = 0
		return nil
	}
	row.UsageBackfillLastSuccessAt = now
	applyUpstreamUsageBackfillYield(row, fmt.Sprintf("已补到 %s，本轮补入 %d 小时，请求预算已用完，稍后继续",
		time.Unix(cursor, 0).In(cstLocation).Format("2006-01-02 15:04"), completed), now)
	return nil
}
