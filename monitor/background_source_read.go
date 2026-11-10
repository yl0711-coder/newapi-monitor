package monitor

import (
	"context"
	"fmt"
	"time"
)

// Waiting is not source work. Allow a bounded low-priority waiter to survive
// the normal bulk cooldown without increasing query concurrency, query time,
// start frequency or priority. Parent cancellation always wins. This is not a
// guarantee against starvation under continuously busy high-priority traffic.
const backgroundSourceLowWaitTimeout = time.Minute

type backgroundSourceReadTiming struct {
	Wait                                 time.Duration
	Query                                time.Duration
	Connection, Execute, Receive, Decode time.Duration
	Stage                                string
}

// The callback must consume and close its source rows before returning. Local
// persistence belongs AFTER this function, so SQLite contention never holds
// the single production-source slot. Separate budgets preserve query time
// without extending the lifetime supplied by the caller.
func (m *Monitor) withBackgroundSourceLowRead(ctx context.Context, waitBudget, queryBudget time.Duration, read func(context.Context) error) (timing backgroundSourceReadTiming, err error) {
	return m.withBackgroundSourceLowOperation(ctx, waitBudget, queryBudget, true, read)
}

// Probes use the same gate and budgets, but are not workload-read evidence.
// Otherwise a successful Ping could suppress future Pings indefinitely.
func (m *Monitor) withBackgroundSourceLowOperation(ctx context.Context, waitBudget, queryBudget time.Duration, workloadRead bool, operation func(context.Context) error) (timing backgroundSourceReadTiming, err error) {
	started := time.Now()
	waitCtx, cancelWait := context.WithTimeout(ctx, waitBudget)
	release, err := m.acquireBackgroundSourceLow(waitCtx)
	cancelWait()
	timing.Wait = time.Since(started)
	if err != nil {
		return timing, fmt.Errorf("source gate wait: %w", err)
	}
	defer release()
	queryCtx, cancelQuery := context.WithTimeout(ctx, queryBudget)
	defer cancelQuery()
	started = time.Now()
	err = operation(queryCtx)
	if err == nil {
		err = queryCtx.Err()
	}
	timing.Query = time.Since(started)
	if err != nil {
		return timing, fmt.Errorf("source query: %w", err)
	}
	if workloadRead {
		// Publish before releasing the slot so a queued health check observes
		// this read. No wait, timeout, warmup or local commit counts as a read.
		completed := time.Now()
		m.sourceReadLastSuccess.Store(&completed)
	}
	return timing, nil
}
