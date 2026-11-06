package monthlydraft

import (
	"context"
	"testing"
	"time"
)

// End-to-end completion is not a wall-clock SLA. Keep one bounded test wait,
// independent of production SQL, capture, retry and shutdown limits. Do not use
// this helper for assertions that specifically test a timeout or cancellation.
func waitMonthlyAcceptance(t *testing.T, ready func() bool) bool {
	t.Helper()
	return waitMonthlyAcceptanceContext(t.Context(), 90*time.Second, ready)
}

func waitMonthlyAcceptanceContext(ctx context.Context, budget time.Duration, ready func() bool) bool {
	if ctx.Err() != nil {
		return false
	}
	timer := time.NewTimer(budget)
	defer timer.Stop()
	tick := time.NewTicker(20 * time.Millisecond)
	defer tick.Stop()
	for {
		if ready() {
			return true
		}
		select {
		case <-ctx.Done():
			return false
		case <-timer.C:
			return false
		case <-tick.C:
		}
	}
}

func TestMonthlyAcceptanceWaitRequiresCompletionAndHonorsCancellation(t *testing.T) {
	if !waitMonthlyAcceptanceContext(t.Context(), time.Second, func() bool { return true }) {
		t.Fatal("completed condition was not reused")
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	if waitMonthlyAcceptanceContext(ctx, time.Second, func() bool { t.Fatal("cancelled wait invoked a new check"); return true }) {
		t.Fatal("cancelled wait reported completion")
	}
	if waitMonthlyAcceptanceContext(t.Context(), time.Millisecond, func() bool { return false }) {
		t.Fatal("timeout was treated as completion")
	}
}
