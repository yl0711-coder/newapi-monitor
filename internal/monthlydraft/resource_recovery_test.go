package monthlydraft

import (
	"context"
	"errors"
	"syscall"
	"testing"
	"time"
)

func TestMonthlyResourceWaitKeepsIntegrityAndCapacityBudgets(t *testing.T) {
	for _, tc := range []struct {
		cause error
		wait  bool
	}{
		{syscall.ENOSPC, true}, {syscall.EDQUOT, true}, {ErrGenerationBusy, true}, {context.DeadlineExceeded, true},
		{ErrGenerationIntegrity, false}, {ErrGenerationCapacity, false}, {ErrGenerationSelectionConflict, false},
		{errors.Join(ErrGenerationIntegrity, syscall.ENOSPC), false}, {syscall.EIO, false}, {syscall.EACCES, false},
		{errors.Join(ErrGenerationCapacity, syscall.ENOSPC), false},
		{errors.Join(ErrGenerationSelectionConflict, syscall.ENOSPC), false},
		{errors.Join(ErrGenerationChanged, context.DeadlineExceeded), false},
	} {
		if got := monthlyResourceWait(tc.cause); got != tc.wait {
			t.Fatalf("%v: wait=%v", tc.cause, got)
		}
	}
	for _, n := range []int{-1, 0, 1, 3, 8, 1000000} {
		if delay := monthlyResourceRetryDelay(n); delay < 30*time.Second || delay > 5*time.Minute {
			t.Fatalf("unbounded cooldown: %d %v", n, delay)
		}
	}
}
