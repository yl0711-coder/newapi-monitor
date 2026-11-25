package monthlydraft

import (
	"context"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/localresource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

// These operations revalidate their pinned input and durable journal on each
// turn. Waiting for local capacity never approves a publication or changes a
// selected version. Integrity/selection/uncertain-journal failures stay paused.
func monthlyResourceWait(err error) bool {
	if errors.Is(err, ErrGenerationIntegrity) || errors.Is(err, ErrGenerationSelectionConflict) || errors.Is(err, ErrGenerationChanged) || errors.Is(err, ErrGenerationCapacity) {
		return false
	}
	return localresource.Temporary(err) || errors.Is(err, ErrGenerationBusy) || errors.Is(err, monthlytasks.ErrBusy) || errors.Is(err, context.DeadlineExceeded)
}

func monthlyResourceRetryDelay(attempt int) time.Duration {
	n := min(max(attempt, 1), 8)
	return min(time.Duration(n*n)*30*time.Second, 5*time.Minute)
}
