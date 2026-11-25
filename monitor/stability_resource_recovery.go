package monitor

import (
	"context"
	"errors"
)

// Only use at LOCAL database operations. A source-query timeout has its own
// bounded fallback policy and must not be mislabeled a local resource wait.
func stabilityLocalFailureStatus(err error) string {
	if localResourceTemporarilyUnavailable(err) || errors.Is(err, context.DeadlineExceeded) {
		// Reuse the durable queued state, which older releases can resume as
		// well. The saved last_error explains why this turn yielded.
		return "queued"
	}
	return "paused"
}
