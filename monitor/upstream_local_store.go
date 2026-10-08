package monitor

import (
	"context"
	"time"
)

// Shared by verified, idempotent local writes only. Never include an upstream
// HTTP request or credential refresh in operation. Each attempt must own and
// finish its transaction before returning (and before waiting).
func retryUpstreamLocalStore(ctx context.Context, operation func() error) error {
	delays := [...]time.Duration{50 * time.Millisecond, 150 * time.Millisecond, 500 * time.Millisecond}
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation()
		if err == nil || !isUpstreamUsageLocalStoreBusy(err) || attempt >= len(delays) {
			return err
		}
		timer := time.NewTimer(delays[attempt])
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
