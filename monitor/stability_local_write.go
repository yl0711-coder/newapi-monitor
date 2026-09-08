package monitor

import (
	"context"
	"errors"
	"time"
)

const (
	stabilityLocalWriteBudget   = 12 * time.Second
	stabilityLocalWriteAttempts = 16
	stabilityLocalWriteDelay    = 50 * time.Millisecond
	stabilityLocalWriteMaxDelay = time.Second
)

// Retry only local, rollback-safe operations. The caller retains fetched
// source facts; neither a production query nor a job cursor is replayed here.
// SQLite's extended BUSY codes retain primary code 5 in their low byte.
func stabilityLocalWriteBusy(err error) bool {
	var coded interface{ Code() int }
	return errors.As(err, &coded) && coded.Code()&255 == 5
}

func retryStabilityLocalWrite(ctx context.Context, operation func(context.Context) error) error {
	return retryStabilityLocalWriteWithWait(ctx, operation, waitStabilityLocalRetry)
}

// The wait dependency is per call, so policy tests need neither real sleeps nor
// shared clock overrides. Both production and tests use the same bounded loop.
func retryStabilityLocalWriteWithWait(ctx context.Context, operation func(context.Context) error, wait func(context.Context, time.Duration) error) error {
	ctx, cancel := context.WithTimeout(ctx, stabilityLocalWriteBudget)
	defer cancel()
	delay := stabilityLocalWriteDelay
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation(ctx)
		if err == nil || !stabilityLocalWriteBusy(err) || attempt+1 >= stabilityLocalWriteAttempts {
			return err
		}
		// A WAL read-to-write upgrade can return BUSY immediately, without
		// honoring busy_timeout. Four such attempts exhausted the old policy
		// in only 350ms. Back off within the same total budget; never refetch
		// source data or wait indefinitely for a competing writer.
		if err := wait(ctx, delay); err != nil {
			return err
		}
		delay = min(delay*2, stabilityLocalWriteMaxDelay)
	}
}

func waitStabilityLocalRetry(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
