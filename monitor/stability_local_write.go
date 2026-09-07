package monitor

import (
	"context"
	"errors"
	"time"
)

const stabilityLocalWriteBudget = 12 * time.Second
const stabilityLocalWriteAttempts = 4

// Retry only local, rollback-safe operations. The caller retains fetched
// source facts; neither a production query nor a job cursor is replayed here.
// SQLite's extended BUSY codes retain primary code 5 in their low byte.
func stabilityLocalWriteBusy(err error) bool {
	var coded interface{ Code() int }
	return errors.As(err, &coded) && coded.Code()&255 == 5
}

func retryStabilityLocalWrite(ctx context.Context, operation func(context.Context) error) error {
	ctx, cancel := context.WithTimeout(ctx, stabilityLocalWriteBudget)
	defer cancel()
	for attempt := 0; ; attempt++ {
		if err := ctx.Err(); err != nil {
			return err
		}
		err := operation(ctx)
		if err == nil || !stabilityLocalWriteBusy(err) || attempt+1 >= stabilityLocalWriteAttempts {
			return err
		}
		timer := time.NewTimer(time.Duration(1<<attempt) * 50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return ctx.Err()
		case <-timer.C:
		}
	}
}
