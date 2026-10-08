package monitor

import (
	"context"
	"time"
)

const (
	infraReadTimeout       = 5 * time.Second
	infraAggregateLockPoll = 10 * time.Millisecond
)

// Keep expensive local aggregates serialized without leaving a goroutine
// waiting on a mutex after its HTTP request has ended. Polling follows the
// existing sync-status lock pattern; it performs no SQL or external calls.
func (m *Monitor) lockInfraAggregate(ctx context.Context) error {
	var ticker *time.Ticker
	defer func() {
		if ticker != nil {
			ticker.Stop()
		}
	}()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if m.infraAggregateMu.TryLock() {
			if err := ctx.Err(); err != nil {
				m.infraAggregateMu.Unlock()
				return err
			}
			return nil
		}
		if ticker == nil {
			ticker = time.NewTicker(infraAggregateLockPoll)
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}
