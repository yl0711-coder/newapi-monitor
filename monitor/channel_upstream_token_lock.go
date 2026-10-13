package monitor

import (
	"context"
	"time"
)

// Keep the existing attribution lock, including mutual exclusion with legacy
// binding writers, but never outlive the caller's budget while waiting for it.
// Do not spawn a goroutine to call Lock: cancellation would leak a waiter that
// can later acquire the mutex with nobody left to release it.
func (m *Monitor) lockUpstreamTokenWrite(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if m.channelCostBindingMu.TryLock() {
		if err := ctx.Err(); err != nil {
			m.channelCostBindingMu.Unlock()
			return err
		}
		return nil
	}
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			if err := ctx.Err(); err != nil {
				return err
			}
			if m.channelCostBindingMu.TryLock() {
				if err := ctx.Err(); err != nil {
					m.channelCostBindingMu.Unlock()
					return err
				}
				return nil
			}
		}
	}
}
