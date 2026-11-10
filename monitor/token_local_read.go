package monitor

import (
	"context"

	"gorm.io/gorm"
)

// Long detection, association and counter-audit reads used to occupy both WAL
// readers at once. Keep their local read phases to one reader in the existing
// pool. Foreground/collection probes and independently bounded cleanup reads
// retain their own opportunity. Never retain this permit while reading another
// store or waiting for a writer; those waits have separate budgets.
func (m *Monitor) tokenBackgroundRead(ctx context.Context, fn func(*gorm.DB) error) error {
	m.tokenBackgroundReadOnce.Do(func() { m.tokenBackgroundReadGate = make(chan struct{}, m.cfg.tokenDetectionWorkers()) })
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case m.tokenBackgroundReadGate <- struct{}{}:
		defer func() { <-m.tokenBackgroundReadGate }()
	case <-ctx.Done():
		return ctx.Err()
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	return fn(m.tokenReadStore().WithContext(ctx))
}
