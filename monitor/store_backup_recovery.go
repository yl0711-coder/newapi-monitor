package monitor

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"strings"
	"time"
)

func newestRuntimeBackupName(dir string) (string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return "", err
	}
	latest := ""
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, storeBackupSetPrefix) || !strings.HasSuffix(name, storeBackupSetSuffix) {
			continue
		}
		stamp := strings.TrimSuffix(strings.TrimPrefix(name, storeBackupSetPrefix), storeBackupSetSuffix)
		if _, err := time.Parse(backupSetStampLayout, stamp); err != nil {
			continue
		}
		if name > latest {
			latest = name
		}
	}
	return latest, nil
}

// This error is constructed only AFTER the backup pair and manifest have
// passed publication verification. It is not a failed snapshot or a license
// to delete an unverified recovery point.
type storeBackupRetentionPendingError struct{ cause error }

var errStoreBackupMaintenanceBusy = errors.New("backup maintenance already running")

func (e *storeBackupRetentionPendingError) Error() string {
	return "backup committed; retention pending: " + e.cause.Error()
}
func (e *storeBackupRetentionPendingError) Unwrap() error { return e.cause }
func storeBackupWasCommitted(err error) bool {
	var pending *storeBackupRetentionPendingError
	return errors.As(err, &pending)
}

// Retry immutable recovery-point maintenance only. Never create a new full
// snapshot, hold the publication barrier, or weaken the retention verifier.
func (m *Monitor) retryStoreBackupRetention(parent context.Context, now time.Time, mainEnabled, factsEnabled bool) error {
	if err := parent.Err(); err != nil {
		return err
	}
	// Cleanup is optional maintenance, not another waiter behind a slow
	// snapshot. Yield rather than extending shutdown or the recovery queue.
	if !m.storeBackupMu.TryLock() {
		return errStoreBackupMaintenanceBusy
	}
	defer m.storeBackupMu.Unlock()
	ctx, cancel := context.WithTimeout(parent, storeBackupTimeout)
	defer cancel()
	var paths []string
	if mainEnabled {
		paths = append(paths, m.cfg.StorePath)
	}
	if factsEnabled {
		paths = append(paths, m.cfg.UsageFactsStorePath)
	}
	err := pruneStoreBackupSetsWithPolicy(ctx, m.storeBackupDir(), m.storeBackupRetention(), m.storeBackupRetentionDays(), now, paths)
	if err != nil && parent.Err() == nil {
		slog.Warn("旧恢复点清理延期，保留现有备份", "err", err)
	}
	return err
}
