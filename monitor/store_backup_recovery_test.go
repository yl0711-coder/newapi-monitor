package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestBackupRetentionRecoveryYieldsToHeldLockAndCancellation(t *testing.T) {
	m := &Monitor{}
	m.storeBackupMu.Lock()
	defer m.storeBackupMu.Unlock()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.retryStoreBackupRetention(ctx, time.Now(), true, false); !errors.Is(err, context.Canceled) {
		t.Fatal("cancelled cleanup waited on snapshot", err)
	}
	if err := m.retryStoreBackupRetention(context.Background(), time.Now(), true, false); !errors.Is(err, errStoreBackupMaintenanceBusy) {
		t.Fatal("cleanup did not yield to snapshot", err)
	}
}

func TestBackupRetentionFailureKeepsCommittedSnapshotAndRetriesOnlyCleanup(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.StoreBackupDir = filepath.Join(t.TempDir(), "backups")
	if err := os.MkdirAll(m.cfg.StoreBackupDir, 0700); err != nil {
		t.Fatal(err)
	}
	// An unrelated damaged old manifest must block deletion, not invalidate
	// the new independently verified snapshot or trigger another VACUUM INTO.
	bad := filepath.Join(m.cfg.StoreBackupDir, storeBackupSetPrefix+"broken"+storeBackupSetSuffix)
	if err := os.WriteFile(bad, []byte("{"), 0600); err != nil {
		t.Fatal(err)
	}
	now := time.Now()
	manifest, err := m.createStoreBackupSet(context.Background(), now, true, false)
	if err == nil || !storeBackupWasCommitted(err) || manifest == "" {
		t.Fatalf("expected committed snapshot with pending cleanup: %s %v", manifest, err)
	}
	if !m.storeBackupSetVerified.Load() || m.storeBackupSetLastSuccess.Load() == 0 || m.storeBackupSetLastFailure.Load() != 0 {
		t.Fatal("retention error erased snapshot success")
	}
	if _, err := verifyStoreBackupSetManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	adopted, err := m.adoptStoreBackupSet(context.Background(), now, true, false, func(time.Time) bool { return true })
	if err != nil || adopted.Unix() != now.Unix() {
		t.Fatalf("old damaged manifest forced a fresh backup on restart: %v %v", adopted, err)
	}
	if err := m.retryStoreBackupRetention(context.Background(), now, true, false); err == nil {
		t.Fatal("unsafe old manifest did not block cleanup")
	}
	before, err := os.ReadDir(m.cfg.StoreBackupDir)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(bad); err != nil {
		t.Fatal(err)
	} // only this test's corrupt fixture
	if err := m.retryStoreBackupRetention(context.Background(), now, true, false); err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadDir(m.cfg.StoreBackupDir)
	if err != nil || len(after) != len(before)-1 {
		t.Fatalf("cleanup created another snapshot: %d -> %d (%v)", len(before), len(after), err)
	}
	if _, err := verifyStoreBackupSetManifest(context.Background(), manifest); err != nil {
		t.Fatal(err)
	}
	// Ordinary snapshot failures must never be mistaken for a committed pair.
	if storeBackupWasCommitted(context.DeadlineExceeded) {
		t.Fatal("pre-commit timeout treated as success")
	}
}
