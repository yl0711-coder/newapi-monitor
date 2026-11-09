package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

type upstreamWriteProbe struct {
	ID    int64 `gorm:"primaryKey"`
	Value int64
}

// A deferred read-then-write transaction is refused with SQLITE_BUSY at once
// when another connection commits in between (it does not honor busy_timeout);
// the upstream writer must instead wait for the writer slot and succeed.
func TestQueuedWriteStoreWaitsForConcurrentWriter(t *testing.T) {
	m := &Monitor{cfg: Settings{LocalSnapshotOnly: true}}
	if err := m.openStore(filepath.Join(t.TempDir(), "monitor.db")); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { m.Close() })
	if err := m.storeDB.AutoMigrate(&upstreamWriteProbe{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&upstreamWriteProbe{ID: 1}).Error; err != nil {
		t.Fatal(err)
	}
	w := m.queuedWriteStore()
	if w == m.storeDB {
		t.Fatal("file store must use a dedicated upstream writer")
	}
	if again := m.queuedWriteStore(); again != w {
		t.Fatal("upstream writer must be reused")
	}
	ctx := context.Background()
	for round := 0; round < 20; round++ {
		var wg sync.WaitGroup
		errs := make(chan error, 6)
		wg.Add(2)
		go func() { // another writer commits repeatedly through the main pool
			defer wg.Done()
			for i := 0; i < 5; i++ {
				errs <- m.storeDB.Model(&upstreamWriteProbe{}).Where("id = 1").Update("value", gorm.Expr("value + 1")).Error
				time.Sleep(time.Millisecond)
			}
		}()
		go func() { // read first, then write: the shape of the usage-sync transactions
			defer wg.Done()
			errs <- w.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
				var p upstreamWriteProbe
				if err := tx.First(&p, 1).Error; err != nil {
					return err
				}
				time.Sleep(5 * time.Millisecond)
				return tx.Model(&upstreamWriteProbe{}).Where("id = 1").Update("value", p.Value+100).Error
			})
		}()
		wg.Wait()
		close(errs)
		for err := range errs {
			if err != nil {
				t.Fatalf("round %d: %v", round, err)
			}
		}
	}
}

func TestQueuedWriteStoreKeepsInMemoryStore(t *testing.T) {
	m, _, _ := newPortalTestMonitor(t)
	t.Cleanup(m.Close)
	if !storeUsesFile(m.cfg.StorePath) && m.queuedWriteStore() != m.storeDB {
		t.Fatal("in-memory store must not open a second database")
	}
}

// A busy local store or an open host circuit is not an upstream failure: the
// account keeps its status, last error and failure count and is retried later.
func TestUpstreamPauseKeepsHealthyStatus(t *testing.T) {
	now := time.Date(2026, 11, 9, 12, 0, 0, 0, time.UTC).Unix()
	for _, err := range []error{
		errors.New("database is locked (5) (SQLITE_BUSY)"),
		&upstreamCircuitOpenError{RetryAt: now + 600},
	} {
		usage := ChannelUpstreamAccount{UsageStatus: upstreamStatusOK, UsageConsecutiveFails: 0}
		applyUpstreamUsageResult(&usage, upstreamUsageResult{}, err, now, Settings{})
		if usage.UsageStatus != upstreamStatusOK || usage.UsageLastError != "" || usage.UsageConsecutiveFails != 0 || usage.UsageNextSyncAt <= now {
			t.Fatalf("%v: usage %+v", err, usage)
		}
		balance := ChannelUpstreamAccount{Status: upstreamStatusOK}
		applyUpstreamSyncResult(&balance, upstreamBalanceResult{}, err, now, Settings{})
		if balance.Status != upstreamStatusOK || balance.LastError != "" || balance.ConsecutiveFails != 0 || balance.NextSyncAt <= now {
			t.Fatalf("%v: balance %+v", err, balance)
		}
	}
	var circuitAt int64
	usage := ChannelUpstreamAccount{}
	applyUpstreamUsageResult(&usage, upstreamUsageResult{}, &upstreamCircuitOpenError{RetryAt: now + 600}, now, Settings{})
	circuitAt = usage.UsageNextSyncAt
	if circuitAt != now+600 {
		t.Fatalf("circuit retry %d, want %d", circuitAt, now+600)
	}
	// A real upstream failure still counts.
	failed := ChannelUpstreamAccount{UsageStatus: upstreamStatusOK}
	applyUpstreamUsageResult(&failed, upstreamUsageResult{}, context.DeadlineExceeded, now, Settings{})
	if failed.UsageStatus != upstreamStatusError || failed.UsageConsecutiveFails != 1 {
		t.Fatalf("real failure not recorded: %+v", failed)
	}
}

func TestUpstreamUsageRequestTimeout(t *testing.T) {
	for _, c := range []struct {
		usage, sync int
		want        time.Duration
	}{{0, 0, 45 * time.Second}, {10, 15, 15 * time.Second}, {300, 15, 120 * time.Second}, {60, 15, 60 * time.Second}} {
		got := upstreamUsageRequestTimeout(Settings{UpstreamUsageTimeoutSec: c.usage, UpstreamSyncTimeoutSec: c.sync})
		if got != c.want {
			t.Fatalf("%+v: got %v", c, got)
		}
	}
	// A tail run must be able to wait for at least two slow usage pages.
	if op := upstreamUsageOperationTimeout(Settings{}); op < 2*upstreamUsageRequestTimeout(Settings{}) {
		t.Fatalf("tail operation timeout %v shorter than two usage requests", op)
	}
}
