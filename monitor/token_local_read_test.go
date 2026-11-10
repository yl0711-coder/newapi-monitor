package monitor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenBackgroundReadsReserveExistingPoolForHealth(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	if err := m.tokenStore().Create(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: now - 600, ScanThrough: now - 600}).Error; err != nil {
		t.Fatal(err)
	}
	if err := initTokenMeteringHealthCounts(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	pool, err := m.tokenReadStore().DB()
	if err != nil || pool.Stats().MaxOpenConnections != 2 {
		t.Fatal("test requires the existing two-reader limit", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	held, release, done := make(chan struct{}), make(chan struct{}), make(chan error, 1)
	go func() {
		done <- m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
			return reader.Connection(func(bound *gorm.DB) error {
				var one int
				if err := bound.Raw("SELECT 1").Scan(&one).Error; err != nil {
					return err
				}
				close(held)
				select {
				case <-release:
				case <-ctx.Done():
				}
				return nil
			})
		})
	}()
	defer func() {
		close(release)
		if err := <-done; err != nil {
			t.Error(err)
		}
	}()
	select {
	case <-held:
	case <-time.After(time.Second):
		t.Fatal("background read did not hold its connection")
	}
	var entered atomic.Bool
	queued, stop := context.WithTimeout(ctx, 150*time.Millisecond)
	defer stop()
	second := make(chan error, 1)
	go func() {
		second <- m.tokenBackgroundRead(queued, func(reader *gorm.DB) error {
			entered.Store(true)
			return reader.Connection(func(*gorm.DB) error { <-queued.Done(); return queued.Err() })
		})
	}()
	// A queued maintenance reader must not take the remaining pool connection.
	// Exercise the real health path while the first background query is held.
	healthCtx, healthCancel := context.WithTimeout(ctx, 100*time.Millisecond)
	defer healthCancel()
	health, err := m.tokenMeteringHealth(healthCtx, "", now)
	if err != nil || !health.Available || health.Facts != 0 {
		t.Fatal("maintenance starved current health", health.HealthCountsState, err)
	}
	if err := <-second; !errors.Is(err, context.DeadlineExceeded) || entered.Load() {
		t.Fatal("cancelled background waiter ran SQL", entered.Load(), err)
	}
}

func TestTokenBackgroundReadCancellationReleasesFollowingTurn(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := m.tokenBackgroundRead(ctx, func(*gorm.DB) error {
		t.Fatal("cancelled reader ran its callback")
		return nil
	}); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	want := errors.New("bounded read failed")
	if err := m.tokenBackgroundRead(context.Background(), func(*gorm.DB) error { return want }); !errors.Is(err, want) {
		t.Fatal(err)
	}
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
		var one int
		return reader.Raw("SELECT 1").Scan(&one).Error
	}); err != nil {
		t.Fatal("failed reader kept the following turn blocked", err)
	}
}
