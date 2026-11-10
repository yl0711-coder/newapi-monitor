package monitor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenMaintenanceCleanupRestoresCadenceAfterCollectionYield(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	tokenCleanupAuthorizeFixture(t, m, now, now-4*86400)
	var reads atomic.Int32
	entered := make(chan struct{}, 10)
	if err := m.tokenReadStore().Callback().Query().Before("gorm:query").Register("observe_cleanup_cadence", func(tx *gorm.DB) {
		if tx.Statement.Context.Value(tokenWriteOperationKey{}) == "detection_cleanup" && tx.Statement.Table == "token_detection_cleanup_v1" {
			reads.Add(1)
			entered <- struct{}{}
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done, collection := make(chan struct{}), make(chan error, 1)
	m.tokenLocalWriteMu.Lock()
	go func() {
		err := m.tokenLocalWriteMu.lock(ctx, "collection")
		if err == nil {
			m.tokenLocalWriteMu.Unlock()
		}
		collection <- err
	}()
	awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
	go func() {
		defer close(done)
		m.runTokenMeteringCleanup(ctx, func() time.Time { return time.Unix(now, 0) }, 500*time.Millisecond)
	}()
	defer func() {
		cancel()
		<-done
		_ = m.tokenReadStore().Callback().Query().Remove("observe_cleanup_cadence")
	}()
	// Force the real scheduler to postpone at least one cleanup turn.
	time.Sleep(650 * time.Millisecond)
	blockedReads := reads.Load()
	m.tokenLocalWriteMu.Unlock()
	if err := <-collection; err != nil || blockedReads != 0 {
		t.Fatal("cleanup did not yield to the collector", blockedReads, err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("cleanup did not resume after collection")
	}
	// A 100ms ticker left over from the yield would immediately sweep again.
	select {
	case <-entered:
		t.Fatal("collection yield permanently accelerated cleanup")
	case <-time.After(250 * time.Millisecond):
	}
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("cleanup failed to retain its regular progress opportunity")
	}
}

func TestTokenMaintenanceDetectionContinuesDuringCleanupRead(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	tokenCleanupAuthorizeFixture(t, m, now, now-4*86400)
	m.cfg.TokenFactsEnabled = false // Keep only the real cleanup and detection loops active.
	m.cfg.TokenProcessingPollSeconds = 10
	entered, finished := make(chan struct{}), make(chan struct{})
	var once sync.Once
	if err := m.tokenReadStore().Callback().Query().Before("gorm:query").Register("slow_cleanup_read", func(tx *gorm.DB) {
		if tx.Statement.Context.Value(tokenWriteOperationKey{}) != "detection_cleanup" {
			return
		}
		once.Do(func() {
			close(entered)
			<-tx.Statement.Context.Done()
			close(finished)
		})
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer func() {
		cancel()
		select {
		case <-finished:
		case <-time.After(3 * time.Second):
			t.Error("cleanup did not honor its independent deadline")
		}
		m.tokenDetectionMu.Lock()
		m.tokenDetectionMu.Unlock()
		_ = m.tokenReadStore().Callback().Query().Remove("slow_cleanup_read")
	}()
	// The first detection round completes before cleanup's ten-second tick.
	// A new fact is already queued when cleanup's bounded read starts.
	m.startTokenMeteringMaintenanceAt(ctx, func() time.Time { return time.Unix(now, 0) })
	time.Sleep(100 * time.Millisecond)
	raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0}`)
	raw.ID, raw.CreatedAt = 7001, now-600
	fact, _ := parseTokenMetering(m.cfg, raw, now)
	fact.LinkState, fact.LinkRevision = "unique", 1
	if err := m.tokenStore().Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(12 * time.Second):
		t.Fatal("scheduled cleanup did not run")
	}
	deadline := time.Now().Add(time.Second)
	for {
		var count int64
		if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=?", fact.SourceEpoch, fact.LogID).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if count == 1 {
			select {
			case <-finished:
				t.Fatal("detection waited for cleanup's deadline")
			default:
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("cleanup consumed detection's execution opportunity")
		}
		time.Sleep(10 * time.Millisecond)
	}
}
