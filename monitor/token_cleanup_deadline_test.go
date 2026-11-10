package monitor

import (
	"context"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenCleanupYieldsCommittedPrefixBeforeAnotherWriterWait(t *testing.T) {
	for _, detection := range []bool{false, true} {
		name := "facts"
		if detection {
			name = "detection"
		}
		t.Run(name, func(t *testing.T) {
			m, now, cutoff := tokenCleanupFixture(t, 24)
			defer m.Close()
			cleanup, table := m.cleanupTokenMetering, "token_metering_facts"
			if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
				Updates(map[string]any{"start_at": cutoff, "last_cleanup_at": now}).Error; err != nil {
				t.Fatal(err)
			}
			if detection {
				cleanup, table = m.cleanupTokenDetection, "token_detection_currents"
				for id := int64(2000); id < 2024; id++ {
					if err := m.tokenStore().Create(&TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, ExpiresAt: now - 2, ResultJSON: "{}"}).Error; err != nil {
						t.Fatal(err)
					}
				}
				tokenCleanupAllowExpiredHeads(t, m, now)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 2200*time.Millisecond)
			defer cancel()
			release, laterDone := make(chan struct{}), make(chan struct{})
			var queued sync.Once
			queuedStarted := false
			if err := m.tokenStore().Callback().Delete().Before("gorm:delete").Register("queue_after_cleanup_prefix", func(tx *gorm.DB) {
				if tx.Statement.Table != table {
					return
				}
				queued.Do(func() {
					queuedStarted = true
					go func() {
						defer close(laterDone)
						if err := m.tokenLocalWriteMu.lock(ctx, "association"); err != nil {
							return
						}
						defer m.tokenLocalWriteMu.Unlock()
						<-release
					}()
					for {
						m.tokenLocalWriteMu.mu.Lock()
						waiting := m.tokenLocalWriteMu.waiters.Len() > 0
						m.tokenLocalWriteMu.mu.Unlock()
						if waiting {
							break
						}
						if !waitSourceLifecycle(ctx, time.Millisecond) {
							tx.AddError(ctx.Err())
							return
						}
					}
				})
			}); err != nil {
				t.Fatal(err)
			}
			m.tokenLocalWriteMu.Lock()
			done := make(chan error, 1)
			go func() { done <- cleanup(ctx, now) }()
			awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
			time.Sleep(1100 * time.Millisecond)
			m.tokenLocalWriteMu.Unlock()
			err := <-done
			close(release)
			if queuedStarted {
				<-laterDone
			}
			_ = m.tokenStore().Callback().Delete().Remove("queue_after_cleanup_prefix")
			if err != nil || ctx.Err() != nil || !queuedStarted {
				t.Fatal("cleanup spent the remaining deadline waiting after a durable prefix", err, ctx.Err())
			}
			if detection {
				var left int64
				if err := m.tokenStore().Table(table).Where("log_id>=2000").Count(&left).Error; err != nil || left != 24-tokenCleanupChunk {
					t.Fatal("detection cleanup did not preserve the exact committed prefix", left, err)
				}
			} else {
				assertTokenCleanupLedgers(t, m, 25-tokenCleanupChunk)
			}
			if err := cleanup(context.Background(), now); err != nil {
				t.Fatal("next cleanup turn failed to resume", err)
			}
			if detection {
				var left int64
				if err := m.tokenStore().Table(table).Where("log_id>=2000").Count(&left).Error; err != nil || left != 0 {
					t.Fatal("detection cleanup did not finish", left, err)
				}
			} else {
				assertTokenCleanupLedgers(t, m, 1)
			}
		})
	}
}
