package monitor

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

// Exercise the real scheduler when collection arrives during queue selection,
// before the background worker has committed even one row. No source query or
// detection wake signal is needed to resume already durable work.
func TestTokenMaintenanceZeroProgressHandoffResumesWithoutIdlePoll(t *testing.T) {
	for _, operation := range []string{"association", "detection"} {
		t.Run(operation, func(t *testing.T) {
			m, now := tokenIsolatedRuntimeFixture(t, "")
			m.cfg.TokenFactsEnabled = operation == "association"
			m.cfg.TokenDetectionEnabled = operation == "detection"
			m.cfg.TokenProcessingPollSeconds = 60
			raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0}`)
			raw.ID, raw.CreatedAt = 7201, now-600
			fact, _ := parseTokenMetering(m.cfg, raw, now)
			if operation == "detection" {
				fact.LinkState = "unique"
				fact.LinkRevision = 1
			}
			if err := m.tokenStore().Create(&fact).Error; err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var once sync.Once
			var reads atomic.Int64
			handoff := make(chan error, 1)
			pause := func(tx *gorm.DB) {
				if tx.Statement.Context.Value(tokenWriteOperationKey{}) != operation {
					return
				}
				reads.Add(1)
				once.Do(func() {
					m.tokenLocalWriteMu.Lock()
					go func() {
						err := m.tokenLocalWriteMu.lock(ctx, "collection")
						if err == nil {
							m.tokenLocalWriteMu.Unlock()
						}
						handoff <- err
					}()
					awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
					go func() {
						waitSourceLifecycle(ctx, 300*time.Millisecond)
						m.tokenLocalWriteMu.Unlock()
					}()
				})
			}
			for _, register := range []func(string, func(*gorm.DB)) error{
				m.tokenReadStore().Callback().Row().Before("gorm:row").Register,
				m.tokenReadStore().Callback().Query().Before("gorm:query").Register,
			} {
				if err := register("collection_after_queue_read", pause); err != nil {
					t.Fatal(err)
				}
			}
			m.startTokenMeteringMaintenanceAt(ctx, func() time.Time { return time.Unix(now, 0) })
			select {
			case err := <-handoff:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(2 * time.Second):
				t.Fatal("worker did not hand off to collection")
			}
			deadline := time.Now().Add(2 * time.Second)
			for {
				var completed int64
				q := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=?", fact.SourceEpoch, fact.LogID)
				if operation == "association" {
					q = m.tokenReadStore().Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=? AND link_checked_at>0", fact.SourceEpoch, fact.LogID)
				}
				if err := q.Count(&completed).Error; err != nil {
					t.Fatal(err)
				}
				if completed == 1 {
					break
				}
				if time.Now().After(deadline) {
					t.Fatalf("zero-progress handoff inserted the 60-second idle poll: operation=%s reads=%d", operation, reads.Load())
				}
				time.Sleep(10 * time.Millisecond)
			}
			cancel()
			m.tokenDetectionMu.Lock()
			m.tokenDetectionMu.Unlock()
		})
	}
}
