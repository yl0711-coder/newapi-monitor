package monitor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionLearnedBatchYieldsWhenRowsBecomeExpensive(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 24; id++ {
		detectionFact(t, m, id, now-600, 3, 20000, 0, 0)
	}
	// A previous cheap round has learned the maximum batch. New expensive
	// evidence must yield within this batch, not only shrink the next one.
	m.tokenDetectionCommitRows = 64
	entered := make(chan struct{})
	var once sync.Once
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("new_expensive_detection_rows", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_detection_currents" {
			once.Do(func() { close(entered) })
			waitSourceLifecycle(tx.Statement.Context, 60*time.Millisecond)
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer m.tokenStore().Callback().Create().Remove("new_expensive_detection_rows")
	ctx, cancel := context.WithCancel(context.Background())
	type outcome struct {
		processed int
		err       error
	}
	done := make(chan outcome, 1)
	go func() {
		n, err := m.runTokenDetectionRound(ctx, now)
		done <- outcome{n, err}
	}()
	joined := false
	defer func() {
		cancel()
		if !joined {
			<-done
		}
	}()
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("detection did not reach its expensive work")
	}
	queued, stopQueue := context.WithTimeout(context.Background(), 550*time.Millisecond)
	defer stopQueue()
	checkpoint := TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: now, EndAt: now + 300, CursorID: 123}
	if err := m.tokenLocalWriteTransaction(queued, func(tx *gorm.DB) error { return tx.Create(&checkpoint).Error }); err != nil {
		t.Fatal("learned batch blocked the queued collection checkpoint", err)
	}
	cancel()
	result := <-done
	joined = true
	if result.err != nil && !errors.Is(result.err, context.Canceled) {
		t.Fatal(result.err)
	}
	var heads int64
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil || heads != int64(result.processed) || heads < 1 || heads >= 16 {
		t.Fatal("partial batch did not preserve exactly its committed prefix", heads, result.processed, err)
	}
	assertTokenCapacityLedger(t, m.tokenStore())
	if err := m.tokenStore().Callback().Create().Remove("new_expensive_detection_rows"); err != nil {
		t.Fatal(err)
	}
	if _, err := m.runTokenDetectionRound(context.Background(), now+1); err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil || heads != 24 {
		t.Fatal("yield skipped jobs or failed to resume them", heads, err)
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}
