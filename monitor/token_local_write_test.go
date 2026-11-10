package monitor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionCostlyWritesYieldToCollection(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 24; id++ {
		detectionFact(t, m, id, now-600, 3, 20000, 0, 0)
	}
	entered := make(chan struct{})
	var once sync.Once
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("costly_detection_insert", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_detection_currents" {
			return
		}
		once.Do(func() { close(entered) })
		time.Sleep(40 * time.Millisecond)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.tokenStore().Callback().Create().Remove("costly_detection_insert"); err != nil {
			t.Error(err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, err := m.runTokenDetectionRound(ctx, now); done <- err }()
	defer func() { cancel(); <-done }()
	select {
	case <-entered:
	case err := <-done:
		done <- err
		t.Fatal("detection never wrote a result", err)
	case <-time.After(3 * time.Second):
		t.Fatal("detection never reached the cost injection")
	}
	collectCtx, collectCancel := context.WithTimeout(context.Background(), 450*time.Millisecond)
	defer collectCancel()
	checkpoint := TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: now, EndAt: now + 300, CursorID: 123}
	if err := m.tokenLocalWriteTransaction(collectCtx, func(tx *gorm.DB) error { return tx.Create(&checkpoint).Error }); err != nil {
		t.Fatal("costly detection starved a queued collection checkpoint", err)
	}
	var persisted TokenMeteringWindow
	if err := m.tokenReadStore().First(&persisted, "source_epoch=? AND start_at=?", checkpoint.SourceEpoch, checkpoint.StartAt).Error; err != nil || persisted.CursorID != 123 {
		t.Fatal("queued collection did not commit", persisted.CursorID, err)
	}
}

func TestTokenLocalWriterCancellationDoesNotRunTransaction(t *testing.T) {
	m, _, _ := tokenMeteringFixture(t)
	defer m.Close()
	m.tokenLocalWriteMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	ran := false
	err := m.tokenLocalWriteTransaction(ctx, func(*gorm.DB) error { ran = true; return nil })
	m.tokenLocalWriteMu.Unlock()
	if !errors.Is(err, context.DeadlineExceeded) || ran {
		t.Fatal("waiting writer ignored cancellation", ran, err)
	}
	if err := m.tokenLocalWriteTransaction(context.Background(), func(*gorm.DB) error { ran = true; return nil }); err != nil || !ran {
		t.Fatal("cancellation left the writer reserved", ran, err)
	}
}

func TestTokenDetectionWaitsForShortWriterContention(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	m.tokenLocalWriteMu.Lock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	type outcome struct {
		count int
		err   error
	}
	done := make(chan outcome, 1)
	go func() { count, err := m.runTokenDetectionRound(ctx, now); done <- outcome{count, err} }()
	select {
	case result := <-done:
		m.tokenLocalWriteMu.Unlock()
		t.Fatal("brief contention discarded the detection turn", result)
	case <-time.After(20 * time.Millisecond):
	}
	m.tokenLocalWriteMu.Unlock()
	result := <-done
	if result.count != 1 || result.err != nil {
		t.Fatal("ready work did not resume when writer was released", result)
	}
}

func TestTokenAssociationReadDoesNotReserveWriter(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=1").Update("link_next_at", now).Error; err != nil {
		t.Fatal(err)
	}
	entered, release := make(chan struct{}), make(chan struct{})
	if err := m.nginxEvidenceDB.Callback().Query().Before("gorm:query").Register("pause_association_evidence", func(tx *gorm.DB) {
		close(entered)
		select {
		case <-release:
		case <-tx.Statement.Context.Done():
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := m.linkTokenMeteringBatch(ctx, now); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("association never entered evidence read")
	}
	processed, detectErr := m.runTokenDetectionRound(ctx, now)
	close(release)
	linkErr := <-done
	if processed != 1 || detectErr != nil || linkErr != nil {
		t.Fatal("association read blocked detection", processed, detectErr, linkErr)
	}
}
