package monitor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionQueueReadDoesNotBlockCollection(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	start := (now-tokenMeteringLagSeconds)/300*300 - 300
	if err := m.tokenStore().Create(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch, SourceMode: "complete", StartAt: start, ScanThrough: start}).Error; err != nil {
		t.Fatal(err)
	}
	tokenMeteringSourceRow(t, m, 9001, start+1, "synthetic-concurrent-source")
	entered, release := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	if err := m.tokenReadStore().Callback().Row().Before("gorm:row").Register("pause_queue_reader", func(tx *gorm.DB) {
		if !first.CompareAndSwap(false, true) {
			return
		}
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
	go func() { _, err := m.runTokenDetectionRound(ctx, now); done <- err }()
	select {
	case <-entered:
	case <-ctx.Done():
		t.Fatal("detector never entered queue read")
	}
	pages, collectErr := m.runTokenMeteringRound(context.Background(), time.Unix(now, 0))
	close(release)
	detectErr := <-done
	_ = m.tokenReadStore().Callback().Row().Remove("pause_queue_reader")
	if collectErr != nil || pages == 0 || detectErr != nil {
		t.Fatalf("queue read blocked source progress: pages=%d collect=%v detect=%v", pages, collectErr, detectErr)
	}
	var count int64
	if err := m.tokenStore().Model(&TokenMeteringFact{}).Where("log_id=9001").Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("collector did not commit during paused detection read: %d %v", count, err)
	}
}

func TestTokenDetectionCompletedOwnerResumesAfterReleaseDeadline(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionFact(t, m, 1, now-600, 3, 20000, 0, 0)
	releaseFailed := false
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("expire_detection_release", func(tx *gorm.DB) {
		fields, ok := tx.Statement.Dest.(map[string]any)
		if tx.Statement.Table == "token_detection_states" && ok && fields["lease_token"] == "" && fields["lease_until"] == 0 {
			<-tx.Statement.Context.Done()
			tx.AddError(tx.Statement.Context.Err())
			releaseFailed = true
		}
	}); err != nil {
		t.Fatal(err)
	}
	n, err := m.runTokenDetectionRound(context.Background(), now)
	_ = m.storeDB.Callback().Update().Remove("expire_detection_release")
	var state TokenDetectionState
	if readErr := m.storeDB.First(&state).Error; readErr != nil {
		t.Fatal(readErr)
	}
	if !releaseFailed || n != 1 || !errors.Is(err, context.DeadlineExceeded) || state.LeaseUntil != now+30 || state.LastSuccessAt != now {
		t.Fatal("release-deadline fixture did not retain its committed result and lease", n, err)
	}
	detectionFact(t, m, 2, now-599, 3, 20000, 0, 0)
	other := &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	if n, err := other.runTokenDetectionRound(context.Background(), now+2); n != 0 || err != nil {
		t.Fatal("another detector bypassed the unexpired lease", n, err)
	}
	n, err = m.runTokenDetectionRound(context.Background(), now+2)
	var heads int64
	if readErr := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&heads).Error; readErr != nil {
		t.Fatal(readErr)
	}
	if err != nil || n == 0 || heads != 2 {
		t.Fatal("finished detector waited for its own lease instead of resuming", n, heads, err)
	}
	if err := m.storeDB.Model(&TokenDetectionState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
		Updates(map[string]any{"lease_token": "other-worker-fixture", "lease_until": now + 60}).Error; err != nil {
		t.Fatal(err)
	}
	if n, err := m.runTokenDetectionRound(context.Background(), now+3); n != 0 || err != nil {
		t.Fatal("old detector bypassed a newer worker's lease", n, err)
	}
	if err := m.storeDB.First(&state).Error; err != nil || state.LeaseToken != "other-worker-fixture" {
		t.Fatal("newer lease was overwritten", err)
	}
}

func TestTokenDetectionIdleRoundDoesNotInventSuccess(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	if n, err := m.runTokenDetectionRound(context.Background(), now); err != nil || n != 0 {
		t.Fatal(n, err)
	}
	var state TokenDetectionState
	if err := m.storeDB.First(&state).Error; err != nil || state.LastSuccessAt != 0 {
		t.Fatal("idle round published detection success", state, err)
	}
}

func TestTokenDetectionSQLInterruptPreservesDeadlineAndProgress(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	for id := int64(1); id <= 32; id++ {
		detectionFact(t, m, id, now-600, 3, 20000, 0, 0)
	}
	creates := 0
	var interrupted error
	if err := m.storeDB.Callback().Create().Before("gorm:create").Register("interrupt_second_detection_chunk", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_detection_currents" {
			return
		}
		creates++
		if creates == 17 {
			// Real SQLite work, bounded by this round's caller deadline. The
			// Earlier whole chunks have committed; the chunk containing this
			// write is aborted. Adaptive chunk sizing decides that boundary.
			// ExecContext keeps the driver's interrupt handler active through
			// SQLite's step. A Rows.Next loop in this driver does not do so.
			interrupted = tx.Session(&gorm.Session{NewDB: true}).Exec("WITH RECURSIVE busy(n) AS (SELECT 1 UNION ALL SELECT n+1 FROM busy) SELECT max(n) FROM busy").Error
			tx.AddError(interrupted)
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	n, err := m.runTokenDetectionRound(ctx, now)
	if interrupted == nil || n <= 0 || n >= 17 || !errors.Is(err, context.DeadlineExceeded) || !errors.Is(ctx.Err(), context.DeadlineExceeded) {
		t.Fatalf("SQL interruption lost deadline/committed work: processed=%d error=%v sqlite=%v", n, err, interrupted)
	}
	if err := m.storeDB.Callback().Create().Remove("interrupt_second_detection_chunk"); err != nil {
		t.Fatal(err)
	}
	var state TokenDetectionState
	var heads int64
	if err := m.storeDB.First(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil {
		t.Fatal(err)
	}
	if heads != int64(n) || state.LeaseUntil != 0 || state.LeaseToken != "" || state.LastReason != "local_detection_incomplete" {
		t.Fatal("interrupted round lost its durable state or lease release", heads, state)
	}
	assertTokenCapacityLedger(t, m.storeDB)
	if _, err := m.runTokenDetectionRound(context.Background(), now+1); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&TokenDetectionCurrent{}).Count(&heads).Error; err != nil || heads != 32 {
		t.Fatal("resumed detection did not finish remaining identities", heads, err)
	}
}

func TestTokenDetectionAmortizesBoundedTransactionLatency(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	m.cfg.TokenProcessingBatchRows = 250
	for id := int64(1); id <= 120; id++ {
		// This test measures bounded transaction latency.  Seed facts with a
		// durable unique association so the worker is not intentionally held
		// at the association-pending gate.
		detectionFact(t, m, id, now-600+id, 3, 100, 0, 0)
	}
	// Model the retained disk's fixed transaction latency, independent of
	// row count. All writes still pass through the actual SQLite transaction,
	// triggers, lease fence and commit; no result or queue state is fabricated.
	chunks := map[gorm.ConnPool]int{}
	if err := m.tokenStore().Callback().Create().Before("gorm:create").Register("fixed_detection_transaction_latency", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_detection_currents" {
			return
		}
		pool := tx.Statement.ConnPool
		if chunks[pool] == 0 {
			time.Sleep(150 * time.Millisecond)
		}
		chunks[pool]++
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	defer cancel()
	n, err := m.runTokenDetectionRound(ctx, now)
	_ = m.tokenStore().Callback().Create().Remove("fixed_detection_transaction_latency")
	var durable int64
	if readErr := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&durable).Error; readErr != nil {
		t.Fatal(readErr)
	}
	if err != nil || n < 64 || durable != int64(n) {
		t.Fatalf("fixed transaction overhead prevented bounded detection throughput: processed=%d durable=%d transactions=%d error=%v", n, durable, len(chunks), err)
	}
	for _, rows := range chunks {
		if rows > 64 {
			t.Fatal("detection exceeded its bounded adaptive transaction size", rows)
		}
	}
	assertTokenCapacityLedger(t, m.tokenStore())
	if _, err := m.runTokenDetectionRound(context.Background(), now+1); err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Count(&durable).Error; err != nil || durable != 120 {
		t.Fatal("durable prefix did not resume to completion", durable, err)
	}
}
