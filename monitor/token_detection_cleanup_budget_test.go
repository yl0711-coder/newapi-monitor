package monitor

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func tokenCleanupExpiredHeads(t *testing.T, m *Monitor, now, count int64) {
	t.Helper()
	heads := make([]TokenDetectionCurrent, 0, count)
	revisions := make([]TokenDetectionRevision, 0, count)
	for id := int64(1); id <= count; id++ {
		heads = append(heads, TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, ExpiresAt: now - 1, ResultJSON: "{}"})
		revisions = append(revisions, TokenDetectionRevision{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, Revision: 1, FrozenJSON: "{}", StoredBytes: 2})
	}
	if err := m.tokenStore().CreateInBatches(heads, 32).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().CreateInBatches(revisions, 32).Error; err != nil {
		t.Fatal(err)
	}
	tokenCleanupAllowExpiredHeads(t, m, now)
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	m.cfg.TokenProcessingBatchRows = 250
	m.tokenDetectionCleanupRows.Store(250)
}

// These throughput fixtures represent settled, backed-up results. Tests for
// protection deliberately do not call this helper: absent state is pending.
func tokenCleanupAllowExpiredHeads(t *testing.T, m *Monitor, now int64) {
	t.Helper()
	db := m.tokenStore()
	if err := db.Model(&TokenDetectionCurrent{}).Where("COALESCE(detection_state,'')=''").Updates(map[string]any{"detection_state": "not_hit", "source_complete": true, "stage": 3, "next_at": 0, "created_at": now - 8*86400, "evaluated_at": now - 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&TokenDetectionRevision{}).Where("frozen_json='{}'").Update("frozen_json", tokenDetectionJSON(tokenDetectionFrozen{Version: tokenDetectionVersion})).Error; err != nil {
		t.Fatal(err)
	}
	for turn := 0; turn < 100; turn++ {
		done, err := migrateTokenRevisionMembers(db)
		if err != nil {
			t.Fatal(err)
		}
		if done {
			break
		}
	}
	tokenCleanupAuthorizeFixture(t, m, now, now-4*86400)
}

func TestTokenDetectionCleanupLargeTurnYieldsAtBoundedStatement(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	tokenCleanupExpiredHeads(t, m, now, 160)
	tokenInjectCommitLatency(t, m, 800*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var once sync.Once
	collection := make(chan error, 1)
	if err := m.tokenStore().Callback().Delete().After("gorm:delete").Register("collection_after_cleanup_statement", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_detection_currents" {
			return
		}
		if len(tx.Statement.Vars) > 33 { // one epoch plus at most 32 identities
			tx.AddError(errors.New("cleanup statement exceeded bounded identities"))
			return
		}
		once.Do(func() {
			go func() {
				if err := m.tokenLocalWriteMu.lock(ctx, "collection"); err != nil {
					collection <- err
					return
				}
				defer m.tokenLocalWriteMu.Unlock()
				var count int64
				err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Count(&count).Error
				if err == nil && count != 128 {
					err = errors.New("collection did not receive the writer after the first committed 32-row prefix")
				}
				collection <- err
			}()
			awaitTokenWriterQueue(t, &m.tokenLocalWriteMu, 1)
			// Account for SQL work before the next atomic statement boundary.
			time.Sleep(60 * time.Millisecond)
		})
	}); err != nil {
		t.Fatal(err)
	}
	defer m.tokenStore().Callback().Delete().Remove("collection_after_cleanup_statement")
	if err := m.cleanupTokenDetection(ctx, now); err != nil {
		t.Fatal("cleanup prefix failed", err)
	}
	select {
	case err := <-collection:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("collection never received a bounded writer turn")
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}

func TestTokenDetectionCleanupMultiStatementRollbackKeepsEvidence(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	tokenCleanupExpiredHeads(t, m, now, 96)
	failure := errors.New("second cleanup statement failed")
	deletes := 0
	if err := m.tokenStore().Callback().Delete().After("gorm:delete").Register("fail_second_cleanup_statement", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_detection_currents" {
			return
		}
		if len(tx.Statement.Vars) > 33 {
			tx.AddError(errors.New("cleanup statement exceeded bounded identities"))
			return
		}
		deletes++
		if deletes == 2 {
			tx.AddError(failure)
		}
	}); err != nil {
		t.Fatal(err)
	}
	err := m.cleanupTokenDetection(context.Background(), now)
	_ = m.tokenStore().Callback().Delete().Remove("fail_second_cleanup_statement")
	if !errors.Is(err, failure) {
		t.Fatal("did not exercise failure inside a multi-statement transaction", deletes, err)
	}
	for _, model := range []any{&TokenDetectionCurrent{}, &TokenDetectionRevision{}} {
		var count int64
		if err := m.tokenReadStore().Model(model).Count(&count).Error; err != nil || count != 96 {
			t.Fatal("failed batch partially removed evidence", count, err)
		}
	}
	assertTokenCapacityLedger(t, m.tokenStore())
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal("cleanup failed to resume", err)
	}
	for _, model := range []any{&TokenDetectionCurrent{}, &TokenDetectionRevision{}} {
		var count int64
		if err := m.tokenReadStore().Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatal("cleanup resume did not finish", count, err)
		}
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}

func TestTokenDetectionFullCapacityCleanupResumesDurableRetries(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	const capacity, requests = int64(512), int64(80)
	tokenCleanupExpiredHeads(t, m, now, capacity)
	m.cfg.TokenDetectionMaxRows = capacity
	// Fill the real ledger through real inserts, rather than changing a
	// counter or a production limit. The scaled capacity exercises recovery
	// without another multi-GB disposable copy of the running database.
	for id := int64(10000); id < 10000+requests; id++ {
		// Capacity recovery is downstream of association.  Seed an exact
		// unique fact so the worker reaches the real capacity ledger instead of
		// correctly deferring an unassociated request.
		detectionFact(t, m, id, now-600, 3, 100, 0, 0)
	}
	n, _, err := m.runTokenDetectionRoundProgress(context.Background(), now)
	if n != 0 || !errors.Is(err, errTokenDetectionCapacity) {
		t.Fatal("full result store consumed pending facts", n, err)
	}
	var retries, results int64
	if err := m.tokenReadStore().Model(&TokenDetectionRetry{}).Count(&retries).Error; err != nil || retries != requests {
		t.Fatal("capacity backoff was not durably recorded", retries, err)
	}
	var state TokenDetectionState
	if err := m.tokenReadStore().First(&state).Error; err != nil || state.LastSuccessAt != 0 || state.LeaseUntil != 0 {
		t.Fatal("capacity failure advanced success or retained its lease", state, err)
	}
	// Reopen with a fresh Monitor; neither a local owner token nor adaptive
	// batch memory is needed to recover the durable retries and evidence.
	for _, db := range []*gorm.DB{m.tokenReadDB, m.tokenDB} {
		pool, err := db.DB()
		if err != nil {
			t.Fatal(err)
		}
		if err := pool.Close(); err != nil {
			t.Fatal(err)
		}
	}
	m.tokenReadDB, m.tokenDB = nil, nil
	m = &Monitor{cfg: m.cfg, storeDB: m.storeDB}
	if err := m.openTokenStore(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	tokenInjectCommitLatency(t, m, 800*time.Millisecond)
	for round := 0; round < 5; round++ {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		err := m.cleanupTokenDetection(ctx, now)
		cancel()
		if err != nil {
			t.Fatal("cleanup did not preserve bounded progress", err)
		}
	}
	var left int64
	if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Count(&left).Error; err != nil || capacity-left < 208 {
		t.Fatal("full capacity did not release enough slots", left, err)
	}
	for round := 0; round < 8; round++ {
		_, _, err := m.runTokenDetectionRoundProgress(context.Background(), now+61+int64(round))
		if err != nil {
			t.Fatal("durable retry could not resume after capacity recovery", err)
		}
		if err := m.tokenReadStore().Model(&TokenDetectionRetry{}).Count(&retries).Error; err != nil {
			t.Fatal(err)
		}
		if retries == 0 {
			break
		}
	}
	if err := m.tokenReadStore().Model(&TokenDetectionCurrent{}).Where("log_id>=10000").Count(&results).Error; err != nil || results != requests || retries != 0 {
		t.Fatal("capacity recovery lost pending work", results, retries, err)
	}
	if err := m.tokenReadStore().First(&state).Error; err != nil || state.LastSuccessAt < now+61 || state.LastReason != "" || state.LeaseUntil != 0 {
		t.Fatal("committed recovery state is not accurate", state, err)
	}
	var facts int64
	if err := m.tokenReadStore().Model(&TokenMeteringFact{}).Count(&facts).Error; err != nil || facts != requests {
		t.Fatal("capacity recovery deleted retained input", facts, err)
	}
	assertTokenCapacityLedger(t, m.tokenStore())
	t.Logf("full_capacity=%d expired_removed=%d facts=%d detected=%d retries=%d", capacity, capacity-left, facts, results, retries)
}
