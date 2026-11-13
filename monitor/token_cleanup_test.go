package monitor

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"gorm.io/gorm"
)

func tokenCleanupFixture(t *testing.T, rows int) (*Monitor, int64, int64) {
	t.Helper()
	m, now, _ := detectionFixture(t)
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	if err := initTokenMeteringHealthCounts(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	cutoff := (now - m.cfg.tokenMeteringRetentionSeconds()) / 300 * 300
	for id := int64(1); id <= int64(rows); id++ {
		fact := detectionFact(t, m, id, cutoff-600+id, 3, 10000, 0, 0)
		tokenCleanupSettledFact(t, m, fact, now)
	}
	detectionFact(t, m, 1000, cutoff, 3, 10000, 0, 0)
	if err := m.tokenStore().Exec(`UPDATE token_metering_states SET
		fact_count=(SELECT COUNT(*) FROM token_metering_facts),
		fact_bytes=(SELECT SUM(stored_bytes) FROM token_metering_facts)`).Error; err != nil {
		t.Fatal(err)
	}
	tokenCleanupAuthorizeFixture(t, m, now, cutoff)
	return m, now, cutoff
}

func tokenCleanupAuthorizeFixture(t *testing.T, m *Monitor, now, cutoff int64) {
	t.Helper()
	if err := m.tokenStore().Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).FirstOrCreate(&TokenMeteringState{SourceEpoch: m.cfg.TokenFactsSourceEpoch}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch<>''").Updates(map[string]any{
		"cleanup_backup_id": "synthetic-verified-backup", "cleanup_verified_at": now, "cleanup_safe_before": cutoff, "cleanup_stable_since": now - 1800,
	}).Error; err != nil {
		t.Fatal(err)
	}
}

func tokenCleanupSettledFact(t *testing.T, m *Monitor, f TokenMeteringFact, now int64) {
	t.Helper()
	start := f.CreatedAt / 300 * 300
	if err := m.tokenStore().Where("source_epoch=? AND start_at=?", f.SourceEpoch, start).FirstOrCreate(&TokenMeteringWindow{SourceEpoch: f.SourceEpoch, StartAt: start, EndAt: start + 300, Done: true, SourceComplete: true}).Error; err != nil {
		t.Fatal(err)
	}
	h := TokenDetectionCurrent{SourceEpoch: f.SourceEpoch, LogID: f.LogID, CreatedAt: f.CreatedAt, UserID: f.UserID, ChannelID: f.ChannelID, Model: f.Model, UserGroup: f.UserGroup,
		FactRevision: f.Revision, LinkRevision: f.LinkRevision, PolicyHash: tokenDetectionPolicyHash(detectionPolicy()), SourceComplete: true, Stage: 3, ExpiresAt: now - 1, DetectionState: "not_hit", ResultJSON: `{}`}
	if err := m.tokenStore().Create(&h).Error; err != nil {
		t.Fatal(err)
	}
}

func assertTokenCleanupLedgers(t *testing.T, m *Monitor, want int64) {
	t.Helper()
	db := m.tokenStore()
	var facts, refs, members, groups int64
	for _, item := range []struct {
		table  string
		target *int64
	}{
		{"token_metering_facts", &facts}, {"token_metering_refs", &refs}, {tokenReportPendingMembers, &members},
	} {
		if err := db.Table(item.table).Count(item.target).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Table(tokenReportPendingGroups).Select("COALESCE(SUM(requests),0)").Scan(&groups).Error; err != nil {
		t.Fatal(err)
	}
	var bytes, health int64
	if err := db.Model(&TokenMeteringFact{}).Select("COALESCE(SUM(stored_bytes),0)").Scan(&bytes).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&tokenMeteringHealthCount{}).Where("source_epoch=? AND dimension='records' AND value='all'", m.cfg.TokenFactsSourceEpoch).Select("count").Scan(&health).Error; err != nil {
		t.Fatal(err)
	}
	state := tokenMeteringStateRead(t, m)
	if facts != want || refs != want || members != want || groups != want || health != want || state.FactCount != want || state.FactBytes != bytes {
		t.Fatalf("cleanup ledger mismatch: facts=%d refs=%d members=%d groups=%d health=%d state=%d bytes=%d/%d want=%d", facts, refs, members, groups, health, state.FactCount, state.FactBytes, bytes, want)
	}
}

func TestTokenCleanupCommittedPrefixSurvivesFailureAndResumes(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 17)
	if err := m.tokenStore().Exec(`CREATE TRIGGER fail_cleanup BEFORE DELETE ON token_metering_facts WHEN OLD.log_id=10 BEGIN SELECT RAISE(ABORT,'injected cleanup failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err == nil {
		t.Fatal("cleanup failure swallowed")
	}
	// The first eight are durable; identity 9 was in the failed transaction.
	assertTokenCleanupLedgers(t, m, 10)
	if err := m.tokenStore().Exec("DROP TRIGGER fail_cleanup").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	assertTokenCleanupLedgers(t, m, 1)
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	assertTokenCleanupLedgers(t, m, 1)
}

func TestTokenCleanupAmortizesBoundedDeleteLatency(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 64)
	defer m.Close()
	db := m.tokenStore()
	// Model a fixed storage/statement cost while the same writer is needed
	// by collection. The existing eight-row transaction and 128-row sweep
	// bounds should not require two separate DELETE statements per identity.
	if err := db.Callback().Delete().Before("gorm:delete").Register("retained_cleanup_delete_latency", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" && tx.Statement.Table != "token_metering_refs" {
			return
		}
		if !waitSourceLifecycle(tx.Statement.Context, 60*time.Millisecond) {
			_ = tx.AddError(tx.Statement.Context.Err())
		}
	}); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2500*time.Millisecond)
	err := m.cleanupTokenMetering(ctx, now)
	cancel()
	_ = db.Callback().Delete().Remove("retained_cleanup_delete_latency")
	if err != nil {
		state := tokenMeteringStateRead(t, m)
		t.Fatalf("bounded cleanup spent its budget on repeated identity statements: remaining=%d error=%v", state.FactCount, err)
	}
	assertTokenCleanupLedgers(t, m, 1)
	// The request on the retention boundary and repeated cleanup are stable.
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	assertTokenCleanupLedgers(t, m, 1)
}

func TestTokenCleanupBatchPreservesSameIdentityInAnotherEpoch(t *testing.T) {
	m, now, cutoff := tokenCleanupFixture(t, 2)
	defer m.Close()
	db := m.tokenStore()
	var fact TokenMeteringFact
	if err := db.First(&fact, "source_epoch=? AND log_id=?", m.cfg.TokenFactsSourceEpoch, 1).Error; err != nil {
		t.Fatal(err)
	}
	var refs []TokenMeteringRef
	if err := db.Where("source_epoch=? AND log_id=?", fact.SourceEpoch, fact.LogID).Find(&refs).Error; err != nil {
		t.Fatal(err)
	}
	fact.SourceEpoch, fact.CreatedAt = "retained-other-epoch", cutoff
	state := TokenMeteringState{SourceEpoch: fact.SourceEpoch, StartAt: cutoff, ScanThrough: cutoff, FactCount: 1, FactBytes: fact.StoredBytes}
	if err := db.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	for i := range refs {
		refs[i].SourceEpoch = fact.SourceEpoch
	}
	if err := db.Create(&refs).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"token_metering_facts", "token_metering_refs", tokenReportPendingMembers} {
		var total, retained int64
		if err := db.Table(table).Count(&total).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Table(table).Where("source_epoch=? AND log_id=?", fact.SourceEpoch, fact.LogID).Count(&retained).Error; err != nil || total != 2 || retained != 1 {
			t.Fatal("cleanup crossed the source/identity boundary", table, total, retained, err)
		}
	}
	if err := db.First(&state, "source_epoch=?", fact.SourceEpoch).Error; err != nil || state.FactCount != 1 || state.FactBytes != fact.StoredBytes {
		t.Fatal("cleanup changed another epoch's capacity", state, err)
	}
	if state := tokenMeteringStateRead(t, m); state.FactCount != 1 {
		t.Fatal("cleanup lost its own committed capacity", state)
	}
}

func TestTokenCleanupReportsOnlyCommittedEpochsWhenCollectionQueues(t *testing.T) {
	m, now, cutoff := tokenCleanupFixture(t, 0)
	defer m.Close()
	db := m.tokenStore()
	// Put one expired fact in the normal epoch and one in a second epoch. The
	// first cleanup transaction is allowed to finish its current epoch, then
	// must yield before deleting the second one when collection joins the queue.
	tokenCleanupSettledFact(t, m, detectionFact(t, m, 1, cutoff-600, 3, 10000, 0, 0), now)
	var fact TokenMeteringFact
	if err := db.First(&fact, "source_epoch=? AND log_id=?", m.cfg.TokenFactsSourceEpoch, 1).Error; err != nil {
		t.Fatal(err)
	}
	var refs []TokenMeteringRef
	if err := db.Where("source_epoch=? AND log_id=?", fact.SourceEpoch, fact.LogID).Find(&refs).Error; err != nil {
		t.Fatal(err)
	}
	fact.SourceEpoch = "cleanup-queued-epoch"
	state := TokenMeteringState{SourceEpoch: fact.SourceEpoch, StartAt: cutoff, ScanThrough: cutoff, FactCount: 1, FactBytes: fact.StoredBytes}
	if err := db.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&fact).Error; err != nil {
		t.Fatal(err)
	}
	tokenCleanupSettledFact(t, m, fact, now)
	tokenCleanupAuthorizeFixture(t, m, now, cutoff)
	for i := range refs {
		refs[i].SourceEpoch = fact.SourceEpoch
	}
	if err := db.Create(&refs).Error; err != nil {
		t.Fatal(err)
	}

	var once sync.Once
	enqueued, handedOff := make(chan struct{}), make(chan struct{})
	if err := db.Callback().Delete().After("gorm:delete").Register("cleanup_queue_after_first_epoch", func(tx *gorm.DB) {
		if tx.Statement.Table != "token_metering_facts" {
			return
		}
		once.Do(func() {
			ready := make(chan struct{})
			m.tokenLocalWriteMu.mu.Lock()
			waiter := &tokenLocalWriterWaiter{ready: ready, kind: "collection"}
			waiter.element = m.tokenLocalWriteMu.waiters.PushBack(waiter)
			m.tokenLocalWriteMu.mu.Unlock()
			close(enqueued)
			go func() {
				<-ready
				m.tokenLocalWriteMu.Unlock()
				close(handedOff)
			}()
		})
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Callback().Delete().Remove("cleanup_queue_after_first_epoch"); err != nil {
			t.Error(err)
		}
	}()

	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	select {
	case <-enqueued:
	case <-time.After(time.Second):
		t.Fatal("collection waiter was not enqueued")
	}
	select {
	case <-handedOff:
	case <-time.After(time.Second):
		t.Fatal("cleanup did not hand the writer to collection")
	}
	var expired int64
	if err := db.Model(&TokenMeteringFact{}).Where("created_at < ?", cutoff).Count(&expired).Error; err != nil {
		t.Fatal(err)
	}
	if expired != 1 {
		t.Fatalf("cleanup reported/deleted more than its committed epoch: expired=%d", expired)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&TokenMeteringFact{}).Where("created_at < ?", cutoff).Count(&expired).Error; err != nil {
		t.Fatal(err)
	}
	if expired != 0 {
		t.Fatalf("cleanup did not resume the uncommitted epoch: expired=%d", expired)
	}
}

func TestTokenCleanupWaitsForSharedWriterAndCancels(t *testing.T) {
	for _, detection := range []bool{false, true} {
		m, now, _ := tokenCleanupFixture(t, 1)
		m.tokenLocalWriteMu.Lock()
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		var err error
		if detection {
			err = m.cleanupTokenDetection(ctx, now)
		} else {
			err = m.cleanupTokenMetering(ctx, now)
		}
		cancel()
		m.tokenLocalWriteMu.Unlock()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("cleanup bypassed shared writer: detection=%t err=%v", detection, err)
		}
		assertTokenCleanupLedgers(t, m, 2)
	}
}

func TestTokenCleanupLedgerFailureRollsBackDeletedPrefix(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 17)
	db := m.tokenStore()
	if err := db.Exec(`CREATE TRIGGER fail_cleanup_ledger BEFORE UPDATE OF fact_count ON token_metering_states
		WHEN OLD.fact_count = 10 AND NEW.fact_count < OLD.fact_count
		BEGIN SELECT RAISE(ABORT,'injected ledger failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err == nil {
		t.Fatal("ledger failure swallowed")
	}
	assertTokenCleanupLedgers(t, m, 10)
	if err := db.Exec("DROP TRIGGER fail_cleanup_ledger").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	assertTokenCleanupLedgers(t, m, 1)
}

func TestTokenCleanupIdentityDeletesUseBothPrimaryKeyColumns(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 2)
	db := m.tokenStore()
	for id := int64(1); id <= 2; id++ {
		if err := db.Create(&TokenDetectionRevision{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, Revision: 1, FrozenJSON: tokenDetectionJSON(tokenDetectionFrozen{Version: tokenDetectionVersion}), StoredBytes: 2}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if _, err := migrateTokenRevisionMembers(db); err != nil {
		t.Fatal(err)
	}
	type statement struct {
		sql  string
		args []any
	}
	queries := map[string]statement{}
	if err := db.Callback().Delete().After("gorm:delete").Register("cleanup_identity_plan", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" || tx.Statement.Table == "token_detection_currents" {
			queries[tx.Statement.Table] = statement{tx.Statement.SQL.String(), append([]any(nil), tx.Statement.Vars...)}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := db.Callback().Delete().Remove("cleanup_identity_plan"); err != nil {
			t.Error(err)
		}
	}()
	// Even an expired head remains while its source fact is retained.
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := db.Model(&TokenDetectionCurrent{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatal("retained evidence recycled", count, err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if len(queries) != 2 {
		t.Fatal("missing actual cleanup statements", len(queries))
	}
	for table, query := range queries {
		var plan []struct{ Detail string }
		if err := db.Raw("EXPLAIN QUERY PLAN "+query.sql, query.args...).Scan(&plan).Error; err != nil {
			t.Fatal(err)
		}
		if len(plan) == 0 || !strings.Contains(plan[0].Detail, "source_epoch=? AND log_id=?") {
			t.Fatalf("cleanup does not seek the full identity: table=%s plan=%v", table, plan)
		}
	}
	for _, table := range []string{"token_detection_currents", "token_detection_revisions", tokenReportIndexTable, tokenReportSummaryTable, tokenReportStatusCounts} {
		if err := db.Table(table).Count(&count).Error; err != nil || count != 0 {
			t.Fatal("orphan result projection", table, count, err)
		}
	}
	assertTokenCleanupLedgers(t, m, 1)
}

func TestTokenCleanupExpiredGapPreservesCursorAndWindow(t *testing.T) {
	m, now, cutoff := tokenCleanupFixture(t, 0)
	db := m.tokenStore()
	if err := db.Model(&TokenMeteringState{}).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).
		Updates(map[string]any{"start_at": cutoff - 600, "scan_through": cutoff, "complete_through": cutoff - 600}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&TokenMeteringWindow{SourceEpoch: m.cfg.TokenFactsSourceEpoch, StartAt: cutoff - 600, EndAt: cutoff - 300, Done: true, SourceComplete: true}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
			t.Fatal(err)
		}
		state := tokenMeteringStateRead(t, m)
		if state.StartAt != cutoff-600 || state.ScanThrough != cutoff || state.CompleteThrough != cutoff-600 || state.ExpiredBefore != 0 || state.ExpiredGapWindows != 0 {
			t.Fatalf("age advanced unproven coverage: %+v", state)
		}
	}
	assertTokenCleanupLedgers(t, m, 1)
}

func TestTokenCleanupDetectionPrefixKeepsFrozenEvidenceAtomic(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 0)
	db := m.tokenStore()
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	for id := int64(1); id <= 17; id++ {
		if err := db.Create(&TokenDetectionCurrent{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, CreatedAt: now - 100, ExpiresAt: now - 100 + id, ResultJSON: `{"rules":[]}`}).Error; err != nil {
			t.Fatal(err)
		}
		if err := db.Create(&TokenDetectionRevision{SourceEpoch: m.cfg.TokenFactsSourceEpoch, LogID: id, Revision: 1, FrozenJSON: `{}`, StoredBytes: 2}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Exec(`CREATE TRIGGER fail_head_cleanup BEFORE DELETE ON token_detection_currents WHEN OLD.log_id=10 BEGIN SELECT RAISE(ABORT,'injected cleanup failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	tokenCleanupAllowExpiredHeads(t, m, now)
	if err := m.cleanupTokenDetection(context.Background(), now); err == nil {
		t.Fatal("cleanup failure swallowed")
	}
	check := func(want int64) {
		t.Helper()
		for _, table := range []string{"token_detection_currents", "token_detection_revisions", tokenReportIndexTable, tokenReportSummaryTable} {
			var count int64
			if err := db.Table(table).Count(&count).Error; err != nil || count != want {
				t.Fatal("partial head/evidence transaction", table, count, want, err)
			}
		}
		var capacity tokenDetectionCapacity
		if err := db.First(&capacity).Error; err != nil {
			t.Fatal(err)
		}
		if capacity.CurrentRows != want || capacity.RevisionBytes != want*2 {
			t.Fatal("cleanup capacity drift", capacity.CurrentRows, capacity.RevisionBytes, want)
		}
	}
	check(9)
	if err := db.Exec("DROP TRIGGER fail_head_cleanup").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	check(0)
	assertTokenCleanupLedgers(t, m, 1)
}
