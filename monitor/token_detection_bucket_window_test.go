package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestTokenDetectionLaterFirstEvaluationKeepsOlderWindowCurrent(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	detectionSeed(t, m, now)
	detectionFact(t, m, 10, now-100, 3, 10000, 0, 0)
	for i := int64(0); i < 4; i++ {
		detectionRun(t, m, now+i)
	}
	db, epoch := m.tokenStore(), m.cfg.TokenFactsSourceEpoch
	query := tokenDetectionQuery{SourceEpoch: epoch, From: now - 1000, To: now}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	if pending, err := tokenReportPendingCount(db, query, hash); err != nil || pending != 0 {
		t.Fatalf("old window was not current before later arrival: pending=%d err=%v", pending, err)
	}
	old, _ := detectionHead(t, m, 10)
	future := now + 7200
	detectionFact(t, m, 20, future-1, 3, 10000, 0, 0)
	if err := db.Model(&TokenDetectionState{}).Where("source_epoch=?", epoch).Updates(map[string]any{"lease_token": "later-window", "lease_until": future + 30}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.processTokenDetection(context.Background(), epoch, 20, detectionPolicy(), "later-window", future); err != nil {
		t.Fatal(err)
	}
	_, result := detectionHead(t, m, 20)
	if result.Quarantined {
		t.Fatal("fixture must be a benign first evaluation, not a changed anchor")
	}
	if pending, err := tokenReportPendingCount(db, query, hash); err != nil || pending != 0 {
		t.Fatalf("a request beyond every old reference horizon invalidated historical decisions: pending=%d err=%v", pending, err)
	}
	current, _ := detectionHead(t, m, 10)
	if current.DecisionHash != old.DecisionHash || current.EvaluatedAt != old.EvaluatedAt {
		t.Fatal("history was rewritten instead of excluding irrelevant invalidation")
	}
	if err := db.Transaction(initTokenDetectionReportIndex); err != nil {
		t.Fatal(err)
	}
	if pending, err := tokenReportPendingCount(db, query, hash); err != nil || pending != 0 {
		t.Fatalf("compact pending projection differs: %d %v", pending, err)
	}
}

func TestTokenDetectionSameChannelArrivalDoesNotInvalidateControls(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, epoch, policy := m.tokenStore(), m.cfg.TokenFactsSourceEpoch, detectionPolicy()
	for id := int64(1); id <= 12; id++ {
		detectionFact(t, m, id, now-120, 3, 10000, 0, 0)
	}
	for turn := int64(0); turn < 4; turn++ {
		detectionRun(t, m, now+turn)
	}
	q := tokenDetectionQuery{SourceEpoch: epoch, From: now - 300, To: now - 60}
	hash := tokenDetectionPolicyHash(policy)
	if pending, err := tokenReportPendingCount(db, q, hash); err != nil || pending != 0 {
		t.Fatal("initial cohort must be current", pending, err)
	}
	process := func(id, channel int64, linkedLater bool) {
		t.Helper()
		fact := detectionFact(t, m, id, now-30, channel, 10000, 0, 0)
		if err := db.Model(&TokenDetectionState{}).Where("source_epoch=?", epoch).Updates(map[string]any{"lease_token": "channel-arrival", "lease_until": now + 30}).Error; err != nil {
			t.Fatal(err)
		}
		if linkedLater {
			if err := db.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id=?", epoch, id).
				Updates(map[string]any{"link_state": "waiting", "link_revision": 0, "request_length": nil, "evidence_json": "[]", "evidence_level": "unavailable", "length_state": "missing"}).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.processTokenDetection(context.Background(), epoch, id, policy, "channel-arrival", now+4); err != nil {
				t.Fatal(err)
			}
			if err := db.Save(&fact).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := m.processTokenDetection(context.Background(), epoch, id, policy, "channel-arrival", now+4); err != nil {
			t.Fatal(err)
		}
		_, result := detectionHead(t, m, id)
		if result.Quarantined {
			t.Fatal("fixture must add benign reference evidence")
		}
	}
	process(20, 3, false)
	if pending, err := tokenReportPendingCount(db, q, hash); err != nil || pending != 0 {
		t.Fatalf("same-channel requests are excluded from controls but invalidated %d results: %v", pending, err)
	}
	process(22, 3, true)
	if pending, err := tokenReportPendingCount(db, q, hash); err != nil || pending != 0 {
		t.Fatalf("first association evidence cannot make a same-channel control: pending=%d err=%v", pending, err)
	}
	process(21, 4, true)
	if pending, err := tokenReportPendingCount(db, q, hash); err != nil || pending != 12 {
		t.Fatal("an eligible cross-channel arrival did not wake the affected cohort", pending, err)
	}
}

func TestTokenDetectionComparableCorrectionsSettleWithoutNewEvidence(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenProcessingBatchRows, m.cfg.TokenProcessingPollSeconds = 250, 15
	for id := int64(1); id <= 300; id++ {
		detectionFact(t, m, id, now-600, 1+id%3, 10000, 0, 0)
	}
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: m.cfg.TokenFactsSourceEpoch, From: now - 900, To: now}
	hash := tokenDetectionPolicyHash(detectionPolicy())
	for round := 0; round < 5; round++ {
		processed, _, err := m.runTokenDetectionRoundProgress(context.Background(), now)
		if err != nil {
			t.Fatal(err)
		}
		pending, err := tokenReportPendingCount(m.tokenStore(), q, hash)
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("comparable cohort round=%d processed=%d pending=%d", round, processed, pending)
		if pending == 0 {
			return
		}
	}
	t.Fatal("settled cross-channel evidence repeatedly invalidated completed results")
}

func TestTokenDetectionBucketScopesRespectExistingTotalCapacity(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	m.cfg.TokenDetectionMaxReferences = 3
	fact := detectionFact(t, m, 1, now, 1, 10000, 0, 0)
	in, err := tokenDetectionInputOf(fact)
	if err != nil {
		t.Fatal(err)
	}
	write := func() error {
		return m.tokenStore().Transaction(func(tx *gorm.DB) error {
			_, _, err := m.tokenDetectionReferenceScope(tx, in, detectionPolicy(), nil, TokenDetectionBucket{}, true, false, now)
			return err
		})
	}
	if err := write(); err != nil {
		t.Fatal(err)
	}
	in.Fact.ChannelID = 2
	if err := write(); err != nil {
		t.Fatal(err)
	}
	var before, after []TokenDetectionBucket
	if err := m.tokenStore().Order("id").Find(&before).Error; err != nil || len(before) != 3 {
		t.Fatal("parent and two scopes should fill the original budget", len(before), err)
	}
	in.Fact.ChannelID = 3
	if err := write(); !errors.Is(err, errTokenDetectionCapacity) {
		t.Fatal("scope creation bypassed the original metadata capacity", err)
	}
	if err := m.tokenStore().Order("id").Find(&after).Error; err != nil || !reflect.DeepEqual(before, after) {
		t.Fatal("failed scope creation leaked notifications", err)
	}
}

func TestTokenDetectionBucketScopeCorrectionRollbackAndCleanup(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, policy := m.tokenStore(), detectionPolicy()
	oldFact := detectionFact(t, m, 1, now-7200, 1, 10000, 0, 0)
	laterFact := detectionFact(t, m, 2, now, 1, 10000, 0, 0)
	createScope := func(fact TokenMeteringFact) TokenDetectionBucket {
		t.Helper()
		input, err := tokenDetectionInputOf(fact)
		if err != nil {
			t.Fatal(err)
		}
		var scope TokenDetectionBucket
		err = db.Transaction(func(tx *gorm.DB) error {
			id, generation, err := m.tokenDetectionReferenceScope(tx, input, policy, nil, TokenDetectionBucket{}, true, false, now)
			if err != nil {
				return err
			}
			if err := tx.Create(&TokenDetectionCurrent{SourceEpoch: fact.SourceEpoch, LogID: fact.LogID, ReferenceBucket: id, ReferenceGeneration: generation}).Error; err != nil {
				return err
			}
			return tx.First(&scope, "id=?", id).Error
		})
		if err != nil {
			t.Fatal(err)
		}
		return scope
	}
	old, later := createScope(oldFact), createScope(laterFact)
	if old.ParentBucket == "" || old.ParentBucket != later.ParentBucket || old.ID == later.ID {
		t.Fatal("fixture did not create independent time scopes for one dimension")
	}
	read := func() []TokenDetectionBucket {
		var rows []TokenDetectionBucket
		if err := db.Order("id").Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		return rows
	}
	before := read()
	in, err := tokenDetectionInputOf(oldFact)
	if err != nil {
		t.Fatal(err)
	}
	in.Fact.Revision++
	previous := &tokenDetectionStoredCurrent{TokenDetectionCurrent: TokenDetectionCurrent{ReferenceBucket: old.ID}}
	injected := errors.New("rollback scope update")
	update := func(tx *gorm.DB) error {
		_, _, err := m.tokenDetectionReferenceScope(tx, in, policy, previous, old, true, false, now+1)
		return err
	}
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := update(tx); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(before, read()) {
		t.Fatal("rolled-back correction leaked a scope generation")
	}
	if err := db.Transaction(update); err != nil {
		t.Fatal(err)
	}
	var revised TokenDetectionBucket
	if err := db.First(&revised, "id=?", later.ID).Error; err != nil || revised.Generation <= later.Generation {
		t.Fatal("real old evidence correction did not invalidate a later anchor consumer", revised, err)
	}
	if err := cleanupTokenDetectionBucketScopes(db, []string{old.ID, later.ID, old.ParentBucket}); err != nil {
		t.Fatal(err)
	}
	if len(read()) != 3 {
		t.Fatal("cleanup removed a parent or a retained scope")
	}
	if err := db.Delete(&TokenDetectionCurrent{}, "source_epoch=? AND log_id=?", oldFact.SourceEpoch, oldFact.LogID).Error; err != nil {
		t.Fatal(err)
	}
	if err := cleanupTokenDetectionBucketScopes(db, []string{old.ID, later.ID, old.ParentBucket}); err != nil {
		t.Fatal(err)
	}
	rows := read()
	if len(rows) != 2 {
		t.Fatal("unreferenced scope was not reclaimed", rows)
	}
	for _, row := range rows {
		if row.ID == old.ID {
			t.Fatal("expired scope survived its last head")
		}
	}
}

func TestTokenDetectionBucketScopeLegacySchemaUpgradeIsTransactional(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	db := m.tokenStore()
	if err := db.Migrator().DropTable(&TokenDetectionBucket{}); err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`CREATE TABLE token_detection_buckets (id TEXT PRIMARY KEY,generation INTEGER,changed_at INTEGER)`).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Exec(`INSERT INTO token_detection_buckets VALUES('legacy',7,1)`).Error; err != nil {
		t.Fatal(err)
	}
	injected := errors.New("rollback additive scope schema")
	if err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.AutoMigrate(&TokenDetectionBucket{}); err != nil {
			return err
		}
		return injected
	}); !errors.Is(err, injected) {
		t.Fatal(err)
	}
	if db.Migrator().HasColumn(&TokenDetectionBucket{}, "parent_bucket") {
		t.Fatal("failed upgrade leaked a partial schema")
	}
	if err := db.Transaction(func(tx *gorm.DB) error { return tx.AutoMigrate(&TokenDetectionBucket{}) }); err != nil {
		t.Fatal(err)
	}
	var legacy TokenDetectionBucket
	if err := db.First(&legacy, "id='legacy'").Error; err != nil || legacy.Generation != 7 || legacy.ParentBucket != "" {
		t.Fatal("upgrade changed the legacy generation", legacy, err)
	}
}

func TestTokenDetectionBucketScopeMetadataBoundAndRollback(t *testing.T) {
	m, now, _ := detectionFixture(t)
	defer m.Close()
	db, policy := m.tokenStore(), detectionPolicy()
	fact := detectionFact(t, m, 1, now, 1, 10000, 0, 0)
	in, err := tokenDetectionInputOf(fact)
	if err != nil {
		t.Fatal(err)
	}
	parent := tokenReferenceBucket(in, policy)
	if err := db.Create(&TokenDetectionBucket{ID: parent, Generation: 7}).Error; err != nil {
		t.Fatal(err)
	}
	start := now / tokenDetectionBucketWindowSeconds * tokenDetectionBucketWindowSeconds
	rows := make([]TokenDetectionBucket, tokenDetectionMaxBucketWindows)
	for i := range rows {
		at := start - int64(i)*tokenDetectionBucketWindowSeconds
		rows[i] = TokenDetectionBucket{ID: tokenDetectionScopeID(parent, at, fact.ChannelID), ParentBucket: parent, WindowStart: at, ChannelID: fact.ChannelID, Generation: 7}
	}
	if err := db.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	previous := &tokenDetectionStoredCurrent{TokenDetectionCurrent: TokenDetectionCurrent{ReferenceBucket: rows[0].ID}}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	began := time.Now()
	if err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		_, _, err := m.tokenDetectionReferenceScope(tx, in, policy, previous, rows[0], true, true, now)
		return err
	}); err != nil {
		t.Fatal("bounded metadata correction did not commit", err)
	}
	t.Logf("corrected_scope_metadata=%d elapsed=%s", len(rows), time.Since(began))
	var corrected int64
	if err := db.Model(&TokenDetectionBucket{}).Where("parent_bucket=? AND generation=8", parent).Count(&corrected).Error; err != nil || corrected != int64(len(rows)) {
		t.Fatal("real correction missed historical scopes", corrected, err)
	}
	// The next hourly scope cannot grow the metadata without a bound. Its
	// failed attempt must roll back the parent and all generation changes.
	in.Fact.CreatedAt = start + tokenDetectionBucketWindowSeconds
	if err := db.Transaction(func(tx *gorm.DB) error {
		_, _, err := m.tokenDetectionReferenceScope(tx, in, policy, previous, rows[0], true, true, now+1)
		return err
	}); !errors.Is(err, errTokenDetectionCapacity) {
		t.Fatal("metadata capacity was bypassed", err)
	}
	var root TokenDetectionBucket
	if err := db.First(&root, "id=?", parent).Error; err != nil || root.Generation != 8 {
		t.Fatal("capacity failure leaked a parent generation", root, err)
	}
	if err := db.Model(&TokenDetectionBucket{}).Where("parent_bucket=? AND generation=8", parent).Count(&corrected).Error; err != nil || corrected != int64(len(rows)) {
		t.Fatal("capacity failure leaked a scoped generation", corrected, err)
	}
}
