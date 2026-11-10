package monitor

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

func TestTokenDetectionCleanupRechecksRenewedHeadsAndRestoredFacts(t *testing.T) {
	m, now := tokenIsolatedRuntimeFixture(t, "")
	epoch := m.cfg.TokenFactsSourceEpoch
	for id := int64(1); id <= 8; id++ {
		if err := m.tokenStore().Create(&TokenDetectionCurrent{SourceEpoch: epoch, LogID: id, ExpiresAt: now - 1, ResultJSON: "{}"}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.tokenStore().Create(&TokenDetectionRevision{SourceEpoch: epoch, LogID: id, Revision: 1, FrozenJSON: "{}", StoredBytes: 2}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.ensureTokenDetectionCapacity(context.Background()); err != nil {
		t.Fatal(err)
	}
	tokenCleanupAllowExpiredHeads(t, m, now)
	injected := false
	reader := m.tokenReadStore()
	if err := reader.Callback().Query().After("gorm:query").Register("renew_after_cleanup_read", func(tx *gorm.DB) {
		if injected || tx.Statement.Table != "token_detection_currents" {
			return
		}
		injected = true
		if err := m.tokenStore().Model(&TokenDetectionCurrent{}).Where("source_epoch=? AND log_id=1", epoch).Update("expires_at", now+60).Error; err != nil {
			_ = tx.AddError(err)
			return
		}
		raw := tokenMeteringRawFixture(`{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`)
		raw.ID, raw.CreatedAt = 2, now-600
		fact, _ := parseTokenMetering(m.cfg, raw, now)
		_ = tx.AddError(m.tokenStore().Create(&fact).Error)
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := reader.Callback().Query().Remove("renew_after_cleanup_read"); err != nil {
			t.Error(err)
		}
	}()
	if err := m.cleanupTokenDetection(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if !injected {
		t.Fatal("did not exercise read-to-write race")
	}
	for _, model := range []any{&TokenDetectionCurrent{}, &TokenDetectionRevision{}} {
		var ids []int64
		if err := m.tokenReadStore().Model(model).Order("log_id").Pluck("log_id", &ids).Error; err != nil || len(ids) != 2 || ids[0] != 1 || ids[1] != 2 {
			t.Fatal("renewed or restored evidence was deleted", ids, err)
		}
	}
	assertTokenCapacityLedger(t, m.tokenStore())
}
