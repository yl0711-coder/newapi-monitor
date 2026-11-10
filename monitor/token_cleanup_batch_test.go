package monitor

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

func TestTokenCleanupRemembersBatchAndAvoidsEmptyCoverageTransaction(t *testing.T) {
	m, now, _ := tokenCleanupFixture(t, 64)
	defer m.Close()
	db := m.tokenStore()
	if err := db.Exec(`CREATE TRIGGER fail_cleanup_batch BEFORE DELETE ON token_metering_facts
		WHEN OLD.log_id=10 BEGIN SELECT RAISE(ABORT,'injected cleanup rollback'); END`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.cleanupTokenMetering(context.Background(), now); err == nil {
		t.Fatal("expected the second delete transaction to roll back")
	}
	assertTokenCleanupLedgers(t, m, 57) // Eight deletions committed before the failed batch.
	if err := db.Exec("DROP TRIGGER fail_cleanup_batch").Error; err != nil {
		t.Fatal(err)
	}
	var firstBatch int64
	coverageWriterReads := 0
	if err := db.Callback().Delete().After("gorm:delete").Register("observe_cleanup_batch", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_facts" && firstBatch == 0 {
			firstBatch = tx.RowsAffected
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer db.Callback().Delete().Remove("observe_cleanup_batch")
	if err := db.Callback().Query().Before("gorm:query").Register("observe_cleanup_coverage_writer", func(tx *gorm.DB) {
		if tx.Statement.Table == "token_metering_states" && tx.Statement.Context.Value(tokenWriteOperationKey{}) == "facts_cleanup" {
			if _, held := tx.Logger.(*tokenWriteTrace); held {
				coverageWriterReads++
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer db.Callback().Query().Remove("observe_cleanup_coverage_writer")
	if err := m.cleanupTokenMetering(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	if firstBatch != 16 || coverageWriterReads > 4 {
		t.Fatal("cleanup repeated its warmup or exceeded bounded authorization/cursor reads", firstBatch, coverageWriterReads)
	}
	assertTokenCleanupLedgers(t, m, 1)
}
