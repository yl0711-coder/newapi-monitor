//go:build unix

package monitor

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
)

// Cooperative budget for each local DB phase, including pool acquisition.
// Never spend the whole three-minute job budget waiting for a normal writer.
// Cooldown and external audit I/O are outside these transactions.
const financeGiftHandoffWriteBudget = 2 * time.Second

// Establish a read snapshot before schema writes. Direct CREATE TABLE waits
// for SQLite's connection busy_timeout (5s), which may outlive ctx. Upgrading
// this read transaction instead yields SQLITE_BUSY to a competing writer;
// schema changes roll back together. Do not change a shared connection's PRAGMA.
func ensureFinanceGiftHandoffLedger(ctx context.Context, db *gorm.DB) error {
	return db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var exists int
		if err := tx.Raw("SELECT EXISTS(SELECT 1 FROM main.sqlite_master WHERE name=?)", (financeGiftHandoffCommit{}).TableName()).Scan(&exists).Error; err != nil {
			return err
		}
		return tx.AutoMigrate(&financeGiftHandoffCommit{})
	})
}

func inspectFinanceGiftHandoffWithBudget(parent context.Context, db *gorm.DB, target financeGiftScopeTarget, evidence []FinanceGiftBoundaryEvent) (financeGiftHandoffCandidate, error) {
	ctx, cancel := context.WithTimeout(parent, financeGiftHandoffWriteBudget)
	defer cancel()
	var candidate financeGiftHandoffCandidate
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		var err error
		candidate, err = inspectFinanceGiftScopeHandoff(ctx, tx, target, evidence)
		return err
	})
	return candidate, err
}

func financeGiftHandoffPauseReason(err error) string {
	if usageFactLocalStoreBusy(err) {
		return "handoff_store_busy"
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return "handoff_budget_exhausted"
	}
	if errors.Is(err, context.Canceled) {
		return "handoff_canceled"
	}
	return ""
}
