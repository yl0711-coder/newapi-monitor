package monitor

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const financeGiftValidationBatchTimeout = 5 * time.Second

// Each bounded batch releases its read snapshot immediately. Do not pin the
// WAL for the entire history or acquire writer locks. Even out-of-band edits
// without a proof update must be detected; a DB-wide commit alone is not one.
func validateFinanceGiftCachedHours(ctx context.Context, db *gorm.DB, keys []financeGiftUserHourKey, states map[financeGiftUserHourKey]FinanceGiftBoundaryState, policies map[string]bool, excluded map[int64]bool) error {
	for start := 0; start < len(keys); {
		end := start
		var rows int64
		for end < len(keys) && end-start < financeGiftEvidenceQueryKeys {
			next := states[keys[end]].Rows
			if end > start && rows+next > financeGiftEvidenceQueryRows {
				break
			}
			rows += next
			end++
		}
		batchCtx, cancel := context.WithTimeout(ctx, financeGiftValidationBatchTimeout)
		err := db.WithContext(batchCtx).Transaction(func(tx *gorm.DB) error {
			_, err := loadFinanceGiftEvidenceBatch(batchCtx, tx, keys[start:end], states, policies, excluded)
			return err
		}, &sql.TxOptions{ReadOnly: true})
		cancel()
		if err != nil {
			if errors.Is(err, errFinanceFactsChanged) {
				return financeFactChange("gift-evidence", "content")
			}
			return fmt.Errorf("复核赠送缓存证据: %w", err)
		}
		start = end
	}
	return ctx.Err()
}
