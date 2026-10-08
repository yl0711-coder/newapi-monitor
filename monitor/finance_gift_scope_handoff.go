//go:build unix

package monitor

import (
	"context"
	"errors"

	"gorm.io/gorm"
)

// applyFinanceGiftScopeHandoff is an internal, bounded handoff primitive.
// It has no production entrypoint, scheduler, credential or source query.
// The caller must first authenticate the exported evidence and its epoch.
// A content hash pins evidence integrity; it is NOT source authentication.
// Never replace a live database with an offline repair copy.
func applyFinanceGiftScopeHandoff(ctx context.Context, db *gorm.DB, target financeGiftScopeTarget, evidence []FinanceGiftBoundaryEvent, confirmedHash string, now int64) (financeGiftScopeRepairResult, error) {
	var result financeGiftScopeRepairResult
	if db == nil || len(evidence) == 0 || len(evidence) > financeGiftLocalMaxRows {
		return result, errors.New("handoff requires a database and 1..3000 evidence rows")
	}
	if err := validateFinanceGiftScopeBatch([]financeGiftScopeTarget{target}, now); err != nil {
		return result, err
	}
	// Take ownership before entering the transaction. No file or network I/O
	// occurs while SQLite holds a transaction.
	verified := append([]FinanceGiftBoundaryEvent(nil), evidence...)
	for _, event := range verified {
		if event.SourceEpoch != target.SourceEpoch || event.HourTs != target.HourTs || event.UserID != target.UserID {
			return result, errors.New("handoff evidence belongs to another source or target")
		}
	}
	if confirmedHash == "" || financeGiftBoundaryContentHash(verified) != confirmedHash {
		return result, errors.New("handoff evidence confirmation mismatch")
	}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		candidate, err := inspectFinanceGiftScopeHandoff(ctx, tx, target, verified)
		if err != nil {
			return err
		}
		current, merged, updated := candidate.current, candidate.merged, candidate.updated
		result = financeGiftScopeRepairResult{RowsChecked: len(current.Events), ContentHash: current.State.ContentHash}
		if updated == 0 {
			return nil // Do not change timestamps, hashes or cursors on replay.
		}
		state, err := replaceFinanceGiftBoundaryUserHour(ctx, tx, target.HourTs, target.UserID, max(now, current.State.UpdatedAt+1), target.SourceEpoch, merged)
		if err != nil {
			return err
		}
		result.RowsUpdated, result.ContentHash = updated, state.ContentHash
		return nil
	})
	if err != nil {
		return financeGiftScopeRepairResult{}, err // Never report rolled-back rows.
	}
	return result, nil
}

type financeGiftHandoffCandidate struct {
	current financeGiftScopeSnapshot
	merged  []FinanceGiftBoundaryEvent
	updated int
}

var (
	errGiftHandoffReceiverProof = errors.New("handoff receiver proof is missing or invalid")
	errGiftHandoffAggregate     = errors.New("handoff receiver aggregate differs from ordering ledger")
	errGiftHandoffConflict      = errors.New("handoff evidence conflicts with receiver")
)

// Shared by readonly preview and atomic application. Call under a transaction;
// preview results never replace this check when a write is eventually applied.
func inspectFinanceGiftScopeHandoff(ctx context.Context, tx *gorm.DB, target financeGiftScopeTarget, verified []FinanceGiftBoundaryEvent) (financeGiftHandoffCandidate, error) {
	var candidate financeGiftHandoffCandidate
	current, err := loadFinanceGiftScopeSnapshot(ctx, tx, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		return candidate, errors.Join(errGiftHandoffReceiverProof, err)
	}
	var aggregate FinanceUserHourFact
	if err := tx.First(&aggregate, "hour_ts=? AND user_id=?", target.HourTs, target.UserID).Error; err != nil {
		return candidate, errors.Join(errGiftHandoffReceiverProof, err)
	}
	if aggregate.Requests != current.State.Requests || aggregate.RefundRecords != current.State.RefundRecords || aggregate.ConsumeQuota != current.State.ConsumeQuota || aggregate.RefundQuota != current.State.RefundQuota {
		return candidate, errGiftHandoffAggregate
	}
	// Check even when every group is already known: "complete" is not proof
	// that old exported evidence agrees with newer corrections in the receiver.
	merged, updated, err := mergeFinanceGiftScopeEvidence(current.Events, verified)
	if err != nil {
		return candidate, errors.Join(errGiftHandoffConflict, err)
	}
	return financeGiftHandoffCandidate{current, merged, updated}, nil
}
