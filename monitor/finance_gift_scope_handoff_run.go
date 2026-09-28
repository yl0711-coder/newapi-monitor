//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"gorm.io/gorm"
)

const financeGiftHandoffRunTimeout = financeGiftTaskTimeout

func RunFinanceGiftLocalHandoff(ctx context.Context, dir, confirmation string) (financeGiftScopeBatchResult, error) {
	return runFinanceGiftLocalHandoff(ctx, dir, confirmation, waitFinanceGiftScopeBatch, nil)
}

// RunFinanceGiftLocalHandoffLimited repairs at most maxHours user-hours in the
// prepared offline copy. Replays of completed targets do not consume the limit.
// Reaching the limit is a successful pause, not completion or an automatic retry.
func RunFinanceGiftLocalHandoffLimited(ctx context.Context, dir, confirmation string, maxHours int) (financeGiftScopeBatchResult, error) {
	return runFinanceGiftLocalHandoffLimited(ctx, dir, confirmation, maxHours, waitFinanceGiftScopeBatch, nil)
}

// auditOverride exists only for deterministic local fault tests. Public runs
// always append and fsync the private journal. No caller-supplied DB path.
func runFinanceGiftLocalHandoff(parent context.Context, dir, confirmation string, wait func(context.Context, time.Duration) error, auditOverride func(financeGiftScopeBatchEntry) error) (financeGiftScopeBatchResult, error) {
	return runFinanceGiftLocalHandoffLimited(parent, dir, confirmation, financeGiftScopeBatchLimit, wait, auditOverride)
}

func runFinanceGiftLocalHandoffLimited(parent context.Context, dir, confirmation string, maxHours int, wait func(context.Context, time.Duration) error, auditOverride func(financeGiftScopeBatchEntry) error) (financeGiftScopeBatchResult, error) {
	rejected := financeGiftScopeBatchResult{Status: "rejected"}
	if err := validateFinanceGiftHandoffLimit(maxHours); err != nil {
		return rejected, err
	}
	ctx, cancel := context.WithTimeout(parent, financeGiftHandoffRunTimeout)
	defer cancel()
	lock, err := giftLocalLock(dir)
	if err != nil {
		return rejected, err
	}
	defer lock.Close()
	_, plan, evidence, err := loadFinanceGiftHandoffJob(dir, confirmation)
	if err != nil {
		return rejected, err
	}
	if err := giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), plan); err != nil {
		return rejected, err
	}
	journal, err := giftLocalOpenRegular(filepath.Join(dir, "audit.jsonl"), os.O_WRONLY|os.O_APPEND)
	if err != nil {
		return rejected, err
	}
	defer journal.Close()
	if err := wait(ctx, financeGiftScopeBatchInterval); err != nil {
		return financeGiftScopeBatchResult{Status: "paused", Remaining: len(plan.Targets)}, err
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return rejected, err
	}
	defer closeDB()
	// Decode all pinned evidence before any writes. This projection is local
	// in-memory SQLite, never an upstream database connection.
	source, err := giftLocalSource(ctx, evidence)
	if err != nil {
		return rejected, err
	}
	defer source.Close()
	verified := make([][]FinanceGiftBoundaryEvent, len(plan.Targets))
	for i, target := range plan.Targets {
		verified[i], err = fetchFinanceGiftBoundaryEvents(ctx, source, target.HourTs, []int64{target.UserID})
		if err != nil {
			return rejected, err
		}
		for j := range verified[i] {
			verified[i][j].SourceEpoch = target.SourceEpoch
		}
		if len(verified[i]) != plan.Rows[i] {
			return rejected, errors.New("handoff evidence row count changed")
		}
	}
	preview, err := previewFinanceGiftHandoffTargets(ctx, db, plan.Targets, verified)
	if err != nil {
		return rejected, err
	}
	if preview.BlockedTargets > 0 {
		return rejected, errors.New("handoff receiver preflight blocked")
	}
	encoder := json.NewEncoder(journal)
	audit := func(entry financeGiftScopeBatchEntry) error {
		if err := encoder.Encode(entry); err != nil {
			return err
		}
		return journal.Sync()
	}
	if auditOverride != nil {
		audit = auditOverride
	}
	if err := recoverFinanceGiftHandoffAudit(ctx, db, confirmation, filepath.Join(dir, "audit.jsonl"), plan, audit); err != nil {
		return financeGiftScopeBatchResult{Status: "audit_failed", Remaining: len(plan.Targets)}, err
	}
	return executeFinanceGiftHandoffLimited(ctx, db, confirmation, plan.Targets, verified, maxHours, wait, audit)
}

func executeFinanceGiftHandoff(ctx context.Context, db *gorm.DB, confirmation string, targets []financeGiftScopeTarget, evidence [][]FinanceGiftBoundaryEvent, wait func(context.Context, time.Duration) error, audit func(financeGiftScopeBatchEntry) error) (financeGiftScopeBatchResult, error) {
	return executeFinanceGiftHandoffLimited(ctx, db, confirmation, targets, evidence, financeGiftScopeBatchLimit, wait, audit)
}

func validateFinanceGiftHandoffLimit(maxHours int) error {
	if maxHours < 1 || maxHours > financeGiftScopeBatchLimit {
		return errors.New("handoff max-hours must be between 1 and 10")
	}
	return nil
}

func executeFinanceGiftHandoffLimited(ctx context.Context, db *gorm.DB, confirmation string, targets []financeGiftScopeTarget, evidence [][]FinanceGiftBoundaryEvent, maxHours int, wait func(context.Context, time.Duration) error, audit func(financeGiftScopeBatchEntry) error) (financeGiftScopeBatchResult, error) {
	return executeFinanceGiftHandoffGuarded(ctx, db, confirmation, targets, evidence, maxHours, wait, audit, nil)
}

// guard admits one target after cooldown and immediately before its bounded
// write. It is not a global transaction: a target already admitted may finish
// while cancellation/revocation is arriving; no subsequent target is admitted.
func executeFinanceGiftHandoffGuarded(ctx context.Context, db *gorm.DB, confirmation string, targets []financeGiftScopeTarget, evidence [][]FinanceGiftBoundaryEvent, maxHours int, wait func(context.Context, time.Duration) error, audit func(financeGiftScopeBatchEntry) error, guard func(context.Context, int) (int, error)) (financeGiftScopeBatchResult, error) {
	result := financeGiftScopeBatchResult{Status: "running", Remaining: len(targets)}
	if err := validateFinanceGiftHandoffLimit(maxHours); err != nil {
		result.Status = "rejected"
		return result, err
	}
	writtenHours := 0
	for i, target := range targets {
		if err := ctx.Err(); err != nil {
			result.Status = "paused"
			return result, err
		}
		// Stop before inspecting the next target. Even an apparently unchanged
		// target could require a write after a concurrent publication; never
		// allow that race to exceed this invocation's mutation allowance.
		if writtenHours >= maxHours {
			result.Status = "paused_limit"
			return result, nil
		}
		candidate, err := inspectFinanceGiftHandoffWithBudget(ctx, db, target, evidence[i])
		if err == nil && candidate.updated > 0 && writtenHours > 0 {
			err = wait(ctx, financeGiftScopeBatchInterval)
		}
		if err == nil {
			err = ctx.Err()
		}
		maxRows := len(evidence[i])
		if err == nil && guard != nil {
			maxRows, err = guard(ctx, i)
		}
		entry := financeGiftScopeBatchEntry{financeGiftScopeTarget: target, FinishedAt: time.Now().Unix()}
		if err == nil {
			entry, err = applyFinanceGiftHandoffWithCommitLimit(ctx, db, confirmation, i, target, evidence[i], entry.FinishedAt, maxRows)
		}
		if err != nil {
			entry = financeGiftScopeBatchEntry{financeGiftScopeTarget: target, Status: "failed", ErrorCode: "handoff_failed", FinishedAt: time.Now().Unix()}
			if reason := financeGiftHandoffPauseReason(err); reason != "" {
				entry.ErrorCode = reason
			}
		} else {
			result.Remaining--
			if entry.RowsUpdated > 0 {
				writtenHours++
			}
		}
		result.Entries = append(result.Entries, entry)
		if auditErr := audit(entry); auditErr != nil {
			result.Status = "audit_failed"
			return result, auditErr
		}
		if err != nil {
			result.Status = "failed"
			if ctx.Err() != nil || financeGiftHandoffPauseReason(err) != "" {
				result.Status = "paused"
			}
			return result, err
		}
	}
	result.Status = "complete"
	return result, nil
}
