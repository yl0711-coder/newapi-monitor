package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
)

const (
	financeGiftScopeBatchLimit    = 10
	financeGiftScopeBatchInterval = 10 * time.Second
)

type financeGiftScopeTarget struct {
	SourceEpoch string `json:"source_epoch"`
	HourTs      int64  `json:"hour_ts"`
	UserID      int64  `json:"user_id"`
}

type financeGiftScopeBatchEntry struct {
	financeGiftScopeTarget
	Status      string `json:"status"`
	RowsChecked int    `json:"rows_checked"`
	RowsUpdated int    `json:"rows_updated"`
	ContentHash string `json:"content_hash,omitempty"`
	FinishedAt  int64  `json:"finished_at"`
	ErrorCode   string `json:"error_code,omitempty"`
}

type financeGiftScopeBatchResult struct {
	Status  string
	Entries []financeGiftScopeBatchEntry
	// Resume repeats the explicit plan. Verified facts are the checkpoint:
	// a crash after commit but before audit cannot lead to double allocation.
	Remaining int
}

// A runner is owned by one repair worker. It is intentionally not connected
// to Monitor startup or a production endpoint. Use the same runner across
// batches to preserve cooldown, and a durable audit sink before enabling it.
type financeGiftScopeBatchRunner struct {
	db          *gorm.DB
	source      financeSourceQuerier
	mu          sync.Mutex
	nextAttempt time.Time
	now         func() time.Time
	wait        func(context.Context, time.Duration) error
}

func newFinanceGiftScopeBatchRunner(db *gorm.DB, source financeSourceQuerier) *financeGiftScopeBatchRunner {
	return &financeGiftScopeBatchRunner{db: db, source: source, now: time.Now, wait: waitFinanceGiftScopeBatch}
}

func waitFinanceGiftScopeBatch(ctx context.Context, delay time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if delay <= 0 {
		return nil
	}
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func validateFinanceGiftScopeBatch(targets []financeGiftScopeTarget, now int64) error {
	if len(targets) == 0 || len(targets) > financeGiftScopeBatchLimit {
		return errors.New("gift scope batch must contain 1 to 10 explicit user-hours")
	}
	seen := map[financeGiftScopeTarget]bool{}
	for _, target := range targets {
		if target.SourceEpoch == "" || len(target.SourceEpoch) > 64 || strings.TrimSpace(target.SourceEpoch) != target.SourceEpoch ||
			target.SourceEpoch != targets[0].SourceEpoch || target.HourTs < 0 || target.HourTs%3600 != 0 || target.HourTs >= now-3600 || target.UserID <= 0 || seen[target] {
			return errors.New("invalid, duplicate, open-hour or mixed-source gift scope batch target")
		}
		seen[target] = true
	}
	return nil
}

func (r *financeGiftScopeBatchRunner) run(ctx context.Context, targets []financeGiftScopeTarget, audit func(financeGiftScopeBatchEntry) error) (financeGiftScopeBatchResult, error) {
	result := financeGiftScopeBatchResult{Status: "rejected", Remaining: len(targets)}
	if r == nil || r.db == nil || audit == nil {
		return result, errors.New("gift scope batch requires local database and audit sink")
	}
	if !r.mu.TryLock() {
		return result, errors.New("gift scope batch already running")
	}
	defer r.mu.Unlock()
	// Freeze caller-owned targets before any asynchronous waiting.
	plan := append([]financeGiftScopeTarget(nil), targets...)
	if err := validateFinanceGiftScopeBatch(plan, r.now().Unix()); err != nil {
		return result, err
	}
	for _, target := range plan {
		if err := r.wait(ctx, r.nextAttempt.Sub(r.now())); err != nil {
			result.Status = "paused"
			return result, err
		}
		if err := ctx.Err(); err != nil {
			result.Status = "paused"
			return result, err
		}
		repair, repairErr := repairFinanceGiftBoundaryScope(ctx, r.db, r.source, target.SourceEpoch, target.HourTs, target.UserID, r.now().Unix())
		r.nextAttempt = r.now().Add(financeGiftScopeBatchInterval)
		entry := financeGiftScopeBatchEntry{financeGiftScopeTarget: target, RowsChecked: repair.RowsChecked, RowsUpdated: repair.RowsUpdated, ContentHash: repair.ContentHash, FinishedAt: r.now().Unix(), Status: "unchanged"}
		if repair.RowsUpdated > 0 {
			entry.Status = "repaired"
		}
		if repairErr != nil {
			entry.Status = "failed"
			entry.ErrorCode = "repair_failed"
		}
		// Record the committed outcome even if cancellation arrives immediately
		// after commit. An audit failure stops all further work, not a rollback
		// of already committed facts. Never persist driver errors or credentials.
		result.Entries = append(result.Entries, entry)
		if repairErr == nil {
			result.Remaining--
		}
		if err := audit(entry); err != nil {
			result.Status = "audit_failed"
			return result, fmt.Errorf("gift scope batch audit failed; verify last committed target: %w", err)
		}
		if repairErr != nil {
			result.Status = "failed"
			if ctx.Err() != nil {
				result.Status = "paused"
			}
			return result, repairErr // No hot retry loop or silent skip on money evidence.
		}
	}
	result.Status = "complete"
	return result, nil
}
