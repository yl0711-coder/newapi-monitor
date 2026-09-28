//go:build unix

package monitor

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"os"

	"gorm.io/gorm"
)

// Local-only outbox: never registered with Monitor's schema migrations.
// One immutable successful mutation per confirmed job/target, in the same
// transaction as the facts. JSONL is a recoverable projection of these commits.
type financeGiftHandoffCommit struct {
	Job   string `gorm:"primaryKey"`
	Index int    `gorm:"primaryKey"`
	Entry string
}

func (financeGiftHandoffCommit) TableName() string { return "local_gift_handoff_commits" }

func applyFinanceGiftHandoffWithCommit(ctx context.Context, db *gorm.DB, job string, index int, target financeGiftScopeTarget, evidence []FinanceGiftBoundaryEvent, now int64) (financeGiftScopeBatchEntry, error) {
	return applyFinanceGiftHandoffWithCommitLimit(ctx, db, job, index, target, evidence, now, len(evidence))
}

func applyFinanceGiftHandoffWithCommitLimit(ctx context.Context, db *gorm.DB, job string, index int, target financeGiftScopeTarget, evidence []FinanceGiftBoundaryEvent, now int64, maxRows int) (financeGiftScopeBatchEntry, error) {
	ctx, cancel := context.WithTimeout(ctx, financeGiftHandoffWriteBudget)
	defer cancel()
	entry := financeGiftScopeBatchEntry{financeGiftScopeTarget: target, Status: "unchanged", FinishedAt: now}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		repaired, err := applyFinanceGiftScopeHandoff(ctx, tx, target, evidence, financeGiftBoundaryContentHash(evidence), now)
		if err != nil {
			return err
		}
		if repaired.RowsUpdated > maxRows {
			return errors.New("repair exceeds approved row allowance")
		}
		entry.RowsChecked, entry.RowsUpdated, entry.ContentHash = repaired.RowsChecked, repaired.RowsUpdated, repaired.ContentHash
		if repaired.RowsUpdated == 0 {
			return nil
		}
		entry.Status = "repaired"
		data, err := json.Marshal(entry)
		if err != nil {
			return err
		}
		// No upsert: a second mutation of the same job/target is a conflict.
		return tx.Create(&financeGiftHandoffCommit{Job: job, Index: index, Entry: string(data)}).Error
	})
	if err != nil {
		return financeGiftScopeBatchEntry{}, err
	}
	return entry, nil
}

// Called after the whole receiver preflight and before any further mutation.
// Validate every commit and existing repair audit before appending anything.
// A partial JSONL line is rejected by giftLocalValidateAudit, not truncated.
func recoverFinanceGiftHandoffAudit(ctx context.Context, db *gorm.DB, job, auditPath string, plan FinanceGiftLocalPlan, appendEntry func(financeGiftScopeBatchEntry) error) error {
	var commits []financeGiftHandoffCommit
	if err := db.WithContext(ctx).Where("job = ?", job).Order("\"index\"").Limit(len(plan.Targets) + 1).Find(&commits).Error; err != nil {
		return err
	}
	if len(commits) > len(plan.Targets) {
		return errors.New("handoff commit count exceeds plan")
	}
	byTarget := make(map[financeGiftScopeTarget]financeGiftScopeBatchEntry, len(commits))
	entries := make([]financeGiftScopeBatchEntry, 0, len(commits))
	for _, commit := range commits {
		var entry financeGiftScopeBatchEntry
		if commit.Index < 0 || commit.Index >= len(plan.Targets) || giftLocalDecodeJSON([]byte(commit.Entry), &entry) != nil ||
			entry.financeGiftScopeTarget != plan.Targets[commit.Index] || entry.Status != "repaired" || entry.ErrorCode != "" ||
			entry.RowsChecked != plan.Rows[commit.Index] || entry.RowsUpdated <= 0 || entry.RowsUpdated > entry.RowsChecked || entry.FinishedAt <= entry.HourTs+3600 {
			return errors.New("invalid handoff commit")
		}
		proof, err := loadFinanceGiftScopeSnapshot(ctx, db, entry.SourceEpoch, entry.HourTs, entry.UserID)
		if err != nil {
			return err
		}
		if proof.State.ContentHash != entry.ContentHash {
			return errors.New("handoff committed facts changed")
		}
		for _, event := range proof.Events {
			if !event.GroupKnown {
				return errors.New("handoff commit refers to unfinished facts")
			}
		}
		byTarget[entry.financeGiftScopeTarget] = entry
		entries = append(entries, entry)
	}
	f, err := giftLocalOpenRegular(auditPath, os.O_RDONLY)
	if err != nil {
		return err
	}
	defer f.Close()
	seen := map[financeGiftScopeTarget]bool{}
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		var entry financeGiftScopeBatchEntry
		if err := giftLocalDecodeJSON(scanner.Bytes(), &entry); err != nil {
			return err
		}
		if entry.Status != "repaired" {
			continue
		}
		committed, ok := byTarget[entry.financeGiftScopeTarget]
		if !ok || entry != committed || seen[entry.financeGiftScopeTarget] {
			return errors.New("handoff audit differs from durable commit")
		}
		seen[entry.financeGiftScopeTarget] = true
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	for _, entry := range entries {
		if seen[entry.financeGiftScopeTarget] {
			continue
		}
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := appendEntry(entry); err != nil {
			return err
		}
	}
	return nil
}
