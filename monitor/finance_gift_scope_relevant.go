//go:build unix

package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
	"gorm.io/gorm"
)

// A relevant inspection measures blockers to gift accounting, not raw-history
// completeness. Its plan remains a bounded read suggestion, never execution
// authority. Existing raw inspect-backup and repair behavior are unchanged.
type FinanceGiftRelevantScopeInspection struct {
	Mode              string                       `json:"mode"`
	SourceEpoch       string                       `json:"source_epoch"`
	RelevantUserHours int64                        `json:"gift_relevant_user_hours"`
	ScopeSHA256       string                       `json:"business_scope_sha256"`
	Coverage          financeGiftCoverageView      `json:"gift_coverage"`
	Candidates        FinanceGiftLocalCandidates   `json:"candidates"`
	ReadPlan          *financegiftexport.BatchPlan `json:"read_plan,omitempty"`
	Confirmation      string                       `json:"confirm_read_plan_sha256,omitempty"`
}

func inspectFinanceGiftRelevantScope(ctx context.Context, m *Monitor, epoch string, seedFrom, to int64) (FinanceGiftRelevantScopeInspection, error) {
	var empty FinanceGiftRelevantScopeInspection
	if m == nil || m.storeDB == nil || epoch == "" || strings.TrimSpace(epoch) != epoch || len(epoch) > 64 || !validFinanceGiftRange(seedFrom, seedFrom, to) ||
		to > time.Now().Unix()/3600*3600 || to-seedFrom > financeReportMaxDays*24*3600 {
		return empty, errors.New("relevant inspection requires an explicit epoch and closed accounting range")
	}
	db := m.financeFactsReadStore()
	if db == nil {
		return empty, errors.New("finance facts store is unavailable")
	}
	var syncState FinanceFactSyncState
	if err := db.WithContext(ctx).First(&syncState, "id=?", financeFactSyncStateID).Error; err != nil {
		return empty, fmt.Errorf("read accounting origin: %w", err)
	}
	if syncState.StartHourTs != seedFrom {
		return empty, errors.New("inspection must begin at the recorded accounting origin; later starts cannot prove gift exhaustion")
	}
	var epochs int64
	if err := db.WithContext(ctx).Model(&FinanceUserHourState{}).Where("source_epoch=? AND hour_ts>=? AND hour_ts<?", epoch, seedFrom, to).Count(&epochs).Error; err != nil {
		return empty, err
	}
	if epochs == 0 {
		return empty, errors.New("source epoch not present in requested accounting range")
	}
	policies, err := loadChannelBusinessGroupPolicies(ctx, m.storeDB)
	if err != nil {
		return empty, err
	}
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		return empty, err
	}
	excluded := make(map[int64]bool, len(accounts))
	for _, account := range accounts {
		excluded[account.UserID] = true
	}
	allocation, err := m.loadFinanceGiftAllocationForScope(ctx, seedFrom, seedFrom, to, policies, excluded)
	if err != nil {
		return empty, err // Never emit partly verified targets after a content error.
	}
	result := FinanceGiftRelevantScopeInspection{Mode: "offline_accounting_scope_plan_no_source_access", SourceEpoch: epoch,
		ScopeSHA256: financeGiftEvidenceScopeKey(policies, excluded), Coverage: allocation.Coverage, RelevantUserHours: allocation.scopeGapUserHours}
	if !allocation.Coverage.Complete && allocation.Coverage.ScopeUnknownEvents == 0 {
		result.Candidates = FinanceGiftLocalCandidates{Mode: "offline_suggestion_only", Status: "blocked", SourceEpoch: epoch, StopReason: "monetary_or_hour_proof_incomplete"}
		return result, nil
	}
	result.Candidates, err = giftRelevantLocalCandidates(ctx, db, allocation.scopeGapStates, epoch, time.Now().Unix())
	if err != nil {
		return empty, err
	}
	if result.Candidates.Status == "ready" {
		plan, digest, err := giftScopeReadPlanFromCandidates(result.Candidates)
		if err != nil {
			return empty, err
		}
		result.ReadPlan, result.Confirmation = &plan, digest
	}
	return result, nil
}

// The allocator retains the earliest verified gaps independent of cache hit
// order. Bound whole-hour reads with the existing export limits and verifier.
func giftRelevantLocalCandidates(ctx context.Context, db *gorm.DB, states []FinanceGiftBoundaryState, epoch string, now int64) (FinanceGiftLocalCandidates, error) {
	result := FinanceGiftLocalCandidates{Mode: "offline_suggestion_only", Status: "ready", SourceEpoch: epoch,
		RowLimit: financeGiftLocalMaxRows, TargetLimit: financeGiftScopeBatchLimit, StopReason: "gift_relevant_scope_exhausted"}
	if len(states) == 0 {
		result.Status = "empty"
		return result, ctx.Err()
	}
	for _, state := range states {
		if err := ctx.Err(); err != nil {
			return result, err
		}
		if len(result.Entries) == financeGiftScopeBatchLimit {
			result.StopReason = "target_limit"
			break
		}
		target := financeGiftScopeTarget{SourceEpoch: state.SourceEpoch, HourTs: state.HourTs, UserID: state.UserID}
		if state.SourceEpoch != epoch || state.Rows > financeGiftLocalMaxRows {
			result.StopReason, result.Blocked = "source_epoch_boundary", &target
			if state.SourceEpoch == epoch {
				result.StopReason = "whole_hour_exceeds_row_limit"
			}
			if len(result.Entries) == 0 {
				result.Status = "blocked"
			}
			break
		}
		if err := validateFinanceGiftScopeBatch([]financeGiftScopeTarget{target}, now); err != nil {
			return result, err
		}
		if state.Rows > int64(financeGiftLocalMaxRows-result.Rows) {
			result.StopReason = "row_budget"
			break
		}
		entry, err := giftLocalVerifyCandidate(ctx, db, target)
		if err != nil {
			return result, fmt.Errorf("verify relevant gift target user %d hour %d: %w", target.UserID, target.HourTs, err)
		}
		if entry.Rows != int(state.Rows) || entry.ContentHash != state.ContentHash {
			return result, financeFactChange("gift-scope-plan", "candidate-proof")
		}
		result.Rows += entry.Rows
		result.UnknownRows += entry.UnknownRows
		result.Entries = append(result.Entries, entry)
	}
	return result, nil
}
