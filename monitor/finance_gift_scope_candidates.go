//go:build unix

package monitor

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"time"

	"gorm.io/gorm"
)

// FinanceGiftLocalCandidates is a bounded suggestion, NOT an executable plan
// or a measurement of overall financial coverage. Original evidence is still
// required before PrepareFinanceGiftLocalJob can create another repair job.
type FinanceGiftLocalCandidates struct {
	Mode        string                      `json:"mode"`
	Status      string                      `json:"status"`
	SourceEpoch string                      `json:"source_epoch,omitempty"`
	StopReason  string                      `json:"stop_reason,omitempty"`
	RowLimit    int                         `json:"row_limit"`
	TargetLimit int                         `json:"target_limit"`
	Rows        int                         `json:"rows"`
	UnknownRows int                         `json:"unknown_rows"`
	Entries     []financeGiftLocalCandidate `json:"entries,omitempty"`
	Blocked     *financeGiftScopeTarget     `json:"blocked_target,omitempty"`
}

type financeGiftLocalCandidate struct {
	financeGiftScopeTarget
	Rows        int    `json:"expected_rows"`
	UnknownRows int    `json:"unknown_rows"`
	ContentHash string `json:"local_content_hash"`
}

// SuggestFinanceGiftLocalCandidates reads only a closed, locked local job.
// Its confirmed plan pins the source epoch; other epochs are not selected.
func SuggestFinanceGiftLocalCandidates(ctx context.Context, dir, confirmation string) (FinanceGiftLocalCandidates, error) {
	rejected := FinanceGiftLocalCandidates{Mode: "offline_suggestion_only", Status: "unverified"}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return rejected, err
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		return rejected, err
	}
	defer lock.Close()
	plan, evidence, err := giftLocalLoadConfirmedInputs(dir, confirmation)
	if err != nil {
		return rejected, err
	}
	if err = giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), plan); err != nil {
		return rejected, err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return rejected, err
	}
	defer closeDB()
	if err = giftLocalCheckEvidence(ctx, db, plan, evidence); err != nil {
		return rejected, err
	}
	result, err := giftLocalSelectCandidates(ctx, db, plan.Targets[0].SourceEpoch, time.Now().Unix())
	if err != nil {
		return rejected, err // Do not expose a partly verified suggestion.
	}
	return result, nil
}

func giftLocalSelectCandidates(ctx context.Context, db *gorm.DB, epoch string, now int64) (FinanceGiftLocalCandidates, error) {
	result := FinanceGiftLocalCandidates{Mode: "offline_suggestion_only", Status: "ready", SourceEpoch: epoch,
		RowLimit: financeGiftLocalMaxRows, TargetLimit: financeGiftScopeBatchLimit, StopReason: "source_scope_exhausted"}
	var states []FinanceGiftBoundaryState
	// One extra candidate distinguishes the target limit from exhaustion.
	// Do not filter out broken/incomplete states: their gaps must stop the list.
	err := db.WithContext(ctx).Table("finance_gift_boundary_states AS s").Select("s.*").
		Where(`s.source_epoch=? AND EXISTS (SELECT 1 FROM finance_gift_boundary_events e
WHERE e.source_epoch=s.source_epoch AND e.hour_ts=s.hour_ts AND e.user_id=s.user_id AND COALESCE(e.group_known,0)=0)`, epoch).
		Order("s.hour_ts,s.user_id").Limit(financeGiftScopeBatchLimit + 1).Find(&states).Error
	if err != nil {
		return result, err
	}
	for _, state := range states {
		target := financeGiftScopeTarget{SourceEpoch: epoch, HourTs: state.HourTs, UserID: state.UserID}
		if len(result.Entries) == financeGiftScopeBatchLimit {
			result.StopReason = "target_limit"
			break
		}
		if err = validateFinanceGiftScopeBatch([]financeGiftScopeTarget{target}, now); err != nil {
			return result, err
		}
		if state.Rows > financeGiftLocalMaxRows {
			result.Status, result.StopReason, result.Blocked = "blocked", "whole_hour_exceeds_row_limit", &target
			break // Never split an hour or skip it to make progress look better.
		}
		if state.Rows > int64(financeGiftLocalMaxRows-result.Rows) {
			result.StopReason = "row_budget"
			break
		}
		entry, err := giftLocalVerifyCandidate(ctx, db, target)
		if err != nil {
			return result, fmt.Errorf("verify candidate user %d hour %d: %w", target.UserID, target.HourTs, err)
		}
		result.Rows += entry.Rows
		result.UnknownRows += entry.UnknownRows
		result.Entries = append(result.Entries, entry)
	}
	if len(states) == 0 {
		result.Status = "empty"
	}
	return result, nil
}

func giftLocalVerifyCandidate(ctx context.Context, db *gorm.DB, target financeGiftScopeTarget) (financeGiftLocalCandidate, error) {
	entry := financeGiftLocalCandidate{financeGiftScopeTarget: target}
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
	if err != nil {
		return entry, err
	}
	var fact FinanceUserHourFact
	if err := db.WithContext(ctx).First(&fact, "hour_ts=? AND user_id=?", target.HourTs, target.UserID).Error; err != nil {
		return entry, err
	}
	var requests, refunds, consume, refund int64
	for _, event := range prior.Events {
		if event.SourceLogID <= 0 || event.Quota < 0 || event.EventAt < target.HourTs || event.EventAt >= target.HourTs+3600 {
			return entry, errors.New("candidate has invalid monetary event identity")
		}
		switch event.Kind {
		case "usage":
			requests++
			err = addEconomicsInt64(&consume, event.Quota)
		case "refund":
			refunds++
			err = addEconomicsInt64(&refund, event.Quota)
		default:
			return entry, errors.New("candidate has invalid monetary event kind")
		}
		if err != nil {
			return entry, err
		}
		if !event.GroupKnown {
			entry.UnknownRows++
		}
	}
	s := prior.State
	if requests != s.Requests || refunds != s.RefundRecords || consume != s.ConsumeQuota || refund != s.RefundQuota ||
		requests != fact.Requests || refunds != fact.RefundRecords || consume != fact.ConsumeQuota || refund != fact.RefundQuota || entry.UnknownRows == 0 {
		return entry, errors.New("candidate does not reconcile with complete monetary facts")
	}
	entry.Rows, entry.ContentHash = len(prior.Events), prior.State.ContentHash
	return entry, nil
}
