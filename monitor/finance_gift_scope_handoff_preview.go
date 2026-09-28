//go:build unix

package monitor

import (
	"context"
	"errors"
	"path/filepath"

	"gorm.io/gorm"
)

type FinanceGiftHandoffPreviewEntry struct {
	financeGiftScopeTarget
	Status       string `json:"status"`
	Reason       string `json:"reason,omitempty"`
	EvidenceRows int    `json:"evidence_rows"`
	RowsToUpdate int    `json:"rows_to_update"`
}

type FinanceGiftHandoffPreview struct {
	Mode           string                           `json:"mode"`
	Status         string                           `json:"status"`
	PlanSHA256     string                           `json:"source_plan_sha256"`
	SourceSHA256   string                           `json:"source_snapshot_sha256"`
	ReceiverSHA256 string                           `json:"receiver_snapshot_sha256"`
	ReadyTargets   int                              `json:"ready_targets"`
	MatchedTargets int                              `json:"matched_targets"`
	BlockedTargets int                              `json:"blocked_targets"`
	RowsToUpdate   int                              `json:"rows_to_update"`
	Entries        []FinanceGiftHandoffPreviewEntry `json:"entries"`
}

// PreviewFinanceGiftLocalHandoff accepts only a confirmed, completed private
// job and a CLOSED local receiver snapshot. It never opens a writable database.
// Hashes identify these snapshots, not authorization to apply to a live store.
func PreviewFinanceGiftLocalHandoff(ctx context.Context, job, confirmation, receiver string) (FinanceGiftHandoffPreview, error) {
	var empty FinanceGiftHandoffPreview
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	var err error
	job, err = filepath.Abs(job)
	if err != nil {
		return empty, err
	}
	receiver, err = filepath.Abs(receiver)
	if err != nil {
		return empty, err
	}
	lock, err := giftLocalLock(job)
	if err != nil {
		return empty, err
	}
	defer lock.Close()
	plan, evidence, err := giftLocalLoadConfirmedInputs(job, confirmation)
	if err != nil {
		return empty, err
	}
	if err := giftLocalValidateAudit(filepath.Join(job, "audit.jsonl"), plan); err != nil {
		return empty, err
	}
	sourcePath := filepath.Join(job, "usage-facts.db")
	sourceHash, err := giftSeriesFileHash(ctx, sourcePath)
	if err != nil {
		return empty, err
	}
	receiverHash, err := giftSeriesFileHash(ctx, receiver)
	if err != nil {
		return empty, err
	}
	source, closeSource, err := giftLocalReadonlyDatabase(sourcePath)
	if err != nil {
		return empty, err
	}
	defer closeSource()
	verified, err := loadFinanceGiftHandoffCompletedEvidence(ctx, source, plan, evidence)
	if err != nil {
		return empty, err
	}
	db, closeReceiver, err := giftLocalReadonlyDatabase(receiver)
	if err != nil {
		return empty, err
	}
	defer closeReceiver()
	report, err := previewFinanceGiftHandoffTargets(ctx, db, plan.Targets, verified)
	if err != nil {
		return empty, err
	}
	// A caller violating the closed-snapshot requirement must not obtain a
	// seemingly valid report from changing files. No partial report on failure.
	for _, check := range []struct{ path, digest string }{{sourcePath, sourceHash}, {receiver, receiverHash}} {
		actual, err := giftSeriesFileHash(ctx, check.path)
		if err != nil {
			return empty, err
		}
		if actual != check.digest {
			return empty, errors.New("handoff snapshot changed during preview")
		}
	}
	report.PlanSHA256, report.SourceSHA256, report.ReceiverSHA256 = confirmation, sourceHash, receiverHash
	return report, nil
}

func loadFinanceGiftHandoffCompletedEvidence(ctx context.Context, source *gorm.DB, plan FinanceGiftLocalPlan, evidence []financeGiftLocalEvidence) ([][]FinanceGiftBoundaryEvent, error) {
	if err := giftLocalCheckEvidence(ctx, source, plan, evidence); err != nil {
		return nil, err
	}
	verified := make([][]FinanceGiftBoundaryEvent, len(plan.Targets))
	for i, target := range plan.Targets {
		proof, err := loadFinanceGiftScopeSnapshot(ctx, source, target.SourceEpoch, target.HourTs, target.UserID)
		if err != nil {
			return nil, err
		}
		for _, event := range proof.Events {
			if !event.GroupKnown {
				return nil, errors.New("handoff preview requires completed source evidence")
			}
		}
		verified[i] = proof.Events
	}
	return verified, nil
}

func previewFinanceGiftHandoffTargets(ctx context.Context, db *gorm.DB, targets []financeGiftScopeTarget, verified [][]FinanceGiftBoundaryEvent) (FinanceGiftHandoffPreview, error) {
	report := FinanceGiftHandoffPreview{Mode: "offline_handoff_preview_only", Status: "already_matched"}
	err := db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		for i, target := range targets {
			if err := ctx.Err(); err != nil {
				return err
			}
			candidate, checkErr := inspectFinanceGiftScopeHandoff(ctx, tx, target, verified[i])
			if err := ctx.Err(); err != nil {
				return err
			}
			entry := FinanceGiftHandoffPreviewEntry{financeGiftScopeTarget: target, EvidenceRows: len(verified[i])}
			if checkErr != nil {
				entry.Status = "blocked"
				entry.Reason = giftHandoffPreviewReason(checkErr)
				report.BlockedTargets++
			} else if candidate.updated == 0 {
				entry.Status = "already_matched"
				report.MatchedTargets++
			} else {
				entry.Status, entry.RowsToUpdate = "ready", candidate.updated
				report.ReadyTargets++
				report.RowsToUpdate += candidate.updated
			}
			report.Entries = append(report.Entries, entry)
		}
		return nil
	})
	if err != nil {
		return FinanceGiftHandoffPreview{}, err
	}
	if report.ReadyTargets > 0 {
		report.Status = "ready"
	}
	if report.BlockedTargets > 0 {
		report.Status = "blocked"
	}
	return report, nil
}

func giftHandoffPreviewReason(err error) string {
	switch {
	case errors.Is(err, gorm.ErrRecordNotFound):
		return "receiver_proof_missing"
	case errors.Is(err, errGiftHandoffAggregate):
		return "receiver_aggregate_mismatch"
	case errors.Is(err, errGiftHandoffConflict):
		return "evidence_conflict"
	default:
		return "receiver_unverifiable"
	}
}
