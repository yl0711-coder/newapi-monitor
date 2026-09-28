//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
	"gorm.io/gorm"
)

// Separate job format: the ordinary 3000-row runner cannot accept this job.
type FinanceGiftLargeLocalPlan struct {
	Version        int    `json:"version"`
	BackupSHA256   string `json:"backup_sha256"`
	ReadPlanSHA256 string `json:"read_plan_sha256"`
	EvidenceSHA256 string `json:"evidence_sha256"`
}

// PrepareFinanceGiftLargeLocalJob creates a NEW private copy only. No source
// connection, credentials, live database path or automatic execution exists.
func PrepareFinanceGiftLargeLocalJob(ctx context.Context, backup, readPath, evidencePath, dir, readConfirmation string) (FinanceGiftLargeLocalPlan, string, error) {
	var job FinanceGiftLargeLocalPlan
	var plan financegiftexport.LargeHourPlan
	readData, err := giftLocalReadJSON(readPath, &plan)
	if err != nil {
		return job, "", err
	}
	digest, err := financegiftexport.LargeHourConfirmation(plan)
	if err != nil || digest != readConfirmation {
		return job, "", errors.New("large-hour read plan confirmation mismatch")
	}
	var evidence financeGiftLargeEvidence
	evidenceData, err := giftLocalReadJSON(evidencePath, &evidence)
	if err != nil {
		return job, "", err
	}
	events, err := validateFinanceGiftLargeEvidence(plan, evidence)
	if err != nil {
		return job, "", err
	}
	if err := ctx.Err(); err != nil {
		return job, "", err
	}
	dir, err = filepath.Abs(dir)
	if err != nil {
		return job, "", err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return job, "", err
	}
	if err := giftLocalWriteNew(filepath.Join(dir, "job.lock"), nil); err != nil {
		return job, "", err
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		return job, "", err
	}
	defer lock.Close()
	job = FinanceGiftLargeLocalPlan{Version: 1, ReadPlanSHA256: digest, EvidenceSHA256: giftLocalDigest(evidenceData)}
	job.BackupSHA256, err = giftLocalCopyBackup(backup, filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return job, "", err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return job, "", err
	}
	defer closeDB()
	if _, _, _, err := checkFinanceGiftLargeLedger(ctx, db, plan, events, false); err != nil {
		return job, "", err
	}
	for _, file := range []struct {
		name string
		data []byte
	}{{"read-plan.json", readData}, {"evidence.json", evidenceData}, {"audit.jsonl", nil}} {
		if err := giftLocalWriteNew(filepath.Join(dir, file.name), file.data); err != nil {
			return job, "", err
		}
	}
	data, err := json.Marshal(job)
	if err != nil {
		return job, "", err
	}
	// Publish executable job metadata last. Failed preparation stays inert.
	if err := giftLocalWriteNew(filepath.Join(dir, "large-plan.json"), data); err != nil {
		return job, "", err
	}
	return job, giftLocalDigest(data), nil
}

func loadFinanceGiftLargeLocalInputs(dir, confirmation string) (financegiftexport.LargeHourPlan, []FinanceGiftBoundaryEvent, error) {
	var job FinanceGiftLargeLocalPlan
	var plan financegiftexport.LargeHourPlan
	data, err := giftLocalReadJSON(filepath.Join(dir, "large-plan.json"), &job)
	if err != nil || giftLocalDigest(data) != confirmation || job.Version != 1 {
		return plan, nil, errors.New("large-hour job confirmation mismatch")
	}
	if _, err := giftLocalReadJSON(filepath.Join(dir, "read-plan.json"), &plan); err != nil {
		return plan, nil, err
	}
	digest, err := financegiftexport.LargeHourConfirmation(plan)
	if err != nil || digest != job.ReadPlanSHA256 {
		return plan, nil, errors.New("large-hour read plan changed")
	}
	var evidence financeGiftLargeEvidence
	data, err = giftLocalReadJSON(filepath.Join(dir, "evidence.json"), &evidence)
	if err != nil || giftLocalDigest(data) != job.EvidenceSHA256 {
		return plan, nil, errors.New("large-hour evidence changed")
	}
	events, err := validateFinanceGiftLargeEvidence(plan, evidence)
	return plan, events, err
}

// RunFinanceGiftLargeLocalJob writes only its locked private job copy. All
// pages are already local: no network access or source cooldown is needed.
func RunFinanceGiftLargeLocalJob(parent context.Context, dir, confirmation string) (financeGiftScopeBatchResult, error) {
	result := financeGiftScopeBatchResult{Status: "rejected", Remaining: 1}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return result, err
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	plan, events, err := loadFinanceGiftLargeLocalInputs(dir, confirmation)
	if err != nil {
		return result, err
	}
	target := financeGiftScopeTarget{SourceEpoch: plan.SourceEpoch, HourTs: plan.HourTs, UserID: plan.UserID}
	if err := giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), FinanceGiftLocalPlan{Targets: []financeGiftScopeTarget{target}}); err != nil {
		return result, err
	}
	journal, err := giftLocalOpenRegular(filepath.Join(dir, "audit.jsonl"), os.O_WRONLY|os.O_APPEND)
	if err != nil {
		return result, err
	}
	defer journal.Close()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return result, err
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return result, err
	}
	defer closeDB()
	entry := financeGiftScopeBatchEntry{financeGiftScopeTarget: target, Status: "unchanged", RowsChecked: len(events)}
	err = db.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		prior, merged, updated, err := checkFinanceGiftLargeLedger(ctx, tx, plan, events, true)
		if err != nil {
			return err
		}
		entry.ContentHash = prior.State.ContentHash
		if updated == 0 {
			return nil
		}
		state, err := replaceFinanceGiftBoundaryUserHour(ctx, tx, plan.HourTs, plan.UserID, max(time.Now().Unix(), prior.State.UpdatedAt+1), plan.SourceEpoch, merged)
		if err != nil {
			return err
		}
		entry.RowsUpdated, entry.ContentHash, entry.Status = updated, state.ContentHash, "repaired"
		return nil
	})
	if err != nil {
		result.Status = "failed"
		return result, err // Transaction rolled back; never report a partial repair.
	}
	entry.FinishedAt = time.Now().Unix()
	result.Status, result.Remaining, result.Entries = "complete", 0, []financeGiftScopeBatchEntry{entry}
	if err := json.NewEncoder(journal).Encode(entry); err != nil {
		result.Status = "audit_failed"
		return result, err
	}
	if err := journal.Sync(); err != nil {
		result.Status = "audit_failed"
		return result, err
	}
	return result, nil
}
