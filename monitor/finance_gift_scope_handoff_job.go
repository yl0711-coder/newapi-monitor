//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"path/filepath"
)

const financeGiftHandoffReceiptName = "handoff.json"

// An offline receipt binds a NEW private receiver copy and its local evidence.
// It is an integrity confirmation, never permission to modify production.
type FinanceGiftHandoffReceipt struct {
	Version                int    `json:"version"`
	Mode                   string `json:"mode"`
	LocalPlanSHA256        string `json:"local_plan_sha256"`
	SourcePlanSHA256       string `json:"source_plan_sha256"`
	SourceSnapshotSHA256   string `json:"source_snapshot_sha256"`
	ReceiverSnapshotSHA256 string `json:"receiver_snapshot_sha256"`
}

func PrepareFinanceGiftLocalHandoff(ctx context.Context, sourceJob, sourceConfirmation, receiver, newJob string) (FinanceGiftHandoffReceipt, string, error) {
	var empty FinanceGiftHandoffReceipt
	preview, err := PreviewFinanceGiftLocalHandoff(ctx, sourceJob, sourceConfirmation, receiver)
	if err != nil {
		return empty, "", err
	}
	if preview.BlockedTargets != 0 {
		return empty, "", errors.New("handoff preflight blocked; no job created")
	}
	// Recheck under the source lock after preview; preparation must not silently
	// switch to newer evidence. The copy's hash also has to match the preview.
	lock, err := giftLocalLock(sourceJob)
	if err != nil {
		return empty, "", err
	}
	defer lock.Close()
	plan, _, err := giftLocalLoadConfirmedInputs(sourceJob, sourceConfirmation)
	if err != nil {
		return empty, "", err
	}
	actual, err := giftSeriesFileHash(ctx, filepath.Join(sourceJob, "usage-facts.db"))
	if err != nil || actual != preview.SourceSHA256 {
		return empty, "", errors.New("handoff source changed since preview")
	}
	paths := make([]string, len(plan.Targets))
	for i := range paths {
		paths[i] = filepath.Join(sourceJob, giftLocalEvidenceName(i))
	}
	local, digest, err := PrepareFinanceGiftLocalJob(ctx, receiver, newJob, paths)
	if err != nil {
		return empty, "", fmt.Errorf("prepare handoff receiver: %w", err)
	}
	if local.BackupSHA256 != preview.ReceiverSHA256 || len(local.Targets) != len(plan.Targets) {
		return empty, "", errors.New("handoff receiver changed during preparation; retain incomplete job for inspection")
	}
	for i := range local.Targets {
		if local.Targets[i] != plan.Targets[i] || local.EvidenceSHA256[i] != plan.EvidenceSHA256[i] || local.Rows[i] != plan.Rows[i] {
			return empty, "", errors.New("handoff copied plan differs from confirmed evidence")
		}
	}
	// Only this newly created offline copy receives the commit ledger.
	db, closeDB, err := giftLocalDatabase(filepath.Join(newJob, "usage-facts.db"))
	if err != nil {
		return empty, "", fmt.Errorf("reopen handoff receiver: %w", err)
	}
	err = db.WithContext(ctx).AutoMigrate(&financeGiftHandoffCommit{})
	closeDB()
	if err != nil {
		return empty, "", fmt.Errorf("prepare handoff ledger: %w", err)
	}
	receipt := FinanceGiftHandoffReceipt{2, "offline_receiver_copy_only", digest, sourceConfirmation, preview.SourceSHA256, preview.ReceiverSHA256}
	data, err := json.MarshalIndent(receipt, "", "  ")
	if err != nil {
		return empty, "", err
	}
	if err := giftLocalWriteNew(filepath.Join(newJob, financeGiftHandoffReceiptName), data); err != nil {
		return empty, "", err
	}
	return receipt, giftLocalDigest(data), nil
}

func loadFinanceGiftHandoffJob(dir, confirmation string) (FinanceGiftHandoffReceipt, FinanceGiftLocalPlan, []financeGiftLocalEvidence, error) {
	var receipt FinanceGiftHandoffReceipt
	var plan FinanceGiftLocalPlan
	data, err := giftLocalReadJSON(filepath.Join(dir, financeGiftHandoffReceiptName), &receipt)
	if err != nil {
		return receipt, plan, nil, err
	}
	if receipt.Version != 2 {
		return receipt, plan, nil, errors.New("handoff requires a newly prepared v2 job; retain legacy job unchanged")
	}
	if giftLocalDigest(data) != confirmation || receipt.Mode != "offline_receiver_copy_only" || !giftSeriesDigestValid(receipt.SourcePlanSHA256) || !giftSeriesDigestValid(receipt.SourceSnapshotSHA256) || !giftSeriesDigestValid(receipt.ReceiverSnapshotSHA256) {
		return receipt, plan, nil, errors.New("invalid handoff confirmation or receipt")
	}
	plan, evidence, err := giftLocalLoadConfirmedInputs(dir, receipt.LocalPlanSHA256)
	if err != nil {
		return receipt, plan, nil, err
	}
	if plan.BackupSHA256 != receipt.ReceiverSnapshotSHA256 {
		return receipt, plan, nil, errors.New("handoff receiver lineage mismatch")
	}
	return receipt, plan, evidence, nil
}
