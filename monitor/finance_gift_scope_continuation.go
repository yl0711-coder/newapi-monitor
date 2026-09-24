//go:build unix

package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// PrepareFinanceGiftLocalContinuation verifies a completed private job and a
// separately confirmed, complete read-only export before creating the next
// private job. It neither contacts the source nor runs the new repair plan.
func PrepareFinanceGiftLocalContinuation(ctx context.Context, priorDir, priorConfirmation, exportDir, readConfirmation, nextDir string) (FinanceGiftLocalPlan, string, error) {
	var empty FinanceGiftLocalPlan
	if err := ctx.Err(); err != nil {
		return empty, "", err
	}
	priorDir, err := filepath.Abs(priorDir)
	if err != nil {
		return empty, "", err
	}
	exportDir, err = filepath.Abs(exportDir)
	if err != nil {
		return empty, "", err
	}
	nextDir, err = filepath.Abs(nextDir)
	if err != nil {
		return empty, "", err
	}
	if nextDir == priorDir || nextDir == exportDir {
		return empty, "", errors.New("next job requires a distinct private directory")
	}
	lock, err := giftLocalLock(priorDir)
	if err != nil {
		return empty, "", err
	}
	defer lock.Close()
	if _, err := os.Lstat(filepath.Join(priorDir, "continuation-intent.json")); !os.IsNotExist(err) {
		return empty, "", errors.New("previous offline job already has a continuation intent; inspect it before proceeding")
	}
	priorPlan, priorEvidence, err := giftLocalLoadConfirmedInputs(priorDir, priorConfirmation)
	if err != nil {
		return empty, "", err
	}
	if err = giftLocalValidateAudit(filepath.Join(priorDir, "audit.jsonl"), priorPlan); err != nil {
		return empty, "", err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(priorDir, "usage-facts.db"))
	if err != nil {
		return empty, "", err
	}
	defer closeDB()
	if err = giftLocalCheckEvidence(ctx, db, priorPlan, priorEvidence); err != nil {
		return empty, "", err
	}
	for _, target := range priorPlan.Targets {
		proof, readErr := loadFinanceGiftScopeSnapshot(ctx, db, target.SourceEpoch, target.HourTs, target.UserID)
		if readErr != nil {
			return empty, "", readErr
		}
		for _, event := range proof.Events {
			if !event.GroupKnown {
				return empty, "", errors.New("previous offline job is not complete")
			}
		}
	}
	suggestion, err := giftLocalSelectCandidates(ctx, db, priorPlan.Targets[0].SourceEpoch, time.Now().Unix())
	if err != nil {
		return empty, "", err
	}
	if suggestion.Status != "ready" || len(suggestion.Entries) == 0 {
		return empty, "", errors.New("next offline batch has no verified, unblocked candidates")
	}
	readPlan := financegiftexport.BatchPlan{SourceEpoch: suggestion.SourceEpoch}
	for _, candidate := range suggestion.Entries {
		readPlan.Targets = append(readPlan.Targets, financegiftexport.BatchTarget{
			UserID: candidate.UserID, HourTs: candidate.HourTs,
			ExpectedRows: candidate.Rows, LocalContentHash: candidate.ContentHash,
		})
	}
	digest, err := financegiftexport.BatchConfirmation(readPlan)
	if err != nil || digest != readConfirmation {
		return empty, "", errors.New("read plan changed or confirmation mismatch")
	}
	info, err := os.Lstat(exportDir)
	if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
		return empty, "", errors.New("export directory must be a private regular directory")
	}
	var exportedPlan financegiftexport.BatchPlan
	if _, err = giftLocalReadJSON(filepath.Join(exportDir, "plan.json"), &exportedPlan); err != nil || !reflect.DeepEqual(exportedPlan, readPlan) {
		return empty, "", errors.New("exported plan differs from current verified suggestion")
	}
	var result financegiftexport.BatchResult
	resultBytes, readErr := giftLocalReadJSON(filepath.Join(exportDir, "result.json"), &result)
	if readErr != nil ||
		result.Status != "complete" || result.Remaining != 0 || len(result.Entries) != len(readPlan.Targets) {
		return empty, "", errors.New("source export did not complete every confirmed target")
	}
	var manifest []string
	if _, err = giftLocalReadJSON(filepath.Join(exportDir, "evidence-manifest.json"), &manifest); err != nil || len(manifest) != len(readPlan.Targets) {
		return empty, "", errors.New("source export manifest is missing or incomplete")
	}
	evidencePaths := make([]string, len(readPlan.Targets))
	for i, target := range readPlan.Targets {
		path := filepath.Join(exportDir, giftLocalEvidenceName(i))
		entry := result.Entries[i]
		if entry.BatchTarget != target || entry.File != giftLocalEvidenceName(i) || manifest[i] != path {
			return empty, "", fmt.Errorf("source export target %d identity differs from confirmed plan", i+1)
		}
		var evidence financeGiftLocalEvidence
		data, readErr := giftLocalReadJSON(path, &evidence)
		if readErr != nil || giftLocalDigest(data) != entry.SHA256 || evidence.validate() != nil ||
			evidence.UserID != target.UserID || evidence.Hour != target.HourTs || len(evidence.Rows) != target.ExpectedRows {
			return empty, "", fmt.Errorf("source export target %d evidence invalid or changed", i+1)
		}
		evidencePaths[i] = path
	}
	if _, err := os.Lstat(nextDir); !os.IsNotExist(err) {
		return empty, "", errors.New("next offline job directory already exists or cannot be checked")
	}
	intent, err := json.Marshal(struct {
		Version        int    `json:"version"`
		NextDir        string `json:"next_job_dir"`
		ReadPlanSHA256 string `json:"read_plan_sha256"`
		ResultSHA256   string `json:"result_sha256"`
	}{1, nextDir, readConfirmation, giftLocalDigest(resultBytes)})
	if err != nil {
		return empty, "", err
	}
	// This durable, no-clobber intent prevents two child jobs from silently
	// forking the same verified predecessor. An interrupted handoff remains
	// blocked for explicit inspection rather than guessing which child to use.
	if err := giftLocalWriteNew(filepath.Join(priorDir, "continuation-intent.json"), intent); err != nil {
		return empty, "", err
	}
	// Prepare copies only the closed local database and the verified evidence.
	// A separate explicit 'run' with the NEW plan digest is still required.
	nextPlan, nextDigest, err := PrepareFinanceGiftLocalJob(ctx, filepath.Join(priorDir, "usage-facts.db"), nextDir, evidencePaths)
	if err != nil {
		return empty, "", err
	}
	for i, entry := range result.Entries {
		if nextPlan.EvidenceSHA256[i] != entry.SHA256 {
			return empty, "", errors.New("source evidence changed during offline handoff; do not run new job")
		}
	}
	completion, err := json.Marshal(struct {
		Version        int    `json:"version"`
		NextPlanSHA256 string `json:"next_plan_sha256"`
	}{1, nextDigest})
	if err != nil {
		return empty, "", err
	}
	if err := giftLocalWriteNew(filepath.Join(priorDir, "continuation-complete.json"), completion); err != nil {
		return empty, "", err
	}
	return nextPlan, nextDigest, nil
}
