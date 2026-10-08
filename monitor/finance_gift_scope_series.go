//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

type FinanceGiftSeriesResult struct {
	Mode         string `json:"mode"`
	Status       string `json:"status"`
	Completed    int    `json:"completed_batches"`
	Total        int    `json:"total_batches"`
	Updated      int    `json:"rows_updated_this_run"`
	JobDir       string `json:"last_job_dir"`
	Confirmation string `json:"last_plan_sha256"`
}

func RunFinanceGiftLocalSeries(ctx context.Context, path, confirmation string) (FinanceGiftSeriesResult, error) {
	return runFinanceGiftLocalSeries(ctx, path, confirmation, waitFinanceGiftScopeBatch)
}

func runFinanceGiftLocalSeries(parent context.Context, path, confirmation string, wait func(context.Context, time.Duration) error) (FinanceGiftSeriesResult, error) {
	result := FinanceGiftSeriesResult{Mode: "offline_finite_series_only", Status: "rejected"}
	plan, preview, lock, err := loadFinanceGiftSeries(path)
	if err != nil {
		return result, err
	}
	defer lock.Close()
	if preview.Confirmation != confirmation {
		return result, errors.New("series confirmation mismatch")
	}
	ctx, cancel := context.WithTimeout(parent, time.Duration(plan.RuntimeSeconds)*time.Second)
	defer cancel()
	prior, digest := plan.PriorDir, plan.PriorSHA256
	progress, err := InspectFinanceGiftLocalJob(ctx, prior, digest)
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			result.Status = "paused"
		}
		return result, err
	}
	if progress.Status != "complete" {
		return result, errors.New("series predecessor must be verified complete")
	}
	result.Total = len(plan.Steps)
	result.JobDir, result.Confirmation = prior, digest
	for _, step := range plan.Steps {
		if err = ctx.Err(); err == nil {
			digest, err = financeGiftSeriesChild(ctx, prior, digest, step)
		}
		if err == nil {
			result.JobDir, result.Confirmation = step.NextDir, digest
			progress, err = InspectFinanceGiftLocalJob(ctx, step.NextDir, digest)
		}
		if err == nil && progress.Status != "complete" {
			var batch financeGiftScopeBatchResult
			batch, err = runFinanceGiftLocalJob(ctx, step.NextDir, digest, wait)
			for _, entry := range batch.Entries {
				result.Updated += entry.RowsUpdated
			}
			if err == nil && batch.Status != "complete" {
				err = errors.New("series child did not complete")
			}
			if err == nil {
				progress, err = InspectFinanceGiftLocalJob(ctx, step.NextDir, digest)
			}
		}
		if err != nil {
			result.Status = "failed"
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				result.Status = "paused"
			}
			return result, err
		}
		if progress.Status != "complete" {
			result.Status = "failed"
			return result, errors.New("child verification incomplete")
		}
		result.Completed++
		prior = step.NextDir
	}
	result.Status = "complete"
	return result, nil
}

// Existing completion markers are a locator, not a checkpoint to trust:
// recheck the intent, parent snapshot hash, child plan, evidence and status.
func financeGiftSeriesChild(ctx context.Context, prior, digest string, step FinanceGiftSeriesStep) (string, error) {
	var checkedResult financegiftexport.BatchResult
	resultBytes, err := giftLocalReadJSON(filepath.Join(step.ExportDir, "result.json"), &checkedResult)
	if err != nil || giftLocalDigest(resultBytes) != step.ResultSHA256 {
		return "", errors.New("series export result changed during execution")
	}
	marker := filepath.Join(prior, "continuation-complete.json")
	if _, err := os.Lstat(marker); os.IsNotExist(err) {
		_, next, err := PrepareFinanceGiftLocalContinuation(ctx, prior, digest, step.ExportDir, step.ReadSHA256, step.NextDir)
		return next, err
	} else if err != nil {
		return "", err
	}
	parentLock, err := giftLocalLock(prior)
	if err != nil {
		return "", err
	}
	defer parentLock.Close()
	childLock, err := giftLocalLock(step.NextDir)
	if err != nil {
		return "", err
	}
	defer childLock.Close()
	var intent struct {
		Version int    `json:"version"`
		NextDir string `json:"next_job_dir"`
		Read    string `json:"read_plan_sha256"`
		Result  string `json:"result_sha256"`
	}
	if _, err = giftLocalReadJSON(filepath.Join(prior, "continuation-intent.json"), &intent); err != nil {
		return "", err
	}
	if intent.Version != 1 || intent.NextDir != step.NextDir || intent.Read != step.ReadSHA256 || intent.Result != step.ResultSHA256 {
		return "", errors.New("recorded child differs from series plan")
	}
	var completion struct {
		Version int    `json:"version"`
		SHA     string `json:"next_plan_sha256"`
	}
	if _, err = giftLocalReadJSON(marker, &completion); err != nil {
		return "", err
	}
	if completion.Version != 1 {
		return "", errors.New("invalid completion marker")
	}
	child, _, err := giftLocalLoadConfirmedInputs(step.NextDir, completion.SHA)
	if err != nil {
		return "", err
	}
	var exported financegiftexport.BatchPlan
	if _, err = giftLocalReadJSON(filepath.Join(step.ExportDir, "plan.json"), &exported); err != nil {
		return "", err
	}
	readDigest, err := financegiftexport.BatchConfirmation(exported)
	if err != nil || readDigest != step.ReadSHA256 {
		return "", errors.New("series read plan changed during execution")
	}
	result := checkedResult
	if len(child.Targets) != len(exported.Targets) || len(result.Entries) != len(child.Targets) {
		return "", errors.New("child export shape changed")
	}
	for i, target := range exported.Targets {
		if child.Targets[i].SourceEpoch != exported.SourceEpoch || child.Targets[i].UserID != target.UserID || child.Targets[i].HourTs != target.HourTs || child.Rows[i] != target.ExpectedRows || child.EvidenceSHA256[i] != result.Entries[i].SHA256 {
			return "", errors.New("child does not match series evidence")
		}
	}
	if _, _, err = giftLocalLoadConfirmedInputs(prior, digest); err != nil {
		return "", err
	}
	parentHash, err := giftSeriesFileHash(ctx, filepath.Join(prior, "usage-facts.db"))
	if err != nil {
		return "", err
	}
	if child.BackupSHA256 != parentHash {
		return "", errors.New("child snapshot no longer matches its predecessor")
	}
	return completion.SHA, nil
}
