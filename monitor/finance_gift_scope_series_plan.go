//go:build unix

package monitor

import (
	"encoding/hex"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
	"golang.org/x/sys/unix"
)

const (
	financeGiftSeriesMaxBatches = 3
	financeGiftSeriesMaxSeconds = 600
)

type FinanceGiftSeriesStep struct {
	ExportDir    string `json:"export_dir"`
	ReadSHA256   string `json:"read_plan_sha256"`
	ResultSHA256 string `json:"result_sha256"`
	NextDir      string `json:"next_job_dir"`
}

type FinanceGiftSeriesPlan struct {
	Version        int                     `json:"version"`
	PriorDir       string                  `json:"prior_job_dir"`
	PriorSHA256    string                  `json:"prior_plan_sha256"`
	MaxBatches     int                     `json:"max_batches"`
	MaxRows        int                     `json:"max_rows"`
	RuntimeSeconds int                     `json:"runtime_seconds"`
	Steps          []FinanceGiftSeriesStep `json:"steps"`
}

type FinanceGiftSeriesPreview struct {
	Mode         string `json:"mode"`
	Confirmation string `json:"confirm_plan_sha256"`
	Batches      int    `json:"batches"`
	Rows         int    `json:"rows"`
	Seconds      int    `json:"runtime_seconds"`
}

func giftSeriesDigestValid(value string) bool {
	bytes, err := hex.DecodeString(value)
	return err == nil && len(bytes) == 32
}

// A finite manifest is locked for the whole invocation. Neither check nor run
// discovers exports, follows directories, contacts a source, or edits it.
func loadFinanceGiftSeries(path string) (FinanceGiftSeriesPlan, FinanceGiftSeriesPreview, *os.File, error) {
	var plan FinanceGiftSeriesPlan
	var preview FinanceGiftSeriesPreview
	file, err := giftLocalOpenRegular(path, os.O_RDONLY)
	if err != nil {
		return plan, preview, nil, err
	}
	ok := false
	defer func() {
		if !ok {
			file.Close()
		}
	}()
	info, err := file.Stat()
	if err != nil || info.Mode().Perm()&0077 != 0 || info.Size() > financeGiftLocalJSONLimit {
		return plan, preview, nil, errors.New("series manifest must be a bounded private file")
	}
	if err := unix.Flock(int(file.Fd()), unix.LOCK_EX|unix.LOCK_NB); err != nil {
		return plan, preview, nil, errors.New("series manifest is already in use")
	}
	data, err := io.ReadAll(io.LimitReader(file, financeGiftLocalJSONLimit+1))
	if err != nil || len(data) > financeGiftLocalJSONLimit {
		return plan, preview, nil, errors.New("cannot read bounded series manifest")
	}
	if err := giftLocalDecodeJSON(data, &plan); err != nil {
		return plan, preview, nil, err
	}
	if plan.Version != 1 || !giftSeriesDigestValid(plan.PriorSHA256) || plan.MaxBatches < 1 || plan.MaxBatches > financeGiftSeriesMaxBatches || len(plan.Steps) < 1 || len(plan.Steps) > plan.MaxBatches || plan.MaxRows < 1 || plan.MaxRows > financeGiftLocalMaxRows || plan.RuntimeSeconds < 1 || plan.RuntimeSeconds > financeGiftSeriesMaxSeconds {
		return plan, preview, nil, errors.New("series requires 1..3 batches, at most 3000 rows and 1..600 seconds")
	}
	paths := map[string]bool{}
	addPath := func(path string) bool {
		if !filepath.IsAbs(path) || filepath.Clean(path) != path || paths[path] {
			return false
		}
		paths[path] = true
		return true
	}
	if !addPath(plan.PriorDir) {
		return plan, preview, nil, errors.New("invalid predecessor path")
	}
	seen, epoch, total := map[[2]int64]bool{}, "", 0
	for _, step := range plan.Steps {
		if !addPath(step.ExportDir) || !addPath(step.NextDir) || !giftSeriesDigestValid(step.ReadSHA256) || !giftSeriesDigestValid(step.ResultSHA256) {
			return plan, preview, nil, errors.New("invalid, duplicate or unconfirmed series paths")
		}
		var readPlan financegiftexport.BatchPlan
		if _, err := giftLocalReadJSON(filepath.Join(step.ExportDir, "plan.json"), &readPlan); err != nil {
			return plan, preview, nil, err
		}
		digest, err := financegiftexport.BatchConfirmation(readPlan)
		if err != nil || digest != step.ReadSHA256 || (epoch != "" && epoch != readPlan.SourceEpoch) {
			return plan, preview, nil, errors.New("series read plan changed or epochs differ")
		}
		epoch = readPlan.SourceEpoch
		var result financegiftexport.BatchResult
		bytes, err := giftLocalReadJSON(filepath.Join(step.ExportDir, "result.json"), &result)
		if err != nil || giftLocalDigest(bytes) != step.ResultSHA256 || result.Status != "complete" || result.Remaining != 0 || len(result.Entries) != len(readPlan.Targets) {
			return plan, preview, nil, errors.New("series export result is incomplete or changed")
		}
		for i, target := range readPlan.Targets {
			key := [2]int64{target.UserID, target.HourTs}
			if seen[key] || result.Entries[i].BatchTarget != target {
				return plan, preview, nil, errors.New("duplicate or mismatched series target")
			}
			seen[key] = true
			total += target.ExpectedRows
			if total > plan.MaxRows {
				return plan, preview, nil, errors.New("series exceeds total row budget")
			}
		}
	}
	preview = FinanceGiftSeriesPreview{"offline_finite_series_only", giftLocalDigest(data), len(plan.Steps), total, plan.RuntimeSeconds}
	ok = true
	return plan, preview, file, nil
}

func InspectFinanceGiftLocalSeries(path string) (FinanceGiftSeriesPreview, error) {
	_, preview, lock, err := loadFinanceGiftSeries(path)
	if err == nil {
		lock.Close()
	}
	return preview, err
}
