//go:build unix

package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// Caller holds the predecessor lock and has revalidated its completion and
// the complete source export. Recover only the gap between preparing an
// untouched child and publishing the completion marker. Never recreate a
// partial directory, switch children, or infer progress from audit counts.
func resumeFinanceGiftPreparedContinuation(ctx context.Context, priorDir, nextDir string, readPlan financegiftexport.BatchPlan, readDigest, resultDigest string, result financegiftexport.BatchResult) (FinanceGiftLocalPlan, string, error) {
	var empty FinanceGiftLocalPlan
	var intent struct {
		Version        int    `json:"version"`
		NextDir        string `json:"next_job_dir"`
		ReadPlanSHA256 string `json:"read_plan_sha256"`
		ResultSHA256   string `json:"result_sha256"`
	}
	if _, err := giftLocalReadJSON(filepath.Join(priorDir, "continuation-intent.json"), &intent); err != nil {
		return empty, "", err
	}
	if intent.Version != 1 || intent.NextDir != nextDir || intent.ReadPlanSHA256 != readDigest || intent.ResultSHA256 != resultDigest {
		return empty, "", errors.New("existing continuation intent differs; cannot create another child")
	}
	completion := filepath.Join(priorDir, "continuation-complete.json")
	if _, err := os.Lstat(completion); !os.IsNotExist(err) {
		return empty, "", errors.New("continuation already completed or marker cannot be checked; use the recorded child")
	}
	lock, err := giftLocalLock(nextDir)
	if err != nil {
		return empty, "", err
	}
	defer lock.Close()
	var child FinanceGiftLocalPlan
	data, err := giftLocalReadJSON(filepath.Join(nextDir, "plan.json"), &child)
	if err != nil {
		return empty, "", errors.New("child preparation incomplete; retain files for explicit inspection")
	}
	digest := giftLocalDigest(data)
	child, _, err = giftLocalLoadConfirmedInputs(nextDir, digest)
	if err != nil || len(child.Targets) != len(readPlan.Targets) {
		return empty, "", errors.New("prepared child plan or evidence is invalid")
	}
	for i, target := range readPlan.Targets {
		if child.Targets[i].SourceEpoch != readPlan.SourceEpoch || child.Targets[i].UserID != target.UserID || child.Targets[i].HourTs != target.HourTs || child.Rows[i] != target.ExpectedRows || child.EvidenceSHA256[i] != result.Entries[i].SHA256 {
			return empty, "", errors.New("prepared child differs from confirmed export")
		}
	}
	for _, dir := range []string{priorDir, nextDir} {
		path := filepath.Join(dir, "usage-facts.db")
		if err := giftLocalClosedBackup(path); err != nil {
			return empty, "", err
		}
		if err := ctx.Err(); err != nil {
			return empty, "", err
		}
		file, err := giftLocalOpenRegular(path, os.O_RDONLY)
		if err != nil {
			return empty, "", err
		}
		hash := sha256.New()
		_, copyErr := io.Copy(hash, file)
		closeErr := file.Close()
		if copyErr != nil || closeErr != nil || hex.EncodeToString(hash.Sum(nil)) != child.BackupSHA256 {
			return empty, "", errors.New("prepared child or predecessor database changed; do not resume handoff")
		}
	}
	audit, err := giftLocalOpenRegular(filepath.Join(nextDir, "audit.jsonl"), os.O_RDONLY)
	if err != nil {
		return empty, "", err
	}
	info, statErr := audit.Stat()
	closeErr := audit.Close()
	if statErr != nil || closeErr != nil || info.Size() != 0 {
		return empty, "", errors.New("child execution has started; inspect it instead of repeating handoff")
	}
	if err := ctx.Err(); err != nil {
		return empty, "", err
	}
	data, err = json.Marshal(struct {
		Version        int    `json:"version"`
		NextPlanSHA256 string `json:"next_plan_sha256"`
	}{1, digest})
	if err != nil {
		return empty, "", err
	}
	if err := giftLocalWriteNew(completion, data); err != nil {
		return empty, "", err
	}
	return child, digest, nil
}
