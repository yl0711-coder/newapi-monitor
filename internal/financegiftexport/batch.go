package financegiftexport

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

const batchTargetLimit = 10
const batchInterval = 10 * time.Second

// BatchPlan pins the caller-reviewed local suggestion. Epoch and content hash
// are provenance, NOT database authentication or a substitute for the later
// exact monetary comparison against the closed local snapshot.
type BatchPlan struct {
	SourceEpoch string        `json:"source_epoch"`
	Targets     []BatchTarget `json:"targets"`
}

type BatchTarget struct {
	UserID           int64  `json:"user_id"`
	HourTs           int64  `json:"hour_ts"`
	ExpectedRows     int    `json:"expected_rows"`
	LocalContentHash string `json:"local_content_hash"`
}

type BatchResult struct {
	Status    string       `json:"status"`
	Remaining int          `json:"remaining"`
	Entries   []BatchEntry `json:"entries"`
}

type BatchEntry struct {
	BatchTarget
	File   string `json:"file"`
	SHA256 string `json:"sha256"`
}

// BatchConfirmation rejects open/duplicate targets and bounds total work.
// It hashes canonical JSON, not arbitrary whitespace in an input document.
func BatchConfirmation(plan BatchPlan) (string, error) {
	if plan.SourceEpoch == "" || len(plan.SourceEpoch) > 64 || strings.TrimSpace(plan.SourceEpoch) != plan.SourceEpoch || len(plan.Targets) == 0 || len(plan.Targets) > batchTargetLimit {
		return "", errors.New("batch requires one source epoch and 1 to 10 explicit targets")
	}
	seen := map[[2]int64]bool{}
	total, now := 0, time.Now().Unix()
	for _, t := range plan.Targets {
		key := [2]int64{t.UserID, t.HourTs}
		hash, err := hex.DecodeString(t.LocalContentHash)
		if t.UserID <= 0 || t.HourTs <= 0 || t.HourTs%3600 != 0 || t.HourTs >= now-3600 || seen[key] ||
			t.ExpectedRows <= 0 || t.ExpectedRows > giftScopeExplicitExportLimit-total || err != nil || len(hash) != sha256.Size {
			return "", errors.New("invalid, duplicate, open-hour or over-budget batch target")
		}
		total += t.ExpectedRows
		seen[key] = true
	}
	data, err := json.Marshal(plan)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:]), nil
}

// ExportBatch accepts a dedicated caller-supplied read-only source connection.
// No credentials, connection setup, scheduler, or Monitor runtime is wired here.
// Each invocation requires a NEW absolute directory. It never resumes or
// rereads existing files automatically after an ambiguous interrupted export.
func ExportBatch(ctx context.Context, db *sql.DB, plan BatchPlan, confirmation, dir string) (BatchResult, error) {
	return exportBatch(ctx, db, plan, confirmation, dir, waitBatch)
}

func exportBatch(ctx context.Context, db *sql.DB, plan BatchPlan, confirmation, dir string, wait func(context.Context, time.Duration) error) (BatchResult, error) {
	result := BatchResult{Status: "rejected", Remaining: len(plan.Targets)}
	// Freeze the caller's slice before waiting or executing any queries.
	plan.Targets = append([]BatchTarget(nil), plan.Targets...)
	digest, err := BatchConfirmation(plan)
	if err != nil {
		return result, err
	}
	if digest != confirmation || db == nil || !filepath.IsAbs(dir) {
		return result, errors.New("confirmed plan, source connection and absolute new directory required")
	}
	if err := ctx.Err(); err != nil {
		return result, err
	}
	if err := os.Mkdir(dir, 0700); err != nil {
		return result, err // Includes an existing directory: no implicit retries.
	}
	if err := publishBatchJSON(filepath.Join(dir, "plan.json"), plan); err != nil {
		return result, err
	}
	for i, target := range plan.Targets {
		if err := wait(ctx, batchInterval); err != nil {
			result.Status = "paused"
			return finishBatch(dir, result, err)
		}
		if err := ctx.Err(); err != nil {
			result.Status = "paused"
			return finishBatch(dir, result, err)
		}
		file := fmt.Sprintf("evidence-%02d.json", i)
		path := filepath.Join(dir, file)
		if err := Export(ctx, db, target.UserID, target.HourTs, path, target.ExpectedRows); err != nil {
			result.Status = "failed"
			if ctx.Err() != nil {
				result.Status = "paused"
			}
			return finishBatch(dir, result, fmt.Errorf("export target %d failed: %w", i+1, err))
		}
		data, err := os.ReadFile(path)
		if err != nil {
			result.Status = "failed"
			return finishBatch(dir, result, err)
		}
		sum := sha256.Sum256(data)
		result.Entries = append(result.Entries, BatchEntry{target, file, hex.EncodeToString(sum[:])})
		result.Remaining--
	}
	// No manifest on failure. Original files alone do not authorize repair.
	paths := make([]string, len(result.Entries))
	for i, e := range result.Entries {
		paths[i] = filepath.Join(dir, e.File)
	}
	if err := publishBatchJSON(filepath.Join(dir, "evidence-manifest.json"), paths); err != nil {
		result.Status = "failed"
		return finishBatch(dir, result, err)
	}
	result.Status = "complete"
	return finishBatch(dir, result, nil)
}

func publishBatchJSON(path string, value any) error {
	data, err := json.MarshalIndent(value, "", "  ")
	if err != nil {
		return err
	}
	return publishGiftScopeEvidence(path, data)
}

func finishBatch(dir string, result BatchResult, cause error) (BatchResult, error) {
	// Persist only status, IDs, paths, counts and hashes, never raw driver errors.
	if err := publishBatchJSON(filepath.Join(dir, "result.json"), result); err != nil {
		result.Status = "record_failed"
		return result, errors.Join(cause, err)
	}
	return result, cause
}

func waitBatch(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
