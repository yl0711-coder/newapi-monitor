package monthlydraft

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// HandoffExportRequest is trusted operator input, never a customer HTTP DTO.
// Export defaults to false: check sources and permissions without writing files.
type HandoffExportRequest struct {
	TaskID, ExpectedHead, ExpectedResult, OutputRoot string
	Context                                          []byte
	Export                                           bool
}

type HandoffExportReceipt struct {
	Mode           string `json:"mode"`
	TaskID         string `json:"task_id"`
	GroupID        int64  `json:"group_id"`
	Month          string `json:"month"`
	SelectionHead  string `json:"selection_head"`
	ResultSHA256   string `json:"result_sha256"`
	ManifestSHA256 string `json:"manifest_sha256"`
	Directory      string `json:"directory,omitempty"`
	Exported       bool   `json:"exported"`
}

type handoffExportManifest struct {
	Version       int                  `json:"version"`
	Mode          string               `json:"mode"`
	Request       monthlytasks.Request `json:"request"`
	TaskID        string               `json:"task_id"`
	SelectionHead string               `json:"selection_head"`
	ResultSHA256  string               `json:"result_sha256"`
	InputSHA256   string               `json:"input_sha256"`
	ContextSHA256 string               `json:"context_sha256"`
	BundleSHA256  string               `json:"bundle_sha256"`
}

const handoffExportMode = "private_review_material_not_approval_or_publication"

// ExportReviewHandoff reads all databases in mode=ro. Only a completed, current,
// explicitly pinned task can be exported. A directory without manifest.json is
// an incomplete copy, not review material; failed copies are preserved privately
// for diagnosis rather than overwriting/deleting operator data on retry.
func ExportReviewHandoff(parent context.Context, cfg DeploymentConfig, request HandoffExportRequest) (HandoffExportReceipt, error) {
	return exportReviewHandoff(parent, cfg, request, writeArchiveFile)
}

func exportReviewHandoff(parent context.Context, cfg DeploymentConfig, request HandoffExportRequest,
	writeFile func(context.Context, *os.Root, string, []byte) error) (receipt HandoffExportReceipt, err error) {
	if parent == nil || !cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 1 || !generationHex(request.TaskID, 16) ||
		!generationHex(request.ExpectedHead, 32) || !generationHex(request.ExpectedResult, 32) ||
		len(request.Context) == 0 || len(request.Context) > monitor.MonthlyPreparationInputLimit {
		return receipt, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	root, err := openArchiveRoot(request.OutputRoot)
	if err != nil {
		return receipt, err
	}
	defer root.Close()
	d, err := openDeployment(ctx, cfg, true)
	if err != nil {
		return receipt, err
	}
	defer func() { err = errors.Join(err, d.closeResources()) }()
	target := cfg.Rollout.Targets[0]
	selection, err := ReadGenerationSelection(ctx, d.receiver, target.GroupID, target.Month)
	if err != nil {
		return receipt, err
	}
	if selection.Head != request.ExpectedHead || selection.Request == nil {
		return receipt, ErrGenerationSelectionConflict
	}
	owner := selection.Request.Owner
	checkCurrent := func() error {
		s, err := ReadGenerationSelection(ctx, d.receiver, target.GroupID, target.Month)
		if err != nil {
			return err
		}
		if s.Head != request.ExpectedHead || s.Request == nil || *s.Request != *selection.Request {
			return ErrGenerationSelectionConflict
		}
		task, err := d.g.tasks.Get(ctx, owner, request.TaskID)
		if err != nil {
			return err
		}
		if task.Request != *selection.Request || task.State != monthlytasks.DraftReady || task.ResultSHA256 != request.ExpectedResult {
			return ErrGenerationEvidence
		}
		return d.g.currentTask(ctx, task)
	}
	if err := checkCurrent(); err != nil {
		return receipt, err
	}
	handoff, err := d.g.PrepareReviewHandoff(ctx, owner, request.TaskID, request.Context)
	if err != nil {
		return receipt, err
	}
	if handoff.Request != *selection.Request {
		return receipt, ErrGenerationChanged
	}
	input, err := monitor.DecodeMonthlyPreparationInput(handoff.Input)
	if err != nil {
		return receipt, err
	}
	m := handoffExportManifest{Version: 1, Mode: handoffExportMode, Request: handoff.Request, TaskID: request.TaskID,
		SelectionHead: selection.Head, ResultSHA256: request.ExpectedResult,
		InputSHA256: generationHash(handoff.Input), ContextSHA256: generationHash(handoff.Context), BundleSHA256: generationHash(handoff.Bundle)}
	manifest, err := json.Marshal(m)
	if err != nil {
		return receipt, err
	}
	if err := checkCurrent(); err != nil {
		return receipt, err
	}
	receipt = HandoffExportReceipt{Mode: "checked_review_material_not_approval", TaskID: request.TaskID, GroupID: target.GroupID,
		Month: target.Month, SelectionHead: selection.Head, ResultSHA256: request.ExpectedResult, ManifestSHA256: generationHash(manifest)}
	if !request.Export {
		return receipt, nil
	}
	name := "handoff-" + rand.Text()
	if err := root.Mkdir(name, 0700); err != nil {
		return receipt, err
	}
	receipt.Directory = filepath.Join(request.OutputRoot, name)
	receipt.Mode = "incomplete_private_copy_do_not_review"
	copyRoot, err := root.OpenRoot(name)
	if err != nil {
		return receipt, err
	}
	defer copyRoot.Close()
	for _, part := range []struct {
		name string
		raw  []byte
	}{
		{"input.json", handoff.Input}, {"context.json", handoff.Context}, {"candidates.json", handoff.Bundle}, {"manifest.pending", manifest},
	} {
		if err := writeFile(ctx, copyRoot, part.name, part.raw); err != nil {
			return receipt, err
		}
		stored, err := readArchiveFile(ctx, copyRoot, part.name, int64(len(part.raw)))
		if err != nil || generationHash(stored) != generationHash(part.raw) {
			return receipt, errors.Join(ErrGenerationIntegrity, err)
		}
	}
	if err := d.g.verifyFiles(ctx, input.Sources); err != nil {
		return receipt, err
	}
	if err := checkCurrent(); err != nil {
		return receipt, err
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	// Publish the completion marker only after read-back and final live checks.
	if err := copyRoot.Link("manifest.pending", "manifest.json"); err != nil {
		return receipt, err
	}
	receipt.Exported = true
	receipt.Mode = handoffExportMode
	// A post-link sync failure may leave completed bytes: preserve the receipt
	// and return the error, never incorrectly claim that no files were written.
	return receipt, errors.Join(syncArchiveDirectory(copyRoot), syncArchiveDirectory(root))
}
