package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const retainedPreparationKind = "retained_normalized_revalidated"

// RetainedPreparationRequest is private operator input, never a customer API.
// It packages an EXISTING normalized input with its already pinned independent
// scope. It does not normalize raw captures, create history, approve, select or
// publish anything. Missing history still fails the original financial checks.
type RetainedPreparationRequest struct {
	Plan, Preparation []byte
	OutputRoot        string
	ExpectedHead      string
	Write             bool
}

func PrepareRetainedGenerationFiles(ctx context.Context, cfg DeploymentConfig, request RetainedPreparationRequest) (AutomaticPreparationReceipt, error) {
	return prepareRetainedGenerationFiles(ctx, cfg, request, writeArchiveFile)
}

func prepareRetainedGenerationFiles(parent context.Context, cfg DeploymentConfig, request RetainedPreparationRequest,
	writeFile func(context.Context, *os.Root, string, []byte) error) (receipt AutomaticPreparationReceipt, err error) {
	if parent == nil || !cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 1 || len(request.Plan) == 0 || len(request.Plan) > generationPlanLimit ||
		len(request.Preparation) == 0 || len(request.Preparation) > monitor.MonthlyPreparationInputLimit ||
		(request.ExpectedHead != "" && !generationHex(request.ExpectedHead, 32)) {
		return receipt, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	select {
	case automaticPreparationFileSlots <- struct{}{}:
		defer func() { <-automaticPreparationFileSlots }()
	default:
		return receipt, ErrGenerationBusy
	}
	request.Plan, request.Preparation = bytes.Clone(request.Plan), bytes.Clone(request.Preparation)
	var plan GenerationPlan
	if json.Unmarshal(request.Plan, &plan) != nil {
		return receipt, ErrGenerationInput
	}
	p, canonical, original, err := canonicalGeneration(plan, request.Preparation, time.Now())
	if err != nil {
		return receipt, err
	}
	if !bytes.Equal(bytes.TrimSpace(request.Plan), canonical) || !validScopeSourceID(p.AutomaticScopeSourceID) ||
		cfg.Rollout.Targets[0] != (GenerationTarget{GroupID: p.Owner.GroupID, Month: p.Funding.Month}) {
		return receipt, ErrGenerationInput
	}
	root, err := openArchiveRoot(request.OutputRoot)
	if err != nil {
		return receipt, err
	}
	defer root.Close()
	if err := automaticPreparationOutputEmpty(root); err != nil {
		return receipt, err
	}
	d, err := openDeployment(ctx, cfg, true)
	if err != nil {
		return receipt, err
	}
	defer func() { err = errors.Join(err, d.closeResources()) }()
	scopePath := filepath.Join(request.OutputRoot, "scope.json")
	if err := d.g.scopeOutputPathAvailable(scopePath); err != nil {
		return receipt, err
	}
	// Full original source/permission/selection/financial preflight. Merely
	// matching a previously rendered balance or hashing an input is insufficient.
	checkOriginal := func() error {
		_, err := prepareGenerationInputOnDeployment(ctx, d, p, canonical, original, request.Preparation, request.ExpectedHead, false)
		return err
	}
	if err := checkOriginal(); err != nil {
		return receipt, err
	}
	var source monitor.MonthlyPreparationSource
	for _, s := range original.Sources {
		if s.ID == p.AutomaticScopeSourceID {
			source = s
		}
	}
	scopeRaw, err := d.g.readPrivatePreparationFile(ctx, monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256}, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return receipt, err
	}
	// Also enforce the ACTUAL automatic-publication policy before producing a
	// completion marker, including independent wallet/history/refund semantics.
	g, release, err := d.g.bindObservationStore(ctx, p, original)
	if err != nil {
		return receipt, err
	}
	defer release()
	scope, err := g.selectedRetainedScope(ctx, p, original)
	if err != nil {
		return receipt, err
	}
	windows, err := g.auditWindows(ctx, p)
	if err != nil {
		return receipt, err
	}
	meta, err := g.observedAutomaticContext(ctx, p, request.Preparation, windows, scope)
	if err != nil {
		return receipt, err
	}
	bundle, err := prepareRetainedContextBundle(ctx, p, request.Preparation, meta, time.Now())
	if err != nil || len(bundle.Entries) != 1 || bundle.Entries[0].Candidate == nil {
		return receipt, ErrGenerationEvidence
	}
	candidate, err := automaticCorrectedCandidate(*bundle.Entries[0].Candidate, meta.Corrections, request.Preparation)
	if err != nil {
		return receipt, err
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(candidate, time.Now().Unix(), time.Now()); err != nil {
		return receipt, ErrGenerationEvidence
	}
	// Relocate exactly one scope reference. Preserve every account, amount,
	// movement, original source and historical statement; never relabel it as
	// machine observations or manufacture usage/wallet audit digests.
	input, err := monitor.DecodeMonthlyPreparationInput(request.Preparation)
	if err != nil {
		return receipt, err
	}
	for i := range input.Sources {
		if input.Sources[i].ID == p.AutomaticScopeSourceID {
			input.Sources[i].Path = scopePath
		}
	}
	inputRaw, err := json.Marshal(input)
	if err != nil || len(inputRaw) > monitor.MonthlyPreparationInputLimit {
		return receipt, ErrGenerationCapacity
	}
	outPlan := p
	outPlan.PreparationSHA256 = generationHash(inputRaw)
	outPlan, outRaw, _, err := canonicalGeneration(outPlan, inputRaw, time.Now())
	if err != nil {
		return receipt, err
	}
	m := automaticPreparationManifest{Version: 1, Mode: automaticPreparationMode, PreparationKind: retainedPreparationKind,
		Owner: p.Owner, Month: p.Funding.Month, SourceIdentity: p.Funding.SourceIdentity,
		OriginalPlanSHA256: generationHash(request.Plan), TemplateSHA256: generationHash(request.Preparation),
		ScopeInputSHA256: generationHash(scopeRaw), PlanSHA256: generationHash(outRaw), PreparationSHA256: generationHash(inputRaw), ScopeSHA256: generationHash(scopeRaw)}
	manifest, err := json.Marshal(m)
	if err != nil {
		return receipt, err
	}
	if err := checkOriginal(); err != nil {
		return receipt, err
	}
	if err := automaticPreparationRootCurrent(root, request.OutputRoot); err != nil {
		return receipt, err
	}
	if err := automaticPreparationOutputEmpty(root); err != nil {
		return receipt, err
	}
	receipt = AutomaticPreparationReceipt{Mode: "checked_retained_input_no_files_written", GroupID: p.Owner.GroupID, Month: p.Funding.Month,
		PlanSHA256: m.PlanSHA256, PreparationSHA256: m.PreparationSHA256, ScopeSHA256: m.ScopeSHA256, ManifestSHA256: generationHash(manifest)}
	if !request.Write {
		return receipt, nil
	}
	receipt.Mode, receipt.Directory = "incomplete_private_input_do_not_select", request.OutputRoot
	parts := []struct {
		name string
		raw  []byte
	}{{"plan.json", outRaw}, {"input.json", inputRaw}, {"scope.json", scopeRaw}, {"manifest.pending", manifest}}
	for _, part := range parts {
		if err := writeFile(ctx, root, part.name, part.raw); err != nil {
			return receipt, err
		}
	}
	if err := checkOriginal(); err != nil {
		return receipt, err
	}
	if _, err := prepareGenerationInputOnDeployment(ctx, d, outPlan, outRaw, input, inputRaw, request.ExpectedHead, false); err != nil {
		return receipt, err
	}
	if err := automaticPreparationRootCurrent(root, request.OutputRoot); err != nil {
		return receipt, err
	}
	for _, part := range parts {
		stored, err := readArchiveFile(ctx, root, part.name, int64(len(part.raw)))
		if err != nil || !bytes.Equal(stored, part.raw) {
			return receipt, errors.Join(ErrGenerationIntegrity, err)
		}
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	if err := root.Link("manifest.pending", "manifest.json"); err != nil {
		return receipt, err
	}
	receipt.Mode, receipt.Complete = automaticPreparationMode, true
	return receipt, errors.Join(syncArchiveDirectory(root), ctx.Err())
}
