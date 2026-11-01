package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// AutomaticPreparationRequest is operator input, never a customer DTO. Scope
// contains independently established history; this tool cannot manufacture it.
// OutputRoot must be an EXISTING empty private directory inside EvidenceRoot.
type AutomaticPreparationRequest struct {
	Plan, Template, Scope              []byte
	ObservationSourceID, ScopeSourceID string
	OutputRoot                         string
	Write                              bool
	// Set only by the pinned organization-history adapter, never a CLI/API flag.
	// Existing manual templates retain the no-overwrite normalization path.
	compareRetainedMonth bool
}

type AutomaticPreparationReceipt struct {
	Mode              string `json:"mode"`
	GroupID           int64  `json:"group_id"`
	Month             string `json:"month"`
	PlanSHA256        string `json:"plan_sha256"`
	PreparationSHA256 string `json:"preparation_sha256"`
	ScopeSHA256       string `json:"scope_sha256"`
	ManifestSHA256    string `json:"manifest_sha256"`
	Directory         string `json:"directory,omitempty"`
	Complete          bool   `json:"complete"`
}

type automaticPreparationManifest struct {
	Version            int                `json:"version"`
	Mode               string             `json:"mode"`
	PreparationKind    string             `json:"preparation_kind,omitempty"`
	Owner              monthlytasks.Owner `json:"owner"`
	Month              string             `json:"month"`
	SourceIdentity     string             `json:"source_identity"`
	OriginalPlanSHA256 string             `json:"original_plan_sha256"`
	TemplateSHA256     string             `json:"template_sha256"`
	ScopeInputSHA256   string             `json:"scope_input_sha256"`
	PlanSHA256         string             `json:"plan_sha256"`
	PreparationSHA256  string             `json:"preparation_sha256"`
	ScopeSHA256        string             `json:"scope_sha256"`
	FundingSHA256      string             `json:"funding_sha256"`
	UsageSHA256        string             `json:"usage_sha256"`
	WalletSHA256       string             `json:"wallet_sha256"`
	TokenSHA256        string             `json:"token_sha256,omitempty"`
}

const automaticPreparationMode = "private_prepared_input_not_selection_or_publication"

var automaticPreparationFileSlots = make(chan struct{}, 1)

// PrepareAutomaticGenerationFiles composes the existing bounded normalizers and
// fixed-scope packager with explicit local file output. All DBs are mode=ro;
// no environment loading, migration, selection, task, worker or publication.
// Default inspection writes nothing. An interrupted directory is retained,
// never overwritten or automatically deleted. manifest.json is linked last,
// only after read-back, original source and current authorization checks.
func PrepareAutomaticGenerationFiles(ctx context.Context, cfg DeploymentConfig, request AutomaticPreparationRequest) (AutomaticPreparationReceipt, error) {
	return prepareAutomaticGenerationFiles(ctx, cfg, request, writeArchiveFile)
}

func prepareAutomaticGenerationFiles(parent context.Context, cfg DeploymentConfig, request AutomaticPreparationRequest,
	writeFile func(context.Context, *os.Root, string, []byte) error) (receipt AutomaticPreparationReceipt, err error) {
	if parent == nil || !cfg.Rollout.Enabled || len(cfg.Rollout.Targets) != 1 || len(request.Plan) == 0 || len(request.Plan) > generationPlanLimit ||
		len(request.Template) == 0 || len(request.Template) > monitor.MonthlyPreparationInputLimit || len(request.Scope) == 0 || len(request.Scope) > monitor.MonthlyPreparationInputLimit ||
		request.ObservationSourceID == "" || !validScopeSourceID(request.ScopeSourceID) {
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
	var plan GenerationPlan
	if json.Unmarshal(request.Plan, &plan) != nil {
		return receipt, ErrGenerationInput
	}
	p, canonical, templateInput, err := canonicalGeneration(plan, request.Template, time.Now())
	if err != nil {
		return receipt, err
	}
	if !bytes.Equal(bytes.TrimSpace(request.Plan), canonical) || cfg.Rollout.Targets[0] != (GenerationTarget{GroupID: p.Owner.GroupID, Month: p.Funding.Month}) {
		return receipt, ErrGenerationInput
	}
	var document RetainedAutomaticScope
	if json.Unmarshal(request.Scope, &document) != nil {
		return receipt, ErrGenerationInput
	}
	scopeCanonical, err := json.Marshal(document)
	if err != nil || !bytes.Equal(bytes.TrimSpace(request.Scope), scopeCanonical) || document.Version != 1 || document.Owner != p.Owner ||
		document.Month != p.Funding.Month || document.SourceIdentity != p.Funding.SourceIdentity {
		return receipt, ErrGenerationEvidence
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
	auth, err := gorm.Open(&sqlite.Dialector{Conn: d.authority}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if err != nil {
		return receipt, err
	}
	checkOwner := func() error {
		owner, _, err := LoadUsageGenerationOwner(ctx, auth, p.Owner.GroupID, p.Owner.AuthVersion)
		if err != nil {
			return err
		}
		if owner != p.Owner {
			return ErrGenerationChanged
		}
		return ctx.Err()
	}
	if err := checkOwner(); err != nil {
		return receipt, err
	}
	g, releaseSource, err := d.g.bindObservationStore(ctx, p, templateInput)
	if err != nil {
		return receipt, err
	}
	defer releaseSource()
	// Validate the final location BEFORE expensive normalization. Its parent is
	// already private and empty; nothing needs to be created during inspection.
	scopePath := filepath.Join(request.OutputRoot, "scope.json")
	if err := g.scopeOutputPathAvailable(scopePath); err != nil {
		return receipt, err
	}
	var observed ObservedPreparation
	if request.compareRetainedMonth {
		observed, err = g.prepareObservedHistoryInput(ctx, p, request.Template, request.ObservationSourceID)
	} else {
		observed, err = g.PrepareObservedInput(ctx, p, request.Template, request.ObservationSourceID)
	}
	if err != nil {
		return receipt, err
	}
	if err := json.Unmarshal(observed.Plan, &p); err != nil {
		return receipt, err
	}
	prepared, err := g.PrepareRetainedScopeInput(ctx, p, observed.Preparation, request.ScopeSourceID, scopePath, document.Scope)
	if err != nil {
		return receipt, err
	}
	m := automaticPreparationManifest{Version: 1, Mode: automaticPreparationMode, Owner: p.Owner, Month: p.Funding.Month,
		SourceIdentity: p.Funding.SourceIdentity, OriginalPlanSHA256: generationHash(request.Plan), TemplateSHA256: generationHash(request.Template),
		ScopeInputSHA256: generationHash(request.Scope), PlanSHA256: generationHash(prepared.Plan), PreparationSHA256: generationHash(prepared.Preparation),
		ScopeSHA256: generationHash(prepared.Scope), FundingSHA256: observed.FundingSHA256, UsageSHA256: observed.UsageSHA256, WalletSHA256: observed.WalletSHA256, TokenSHA256: observed.TokenSHA256}
	manifest, err := json.Marshal(m)
	if err != nil {
		return receipt, err
	}
	if err := checkOwner(); err != nil {
		return receipt, err
	}
	if err := automaticPreparationRootCurrent(root, request.OutputRoot); err != nil {
		return receipt, err
	}
	if err := automaticPreparationOutputEmpty(root); err != nil {
		return receipt, err
	}
	receipt = AutomaticPreparationReceipt{Mode: "checked_input_no_files_written", GroupID: p.Owner.GroupID, Month: p.Funding.Month,
		PlanSHA256: m.PlanSHA256, PreparationSHA256: m.PreparationSHA256, ScopeSHA256: m.ScopeSHA256, ManifestSHA256: generationHash(manifest)}
	if !request.Write {
		return receipt, nil
	}
	// Preserve an explicit incomplete receipt even if a disk error occurs on
	// the first write. Retrying is NEVER permission to replace existing files.
	receipt.Directory, receipt.Mode = request.OutputRoot, "incomplete_private_input_do_not_select"
	parts := []struct {
		name string
		raw  []byte
	}{{"plan.json", prepared.Plan}, {"input.json", prepared.Preparation}, {"scope.json", prepared.Scope}, {"manifest.pending", manifest}}
	for _, part := range parts {
		if err := writeFile(ctx, root, part.name, part.raw); err != nil {
			return receipt, err
		}
		stored, err := readArchiveFile(ctx, root, part.name, int64(len(part.raw)))
		if err != nil || !bytes.Equal(stored, part.raw) {
			return receipt, errors.Join(ErrGenerationIntegrity, err)
		}
	}
	input, err := monitor.DecodeMonthlyPreparationInput(prepared.Preparation)
	if err != nil {
		return receipt, err
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return receipt, err
	}
	if err := checkOwner(); err != nil {
		return receipt, err
	}
	if err := automaticPreparationRootCurrent(root, request.OutputRoot); err != nil {
		return receipt, err
	}
	if err := ctx.Err(); err != nil {
		return receipt, err
	}
	// Re-read the WHOLE output after the last write/live checks, not just each
	// file immediately after its own write. A later corruption must not obtain
	// a completion marker from an earlier successful per-file check.
	for _, part := range parts {
		stored, err := readArchiveFile(ctx, root, part.name, int64(len(part.raw)))
		if err != nil || !bytes.Equal(stored, part.raw) {
			return receipt, errors.Join(ErrGenerationIntegrity, err)
		}
	}
	if err := root.Link("manifest.pending", "manifest.json"); err != nil {
		return receipt, err
	}
	receipt.Mode, receipt.Complete = automaticPreparationMode, true
	// Link success is a completed copy even if syncing or cancellation now
	// fails. Report that fact instead of implying the operation wrote nothing.
	return receipt, errors.Join(syncArchiveDirectory(root), ctx.Err())
}

func automaticPreparationOutputEmpty(root *os.Root) error {
	f, err := root.Open(".")
	if err != nil {
		return err
	}
	defer f.Close()
	entries, err := f.ReadDir(1)
	if len(entries) != 0 || !errors.Is(err, io.EOF) {
		return ErrGenerationIntegrity
	}
	return nil
}

func automaticPreparationRootCurrent(root *os.Root, path string) error {
	opened, err := root.Stat(".")
	if err != nil {
		return err
	}
	current, err := os.Lstat(path)
	if err != nil || !current.IsDir() || current.Mode().Perm()&0077 != 0 || !os.SameFile(opened, current) {
		return ErrGenerationIntegrity
	}
	return nil
}
