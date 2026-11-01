package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// RetainedScopePreparation contains private preparation files, NOT an issued
// bill. The caller must persist Scope at the selected new source path, then use
// PrepareGenerationInput, which rechecks all files and current authorization.
type RetainedScopePreparation struct {
	Plan, Preparation, Scope []byte
}

var retainedScopePreparationSlots = make(chan struct{}, 1)

// PrepareRetainedScopeInput packages independently established historical scope
// with already normalized funding/usage/wallet input (PrepareObservedInput).
// It does not infer history from current memberships or matching balances.
// It writes no files/DBs, selects no input, queues no task and publishes nothing.
// The new scope file is hash-bound to the output plan without circular hashes.
func (g *Generation) PrepareRetainedScopeInput(parent context.Context, plan GenerationPlan, raw []byte, sourceID, path string, scope AutomaticScope) (RetainedScopePreparation, error) {
	return g.prepareRetainedScopeInput(parent, plan, raw, sourceID, path, scope, nil)
}

// PrepareExistingRetainedScopeInput binds an ALREADY retained independent scope
// through the same candidate/financial checks, without making another file or
// inventing history. Its exact bytes/path remain part of the immutable input.
func (g *Generation) PrepareExistingRetainedScopeInput(ctx context.Context, plan GenerationPlan, raw []byte, sourceID string, source monthlysource.MonthlyObservationFile) (RetainedScopePreparation, error) {
	if ctx == nil || g == nil || g.root == nil {
		return RetainedScopePreparation{}, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(ctx, time.Minute)
	defer cancel()
	body, err := g.readPrivatePreparationFile(ctx, source, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return RetainedScopePreparation{}, err
	}
	var document RetainedAutomaticScope
	if json.Unmarshal(body, &document) != nil {
		return RetainedScopePreparation{}, ErrGenerationIntegrity
	}
	canonical, err := json.Marshal(document)
	if err != nil || !bytes.Equal(bytes.TrimSpace(body), canonical) || document.Version != 1 || document.Owner != plan.Owner || document.Month != plan.Funding.Month || document.SourceIdentity != plan.Funding.SourceIdentity {
		return RetainedScopePreparation{}, ErrGenerationEvidence
	}
	return g.prepareRetainedScopeInput(ctx, plan, raw, sourceID, source.Path, document.Scope, &source)
}

func (g *Generation) prepareRetainedScopeInput(parent context.Context, plan GenerationPlan, raw []byte, sourceID, path string, scope AutomaticScope, existing *monthlysource.MonthlyObservationFile) (RetainedScopePreparation, error) {
	var empty RetainedScopePreparation
	if parent == nil || g == nil || g.funding == nil || g.root == nil || !validScopeSourceID(sourceID) {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	select {
	case retainedScopePreparationSlots <- struct{}{}:
		defer func() { <-retainedScopePreparationSlots }()
	default:
		return empty, ErrGenerationBusy
	}
	p, _, input, err := canonicalGeneration(plan, raw, time.Now())
	if err != nil {
		return empty, err
	}
	g, releaseSource, err := g.bindObservationStore(ctx, p, input)
	if err != nil {
		return empty, err
	}
	defer releaseSource()
	if p.AutomaticScopeSourceID != "" || !planHasUsage(p) || p.WalletObservations == nil {
		return empty, ErrGenerationInput
	}
	for _, source := range input.Sources {
		if source.ID == sourceID || source.Path == path {
			return empty, ErrGenerationInput // never replace an existing source
		}
	}
	if existing == nil {
		if err := g.scopeOutputPathAvailable(path); err != nil {
			return empty, err
		}
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	if _, err := g.auditUsageInput(ctx, p, input); err != nil {
		return empty, err
	}
	if _, err := g.auditWalletInput(ctx, p, input); err != nil {
		return empty, err
	}
	windows, err := g.auditWindows(ctx, p)
	if err != nil {
		return empty, err
	}
	meta, err := g.observedAutomaticContext(ctx, p, raw, windows, scope)
	if err != nil {
		return empty, err
	}
	// Use the same publication policy, including correction bounds and original
	// log consumption. Validation here is not approval or authority to publish.
	now := time.Now()
	bundle, err := prepareRetainedContextBundle(ctx, p, raw, meta, now)
	if err != nil || len(bundle.Entries) != 1 || bundle.Entries[0].Candidate == nil {
		return empty, ErrGenerationEvidence
	}
	candidate, err := automaticCorrectedCandidate(*bundle.Entries[0].Candidate, meta.Corrections, raw)
	if err != nil {
		return empty, err
	}
	if _, err := monitor.PrepareAutomaticMonthlyBill(candidate, now.Unix(), now); err != nil {
		return empty, ErrGenerationEvidence
	}
	document := RetainedAutomaticScope{Version: 1, Owner: p.Owner, Month: p.Funding.Month,
		SourceIdentity: p.Funding.SourceIdentity, Scope: scope}
	scopeRaw, err := json.Marshal(document)
	if err != nil || len(scopeRaw) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	if existing != nil {
		body, err := g.readPrivatePreparationFile(ctx, *existing, monitor.MonthlyPreparationInputLimit)
		if err != nil {
			return empty, err
		}
		if !bytes.Equal(bytes.TrimSpace(body), scopeRaw) {
			return empty, ErrGenerationIntegrity
		}
		// Preserve permitted legacy surrounding whitespace AND its original
		// file digest, rather than rewriting an already retained scope file.
		scopeRaw = body
	}
	source := monitor.MonthlyPreparationSource{ID: sourceID, Path: path, SHA256: generationHash(scopeRaw)}
	if existing != nil && (existing.Path != source.Path || existing.SHA256 != source.SHA256) {
		return empty, ErrGenerationIntegrity
	}
	if !independentRetainedScope(scope, source) {
		return empty, ErrGenerationEvidence
	}
	input.Sources = append(input.Sources, source)
	preparation, err := json.Marshal(input)
	if err != nil || len(preparation) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.AutomaticScopeSourceID = sourceID
	p.PreparationSHA256 = generationHash(preparation)
	_, planRaw, _, err := canonicalGeneration(p, preparation, now)
	if err != nil {
		return empty, err
	}
	// Recheck original source identities and the total evidence budget including
	// the not-yet-written scope. Do not pass a missing file to verifyFiles or
	// pretend that this new document attests its own historical claims.
	total := int64(len(scopeRaw))
	for _, existing := range input.Sources[:len(input.Sources)-1] {
		n, err := g.verifyFile(ctx, existing, generationEvidenceLimit-total)
		if err != nil {
			return empty, err
		}
		total += n
	}
	if err := g.verifyBoundObservationStore(ctx, input.Sources[:len(input.Sources)-1]); err != nil {
		return empty, err
	}
	if existing == nil {
		if err := g.scopeOutputPathAvailable(path); err != nil {
			return empty, err
		}
	} else {
		body, err := g.readPrivatePreparationFile(ctx, *existing, monitor.MonthlyPreparationInputLimit)
		if err != nil {
			return empty, err
		}
		if !bytes.Equal(body, scopeRaw) {
			return empty, ErrGenerationIntegrity
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return RetainedScopePreparation{Plan: planRaw, Preparation: preparation, Scope: scopeRaw}, nil
}

func (g *Generation) scopeOutputPathAvailable(path string) error {
	rel, err := g.relativeSource(path)
	if err != nil {
		return err
	}
	// Check each existing parent, not just the leaf. os.Root prevents escapes;
	// this additionally rejects in-root symlink aliases and public directories.
	parts := strings.Split(rel, string(filepath.Separator))
	for i := 1; i < len(parts); i++ {
		info, err := g.root.Lstat(filepath.Join(parts[:i]...))
		if err != nil || !info.IsDir() || info.Mode().Perm()&0077 != 0 {
			return ErrGenerationIntegrity
		}
	}
	if _, err := g.root.Lstat(rel); !errors.Is(err, os.ErrNotExist) {
		return ErrGenerationIntegrity // refuse all existing files and aliases
	}
	return nil
}
