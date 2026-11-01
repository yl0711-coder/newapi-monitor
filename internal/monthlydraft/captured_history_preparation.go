package monthlydraft

import (
	"context"
	"encoding/json"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

// Server-only selections of EXISTING independent history. No customer-supplied
// amounts, balances, member IDs or editable approval flags are accepted.
type CapturedHistorySources struct {
	History, Scope monthlysource.MonthlyObservationFile
	// Only the server-owned catalogue adapter may bind this additional original
	// source. A derived scope envelope is not its own historical evidence.
	catalogue *monthlysource.MonthlyObservationFile
	assembly  *retainedHistoryAssembly // private, exact-byte-bound producer capability
}
type CapturedHistoryLoader func(context.Context, SourcePreparationRequest) (CapturedHistorySources, error)

var capturedHistoryPreparationSlots = make(chan struct{}, 1)

// NewCapturedHistoryPreparation connects sealed captures to the original
// history normalizers and running deployment's checked selection transaction.
// It starts nothing, creates no schema/files/tasks and does not publish bills.
// The loader must locate actual retained history; missing evidence stays an
// error, not a current-wallet-as-historical-balance or a fabricated zero.
func NewCapturedHistoryPreparation(d *Deployment, scopeID string, load CapturedHistoryLoader) (func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error), error) {
	if load == nil {
		return nil, ErrGenerationInput
	}
	return newCapturedHistoryPreparation(d, scopeID, func(ctx context.Context, req SourcePreparationRequest, _ CapturedSourceFiles) (CapturedHistorySources, error) {
		return load(ctx, req)
	})
}

func newCapturedHistoryPreparation(d *Deployment, scopeID string, load func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (CapturedHistorySources, error)) (func(context.Context, SourcePreparationRequest, CapturedSourceFiles) (bool, error), error) {
	if d == nil || d.readOnly || d.g == nil || d.runtime == nil || d.authority == nil || !validScopeSourceID(scopeID) || load == nil {
		return nil, ErrGenerationInput
	}
	auth, err := gorm.Open(&sqlite.Dialector{Conn: d.authority}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if err != nil {
		return nil, err
	}
	return func(parent context.Context, req SourcePreparationRequest, files CapturedSourceFiles) (bool, error) {
		ctx, done, err := d.runtime.beginPreparation(parent, req.Owner.GroupID, req.Month)
		if err != nil {
			return false, err
		}
		defer done()
		ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
		defer cancel()
		select {
		case capturedHistoryPreparationSlots <- struct{}{}:
			defer func() { <-capturedHistoryPreparationSlots }()
		default:
			return false, ErrGenerationBusy
		}
		owner, _, err := LoadUsageGenerationOwner(ctx, auth, req.Owner.GroupID, req.Owner.AuthVersion)
		if err != nil {
			return false, err
		}
		if owner != req.Owner {
			return false, ErrGenerationChanged
		}
		sources, err := load(ctx, req, files)
		if err != nil {
			return false, err
		}
		prepared, err := d.g.prepareCapturedHistoryInput(ctx, req, files, sources, scopeID)
		if err != nil {
			if ctx.Err() != nil {
				return false, ctx.Err()
			}
			return false, err
		}
		var plan GenerationPlan
		if json.Unmarshal(prepared.Plan, &plan) != nil {
			return false, ErrGenerationIntegrity
		}
		selected, err := ReadGenerationSelection(ctx, d.receiver, req.Owner.GroupID, req.Month)
		if err != nil {
			return false, err
		}
		request := generationRequest(plan, prepared.Plan)
		if selected.Head != "" && (selected.Request == nil || *selected.Request != request) {
			return false, ErrGenerationSelectionConflict
		}
		// Same committed request can be recovered, but a different version is
		// never silently overwritten. The shared CAS rechecks this head, ALL
		// financial sources and current authorization before its atomic write.
		result, err := d.PrepareMonthlyInput(ctx, prepared.Plan, prepared.Preparation, selected.Head, true)
		return result.Applied, err // preserve a commit even if cancellation races return
	}, nil
}

func (g *Generation) prepareCapturedHistoryInput(ctx context.Context, req SourcePreparationRequest, files CapturedSourceFiles, sources CapturedHistorySources, scopeID string) (RetainedScopePreparation, error) {
	var empty RetainedScopePreparation
	if ctx == nil || g == nil || g.root == nil || files.Mode != capturedSourceFilesMode || !validScopeSourceID(scopeID) || !generationHex(files.Material.SHA256, 32) || !generationHex(files.Selection.SHA256, 32) {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	scopeRaw, err := g.readPrivatePreparationFile(ctx, sources.Scope, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return empty, err
	}
	var document RetainedAutomaticScope
	if json.Unmarshal(scopeRaw, &document) != nil || document.Owner != req.Owner || document.Month != req.Month {
		return empty, ErrGenerationEvidence
	}
	r := ObservationMaterialPreparationRequest{Material: files.Material, Scope: scopeRaw, History: &sources.History, Organization: &files.Selection, ScopeSourceID: scopeID, capturedRosterKey: req.Key()}
	r.scopeCatalogue = sources.catalogue
	r.historyAssembly = sources.assembly
	base, err := g.observationMaterialInput(ctx, r)
	if err != nil {
		return empty, err
	}
	var plan GenerationPlan
	if json.Unmarshal(base.Plan, &plan) != nil {
		return empty, ErrGenerationIntegrity
	}
	observed, err := g.prepareObservedHistoryInput(ctx, plan, base.Template, base.ObservationSourceID)
	if err != nil {
		return empty, err
	}
	if json.Unmarshal(observed.Plan, &plan) != nil {
		return empty, ErrGenerationIntegrity
	}
	return g.PrepareExistingRetainedScopeInput(ctx, plan, observed.Preparation, scopeID, sources.Scope)
}
