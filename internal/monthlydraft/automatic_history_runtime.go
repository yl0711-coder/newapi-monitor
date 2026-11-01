package monthlydraft

import (
	"context"
	"encoding/json"
	"path/filepath"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type automaticHistoryLocatorKey struct{}
type automaticHistoryLocator struct {
	producer *automaticHistoryPreparation
	parent   SourcePreparationRequest
	original monthlysource.MonthlyObservationFile
}

// The locator selects existing originals, not money or publication authority.
// It is supplied only by the outer raw-source coordinator in this process.
func (automaticHistoryLocator) MarshalJSON() ([]byte, error) { return nil, ErrGenerationInput }
func (l *automaticHistoryLocator) UnmarshalJSON([]byte) error {
	if l != nil {
		*l = automaticHistoryLocator{}
	}
	return ErrGenerationInput
}

// No I/O against the upstream or financial writers at construction. Each call
// uses the existing authority, original audits, monetary policy and selection
// CAS. An established/withdrawn version is never silently replaced.
func newAutomaticHistorySelectedPreparation(d *Deployment, p *automaticHistoryPreparation, scopeID string) (SourcePreparationStep, error) {
	if d == nil || d.readOnly || d.g == nil || d.runtime == nil || d.authority == nil || p == nil || p.spool == nil || p.core == nil ||
		d.automaticScopeID != scopeID || !validScopeSourceID(scopeID) {
		return nil, ErrGenerationInput
	}
	if _, err := d.g.relativeSource(filepath.Join(p.spool.directory, "boundary-check")); err != nil {
		return nil, err
	}
	auth, err := gorm.Open(&sqlite.Dialector{Conn: d.authority}, &gorm.Config{Logger: logger.Default.LogMode(logger.Silent), DisableAutomaticPing: true})
	if err != nil {
		return nil, err
	}
	return func(parent context.Context, req SourcePreparationRequest) (bool, error) {
		if parent == nil {
			return false, ErrGenerationInput
		}
		locator, ok := parent.Value(automaticHistoryLocatorKey{}).(automaticHistoryLocator)
		if !ok || locator.producer != p || locator.parent.Owner != req.Owner || req.Month > locator.parent.Month {
			return false, ErrGenerationInput
		}
		ctx, done, err := d.runtime.beginPreparation(parent, req.Owner.GroupID, req.Month)
		if err != nil {
			return false, err
		}
		defer done()
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
		review, err := d.g.reviewAutomaticOrigins(ctx, locator.original, req)
		if err != nil {
			return false, err
		}
		spool := p.core
		if req == locator.parent {
			spool = p.spool
		}
		var policy *monthlysource.MonthlyObservationFile
		needsPolicy := false
		for _, a := range review.accounts {
			if a.Status == "no_difference" {
				continue
			}
			if a.Status != "bounded_policy_candidate_not_approval" {
				return false, ErrGenerationEvidence
			}
			needsPolicy = true
		}
		if needsPolicy {
			files, err := automaticOriginTargetFiles(review.originals, req)
			if err != nil {
				return false, err
			}
			ref, err := spool.retainHistoryScope(ctx, req, files, automaticOriginPolicyBytes())
			if err != nil {
				return false, err
			}
			policy = &ref
		}
		prepared, err := d.g.prepareAutomaticHistoryContextInputWithPolicy(ctx, auth, locator.original, req, policy)
		if err != nil {
			return false, err
		}
		originals := prepared.input.originals
		if originals.target < 0 || originals.target >= len(originals.months) {
			return false, ErrGenerationEvidence
		}
		month := originals.months[originals.target]
		if len(month.sources) < 3 || month.material.Funding.Month != req.Month {
			return false, ErrGenerationEvidence
		}
		files := CapturedSourceFiles{Mode: capturedSourceFilesMode,
			Material:  monthlysource.MonthlyObservationFile{Path: month.sources[0].Path, SHA256: month.sources[0].SHA256},
			Selection: monthlysource.MonthlyObservationFile{Path: month.sources[1].Path, SHA256: month.sources[1].SHA256}}
		// Each dependency uses its OWN sealed directory and unchanged eight-file
		// envelope budget. No unbounded prefix is accumulated in one target seal.
		directory, err := spool.retainHistoryScope(ctx, req, files, prepared.directory)
		if err != nil {
			return false, err
		}
		candidate, err := d.g.prepareAutomaticHistoryCandidate(ctx, prepared, directory)
		if err != nil {
			return false, err
		}
		document := RetainedAutomaticScope{Version: 1, Owner: req.Owner, Month: req.Month, SourceIdentity: candidate.plan.Funding.SourceIdentity, Scope: candidate.scope}
		body, err := json.Marshal(document)
		if err != nil {
			return false, err
		}
		scope, err := spool.retainHistoryScope(ctx, req, files, body)
		if err != nil {
			return false, err
		}
		retained, err := d.g.PrepareExistingRetainedScopeInput(ctx, candidate.plan, candidate.input, scopeID, scope)
		if err != nil {
			return false, err
		}
		var plan GenerationPlan
		if json.Unmarshal(retained.Plan, &plan) != nil {
			return false, ErrGenerationIntegrity
		}
		selected, err := ReadGenerationSelection(ctx, d.receiver, req.Owner.GroupID, req.Month)
		if err != nil {
			return false, err
		}
		request := generationRequest(plan, retained.Plan)
		if selected.Head != "" && (selected.Request == nil || *selected.Request != request) {
			return false, ErrGenerationSelectionConflict
		}
		result, err := d.PrepareMonthlyInput(ctx, retained.Plan, retained.Preparation, selected.Head, true)
		return result.Applied, err // A committed result survives cancellation races.
	}, nil
}

// Raw collection must wrap the registered financial-prefix coordinator, not
// vice versa: one actual requested-month view supplies every prefix month, and
// token-work dispatches retain their original customer parent and limits.
func (p *automaticHistoryPreparation) installRuntime(r *LocalRuntime) error {
	if p == nil || r == nil {
		return ErrGenerationInput
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	q := r.preparation
	if r.phase != "disabled" || q == nil || q.dependencies == nil || q.step == nil ||
		(q.accountTokenWork && q.historySourceWork != p) {
		return ErrGenerationInput
	}
	inner, dependencies := q.step, q.dependencies
	outer, err := p.Before(func(ctx context.Context, parent SourcePreparationRequest, ref monthlysource.MonthlyObservationFile) (bool, error) {
		local := context.WithValue(ctx, automaticHistoryLocatorKey{}, automaticHistoryLocator{producer: p, parent: parent, original: ref})
		return inner(local, parent)
	})
	if err != nil {
		return err
	}
	q.step = func(ctx context.Context, req SourcePreparationRequest) (bool, error) {
		// Scan ALL prefix heads before source work; a withdrawn dependency must
		// not trigger capture, replacement inputs or new financial versions.
		_, missing, err := dependencies.missing(ctx, req)
		if err != nil {
			return false, err
		}
		if missing == "" {
			return true, nil
		}
		return outer(ctx, req)
	}
	return nil
}
