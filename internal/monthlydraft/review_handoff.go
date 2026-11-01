package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// ReviewHandoff is private operator material, not a customer response or an
// importable bill. The context is supplied explicitly; no reviewer, approval,
// event classification or wallet attestation is inferred from task success.
type ReviewHandoff struct {
	Request monthlytasks.Request
	Input   []byte
	Context []byte
	Bundle  []byte
}

var reviewHandoffSlots = make(chan struct{}, 1)

// PrepareReviewHandoff binds an already-complete local task to the existing
// candidate/review workflow. It never starts a queued task, publishes a bill or
// writes funding/usage data. Sources and current authority are checked anew.
// Returned material is a point-in-time candidate: later review/import still
// needs live authority checks and independent source attestation.
func (g *Generation) PrepareReviewHandoff(parent context.Context, owner monthlytasks.Owner, id string, contextRaw []byte) (ReviewHandoff, error) {
	if len(contextRaw) == 0 || len(contextRaw) > monitor.MonthlyPreparationInputLimit {
		return ReviewHandoff{}, ErrGenerationInput
	}
	select {
	case reviewHandoffSlots <- struct{}{}:
		defer func() { <-reviewHandoffSlots }()
	default:
		return ReviewHandoff{}, ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	contextRaw = bytes.Clone(contextRaw)
	task, err := g.tasks.Get(ctx, owner, id)
	if err != nil {
		return ReviewHandoff{}, err
	}
	if task.State != monthlytasks.DraftReady {
		return ReviewHandoff{}, ErrGenerationEvidence
	}
	// Ready-only Run re-audits funding, selected usage captures and files, and
	// validates the immutable cached artifact without recalculating the wallet.
	artifact, err := g.Run(ctx, owner, id)
	if err != nil {
		return ReviewHandoff{}, err
	}
	saved, err := g.loadRetainedGeneration(ctx, task.Request, time.Now())
	if err != nil {
		return ReviewHandoff{}, err
	}
	if saved.digest != task.ResultSHA256 || generationHash(saved.artifact) != task.ResultSHA256 {
		return ReviewHandoff{}, ErrGenerationIntegrity
	}
	// Caller JSON cannot confer same-view authority. Rebuild the exact pinned
	// scope and metadata from originals before entering this handoff calculator.
	meta := AutomaticSourceContext{BundleContext: contextRaw}
	if saved.plan.AutomaticHistorySourceID != "" {
		bound, release, err := g.bindObservationStore(ctx, saved.plan, saved.input)
		if err != nil {
			return ReviewHandoff{}, err
		}
		defer release()
		scope, err := bound.selectedRetainedScope(ctx, saved.plan, saved.input)
		if err != nil {
			return ReviewHandoff{}, err
		}
		windows, err := bound.auditWindows(ctx, saved.plan)
		if err != nil {
			return ReviewHandoff{}, err
		}
		meta, err = bound.observedAutomaticContext(ctx, saved.plan, saved.inputRaw, windows, scope)
		if err != nil {
			return ReviewHandoff{}, err
		}
		if !bytes.Equal(meta.BundleContext, contextRaw) {
			return ReviewHandoff{}, ErrGenerationIntegrity
		}
	}
	bundle, err := prepareRetainedContextBundle(ctx, saved.plan, saved.inputRaw, meta, time.Now())
	if err != nil {
		return ReviewHandoff{}, err
	}
	if len(bundle.Entries) != 1 || bundle.ReadyForReview != 1 || bundle.Blocked != 0 {
		return ReviewHandoff{}, ErrGenerationEvidence
	}
	entry := bundle.Entries[0]
	if artifact.ScopeReview != nil && (bundle.ContextSHA256 != artifact.ScopeReview.ContextSHA256 || entry.CandidateSHA256 != artifact.ScopeReview.CandidateSHA256) {
		return ReviewHandoff{}, ErrGenerationIntegrity
	}
	if entry.GroupID != owner.GroupID || entry.Month != task.Request.Month || entry.Status != "needs_review" ||
		entry.Candidate == nil || len(entry.Issues) != 0 || len(entry.Members) != len(saved.plan.Funding.UserIDs) {
		return ReviewHandoff{}, ErrGenerationEvidence
	}
	for i, member := range entry.Members { // Both canonical lists are sorted.
		if member.UserID != saved.plan.Funding.UserIDs[i] {
			return ReviewHandoff{}, ErrGenerationEvidence
		}
	}
	bundle.SourceIntegrity = artifact.Reconciliation.SourceIntegrity
	bundle.Reconciliation.SourceIntegrity = artifact.Reconciliation.SourceIntegrity
	want, err := json.Marshal(artifact.Reconciliation)
	if err != nil {
		return ReviewHandoff{}, err
	}
	got, err := json.Marshal(bundle.Reconciliation)
	if err != nil || !bytes.Equal(want, got) {
		return ReviewHandoff{}, ErrGenerationIntegrity
	}
	raw, err := json.Marshal(bundle)
	if err != nil || len(raw) > monitor.MonthlyCandidateBundleLimit {
		return ReviewHandoff{}, ErrGenerationCapacity
	}
	if err := g.verifyFiles(ctx, saved.input.Sources); err != nil {
		return ReviewHandoff{}, err
	}
	if err := g.currentTask(ctx, task); err != nil {
		return ReviewHandoff{}, err
	}
	final, err := g.tasks.Get(ctx, owner, id)
	if err != nil || final.State != monthlytasks.DraftReady || final.ResultSHA256 != task.ResultSHA256 {
		return ReviewHandoff{}, errors.Join(ErrGenerationIntegrity, err)
	}
	if err := ctx.Err(); err != nil {
		return ReviewHandoff{}, err
	}
	return ReviewHandoff{Request: task.Request, Input: bytes.Clone(saved.inputRaw), Context: contextRaw, Bundle: raw}, nil
}
