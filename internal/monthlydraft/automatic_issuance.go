package monthlydraft

import (
	"context"
	"encoding/json"
	"errors"
	"math/big"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// AutomaticContext loads machine-produced PRIVATE source metadata, not HTTP
// request JSON. It must attest the normalized source's wallet-only scope and
// event classification from retained original data. It cannot invent captures
// or mark an incomplete source complete merely because totals happen to match.
type AutomaticContext func(context.Context, monthlytasks.Request) (AutomaticSourceContext, error)

type AutomaticSourceContext struct {
	BundleContext  []byte
	Corrections    []AutomaticAccountCorrection
	historyWitness *historyCandidateWitness
}

// JSON metadata cannot carry or preserve executable source-audit authority.
func (a *AutomaticSourceContext) UnmarshalJSON(raw []byte) error {
	type public AutomaticSourceContext
	*a = AutomaticSourceContext{}
	var decoded public
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return err
	}
	*a = AutomaticSourceContext(decoded)
	return nil
}

type AutomaticAccountCorrection struct {
	UserID   int64                                `json:"user_id"`
	Evidence monitor.MonthlyBillBalanceCorrection `json:"evidence"`
}

// Preserve the original log consumption instead of quietly replacing it with
// the lower wallet debit. An independently proven small difference is disclosed
// separately. The source file hashes must be part of the already-audited input.
func automaticCorrectedCandidate(candidate monitor.MonthlyBillCandidate, corrections []AutomaticAccountCorrection, inputRaw []byte) (monitor.MonthlyBillCandidate, error) {
	if len(corrections) == 0 {
		return candidate, nil
	}
	input, err := monitor.DecodeMonthlyPreparationInput(inputRaw)
	if err != nil || len(corrections) > len(candidate.Users) {
		return candidate, ErrGenerationEvidence
	}
	candidate.Users = append([]monitor.MonthlyBillUserEvidence(nil), candidate.Users...)
	sources := map[string]bool{}
	for _, source := range input.Sources {
		sources[source.SHA256] = true
	}
	byUser := map[int64]monitor.MonthlyBillBalanceCorrection{}
	for _, correction := range corrections {
		if _, duplicate := byUser[correction.UserID]; duplicate || correction.UserID <= 0 {
			return candidate, ErrGenerationEvidence
		}
		for _, capture := range correction.Evidence.Observations {
			if !sources[capture.SourceSHA256] {
				return candidate, ErrGenerationEvidence
			}
		}
		byUser[correction.UserID] = correction.Evidence
	}
	for i := range candidate.Users {
		u := &candidate.Users[i]
		proof, selected := byUser[u.UserID]
		if !selected {
			continue
		}
		if u.Accounting == nil || u.BalanceCorrection != nil || u.RefundQuota != u.Accounting.LogRefundQuota || u.ConsumeQuota < 0 || u.Accounting.LogConsumeQuota < 0 {
			return candidate, ErrGenerationEvidence
		}
		gap := new(big.Int).Sub(big.NewInt(u.Accounting.LogConsumeQuota), big.NewInt(u.ConsumeQuota))
		if !gap.IsInt64() || gap.Sign() <= 0 || proof.Quota != gap.Int64() {
			return candidate, ErrGenerationEvidence
		}
		u.ConsumeQuota = u.Accounting.LogConsumeQuota
		u.BalanceCorrection = &proof
		delete(byUser, u.UserID)
	}
	if len(byUser) != 0 {
		return candidate, ErrGenerationEvidence
	}
	return candidate, nil
}

// AutomaticPublisher owns the local publication transaction, including current
// authorization checks against Request.Owner. No source database handle is
// made writable by constructing the worker. The caller owns its DB/lifetime.
type AutomaticPublisher func(context.Context, monthlytasks.Request, []byte) error

// NewAutomaticLocalWorker explicitly opts into generation -> source re-audit ->
// machine policy -> immutable publication. No goroutine starts here. Existing
// NewLocalWorker/OpenDeployment remain draft-only until a composition root
// supplies BOTH trusted dependencies and owns Serve/Stop. There is no customer
// endpoint for supplying evidence, review flags, account IDs or corrections.
func NewAutomaticLocalWorker(g *Generation, metadata AutomaticContext, publish AutomaticPublisher) (*LocalWorker, error) {
	if metadata == nil || publish == nil {
		return nil, ErrGenerationInput
	}
	w, err := NewLocalWorker(g)
	if err != nil {
		return nil, err
	}
	w.publications, err = newPublicationStore(g.receiver)
	if err != nil {
		return nil, err
	}
	w.finalize = func(parent context.Context, task monthlytasks.Task, lease publicationLease) error {
		ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
		defer cancel()
		evidence, err := automaticBillEvidence(ctx, g, metadata, task)
		if err != nil {
			return err
		}
		if err := w.publications.bind(ctx, lease, evidence); err != nil {
			return err
		}
		return publish(ctx, task.Request, evidence)
	}
	return w, nil
}

// Shared original-source validation; neither variant introduces a second money
// calculator. The caller owns the deadline and publication/recovery boundary.
func automaticBillEvidence(ctx context.Context, g *Generation, metadata AutomaticContext, task monthlytasks.Task) ([]byte, error) {
	if task.State != monthlytasks.DraftReady {
		return nil, ErrGenerationEvidence
	}
	if err := g.currentTask(ctx, task); err != nil {
		return nil, err
	}
	meta, err := metadata(ctx, task.Request)
	if err != nil {
		return nil, err
	}
	handoff, err := g.PrepareReviewHandoff(ctx, task.Request.Owner, task.ID, meta.BundleContext)
	if err != nil {
		return nil, err
	}
	var bundle monitor.MonthlyPreparationBundle
	if json.Unmarshal(handoff.Bundle, &bundle) != nil || handoff.Request != task.Request || len(bundle.Entries) != 1 || bundle.Entries[0].Candidate == nil {
		return nil, ErrGenerationIntegrity
	}
	candidate, err := automaticCorrectedCandidate(*bundle.Entries[0].Candidate, meta.Corrections, handoff.Input)
	if err != nil {
		return nil, err
	}
	// Retained completion time, not a new evidence version on each retry.
	evidence, err := monitor.PrepareAutomaticMonthlyBill(candidate, task.UpdatedAtMS/1000, time.Now())
	if err != nil {
		if errors.Is(err, monitor.ErrMonthlyObservationUnstable) {
			return nil, err
		}
		return nil, ErrGenerationEvidence
	}
	if err := g.currentTask(ctx, task); err != nil {
		return nil, err
	}
	return evidence, nil
}
