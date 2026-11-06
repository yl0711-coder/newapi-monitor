package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// PRIVATE immutable binding only, never new money, approval or an HTTP DTO.
// Every reuse rebuilds this from the pinned scope and existing original-source
// candidate validator; matching strings alone cannot establish reconciliation.
type GenerationScopeReview struct {
	ContextSHA256   string `json:"context_sha256"`
	CandidateSHA256 string `json:"candidate_sha256"`
}

func (g *Generation) selectedGenerationReview(ctx context.Context, p GenerationPlan, inputRaw []byte, windows []monthlysource.FundingWindowObservation, report monitor.MonthlyPreparationReport) (*GenerationScopeReview, error) {
	if p.AutomaticScopeSourceID == "" {
		return nil, nil // Preserve the unchanged exact-wallet/legacy path.
	}
	input, err := monitor.DecodeMonthlyPreparationInput(inputRaw)
	if err != nil {
		return nil, err
	}
	scope, err := g.selectedRetainedScope(ctx, p, input)
	if err != nil {
		return nil, err
	}
	selected := false
	preRegistration := false
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return nil, ErrGenerationInput
	}
	for _, a := range scope.Accounts {
		selected = selected || len(a.Reconstructions) != 0
		preRegistration = preRegistration || (a.Registration != nil && a.Registration.At >= month.AddDate(0, 1, 0).Unix())
	}
	if !selected && !preRegistration {
		return nil, nil
	}
	// A selected reconstruction is never ignored in favor of a convenient
	// ordinary anchor. This audits account scope and every funding movement,
	// then uses the existing exact forward/reverse reconstruction calculator.
	meta, err := g.observedAutomaticContext(ctx, p, inputRaw, windows, scope)
	if err != nil {
		return nil, err
	}
	bundle, err := prepareRetainedContextBundle(ctx, p, inputRaw, meta, time.Now())
	if err != nil || bundle.ReadyForReview != 1 || bundle.Blocked != 0 || len(bundle.Entries) != 1 || bundle.Entries[0].Candidate == nil ||
		(len(bundle.Entries[0].Candidate.ReconstructedUsers) == 0 && !preRegistration) || !generationReportScope(p, bundle.Reconciliation) {
		return nil, ErrGenerationEvidence
	}
	// Preserve the original report (including missing ordinary cutoffs). Do
	// not let a cached artifact silently replace it with different money/errors.
	if !sameGenerationReport(report, bundle.Reconciliation) {
		return nil, ErrGenerationIntegrity
	}
	return &GenerationScopeReview{ContextSHA256: bundle.ContextSHA256, CandidateSHA256: bundle.Entries[0].CandidateSHA256}, ctx.Err()
}

func sameGenerationReport(a, b monitor.MonthlyPreparationReport) bool {
	a.SourceIntegrity, b.SourceIntegrity = "", ""
	x, err := json.Marshal(a)
	if err != nil {
		return false
	}
	y, err := json.Marshal(b)
	return err == nil && bytes.Equal(x, y)
}

func generationReportScope(p GenerationPlan, r monitor.MonthlyPreparationReport) bool {
	if r.Mode != "local_reconciliation_draft_not_publication" || r.InputSHA256 != p.PreparationSHA256 || len(r.Accounts) != len(p.Funding.UserIDs) {
		return false
	}
	ids := map[int64]bool{}
	for _, id := range p.Funding.UserIDs {
		ids[id] = true
	}
	for _, a := range r.Accounts {
		if !ids[a.UserID] || a.GroupID != p.Owner.GroupID || a.Error != "" || len(a.Months) != 1 || a.Months[0].Month != p.Funding.Month {
			return false
		}
		delete(ids, a.UserID)
	}
	return len(ids) == 0
}

// Structural guard only. Run additionally rebuilds the selected proof from
// original retained files; callers may not use this as financial attestation.
func generationArtifactReconciled(p GenerationPlan, report monitor.MonthlyPreparationReport, proof *GenerationScopeReview) bool {
	if p.AutomaticHistorySourceID != "" && (p.AutomaticScopeSourceID == "" || proof == nil) {
		return false // Source normalization alone is not historical financial approval.
	}
	if proof == nil {
		return generationAllAccounts(p, report)
	}
	return p.AutomaticScopeSourceID != "" && generationReportScope(p, report) &&
		generationHex(proof.ContextSHA256, 32) && generationHex(proof.CandidateSHA256, 32)
}
