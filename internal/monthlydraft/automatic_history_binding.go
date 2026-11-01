package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Structural binding only. Money/permissions still require original-file
// review and the existing selected wallet/usage/token and authorization gates.
func canonicalAutomaticHistoryBinding(p GenerationPlan, input monitor.MonthlyPreparationInput) error {
	policyFound := false
	for _, s := range input.Sources {
		policyFound = policyFound || s.ID == automaticOriginPolicySourceID
	}
	if policyFound != (p.AutomaticHistoryPolicySourceID != "") || p.AutomaticHistoryPolicySourceID != "" && (p.AutomaticHistoryPolicySourceID != automaticOriginPolicySourceID || p.AutomaticHistorySourceID != automaticHistorySourceID) {
		return ErrGenerationInput
	}
	found := false
	for _, source := range input.Sources {
		found = found || source.ID == automaticHistorySourceID
	}
	for _, a := range input.Accounts {
		found = found || strings.HasPrefix(a.Baseline.Source, automaticHistorySourceID+"#derived-origin:") ||
			strings.HasPrefix(a.BasisSource, automaticHistorySourceID+"#derived-origin:") || strings.HasPrefix(a.Baseline.Source, automaticOriginPolicySourceID+"#") || strings.HasPrefix(a.BasisSource, automaticOriginPolicySourceID+"#")
	}
	if p.AutomaticHistorySourceID == "" {
		if found {
			return ErrGenerationInput // A new plan must not silently become legacy.
		}
		return nil // Preserve legacy behavior and JSON bytes.
	}
	manifest := false
	for _, source := range input.Sources {
		manifest = manifest || source.ID == automaticHistorySourceID
	}
	if !manifest || p.AutomaticHistorySourceID != automaticHistorySourceID || p.Version != 2 ||
		p.ObservationStoreSourceID == "" || p.CapturedUsageSourceID == "" ||
		p.WalletObservations == nil || p.WalletObservations.ReuseSourceID != "" || len(p.UsageWindows) != 0 ||
		len(p.TokenUsageSources) != len(p.Funding.UserIDs) {
		return ErrGenerationInput
	}
	return nil
}

// Runs on normalization, formal review, Run (including ready-artifact reuse)
// and automatic handoff through the existing wallet-input audit. No source
// query, DB initialization, task/selection write or publication occurs here.
func (g *Generation) auditAutomaticHistoryBinding(parent context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) error {
	_, err := g.auditAutomaticHistoryScope(parent, p, input)
	return err
}

func (g *Generation) auditAutomaticHistoryScope(parent context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (*AutomaticScope, error) {
	if err := canonicalAutomaticHistoryBinding(p, input); err != nil {
		return nil, err
	}
	if p.AutomaticHistorySourceID == "" {
		return nil, nil
	}
	if parent == nil || g == nil || g.root == nil || g.funding == nil {
		return nil, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 90*time.Second)
	defer cancel()
	var source monitor.MonthlyPreparationSource
	for _, s := range input.Sources {
		if s.ID == p.AutomaticHistorySourceID {
			source = s
		}
	}
	if err := g.verifyBoundObservationStore(ctx, input.Sources); err != nil {
		return nil, err
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return nil, err
	}
	// Call-local reader: every month explicitly names its sealed DB. The target
	// month's scoped handle is never a fallback for another month, nor mutated
	// or closed while its caller is still using it.
	reader := *g
	reader.observationSource, reader.observationFile = nil, nil
	req := SourcePreparationRequest{Owner: p.Owner, Month: p.Funding.Month}
	policy, err := selectedAutomaticOriginPolicy(p, input)
	if err != nil {
		return nil, err
	}
	audited, err := reader.auditAutomaticHistoryForPlan(ctx, monthlysource.MonthlyObservationFile{Path: source.Path, SHA256: source.SHA256}, req, policy)
	if err != nil {
		return nil, err
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return nil, ErrGenerationCapacity
	}
	current := p
	current.PreparationSHA256 = generationHash(raw)
	if _, _, _, err := canonicalGeneration(current, raw, time.Now()); err != nil {
		return nil, err // Reject duplicate/ambiguous metadata before stripping it.
	}
	// Detach before stripping only the explicitly selected metadata. All money
	// and every other source must still match freshly audited normalization.
	var detached monitor.MonthlyPreparationInput
	if json.Unmarshal(raw, &detached) != nil {
		return nil, ErrGenerationIntegrity
	}
	var directory monitor.MonthlyPreparationSource
	for _, s := range detached.Sources {
		if s.ID == automaticHistoryDirectorySourceID {
			directory = s
		}
	}
	var derived *AutomaticScope
	if directory.ID != "" {
		body, err := g.readPrivatePreparationFile(ctx, monthlysource.MonthlyObservationFile{Path: directory.Path, SHA256: directory.SHA256}, 1<<20)
		if err != nil {
			return nil, err
		}
		d, err := decodeAutomaticHistoryDirectory(body, p.Owner)
		if err != nil {
			return nil, err
		}
		scope, err := deriveAutomaticHistoryScope(ctx, p, audited, d)
		if err != nil {
			return nil, err
		}
		derived = &scope
		if len(detached.Accounts) != len(d.Members) {
			return nil, ErrGenerationEvidence
		}
		for i := range detached.Accounts {
			a := &detached.Accounts[i]
			if a.UserID != d.Members[i].UserID || a.Username != d.Members[i].Username {
				return nil, ErrGenerationEvidence
			}
			a.Username = ""
		}
	}
	if p.AutomaticScopeSourceID != "" {
		if derived == nil || p.AutomaticScopeSourceID == automaticHistoryDirectorySourceID || p.AutomaticScopeSourceID == automaticHistorySourceID || p.AutomaticScopeSourceID == automaticOriginPolicySourceID {
			return nil, ErrGenerationEvidence
		}
		selected, err := g.selectedRetainedScope(ctx, p, input)
		if err != nil {
			return nil, err
		}
		want, err := json.Marshal(derived)
		if err != nil {
			return nil, err
		}
		got, err := json.Marshal(selected)
		if err != nil || !bytes.Equal(want, got) {
			return nil, ErrGenerationEvidence
		}
	}
	kept := detached.Sources[:0]
	for _, s := range detached.Sources {
		if s.ID != automaticHistoryDirectorySourceID && s.ID != p.AutomaticScopeSourceID {
			kept = append(kept, s)
		}
	}
	detached.Sources = kept
	raw, err = json.Marshal(detached)
	if err != nil {
		return nil, err
	}
	// A normalizer audits newly appended observations before updating its
	// private input hash. Update only this call-local comparison, not any
	// retained task, plan or original file.
	p.PreparationSHA256 = generationHash(raw)
	p.AutomaticScopeSourceID = ""
	_, planRaw, _, err := canonicalGeneration(p, raw, time.Now())
	if err != nil {
		return nil, err
	}
	if err := verifyAutomaticHistoryInput(ctx, ObservedPreparation{Plan: planRaw, Preparation: raw}, audited, req); err != nil {
		return nil, err
	}
	if err := g.verifyBoundObservationStore(ctx, input.Sources); err != nil {
		return nil, err
	}
	return derived, ctx.Err()
}
