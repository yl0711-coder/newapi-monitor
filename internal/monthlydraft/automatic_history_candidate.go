package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

const automaticHistoryDirectorySourceID = "automatic-history-directory"

type historyCandidateWitnessKey struct{}
type historyCandidateWitness struct{ input, scope, plan, metadata string }

func (historyCandidateWitness) MarshalJSON() ([]byte, error) {
	return nil, errors.New("history candidate witness is process local")
}
func (w *historyCandidateWitness) UnmarshalJSON([]byte) error {
	if w != nil {
		*w = historyCandidateWitness{}
	}
	return errors.New("history candidate witness cannot be decoded")
}

func prepareHistoryContextBundle(ctx context.Context, p GenerationPlan, input, scope, metadata []byte, now time.Time) (monitor.MonthlyPreparationBundle, error) {
	if p.AutomaticHistorySourceID == "" {
		return monitor.PrepareMonthlyCandidateBundle(input, metadata, now)
	}
	w, ok := ctx.Value(historyCandidateWitnessKey{}).(historyCandidateWitness)
	planRaw, err := json.Marshal(p)
	if err != nil || !ok || w.input != generationHash(input) || w.scope != generationHash(scope) || w.plan != generationHash(planRaw) {
		return monitor.MonthlyPreparationBundle{}, ErrGenerationEvidence
	}
	contextSHA := generationHash(metadata)
	return monitor.PrepareMonthlyCandidateBundleWithSameView(input, metadata, now, func(i, c []byte) error {
		if generationHash(i) != w.input || generationHash(c) != contextSHA {
			return ErrGenerationEvidence
		}
		return ctx.Err()
	})
}

// A retained scope is not authority by itself. Only the in-process result of a
// fresh original-source audit can enter the same-view calculator after reuse.
func prepareRetainedContextBundle(ctx context.Context, p GenerationPlan, input []byte, meta AutomaticSourceContext, now time.Time) (monitor.MonthlyPreparationBundle, error) {
	if p.AutomaticHistorySourceID == "" {
		return monitor.PrepareMonthlyCandidateBundle(input, meta.BundleContext, now)
	}
	w := meta.historyWitness
	planRaw, err := json.Marshal(p)
	if err != nil || w == nil || w.input != generationHash(input) || w.plan != generationHash(planRaw) || w.metadata != generationHash(meta.BundleContext) {
		return monitor.MonthlyPreparationBundle{}, ErrGenerationEvidence
	}
	return monitor.PrepareMonthlyCandidateBundleWithSameView(input, meta.BundleContext, now, func(i, c []byte) error {
		if generationHash(i) != w.input || generationHash(c) != w.metadata {
			return ErrGenerationEvidence
		}
		return ctx.Err()
	})
}

// Local source-to-candidate adapter only. It does NOT select a generation,
// retain a financial scope, write a bill, or install the no-history runtime.
type automaticHistoryCandidate struct {
	plan     GenerationPlan
	input    []byte
	scope    AutomaticScope
	metadata []byte
	bundle   monitor.MonthlyPreparationBundle
}

func (automaticHistoryCandidate) MarshalJSON() ([]byte, error) {
	return nil, errors.New("candidate adapter is not publication approval")
}
func (a *automaticHistoryCandidate) UnmarshalJSON([]byte) error {
	if a != nil {
		*a = automaticHistoryCandidate{}
	}
	return errors.New("candidate adapter must be rebuilt")
}

func (g *Generation) prepareAutomaticHistoryCandidate(parent context.Context, prepared automaticHistoryContextInput, directoryRef monthlysource.MonthlyObservationFile) (automaticHistoryCandidate, error) {
	var empty automaticHistoryCandidate
	if parent == nil || len(prepared.input.observed.Preparation) == 0 {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	var p GenerationPlan
	if json.Unmarshal(prepared.input.observed.Plan, &p) != nil {
		return empty, ErrGenerationInput
	}
	base, err := monitor.DecodeMonthlyPreparationInput(prepared.input.observed.Preparation)
	if err != nil {
		return empty, err
	}
	// Rebuild both the original proof and bridge from fresh originals. Never
	// use even the opaque earlier preparation's diagnostics as admission proof.
	var original monitor.MonthlyPreparationSource
	for _, source := range base.Sources {
		if source.ID == automaticHistorySourceID {
			original = source
		}
	}
	reader := *g
	reader.observationSource, reader.observationFile = nil, nil
	policy, err := selectedAutomaticOriginPolicy(p, base)
	if err != nil {
		return empty, err
	}
	audited, err := reader.auditAutomaticHistoryForPlan(ctx, monthlysource.MonthlyObservationFile{Path: original.Path, SHA256: original.SHA256}, SourcePreparationRequest{Owner: p.Owner, Month: p.Funding.Month}, policy)
	if err != nil {
		return empty, err
	}
	if err := verifyAutomaticHistoryInput(ctx, prepared.input.observed, audited, SourcePreparationRequest{Owner: p.Owner, Month: p.Funding.Month}); err != nil {
		return empty, err
	}
	dirRaw, err := g.readPrivatePreparationFile(ctx, directoryRef, 1<<20)
	if err != nil {
		return empty, err
	}
	if !bytes.Equal(dirRaw, prepared.directory) {
		return empty, ErrGenerationIntegrity
	}
	d, err := decodeAutomaticHistoryDirectory(dirRaw, p.Owner)
	if err != nil {
		return empty, err
	}
	views, err := projectAutomaticHistoryView(ctx, audited, d, p.Funding.Month)
	if err != nil {
		return empty, err
	}
	for i, v := range views {
		if v.OriginRequiresResolution && automaticOriginDecision(audited, v.UserID) == nil {
			return empty, ErrGenerationEvidence
		} // Do not hide a historical residual in an opening balance.
		if i >= len(base.Accounts) || base.Accounts[i].UserID != v.UserID {
			return empty, ErrGenerationEvidence
		}
		base.Accounts[i].Username = v.Username
	}
	for _, s := range base.Sources {
		if s.ID == automaticHistoryDirectorySourceID || s.Path == directoryRef.Path {
			return empty, ErrGenerationIntegrity
		}
	}
	base.Sources = append(base.Sources, monitor.MonthlyPreparationSource{ID: automaticHistoryDirectorySourceID, Path: directoryRef.Path, SHA256: directoryRef.SHA256})
	raw, err := json.Marshal(base)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.PreparationSHA256 = generationHash(raw)
	p, _, _, err = canonicalGeneration(p, raw, time.Now())
	if err != nil {
		return empty, err
	}
	scope, err := deriveAutomaticHistoryScope(ctx, p, audited, d)
	if err != nil {
		return empty, err
	}
	scopeRaw, err := json.Marshal(scope)
	if err != nil {
		return empty, err
	}
	planRaw, err := json.Marshal(p)
	if err != nil {
		return empty, err
	}
	witness := historyCandidateWitness{input: generationHash(raw), scope: generationHash(scopeRaw), plan: generationHash(planRaw)}
	ctx = context.WithValue(ctx, historyCandidateWitnessKey{}, witness)
	bound, release, err := g.bindObservationStore(ctx, p, base)
	if err != nil {
		return empty, err
	}
	defer release()
	windows, err := bound.auditWindows(ctx, p)
	if err != nil {
		return empty, err
	}
	metadata, err := observedAutomaticContext(ctx, p, raw, windows, scope)
	if err != nil {
		return empty, err
	}
	bundle, err := prepareRetainedContextBundle(ctx, p, raw, metadata, time.Now())
	if err != nil || bundle.ReadyForReview != 1 || bundle.Blocked != 0 {
		return empty, errors.Join(ErrGenerationEvidence, err)
	}
	if err := bound.verifyFiles(ctx, base.Sources); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return automaticHistoryCandidate{plan: p, input: raw, scope: scope, metadata: metadata.BundleContext, bundle: bundle}, nil
}

// Rebuilt from audited originals, never from cached candidate JSON or the
// selected scope document. Shared by first preparation and every later reuse.
func deriveAutomaticHistoryScope(ctx context.Context, p GenerationPlan, audited auditedAutomaticHistory, d automaticHistoryDirectory) (AutomaticScope, error) {
	views, err := projectAutomaticHistoryView(ctx, audited, d, p.Funding.Month)
	if err != nil {
		return AutomaticScope{}, err
	}
	scope := AutomaticScope{Group: monitor.MonthlyBundleGroup{GroupID: p.Owner.GroupID, Name: d.Name, Source: automaticHistoryDirectorySourceID + "#directory", Months: []string{p.Funding.Month}, Members: d.Members}}
	for _, v := range views {
		h := automaticOriginDecision(audited, v.UserID)
		if v.OriginRequiresResolution && h == nil {
			return AutomaticScope{}, ErrGenerationEvidence
		}
		balance, used, consume, refund, bridgeConsume, bridgeRefund := v.BalanceQuota, v.UsedQuota, v.MonthConsumeQuota, v.MonthRefundQuota, v.BridgeConsumeQuota, v.BridgeRefundQuota
		snapshot := automaticHistorySourceID + "#wallet-tail:" + v.OriginalSHA256[1]
		view := &monitor.MonthlySameViewSnapshot{Version: 1, Boundary: audited.tails.Boundary, CompletedAtMS: v.CompletedAtMS, OriginalSHA256: v.OriginalSHA256, Funding: []monitor.MonthlySameViewFunding{}}
		if h != nil {
			var tailConsume, tailRefund int64
			for _, pair := range audited.tails.Pairs {
				if pair.Newer.User.UserID == v.UserID {
					for _, row := range pair.Newer.Usage {
						if row.Type == 2 {
							if err := addOriginQuota(&tailConsume, row.Quota); err != nil {
								return AutomaticScope{}, err
							}
						}
						if row.Type == 6 {
							if err := addOriginQuota(&tailRefund, row.Quota); err != nil {
								return AutomaticScope{}, err
							}
						}
					}
				}
			}
			view.TailConsumeQuota, view.TailRefundQuota = &tailConsume, &tailRefund
		}
		ids := []int64{}
		for _, m := range v.Funding {
			if m.At < view.Boundary {
				ids = append(ids, m.LogID)
			} else {
				view.Funding = append(view.Funding, monitor.MonthlySameViewFunding{ID: m.LogID, At: m.At, DeltaQuota: m.DeltaQuota, Source: snapshot + ":funding:" + strconv.FormatInt(m.LogID, 10) + ":" + m.SourceDigest})
			}
		}
		prefix := automaticHistorySourceID + "#user:" + strconv.FormatInt(v.UserID, 10)
		scope.Accounts = append(scope.Accounts, AutomaticWalletScope{UserID: v.UserID, WalletOnly: true, WalletSource: prefix + ":wallet-source", Reconstructions: []monitor.MonthlySnapshotReconstruction{{
			UserID: v.UserID, Month: v.Month, ObservedAt: v.CompletedAtMS / 1000, SnapshotSource: snapshot, ScopeSource: prefix + ":same-view", MonthSource: prefix + ":closed-month", BridgeSource: prefix + ":complete-bridge", FundingSource: prefix + ":funding", OpeningBasisSource: prefix + ":registered-origin",
			BalanceQuota: &balance, UsedQuota: &used, MonthConsumeQuota: &consume, MonthRefundQuota: &refund, BridgeConsumeQuota: &bridgeConsume, BridgeRefundQuota: &bridgeRefund, FundingEventIDs: ids, SameView: view, HistoricalBalance: h,
		}}})
	}
	return scope, ctx.Err()
}
