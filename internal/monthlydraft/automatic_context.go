package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"sort"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// AutomaticScope is PRIVATE source-preparation metadata, never customer input.
// The trusted producer must establish directory, wallet-only, boundary and
// refund semantics from original records. Hashes/arithmetic cannot attest them.
// Funding event kind/method are deliberately absent: they come from the audited
// source-page normalizer, not this scope or a manually prepared customer bill.
type AutomaticScope struct {
	Group       monitor.MonthlyBundleGroup
	Accounts    []AutomaticWalletScope
	Corrections []AutomaticAccountCorrection
}

type AutomaticWalletScope struct {
	UserID          int64
	WalletOnly      bool
	WalletSource    string
	Registration    *monitor.MonthlyBundleRegistration
	Reconstructions []monitor.MonthlySnapshotReconstruction
}

// AutomaticScopeLoader must read trusted retained source preparation for the
// exact Request, honoring ctx. It must not use current memberships as proof of
// historical wallet semantics, or set WalletOnly just because totals match.
type AutomaticScopeLoader func(context.Context, monthlytasks.Request) (AutomaticScope, error)

// NewObservedAutomaticContext connects retained original funding pages to the
// automatic publisher's metadata dependency. Construction starts nothing and
// registers no routes. Reads use the Generation's existing bounded, read-only
// source handles; no upstream connections, migrations, repair or publications occur.
// Independent balance/refund/wallet/directory production remains explicit.
func NewObservedAutomaticContext(g *Generation, load AutomaticScopeLoader) (AutomaticContext, error) {
	if g == nil || g.receiver == nil || g.funding == nil || g.root == nil || g.verifyCurrent == nil || load == nil {
		return nil, ErrGenerationInput
	}
	return func(parent context.Context, request monthlytasks.Request) (AutomaticSourceContext, error) {
		if parent == nil {
			return AutomaticSourceContext{}, ErrGenerationInput
		}
		ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
		defer cancel()
		if err := g.current(ctx, request); err != nil {
			return AutomaticSourceContext{}, err
		}
		saved, err := g.loadRetainedGeneration(ctx, request, time.Now())
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		g, releaseSource, err := g.bindObservationStore(ctx, saved.plan, saved.input)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		defer releaseSource()
		if err := g.verifyFiles(ctx, saved.input.Sources); err != nil {
			return AutomaticSourceContext{}, err
		}
		if _, err := g.auditUsageInput(ctx, saved.plan, saved.input); err != nil {
			return AutomaticSourceContext{}, err
		}
		if _, err := g.auditWalletInput(ctx, saved.plan, saved.input); err != nil {
			return AutomaticSourceContext{}, err
		}
		if _, err := g.auditTokenInput(ctx, saved.plan, saved.input); err != nil {
			return AutomaticSourceContext{}, err
		}
		scope, err := load(ctx, request)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		if saved.plan.AutomaticScopeSourceID != "" {
			// A retained selection cannot be bypassed by accidentally composing
			// a different trusted callback after a restart/configuration change.
			pinned, err := NewRetainedAutomaticScopeLoader(g, saved.plan.AutomaticScopeSourceID)
			if err != nil {
				return AutomaticSourceContext{}, err
			}
			selected, err := pinned(ctx, request)
			if err != nil {
				return AutomaticSourceContext{}, err
			}
			want, err := json.Marshal(selected)
			if err != nil {
				return AutomaticSourceContext{}, err
			}
			got, err := json.Marshal(scope)
			if err != nil || !bytes.Equal(want, got) {
				return AutomaticSourceContext{}, ErrGenerationEvidence
			}
		}
		windows, err := g.auditWindows(ctx, saved.plan)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		out, err := g.observedAutomaticContext(ctx, saved.plan, saved.inputRaw, windows, scope)
		if err != nil {
			return AutomaticSourceContext{}, err
		}
		if err := g.verifyFiles(ctx, saved.input.Sources); err != nil {
			return AutomaticSourceContext{}, err
		}
		if err := g.current(ctx, request); err != nil {
			return AutomaticSourceContext{}, err
		}
		if err := ctx.Err(); err != nil {
			return AutomaticSourceContext{}, err
		}
		return out, nil
	}, nil
}

func observedAutomaticContext(ctx context.Context, plan GenerationPlan, raw []byte, windows []monthlysource.FundingWindowObservation, scope AutomaticScope, registrations ...*registrationContext) (AutomaticSourceContext, error) {
	p, _, input, err := canonicalGeneration(plan, raw, time.Now())
	if err != nil {
		return AutomaticSourceContext{}, err
	}
	if scope.Group.GroupID != p.Owner.GroupID || len(scope.Group.Months) != 1 || scope.Group.Months[0] != p.Funding.Month ||
		len(scope.Group.Members) != len(p.Funding.UserIDs) || len(scope.Accounts) != len(p.Funding.UserIDs) || len(scope.Corrections) > len(scope.Accounts) {
		return AutomaticSourceContext{}, ErrGenerationEvidence
	}
	// Detach caller-owned slices (including nested reconstruction/correction
	// observations). Returned metadata cannot change after the loader returns.
	scopeRaw, err := json.Marshal(scope)
	if err != nil || len(scopeRaw) > monitor.MonthlyPreparationInputLimit {
		return AutomaticSourceContext{}, ErrGenerationCapacity
	}
	var registration *registrationContext
	if len(registrations) > 1 {
		return AutomaticSourceContext{}, ErrGenerationInput
	}
	if len(registrations) == 1 {
		registration = registrations[0]
		if registration == nil || registration.input != generationHash(raw) || registration.scope != generationHash(scopeRaw) {
			return AutomaticSourceContext{}, ErrGenerationEvidence
		}
	}
	if err := json.Unmarshal(scopeRaw, &scope); err != nil {
		return AutomaticSourceContext{}, ErrGenerationInput
	}
	sort.Slice(scope.Group.Members, func(i, j int) bool { return scope.Group.Members[i].UserID < scope.Group.Members[j].UserID })
	funding, err := monthlysource.ReviewFundingMoney(ctx, p.Funding, windows)
	if err != nil {
		return AutomaticSourceContext{}, err
	}
	metadata := map[int64]AutomaticWalletScope{}
	for _, a := range scope.Accounts {
		if _, duplicate := metadata[a.UserID]; duplicate || a.UserID <= 0 || !a.WalletOnly || a.WalletSource == "" {
			return AutomaticSourceContext{}, ErrGenerationEvidence
		}
		if p.WalletObservations != nil {
			prefix := p.WalletObservations.SourceID + "#wallet:"
			if strings.HasPrefix(a.WalletSource, prefix) || (a.Registration != nil &&
				(strings.HasPrefix(a.Registration.Source, prefix) || strings.HasPrefix(a.Registration.NoEarlierActivitySource, prefix))) {
				return AutomaticSourceContext{}, ErrGenerationEvidence
			}
			for _, r := range a.Reconstructions {
				for _, ref := range []string{r.SnapshotSource, r.ScopeSource, r.MonthSource, r.BridgeSource, r.FundingSource, r.OpeningBasisSource} {
					if strings.HasPrefix(ref, prefix) {
						return AutomaticSourceContext{}, ErrGenerationEvidence
					}
				}
			}
		}
		metadata[a.UserID] = a
	}
	accounts := map[int64]monitor.MonthlyPreparationAccount{}
	for _, a := range input.Accounts {
		accounts[a.UserID] = a
	}
	result := monitor.MonthlyBundleContext{Version: 1, InputSHA256: generationHash(raw), Groups: []monitor.MonthlyBundleGroup{scope.Group}, Accounts: []monitor.MonthlyBundleAccountContext{}}
	for _, observed := range funding.Accounts { // canonical per-user order
		if err := ctx.Err(); err != nil {
			return AutomaticSourceContext{}, err
		}
		a, exists := accounts[observed.UserID]
		meta, scoped := metadata[observed.UserID]
		if !exists || !scoped || observed.Status != "observed_month" || observed.Totals == nil ||
			!sameMovements(a, observed, funding.Windows.From, funding.Windows.To) {
			return AutomaticSourceContext{}, ErrGenerationEvidence
		}
		byID := make(map[int64]monitor.MonthlyPreparationMovement, len(a.Movements))
		for _, m := range a.Movements {
			if m.At >= funding.Windows.From && m.At < funding.Windows.To {
				byID[m.ID] = m
			}
		}
		entry := monitor.MonthlyBundleAccountContext{UserID: a.UserID, WalletOnly: true, WalletSource: meta.WalletSource,
			Events: []monitor.MonthlyBundleEvent{}, Registration: meta.Registration, Reconstructions: meta.Reconstructions}
		for _, movement := range observed.Movements {
			// TargetUserID was resolved from the original event, NOT its owner
			// or operator. Both sides of transfers remain distinct entries.
			m := byID[movement.LogID]
			entry.Events = append(entry.Events, monitor.MonthlyBundleEvent{ID: movement.LogID, At: movement.At, DeltaQuota: movement.DeltaQuota,
				Kind: movement.Kind, Method: movement.Method, Source: m.Source})
		}
		if meta.Registration != nil && meta.Registration.At >= funding.Windows.To {
			if registration == nil {
				return AutomaticSourceContext{}, ErrGenerationEvidence
			}
			event, exists := registration.events[a.UserID]
			if !exists {
				return AutomaticSourceContext{}, ErrGenerationEvidence
			}
			// This supports the proof of a not-yet-registered member. The
			// ordinary month filter must NOT book this future gift as a credit.
			entry.Events = append(entry.Events, event)
		}
		result.Accounts = append(result.Accounts, entry)
	}
	contextRaw, err := json.Marshal(result)
	if err != nil || len(contextRaw) > monitor.MonthlyPreparationInputLimit {
		return AutomaticSourceContext{}, ErrGenerationCapacity
	}
	// Reuse the existing strict scope/reference/accounting validator. This
	// generates only a private candidate, never a reviewed or published bill.
	bundle, err := prepareHistoryContextBundle(ctx, p, raw, scopeRaw, contextRaw, time.Now())
	if err != nil || len(bundle.Entries) != 1 || bundle.ReadyForReview != 1 || bundle.Blocked != 0 || bundle.Entries[0].Candidate == nil {
		return AutomaticSourceContext{}, ErrGenerationEvidence
	}
	if err := ctx.Err(); err != nil {
		return AutomaticSourceContext{}, err
	}
	meta := AutomaticSourceContext{BundleContext: contextRaw, Corrections: scope.Corrections}
	if p.AutomaticHistorySourceID != "" {
		w := ctx.Value(historyCandidateWitnessKey{}).(historyCandidateWitness)
		w.metadata = generationHash(contextRaw)
		meta.historyWitness = &w
	}
	return meta, nil
}
