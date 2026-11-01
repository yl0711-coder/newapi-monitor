package monthlydraft

import (
	"bytes"
	"context"
	"encoding/json"
	"strings"
	"syscall"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// RetainedAutomaticScope is PRIVATE trusted preparation, not a customer DTO or
// proof manufactured from matching balances. The producer must independently
// establish its historical directory, wallet and reconstruction semantics.
// Its file digest belongs in preparation.Sources, hence the immutable task
// identity. Do not include that task/input digest here: it would be circular.
type RetainedAutomaticScope struct {
	Version        int                `json:"version"`
	Owner          monthlytasks.Owner `json:"owner"`
	Month          string             `json:"month"`
	SourceIdentity string             `json:"source_identity"`
	Scope          AutomaticScope     `json:"scope"`
}

var retainedScopeSlots = make(chan struct{}, 1)

func validScopeSourceID(id string) bool {
	if len(id) == 0 || len(id) > 128 {
		return false
	}
	for _, ch := range id {
		if !(ch >= 'a' && ch <= 'z' || ch >= 'A' && ch <= 'Z' || ch >= '0' && ch <= '9' || ch == '_' || ch == '-' || ch == '.') {
			return false
		}
	}
	return true
}

// NewRetainedAutomaticScopeLoader reads a named source from the EXACT retained
// task, never an HTTP path or current membership used as historical evidence.
// It owns no resources; the Generation owner must drain callers before Close.
// Construction does not read files or start work. Use it through the observed
// context adapter, which still audits original funding/usage/wallet and scope.
func NewRetainedAutomaticScopeLoader(g *Generation, sourceID string) (AutomaticScopeLoader, error) {
	if g == nil || g.receiver == nil || g.root == nil || g.verifyCurrent == nil || !validScopeSourceID(sourceID) {
		return nil, ErrGenerationInput
	}
	return func(parent context.Context, request monthlytasks.Request) (AutomaticScope, error) {
		if parent == nil {
			return AutomaticScope{}, ErrGenerationInput
		}
		if err := parent.Err(); err != nil {
			return AutomaticScope{}, err
		}
		select {
		case retainedScopeSlots <- struct{}{}:
			defer func() { <-retainedScopeSlots }()
		default:
			return AutomaticScope{}, ErrGenerationBusy
		}
		ctx, cancel := context.WithTimeout(parent, 8*time.Second)
		defer cancel()
		if err := g.current(ctx, request); err != nil {
			return AutomaticScope{}, err
		}
		saved, err := g.loadRetainedGeneration(ctx, request, time.Now())
		if err != nil {
			return AutomaticScope{}, err
		}
		if saved.plan.AutomaticScopeSourceID != sourceID {
			return AutomaticScope{}, ErrGenerationEvidence
		}
		scope, err := g.selectedRetainedScope(ctx, saved.plan, saved.input)
		if err != nil {
			return AutomaticScope{}, err
		}
		if err := g.current(ctx, request); err != nil {
			return AutomaticScope{}, err
		}
		if err := ctx.Err(); err != nil {
			return AutomaticScope{}, err
		}
		return scope, nil
	}, nil
}

// Shared by pre-admission inspection and task execution. The caller owns its
// existing authority/source checks; this does not load an alternate task or
// infer a trusted scope from current balances. The exact selected file, owner,
// month, source identity and independent references remain mandatory.
func (g *Generation) selectedRetainedScope(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (AutomaticScope, error) {
	if !validScopeSourceID(p.AutomaticScopeSourceID) {
		return AutomaticScope{}, ErrGenerationEvidence
	}
	var source monitor.MonthlyPreparationSource
	for _, candidate := range input.Sources {
		if candidate.ID == p.AutomaticScopeSourceID {
			source = candidate
		}
	}
	if source.ID == "" {
		return AutomaticScope{}, ErrGenerationEvidence
	}
	rel, err := g.relativeSource(source.Path)
	if err != nil {
		return AutomaticScope{}, err
	}
	info, err := g.root.Lstat(rel)
	if err != nil {
		return AutomaticScope{}, ErrGenerationIntegrity
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat.Nlink != 1 {
		return AutomaticScope{}, ErrGenerationIntegrity
	}
	raw, err := readArchiveFile(ctx, g.root, rel, monitor.MonthlyPreparationInputLimit)
	if err != nil {
		return AutomaticScope{}, err
	}
	if generationHash(raw) != source.SHA256 {
		return AutomaticScope{}, ErrGenerationIntegrity
	}
	var document RetainedAutomaticScope
	if json.Unmarshal(raw, &document) != nil {
		return AutomaticScope{}, ErrGenerationIntegrity
	}
	canonical, err := json.Marshal(document)
	if err != nil || !bytes.Equal(bytes.TrimSpace(raw), canonical) || document.Version != 1 ||
		document.Owner != p.Owner || document.Month != p.Funding.Month || document.SourceIdentity != p.Funding.SourceIdentity {
		return AutomaticScope{}, ErrGenerationIntegrity
	}
	if !independentRetainedScope(document.Scope, source) {
		return AutomaticScope{}, ErrGenerationEvidence
	}
	if _, err := g.verifyFile(ctx, source, monitor.MonthlyPreparationInputLimit); err != nil {
		return AutomaticScope{}, err
	}
	return document.Scope, ctx.Err()
}

func independentRetainedScope(scope AutomaticScope, source monitor.MonthlyPreparationSource) bool {
	self := func(ref string) bool { id, _, _ := strings.Cut(ref, "#"); return id == source.ID }
	if self(scope.Group.Source) {
		return false
	}
	for _, account := range scope.Accounts {
		if self(account.WalletSource) || (account.Registration != nil &&
			(self(account.Registration.Source) || self(account.Registration.NoEarlierActivitySource))) {
			return false
		}
		for _, r := range account.Reconstructions {
			if r.HistoricalBalance != nil && (self(r.HistoricalBalance.DecisionSource) || r.HistoricalBalance.DecisionSHA256 == source.SHA256) {
				return false
			}
			for _, ref := range []string{r.SnapshotSource, r.ScopeSource, r.MonthSource, r.BridgeSource, r.FundingSource, r.OpeningBasisSource} {
				if self(ref) {
					return false
				}
			}
		}
	}
	for _, correction := range scope.Corrections {
		for _, observation := range correction.Evidence.Observations {
			if observation.SourceSHA256 == source.SHA256 {
				return false
			}
		}
	}
	return true
}
