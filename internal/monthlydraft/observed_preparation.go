package monthlydraft

import (
	"context"
	"encoding/json"
	"sort"
	"strconv"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// ObservedPreparation is private source input, not a saved or approved bill.
// Independent history, directory, wallet scope and actual-refund evidence must
// already be supplied by the trusted template producer. No source is invented.
type ObservedPreparation struct {
	Plan, Preparation                        []byte
	FundingSHA256, UsageSHA256, WalletSHA256 string
	TokenSHA256                              string
}

var observedPreparationSlots = make(chan struct{}, 1)

// PrepareObservedInput composes funding, usage and wallet normalization in one
// bounded, read-only operation. Failure returns no partial input. It never
// initializes a DB, retains/selects a plan, submits a task or publishes a bill.
// sourceID must identify the actual sealed observation DB for all three inputs.
// Callers must still use PrepareGenerationInput and the publication re-audits.
func (g *Generation) PrepareObservedInput(parent context.Context, plan GenerationPlan, template []byte, sourceID string) (ObservedPreparation, error) {
	var empty ObservedPreparation
	if parent == nil || g == nil || g.funding == nil || g.root == nil || sourceID == "" {
		return empty, ErrGenerationInput
	}
	ctx, cancel := context.WithTimeout(parent, time.Minute)
	defer cancel()
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	select {
	case observedPreparationSlots <- struct{}{}:
		defer func() { <-observedPreparationSlots }()
	default:
		return empty, ErrGenerationBusy
	}
	p, _, input, err := canonicalGeneration(plan, template, time.Now())
	if err != nil {
		return empty, err
	}
	if p.ObservationStoreSourceID != "" && p.ObservationStoreSourceID != sourceID {
		return empty, ErrGenerationInput
	}
	g, releaseSource, err := g.bindObservationStore(ctx, p, input)
	if err != nil {
		return empty, err
	}
	defer releaseSource()
	if !planHasUsage(p) || p.WalletObservations == nil || p.WalletObservations.SourceID != sourceID {
		return empty, ErrGenerationInput
	}
	var source monitor.MonthlyPreparationSource
	for _, s := range input.Sources {
		if s.ID == sourceID {
			source = s
		}
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	if err := g.sealedUsageSource(ctx, source); err != nil {
		return empty, err
	}
	windows, err := g.auditWindows(ctx, p)
	if err != nil {
		return empty, err
	}
	funding, err := monthlysource.ReviewFundingMoney(ctx, p.Funding, windows)
	if err != nil {
		return empty, err
	}
	if err := projectObservedFunding(ctx, &input, funding, sourceID); err != nil {
		return empty, err
	}
	raw, err := json.Marshal(input)
	if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.PreparationSHA256 = generationHash(raw)
	usage, err := g.PrepareUsageSpans(ctx, p, raw, sourceID)
	if err != nil {
		return empty, err
	}
	if err := json.Unmarshal(usage.Plan, &p); err != nil {
		return empty, err
	}
	wallet, err := g.PrepareWalletObservations(ctx, p, usage.Preparation)
	if err != nil {
		return empty, err
	}
	if err := g.sealedUsageSource(ctx, source); err != nil {
		return empty, err
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	fundingRaw, err := json.Marshal(funding)
	if err != nil {
		return empty, err
	}
	out := ObservedPreparation{Plan: wallet.Plan, Preparation: wallet.Preparation,
		FundingSHA256: generationHash(fundingRaw), UsageSHA256: usage.UsageSHA256, WalletSHA256: wallet.WalletSHA256}
	if len(p.TokenUsageSources) != 0 {
		var tokenPlan GenerationPlan
		if err := json.Unmarshal(wallet.Plan, &tokenPlan); err != nil {
			return empty, err
		}
		tokens, err := g.PrepareTokenUsageInput(ctx, tokenPlan, wallet.Preparation)
		if err != nil {
			return empty, err
		}
		out.Plan, out.Preparation, out.TokenSHA256 = tokens.Plan, tokens.Preparation, tokens.TokenSHA256
	}
	return out, ctx.Err()
}

func projectObservedFunding(ctx context.Context, input *monitor.MonthlyPreparationInput, funding monthlysource.FundingMoneyReview, sourceID string) error {
	if len(input.Accounts) != len(funding.Accounts) {
		return ErrGenerationEvidence
	}
	accounts := make(map[int64]int, len(input.Accounts))
	ids := make(map[int64]bool)
	for i, a := range input.Accounts {
		accounts[a.UserID] = i
		for _, m := range a.Movements {
			if ids[m.ID] || (m.At >= funding.Windows.From && m.At < funding.Windows.To) {
				return ErrGenerationInput // never overwrite even an equal prior amount
			}
			ids[m.ID] = true
		}
	}
	for _, observed := range funding.Accounts {
		if err := ctx.Err(); err != nil {
			return err
		}
		i, exists := accounts[observed.UserID]
		if !exists || observed.Status != "observed_month" || observed.Totals == nil {
			return ErrGenerationEvidence // an unknown month is not an empty month
		}
		a := &input.Accounts[i]
		for _, m := range observed.Movements {
			if ids[m.LogID] {
				return ErrGenerationEvidence // also reject collisions with history/other members
			}
			ids[m.LogID] = true
			a.Movements = append(a.Movements, monitor.MonthlyPreparationMovement{
				UserID: a.UserID, ID: m.LogID, At: m.At, DeltaQuota: m.DeltaQuota,
				Source: sourceID + "#funding:" + m.PageID + ":row:" + m.SourceDigest + ":id:" + strconv.FormatInt(m.LogID, 10),
			})
		}
		if a.Movements == nil {
			a.Movements = []monitor.MonthlyPreparationMovement{}
		}
		if len(a.Movements) > 25000 || !sameMovements(*a, observed, funding.Windows.From, funding.Windows.To) {
			return ErrGenerationEvidence
		}
		sort.Slice(a.Movements, func(i, j int) bool {
			if a.Movements[i].At != a.Movements[j].At {
				return a.Movements[i].At < a.Movements[j].At
			}
			return a.Movements[i].ID < a.Movements[j].ID
		})
		delete(accounts, a.UserID)
	}
	return nil
}
