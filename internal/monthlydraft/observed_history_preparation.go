package monthlydraft

import (
	"context"
	"encoding/json"
	"reflect"
	"slices"
	"sort"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// A pinned history catalogue may already contain this month's original log
// aggregates and movements. Compare them to the SAME sealed raw sources first;
// do not silently discard them to make the absent-only normalizers succeed.
// No balances, counters, cutoffs or historical-scope assertions are derived.
func (g *Generation) prepareObservedHistoryInput(ctx context.Context, plan GenerationPlan, raw []byte, sourceID string) (ObservedPreparation, error) {
	var empty ObservedPreparation
	p, _, input, err := canonicalGeneration(plan, raw, time.Now())
	if err != nil {
		return empty, err
	}
	changed, err := g.detachObservedHistoryTokens(ctx, p, &input)
	if err != nil {
		return empty, err
	}
	if changed {
		raw, err = json.Marshal(input)
		if err != nil || len(raw) > monitor.MonthlyPreparationInputLimit {
			return empty, ErrGenerationCapacity
		}
		p.PreparationSHA256 = generationHash(raw)
	}
	month, err := time.ParseInLocation("2006-01", p.Funding.Month, time.FixedZone("Usage-UTC+8", 8*3600))
	if err != nil {
		return empty, ErrGenerationInput
	}
	from, to := month.Unix(), month.AddDate(0, 1, 0).Unix()
	retained := map[int64]monitor.MonthlyPreparationAccount{}
	for _, a := range input.Accounts {
		for _, s := range a.Spans {
			if s.From < to && s.To > from {
				retained[a.UserID] = a
			}
		}
		for _, m := range a.Movements {
			if m.At >= from && m.At < to {
				retained[a.UserID] = a
			}
		}
	}
	if len(retained) == 0 {
		return g.PrepareObservedInput(ctx, p, raw, sourceID)
	}
	g, release, err := g.bindObservationStore(ctx, p, input)
	if err != nil {
		return empty, err
	}
	defer release()
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
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
	usage, err := g.reviewPlanUsage(ctx, p, input)
	if err != nil {
		return empty, err
	}
	byUsage := map[int64]monthlysource.UsageMonthAccountReport{}
	for _, a := range usage.Report().Accounts {
		byUsage[a.UserID] = a
	}
	byFunding := map[int64]monthlysource.FundingMoneyAccount{}
	for _, a := range funding.Accounts {
		byFunding[a.UserID] = a
	}
	// Match EACH account and each whole source window. Opposite consumption
	// and refund errors or another account's difference cannot cancel here.
	preserveSpans := map[int64]bool{}
	for id, a := range retained {
		if err := ctx.Err(); err != nil {
			return empty, err
		}
		for _, s := range a.Spans {
			if s.From < to && s.To > from {
				preserveSpans[id] = true
			}
		}
		if preserveSpans[id] && !sameUsageSpans(ctx, a, byUsage[id], from, to) {
			return empty, ErrGenerationEvidence
		}
		f, exists := byFunding[id]
		if !exists || f.Status != "observed_month" || f.Totals == nil || !sameMovements(a, f, from, to) {
			return empty, ErrGenerationEvidence
		}
	}
	// Detach only the already-compared target records in memory. All original
	// bytes and references remain in the pinned catalogue and source list.
	for i := range input.Accounts {
		a := &input.Accounts[i]
		if _, exists := retained[a.UserID]; !exists {
			continue
		}
		a.Spans = slices.DeleteFunc(slices.Clone(a.Spans), func(s monitor.MonthlyPreparationSpan) bool { return s.From < to && s.To > from })
		a.Movements = slices.DeleteFunc(slices.Clone(a.Movements), func(m monitor.MonthlyPreparationMovement) bool { return m.At >= from && m.At < to })
	}
	template, err := json.Marshal(input)
	if err != nil || len(template) > monitor.MonthlyPreparationInputLimit {
		return empty, ErrGenerationCapacity
	}
	p.PreparationSHA256 = generationHash(template)
	normalized, err := g.PrepareObservedInput(ctx, p, template, sourceID)
	if err != nil {
		return empty, err
	}
	// Retain previously known request/token statistics and original interval
	// granularity, rather than downgrade them to unknown or interpolate them.
	// The existing raw usage auditor will continue to recheck these intervals.
	if len(preserveSpans) != 0 {
		p, input, err = restoreObservedHistorySpans(normalized, retained, preserveSpans, from, to)
		if err != nil {
			return empty, err
		}
		if _, err := g.auditUsageInput(ctx, p, input); err != nil {
			return empty, err
		}
		if _, err := g.auditTokenInput(ctx, p, input); err != nil {
			return empty, err
		}
		body, err := json.Marshal(input)
		if err != nil || len(body) > monitor.MonthlyPreparationInputLimit {
			return empty, ErrGenerationCapacity
		}
		p.PreparationSHA256 = generationHash(body)
		_, canonical, _, err := canonicalGeneration(p, body, time.Now())
		if err != nil {
			return empty, err
		}
		normalized.Plan, normalized.Preparation = canonical, body
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return empty, err
	}
	return normalized, ctx.Err()
}

// Original history may already include token-month projections. Verify EACH
// projection against its own pinned archive, then against the newly selected
// original archive before detaching it IN MEMORY for the absent-only projector.
// Never clear unverified metadata to make a normalizer pass. Old source files
// stay in the final source set; original history/saved bills are not rewritten.
func (g *Generation) detachObservedHistoryTokens(ctx context.Context, p GenerationPlan, input *monitor.MonthlyPreparationInput) (bool, error) {
	needed := false
	for _, a := range input.Accounts {
		needed = needed || len(a.TokenMonths) != 0
	}
	if !needed {
		return false, nil
	}
	if err := g.verifyFiles(ctx, input.Sources); err != nil {
		return false, err
	}
	selected := map[int64]string{}
	for _, s := range p.TokenUsageSources {
		selected[s.UserID] = s.SourceID
	}
	// Only mutate after ALL accounts pass: no half-normalized projection can
	// escape a mismatch, even when organization-level totals happen to match.
	for _, a := range input.Accounts {
		if len(a.TokenMonths) == 0 {
			continue
		}
		if len(a.TokenMonths) != 1 || a.TokenMonths[0].UserID != a.UserID || a.TokenMonths[0].Month != p.Funding.Month || selected[a.UserID] == "" {
			return false, ErrGenerationEvidence
		}
		old := a.TokenMonths[0]
		oldSource, err := tokenSource(*input, tokenReferenceSource(old.Source))
		if err != nil {
			return false, err
		}
		_, oldReport, err := g.loadTokenArchive(ctx, oldSource, p, a.UserID)
		if err != nil {
			return false, err
		}
		if !reflect.DeepEqual(old, projectTokenReport(oldReport, oldSource.ID)) {
			return false, ErrGenerationEvidence
		}
		newSource, err := tokenSource(*input, selected[a.UserID])
		if err != nil {
			return false, err
		}
		_, newReport, err := g.loadTokenArchive(ctx, newSource, p, a.UserID)
		if err != nil {
			return false, err
		}
		expected := projectTokenReport(newReport, newSource.ID)
		old.Source = expected.Source // reference names may differ, money/IDs may not
		if !reflect.DeepEqual(old, expected) {
			return false, ErrGenerationEvidence
		}
	}
	for i := range input.Accounts {
		input.Accounts[i].TokenMonths = nil
	}
	return true, nil
}

func restoreObservedHistorySpans(normalized ObservedPreparation, retained map[int64]monitor.MonthlyPreparationAccount,
	selected map[int64]bool, from, to int64) (GenerationPlan, monitor.MonthlyPreparationInput, error) {
	var p GenerationPlan
	input, err := monitor.DecodeMonthlyPreparationInput(normalized.Preparation)
	if err != nil || json.Unmarshal(normalized.Plan, &p) != nil {
		return p, input, ErrGenerationInput
	}
	for i := range input.Accounts {
		a := &input.Accounts[i]
		if !selected[a.UserID] {
			continue
		}
		a.Spans = slices.DeleteFunc(a.Spans, func(s monitor.MonthlyPreparationSpan) bool { return s.From < to && s.To > from })
		for _, s := range retained[a.UserID].Spans {
			if s.From < to && s.To > from {
				a.Spans = append(a.Spans, s)
			}
		}
		sort.Slice(a.Spans, func(i, j int) bool { return a.Spans[i].From < a.Spans[j].From })
	}
	return p, input, nil
}
