package monthlydraft

import (
	"context"
	"encoding/json"
	"math"
	"slices"
	"sort"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlysource"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// auditUsageInput adds an independent observed-log check, not an attestation of
// wallet boundaries or successful refund application. It never changes the
// normalized amounts, unknown statistics, source records or existing bills.
// Legacy plans keep their existing independent-source contract; capture IDs
// must be explicitly selected before this additional path is enabled.
func (g *Generation) auditUsageInput(ctx context.Context, p GenerationPlan, input monitor.MonthlyPreparationInput) (string, error) {
	if err := g.requireObservationStore(p); err != nil {
		return "", err
	}
	if !planHasUsage(p) {
		return "", nil
	}
	observed, err := g.reviewPlanUsage(ctx, p, input)
	if err != nil {
		return "", err // Cancellation/busy/SQL failures are not proven bad money.
	}
	report := observed.Report()
	if len(report.Accounts) != len(input.Accounts) {
		return "", ErrGenerationEvidence
	}
	accounts := make(map[int64]monitor.MonthlyPreparationAccount, len(input.Accounts))
	for _, a := range input.Accounts {
		if _, duplicate := accounts[a.UserID]; duplicate {
			return "", ErrGenerationEvidence
		}
		accounts[a.UserID] = a
	}
	for _, a := range report.Accounts {
		candidate, exists := accounts[a.UserID]
		if !exists || !sameUsageSpans(ctx, candidate, a, report.From, report.To) {
			if err := ctx.Err(); err != nil {
				return "", err
			}
			return "", ErrGenerationEvidence
		}
		delete(accounts, a.UserID)
	}
	if err := ctx.Err(); err != nil {
		return "", err
	}
	raw, err := json.Marshal(report)
	if err != nil {
		return "", err
	}
	return generationHash(raw), nil
}

// A normalized span may combine whole observed windows, never split one or
// use monthly netting to conceal offsetting consume/refund or time differences.
// Only explicit wallet rows are projected by this first adapter. Subscription
// and unknown sources need independent scope handling, not a wallet guess.
func sameUsageSpans(ctx context.Context, a monitor.MonthlyPreparationAccount, observed monthlysource.UsageMonthAccountReport, from, to int64) bool {
	if a.UserID != observed.UserID || observed.Totals == nil || len(observed.MissingWindows) != 0 ||
		observed.MatchingSeconds != to-from || observed.ObservedSeconds != to-from {
		return false
	}
	if futureZeroOrigin(a, to) {
		return emptyObservedBeforeOrigin(a, observed, to)
	}
	spans := slices.Clone(a.Spans)
	sort.Slice(spans, func(i, j int) bool { return spans[i].From < spans[j].From })
	cursor, index := from, 0
	var monthRequests int64
	for _, span := range spans {
		if ctx.Err() != nil {
			return false
		}
		if span.To <= from || span.From >= to {
			continue // baseline/history bridges retain their independent checks
		}
		if span.UserID != a.UserID || span.From != cursor || span.To <= span.From || span.To > to || span.ConsumeQuota < 0 || span.RefundQuota < 0 {
			return false
		}
		var consume, refund, requests int64
		for cursor < span.To {
			if index >= len(observed.Windows) {
				return false
			}
			w := observed.Windows[index]
			if !w.Matching || len(w.Issues) != 0 || w.From != cursor || w.To <= w.From || w.To > span.To {
				return false
			}
			for _, row := range w.Rows {
				if row.BillingSource != "wallet" || row.UserID != a.UserID || row.Quota < 0 || row.Records <= 0 {
					return false
				}
				target := &consume
				if row.Type == 6 {
					target = &refund
				} else if row.Type != 2 || row.Records > math.MaxInt64-requests {
					return false
				} else {
					requests += row.Records
				}
				if row.Quota > math.MaxInt64-*target {
					return false
				}
				*target += row.Quota
			}
			cursor = w.To
			index++
		}
		if consume != span.ConsumeQuota || refund != span.RefundQuota || (span.Requests != nil && *span.Requests != requests) {
			return false
		}
		if requests > math.MaxInt64-monthRequests {
			return false
		}
		monthRequests += requests
	}
	month := time.Unix(from, 0).In(time.FixedZone("Usage-UTC+8", 8*3600)).Format("2006-01")
	for _, s := range a.MonthStatistics {
		if s.Month == month && s.Requests != monthRequests {
			return false
		}
	}
	return cursor == to && index == len(observed.Windows)
}
