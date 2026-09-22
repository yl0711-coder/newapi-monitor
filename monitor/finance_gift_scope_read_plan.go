//go:build unix

package monitor

import (
	"context"
	"errors"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

// PrepareFinanceGiftReadPlan freezes a freshly verified local suggestion.
// It does NOT establish a source connection or authorize production reads.
func PrepareFinanceGiftReadPlan(ctx context.Context, dir, confirmation string) (financegiftexport.BatchPlan, string, error) {
	var plan financegiftexport.BatchPlan
	suggestion, err := SuggestFinanceGiftLocalCandidates(ctx, dir, confirmation)
	if err != nil {
		return plan, "", err
	}
	if suggestion.Status != "ready" || len(suggestion.Entries) == 0 {
		return plan, "", errors.New("read plan requires a nonempty verified, unblocked suggestion")
	}
	plan.SourceEpoch = suggestion.SourceEpoch
	for _, e := range suggestion.Entries {
		plan.Targets = append(plan.Targets, financegiftexport.BatchTarget{
			UserID: e.UserID, HourTs: e.HourTs, ExpectedRows: e.Rows, LocalContentHash: e.ContentHash,
		})
	}
	digest, err := financegiftexport.BatchConfirmation(plan)
	return plan, digest, err
}
