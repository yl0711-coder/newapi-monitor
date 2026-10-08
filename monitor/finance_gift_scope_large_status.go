//go:build unix

package monitor

import (
	"context"
	"path/filepath"
)

// InspectFinanceGiftLargeLocalJob checks evidence and current facts without
// writing SQLite, recovering a journal, or appending audit records.
func InspectFinanceGiftLargeLocalJob(ctx context.Context, dir, confirmation string) (FinanceGiftLocalProgress, error) {
	rejected := FinanceGiftLocalProgress{Mode: "offline_large_hour_only", Status: "unverified"}
	if err := ctx.Err(); err != nil {
		return rejected, err
	}
	dir, err := filepath.Abs(dir)
	if err != nil {
		return rejected, err
	}
	lock, err := giftLocalLock(dir)
	if err != nil {
		return rejected, err
	}
	defer lock.Close()
	plan, events, err := loadFinanceGiftLargeLocalInputs(dir, confirmation)
	if err != nil {
		return rejected, err
	}
	target := financeGiftScopeTarget{SourceEpoch: plan.SourceEpoch, HourTs: plan.HourTs, UserID: plan.UserID}
	if err := giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), FinanceGiftLocalPlan{Targets: []financeGiftScopeTarget{target}}); err != nil {
		return rejected, err
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		return rejected, err
	}
	defer closeDB()
	_, _, missing, err := checkFinanceGiftLargeLedger(ctx, db, plan, events, true)
	if err != nil {
		return rejected, err
	}
	entry := financeGiftLocalProgressEntry{financeGiftScopeTarget: target, Status: "pending", Rows: len(events), RemainingRows: missing, VerifiedRows: len(events) - missing}
	result := FinanceGiftLocalProgress{Mode: "offline_large_hour_only", Status: "pending", Targets: 1, RemainingTargets: 1, Rows: len(events), RemainingRows: missing, VerifiedRows: entry.VerifiedRows}
	if missing == 0 {
		entry.Status, result.Status, result.CompletedTargets, result.RemainingTargets = "complete", "complete", 1, 0
	} else if entry.VerifiedRows > 0 {
		entry.Status, result.Status = "partial", "partial"
	}
	result.Entries = []financeGiftLocalProgressEntry{entry}
	return result, nil
}
