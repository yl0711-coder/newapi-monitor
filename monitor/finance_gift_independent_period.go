package monitor

import "github.com/yl0711-coder/newapi-monitor/internal/financecredit"

// Called only after all monetary/hour/content checks succeed. A missing group
// cannot affect PERIOD gift consumption after irreversible depletion, or where
// complete facts prove no further usage/refunds up to the requested end. Omit
// that user's entire ledger, not merely unknown events (which would invent an
// opening wallet). Other users retain their full ordered evidence. This does
// not prove the inactive user's opening balance or lifetime consumed gift.
// Nothing is written back, and no historical consumption/group is inferred.
func financeGiftIndependentSuffix(result financeGiftAllocationResult, ledger []financecredit.LedgerEvent, gapUsers map[int64]bool, boundaries map[int64]int64) (*financeGiftAllocationResult, error) {
	if len(gapUsers) == 0 || result.Coverage.ScopeUnknownEvents == 0 {
		return nil, nil
	}
	from, to := result.Coverage.FromTs, result.Coverage.ToTs
	for userID := range gapUsers {
		horizon, ok := boundaries[userID]
		if !ok || horizon <= 0 || horizon >= to {
			return nil, nil // One unresolved wallet can still affect this period.
		}
		from = max(from, horizon)
	}
	retained := make([]financecredit.LedgerEvent, 0, len(ledger))
	for _, event := range ledger {
		if !gapUsers[event.UserID] {
			retained = append(retained, event)
		}
	}
	allocation, err := financecredit.AllocateTrialGiftConsumption(retained, from, to)
	if err != nil {
		return nil, err
	}
	coverage := result.Coverage
	coverage.FromTs, coverage.Complete = from, true
	coverage.PriorScopeUnknownEvents = coverage.ScopeUnknownEvents
	coverage.ScopeUnknownEvents = 0
	coverage.VerifiedSuffixFromTs = from
	return &financeGiftAllocationResult{Allocation: allocation, Coverage: coverage, ledger: retained}, nil
}

// Monetary facts must have been checked against EVERY published hour before
// calling this function. The last activity boundary is query-end-specific:
// later usage/refunds move it forward, and changed facts invalidate reports.
func financeGiftIndependentBoundaries(facts []FinanceUserHourFact, horizons map[int64]int64) map[int64]int64 {
	boundaries := make(map[int64]int64)
	for _, fact := range facts {
		boundaries[fact.UserID] = max(boundaries[fact.UserID], fact.HourTs+3600)
	}
	for userID, horizon := range horizons {
		if last, ok := boundaries[userID]; !ok || horizon < last {
			boundaries[userID] = horizon
		}
	}
	return boundaries
}
