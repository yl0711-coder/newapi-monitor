package monitor

import "sort"

// Cache hits can be visited before an earlier pending cache miss. Retain the
// earliest bounded gap set independent of callback order, rather than keeping
// the first visited set and sorting it after earlier gaps have been dropped.
func retainEarliestFinanceGiftScopeGap(kept []FinanceGiftBoundaryState, state FinanceGiftBoundaryState) []FinanceGiftBoundaryState {
	index := sort.Search(len(kept), func(i int) bool {
		return kept[i].HourTs > state.HourTs || kept[i].HourTs == state.HourTs && kept[i].UserID >= state.UserID
	})
	if index == financeGiftScopeBatchLimit+1 {
		return kept
	}
	if len(kept) < financeGiftScopeBatchLimit+1 {
		kept = append(kept, FinanceGiftBoundaryState{})
	}
	copy(kept[index+1:], kept[index:len(kept)-1])
	kept[index] = state
	return kept
}
