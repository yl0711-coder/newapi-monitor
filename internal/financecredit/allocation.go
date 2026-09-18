package financecredit

import (
	"errors"
	"math"
	"sort"
)

const (
	EventTrialGiftGrant = "trial_gift_grant"
	EventNetUsage       = "net_usage"
)

// LedgerEvent is a normalized, fixed-point finance event. AmountMicroUSD is
// always positive for a gift grant. For net usage it is positive for charged
// usage and negative for a refund. Sequence must preserve the source ledger's
// order when two events share a timestamp.
type LedgerEvent struct {
	UserID         int64
	At             int64
	Sequence       int64
	Kind           string
	AmountMicroUSD int64
}

// GiftAllocation is the conservative gift-first allocation for [From, To).
// PeriodGiftConsumptionMicroUSD can be negative when a refund in the period
// reverses gift-funded consumption from an earlier period.
type GiftAllocation struct {
	From                          int64
	To                            int64
	EligibleGrantMicroUSD         int64
	PeriodNetUsageMicroUSD        int64
	PeriodGiftConsumptionMicroUSD int64
	GiftBalanceAtFromMicroUSD     int64
	GiftBalanceAtToMicroUSD       int64
	GiftConsumedAtFromMicroUSD    int64
	GiftConsumedAtToMicroUSD      int64
	Users                         int64
}

// AllocationRange is one half-open interval used by the batch allocator.
// Ranges must be ordered and must not overlap; gaps are allowed.
type AllocationRange struct {
	From int64
	To   int64
}

type giftAllocationState struct {
	balance  int64
	consumed int64
}

func orderedLedgerEvents(events []LedgerEvent) ([]LedgerEvent, error) {
	ordered := append([]LedgerEvent(nil), events...)
	for _, event := range ordered {
		if event.UserID <= 0 || event.At < 0 || event.Sequence < 0 {
			return nil, errors.New("invalid finance event identity")
		}
		switch event.Kind {
		case EventTrialGiftGrant:
			if event.AmountMicroUSD <= 0 {
				return nil, errors.New("gift grant must be positive")
			}
		case EventNetUsage:
			if event.AmountMicroUSD == math.MinInt64 {
				return nil, errors.New("net usage is out of range")
			}
		default:
			return nil, errors.New("unknown finance event kind")
		}
	}
	sort.SliceStable(ordered, func(i, j int) bool {
		if ordered[i].At != ordered[j].At {
			return ordered[i].At < ordered[j].At
		}
		if ordered[i].Sequence != ordered[j].Sequence {
			return ordered[i].Sequence < ordered[j].Sequence
		}
		// A grant is available to a usage event recorded at the same source
		// position. Real callers should still provide distinct source IDs.
		return ordered[i].Kind == EventTrialGiftGrant && ordered[j].Kind != EventTrialGiftGrant
	})
	return ordered, nil
}

// AllocateTrialGiftConsumption applies a gift-first policy without crossing
// user boundaries. The caller must pass only grants already proven eligible
// by the registration-time and amount rules. Events before From seed the
// opening balance; events at or after To are ignored.
//
// A refund first restores previously consumed gift, capped at that user's
// consumed gift. This keeps the calculation reversible and prevents either a
// refund or malformed input from creating a negative gift balance.
func AllocateTrialGiftConsumption(events []LedgerEvent, from, to int64) (GiftAllocation, error) {
	result := GiftAllocation{From: from, To: to}
	if from < 0 || to <= from {
		return result, errors.New("invalid allocation range")
	}
	ordered, err := orderedLedgerEvents(events)
	if err != nil {
		return result, err
	}

	states := make(map[int64]giftAllocationState)
	seenUsers := make(map[int64]struct{})
	openingCaptured := false
	captureOpening := func() error {
		if openingCaptured {
			return nil
		}
		for _, state := range states {
			if err := checkedAdd(&result.GiftBalanceAtFromMicroUSD, state.balance); err != nil {
				return err
			}
			if err := checkedAdd(&result.GiftConsumedAtFromMicroUSD, state.consumed); err != nil {
				return err
			}
		}
		openingCaptured = true
		return nil
	}

	for _, event := range ordered {
		if event.At >= to {
			break
		}
		if event.At >= from {
			if err := captureOpening(); err != nil {
				return result, err
			}
			seenUsers[event.UserID] = struct{}{}
		}
		state := states[event.UserID]
		switch event.Kind {
		case EventTrialGiftGrant:
			if err := checkedAdd(&state.balance, event.AmountMicroUSD); err != nil {
				return result, err
			}
			if event.At >= from {
				if err := checkedAdd(&result.EligibleGrantMicroUSD, event.AmountMicroUSD); err != nil {
					return result, err
				}
			}
		case EventNetUsage:
			if event.At >= from {
				if err := checkedAdd(&result.PeriodNetUsageMicroUSD, event.AmountMicroUSD); err != nil {
					return result, err
				}
			}
			if event.AmountMicroUSD > 0 {
				allocated := min64(state.balance, event.AmountMicroUSD)
				state.balance -= allocated
				if err := checkedAdd(&state.consumed, allocated); err != nil {
					return result, err
				}
			} else if event.AmountMicroUSD < 0 {
				restored := min64(state.consumed, -event.AmountMicroUSD)
				state.consumed -= restored
				if err := checkedAdd(&state.balance, restored); err != nil {
					return result, err
				}
			}
		}
		states[event.UserID] = state
	}
	if err := captureOpening(); err != nil {
		return result, err
	}
	for _, state := range states {
		if err := checkedAdd(&result.GiftBalanceAtToMicroUSD, state.balance); err != nil {
			return result, err
		}
		if err := checkedAdd(&result.GiftConsumedAtToMicroUSD, state.consumed); err != nil {
			return result, err
		}
	}
	result.PeriodGiftConsumptionMicroUSD = result.GiftConsumedAtToMicroUSD - result.GiftConsumedAtFromMicroUSD
	result.Users = int64(len(seenUsers))
	return result, nil
}

// AllocateTrialGiftConsumptionSeries evaluates many ordered, non-overlapping
// periods with one validated sort and one ledger pass. Each returned item is
// semantically identical to calling AllocateTrialGiftConsumption for the same
// range, including opening balances and refund reversals.
func AllocateTrialGiftConsumptionSeries(events []LedgerEvent, ranges []AllocationRange) ([]GiftAllocation, error) {
	results := make([]GiftAllocation, len(ranges))
	for index, period := range ranges {
		results[index] = GiftAllocation{From: period.From, To: period.To}
		if period.From < 0 || period.To <= period.From {
			return nil, errors.New("invalid allocation range")
		}
		if index > 0 && period.From < ranges[index-1].To {
			return nil, errors.New("allocation ranges must be ordered and non-overlapping")
		}
	}
	if len(ranges) == 0 {
		return results, nil
	}
	ordered, err := orderedLedgerEvents(events)
	if err != nil {
		return nil, err
	}

	states := make(map[int64]giftAllocationState)
	var totalBalance, totalConsumed int64
	eventIndex := 0
	apply := func(event LedgerEvent) error {
		state := states[event.UserID]
		oldBalance, oldConsumed := state.balance, state.consumed
		switch event.Kind {
		case EventTrialGiftGrant:
			if err := checkedAdd(&state.balance, event.AmountMicroUSD); err != nil {
				return err
			}
		case EventNetUsage:
			if event.AmountMicroUSD > 0 {
				allocated := min64(state.balance, event.AmountMicroUSD)
				state.balance -= allocated
				if err := checkedAdd(&state.consumed, allocated); err != nil {
					return err
				}
			} else if event.AmountMicroUSD < 0 {
				restored := min64(state.consumed, -event.AmountMicroUSD)
				state.consumed -= restored
				if err := checkedAdd(&state.balance, restored); err != nil {
					return err
				}
			}
		}
		states[event.UserID] = state
		if err := checkedAdd(&totalBalance, state.balance-oldBalance); err != nil {
			return err
		}
		return checkedAdd(&totalConsumed, state.consumed-oldConsumed)
	}
	advanceBefore := func(boundary int64) error {
		for eventIndex < len(ordered) && ordered[eventIndex].At < boundary {
			if err := apply(ordered[eventIndex]); err != nil {
				return err
			}
			eventIndex++
		}
		return nil
	}

	for index, period := range ranges {
		if err := advanceBefore(period.From); err != nil {
			return nil, err
		}
		result := &results[index]
		result.GiftBalanceAtFromMicroUSD = totalBalance
		result.GiftConsumedAtFromMicroUSD = totalConsumed
		seenUsers := make(map[int64]struct{})
		for eventIndex < len(ordered) && ordered[eventIndex].At < period.To {
			event := ordered[eventIndex]
			seenUsers[event.UserID] = struct{}{}
			switch event.Kind {
			case EventTrialGiftGrant:
				if err := checkedAdd(&result.EligibleGrantMicroUSD, event.AmountMicroUSD); err != nil {
					return nil, err
				}
			case EventNetUsage:
				if err := checkedAdd(&result.PeriodNetUsageMicroUSD, event.AmountMicroUSD); err != nil {
					return nil, err
				}
			}
			if err := apply(event); err != nil {
				return nil, err
			}
			eventIndex++
		}
		result.GiftBalanceAtToMicroUSD = totalBalance
		result.GiftConsumedAtToMicroUSD = totalConsumed
		result.PeriodGiftConsumptionMicroUSD = totalConsumed - result.GiftConsumedAtFromMicroUSD
		result.Users = int64(len(seenUsers))
	}
	return results, nil
}

func checkedAdd(target *int64, value int64) error {
	if (value > 0 && *target > math.MaxInt64-value) || (value < 0 && *target < math.MinInt64-value) {
		return errors.New("finance amount overflow")
	}
	*target += value
	return nil
}

func min64(left, right int64) int64 {
	if left < right {
		return left
	}
	return right
}
