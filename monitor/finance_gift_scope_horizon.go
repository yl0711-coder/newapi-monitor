package monitor

import (
	"context"
	"fmt"
	"sort"
	"strconv"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
)

// A horizon is an exclusive closed-hour boundary after which scope cannot
// change gift consumption. It is NOT proof of a request's historical group.
// All grant/user-hour proofs and boundary metadata must still pass first.
//
// Only refund-free users qualify. Count consumption strictly AFTER the hour
// of the last eligible grant: including that hour could spend a gift before
// it existed. Once this later consumption covers ALL eligible grants, the
// wallet must be empty regardless of business/excluded scope. No later grant
// or refund in the requested evidence range can then restore it.
func financeGiftScopeHorizons(ctx context.Context, facts []FinanceUserHourFact, grants []financecredit.LedgerEvent) (map[int64]int64, error) {
	type wallet struct {
		lastGrantHour int64
		remaining     int64
		refunded      bool
	}
	wallets := make(map[int64]wallet)
	for _, grant := range grants {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if grant.Kind != financecredit.EventTrialGiftGrant || grant.AmountMicroUSD <= 0 || grant.UserID <= 0 || grant.At < 0 {
			return nil, fmt.Errorf("invalid gift horizon grant")
		}
		w := wallets[grant.UserID]
		w.lastGrantHour = max(w.lastGrantHour, grant.At/3600*3600)
		if err := addEconomicsInt64(&w.remaining, grant.AmountMicroUSD); err != nil {
			return nil, err
		}
		wallets[grant.UserID] = w
	}
	seen := make(map[financeGiftUserHourKey]bool, len(facts))
	for _, fact := range facts {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if err := validateFinanceUserHourFact(fact); err != nil {
			return nil, err
		}
		key := financeGiftUserHourKey{HourTs: fact.HourTs, UserID: fact.UserID}
		if seen[key] {
			return nil, fmt.Errorf("duplicate gift horizon user hour")
		}
		seen[key] = true
		w, ok := wallets[fact.UserID]
		if ok && (fact.RefundRecords != 0 || fact.RefundQuota != 0) {
			w.refunded = true // Even a zero-value refund retains the full path.
			wallets[fact.UserID] = w
		}
	}
	ordered := append([]FinanceUserHourFact(nil), facts...)
	sort.Slice(ordered, func(i, j int) bool { return ordered[i].HourTs < ordered[j].HourTs })
	result := make(map[int64]int64)
	unit := strconv.FormatInt(int64(quotaPerUSD), 10)
	for _, fact := range ordered {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		w, ok := wallets[fact.UserID]
		if !ok || w.refunded || w.remaining == 0 || fact.HourTs <= w.lastGrantHour {
			continue
		}
		amount, err := signedUnitsToMicroUSDCanonical(fact.ConsumeQuota, unit)
		if err != nil {
			return nil, err
		}
		w.remaining -= min(w.remaining, amount)
		wallets[fact.UserID] = w
		if w.remaining == 0 {
			result[fact.UserID] = fact.HourTs + 3600
		}
	}
	return result, nil
}
