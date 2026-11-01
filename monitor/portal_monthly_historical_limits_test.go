package monitor

import (
	"math"
	"testing"
)

func TestMonthlyHistoricalAmountLimitsRemainExactAndUnchanged(t *testing.T) {
	for _, c := range []struct {
		quota, net int64
		want       bool
	}{
		{1, 10000, true}, {1, 9999, false}, {0, 10000, false}, {-1, 10000, false}, {1, 0, false}, {1, -1, false},
		{monthlyCorrectionQuotaLimit, monthlyCorrectionQuotaLimit * 10000, true},
		{monthlyCorrectionQuotaLimit + 1, math.MaxInt64, false}, {math.MaxInt64, math.MaxInt64, false},
	} {
		if got := MonthlyHistoricalBalanceWithinLimits(c.quota, c.net); got != c.want {
			t.Fatal(c, got)
		}
	}
}
