package monthlydraft

import (
	"math/big"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// Independent rational-number oracle: customer exports show cents while the
// HTTP evidence retains all six decimals. Do not reuse production rounding or
// compare two outputs produced by the same formatter.
func customerMoneyCents(t *testing.T, raw string) string {
	t.Helper()
	n, ok := new(big.Rat).SetString(raw)
	if !ok {
		t.Fatal("invalid source money", raw)
	}
	n.Mul(n, big.NewRat(100, 1))
	sign := n.Sign()
	n.Abs(n)
	q, remainder := new(big.Int), new(big.Int)
	q.QuoRem(n.Num(), n.Denom(), remainder)
	if remainder.Mul(remainder, big.NewInt(2)).Cmp(n.Denom()) >= 0 {
		q.Add(q, big.NewInt(1))
	}
	digits := q.String()
	if len(digits) < 3 {
		digits = strings.Repeat("0", 3-len(digits)) + digits
	}
	prefix := ""
	if sign < 0 && q.Sign() != 0 {
		prefix = "-"
	}
	return prefix + digits[:len(digits)-2] + "." + digits[len(digits)-2:]
}

func customerMoneyAmounts(t *testing.T, a monitor.MonthlyPreparationAmounts) monitor.MonthlyPreparationAmounts {
	t.Helper()
	return monitor.MonthlyPreparationAmounts{
		Opening: customerMoneyCents(t, a.Opening), Increase: customerMoneyCents(t, a.Increase),
		Decrease: customerMoneyCents(t, a.Decrease), Consume: customerMoneyCents(t, a.Consume),
		Refund: customerMoneyCents(t, a.Refund), Closing: customerMoneyCents(t, a.Closing),
	}
}

func TestCustomerMoneyOracleHalfAwayFromZero(t *testing.T) {
	for raw, want := range map[string]string{
		"1.005000": "1.01", "-1.005000": "-1.01", "0.004999": "0.00", "-0.004999": "0.00",
		"3026.195784": "3026.20", "2321.744170": "2321.74", "9999999999999999.995000": "10000000000000000.00",
	} {
		if got := customerMoneyCents(t, raw); got != want {
			t.Fatalf("%s: got %s want %s", raw, got, want)
		}
	}
}
