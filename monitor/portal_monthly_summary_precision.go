package monitor

import (
	"math/big"
	"strings"
)

// Presentation only. Every bill amount displays cents. Reconciliation and
// aggregation still use the original quota values, never these rounded units.
func monthlySummaryPrecision(_ monthlyStatement) int {
	return 2
}

func monthlyRoundedDisplayUnits(value string, places int) (*big.Int, bool) {
	if places < 2 || places > 6 || len(value) > 30 {
		return nil, false
	}
	whole, fraction, found := strings.Cut(strings.TrimPrefix(value, "-"), ".")
	if !found || whole == "" || len(fraction) != 6 {
		return nil, false
	}
	for _, r := range whole + fraction {
		if r < '0' || r > '9' {
			return nil, false
		}
	}
	n, ok := new(big.Int).SetString(whole+fraction, 10)
	if !ok {
		return nil, false
	}
	divisor := big.NewInt([]int64{10000, 1000, 100, 10, 1}[places-2])
	n.Add(n, new(big.Int).Quo(new(big.Int).Set(divisor), big.NewInt(2))).Quo(n, divisor)
	if strings.HasPrefix(value, "-") {
		n.Neg(n)
	}
	return n, true
}

func monthlySummaryMoneyTextAt(value string, places int) string {
	n, ok := monthlyRoundedDisplayUnits(value, places)
	if !ok {
		return value
	}
	sign := ""
	if n.Sign() < 0 {
		sign = "-" // Rounded zero has no sign; the original wallet is unchanged.
	}
	digits := n.Abs(n).String()
	if len(digits) < places+1 {
		digits = strings.Repeat("0", places+1-len(digits)) + digits
	}
	return sign + digits[:len(digits)-places] + "." + digits[len(digits)-places:]
}
