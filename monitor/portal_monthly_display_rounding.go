package monitor

// Presentation arithmetic only. A residual is allowed only when the original
// six-decimal equation holds exactly. It must never repair missing evidence or
// become a stored credit, wallet correction, consumption or token allocation.
import (
	"math/big"
	"strings"
)

type monthlyDisplayTerm struct {
	amount string
	sign   int64
}

func monthlyDisplayResidual(total string, terms []monthlyDisplayTerm) string {
	exact, ok := monthlyRoundedDisplayUnits(total, 6)
	if !ok {
		return ""
	}
	cents, _ := monthlyRoundedDisplayUnits(total, 2)
	for _, term := range terms {
		if term.sign != 1 && term.sign != -1 {
			return ""
		}
		value, valid := monthlyRoundedDisplayUnits(term.amount, 6)
		if !valid {
			return ""
		}
		rounded, _ := monthlyRoundedDisplayUnits(term.amount, 2)
		exact.Sub(exact, value.Mul(value, big.NewInt(term.sign)))
		cents.Sub(cents, rounded.Mul(rounded, big.NewInt(term.sign)))
	}
	if exact.Sign() != 0 {
		return ""
	}
	return monthlyDisplayCentsMoney(cents)
}

func monthlyDisplayCentsMoney(cents *big.Int) string {
	sign := ""
	if cents.Sign() < 0 {
		sign = "-"
	}
	digits := new(big.Int).Abs(cents).String()
	if len(digits) < 3 {
		digits = strings.Repeat("0", 3-len(digits)) + digits
	}
	return sign + digits[:len(digits)-2] + "." + digits[len(digits)-2:] + "0000"
}

func monthlyWalletDisplayResidual(opening, credit, adjustment string, consumption monthlyConsumption, correction, closing string) string {
	if correction == "" {
		correction = "0.000000"
	}
	return monthlyDisplayResidual(closing, []monthlyDisplayTerm{
		{opening, 1}, {credit, 1}, {adjustment, 1}, {consumption.Amount, -1},
		{consumption.Refund, 1}, {correction, 1},
	})
}

func monthlyDisplayNonzero(value string) bool {
	return value != "" && value != "0.000000"
}

// Derived display balances use the visible cents operands. Validate the exact
// source equation first, never conceal missing evidence or a real discrepancy.
func monthlyWalletDisplayClosing(opening, credit, adjustment string, consumption monthlyConsumption, correction, closing string) string {
	residual := monthlyWalletDisplayResidual(opening, credit, adjustment, consumption, correction, closing)
	if residual == "" {
		return ""
	}
	result, _ := monthlyRoundedDisplayUnits(closing, 2)
	tail, _ := monthlyRoundedDisplayUnits(residual, 2)
	return monthlyDisplayCentsMoney(result.Sub(result, tail))
}

func monthlyConsumptionDisplayResiduals(total monthlyConsumption, parts []monthlyConsumption) []string {
	values := []string{total.Amount, total.Refund, total.Net}
	result := make([]string, 3)
	for i, value := range values {
		terms := make([]monthlyDisplayTerm, 0, len(parts))
		for _, p := range parts {
			terms = append(terms, monthlyDisplayTerm{[]string{p.Amount, p.Refund, p.Net}[i], 1})
		}
		result[i] = monthlyDisplayResidual(value, terms)
	}
	return result
}

func monthlyNetDisplayResidual(c monthlyConsumption) string {
	return monthlyDisplayResidual(c.Net, []monthlyDisplayTerm{{c.Amount, 1}, {c.Refund, -1}})
}

// For an already computed display-only subtotal row. The row's gross/refund/
// net residuals need their own cents bridge; this is not a source adjustment.
func monthlyDisplayNetColumnResidual(values []string) string {
	if len(values) < 3 {
		return ""
	}
	n := new(big.Int)
	for i, sign := range []int64{-1, 1, 1} {
		value, ok := monthlyRoundedDisplayUnits(values[i], 2)
		if !ok {
			return ""
		}
		n.Add(n, value.Mul(value, big.NewInt(sign)))
	}
	return monthlyDisplayCentsMoney(n)
}

func monthlyMemberDisplayAmounts(m monthlyMemberStatement, correction, rounding bool, frozen ...*monthlyDisplayBalance) []string {
	var balance *monthlyDisplayBalance
	if len(frozen) > 0 {
		balance = frozen[0]
	}
	opening, closing, _ := monthlyDisplayWallet(m, balance)
	amounts := []string{opening, m.CreditTotal, m.Adjustment, m.Consumption.Amount, m.Consumption.Refund}
	if correction {
		amounts = append(amounts, m.BalanceCorrection)
	}
	if rounding {
		amounts = append(amounts, monthlyWalletDisplayResidual(m.Opening, m.CreditTotal, m.Adjustment, m.Consumption, m.BalanceCorrection, m.Closing))
	}
	return append(amounts, closing)
}

// Footer bridges rounded independent accounts to the rounded organization;
// actual account amounts, including negative balances, are never redistributed.
func monthlyMemberDisplayFooters(p monthlyStatement) (bool, bool, []string, []string) {
	correction := p.Credits.BalanceCorrection != ""
	if p.MonthOpeningBalance == nil || p.MonthEndBalance == nil || p.Consumption == nil {
		return false, false, nil, nil
	}
	org := monthlyMemberStatement{Opening: *p.MonthOpeningBalance, CreditTotal: p.Credits.KnownTotal,
		Adjustment: p.Credits.Adjustment, BalanceCorrection: p.Credits.BalanceCorrection,
		Consumption: *p.Consumption, Closing: *p.MonthEndBalance}
	// The wallet equation closes in cents, so no wallet-tail column is needed.
	// Independent account subtotals can still differ from the organization.
	rounding := false
	var frozenOrg *monthlyDisplayBalance
	if p.DisplayBalances != nil {
		frozenOrg = &p.DisplayBalances.Organization
	}
	frozenMembers := monthlyDisplayMemberIndex(p)
	totals := monthlyMemberDisplayAmounts(org, correction, rounding, frozenOrg)
	memberAmounts := make([][]string, len(p.Members))
	for i, member := range p.Members {
		memberAmounts[i] = monthlyMemberDisplayAmounts(member, correction, rounding, frozenMembers[member.UserID])
	}
	residuals := make([]string, len(totals))
	for i, total := range totals {
		terms := make([]monthlyDisplayTerm, 0, len(p.Members))
		for _, amounts := range memberAmounts {
			terms = append(terms, monthlyDisplayTerm{amounts[i], 1})
		}
		if i == 0 || i == len(totals)-1 {
			original := make([]monthlyDisplayTerm, 0, len(p.Members))
			sourceTotal := org.Closing
			if i == 0 {
				sourceTotal = org.Opening
			}
			for _, member := range p.Members {
				value := member.Closing
				if i == 0 {
					value = member.Opening
				}
				original = append(original, monthlyDisplayTerm{value, 1})
			}
			if monthlyDisplayResidual(sourceTotal, original) == "" {
				return correction, rounding, nil, nil
			}
			// Display residuals are not source amounts: their sum may differ
			// because of the independently rounded organization columns.
			n, valid := monthlyRoundedDisplayUnits(total, 2)
			if !valid {
				return correction, rounding, nil, nil
			}
			for _, term := range terms {
				v, ok := monthlyRoundedDisplayUnits(term.amount, 2)
				if !ok {
					return correction, rounding, nil, nil
				}
				n.Sub(n, v)
			}
			residuals[i] = monthlyDisplayCentsMoney(n)
		} else {
			residuals[i] = monthlyDisplayResidual(total, terms)
		}
	}
	return correction, rounding, totals, residuals
}
