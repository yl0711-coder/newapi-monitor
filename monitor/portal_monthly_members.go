package monitor

// Member details are a frozen customer view of the already-verified evidence.
// They never query a current balance or redistribute a discrepancy. Validate
// each account and its own credit rows before checking organization totals.
func monthlyMembersComplete(p monthlyStatement) bool {
	if !monthlyTokensComplete(p) {
		return false
	}
	if p.MemberDetailsVersion == 0 && len(p.Members) == 0 {
		return p.Credits.BalanceCorrection == "" && p.Consumption != nil && !p.Consumption.RequestsUnknown && !p.Consumption.TokensUnknown // legacy publications cannot contain nullable statistics or corrections
	}
	if (p.MemberDetailsVersion != 1 && p.MemberDetailsVersion != 2) || len(p.Members) == 0 || len(p.Members) > maxTrackedUsers {
		return false
	}
	byUser := make(map[int64][]monthlyCredit, len(p.Members))
	for _, row := range p.Credits.Rows {
		byUser[row.UserID] = append(byUser[row.UserID], row)
	}
	seen := make(map[int64]bool, len(p.Members))
	var totals [7]int64 // opening, credit, adjustment, consumption, refund, net, closing
	var correctionTotal int64
	for _, member := range p.Members {
		if member.UserID <= 0 || seen[member.UserID] {
			return false
		}
		seen[member.UserID] = true
		var recharge, gift int64
		count := 0
		for _, row := range byUser[member.UserID] {
			q, ok := newMonthlyMoney(row.Amount)
			if !ok || row.Username != member.Username {
				return false
			}
			switch row.Kind {
			case "recharge":
				if addEconomicsInt64(&recharge, q) != nil {
					return false
				}
				count++
			case "gift":
				if addEconomicsInt64(&gift, q) != nil {
					return false
				}
			}
		}
		r, g := monthlyQuotaMoney(recharge), monthlyQuotaMoney(gift)
		// Reuse the exact same per-statement money validation with no nested
		// member data. This also checks the member's signed debit rows.
		child := monthlyStatement{
			MonthOpeningBalance: &member.Opening, MonthEndBalance: &member.Closing, Consumption: &member.Consumption,
			Credits: monthlyCredits{Available: true, Rows: byUser[member.UserID], RechargeCount: &count, RechargeTotal: &r, GiftTotal: &g, KnownTotal: member.CreditTotal, Adjustment: member.Adjustment, BalanceCorrection: member.BalanceCorrection},
		}
		if !monthlyStatementMoneyComplete(child) {
			return false
		}
		q, ok := monthlyCorrectionMoney(member.BalanceCorrection)
		if !ok || (p.Credits.BalanceCorrection == "") != (member.BalanceCorrection == "") || addEconomicsInt64(&correctionTotal, q) != nil {
			return false
		}
		for i, amount := range []string{member.Opening, member.CreditTotal, member.Adjustment, member.Consumption.Amount, member.Consumption.Refund, member.Consumption.Net, member.Closing} {
			q, ok := newMonthlyMoney(amount)
			if !ok || addEconomicsInt64(&totals[i], q) != nil {
				return false
			}
		}
		delete(byUser, member.UserID)
	}
	q, ok := monthlyCorrectionMoney(p.Credits.BalanceCorrection)
	if !ok || q != correctionTotal {
		return false
	}
	stats, err := monthlyAggregateStatistics(p.Members)
	if err != nil || len(byUser) != 0 || stats.Requests != p.Consumption.Requests || stats.Tokens != p.Consumption.Tokens || stats.RequestsUnknown != p.Consumption.RequestsUnknown || stats.TokensUnknown != p.Consumption.TokensUnknown {
		return false
	}
	if (p.MemberDetailsVersion == 2) != (stats.RequestsUnknown || stats.TokensUnknown) {
		return false
	}
	for i, amount := range []string{*p.MonthOpeningBalance, p.Credits.KnownTotal, p.Credits.Adjustment, p.Consumption.Amount, p.Consumption.Refund, p.Consumption.Net, *p.MonthEndBalance} {
		q, ok := newMonthlyMoney(amount)
		if !ok || q != totals[i] {
			return false
		}
	}
	return true
}
