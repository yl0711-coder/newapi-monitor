package monitor

// Linear integrity checks for retained presentation data. Do not solve the
// allocation flow again on reads. These checks are not source attestation or
// authorization; the publisher must separately recheck the trusted series.
import "math/big"

func monthlyCentRowsMatchSource(total monthlyConsumption, raw []monthlyCentRow, shown monthlyCentPlan) bool {
	if len(raw) != len(shown.Rows) || len(raw) > monthlyTokenPerUserLimit {
		return false
	}
	byID := make(map[int64]monthlyCentRow, len(raw))
	for _, r := range raw {
		if _, duplicate := byID[r.ID]; duplicate {
			return false
		}
		byID[r.ID] = r
	}
	totals := []string{total.Amount, total.Refund, total.Net}
	targets := []string{shown.Amount, shown.Refund, shown.Net}
	sums := [3]*big.Int{new(big.Int), new(big.Int), new(big.Int)}
	last := int64(-1)
	for _, r := range shown.Rows {
		o, exists := byID[r.ID]
		if !exists || r.ID <= last {
			return false
		}
		last = r.ID
		for i, s := range []string{r.Amount, r.Refund, r.Net} {
			n, err := monthlyCentParse([]string{o.Amount, o.Refund, o.Net}[i])
			if err != nil {
				return false
			}
			value, err := monthlyCentTarget(n, s)
			if err != nil {
				return false
			}
			sums[i].Add(sums[i], value)
		}
		net, err := monthlyCentSum(monthlyDisplayTerm{r.Amount, 1}, monthlyDisplayTerm{r.Refund, -1})
		if err != nil || net != r.Net {
			return false
		}
	}
	for i, s := range targets {
		n, err := monthlyCentParse(totals[i])
		if err != nil {
			return false
		}
		value, err := monthlyCentTarget(n, s)
		if err != nil || sums[i].Cmp(value) != 0 {
			return false
		}
	}
	net, err := monthlyCentSum(monthlyDisplayTerm{shown.Amount, 1}, monthlyDisplayTerm{shown.Refund, -1})
	return err == nil && net == shown.Net
}

func monthlyBillCentMatchesSource(p monthlyStatement, plan monthlyBillCentMonth) bool {
	if _, ok := monthlyDisplayChainMonth(p.Month); !ok {
		return false
	}
	if !p.Published || p.CurrentMonth || p.DisplayBalances != nil || p.TokenDetailsVersion != 1 ||
		!monthlyStatementAmountsComplete(p) || plan.Version != 1 || plan.GroupID <= 0 || plan.Month != p.Month ||
		!validMonthlyPublication(plan.SourceIdentity) || plan.Digest != monthlyBillCentMonthDigest(plan) ||
		plan.Consumption.Version != 1 || len(plan.Members) != len(p.Members) || len(plan.Consumption.Members) != len(p.Members) ||
		len(plan.CreditRows) != len(p.Credits.Rows) || plan.Consumption.Organization.ID != 0 {
		return false
	}
	digest, err := monthlyConsumptionCentSourceDigest(p)
	if err != nil || digest != plan.SourceSHA256 || digest != plan.Consumption.SourceSHA256 {
		return false
	}
	byID := make(map[int64]monthlyMemberStatement, len(p.Members))
	index := make(map[int64]int, len(p.Members))
	rawMembers := make([]monthlyCentRow, 0, len(p.Members))
	shownMembers := make([]monthlyCentRow, 0, len(p.Members))
	for _, m := range p.Members {
		byID[m.UserID] = m
		rawMembers = append(rawMembers, monthlyCentRow{m.UserID, m.Consumption.Amount, m.Consumption.Refund, m.Consumption.Net})
	}
	last := int64(0)
	for i, member := range plan.Members {
		raw, exists := byID[member.UserID]
		consumption := plan.Consumption.Members[i]
		if !exists || member.UserID <= last || member.UserID != consumption.UserID || member.UserID != consumption.Consumption.ID ||
			member.Wallet.SourceOpening != raw.Opening || member.Wallet.SourceClosing != raw.Closing {
			return false
		}
		last = member.UserID
		index[member.UserID] = i
		tokens := make([]monthlyCentRow, len(raw.TokenConsumption))
		for j, token := range raw.TokenConsumption {
			tokens[j] = monthlyCentRow{token.TokenID, token.Amount, token.Refund, token.Net}
		}
		shown := consumption.Consumption
		if !monthlyCentRowsMatchSource(raw.Consumption, tokens, monthlyCentPlan{shown.Amount, shown.Refund, shown.Net, consumption.Tokens}) {
			return false
		}
		shownMembers = append(shownMembers, shown)
	}
	org := plan.Consumption.Organization
	if !monthlyCentRowsMatchSource(*p.Consumption, rawMembers, monthlyCentPlan{org.Amount, org.Refund, org.Net, shownMembers}) ||
		plan.Organization.SourceOpening != *p.MonthOpeningBalance || plan.Organization.SourceClosing != *p.MonthEndBalance {
		return false
	}
	for _, pair := range [][2]string{{p.Consumption.Amount, org.Amount}, {p.Consumption.Refund, org.Refund}} {
		n, ok := monthlyRoundedDisplayUnits(pair[0], 2)
		if !ok || monthlyDisplayCentsMoney(n) != pair[1] {
			return false
		}
	}
	if !monthlyBillCentFundingMatchesSource(p, plan, byID, index) {
		return false
	}
	walletFields := func(w monthlyBillCentWallet) []string {
		return []string{w.Opening, w.Recharge, w.Gift, w.CreditTotal, w.Adjustment, w.BalanceCorrection, w.Closing}
	}
	checkWallet := func(w monthlyBillCentWallet, c monthlyCentRow) bool {
		checked := w
		return monthlyBillCentClose(&checked, c) == nil && checked.CreditTotal == w.CreditTotal && checked.Closing == w.Closing
	}
	if !checkWallet(plan.Organization, org) {
		return false
	}
	for i, member := range plan.Members {
		if !checkWallet(member.Wallet, plan.Consumption.Members[i].Consumption) {
			return false
		}
	}
	for column, total := range walletFields(plan.Organization) {
		terms := make([]monthlyDisplayTerm, len(plan.Members))
		for i, member := range plan.Members {
			terms[i] = monthlyDisplayTerm{walletFields(member.Wallet)[column], 1}
		}
		sum, err := monthlyCentSum(terms...)
		if err != nil || sum != total {
			return false
		}
	}
	return true
}

// Check retained choices without reallocating funding on reads. In particular,
// a future producer's tie-breaking change cannot silently alter a saved bill.
func monthlyBillCentFundingMatchesSource(p monthlyStatement, plan monthlyBillCentMonth, byID map[int64]monthlyMemberStatement, index map[int64]int) bool {
	rawSums, shownSums := make([][3]*big.Int, len(plan.Members)), make([][3]*big.Int, len(plan.Members))
	for i := range rawSums {
		for k := range 3 {
			rawSums[i][k], shownSums[i][k] = new(big.Int), new(big.Int)
		}
	}
	for i, row := range p.Credits.Rows {
		member, exists := index[row.UserID]
		if !exists || plan.CreditRows[i].ID != int64(i) {
			return false
		}
		category := -1
		switch row.Kind {
		case "recharge":
			category = 0
		case "gift":
			category = 1
		case "adjustment":
			category = 2
		}
		if category < 0 {
			return false
		}
		raw, err := monthlyCentParse(row.Amount)
		if err != nil {
			return false
		}
		shown, err := monthlyCentTarget(raw, plan.CreditRows[i].Amount)
		if err != nil {
			return false
		}
		rawSums[member][category].Add(rawSums[member][category], raw)
		shownSums[member][category].Add(shownSums[member][category], shown)
	}
	for i, member := range plan.Members {
		for k, amount := range []string{member.Wallet.Recharge, member.Wallet.Gift, member.Wallet.Adjustment} {
			value, err := monthlyCentTarget(rawSums[i][k], amount)
			if err != nil || value.Cmp(shownSums[i][k]) != 0 {
				return false
			}
		}
		correction, err := monthlyCentParse(monthlyCentOptional(byID[member.UserID].BalanceCorrection))
		if err != nil {
			return false
		}
		if _, err := monthlyCentTarget(correction, member.Wallet.BalanceCorrection); err != nil {
			return false
		}
	}
	for _, pair := range [][2]string{{*p.Credits.RechargeTotal, plan.Organization.Recharge}, {*p.Credits.GiftTotal, plan.Organization.Gift},
		{p.Credits.Adjustment, plan.Organization.Adjustment}, {monthlyCentOptional(p.Credits.BalanceCorrection), plan.Organization.BalanceCorrection}} {
		value, ok := monthlyRoundedDisplayUnits(pair[0], 2)
		if !ok || monthlyDisplayCentsMoney(value) != pair[1] {
			return false
		}
	}
	return true
}
