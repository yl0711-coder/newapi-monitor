package monitor

// A customer-only projection, deliberately a different Go type from source
// statements. Never feed this rounded view back to a source/publication builder.
type monthlyCentStatementView monthlyStatement

func monthlyCentBillView(retained monthlyRetainedCentBill) (monthlyCentStatementView, error) {
	p, plan := retained.Source, retained.Plan
	if !monthlyBillCentMatchesSource(p, plan) {
		return monthlyCentStatementView{}, errMonthlyCentBillStorage
	}
	// Copy every collection and money pointer we change. Source/statistics and
	// retained labels remain intact, including deleted/renamed token labels.
	opening, closing := plan.Organization.Opening, plan.Organization.Closing
	recharge, gift := plan.Organization.Recharge, plan.Organization.Gift
	p.MonthOpeningBalance, p.MonthEndBalance = &opening, &closing
	p.Credits.RechargeTotal, p.Credits.GiftTotal = &recharge, &gift
	p.Credits.KnownTotal, p.Credits.Adjustment = plan.Organization.CreditTotal, plan.Organization.Adjustment
	if p.Credits.BalanceCorrection != "" {
		p.Credits.BalanceCorrection = plan.Organization.BalanceCorrection
	}
	consume := *p.Consumption
	consume.Amount, consume.Refund, consume.Net = plan.Consumption.Organization.Amount, plan.Consumption.Organization.Refund, plan.Consumption.Organization.Net
	p.Consumption = &consume
	p.Credits.Rows = append([]monthlyCredit(nil), p.Credits.Rows...)
	for i := range p.Credits.Rows {
		p.Credits.Rows[i].Amount = plan.CreditRows[i].Amount
	}
	index := make(map[int64]int, len(plan.Members))
	for i, member := range plan.Members {
		index[member.UserID] = i
	}
	p.Members = append([]monthlyMemberStatement(nil), p.Members...)
	for i := range p.Members {
		m := &p.Members[i]
		j := index[m.UserID]
		w, c := plan.Members[j].Wallet, plan.Consumption.Members[j]
		m.Opening, m.Closing, m.CreditTotal, m.Adjustment = w.Opening, w.Closing, w.CreditTotal, w.Adjustment
		if m.BalanceCorrection != "" {
			m.BalanceCorrection = w.BalanceCorrection
		}
		m.Consumption.Amount, m.Consumption.Refund, m.Consumption.Net = c.Consumption.Amount, c.Consumption.Refund, c.Consumption.Net
		byToken := make(map[int64]monthlyCentRow, len(c.Tokens))
		for _, token := range c.Tokens {
			byToken[token.ID] = token
		}
		m.TokenConsumption = append([]monthlyTokenConsumption(nil), m.TokenConsumption...)
		for k := range m.TokenConsumption {
			token := &m.TokenConsumption[k]
			shown := byToken[token.TokenID]
			token.Amount, token.Refund, token.Net = shown.Amount, shown.Refund, shown.Net
		}
	}
	return monthlyCentStatementView(p), nil
}

func monthlyCentBillXLSX(retained monthlyRetainedCentBill) ([]byte, error) {
	view, err := monthlyCentBillView(retained)
	if err != nil {
		return nil, err
	}
	return monthlyRenderStatementXLSX(monthlyStatement(view))
}

func monthlyCentBillSummary(retained monthlyRetainedCentBill) (monthlyStatementRow, error) {
	view, err := monthlyCentBillView(retained)
	if err != nil {
		return monthlyStatementRow{}, err
	}
	row := monthlySummary(retained.Source)
	row.Consumption = view.Consumption
	row.RechargeTotal, row.GiftTotal, row.CreditTotal = view.Credits.RechargeTotal, view.Credits.GiftTotal, &view.Credits.KnownTotal
	return row, nil
}
