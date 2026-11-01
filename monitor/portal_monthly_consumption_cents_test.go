package monitor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func monthlyCentStatementFixture(t *testing.T, conflict bool) monthlyStatement {
	t.Helper()
	e, now := monthlyTokenFixture()
	if conflict {
		for i := 0; i < 2; i++ {
			u := &e.Users[i]
			u.ConsumeQuota, u.RefundQuota = 2000, 0 // two separate exact 0.004 amounts
			u.ClosingQuota = u.OpeningQuota - u.ConsumeQuota
			u.ReverseClosingQuota = u.ClosingQuota
			u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
			u.TokenUsage.Rows = []MonthlyBillTokenEvidence{{TokenID: u.UserID * 10, Name: "合成令牌", ConsumeQuota: u.ConsumeQuota}}
		}
	}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMonthlyConsumptionCentsHierarchyAndFrozenRoundTrip(t *testing.T) {
	p := monthlyCentStatementFixture(t, false)
	before, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlyPlanConsumptionCents(p)
	if err != nil || !monthlyConsumptionCentPlanValid(p, plan) || !monthlyConsumptionCentsFitsFrozenWallets(p, plan) {
		t.Fatal("consistent complete source rejected", err)
	}
	rows := make([]monthlyCentRow, 0, len(plan.Members))
	for i, member := range plan.Members {
		if member.UserID != member.Consumption.ID {
			t.Fatal("account attribution changed")
		}
		rows = append(rows, member.Consumption)
		m := p.Members[i]
		raw := make([]monthlyCentRow, len(m.TokenConsumption))
		for j, row := range m.TokenConsumption {
			raw[j] = monthlyCentRow{ID: row.TokenID, Amount: row.Amount, Refund: row.Refund, Net: row.Net}
		}
		monthlyCentTestCheck(t, m.Consumption, raw, monthlyCentPlan{Amount: member.Consumption.Amount,
			Refund: member.Consumption.Refund, Net: member.Consumption.Net, Rows: member.Tokens})
	}
	if plan.Organization.Amount != "4.000000" || plan.Organization.Refund != "0.000000" || plan.Organization.Net != "4.000000" {
		t.Fatal("wrong organization totals", plan.Organization)
	}
	if total := monthlyCentTestTotal(t, rows); total.Amount != plan.Organization.Amount || total.Refund != plan.Organization.Refund || total.Net != plan.Organization.Net {
		t.Fatal("organization and account displays differ")
	}
	raw, err := json.Marshal(plan)
	if err != nil {
		t.Fatal(err)
	}
	var saved monthlyConsumptionCentPlan
	if json.Unmarshal(raw, &saved) != nil || !reflect.DeepEqual(plan, saved) || !monthlyConsumptionCentPlanValid(p, saved) {
		t.Fatal("saved display plan changed on reload")
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(before, after) {
		t.Fatal("planning modified source bill")
	}
}

func TestMonthlyConsumptionCentsRefusesMixedLegacyWalletAndKeepsBill(t *testing.T) {
	p := monthlyCentStatementFixture(t, true)
	raw, err := json.Marshal(p)
	if err != nil {
		t.Fatal(err)
	}
	record := PortalMonthlyBill{Month: p.Month, Payload: string(raw), PayloadHash: monthlyDigest(raw)}
	old, err := readPublishedMonthlyBill(record)
	if err != nil {
		t.Fatal("valid legacy bill must remain readable", err)
	}
	book, err := monthlyStatementXLSX(old)
	if err != nil {
		t.Fatal(err)
	}
	plan, err := monthlyPlanConsumptionCents(p)
	if err != nil || !monthlyConsumptionCentPlanValid(p, plan) {
		t.Fatal("valid consumption allocation failed", err)
	}
	if plan.Organization.Amount != "0.010000" || monthlyConsumptionCentsFitsFrozenWallets(p, plan) {
		t.Fatal("one-cent legacy wallet conflict was hidden")
	}
	again, err := readPublishedMonthlyBill(record)
	if err != nil || !reflect.DeepEqual(old, again) {
		t.Fatal("legacy reader changed after planning", err)
	}
	afterBook, err := monthlyStatementXLSX(again)
	if err != nil || !bytes.Equal(book, afterBook) {
		t.Fatal("legacy export changed after planning", err)
	}
	after, _ := json.Marshal(p)
	if !bytes.Equal(raw, after) || record.PayloadHash != monthlyDigest(raw) {
		t.Fatal("legacy bill/source was rewritten")
	}
}

func TestMonthlyConsumptionCentsRejectsDamageAndForeignSource(t *testing.T) {
	p := monthlyCentStatementFixture(t, false)
	plan, err := monthlyPlanConsumptionCents(p)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func(*monthlyConsumptionCentPlan){
		"unsupported_version": func(p *monthlyConsumptionCentPlan) { p.Version++ },
		"wrong_source":        func(p *monthlyConsumptionCentPlan) { p.SourceSHA256 = monthlyDigest([]byte("other bill")) },
		"wrong_total":         func(p *monthlyConsumptionCentPlan) { p.Organization.Amount = "4.010000" },
		"wrong_account":       func(p *monthlyConsumptionCentPlan) { p.Members[0].UserID++ },
		"missing_account":     func(p *monthlyConsumptionCentPlan) { p.Members = p.Members[1:] },
		"wrong_token":         func(p *monthlyConsumptionCentPlan) { p.Members[0].Tokens[0].ID++ },
		"changed_token_money": func(p *monthlyConsumptionCentPlan) { p.Members[0].Tokens[0].Amount = "2.010000" },
		"missing_token":       func(p *monthlyConsumptionCentPlan) { p.Members[0].Tokens = p.Members[0].Tokens[1:] },
	} {
		t.Run(name, func(t *testing.T) {
			encoded, _ := json.Marshal(plan)
			var changed monthlyConsumptionCentPlan
			if err := json.Unmarshal(encoded, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if monthlyConsumptionCentPlanValid(p, changed) || monthlyConsumptionCentsFitsFrozenWallets(p, changed) {
				t.Fatal("damaged/foreign plan accepted")
			}
		})
	}
	for name, mutate := range map[string]func(*monthlyStatement){
		"unpublished":      func(p *monthlyStatement) { p.Published = false },
		"current_month":    func(p *monthlyStatement) { p.CurrentMonth = true },
		"missing_balance":  func(p *monthlyStatement) { p.MonthOpeningBalance = nil },
		"missing_member":   func(p *monthlyStatement) { p.Members = p.Members[1:] },
		"missing_token":    func(p *monthlyStatement) { p.Members[0].TokenConsumption = nil },
		"legacy_no_tokens": func(p *monthlyStatement) { p.TokenDetailsVersion = 0 },
		"changed_wallet":   func(p *monthlyStatement) { p.Members[0].Closing = "99.000000" },
		"changed_month":    func(p *monthlyStatement) { p.Month = "invalid" },
	} {
		t.Run(name, func(t *testing.T) {
			encoded, _ := json.Marshal(p)
			var changed monthlyStatement
			if err := json.Unmarshal(encoded, &changed); err != nil {
				t.Fatal(err)
			}
			mutate(&changed)
			if _, err := monthlyPlanConsumptionCents(changed); err == nil {
				t.Fatal("invalid source made to look consistent")
			}
		})
	}
}

func monthlyCentCapacitySource(t *testing.T) monthlyStatement {
	t.Helper()
	e, now := monthlyTokenFixture()
	base := e.Users[0]
	e.Users = nil
	for account := range 10 {
		u := base
		u.UserID = int64(1000 + account)
		u.ConsumeQuota, u.RefundQuota = 0, 0
		u.TokenUsage = &MonthlyBillTokenUsage{UserID: u.UserID, Month: e.Month,
			Source: "synthetic-cents-capacity-only", Rows: []MonthlyBillTokenEvidence{}}
		for token := range monthlyTokenPerUserLimit {
			// Integer-cent rows exercise maximum hierarchy size without
			// repeating the allocator's separate worst-case fractional test.
			gross, refund := int64(1+token)*5000, int64(token%7)*5000
			u.TokenUsage.Rows = append(u.TokenUsage.Rows, MonthlyBillTokenEvidence{
				TokenID: u.UserID*10000 + int64(token+1), Name: "合成令牌",
				ConsumeQuota: gross, RefundQuota: refund})
			u.ConsumeQuota += gross
			u.RefundQuota += refund
		}
		u.ClosingQuota = u.OpeningQuota - u.ConsumeQuota + u.RefundQuota
		u.ReverseClosingQuota = u.ClosingQuota
		u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
		e.Users = append(e.Users, u)
	}
	p, _, err := buildPublishedMonthlyBill(e, now)
	if err != nil {
		t.Fatal(err)
	}
	return p
}

func TestMonthlyConsumptionCentsCapacity(t *testing.T) {
	p := monthlyCentCapacitySource(t)
	plan, err := monthlyPlanConsumptionCents(p)
	if err != nil || !monthlyConsumptionCentPlanValid(p, plan) || len(plan.Members) != 10 {
		t.Fatal("full-size bill cannot be projected", err)
	}
	count := 0
	for _, m := range plan.Members {
		count += len(m.Tokens)
	}
	if count != monthlyTokenBillLimit || !monthlyConsumptionCentsFitsFrozenWallets(p, plan) {
		t.Fatal("full-size hierarchy lost rows or changed wallet equation", count)
	}
}
