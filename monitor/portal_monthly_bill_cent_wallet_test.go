package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
	"time"
)

type monthlyCentBillSpec struct {
	id, opening, recharge, gift, adjustment, consume, refund, correction int64
}

// Synthetic exact originals; no database, account credentials or live source.
func monthlyCentBillSource(t *testing.T, month string, specs ...monthlyCentBillSpec) monthlyStatement {
	t.Helper()
	start, end, err := monthlyWindow(month, time.Date(2026, 11, 1, 0, 0, 0, 0, usageCST))
	if err != nil {
		t.Fatal(err)
	}
	p := monthlyStatement{Account: "合成组织", Month: month, From: start.Format("2006-01-02"),
		To: end.AddDate(0, 0, -1).Format("2006-01-02"), Published: true,
		MemberDetailsVersion: 1, TokenDetailsVersion: 1}
	var totals [8]int64
	recharges := 0
	hasCorrection := false
	for _, s := range specs {
		hasCorrection = hasCorrection || s.correction != 0
	}
	for _, s := range specs {
		closing := s.opening + s.recharge + s.gift + s.adjustment - s.consume + s.refund + s.correction
		name := fmt.Sprintf("合成用户%d", s.id)
		member := monthlyMemberStatement{UserID: s.id, Username: name, Opening: monthlyQuotaMoney(s.opening),
			CreditTotal: monthlyQuotaMoney(s.recharge + s.gift), Adjustment: monthlyQuotaMoney(s.adjustment), Closing: monthlyQuotaMoney(closing),
			Consumption:      monthlyConsumption{Amount: monthlyQuotaMoney(s.consume), Refund: monthlyQuotaMoney(s.refund), Net: monthlyQuotaMoney(s.consume - s.refund)},
			TokenConsumption: []monthlyTokenConsumption{{TokenID: s.id * 100, Name: "合成令牌", Amount: monthlyQuotaMoney(s.consume), Refund: monthlyQuotaMoney(s.refund), Net: monthlyQuotaMoney(s.consume - s.refund)}}}
		if hasCorrection {
			member.BalanceCorrection = monthlyQuotaMoney(s.correction)
		}
		p.Members = append(p.Members, member)
		for _, row := range []struct {
			kind string
			q    int64
		}{{"recharge", s.recharge}, {"gift", s.gift}, {"adjustment", s.adjustment}} {
			if row.q == 0 {
				continue
			}
			p.Credits.Rows = append(p.Credits.Rows, monthlyCredit{UserID: s.id, Username: name,
				At: end.Unix() - 1, Amount: monthlyQuotaMoney(row.q), Kind: row.kind})
			if row.kind == "recharge" {
				recharges++
			}
		}
		for i, q := range []int64{s.opening, s.recharge, s.gift, s.adjustment, s.consume, s.refund, s.correction, closing} {
			totals[i] += q
		}
	}
	opening, closing, recharge, gift := monthlyQuotaMoney(totals[0]), monthlyQuotaMoney(totals[7]), monthlyQuotaMoney(totals[1]), monthlyQuotaMoney(totals[2])
	p.MonthOpeningBalance, p.MonthEndBalance = &opening, &closing
	p.Credits.Available, p.Credits.RechargeCount = true, &recharges
	p.Credits.RechargeTotal, p.Credits.GiftTotal = &recharge, &gift
	p.Credits.KnownTotal, p.Credits.Adjustment = monthlyQuotaMoney(totals[1]+totals[2]), monthlyQuotaMoney(totals[3])
	if hasCorrection {
		p.Credits.BalanceCorrection = monthlyQuotaMoney(totals[6])
	}
	p.Consumption = &monthlyConsumption{Amount: monthlyQuotaMoney(totals[4]), Refund: monthlyQuotaMoney(totals[5]), Net: monthlyQuotaMoney(totals[4] - totals[5])}
	if !monthlyStatementAmountsComplete(p) {
		t.Fatal("synthetic source is not internally exact")
	}
	return p
}

func monthlyBillCentAudit(t *testing.T, source monthlyStatement, plan monthlyBillCentMonth) {
	t.Helper()
	if plan.SourceSHA256 != plan.Consumption.SourceSHA256 || !monthlyConsumptionCentPlanValid(source, plan.Consumption) ||
		len(plan.Members) != len(source.Members) || plan.Digest != monthlyBillCentMonthDigest(plan) {
		t.Fatal("wrong source/plan identity")
	}
	columns := func(w monthlyBillCentWallet) []string {
		return []string{w.Opening, w.Recharge, w.Gift, w.CreditTotal, w.Adjustment, w.BalanceCorrection, w.Closing}
	}
	auditWallet := func(w monthlyBillCentWallet, c monthlyCentRow) {
		for _, value := range columns(w) {
			r := monthlyCentTestRat(t, value)
			if !new(big.Rat).Mul(r, big.NewRat(100, 1)).IsInt() {
				t.Fatal("wallet exposes sub-cent money")
			}
		}
		credit := new(big.Rat).Add(monthlyCentTestRat(t, w.Recharge), monthlyCentTestRat(t, w.Gift))
		if credit.Cmp(monthlyCentTestRat(t, w.CreditTotal)) != 0 {
			t.Fatal("credit categories do not add up")
		}
		sum := new(big.Rat)
		for _, term := range []struct {
			value string
			sign  int64
		}{{w.Opening, 1}, {w.CreditTotal, 1}, {w.Adjustment, 1}, {w.BalanceCorrection, 1}, {c.Amount, -1}, {c.Refund, 1}} {
			r := monthlyCentTestRat(t, term.value)
			sum.Add(sum, r.Mul(r, big.NewRat(term.sign, 1)))
		}
		if sum.Cmp(monthlyCentTestRat(t, w.Closing)) != 0 {
			t.Fatal("display wallet equation does not close")
		}
	}
	auditWallet(plan.Organization, plan.Consumption.Organization)
	for i, m := range plan.Members {
		if m.UserID != plan.Consumption.Members[i].UserID {
			t.Fatal("wrong account attribution")
		}
		auditWallet(m.Wallet, plan.Consumption.Members[i].Consumption)
	}
	for column, value := range columns(plan.Organization) {
		sum := new(big.Rat)
		for _, m := range plan.Members {
			sum.Add(sum, monthlyCentTestRat(t, columns(m.Wallet)[column]))
		}
		if sum.Cmp(monthlyCentTestRat(t, value)) != 0 {
			t.Fatal("account column differs from organization", column)
		}
	}
	if len(plan.CreditRows) != len(source.Credits.Rows) {
		t.Fatal("missing displayed money movement")
	}
	for _, m := range plan.Members {
		for _, category := range []struct{ kind, value string }{{"recharge", m.Wallet.Recharge}, {"gift", m.Wallet.Gift}, {"adjustment", m.Wallet.Adjustment}} {
			sum := new(big.Rat)
			for i, row := range source.Credits.Rows {
				if plan.CreditRows[i].ID != int64(i) {
					t.Fatal("movement identity changed")
				}
				if row.UserID == m.UserID && row.Kind == category.kind {
					sum.Add(sum, monthlyCentTestRat(t, plan.CreditRows[i].Amount))
				}
			}
			if sum.Cmp(monthlyCentTestRat(t, category.value)) != 0 {
				t.Fatal("movement rows differ from account subtotal")
			}
		}
	}
}

func TestMonthlyBillCentWalletLegacyConflictResolvedInSeparatePlan(t *testing.T) {
	p := monthlyCentStatementFixture(t, true)
	before, _ := json.Marshal(p)
	plan, err := monthlyPlanBillCentMonth(99, monthlyDigest([]byte("synthetic-origin")), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	monthlyBillCentAudit(t, p, plan)
	if plan.Organization.Closing != "29.990000" || plan.Members[0].Wallet.Closing != "9.990000" || plan.Members[1].Wallet.Closing != "10.000000" {
		t.Fatal("new account wallets do not follow allocated cents", plan)
	}
	after, _ := json.Marshal(p)
	if string(before) != string(after) {
		t.Fatal("new projection rewrote old bill")
	}
}

func TestMonthlyBillCentWalletFundingRefundsAndNegativeBalances(t *testing.T) {
	p := monthlyCentBillSource(t, "2026-09",
		monthlyCentBillSpec{id: 2, opening: -2500, recharge: 2500, gift: 1500, adjustment: -1500, consume: 3000, refund: 4000, correction: 2000},
		monthlyCentBillSpec{id: 1, opening: 2500, recharge: 2500, gift: 2500, adjustment: -2500, consume: 2000, refund: 1000, correction: 1000})
	before, _ := json.Marshal(p)
	plan, err := monthlyPlanBillCentMonth(99, monthlyDigest([]byte("synthetic-origin")), p, nil)
	if err != nil {
		t.Fatal(err)
	}
	monthlyBillCentAudit(t, p, plan)
	after, _ := json.Marshal(p)
	if string(before) != string(after) {
		t.Fatal("planning changed source wallet money")
	}
	// Visible credit is the sum of displayed recharge and gift, not a third
	// independent rounding that can contradict both categories.
	one := monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 1, recharge: 2500, gift: 2500, consume: 5000})
	shown, err := monthlyPlanBillCentMonth(99, monthlyDigest([]byte("synthetic-origin")), one, nil)
	if err != nil || shown.Organization.Recharge != "0.010000" || shown.Organization.Gift != "0.010000" ||
		shown.Organization.CreditTotal != "0.020000" || shown.Organization.Closing != "0.010000" {
		t.Fatal("visible funding operands were independently rounded again", err, shown.Organization)
	}
	monthlyBillCentAudit(t, one, shown)
}

func TestMonthlyBillCentSeriesFixedOriginAndImmutableEarlierMonths(t *testing.T) {
	identity := monthlyDigest([]byte("synthetic-origin"))
	aug := monthlyCentBillSource(t, "2026-08", monthlyCentBillSpec{id: 1, opening: 500000, consume: 2000}, monthlyCentBillSpec{id: 2, opening: 500000, consume: 2000})
	sep := monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 1, opening: 498000, consume: 2000}, monthlyCentBillSpec{id: 2, opening: 498000, consume: 2000})
	oct := monthlyCentBillSource(t, "2026-10", monthlyCentBillSpec{id: 1, opening: 496000, consume: 2000}, monthlyCentBillSpec{id: 2, opening: 496000, consume: 2000})
	scope := monthlyDisplaySeriesScope{GroupID: 99, SourceIdentity: identity, From: "2026-08", Through: "2026-09"}
	sources := []monthlyDisplaySeriesSource{{99, identity, sep}, {99, identity, aug}}
	plan, err := monthlyPlanBillCentSeries(context.Background(), scope, sources, nil)
	if err != nil || len(plan.Months) != 2 {
		t.Fatal(err)
	}
	for i, source := range []monthlyStatement{aug, sep} {
		monthlyBillCentAudit(t, source, plan.Months[i])
	}
	if plan.Months[0].Organization.Closing != plan.Months[1].Organization.Opening ||
		plan.Months[0].Members[0].Wallet.Closing != plan.Months[1].Members[0].Wallet.Opening {
		t.Fatal("next month did not retain previous displayed closing")
	}
	again, err := monthlyPlanBillCentSeries(context.Background(), scope, []monthlyDisplaySeriesSource{sources[1], sources[0]}, []monthlyBillCentMonth{plan.Months[1]})
	if err != nil || !reflect.DeepEqual(plan, again) {
		t.Fatal("request order changed earlier month", err)
	}
	scope.Through = "2026-10"
	extended, err := monthlyPlanBillCentSeries(context.Background(), scope, append(sources, monthlyDisplaySeriesSource{99, identity, oct}), plan.Months)
	if err != nil || !reflect.DeepEqual(plan.Months, extended.Months[:2]) {
		t.Fatal("later month changed frozen preceding months", err)
	}
	monthlyBillCentAudit(t, oct, extended.Months[2])
	encoded, _ := json.Marshal(plan)
	var frozen monthlyBillCentSeries
	if json.Unmarshal(encoded, &frozen) != nil || !reflect.DeepEqual(plan, frozen) {
		t.Fatal("saved series changed on reload")
	}
	// A caller can rehash a tampered body; that still must not authorize it.
	frozen.Months[0].Members[0].Wallet.Closing = "999.000000"
	frozen.Months[0].Digest = monthlyBillCentMonthDigest(frozen.Months[0])
	if _, err := monthlyPlanBillCentSeries(context.Background(), scope, append(sources, monthlyDisplaySeriesSource{99, identity, oct}), frozen.Months); err == nil {
		t.Fatal("tampered issued money accepted after rehash")
	}
}

func TestMonthlyBillCentSeriesRejectsMissingSourcesAndForeignBoundaries(t *testing.T) {
	identity := monthlyDigest([]byte("synthetic-origin"))
	aug := monthlyCentBillSource(t, "2026-08", monthlyCentBillSpec{id: 1, opening: 500000, consume: 2000})
	sep := monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 1, opening: 498000, consume: 2000})
	scope := monthlyDisplaySeriesScope{99, identity, "2026-08", "2026-09"}
	sources := []monthlyDisplaySeriesSource{{99, identity, aug}, {99, identity, sep}}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	for _, badContext := range []context.Context{nil, ctx} {
		if _, err := monthlyPlanBillCentSeries(badContext, scope, sources, nil); err == nil {
			t.Fatal("nil or cancelled context accepted")
		}
	}
	for name, change := range map[string]func([]monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource{
		"missing_month":   func(s []monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource { return s[:1] },
		"duplicate_month": func(s []monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource { s[1] = s[0]; return s },
		"foreign_group":   func(s []monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource { s[1].GroupID++; return s },
		"foreign_origin": func(s []monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource {
			s[1].SourceIdentity = monthlyDigest([]byte("other"))
			return s
		},
		"wrong_boundary": func(s []monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource {
			s[1].Statement = monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 1, opening: 498001, consume: 2000})
			return s
		},
		"lost_member_money": func(s []monthlyDisplaySeriesSource) []monthlyDisplaySeriesSource {
			s[1].Statement = monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 2})
			return s
		},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := monthlyPlanBillCentSeries(context.Background(), scope, change(append([]monthlyDisplaySeriesSource(nil), sources...)), nil); err == nil {
				t.Fatal("invalid source/boundary was concealed")
			}
		})
	}
}

func TestMonthlyBillCentSeriesZeroBalanceRosterChanges(t *testing.T) {
	identity := monthlyDigest([]byte("synthetic-origin"))
	aug := monthlyCentBillSource(t, "2026-08", monthlyCentBillSpec{id: 1}, monthlyCentBillSpec{id: 2})
	sep := monthlyCentBillSource(t, "2026-09", monthlyCentBillSpec{id: 1}, monthlyCentBillSpec{id: 3})
	plan, err := monthlyPlanBillCentSeries(context.Background(), monthlyDisplaySeriesScope{99, identity, "2026-08", "2026-09"},
		[]monthlyDisplaySeriesSource{{99, identity, aug}, {99, identity, sep}}, nil)
	if err != nil || len(plan.Months) != 2 {
		t.Fatal("zero-balance roster changes rejected", err)
	}
	monthlyBillCentAudit(t, aug, plan.Months[0])
	monthlyBillCentAudit(t, sep, plan.Months[1])
}

func TestMonthlyBillCentSeriesRandomConservation(t *testing.T) {
	rng := rand.New(rand.NewSource(20261103))
	identity := monthlyDigest([]byte("synthetic-origin"))
	for range 100 {
		balances := make([]int64, 4)
		for i := range balances {
			balances[i] = rng.Int63n(1000000) - 500000
		}
		sources := make([]monthlyDisplaySeriesSource, 0, 6)
		for month := 5; month <= 10; month++ {
			specs := make([]monthlyCentBillSpec, len(balances))
			for i, opening := range balances {
				s := monthlyCentBillSpec{id: int64(i + 1), opening: opening,
					recharge: rng.Int63n(100000), gift: rng.Int63n(10000), adjustment: -rng.Int63n(10000),
					consume: rng.Int63n(100000), refund: rng.Int63n(100000), correction: rng.Int63n(1000)}
				if rng.Intn(4) == 0 {
					s.refund = s.consume // fully refunded account must retain zero net
				}
				specs[i] = s
				balances[i] = s.opening + s.recharge + s.gift + s.adjustment - s.consume + s.refund + s.correction
			}
			p := monthlyCentBillSource(t, fmt.Sprintf("2026-%02d", month), specs...)
			sources = append(sources, monthlyDisplaySeriesSource{99, identity, p})
		}
		plan, err := monthlyPlanBillCentSeries(context.Background(), monthlyDisplaySeriesScope{99, identity, "2026-05", "2026-10"}, sources, nil)
		if err != nil {
			t.Fatal(err)
		}
		for i, month := range plan.Months {
			monthlyBillCentAudit(t, sources[i].Statement, month)
			if i == 0 {
				continue
			}
			previous := plan.Months[i-1]
			if previous.Organization.Closing != month.Organization.Opening {
				t.Fatal("organization boundary changed")
			}
			for j, member := range month.Members {
				if member.Wallet.Opening != previous.Members[j].Wallet.Closing {
					t.Fatal("account boundary changed")
				}
			}
		}
	}
}
