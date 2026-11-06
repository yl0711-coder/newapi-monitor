package monitor

// Customer-independent arithmetic acceptance. All evidence is SYNTHETIC and
// explicitly pre-attested only for testing the calculator; these tests do not
// establish historical wallet proof or bypass production source/admission gates.
import (
	"encoding/json"
	"fmt"
	"math/big"
	"math/rand/v2"
	"reflect"
	"slices"
	"sort"
	"testing"
	"time"
)

// Independent exact quota-to-money oracle. Do not call the formatter or
// aggregation implementation being tested to generate expected money.
func monthlyUniversalMoney(q int64) string {
	n := big.NewInt(q)
	sign := ""
	if n.Sign() < 0 {
		sign = "-"
		n.Neg(n)
	}
	units, rest := new(big.Int), new(big.Int)
	units.QuoRem(n, big.NewInt(500000), rest)
	return fmt.Sprintf("%s%s.%06d", sign, units.String(), rest.Int64()*2)
}

type monthlyUniversalExpected struct {
	money  [7]int64 // opening, positive funding, debit, consumption, refund, net, closing
	tokens map[int64][2]int64
}

func monthlyUniversalEvidence(t *testing.T, seed int) (MonthlyBillEvidence, map[int64]monthlyUniversalExpected, time.Time) {
	t.Helper()
	now := time.Date(2026, 10, 25, 12, 0, 0, 0, usageCST)
	months := []string{"2024-02", "2025-02", "2026-04", "2026-05", "2025-12", "2026-01"}
	month := months[seed%len(months)]
	start, end, err := monthlyWindow(month, now)
	if err != nil {
		t.Fatal(err)
	}
	random := rand.New(rand.NewPCG(uint64(seed)+71, uint64(seed)+719))
	e := MonthlyBillEvidence{Version: 5, GroupID: int64(1000000000 + seed*37), Month: month, ReviewedBy: "synthetic-generic-acceptance",
		ReviewedAt: now.Unix(), SourceReference: "synthetic independent scope; not real customer evidence"}
	count := 1 + seed%9
	if seed == 95 {
		count = 200
	}
	expected := make(map[int64]monthlyUniversalExpected, count)
	for i := 0; i < count; i++ {
		uid := int64(1000000 + seed*1000 + i)
		u := MonthlyBillUserEvidence{UserID: uid, Complete: true, WalletOnly: true, Through: end.Unix(), OpeningQuota: random.Int64N(10000001) - 5000000,
			Events: []MonthlyBillCreditEvidence{}, Reversals: []MonthlyBillReversal{}}
		want := monthlyUniversalExpected{tokens: map[int64][2]int64{}}
		logs := []MonthlyTokenUsageLog{}
		if seed%13 != 0 {
			for token := 0; token < 1+seed%7; token++ {
				id := uid*100 + int64(token+1)
				if token == 0 && seed%3 == 0 {
					id = 0
				} // historical unknown belongs to its own USER
				consume, extra, refund := random.Int64N(1000001), int64(1), random.Int64N(1001)
				if seed%11 == 0 {
					consume, extra, refund = 0, 0, 1000003
				} // refund of an earlier month
				for j, row := range []struct {
					kind int
					q    int64
					name string
				}{{2, consume, "同名令牌"}, {2, extra, "已改名令牌"}, {6, refund, "已改名令牌"}} {
					if seed%11 == 0 && row.kind == 2 {
						continue
					}
					logID := uid*1000 + int64(token*3+j+1)
					logs = append(logs, MonthlyTokenUsageLog{ID: logID, UserID: uid, At: start.Unix() + int64(token*3+j), Type: row.kind, BillingSource: "wallet", TokenID: id, Name: row.name, Quota: row.q})
					value := want.tokens[id]
					if row.kind == 2 {
						u.ConsumeQuota += row.q
						u.Requests++
						value[0] += row.q
					} else {
						u.RefundQuota += row.q
						value[1] += row.q
					}
					want.tokens[id] = value
				}
			}
			for kind, q := range []int64{random.Int64N(1000001) + 1, random.Int64N(50001) + 1, -random.Int64N(100001) - 1} {
				u.Events = append(u.Events, MonthlyBillCreditEvidence{ID: uid*10 + int64(kind+1), At: start.Unix() + 100 + int64(kind), DeltaQuota: q,
					Kind: []string{"recharge", "gift", "adjustment"}[kind], Method: []string{"后台充值", "活动赠送", "余额扣减"}[kind]})
			}
			// Preserve two independently owned source events. Organization net
			// funding must not hide a transfer or use B's wallet to pay A's bill.
			if count > 1 && i < 2 {
				q, kind, method := int64(-175001), "adjustment", "余额扣减"
				if i == 1 {
					q, kind, method = 175001, "recharge", "后台充值"
				}
				u.Events = append(u.Events, MonthlyBillCreditEvidence{ID: uid*10 + 4, At: start.Unix() + 104, DeltaQuota: q, Kind: kind, Method: method})
			}
		}
		tokens, err := PrepareMonthlyTokenUsage(uid, month, "synthetic#tokens", logs, now)
		if err != nil {
			t.Fatal("generic original token normalization", seed, uid, err)
		}
		u.TokenUsage = &tokens
		u.ExpectedCreditEvents = len(u.Events)
		u.Accounting = monthlyAccountingFixture(u.ConsumeQuota, u.RefundQuota)
		balance := big.NewInt(u.OpeningQuota)
		for _, event := range u.Events {
			balance.Add(balance, big.NewInt(event.DeltaQuota))
			if event.DeltaQuota > 0 {
				want.money[1] += event.DeltaQuota
			} else {
				want.money[2] += event.DeltaQuota
			}
		}
		balance.Sub(balance, big.NewInt(u.ConsumeQuota))
		balance.Add(balance, big.NewInt(u.RefundQuota))
		if !balance.IsInt64() {
			t.Fatal("invalid synthetic balance")
		}
		u.ClosingQuota, u.ReverseClosingQuota = balance.Int64(), balance.Int64()
		want.money[0], want.money[3], want.money[4], want.money[5], want.money[6] = u.OpeningQuota, u.ConsumeQuota, u.RefundQuota, u.ConsumeQuota-u.RefundQuota, u.ClosingQuota
		expected[uid] = want
		e.Users = append(e.Users, u)
	}
	if seed%2 == 0 {
		slices.Reverse(e.Users)
	}
	return e, expected, now
}

func TestMonthlyUniversalOrganizationMoneyAndTokenConservation(t *testing.T) {
	for seed := 0; seed < 96; seed++ {
		t.Run(fmt.Sprintf("case_%03d", seed), func(t *testing.T) {
			e, expected, now := monthlyUniversalEvidence(t, seed)
			raw, err := json.Marshal(e)
			if err != nil {
				t.Fatal(err)
			}
			decoded, err := decodeMonthlyBillEvidence(raw)
			if err != nil {
				t.Fatal("generic evidence rejected", err)
			}
			p, ids, err := buildPublishedMonthlyBill(decoded, now)
			if err != nil || !monthlyStatementAmountsComplete(p) || len(ids) != len(expected) || len(p.Members) != len(expected) {
				t.Fatal("generic bill rejected", err)
			}
			var totals [7]big.Int
			for _, m := range p.Members {
				want, ok := expected[m.UserID]
				if !ok {
					t.Fatal("unexpected account")
				}
				got := []string{m.Opening, m.CreditTotal, m.Adjustment, m.Consumption.Amount, m.Consumption.Refund, m.Consumption.Net, m.Closing}
				for i, q := range want.money {
					if got[i] != monthlyUniversalMoney(q) {
						t.Fatal("per-user money differs", m.UserID, i, got[i], q)
					}
					totals[i].Add(&totals[i], big.NewInt(q))
				}
				if len(m.TokenConsumption) != len(want.tokens) {
					t.Fatal("token identity merged by name or omitted")
				}
				for _, token := range m.TokenConsumption {
					q, ok := want.tokens[token.TokenID]
					if !ok || token.Amount != monthlyUniversalMoney(q[0]) || token.Refund != monthlyUniversalMoney(q[1]) || token.Net != monthlyUniversalMoney(q[0]-q[1]) {
						t.Fatal("token owner or gross/refund money changed")
					}
				}
			}
			got := []string{*p.MonthOpeningBalance, p.Credits.KnownTotal, p.Credits.Adjustment, p.Consumption.Amount, p.Consumption.Refund, p.Consumption.Net, *p.MonthEndBalance}
			for i := range totals {
				if !totals[i].IsInt64() || got[i] != monthlyUniversalMoney(totals[i].Int64()) {
					t.Fatal("organization is not sum of independent accounts", i)
				}
			}
			if p.Credits.BalanceCorrection != "" {
				t.Fatal("calculator invented a balance adjustment")
			}
			saved, err := json.Marshal(p)
			var loaded monthlyStatement
			if err != nil || json.Unmarshal(saved, &loaded) != nil || !monthlyStatementAmountsComplete(loaded) {
				t.Fatal("serialized customer amounts changed", err)
			}
			slices.Reverse(decoded.Users)
			again, _, err := buildPublishedMonthlyBill(decoded, now)
			// Display rows with identical timestamps retain input order. Their
			// ordering is not a financial invariant: compare their entire record
			// multiset, not an accidental tie order. No row or amount is omitted.
			ordered := func(value monthlyStatement) monthlyStatement {
				value.Credits.Rows = slices.Clone(value.Credits.Rows)
				sort.Slice(value.Credits.Rows, func(i, j int) bool {
					a, b := value.Credits.Rows[i], value.Credits.Rows[j]
					if a.At != b.At {
						return a.At < b.At
					}
					if a.UserID != b.UserID {
						return a.UserID < b.UserID
					}
					if a.Kind != b.Kind {
						return a.Kind < b.Kind
					}
					if a.Amount != b.Amount {
						return a.Amount < b.Amount
					}
					return a.Method < b.Method
				})
				return value
			}
			if err != nil || !reflect.DeepEqual(ordered(p), ordered(again)) {
				t.Fatal("member ordering changed money or credit records", err)
			}
		})
	}
}

func TestMonthlyUniversalOffsettingAccountErrorsCannotHideInOrganization(t *testing.T) {
	for seed := 1; seed <= 32; seed++ {
		e, _, now := monthlyUniversalEvidence(t, seed)
		if len(e.Users) < 2 {
			continue
		}
		p, _, err := buildPublishedMonthlyBill(e, now)
		if err != nil {
			t.Fatal(err)
		}
		// Keep the organization sum EXACTLY unchanged while A gains one quota
		// and B loses one. A total-only assertion would incorrectly pass.
		a, b := p.Members[0].UserID, p.Members[1].UserID
		for _, u := range e.Users {
			if u.UserID == a {
				p.Members[0].Closing = monthlyUniversalMoney(u.ClosingQuota + 1)
			}
			if u.UserID == b {
				p.Members[1].Closing = monthlyUniversalMoney(u.ClosingQuota - 1)
			}
		}
		if !monthlyStatementMoneyComplete(p) || monthlyStatementAmountsComplete(p) {
			t.Fatal("offsetting account money accepted or organization control not preserved", seed)
		}
		// Scope identity is independent of money equality or a matching token name.
		e.Users[0].TokenUsage.UserID = e.Users[1].UserID
		if _, _, err := buildPublishedMonthlyBill(e, now); err == nil {
			t.Fatal("wrong token owner accepted", seed)
		}
	}
}
