package monitor

import (
	"math/rand"
	"testing"
	"time"
)

func TestSimpleHalfUpCents(t *testing.T) {
	for _, c := range []struct{ q, want int64 }{
		{0, 0}, {2499, 0}, {2500, 1}, {5000, 1}, {502500, 101}, // 1.005 -> 1.01
		{-2500, -1}, {-2499, 0}, {-502500, -101},
	} {
		if got := simpleHalfUpCents(c.q); got != c.want {
			t.Fatalf("halfUp(%d)=%d want %d", c.q, got, c.want)
		}
	}
}

// The 8XMRT7 case: independent rounding showed closing one cent off.
func TestSimpleSolveMonthBalancesIndependentRoundingCase(t *testing.T) {
	opening := simpleHalfUpCents(1160872000) // 2321.744
	u := simpleRoundUser{OpeningCents: opening, CreditCents: 500000, Closing: 2147773000, Consume: 1513099000,
		Tokens: []simpleRoundToken{{Consume: 1513099000}}}
	res, err := simpleSolveMonth([]simpleRoundUser{u})
	if err != nil {
		t.Fatal(err)
	}
	r := res[0]
	if u.OpeningCents+u.CreditCents-r.ConsumeCents+r.RefundCents != r.ClosingCents {
		t.Fatalf("does not balance: %+v", r)
	}
	if !simpleWithinCent(r.ClosingCents, u.Closing) || !simpleWithinCent(r.ConsumeCents, u.Consume) {
		t.Fatalf("deviation over one cent: %+v", r)
	}
}

func TestSimpleSolveChainRandomProperties(t *testing.T) {
	for _, seed := range []int64{20261106, 77, 4242} {
		rng := rand.New(rand.NewSource(seed))
		months, infeasible := 0, 0
		for trial := 0; trial < 20000; trial++ {
			n := []int{1, 1, 1, 2, 2, 3, 4, 6, 8, 10}[rng.Intn(10)]
			const span = 6
			plan := make([][]simpleRoundUser, span) // true values; openings filled by the chain
			trueBal := make([]int64, n)
			for m := 0; m < span; m++ {
				plan[m] = make([]simpleRoundUser, n)
				for i := 0; i < n; i++ {
					credit := int64(0)
					if rng.Intn(4) == 0 {
						credit = int64(rng.Intn(500)+1) * 500000
					}
					var toks []simpleRoundToken
					var consume, refund int64
					for k := 0; k < []int{0, 0, 1, 1, 2, 3, 5}[rng.Intn(7)]; k++ {
						c := rng.Int63n(4_000_000_000) / []int64{1, 1, 7, 1000, 100000}[rng.Intn(5)]
						r := int64(0)
						if rng.Intn(5) == 0 {
							r = rng.Int63n(c/3 + 1)
						}
						toks = append(toks, simpleRoundToken{Consume: c, Refund: r})
						consume += c
						refund += r
					}
					trueBal[i] += credit - consume + refund
					plan[m][i] = simpleRoundUser{CreditCents: credit / simpleCent, Closing: trueBal[i], Consume: consume, Refund: refund, Tokens: toks}
				}
			}
			build := func(m int, open []int64) []simpleRoundUser {
				users := append([]simpleRoundUser(nil), plan[m]...)
				for i := range users {
					users[i].OpeningCents = open[i]
				}
				return users
			}
			months += span
			res, err := simpleSolveChain(make([]int64, n), span, build)
			if err != nil {
				infeasible++
				continue
			}
			open := make([]int64, n)
			for m := 0; m < span; m++ {
				users := build(m, open)
				checkSimpleMonth(t, users, res[m])
				for i := range users {
					open[i] = res[m][i].ClosingCents
				}
			}
		}
		if infeasible != 0 {
			t.Fatalf("seed %d: %d of %d random organization chains had no balanced solution", seed, infeasible, months/6)
		}
	}
}

func checkSimpleMonth(t *testing.T, users []simpleRoundUser, res []simpleRoundResult) {
	t.Helper()
	var sc, sx, sr, tc, tx, tr int64
	for i, u := range users {
		r := res[i]
		if u.OpeningCents+u.CreditCents-r.ConsumeCents+r.RefundCents != r.ClosingCents {
			t.Fatalf("account does not balance: %+v %+v", u, r)
		}
		if !simpleWithinCent(r.ClosingCents, u.Closing) || !simpleWithinCent(r.ConsumeCents, u.Consume) || !simpleWithinCent(r.RefundCents, u.Refund) {
			t.Fatalf("account amount over one cent: %+v %+v", u, r)
		}
		if (u.Consume == 0 && r.ConsumeCents != 0) || (u.Refund == 0 && r.RefundCents != 0) || r.ConsumeCents < 0 || r.RefundCents < 0 {
			t.Fatalf("zero or sign rule violated: %+v %+v", u, r)
		}
		var a, b int64
		for k, tok := range u.Tokens {
			a += r.TokenConsume[k]
			b += r.TokenRefund[k]
			if !simpleWithinCent(r.TokenConsume[k], tok.Consume) || !simpleWithinCent(r.TokenRefund[k], tok.Refund) ||
				(tok.Consume == 0 && r.TokenConsume[k] != 0) || (tok.Refund == 0 && r.TokenRefund[k] != 0) {
				t.Fatalf("token amount over one cent: %+v %+v", tok, r)
			}
			if !simpleWithinCent(r.TokenConsume[k]-r.TokenRefund[k], tok.Consume-tok.Refund) {
				t.Fatalf("token net over one cent: %+v %+v", tok, r)
			}
		}
		if !simpleWithinCent(r.ConsumeCents-r.RefundCents, u.Consume-u.Refund) {
			t.Fatalf("account net over one cent: %+v %+v", u, r)
		}
		if len(u.Tokens) > 0 && (a != r.ConsumeCents || b != r.RefundCents) {
			t.Fatalf("tokens do not sum to account: %+v", r)
		}
		sc, sx, sr = sc+r.ClosingCents, sx+r.ConsumeCents, sr+r.RefundCents
		tc, tx, tr = tc+u.Closing, tx+u.Consume, tr+u.Refund
	}
	if !simpleWithinCent(sc, tc) || !simpleWithinCent(sx, tx) || !simpleWithinCent(sr, tr) || !simpleWithinCent(sx-sr, tx-tr) {
		t.Fatalf("organization total over one cent")
	}
}

// Large organizations whose credits cannot be balanced must get an answer
// (infeasible) quickly instead of an exhaustive search that blocks the worker.
func TestSimpleSolveIsBoundedWithoutSolution(t *testing.T) {
	for _, n := range []int{60, 80} {
		r := rand.New(rand.NewSource(int64(n)))
		users := make([]simpleRoundUser, n)
		for i := range users {
			creditTrue, creditCents := 100*simpleCent, int64(100)
			for k := 0; k < 3; k++ {
				q := int64(r.Intn(20000) + 1000)
				creditTrue += q
				creditCents += simpleHalfUpCents(q)
			}
			consume := int64(r.Intn(50*int(simpleCent)) + 1)
			users[i] = simpleRoundUser{CreditCents: creditCents, Closing: creditTrue - consume, Consume: consume,
				Tokens: []simpleRoundToken{{Consume: consume}}}
		}
		start := time.Now()
		_, _ = simpleSolveMonthTop(users, 12)
		chainStart := time.Now()
		_, _ = simpleSolveChain(make([]int64, n), 3, func(int, []int64) []simpleRoundUser { return users })
		if d := time.Since(start); d > 30*time.Second {
			t.Fatalf("%d accounts: month %v, chain %v", n, chainStart.Sub(start), time.Since(chainStart))
		}
	}
}

// After a membership change, a past month whose closings are all frozen may
// no longer sum to within a cent for the remaining members. It is fully
// determined by earlier bills and must not block later months.
func TestSimpleSolveChainAcceptsFrozenPastMonthAfterMembershipChange(t *testing.T) {
	// Three remaining accounts, each frozen 0.4 cent below its true closing.
	frozen := []int64{100, 200, 300}
	build := func(m int, open []int64) []simpleRoundUser {
		users := make([]simpleRoundUser, 3)
		for i := range users {
			if m == 0 {
				c := frozen[i]
				users[i] = simpleRoundUser{OpeningCents: 0, CreditCents: frozen[i] + 10, Closing: c*simpleCent + 2000, Consume: 10*simpleCent - 2000,
					FixedClosing: &c, Tokens: []simpleRoundToken{{Consume: 10*simpleCent - 2000}}}
				continue
			}
			users[i] = simpleRoundUser{OpeningCents: open[i], CreditCents: 0, Closing: frozen[i]*simpleCent + 2000 - 5*simpleCent, Consume: 5 * simpleCent,
				Tokens: []simpleRoundToken{{Consume: 5 * simpleCent}}}
		}
		return users
	}
	chain, err := simpleSolveChain([]int64{0, 0, 0}, 2, build)
	if err != nil {
		t.Fatalf("later month blocked by a frozen past month: %v", err)
	}
	for i, r := range chain[0] {
		if r.ClosingCents != frozen[i] {
			t.Fatalf("frozen closing changed: %+v", chain[0])
		}
	}
	// The requested month itself is still held to one cent.
	if _, err := simpleSolveChain([]int64{0, 0, 0}, 1, build); err == nil {
		t.Fatal("requested month accepted with an organization total over one cent")
	}
}

// Ordinary large organizations with long histories must still be solved: the
// search budgets may only stop pathological cases.
func TestSimpleSolveChainLongAndLarge(t *testing.T) {
	for _, tc := range []struct{ members, months int }{{50, 12}, {50, 24}, {20, 24}} {
		rng := rand.New(rand.NewSource(int64(tc.members*100 + tc.months)))
		plan := make([][]simpleRoundUser, tc.months)
		bal := make([]int64, tc.members)
		for m := range plan {
			plan[m] = make([]simpleRoundUser, tc.members)
			for i := range plan[m] {
				credit := int64(rng.Intn(3)) * 1000 * 500000
				var toks []simpleRoundToken
				var c, r int64
				for k := 0; k < 1+rng.Intn(4); k++ {
					tcq := rng.Int63n(400_000_000)
					tr := int64(0)
					if rng.Intn(4) == 0 {
						tr = rng.Int63n(tcq/3 + 1)
					}
					toks = append(toks, simpleRoundToken{Consume: tcq, Refund: tr})
					c, r = c+tcq, r+tr
				}
				bal[i] += credit - c + r
				plan[m][i] = simpleRoundUser{CreditCents: credit / simpleCent, Closing: bal[i], Consume: c, Refund: r, Tokens: toks}
			}
		}
		build := func(m int, open []int64) []simpleRoundUser {
			users := append([]simpleRoundUser(nil), plan[m]...)
			for i := range users {
				users[i].OpeningCents = open[i]
			}
			return users
		}
		start := time.Now()
		if _, err := simpleSolveChain(make([]int64, tc.members), tc.months, build); err != nil {
			t.Fatalf("%d members x %d months: %v after %v", tc.members, tc.months, err, time.Since(start))
		}
		if d := time.Since(start); d > 60*time.Second {
			t.Fatalf("%d members x %d months took %v", tc.members, tc.months, d)
		}
	}
}
