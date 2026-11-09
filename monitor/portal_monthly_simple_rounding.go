package monitor

import (
	"errors"
	"sort"
)

// Controlled rounding for customer bills (amounts in whole cents).
//
// Every displayed amount is within one cent (inclusive) of its true value, a
// true zero is displayed as zero, and the bill balances exactly:
//
//	per account: closing = opening + credits - consumption + refund
//	per account: sum(tokens) = consumption / refund
//	organization: each total = sum of accounts and within one cent of the true total
//
// Openings are fixed (the previous month's frozen closing). The solver picks
// each account's closing/refund cents; consumption is derived.

const simpleCent = int64(5000) // quota per cent (500000 quota = 1 yuan)

var errSimpleRoundingInfeasible = errors.New("monthly bill rounding has no balanced solution")

type simpleRoundToken struct {
	Consume, Refund int64 // true quota
}

type simpleRoundUser struct {
	OpeningCents int64  // fixed
	CreditCents  int64  // fixed: displayed credits + adjustments (signed)
	Closing      int64  // true closing quota
	Consume      int64  // true (settled) consumption quota, >= 0
	Refund       int64  // true refund quota, >= 0
	FixedClosing *int64 // closing cents already frozen (e.g. a later month was generated first)
	Tokens       []simpleRoundToken
}

type simpleRoundResult struct {
	ClosingCents, ConsumeCents, RefundCents int64
	TokenConsume, TokenRefund               []int64
}

func simpleHalfUpCents(q int64) int64 {
	if q >= 0 {
		return (q + simpleCent/2) / simpleCent
	}
	return -((-q + simpleCent/2) / simpleCent)
}

func simpleWithinCent(cents, q int64) bool {
	d := cents*simpleCent - q
	if d < 0 {
		d = -d
	}
	return d <= simpleCent
}

// simpleCentCandidates lists cents within one cent of q, nearest first. A true
// zero only allows zero.
func simpleCentCandidates(q int64) []int64 {
	if q == 0 {
		return []int64{0}
	}
	n := simpleHalfUpCents(q)
	out := []int64{n}
	for _, c := range []int64{n - 1, n + 1, n - 2, n + 2} {
		if simpleWithinCent(c, q) {
			out = append(out, c)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return simpleAbs(out[i]*simpleCent-q) < simpleAbs(out[j]*simpleCent-q) })
	return out
}

func simpleAbs(v int64) int64 {
	if v < 0 {
		return -v
	}
	return v
}

// simpleAllocate splits total cents across parts (true quotas) so each part is
// within one cent of its true value, zero parts stay zero and the sum is exact.
func simpleAllocate(total int64, parts []int64) ([]int64, bool) {
	out := make([]int64, len(parts))
	var sum int64
	for i, p := range parts {
		out[i] = simpleHalfUpCents(p)
		sum += out[i]
	}
	k := total - sum
	if k == 0 {
		return out, true
	}
	step := int64(1)
	if k < 0 {
		step, k = -1, -k
	}
	idx := make([]int, 0, len(parts))
	for i, p := range parts {
		if p != 0 && simpleWithinCent(out[i]+step, p) {
			idx = append(idx, i)
		}
	}
	if int64(len(idx)) < k {
		return nil, false
	}
	// Prefer the parts closest to rounding the other way (largest remainder).
	sort.SliceStable(idx, func(a, b int) bool {
		da := step * (parts[idx[a]] - out[idx[a]]*simpleCent)
		db := step * (parts[idx[b]] - out[idx[b]]*simpleCent)
		if da != db {
			return da > db
		}
		return idx[a] < idx[b]
	})
	for _, i := range idx[:k] {
		out[i] += step
	}
	return out, true
}

type simpleRoundOption struct {
	res                  simpleRoundResult
	devClose, devConsume int64 // displayed - true, quota units
	devRefund            int64
	score                int64
}

func simpleUserOptions(u simpleRoundUser) []simpleRoundOption {
	closings := simpleCentCandidates(u.Closing)
	if u.FixedClosing != nil {
		closings = []int64{*u.FixedClosing}
	}
	var opts []simpleRoundOption
	tc := make([]int64, len(u.Tokens))
	tr := make([]int64, len(u.Tokens))
	for i, t := range u.Tokens {
		tc[i], tr[i] = t.Consume, t.Refund
	}
	for _, closing := range closings {
		for _, refund := range simpleCentCandidates(u.Refund) {
			if refund < 0 {
				continue
			}
			consume := u.OpeningCents + u.CreditCents + refund - closing
			if consume < 0 || !simpleWithinCent(consume, u.Consume) || (u.Consume == 0 && consume != 0) {
				continue
			}
			var tokConsume, tokRefund []int64
			if len(u.Tokens) > 0 {
				var ok bool
				if tokConsume, ok = simpleAllocate(consume, tc); !ok {
					continue
				}
				if tokRefund, ok = simpleAllocate(refund, tr); !ok {
					continue
				}
				if !simpleNetWithinCent(tokConsume, tokRefund, tc, tr) {
					// Re-split the refund jointly with the consumption split so
					// that every token row's net is also within one cent.
					if tokRefund, ok = simpleAllocateRefund(refund, tokConsume, tc, tr); !ok {
						continue
					}
				}
			} else if consume != 0 || refund != 0 {
				continue
			}
			o := simpleRoundOption{
				res:      simpleRoundResult{ClosingCents: closing, ConsumeCents: consume, RefundCents: refund, TokenConsume: tokConsume, TokenRefund: tokRefund},
				devClose: closing*simpleCent - u.Closing, devConsume: consume*simpleCent - u.Consume, devRefund: refund*simpleCent - u.Refund,
			}
			if simpleAbs(o.devConsume-o.devRefund) > simpleCent {
				continue // the account's net would be shown over one cent off
			}
			o.score = simpleAbs(o.devClose) + simpleAbs(o.devConsume) + simpleAbs(o.devRefund)
			opts = append(opts, o)
		}
	}
	sort.SliceStable(opts, func(i, j int) bool { return opts[i].score < opts[j].score })
	return opts
}

// simpleSolveMonth returns the preferred per-account results satisfying every
// constraint, or errSimpleRoundingInfeasible.
func simpleSolveMonth(users []simpleRoundUser) ([]simpleRoundResult, error) {
	top, err := simpleSolveMonthTop(users, 1)
	if err != nil {
		return nil, err
	}
	return top[0], nil
}

// simpleSolveMonthTop returns up to k balanced solutions, best first. Among
// balanced choices it prefers the smallest organization closing deviation (it
// carries into next month's opening), then the nearest individual amounts.
// Depth-first branch-and-bound with interval pruning on the three
// organization totals and a node budget.
func simpleSolveMonthTop(users []simpleRoundUser, k int) ([][]simpleRoundResult, error) {
	budget := int64(simpleMonthNodeBudget)
	return simpleSolveMonthBounded(users, k, simpleCent, &budget)
}

// Search budgets. The search stops when a budget is spent, with or without a
// solution, so a month without any balanced rounding can never keep the
// single worker busy: it is reported infeasible and the request is held.
const (
	simpleMonthNodeBudget = 2_000_000
	// Once enough balanced solutions are known, a month's search stops after
	// this many nodes instead of refining them up to the month budget.
	simpleMonthRefineNodes = 300_000
	// Chain budget per month of the chain (plus headroom for backtracking).
	simpleChainNodesPerMonth = 3_000_000
)

// simpleSolveMonthBounded is simpleSolveMonthTop with an organization-total
// limit (in quota) and a node budget shared by the caller.
func simpleSolveMonthBounded(users []simpleRoundUser, k int, limit int64, budget *int64) ([][]simpleRoundResult, error) {
	n := len(users)
	opts := make([][]simpleRoundOption, n)
	for i, u := range users {
		opts[i] = simpleUserOptions(u)
		if len(opts[i]) == 0 {
			return nil, errSimpleRoundingInfeasible
		}
	}
	type span struct{ lo, hi [3]int64 }
	rest := make([]span, n+1)
	for i := n - 1; i >= 0; i-- {
		var lo, hi [3]int64
		for d := 0; d < 3; d++ {
			lo[d], hi[d] = 1<<62, -(1 << 62)
		}
		for _, o := range opts[i] {
			for d, v := range [3]int64{o.devClose, o.devConsume, o.devRefund} {
				lo[d] = min(lo[d], v)
				hi[d] = max(hi[d], v)
			}
		}
		for d := 0; d < 3; d++ {
			rest[i].lo[d] = rest[i+1].lo[d] + lo[d]
			rest[i].hi[d] = rest[i+1].hi[d] + hi[d]
		}
	}
	type leaf struct {
		score  int64
		choice []int
	}
	var leaves []leaf // sorted ascending, at most k
	worst := func() int64 {
		if len(leaves) < k {
			return 1 << 62
		}
		return leaves[len(leaves)-1].score
	}
	choice := make([]int, n)
	nodes := int64(0)
	var dfs func(i int, acc [3]int64, indiv int64)
	dfs = func(i int, acc [3]int64, indiv int64) {
		nodes++
		*budget--
		if nodes > simpleMonthNodeBudget || *budget < 0 || (len(leaves) >= k && nodes > simpleMonthRefineNodes) {
			return
		}
		for d := 0; d < 3; d++ {
			if acc[d]+rest[i].lo[d] > limit || acc[d]+rest[i].hi[d] < -limit {
				return
			}
		}
		if indiv > worst() {
			return
		}
		if i == n {
			// Net (consumption minus refund) is shown too: the organization's
			// net must also be within one cent.
			if simpleAbs(acc[1]-acc[2]) > limit {
				return
			}
			score := 8*simpleAbs(acc[0]) + indiv
			if score >= worst() {
				return
			}
			c := append([]int(nil), choice...)
			at := sort.Search(len(leaves), func(j int) bool { return leaves[j].score > score })
			leaves = append(leaves, leaf{})
			copy(leaves[at+1:], leaves[at:])
			leaves[at] = leaf{score, c}
			if len(leaves) > k {
				leaves = leaves[:k]
			}
			return
		}
		for j, o := range opts[i] {
			choice[i] = j
			dfs(i+1, [3]int64{acc[0] + o.devClose, acc[1] + o.devConsume, acc[2] + o.devRefund}, indiv+o.score)
		}
	}
	dfs(0, [3]int64{}, 0)
	if len(leaves) == 0 {
		return nil, errSimpleRoundingInfeasible
	}
	out := make([][]simpleRoundResult, len(leaves))
	for li, l := range leaves {
		out[li] = make([]simpleRoundResult, n)
		for i := range users {
			out[li][i] = opts[i][l.choice[i]].res
		}
	}
	return out, nil
}

// simpleSolveChain solves consecutive months for one organization. build
// returns month m's accounts given the openings (cents) carried from month
// m-1. If a month has no balanced solution it backtracks into earlier,
// not-yet-frozen months and tries their alternative solutions.
func simpleSolveChain(openings []int64, months int, build func(m int, openings []int64) []simpleRoundUser) ([][]simpleRoundResult, error) {
	const alternatives = 12
	out := make([][]simpleRoundResult, months)
	calls := 5000
	nodes := int64(simpleChainNodesPerMonth) * int64(months+4)
	var solve func(m int, open []int64) bool
	solve = func(m int, open []int64) bool {
		if m == months {
			return true
		}
		calls--
		if calls < 0 || nodes < 0 {
			return false
		}
		users := build(m, open)
		// A month before the requested one whose closings are all frozen is
		// fully determined by the earlier bills; it is not re-balanced as an
		// organization (a membership change may have altered that sum). The
		// requested month's own totals are always held to one cent.
		limit := simpleCent
		k := alternatives
		if m < months-1 {
			fixed := true
			for _, u := range users {
				if u.FixedClosing == nil {
					fixed = false
					break
				}
			}
			if fixed {
				// Every alternative would carry the same closings: one is enough.
				limit, k = 1<<50, 1
			}
		}
		top, err := simpleSolveMonthBounded(users, k, limit, &nodes)
		if err != nil {
			return false
		}
		for _, res := range top {
			next := make([]int64, len(res))
			for i, r := range res {
				next[i] = r.ClosingCents
			}
			out[m] = res
			if solve(m+1, next) {
				return true
			}
		}
		return false
	}
	if !solve(0, openings) {
		return nil, errSimpleRoundingInfeasible
	}
	return out, nil
}

func simpleNetWithinCent(cons, refs, tc, tr []int64) bool {
	for j := range tc {
		if !simpleWithinCent(cons[j]-refs[j], tc[j]-tr[j]) {
			return false
		}
	}
	return true
}

// simpleAllocateRefund splits total refund cents across tokens so that each
// token's refund and net (given its consumption cents) are within one cent of
// the true values, a true zero refund stays zero, and the parts sum to total.
func simpleAllocateRefund(total int64, cons, tc, tr []int64) ([]int64, bool) {
	n := len(tr)
	lo, hi, out := make([]int64, n), make([]int64, n), make([]int64, n)
	var sum int64
	for j := 0; j < n; j++ {
		if tr[j] == 0 {
			if !simpleWithinCent(cons[j], tc[j]) {
				return nil, false
			}
			continue // lo = hi = 0
		}
		lo[j], hi[j] = 1<<62, -(1 << 62)
		base := tr[j] / simpleCent
		for r := base - 2; r <= base+2; r++ {
			if r >= 0 && simpleWithinCent(r, tr[j]) && simpleWithinCent(cons[j]-r, tc[j]-tr[j]) {
				lo[j], hi[j] = min(lo[j], r), max(hi[j], r)
			}
		}
		if lo[j] > hi[j] {
			return nil, false
		}
		out[j] = min(max(simpleHalfUpCents(tr[j]), lo[j]), hi[j])
		sum += out[j]
	}
	for sum != total {
		step := int64(1)
		if sum > total {
			step = -1
		}
		best, bestGap := -1, int64(0)
		for j := 0; j < n; j++ {
			next := out[j] + step
			if tr[j] == 0 || next < lo[j] || next > hi[j] {
				continue
			}
			gap := step * (tr[j] - out[j]*simpleCent) // prefer the part furthest behind
			if best < 0 || gap > bestGap {
				best, bestGap = j, gap
			}
		}
		if best < 0 {
			return nil, false
		}
		out[best] += step
		sum += step
	}
	return out, true
}
