package monitor

// Exact, bounded display-cent allocation. This is presentation arithmetic only;
// publication adapters must keep original charges and frozen legacy wallets separate.
// Exact original data is an input, never modified or repaired by allocation.
import (
	"container/heap"
	"errors"
	"math/big"
	"sort"
)

var errMonthlyCentAllocation = errors.New("cent display allocation requires exact consistent originals and feasible targets")

type monthlyCentRow struct {
	ID     int64  `json:"id"`
	Amount string `json:"amount"`
	Refund string `json:"refund"`
	Net    string `json:"net"`
}
type monthlyCentPlan struct {
	Amount, Refund, Net string
	Rows                []monthlyCentRow
}
type monthlyCentOriginal struct {
	row    monthlyCentRow
	money  [3]*big.Int
	floor  [3]*big.Int
	rem    [3]int64
	ge, re int
}

func monthlyCentMicroText(n *big.Int) string {
	sign := ""
	v := new(big.Int).Set(n)
	if v.Sign() < 0 {
		sign = "-"
		v.Neg(v)
	}
	s := v.String()
	for len(s) < 7 {
		s = "0" + s
	}
	return sign + s[:len(s)-6] + "." + s[len(s)-6:]
}
func monthlyCentParse(s string) (*big.Int, error) {
	n, ok := monthlyRoundedDisplayUnits(s, 6)
	if !ok || monthlyCentMicroText(n) != s {
		return nil, errMonthlyCentAllocation
	}
	return n, nil
}
func monthlyCentBounds(n *big.Int) (*big.Int, int64) {
	q, r := new(big.Int), new(big.Int)
	q.DivMod(n, big.NewInt(10000), r)
	return q, r.Int64()
}
func monthlyCentTarget(n *big.Int, s string) (*big.Int, error) {
	v, e := monthlyCentParse(s)
	if e != nil {
		return nil, e
	}
	q, r := monthlyCentBounds(v)
	if r != 0 {
		return nil, errMonthlyCentAllocation
	}
	lo, rem := monthlyCentBounds(n)
	hi := new(big.Int).Set(lo)
	if rem != 0 {
		hi.Add(hi, big.NewInt(1))
	}
	if q.Cmp(lo) < 0 || q.Cmp(hi) > 0 {
		return nil, errMonthlyCentAllocation
	}
	return q, nil
}

// At the root, retain existing gross/refund half-up totals. The displayed net
// is their difference, NOT a third independent rounding. At signed half-cent
// boundaries all three independently rounded values cannot always reconcile.
func monthlyCentDefault(total monthlyConsumption, rows []monthlyCentRow) (monthlyCentPlan, error) {
	g, ok := monthlyRoundedDisplayUnits(total.Amount, 2)
	if !ok {
		return monthlyCentPlan{}, errMonthlyCentAllocation
	}
	r, ok := monthlyRoundedDisplayUnits(total.Refund, 2)
	if !ok {
		return monthlyCentPlan{}, errMonthlyCentAllocation
	}
	return monthlyCentAllocate(total, monthlyDisplayCentsMoney(g), monthlyDisplayCentsMoney(r), rows)
}

// Parent targets can be its independently rounded totals or an allocation
// from its parent. Each stays within floor/ceiling of the EXACT parent amount.
// A net column is constrained jointly, not independently rounded afterwards.
func monthlyCentAllocate(total monthlyConsumption, shownAmount, shownRefund string, rows []monthlyCentRow) (monthlyCentPlan, error) {
	var empty monthlyCentPlan
	if len(rows) > monthlyTokenPerUserLimit {
		return empty, errMonthlyCentAllocation
	}
	raw := []string{total.Amount, total.Refund, total.Net}
	var totals [3]*big.Int
	for i, s := range raw {
		n, e := monthlyCentParse(s)
		if e != nil {
			return empty, e
		}
		totals[i] = n
	}
	if totals[0].Sign() < 0 || totals[1].Sign() < 0 || new(big.Int).Sub(totals[0], totals[1]).Cmp(totals[2]) != 0 {
		return empty, errMonthlyCentAllocation
	}
	a, e := monthlyCentTarget(totals[0], shownAmount)
	if e != nil {
		return empty, e
	}
	r, e := monthlyCentTarget(totals[1], shownRefund)
	if e != nil {
		return empty, e
	}
	net := new(big.Int).Sub(a, r)
	if _, e := monthlyCentTarget(totals[2], monthlyDisplayCentsMoney(net)); e != nil {
		return empty, e
	}
	originals := make([]monthlyCentOriginal, len(rows))
	sums := [3]*big.Int{new(big.Int), new(big.Int), new(big.Int)}
	floorA, floorR := new(big.Int), new(big.Int)
	for i, row := range rows {
		if row.ID < 0 || row.ID > 9007199254740991 {
			return empty, errMonthlyCentAllocation
		}
		o := monthlyCentOriginal{row: row, ge: -1, re: -1}
		for k, s := range []string{row.Amount, row.Refund, row.Net} {
			n, e := monthlyCentParse(s)
			if e != nil {
				return empty, e
			}
			o.money[k] = n
			o.floor[k], o.rem[k] = monthlyCentBounds(n)
			sums[k].Add(sums[k], n)
		}
		if o.money[0].Sign() < 0 || o.money[1].Sign() < 0 || new(big.Int).Sub(o.money[0], o.money[1]).Cmp(o.money[2]) != 0 {
			return empty, errMonthlyCentAllocation
		}
		floorA.Add(floorA, o.floor[0])
		floorR.Add(floorR, o.floor[1])
		originals[i] = o
	}
	for k := range sums {
		if sums[k].Cmp(totals[k]) != 0 {
			return empty, errMonthlyCentAllocation
		}
	}
	sort.Slice(originals, func(i, j int) bool { return originals[i].row.ID < originals[j].row.ID })
	for i := 1; i < len(originals); i++ {
		if originals[i-1].row.ID == originals[i].row.ID {
			return empty, errMonthlyCentAllocation
		}
	}
	needA, needR := new(big.Int).Sub(a, floorA), new(big.Int).Sub(r, floorR)
	if !needA.IsInt64() || !needR.IsInt64() || needA.Sign() < 0 || needR.Sign() < 0 || needA.Int64() > int64(len(rows)) || needR.Int64() > int64(len(rows)) {
		return empty, errMonthlyCentAllocation
	}
	n := len(rows)
	g := monthlyCentGraph{edges: make([][]monthlyCentEdge, n+5)}
	gn, rn, on, source, sink := n, n+1, n+2, n+3, n+4
	scale := int64(10*n*n + 1)
	for i := range originals {
		o := &originals[i]
		rank := int64(i + 1)
		if o.rem[0] > 0 {
			o.ge = g.add(gn, i, 1, (10000-2*o.rem[0])*scale+rank)
		}
		if o.rem[1] > 0 {
			o.re = g.add(i, rn, 1, (10000-2*o.rem[1])*scale+rank)
		}
		if o.rem[0] > o.rem[1] {
			g.add(i, on, 1, (10000-2*(o.rem[0]-o.rem[1]))*scale)
		}
		if o.rem[0] < o.rem[1] {
			g.add(on, i, 1, (2*o.rem[2]-10000)*scale)
		}
	}
	na, nr := int(needA.Int64()), int(needR.Int64())
	g.add(source, gn, na, 0)
	g.add(rn, sink, nr, 0)
	want := na
	if nr > na {
		g.add(source, on, nr-na, 0)
		want = nr
	} else if na > nr {
		g.add(on, sink, na-nr, 0)
	}
	if err := g.fill(source, sink, want); err != nil {
		return empty, err
	}
	result := monthlyCentPlan{Amount: monthlyDisplayCentsMoney(a), Refund: monthlyDisplayCentsMoney(r), Net: monthlyDisplayCentsMoney(net), Rows: make([]monthlyCentRow, 0, n)}
	shownSums := [3]*big.Int{new(big.Int), new(big.Int), new(big.Int)}
	for i, o := range originals {
		ga, rr := new(big.Int).Set(o.floor[0]), new(big.Int).Set(o.floor[1])
		if o.ge >= 0 && g.edges[gn][o.ge].cap == 0 {
			ga.Add(ga, big.NewInt(1))
		}
		if o.re >= 0 && g.edges[i][o.re].cap == 0 {
			rr.Add(rr, big.NewInt(1))
		}
		nn := new(big.Int).Sub(ga, rr)
		for k, v := range []*big.Int{ga, rr, nn} {
			lo := o.floor[k]
			hi := new(big.Int).Set(lo)
			if o.rem[k] > 0 {
				hi.Add(hi, big.NewInt(1))
			}
			if v.Cmp(lo) < 0 || v.Cmp(hi) > 0 {
				return empty, errMonthlyCentAllocation
			}
			shownSums[k].Add(shownSums[k], v)
		}
		result.Rows = append(result.Rows, monthlyCentRow{ID: o.row.ID, Amount: monthlyDisplayCentsMoney(ga), Refund: monthlyDisplayCentsMoney(rr), Net: monthlyDisplayCentsMoney(nn)})
	}
	for k, v := range []*big.Int{a, r, net} {
		if shownSums[k].Cmp(v) != 0 {
			return empty, errMonthlyCentAllocation
		}
	}
	return result, nil
}

// Integer min-cost rounding flow: G -> item -> R; only the direction allowed
// by the item's exact net remainder may connect that item to O. Consequently
// a fully refunded item cannot acquire a fictitious one-cent net charge.
type monthlyCentEdge struct {
	to, rev, cap int
	cost         int64
}
type monthlyCentGraph struct{ edges [][]monthlyCentEdge }

func (g *monthlyCentGraph) add(from, to, cap int, cost int64) int {
	i, j := len(g.edges[from]), len(g.edges[to])
	g.edges[from] = append(g.edges[from], monthlyCentEdge{to, j, cap, cost})
	g.edges[to] = append(g.edges[to], monthlyCentEdge{from, i, 0, -cost})
	return i
}

type monthlyCentHeapItem struct {
	v int
	d int64
}
type monthlyCentHeap []monthlyCentHeapItem

func (h monthlyCentHeap) Len() int { return len(h) }
func (h monthlyCentHeap) Less(i, j int) bool {
	if h[i].d != h[j].d {
		return h[i].d < h[j].d
	}
	return h[i].v < h[j].v
}
func (h monthlyCentHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *monthlyCentHeap) Push(v any)   { *h = append(*h, v.(monthlyCentHeapItem)) }
func (h *monthlyCentHeap) Pop() any     { old := *h; v := old[len(old)-1]; *h = old[:len(old)-1]; return v }
func (g *monthlyCentGraph) fill(source, sink, want int) error {
	if want == 0 {
		return nil
	}
	const inf int64 = 1 << 60
	n := len(g.edges)
	pot := make([]int64, n)
	for i := range pot {
		pot[i] = inf
	}
	pot[source] = 0
	// Initial positive-capacity graph is acyclic. Bellman-Ford establishes
	// valid potentials for the negative rounding-error costs, then Dijkstra
	// handles bounded augmentations. No floating-point money or random ties.
	for round := 0; round < n; round++ {
		changed := false
		for u, edges := range g.edges {
			if pot[u] == inf {
				continue
			}
			for _, e := range edges {
				if e.cap > 0 && pot[e.to] > pot[u]+e.cost {
					pot[e.to] = pot[u] + e.cost
					changed = true
				}
			}
		}
		if !changed {
			break
		}
		if round == n-1 {
			return errMonthlyCentAllocation
		}
	}
	for i := range pot {
		if pot[i] == inf {
			pot[i] = 0
		}
	}
	for flow := 0; flow < want; flow++ {
		dist := make([]int64, n)
		prev, edge := make([]int, n), make([]int, n)
		for i := range dist {
			dist[i] = inf
			prev[i] = -1
		}
		dist[source] = 0
		q := &monthlyCentHeap{{source, 0}}
		heap.Init(q)
		for q.Len() > 0 {
			cur := heap.Pop(q).(monthlyCentHeapItem)
			if cur.d != dist[cur.v] {
				continue
			}
			for ei, e := range g.edges[cur.v] {
				if e.cap == 0 {
					continue
				}
				cost := e.cost + pot[cur.v] - pot[e.to]
				if cost < 0 {
					return errMonthlyCentAllocation
				}
				if cur.d+cost < dist[e.to] {
					dist[e.to] = cur.d + cost
					prev[e.to] = cur.v
					edge[e.to] = ei
					heap.Push(q, monthlyCentHeapItem{e.to, dist[e.to]})
				}
			}
		}
		if prev[sink] < 0 {
			return errMonthlyCentAllocation
		}
		for i, d := range dist {
			if d != inf {
				pot[i] += d
			}
		}
		for v := sink; v != source; v = prev[v] {
			u, ei := prev[v], edge[v]
			to, rev := g.edges[u][ei].to, g.edges[u][ei].rev
			g.edges[u][ei].cap--
			g.edges[to][rev].cap++
		}
	}
	return nil
}
