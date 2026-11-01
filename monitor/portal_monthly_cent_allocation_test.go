package monitor

import (
	"bytes"
	"encoding/json"
	"math/big"
	"math/rand"
	"reflect"
	"testing"
)

func monthlyCentTestRat(t *testing.T, s string) *big.Rat {
	t.Helper()
	r, ok := new(big.Rat).SetString(s)
	if !ok {
		t.Fatal("invalid audit decimal")
	}
	return r
}
func monthlyCentTestTotal(t *testing.T, rows []monthlyCentRow) monthlyConsumption {
	t.Helper()
	g, r, n := new(big.Rat), new(big.Rat), new(big.Rat)
	for _, v := range rows {
		g.Add(g, monthlyCentTestRat(t, v.Amount))
		r.Add(r, monthlyCentTestRat(t, v.Refund))
		n.Add(n, monthlyCentTestRat(t, v.Net))
	}
	return monthlyConsumption{Amount: g.FloatString(6), Refund: r.FloatString(6), Net: n.FloatString(6)}
}
func monthlyCentTestRounded(t *testing.T, s string) string {
	t.Helper()
	v := monthlyCentTestRat(t, s).FloatString(2)
	if v == "-0.00" {
		v = "0.00"
	}
	return v + "0000"
}
func monthlyCentTestPlan(t *testing.T, rows []monthlyCentRow) monthlyCentPlan {
	t.Helper()
	v := monthlyCentTestTotal(t, rows)
	p, e := monthlyCentDefault(v, rows)
	if e != nil {
		t.Fatal("consistent originals could not allocate", v, rows, e)
	}
	monthlyCentTestCheck(t, v, rows, p)
	if p.Amount != monthlyCentTestRounded(t, v.Amount) || p.Refund != monthlyCentTestRounded(t, v.Refund) {
		t.Fatal("root gross/refund totals changed")
	}
	return p
}
func monthlyCentTestCheck(t *testing.T, original monthlyConsumption, rows []monthlyCentRow, p monthlyCentPlan) {
	t.Helper()
	byID := map[int64]monthlyCentRow{}
	for _, r := range rows {
		byID[r.ID] = r
	}
	if len(p.Rows) != len(rows) {
		t.Fatal("row loss")
	}
	sums := [3]*big.Rat{new(big.Rat), new(big.Rat), new(big.Rat)}
	last := int64(-1)
	for _, r := range p.Rows {
		old, ok := byID[r.ID]
		if !ok || r.ID <= last {
			t.Fatal("wrong/unstable identity")
		}
		last = r.ID
		for k, s := range []string{r.Amount, r.Refund, r.Net} {
			v := monthlyCentTestRat(t, s)
			scaled := new(big.Rat).Mul(v, big.NewRat(100, 1))
			if !scaled.IsInt() {
				t.Fatal("display has hidden sub-cent amount")
			}
			sums[k].Add(sums[k], v)
			raw := monthlyCentTestRat(t, []string{old.Amount, old.Refund, old.Net}[k])
			gap := new(big.Rat).Sub(v, raw)
			gap.Abs(gap)
			if gap.Cmp(big.NewRat(1, 100)) >= 0 {
				t.Fatal("allocation changed a row by one cent or more", r, old)
			}
			if raw.Sign() == 0 && v.Sign() != 0 {
				t.Fatal("invented charge/refund on an exact-zero source")
			}
		}
		if new(big.Rat).Sub(monthlyCentTestRat(t, r.Amount), monthlyCentTestRat(t, r.Refund)).Cmp(monthlyCentTestRat(t, r.Net)) != 0 {
			t.Fatal("row gross-refund differs from net")
		}
	}
	for k, s := range []string{p.Amount, p.Refund, p.Net} {
		if sums[k].Cmp(monthlyCentTestRat(t, s)) != 0 {
			t.Fatal("column does not add to its parent")
		}
	}
	if new(big.Rat).Sub(monthlyCentTestRat(t, p.Amount), monthlyCentTestRat(t, p.Refund)).Cmp(monthlyCentTestRat(t, p.Net)) != 0 {
		t.Fatal("parent gross-refund differs from net")
	}
	if new(big.Rat).Sub(monthlyCentTestRat(t, original.Amount), monthlyCentTestRat(t, original.Refund)).Cmp(monthlyCentTestRat(t, original.Net)) != 0 {
		t.Fatal("invalid original totals")
	}
}

func TestMonthlyCentAllocationExamples(t *testing.T) {
	cases := []struct {
		name string
		rows []monthlyCentRow
	}{
		{"empty_month", nil},
		{"half_up_1_005", []monthlyCentRow{{1, "1.005000", "0.000000", "1.005000"}}},
		{"zero_and_unknown_token_id", []monthlyCentRow{{0, "0.000000", "0.000000", "0.000000"}, {2, "0.008000", "0.000000", "0.008000"}}},
		{"fully_refunded_token", []monthlyCentRow{{1, "0.004000", "0.004000", "0.000000"}, {2, "0.004000", "0.000000", "0.004000"}}},
		{"refund_exceeds_month_consumption", []monthlyCentRow{{1, "0.000000", "0.006000", "-0.006000"}, {2, "0.004000", "0.000000", "0.004000"}}},
		{"negative_net_half_cent", []monthlyCentRow{{1, "0.005000", "0.010000", "-0.005000"}}},
		{"equal_fraction_tie_by_id", []monthlyCentRow{{3, "0.002000", "0.000000", "0.002000"}, {2, "0.004000", "0.000000", "0.004000"}, {1, "0.004000", "0.000000", "0.004000"}}},
		{"large_integer_exactness", []monthlyCentRow{{1, "999999999999999999999.004000", "0.002000", "999999999999999999999.002000"}, {2, "0.004000", "0.000000", "0.004000"}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before, _ := json.Marshal(tc.rows)
			p := monthlyCentTestPlan(t, tc.rows)
			after, _ := json.Marshal(tc.rows)
			if !bytes.Equal(before, after) {
				t.Fatal("originals modified")
			}
			again := append([]monthlyCentRow(nil), tc.rows...)
			for i, j := 0, len(again)-1; i < j; i, j = i+1, j-1 {
				again[i], again[j] = again[j], again[i]
			}
			if !reflect.DeepEqual(p, monthlyCentTestPlan(t, again)) {
				t.Fatal("query order changed allocation")
			}
			if tc.name == "half_up_1_005" && p.Amount != "1.010000" {
				t.Fatal("wrong monetary half-up rounding")
			}
			if tc.name == "fully_refunded_token" && p.Rows[0].Net != "0.000000" {
				t.Fatal("fully refunded token has invented net charge")
			}
		})
	}
}

func TestMonthlyCentAllocationRejectsWrongOriginals(t *testing.T) {
	base := []monthlyCentRow{{1, "0.004000", "0.000000", "0.004000"}}
	total := monthlyCentTestTotal(t, base)
	for _, name := range []string{"lost_log", "wrong_net", "duplicate_id", "bad_precision", "negative_gross", "incompatible_target", "zero_to_cent", "capacity"} {
		t.Run(name, func(t *testing.T) {
			rows := append([]monthlyCentRow(nil), base...)
			v := total
			g, r := "0.000000", "0.000000"
			switch name {
			case "lost_log":
				v.Amount = "0.014000"
				v.Net = "0.014000"
				g = monthlyCentTestRounded(t, v.Amount) // valid target; reject the missing original, not target precision
			case "wrong_net":
				rows[0].Net = "0.002000"
			case "duplicate_id":
				rows = append(rows, rows[0])
				v = monthlyCentTestTotal(t, rows)
			case "bad_precision":
				rows[0].Amount = "0.0040001"
			case "negative_gross":
				rows[0].Amount = "-0.004000"
				rows[0].Net = "-0.004000"
				v = monthlyCentTestTotal(t, rows)
			case "incompatible_target":
				g = "0.020000"
			case "zero_to_cent":
				rows[0] = monthlyCentRow{1, "0.000000", "0.000000", "0.000000"}
				v = monthlyCentTestTotal(t, rows)
				g = "0.010000"
			case "capacity":
				rows = make([]monthlyCentRow, monthlyTokenPerUserLimit+1)
			}
			if _, e := monthlyCentAllocate(v, g, r, rows); e == nil {
				t.Fatal("invalid facts/target silently made to add up")
			}
		})
	}
}

func TestMonthlyCentAllocationRandomHierarchy(t *testing.T) {
	rng := rand.New(rand.NewSource(20261101))
	for run := 0; run < 2000; run++ {
		members := make([]monthlyCentRow, 1+rng.Intn(8))
		children := make([][]monthlyCentRow, len(members))
		for i := range members {
			rows := make([]monthlyCentRow, 1+rng.Intn(10))
			for j := range rows {
				g, r := rng.Int63n(1000000)*2, rng.Int63n(1000000)*2
				if rng.Intn(5) == 0 {
					r = g
				}
				rows[j] = monthlyCentRow{int64(j), monthlyCentMicroText(big.NewInt(g)), monthlyCentMicroText(big.NewInt(r)), monthlyCentMicroText(big.NewInt(g - r))}
			}
			v := monthlyCentTestTotal(t, rows)
			members[i] = monthlyCentRow{int64(i + 1), v.Amount, v.Refund, v.Net}
			children[i] = rows
		}
		p := monthlyCentTestPlan(t, members)
		for i, m := range p.Rows {
			v := monthlyCentTestTotal(t, children[i])
			child, e := monthlyCentAllocate(v, m.Amount, m.Refund, children[i])
			if e != nil {
				t.Fatal("parent allocation is infeasible for its own children", run, i, e)
			}
			monthlyCentTestCheck(t, v, children[i], child)
			if child.Net != m.Net {
				t.Fatal("hierarchy net does not match")
			}
		}
	}
}

func TestMonthlyCentAllocationThousandTokens(t *testing.T) {
	rows := make([]monthlyCentRow, monthlyTokenPerUserLimit)
	for i := range rows {
		g, r := int64(i%19+1)*234568, int64(i%17)*123456
		rows[i] = monthlyCentRow{int64(i), monthlyCentMicroText(big.NewInt(g)), monthlyCentMicroText(big.NewInt(r)), monthlyCentMicroText(big.NewInt(g - r))}
	}
	monthlyCentTestPlan(t, rows)
}

func TestMonthlyCentAllocationIndependentRefundCounterexample(t *testing.T) {
	gross := monthlyCentTestPlan(t, []monthlyCentRow{{1, "0.004000", "0.000000", "0.004000"}, {2, "0.004000", "0.000000", "0.004000"}})
	refund := monthlyCentTestPlan(t, []monthlyCentRow{{1, "0.004000", "0.000000", "0.004000"}, {2, "0.000000", "0.000000", "0.000000"}})
	falseNet := new(big.Rat).Sub(monthlyCentTestRat(t, gross.Rows[0].Amount), monthlyCentTestRat(t, refund.Rows[0].Amount))
	if falseNet.Sign() == 0 {
		t.Fatal("independent-rounding counterexample was not reproduced")
	}
	joint := monthlyCentTestPlan(t, []monthlyCentRow{{1, "0.004000", "0.004000", "0.000000"}, {2, "0.004000", "0.000000", "0.004000"}})
	if joint.Rows[0].Net != "0.000000" || joint.Rows[1].Net != "0.010000" || joint.Net != "0.010000" {
		t.Fatal("joint allocation did not preserve exact-zero rows and displayed gross-minus-refund")
	}
}

func TestMonthlyCentAllocationConcurrentDeterministic(t *testing.T) {
	rows := []monthlyCentRow{{1, "0.004000", "0.004000", "0.000000"}, {2, "0.004000", "0.000000", "0.004000"}}
	total := monthlyCentTestTotal(t, rows)
	want := monthlyCentTestPlan(t, rows)
	result := make(chan bool, 8)
	for range 8 {
		go func() {
			for range 50 {
				p, e := monthlyCentDefault(total, rows)
				if e != nil || !reflect.DeepEqual(p, want) {
					result <- false
					return
				}
			}
			result <- true
		}()
	}
	for range 8 {
		if !<-result {
			t.Fatal("concurrent allocation changed result")
		}
	}
}

func TestMonthlyCentAllocationExhaustiveThreeRows(t *testing.T) {
	values := []int64{0, 2000, 4000, 5000, 6000, 8000}
	abs := func(v int64) int64 {
		if v < 0 {
			return -v
		}
		return v
	}
	floor := func(v int64) int64 {
		q := v / 10000
		if v < 0 && v%10000 != 0 {
			q--
		}
		return q
	}
	for sample := 0; sample < 46656; sample++ {
		code := sample
		var source [3][3]int64
		rows := make([]monthlyCentRow, 3)
		for i := 0; i < 3; i++ {
			g, r := values[code%6], values[(code/6)%6]
			code /= 36
			source[i] = [3]int64{g, r, g - r}
			rows[i] = monthlyCentRow{int64(i + 1), monthlyCentMicroText(big.NewInt(g)), monthlyCentMicroText(big.NewInt(r)), monthlyCentMicroText(big.NewInt(g - r))}
		}
		p := monthlyCentTestPlan(t, rows)
		wantG := monthlyCentTestRat(t, p.Amount)
		wantR := monthlyCentTestRat(t, p.Refund)
		wantG.Mul(wantG, big.NewRat(100, 1))
		wantR.Mul(wantR, big.NewRat(100, 1))
		best := int64(1 << 60)
		for mask := 0; mask < 64; mask++ {
			valid := true
			var sumG, sumR, cost int64
			for i := 0; i < 3; i++ {
				g := floor(source[i][0]) + int64((mask>>(2*i))&1)
				r := floor(source[i][1]) + int64((mask>>(2*i+1))&1)
				for k, v := range []int64{g, r, g - r} {
					lo := floor(source[i][k])
					hi := lo
					if source[i][k]%10000 != 0 {
						hi++
					}
					if v < lo || v > hi {
						valid = false
						break
					}
					cost += abs(v*10000 - source[i][k])
				}
				if !valid {
					break
				}
				sumG += g
				sumR += r
			}
			if valid && sumG == wantG.Num().Int64() && sumR == wantR.Num().Int64() && cost < best {
				best = cost
			}
		}
		var got int64
		for i, row := range p.Rows {
			for k, s := range []string{row.Amount, row.Refund, row.Net} {
				v := monthlyCentTestRat(t, s)
				v.Mul(v, big.NewRat(1000000, 1))
				got += abs(v.Num().Int64() - source[i][k])
			}
		}
		if best == 1<<60 || got != best {
			t.Fatal("allocation did not minimize total rounding error against independent exhaustive oracle", sample, got, best)
		}
	}
}
