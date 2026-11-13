package metering

import (
	"math/big"
	"strings"
	"testing"
)

func TestPriceExpressionExactTiersAndCanonical(t *testing.T) {
	e, err := ParsePriceExpression(`v1: len <= 100 ? tier("short", p*0.1+c*2) : tier("long",p*0.3+c*2)`)
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		n          int64
		cost, tier string
	}{{100, "14", "short"}, {101, "343/10", "long"}} {
		v, tier, err := e.Evaluate(map[string]*big.Rat{"len": big.NewRat(tt.n, 1), "p": big.NewRat(tt.n, 1), "c": big.NewRat(2, 1)})
		if err != nil || v.RatString() != tt.cost || tier != tt.tier {
			t.Fatalf("%v %v %v", v, tier, err)
		}
	}
	rebuilt, err := ParsePriceExpression(e.Canonical)
	if err != nil || rebuilt.Canonical != e.Canonical {
		t.Fatalf("canonical roundtrip %s %v", e.Canonical, err)
	}
}
func TestPriceExpressionRejectsPayloadAndBudgets(t *testing.T) {
	for _, raw := range []string{`p /* PRIVATE */ * 0.1`, `p // PRIVATE`, `header("Authorization")`, `param("body")`, `hour("UTC")`, `p.foo`, `p[0]`, `tier("PRIVATE TEXT",p)`, `v2: p`, strings.Repeat("(", 30) + "p" + strings.Repeat(")", 30), strings.Repeat("p+", 100) + "p"} {
		if e, err := ParsePriceExpression(raw); err == nil {
			t.Fatalf("accepted unsafe expression %+v", e)
		}
	}
	for _, raw := range []string{"p/0", "p-10", "p?1:2", "p<=1"} {
		e, err := ParsePriceExpression(raw)
		if err == nil {
			if _, _, err = e.Evaluate(map[string]*big.Rat{"p": big.NewRat(1, 1)}); err == nil {
				t.Fatalf("accepted invalid result %s", raw)
			}
		}
	}
}

func TestPriceExpressionRecordedTierModifiers(t *testing.T) {
	for _, raw := range []string{`tier("base",p*0.1)*2*1`, `2*tier("base",p*0.1)*1`} {
		e, err := ParsePriceExpression(raw)
		if err != nil {
			t.Fatal(err)
		}
		r, tier, err := e.Evaluate(map[string]*big.Rat{"p": big.NewRat(100, 1)})
		if err != nil || r.Cmp(big.NewRat(20, 1)) != 0 || tier != "base" {
			t.Fatal(r, tier, err)
		}
		v, tier, err := e.EvaluateFloat(map[string]float64{"p": 100})
		if err != nil || v != 20 || tier != "base" {
			t.Fatal(v, tier, err)
		}
	}
	for _, raw := range []string{`tier("a",p)+tier("b",p)`, `tier("a",p)>1?2:3`, `tier("a",tier("b",p))`} {
		e, err := ParsePriceExpression(raw)
		if err != nil {
			t.Fatal(err)
		}
		if _, _, err = e.Evaluate(map[string]*big.Rat{"p": big.NewRat(100, 1)}); err == nil {
			t.Fatal("unbound tiers", raw)
		}
		if _, _, err = e.EvaluateFloat(map[string]float64{"p": 100}); err == nil {
			t.Fatal("unbound tiers", raw)
		}
	}
}
