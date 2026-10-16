package usagebill

import (
	"errors"
	"math"
	"math/big"
	"strings"
	"sync"
	"testing"
)

func charge(t *testing.T, quota int64) Decimal {
	t.Helper()
	value, err := ActualChargeCNY(&quota)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func original(t *testing.T, components ...PriceComponent) Decimal {
	t.Helper()
	value, err := OfficialOriginalCNY(components)
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func fixed(t *testing.T, value Decimal, expected string) {
	t.Helper()
	actual, err := value.Fixed(6)
	if err != nil || actual != expected {
		t.Fatalf("amount=%s error=%v; want %s", actual, err, expected)
	}
}

func TestMoneyRequirementAT01Demonstration(t *testing.T) {
	// Demonstration prices only, NOT a real model's verified official catalogue.
	components := []PriceComponent{{1000, 1000000, "2"}, {500, 1000000, "8"}, {2000, 1000000, "0.2"}, {0, 1000000, "0"}}
	official := original(t, components...)
	actual := charge(t, 2688)
	fixed(t, official, "0.044800")
	fixed(t, actual, "0.005376")
	fold, ok := DiscountFold(actual, official)
	if !ok || fold.Exact() != "6/5" {
		t.Fatalf("want numeric 1.20 fold, got %s defined=%v", fold.Exact(), ok)
	}
	if components[0].Tokens != 1000 || components[2].UnitPriceUSD != "0.2" {
		t.Fatal("caller-owned components mutated")
	}
}

func TestMoneyActualQuotaMissingZeroAndBoundary(t *testing.T) {
	if _, err := ActualChargeCNY(nil); !errors.Is(err, ErrMissingQuota) {
		t.Fatal("missing quota became zero")
	}
	negative := int64(-1)
	if _, err := ActualChargeCNY(&negative); !errors.Is(err, ErrInvalidQuota) {
		t.Fatal("refund/negative quota treated as wallet consumption")
	}
	for _, tc := range []struct {
		quota int64
		want  string
	}{
		{0, "0.000000"}, {1, "0.000002"}, {500000, "1.000000"},
		{math.MaxInt64, "18446744073709.551614"},
	} {
		fixed(t, charge(t, tc.quota), tc.want)
	}
}

func TestMoneyDiscountMissingZeroAndAboveTen(t *testing.T) {
	for _, tc := range []struct {
		actual, base int64
		want         string
		defined      bool
	}{
		{60000, 500000, "1.200000", true},
		{0, 500000, "0.000000", true},
		{600000, 500000, "12.000000", true},
		{60000, 0, "0.000000", false},
		{0, 0, "0.000000", false},
	} {
		fold, defined := DiscountFold(charge(t, tc.actual), charge(t, tc.base))
		if defined != tc.defined {
			t.Fatal("zero original confused with zero actual")
		}
		fixed(t, fold, tc.want)
	}
	var empty Summary
	if _, ok := empty.Totals().DiscountFold(); ok {
		t.Fatal("empty bill has a fabricated discount")
	}
}

func TestMoneyOfficialUnitsAndCacheWriteVariants(t *testing.T) {
	perThousand := original(t, PriceComponent{3000, 1000, "0.002"})
	perMillion := original(t, PriceComponent{3000, 1000000, "2"})
	if perThousand.Exact() != perMillion.Exact() {
		t.Fatal("official units mixed")
	}
	// Write 5m and 1h are disjoint, normalized components, not a duplicated total.
	write := original(t, PriceComponent{100, 1000000, "2.5"}, PriceComponent{200, 1000000, "4"})
	fixed(t, write, "0.007350")
	zero := original(t, PriceComponent{999, 1000, "0"})
	fixed(t, zero, "0.000000")
	for _, components := range [][]PriceComponent{nil, {}, make([]PriceComponent, 33)} {
		if _, err := OfficialOriginalCNY(components); !errors.Is(err, ErrIncompletePrice) {
			t.Fatal("missing/unbounded components accepted")
		}
	}
	for _, tc := range []PriceComponent{
		{1, 1000, ""}, {-1, 1000, "1"}, {1, 0, "1"}, {1, -1, "1"},
		{1, 1000, "-1"}, {1, 1000, "+1"}, {1, 1000, " 1"}, {1, 1000, "1/7"},
		{1, 1000, "1e3"}, {1, 1000, "NaN"}, {1, 1000, "Inf"}, {1, 1000, "0x10"},
		{1, 1000, "1."}, {1, 1000, ".1"}, {1, 1000, "1,000"},
		{1, 1000, strings.Repeat("9", 33)}, {1, 1000, "0." + strings.Repeat("1", 19)},
		{0, 1000, ""}, // Zero usage does not prove a missing price is zero.
	} {
		if _, err := OfficialOriginalCNY([]PriceComponent{tc}); !errors.Is(err, ErrIncompletePrice) {
			t.Fatalf("invalid component accepted: %+v", tc)
		}
	}
}

func TestMoneySummaryWeightedSameSubsetAndSnapshot(t *testing.T) {
	var summary Summary
	baseA, baseB := charge(t, 500000), charge(t, 4500000)
	if err := summary.Add(charge(t, 50000), &baseA); err != nil {
		t.Fatal(err)
	}
	first := summary.Totals()
	if err := summary.Add(charge(t, 2250000), &baseB); err != nil {
		t.Fatal(err)
	}
	if err := summary.Add(charge(t, 5000000), nil); err != nil {
		t.Fatal(err)
	}
	totals := summary.Totals()
	if totals.Rows != 3 || totals.MatchedRows != 2 {
		t.Fatalf("missing-price row disappeared: %+v", totals)
	}
	fixed(t, totals.ActualCNY, "14.600000")
	fixed(t, totals.MatchedActualCNY, "4.600000")
	fixed(t, totals.MatchedOriginalCNY, "10.000000")
	fold, ok := totals.DiscountFold()
	if !ok {
		t.Fatal("matched subset discount undefined")
	}
	fixed(t, fold, "4.600000") // NOT row-fold average (1 + 5) / 2 = 3.
	fixed(t, first.ActualCNY, "0.100000")
	if first.Rows != 1 || baseA.Exact() != "1" || baseB.Exact() != "9" {
		t.Fatal("previous snapshot or caller value mutated")
	}
	// A known zero is a matched price, unlike nil, but no division by zero.
	var zeros Summary
	zero := Decimal{}
	if err := zeros.Add(charge(t, 1), &zero); err != nil {
		t.Fatal(err)
	}
	if zeros.Totals().MatchedRows != 1 {
		t.Fatal("known zero price counted as absent")
	}
	if _, defined := zeros.Totals().DiscountFold(); defined {
		t.Fatal("zero denominator produced a discount")
	}
}

func TestMoneyPrecisionAndNoIntegerOverflow(t *testing.T) {
	var summary Summary
	tiny := original(t, PriceComponent{1, 1000000, "0.01"})
	for i := 0; i < 100; i++ {
		if err := summary.Add(charge(t, 1), &tiny); err != nil {
			t.Fatal(err)
		}
	}
	fixed(t, tiny, "0.000000") // Display rounding must not erase internal amounts.
	fixed(t, summary.Totals().MatchedOriginalCNY, "0.000007")
	fixed(t, summary.Totals().ActualCNY, "0.000200")
	large := charge(t, math.MaxInt64)
	var huge Summary
	for i := 0; i < 2; i++ {
		if err := huge.Add(large, &large); err != nil {
			t.Fatal(err)
		}
	}
	fixed(t, huge.Totals().ActualCNY, "36893488147419.103228")
	if _, err := large.Fixed(-1); !errors.Is(err, ErrInvalidPrecision) {
		t.Fatal("negative precision accepted")
	}
	if _, err := large.Fixed(19); !errors.Is(err, ErrInvalidPrecision) {
		t.Fatal("unbounded precision accepted")
	}
	full := Summary{rows: math.MaxUint64, actual: large}
	if err := full.Add(large, &large); !errors.Is(err, ErrRowLimit) {
		t.Fatal("counter overflow not stopped")
	}
	if full.rows != math.MaxUint64 || full.actual.Exact() != large.Exact() {
		t.Fatal("failed addition changed summary")
	}
}

func TestMoneyImmutableDecimalConcurrentRead(t *testing.T) {
	actual, base := charge(t, 2688), charge(t, 22400)
	var readers sync.WaitGroup
	for i := 0; i < 8; i++ {
		readers.Go(func() {
			for j := 0; j < 100; j++ {
				fold, ok := DiscountFold(actual, base)
				if !ok || fold.Exact() != "6/5" {
					t.Error("shared decimal mutated")
				}
				if _, err := actual.Fixed(6); err != nil {
					t.Error(err)
				}
			}
		})
	}
	readers.Wait()
	if actual.Exact() != "84/15625" || base.Exact() != "28/625" {
		t.Fatal("input rationals changed")
	}
}

func FuzzMoneyQuotaPartitionConservesAmount(f *testing.F) {
	f.Add(uint64(2688), uint64(500000))
	f.Add(uint64(math.MaxInt64), uint64(math.MaxInt64))
	f.Fuzz(func(t *testing.T, a, b uint64) {
		// The two individually valid int64 charges may have a total above int64.
		a &= math.MaxInt64
		b &= math.MaxInt64
		left, right := charge(t, int64(a)), charge(t, int64(b))
		result := add(left, right)
		scaled := new(big.Rat).Mul(result.rational(), big.NewRat(500000, 1))
		expected := new(big.Int).Add(new(big.Int).SetUint64(a), new(big.Int).SetUint64(b))
		if !scaled.IsInt() || scaled.Num().Cmp(expected) != 0 {
			t.Fatal("partitioned quota sum lost precision")
		}
	})
}
