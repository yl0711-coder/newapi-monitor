package monitor

import (
	"math/big"
	"math/rand"
	"reflect"
	"testing"
)

func TestMonthlyCentScalarSignedAndStable(t *testing.T) {
	for _, tc := range []struct {
		total string
		rows  []monthlyCentScalar
	}{
		{"0.000000", nil},
		{"1.005000", []monthlyCentScalar{{1, "1.005000"}}},
		{"-1.005000", []monthlyCentScalar{{1, "-1.005000"}}},
		{"0.008000", []monthlyCentScalar{{2, "0.004000"}, {1, "0.004000"}}},
		{"-0.008000", []monthlyCentScalar{{2, "-0.004000"}, {1, "-0.004000"}}},
		{"0.000000", []monthlyCentScalar{{0, "0.000000"}, {2, "-0.005000"}, {1, "0.005000"}}},
	} {
		shown, err := monthlyCentSplitDefault(tc.total, tc.rows)
		if err != nil {
			t.Fatal(tc, err)
		}
		monthlyCentScalarAudit(t, tc.total, tc.rows, shown)
		reversed := append([]monthlyCentScalar(nil), tc.rows...)
		for i, j := 0, len(reversed)-1; i < j; i, j = i+1, j-1 {
			reversed[i], reversed[j] = reversed[j], reversed[i]
		}
		again, err := monthlyCentSplitDefault(tc.total, reversed)
		if err != nil || !reflect.DeepEqual(shown, again) {
			t.Fatal("input order changed displayed allocation", err)
		}
	}
}

func monthlyCentScalarAudit(t *testing.T, total string, rows, shown []monthlyCentScalar) {
	t.Helper()
	if len(rows) != len(shown) {
		t.Fatal("lost source row")
	}
	byID := make(map[int64]string, len(rows))
	for _, row := range rows {
		byID[row.ID] = row.Amount
	}
	sum := new(big.Rat)
	last := int64(-1)
	for _, row := range shown {
		original, exists := byID[row.ID]
		if !exists || row.ID <= last {
			t.Fatal("wrong or unstable identity")
		}
		last = row.ID
		raw := monthlyCentTestRat(t, original)
		value := monthlyCentTestRat(t, row.Amount)
		if !new(big.Rat).Mul(new(big.Rat).Set(value), big.NewRat(100, 1)).IsInt() {
			t.Fatal("hidden sub-cent money")
		}
		diff := new(big.Rat).Sub(value, raw)
		diff.Abs(diff)
		if diff.Cmp(big.NewRat(1, 100)) >= 0 || raw.Sign() == 0 && value.Sign() != 0 {
			t.Fatal("display drift or invented zero-source amount")
		}
		sum.Add(sum, value)
	}
	if sum.Cmp(monthlyCentTestRat(t, monthlyAuditCents(total))) != 0 {
		t.Fatal("visible amounts do not add to rounded parent")
	}
}

func TestMonthlyCentScalarRandomSigned(t *testing.T) {
	rng := rand.New(rand.NewSource(20261102))
	for range 1000 {
		rows := make([]monthlyCentScalar, 1+rng.Intn(12))
		total := new(big.Rat)
		for i := range rows {
			value := new(big.Rat).SetFrac(big.NewInt(rng.Int63n(2000000)-1000000), big.NewInt(500000))
			rows[i] = monthlyCentScalar{int64(i), value.FloatString(6)}
			total.Add(total, value)
		}
		shown, err := monthlyCentSplitDefault(total.FloatString(6), rows)
		if err != nil {
			t.Fatal(err)
		}
		monthlyCentScalarAudit(t, total.FloatString(6), rows, shown)
	}
}

func TestMonthlyCentScalarRejectsWrongFactsAndTargets(t *testing.T) {
	for name, tc := range map[string]struct {
		total, target string
		rows          []monthlyCentScalar
	}{
		"missing_original": {"0.014000", "0.010000", []monthlyCentScalar{{1, "0.004000"}}},
		"duplicate_id":     {"0.008000", "0.010000", []monthlyCentScalar{{1, "0.004000"}, {1, "0.004000"}}},
		"wrong_precision":  {"0.004000", "0.000000", []monthlyCentScalar{{1, "0.0040001"}}},
		"invalid_id":       {"0.004000", "0.000000", []monthlyCentScalar{{-1, "0.004000"}}},
		"unsafe_id":        {"0.004000", "0.000000", []monthlyCentScalar{{9007199254740992, "0.004000"}}},
		"zero_to_cent":     {"0.000000", "0.010000", []monthlyCentScalar{{1, "0.000000"}}},
		"target_too_large": {"0.004000", "0.020000", []monthlyCentScalar{{1, "0.004000"}}},
		"not_exact_cents":  {"0.004000", "0.004000", []monthlyCentScalar{{1, "0.004000"}}},
		"capacity":         {"0.000000", "0.000000", make([]monthlyCentScalar, monthlyCreditLimit+1)},
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := monthlyCentSplit(tc.total, tc.target, tc.rows); err == nil {
				t.Fatal("invalid original/target was made to add up")
			}
		})
	}
}
