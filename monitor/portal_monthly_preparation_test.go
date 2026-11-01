package monitor

import (
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"
)

func preparationAt(s string) int64 {
	t, err := time.ParseInLocation("2006-01-02", s, usageCST)
	if err != nil {
		panic(err)
	}
	return t.Unix()
}

func preparationInt(n int64) *int64 { return &n }

func preparationFixture() MonthlyPreparationInput {
	jun, jul, aug, sep := preparationAt("2026-06-01"), preparationAt("2026-07-01"), preparationAt("2026-08-01"), preparationAt("2026-09-01")
	span := func(from, to, consume, refund, requests, tokens int64) MonthlyPreparationSpan {
		return MonthlyPreparationSpan{UserID: 102, From: from, To: to, ConsumeQuota: consume, RefundQuota: refund, Requests: preparationInt(requests), Tokens: preparationInt(tokens), Source: "logs#range"}
	}
	return MonthlyPreparationInput{Version: 1,
		Sources: []MonthlyPreparationSource{{ID: "logs", Path: "/retained/logs.json", SHA256: strings.Repeat("a", 64)}},
		Accounts: []MonthlyPreparationAccount{{UserID: 102, GroupID: 5, Username: "account-102", Months: []string{"2026-06", "2026-07", "2026-08"},
			CounterBasis: "net_consumption", BasisSource: "logs#counter-semantics",
			Baseline:         MonthlyPreparationBalance{UserID: 102, At: jun, BalanceQuota: 500000, UsedQuota: 0, Source: "logs#baseline"},
			Spans:            []MonthlyPreparationSpan{span(jun, jul, 3000000, 0, 20, 200), span(jul, aug, 4000000, 250000, 10, 500), span(aug, sep, 0, 0, 0, 0)},
			Movements:        []MonthlyPreparationMovement{{UserID: 102, ID: 1, At: jun + 10, DeltaQuota: 5000000, Source: "logs#credit"}, {UserID: 102, ID: 2, At: jul, DeltaQuota: -500000, Source: "logs#debit"}},
			MovementCoverage: []MonthlyPreparationCoverage{{UserID: 102, From: jun, To: sep, Source: "logs#funds-query"}},
			Snapshots: []MonthlyPreparationBalance{
				{UserID: 102, At: jul, BalanceQuota: 2500000, UsedQuota: 3000000, Source: "logs#july"},
				{UserID: 102, At: aug, BalanceQuota: -1750000, UsedQuota: 6750000, Source: "logs#august"},
				{UserID: 102, At: sep, BalanceQuota: -1750000 + 909785, UsedQuota: 6750000 - 909785, Source: "logs#september"},
			},
		}},
	}
}

func prepareTest(t *testing.T, input MonthlyPreparationInput) MonthlyPreparationReport {
	t.Helper()
	raw, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	r, err := PrepareMonthlyReconciliation(raw, time.Unix(preparationAt("2026-10-20"), 0))
	if err != nil {
		t.Fatal(err)
	}
	if r.Mode != "local_reconciliation_draft_not_publication" || r.SourceIntegrity != "not_checked_by_pure_calculator" {
		t.Fatal("unexpected publication or source attestation", r)
	}
	return r
}

func TestMonthlyPreparationEarlierMonthsIgnoreLaterGap(t *testing.T) {
	r := prepareTest(t, preparationFixture()).Accounts[0]
	if r.Error != "" {
		t.Fatal(r.Error)
	}
	if r.Months[0].Status != "reconciled_draft" || r.Months[1].Status != "reconciled_draft" || r.Months[2].Status != "no_matching_balance_anchor" {
		t.Fatalf("unexpected monthly outcomes: %+v", r.Months)
	}
	want := []MonthlyPreparationAmounts{
		{Opening: "1.000000", Increase: "10.000000", Decrease: "0.000000", Consume: "6.000000", Refund: "0.000000", Closing: "5.000000"},
		{Opening: "5.000000", Increase: "0.000000", Decrease: "-1.000000", Consume: "8.000000", Refund: "0.500000", Closing: "-3.500000"},
	}
	for i := range want {
		if r.Months[i].Amounts == nil || *r.Months[i].Amounts != want[i] {
			t.Fatalf("month %d: got %+v", i, r.Months[i])
		}
	}
	if r.Months[2].Amounts != nil || len(r.Months[2].Sources) == 0 || r.Anchors[2].BalanceGapQuota != "909785" || r.Anchors[2].CounterGapQuota != "-909785" {
		t.Fatal("gap fabricated, hidden or sources lost", r)
	}
	if r.Months[0].AnchorAt != preparationAt("2026-07-01") {
		t.Fatal("not the earliest matching anchor")
	}
}

func TestMonthlyPreparationPostMonthInteriorAnchor(t *testing.T) {
	fixture := func() MonthlyPreparationInput {
		input := preparationFixture()
		a := &input.Accounts[0]
		aug, sep := preparationAt("2026-08-01"), preparationAt("2026-09-01")
		cutoff := preparationAt("2026-08-15") + 11*3600 + 35
		a.Months = []string{"2026-07"}
		a.Spans = append(a.Spans[:2],
			MonthlyPreparationSpan{UserID: a.UserID, From: aug, To: cutoff, ConsumeQuota: 1000000, RefundQuota: 100000, Requests: preparationInt(4), Tokens: preparationInt(40), Source: "logs#partial-day"},
			MonthlyPreparationSpan{UserID: a.UserID, From: cutoff, To: sep, ConsumeQuota: 2000000, Requests: preparationInt(5), Tokens: preparationInt(50), Source: "logs#remaining-day-and-month"},
		)
		// The source snapshot excludes the movement at its exact cutoff. That
		// later credit must not be pulled back into the historical month.
		a.Movements = append(a.Movements, MonthlyPreparationMovement{UserID: a.UserID, ID: 3, At: cutoff, DeltaQuota: 7000000, Source: "logs#cutoff-credit"})
		a.Snapshots = []MonthlyPreparationBalance{
			{UserID: a.UserID, At: cutoff, BalanceQuota: -2650000, UsedQuota: 7650000, Source: "logs#interior-snapshot"},
			{UserID: a.UserID, At: sep, BalanceQuota: 2350000 + 909785, UsedQuota: 9650000 - 909785, Source: "logs#later-active-snapshot"},
		}
		return input
	}
	t.Run("exact bridge excludes later movements and discrepancy", func(t *testing.T) {
		input := fixture()
		r := prepareTest(t, input).Accounts[0]
		if r.Error != "" || len(r.Anchors) != 2 || r.Anchors[0].Status != "matched" || r.Anchors[1].Status != "mismatch" {
			t.Fatalf("unexpected anchor checks: %+v", r)
		}
		row := r.Months[0]
		want := MonthlyPreparationAmounts{Opening: "5.000000", Increase: "0.000000", Decrease: "-1.000000", Consume: "8.000000", Refund: "0.500000", Closing: "-3.500000"}
		if row.Status != "reconciled_draft" || row.Amounts == nil || *row.Amounts != want || row.AnchorAt != input.Accounts[0].Snapshots[0].At {
			t.Fatalf("later discrepancy changed July: %+v", row)
		}
	})
	for name, change := range map[string]func(*MonthlyPreparationAccount){
		"missing bridge":        func(a *MonthlyPreparationAccount) { a.Spans = append(a.Spans[:2], a.Spans[3:]...) },
		"bridge crosses cutoff": func(a *MonthlyPreparationAccount) { a.Spans[2].To++ },
		"unknown bridge funding": func(a *MonthlyPreparationAccount) {
			a.MovementCoverage[0].To = preparationAt("2026-08-01")
			a.Movements = a.Movements[:2]
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := fixture()
			change(&input.Accounts[0])
			r := prepareTest(t, input).Accounts[0]
			if r.Error != "" || len(r.Months) != 1 {
				t.Fatalf("invalid test fixture: %+v", r)
			}
			row := r.Months[0]
			if row.Status != "no_matching_balance_anchor" || row.Amounts != nil || row.LogConsume != "8.000000" || row.LogRefund != "0.500000" {
				t.Fatalf("missing bridge guessed a balance or erased known logs: %+v", row)
			}
		})
	}
}

func TestMonthlyPreparationAccountIsolationAndDeterminism(t *testing.T) {
	input := preparationFixture()
	second := preparationFixture().Accounts[0]
	second.UserID, second.GroupID = 125, 4 // deliberately leave foreign rows in account 125
	input.Accounts = append(input.Accounts, second)
	r := prepareTest(t, input)
	if r.Accounts[1].Error == "" || r.Accounts[0].Months[0].Status != "reconciled_draft" {
		t.Fatal(r)
	}
	if !reflect.DeepEqual(r, prepareTest(t, input)) {
		t.Fatal("nondeterministic output")
	}
}

func TestMonthlyPreparationUnknownStatisticsNotInvented(t *testing.T) {
	input := preparationFixture()
	input.Accounts[0].Spans[1].Tokens = nil
	input.Accounts[0].Snapshots = input.Accounts[0].Snapshots[1:]
	r := prepareTest(t, input).Accounts[0]
	if r.Anchors[0].Status != "matched" || r.Months[0].Status != "reconciled_draft" {
		t.Fatal("unknown later token statistics broke earlier month money", r)
	}
	if r.Months[1].Tokens != nil || r.Months[1].Amounts == nil || r.Months[1].LogConsume != "8.000000" || r.Months[1].Status != "reconciled_draft" {
		t.Fatal("unknown token count became a genuine zero", r)
	}
}

func TestMonthlyPreparationKnownLogTotalSurvivesMissingFunding(t *testing.T) {
	input := preparationFixture()
	input.Accounts[0].Movements = []MonthlyPreparationMovement{}
	input.Accounts[0].MovementCoverage[0].From++
	r := prepareTest(t, input).Accounts[0].Months[0]
	if r.Status != "incomplete_sources" || r.Amounts != nil || r.LogConsume != "6.000000" || r.Requests == nil || *r.Requests != 20 || len(r.Sources) == 0 {
		t.Fatal("verified log totals disappeared with unrelated missing funding", r)
	}
}

func TestMonthlyPreparationSourceDefects(t *testing.T) {
	tests := map[string]func(*MonthlyPreparationAccount){
		"foreign consumption":      func(a *MonthlyPreparationAccount) { a.Spans[0].UserID = 125 },
		"foreign funding":          func(a *MonthlyPreparationAccount) { a.Movements[0].UserID = 125 },
		"foreign snapshot":         func(a *MonthlyPreparationAccount) { a.Snapshots[0].UserID = 125 },
		"duplicate funding":        func(a *MonthlyPreparationAccount) { a.Movements = append(a.Movements, a.Movements[0]) },
		"duplicate snapshot":       func(a *MonthlyPreparationAccount) { a.Snapshots = append(a.Snapshots, a.Snapshots[0]) },
		"duplicate month":          func(a *MonthlyPreparationAccount) { a.Months = append(a.Months, a.Months[0]) },
		"unknown source":           func(a *MonthlyPreparationAccount) { a.Spans[0].Source = "missing#query" },
		"unknown semantics":        func(a *MonthlyPreparationAccount) { a.CounterBasis = "gross_unknown" },
		"future snapshot":          func(a *MonthlyPreparationAccount) { a.Snapshots[0].At = preparationAt("2027-01-01") },
		"future source window":     func(a *MonthlyPreparationAccount) { a.Spans[2].To = preparationAt("2027-01-01") },
		"funding outside coverage": func(a *MonthlyPreparationAccount) { a.MovementCoverage[0].From += 100 },
		"overlap funding coverage": func(a *MonthlyPreparationAccount) {
			a.MovementCoverage = append(a.MovementCoverage, a.MovementCoverage[0])
		},
		"negative gross": func(a *MonthlyPreparationAccount) { a.Spans[0].ConsumeQuota = -1 },
		"negative stats": func(a *MonthlyPreparationAccount) { a.Spans[0].Tokens = preparationInt(-1) },
	}
	for name, change := range tests {
		t.Run(name, func(t *testing.T) {
			input := preparationFixture()
			change(&input.Accounts[0])
			if r := prepareTest(t, input).Accounts[0]; r.Error == "" {
				t.Fatal("bad account accepted", r)
			}
		})
	}
}

func TestMonthlyPreparationIncompleteWindowsNeverProrated(t *testing.T) {
	for name, change := range map[string]func(*MonthlyPreparationAccount){
		"gap":                    func(a *MonthlyPreparationAccount) { a.Spans[0].From++ },
		"overlap":                func(a *MonthlyPreparationAccount) { a.Spans = append(a.Spans, a.Spans[0]) },
		"nonempty crosses month": func(a *MonthlyPreparationAccount) { a.Spans[0].To++ },
		"amount overflow":        func(a *MonthlyPreparationAccount) { a.Baseline.BalanceQuota = math.MaxInt64 },
		"counter overflow":       func(a *MonthlyPreparationAccount) { a.Baseline.UsedQuota = math.MaxInt64 },
		"unsafe stats":           func(a *MonthlyPreparationAccount) { a.Spans[0].Tokens = preparationInt(9007199254740992) },
		"funding gap": func(a *MonthlyPreparationAccount) {
			a.Movements = []MonthlyPreparationMovement{}
			a.MovementCoverage[0].From++
		},
	} {
		t.Run(name, func(t *testing.T) {
			input := preparationFixture()
			change(&input.Accounts[0])
			r := prepareTest(t, input).Accounts[0]
			if len(r.Months) != 3 || r.Months[0].Amounts != nil {
				t.Fatal("bad month reconstructed", r)
			}
		})
	}
}

func TestMonthlyPreparationZeroWindowsAndSignedTinyBalance(t *testing.T) {
	input := preparationFixture()
	a := &input.Accounts[0]
	a.Spans = []MonthlyPreparationSpan{{UserID: a.UserID, From: a.Baseline.At, To: preparationAt("2026-09-01"), Requests: preparationInt(0), Tokens: preparationInt(0), Source: "logs#empty-range"}}
	a.Movements = []MonthlyPreparationMovement{}
	a.Baseline.BalanceQuota = -1
	a.Snapshots = []MonthlyPreparationBalance{{UserID: a.UserID, At: preparationAt("2026-09-01"), BalanceQuota: -1, Source: "logs#snapshot"}}
	r := prepareTest(t, input).Accounts[0]
	for _, row := range r.Months {
		if row.Status != "reconciled_draft" || row.Amounts.Closing != "-0.000002" || row.Amounts.Opening != "-0.000002" {
			t.Fatal(row)
		}
	}
}

func TestMonthlyPreparationMonthlyBoundaryAndInvalidMonths(t *testing.T) {
	input := preparationFixture()
	input.Accounts[0].Months = []string{"", "2026-7", "2026-10", "2026-11", "2019-01"}
	r := prepareTest(t, input).Accounts[0]
	for _, row := range r.Months {
		if row.Status != "invalid_month" {
			t.Fatal(row)
		}
	}
	if monthlyPreparationDifference(math.MaxInt64, math.MinInt64) != "18446744073709551615" {
		t.Fatal("diagnostic subtraction overflow")
	}
}

func TestMonthlyPreparationStrictJSON(t *testing.T) {
	base, err := json.Marshal(preparationFixture())
	if err != nil {
		t.Fatal(err)
	}
	tests := map[string]string{
		"missing balance":  strings.Replace(string(base), `"balance_quota":500000,`, "", 1),
		"null balance":     strings.Replace(string(base), `"balance_quota":500000`, `"balance_quota":null`, 1),
		"missing stat":     strings.Replace(string(base), `"tokens":200,`, "", 1),
		"wrong case":       strings.Replace(string(base), `"balance_quota"`, `"BALANCE_QUOTA"`, 1),
		"duplicate key":    strings.Replace(string(base), `"version":1`, `"version":1,"version":1`, 1),
		"extra field":      strings.Replace(string(base), `"version":1`, `"apply":true,"version":1`, 1),
		"extra object":     string(base) + `{}`,
		"fractional quota": strings.Replace(string(base), `"balance_quota":500000`, `"balance_quota":500000.5`, 1),
	}
	for name, raw := range tests {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeMonthlyPreparationInput([]byte(raw)); err == nil {
				t.Fatal("malformed input accepted")
			}
		})
	}
}

func TestMonthlyPreparationTransferRemainsPerAccount(t *testing.T) {
	input := preparationFixture()
	input.Accounts = nil
	for i, id := range []int64{122, 125} {
		a := preparationFixture().Accounts[0]
		a.UserID, a.GroupID, a.Months = id, 4, []string{"2026-08"}
		a.Baseline = MonthlyPreparationBalance{UserID: id, At: preparationAt("2026-08-01"), BalanceQuota: 500000000, Source: "logs#opening"}
		a.Spans = []MonthlyPreparationSpan{{UserID: id, From: a.Baseline.At, To: preparationAt("2026-09-01"), ConsumeQuota: 1, Requests: preparationInt(1), Tokens: preparationInt(10), Source: "logs#consumption"}}
		delta := int64(-400000000)
		if i == 1 {
			delta = -delta
		}
		a.Movements = []MonthlyPreparationMovement{{UserID: id, ID: id, At: preparationAt("2026-08-25"), DeltaQuota: delta, Source: "logs#transfer-target"}}
		a.MovementCoverage = []MonthlyPreparationCoverage{{UserID: id, From: a.Baseline.At, To: preparationAt("2026-09-01"), Source: "logs#all-money-ops"}}
		a.Snapshots = []MonthlyPreparationBalance{{UserID: id, At: preparationAt("2026-09-01"), BalanceQuota: 500000000 + delta - 1, UsedQuota: 1, Source: "logs#closing"}}
		input.Accounts = append(input.Accounts, a)
	}
	r := prepareTest(t, input)
	if r.Accounts[0].Months[0].Amounts.Decrease != "-800.000000" || r.Accounts[1].Months[0].Amounts.Increase != "800.000000" {
		t.Fatal("transfer was netted away", r)
	}
	input.Accounts[1].Snapshots[0].BalanceQuota++
	r = prepareTest(t, input)
	if r.Accounts[0].Months[0].Status != "reconciled_draft" || r.Accounts[1].Months[0].Amounts != nil {
		t.Fatal("cross account balance netting", r)
	}
}
