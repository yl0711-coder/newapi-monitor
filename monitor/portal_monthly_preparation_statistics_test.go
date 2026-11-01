package monitor

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
	"time"
)

func monthlyStatisticsFixture() (MonthlyPreparationInput, MonthlyBundleContext) {
	i, c := monthlyForwardReconstructionFixture()
	for j := range i.Accounts[0].Spans {
		i.Accounts[0].Spans[j].Tokens = nil
	}
	i.Accounts[0].MonthStatistics = []MonthlyPreparationStatistics{{UserID: i.Accounts[0].UserID, Month: "2026-06", Requests: 30, Tokens: 300, Source: "logs#independent-full-month-statistics"}}
	return i, c
}

func TestMonthlyPreparationStatisticsPreserveKnownCountsNotMoney(t *testing.T) {
	i, c := monthlyStatisticsFixture()
	before, _ := json.Marshal(i)
	with := monthlyBundleRun(t, i, c)
	if with.ReadyForReview != 1 || with.Blocked != 0 {
		t.Fatalf("source-backed month statistics rejected: %+v", with.Entries)
	}
	u := with.Entries[0].Candidate.Users[0]
	if u.Requests != 30 || u.Tokens != 300 || u.Statistics != nil {
		t.Fatal("known counts were lost", u.Statistics)
	}
	i.Accounts[0].MonthStatistics = nil
	without := monthlyBundleRun(t, i, c)
	old := without.Entries[0].Candidate.Users[0]
	if old.Statistics == nil || old.Statistics.TokensKnown || old.Tokens != 0 {
		t.Fatal("absent source was relabeled as known zero")
	}
	u.Requests, u.Tokens, u.Statistics = old.Requests, old.Tokens, old.Statistics
	// The whole input/context fingerprint changes, not the original money.
	u.Accounting.ReconciliationSource, old.Accounting.ReconciliationSource = "", ""
	u.Accounting.MovementSource, old.Accounting.MovementSource = "", ""
	if !reflect.DeepEqual(u, old) {
		t.Fatal("statistics changed funding, refunds, balances or provenance", u, old)
	}
	i.Accounts[0].MonthStatistics = []MonthlyPreparationStatistics{{UserID: i.Accounts[0].UserID, Month: "2026-06", Requests: 30, Tokens: 300, Source: "logs#independent-full-month-statistics"}}
	after, _ := json.Marshal(i)
	if !bytes.Equal(before, after) {
		t.Fatal("original input was mutated")
	}
}

func TestMonthlyPreparationStatisticsOrdinaryAndReconstructedAgree(t *testing.T) {
	i := preparationFixture()
	i.Accounts[0].Months = []string{"2026-06"}
	i.Accounts[0].Spans[0].Tokens = nil
	i.Accounts[0].MonthStatistics = []MonthlyPreparationStatistics{{UserID: 102, Month: "2026-06", Requests: 20, Tokens: 200, Source: "logs#independent-full-june-statistics"}}
	report := prepareTest(t, i)
	row := report.Accounts[0].Months[0]
	if row.Status != "reconciled_draft" || row.Requests == nil || *row.Requests != 20 || row.Tokens == nil || *row.Tokens != 200 {
		t.Fatal("ordinary report lost independently known counts", row)
	}
	b := monthlyBundleRun(t, i, monthlyBundleFixture(i))
	if b.ReadyForReview != 1 || b.Entries[0].Candidate.Users[0].Tokens != 200 {
		t.Fatal("ordinary bill did not retain statistics", b.Entries)
	}
}

func TestMonthlyPreparationStatisticsRejectConflictAndIncompleteMoney(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyPreparationInput){
		"wrong account":  func(i *MonthlyPreparationInput) { i.Accounts[0].MonthStatistics[0].UserID++ },
		"wrong month":    func(i *MonthlyPreparationInput) { i.Accounts[0].MonthStatistics[0].Month = "2026-05" },
		"missing source": func(i *MonthlyPreparationInput) { i.Accounts[0].MonthStatistics[0].Source = "missing#month" },
		"duplicate month": func(i *MonthlyPreparationInput) {
			a := &i.Accounts[0]
			a.MonthStatistics = append(a.MonthStatistics, a.MonthStatistics[0])
		},
		"negative tokens":         func(i *MonthlyPreparationInput) { i.Accounts[0].MonthStatistics[0].Tokens = -1 },
		"unsafe integer":          func(i *MonthlyPreparationInput) { i.Accounts[0].MonthStatistics[0].Tokens = 9007199254740992 },
		"known requests mismatch": func(i *MonthlyPreparationInput) { i.Accounts[0].MonthStatistics[0].Requests++ },
		"known tokens mismatch": func(i *MonthlyPreparationInput) {
			i.Accounts[0].Spans[0].Tokens = preparationInt(99)
			i.Accounts[0].Spans[1].Tokens = preparationInt(200)
		},
		"missing usage tail":    func(i *MonthlyPreparationInput) { i.Accounts[0].Spans = i.Accounts[0].Spans[:1] },
		"missing funding tail":  func(i *MonthlyPreparationInput) { i.Accounts[0].MovementCoverage[0].To = i.Accounts[0].Snapshots[0].At },
		"wallet does not match": func(i *MonthlyPreparationInput) { i.Accounts[0].Snapshots[0].BalanceQuota++ },
	} {
		t.Run(name, func(t *testing.T) {
			i, c := monthlyStatisticsFixture()
			mutate(&i)
			raw, meta := monthlyBundleRaw(t, i, c)
			b, err := PrepareMonthlyCandidateBundle(raw, meta, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && b.ReadyForReview != 0 {
				t.Fatal("invalid counts or incomplete money accepted", b.Entries)
			}
		})
	}
}

func TestMonthlyPreparationStatisticsJSONMissingIsNotZero(t *testing.T) {
	i, _ := monthlyStatisticsFixture()
	raw, err := json.Marshal(i)
	if err != nil {
		t.Fatal(err)
	}
	for name, mutate := range map[string]func([]byte) []byte{
		"null array": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"month_statistics":[`), []byte(`"month_statistics":null,"unused":[`), 1)
		},
		"array alias": func(b []byte) []byte {
			return bytes.Replace(b, []byte(`"month_statistics"`), []byte(`"Month_Statistics"`), 1)
		},
		"missing tokens": func(b []byte) []byte { return bytes.Replace(b, []byte(`"tokens":300,`), nil, 1) },
		"null tokens":    func(b []byte) []byte { return bytes.Replace(b, []byte(`"tokens":300`), []byte(`"tokens":null`), 1) },
		"tokens alias":   func(b []byte) []byte { return bytes.Replace(b, []byte(`"tokens":300`), []byte(`"Tokens":300`), 1) },
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := DecodeMonthlyPreparationInput(mutate(bytes.Clone(raw))); err == nil {
				t.Fatal("missing/null/aliased counts accepted as a genuine zero")
			}
		})
	}
}

func TestMonthlyPreparationStatisticsOmittedKeepsLegacyBytes(t *testing.T) {
	i := preparationFixture()
	raw, err := json.Marshal(i)
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("month_statistics")) {
		t.Fatal("optional field changed legacy input")
	}
	decoded, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	again, err := json.Marshal(decoded)
	if err != nil || !bytes.Equal(raw, again) {
		t.Fatal("legacy input identity changed", err)
	}
}
