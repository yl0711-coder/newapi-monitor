package monitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
)

func TestMonthlyCoverageIssueExactRanges(t *testing.T) {
	jun, jul := preparationAt("2026-06-01"), preparationAt("2026-07-01")
	for _, tc := range []struct {
		name   string
		change func(*MonthlyPreparationAccount)
		want   MonthlyPreparationCoverageIssue
	}{
		{"usage head", func(a *MonthlyPreparationAccount) { a.Spans[0].From++ }, MonthlyPreparationCoverageIssue{Kind: "usage_gap", From: jun, To: jun + 1}},
		{"usage tail", func(a *MonthlyPreparationAccount) { a.Spans[0].To-- }, MonthlyPreparationCoverageIssue{Kind: "usage_gap", From: jul - 1, To: jul}},
		{"usage overlap", func(a *MonthlyPreparationAccount) {
			duplicate := a.Spans[0]
			duplicate.From, duplicate.To = jun+5, jun+10
			a.Spans = append(a.Spans[:1], duplicate)
		}, MonthlyPreparationCoverageIssue{Kind: "usage_overlap", From: jun + 5, To: jun + 10}},
		{"usage split end", func(a *MonthlyPreparationAccount) { a.Spans[0].To++ }, MonthlyPreparationCoverageIssue{Kind: "usage_window_requires_split", From: jun, To: jul, WindowFrom: jun, WindowTo: jul + 1, Source: "logs#range"}},
		{"funding head", func(a *MonthlyPreparationAccount) { a.MovementCoverage[0].From++ }, MonthlyPreparationCoverageIssue{Kind: "funding_gap", From: jun, To: jun + 1}},
		{"funding tail", func(a *MonthlyPreparationAccount) { a.MovementCoverage[0].To = jul - 1 }, MonthlyPreparationCoverageIssue{Kind: "funding_gap", From: jul - 1, To: jul}},
		{"funding overlap", func(a *MonthlyPreparationAccount) {
			a.MovementCoverage = []MonthlyPreparationCoverage{{From: jun, To: jul}, {From: jun + 5, To: jun + 10}}
		}, MonthlyPreparationCoverageIssue{Kind: "funding_overlap", From: jun + 5, To: jun + 10}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			a := preparationFixture().Accounts[0]
			tc.change(&a)
			_, err := monthlyPreparationRange(a, jun, jul)
			var coverageErr *monthlyPreparationCoverageError
			if !errors.As(err, &coverageErr) || coverageErr.issue != tc.want {
				t.Fatalf("wrong exact first defect: got %#v / %v, want %#v", coverageErr, err, tc.want)
			}
			if tc.want.Kind == "usage_window_requires_split" && !strings.Contains(err.Error(), strconv.FormatInt(jul, 10)) {
				t.Fatal("reported window start instead of actual crossed end boundary", err)
			}
		})
	}
	// Clipping a nonempty aggregate at its beginning is also a split, not a
	// missing interval or a proportional fraction of its original money.
	a := preparationFixture().Accounts[0]
	_, err := monthlyPreparationRange(a, jun+1, jul)
	var split *monthlyPreparationCoverageError
	want := MonthlyPreparationCoverageIssue{Kind: "usage_window_requires_split", From: jun + 1, To: jul, WindowFrom: jun, WindowTo: jul, Source: "logs#range"}
	if !errors.As(err, &split) || split.issue != want {
		t.Fatal("start clipping lost original window", err)
	}
}

func TestMonthlyCoverageReportKeepsEarlierMoneyAndObservationSemantics(t *testing.T) {
	input := preparationFixture()
	a := &input.Accounts[0]
	jul, aug := preparationAt("2026-07-01"), preparationAt("2026-08-01")
	a.Months = []string{"2026-06"}
	a.Spans, a.MovementCoverage[0].To = a.Spans[:1], jul
	a.Movements, a.Snapshots = a.Movements[:1], a.Snapshots[:1]
	before := prepareTest(t, input).Accounts[0].Months[0]
	a.Snapshots = append(a.Snapshots, MonthlyPreparationBalance{UserID: a.UserID, At: aug, BalanceQuota: 2500000, UsedQuota: 3000000, Source: "logs#new-sampling", TimeBasis: monthlyCollectionObservation})
	r := prepareTest(t, input).Accounts[0]
	if r.Error != "" || len(r.Anchors) != 2 || !reflect.DeepEqual(before, r.Months[0]) {
		t.Fatal("new incomplete observation poisoned an already matched month", r)
	}
	check := r.Anchors[1]
	want := MonthlyPreparationCoverageIssue{Kind: "usage_gap", From: jul, To: aug}
	if check.Status != "incomplete_sources" || check.TimeBasis != monthlyCollectionObservation || check.CoverageIssue == nil || *check.CoverageIssue != want || check.BalanceGapQuota != "" || check.CounterGapQuota != "" {
		t.Fatal("coverage or timing ambiguity hidden, or missing money invented", check)
	}
	// Fill only SYNTHETIC zero-activity evidence. A now-exact arithmetic match
	// still cannot promote the observation to a source cutoff or month boundary.
	a.Spans = append(a.Spans, MonthlyPreparationSpan{UserID: a.UserID, From: jul, To: aug, Requests: preparationInt(0), Tokens: preparationInt(0), Source: "logs#zero-activity-query"})
	a.MovementCoverage[0].To = aug
	r = prepareTest(t, input).Accounts[0]
	check = r.Anchors[1]
	if check.Status != "observation_only" || check.CoverageIssue != nil || check.TimeBasis != monthlyCollectionObservation || check.BalanceGapQuota != "0" || check.CounterGapQuota != "0" || !reflect.DeepEqual(before, r.Months[0]) {
		t.Fatal("coverage became publication authority or changed earlier money", r)
	}
	// Do not change that conclusion if only the baseline is a collection view.
	a.Baseline.TimeBasis, a.Snapshots[1].TimeBasis = monthlyCollectionObservation, monthlySourceLogCutoff
	a.Snapshots[1].CutoffSource = "logs#reviewed-later-cutoff"
	r = prepareTest(t, input).Accounts[0]
	if r.Anchors[1].TimeBasis != monthlyCollectionObservation || r.Anchors[1].Status != "observation_only" {
		t.Fatal("later reviewed cutoff erased unreviewed baseline", r)
	}
}

func TestMonthlyCoverageDiagnosticsPreserveLegacyBytesAndNonCoverageErrors(t *testing.T) {
	old := MonthlyPreparationAnchorCheck{At: 123, Source: "logs#view", Status: "matched", BalanceGapQuota: "0", CounterGapQuota: "0"}
	raw, err := json.Marshal(old)
	want := []byte(`{"at":123,"source":"logs#view","status":"matched","balance_gap_quota":"0","counter_gap_quota":"0"}`)
	if err != nil || !bytes.Equal(raw, want) {
		t.Fatal("optional diagnostics changed existing complete report bytes", string(raw), err)
	}
	input := preparationFixture()
	input.Accounts[0].Baseline.BalanceQuota = math.MaxInt64
	r := prepareTest(t, input).Accounts[0]
	if r.Anchors[0].Status != "incomplete_sources" || r.Anchors[0].Reason == "" || r.Anchors[0].CoverageIssue != nil {
		t.Fatal("numeric overflow mislabeled as a recoverable missing source", r.Anchors[0])
	}
	input = preparationFixture()
	input.Accounts[0].Spans[0].ConsumeQuota = 0
	input.Accounts[0].Spans[0].Requests = nil
	input.Accounts[0].Spans[0].Tokens = preparationInt(0)
	input.Accounts[0].Spans[0].To++
	if check := prepareTest(t, input).Accounts[0].Anchors[0]; check.CoverageIssue == nil || check.CoverageIssue.Kind != "usage_window_requires_split" {
		t.Fatal("unknown statistics treated as an empty divisible window", check)
	}
}
