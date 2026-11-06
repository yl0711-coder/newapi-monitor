package monitor

import (
	"encoding/json"
	"os"
	"reflect"
	"testing"
	"time"
)

// Optional retained-data regression; inputs are read only and never refreshed
// from production. Legacy parity checks arithmetic, not historical timing proof.
func TestMonthlyPreparationRetainedCutoffRegression(t *testing.T) {
	path, reportPath := os.Getenv("MONTHLY_CUTOFF_INPUT"), os.Getenv("MONTHLY_CUTOFF_REPORT")
	if path == "" || reportPath == "" {
		t.Skip("retained local cutoff evidence not supplied")
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	prior, err := os.ReadFile(reportPath)
	if err != nil {
		t.Fatal(err)
	}
	var expected MonthlyPreparationReport
	if err := json.Unmarshal(prior, &expected); err != nil {
		t.Fatal(err)
	}
	now := time.Unix(preparationAt("2026-10-22"), 0)
	actual, err := PrepareMonthlyReconciliation(raw, now)
	if err != nil || !reflect.DeepEqual(actual.Accounts, expected.Accounts) {
		t.Fatal("retained draft arithmetic changed", err)
	}
	matched := 0
	for _, a := range actual.Accounts {
		for _, row := range a.Months {
			if row.Status == "reconciled_draft" {
				matched++
			}
		}
	}
	t.Logf("unchanged retained draft account-months: %d", matched)
	if matched != 38 {
		t.Fatal("unexpected retained acceptance scope", matched)
	}
	for _, gap := range []int64{909785, 1200521} {
		p, err := PrepareMonthlyHistoricalSettlementPreview(raw, MonthlyHistoricalSettlementRequest{
			InputSHA256: monthlyDigest(raw), UserID: 102, AnchorAt: 1791514835, SettlementMonth: "2026-09", ExpectedGapQuota: gap,
		}, now)
		if err != nil || p.Status != "blocked" || len(p.Months) != 0 || p.ObservedBalanceGap != "1200521" {
			t.Fatal("retained sampling residual became monthly debit", p, err)
		}
	}
	input, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Only the known v1.18.16 snapshot collection timestamp is classified here.
	// Do not relabel other observations as reviewed cutoffs without evidence.
	for i := range input.Accounts {
		for j := range input.Accounts[i].Snapshots {
			b := &input.Accounts[i].Snapshots[j]
			if b.At == 1791514835 {
				b.TimeBasis = monthlyCollectionObservation
			}
		}
	}
	classified, err := PrepareMonthlyReconciliation(monthlyReviewJSON(t, input), now)
	if err != nil {
		t.Fatal(err)
	}
	changed := 0
	for i, a := range classified.Accounts {
		for j, row := range a.Months {
			old := actual.Accounts[i].Months[j]
			if !reflect.DeepEqual(old, row) {
				changed++
				t.Logf("timing evidence reclassified: account=%d month=%s %s -> %s", a.UserID, row.Month, old.Status, row.Status)
				if a.UserID != 152 || row.Month != "2026-09" || old.AnchorAt != 1791514835 || row.Status != "no_matching_balance_anchor" || row.Amounts != nil || old.LogConsume != row.LogConsume {
					t.Fatal("classification changed unrelated amounts or hid logs", old, row)
				}
			}
		}
	}
	if changed != 1 {
		t.Fatal("known observation-dependent month was not reclassified", changed)
	}
}

func TestMonthlyPreparationCollectionObservationCannotCertifyMonth(t *testing.T) {
	input := preparationFixture()
	a := &input.Accounts[0]
	for i := range a.Snapshots {
		a.Snapshots[i].TimeBasis = monthlyCollectionObservation
	}
	r := prepareTest(t, input).Accounts[0]
	if r.Error != "" || r.Anchors[0].Status != "observation_only" || r.Anchors[0].BalanceGapQuota != "0" || r.Anchors[2].BalanceGapQuota != "909785" {
		t.Fatal("lost diagnostic arithmetic or certified collection time", r)
	}
	for _, row := range r.Months {
		if row.Amounts != nil || row.Status != "no_matching_balance_anchor" || row.LogConsume == "" {
			t.Fatal("observation became monthly evidence or hid logs", row)
		}
	}
	// A bad later observation must not invalidate independently matched drafts.
	input = preparationFixture()
	before := prepareTest(t, input)
	input.Accounts[0].Snapshots[2].TimeBasis = monthlyCollectionObservation
	after := prepareTest(t, input)
	if !reflect.DeepEqual(before.Accounts[0].Months, after.Accounts[0].Months) {
		t.Fatal("unrelated monthly calculations changed")
	}
}

func TestMonthlyPreparationCutoffMetadataValidation(t *testing.T) {
	for _, tc := range []struct{ basis, source string }{
		{"typo", ""}, {monthlySourceLogCutoff, ""}, {monthlySourceLogCutoff, "missing#cutoff"},
		{monthlyCollectionObservation, "logs#cutoff"}, {"", "logs#cutoff"},
	} {
		input := preparationFixture()
		input.Accounts[0].Snapshots[0].TimeBasis = tc.basis
		input.Accounts[0].Snapshots[0].CutoffSource = tc.source
		r := prepareTest(t, input).Accounts[0]
		if r.Error == "" || len(r.Months) != 0 {
			t.Fatal("invalid cutoff metadata accepted", tc, r)
		}
	}
}

func TestMonthlyWalletRequiresReviewedCutoffsEvenAtMidnight(t *testing.T) {
	for _, endpoint := range []string{"opening", "closing"} {
		for _, basis := range []string{"", monthlyCollectionObservation} {
			input := monthlyWalletFixture()
			b := &input.Accounts[0].Baseline
			if endpoint == "closing" {
				b = &input.Accounts[0].Snapshots[0]
			}
			b.TimeBasis, b.CutoffSource = basis, ""
			r := prepareTest(t, input).Accounts[0]
			if r.Error != "" || r.Months[0].Status != "incomplete_wallet_sources" || r.Months[0].Amounts != nil {
				t.Fatal("wall clock midnight treated as settled boundary", endpoint, basis, r)
			}
		}
	}
}

func TestMonthlyHistoricalPreviewRequiresReviewedCutoffs(t *testing.T) {
	for _, endpoint := range []string{"baseline", "selected"} {
		for _, basis := range []string{"", monthlyCollectionObservation} {
			raw, request := historicalPreviewFixture(t)
			input, err := DecodeMonthlyPreparationInput(raw)
			if err != nil {
				t.Fatal(err)
			}
			b := &input.Accounts[0].Baseline
			if endpoint == "selected" {
				b = &input.Accounts[0].Snapshots[2]
			}
			b.TimeBasis, b.CutoffSource = basis, ""
			raw = monthlyReviewJSON(t, input)
			request.InputSHA256 = monthlyDigest(raw)
			p, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, time.Unix(preparationAt("2026-10-20"), 0))
			if err != nil || p.Status != "blocked" || len(p.Months) != 0 || p.Reason == "" {
				t.Fatal("unverified timing allocated into historical consumption", endpoint, basis, p, err)
			}
		}
	}
}
