package monitor

import (
	"bytes"
	"reflect"
	"testing"
	"time"
)

func resolutionFixture(t *testing.T) MonthlyPreparationInput {
	t.Helper()
	raw, _ := historicalPreviewFixture(t)
	x, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	x.Accounts[0].Months = []string{"2026-08"}
	return x
}

func resolutionLaterAnchor(a *MonthlyPreparationAccount, change int64) {
	from, to := preparationAt("2026-09-01"), preparationAt("2026-10-01")
	a.Spans = append(a.Spans, MonthlyPreparationSpan{UserID: a.UserID, From: from, To: to, Source: "logs#empty-later-month"})
	a.MovementCoverage[0].To = to
	b := a.Snapshots[len(a.Snapshots)-1]
	b.At, b.Source = to, "logs#later-reviewed"
	b.BalanceQuota += change
	b.UsedQuota -= change
	a.Snapshots = append(a.Snapshots, b)
}

func TestMonthlyResolutionPhenomenaHaveDistinctActions(t *testing.T) {
	for _, tc := range []struct {
		name, code string
		mutate     func(*MonthlyPreparationAccount)
	}{
		{"one residual", "confirm_residual_with_more_evidence", func(*MonthlyPreparationAccount) {}},
		{"timing unknown", "verify_cutoff_evidence", func(a *MonthlyPreparationAccount) {
			a.Snapshots[2].TimeBasis, a.Snapshots[2].CutoffSource = monthlyCollectionObservation, ""
		}},
		{"usage gap", "complete_usage_coverage", func(a *MonthlyPreparationAccount) { a.Spans = a.Spans[:2] }},
		{"funding gap", "complete_funding_or_bridge", func(a *MonthlyPreparationAccount) { a.MovementCoverage[0].To-- }},
		{"counter conflict", "balance_counter_conflict", func(a *MonthlyPreparationAccount) { a.Snapshots[2].UsedQuota++ }},
		{"possible overcharge", "debit_above_logs_review", func(a *MonthlyPreparationAccount) {
			a.Snapshots[2].BalanceQuota -= 2000000
			a.Snapshots[2].UsedQuota += 2000000
		}},
		{"persistent residual candidate", "historical_adjustment_review", func(a *MonthlyPreparationAccount) { resolutionLaterAnchor(a, 0) }},
		{"varying residual", "varying_residual_review", func(a *MonthlyPreparationAccount) { resolutionLaterAnchor(a, 1) }},
		{"bad input", "repair_input", func(a *MonthlyPreparationAccount) { a.Baseline.Source = "missing#source" }},
		{"ready", "ready_for_review", func(a *MonthlyPreparationAccount) { a.Months = []string{"2026-06"} }},
		{"legacy draft", "verify_legacy_cutoff", func(a *MonthlyPreparationAccount) {
			a.Months = []string{"2026-06"}
			a.Baseline.TimeBasis, a.Baseline.CutoffSource = "", ""
		}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			x := resolutionFixture(t)
			tc.mutate(&x.Accounts[0])
			raw := monthlyReviewJSON(t, x)
			before := bytes.Clone(raw)
			now := time.Unix(preparationAt("2026-10-22"), 0)
			p, err := PlanMonthlyResolution(raw, now)
			if err != nil || len(p.Cases) != 1 || p.Cases[0].Code != tc.code || p.Cases[0].NextAction == "" {
				t.Fatal(p, err)
			}
			again, err := PlanMonthlyResolution(raw, now)
			if err != nil || !reflect.DeepEqual(p, again) || !bytes.Equal(raw, before) {
				t.Fatal("not stable or changed input", err)
			}
			if _, err := decodeMonthlyBillEvidence(monthlyReviewJSON(t, p)); err == nil {
				t.Fatal("internal plan became publication evidence")
			}
		})
	}
}

func TestMonthlyResolutionIndependentOfAccountIdentityAndBatchIsolation(t *testing.T) {
	x := resolutionFixture(t)
	template := x.Accounts[0]
	x.Accounts = nil
	for i := int64(1); i <= 200; i++ {
		// Decode a copy so no test account shares mutable slices with another.
		copyInput, err := DecodeMonthlyPreparationInput(monthlyReviewJSON(t, MonthlyPreparationInput{Version: 1, Sources: x.Sources, Accounts: []MonthlyPreparationAccount{template}}))
		if err != nil {
			t.Fatal(err)
		}
		a := copyInput.Accounts[0]
		a.UserID, a.GroupID, a.Username = 10000+i, 1+i%17, "arbitrary-new-account"
		a.Baseline.UserID = a.UserID
		for j := range a.Spans {
			a.Spans[j].UserID = a.UserID
		}
		for j := range a.Movements {
			a.Movements[j].UserID = a.UserID
		}
		for j := range a.MovementCoverage {
			a.MovementCoverage[j].UserID = a.UserID
		}
		for j := range a.Snapshots {
			a.Snapshots[j].UserID = a.UserID
		}
		if i == 1 {
			a.Spans = []MonthlyPreparationSpan{}
		}
		x.Accounts = append(x.Accounts, a)
	}
	p, err := PlanMonthlyResolution(monthlyReviewJSON(t, x), time.Unix(preparationAt("2026-10-22"), 0))
	if err != nil || len(p.Cases) != 200 {
		t.Fatal(p, err)
	}
	for i, c := range p.Cases {
		want := "confirm_residual_with_more_evidence"
		if i == 0 {
			want = "complete_usage_coverage"
		}
		if c.Code != want || c.UserID != x.Accounts[i].UserID || c.GroupID != x.Accounts[i].GroupID {
			t.Fatal("identity-specific rule or cross-account contamination", c)
		}
	}
}

func TestMonthlyResolutionWalletPathDoesNotHideEvidenceFailures(t *testing.T) {
	for _, tc := range []struct {
		code   string
		change func(*MonthlyPreparationAccount)
	}{
		{"ready_for_review", func(*MonthlyPreparationAccount) {}},
		{"verify_exact_wallet_boundaries", func(a *MonthlyPreparationAccount) {
			a.Snapshots[0].TimeBasis, a.Snapshots[0].CutoffSource = monthlyCollectionObservation, ""
		}},
		{"balance_counter_conflict", func(a *MonthlyPreparationAccount) { a.Snapshots[0].UsedQuota++ }},
		{"debit_above_logs_review", func(a *MonthlyPreparationAccount) {
			a.Snapshots[0].BalanceQuota -= 100000
			a.Snapshots[0].UsedQuota += 100000
		}},
	} {
		x := monthlyWalletFixture()
		tc.change(&x.Accounts[0])
		p, err := PlanMonthlyResolution(monthlyReviewJSON(t, x), time.Unix(preparationAt("2026-10-22"), 0))
		if err != nil || len(p.Cases) != 1 || p.Cases[0].Code != tc.code {
			t.Fatal(tc.code, p, err)
		}
	}
}

func TestMonthlyHistoricalPreviewIgnoresUnreviewedObservationsNotConflicts(t *testing.T) {
	raw, request := historicalPreviewFixture(t)
	x, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Unix(preparationAt("2026-10-22"), 0)
	before, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, now)
	if err != nil {
		t.Fatal(err)
	}
	b := x.Accounts[0].Snapshots[0]
	b.At++
	b.BalanceQuota += 777777
	b.TimeBasis, b.CutoffSource = monthlyCollectionObservation, ""
	x.Accounts[0].Snapshots = append(x.Accounts[0].Snapshots, b)
	raw = monthlyReviewJSON(t, x)
	request.InputSHA256 = monthlyDigest(raw)
	after, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, now)
	if err != nil || after.Status != "proposal_requires_internal_review" || len(after.ExcludedObservations) != 1 || !reflect.DeepEqual(before.Months, after.Months) {
		t.Fatal("irrelevant collection observation blocked reviewed evidence", after, err)
	}
	// A conflicting REVIEWED cutoff must still stop allocation.
	x.Accounts[0].Snapshots = x.Accounts[0].Snapshots[:3]
	x.Accounts[0].Snapshots[0].BalanceQuota++
	x.Accounts[0].Snapshots[0].UsedQuota--
	raw = monthlyReviewJSON(t, x)
	request.InputSHA256 = monthlyDigest(raw)
	conflict, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, now)
	if err != nil || conflict.Status != "blocked" || len(conflict.Months) != 0 {
		t.Fatal(conflict, err)
	}
}
