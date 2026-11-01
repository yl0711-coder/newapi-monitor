package monitor

import (
	"bytes"
	"reflect"
	"strings"
	"testing"
	"time"
)

func historicalPreviewFixture(t *testing.T) ([]byte, MonthlyHistoricalSettlementRequest) {
	t.Helper()
	input := preparationFixture()
	a := &input.Accounts[0]
	a.Baseline.TimeBasis, a.Baseline.CutoffSource = monthlySourceLogCutoff, "logs#synthetic-baseline-cutoff"
	for i := range a.Snapshots {
		a.Snapshots[i].TimeBasis, a.Snapshots[i].CutoffSource = monthlySourceLogCutoff, "logs#synthetic-settled-cutoff"
	}
	a.Spans[2].ConsumeQuota = 2000000
	a.Snapshots[2].BalanceQuota -= 2000000
	a.Snapshots[2].UsedQuota += 2000000
	raw := monthlyReviewJSON(t, input)
	return raw, MonthlyHistoricalSettlementRequest{InputSHA256: monthlyDigest(raw), UserID: 102, AnchorAt: preparationAt("2026-09-01"), SettlementMonth: "2026-08", ExpectedGapQuota: 909785}
}

func TestMonthlyHistoricalPreviewPreservesFactsAndEarlierMonths(t *testing.T) {
	raw, request := historicalPreviewFixture(t)
	original := bytes.Clone(raw)
	now := time.Unix(preparationAt("2026-10-20"), 0)
	before, err := PrepareMonthlyReconciliation(raw, now)
	if err != nil {
		t.Fatal(err)
	}
	p, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, now)
	if err != nil || p.Status != "proposal_requires_internal_review" || len(p.Months) != 3 {
		t.Fatal(p, err)
	}
	for i := 0; i < 2; i++ {
		if !reflect.DeepEqual(p.Months[i].MonthlyPreparationRow, before.Accounts[0].Months[i]) || p.Months[i].BalanceAdjustment != "" {
			t.Fatal("verified month changed")
		}
	}
	row := p.Months[2]
	want := MonthlyPreparationAmounts{Opening: "-3.500000", Increase: "0.000000", Decrease: "0.000000", Consume: "4.000000", Refund: "0.000000", Closing: "-5.680430"}
	if row.Amounts == nil || *row.Amounts != want || row.BalanceAdjustment != "1.819570" || row.LogConsume != row.Amounts.Consume || row.Status == "reconciled_draft" || p.Mode != "historical_allocation_preview_not_publication" {
		t.Fatal("proposal misstated", row, p.Mode)
	}
	after, err := PrepareMonthlyReconciliation(raw, now)
	if err != nil || !reflect.DeepEqual(before, after) || !bytes.Equal(raw, original) || after.Accounts[0].Months[2].Amounts != nil {
		t.Fatal("preview changed actual preparation", err)
	}
	again, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, now)
	if err != nil || !reflect.DeepEqual(p, again) {
		t.Fatal("preview not stable", err)
	}
}

func TestMonthlyHistoricalPreviewRejectsUnboundOrContradictoryRequests(t *testing.T) {
	for name, mutate := range map[string]func(*MonthlyHistoricalSettlementRequest){
		"wrong digest":               func(r *MonthlyHistoricalSettlementRequest) { r.InputSHA256 = strings.Repeat("f", 64) },
		"unknown account":            func(r *MonthlyHistoricalSettlementRequest) { r.UserID++ },
		"unknown cutoff":             func(r *MonthlyHistoricalSettlementRequest) { r.AnchorAt++ },
		"no gap":                     func(r *MonthlyHistoricalSettlementRequest) { r.ExpectedGapQuota = 0 },
		"overcharge":                 func(r *MonthlyHistoricalSettlementRequest) { r.ExpectedGapQuota = -1 },
		"different residual":         func(r *MonthlyHistoricalSettlementRequest) { r.ExpectedGapQuota++ },
		"overwrite reconciled month": func(r *MonthlyHistoricalSettlementRequest) { r.SettlementMonth = "2026-07" },
		"cutoff before month end":    func(r *MonthlyHistoricalSettlementRequest) { r.SettlementMonth = "2026-09" },
	} {
		t.Run(name, func(t *testing.T) {
			raw, request := historicalPreviewFixture(t)
			mutate(&request)
			p, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && p.Status != "blocked" {
				t.Fatal("invalid allocation accepted", p)
			}
			if len(p.Months) != 0 {
				t.Fatal("failure exposed partial results", p)
			}
		})
	}
}

func TestMonthlyHistoricalPreviewRejectsMissingFundingAndNegativeConsumption(t *testing.T) {
	for _, kind := range []string{"funding-gap", "counter-disagrees", "negative-consumption", "earlier-conflicting-snapshot"} {
		t.Run(kind, func(t *testing.T) {
			raw, request := historicalPreviewFixture(t)
			input, err := DecodeMonthlyPreparationInput(raw)
			if err != nil {
				t.Fatal(err)
			}
			a := &input.Accounts[0]
			switch kind {
			case "funding-gap":
				a.MovementCoverage[0].To--
			case "counter-disagrees":
				a.Snapshots[2].UsedQuota++
			case "negative-consumption":
				a.Spans[2].ConsumeQuota = 0
				a.Snapshots[2].BalanceQuota += 2000000
				a.Snapshots[2].UsedQuota -= 2000000
			case "earlier-conflicting-snapshot":
				a.Snapshots[1].BalanceQuota++
				a.Snapshots[1].UsedQuota--
			}
			raw = monthlyReviewJSON(t, input)
			request.InputSHA256 = monthlyDigest(raw)
			p, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, time.Unix(preparationAt("2026-10-20"), 0))
			if err == nil && p.Status != "blocked" || len(p.Months) != 0 {
				t.Fatal("unsafe preview", p, err)
			}
		})
	}
}

func TestMonthlyHistoricalPreviewTwoMonthsContinuityAndNoPublication(t *testing.T) {
	raw, request := historicalPreviewFixture(t)
	input, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	// Remove the middle monthly boundary, retaining earlier and later facts.
	input.Accounts[0].Snapshots = append(input.Accounts[0].Snapshots[:1], input.Accounts[0].Snapshots[2:]...)
	raw = monthlyReviewJSON(t, input)
	request.InputSHA256 = monthlyDigest(raw)
	p, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, time.Unix(preparationAt("2026-10-20"), 0))
	if err != nil || p.Status != "proposal_requires_internal_review" || len(p.Months) != 3 {
		t.Fatal(p, err)
	}
	for i := 1; i < len(p.Months); i++ {
		if p.Months[i-1].Amounts.Closing != p.Months[i].Amounts.Opening {
			t.Fatal("adjacent month discontinuity")
		}
	}
	if p.Months[1].Amounts.Refund != "0.500000" || p.Months[1].Amounts.Decrease != "-1.000000" || p.Months[1].Amounts.Consume != "8.000000" {
		t.Fatal("funding/refund changed", p.Months[1])
	}
	if _, err := decodeMonthlyBillEvidence(monthlyReviewJSON(t, p)); err == nil {
		t.Fatal("preview accepted as publishable evidence")
	}
	actual := prepareTest(t, input)
	if actual.Accounts[0].Months[1].Status != "no_matching_balance_anchor" || actual.Accounts[0].Months[2].Status != "no_matching_balance_anchor" {
		t.Fatal("original accounting path relaxed")
	}
}

func TestMonthlyHistoricalPreviewAdjustmentIsSeparateAndCarriedOnlyOnce(t *testing.T) {
	raw, request := historicalPreviewFixture(t)
	input, err := DecodeMonthlyPreparationInput(raw)
	if err != nil {
		t.Fatal(err)
	}
	a := &input.Accounts[0]
	sep, oct := preparationAt("2026-09-01"), preparationAt("2026-10-01")
	a.Months = append(a.Months, "2026-09")
	a.Spans = append(a.Spans, MonthlyPreparationSpan{UserID: a.UserID, From: sep, To: oct, ConsumeQuota: 500000, Requests: preparationInt(1), Tokens: preparationInt(20), Source: "logs#september-usage"})
	a.MovementCoverage[0].To = oct
	closing := a.Snapshots[2]
	closing.At, closing.Source = oct, "logs#october"
	closing.BalanceQuota -= 500000
	closing.UsedQuota += 500000
	a.Snapshots = append(a.Snapshots, closing)
	raw = monthlyReviewJSON(t, input)
	request.InputSHA256, request.AnchorAt = monthlyDigest(raw), oct
	p, err := PrepareMonthlyHistoricalSettlementPreview(raw, request, time.Unix(preparationAt("2026-10-20"), 0))
	if err != nil || p.Status != "proposal_requires_internal_review" || len(p.Months) != 4 {
		t.Fatal(p, err)
	}
	var totalAdjustment int64
	for i, row := range p.Months {
		if row.Amounts == nil || row.Amounts.Consume != row.LogConsume || row.Amounts.Refund != row.LogRefund {
			t.Fatal("statement usage no longer matches usage logs", row)
		}
		if i > 0 && p.Months[i-1].Amounts.Closing != row.Amounts.Opening {
			t.Fatal("cross-month balance discontinuity", row)
		}
		var balance int64
		for _, part := range []struct {
			amount string
			sign   int64
		}{
			{row.Amounts.Opening, 1}, {row.Amounts.Increase, 1}, {row.Amounts.Decrease, 1},
			{row.Amounts.Consume, -1}, {row.Amounts.Refund, 1},
		} {
			q, ok := newMonthlyMoney(part.amount)
			if !ok || addEconomicsInt64(&balance, q*part.sign) != nil {
				t.Fatal("invalid synthetic amount", part)
			}
		}
		if row.BalanceAdjustment != "" {
			q, ok := newMonthlyMoney(row.BalanceAdjustment)
			if !ok || row.Month != request.SettlementMonth || addEconomicsInt64(&balance, q) != nil || addEconomicsInt64(&totalAdjustment, q) != nil {
				t.Fatal("adjustment duplicated or allocated to wrong month", row)
			}
		}
		if monthlyQuotaMoney(balance) != row.Amounts.Closing {
			t.Fatal("customer arithmetic does not balance", row)
		}
	}
	if totalAdjustment != request.ExpectedGapQuota || p.Months[3].BalanceAdjustment != "" || p.Months[3].Amounts.Closing != "-6.680430" {
		t.Fatal("historical adjustment charged again in later month", p)
	}
	if _, err := decodeMonthlyBillEvidence(monthlyReviewJSON(t, p)); err == nil {
		t.Fatal("local proposal bypassed review and became publication evidence")
	}
}
