package monitor

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestFinanceRechargeFundReferencesCannotAuthorizeHistoricalTerms(t *testing.T) {
	base := ChannelUpstreamFundEvent{OccurredAt: 3600, Provider: upstreamProviderNewAPI, SourceType: 1, Kind: upstreamFundKindTopup,
		Direction: "credit", Confidence: upstreamFundConfidenceLegacyText, PaidAmount: 1, PaidKnown: true,
		UpstreamAmount: 7.14, UpstreamAmountKnown: true, UpstreamCurrency: "CNY", ObservedCount: 1,
		ParserVersion: upstreamFundParserVersion, Content: "private-content", RawJSON: "private-raw", RequestID: "private-request"}
	equivalent := base
	equivalent.OccurredAt, equivalent.PaidAmount, equivalent.UpstreamAmount, equivalent.UpstreamCurrency = 4000, 2, 14.28, "USD"
	events := []ChannelUpstreamFundEvent{base, equivalent}
	for _, kind := range []string{upstreamFundKindManualTopup, upstreamFundKindSignupGrant, upstreamFundKindRefund, upstreamFundKindRedemption} {
		row := base
		row.Kind = kind
		events = append(events, row)
	}
	for _, mode := range []string{"unknown_payment", "zero_credit", "negative", "infinite", "truncated", "parse_error", "duplicate", "unknown_confidence", "old_parser", "other_provider", "wrong_type", "debit"} {
		row := base
		switch mode {
		case "unknown_payment":
			row.PaidKnown = false
		case "zero_credit":
			row.UpstreamAmount = 0
		case "negative":
			row.PaidAmount = -1
		case "infinite":
			row.UpstreamAmount = math.Inf(1)
		case "truncated":
			row.RawTruncated = true
		case "parse_error":
			row.ReparseError = "private-error"
		case "duplicate":
			row.ObservedCount = 2
		case "unknown_confidence":
			row.Confidence = upstreamFundConfidenceUnknown
		case "old_parser":
			row.ParserVersion--
		case "other_provider":
			row.Provider = upstreamProviderSub2API
		case "wrong_type":
			row.SourceType = 3
		case "debit":
			row.Direction = "debit"
		}
		events = append(events, row)
	}
	outside := base
	outside.OccurredAt = 7200 // Exclusive end: do not pull another period's transaction into this one.
	events = append(events, outside)
	before := append([]ChannelUpstreamFundEvent(nil), events...)
	review := reviewFinanceRechargeFunds(events, upstreamProviderNewAPI, stabilityScope{FromTs: 3600, ToTs: 7200})
	if review.Rows != 18 || review.ReferenceTopups != 2 || review.TextOnlyReferences != 2 || review.NonPaymentRows != 4 || review.UnusableTopups != 12 || len(review.References) != 1 {
		t.Fatalf("payment evidence classes were conflated: %+v", review)
	}
	ref := review.References[0]
	if ref.Records != 2 || ref.CorrectionFraction != "50/357" || ref.CanProveEffectiveRange || ref.FirstAt != 3600 || ref.LastAt != 4000 {
		t.Fatalf("references implied a historical interval or FX: %+v", ref)
	}
	// Compare separately around the intentional NaN/Inf test values.
	for i := range events {
		if events[i] != before[i] {
			t.Fatal("review changed stored evidence", i)
		}
	}
	data, err := json.Marshal(review)
	if err != nil || strings.Contains(string(data), "private-") || strings.Contains(string(data), "raw_json") || strings.Contains(string(data), "request_id") {
		t.Fatal("review exposed private evidence", err)
	}
}

func TestFinanceRechargeReferencePreviewPreservesBucketsAndRounding(t *testing.T) {
	may, err := financeStartHour("2026-05-31")
	if err != nil {
		t.Fatal(err)
	}
	june := may + 86400
	rows := []ChannelUpstreamUsageHour{
		{HourTs: may, BucketSeconds: 3600, CostUSD: 0.00000357},
		{HourTs: june, BucketSeconds: 3600, CostUSD: 0.00000357},
		{HourTs: may + 3600, BucketSeconds: 3600, CostUSD: 0},
		{HourTs: june + 3600, BucketSeconds: 3600, CostUSD: 123},
	}
	versions := []channelRechargeVersion{{Version: 1, EffectiveAt: june + 3600, Paid: 1, Credit: 7.14, Valid: true}}
	beforeRows, beforeVersions := append([]ChannelUpstreamUsageHour(nil), rows...), append([]channelRechargeVersion(nil), versions...)
	ref := financeRechargeReviewReference(versions[0])
	preview, err := previewFinanceRechargeReference(context.Background(), rows, versions, &ref)
	if err != nil || preview == nil || preview.Publishable || preview.Status != "unconfirmed_reference_only" {
		t.Fatal("preview became an accounting result", preview, err)
	}
	if preview.Total.Buckets != 2 || preview.Total.FaceValueMicroUnits != "8" || preview.Total.HypotheticalMicroUnits != "2" || len(preview.Days) != 2 || len(preview.Months) != 2 {
		t.Fatalf("bucket rounding or known/zero exclusions failed: %+v", preview)
	}
	for i := range preview.Days {
		if preview.Days[i].HypotheticalMicroUnits != "1" || preview.Months[i].HypotheticalMicroUnits != "1" {
			t.Fatal("grouping recomputed rather than summed bucket costs")
		}
	}
	if preview.Days[0].Period != "2026-05-31" || preview.Months[1].Period != "2026-06" || !reflect.DeepEqual(rows, beforeRows) || !reflect.DeepEqual(versions, beforeVersions) {
		t.Fatal("preview changed evidence, historical terms, or accounting timezone")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if value, err := previewFinanceRechargeReference(ctx, rows, versions, &ref); err == nil || value != nil {
		t.Fatal("canceled work published a preview")
	}
}

func TestFinanceRechargeReferencePreviewSupportsArbitraryTermsAndRejectsInvalidAmounts(t *testing.T) {
	for _, pair := range [][2]float64{{1, 1}, {1, 10}, {7, 1}, {2.5, 18}, {1, 7.14}} {
		ref := FinanceRechargeVersionReference{Valid: true, Paid: &pair[0], Credited: &pair[1]}
		row := ChannelUpstreamUsageHour{HourTs: 3600, CostUSD: 123.456789}
		preview, err := previewFinanceRechargeReference(context.Background(), []ChannelUpstreamUsageHour{row}, nil, &ref)
		want := financeBillSum{}
		want.addCorrectedUSD(row.CostUSD, pair[0], pair[1])
		if err != nil || preview == nil || preview.Total.HypotheticalMicroUnits != economicsMoney(want.micro).MicroUSD {
			t.Fatal("reference preview differs from accounting decimal precision", pair, preview, err)
		}
	}
	one, zero := 1.0, 0.0
	for _, ref := range []*FinanceRechargeVersionReference{nil, {}, {Valid: true}, {Valid: true, Paid: &one, Credited: &zero}} {
		if value, err := previewFinanceRechargeReference(context.Background(), nil, nil, ref); err != nil || value != nil {
			t.Fatal("missing/invalid rate yielded a reference cost", err)
		}
	}
	ref := FinanceRechargeVersionReference{Valid: true, Paid: &one, Credited: &one}
	for _, raw := range []float64{-1, math.Inf(1), math.NaN(), 1e15} {
		if value, err := previewFinanceRechargeReference(context.Background(), []ChannelUpstreamUsageHour{{HourTs: 3600, CostUSD: raw}}, nil, &ref); err == nil || value != nil {
			t.Fatal("invalid/overflow original amount published a cost", raw)
		}
	}
}

func TestFinanceRechargeRecordedChangesIgnoreUnrelatedSavesAndRetainInvalidity(t *testing.T) {
	versions := []channelRechargeVersion{
		{Version: 1, EffectiveAt: 3600, Paid: 1, Credit: 7, Valid: true},
		{Version: 2, EffectiveAt: 4000, Paid: 2, Credit: 14, Valid: true},
		{Version: 3, EffectiveAt: 5000, Paid: 1, Credit: 1, Valid: true},
		{Version: 4, EffectiveAt: 6000, Valid: false},
		{Version: 5, EffectiveAt: 7200, Paid: 1, Credit: 10, Valid: true},
	}
	changes := financeRechargeRecordedChanges(versions, 7200)
	if len(changes) != 2 || changes[0].Before.Version != 2 || changes[0].After.Version != 3 || changes[1].After.Valid {
		t.Fatal("unrelated save/future version masked a real record discrepancy", changes)
	}
}

func TestFinanceRechargeRecordedChangesUseEffectiveVersions(t *testing.T) {
	for _, tc := range []struct {
		name        string
		winner      channelRechargeVersion
		wantChanges int
	}{
		{"equivalent_correction", channelRechargeVersion{Version: 4, EffectiveAt: 1800, Paid: 2, Credit: 20, Valid: true}, 0},
		{"real_change", channelRechargeVersion{Version: 4, EffectiveAt: 1800, Paid: 1, Credit: 7.14, Valid: true}, 1},
		{"invalid_winner", channelRechargeVersion{Version: 4, EffectiveAt: 1800}, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			versions := []channelRechargeVersion{
				{Version: 1, EffectiveAt: 0}, // Superseded invalid initial entry.
				{Version: 2, EffectiveAt: 0, Paid: 1, Credit: 10, Valid: true},
				{Version: 3, EffectiveAt: 1800, Paid: 1, Credit: 6.66, Valid: true},
				tc.winner,
				{Version: 5, EffectiveAt: 3600, Paid: 1, Credit: 1, Valid: true},
			}
			before := append([]channelRechargeVersion(nil), versions...)
			changes := financeRechargeRecordedChanges(versions, 3600)
			if len(changes) != tc.wantChanges {
				t.Fatalf("superseded/future entry reported as effective change: %+v", changes)
			}
			if len(changes) == 1 && (changes[0].Before.Version != 2 || changes[0].After.Version != 4 || changes[0].After.Valid != tc.winner.Valid) {
				t.Fatalf("wrong effective transition: %+v", changes)
			}
			if !reflect.DeepEqual(versions, before) {
				t.Fatal("review changed immutable audit history")
			}
		})
	}
	if got := financeRechargeRecordedChanges(nil, 3600); got == nil || len(got) != 0 {
		t.Fatal("empty history must remain an empty JSON array", got)
	}
}
