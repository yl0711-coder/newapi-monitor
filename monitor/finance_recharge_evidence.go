package monitor

// Local evidence review and unconfirmed sensitivity analysis. These projections
// do not create historical terms, change reports, or authorize database writes.

import (
	"context"
	"errors"
	"math/big"
	"sort"
	"strconv"
	"time"
)

type FinanceRechargeFundReference struct {
	Paid                   string `json:"paid"`
	Credited               string `json:"credited"`
	CorrectionFraction     string `json:"correction_fraction"`
	Records                int    `json:"records"`
	FirstAt                int64  `json:"first_at"`
	LastAt                 int64  `json:"last_at"`
	CanProveEffectiveRange bool   `json:"can_prove_effective_range"`
}

type FinanceRechargeFundReview struct {
	Rows                int                            `json:"rows"`
	ReferenceTopups     int                            `json:"reference_topups"`
	TextOnlyReferences  int                            `json:"text_only_references"`
	NonPaymentRows      int                            `json:"non_payment_rows"`
	UnusableTopups      int                            `json:"unusable_topups"`
	ExcludedAccountRows int64                          `json:"excluded_account_rows"`
	SyncStateKnown      bool                           `json:"sync_state_known"`
	CoverageFrom        int64                          `json:"coverage_from"`
	DataUntil           int64                          `json:"data_until"`
	HistoryCoversQuery  bool                           `json:"history_covers_query"`
	References          []FinanceRechargeFundReference `json:"references"`
}

type FinanceRechargeRecordedChange struct {
	Before FinanceRechargeVersionReference `json:"before"`
	After  FinanceRechargeVersionReference `json:"after"`
}

type FinanceRechargeReferencePeriod struct {
	Period                 string `json:"period"`
	Buckets                int64  `json:"buckets"`
	FaceValueMicroUnits    string `json:"face_value_micro_units"`
	HypotheticalMicroUnits string `json:"hypothetical_micro_units"`
}

type FinanceRechargeReferencePreview struct {
	Status      string                           `json:"status"`
	Publishable bool                             `json:"publishable"`
	Reference   FinanceRechargeVersionReference  `json:"reference"`
	Total       FinanceRechargeReferencePeriod   `json:"total"`
	Months      []FinanceRechargeReferencePeriod `json:"months"`
	Days        []FinanceRechargeReferencePeriod `json:"days"`
}

type FinanceRechargeEvidenceDomain struct {
	Domain           string                           `json:"domain"`
	Provider         string                           `json:"provider"`
	BillComplete     bool                             `json:"bill_complete"`
	BillStatus       string                           `json:"bill_status"`
	Funds            FinanceRechargeFundReview        `json:"funds"`
	RecordedChanges  []FinanceRechargeRecordedChange  `json:"recorded_changes"`
	ReferencePreview *FinanceRechargeReferencePreview `json:"reference_preview,omitempty"`
}

type FinanceRechargeEvidenceInspection struct {
	Mode                 string                          `json:"mode"`
	SourceSnapshotSHA256 string                          `json:"source_snapshot_sha256"`
	FromTs               int64                           `json:"from_ts"`
	ToTs                 int64                           `json:"to_ts"`
	AutoApplyAllowed     bool                            `json:"auto_apply_allowed"`
	Domains              []FinanceRechargeEvidenceDomain `json:"domains"`
}

func financeRechargeFraction(paid, credit float64) (*big.Rat, bool) {
	if !validChannelFinanceNumber(paid) || !validChannelFinanceNumber(credit) {
		return nil, false
	}
	p, _ := nonnegativeFloatRat(paid)
	c, _ := nonnegativeFloatRat(credit)
	return p.Quo(p, c), true
}

func reviewFinanceRechargeFunds(events []ChannelUpstreamFundEvent, provider string, scope stabilityScope) FinanceRechargeFundReview {
	out := FinanceRechargeFundReview{References: []FinanceRechargeFundReference{}}
	groups := make(map[string]FinanceRechargeFundReference)
	for _, event := range events {
		if event.OccurredAt < scope.FromTs || event.OccurredAt >= scope.ToTs {
			continue
		}
		out.Rows++
		if event.Kind != upstreamFundKindTopup {
			out.NonPaymentRows++ // Manual credit, gifts, refunds and redemptions are not proof of payment.
			continue
		}
		fraction, valid := financeRechargeFraction(event.PaidAmount, event.UpstreamAmount)
		confidenceKnown := event.Confidence == upstreamFundConfidenceStructured || event.Confidence == upstreamFundConfidenceField || event.Confidence == upstreamFundConfidenceLegacyText
		if !valid || event.Provider != provider || event.SourceType != 1 || event.Direction != "credit" || !event.PaidKnown || !event.UpstreamAmountKnown ||
			!confidenceKnown || event.RawTruncated || event.ReparseError != "" || event.ObservedCount != 1 || event.ParserVersion != upstreamFundParserVersion {
			out.UnusableTopups++
			continue
		}
		out.ReferenceTopups++
		if event.Confidence == upstreamFundConfidenceLegacyText {
			out.TextOnlyReferences++
		}
		key := fraction.RatString()
		group, found := groups[key]
		if !found {
			group = FinanceRechargeFundReference{Paid: strconv.FormatFloat(event.PaidAmount, 'f', -1, 64),
				Credited: strconv.FormatFloat(event.UpstreamAmount, 'f', -1, 64), CorrectionFraction: key,
				FirstAt: event.OccurredAt, LastAt: event.OccurredAt}
		}
		group.Records++
		if event.OccurredAt < group.FirstAt {
			group.FirstAt = event.OccurredAt
		}
		if event.OccurredAt > group.LastAt {
			group.LastAt = event.OccurredAt
		}
		groups[key] = group
	}
	for _, group := range groups {
		out.References = append(out.References, group)
	}
	sort.Slice(out.References, func(i, j int) bool {
		return out.References[i].CorrectionFraction < out.References[j].CorrectionFraction
	})
	return out // Transaction references never establish an interval's commercial terms.
}

func financeRechargeRecordedChanges(versions []channelRechargeVersion, end int64) []FinanceRechargeRecordedChange {
	out := []FinanceRechargeRecordedChange{}
	for i := 1; i < len(versions) && versions[i].EffectiveAt < end; i++ {
		before, after := versions[i-1], versions[i]
		left, leftOK := financeRechargeFraction(before.Paid, before.Credit)
		right, rightOK := financeRechargeFraction(after.Paid, after.Credit)
		if before.Valid && after.Valid && leftOK && rightOK && left.Cmp(right) == 0 {
			continue // Saving another field does not itself indicate a recharge change.
		}
		out = append(out, FinanceRechargeRecordedChange{Before: financeRechargeReviewReference(before), After: financeRechargeReviewReference(after)})
	}
	return out
}

type financeRechargeReferenceSum struct {
	buckets         int64
	face, corrected financeBillSum
}

func (sum *financeRechargeReferenceSum) add(row ChannelUpstreamUsageHour, paid, credit float64) {
	sum.buckets++
	sum.face.addUSD(row.CostUSD)
	sum.corrected.addCorrectedUSD(row.CostUSD, paid, credit)
}

func (sum financeRechargeReferenceSum) view(period string) (FinanceRechargeReferencePeriod, error) {
	if sum.face.err != nil || sum.corrected.err != nil {
		return FinanceRechargeReferencePeriod{}, errors.New("reference preview amount exceeds accounting precision")
	}
	return FinanceRechargeReferencePeriod{Period: period, Buckets: sum.buckets,
		FaceValueMicroUnits: strconv.FormatInt(sum.face.micro, 10), HypotheticalMicroUnits: strconv.FormatInt(sum.corrected.micro, 10)}, nil
}

// This is a what-if on missing/ambiguous buckets only, not a proposed repair.
// Known buckets are untouched; absent bills, valid zeros and subscriptions are
// not made into costs. Every bucket is rounded once, then summed into day/month.
func previewFinanceRechargeReference(ctx context.Context, rows []ChannelUpstreamUsageHour, versions []channelRechargeVersion, ref *FinanceRechargeVersionReference) (*FinanceRechargeReferencePreview, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ref == nil || !ref.Valid || ref.Paid == nil || ref.Credited == nil {
		return nil, nil
	}
	if _, ok := financeRechargeFraction(*ref.Paid, *ref.Credited); !ok {
		return nil, nil
	}
	out := &FinanceRechargeReferencePreview{Status: "unconfirmed_reference_only", Reference: *ref,
		Months: []FinanceRechargeReferencePeriod{}, Days: []FinanceRechargeReferencePeriod{}}
	total := financeRechargeReferenceSum{}
	days := map[string]financeRechargeReferenceSum{}
	months := map[string]financeRechargeReferenceSum{}
	zone := time.FixedZone("Asia/Shanghai", 8*3600)
	for _, row := range rows {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		_, _, status := rechargeTermsForBucket(versions, row.HourTs, row.HourTs+seconds)
		if status == upstreamAdjustedCostComplete || upstreamBillHasZeroFaceValue(row) || row.Provider == upstreamProviderOpenOx {
			continue
		}
		date := time.Unix(row.HourTs, 0).In(zone).Format("2006-01-02")
		day, month := days[date], months[date[:7]]
		day.add(row, *ref.Paid, *ref.Credited)
		month.add(row, *ref.Paid, *ref.Credited)
		total.add(row, *ref.Paid, *ref.Credited)
		days[date], months[date[:7]] = day, month
	}
	var err error
	if out.Total, err = total.view("selected_missing_buckets"); err != nil {
		return nil, err
	}
	for _, item := range []struct {
		values map[string]financeRechargeReferenceSum
		target *[]FinanceRechargeReferencePeriod
	}{{days, &out.Days}, {months, &out.Months}} {
		keys := make([]string, 0, len(item.values))
		for key := range item.values {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			value, err := item.values[key].view(key)
			if err != nil {
				return nil, err
			}
			*item.target = append(*item.target, value)
		}
	}
	return out, ctx.Err()
}
