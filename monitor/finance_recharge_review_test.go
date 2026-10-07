package monitor

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
)

func TestFinanceRechargeReviewShowsRecordedBoundaryWithoutInventingRealChange(t *testing.T) {
	in := rechargeGapFixture(t)
	start := in.scope.FromTs
	in.versions["day.example"] = []channelRechargeVersion{
		{Version: 1, EffectiveAt: start, Paid: 1, Credit: 1, Valid: true},
		{Version: 2, EffectiveAt: start + 3600, Paid: 1, Credit: 7.14, Valid: true},
		{Version: 3, EffectiveAt: in.scope.ToTs, Paid: 7, Credit: 1, Valid: true},
	}
	plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
	if err != nil || len(plan.Domains) != 2 {
		t.Fatal(plan, err)
	}
	day := plan.Domains[0]
	gap := day.Gaps[0]
	if len(day.Gaps) != 1 || gap.Reason != upstreamAdjustedCostBucketAmbiguous || gap.BucketSeconds != 86400 ||
		gap.ReviewAction != "distinguish_configuration_correction_from_real_rate_change" || gap.StartVersion == nil ||
		gap.StartVersion.Version != 1 || len(gap.RecordedChanges) != 1 || gap.RecordedChanges[0].Version != 2 ||
		*gap.RecordedChanges[0].Credited != 7.14 || day.ReferenceAtRangeEnd == nil || day.ReferenceAtRangeEnd.Version != 2 {
		t.Fatal("recorded evidence lost, guessed or crossed the exclusive query boundary", day)
	}
	// A review is not an override: the same bill is still ambiguous afterward.
	_, _, status := rechargeTermsForBucket(in.versions[day.Domain], gap.FromTs, gap.ToTs)
	if status != upstreamAdjustedCostBucketAmbiguous {
		t.Fatal("inspection changed accounting authority")
	}
	raw, err := json.Marshal(plan)
	if err != nil || strings.Contains(string(raw), "snapshot_json") || strings.Contains(string(raw), "channel_rates") || strings.Contains(string(raw), "api_key") {
		t.Fatal("review contains non-recharge snapshot or credential data", err)
	}
}

func TestFinanceRechargeReviewDoesNotMergeAwayFirstVersionOrInvalidReplacement(t *testing.T) {
	for _, invalid := range []bool{false, true} {
		in := rechargeGapFixture(t)
		start := in.scope.FromTs
		in.rows = []ChannelUpstreamUsageHour{
			{Domain: "hour.example", HourTs: start, BucketSeconds: 3600, Provider: upstreamProviderNewAPI, CostUSD: 1},
			{Domain: "hour.example", HourTs: start + 3600, BucketSeconds: 3600, Provider: upstreamProviderNewAPI, CostUSD: 2},
		}
		if invalid {
			in.versions["hour.example"] = []channelRechargeVersion{{Version: 1, EffectiveAt: start}, {Version: 2, EffectiveAt: start + 3600}}
		} else {
			in.versions["hour.example"] = []channelRechargeVersion{{Version: 1, EffectiveAt: start + 5400, Paid: 1, Credit: 1, Valid: true}}
		}
		plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
		if err != nil || len(plan.Domains) != 1 || len(plan.Domains[0].Gaps) != 2 {
			t.Fatal("separate recorded boundaries merged into one claim", invalid, plan, err)
		}
		left, right := plan.Domains[0].Gaps[0], plan.Domains[0].Gaps[1]
		if left.ToTs != right.FromTs || left.FaceValueMicroUnits != "1000000" || right.FaceValueMicroUnits != "2000000" {
			t.Fatal("review segmentation changed source money", plan)
		}
		if invalid {
			if left.StartVersion.Version != 1 || right.StartVersion.Version != 2 || left.StartVersion.Paid != nil || right.StartVersion.Credited != nil {
				t.Fatal("invalid version values were suggested as usable rates", plan)
			}
		} else if left.StartVersion != nil || len(left.RecordedChanges) != 0 || len(right.RecordedChanges) != 1 || right.RecordedChanges[0].EffectiveAt != start+5400 ||
			right.ReviewAction != "confirm_historical_rate_and_review_boundary_records" {
			t.Fatal("first recorded version inside an unpriced bucket was hidden", plan)
		}
	}
}

func TestFinanceRechargeReviewInvalidNumbersStaySerializable(t *testing.T) {
	for _, paid := range []float64{math.NaN(), math.Inf(1), -1, 0} {
		ref := financeRechargeReviewReference(channelRechargeVersion{Version: 1, EffectiveAt: 3600, Paid: paid, Credit: 1, Valid: true})
		if ref.Valid || ref.Paid != nil || ref.Credited != nil {
			t.Fatal("invalid terms published as a reference", ref)
		}
		if _, err := json.Marshal(ref); err != nil {
			t.Fatal("invalid evidence prevents a diagnostic from rendering", err)
		}
	}
}

func TestFinanceRechargeAmbiguousActionRequiresBusinessEvidence(t *testing.T) {
	detail := financeCostDetailView{KnownBilledCost: economicsMoney(1), RechargeHistoryStatus: upstreamAdjustedCostBucketAmbiguous}
	applyFinanceClosureReadiness(&detail)
	if detail.ClosureReadiness != "correction_ambiguous" || !strings.Contains(detail.ClosureNextAction, "配置录入纠正") ||
		!strings.Contains(detail.ClosureNextAction, "真实比例变化") || !strings.Contains(detail.ClosureNextAction, "确认前不发布") ||
		detail.CorrectedCost != nil || detail.Contribution != nil {
		t.Fatal("a recorded boundary was asserted to be a real change or published cost", detail)
	}
}
