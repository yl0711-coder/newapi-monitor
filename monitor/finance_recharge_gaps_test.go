package monitor

import (
	"context"
	"math"
	"reflect"
	"testing"
)

func rechargeGapFixture(t *testing.T) *financeBillInputs {
	t.Helper()
	m, scope, accounts := dailyBillFixture(t)
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs+86400, 1, 1)
	createChannelRechargeVersion(t, m, "day.example", 1, scope.FromTs+3600, 1, 1)
	inputs, err := m.loadFinanceBillInputs(context.Background(), scope, accounts, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	return inputs
}

func TestFinanceRechargeGapPlanIdentifiesExactSourceBuckets(t *testing.T) {
	in := rechargeGapFixture(t)
	before := append([]ChannelUpstreamUsageHour(nil), in.rows...)
	plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
	if err != nil || len(plan.Domains) != 2 || len(plan.Skipped) != 0 {
		t.Fatal(plan, err)
	}
	day, hour := plan.Domains[0], plan.Domains[1]
	if day.Domain != "day.example" || hour.Domain != "hour.example" || len(day.Gaps) != 1 || len(hour.Gaps) != 1 {
		t.Fatal("gap plan lacks deterministic source separation", plan)
	}
	for _, item := range []FinanceRechargeGapDomain{day, hour} {
		gap := item.Gaps[0]
		if gap.FromTs != in.scope.FromTs || gap.ToTs != in.scope.FromTs+gap.BucketSeconds || gap.Reason != "before_first_version" || !item.BillComplete || item.FirstVersionNumber != 1 {
			t.Fatal("missing prefix confused with missing bills", item)
		}
	}
	if day.Gaps[0].Buckets != 1 || day.Gaps[0].BucketSeconds != 86400 || day.Gaps[0].FaceValueMicroUnits != "10000000" ||
		hour.Gaps[0].Buckets != 1 || hour.Gaps[0].NonzeroCostBuckets != 1 || hour.Gaps[0].FaceValueMicroUnits != "1000000" ||
		day.FirstVersionAt != in.scope.FromTs+3600 || hour.FirstVersionAt != in.scope.FromTs+86400 || plan.MonetaryUnit != "platform_accounting_usd" {
		t.Fatal("bucket granularity, zero rows, currency or affected amount changed", plan)
	}
	// The first daily bucket ends after the first finance version. Do not
	// silently round the effective time down or treat it as a safe repair span.
	if day.Gaps[0].ToTs <= day.FirstVersionAt {
		t.Fatal("daily boundary was fabricated")
	}
	for i, j := 0, len(in.rows)-1; i < j; i, j = i+1, j-1 {
		in.rows[i], in.rows[j] = in.rows[j], in.rows[i]
	}
	reversed := append([]ChannelUpstreamUsageHour(nil), in.rows...)
	repeated, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
	if err != nil || !reflect.DeepEqual(plan, repeated) || !reflect.DeepEqual(reversed, in.rows) {
		t.Fatal("inspection depends on row order or rewrites the input", err)
	}
	if reflect.DeepEqual(before, in.rows) {
		t.Fatal("fixture did not change source iteration order")
	}
}

func TestFinanceRechargeGapReasonsRemainDistinct(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		versions     []channelRechargeVersion
	}{
		{"absent", "no_history", nil},
		{"invalid", "invalid_effective_version", []channelRechargeVersion{{Version: 1, EffectiveAt: 0, Valid: false}}},
		{"change_inside_day", upstreamAdjustedCostBucketAmbiguous, []channelRechargeVersion{{Version: 1, EffectiveAt: 0, Paid: 1, Credit: 1, Valid: true}, {Version: 2, EffectiveAt: 1, Paid: 1, Credit: 2, Valid: true}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			in := rechargeGapFixture(t)
			history := append([]channelRechargeVersion(nil), tc.versions...)
			for i := range history {
				history[i].EffectiveAt += in.scope.FromTs
			}
			in.versions["day.example"] = history
			plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
			if err != nil || len(plan.Domains) != 2 || plan.Domains[0].Gaps[0].Reason != tc.reason {
				t.Fatal("ambiguous and absent terms were conflated", plan, err)
			}
		})
	}
}

func TestFinanceRechargeGapPlanRejectsUnsafeBillSources(t *testing.T) {
	for _, mode := range []string{"overlap", "invalid_amount", "provisional", "partial_day", "unsupported_subscription"} {
		t.Run(mode, func(t *testing.T) {
			in := rechargeGapFixture(t)
			for i := range in.rows {
				if in.rows[i].Domain != "day.example" {
					continue
				}
				switch mode {
				case "overlap":
					if in.rows[i].HourTs > in.scope.FromTs {
						in.rows[i].HourTs -= 3600
					}
				case "invalid_amount":
					in.rows[i].CostUSD = -1
				case "provisional":
					in.rows[i].Provisional = true
				case "unsupported_subscription":
					in.rows[i].Provider = financeRechargeSubscriptionProvider
				}
			}
			if mode == "partial_day" {
				in.scope.FromTs += 3600
			}
			if mode == "unsupported_subscription" {
				account := in.accounts["day.example"]
				account.Provider = financeRechargeSubscriptionProvider
				in.accounts["day.example"] = account
			}
			plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
			wantDomains := 1
			if mode == "partial_day" {
				wantDomains = 0 // The only unpriced nonzero hour is outside this window.
			}
			if err != nil || len(plan.Domains) != wantDomains || (wantDomains > 0 && plan.Domains[0].Domain != "hour.example") || len(plan.Skipped) != 1 || plan.Skipped[0].Domain != "day.example" || plan.Skipped[0].Reason == "" {
				t.Fatal("unsafe bill became a recharge repair plan", plan, err)
			}
		})
	}
}

func TestFinanceRechargeGapIgnoresUnconfiguredSourcesAndNeverBridgesAbsentHours(t *testing.T) {
	in := rechargeGapFixture(t)
	account := in.accounts["day.example"]
	account.Configured = false
	in.accounts["day.example"] = account
	var rows []ChannelUpstreamUsageHour
	for _, row := range in.rows {
		if row.Domain == "hour.example" && row.HourTs == in.scope.FromTs+3600 {
			continue
		}
		if row.Domain == "hour.example" && row.HourTs == in.scope.FromTs+7200 {
			row.CostUSD, row.Quota = 1, quotaPerUSD
		}
		rows = append(rows, row)
	}
	rows = append(rows, ChannelUpstreamUsageHour{Domain: "ignored.example", HourTs: math.MaxInt64, BucketSeconds: 86400})
	in.rows = rows
	plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
	if err != nil || len(plan.Domains) != 1 || len(plan.Skipped) != 0 || len(plan.Domains[0].Gaps) != 2 || plan.Domains[0].BillComplete {
		t.Fatal("unconfigured data counted or uncollected hours were invented", plan, err)
	}
	if plan.Domains[0].Gaps[0].ToTs != in.scope.FromTs+3600 || plan.Domains[0].Gaps[1].FromTs != in.scope.FromTs+7200 {
		t.Fatal("missing hour bridged into evidence", plan)
	}
}

func TestFinanceRechargeGapPlanBudgetRangeAndCancellation(t *testing.T) {
	in := rechargeGapFixture(t)
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if result, err := inspectFinanceRechargeGaps(canceled, in, in.scope.ToTs+3600); err == nil || result.Mode != "" {
		t.Fatal("cancellation returned a partial plan")
	}
	in.scope.ToTs = in.scope.FromTs
	if _, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600); err == nil {
		t.Fatal("empty range accepted")
	}
	in.scope.ToTs = in.scope.FromTs + 86400
	in.rows = []ChannelUpstreamUsageHour{{Domain: "hour.example", Provider: upstreamProviderNewAPI, HourTs: math.MaxInt64, BucketSeconds: 3600}}
	if _, err := inspectFinanceRechargeGaps(context.Background(), in, math.MaxInt64); err == nil {
		t.Fatal("overflowing source interval accepted")
	}
	in.rows = make([]ChannelUpstreamUsageHour, financeRechargeInspectBucketLimit+1)
	if _, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600); err == nil {
		t.Fatal("unbounded source read accepted")
	}
}

func TestFinanceRechargeGapPlanCompleteHistoryNeedsNoRepair(t *testing.T) {
	in := rechargeGapFixture(t)
	for domain := range in.accounts {
		in.versions[domain] = []channelRechargeVersion{{Version: 1, EffectiveAt: in.scope.FromTs, Paid: 1, Credit: 1, Valid: true}}
	}
	plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
	if err != nil || len(plan.Domains) != 0 || len(plan.Skipped) != 0 {
		t.Fatal("complete existing history produced a redundant repair", plan, err)
	}
}

func TestFinanceRechargeGapPlanRangeBudgetIsAllOrNothing(t *testing.T) {
	start, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	perDomain := financeRechargeGapRangeLimit/2 + 1
	in := &financeBillInputs{scope: stabilityScope{FromTs: start, ToTs: start + int64(perDomain)*7200}, accounts: map[string]ChannelUpstreamAccountView{}}
	for _, domain := range []string{"a.example", "b.example"} {
		in.accounts[domain] = ChannelUpstreamAccountView{Configured: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI, UsageGranularity: "hour"}
		for i := 0; i < perDomain; i++ {
			in.rows = append(in.rows, ChannelUpstreamUsageHour{Domain: domain, HourTs: start + int64(i)*7200, BucketSeconds: 3600, Provider: upstreamProviderNewAPI, CostUSD: 1, Quota: quotaPerUSD, UnitPerUSD: quotaPerUSD})
		}
	}
	plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
	if err == nil || plan.Mode != "" || len(plan.Domains) != 0 {
		t.Fatal("over-budget inspection emitted a partial plan", err)
	}
}
