package monitor

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestFinanceRechargeInspectionIsolatesUnsupportedIntervals(t *testing.T) {
	for _, seconds := range []int64{1969, 7200, 86401} {
		t.Run(fmt.Sprint(seconds), func(t *testing.T) {
			in := rechargeGapFixture(t)
			for i := range in.rows {
				if in.rows[i].Domain == "day.example" && in.rows[i].HourTs == in.scope.FromTs {
					in.rows[i].BucketSeconds = seconds
				}
			}
			before := append([]ChannelUpstreamUsageHour(nil), in.rows...)
			plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
			if err != nil || len(plan.Domains) != 1 || plan.Domains[0].Domain != "hour.example" ||
				!reflect.DeepEqual(plan.Skipped, []FinanceRechargeGapSkipped{{Domain: "day.example", Reason: "bill_interval_unsupported"}}) {
				t.Fatalf("one unsupported source blocked peers or produced a correction: %+v %v", plan, err)
			}
			if !reflect.DeepEqual(before, in.rows) {
				t.Fatal("inspection rewrote the source interval")
			}
			if len(plan.BillCoverage) != 2 || plan.BillCoverage[0].Status != "bill_interval_unsupported" ||
				plan.BillCoverage[0].Complete || plan.BillCoverage[0].RecordedHours != 0 {
				t.Fatal("unsupported bill was certified or hidden", plan.BillCoverage)
			}
		})
	}
}

func TestFinanceRechargeInspectionShowsCoverageWithoutRatioGaps(t *testing.T) {
	for _, mode := range []string{"complete", "bill_missing", "bill_incomplete", "bill_provisional"} {
		t.Run(mode, func(t *testing.T) {
			in := rechargeGapFixture(t)
			for domain := range in.accounts {
				in.versions[domain] = []channelRechargeVersion{{Version: 1, EffectiveAt: in.scope.FromTs, Paid: 2.5, Credit: 8, Valid: true}}
			}
			var rows []ChannelUpstreamUsageHour
			for _, row := range in.rows {
				if row.Domain == "hour.example" {
					if mode == "bill_missing" || (mode == "bill_incomplete" && row.HourTs == in.scope.FromTs) {
						continue
					}
					if mode == "bill_provisional" {
						row.Provisional = true
					}
				}
				rows = append(rows, row)
			}
			in.rows = rows
			plan, err := inspectFinanceRechargeGaps(context.Background(), in, in.scope.ToTs+3600)
			if err != nil || len(plan.Domains) != 0 || len(plan.BillCoverage) != 2 {
				t.Fatalf("valid terms hid incomplete bills or fabricated repairs: %+v %v", plan, err)
			}
			hour := plan.BillCoverage[1]
			if hour.Domain != "hour.example" || hour.Status != mode || hour.ExpectedHours != 48 || hour.Complete != (mode == "complete") {
				t.Fatalf("wrong bill coverage: %+v", hour)
			}
			if mode == "bill_missing" && hour.RecordedHours != 0 || mode == "bill_incomplete" && hour.RecordedHours != 47 {
				t.Fatal("missing hours were invented", hour)
			}
		})
	}
}
