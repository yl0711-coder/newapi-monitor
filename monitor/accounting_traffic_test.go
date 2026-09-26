package monitor

import (
	"context"
	"fmt"
	"reflect"
	"testing"
)

func TestAccountingDeliveryUpgradePreservesMoneyAndInternalExclusions(t *testing.T) {
	if !accountingTrafficVersionSupported(stabilityTrafficClassificationVersion) {
		t.Fatal("delivery policy changed: review monetary compatibility before upgrading")
	}
	for _, versions := range [][]int{{6, 6}, {6, 7}, {7, 7}} {
		t.Run(fmt.Sprint(versions), func(t *testing.T) {
			m := newFinanceReportTestMonitor(t, "upgrade.example")
			ctx := context.Background()
			hour := int64(1_788_195_600)
			scope := stabilityScope{FromTs: hour, ToTs: hour + 7200}
			if err := m.storeDB.Create(&ChannelSnap{ID: 5, BaseDomain: "upgrade.example", Status: 1}).Error; err != nil {
				t.Fatal(err)
			}
			internal := financeConfiguredInternalEvidence{Complete: true, Accounts: 1, Requests: 2, NetQuota: 2_000_000}
			for i, version := range versions {
				h := hour + int64(i)*3600
				for _, row := range []any{
					&StabilityHourSample{HourTs: h, ChannelID: 5, Grp: "paid", ModelName: "m", Success: 2, Anomaly: 1, Tokens: 30, Quota: 3_000_000, TrafficClassVersion: version},
					&StabilityHourSample{HourTs: h, ChannelID: 5, Grp: "test-only", ModelName: "m", Success: 1, Quota: 8_000_000, TrafficClassVersion: version},
					&ChannelTestHourSample{HourTs: h, ChannelID: 5, ModelName: "m", Requests: 1, Success: 1, Quota: 500_000, TrafficClassVersion: version},
					&StabilityHourIngestState{HourTs: h, Status: "complete", Requests: 4, TrafficClassVersion: version},
				} {
					if err := m.storeDB.Create(row).Error; err != nil {
						t.Fatal(err)
					}
				}
				internal.Rows = append(internal.Rows, FinanceInternalAccountHourFact{HourTs: h, UserID: 7, ChannelID: 5, Grp: "paid", Requests: 1, Tokens: 10, ConsumeQuota: 1_000_000})
			}
			policies := map[string]bool{"test-only": false}
			statement, coverage, domains, err := m.loadFinanceUserFacts(ctx, scope, scope.ToTs+7200, internal, policies)
			if err != nil {
				t.Fatal(err)
			}
			if !coverage.Complete || statement.UserConsumption == nil || statement.UserConsumption.MicroUSD != "8000000" ||
				statement.InternalTestConsumption.MicroUSD != "6000000" || statement.InternalTestRequests != 4 || domains["upgrade.example"].Requests != 4 {
				t.Fatalf("monetary upgrade changed totals: statement=%+v coverage=%+v domains=%+v", statement, coverage, domains)
			}
			days, dayCoverage, err := m.loadFinanceDailyUserFacts(ctx, scope, scope.ToTs+7200, internal, policies)
			if err != nil {
				t.Fatal(err)
			}
			day := cstDayStart(hour)
			if days[day].ConsumeQuota != 4_000_000 || days[day].TestQuota != 3_000_000 || !dayCoverage[day].Complete {
				t.Fatalf("daily money differs from monthly: %+v %+v", days, dayCoverage)
			}
			starts, err := m.loadFinanceUpstreamActivityStarts(ctx, scope.ToTs)
			if err != nil || starts["upgrade.example"] != hour {
				t.Fatalf("historical upstream scope disappeared: %v %v", starts, err)
			}
			if versions[0] == 6 && m.stabilityDataCoverage(ctx, scope.FromTs, scope.ToTs, scope.ToTs+7200).Complete {
				t.Fatal("monetary compatibility must not mark old delivery policy as current")
			}
			internal.Rows[0].ConsumeQuota = 20_000_000
			if _, _, _, err := m.loadFinanceUserFacts(ctx, scope, scope.ToTs+7200, internal, policies); err == nil {
				t.Fatal("genuine internal amount inconsistency was hidden")
			}
		})
	}
}

func TestAccountingCoverageRejectsUnknownVersionsAndZeroContradictions(t *testing.T) {
	for _, version := range []int{0, 5, 6, 7, 8} {
		t.Run(fmt.Sprint(version), func(t *testing.T) {
			m := newStabilityTestMonitor(t)
			hour := int64(1_788_195_600)
			state := StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: 7}
			if err := m.storeDB.Create(&state).Error; err != nil {
				t.Fatal(err)
			}
			if err := m.storeDB.Model(&state).Update("traffic_class_version", version).Error; err != nil {
				t.Fatal(err)
			}
			ctx := context.Background()
			coverage := m.accountingDataCoverage(ctx, hour, hour+3600, hour+7200)
			if coverage.Complete != (version == 6 || version == 7) {
				t.Fatalf("unexpected version acceptance: %+v", coverage)
			}
			if version != 6 && version != 7 {
				return
			}
			minuteVersion := 13 - version // v6 zero vs v7 minute and the reverse.
			if err := m.storeDB.Create(&MetricSample{BucketTs: hour, ChannelID: 5, Success: 1, TrafficClassVersion: minuteVersion}).Error; err != nil {
				t.Fatal(err)
			}
			if got := m.accountingDataCoverage(ctx, hour, hour+3600, hour+7200); got.Complete || got.CompletedHours != 0 {
				t.Fatalf("cross-version zero contradiction was accepted: %+v", got)
			}
			if err := m.storeDB.Model(&state).Updates(map[string]any{"requests": 1, "status": "failed"}).Error; err != nil {
				t.Fatal(err)
			}
			if got := m.accountingDataCoverage(ctx, hour, hour+3600, hour+7200); got.Complete || got.LatestHourPending {
				t.Fatalf("failed historical hour hidden as complete/pending: %+v", got)
			}
		})
	}
}

func TestAccountingHourReplacementPreservesRefundsAndDoesNotDoubleCount(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "upgrade.example")
	hour := int64(1_788_195_600)
	ctx := context.Background()
	row := StabilityHourSample{HourTs: hour, ChannelID: 5, Grp: "paid", ModelName: "m", Success: 2, Anomaly: 1, Quota: 3_000_000, RefundQuota: 100_000, RefundRecords: 1, TrafficClassVersion: 6}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 3, TrafficClassVersion: 6}).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: hour, ToTs: hour + 3600}
	before, _, _, err := m.loadFinanceUserFacts(ctx, scope, hour+7200, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	row.Success, row.Anomaly = 3, 0
	for i := 0; i < 2; i++ {
		if err := m.replaceStabilityHour(hour, []StabilityHourSample{row}, StabilityHourIngestState{}); err != nil {
			t.Fatal(err)
		}
		after, coverage, _, err := m.loadFinanceUserFacts(ctx, scope, hour+7200, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil || !coverage.Complete || !reflect.DeepEqual(before, after) {
			t.Fatalf("reclassification/retry changed money: before=%+v after=%+v coverage=%+v err=%v", before, after, coverage, err)
		}
	}
	var count int64
	if err := m.storeDB.Model(&StabilityHourSample{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("duplicate hour rows: %d %v", count, err)
	}
}

func TestAccountingChannelUsagePreservedWithoutMixingSuccessPolicies(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 5, Name: "upgrade", BaseDomain: "upgrade.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for i, version := range []int{6, 7} {
		h := hour + int64(i)*3600
		if err := m.storeDB.Create(&StabilityHourSample{HourTs: h, ChannelID: 5, Grp: "paid", ModelName: "m", Success: 2, Anomaly: 1, Tokens: 30, Quota: 3_000_000, TrafficClassVersion: version}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: h, Status: "complete", Requests: 3, TrafficClassVersion: version}).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, tc := range []struct {
		from, to int64
		requests int64
		basis    string
		rate     bool
	}{
		{hour, hour + 3600, 3, "legacy", true}, {hour + 3600, hour + 7200, 3, "", true}, {hour, hour + 7200, 6, "mixed", false},
	} {
		report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: tc.from, ToTs: tc.to}, hour+10800)
		if err != nil {
			t.Fatal(err)
		}
		if report.Summary.Usage.Requests != tc.requests || report.Summary.Usage.CostUSD != float64(tc.requests)*2 || !report.Meta.DataCoverage.Complete {
			t.Fatalf("legacy usage lost: %+v %+v", report.Summary.Usage, report.Meta)
		}
		found := false
		for _, d := range report.Domains {
			for _, v := range d.Vendors {
				for _, ch := range v.Channels {
					if ch.ID == 5 {
						found = true
						if ch.StabilityBasis != tc.basis || (ch.Stability != nil) != tc.rate {
							t.Fatalf("mixed stability: %+v", ch)
						}
					}
				}
			}
		}
		if !found {
			t.Fatal("historical channel missing")
		}
	}
}
