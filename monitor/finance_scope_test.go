package monitor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestFinanceGiftGroupEvidenceAndLegacyCompatibility(t *testing.T) {
	for _, known := range []bool{true, false} {
		t.Run(map[bool]string{true: "new", false: "legacy"}[known], func(t *testing.T) {
			m := newFinanceReportTestMonitor(t, "gift-scope.example")
			db, ctx := m.usageFactsStore(), context.Background()
			if _, err := replaceFinanceUserHourFacts(ctx, db, 0, "v1", []FinanceUserHourFact{{HourTs: 0, UserID: 7, Requests: 3, ConsumeQuota: 80_000_000}}, 10000); err != nil {
				t.Fatal(err)
			}
			grant := FinanceCreditEvent{SourceLogID: 1, EventAt: 10, TargetUserID: 7, Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000, EligibleTrial: true}
			grant.EvidenceHash = financeCreditEventHash(grant)
			if _, err := replaceFinanceCreditHour(ctx, db, 0, 10000, "v1", financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}); err != nil {
				t.Fatal(err)
			}
			events := []FinanceGiftBoundaryEvent{
				{SourceLogID: 2, UserID: 7, EventAt: 20, Kind: "usage", Quota: 30_000_000, Group: "test", GroupKnown: known},
				{SourceLogID: 3, UserID: 7, EventAt: 30, Kind: "usage", Quota: 50_000_000, Group: "business", GroupKnown: known},
				// Zero-quota requests remain in the complete boundary proof,
				// but unknown scope cannot change gift allocation or revenue.
				{SourceLogID: 4, UserID: 7, EventAt: 40, Kind: "usage", Quota: 0, GroupKnown: false},
			}
			for i := range events {
				events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
			}
			if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, 0, 7, 10000, "v1", events); err != nil {
				t.Fatal(err)
			}
			result, err := m.loadFinanceGiftAllocationForScope(ctx, 0, 0, 3600, map[string]bool{"test": false}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if known {
				if !result.Coverage.Complete || result.Allocation.PeriodGiftConsumptionMicroUSD != 40_000_000 {
					t.Fatalf("wrong scoped allocation: %+v", result)
				}
				s := financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(100_000_000))}
				if err := applyFinanceGiftAllocation(&s, result); err != nil {
					t.Fatal(err)
				}
				if s.OperatingRevenue == nil || s.OperatingRevenue.MicroUSD != "60000000" {
					t.Fatalf("double deduction: %+v", s)
				}
			} else if result.Coverage.Complete || result.Coverage.ScopeUnknownEvents != 2 {
				t.Fatalf("invented legacy scope: %+v", result.Coverage)
			}
			all, err := m.loadFinanceGiftAllocation(ctx, 0, 0, 3600)
			if err != nil || !all.Coverage.Complete || all.Allocation.PeriodGiftConsumptionMicroUSD != 100_000_000 {
				t.Fatalf("all-group compatibility: %+v %v", all, err)
			}
			internal, err := m.loadFinanceGiftAllocationForScope(ctx, 0, 0, 3600, map[string]bool{"test": false}, map[int64]bool{7: true})
			if err != nil || !internal.Coverage.Complete {
				t.Fatalf("excluded user blocked scope: %+v %v", internal, err)
			}
		})
	}
}

func TestFinanceBusinessGroupsExcludeStrictAndMixedContribution(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	ctx := context.Background()
	first := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "test", Included: false}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		hour, channel := first+int64(i)*3600, 71+i
		if err := m.storeDB.Create(&ChannelSnap{ID: channel, BaseDomain: "4sapi.com", Status: 1}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		groups := []string{"test"}
		if i == 1 {
			groups = []string{"test", "business"}
		}
		if i == 2 {
			groups = []string{"business"}
		}
		for _, g := range groups {
			if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: channel, Grp: g, ModelName: "m", Success: 1, Quota: 500_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
				t.Fatal(err)
			}
		}
		revenue := int64(len(groups)) * 1_000_000
		pub := insertEconomicsReportHour(t, m, "4sapi.com", strings.Repeat("9", 64), hour, channel, revenue, 500_000, 500_000, revenue-500_000)
		if err := m.storeDB.Model(&pub).Update("local_requests", len(groups)).Error; err != nil {
			t.Fatal(err)
		}
		insertEconomicsReportManifest(t, m, "4sapi.com", pub.AccountEpoch, hour, pub)
		if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "4sapi.com", HourTs: hour, BucketSeconds: 3600, Requests: int64(len(groups)), CostUSD: 0.5, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI}).Error; err != nil {
			t.Fatal(err)
		}
	}
	createChannelRechargeVersion(t, m, "4sapi.com", 1, first, 1, 1)
	report, err := m.buildFinanceOperatingReport(ctx, time.Unix(first, 0), time.Unix(first+10800, 0))
	if err != nil {
		t.Fatal(err)
	}
	s := report.Statement
	if s.KnownUserConsumption.MicroUSD != "2000000" || s.KnownInternalTestUpstreamCost.MicroUSD != "500000" || s.InternalTestMixedRows != 1 {
		t.Fatalf("scope totals wrong: %+v", s)
	}
	if s.PairedUserConsumption.MicroUSD != "1000000" || s.PairedCorrectedCost.MicroUSD != "500000" || s.KnownContributionProfit.MicroUSD != "500000" {
		t.Fatalf("excluded/mixed contribution leaked: %+v", s)
	}
	if s.KnownRawCorrectedUpstreamCost.MicroUSD != "1500000" {
		t.Fatal("platform expense disappeared")
	}
	if report.Days[0].Statement.KnownContributionProfit != s.KnownContributionProfit || report.CostDetails[0].KnownContribution != s.KnownContributionProfit {
		t.Fatal("day/domain/total scope mismatch")
	}
	// Changing a display policy must not mutate the immutable source ledger.
	var pubs int64
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Count(&pubs).Error; err != nil || pubs != 3 {
		t.Fatalf("source rewritten: %d %v", pubs, err)
	}
}

func TestFinanceCostScopeDoesNotDoubleCountInternalUsersInExcludedGroup(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "overlap.example")
	ctx := context.Background()
	if err := m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "test", Included: false}).Error; err != nil {
		t.Fatal(err)
	}
	for _, row := range []StabilityHourSample{
		{HourTs: 3600, ChannelID: 1, Grp: "test", Success: 2, Quota: 1_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
		{HourTs: 7200, ChannelID: 2, Grp: "test", RefundRecords: 1, RefundQuota: 500_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	configured := financeConfiguredInternalEvidence{Complete: true, Rows: []FinanceInternalAccountHourFact{
		{HourTs: 3600, ChannelID: 1, UserID: 7, Grp: "test", Requests: 1, ConsumeQuota: 500_000},
		{HourTs: 3600, ChannelID: 3, UserID: 7, Grp: "business", Requests: 1, ConsumeQuota: 500_000},
	}}
	filtered, err := m.financeCostExclusions(ctx, stabilityScope{FromTs: 3600, ToTs: 10800}, configured)
	if err != nil {
		t.Fatal(err)
	}
	var requests, consume, refund int64
	for _, row := range filtered.Rows {
		requests += row.Requests
		consume += row.ConsumeQuota
		refund += row.RefundQuota
	}
	if requests != 3 || consume != 1_500_000 || refund != 500_000 || len(filtered.Rows) != 3 {
		t.Fatalf("overlapping scope: %+v", filtered.Rows)
	}
	if len(configured.Rows) != 2 || configured.Rows[0].UserID != 7 {
		t.Fatal("input mutated")
	}
	evidence, err := m.loadFinanceInternalTestCostEvidence(ctx, stabilityScope{FromTs: 3600, ToTs: 10800}, configured)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.TestPairs != 3 || evidence.UnverifiedPairs != 3 || evidence.Complete {
		t.Fatalf("refund-only or unresolved pair lost: %+v", evidence)
	}
}
