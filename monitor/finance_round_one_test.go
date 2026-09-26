package monitor

import (
	"context"
	"testing"
	"time"
)

func TestFinancePriorGiftChangeInvalidatesLaterReport(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "scope.example")
	ctx := context.Background()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	seed := time.Date(2026, 5, 1, 0, 0, 0, 0, loc).Unix()
	from := time.Date(2026, 9, 1, 0, 0, 0, 0, loc).Unix()
	db := m.usageFactsStore()
	row := FinanceCreditHourState{HourTs: seed, SourceEpoch: "source", Status: "complete", EligibleTrialRows: 1, UpdatedAt: 10, ContentHash: "before"}
	if err := db.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	before, err := m.financeReportSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	monthBefore, err := m.financeReportPeriodSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	// Same count/timestamp: changed monetary evidence must still invalidate.
	if err := db.Model(&row).Update("content_hash", "after").Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("prior gift evidence did not invalidate later report")
	}
	monthAfter, err := m.financeReportPeriodSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if monthBefore != monthAfter {
		t.Fatal("base monthly facts do not contain gift allocation; no need to rebuild them")
	}
}

func TestFinancePlatformProfitIncludesInternalSpend(t *testing.T) {
	s := financeStatementView{
		OperatingRevenue:              financeMoneyPointer(economicsMoney(100_000_000)),
		RawCorrectedUpstreamCost:      financeMoneyPointer(economicsMoney(60_000_000)),
		CorrectedUpstreamCost:         financeMoneyPointer(economicsMoney(50_000_000)),
		KnownInternalTestUpstreamCost: economicsMoney(10_000_000),
	}
	cur := financeCURCostView{Loaded: true, IncludedDays: 1, KnownCost: economicsMoney(5_000_000), ExactCost: financeMoneyPointer(economicsMoney(5_000_000))}
	applyFinanceCURCost(&s, cur)
	if s.OperatingProfit == nil || s.OperatingProfit.MicroUSD != "35000000" {
		t.Fatalf("platform profit must retain internal spend: %+v", s.OperatingProfit)
	}
	s.CorrectedUpstreamCost = nil // No need to split internal cost to know the total.
	applyFinanceCURCost(&s, cur)
	if s.OperatingProfit == nil || s.OperatingProfit.MicroUSD != "35000000" {
		t.Fatal("mixed internal attribution blocked complete platform cost")
	}
	s.RawCorrectedUpstreamCost = nil
	applyFinanceCURCost(&s, cur)
	if s.OperatingProfit != nil {
		t.Fatal("missing total cost reused previous profit")
	}
}

func TestFinanceMissingCorrectionIsNotZeroInMonthRow(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "missing-correction.example")
	start := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{
		Domain: "missing-correction.example", HourTs: start, BucketSeconds: 3600,
		Requests: 1, Quota: 500_000, CostUSD: 1, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI,
	}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildFinanceOperatingReport(context.Background(), time.Unix(start, 0), time.Unix(start+3600, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Periods) != 1 || report.Periods[0].UpstreamCoverage.AvailableDomains != 1 || report.Periods[0].UpstreamCoverage.CorrectedDomains != 0 {
		t.Fatalf("fixture must contain a bill without correction evidence: %+v", report.UpstreamCoverage)
	}
	for _, statement := range []financeStatementView{report.Statement, report.Periods[0].Statement} {
		if statement.KnownUpstreamBilledCost.MicroUSD != "1000000" || statement.KnownRawCorrectedUpstreamCost.MicroUSD != "" || statement.RawCorrectedUpstreamCost != nil {
			t.Fatalf("missing correction presented as zero: %+v", statement)
		}
	}
}

func TestFinanceBilledTotalDoesNotRequireCorrectionEvidence(t *testing.T) {
	bill := economicsMoney(1_000_000)
	coverage := financeUpstreamCoverageView{RelevantDomains: 1, AvailableDomains: 1, CompleteDomains: 1, UsageEnabledDomains: 1}
	statement, err := aggregateFinancePeriods([]financePeriodView{{
		Statement:        financeStatementView{KnownUpstreamBilledCost: bill, UpstreamBilledCost: financeMoneyPointer(bill)},
		UpstreamCoverage: coverage,
	}}, StabilityDataCoverage{}, coverage)
	if err != nil {
		t.Fatal(err)
	}
	if statement.UpstreamBilledCost == nil || statement.UpstreamBilledCost.MicroUSD != bill.MicroUSD || statement.RawCorrectedUpstreamCost != nil {
		t.Fatalf("complete raw bills incorrectly depend on recharge correction: %+v", statement)
	}
}
