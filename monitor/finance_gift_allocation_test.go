package monitor

import (
	"context"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
)

func publishFinanceGiftTestHour(t *testing.T, m *Monitor, hour int64, userID int64, sourceEpoch string, quota int64, logID int64) {
	t.Helper()
	facts := []FinanceUserHourFact{}
	if quota > 0 {
		facts = append(facts, FinanceUserHourFact{HourTs: hour, UserID: userID, Requests: 1, ConsumeQuota: quota})
	}
	if _, err := replaceFinanceUserHourFacts(context.Background(), m.usageFactsStore(), hour, sourceEpoch, facts, hour+10_000); err != nil {
		t.Fatal(err)
	}
	if quota > 0 {
		event := FinanceGiftBoundaryEvent{SourceLogID: logID, HourTs: hour, UserID: userID, EventAt: hour + 100, Kind: "usage", Quota: quota}
		event.EvidenceHash = financeGiftBoundaryEventHash(event)
		if _, err := replaceFinanceGiftBoundaryUserHour(context.Background(), m.usageFactsStore(), hour, userID, hour+10_000, sourceEpoch, []FinanceGiftBoundaryEvent{event}); err != nil {
			t.Fatal(err)
		}
	}
}

func TestFinanceGiftBoundaryReadPreservesOrderIndependentProof(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gift-order.example")
	db := m.usageFactsStore()
	events := []FinanceGiftBoundaryEvent{
		{SourceEpoch: "epoch-1", SourceLogID: 9, HourTs: 3600, UserID: 7, EventAt: 3700, Kind: "usage", Quota: 500_000},
		{SourceEpoch: "epoch-1", SourceLogID: 8, HourTs: 3600, UserID: 7, EventAt: 3650, Kind: "refund", Quota: 100_000},
	}
	for i := range events {
		events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
	}
	if err := db.Create(&events).Error; err != nil {
		t.Fatal(err)
	}
	loaded, err := loadFinanceGiftBoundaryEvents(context.Background(), db, 3600, 7200, []int64{7})
	if err != nil {
		t.Fatal(err)
	}
	if len(loaded) != len(events) || financeGiftBoundaryContentHash(loaded) != financeGiftBoundaryContentHash(events) {
		t.Fatalf("gift boundary proof changed with database row order: loaded=%+v", loaded)
	}
}

func TestLoadFinanceGiftAllocationUsesOpeningBalanceAndExactBoundaries(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gift.example")
	db := m.usageFactsStore()
	publishFinanceGiftTestHour(t, m, 3600, 7, "source-v1", 5_000_000, 2)  // $10 before report range
	publishFinanceGiftTestHour(t, m, 7200, 7, "source-v1", 15_000_000, 3) // $30 in report range
	grant := FinanceCreditEvent{
		SourceLogID: 1, EventAt: 3650, TargetUserID: 7, UserCreatedAt: 1,
		Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000,
		Cohort: "trial_candidate_within_24h", EligibleTrial: true,
	}
	grant.EvidenceHash = financeCreditEventHash(grant)
	if _, err := replaceFinanceCreditHour(context.Background(), db, 3600, 20_000, "source-v1", financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}); err != nil {
		t.Fatal(err)
	}
	// A prior source epoch is retained for audit but must not be counted by the
	// current hour pointer.
	stale := grant
	stale.SourceEpoch, stale.SourceLogID = "source-old", 99
	stale.EvidenceHash = financeCreditEventHash(stale)
	if err := db.Create(&stale).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := replaceFinanceCreditHour(context.Background(), db, 7200, 20_000, "source-v1", financeCreditHourFetch{}); err != nil {
		t.Fatal(err)
	}
	result, err := m.loadFinanceGiftAllocation(context.Background(), 3600, 7200, 10_800)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Coverage.Complete || result.Coverage.ExpectedHours != 2 || result.Coverage.CompletedBoundaryUserHours != 2 || result.Coverage.GiftUsers != 1 || result.Coverage.EligibleGrants != 1 {
		t.Fatalf("coverage=%+v", result.Coverage)
	}
	allocation := result.Allocation
	if allocation.GiftBalanceAtFromMicroUSD != 90_000_000 || allocation.GiftConsumedAtFromMicroUSD != 10_000_000 ||
		allocation.PeriodGiftConsumptionMicroUSD != 30_000_000 || allocation.GiftBalanceAtToMicroUSD != 60_000_000 {
		t.Fatalf("allocation=%+v", allocation)
	}
}

func TestLoadFinanceGiftAllocationFailsClosedWhenBoundaryMissing(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gift-missing.example")
	db := m.usageFactsStore()
	if _, err := replaceFinanceUserHourFacts(context.Background(), db, 0, "source-v1", []FinanceUserHourFact{{HourTs: 0, UserID: 7, Requests: 1, ConsumeQuota: 500_000}}, 10_000); err != nil {
		t.Fatal(err)
	}
	grant := FinanceCreditEvent{SourceLogID: 1, EventAt: 10, TargetUserID: 7, UserCreatedAt: 1, Action: "quota_add", Unit: "USD_quota", ValueMicro: 100_000_000, NetChangeMicro: 100_000_000, Cohort: "trial_candidate_within_24h", EligibleTrial: true}
	grant.EvidenceHash = financeCreditEventHash(grant)
	if _, err := replaceFinanceCreditHour(context.Background(), db, 0, 10_000, "source-v1", financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}); err != nil {
		t.Fatal(err)
	}
	result, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, 3600)
	if err != nil {
		t.Fatal(err)
	}
	if result.Coverage.Complete || result.Coverage.ExpectedBoundaryUserHours != 1 || result.Coverage.CompletedBoundaryUserHours != 0 {
		t.Fatalf("missing boundary was published: %+v", result)
	}
}

func TestLoadFinanceGiftAllocationFailsClosedOnSourceMismatch(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gift-epoch.example")
	db := m.usageFactsStore()
	if _, err := replaceFinanceUserHourFacts(context.Background(), db, 0, "user-source", nil, 10_000); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceFinanceCreditHour(context.Background(), db, 0, 10_000, "credit-source", financeCreditHourFetch{}); err != nil {
		t.Fatal(err)
	}
	result, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, 3600)
	if err != nil {
		t.Fatal(err)
	}
	if result.Coverage.Complete || result.Coverage.SourceMismatchHours != 1 {
		t.Fatalf("source mismatch was published: %+v", result.Coverage)
	}
}

func TestLoadFinanceGiftAllocationPublishesProvenZero(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gift-zero.example")
	if _, err := replaceFinanceUserHourFacts(context.Background(), m.usageFactsStore(), 0, "source-v1", nil, 10_000); err != nil {
		t.Fatal(err)
	}
	if _, err := replaceFinanceCreditHour(context.Background(), m.usageFactsStore(), 0, 10_000, "source-v1", financeCreditHourFetch{}); err != nil {
		t.Fatal(err)
	}
	result, err := m.loadFinanceGiftAllocation(context.Background(), 0, 0, 3600)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Coverage.Complete || result.Allocation.PeriodGiftConsumptionMicroUSD != 0 || result.Allocation != (financecredit.GiftAllocation{From: 0, To: 3600}) {
		t.Fatalf("proven zero mismatch: %+v", result)
	}
}

func TestFinanceGiftSubrangePreservesOpeningBalanceAndRefundReversal(t *testing.T) {
	result := financeGiftAllocationResult{
		Coverage: financeGiftCoverageView{FromTs: 0, ToTs: 7200, Complete: true},
		ledger: []financecredit.LedgerEvent{
			{UserID: 7, At: 10, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000},
			{UserID: 7, At: 1000, Sequence: 2, Kind: financecredit.EventNetUsage, AmountMicroUSD: 60_000_000},
			{UserID: 7, At: 4000, Sequence: 3, Kind: financecredit.EventNetUsage, AmountMicroUSD: -20_000_000},
		},
	}
	first, err := financeGiftSubrange(result, 0, 3600)
	if err != nil {
		t.Fatal(err)
	}
	second, err := financeGiftSubrange(result, 3600, 7200)
	if err != nil {
		t.Fatal(err)
	}
	if first.Allocation.PeriodGiftConsumptionMicroUSD != 60_000_000 || first.Allocation.GiftBalanceAtToMicroUSD != 40_000_000 {
		t.Fatalf("first subrange=%+v", first.Allocation)
	}
	if second.Allocation.GiftBalanceAtFromMicroUSD != 40_000_000 || second.Allocation.GiftConsumedAtFromMicroUSD != 60_000_000 ||
		second.Allocation.PeriodGiftConsumptionMicroUSD != -20_000_000 || second.Allocation.GiftBalanceAtToMicroUSD != 60_000_000 {
		t.Fatalf("second subrange=%+v", second.Allocation)
	}
	if _, err := financeGiftSubrange(result, -3600, 3600); err == nil {
		t.Fatal("subrange outside verified coverage must fail closed")
	}
}
