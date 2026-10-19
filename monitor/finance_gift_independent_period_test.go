package monitor

import (
	"context"
	"reflect"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
)

func giftIndependentMissingFirstGroup(t *testing.T, m *Monitor, hour int64) {
	t.Helper()
	event := FinanceGiftBoundaryEvent{SourceLogID: 10, HourTs: hour, UserID: 7, EventAt: hour + 100, Kind: "usage", Quota: 5_000_000, GroupKnown: false}
	event.EvidenceHash = financeGiftBoundaryEventHash(event)
	if _, err := replaceFinanceGiftBoundaryUserHour(context.Background(), m.usageFactsStore(), hour, 7, 30000, "epoch", []FinanceGiftBoundaryEvent{event}); err != nil {
		t.Fatal(err)
	}
}

func TestFinanceGiftIndependentPeriodPreservesHistoricalGap(t *testing.T) {
	m, hour := giftScopeHorizonFixture(t, false, false)
	giftIndependentMissingFirstGroup(t, m, hour)
	ctx := context.Background()
	policies := map[string]bool{"test": false}
	read := func(from int64) financeGiftAllocationResult {
		t.Helper()
		got, err := m.loadFinanceGiftAllocationForScope(ctx, hour, from, hour+14400, policies, nil)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	before := giftMixedMonetarySnapshot(t, m.usageFactsStore())
	full := read(hour)
	if full.Coverage.Complete || full.Coverage.ScopeUnknownEvents != 1 || full.verifiedSuffix == nil || full.verifiedSuffix.Coverage.FromTs != hour+10800 {
		t.Fatalf("history was certified or safe suffix missing: %+v", full.Coverage)
	}
	for _, from := range []int64{hour, hour + 3600, hour + 7200} {
		if read(from).Coverage.Complete {
			t.Fatal("unproven opening wallet was accepted")
		}
	}
	narrow := read(hour + 10800)
	if !narrow.Coverage.Complete || narrow.Coverage.ScopeUnknownEvents != 0 || narrow.Coverage.PriorScopeUnknownEvents != 1 || narrow.Allocation.PeriodGiftConsumptionMicroUSD != 0 || narrow.Allocation.GiftBalanceAtFromMicroUSD != 0 || narrow.Allocation.GiftBalanceAtToMicroUSD != 0 {
		t.Fatalf("proven depleted period blocked or invented gift: %+v %+v", narrow.Coverage, narrow.Allocation)
	}
	if narrow.Allocation != full.verifiedSuffix.Allocation {
		t.Fatal("narrow query differs from full report suffix")
	}
	statement := func() financeStatementView {
		return financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(50_000_000))}
	}
	report := &financeOperatingReport{Statement: statement(),
		Periods: []financePeriodView{{From: hour, To: hour + 14400, Statement: statement()}, {From: hour + 10800, To: hour + 14400, Statement: statement()}},
		Days:    []financeDailyView{{From: hour + 7200, To: hour + 14400, Statement: statement()}, {From: hour + 10800, To: hour + 14400, Statement: statement()}},
	}
	if err := applyFinanceGiftAllocation(&report.Statement, full); err != nil {
		t.Fatal(err)
	}
	if err := applyFinanceGiftBreakdowns(report, full); err != nil {
		t.Fatal(err)
	}
	if report.Statement.OperatingRevenue != nil || report.Periods[0].Statement.OperatingRevenue != nil || report.Days[0].Statement.OperatingRevenue != nil {
		t.Fatal("historical/cross-gap total incorrectly published")
	}
	for _, s := range []financeStatementView{report.Periods[1].Statement, report.Days[1].Statement} {
		if s.OperatingRevenue == nil || s.OperatingRevenue.MicroUSD != "50000000" || s.RegistrationGiftConsumption.MicroUSD != "0" || s.OperatingProfit != nil {
			t.Fatalf("verified period not usable or fabricated cost/profit: %+v", s)
		}
	}
	if before != giftMixedMonetarySnapshot(t, m.usageFactsStore()) {
		t.Fatal("readonly calculation changed facts")
	}
	// Both possible historical scopes yield exactly the same future gift
	// deduction. Do not assume one just to fill the missing historical period.
	for _, group := range []string{"business", "test"} {
		event := FinanceGiftBoundaryEvent{SourceLogID: 10, HourTs: hour, UserID: 7, EventAt: hour + 100, Kind: "usage", Quota: 5_000_000, Group: group, GroupKnown: true}
		event.EvidenceHash = financeGiftBoundaryEventHash(event)
		if _, err := replaceFinanceGiftBoundaryUserHour(ctx, m.usageFactsStore(), hour, 7, 30001, "epoch", []FinanceGiftBoundaryEvent{event}); err != nil {
			t.Fatal(err)
		}
		repaired := read(hour + 10800)
		if !repaired.Coverage.Complete || repaired.Allocation.PeriodGiftConsumptionMicroUSD != narrow.Allocation.PeriodGiftConsumptionMicroUSD || repaired.Coverage.PriorScopeUnknownEvents != 0 {
			t.Fatal("historical repair changed proven future gift")
		}
	}
}

func TestFinanceGiftIndependentPeriodRejectsReopenedOrInvalidWallet(t *testing.T) {
	for _, mode := range []string{"refund", "new_grant", "missing_receipt", "epoch", "content_hash"} {
		t.Run(mode, func(t *testing.T) {
			m, hour := giftScopeHorizonFixture(t, mode == "refund", mode == "new_grant")
			giftIndependentMissingFirstGroup(t, m, hour)
			switch mode {
			case "missing_receipt":
				if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", hour+10800).Update("status", "failed").Error; err != nil {
					t.Fatal(err)
				}
			case "epoch":
				if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", hour).Update("source_epoch", "other").Error; err != nil {
					t.Fatal(err)
				}
			case "content_hash":
				if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", hour).Update("content_hash", "corrupt").Error; err != nil {
					t.Fatal(err)
				}
			}
			got, err := m.loadFinanceGiftAllocationForScope(context.Background(), hour, hour+10800, hour+14400, map[string]bool{"test": false}, nil)
			if got.Coverage.Complete || got.verifiedSuffix != nil || (mode == "content_hash" && err == nil) {
				t.Fatalf("invalid proof bypassed: %+v %v", got.Coverage, err)
			}
		})
	}
}

func TestFinanceGiftIndependentPeriodKeepsOtherWalletsAndExclusions(t *testing.T) {
	ledger := []financecredit.LedgerEvent{
		{UserID: 7, At: 10, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100},
		{UserID: 8, At: 10, Sequence: 2, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100},
		{UserID: 8, At: 10810, Sequence: 3, Kind: financecredit.EventNetUsage, AmountMicroUSD: 30, Scope: "business"},
	}
	original := append([]financecredit.LedgerEvent(nil), ledger...)
	result := financeGiftAllocationResult{Coverage: financeGiftCoverageView{FromTs: 0, ToTs: 14400, ScopeUnknownEvents: 1}}
	suffix, err := financeGiftIndependentSuffix(result, ledger, map[int64]bool{7: true}, map[int64]int64{7: 10800})
	if err != nil || suffix == nil || suffix.Allocation.PeriodGiftConsumptionMicroUSD != 30 || suffix.Allocation.GiftBalanceAtToMicroUSD != 70 || !reflect.DeepEqual(original, ledger) {
		t.Fatalf("another wallet changed: %+v %v", suffix, err)
	}
	result.verifiedSuffix = suffix
	excluded, err := excludeFinanceGiftUsers(result, map[int64]bool{8: true})
	if err != nil || excluded.verifiedSuffix == nil || excluded.verifiedSuffix.Allocation.PeriodGiftConsumptionMicroUSD != 0 || excluded.Coverage.Complete {
		t.Fatal("internal account double deducted or history promoted", err)
	}
	for _, horizons := range []map[int64]int64{nil, {7: 14400}, {7: 10800}} {
		got, err := financeGiftIndependentSuffix(result, ledger, map[int64]bool{7: true, 8: true}, horizons)
		if err != nil || got != nil {
			t.Fatal("one unresolved wallet ignored", err)
		}
	}
}

func TestFinanceGiftIndependentPeriodInactiveWalletIsNotAnEmptyWallet(t *testing.T) {
	for _, corrupt := range []bool{false, true} {
		m, hour := giftScopeHorizonFixture(t, false, false)
		ctx, db := context.Background(), m.usageFactsStore()
		giftIndependentMissingFirstGroup(t, m, hour)
		// Only $14 has been consumed from a $100 gift. The last hour has no
		// usage/refunds, proven by its empty published user-hour receipt.
		for index := int64(1); index < 4; index++ {
			h := hour + index*3600
			var facts []FinanceUserHourFact
			if index < 3 {
				facts = []FinanceUserHourFact{{HourTs: h, UserID: 7, Requests: 1, ConsumeQuota: 1_000_000}}
			}
			if _, err := replaceFinanceUserHourFacts(ctx, db, h, "epoch", facts, 30001); err != nil {
				t.Fatal(err)
			}
			if index < 3 {
				event := FinanceGiftBoundaryEvent{SourceLogID: 10 + index, HourTs: h, UserID: 7, EventAt: h + 100, Kind: "usage", Quota: 1_000_000, Group: "business", GroupKnown: true}
				event.EvidenceHash = financeGiftBoundaryEventHash(event)
				if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, h, 7, 30001, "epoch", []FinanceGiftBoundaryEvent{event}); err != nil {
					t.Fatal(err)
				}
			}
		}
		read := func() (financeGiftAllocationResult, error) {
			return m.loadFinanceGiftAllocationForScope(ctx, hour, hour+10800, hour+14400, map[string]bool{"test": false}, nil)
		}
		got, err := read()
		if err != nil || !got.Coverage.Complete || got.Coverage.ScopeIndependentUserHours != 0 || got.Coverage.PriorScopeUnknownEvents != 1 || got.Allocation.PeriodGiftConsumptionMicroUSD != 0 {
			t.Fatalf("inactive wallet blocked or incorrectly marked depleted: %+v %v", got.Coverage, err)
		}
		if corrupt {
			if err := db.Delete(&FinanceUserHourFact{}, "hour_ts=? AND user_id=?", hour+7200, 7).Error; err != nil {
				t.Fatal(err)
			}
			if got, err := read(); err == nil || got.Coverage.Complete {
				t.Fatal("unpublished row deletion accepted as inactivity")
			}
			continue
		}
		// A later real consumption replaces the empty receipt. Warm caches must
		// not reuse the previous no-activity proof for the same requested range.
		h := hour + 10800
		if _, err := replaceFinanceUserHourFacts(ctx, db, h, "epoch", []FinanceUserHourFact{{HourTs: h, UserID: 7, Requests: 1, ConsumeQuota: 1_000_000}}, 30002); err != nil {
			t.Fatal(err)
		}
		event := FinanceGiftBoundaryEvent{SourceLogID: 13, HourTs: h, UserID: 7, EventAt: h + 100, Kind: "usage", Quota: 1_000_000, Group: "business", GroupKnown: true}
		event.EvidenceHash = financeGiftBoundaryEventHash(event)
		if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, h, 7, 30002, "epoch", []FinanceGiftBoundaryEvent{event}); err != nil {
			t.Fatal(err)
		}
		if got, err := read(); err != nil || got.Coverage.Complete || got.verifiedSuffix != nil {
			t.Fatal("new activity reused stale inactivity proof", err)
		}
	}
}
