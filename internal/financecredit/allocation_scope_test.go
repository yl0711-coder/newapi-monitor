package financecredit

import (
	"reflect"
	"testing"
)

func TestGiftAllocationExcludedScopeStillDepletesWallet(t *testing.T) {
	events := []LedgerEvent{
		{UserID: 1, At: 10, Kind: EventTrialGiftGrant, AmountMicroUSD: 100},
		{UserID: 1, At: 20, Kind: EventNetUsage, AmountMicroUSD: 60, Scope: "test", Excluded: true},
		{UserID: 1, At: 30, Kind: EventNetUsage, AmountMicroUSD: 100, Scope: "business"},
		{UserID: 1, At: 40, Kind: EventNetUsage, AmountMicroUSD: -30, Scope: "test", Excluded: true},
		{UserID: 1, At: 50, Kind: EventNetUsage, AmountMicroUSD: -80, Scope: "business"},
	}
	first, err := AllocateTrialGiftConsumption(events, 0, 40)
	if err != nil {
		t.Fatal(err)
	}
	if first.PeriodGiftConsumptionMicroUSD != 40 || first.PeriodNetUsageMicroUSD != 100 || first.GiftBalanceAtToMicroUSD != 0 {
		t.Fatalf("wrong business allocation: %+v", first)
	}
	ranges := []AllocationRange{{From: 0, To: 40}, {From: 40, To: 60}}
	series, err := AllocateTrialGiftConsumptionSeries(events, ranges)
	if err != nil {
		t.Fatal(err)
	}
	for i, r := range ranges {
		single, err := AllocateTrialGiftConsumption(events, r.From, r.To)
		if err != nil || !reflect.DeepEqual(single, series[i]) {
			t.Fatalf("series differs: single=%+v series=%+v err=%v", single, series[i], err)
		}
	}
	if series[1].PeriodGiftConsumptionMicroUSD != -40 || series[1].GiftBalanceAtToMicroUSD != 70 {
		t.Fatalf("refund crossed scope: %+v", series[1])
	}
	whole, err := AllocateTrialGiftConsumption(events, 0, 60)
	if err != nil || whole.PeriodGiftConsumptionMicroUSD != series[0].PeriodGiftConsumptionMicroUSD+series[1].PeriodGiftConsumptionMicroUSD {
		t.Fatal("period sums differ")
	}
	if events[1].AmountMicroUSD != 60 {
		t.Fatal("allocator mutated evidence")
	}
}

func TestGiftAllocationRejectsContradictoryScopePolicy(t *testing.T) {
	events := []LedgerEvent{{UserID: 1, Kind: EventNetUsage, AmountMicroUSD: 1, Scope: "g"}, {UserID: 1, Kind: EventNetUsage, AmountMicroUSD: 1, Scope: "g", Excluded: true}}
	if _, err := AllocateTrialGiftConsumption(events, 0, 10); err == nil {
		t.Fatal("inconsistent scope accepted")
	}
	if _, err := AllocateTrialGiftConsumptionSeries(events, []AllocationRange{{From: 0, To: 10}}); err == nil {
		t.Fatal("inconsistent series scope accepted")
	}
}
