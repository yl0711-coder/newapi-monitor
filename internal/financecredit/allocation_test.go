package financecredit

import (
	"math"
	"testing"
)

func TestAllocateTrialGiftConsumption(t *testing.T) {
	const usd = int64(1_000_000)
	tests := []struct {
		name      string
		events    []LedgerEvent
		from      int64
		to        int64
		wantGrant int64
		wantUsage int64
		wantGift  int64
		wantOpen  int64
		wantClose int64
		wantUsers int64
	}{
		{
			name: "grant without consumption",
			events: []LedgerEvent{
				{UserID: 1, At: 10, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
			},
			from: 10, to: 30, wantGrant: 100 * usd, wantClose: 100 * usd, wantUsers: 1,
		},
		{
			name: "usage below grant",
			events: []LedgerEvent{
				{UserID: 1, At: 10, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 20, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 40 * usd},
			},
			from: 10, to: 30, wantGrant: 100 * usd, wantUsage: 40 * usd, wantGift: 40 * usd, wantClose: 60 * usd, wantUsers: 1,
		},
		{
			name: "usage capped by grant",
			events: []LedgerEvent{
				{UserID: 1, At: 10, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 20, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 140 * usd},
			},
			from: 10, to: 30, wantGrant: 100 * usd, wantUsage: 140 * usd, wantGift: 100 * usd, wantUsers: 1,
		},
		{
			name: "opening gift seeds period",
			events: []LedgerEvent{
				{UserID: 1, At: 5, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 20, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 30 * usd},
			},
			from: 10, to: 30, wantUsage: 30 * usd, wantGift: 30 * usd, wantOpen: 100 * usd, wantClose: 70 * usd, wantUsers: 1,
		},
		{
			name: "multiple grants accumulate",
			events: []LedgerEvent{
				{UserID: 1, At: 10, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 15, Sequence: 2, Kind: EventTrialGiftGrant, AmountMicroUSD: 150 * usd},
				{UserID: 1, At: 20, Sequence: 3, Kind: EventNetUsage, AmountMicroUSD: 220 * usd},
			},
			from: 10, to: 30, wantGrant: 250 * usd, wantUsage: 220 * usd, wantGift: 220 * usd, wantClose: 30 * usd, wantUsers: 1,
		},
		{
			name: "usage before grant is paid usage",
			events: []LedgerEvent{
				{UserID: 1, At: 10, Sequence: 1, Kind: EventNetUsage, AmountMicroUSD: 40 * usd},
				{UserID: 1, At: 20, Sequence: 2, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
			},
			from: 10, to: 30, wantGrant: 100 * usd, wantUsage: 40 * usd, wantClose: 100 * usd, wantUsers: 1,
		},
		{
			name: "refund reverses earlier gift spend",
			events: []LedgerEvent{
				{UserID: 1, At: 1, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 2, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 70 * usd},
				{UserID: 1, At: 20, Sequence: 3, Kind: EventNetUsage, AmountMicroUSD: -25 * usd},
			},
			from: 10, to: 30, wantUsage: -25 * usd, wantGift: -25 * usd, wantOpen: 30 * usd, wantClose: 55 * usd, wantUsers: 1,
		},
		{
			name: "refund cannot restore more than consumed gift",
			events: []LedgerEvent{
				{UserID: 1, At: 1, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 2, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 20 * usd},
				{UserID: 1, At: 20, Sequence: 3, Kind: EventNetUsage, AmountMicroUSD: -80 * usd},
			},
			from: 10, to: 30, wantUsage: -80 * usd, wantGift: -20 * usd, wantOpen: 80 * usd, wantClose: 100 * usd, wantUsers: 1,
		},
		{
			name: "users never share gift",
			events: []LedgerEvent{
				{UserID: 1, At: 10, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 2, At: 20, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 80 * usd},
			},
			from: 10, to: 30, wantGrant: 100 * usd, wantUsage: 80 * usd, wantClose: 100 * usd, wantUsers: 2,
		},
		{
			name: "events outside range only seed opening",
			events: []LedgerEvent{
				{UserID: 1, At: 1, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
				{UserID: 1, At: 5, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 10 * usd},
				{UserID: 1, At: 40, Sequence: 3, Kind: EventNetUsage, AmountMicroUSD: 30 * usd},
			},
			from: 10, to: 30, wantOpen: 90 * usd, wantClose: 90 * usd,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := AllocateTrialGiftConsumption(test.events, test.from, test.to)
			if err != nil {
				t.Fatal(err)
			}
			if got.EligibleGrantMicroUSD != test.wantGrant || got.PeriodNetUsageMicroUSD != test.wantUsage ||
				got.PeriodGiftConsumptionMicroUSD != test.wantGift || got.GiftBalanceAtFromMicroUSD != test.wantOpen ||
				got.GiftBalanceAtToMicroUSD != test.wantClose || got.Users != test.wantUsers {
				t.Fatalf("allocation=%+v", got)
			}
		})
	}
}

func TestAllocateTrialGiftConsumptionSeriesMatchesIndependentRanges(t *testing.T) {
	const usd = int64(1_000_000)
	events := []LedgerEvent{
		{UserID: 1, At: 5, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100 * usd},
		{UserID: 1, At: 12, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 40 * usd},
		{UserID: 2, At: 18, Sequence: 3, Kind: EventTrialGiftGrant, AmountMicroUSD: 50 * usd},
		{UserID: 2, At: 19, Sequence: 4, Kind: EventNetUsage, AmountMicroUSD: 20 * usd},
		{UserID: 1, At: 32, Sequence: 5, Kind: EventNetUsage, AmountMicroUSD: -15 * usd},
		{UserID: 1, At: 40, Sequence: 6, Kind: EventNetUsage, AmountMicroUSD: 70 * usd},
		{UserID: 2, At: 55, Sequence: 7, Kind: EventNetUsage, AmountMicroUSD: 60 * usd},
	}
	ranges := []AllocationRange{{From: 0, To: 20}, {From: 20, To: 35}, {From: 40, To: 60}}
	got, err := AllocateTrialGiftConsumptionSeries(events, ranges)
	if err != nil {
		t.Fatal(err)
	}
	for index, period := range ranges {
		want, err := AllocateTrialGiftConsumption(events, period.From, period.To)
		if err != nil {
			t.Fatal(err)
		}
		if got[index] != want {
			t.Fatalf("range %d mismatch:\n got=%+v\nwant=%+v", index, got[index], want)
		}
	}
}

func TestAllocateTrialGiftConsumptionSeriesRejectsOverlappingRanges(t *testing.T) {
	_, err := AllocateTrialGiftConsumptionSeries(nil, []AllocationRange{{From: 0, To: 20}, {From: 10, To: 30}})
	if err == nil {
		t.Fatal("overlapping allocation ranges must fail closed")
	}
}

func TestAllocateTrialGiftConsumptionOrdering(t *testing.T) {
	events := []LedgerEvent{
		{UserID: 1, At: 20, Sequence: 2, Kind: EventNetUsage, AmountMicroUSD: 60},
		{UserID: 1, At: 20, Sequence: 1, Kind: EventTrialGiftGrant, AmountMicroUSD: 100},
	}
	got, err := AllocateTrialGiftConsumption(events, 10, 30)
	if err != nil {
		t.Fatal(err)
	}
	if got.PeriodGiftConsumptionMicroUSD != 60 || got.GiftBalanceAtToMicroUSD != 40 {
		t.Fatalf("source ordering not preserved: %+v", got)
	}
}

func TestAllocateTrialGiftConsumptionRejectsInvalidEvidence(t *testing.T) {
	for _, test := range []struct {
		name   string
		events []LedgerEvent
		from   int64
		to     int64
	}{
		{name: "range", from: 10, to: 10},
		{name: "identity", from: 1, to: 2, events: []LedgerEvent{{Kind: EventNetUsage, UserID: 0, At: 1, AmountMicroUSD: 1}}},
		{name: "grant zero", from: 1, to: 2, events: []LedgerEvent{{Kind: EventTrialGiftGrant, UserID: 1, At: 1}}},
		{name: "unknown", from: 1, to: 2, events: []LedgerEvent{{Kind: "cash", UserID: 1, At: 1, AmountMicroUSD: 1}}},
		{name: "min refund", from: 1, to: 2, events: []LedgerEvent{{Kind: EventNetUsage, UserID: 1, At: 1, AmountMicroUSD: math.MinInt64}}},
		{name: "overflow", from: 1, to: 3, events: []LedgerEvent{
			{Kind: EventTrialGiftGrant, UserID: 1, At: 1, Sequence: 1, AmountMicroUSD: math.MaxInt64},
			{Kind: EventTrialGiftGrant, UserID: 1, At: 2, Sequence: 2, AmountMicroUSD: 1},
		}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := AllocateTrialGiftConsumption(test.events, test.from, test.to); err == nil {
				t.Fatal("expected invalid evidence to fail closed")
			}
		})
	}
}
