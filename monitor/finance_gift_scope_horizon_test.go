package monitor

import (
	"context"
	"errors"
	"math"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
	"gorm.io/gorm"
)

func TestFinanceGiftScopeHorizonRequiresIrreversibleDepletion(t *testing.T) {
	grant := financecredit.LedgerEvent{UserID: 7, At: 3590, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000}
	base := []FinanceUserHourFact{
		{UserID: 7, HourTs: 0, Requests: 1, ConsumeQuota: 100_000_000}, // May be before the grant.
		{UserID: 7, HourTs: 3600, Requests: 1, ConsumeQuota: 25_000_000},
		{UserID: 7, HourTs: 7200, Requests: 1, ConsumeQuota: 25_000_000},
		{UserID: 7, HourTs: 10800, Requests: 1, ConsumeQuota: 50_000_000},
	}
	for _, tc := range []struct {
		name string
		edit func([]FinanceUserHourFact, *[]financecredit.LedgerEvent)
		want int64
	}{
		{"depleted", nil, 10800},
		{"not_depleted", func(f []FinanceUserHourFact, _ *[]financecredit.LedgerEvent) {
			f[1].ConsumeQuota, f[2].ConsumeQuota, f[3].ConsumeQuota = 0, 0, 0
		}, 0},
		{"late_refund", func(f []FinanceUserHourFact, _ *[]financecredit.LedgerEvent) {
			f[3].RefundRecords, f[3].RefundQuota = 1, 500_000
		}, 0},
		{"zero_refund", func(f []FinanceUserHourFact, _ *[]financecredit.LedgerEvent) { f[3].RefundRecords = 1 }, 0},
		{"prior_refund", func(f []FinanceUserHourFact, _ *[]financecredit.LedgerEvent) {
			f[0].RefundRecords, f[0].RefundQuota = 1, 500_000
		}, 0},
		{"new_grant_reopens", func(_ []FinanceUserHourFact, g *[]financecredit.LedgerEvent) {
			*g = append(*g, financecredit.LedgerEvent{UserID: 7, At: 10810, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000})
		}, 0},
		{"grant_same_hour_excluded", func(_ []FinanceUserHourFact, g *[]financecredit.LedgerEvent) { (*g)[0].At = 7190 }, 14400},
	} {
		t.Run(tc.name, func(t *testing.T) {
			facts, grants := append([]FinanceUserHourFact(nil), base...), []financecredit.LedgerEvent{grant}
			if tc.edit != nil {
				tc.edit(facts, &grants)
			}
			before := append([]FinanceUserHourFact(nil), facts...)
			got, err := financeGiftScopeHorizons(context.Background(), facts, grants)
			if err != nil || got[7] != tc.want {
				t.Fatalf("horizon=%v want=%d err=%v", got, tc.want, err)
			}
			if !reflect.DeepEqual(before, facts) {
				t.Fatal("horizon calculation changed source facts")
			}
		})
	}
	// Input order, another wallet, and zero/quota-free hours cannot change the proof.
	facts := []FinanceUserHourFact{base[3], base[1], base[0], base[2], {UserID: 8, HourTs: 10800, Requests: 1, ConsumeQuota: 5_000_000}}
	grants := []financecredit.LedgerEvent{grant, {UserID: 8, At: 10, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000}}
	got, err := financeGiftScopeHorizons(context.Background(), facts, grants)
	if err != nil || !reflect.DeepEqual(got, map[int64]int64{7: 10800}) {
		t.Fatalf("wallet isolation/order lost: %v %v", got, err)
	}
}

func TestFinanceGiftScopeHorizonRejectsUnprovenInputs(t *testing.T) {
	grant := financecredit.LedgerEvent{UserID: 7, At: 10, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000}
	fact := FinanceUserHourFact{UserID: 7, HourTs: 3600, Requests: 1, ConsumeQuota: 50_000_000}
	for _, tc := range []struct {
		name   string
		facts  []FinanceUserHourFact
		grants []financecredit.LedgerEvent
	}{
		{"duplicate_hour", []FinanceUserHourFact{fact, fact}, []financecredit.LedgerEvent{grant}},
		{"bad_fact", []FinanceUserHourFact{{UserID: 7, HourTs: 3600, Requests: 0, ConsumeQuota: 1}}, []financecredit.LedgerEvent{grant}},
		{"bad_grant", []FinanceUserHourFact{fact}, []financecredit.LedgerEvent{{UserID: 7, Kind: financecredit.EventNetUsage, AmountMicroUSD: 1}}},
		{"overflow", []FinanceUserHourFact{fact}, []financecredit.LedgerEvent{{UserID: 7, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: math.MaxInt64}, grant}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := financeGiftScopeHorizons(context.Background(), tc.facts, tc.grants); err == nil {
				t.Fatal("invalid proof produced a horizon")
			}
		})
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := financeGiftScopeHorizons(ctx, []FinanceUserHourFact{fact}, []financecredit.LedgerEvent{grant}); err == nil {
		t.Fatal("cancellation bypassed")
	}
}

func giftScopeHorizonFixture(t *testing.T, refund, laterGrant bool) (*Monitor, int64) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "gift-horizon.example")
	t.Cleanup(m.Close)
	ctx, db := context.Background(), m.usageFactsStore()
	hour, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	for index := int64(0); index < 4; index++ {
		h := hour + index*3600
		quota := int64(25_000_000)
		if index == 0 {
			quota = 5_000_000 // $10 gift used, then two whole $50 hours guarantee exhaustion.
		}
		fact := FinanceUserHourFact{HourTs: h, UserID: 7, Requests: 1, ConsumeQuota: quota}
		events := []FinanceGiftBoundaryEvent{{SourceLogID: index + 10, HourTs: h, UserID: 7, EventAt: h + 100, Kind: "usage", Quota: quota, Group: "business", GroupKnown: index < 3}}
		if index == 3 && refund {
			fact.RefundRecords, fact.RefundQuota = 1, 1_000_000
			events = append(events, FinanceGiftBoundaryEvent{SourceLogID: 20, HourTs: h, UserID: 7, EventAt: h + 200, Kind: "refund", Quota: fact.RefundQuota, GroupKnown: false})
		}
		if _, err := replaceFinanceUserHourFacts(ctx, db, h, "epoch", []FinanceUserHourFact{fact}, 30000); err != nil {
			t.Fatal(err)
		}
		credits := financeCreditHourFetch{}
		if index == 0 || (index == 3 && laterGrant) {
			grant := FinanceCreditEvent{SourceLogID: index + 1, EventAt: h + 10, TargetUserID: 7, Action: "quota_add", Unit: "USD_quota", NetChangeMicro: 100_000_000, EligibleTrial: true}
			grant.EvidenceHash = financeCreditEventHash(grant)
			credits = financeCreditHourFetch{Events: []FinanceCreditEvent{grant}, SourceRows: 1}
		}
		if _, err := replaceFinanceCreditHour(ctx, db, h, 30000, "epoch", credits); err != nil {
			t.Fatal(err)
		}
		for i := range events {
			events[i].EvidenceHash = financeGiftBoundaryEventHash(events[i])
		}
		if _, err := replaceFinanceGiftBoundaryUserHour(ctx, db, h, 7, 30000, "epoch", events); err != nil {
			t.Fatal(err)
		}
	}
	if err := db.Create(&FinanceFactSyncState{ID: financeFactSyncStateID, SourceEpoch: "epoch", StartHourTs: hour, NextHourTs: hour + 14400, LastCompletedHour: hour + 10800, Status: "caught_up"}).Error; err != nil {
		t.Fatal(err)
	}
	return m, hour
}

func TestFinanceGiftScopeHorizonDoesNotRequireIrrelevantHistoricalGroups(t *testing.T) {
	m, hour := giftScopeHorizonFixture(t, false, false)
	ctx := context.Background()
	policies := map[string]bool{"test": false}
	full, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+14400, policies, nil)
	if err != nil || !full.Coverage.Complete || full.Coverage.ScopeUnknownEvents != 0 || full.Coverage.ScopeIndependentUserHours != 1 || full.Coverage.ScopeIndependentRows != 1 {
		t.Fatalf("gift-neutral history blocked revenue: %+v %v", full.Coverage, err)
	}
	if full.Allocation.PeriodGiftConsumptionMicroUSD != 100_000_000 || full.Allocation.GiftBalanceAtToMicroUSD != 0 {
		t.Fatalf("gift changed: %+v", full.Allocation)
	}
	var splitGift int64
	for from := hour; from < hour+14400; from += 3600 {
		part, err := financeGiftSubrange(full, from, from+3600)
		if err != nil {
			t.Fatal(err)
		}
		splitGift += part.Allocation.PeriodGiftConsumptionMicroUSD
	}
	if splitGift != full.Allocation.PeriodGiftConsumptionMicroUSD {
		t.Fatal("day/hour gifts do not reconcile")
	}
	// Repairing a gift-neutral historical group cannot change any financial amount.
	event := FinanceGiftBoundaryEvent{SourceLogID: 13, HourTs: hour + 10800, UserID: 7, EventAt: hour + 10900, Kind: "usage", Quota: 25_000_000, Group: "test", GroupKnown: true}
	event.EvidenceHash = financeGiftBoundaryEventHash(event)
	if _, err := replaceFinanceGiftBoundaryUserHour(ctx, m.usageFactsStore(), event.HourTs, 7, 30001, "epoch", []FinanceGiftBoundaryEvent{event}); err != nil {
		t.Fatal(err)
	}
	repaired, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+14400, policies, nil)
	if err != nil || repaired.Allocation != full.Allocation {
		t.Fatalf("irrelevant repair changed money: %+v %v", repaired.Allocation, err)
	}
	// Irrelevance does not excuse an incomplete monetary proof or mismatched epoch.
	if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", event.HourTs).Update("status", "failed").Error; err != nil {
		t.Fatal(err)
	}
	failed, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+14400, policies, nil)
	if err != nil || failed.Coverage.Complete {
		t.Fatal("horizon concealed an incomplete hour", err)
	}
}

func TestFinanceGiftScopeHorizonRefundOrNewGrantInvalidatesShortcut(t *testing.T) {
	for _, tc := range []struct {
		name          string
		refund, grant bool
	}{{"refund", true, false}, {"grant", false, true}} {
		t.Run(tc.name, func(t *testing.T) {
			m, hour := giftScopeHorizonFixture(t, tc.refund, tc.grant)
			got, err := m.loadFinanceGiftAllocationForScope(context.Background(), hour, hour, hour+14400, map[string]bool{"test": false}, nil)
			if err != nil || got.Coverage.Complete || got.Coverage.ScopeUnknownEvents == 0 || got.Coverage.ScopeIndependentUserHours != 0 {
				t.Fatalf("wallet can reopen but unknown scope was ignored: %+v %v", got.Coverage, err)
			}
		})
	}
}

func TestFinanceGiftScopeHorizonReportPublishesRevenueWithoutChangingUsageOrCosts(t *testing.T) {
	m, hour := giftScopeHorizonFixture(t, false, false)
	ctx := context.Background()
	if err := m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "test", Included: false}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, BaseDomain: "gift-horizon.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	for index := int64(0); index < 4; index++ {
		h := hour + index*3600
		quota := int64(25_000_000)
		group := "business"
		if index == 0 {
			quota = 5_000_000
		} else if index == 3 {
			group = "test" // Excluded tail still depletes cash, not business revenue.
		}
		if err := m.storeDB.Create(&StabilityHourSample{HourTs: h, ChannelID: 1, Grp: group, ModelName: "m", Success: 1, Quota: quota, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: h, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	build := func() *financeOperatingReport {
		t.Helper()
		r, err := m.buildFinanceOperatingReport(ctx, time.Unix(hour, 0), time.Unix(hour+14400, 0))
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	cold, warm := build(), build()
	for _, report := range []*financeOperatingReport{cold, warm} {
		if !report.GiftCoverage.Complete || report.GiftCoverage.ScopeIndependentUserHours != 1 || len(report.Periods) != 1 || len(report.Days) != 1 {
			t.Fatalf("inconsistent report coverage: %+v", report.GiftCoverage)
		}
		for _, statement := range []financeStatementView{report.Statement, report.Periods[0].Statement, report.Days[0].Statement} {
			if statement.UserConsumption == nil || statement.UserConsumption.MicroUSD != "110000000" || statement.RegistrationGiftConsumption == nil || statement.RegistrationGiftConsumption.MicroUSD != "100000000" || statement.OperatingRevenue == nil || statement.OperatingRevenue.MicroUSD != "10000000" {
				t.Fatalf("gift-only ledger altered revenue/usage reconciliation: %+v", statement)
			}
			if statement.RawCorrectedUpstreamCost != nil || statement.OperatingProfit != nil {
				t.Fatal("missing cost fabricated profit")
			}
		}
	}
	// Unscoped legacy allocation continues reading all requests: the shortcut
	// must not change the established all-group net-usage API semantics.
	legacy, err := m.loadFinanceGiftAllocation(ctx, hour, hour, hour+14400)
	if err != nil || !legacy.Coverage.Complete || legacy.Allocation.PeriodNetUsageMicroUSD != 160_000_000 || legacy.Coverage.ScopeIndependentUserHours != 0 {
		t.Fatalf("legacy allocation changed: %+v %v", legacy, err)
	}
}

func TestFinanceGiftScopeHorizonRejectsUnpublishedAggregateChanges(t *testing.T) {
	for _, scenario := range []string{"missing_tail", "quota_changed", "deleted_refund", "another_user"} {
		t.Run(scenario, func(t *testing.T) {
			m, hour := giftScopeHorizonFixture(t, false, false)
			db, ctx := m.usageFactsStore(), context.Background()
			tail := hour + 10800
			switch scenario {
			case "missing_tail":
				if err := db.Where("hour_ts=? AND user_id=?", tail, 7).Delete(&FinanceUserHourFact{}).Error; err != nil {
					t.Fatal(err)
				}
			case "quota_changed":
				if err := db.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", tail, 7).Update("consume_quota", 1).Error; err != nil {
					t.Fatal(err)
				}
			case "deleted_refund":
				// A refund present in the publication proof but absent from both
				// selected aggregates/boundary metadata cannot authorize a shortcut.
				if err := db.Model(&FinanceUserHourState{}).Where("hour_ts=?", tail).Updates(map[string]any{"refund_records": 1, "refund_quota": 500_000}).Error; err != nil {
					t.Fatal(err)
				}
			case "another_user":
				if err := db.Create(&FinanceUserHourFact{HourTs: tail, UserID: 8, Requests: 1, ConsumeQuota: 500_000}).Error; err != nil {
					t.Fatal(err)
				}
			}
			got, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+14400, map[string]bool{"test": false}, nil)
			if got.Coverage.Complete || got.Coverage.ScopeIndependentUserHours != 0 {
				t.Fatalf("unverified monetary aggregate authorized gift horizon: coverage=%+v err=%v", got.Coverage, err)
			}
			if scenario == "quota_changed" {
				// Existing boundary/aggregate reconciliation rejects this case
				// before a horizon is even considered; keep that path intact.
				if err != nil || got.Coverage.CompletedBoundaryUserHours >= got.Coverage.ExpectedBoundaryUserHours {
					t.Fatal("original monetary completeness guard changed", err)
				}
			} else if !errors.Is(err, errFinanceFactsChanged) {
				t.Fatalf("missing aggregate content proof rejection: %v", err)
			}
		})
	}
}

func TestFinanceGiftScopeHorizonRejectsFactsChangingBetweenProofReads(t *testing.T) {
	m, hour := giftScopeHorizonFixture(t, false, false)
	db := m.usageFactsStore()
	changed := false
	const callback = "test:gift-horizon-fact-race"
	if err := db.Callback().Row().Before("gorm:row").Register(callback, func(tx *gorm.DB) {
		if !changed && tx.Statement.Table == "finance_user_hour_facts" {
			changed = true
			if err := db.Model(&FinanceUserHourFact{}).Where("hour_ts=? AND user_id=?", hour+10800, 7).Update("consume_quota", 1).Error; err != nil {
				t.Fatal(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = db.Callback().Row().Remove(callback) })
	got, err := m.loadFinanceGiftAllocationForScope(context.Background(), hour, hour, hour+14400, map[string]bool{"test": false}, nil)
	if !changed || !errors.Is(err, errFinanceFactsChanged) || got.Coverage.Complete || got.Coverage.ScopeIndependentUserHours != 0 {
		t.Fatalf("mixed fact/proof versions authorized a horizon: changed=%t coverage=%+v err=%v", changed, got.Coverage, err)
	}
}
