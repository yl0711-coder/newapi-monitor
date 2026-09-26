package monitor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestFinanceDailyExactProfitExcludesStrictInternalAccount(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "daily-internal.example")
	first := int64(1_788_195_600)
	scope := stabilityScope{FromTs: first, ToTs: first + 7200}
	epoch := strings.Repeat("d", 64)
	for index, sample := range []struct {
		channel int
		group   string
		quota   int64
		revenue int64
		cost    int64
	}{
		{channel: 69, group: "internal", quota: 1_000_000, revenue: 2_000_000, cost: 1_000_000},
		{channel: 70, group: "paid", quota: 5_000_000, revenue: 10_000_000, cost: 3_000_000},
	} {
		hour := first + int64(index)*3600
		if err := m.storeDB.Create(&ChannelSnap{ID: sample.channel, BaseDomain: "daily-internal.example", Status: 1}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: sample.channel, Grp: sample.group, Success: 1,
			Quota: sample.quota, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 1,
			TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		publication := insertEconomicsReportHour(t, m, "daily-internal.example", epoch, hour, sample.channel,
			sample.revenue, sample.cost, sample.cost, sample.revenue-sample.cost)
		insertEconomicsReportManifest(t, m, "daily-internal.example", epoch, hour, publication)
	}
	internalFact := financeInternalTestCostFact{RevenueMicroUSD: 2_000_000, CorrectedCostMicroUSD: 1_000_000,
		UpstreamCostMicroUSD: 1_000_000, ProfitMicroUSD: 1_000_000, Rows: 1}
	evidence := financeInternalTestCostEvidence{SourceComplete: true, SourceScope: scope,
		ExcludeByDay: map[int64]financeInternalTestCostFact{cstDayStart(first): internalFact},
		Events:       []financeInternalTestCostEvent{{HourTs: first, Domain: "daily-internal.example", State: "strict", Fact: internalFact}}}
	accounts := financeConfiguredInternalEvidence{Complete: true, VerifiedScope: scope, Accounts: 1,
		Rows: []FinanceInternalAccountHourFact{{HourTs: first, UserID: 7, ChannelID: 69, Grp: "internal", Requests: 1, ConsumeQuota: 1_000_000}}}
	days, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, evidence, accounts, nil)
	if err != nil || len(days) != 1 {
		t.Fatalf("daily exact profit: days=%d err=%v", len(days), err)
	}
	s := days[0].Statement
	if s.KnownUserConsumption.MicroUSD != "10000000" || s.PairedUserConsumption.MicroUSD != "10000000" ||
		s.KnownContributionProfit.MicroUSD != "7000000" || s.ContributionProfit == nil || s.ContributionProfit.MicroUSD != "7000000" {
		t.Fatalf("strict internal traffic incorrectly blocked exact customer profit: %+v", s)
	}
	// The new customer-only equality must still reject a ledger that contains
	// revenue from a group explicitly excluded from business reporting.
	excluded, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, evidence, accounts, map[string]bool{"paid": false})
	if err != nil || len(excluded) != 1 || excluded[0].Statement.ContributionProfit != nil {
		t.Fatalf("non-business group was published as exact profit: days=%+v err=%v", excluded, err)
	}
	partial := evidence
	partial.SourceComplete = false
	partial.SourceScope = stabilityScope{}
	missingCost, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, partial, accounts, nil)
	if err != nil || len(missingCost) != 1 || missingCost[0].Statement.ContributionProfit != nil {
		t.Fatalf("incomplete internal-cost evidence was published as exact profit: days=%+v err=%v", missingCost, err)
	}
	if err := m.storeDB.Delete(&StabilityHourIngestState{}, "hour_ts = ?", first+3600).Error; err != nil {
		t.Fatal(err)
	}
	missingUsage, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, evidence, accounts, nil)
	if err != nil || len(missingUsage) != 1 || missingUsage[0].Statement.ContributionProfit != nil {
		t.Fatalf("incomplete user coverage was published as exact profit: days=%+v err=%v", missingUsage, err)
	}
}

func TestFinanceDailyInternalCostPreservesEvidenceState(t *testing.T) {
	for _, mode := range []string{"strict", "mixed", "unverified", "source_gap", "account_gap", "verified_prefix", "zero", "no_events"} {
		t.Run(mode, func(t *testing.T) {
			m, scope, _ := dailyBillFixture(t)
			fact := financeInternalTestCostFact{Rows: 1, UpstreamCostMicroUSD: 4, CorrectedCostMicroUSD: 4}
			if mode == "zero" {
				fact.UpstreamCostMicroUSD, fact.CorrectedCostMicroUSD = 0, 0
			}
			evidence := financeInternalTestCostEvidence{SourceComplete: true, SourceScope: scope,
				Events: []financeInternalTestCostEvent{
					{HourTs: scope.FromTs, Domain: "hour.example", State: "strict", Fact: fact},
					{HourTs: scope.FromTs + 86400, Domain: "hour.example", State: "strict", Fact: fact},
				}}
			accounts := financeConfiguredInternalEvidence{Complete: true, VerifiedScope: scope}
			if mode == "mixed" || mode == "unverified" {
				evidence.Events = append(evidence.Events, financeInternalTestCostEvent{HourTs: scope.FromTs + 3600, Domain: "hour.example", State: mode})
			}
			if mode == "source_gap" {
				evidence.SourceComplete = false
				evidence.SourceScope = stabilityScope{}
			}
			if mode == "account_gap" {
				accounts.Complete = false
				accounts.VerifiedScope = stabilityScope{}
			}
			if mode == "verified_prefix" {
				evidence.SourceComplete, accounts.Complete = false, false
				evidence.SourceScope.ToTs = scope.FromTs + 86400
				accounts.VerifiedScope = evidence.SourceScope
			}
			if mode == "no_events" {
				evidence.Events = nil
			}
			days, err := m.buildFinanceDailyViews(context.Background(), scope, scope.ToTs+86400, evidence, accounts, nil)
			if err != nil {
				t.Fatal(err)
			}
			for index, day := range days {
				s := day.Statement
				wantMixed, wantUnverified := int64(0), int64(0)
				if index == 0 && mode == "mixed" {
					wantMixed = 1
				}
				if index == 0 && mode == "unverified" {
					wantUnverified = 1
				}
				if s.InternalTestMixedRows != wantMixed || s.InternalTestUnverifiedPairs != wantUnverified {
					t.Fatalf("daily evidence counters lost: mixed=%d unverified=%d, want %d/%d", s.InternalTestMixedRows, s.InternalTestUnverifiedPairs, wantMixed, wantUnverified)
				}
				complete := wantMixed == 0 && wantUnverified == 0 && mode != "source_gap" && mode != "account_gap"
				if mode == "verified_prefix" && index == 1 {
					complete = false
				}
				if day.InternalCostComplete != complete {
					t.Fatal("daily coverage flag does not match evidence")
				}
				if (s.InternalTestUpstreamCost != nil) != (complete && mode != "no_events") {
					t.Fatalf("daily internal evidence completeness lost: exact=%v want=%v", s.InternalTestUpstreamCost, complete)
				}
				want := economicsMoney(fact.CorrectedCostMicroUSD)
				if mode == "no_events" {
					want = channelEconomicsMoneyView{}
				}
				if s.KnownInternalTestUpstreamCost != want {
					t.Fatal("independently identified amount changed")
				}
				if s.KnownCorrectedUpstreamCost.MicroUSD != "" || s.ContributionProfit != nil {
					t.Fatal("internal evidence fabricated missing gross cost or profit")
				}
			}
			payload, err := json.Marshal(days)
			if err != nil {
				t.Fatal(err)
			}
			var cached []financeDailyView
			if err := json.Unmarshal(payload, &cached); err != nil {
				t.Fatal(err)
			}
			for index := range days {
				if cached[index].InternalCostComplete != days[index].InternalCostComplete || cached[index].Statement.InternalTestMixedRows != days[index].Statement.InternalTestMixedRows || cached[index].Statement.InternalTestUnverifiedPairs != days[index].Statement.InternalTestUnverifiedPairs {
					t.Fatal("cache lost daily evidence state")
				}
			}
		})
	}
}
