package monitor

import (
	"context"
	"encoding/json"
	"testing"
)

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
