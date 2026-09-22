package monitor

import (
	"context"
	"math"
	"testing"
)

func TestFinanceRechargeDeductionPricing(t *testing.T) {
	for _, mode := range []string{"matched", "same_ratio_new_version", "changed_ratio", "missing_version", "future_version", "invalid_version", "mid_hour_change", "tiny_mid_hour_change", "missing_events", "wrong_amount", "wrong_rows", "negative_raw", "zero", "round_half_micro", "overflow_hour"} {
		t.Run(mode, func(t *testing.T) {
			fact := financeInternalTestCostFact{Rows: 1, UpstreamCostMicroUSD: 2_000_000, CorrectedCostMicroUSD: 1_000_000}
			versions := []channelRechargeVersion{{Version: 1, EffectiveAt: 0, Paid: 1, Credit: 2, Valid: true}}
			event := financeInternalTestCostEvent{FinanceVersion: 1, HourTs: 3600, State: "strict", Domain: "a", Fact: fact}
			want := ""
			switch mode {
			case "same_ratio_new_version":
				versions = append(versions, channelRechargeVersion{Version: 2, EffectiveAt: 3600, Paid: 2, Credit: 4, Valid: true})
			case "changed_ratio":
				versions = append(versions, channelRechargeVersion{Version: 2, EffectiveAt: 3600, Paid: 1, Credit: 1, Valid: true})
				want = "cost_pricing_basis_mismatch"
			case "missing_version":
				event.FinanceVersion = 0
				want = "cost_pricing_history_missing"
			case "future_version":
				versions[0].EffectiveAt = 7200
				want = "cost_pricing_history_missing"
			case "invalid_version":
				versions[0].Valid = false
				want = "cost_pricing_history_missing"
			case "mid_hour_change":
				versions = append(versions, channelRechargeVersion{Version: 2, EffectiveAt: 5400, Paid: 1, Credit: 1, Valid: true})
				want = "cost_pricing_window_incomplete"
			case "tiny_mid_hour_change":
				versions = append(versions, channelRechargeVersion{Version: 2, EffectiveAt: 5400, Paid: 1.000000000001, Credit: 2, Valid: true})
				want = "cost_pricing_window_incomplete"
			case "missing_events":
				want = "cost_exclusion_evidence_mismatch"
			case "wrong_amount":
				event.Fact.CorrectedCostMicroUSD++
				want = "cost_pricing_amount_mismatch"
			case "wrong_rows":
				fact.Rows++
				want = "cost_exclusion_evidence_mismatch"
			case "negative_raw":
				event.Fact.UpstreamCostMicroUSD = -1
				want = "cost_pricing_amount_mismatch"
			case "zero":
				fact.UpstreamCostMicroUSD, fact.CorrectedCostMicroUSD = 0, 0
				event.Fact = fact
			case "round_half_micro":
				fact.UpstreamCostMicroUSD, fact.CorrectedCostMicroUSD = 1, 1
				event.Fact = fact
			case "overflow_hour":
				event.HourTs = math.MaxInt64 - math.MaxInt64%3600
				want = "cost_exclusion_evidence_mismatch"
			}
			events := []financeInternalTestCostEvent{event}
			if mode == "missing_events" {
				events = nil
			}
			if got := financeRechargeDeductionStatus(events, fact, versions); got != want {
				t.Fatalf("got %q want %q", got, want)
			}
		})
	}
}

func TestFinanceInternalCostEvidenceLoadsPublicationPricingVersion(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	publication := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs, 1, 0, 2, 1, -1)
	publication.FinanceVersion = 7
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", publication.PublicationID).Update("finance_version", publication.FinanceVersion).Error; err != nil {
		t.Fatal(err)
	}
	insertEconomicsReportManifest(t, m, publication.Domain, publication.AccountEpoch, publication.HourTs, publication)
	if err := m.storeDB.Create(&ChannelTestHourSample{HourTs: scope.FromTs, ChannelID: 1, Requests: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	evidence, err := m.loadFinanceInternalCostEvidence(context.Background(), scope, financeConfiguredInternalEvidence{Complete: true, VerifiedScope: scope}, true)
	if err != nil || len(evidence.Events) != 1 || evidence.Events[0].State != "strict" || evidence.Events[0].FinanceVersion != 7 {
		t.Fatalf("publication pricing not carried by SQL projection: %+v %v", evidence.Events, err)
	}
}

func TestFinancePricingProofSurvivesSubrangeAndInvalidatesCache(t *testing.T) {
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	fact := financeInternalTestCostFact{Rows: 1, UpstreamCostMicroUSD: 2, CorrectedCostMicroUSD: 1}
	evidence := financeInternalTestCostEvidence{SourceComplete: true, SourceScope: scope,
		Events: []financeInternalTestCostEvent{{FinanceVersion: 7, HourTs: 3600, Domain: "a", State: "strict", Fact: fact}}}
	part, err := financeInternalTestCostSubrange(evidence, scope)
	if err != nil || len(part.Events) != 1 || part.Events[0].FinanceVersion != 7 {
		t.Fatalf("lost historical pricing: %+v %v", part.Events, err)
	}
	before, err := financePeriodInputFingerprint(nil, channelFinanceSnapshot{}, financeConfiguredInternalEvidence{}, part, nil)
	if err != nil {
		t.Fatal(err)
	}
	part.Events[0].FinanceVersion++
	after, err := financePeriodInputFingerprint(nil, channelFinanceSnapshot{}, financeConfiguredInternalEvidence{}, part, nil)
	if err != nil || before == after {
		t.Fatal("changed publication pricing reused cached input")
	}
}

func TestFinancePeriodPricingProofKeepsValidAndLedgerCosts(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for domain := range accounts {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 1)
	}
	fact := financeInternalTestCostFact{Rows: 1, UpstreamCostMicroUSD: 1_000_000, CorrectedCostMicroUSD: 1_000_000}
	evidence := financeInternalTestCostEvidence{Total: fact, ByDomain: map[string]financeInternalTestCostFact{"hour.example": fact}, Complete: true,
		Events: []financeInternalTestCostEvent{{FinanceVersion: 1, HourTs: scope.FromTs, Domain: "hour.example", State: "strict", Fact: fact}}}
	statement, _, _, _, err := m.buildFinancePeriod(context.Background(), scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, evidence, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil || statement.KnownCorrectedUpstreamCost.MicroUSD != "32000000" || statement.InternalCostDeductionStatus != "" {
		t.Fatalf("valid account cost changed: %+v %v", statement, err)
	}
	// Ledger fallback uses the same immutable cost as the deduction. It does
	// not depend on today's recharge configuration or require an extra query.
	details := []financeCostDetailView{{Domain: "hour.example", CorrectionSource: "经济事实账", KnownCorrectedCost: economicsMoney(3_000_000)}}
	sources, err := (&Monitor{}).loadFinancePeriodDeductionSources(context.Background(), details, evidence)
	if err != nil {
		t.Fatal(err)
	}
	got, status := financeInternalCostDeduction(economicsMoney(3_000_000), evidence, sources)
	if status != "" || got.MicroUSD != "2000000" {
		t.Fatalf("same-ledger deduction blocked: %+v %s", got, status)
	}
}
