package monitor

import (
	"context"
	"strings"
	"testing"
)

// An unknown cost must not become an observed zero through ledger fallback.
// Conversely a verified zero must survive, even when other hours are missing.
func TestFinanceLedgerCostDistinguishesUnknownAndObservedZero(t *testing.T) {
	for _, tc := range []struct {
		name   string
		known  bool
		amount int64
		want   string
	}{
		{"unknown", false, 0, ""},
		{"observed_zero", true, 0, "0"},
		{"partial_nonzero", true, 2_000_000, "2000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, scope, accounts := dailyBillFixture(t)
			if !tc.known {
				// The fixture also contains verified empty hours. For the
				// unknown case, use tiny positive bills instead: rounding
				// them to zero must not manufacture a known corrected cost.
				if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).
					Where("cost_usd = 0 AND quota = 0").
					Updates(map[string]any{"cost_usd": 1e-7, "quota": quotaPerUSD * 1e-7, "unit_per_usd": quotaPerUSD}).Error; err != nil {
					t.Fatal(err)
				}
			}
			pub := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs, 1, 0, 2_000_000, tc.amount, 0)
			if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", pub.PublicationID).
				Updates(map[string]any{"corrected_cost_known": tc.known, "profit_known": false, "coverage_status": "finance_version_missing"}).Error; err != nil {
				t.Fatal(err)
			}
			insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
			component, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "amount-semantics", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
			if err != nil {
				t.Fatal(err)
			}
			if got := component.Statement.KnownRawCorrectedUpstreamCost.MicroUSD; got != tc.want {
				t.Fatalf("known cost = %q, want %q", got, tc.want)
			}
			if component.Statement.RawCorrectedUpstreamCost != nil || component.Statement.OperatingProfit != nil || component.Statement.ContributionProfit != nil {
				t.Fatal("partial/unknown evidence published exact cost or profit")
			}
			if component.Statement.UpstreamBilledCost == nil || component.Statement.UpstreamBilledCost.MicroUSD != "33000000" {
				t.Fatal("unaffected account bills disappeared")
			}
			for _, detail := range component.CostDetails {
				if detail.Domain == "hour.example" && detail.KnownCorrectedCost.MicroUSD != tc.want {
					t.Fatalf("detail = %+v", detail)
				}
			}
		})
	}
}

func TestFinanceCoverageCountsOnlyCompleteCorrection(t *testing.T) {
	zero := economicsMoney(0)
	accounts := map[string]ChannelUpstreamAccountView{}
	details := []financeCostDetailView{}
	for _, domain := range []string{"complete", "partial", "missing"} {
		accounts[domain] = ChannelUpstreamAccountView{Configured: true, UsageSyncEnabled: true}
		detail := financeCostDetailView{Domain: domain, KnownBilledCost: zero, BilledCost: &zero}
		if domain != "missing" {
			detail.KnownCorrectedCost = zero
		}
		if domain == "complete" {
			detail.CorrectedCost = &zero
		}
		details = append(details, detail)
	}
	coverage, _, err := mergeFinanceCostDetails([][]financeCostDetailView{details}, accounts)
	if err != nil {
		t.Fatal(err)
	}
	if coverage.CompleteDomains != 3 || coverage.CorrectedDomains != 2 || coverage.CorrectedCompleteDomains != 1 || coverage.Complete {
		t.Fatalf("partial and complete correction conflated: %+v", coverage)
	}
	// A later incomplete month invalidates full-range exactness, not its known
	// zero cost or other suppliers' valid facts.
	partial := details[0]
	partial.CorrectedCost = nil
	coverage, merged, err := mergeFinanceCostDetails([][]financeCostDetailView{details, {partial}}, accounts)
	if err != nil {
		t.Fatal(err)
	}
	if coverage.CorrectedCompleteDomains != 0 || coverage.CorrectedDomains != 2 {
		t.Fatalf("merged coverage = %+v", coverage)
	}
	for _, d := range merged {
		if d.Domain == "complete" && (d.CorrectedCost != nil || d.KnownCorrectedCost.MicroUSD != "0") {
			t.Fatalf("merged amount = %+v", d)
		}
	}
}

func TestFinanceSourceDescriptionsExposeFieldGaps(t *testing.T) {
	sources := financeSourceViews(StabilityDataCoverage{}, financeUpstreamCoverageView{
		RelevantDomains: 39, AvailableDomains: 39, CompleteDomains: 37, CorrectedDomains: 15,
		UnconfiguredDomains: 1, UnconfiguredUserConsumption: economicsMoney(1),
	}, financeStatementView{}, financeGiftCoverageView{
		ExpectedHours: 1, UserCompletedHours: 1, CreditCompletedHours: 1, ScopeUnknownEvents: 12,
	}, financeCURCostView{})
	for _, source := range sources {
		switch source.Key {
		case "signup_gift":
			if source.Status == "verified" || !strings.Contains(source.Description, "12 条历史事件缺分组依据") {
				t.Fatalf("hidden gift gap: %+v", source)
			}
		case "upstream_bill":
			for _, want := range []string{"账单区间完整 37/39", "修正成本区间完整 0/39", "不安排自动补采", "不能代表全站利润"} {
				if !strings.Contains(source.Description, want) {
					t.Fatalf("missing %q: %+v", want, source)
				}
			}
		}
	}
}

func TestFinanceNoCostEvidenceIsNotZero(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	if err := m.storeDB.Where("1=1").Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		t.Fatal(err)
	}
	raw, exactRaw, corrected, exactCorrected, _, _, err := m.loadFinanceUpstreamFacts(context.Background(), scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if raw.MicroUSD != "" || corrected.MicroUSD != "" || exactRaw != nil || exactCorrected != nil {
		t.Fatalf("missing evidence became zero: raw=%+v corrected=%+v", raw, corrected)
	}
}

func TestFinanceVerifiedEmptyHourSurvivesUnknownNeighbour(t *testing.T) {
	m := economicsReportTestMonitor(t, "example.test")
	const hour int64 = 7200
	insertEconomicsReportManifest(t, m, "example.test", "epoch", hour)
	pub := insertEconomicsReportHour(t, m, "example.test", "epoch", hour+3600, 1, 1, 1, 0, 0)
	if err := m.storeDB.Model(&ChannelEconomicsHourPublication{}).Where("publication_id=?", pub.PublicationID).
		Updates(map[string]any{"corrected_cost_known": false, "profit_known": false, "coverage_status": "finance_version_missing"}).Error; err != nil {
		t.Fatal(err)
	}
	insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
	report, err := m.buildChannelEconomicsReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 7200}, "example.test")
	if err != nil {
		t.Fatal(err)
	}
	for name, totals := range map[string]channelEconomicsTotalsView{"site": report.Totals, "domain": report.Domains[0].Totals} {
		if totals.KnownCorrectedCost.MicroUSD != "0" || totals.CorrectedCost != nil || totals.Profit != nil {
			t.Fatalf("%s confused proven empty hour and unknown neighbour: %+v", name, totals)
		}
	}
	if report.Domains[0].Hourly[1].Totals.KnownCorrectedCost.MicroUSD != "" {
		t.Fatal("unknown neighbour borrowed the verified hour's zero")
	}
}
