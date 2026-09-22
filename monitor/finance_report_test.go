package monitor

import (
	"context"
	"math"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecredit"
)

func TestFinanceSettingsDefaultOffAndValidateDate(t *testing.T) {
	t.Setenv("MONITOR_FINANCE_ENABLED", "")
	t.Setenv("MONITOR_FINANCE_FACTS_SYNC_ENABLED", "")
	t.Setenv("MONITOR_FINANCE_START_DATE", "")
	defaults := LoadSettings()
	if defaults.FinanceEnabled || defaults.FinanceFactsSyncEnabled || defaults.FinanceStartDate != "2026-05-01" {
		t.Fatalf("unsafe finance defaults: enabled=%v sync=%v start=%q", defaults.FinanceEnabled, defaults.FinanceFactsSyncEnabled, defaults.FinanceStartDate)
	}
	if err := validateFinanceSettings(Settings{FinanceEnabled: true, FinanceStartDate: "not-a-date"}); err == nil {
		t.Fatal("invalid finance start date accepted")
	}
	if err := validateFinanceSettings(Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01"}); err != nil {
		t.Fatal(err)
	}
	if err := validateFinanceSettings(Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", FinanceReportSnapshotReadEnabled: true}); err == nil {
		t.Fatal("经营核算快照只读不写的失效配置被接受")
	}
	if err := validateFinanceSettings(Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", FinanceReportSnapshotReadEnabled: true, FinanceReportSnapshotShadowEnabled: true}); err != nil {
		t.Fatal(err)
	}
	validSync := Settings{FinanceFactsSyncEnabled: true, FinanceStartDate: "2026-05-01",
		UsageFactsEnabled: true, UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "finance-source-v1"}
	if err := validateFinanceSettings(validSync); err != nil {
		t.Fatal(err)
	}
	invalidSync := []Settings{
		{FinanceEnabled: true, FinanceFactsSyncEnabled: true, FinanceStartDate: "2026-05-01", UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "finance-source-v1"},
		{FinanceEnabled: true, FinanceFactsSyncEnabled: true, FinanceStartDate: "2026-05-01", UsageFactsEnabled: true, UsageFactsHistorySourceMode: "unverified", UsageFactsHistorySourceEpoch: "finance-source-v1"},
		{FinanceEnabled: true, FinanceFactsSyncEnabled: true, FinanceStartDate: "2026-05-01", UsageFactsEnabled: true, UsageFactsHistorySourceMode: "complete"},
		{FinanceEnabled: true, FinanceFactsSyncEnabled: true, FinanceStartDate: "2026-05-01", UsageFactsEnabled: true, UsageFactsHistorySourceMode: "complete", UsageFactsHistorySourceEpoch: "finance-source-v1", LocalSnapshotOnly: true},
	}
	for index, cfg := range invalidSync {
		if err := validateFinanceSettings(cfg); err == nil {
			t.Fatalf("unsafe finance sync settings[%d] accepted", index)
		}
	}
}

func TestFinanceLocalSnapshotRangeClampsOnlyAbsentTail(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.LocalSnapshotOnly = true
	for _, hour := range []int64{3600, 7200} {
		if err := m.storeDB.Create(&StabilityHourIngestState{
			HourTs: hour, Status: "complete", Requests: 1,
			TrafficClassVersion: stabilityTrafficClassificationVersion,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	// A later failed state must not extend the trusted snapshot boundary.
	if err := m.storeDB.Create(&StabilityHourIngestState{
		HourTs: 10800, Status: "failed", Requests: 1,
		TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	wanted := time.Unix(18000, 0).In(loc)
	clamped, asOf, changed, err := m.clampFinanceRangeToLocalSnapshot(context.Background(), wanted)
	if err != nil {
		t.Fatal(err)
	}
	if !changed || asOf != 10800 || clamped.Unix() != 10800 {
		t.Fatalf("snapshot range was not clamped to latest complete hour: to=%d asOf=%d changed=%v", clamped.Unix(), asOf, changed)
	}
	m.cfg.LocalSnapshotOnly = false
	unchanged, asOf, changed, err := m.clampFinanceRangeToLocalSnapshot(context.Background(), wanted)
	if err != nil {
		t.Fatal(err)
	}
	if changed || asOf != 0 || !unchanged.Equal(wanted) {
		t.Fatalf("production range was changed: to=%d asOf=%d changed=%v", unchanged.Unix(), asOf, changed)
	}
}

func TestFinanceMonthRangesAlwaysUseShanghaiCalendar(t *testing.T) {
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	from := time.Date(2026, 5, 1, 0, 0, 0, 0, loc)
	to := time.Date(2026, 6, 3, 0, 0, 0, 0, loc)

	// Simulate an Ubuntu CI runner: time.Unix creates a UTC-located value.
	got := financeMonthRanges(time.Unix(from.Unix(), 0), time.Unix(to.Unix(), 0))
	if len(got) != 2 || got[0][0].Unix() != from.Unix() || got[0][1].Unix() != time.Date(2026, 6, 1, 0, 0, 0, 0, loc).Unix() || got[1][1].Unix() != to.Unix() {
		t.Fatalf("finance months depend on process timezone: %+v", got)
	}
	for _, period := range got {
		if period[0].Location().String() != "Asia/Shanghai" || period[1].Location().String() != "Asia/Shanghai" {
			t.Fatalf("finance month did not retain Shanghai calendar: %+v", period)
		}
	}
}

func newFinanceReportTestMonitor(t *testing.T, domain string) *Monitor {
	t.Helper()
	m := newStabilityTestMonitor(t)
	m.cfg.FinanceEnabled = true
	m.cfg.FinanceStartDate = "2026-05-01"
	m.cfg.ChannelEconomicsReportEnabled = true
	if err := m.storeDB.Create(&ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: "https://" + domain, Enabled: true, UsageSyncEnabled: true}).Error; err != nil {
		t.Fatal(err)
	}
	return m
}

func TestFinanceUpstreamCoverageStartsAtFirstRelevantActivity(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "active.example")
	start := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 41, Name: "active", BaseDomain: "active.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: start + 3600, ChannelID: 41, ModelName: "gpt", Grp: "g", Success: 1, Quota: 500_000,
		TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, hour := range []int64{start + 3600, start + 7200} {
		if err := m.storeDB.Create(&ChannelUpstreamUsageHour{
			Domain: "active.example", HourTs: hour, BucketSeconds: 3600, Requests: 1,
			Quota: 500_000, CostUSD: 1, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	createChannelRechargeVersion(t, m, "active.example", 1, start+3600, 1, 1)
	accounts := map[string]ChannelUpstreamAccountView{
		"active.example": {Configured: true, Provider: upstreamProviderNewAPI, UsageSyncEnabled: true, UsageGranularity: "hour"},
		"idle.example":   {Configured: true, Provider: upstreamProviderNewAPI, UsageSyncEnabled: true, UsageGranularity: "hour"},
	}
	finance := channelFinanceSnapshot{domainCosts: map[string]ChannelDomainCost{}}
	_, billed, _, corrected, coverage, details, err := m.loadFinanceUpstreamFacts(context.Background(),
		stabilityScope{FromTs: start, ToTs: start + 10800}, start+14400, accounts, finance,
		map[string]financeDomainUserFact{"active.example": {Domain: "active.example", Requests: 1, ConsumeQuota: 500_000}})
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || coverage.RelevantDomains != 1 || coverage.UsageEnabledDomains != 1 ||
		coverage.ExpectedDomainHours != 2 || coverage.CompletedDomainHours != 2 || billed == nil || corrected == nil {
		t.Fatalf("finance lifecycle boundary did not close exact coverage: coverage=%+v billed=%+v corrected=%+v", coverage, billed, corrected)
	}
	if len(details) != 1 || details[0].Domain != "active.example" || details[0].ExpectedHours != 2 || details[0].CompletedHours != 2 || details[0].Status != "verified" {
		t.Fatalf("inactive account leaked into finance detail or active coverage is wrong: %+v", details)
	}
}

func TestFinanceUpstreamCoveragePreservesRealPreEvidenceGap(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "gap.example")
	start := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 42, Name: "gap", BaseDomain: "gap.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: start, ChannelID: 42, ModelName: "gpt", Grp: "g", Success: 1, Quota: 500_000,
		TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, hour := range []int64{start + 3600, start + 7200} {
		if err := m.storeDB.Create(&ChannelUpstreamUsageHour{
			Domain: "gap.example", HourTs: hour, BucketSeconds: 3600, Requests: 1,
			Quota: 500_000, CostUSD: 1, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	createChannelRechargeVersion(t, m, "gap.example", 1, start, 1, 1)
	accounts := map[string]ChannelUpstreamAccountView{
		"gap.example": {Configured: true, Provider: upstreamProviderNewAPI, UsageSyncEnabled: true, UsageGranularity: "hour"},
	}
	_, billed, _, corrected, coverage, details, err := m.loadFinanceUpstreamFacts(context.Background(),
		stabilityScope{FromTs: start, ToTs: start + 10800}, start+14400, accounts,
		channelFinanceSnapshot{domainCosts: map[string]ChannelDomainCost{}},
		map[string]financeDomainUserFact{"gap.example": {Domain: "gap.example", Requests: 1, ConsumeQuota: 500_000}})
	if err != nil {
		t.Fatal(err)
	}
	if coverage.Complete || coverage.ExpectedDomainHours != 3 || coverage.CompletedDomainHours != 2 || billed != nil || corrected != nil {
		t.Fatalf("real pre-evidence gap was hidden: coverage=%+v billed=%+v corrected=%+v", coverage, billed, corrected)
	}
	if len(details) != 1 || details[0].ExpectedHours != 3 || details[0].CompletedHours != 2 || details[0].Status != "incomplete" {
		t.Fatalf("real gap detail mismatch: %+v", details)
	}
}

func TestFinanceReportUsesFullUserFactsAndVerifiedUpstreamCost(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	hour := int64(1_788_195_600) // aligned historical hour
	if err := m.storeDB.Create(&ChannelSnap{ID: 59, Name: "4s", BaseDomain: "4sapi.com", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 59, ModelName: "gpt", Grp: "g", Success: 20, Quota: 6_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 20, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "4sapi.com", HourTs: hour, BucketSeconds: 3600, Requests: 18, Quota: 2_500_000, CostUSD: 5, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI}).Error; err != nil {
		t.Fatal(err)
	}
	createChannelRechargeVersion(t, m, "4sapi.com", 1, hour, 4, 5)
	epoch := strings.Repeat("1", 64)
	publication := insertEconomicsReportHour(t, m, "4sapi.com", epoch, hour, 59, 12_000_000, 5_000_000, 4_000_000, 8_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, hour, publication)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from, to := time.Unix(hour, 0).In(loc), time.Unix(hour+3600, 0).In(loc)
	report, err := m.buildFinanceOperatingReport(context.Background(), from, to)
	if err != nil {
		t.Fatal(err)
	}
	if report.Statement.UserConsumption == nil || report.Statement.UserConsumption.MicroUSD != "12000000" {
		t.Fatalf("user consumption mismatch: %+v", report.Statement)
	}
	if report.Statement.CorrectedUpstreamCost == nil || report.Statement.CorrectedUpstreamCost.MicroUSD != "4000000" {
		t.Fatalf("corrected upstream cost mismatch: %+v", report.Statement)
	}
	if report.Statement.ContributionProfit == nil || report.Statement.ContributionProfit.MicroUSD != "8000000" {
		t.Fatalf("contribution profit mismatch: %+v", report.Statement)
	}
	if report.Statement.PairedUserConsumption.MicroUSD != "12000000" || report.Statement.PairedCorrectedCost.MicroUSD != "4000000" {
		t.Fatalf("paired economics mismatch: %+v", report.Statement)
	}
	if report.Statement.InternalTestConsumption.MicroUSD != "0" || report.Statement.InternalTestRequests != 0 {
		t.Fatalf("unexpected internal test usage: %+v", report.Statement)
	}
	if !strings.Contains(report.SemanticsNote, "修正上游总成本包含内部测试实际支出") ||
		!strings.Contains(report.SemanticsNote, "平台经营结果全额扣除") ||
		!strings.Contains(report.SemanticsNote, "不重复扣除") {
		t.Fatalf("finance semantics no longer distinguishes gross platform cost from business cost: %q", report.SemanticsNote)
	}
	if len(report.Periods) != 1 || report.Periods[0].Status != "verified" || len(report.CostDetails) != 1 {
		t.Fatalf("unexpected period/detail projection: %+v %+v", report.Periods, report.CostDetails)
	}
	if len(report.Days) != 1 || report.Days[0].Statement.ContributionProfit == nil || report.Days[0].Statement.ContributionProfit.MicroUSD != "8000000" {
		t.Fatalf("daily paired projection mismatch: %+v", report.Days)
	}
	if report.PairingAudit.PairedRows != 1 || report.PairingAudit.BlockedRows != 0 || len(report.PairingAudit.Blockers) != 0 {
		t.Fatalf("paired audit mismatch: %+v", report.PairingAudit)
	}
	if report.PairingAudit.RelevantDomains != 1 || !reflect.DeepEqual(report.PairingAudit.LedgerDomains, []string{"4sapi.com"}) || len(report.PairingAudit.UnenrolledDomains) != 0 {
		t.Fatalf("pairing domain coverage mismatch: %+v", report.PairingAudit)
	}
	if report.CostDetails[0].PairingStatus != "paired_verified" {
		t.Fatalf("verified pairing was not projected onto cost detail: %+v", report.CostDetails[0])
	}
}

func TestFinanceReportSeparatesStrictInternalTestCostAndExcludesMixedHoursFromCustomerContribution(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	first := int64(1_788_195_600)
	for _, channel := range []ChannelSnap{
		{ID: 69, Name: "4sapi-test-only", BaseDomain: "4sapi.com", Status: 1},
		{ID: 70, Name: "4sapi-mixed", BaseDomain: "4sapi.com", Status: 1},
		{ID: 71, Name: "4sapi-customer", BaseDomain: "4sapi.com", Status: 1},
	} {
		if err := m.storeDB.Create(&channel).Error; err != nil {
			t.Fatal(err)
		}
	}
	for index := 0; index < 3; index++ {
		hour := first + int64(index)*3600
		if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: int64(index), TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: first + 3600, ChannelID: 70, ModelName: "gpt", Grp: "customer", Success: 3, Quota: 3_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: first + 7200, ChannelID: 71, ModelName: "gpt", Grp: "customer", Success: 5, Quota: 5_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	for _, sample := range []ChannelTestHourSample{
		{HourTs: first, ChannelID: 69, ModelName: "gpt", Grp: "internal", Origin: "scheduled", Requests: 4, Quota: 1_000_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
		{HourTs: first + 3600, ChannelID: 70, ModelName: "gpt", Grp: "internal", Origin: "scheduled", Requests: 2, Quota: 500_000, TrafficClassVersion: stabilityTrafficClassificationVersion},
	} {
		if err := m.storeDB.Create(&sample).Error; err != nil {
			t.Fatal(err)
		}
	}
	for index, cost := range []float64{4, 2, 3} {
		if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "4sapi.com", HourTs: first + int64(index)*3600, BucketSeconds: 3600, Requests: int64(index + 1), CostUSD: cost, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI}).Error; err != nil {
			t.Fatal(err)
		}
	}
	createChannelRechargeVersion(t, m, "4sapi.com", 1, first, 9, 9)
	epoch := strings.Repeat("9", 64)
	strict := insertEconomicsReportHour(t, m, "4sapi.com", epoch, first, 69, 0, 4_000_000, 4_000_000, -4_000_000)
	mixed := insertEconomicsReportHour(t, m, "4sapi.com", epoch, first+3600, 70, 6_000_000, 2_000_000, 2_000_000, 4_000_000)
	customer := insertEconomicsReportHour(t, m, "4sapi.com", epoch, first+7200, 71, 10_000_000, 3_000_000, 3_000_000, 7_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, first, strict)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, first+3600, mixed)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, first+7200, customer)

	loc, _ := time.LoadLocation("Asia/Shanghai")
	report, err := m.buildFinanceOperatingReport(context.Background(), time.Unix(first, 0).In(loc), time.Unix(first+10800, 0).In(loc))
	if err != nil {
		t.Fatal(err)
	}
	statement := report.Statement
	if statement.KnownInternalTestUpstreamCost.MicroUSD != "4000000" || statement.InternalTestUpstreamCost != nil || statement.InternalTestCostRows != 1 || statement.InternalTestMixedRows != 1 {
		t.Fatalf("internal-test cost classification mismatch: %+v", statement)
	}
	if statement.PairedUserConsumption.MicroUSD != "10000000" || statement.PairedCorrectedCost.MicroUSD != "3000000" || statement.KnownContributionProfit.MicroUSD != "7000000" {
		t.Fatalf("customer contribution still contains test-bearing hours: %+v", statement)
	}
	if len(report.CostDetails) != 1 || report.CostDetails[0].PairedRows != 1 || report.CostDetails[0].PairedCost.MicroUSD != "3000000" {
		t.Fatalf("domain contribution detail was not separated: %+v", report.CostDetails)
	}
	if len(report.Days) != 1 || report.Days[0].Statement.KnownInternalTestUpstreamCost.MicroUSD != "4000000" || report.Days[0].Statement.KnownContributionProfit.MicroUSD != "7000000" {
		t.Fatalf("daily internal-test separation mismatch: %+v", report.Days)
	}
	if report.Days[0].InternalCostComplete || report.Days[0].Statement.InternalTestMixedRows != 1 || report.Days[0].Statement.InternalTestUpstreamCost != nil {
		t.Fatal("daily response concealed unresolved mixed cost")
	}
}

func TestApplyFinancePairingDomainCoverageListsUnenrolledDomains(t *testing.T) {
	audit := financePairingAuditView{LedgerDomains: []string{"covered.example"}}
	details := []financeCostDetailView{
		{Domain: "missing-b.example"},
		{Domain: "covered.example"},
		{Domain: "missing-a.example"},
		{Domain: "missing-a.example"},
		{Domain: "ignored.example", Status: "not_configured"},
	}
	applyFinancePairingDomainCoverage(&audit, details)
	if audit.RelevantDomains != 3 || !reflect.DeepEqual(audit.UnenrolledDomains, []string{"missing-a.example", "missing-b.example"}) {
		t.Fatalf("unexpected pairing domain coverage: %+v", audit)
	}
}

func TestFinanceCoverageExcludesUnconfiguredWithoutTreatingItsCostAsZero(t *testing.T) {
	configuredBilled := economicsMoney(2_000_000)
	configuredCorrected := economicsMoney(1_500_000)
	configuredContribution := economicsMoney(3_500_000)
	periodDetails := [][]financeCostDetailView{{
		{
			Domain: "configured.example", UserRequests: 10, UserConsumption: economicsMoney(5_000_000),
			KnownBilledCost: configuredBilled, BilledCost: &configuredBilled,
			KnownCorrectedCost: configuredCorrected, CorrectedCost: &configuredCorrected,
			PairedRevenue: economicsMoney(5_000_000), PairedCost: configuredCorrected,
			KnownContribution: configuredContribution, Contribution: &configuredContribution,
			ExpectedHours: 2, CompletedHours: 2, Status: "verified",
		},
		{
			Domain: "unconfigured.example", UserRequests: 4, UserConsumption: economicsMoney(900_000),
			Status: "not_configured",
		},
	}}
	accounts := map[string]ChannelUpstreamAccountView{
		"configured.example": {Configured: true, UsageSyncEnabled: true},
	}

	coverage, details, err := mergeFinanceCostDetails(periodDetails, accounts)
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || coverage.RelevantDomains != 1 || coverage.AvailableDomains != 1 ||
		coverage.CorrectedDomains != 1 || coverage.UnconfiguredDomains != 1 ||
		coverage.UnconfiguredUserRequests != 4 || coverage.UnconfiguredUserConsumption.MicroUSD != "900000" {
		t.Fatalf("configured coverage or excluded-domain disclosure mismatch: %+v", coverage)
	}
	if len(details) != 2 || details[1].Domain != "unconfigured.example" || details[1].Status != "not_configured" ||
		details[1].KnownBilledCost.MicroUSD != "" || details[1].KnownCorrectedCost.MicroUSD != "" ||
		details[1].KnownContribution.MicroUSD != "" {
		t.Fatalf("unconfigured domain was hidden or treated as known zero cost: %+v", details)
	}

	audit := financePairingAuditView{LedgerDomains: []string{"configured.example"}}
	applyFinancePairingDomainCoverage(&audit, details)
	applyFinanceCostPairingStatuses(details, audit)
	if audit.RelevantDomains != 1 || len(audit.UnenrolledDomains) != 0 {
		t.Fatalf("unconfigured domain polluted pairing coverage: %+v", audit)
	}
	if details[1].PairingStatus != "not_required" || details[1].ClosureReadiness != "not_required" {
		t.Fatalf("unconfigured domain was turned into remediation work: %+v", details[1])
	}

	periodCost := economicsMoney(1_500_000)
	statement, err := aggregateFinancePeriods([]financePeriodView{{
		Statement: financeStatementView{
			KnownUserConsumption:       economicsMoney(5_900_000),
			KnownCorrectedUpstreamCost: periodCost,
			CorrectedUpstreamCost:      &periodCost,
		},
		UpstreamCoverage: coverage,
	}}, StabilityDataCoverage{Complete: true}, coverage)
	if err != nil {
		t.Fatal(err)
	}
	if statement.KnownCorrectedUpstreamCost.MicroUSD != "1500000" || statement.CorrectedUpstreamCost != nil || statement.OperatingProfit != nil {
		t.Fatalf("unconfigured traffic was implicitly assigned zero upstream cost: %+v", statement)
	}
}

func TestApplyFinancePairingSourceCandidatesAreAdvisoryAndConservative(t *testing.T) {
	audit := financePairingAuditView{Sources: []financePairingSourceView{
		{Domain: "4SAPI.COM", SourceGroups: []string{"Gpt-codex"}},
		{Domain: "4sapi.com", SourceGroups: []string{"claude-code", "historical-group"}},
		{Domain: "4sapi.com", SourceGroups: []string{"shared"}},
		{Domain: "4sapi.com", SourceGroups: []string{"not-configured"}},
	}}
	finance := channelFinanceSnapshot{
		channelCanonicalCost: map[int]ChannelFinanceChannelCost{
			59: {ChannelID: 59, UpstreamGroupName: "gpt-CODEX"},
			60: {ChannelID: 60, UpstreamGroupName: "claude-code"},
			61: {ChannelID: 61, UpstreamGroupName: "shared"},
			62: {ChannelID: 62, UpstreamGroupName: "SHARED"},
			63: {ChannelID: 63, UpstreamGroupName: "Gpt-codex"},
			64: {ChannelID: 64, UpstreamGroupName: "Gpt-codex"},
		},
		channelCostConflict: map[int]bool{63: true},
	}
	channels := []ChannelSnap{
		{ID: 59, Name: "4sapi_Gpt-codex", BaseDomain: "4sapi.com"},
		{ID: 60, Name: "4sapi_claude-code", BaseDomain: "4sapi.com"},
		{ID: 61, Name: "shared-a", BaseDomain: "4sapi.com"},
		{ID: 62, Name: "shared-b", BaseDomain: "4sapi.com"},
		{ID: 63, Name: "conflicting", BaseDomain: "4sapi.com"},
		{ID: 64, Name: "deleted", BaseDomain: "4sapi.com", DeletedAt: 1},
	}
	applyFinancePairingSourceCandidates(&audit, finance, channels)

	if audit.Sources[0].CandidateState != "configured_unique" ||
		!reflect.DeepEqual(audit.Sources[0].CurrentConfigCandidates, []financePairingCandidate{{
			ChannelID: 59, ChannelName: "4sapi_Gpt-codex", MatchedGroups: []string{"Gpt-codex"},
		}}) {
		t.Fatalf("unique candidate mismatch: %+v", audit.Sources[0])
	}
	if audit.Sources[1].CandidateState != "configured_partial" || len(audit.Sources[1].CurrentConfigCandidates) != 1 || audit.Sources[1].CurrentConfigCandidates[0].ChannelID != 60 {
		t.Fatalf("partial candidate mismatch: %+v", audit.Sources[1])
	}
	if audit.Sources[2].CandidateState != "configured_ambiguous" || len(audit.Sources[2].CurrentConfigCandidates) != 2 {
		t.Fatalf("ambiguous candidate mismatch: %+v", audit.Sources[2])
	}
	if audit.Sources[3].CandidateState != "no_configured_match" || len(audit.Sources[3].CurrentConfigCandidates) != 0 {
		t.Fatalf("missing candidate mismatch: %+v", audit.Sources[3])
	}
}

func TestApplyFinanceCostPairingStatusesKeepsBillAndPairingIndependent(t *testing.T) {
	details := []financeCostDetailView{
		{Domain: "outside.example", Status: "verified", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), CostEvidenceHours: 1, CostVerifiedHours: 1, CostEvidenceRequests: 10, CostVerifiedRequests: 10, UpstreamRequests: 10, FinanceVersions: 1, FinanceHistoryCovered: true},
		{Domain: "blocked.example", Status: "verified", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), CostEvidenceHours: 1, CostVerifiedHours: 1, CostEvidenceRequests: 10, CostVerifiedRequests: 10, UpstreamRequests: 10, FinanceVersions: 1, FinanceHistoryCovered: true},
		{Domain: "partial.example", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), KnownContribution: economicsMoney(2_000_000)},
		{Domain: "verified.example", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), Contribution: financeMoneyPointer(economicsMoney(3_000_000))},
	}
	audit := financePairingAuditView{
		LedgerDomains:       []string{"blocked.example", "partial.example", "verified.example"},
		UnallocatedByDomain: map[string]int64{"outside.example": 2, "blocked.example": 1},
		Blockers:            []financePairingBlockerView{{Key: "unallocated_cost", DomainNames: []string{"blocked.example"}}},
	}
	applyFinanceCostPairingStatuses(details, audit)
	want := []string{"not_enrolled", "binding_required", "partially_paired", "paired_verified"}
	for index := range details {
		if details[index].PairingStatus != want[index] {
			t.Fatalf("detail %d pairing status=%q want=%q: %+v", index, details[index].PairingStatus, want[index], details[index])
		}
	}
	if details[0].Status != "verified" || details[1].Status != "verified" {
		t.Fatalf("upstream bill status was changed: %+v", details)
	}
	if details[0].ClosureReadiness != "source_binding_required" || details[0].UnallocatedSources != 2 {
		t.Fatalf("unenrolled source binding readiness mismatch: %+v", details[0])
	}
	if details[1].ClosureReadiness != "in_progress" || !reflect.DeepEqual(details[1].ClosureBlockers, []string{"unallocated_cost"}) {
		t.Fatalf("ledger blocker readiness mismatch: %+v", details[1])
	}
}

func TestApplyFinanceClosureReadinessFailsClosedByEvidenceStage(t *testing.T) {
	details := []financeCostDetailView{
		{Status: "not_configured", PairingStatus: "not_required"},
		{Status: "not_connected", PairingStatus: "not_enrolled"},
		{Status: "no_data", PairingStatus: "not_enrolled"},
		{Status: "incomplete", PairingStatus: "not_enrolled", KnownBilledCost: economicsMoney(1_000_000)},
		{Status: "incomplete", PairingStatus: "not_enrolled", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000)},
		{Status: "incomplete", PairingStatus: "not_enrolled", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), CostEvidenceHours: 2, CostVerifiedHours: 1, CostEvidenceRequests: 10, CostVerifiedRequests: 5, UpstreamRequests: 10},
		{Status: "incomplete", PairingStatus: "not_enrolled", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), CostEvidenceHours: 1, CostVerifiedHours: 1, CostEvidenceRequests: 10, CostVerifiedRequests: 10, UpstreamRequests: 10},
		{Status: "incomplete", PairingStatus: "not_enrolled", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), CostEvidenceHours: 1, CostVerifiedHours: 1, CostEvidenceRequests: 10, CostVerifiedRequests: 10, UpstreamRequests: 10, FinanceVersions: 1, FinanceHistoryCovered: true, UnallocatedSources: 2},
		{Status: "incomplete", PairingStatus: "not_enrolled", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000), CostEvidenceHours: 1, CostVerifiedHours: 1, CostEvidenceRequests: 10, CostVerifiedRequests: 10, UpstreamRequests: 10, FinanceVersions: 1, FinanceHistoryCovered: true},
		{Status: "incomplete", PairingStatus: "partially_paired", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000)},
		{Status: "verified", PairingStatus: "paired_verified", KnownBilledCost: economicsMoney(1_000_000), KnownCorrectedCost: economicsMoney(900_000)},
	}
	want := []string{"not_required", "bill_not_connected", "bill_missing", "correction_missing", "cost_evidence_missing", "cost_evidence_incomplete", "finance_history_missing", "source_binding_required", "ledger_backfill_required", "in_progress", "verified"}
	for index := range details {
		applyFinanceClosureReadiness(&details[index])
		if details[index].ClosureReadiness != want[index] || details[index].ClosureNextAction == "" {
			t.Fatalf("detail %d readiness=%q want=%q: %+v", index, details[index].ClosureReadiness, want[index], details[index])
		}
	}
}

func TestLoadFinanceClosureEvidenceSeparatesVerifiedActivityAndHistory(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	emptyCost, emptyVersions, err := m.loadFinanceClosureEvidence(context.Background(), stabilityScope{}, nil)
	if err != nil || len(emptyCost) != 0 || len(emptyVersions) != 0 {
		t.Fatalf("empty closure scope should not query or invent evidence: cost=%v versions=%v err=%v", emptyCost, emptyVersions, err)
	}
	hour := int64(1_788_195_600)
	epoch := strings.Repeat("9", 64)
	states := []ChannelUpstreamCostHourState{
		{Domain: "4sapi.com", AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched", Requests: 7, EvidenceRows: 1},
		{Domain: "4sapi.com", AccountEpoch: epoch, HourTs: hour + 3600, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "observed", ReconcileStatus: "pending", Requests: 3, EvidenceRows: 1},
		{Domain: "4sapi.com", AccountEpoch: epoch, HourTs: hour + 7200, SemanticsVersion: channelCostEvidenceSemanticsVersion, Status: "verified", ReconcileStatus: "matched"},
	}
	for _, state := range states {
		if err := m.storeDB.Create(&state).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&ChannelFinanceVersion{Domain: "4sapi.com", Version: 1, SnapshotJSON: `{}`, EffectiveAt: hour - 3600, CreatedAt: hour - 3600}).Error; err != nil {
		t.Fatal(err)
	}
	cost, versions, err := m.loadFinanceClosureEvidence(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 10_800}, []financeCostDetailView{{Domain: "4sapi.com"}})
	if err != nil {
		t.Fatal(err)
	}
	row := cost["4sapi.com"]
	if row.FirstHour != hour || row.EvidenceHours != 2 || row.VerifiedHours != 1 || row.EvidenceRequests != 10 || row.VerifiedRequests != 7 {
		t.Fatalf("unexpected closure cost evidence: %+v", row)
	}
	if versions["4sapi.com"].Versions != 1 || versions["4sapi.com"].FirstEffective != hour-3600 {
		t.Fatalf("unexpected finance history: %+v", versions["4sapi.com"])
	}
	details := []financeCostDetailView{{Domain: "4sapi.com"}}
	applyFinanceClosureEvidence(details, cost, versions)
	if !details[0].FinanceHistoryCovered || details[0].CostEvidenceHours != 2 || details[0].FinanceVersions != 1 {
		t.Fatalf("closure evidence was not applied: %+v", details[0])
	}
}

func TestFinanceEvidenceBackfillEstimateIsReadOnlyAndBounded(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	hour := int64(1_788_195_600)
	for _, row := range []ChannelUpstreamUsageHour{
		{Domain: "4sapi.com", HourTs: hour, BucketSeconds: 3600, Requests: 50, Quota: 1, CostUSD: 1, Provider: upstreamProviderNewAPI},
		{Domain: "4sapi.com", HourTs: hour + 7200, BucketSeconds: 3600, Requests: 250, Quota: 1, CostUSD: 1, Provider: upstreamProviderNewAPI},
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	details := []financeCostDetailView{{Domain: "4sapi.com", ClosureReadiness: "cost_evidence_missing"}}
	accounts := map[string]ChannelUpstreamAccountView{
		"4sapi.com": {Configured: true, Enabled: true, UsageSyncEnabled: true, Provider: upstreamProviderNewAPI, ProviderName: "NewAPI"},
	}
	if err := m.applyFinanceEvidenceBackfillEstimate(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 10_800}, hour+86_400, details, accounts); err != nil {
		t.Fatal(err)
	}
	got := details[0]
	if got.Provider != upstreamProviderNewAPI || got.ProviderName != "NewAPI" || !got.EvidenceBackfillClosesCost || got.EvidenceBackfillStatus != "local_estimate_ready" || got.EvidenceBackfillGranularity != "hour" || got.EvidenceBackfillCalendarHours != 3 || got.EvidenceBackfillActiveHours != 2 || got.EvidenceBackfillEstimatedCalls != 12 || got.EvidenceBackfillEstimatedRuns != 4 {
		t.Fatalf("unexpected backfill estimate: %+v", got)
	}
	if !strings.Contains(got.ClosureNextAction, "12 次上游读取") {
		t.Fatalf("estimate was not explained in next action: %+v", got)
	}
	var states int64
	if err := m.storeDB.Model(&ChannelUpstreamPricingSyncState{}).Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("read-only estimate created sync state: count=%d err=%v", states, err)
	}
}

func TestFinanceEvidenceBackfillEstimateUsesSub2NaturalDayPaging(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "sub2.example")
	day := cstDayStart(time.Now().Add(-72 * time.Hour).Unix())
	for _, row := range []ChannelUpstreamUsageHour{
		{Domain: "sub2.example", HourTs: day + 3600, BucketSeconds: 3600, Requests: 150, Quota: 1, CostUSD: 1, Provider: upstreamProviderSub2API},
		{Domain: "sub2.example", HourTs: day + 2*86400 + 3600, BucketSeconds: 3600, Requests: 2001, Quota: 1, CostUSD: 1, Provider: upstreamProviderSub2API},
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	details := []financeCostDetailView{{Domain: "sub2.example", ClosureReadiness: "cost_evidence_missing"}}
	accounts := map[string]ChannelUpstreamAccountView{
		"sub2.example": {Configured: true, Enabled: true, UsageSyncEnabled: true, Provider: upstreamProviderSub2API, ProviderName: "Sub2API", UsageAdapter: upstreamUsageAdapterSub2Trend},
	}
	if err := m.applyFinanceEvidenceBackfillEstimate(context.Background(), stabilityScope{FromTs: day, ToTs: day + 3*86400}, day+4*86400, details, accounts); err != nil {
		t.Fatal(err)
	}
	got := details[0]
	// Day one: 2 pages => 3 reads per complete scan. Empty day: 1 read per
	// scan. Day three: 21 pages => 22 reads per complete scan. Every day needs
	// two independent complete scans.
	if got.Provider != upstreamProviderSub2API || got.EvidenceBackfillClosesCost || got.EvidenceBackfillStatus != "pricing_evidence_only" || got.EvidenceBackfillGranularity != "natural_day" || got.EvidenceBackfillCalendarHours != 72 || got.EvidenceBackfillActiveHours != 2 || got.EvidenceBackfillActiveDays != 2 || got.EvidenceBackfillEstimatedCalls != 52 || got.EvidenceBackfillEstimatedRuns != 8 {
		t.Fatalf("unexpected Sub2API estimate: %+v", got)
	}
	if !strings.Contains(got.ClosureNextAction, "3 个自然日") || !strings.Contains(got.EvidenceBackfillNote, "Sub2API") {
		t.Fatalf("Sub2API estimate explanation missing: %+v", got)
	}
}

func TestPagedPricingScanEstimateIncludesDurableResumeValidation(t *testing.T) {
	tests := []struct {
		requests int64
		calls    int64
	}{
		{0, 1}, {100, 1}, {101, 3}, {1900, 20}, {2000, 21}, {2100, 22}, {3900, 40}, {4000, 42},
	}
	for _, test := range tests {
		if got := estimatePagedPricingScanCalls(test.requests); got != test.calls {
			t.Fatalf("requests=%d calls=%d want=%d", test.requests, got, test.calls)
		}
	}
}

func TestFinanceEvidenceBackfillEstimateUsesAICodeWithConfiguredKeyCount(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "aicodewith.com")
	day := cstDayStart(time.Now().Add(-48 * time.Hour).Unix())
	for _, row := range []ChannelUpstreamUsageHour{
		{Domain: "aicodewith.com", HourTs: day, BucketSeconds: 86400, Requests: 10, Quota: 1, CostUSD: 1, Provider: upstreamProviderAICodeWith},
		{Domain: "aicodewith.com", HourTs: day + 86400, BucketSeconds: 86400, Requests: 20, Quota: 1, CostUSD: 1, Provider: upstreamProviderAICodeWith},
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	details := []financeCostDetailView{{Domain: "aicodewith.com", ClosureReadiness: "cost_evidence_missing"}}
	accounts := map[string]ChannelUpstreamAccountView{
		"aicodewith.com": {Configured: true, Enabled: true, UsageSyncEnabled: true, Provider: upstreamProviderAICodeWith, ProviderName: "AICodeWith", APIKeyCount: 5},
	}
	if err := m.applyFinanceEvidenceBackfillEstimate(context.Background(), stabilityScope{FromTs: day, ToTs: day + 2*86400}, day+3*86400, details, accounts); err != nil {
		t.Fatal(err)
	}
	got := details[0]
	if got.Provider != upstreamProviderAICodeWith || got.EvidenceBackfillClosesCost || got.EvidenceBackfillStatus != "pricing_evidence_only" || got.EvidenceBackfillGranularity != "natural_day" || got.EvidenceBackfillCalendarHours != 48 || got.EvidenceBackfillActiveDays != 2 || got.EvidenceBackfillEstimatedCalls != 20 || got.EvidenceBackfillEstimatedRuns != 8 {
		t.Fatalf("unexpected AICodeWith estimate: %+v", got)
	}
	if !strings.Contains(got.ClosureNextAction, "2 个自然日×5 把 Key") || !strings.Contains(got.EvidenceBackfillNote, "1000 条上限") {
		t.Fatalf("AICodeWith estimate risks missing: %+v", got)
	}
}

func TestFinanceEvidenceBackfillEstimateRejectsSub2DailyAggregate(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "legacy-sub2.example")
	day := cstDayStart(time.Now().Add(-24 * time.Hour).Unix())
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "legacy-sub2.example", HourTs: day, BucketSeconds: 86400, Requests: 10, Quota: 1, CostUSD: 1, Provider: upstreamProviderSub2API}).Error; err != nil {
		t.Fatal(err)
	}
	details := []financeCostDetailView{{Domain: "legacy-sub2.example", ClosureReadiness: "cost_evidence_missing"}}
	accounts := map[string]ChannelUpstreamAccountView{
		"legacy-sub2.example": {Configured: true, Enabled: true, UsageSyncEnabled: true, Provider: upstreamProviderSub2API, ProviderName: "Sub2API", UsageAdapter: upstreamUsageAdapterSub2Stats},
	}
	if err := m.applyFinanceEvidenceBackfillEstimate(context.Background(), stabilityScope{FromTs: day, ToTs: day + 86400}, day+2*86400, details, accounts); err != nil {
		t.Fatal(err)
	}
	if details[0].EvidenceBackfillStatus != "granularity_unsupported" || details[0].EvidenceBackfillEstimatedCalls != 0 {
		t.Fatalf("daily-only Sub2API must not be presented as estimable: %+v", details[0])
	}
}

func TestFinancePairingAuditExplainsUnallocatedCostAndMissingCost(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	hour := int64(1_788_195_600)
	epoch := strings.Repeat("7", 64)
	rows := []ChannelEconomicsHourPublication{
		{PublicationID: strings.Repeat("a", 64), LogicalKey: economicsLogicalKey("4sapi.com", epoch, hour, 0), Revision: 1,
			Domain: "4sapi.com", AccountEpoch: epoch, HourTs: hour, LocalChannelID: 0, SemanticsVersion: channelEconomicsSemanticsVersion,
			UpstreamCostMicroUSD: 3_000_000, CorrectedCostMicroUSD: 2_000_000, CorrectedCostKnown: true, CoverageStatus: "unallocated_cost"},
		{PublicationID: strings.Repeat("b", 64), LogicalKey: economicsLogicalKey("4sapi.com", epoch, hour, 59), Revision: 1,
			Domain: "4sapi.com", AccountEpoch: epoch, HourTs: hour, LocalChannelID: 59, SemanticsVersion: channelEconomicsSemanticsVersion,
			RevenueMicroUSD: 5_000_000, CoverageStatus: "upstream_cost_missing"},
	}
	for _, row := range rows {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&ChannelEconomicsHourCurrent{LogicalKey: row.LogicalKey, PublicationID: row.PublicationID, Revision: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&ChannelUpstreamCostHourEvidence{
		Domain: "4sapi.com", AccountEpoch: epoch, HourTs: hour, SemanticsVersion: channelCostEvidenceSemanticsVersion,
		SourceRef: strings.Repeat("c", 64), DimensionHash: strings.Repeat("d", 64), SourceGroup: "Gpt-codex", UpstreamModel: "gpt-5.6-sol",
		ChargeUnits: 500_000, ChargeUnitsPerUSD: "500000", Requests: 3,
	}).Error; err != nil {
		t.Fatal(err)
	}
	audit, err := m.loadFinancePairingAudit(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600})
	if err != nil {
		t.Fatal(err)
	}
	if audit.PublicationRows != 2 || audit.PairedRows != 0 || audit.BlockedRows != 2 || audit.UnallocatedSources != 1 {
		t.Fatalf("audit totals mismatch: %+v", audit)
	}
	if audit.UnallocatedByDomain["4sapi.com"] != 1 {
		t.Fatalf("audit per-domain source count mismatch: %+v", audit.UnallocatedByDomain)
	}
	if len(audit.Blockers) != 2 || audit.StatusCounts["unallocated_cost"] != 1 || audit.StatusCounts["upstream_cost_missing"] != 1 {
		t.Fatalf("audit blockers mismatch: %+v", audit)
	}
	for _, blocker := range audit.Blockers {
		if blocker.Domains != 1 || len(blocker.DomainNames) != 1 || blocker.DomainNames[0] != "4sapi.com" {
			t.Fatalf("audit blocker domain evidence mismatch: %+v", blocker)
		}
		if blocker.FirstHour != hour || blocker.LastHour != hour || len(blocker.ChannelIDs) != 1 {
			t.Fatalf("audit blocker time/channel evidence mismatch: %+v", blocker)
		}
		wantChannel := 59
		if blocker.Key == "unallocated_cost" {
			wantChannel = 0
		}
		if blocker.ChannelIDs[0] != wantChannel {
			t.Fatalf("audit blocker channel mismatch: got=%v want=%d", blocker.ChannelIDs, wantChannel)
		}
	}
	if audit.SourcesTruncated || len(audit.Sources) != 1 {
		t.Fatalf("audit source detail mismatch: %+v", audit)
	}
	source := audit.Sources[0]
	if source.Domain != "4sapi.com" || source.EvidenceHours != 1 || source.Requests != 3 ||
		!reflect.DeepEqual(source.SourceGroups, []string{"Gpt-codex"}) || !reflect.DeepEqual(source.UpstreamModels, []string{"gpt-5.6-sol"}) ||
		source.BilledCost.MicroUSD != "1000000" {
		t.Fatalf("unexpected source detail: %+v", source)
	}
}

func TestFinancePairingAuditAggregatesAndSortsDomainEvidence(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	hour := int64(1_788_195_600)
	epoch := strings.Repeat("8", 64)
	rows := []ChannelEconomicsHourPublication{
		{PublicationID: strings.Repeat("d", 64), LogicalKey: economicsLogicalKey("z.example", epoch, hour, 71), Revision: 1,
			Domain: "z.example", AccountEpoch: epoch, HourTs: hour, LocalChannelID: 71, SemanticsVersion: channelEconomicsSemanticsVersion,
			RevenueMicroUSD: 3_000_000, CoverageStatus: "upstream_cost_missing"},
		{PublicationID: strings.Repeat("e", 64), LogicalKey: economicsLogicalKey("a.example", epoch, hour, 72), Revision: 1,
			Domain: "a.example", AccountEpoch: epoch, HourTs: hour, LocalChannelID: 72, SemanticsVersion: channelEconomicsSemanticsVersion,
			RevenueMicroUSD: 2_000_000, CoverageStatus: "upstream_cost_missing"},
	}
	for _, row := range rows {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&ChannelEconomicsHourCurrent{LogicalKey: row.LogicalKey, PublicationID: row.PublicationID, Revision: 1}).Error; err != nil {
			t.Fatal(err)
		}
	}
	audit, err := m.loadFinancePairingAudit(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600})
	if err != nil {
		t.Fatal(err)
	}
	if len(audit.Blockers) != 1 {
		t.Fatalf("expected one aggregated blocker, got %+v", audit.Blockers)
	}
	blocker := audit.Blockers[0]
	if blocker.Rows != 2 || blocker.Domains != 2 || !reflect.DeepEqual(blocker.DomainNames, []string{"a.example", "z.example"}) {
		t.Fatalf("unexpected aggregated domain evidence: %+v", blocker)
	}
	if blocker.Revenue.MicroUSD != "5000000" {
		t.Fatalf("unexpected aggregated blocker revenue: %+v", blocker.Revenue)
	}
}

func TestFinanceUserFactsSubtractRefundsAndKeepGross(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	hour := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 62, Name: "refund", BaseDomain: "4sapi.com", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: hour, ChannelID: 62, ModelName: "gpt", Grp: "g", Success: 2,
		Quota: 5_000_000, RefundQuota: 1_500_000, TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 2, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	statement, coverage, domains, err := m.loadFinanceUserFacts(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+86400, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || statement.GrossUserConsumption.MicroUSD != "10000000" || statement.UserRefunds.MicroUSD != "3000000" || statement.KnownUserConsumption.MicroUSD != "7000000" {
		t.Fatalf("refund netting mismatch: statement=%+v coverage=%+v", statement, coverage)
	}
	if domains["4sapi.com"].ConsumeQuota != 5_000_000 || domains["4sapi.com"].RefundQuota != 1_500_000 {
		t.Fatalf("domain refund facts mismatch: %+v", domains)
	}
}

func TestFinanceClosedNaturalDayScopeExcludesPartialDays(t *testing.T) {
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 9, 1, 10, 0, 0, 0, loc).Unix()
	to := time.Date(2026, 9, 4, 17, 0, 0, 0, loc).Unix()
	now := time.Date(2026, 9, 4, 18, 0, 0, 0, loc).Unix()
	scope := financeClosedNaturalDayScope(stabilityScope{FromTs: from, ToTs: to}, now)
	wantFrom := time.Date(2026, 9, 2, 0, 0, 0, 0, loc).Unix()
	wantTo := time.Date(2026, 9, 4, 0, 0, 0, 0, loc).Unix()
	if scope.FromTs != wantFrom || scope.ToTs != wantTo {
		t.Fatalf("closed natural-day scope mismatch: got=%+v want=[%d,%d)", scope, wantFrom, wantTo)
	}
}

func TestFinanceReportMissingHourFailsClosed(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "4sapi.com")
	hour := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 60, Name: "4s", BaseDomain: "4sapi.com", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 60, ModelName: "gpt", Grp: "g", Success: 3, Quota: 1_500_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 3, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: "4sapi.com", HourTs: hour, BucketSeconds: 3600, Requests: 2, Quota: 500_000, CostUSD: 1, UnitPerUSD: 500_000, Provider: upstreamProviderNewAPI}).Error; err != nil {
		t.Fatal(err)
	}
	createChannelRechargeVersion(t, m, "4sapi.com", 1, hour, 1, 1)
	epoch := strings.Repeat("2", 64)
	publication := insertEconomicsReportHour(t, m, "4sapi.com", epoch, hour, 60, 3_000_000, 1_000_000, 1_000_000, 2_000_000)
	insertEconomicsReportManifest(t, m, "4sapi.com", epoch, hour, publication)
	loc, _ := time.LoadLocation("Asia/Shanghai")
	report, err := m.buildFinanceOperatingReport(context.Background(), time.Unix(hour, 0).In(loc), time.Unix(hour+7200, 0).In(loc))
	if err != nil {
		t.Fatal(err)
	}
	if report.Statement.UserConsumption != nil || report.Statement.CorrectedUpstreamCost != nil || report.Statement.ContributionProfit != nil {
		t.Fatalf("partial interval leaked as exact money: %+v", report.Statement)
	}
	if report.Statement.KnownUserConsumption.MicroUSD != "3000000" || report.Statement.KnownCorrectedUpstreamCost.MicroUSD != "1000000" {
		t.Fatalf("known partial evidence was lost: %+v", report.Statement)
	}
	if report.Statement.KnownContributionProfit.MicroUSD != "2000000" || report.Statement.PairedUserConsumption.MicroUSD != "3000000" {
		t.Fatalf("verified paired evidence was lost or replaced by whole-range subtraction: %+v", report.Statement)
	}
	if report.UserCoverage.MissingHours != 1 || len(report.Periods) != 1 || report.Periods[0].Status != "incomplete" {
		t.Fatalf("missing hour not exposed: %+v %+v", report.UserCoverage, report.Periods)
	}
}

func TestFinanceReportDoesNotTreatMissingUpstreamEvidenceAsZero(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "missing.example")
	hour := int64(1_788_195_600)
	if err := m.storeDB.Create(&ChannelSnap{ID: 61, Name: "missing", BaseDomain: "missing.example", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: hour, ChannelID: 61, ModelName: "gpt", Grp: "g", Success: 1, Quota: 500_000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: hour, Status: "complete", Requests: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	report, err := m.buildFinanceOperatingReport(context.Background(), time.Unix(hour, 0).In(loc), time.Unix(hour+3600, 0).In(loc))
	if err != nil {
		t.Fatal(err)
	}
	if report.Statement.KnownUpstreamBilledCost.MicroUSD != "" || report.Statement.KnownCorrectedUpstreamCost.MicroUSD != "" {
		t.Fatalf("missing upstream evidence was presented as zero: %+v", report.Statement)
	}
	if report.Statement.UpstreamBilledCost != nil || report.Statement.CorrectedUpstreamCost != nil || report.Statement.ContributionProfit != nil {
		t.Fatalf("missing upstream evidence leaked into exact totals: %+v", report.Statement)
	}
	if len(report.Periods) != 1 || report.Periods[0].Status != "incomplete" {
		t.Fatalf("missing evidence status mismatch: %+v", report.Periods)
	}
}

func TestFinancePageAndNavigationAreWired(t *testing.T) {
	for _, required := range []string{
		`data-tab="finance"`, `id="tab-finance"`, `/finance.css?v=14`, `/finance.js?v=`,
		`id="finPeriodRows"`, `id="finDailyRows"`, `id="finCostRows"`, `id="finPairingRows"`, `id="finPairingCoverage"`, `id="finClosureReadiness"`, `id="finUnallocatedSourceRows"`, `id="finBridgeRevenue"`, `id="finBridgeProfit"`, `window.financeActivate`,
		`id="finEvidenceRollout"`, `id="finEvidenceRolloutRows"`, `id="finEvidenceRolloutSummary"`, `历史成本补证计划`, `仅改善上游证据`, `建议首个灰度`,
		`function operatingProfitBlockers`, `上游账单未接入/缺失`, `缺历史充值修正依据`, `AWS 当期未封账`,
		`id="finCURProductRows"`, `AWS 基础设施成本明细`, `renderCURProducts`,
		`id="finGiftEvidence"`, `注册赠送消耗`, `经营收入`, `已配对计费贡献（赠送前）`, `减：修正上游总成本`, `其中：内部测试上游成本`, `id="finInternalAccounts"`, `分组名称候选（非归属证据）`, `不会自动绑定或改变核算金额`, `仅分组名称得到一个候选；不代表令牌归属`, `去渠道管理精确核对`, `window.channelManagementOpenCostSource`, `当前区间没有可发布的上游账单`, `当前区间没有同时核验的收入与成本`, `缺账单证据`,
		`load(true)`, `query.set('fresh', '1')`, `X-Monitor-Finance-Cache`, `生成于`, `后台更新中`,
		`function scheduleStaleRefresh`, `state.refreshAttempts >= 5`, `load(false, true)`,
	} {
		if !strings.Contains(pageHTML+string(financeJS), required) {
			t.Fatalf("finance page wiring missing %q", required)
		}
	}
	js := string(financeJS)
	start := strings.Index(js, "function renderPairingAudit")
	end := strings.Index(js, "function renderPairingSources")
	if start < 0 || end <= start || strings.Count(js[start:end], "row.name || row.key") != 1 {
		t.Fatalf("pairing blocker row must render its name exactly once")
	}
	sidebarGovernance := strings.Index(pageHTML, `data-tab="group-governance" title="分组治理（测试）"`)
	sidebarFinance := strings.Index(pageHTML, `data-tab="finance" title="经营核算"`)
	mobileGovernance := strings.Index(pageHTML, `<div class="tab" data-tab="group-governance">分组治理</div>`)
	mobileFinance := strings.Index(pageHTML, `<div class="tab" data-tab="finance">经营核算</div>`)
	if sidebarGovernance < 0 || sidebarFinance < sidebarGovernance || mobileGovernance < 0 || mobileFinance < mobileGovernance {
		t.Fatal("经营核算必须位于桌面侧栏和移动导航的最后")
	}
}

func TestApplyFinanceGiftAllocationPublishesRevenueOnlyWithCompleteProof(t *testing.T) {
	statement := financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(50_000_000))}
	complete := financeGiftAllocationResult{
		Allocation: financecredit.GiftAllocation{PeriodGiftConsumptionMicroUSD: 12_000_000},
		Coverage:   financeGiftCoverageView{Complete: true},
	}
	if err := applyFinanceGiftAllocation(&statement, complete); err != nil {
		t.Fatal(err)
	}
	if statement.RegistrationGiftConsumption == nil || statement.RegistrationGiftConsumption.MicroUSD != "12000000" ||
		statement.OperatingRevenue == nil || statement.OperatingRevenue.MicroUSD != "38000000" {
		t.Fatalf("statement=%+v", statement)
	}

	incomplete := financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(50_000_000))}
	if err := applyFinanceGiftAllocation(&incomplete, financeGiftAllocationResult{}); err != nil {
		t.Fatal(err)
	}
	if incomplete.RegistrationGiftConsumption != nil || incomplete.OperatingRevenue != nil {
		t.Fatalf("incomplete proof published revenue: %+v", incomplete)
	}
}

func TestApplyFinanceKnownGiftRevenueKeepsPartialAmountNonExact(t *testing.T) {
	statement := financeStatementView{KnownUserConsumption: economicsMoney(50_000_000)}
	result := financeGiftAllocationResult{
		Allocation: financecredit.GiftAllocation{PeriodGiftConsumptionMicroUSD: 12_000_000},
		Coverage:   financeGiftCoverageView{Complete: true},
	}
	if err := applyFinanceKnownGiftRevenue(&statement, statement.KnownUserConsumption, result); err != nil {
		t.Fatal(err)
	}
	if statement.KnownOperatingRevenue.MicroUSD != "38000000" {
		t.Fatalf("known prefix revenue mismatch: %+v", statement)
	}
	if statement.OperatingRevenue != nil {
		t.Fatalf("partial prefix was presented as exact revenue: %+v", statement)
	}
}

func TestApplyFinanceVerifiedPrefixRevenueRefusesLaterFacts(t *testing.T) {
	m := newStabilityTestMonitor(t)
	result := financeGiftAllocationResult{
		Allocation: financecredit.GiftAllocation{PeriodGiftConsumptionMicroUSD: 12_000_000},
		Coverage:   financeGiftCoverageView{Complete: true},
	}
	statement := financeStatementView{KnownUserConsumption: economicsMoney(50_000_000)}
	if err := m.applyFinanceVerifiedPrefixRevenue(context.Background(), &statement, result, 3600, 7200); err != nil {
		t.Fatal(err)
	}
	if statement.KnownOperatingRevenue.MicroUSD != "38000000" || statement.OperatingRevenue != nil {
		t.Fatalf("verified prefix was not retained as partial: %+v", statement)
	}

	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: 3600, ChannelID: 1, TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	withLaterFact := financeStatementView{KnownUserConsumption: economicsMoney(50_000_000)}
	if err := m.applyFinanceVerifiedPrefixRevenue(context.Background(), &withLaterFact, result, 3600, 7200); err != nil {
		t.Fatal(err)
	}
	if withLaterFact.KnownOperatingRevenue.MicroUSD != "" || withLaterFact.OperatingRevenue != nil {
		t.Fatalf("later facts were incorrectly folded into verified prefix revenue: %+v", withLaterFact)
	}
}

func TestApplyFinanceGiftBreakdownsPublishesMonthAndDayRevenue(t *testing.T) {
	report := &financeOperatingReport{
		Periods: []financePeriodView{{
			Period: "2026-05", From: 0, To: 7200,
			Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(70_000_000))},
		}},
		Days: []financeDailyView{
			{Date: "2026-05-01", From: 0, To: 3600, Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(40_000_000))}},
			{Date: "2026-05-02", From: 3600, To: 7200, Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(30_000_000))}},
		},
	}
	result := financeGiftAllocationResult{
		Coverage: financeGiftCoverageView{FromTs: 0, ToTs: 7200, Complete: true},
		ledger: []financecredit.LedgerEvent{
			{UserID: 7, At: 10, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000},
			{UserID: 7, At: 1000, Sequence: 2, Kind: financecredit.EventNetUsage, AmountMicroUSD: 30_000_000},
			{UserID: 7, At: 4000, Sequence: 3, Kind: financecredit.EventNetUsage, AmountMicroUSD: 20_000_000},
		},
	}
	if err := applyFinanceGiftBreakdowns(report, result); err != nil {
		t.Fatal(err)
	}
	if report.Periods[0].Statement.RegistrationGiftConsumption.MicroUSD != "50000000" ||
		report.Periods[0].Statement.OperatingRevenue.MicroUSD != "20000000" {
		t.Fatalf("period statement=%+v", report.Periods[0].Statement)
	}
	if report.Days[0].Statement.RegistrationGiftConsumption.MicroUSD != "30000000" || report.Days[0].Statement.OperatingRevenue.MicroUSD != "10000000" ||
		report.Days[1].Statement.RegistrationGiftConsumption.MicroUSD != "20000000" || report.Days[1].Statement.OperatingRevenue.MicroUSD != "10000000" {
		t.Fatalf("daily statements=%+v", report.Days)
	}
}

func TestApplyFinanceGiftBreakdownsKeepsClosedPeriodsWhenLatestRangeIsPending(t *testing.T) {
	report := &financeOperatingReport{
		Periods: []financePeriodView{
			{Period: "closed", From: 0, To: 3600, Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(40_000_000))}},
			{Period: "pending", From: 3600, To: 7200, Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(30_000_000))}},
		},
	}
	result := financeGiftAllocationResult{
		Coverage: financeGiftCoverageView{FromTs: 0, ToTs: 3600, RequestedToTs: 7200, LatestHourPending: true, Complete: true},
		ledger: []financecredit.LedgerEvent{
			{UserID: 7, At: 10, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000},
			{UserID: 7, At: 1000, Sequence: 2, Kind: financecredit.EventNetUsage, AmountMicroUSD: 30_000_000},
		},
	}
	if err := applyFinanceGiftBreakdowns(report, result); err != nil {
		t.Fatal(err)
	}
	if report.Periods[0].Statement.RegistrationGiftConsumption == nil || report.Periods[0].Statement.RegistrationGiftConsumption.MicroUSD != "30000000" {
		t.Fatalf("closed period was not published: %+v", report.Periods[0].Statement)
	}
	if report.Periods[1].Statement.RegistrationGiftConsumption != nil || report.Periods[1].Statement.OperatingRevenue != nil {
		t.Fatalf("pending period was published: %+v", report.Periods[1].Statement)
	}
}

func TestApplyFinanceGiftBreakdownsPublishesVerifiedPrefixOfOpenPeriod(t *testing.T) {
	report := &financeOperatingReport{
		Periods: []financePeriodView{{
			Period: "open", From: 0, To: 7200,
			Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(40_000_000))},
		}},
		Days: []financeDailyView{{
			Date: "open-day", From: 0, To: 7200,
			Statement: financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(40_000_000))},
		}},
	}
	result := financeGiftAllocationResult{
		Coverage: financeGiftCoverageView{FromTs: 0, ToTs: 3600, RequestedToTs: 7200, LatestHourPending: true, Complete: true},
		ledger: []financecredit.LedgerEvent{
			{UserID: 7, At: 10, Sequence: 1, Kind: financecredit.EventTrialGiftGrant, AmountMicroUSD: 100_000_000},
			{UserID: 7, At: 1000, Sequence: 2, Kind: financecredit.EventNetUsage, AmountMicroUSD: 30_000_000},
		},
	}
	if err := applyFinanceGiftBreakdowns(report, result); err != nil {
		t.Fatal(err)
	}
	for _, statement := range []financeStatementView{report.Periods[0].Statement, report.Days[0].Statement} {
		if statement.RegistrationGiftConsumption == nil || statement.RegistrationGiftConsumption.MicroUSD != "30000000" ||
			statement.OperatingRevenue == nil || statement.OperatingRevenue.MicroUSD != "10000000" {
			t.Fatalf("verified open-period prefix was not published: %+v", statement)
		}
	}
}

func TestFinanceSubtractSupportsRefundReversalAndOverflowGuard(t *testing.T) {
	value, err := financeSubtract(economicsMoney(10_000_000), economicsMoney(-2_000_000))
	if err != nil || value.MicroUSD != "12000000" {
		t.Fatalf("refund reversal difference=%+v err=%v", value, err)
	}
	if _, err := financeSubtract(economicsMoney(math.MaxInt64), economicsMoney(-1)); err == nil {
		t.Fatal("signed subtraction overflow must fail closed")
	}
}
