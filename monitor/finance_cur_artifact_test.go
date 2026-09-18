package monitor

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

func TestFinanceCURProjectionDraftRemainsKnownOnly(t *testing.T) {
	artifact := financeCURTestArtifact(t, false)
	day := artifact.Timeline.Days[0]
	view, err := projectFinanceCURCost(artifact, day.FromUnix, day.ToUnix)
	if err != nil {
		t.Fatal(err)
	}
	if !view.Loaded || view.Status != "draft" || view.RangeComplete || view.ExactCost != nil || view.KnownCost.MicroUSD != "1000000" {
		t.Fatalf("unsafe draft projection: %+v", view)
	}
	if view.CoveragePPM != 1_000_000 || view.UnallocatedNanoUSD != "0" || view.UnallocatedCost.MicroUSD != "0" ||
		view.ConflictNanoUSD != "0" || view.ConflictCost.MicroUSD != "0" {
		t.Fatalf("allocation evidence mismatch: %+v", view)
	}
	if !view.AllocationComplete || view.BillingFinalized {
		t.Fatalf("draft blockers were not preserved: %+v", view)
	}
	statement := financeStatementView{}
	applyFinanceCURCost(&statement, view)
	if statement.AWSInfrastructureCost != nil || statement.KnownAWSInfrastructureCost.MicroUSD != "1000000" || statement.OperatingProfit != nil {
		t.Fatalf("draft cost was treated as exact: %+v", statement)
	}
}

func TestFinanceCURProjectionVerifiedCanCloseExactRange(t *testing.T) {
	artifact := financeCURTestArtifact(t, true)
	day := artifact.Timeline.Days[0]
	view, err := projectFinanceCURCost(artifact, day.FromUnix, day.ToUnix)
	if err != nil {
		t.Fatal(err)
	}
	if view.Status != "verified" || !view.RangeComplete || view.ExactCost == nil || view.ExactCost.MicroUSD != "1000000" {
		t.Fatalf("verified projection mismatch: %+v", view)
	}
	if !view.AllocationComplete || !view.BillingFinalized {
		t.Fatalf("verified CUR state mismatch: %+v", view)
	}
	statement := financeStatementView{
		OperatingRevenue:      financeMoneyPointer(economicsMoney(5_000_000)),
		CorrectedUpstreamCost: financeMoneyPointer(economicsMoney(2_000_000)),
	}
	applyFinanceCURCost(&statement, view)
	if statement.OperatingProfit == nil || statement.OperatingProfit.MicroUSD != "2000000" {
		t.Fatalf("operating profit mismatch: %+v", statement)
	}
}

func TestFinanceCURDraftStatusSeparatesAllocationAndBillingBlockers(t *testing.T) {
	view := financeCURCostView{
		Status:             "draft",
		CoveragePPM:        999_992,
		IssueCount:         87,
		AllocationComplete: false,
		BillingFinalized:   false,
		KnownCost:          economicsMoney(305_998_257),
		UnallocatedCost:    economicsMoney(2_891),
	}
	status, description := financeCURSourceStatus(view)
	if status != "incomplete" || !strings.Contains(description, "归属未闭合（87 条，$0.002891）") ||
		!strings.Contains(description, "AWS 当期账单未封账") {
		t.Fatalf("draft blockers are ambiguous: status=%q description=%q", status, description)
	}
}

func TestFinanceCURLoaderFailsClosedWithoutBreakingOtherReportData(t *testing.T) {
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: filepath.Join(t.TempDir(), "missing.json")}}
	view := m.loadFinanceCURCost(1, 2)
	if view.Status != "error" || view.Loaded || view.KnownCost.MicroUSD != "" || view.Error == "" {
		t.Fatalf("missing artifact did not fail closed: %+v", view)
	}
	path := filepath.Join(t.TempDir(), "artifact.json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString(`{"schema_version":1}`); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	m.cfg.FinanceCURArtifactPath = path
	view = m.loadFinanceCURCost(1, 2)
	if view.Status != "error" || view.Loaded || view.KnownCost.MicroUSD != "" {
		t.Fatalf("invalid artifact did not fail closed: %+v", view)
	}
}

func TestFinanceCURArtifactProjectsTotalMonthAndDayFromOneSource(t *testing.T) {
	artifact := financeCURTestArtifact(t, false)
	path := filepath.Join(t.TempDir(), "artifact.json")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	if err := financecur.EncodeArtifact(file, artifact); err != nil {
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}
	day := artifact.Timeline.Days[0]
	report := &financeOperatingReport{
		From: day.FromUnix, To: day.ToUnix,
		Periods: []financePeriodView{{From: day.FromUnix, To: day.ToUnix, Status: "verified"}},
		Days:    []financeDailyView{{From: day.FromUnix, To: day.ToUnix, Status: "verified"}},
	}
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	m.applyFinanceCURArtifactToReport(report)
	if report.CURCost.Status != "draft" || report.Statement.KnownAWSInfrastructureCost.MicroUSD != "1000000" {
		t.Fatalf("total projection mismatch: cur=%+v statement=%+v", report.CURCost, report.Statement)
	}
	if report.Periods[0].Statement.KnownAWSInfrastructureCost.MicroUSD != "1000000" || report.Periods[0].Status != "incomplete" {
		t.Fatalf("month projection mismatch: %+v", report.Periods[0])
	}
	if report.Days[0].Statement.KnownAWSInfrastructureCost.MicroUSD != "1000000" || report.Days[0].Status != "incomplete" {
		t.Fatalf("day projection mismatch: %+v", report.Days[0])
	}
	if len(report.CURProducts) != 1 || report.CURProducts[0].ProductCode != "AmazonECS" ||
		report.CURProducts[0].KnownCost.MicroUSD != "1000000" || report.CURProducts[0].SharePercent != "100.00" ||
		report.CURProducts[0].Status != "incomplete" {
		t.Fatalf("product projection mismatch: %+v", report.CURProducts)
	}
}

func TestFinanceCURProjectionDoesNotProratePartialNaturalDay(t *testing.T) {
	artifact := financeCURTestArtifact(t, false)
	day := artifact.Timeline.Days[0]
	view, err := projectFinanceCURCost(artifact, day.FromUnix, day.ToUnix-1)
	if err != nil {
		t.Fatal(err)
	}
	if view.IncludedDays != 0 || view.KnownCost.MicroUSD != "" || view.ExactCost != nil {
		t.Fatalf("partial day was estimated: %+v", view)
	}
}

func TestFinanceCURRoundedChildrenReconcileToParent(t *testing.T) {
	parent := financeCURCostView{IncludedDays: 2, KnownNanoUSD: "3000", KnownCost: economicsMoney(3)}
	children := []financeCURCostView{
		{IncludedDays: 1, KnownNanoUSD: "1500", KnownCost: economicsMoney(2)},
		{IncludedDays: 1, KnownNanoUSD: "1500", KnownCost: economicsMoney(2)},
	}
	adjustment, err := reconcileFinanceCURRoundedCosts(parent, children)
	if err != nil {
		t.Fatal(err)
	}
	first, _ := financeMoneyInt64(children[0].KnownCost)
	last, _ := financeMoneyInt64(children[1].KnownCost)
	if adjustment != -1 || first != 2 || last != 1 || first+last != 3 {
		t.Fatalf("rounding did not reconcile: adjustment=%d children=%+v", adjustment, children)
	}
}

func TestFinanceCURRoundedChildrenRejectNanoMismatch(t *testing.T) {
	parent := financeCURCostView{IncludedDays: 2, KnownNanoUSD: "3000", KnownCost: economicsMoney(3)}
	children := []financeCURCostView{{IncludedDays: 1, KnownNanoUSD: "1500", KnownCost: economicsMoney(2)}}
	if _, err := reconcileFinanceCURRoundedCosts(parent, children); err == nil {
		t.Fatal("non-reconciling child evidence accepted")
	}
}

func TestFinanceCURSettingsDefaultOffAndRequireAbsolutePath(t *testing.T) {
	t.Setenv("MONITOR_FINANCE_CUR_ARTIFACT_ENABLED", "")
	t.Setenv("MONITOR_FINANCE_CUR_ARTIFACT_PATH", "")
	settings := LoadSettings()
	if settings.FinanceCURArtifactEnabled || settings.FinanceCURArtifactPath != "" {
		t.Fatalf("unsafe CUR defaults: %+v", settings)
	}
	if err := validateFinanceSettings(Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: "relative.json"}); err == nil {
		t.Fatal("relative CUR artifact path accepted")
	}
	if err := validateFinanceSettings(Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: "/data/cur.json"}); err != nil {
		t.Fatal(err)
	}
}

func TestNanoUSDToRoundedMicroUSD(t *testing.T) {
	tests := map[int64]int64{1_000: 1, 1_499: 1, 1_500: 2, -1_499: -1, -1_500: -2}
	for input, want := range tests {
		got, err := nanoUSDToRoundedMicroUSD(input)
		if err != nil || got != want {
			t.Fatalf("nano=%d got=%d want=%d err=%v", input, got, want, err)
		}
	}
}

func TestFinanceCURSharePercentAndAbsoluteMagnitude(t *testing.T) {
	if got := financeCURSharePercent("250", "1000"); got != "25.00" {
		t.Fatalf("share=%q", got)
	}
	if got := financeCURSharePercent("-25", "100"); got != "-25.00" {
		t.Fatalf("negative share=%q", got)
	}
	if got := financeCURSharePercent("1", "0"); got != "" {
		t.Fatalf("zero-total share=%q", got)
	}
	if got := financeCURAbs(-1 << 63); got != uint64(1)<<63 {
		t.Fatalf("min-int magnitude=%d", got)
	}
}

func financeCURTestArtifact(t *testing.T, finalized bool) financecur.Artifact {
	t.Helper()
	loc, _ := time.LoadLocation("Asia/Shanghai")
	start := time.Date(2026, 9, 2, 0, 0, 0, 0, loc).UTC()
	entry := financecur.CostEntry{
		FromUnix: start.Unix(), ToUnix: start.Add(24 * time.Hour).Unix(), LineItemType: "Usage",
		ProductCode: "AmazonECS", Rows: 1, NanoUSD: 1_000_000_000,
	}
	rules := []financecur.AllocationRule{{
		ID: "ecs", ProductCode: "AmazonECS", ResourceMatch: financecur.ResourceAny,
		NexusAPIPPM: financecur.AllocationPPM, Reason: "dedicated",
	}}
	report, err := financecur.Allocate([]financecur.CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	audit := financecur.Audit{
		Rows: 1, Currency: "USD", BillingPeriodStart: start.Add(-time.Hour).Unix(),
		BillingPeriodEnd: start.AddDate(0, 1, 0).Unix(), FirstUsageUnix: start.Unix(),
		LastUsageThroughUnix: start.Add(24 * time.Hour).Unix(), TotalNanoUSD: 1_000_000_000,
	}
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	statement, timeline, err := financecur.BuildStatement(audit, report, digest, digest, "Asia/Shanghai", finalized)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := financecur.NewArtifact(statement, timeline)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}
