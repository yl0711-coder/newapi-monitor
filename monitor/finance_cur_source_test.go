package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

func TestFinanceCURSourceRejectsCEMonthlyAsDailyCost(t *testing.T) {
	monthly := financecur.CEMonthlyEvidence{
		Kind: "aws_ce_monthly_evidence", TimeZone: "UTC", Currency: "USD", Metric: "NetUnblendedCost",
		Months: []financecur.CEMonthEvidence{{Period: "2026-08", FromUTC: "2026-08-01", ToUTC: "2026-09-01", NetUSD: "197.1008700236", VerifiedDays: 31}},
	}
	raw, err := json.Marshal(monthly)
	if err != nil {
		t.Fatal(err)
	}
	if source, err := readFinanceCURSource(bytes.NewReader(raw)); err == nil || source != nil {
		t.Fatal("UTC monthly evidence was accepted as Shanghai daily cost")
	}
	path := filepath.Join(t.TempDir(), "monthly-evidence.json")
	if err := os.WriteFile(path, raw, 0600); err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	r := financeCURRangeReport(t, 1785513600, 1788192000)
	r.Statement.AWSInfrastructureCost = financeMoneyPointer(economicsMoney(123))
	r.Statement.OperatingProfit = financeMoneyPointer(economicsMoney(456))
	m.applyFinanceCURArtifactToReport(r)
	if r.CURCost.Status != "error" || r.Statement.AWSInfrastructureCost != nil || r.Statement.OperatingProfit != nil || r.Statement.OperatingRevenue.MicroUSD != "5000000" || r.Statement.RawCorrectedUpstreamCost.MicroUSD != "2000000" {
		t.Fatal("misconfigured monthly evidence left a stale profit or changed business facts")
	}
}

func financeCURMonthFixture(t *testing.T, month time.Month, cost int64, finalized bool) financecur.Artifact {
	t.Helper()
	start := time.Date(2026, month, 1, 0, 0, 0, 0, time.UTC)
	end := start.AddDate(0, 1, 0)
	r, err := financecur.Allocate([]financecur.CostEntry{{FromUnix: start.Unix(), ToUnix: end.Unix(), ProductCode: "AmazonECS", LineItemType: "Usage", Rows: 1, NanoUSD: cost}}, []financecur.AllocationRule{{ID: "ecs", ProductCode: "AmazonECS", ResourceMatch: financecur.ResourceAny, NexusAPIPPM: 1_000_000, Reason: "synthetic source"}})
	if err != nil {
		t.Fatal(err)
	}
	a := financecur.Audit{Currency: "USD", Rows: 1, BillingPeriodStart: start.Unix(), BillingPeriodEnd: end.Unix(), FirstUsageUnix: start.Unix(), LastUsageThroughUnix: end.Unix(), TotalNanoUSD: cost}
	digest := strings.Repeat("a", 64)
	s, tl, err := financecur.BuildStatement(a, r, digest, digest, "Asia/Shanghai", finalized)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := financecur.NewArtifact(s, tl)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func financeCURWriteSet(t *testing.T, path string, artifacts ...financecur.Artifact) {
	t.Helper()
	set, err := financecur.NewSourceSet(artifacts)
	if err != nil {
		t.Fatal(err)
	}
	var b bytes.Buffer
	if err := financecur.EncodeSourceSet(&b, set); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, b.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
}

func financeCURRangeReport(t *testing.T, from, to int64) *financeOperatingReport {
	t.Helper()
	loc, err := time.LoadLocation("Asia/Shanghai")
	if err != nil {
		t.Fatal(err)
	}
	r := &financeOperatingReport{From: from, To: to}
	for cursor := from; cursor < to; {
		d := time.Unix(cursor, 0).In(loc)
		end := min(to, time.Date(d.Year(), d.Month()+1, 1, 0, 0, 0, 0, loc).Unix())
		r.Periods = append(r.Periods, financePeriodView{From: cursor, To: end})
		cursor = end
	}
	for cursor := from; cursor < to; {
		end := min(to, time.Unix(cursor, 0).In(loc).AddDate(0, 0, 1).Unix())
		r.Days = append(r.Days, financeDailyView{From: cursor, To: end})
		cursor = end
	}
	r.Statement.OperatingRevenue = financeMoneyPointer(economicsMoney(5_000_000))
	r.Statement.RawCorrectedUpstreamCost = financeMoneyPointer(economicsMoney(2_000_000))
	return r
}

func TestFinanceCURSourceSetReportAndSafeFailure(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cur.json")
	sep := financeCURMonthFixture(t, 9, 720_000_000, true)
	oct := financeCURMonthFixture(t, 10, 744_000_000, true)
	financeCURWriteSet(t, path, sep, oct)
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 9, 30, 0, 0, 0, 0, loc).Unix()
	to := time.Date(2026, 10, 2, 0, 0, 0, 0, loc).Unix()
	r := financeCURRangeReport(t, from, to)
	m.applyFinanceCURArtifactToReport(r)
	if r.CURCost.Status != "verified" || r.Statement.AWSInfrastructureCost == nil || r.Statement.AWSInfrastructureCost.MicroUSD != "48000" || r.Statement.OperatingProfit == nil || r.Statement.OperatingProfit.MicroUSD != "2952000" {
		t.Fatalf("report mismatch: %+v %+v", r.CURCost, r.Statement)
	}
	for _, period := range r.Periods {
		if period.Statement.AWSInfrastructureCost == nil || period.Statement.AWSInfrastructureCost.MicroUSD != "24000" {
			t.Fatal("month lost cost", period.Statement)
		}
	}
	for _, day := range r.Days {
		if day.Statement.AWSInfrastructureCost == nil || day.Statement.AWSInfrastructureCost.MicroUSD != "24000" {
			t.Fatal("day lost cost", day.Statement)
		}
	}
	if len(r.CURProducts) != 1 || r.CURProducts[0].ExactCost == nil || r.CURProducts[0].ExactCost.MicroUSD != "48000" {
		t.Fatal("product mismatch", r.CURProducts)
	}
	if err := os.WriteFile(path, []byte("broken"), 0600); err != nil {
		t.Fatal(err)
	}
	m.applyFinanceCURArtifactToReport(r)
	if r.CURCost.Status != "error" || r.Statement.AWSInfrastructureCost != nil || r.Statement.OperatingProfit != nil || r.Statement.KnownAWSInfrastructureCost.MicroUSD != "" || len(r.CURProducts) != 0 {
		t.Fatal("old AWS result survived failure")
	}
	if r.Statement.OperatingRevenue.MicroUSD != "5000000" || r.Statement.RawCorrectedUpstreamCost.MicroUSD != "2000000" {
		t.Fatal("unrelated money changed")
	}
	for _, day := range r.Days {
		if day.Statement.AWSInfrastructureCost != nil {
			t.Fatal("old day cost survived")
		}
	}
	financeCURWriteSet(t, path, sep, oct)
	m.applyFinanceCURArtifactToReport(r)
	if r.Statement.AWSInfrastructureCost == nil {
		t.Fatal("recovered source not loaded")
	}
	m.cfg.FinanceCURArtifactEnabled = false
	m.applyFinanceCURArtifactToReport(r)
	if r.CURCost.Status != "disabled" || r.Statement.AWSInfrastructureCost != nil || r.Statement.OperatingProfit != nil {
		t.Fatal("disabled source reused old cost")
	}
}

func TestFinanceCURSourceSetCacheIdentityAndDraftIsolation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cur.json")
	sep := financeCURMonthFixture(t, 9, 720_000_000, true)
	oct := financeCURMonthFixture(t, 10, 744_000_000, false)
	financeCURWriteSet(t, path, sep, oct)
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	loc, _ := time.LoadLocation("Asia/Shanghai")
	from := time.Date(2026, 9, 2, 0, 0, 0, 0, loc).Unix()
	v := m.loadFinanceCURCost(from, from+86400)
	if v.ExactCost == nil || !v.BillingFinalized || v.SourceCount != 1 {
		t.Fatal("unrelated October draft blocked September", v)
	}
	ctx := context.Background()
	before, err := financeCURArtifactSourceVersion(ctx, path)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	updated := financeCURMonthFixture(t, 9, 730_000_000, true)
	financeCURWriteSet(t, path, updated, oct)
	if err := os.Chtimes(path, info.ModTime(), info.ModTime()); err != nil {
		t.Fatal(err)
	}
	after, err := financeCURArtifactSourceVersion(ctx, path)
	if err != nil || before == after {
		t.Fatal("revision failed to invalidate cache", err)
	}
	financeCURWriteSet(t, path, oct, updated)
	reordered, err := financeCURArtifactSourceVersion(ctx, path)
	if err != nil || after != reordered {
		t.Fatal("source order changed cache identity", err)
	}
	boundary := time.Date(2026, 10, 1, 0, 0, 0, 0, loc).Unix()
	v = m.loadFinanceCURCost(boundary, boundary+86400)
	if v.ExactCost != nil || v.BillingFinalized || v.SourceCount != 2 {
		t.Fatal("draft boundary became exact", v)
	}
	financeCURWriteSet(t, path, sep)
	v = m.loadFinanceCURCost(boundary, boundary+86400)
	if v.ExactCost != nil || len(v.MissingBillingPeriods) != 1 || v.KnownCost.MicroUSD != "8000" {
		t.Fatal("missing October treated as zero", v)
	}
	_, description := financeCURSourceStatus(v)
	if !strings.Contains(description, "缺少 1 段账单来源") {
		t.Fatal(description)
	}
}

func TestFinanceCURSourceStrictReader(t *testing.T) {
	for _, input := range []string{`{"kind":"unknown"}`, `{} {}`, `{"kind":"cur_source_set","schema_version":1,"artifacts":[],"extra":1}`, strings.Repeat(" ", maxFinanceCURSourceBytes+1)} {
		if _, err := readFinanceCURSource(strings.NewReader(input)); err == nil {
			t.Fatal("invalid source accepted")
		}
	}
	if _, err := readFinanceCURSource(nil); err == nil {
		t.Fatal("nil reader accepted")
	}
	legacy := financeCURTestArtifact(t, true)
	var b bytes.Buffer
	if err := financecur.EncodeArtifact(&b, legacy); err != nil {
		t.Fatal(err)
	}
	s, err := readFinanceCURSource(&b)
	if err != nil || s.version() != "statement:"+legacy.Statement.StatementID {
		t.Fatal("legacy compatibility failed", err)
	}
	set, err := financecur.NewSourceSet([]financecur.Artifact{legacy})
	if err != nil {
		t.Fatal(err)
	}
	set.Artifacts[0].Statement.NexusAPINanoUSD++
	corrupt, err := json.Marshal(set)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := readFinanceCURSource(bytes.NewReader(corrupt)); err == nil {
		t.Fatal("corrupt child artifact accepted")
	}
}

func TestFinanceCURSourceFormatRollbackWithoutDataMigration(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cur.json")
	legacy := financeCURTestArtifact(t, true)
	day := legacy.Timeline.Days[0]
	financeCURWriteSet(t, path, legacy)
	m := &Monitor{cfg: Settings{FinanceCURArtifactEnabled: true, FinanceCURArtifactPath: path}}
	before := m.loadFinanceCURCost(day.FromUnix, day.ToUnix)
	version, err := financeCURArtifactSourceVersion(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := financecur.EncodeArtifact(&encoded, legacy); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, encoded.Bytes(), 0600); err != nil {
		t.Fatal(err)
	}
	after := m.loadFinanceCURCost(day.FromUnix, day.ToUnix)
	restored, err := financeCURArtifactSourceVersion(context.Background(), path)
	if err != nil {
		t.Fatal(err)
	}
	if before.ExactCost == nil || after.ExactCost == nil || *before.ExactCost != *after.ExactCost || restored == version || restored != "statement:"+legacy.Statement.StatementID {
		t.Fatal("legacy format rollback failed")
	}
}

func TestFinanceCURSourceSetFullFingerprintLeavesBaseFactsUnchanged(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "cur-source-set.example")
	path := filepath.Join(t.TempDir(), "cur.json")
	m.cfg.FinanceCURArtifactEnabled = true
	m.cfg.FinanceCURArtifactPath = path
	sep := financeCURMonthFixture(t, 9, 720_000_000, true)
	oct := financeCURMonthFixture(t, 10, 744_000_000, false)
	financeCURWriteSet(t, path, sep, oct)
	ctx := context.Background()
	from, to := sep.Timeline.Days[1].FromUnix, sep.Timeline.Days[1].ToUnix
	full, err := m.financeReportSourceFingerprintForScope(ctx, from, to, true)
	if err != nil {
		t.Fatal(err)
	}
	base, err := m.financeReportSourceFingerprintForScope(ctx, from, to, false)
	if err != nil {
		t.Fatal(err)
	}
	sep = financeCURMonthFixture(t, 9, 730_000_000, true)
	financeCURWriteSet(t, path, sep, oct)
	changed, err := m.financeReportSourceFingerprintForScope(ctx, from, to, true)
	if err != nil || changed == full {
		t.Fatal("full report cache not invalidated", err)
	}
	same, err := m.financeReportSourceFingerprintForScope(ctx, from, to, false)
	if err != nil || same != base {
		t.Fatal("cost update invalidated unrelated base facts", err)
	}
}
