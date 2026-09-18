package financecur

import (
	"math"
	"testing"
	"time"
)

const testDigest = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

func TestBuildAndVerifyDraftStatement(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entries := []CostEntry{
		{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", ResourceID: "nexus", Rows: 1, NanoUSD: 80},
		{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "Unknown", ResourceID: "unknown", Rows: 1, NanoUSD: 10},
	}
	rules := []AllocationRule{{ID: "nexus", ProductCode: "AmazonECS", ResourceMatch: ResourceExact, ResourceID: "nexus", NexusAPIPPM: AllocationPPM, Reason: "dedicated"}}
	report, err := Allocate(entries, rules)
	if err != nil {
		t.Fatal(err)
	}
	statement, timeline, err := BuildStatement(statementTestAudit(start, report), report, testDigest, testDigest, "Asia/Shanghai", false)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Status != StatementStatusDraft || statement.StatementID == "" || statement.CoveragePPM != 888_888 {
		t.Fatalf("statement=%+v", statement)
	}
	if err := VerifyStatement(statement, timeline); err != nil {
		t.Fatal(err)
	}

	tampered := statement
	tampered.NexusAPINanoUSD++
	if err := VerifyStatement(tampered, timeline); err == nil {
		t.Fatal("tampered statement must fail verification")
	}
	tamperedTimeline := timeline
	tamperedTimeline.Days = append([]TimeBucket(nil), timeline.Days...)
	tamperedTimeline.Days[0].NexusAPINanoUSD++
	if err := VerifyStatement(statement, tamperedTimeline); err == nil {
		t.Fatal("tampered timeline must fail verification")
	}
}

func TestBuildPublishableStatementIsDeterministic(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entries := []CostEntry{
		{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", ResourceID: "nexus", Rows: 1, NanoUSD: 100},
		{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Credit", ProductCode: "Other", ResourceID: "other", Rows: 1, NanoUSD: -25},
	}
	rules := []AllocationRule{
		{ID: "nexus", ProductCode: "AmazonECS", ResourceMatch: ResourceExact, ResourceID: "nexus", NexusAPIPPM: AllocationPPM, Reason: "dedicated"},
		{ID: "other", ProductCode: "Other", ResourceMatch: ResourceExact, ResourceID: "other", NexusAPIPPM: 0, Reason: "other project"},
	}
	report, err := Allocate(entries, rules)
	if err != nil {
		t.Fatal(err)
	}
	audit := statementTestAudit(start, report)
	first, firstTimeline, err := BuildStatement(audit, report, testDigest, testDigest, "Asia/Shanghai", true)
	if err != nil {
		t.Fatal(err)
	}
	second, secondTimeline, err := BuildStatement(audit, report, testDigest, testDigest, "Asia/Shanghai", true)
	if err != nil {
		t.Fatal(err)
	}
	if first.Status != StatementStatusPublishable || first.CoveragePPM != AllocationPPM || first.StatementID != second.StatementID || first.TimelineSHA256 != second.TimelineSHA256 {
		t.Fatalf("first=%+v second=%+v", first, second)
	}
	if err := VerifyStatement(first, firstTimeline); err != nil {
		t.Fatal(err)
	}
	if err := VerifyStatement(second, secondTimeline); err != nil {
		t.Fatal(err)
	}
}

func TestBuildStatementRejectsInconsistentEvidence(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 10}
	rules := []AllocationRule{{ID: "rule", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "dedicated"}}
	report, err := Allocate([]CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	audit := statementTestAudit(start, report)
	audit.TotalNanoUSD--
	if _, _, err := BuildStatement(audit, report, testDigest, testDigest, "Asia/Shanghai", false); err == nil {
		t.Fatal("mismatched source and allocation totals must fail")
	}
	audit.TotalNanoUSD = report.TotalNanoUSD
	if _, _, err := BuildStatement(audit, report, "bad", testDigest, "Asia/Shanghai", false); err == nil {
		t.Fatal("invalid source digest must fail")
	}
}

func TestCompleteAllocationRemainsDraftUntilBillingIsFinalized(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 10}
	rules := []AllocationRule{{ID: "rule", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "dedicated"}}
	report, err := Allocate([]CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	statement, _, err := BuildStatement(statementTestAudit(start, report), report, testDigest, testDigest, "Asia/Shanghai", false)
	if err != nil {
		t.Fatal(err)
	}
	if statement.Status != StatementStatusDraft || !statement.AllocationComplete || statement.BillingFinalized {
		t.Fatalf("statement=%+v", statement)
	}
}

func TestCoveragePPMDoesNotOverflowAtUint64Boundary(t *testing.T) {
	if got := coveragePPM(math.MaxUint64-1, math.MaxUint64); got != AllocationPPM-1 {
		t.Fatalf("coverage=%d", got)
	}
	if got := coveragePPM(0, math.MaxUint64); got != 0 {
		t.Fatalf("zero coverage=%d", got)
	}
}

func statementTestAudit(start time.Time, report AllocationReport) Audit {
	return Audit{
		Rows: report.Rows, Currency: "USD", BillingPeriodStart: start.Unix(), BillingPeriodEnd: start.AddDate(0, 1, 0).Unix(),
		FirstUsageUnix: start.Unix(), LastUsageThroughUnix: start.Add(time.Hour).Unix(), TotalNanoUSD: report.TotalNanoUSD,
	}
}
