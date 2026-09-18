package main

import (
	"bytes"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
)

func TestReadPolicyRejectsUnknownAndTrailingJSON(t *testing.T) {
	dir := t.TempDir()
	tests := map[string]string{
		"unknown":  `[{"id":"one","unknown":true}]`,
		"trailing": `[] {}`,
	}
	for name, content := range tests {
		path := filepath.Join(dir, name+".json")
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readPolicy(path); err == nil {
			t.Fatalf("%s policy must fail", name)
		}
	}
}

func TestWriteArtifactExclusiveRoundTripAndNoOverwrite(t *testing.T) {
	artifact := testArtifact(t)
	path := filepath.Join(t.TempDir(), "finance-artifact.json")
	if err := writeArtifactExclusive(path, artifact); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode=%#o", got)
	}
	content, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := financecur.ReadArtifact(bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Statement.StatementID != artifact.Statement.StatementID {
		t.Fatalf("statement_id=%q want=%q", decoded.Statement.StatementID, artifact.Statement.StatementID)
	}
	before := string(content)
	if err := writeArtifactExclusive(path, artifact); err == nil {
		t.Fatal("existing artifact must not be overwritten")
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(after) != before {
		t.Fatal("existing artifact changed")
	}
}

func TestWriteArtifactExclusiveRejectsEmptyPath(t *testing.T) {
	if err := writeArtifactExclusive("", testArtifact(t)); err == nil {
		t.Fatal("empty path must fail")
	}
}

func TestLimitAllocationOutputIsBoundedAndCanBeDisabled(t *testing.T) {
	items := []int{1, 2, 3}
	if got := limitAllocationOutput(items, 2); len(got) != 2 || got[1] != 2 {
		t.Fatalf("bounded output=%v", got)
	}
	if got := limitAllocationOutput(items, 0); got != nil {
		t.Fatalf("disabled output=%v", got)
	}
	if got := limitAllocationOutput(items, 10); len(got) != len(items) {
		t.Fatalf("full output=%v", got)
	}
}

func testArtifact(t *testing.T) financecur.Artifact {
	t.Helper()
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := financecur.CostEntry{
		FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(),
		LineItemType: "Usage", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 10,
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
		Rows: 1, Currency: "USD", BillingPeriodStart: start.Unix(),
		BillingPeriodEnd: start.AddDate(0, 1, 0).Unix(), FirstUsageUnix: start.Unix(),
		LastUsageThroughUnix: start.Add(time.Hour).Unix(), TotalNanoUSD: 10,
	}
	const digest = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	statement, timeline, err := financecur.BuildStatement(audit, report, digest, digest, "Asia/Shanghai", false)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := financecur.NewArtifact(statement, timeline)
	if err != nil {
		t.Fatal(err)
	}
	return artifact
}

func TestReadPolicyAcceptsExplicitRules(t *testing.T) {
	path := filepath.Join(t.TempDir(), "policy.json")
	content := `[{"id":"ecs","product_code":"AmazonECS","resource_match":"prefix","resource_id":"arn:aws:ecs:","nexusapi_ppm":1000000,"reason":"dedicated"}]`
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	rules, err := readPolicy(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(rules) != 1 || rules[0].ID != "ecs" {
		t.Fatalf("rules=%+v", rules)
	}
}
