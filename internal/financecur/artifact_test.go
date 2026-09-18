package financecur

import (
	"bytes"
	"strings"
	"testing"
	"time"
)

func TestArtifactStrictRoundTrip(t *testing.T) {
	start := time.Date(2026, 9, 1, 0, 0, 0, 0, time.UTC)
	entry := CostEntry{FromUnix: start.Unix(), ToUnix: start.Add(time.Hour).Unix(), LineItemType: "Usage", ProductCode: "AmazonECS", Rows: 1, NanoUSD: 10}
	rules := []AllocationRule{{ID: "rule", ProductCode: "AmazonECS", ResourceMatch: ResourceAny, NexusAPIPPM: AllocationPPM, Reason: "dedicated"}}
	report, err := Allocate([]CostEntry{entry}, rules)
	if err != nil {
		t.Fatal(err)
	}
	statement, timeline, err := BuildStatement(statementTestAudit(start, report), report, testDigest, testDigest, "Asia/Shanghai", false)
	if err != nil {
		t.Fatal(err)
	}
	artifact, err := NewArtifact(statement, timeline)
	if err != nil {
		t.Fatal(err)
	}
	var encoded bytes.Buffer
	if err := EncodeArtifact(&encoded, artifact); err != nil {
		t.Fatal(err)
	}
	decoded, err := ReadArtifact(bytes.NewReader(encoded.Bytes()))
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Statement.StatementID != artifact.Statement.StatementID || decoded.Statement.TimelineSHA256 != artifact.Statement.TimelineSHA256 {
		t.Fatalf("decoded=%+v artifact=%+v", decoded, artifact)
	}
}

func TestArtifactRejectsUnknownTrailingAndTamperedJSON(t *testing.T) {
	tests := []string{
		`{"schema_version":1,"statement":{},"timeline":{},"unknown":true}`,
		`{} {}`,
	}
	for _, input := range tests {
		if _, err := ReadArtifact(strings.NewReader(input)); err == nil {
			t.Fatalf("artifact must fail: %s", input)
		}
	}
}
