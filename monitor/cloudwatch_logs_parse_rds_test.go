package monitor

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCloudWatchParseRDSErrorUsesClosedCategoryAndNoRawText(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	message := "2026-09-14T10:00:00Z [ERROR] [MY-000000] Too many connections for user secret-user from 203.0.113.9"
	evidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceRDSError, EventID: "rds-event", TimestampMS: 1789363200000, Message: message})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Kind != cwEvidenceRDSError || evidence.Category != "connection_capacity" || evidence.FaultClass != "rate_limit_capacity" || evidence.Severity != "error" {
		t.Fatalf("rds=%+v", evidence)
	}
	assertCloudWatchEvidenceOmits(t, evidence, message, "secret-user", "203.0.113.9")
}

func TestCloudWatchParseRDSSlowQueryKeepsOnlyStatsOperationAndHMAC(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	message := "# Time: 2026-09-14T10:00:00Z\n# User@Host: nexus[nexus] @  [203.0.113.8]\n# Query_time: 12.345 Lock_time: 0.125 Rows_sent: 2 Rows_examined: 987654\nSET timestamp=1789363200;\nSELECT * FROM users WHERE email='person@example.com' AND api_key='sk-super-secret-value';"
	input := cloudWatchEvidenceInput{Source: cwSourceRDSSlowQuery, EventID: "slow-event", TimestampMS: 1789363200000, Message: message}
	evidence, err := parser.parse(input)
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Kind != cwEvidenceRDSSlowQuery || evidence.Category != "slow_query" || evidence.DBOperation != "SELECT" || evidence.QueryHMAC == "" {
		t.Fatalf("slow=%+v", evidence)
	}
	if evidence.QueryTimeMS == nil || *evidence.QueryTimeMS != 12345 || evidence.LockTimeMS == nil || *evidence.LockTimeMS != 125 || evidence.RowsSent == nil || *evidence.RowsSent != 2 || evidence.RowsExamined == nil || *evidence.RowsExamined != 987654 {
		t.Fatalf("stats=%+v", evidence)
	}
	assertCloudWatchEvidenceOmits(t, evidence, "users", "person@example.com", "sk-super-secret-value", "203.0.113.8", "nexus")
	again, err := parser.parse(input)
	if err != nil || again.QueryHMAC != evidence.QueryHMAC {
		t.Fatal("same query did not produce stable HMAC")
	}
	input.Message = strings.Replace(input.Message, "users", "channels", 1)
	different, err := parser.parse(input)
	if err != nil || different.QueryHMAC == evidence.QueryHMAC {
		t.Fatal("different query did not change HMAC")
	}
}

func TestCloudWatchRDSRejectsOrdinaryAndMalformedRows(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	cases := []struct {
		source  cloudWatchLogSourceID
		message string
		want    cloudWatchEvidenceParseErrorKind
	}{
		{cwSourceRDSError, "routine checkpoint complete", cwParseUnsupported},
		{cwSourceRDSSlowQuery, "SELECT 1", cwParseUnsupported},
		{cwSourceRDSSlowQuery, "# Query_time: NaN Lock_time: 0 Rows_sent: 1 Rows_examined: 2\nSELECT 1", cwParseMalformed},
	}
	for _, tc := range cases {
		_, err := parser.parse(cloudWatchEvidenceInput{Source: tc.source, TimestampMS: 1, Message: tc.message})
		if cwParseErrorKind(err) != tc.want {
			t.Fatalf("case=%+v err=%v", tc, err)
		}
	}
}

func TestCloudWatchEvidenceJSONHasNoRawMessageField(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	evidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Message: "panic: api_key=sk-sensitive email=person@example.com"})
	if err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"message", "panic:", "api_key", "sk-sensitive", "person@example.com"} {
		if strings.Contains(strings.ToLower(string(encoded)), strings.ToLower(forbidden)) {
			t.Fatalf("evidence leaked %q: %s", forbidden, encoded)
		}
	}
}
