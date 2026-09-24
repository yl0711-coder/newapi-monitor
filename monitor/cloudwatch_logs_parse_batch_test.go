package monitor

import (
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestCloudWatchEvidenceParserRejectsUnsafeConfigurationAndInput(t *testing.T) {
	for _, tc := range []struct{ key, id string }{{"short", "key-1"}, {cloudWatchParserTestKey, "-bad"}, {cloudWatchParserTestKey, strings.Repeat("x", 33)}} {
		if _, err := newCloudWatchEvidenceParser(tc.key, tc.id, false); cwParseErrorKind(err) != cwParseUnsafe {
			t.Fatalf("unsafe config accepted: %+v err=%v", tc, err)
		}
	}
	parser := newCloudWatchParserForTest(t, false)
	for _, input := range []cloudWatchEvidenceInput{
		{Source: "arbitrary", TimestampMS: 1, Message: "panic: x"},
		{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Message: strings.Repeat("x", cloudWatchEvidenceMaxMessageBytes+1)},
		{Source: cwSourceWorkerNewAPI, EventID: "bad id", TimestampMS: 1, Message: "panic: x"},
		{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Message: string([]byte{0xff, 0xfe})},
		{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Fields: map[string]string{"@message": "panic: x", strings.Repeat("k", 257): "bad"}},
		{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Fields: tooManyCloudWatchFields()},
	} {
		_, err := parser.parse(input)
		if kind := cwParseErrorKind(err); kind != cwParseUnsupported && kind != cwParseUnsafe {
			t.Fatalf("input=%+v err=%v", input.Source, err)
		}
	}
}

func tooManyCloudWatchFields() map[string]string {
	fields := make(map[string]string, cloudWatchEvidenceMaxFields+1)
	for index := 0; index <= cloudWatchEvidenceMaxFields; index++ {
		fields[fmt.Sprintf("field-%03d", index)] = "x"
	}
	return fields
}

func TestCloudWatchDerivedEventReferencesAreStableAndDistinct(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	first := cloudWatchEvidenceInput{Source: cwSourceCloudFrontAccess, TimestampMS: 10, Fields: map[string]string{"b": "2", "a": "1"}}
	reordered := cloudWatchEvidenceInput{Source: cwSourceCloudFrontAccess, TimestampMS: 10, Fields: map[string]string{"a": "1", "b": "2"}}
	changed := cloudWatchEvidenceInput{Source: cwSourceCloudFrontAccess, TimestampMS: 10, Fields: map[string]string{"a": "1", "b": "3"}}
	one, two, three := parser.base(first).EventRef, parser.base(reordered).EventRef, parser.base(changed).EventRef
	if one == "" || one != two || one == three {
		t.Fatalf("unstable or colliding refs: %q %q %q", one, two, three)
	}
	otherSource := changed
	otherSource.Source = cwSourceWorkerNewAPI
	if parser.base(otherSource).EventRef == three {
		t.Fatal("event reference did not separate source domains")
	}
}

func TestCloudWatchParseBatchCountsFailuresWithoutDroppingThem(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	result := cloudWatchFilterResult{Source: cwSourceWorkerNewAPI, Events: []cloudWatchRawEvent{
		{EventID: "valid-event", LogStream: "new-api/task-1", TimestampMS: 1789363200000, Message: "panic: credential=never-output"},
		{EventID: "unknown-event", LogStream: "new-api/task-2", TimestampMS: 1789363200001, Message: "ordinary request complete"},
		{EventID: "unsafe-event", LogStream: "new-api/task-3", TimestampMS: 1789363200002, Message: strings.Repeat("x", cloudWatchEvidenceMaxMessageBytes+1)},
	}}
	runtime := newCloudWatchLogsRuntime(false, nil)
	batch := runtime.parseFilterEvidence(parser, result)
	if batch.Source != cwSourceWorkerNewAPI || batch.Parsed != 1 || batch.ParseFailed != 2 || len(batch.Evidence) != 1 || len(batch.Failures) != 2 {
		t.Fatalf("batch=%+v", batch)
	}
	if batch.Failures[0].Kind != cwParseUnsupported || batch.Failures[1].Kind != cwParseUnsafe || batch.Failures[0].EventRef == "" {
		t.Fatalf("failures=%+v", batch.Failures)
	}
	metric := runtime.metricsSnapshot()[cwSourceWorkerNewAPI]
	if !metric.Observed || metric.Parsed != 1 || metric.ParseFailures != 2 || metric.LastParseFailureUnix == 0 || metric.Successes != 0 || metric.Events != 0 {
		t.Fatalf("metric=%+v", metric)
	}
	encoded, _ := json.Marshal(batch)
	for _, forbidden := range []string{"valid-event", "unknown-event", "unsafe-event", "task-1", "never-output", "ordinary request complete"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("batch leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestCloudWatchParseInsightsRowsSupportsTimestampFields(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	result := cloudWatchInsightsResult{Source: cwSourceCloudFrontAccess, Rows: []map[string]string{{
		"@timestamp": "2026-09-14 10:00:00.000", "@ptr": "opaque-pointer", "x-edge-request-id": "edge-id",
		"sc-status": "200", "cs-method": "GET", "cs-uri-stem": "/v1/models", "time-taken": "0.010",
	}}}
	batch := parser.parseInsightsResult(result)
	if batch.Parsed != 1 || batch.ParseFailed != 0 || len(batch.Evidence) != 1 || batch.Evidence[0].EventMS <= 0 || batch.Evidence[0].Summary != "CloudFront记录到 2xx 响应" {
		t.Fatalf("batch=%+v", batch)
	}
	assertCloudWatchEvidenceOmits(t, batch.Evidence[0], "opaque-pointer", "edge-id")
}

func TestCloudWatchEveryCatalogSourceHasAParseContract(t *testing.T) {
	parser := newCloudWatchParserForTest(t, true)
	fixtures := map[cloudWatchLogSourceID]cloudWatchEvidenceInput{
		cwSourceCloudFrontAccess:     cloudFrontFixture(cwSourceCloudFrontAccess),
		cwSourceCloudFrontDiagnostic: cloudFrontFixture(cwSourceCloudFrontDiagnostic),
		cwSourceWorkerNginx:          {Source: cwSourceWorkerNginx, TimestampMS: 1, Message: "2026/09/14 10:00:00 [error] upstream timed out"},
		cwSourceWorkerNewAPI:         {Source: cwSourceWorkerNewAPI, TimestampMS: 1, Message: "panic: fixture"},
		cwSourceMaster:               {Source: cwSourceMaster, TimestampMS: 1, Message: "fatal error: fixture"},
		cwSourceRDSError:             {Source: cwSourceRDSError, TimestampMS: 1, Message: "[ERROR] Too many connections"},
		cwSourceRDSSlowQuery:         {Source: cwSourceRDSSlowQuery, TimestampMS: 1, Message: "# Query_time: 1 Lock_time: 0 Rows_sent: 1 Rows_examined: 2\nSELECT 1;"},
	}
	if len(fixtures) != len(cloudWatchLogSourceCatalog) {
		t.Fatalf("fixture/catalog mismatch: %d/%d", len(fixtures), len(cloudWatchLogSourceCatalog))
	}
	for source := range cloudWatchLogSourceCatalog {
		input, ok := fixtures[source]
		if !ok {
			t.Fatalf("source %s has no fixture", source)
		}
		if _, err := parser.parse(input); err != nil {
			t.Fatalf("source %s: %v", source, err)
		}
	}
}
