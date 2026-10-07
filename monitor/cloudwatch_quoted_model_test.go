package monitor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func TestCloudWatchNewAPIQuotedModelContext(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	for _, tc := range []struct {
		message, model, category string
	}{
		{"Model not found: 'gpt-example'", "gpt-example", "model_not_found"},
		{`Model not found: "gpt-example"`, "gpt-example", "model_not_found"},
		{"Model 'gpt-example' does not exist", "gpt-example", "model_not_found"},
		{"模型 'gpt-example' 不存在", "gpt-example", "model_not_found"},
		{"Token model forbidden: 'gpt-example'", "gpt-example", "model_forbidden"},
		{"No available channel for model 'gpt-example' under group paid", "gpt-example", "route_no_channel"},
		{"Model not found: gpt-example", "gpt-example", "model_not_found"},
	} {
		t.Run(tc.message, func(t *testing.T) {
			in := cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1790596745805,
				Message: "[ERR] 2026/09/28 - 11:59:05 | ABCDEF1234567890 | user 7 | " + tc.message}
			evidence, err := parser.parse(in)
			if err != nil || evidence.Model != tc.model || evidence.Category != tc.category {
				t.Fatalf("quoted context: model=%q category=%q err=%v", evidence.Model, evidence.Category, err)
			}
		})
	}
}

func TestCloudWatchQuotedModelWindowRecoveryAndReplay(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	from := time.Date(2026, 9, 28, 11, 59, 0, 0, time.UTC).Unix()
	message := "Model not found: 'gpt-example'"
	client := &fakeCloudWatchLogsClient{getFn: func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusComplete, Results: [][]cwlogtypes.ResultField{{
			{Field: aws.String("@timestamp"), Value: aws.String("2026-09-28 11:59:05.805")},
			{Field: aws.String("@ptr"), Value: aws.String("quoted-event")},
			{Field: aws.String("@message"), Value: aws.String(message)},
		}}}, nil
	}}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	m.cfg.CloudWatchEvidenceHMACKey = strings.Repeat("k", 32)
	m.cfg.CloudWatchEvidenceHMACKeyID = "fixture-v1"
	state := CloudWatchPreRouteCursor{ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from, ThroughTs: from,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "degraded", LastError: "parse_failed"}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	for range 2 {
		if _, err := m.runCloudWatchPreRouteWindow(context.Background(), &state, from, from+60, from+60); err != nil {
			t.Fatal(err)
		}
	}
	var rows []RejectionSample
	if err := m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].Count != 1 || rows[0].Model != "gpt-example" || rows[0].Reason != "model_not_found" || state.ThroughTs != from+60 || state.LastError != "" {
		t.Fatal("quoted rejection did not recover idempotently")
	}
	// A genuinely unsafe row must still block publication and retain old facts.
	message = "Model not found: 'sk-sensitive'"
	if _, err := m.runCloudWatchPreRouteWindow(context.Background(), &state, from, from+120, from+120); err == nil {
		t.Fatal("unsafe row published")
	}
	var persisted CloudWatchPreRouteCursor
	if err := m.storeDB.First(&persisted, "id = ?", state.ID).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.ThroughTs != from+60 {
		t.Fatal("failed parse advanced durable coverage")
	}
}

func TestCloudWatchQuotedModelStillRejectsUnsafeContent(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	for _, model := range []string{"'gpt-example", "gpt-example'", "'gpt'example'", "'sk-private-value'", "'api_key=value'", "'<script>'", "'" + strings.Repeat("x", 129) + "'"} {
		_, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1790596745805,
			Message: "Model not found: " + model})
		if err == nil {
			t.Fatal("unsafe or unbalanced quoted model accepted")
		}
	}
	_, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1790596745805,
		Fields: map[string]string{"@message": "invalid token", "model": "'gpt-example'"}})
	if err == nil {
		t.Fatal("structured field validation unexpectedly relaxed")
	}
}
