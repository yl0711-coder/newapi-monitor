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

func TestValidateCloudWatchPreRouteSettings(t *testing.T) {
	valid := Settings{CloudWatchLogsEnabled: true, CloudWatchPreRouteEnabled: true,
		CloudWatchPreRoutePollSeconds: 300, CloudWatchPreRouteLookbackHours: 168}
	if err := validateCloudWatchPreRouteSettings(valid); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []Settings{
		{CloudWatchPreRouteEnabled: true, CloudWatchPreRoutePollSeconds: 300, CloudWatchPreRouteLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchPreRouteEnabled: true, LocalSnapshotOnly: true, CloudWatchPreRoutePollSeconds: 300, CloudWatchPreRouteLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchPreRouteEnabled: true, CloudWatchPreRoutePollSeconds: 59, CloudWatchPreRouteLookbackHours: 168},
		{CloudWatchLogsEnabled: true, CloudWatchPreRouteEnabled: true, CloudWatchPreRoutePollSeconds: 300, CloudWatchPreRouteLookbackHours: 169},
	} {
		if err := validateCloudWatchPreRouteSettings(bad); err == nil {
			t.Fatalf("unsafe config accepted: %+v", bad)
		}
	}
}

func TestCloudWatchPreRouteQueryHasDedicatedBoundedLimit(t *testing.T) {
	from := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	request := cloudWatchInsightsRequest{Kind: cwQueryWorkerPreRouteReject, From: from, To: from.Add(time.Hour), Limit: cloudWatchLogsPreRouteLimit}
	source, query, limit, err := buildCloudWatchInsightsQuery(request)
	if err != nil || source.ID != cwSourceWorkerNewAPI || limit != cloudWatchLogsPreRouteLimit || !strings.Contains(query, "limit 10000") {
		t.Fatalf("source=%+v query=%q limit=%d err=%v", source, query, limit, err)
	}
	request.Limit++
	if _, _, _, err := buildCloudWatchInsightsQuery(request); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
		t.Fatalf("continuous limit overflow accepted: %v", err)
	}
	request.Kind, request.Limit = cwQueryWorkerShadowReject, cloudWatchLogsPreRouteLimit
	if _, _, _, err := buildCloudWatchInsightsQuery(request); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
		t.Fatalf("Shadow must keep the generic 500-row bound: %v", err)
	}
}

func TestCloudWatchPreRouteCursorBumpsMigrationPlan(t *testing.T) {
	if !strings.Contains(preMigrationPlanID, "v52") || !strings.Contains(preMigrationPlanID, "preroute-cursor-nginx-cursor-v1-nginx-repair-cursor-v1") {
		t.Fatalf("前置拒绝水位表加入 AutoMigrate 后必须产生独立迁移快照: %s", preMigrationPlanID)
	}
}

func TestCloudWatchPreRouteSamplesDeduplicateAndKeepClosedCategories(t *testing.T) {
	from := int64(1_800_000_000)
	uid := int64(7)
	evidence := []cloudWatchStructuredEvidence{
		{Kind: cwEvidenceNewAPIError, EventRef: "same", EventMS: (from + 1) * 1000, Category: "route_no_channel", Model: "gpt-5", Group: "paid", UserID: &uid},
		{Kind: cwEvidenceNewAPIError, EventRef: "same", EventMS: (from + 1) * 1000, Category: "route_no_channel", Model: "gpt-5", Group: "paid", UserID: &uid},
		{Kind: cwEvidenceNewAPIError, EventRef: "token", EventMS: (from + 2) * 1000, Category: "invalid_token"},
		{Kind: cwEvidenceNewAPIError, EventRef: "ordinary", EventMS: (from + 3) * 1000, Category: "upstream_5xx"},
	}
	rows := cloudWatchPreRouteSamples(evidence, from, from+60)
	if len(rows) != 2 {
		t.Fatalf("rows=%+v", rows)
	}
	counts := map[string]int64{}
	for _, row := range rows {
		if row.Node != cloudWatchPreRouteNode || row.BucketTs != from/60*60 {
			t.Fatalf("unexpected row=%+v", row)
		}
		counts[row.Reason] += row.Count
	}
	if counts["route_no_channel"] != 1 || counts["invalid_token"] != 1 || counts["upstream_5xx"] != 0 {
		t.Fatalf("counts=%v", counts)
	}
}

func TestPublishCloudWatchPreRouteWindowReplacesOnlyItsOwnSource(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	from := int64(1_800_000_000) / 60 * 60
	old := []RejectionSample{
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "old", Grp: "g", Count: 9},
		{BucketTs: from, Node: "legacy", Reason: "route_no_channel", Model: "legacy", Grp: "g", Count: 4},
	}
	if err := m.storeDB.Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	state := CloudWatchPreRouteCursor{ID: cloudWatchPreRouteCursorID, CoverageFromTs: from,
		NextTs: from, ThroughTs: from, TargetThroughTs: from + 60, SemanticsVersion: cloudWatchPreRouteVersion}
	rows := []RejectionSample{{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "new", Grp: "g", Count: 2}}
	if err := m.publishCloudWatchPreRouteWindow(context.Background(), &state, from, from+60, rows, from+60); err != nil {
		t.Fatal(err)
	}
	var stored []RejectionSample
	if err := m.storeDB.Order("node ASC").Find(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if len(stored) != 2 || stored[0].Node != cloudWatchPreRouteNode || stored[0].Model != "new" || stored[0].Count != 2 || stored[1].Node != "legacy" {
		t.Fatalf("stored=%+v", stored)
	}
	if state.Status != "caught_up" || state.ThroughTs != from+60 || state.NextTs != from+60 {
		t.Fatalf("state=%+v", state)
	}
}

func TestCloudWatchPreRouteWindowQueriesParsesAndPublishes(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	from := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	client := &fakeCloudWatchLogsClient{getFn: func(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusComplete, Results: [][]cwlogtypes.ResultField{{
			{Field: aws.String("@timestamp"), Value: aws.String("2026-09-20 00:01:00.000")},
			{Field: aws.String("@ptr"), Value: aws.String("event-1")},
			{Field: aws.String("@logStream"), Value: aws.String("new-api/task")},
			{Field: aws.String("@message"), Value: aws.String("[ERR] 2026/09/20 - 00:01:00 | ABCDEF1234567890 | user 7 | No available channel for model gpt-5 under group paid")},
		}}, Statistics: &cwlogtypes.QueryStatistics{RecordsMatched: 1}}, nil
	}}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	m.cfg.CloudWatchEvidenceHMACKey = strings.Repeat("k", 32)
	m.cfg.CloudWatchEvidenceHMACKeyID = "fixture-v1"
	state := CloudWatchPreRouteCursor{ID: cloudWatchPreRouteCursorID, CoverageFromTs: from.Unix(),
		NextTs: from.Unix(), ThroughTs: from.Unix(), TargetThroughTs: from.Add(time.Hour).Unix(), SemanticsVersion: cloudWatchPreRouteVersion}
	queries, err := m.runCloudWatchPreRouteWindow(context.Background(), &state, from.Unix(), from.Add(time.Hour).Unix(), from.Add(time.Hour).Unix())
	if err != nil {
		t.Fatal(err)
	}
	if queries != 1 {
		t.Fatalf("queries=%d", queries)
	}
	var row RejectionSample
	if err := m.storeDB.First(&row, "node = ?", cloudWatchPreRouteNode).Error; err != nil {
		t.Fatal(err)
	}
	if row.Reason != "route_no_channel" || row.Model != "gpt-5" || row.Grp != "paid" || row.UserID != 7 || row.Count != 1 {
		t.Fatalf("row=%+v", row)
	}
}

func TestCloudWatchPreRouteTruncatedMinuteSplitsToSeconds(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	var gets int
	client := &fakeCloudWatchLogsClient{getFn: func(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		gets++
		if gets == 1 {
			return &cloudwatchlogs.GetQueryResultsOutput{
				Status: cwlogtypes.QueryStatusComplete,
				Statistics: &cwlogtypes.QueryStatistics{
					RecordsMatched: cloudWatchLogsHardLimit + 1,
				},
			}, nil
		}
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusComplete}, nil
	}}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	parser := newCloudWatchParserForTest(t, false)
	from := time.Date(2026, 9, 20, 0, 0, 0, 0, time.UTC)
	queries := 0
	evidence, err := m.queryCloudWatchPreRouteRange(context.Background(), parser, from, from.Add(time.Minute), &queries, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(evidence) != 0 || queries != 3 || len(client.startInputs) != 3 {
		t.Fatalf("minute split evidence=%d queries=%d starts=%d", len(evidence), queries, len(client.startInputs))
	}
	if got := aws.ToInt64(client.startInputs[1].EndTime) - aws.ToInt64(client.startInputs[1].StartTime); got != 30 {
		t.Fatalf("first child span=%d", got)
	}
}
