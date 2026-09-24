package monitor

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestCloudWatchShadowQueriesAreClosedAndBounded(t *testing.T) {
	from := time.Unix(1_900_000_000, 0).UTC()
	to := from.Add(15 * time.Minute)
	for _, tc := range []struct {
		kind   cloudWatchFixedQueryKind
		source cloudWatchLogSourceID
		field  string
	}{
		{cwQueryWorkerShadowNginx, cwSourceWorkerNginx, "request_time"},
		{cwQueryWorkerShadowReject, cwSourceWorkerNewAPI, "@message"},
	} {
		source, query, limit, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{Kind: tc.kind, From: from, To: to, Limit: cloudWatchLogsHardLimit})
		if err != nil || source.ID != tc.source || limit != cloudWatchLogsHardLimit || !strings.Contains(query, tc.field) || !strings.Contains(query, "sort @timestamp asc") {
			t.Fatalf("kind=%s source=%+v limit=%d query=%q err=%v", tc.kind, source, limit, query, err)
		}
		if tc.kind == cwQueryWorkerShadowNginx {
			for _, required := range []string{"ispresent(request_method)", "ispresent(uri)", "ispresent(status)", "ispresent(request_time)"} {
				if !strings.Contains(query, required) {
					t.Fatalf("nginx shadow query did not exclude non-access logs: missing %q in %q", required, query)
				}
			}
		}
		if _, _, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{Kind: tc.kind, From: from, To: to, Value: "unexpected"}); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
			t.Fatalf("shadow query %s accepted a caller value: %v", tc.kind, err)
		}
	}
}

func TestCloudWatchShadowLaneSplitsTruncatedWindows(t *testing.T) {
	var sequence int
	client := &fakeCloudWatchLogsClient{
		startFn: func(_ context.Context, _ *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.StartQueryOutput, error) {
			sequence++
			return &cloudwatchlogs.StartQueryOutput{QueryId: aws.String(fmt.Sprintf("q-%d", sequence))}, nil
		},
		getFn: func(_ context.Context, in *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			if aws.ToString(in.QueryId) == "q-1" {
				return &cloudwatchlogs.GetQueryResultsOutput{
					Status:     cwlogtypes.QueryStatusComplete,
					Statistics: &cwlogtypes.QueryStatistics{BytesScanned: 1, RecordsMatched: 501},
				}, nil
			}
			minute := "01"
			if aws.ToString(in.QueryId) == "q-3" {
				minute = "06"
			}
			row := []cwlogtypes.ResultField{
				{Field: aws.String("@timestamp"), Value: aws.String("2030-03-17T17:" + minute + ":00Z")},
				{Field: aws.String("@ptr"), Value: aws.String(aws.ToString(in.QueryId))},
				{Field: aws.String("request_method"), Value: aws.String("POST")},
				{Field: aws.String("uri"), Value: aws.String("/v1/responses")},
				{Field: aws.String("status"), Value: aws.String("200")},
				{Field: aws.String("request_time"), Value: aws.String("0.100")},
				{Field: aws.String("upstream_status"), Value: aws.String("200")},
				{Field: aws.String("upstream_response_time"), Value: aws.String("0.080")},
			}
			bytes := float64(2)
			if aws.ToString(in.QueryId) == "q-3" {
				bytes = 3
			}
			return &cloudwatchlogs.GetQueryResultsOutput{
				Status: cwlogtypes.QueryStatusComplete, Results: [][]cwlogtypes.ResultField{row},
				Statistics: &cwlogtypes.QueryStatistics{BytesScanned: bytes, RecordsMatched: 1},
			}, nil
		},
	}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	runtime.poll = func(context.Context) error { return nil }
	parser, err := newCloudWatchEvidenceParser(strings.Repeat("k", 32), "fixture-v1", false)
	if err != nil {
		t.Fatal(err)
	}
	m := &Monitor{cloudWatchLogs: runtime}
	from := time.Date(2030, 3, 17, 17, 0, 0, 0, time.UTC)
	result := m.runCloudWatchShadowLane(context.Background(), parser, cwQueryWorkerShadowNginx,
		from, from.Add(10*time.Minute), 2, true, true)
	if result.status != cloudWatchShadowStatusComplete || result.queries != 3 || result.bytes != 6 || len(result.evidence) != 2 || len(result.blindSpots) != 0 {
		t.Fatalf("split shadow result=%+v", result)
	}
	if len(client.startInputs) != 3 || aws.ToInt64(client.startInputs[1].StartTime) != from.Unix() ||
		aws.ToInt64(client.startInputs[1].EndTime) != from.Add(5*time.Minute).Unix() ||
		aws.ToInt64(client.startInputs[2].StartTime) != from.Add(5*time.Minute).Unix() ||
		aws.ToInt64(client.startInputs[2].EndTime) != from.Add(10*time.Minute).Unix() {
		t.Fatalf("unexpected split query windows: %+v", client.startInputs)
	}
}

func TestCloudWatchShadowRunnerPublishesCompleteEmptyWindow(t *testing.T) {
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	sqlDB.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = sqlDB.Close() })
	if err := db.AutoMigrate(&NginxMinuteSample{}, &RejectionSample{}, &CloudWatchShadowReconciliationRun{}, &CloudWatchShadowReconciliationDiff{}); err != nil {
		t.Fatal(err)
	}
	client := &fakeCloudWatchLogsClient{getFn: func(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusComplete, Statistics: &cwlogtypes.QueryStatistics{BytesScanned: 7}}, nil
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	runtime.poll = func(context.Context) error { return nil }
	m := &Monitor{storeDB: db, cloudWatchLogs: runtime, cfg: Settings{
		CloudWatchShadowEnabled: true, CloudWatchShadowNginxContractReady: true,
		CloudWatchShadowLookbackMinutes: 15, CloudWatchShadowRetentionDays: 14,
		CloudWatchEvidenceHMACKey: strings.Repeat("k", 32), CloudWatchEvidenceHMACKeyID: "fixture-v1",
	}}
	at := time.Now().UTC()
	if err := m.runCloudWatchShadowComparison(context.Background(), at); err != nil {
		t.Fatalf("empty structured window should publish complete run: %v", err)
	}
	var run CloudWatchShadowReconciliationRun
	if err := db.First(&run).Error; err != nil {
		t.Fatal(err)
	}
	if run.Status != cloudWatchShadowStatusComplete || run.NginxStatus != cloudWatchShadowStatusComplete || run.RejectionStatus != cloudWatchShadowStatusComplete || run.QueryCount != 2 || run.BytesScanned != 14 {
		t.Fatalf("unexpected shadow run: %+v", run)
	}
}

func TestCloudWatchShadowLaneBlocksMissingLegacyBaseline(t *testing.T) {
	if got := cloudWatchShadowLaneStatus(nil, false, 0, 1, 0, false, true); got != cloudWatchShadowStatusBlockedLegacy {
		t.Fatalf("missing nginx legacy baseline status=%q", got)
	}
	if got := cloudWatchShadowLaneStatus(nil, false, 0, 1, 0, false, false); got != cloudWatchShadowStatusBlockedLegacy {
		t.Fatalf("missing rejection legacy baseline status=%q", got)
	}
	if got := cloudWatchShadowLaneStatus(nil, false, 0, 0, 0, false, true); got != cloudWatchShadowStatusComplete {
		t.Fatalf("empty window without legacy rows should remain complete, got=%q", got)
	}
}
