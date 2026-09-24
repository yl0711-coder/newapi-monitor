package monitor

import (
	"context"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
)

type fakeCloudWatchLogsClient struct {
	mu           sync.Mutex
	filterInputs []*cloudwatchlogs.FilterLogEventsInput
	startInputs  []*cloudwatchlogs.StartQueryInput
	getInputs    []*cloudwatchlogs.GetQueryResultsInput
	stopInputs   []*cloudwatchlogs.StopQueryInput
	filterFn     func(context.Context, *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error)
	startFn      func(context.Context, *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.StartQueryOutput, error)
	getFn        func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error)
	stopFn       func(context.Context, *cloudwatchlogs.StopQueryInput) (*cloudwatchlogs.StopQueryOutput, error)
}

func (f *fakeCloudWatchLogsClient) FilterLogEvents(ctx context.Context, in *cloudwatchlogs.FilterLogEventsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error) {
	f.mu.Lock()
	f.filterInputs = append(f.filterInputs, in)
	fn := f.filterFn
	f.mu.Unlock()
	if fn == nil {
		return &cloudwatchlogs.FilterLogEventsOutput{}, nil
	}
	return fn(ctx, in)
}
func (f *fakeCloudWatchLogsClient) StartQuery(ctx context.Context, in *cloudwatchlogs.StartQueryInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error) {
	f.mu.Lock()
	f.startInputs = append(f.startInputs, in)
	fn := f.startFn
	f.mu.Unlock()
	if fn == nil {
		return &cloudwatchlogs.StartQueryOutput{QueryId: aws.String("q")}, nil
	}
	return fn(ctx, in)
}
func (f *fakeCloudWatchLogsClient) GetQueryResults(ctx context.Context, in *cloudwatchlogs.GetQueryResultsInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error) {
	f.mu.Lock()
	f.getInputs = append(f.getInputs, in)
	fn := f.getFn
	f.mu.Unlock()
	if fn == nil {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusComplete}, nil
	}
	return fn(ctx, in)
}
func (f *fakeCloudWatchLogsClient) StopQuery(ctx context.Context, in *cloudwatchlogs.StopQueryInput, _ ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error) {
	f.mu.Lock()
	f.stopInputs = append(f.stopInputs, in)
	fn := f.stopFn
	f.mu.Unlock()
	if fn == nil {
		return &cloudwatchlogs.StopQueryOutput{}, nil
	}
	return fn(ctx, in)
}

func cloudWatchTestRange() (time.Time, time.Time) {
	from := time.Date(2026, 9, 14, 0, 0, 0, 0, time.UTC)
	return from, from.Add(10 * time.Minute)
}

func TestCloudWatchLogSourceCatalogIsClosedAndProductionOnly(t *testing.T) {
	if len(cloudWatchLogSourceCatalog) != 7 {
		t.Fatalf("sources=%d", len(cloudWatchLogSourceCatalog))
	}
	diagnostic, err := cloudWatchLogSourceByID(cwSourceCloudFrontDiagnostic)
	if err != nil || diagnostic.Region != cloudWatchLogsEastRegion || !diagnostic.Sensitive {
		t.Fatalf("diagnostic=%+v err=%v", diagnostic, err)
	}
	for _, source := range cloudWatchLogSourceCatalog {
		if strings.Contains(source.LogGroup, "buffer") || strings.Contains(source.LogGroup, "bridge") {
			t.Fatalf("实验/桥接日志进入生产白名单: %+v", source)
		}
		if source.Region != cloudWatchLogsEastRegion && source.Region != cloudWatchLogsWestRegion {
			t.Fatalf("region=%q", source.Region)
		}
	}
	if _, err := cloudWatchLogSourceByID("arbitrary"); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
		t.Fatalf("unknown source err=%v", err)
	}
}

func TestCloudWatchLogsNewDefaultsToDormantRuntime(t *testing.T) {
	m, err := New(Settings{
		StorePath: t.TempDir() + "/monitor.db", UsageFactsStorePath: t.TempDir() + "/usage.db",
		LocalSnapshotOnly: true, SessionSecret: "cloudwatch-test", AlertsDisabled: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	if m.cloudWatchLogs == nil || m.cloudWatchLogs.enabled {
		t.Fatalf("runtime=%+v", m.cloudWatchLogs)
	}
	for region, slot := range m.cloudWatchLogs.clients {
		slot.mu.Lock()
		client, loading := slot.client, slot.loading
		slot.mu.Unlock()
		if client != nil || loading {
			t.Fatalf("region=%s client=%v loading=%v", region, client, loading)
		}
	}
}

func TestCloudWatchLogsRuntimeIsLazyAndReusesRegionClients(t *testing.T) {
	var calls atomic.Int32
	clients := map[string]*fakeCloudWatchLogsClient{
		cloudWatchLogsEastRegion: {}, cloudWatchLogsWestRegion: {},
	}
	factory := func(_ context.Context, region string) (cloudWatchLogsAPI, error) {
		calls.Add(1)
		return clients[region], nil
	}
	disabled := newCloudWatchLogsRuntime(false, factory)
	if _, err := disabled.client(context.Background(), cloudWatchLogsEastRegion); cloudWatchLogsErrorKindOf(err) != cwLogsErrDisabled || calls.Load() != 0 {
		t.Fatalf("disabled err=%v calls=%d", err, calls.Load())
	}
	runtime := newCloudWatchLogsRuntime(true, factory)
	if calls.Load() != 0 {
		t.Fatal("构造阶段不得初始化 AWS")
	}
	for _, region := range []string{cloudWatchLogsEastRegion, cloudWatchLogsEastRegion, cloudWatchLogsWestRegion} {
		if _, err := runtime.client(context.Background(), region); err != nil {
			t.Fatal(err)
		}
	}
	if calls.Load() != 2 {
		t.Fatalf("factory calls=%d", calls.Load())
	}
}

func TestCloudWatchFilterHandlesEmptyPagesDedupAndPrefix(t *testing.T) {
	from, to := cloudWatchTestRange()
	client := &fakeCloudWatchLogsClient{}
	client.filterFn = func(_ context.Context, in *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		switch len(client.filterInputs) {
		case 1:
			return &cloudwatchlogs.FilterLogEventsOutput{NextToken: aws.String("p2")}, nil
		case 2:
			return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{
				{EventId: aws.String("e1"), Message: aws.String("first"), Timestamp: aws.Int64(from.UnixMilli())},
				{EventId: aws.String("e1"), Message: aws.String("duplicate"), Timestamp: aws.Int64(from.UnixMilli())},
			}, NextToken: aws.String("p3")}, nil
		default:
			return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{EventId: aws.String("e2"), Message: aws.String("second"), Timestamp: aws.Int64(to.Add(-time.Second).UnixMilli())}}}, nil
		}
	}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	result, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: cwFilterWorkerNewAPIID, From: from, To: to, Value: "req-123", Limit: 10})
	if err != nil || len(result.Events) != 2 || result.Events[0].Message != "first" || result.Truncated {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(client.filterInputs) != 3 {
		t.Fatalf("pages=%d", len(client.filterInputs))
	}
	input := client.filterInputs[0]
	if aws.ToString(input.LogGroupName) != "/ecs/nexusapi-prod-worker" || aws.ToString(input.LogStreamNamePrefix) != "new-api/" || input.Unmask || aws.ToInt64(input.StartTime) != from.UnixMilli() || aws.ToInt64(input.EndTime) != to.UnixMilli() {
		t.Fatalf("unsafe input=%+v", input)
	}
	metric := runtime.metricsSnapshot()[cwSourceWorkerNewAPI]
	if !metric.Observed || metric.Successes != 1 || metric.Events != 2 || metric.Deduplicated != 1 || metric.LastEventUnixMS == 0 || metric.Failures != 0 {
		t.Fatalf("metric=%+v", metric)
	}
}

func TestCloudWatchFilterRejectsUnsafeInputsAndPaginationLoop(t *testing.T) {
	from, to := cloudWatchTestRange()
	client := &fakeCloudWatchLogsClient{filterFn: func(_ context.Context, _ *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		return &cloudwatchlogs.FilterLogEventsOutput{NextToken: aws.String("same")}, nil
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	for _, req := range []cloudWatchFilterRequest{
		{Kind: "unknown", From: from, To: to, Value: "req"},
		{Kind: cwFilterWorkerNewAPIID, From: from, To: to.Add(3 * time.Hour), Value: "req"},
		{Kind: cwFilterWorkerNewAPIID, From: from, To: to, Value: `req"inject`},
		{Kind: cwFilterWorkerFixedErrors, From: from, To: to, Value: "user input"},
	} {
		if _, err := runtime.filter(context.Background(), req); cloudWatchLogsErrorKindOf(err) != cwLogsErrInvalid {
			t.Fatalf("req=%+v err=%v", req, err)
		}
	}
	if len(client.filterInputs) != 0 {
		t.Fatal("非法输入不得发 AWS 请求")
	}
	_, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: cwFilterWorkerNewAPIID, From: from, To: to, Value: "req-123"})
	if cloudWatchLogsErrorKindOf(err) != cwLogsErrFailed || len(client.filterInputs) != 2 {
		t.Fatalf("pagination err=%v calls=%d", err, len(client.filterInputs))
	}
}

func TestCloudWatchInsightsWaitsForCompleteAndRecordsCost(t *testing.T) {
	from, to := cloudWatchTestRange()
	client := &fakeCloudWatchLogsClient{}
	statuses := []cwlogtypes.QueryStatus{cwlogtypes.QueryStatusScheduled, cwlogtypes.QueryStatusRunning, cwlogtypes.QueryStatusComplete}
	client.getFn = func(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		status := statuses[len(client.getInputs)-1]
		out := &cloudwatchlogs.GetQueryResultsOutput{Status: status}
		if status == cwlogtypes.QueryStatusComplete {
			out.Results = [][]cwlogtypes.ResultField{{
				{Field: aws.String("@timestamp"), Value: aws.String("2026-09-14 00:01:00.000")},
				{Field: aws.String("oneapi_request_id"), Value: aws.String("req-123")},
			}}
			out.Statistics = &cwlogtypes.QueryStatistics{BytesScanned: 4096, RecordsMatched: 1, RecordsScanned: 10}
		}
		return out, nil
	}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	runtime.poll = func(context.Context) error { return nil }
	result, err := runtime.insights(context.Background(), cloudWatchInsightsRequest{Kind: cwQueryWorkerRequestID, From: from, To: to, Value: "req-123", Limit: 20})
	if err != nil || result.Status != cwlogtypes.QueryStatusComplete || len(result.Rows) != 1 || result.BytesScanned != 4096 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if len(client.getInputs) != 3 || len(client.stopInputs) != 0 || len(client.startInputs) != 1 {
		t.Fatalf("start=%d get=%d stop=%d", len(client.startInputs), len(client.getInputs), len(client.stopInputs))
	}
	input := client.startInputs[0]
	if aws.ToString(input.LogGroupName) != "/ecs/nexusapi-prod-worker" || !strings.Contains(aws.ToString(input.QueryString), "oneapi_request_id") || aws.ToInt32(input.Limit) != 20 {
		t.Fatalf("input=%+v", input)
	}
	metric := runtime.metricsSnapshot()[cwSourceWorkerNginx]
	if metric.Successes != 1 || metric.Events != 1 || metric.BytesScanned != 4096 || metric.LastEventUnixMS == 0 {
		t.Fatalf("metric=%+v", metric)
	}
}

func TestCloudWatchInsightsStopsOnCancellationButNotTerminalFailure(t *testing.T) {
	from, to := cloudWatchTestRange()
	client := &fakeCloudWatchLogsClient{getFn: func(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusRunning}, nil
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	runtime.poll = func(context.Context) error { return context.Canceled }
	_, err := runtime.insights(context.Background(), cloudWatchInsightsRequest{Kind: cwQueryCloudFrontRequestID, From: from, To: to, Value: "edge-123"})
	if cloudWatchLogsErrorKindOf(err) != cwLogsErrCancelled || len(client.stopInputs) != 1 {
		t.Fatalf("cancel err=%v stops=%d", err, len(client.stopInputs))
	}

	terminal := &fakeCloudWatchLogsClient{getFn: func(_ context.Context, _ *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusFailed, Statistics: &cwlogtypes.QueryStatistics{BytesScanned: 2048}}, nil
	}}
	runtime = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return terminal, nil })
	_, err = runtime.insights(context.Background(), cloudWatchInsightsRequest{Kind: cwQueryCloudFrontPath, From: from, To: to, Value: "/v1/responses"})
	metric := runtime.metricsSnapshot()[cwSourceCloudFrontAccess]
	if cloudWatchLogsErrorKindOf(err) != cwLogsErrFailed || len(terminal.stopInputs) != 0 || metric.BytesScanned != 2048 {
		t.Fatalf("terminal err=%v stops=%d metric=%+v", err, len(terminal.stopInputs), metric)
	}
}

func TestCloudWatchLogsDefensivelyHandlesNilSDKOutputs(t *testing.T) {
	from, to := cloudWatchTestRange()
	filterClient := &fakeCloudWatchLogsClient{filterFn: func(context.Context, *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		return nil, nil
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return filterClient, nil })
	if _, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: cwFilterCloudFrontRequestID, From: from, To: to, Value: "Ab+/=1"}); cloudWatchLogsErrorKindOf(err) != cwLogsErrFailed {
		t.Fatalf("filter err=%v", err)
	}
	startClient := &fakeCloudWatchLogsClient{startFn: func(context.Context, *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.StartQueryOutput, error) {
		return nil, nil
	}}
	runtime = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return startClient, nil })
	if _, err := runtime.insights(context.Background(), cloudWatchInsightsRequest{Kind: cwQueryCloudFrontRequestID, From: from, To: to, Value: "edge"}); cloudWatchLogsErrorKindOf(err) != cwLogsErrFailed || len(startClient.stopInputs) != 0 {
		t.Fatalf("start err=%v stops=%d", err, len(startClient.stopInputs))
	}
	getClient := &fakeCloudWatchLogsClient{getFn: func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return nil, nil
	}}
	runtime = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return getClient, nil })
	if _, err := runtime.insights(context.Background(), cloudWatchInsightsRequest{Kind: cwQueryCloudFrontRequestID, From: from, To: to, Value: "edge"}); cloudWatchLogsErrorKindOf(err) != cwLogsErrFailed || len(getClient.stopInputs) != 1 {
		t.Fatalf("get err=%v stops=%d", err, len(getClient.stopInputs))
	}
}

func TestCloudWatchLogsTimeoutStopsQueryAndThrottlingIsCounted(t *testing.T) {
	from, to := cloudWatchTestRange()
	timeoutClient := &fakeCloudWatchLogsClient{getFn: func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
		return nil, context.DeadlineExceeded
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return timeoutClient, nil })
	if _, err := runtime.insights(context.Background(), cloudWatchInsightsRequest{Kind: cwQueryCloudFrontRequestID, From: from, To: to, Value: "edge"}); cloudWatchLogsErrorKindOf(err) != cwLogsErrTimeout || len(timeoutClient.stopInputs) != 1 {
		t.Fatalf("timeout err=%v stops=%d", err, len(timeoutClient.stopInputs))
	}
	throttled := &fakeCloudWatchLogsClient{filterFn: func(context.Context, *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		return nil, &smithy.GenericAPIError{Code: "ThrottlingException", Message: "private"}
	}}
	runtime = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return throttled, nil })
	_, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: cwFilterCloudFrontRequestID, From: from, To: to, Value: "Ab+/=1"})
	metric := runtime.metricsSnapshot()[cwSourceCloudFrontAccess]
	if cloudWatchLogsErrorKindOf(err) != cwLogsErrThrottled || metric.Failures != 1 || metric.Throttled != 1 || metric.LastError != cwLogsErrThrottled {
		t.Fatalf("err=%v metric=%+v", err, metric)
	}
}

func TestCloudWatchLogsClassifiesAWSErrorsWithoutLeakingDetails(t *testing.T) {
	cases := []struct {
		code string
		want cloudWatchLogsErrorKind
	}{
		{"AccessDeniedException", cwLogsErrAccessDenied}, {"ThrottlingException", cwLogsErrThrottled}, {"ServiceUnavailableException", cwLogsErrUnavailable}, {"Other", cwLogsErrFailed},
	}
	for _, tc := range cases {
		secret := "secret-request-id"
		err := &smithy.GenericAPIError{Code: tc.code, Message: secret}
		classified := newCloudWatchLogsError(classifyCloudWatchLogsError(err), "call", err)
		if cloudWatchLogsErrorKindOf(classified) != tc.want || strings.Contains(classified.Error(), secret) {
			t.Fatalf("code=%s err=%v", tc.code, classified)
		}
	}
}

func TestCloudWatchLogsGateLimitsConcurrencyAndHonorsWaitingCancellation(t *testing.T) {
	from, to := cloudWatchTestRange()
	var active atomic.Int32
	var maximum atomic.Int32
	entered := make(chan struct{}, 2)
	release := make(chan struct{})
	client := &fakeCloudWatchLogsClient{filterFn: func(ctx context.Context, _ *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		current := active.Add(1)
		defer active.Add(-1)
		for {
			old := maximum.Load()
			if current <= old || maximum.CompareAndSwap(old, current) {
				break
			}
		}
		entered <- struct{}{}
		select {
		case <-release:
			return &cloudwatchlogs.FilterLogEventsOutput{}, nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	done := make(chan error, 2)
	req := cloudWatchFilterRequest{Kind: cwFilterWorkerNewAPIID, From: from, To: to, Value: "req-123"}
	for range 2 {
		go func() { _, err := runtime.filter(context.Background(), req); done <- err }()
	}
	<-entered
	<-entered
	waiting, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.filter(waiting, req); cloudWatchLogsErrorKindOf(err) != cwLogsErrCancelled {
		t.Fatalf("waiting err=%v", err)
	}
	client.mu.Lock()
	calls := len(client.filterInputs)
	client.mu.Unlock()
	if calls != 2 {
		t.Fatalf("cancelled waiter sent AWS call: %d", calls)
	}
	close(release)
	for range 2 {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if maximum.Load() != 2 {
		t.Fatalf("max=%d", maximum.Load())
	}
}

func TestCloudWatchFixedQueriesEscapeValuesAndTruncateFilterResults(t *testing.T) {
	from, to := cloudWatchTestRange()
	_, query, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{Kind: cwQueryCloudFrontPath, From: from, To: to, Value: `/v1/"escape`})
	if err != nil || !strings.Contains(query, `"/v1/\"escape"`) || strings.Contains(query, `"/v1/"escape"`) {
		t.Fatalf("query=%q err=%v", query, err)
	}
	client := &fakeCloudWatchLogsClient{filterFn: func(_ context.Context, _ *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{EventId: aws.String("e1")}, {EventId: aws.String("e2")}}}, nil
	}}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	result, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: cwFilterCloudFrontRequestID, From: from, To: to, Value: "edge-1", Limit: 1})
	if err != nil || len(result.Events) != 1 || !result.Truncated {
		t.Fatalf("result=%+v err=%v", result, err)
	}
}

func TestCloudWatchLogsClientWaiterCanCancelDuringInitialization(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	client := &fakeCloudWatchLogsClient{}
	var calls atomic.Int32
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) {
		if calls.Add(1) == 1 {
			close(started)
			<-release
		}
		return client, nil
	})
	first := make(chan error, 1)
	go func() { _, err := runtime.client(context.Background(), cloudWatchLogsEastRegion); first <- err }()
	<-started
	waiting, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := runtime.client(waiting, cloudWatchLogsEastRegion); cloudWatchLogsErrorKindOf(err) != cwLogsErrCancelled {
		t.Fatalf("waiting err=%v", err)
	}
	close(release)
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 {
		t.Fatalf("calls=%d", calls.Load())
	}
}

func TestCloudWatchLogsClientInitializationCanRetryAfterFailure(t *testing.T) {
	var calls atomic.Int32
	client := &fakeCloudWatchLogsClient{}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) {
		if calls.Add(1) == 1 {
			return nil, &smithy.GenericAPIError{Code: "AccessDeniedException", Message: "private detail"}
		}
		return client, nil
	})
	if _, err := runtime.client(context.Background(), cloudWatchLogsEastRegion); cloudWatchLogsErrorKindOf(err) != cwLogsErrAccessDenied {
		t.Fatalf("first err=%v", err)
	}
	got, err := runtime.client(context.Background(), cloudWatchLogsEastRegion)
	if err != nil || got != client || calls.Load() != 2 {
		t.Fatalf("client=%v calls=%d err=%v", got, calls.Load(), err)
	}
}

func TestCloudWatchLogsMetricsDistinguishUnobservedFromSuccessfulEmpty(t *testing.T) {
	from, to := cloudWatchTestRange()
	client := &fakeCloudWatchLogsClient{}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	before := runtime.metricsSnapshot()[cwSourceWorkerNewAPI]
	if before.Observed || before.Successes != 0 {
		t.Fatalf("before=%+v", before)
	}
	result, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: cwFilterWorkerNewAPIID, From: from, To: to, Value: "req-empty"})
	if err != nil || len(result.Events) != 0 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	after := runtime.metricsSnapshot()[cwSourceWorkerNewAPI]
	if !after.Observed || after.Successes != 1 || after.Events != 0 || after.LastSuccessUnix == 0 {
		t.Fatalf("after=%+v", after)
	}
}

func TestCloudWatchWorkerFilterKindsUseSeparateStreamPrefixes(t *testing.T) {
	from, to := cloudWatchTestRange()
	client := &fakeCloudWatchLogsClient{}
	runtime := newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	for _, kind := range []cloudWatchFixedFilterKind{cwFilterWorkerNginxID, cwFilterWorkerNewAPIID} {
		if _, err := runtime.filter(context.Background(), cloudWatchFilterRequest{Kind: kind, From: from, To: to, Value: "req-1"}); err != nil {
			t.Fatal(err)
		}
	}
	if len(client.filterInputs) != 2 || aws.ToString(client.filterInputs[0].LogStreamNamePrefix) != "nginx/" || aws.ToString(client.filterInputs[1].LogStreamNamePrefix) != "new-api/" {
		t.Fatalf("inputs=%+v", client.filterInputs)
	}
}
