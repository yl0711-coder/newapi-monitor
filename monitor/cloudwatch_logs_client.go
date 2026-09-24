package monitor

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	"github.com/aws/smithy-go"
)

const (
	cloudWatchLogsCallTimeout  = 12 * time.Second
	cloudWatchLogsTotalTimeout = 25 * time.Second
	cloudWatchLogsStopTimeout  = 2 * time.Second
)

type cloudWatchLogsErrorKind string

const (
	cwLogsErrDisabled     cloudWatchLogsErrorKind = "disabled"
	cwLogsErrInvalid      cloudWatchLogsErrorKind = "invalid"
	cwLogsErrAccessDenied cloudWatchLogsErrorKind = "access_denied"
	cwLogsErrThrottled    cloudWatchLogsErrorKind = "throttled"
	cwLogsErrTimeout      cloudWatchLogsErrorKind = "timeout"
	cwLogsErrCancelled    cloudWatchLogsErrorKind = "cancelled"
	cwLogsErrUnavailable  cloudWatchLogsErrorKind = "unavailable"
	cwLogsErrFailed       cloudWatchLogsErrorKind = "failed"
)

type cloudWatchLogsError struct {
	Kind      cloudWatchLogsErrorKind
	Operation string
	cause     error
}

func (e *cloudWatchLogsError) Error() string {
	return fmt.Sprintf("cloudwatch logs %s (%s)", e.Kind, e.Operation)
}

func (e *cloudWatchLogsError) Unwrap() error { return e.cause }

func newCloudWatchLogsError(kind cloudWatchLogsErrorKind, operation string, cause error) error {
	return &cloudWatchLogsError{Kind: kind, Operation: operation, cause: cause}
}

func cloudWatchLogsErrorKindOf(err error) cloudWatchLogsErrorKind {
	var classified *cloudWatchLogsError
	if errors.As(err, &classified) {
		return classified.Kind
	}
	return classifyCloudWatchLogsError(err)
}

func classifyCloudWatchLogsError(err error) cloudWatchLogsErrorKind {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, context.Canceled):
		return cwLogsErrCancelled
	case errors.Is(err, context.DeadlineExceeded):
		return cwLogsErrTimeout
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "AccessDeniedException", "UnrecognizedClientException", "InvalidClientTokenId", "ExpiredTokenException":
			return cwLogsErrAccessDenied
		case "ThrottlingException", "LimitExceededException", "TooManyRequestsException":
			return cwLogsErrThrottled
		case "ServiceUnavailableException":
			return cwLogsErrUnavailable
		}
	}
	return cwLogsErrFailed
}

type cloudWatchLogsAPI interface {
	FilterLogEvents(context.Context, *cloudwatchlogs.FilterLogEventsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.FilterLogEventsOutput, error)
	StartQuery(context.Context, *cloudwatchlogs.StartQueryInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StartQueryOutput, error)
	GetQueryResults(context.Context, *cloudwatchlogs.GetQueryResultsInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.GetQueryResultsOutput, error)
	StopQuery(context.Context, *cloudwatchlogs.StopQueryInput, ...func(*cloudwatchlogs.Options)) (*cloudwatchlogs.StopQueryOutput, error)
}

type cloudWatchLogsClientFactory func(context.Context, string) (cloudWatchLogsAPI, error)

type cloudWatchLogsClientSlot struct {
	mu      sync.Mutex
	client  cloudWatchLogsAPI
	loading bool
	ready   chan struct{}
}

type cloudWatchLogsRuntime struct {
	enabled bool
	factory cloudWatchLogsClientFactory
	gate    chan struct{}
	clients map[string]*cloudWatchLogsClientSlot

	metricsMu sync.RWMutex
	metrics   map[cloudWatchLogSourceID]cloudWatchLogsSourceMetrics
	poll      func(context.Context) error
}

type cloudWatchLogsSourceMetrics struct {
	Observed             bool
	Attempts             uint64
	Successes            uint64
	Failures             uint64
	Throttled            uint64
	Events               uint64
	Deduplicated         uint64
	Truncated            uint64
	BytesScanned         uint64
	Parsed               uint64
	ParseFailures        uint64
	LastSuccessUnix      int64
	LastParseFailureUnix int64
	LastEventUnixMS      int64
	LastDurationNanos    int64
	LastError            cloudWatchLogsErrorKind
}

func newCloudWatchLogsRuntime(enabled bool, factory cloudWatchLogsClientFactory) *cloudWatchLogsRuntime {
	if factory == nil {
		factory = func(ctx context.Context, region string) (cloudWatchLogsAPI, error) {
			cfg, err := awsconfig.LoadDefaultConfig(ctx, awsconfig.WithRegion(region), awsconfig.WithRetryMaxAttempts(2))
			if err != nil {
				return nil, err
			}
			return cloudwatchlogs.NewFromConfig(cfg), nil
		}
	}
	return &cloudWatchLogsRuntime{
		enabled: enabled,
		factory: factory,
		gate:    make(chan struct{}, 2),
		clients: map[string]*cloudWatchLogsClientSlot{
			cloudWatchLogsEastRegion: {},
			cloudWatchLogsWestRegion: {},
		},
		metrics: make(map[cloudWatchLogSourceID]cloudWatchLogsSourceMetrics),
		poll: func(ctx context.Context) error {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
				return nil
			}
		},
	}
}

func (r *cloudWatchLogsRuntime) client(ctx context.Context, region string) (cloudWatchLogsAPI, error) {
	if r == nil || !r.enabled {
		return nil, newCloudWatchLogsError(cwLogsErrDisabled, "client", nil)
	}
	slot := r.clients[region]
	if slot == nil {
		return nil, newCloudWatchLogsError(cwLogsErrInvalid, "region", nil)
	}
	for {
		slot.mu.Lock()
		if slot.client != nil {
			client := slot.client
			slot.mu.Unlock()
			return client, nil
		}
		if slot.loading {
			ready := slot.ready
			slot.mu.Unlock()
			select {
			case <-ready:
				continue
			case <-ctx.Done():
				return nil, newCloudWatchLogsError(classifyCloudWatchLogsError(ctx.Err()), "client", ctx.Err())
			}
		}
		slot.loading = true
		slot.ready = make(chan struct{})
		ready := slot.ready
		slot.mu.Unlock()

		client, err := r.factory(ctx, region)
		slot.mu.Lock()
		if err == nil && client != nil {
			slot.client = client
		}
		slot.loading = false
		close(ready)
		slot.ready = nil
		slot.mu.Unlock()
		if err != nil {
			return nil, newCloudWatchLogsError(classifyCloudWatchLogsError(err), "client", err)
		}
		if client == nil {
			return nil, newCloudWatchLogsError(cwLogsErrUnavailable, "client", nil)
		}
		return client, nil
	}
}

func (r *cloudWatchLogsRuntime) acquire(ctx context.Context) error {
	if r == nil || !r.enabled {
		return newCloudWatchLogsError(cwLogsErrDisabled, "acquire", nil)
	}
	select {
	case r.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return newCloudWatchLogsError(classifyCloudWatchLogsError(ctx.Err()), "acquire", ctx.Err())
	}
}

func (r *cloudWatchLogsRuntime) release() { <-r.gate }

type cloudWatchFixedFilterKind string

const (
	cwFilterCloudFrontRequestID           cloudWatchFixedFilterKind = "cloudfront_request_id"
	cwFilterCloudFrontDiagnosticRequestID cloudWatchFixedFilterKind = "cloudfront_diagnostic_request_id"
	cwFilterCloudFrontDiagnosticIP        cloudWatchFixedFilterKind = "cloudfront_diagnostic_ip"
	cwFilterWorkerNginxID                 cloudWatchFixedFilterKind = "worker_nginx_request_id"
	cwFilterWorkerNewAPIID                cloudWatchFixedFilterKind = "worker_newapi_request_id"
	cwFilterWorkerFixedErrors             cloudWatchFixedFilterKind = "worker_fixed_errors"
	cwFilterMasterFixedErrors             cloudWatchFixedFilterKind = "master_fixed_errors"
	cwFilterRDSErrors                     cloudWatchFixedFilterKind = "rds_errors"
	cwFilterRDSSlowQueries                cloudWatchFixedFilterKind = "rds_slow_queries"
)

type cloudWatchFilterRequest struct {
	Kind  cloudWatchFixedFilterKind
	From  time.Time
	To    time.Time
	Value string
	Limit int
}

type cloudWatchRawEvent struct {
	EventID     string
	LogStream   string
	TimestampMS int64
	IngestedMS  int64
	Message     string
}

type cloudWatchFilterResult struct {
	Source       cloudWatchLogSourceID
	Events       []cloudWatchRawEvent
	Deduplicated uint64
	Truncated    bool
}

func buildCloudWatchFilter(req cloudWatchFilterRequest) (cloudWatchLogSource, string, int, error) {
	limit, err := validateCloudWatchLogRange(req.From, req.To, req.Limit)
	if err != nil {
		return cloudWatchLogSource{}, "", 0, err
	}
	var sourceID cloudWatchLogSourceID
	var pattern string
	switch req.Kind {
	case cwFilterCloudFrontRequestID, cwFilterCloudFrontDiagnosticRequestID:
		value, err := validateCloudWatchFilterToken(req.Value)
		if err != nil {
			return cloudWatchLogSource{}, "", 0, err
		}
		sourceID, pattern = cwSourceCloudFrontAccess, `"`+value+`"`
		if req.Kind == cwFilterCloudFrontDiagnosticRequestID {
			sourceID = cwSourceCloudFrontDiagnostic
		}
	case cwFilterCloudFrontDiagnosticIP:
		value, err := validateCloudWatchFilterToken(req.Value)
		if err != nil {
			return cloudWatchLogSource{}, "", 0, err
		}
		sourceID, pattern = cwSourceCloudFrontDiagnostic, `"`+value+`"`
	case cwFilterWorkerNginxID, cwFilterWorkerNewAPIID:
		value, err := validateCloudWatchFilterToken(req.Value)
		if err != nil {
			return cloudWatchLogSource{}, "", 0, err
		}
		if req.Kind == cwFilterWorkerNginxID {
			sourceID = cwSourceWorkerNginx
		} else {
			sourceID = cwSourceWorkerNewAPI
		}
		pattern = `"` + value + `"`
	case cwFilterWorkerFixedErrors:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "fixed_filter_value", nil)
		}
		sourceID = cwSourceWorkerNewAPI
		pattern = `?"No available channel" ?panic ?fatal ?timeout ?"broken pipe"`
	case cwFilterMasterFixedErrors:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "fixed_filter_value", nil)
		}
		sourceID = cwSourceMaster
		pattern = `?panic ?fatal ?"database error" ?"too many connections" ?deadlock ?timeout`
	case cwFilterRDSErrors:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "fixed_filter_value", nil)
		}
		sourceID = cwSourceRDSError
		pattern = `?"too many connections" ?deadlock ?timeout ?"aborted connection" ?"lost connection" ?"[ERROR]" ?"error:"`
	case cwFilterRDSSlowQueries:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "fixed_filter_value", nil)
		}
		sourceID = cwSourceRDSSlowQuery
		pattern = `"Query_time:"`
	default:
		return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "filter_kind", nil)
	}
	source, err := cloudWatchLogSourceByID(sourceID)
	return source, pattern, limit, err
}

func validateCloudWatchFilterToken(value string) (string, error) {
	value, err := validateCloudWatchQueryValue(value, 256)
	if err != nil {
		return "", err
	}
	for _, r := range value {
		if !((r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z') || (r >= '0' && r <= '9') || strings.ContainsRune("._:-+/=", r)) {
			return "", newCloudWatchLogsError(cwLogsErrInvalid, "filter_value", nil)
		}
	}
	return value, nil
}

func (r *cloudWatchLogsRuntime) filter(ctx context.Context, req cloudWatchFilterRequest) (result cloudWatchFilterResult, err error) {
	source, pattern, limit, err := buildCloudWatchFilter(req)
	if err != nil {
		return result, err
	}
	started := time.Now()
	result.Source = source.ID
	r.recordStart(source.ID)
	defer func() { r.recordFinish(source.ID, result, 0, started, err) }()

	totalCtx, cancel := context.WithTimeout(ctx, cloudWatchLogsTotalTimeout)
	defer cancel()
	if err = r.acquire(totalCtx); err != nil {
		return result, err
	}
	defer r.release()
	client, err := r.client(totalCtx, source.Region)
	if err != nil {
		return result, err
	}

	result.Events = make([]cloudWatchRawEvent, 0, limit)
	seenEvents := make(map[string]struct{}, limit)
	seenTokens := make(map[string]struct{})
	var nextToken *string
	for len(result.Events) < limit {
		pageLimit := int32(limit - len(result.Events))
		input := &cloudwatchlogs.FilterLogEventsInput{
			LogGroupName: aws.String(source.LogGroup),
			StartTime:    aws.Int64(req.From.UnixMilli()), EndTime: aws.Int64(req.To.UnixMilli()),
			FilterPattern: aws.String(pattern), Limit: &pageLimit, NextToken: nextToken,
			StartFromHead: aws.Bool(true), Unmask: false,
		}
		if source.StreamPrefix != "" {
			input.LogStreamNamePrefix = aws.String(source.StreamPrefix)
		}
		callCtx, callCancel := context.WithTimeout(totalCtx, cloudWatchLogsCallTimeout)
		output, callErr := client.FilterLogEvents(callCtx, input)
		callCancel()
		if callErr != nil {
			return result, newCloudWatchLogsError(classifyCloudWatchLogsError(callErr), "filter", callErr)
		}
		if output == nil {
			return result, newCloudWatchLogsError(cwLogsErrFailed, "filter", nil)
		}
		for index, event := range output.Events {
			id := aws.ToString(event.EventId)
			if id != "" {
				if _, exists := seenEvents[id]; exists {
					result.Deduplicated++
					continue
				}
				seenEvents[id] = struct{}{}
			}
			result.Events = append(result.Events, cloudWatchRawEvent{
				EventID: id, LogStream: aws.ToString(event.LogStreamName),
				TimestampMS: aws.ToInt64(event.Timestamp), IngestedMS: aws.ToInt64(event.IngestionTime),
				Message: aws.ToString(event.Message),
			})
			if len(result.Events) == limit {
				result.Truncated = index+1 < len(output.Events)
				break
			}
		}
		token := strings.TrimSpace(aws.ToString(output.NextToken))
		if token == "" {
			return result, nil
		}
		if _, exists := seenTokens[token]; exists {
			return result, newCloudWatchLogsError(cwLogsErrFailed, "filter_pagination", nil)
		}
		seenTokens[token] = struct{}{}
		nextToken = aws.String(token)
	}
	result.Truncated = result.Truncated || nextToken != nil
	return result, nil
}

func (r *cloudWatchLogsRuntime) recordStart(source cloudWatchLogSourceID) {
	if r == nil {
		return
	}
	r.metricsMu.Lock()
	metric := r.metrics[source]
	metric.Observed = true
	metric.Attempts++
	r.metrics[source] = metric
	r.metricsMu.Unlock()
}

func (r *cloudWatchLogsRuntime) recordFinish(source cloudWatchLogSourceID, result cloudWatchFilterResult, bytesScanned uint64, started time.Time, err error) {
	if r == nil {
		return
	}
	r.metricsMu.Lock()
	metric := r.metrics[source]
	metric.Observed = true
	metric.LastDurationNanos = time.Since(started).Nanoseconds()
	if err != nil {
		metric.Failures++
		metric.LastError = cloudWatchLogsErrorKindOf(err)
		if metric.LastError == cwLogsErrThrottled {
			metric.Throttled++
		}
	} else {
		metric.Successes++
		metric.Events += uint64(len(result.Events))
		metric.Deduplicated += result.Deduplicated
		metric.BytesScanned += bytesScanned
		metric.LastSuccessUnix = time.Now().Unix()
		metric.LastError = ""
		if result.Truncated {
			metric.Truncated++
		}
		for _, event := range result.Events {
			if event.TimestampMS > metric.LastEventUnixMS {
				metric.LastEventUnixMS = event.TimestampMS
			}
		}
	}
	r.metrics[source] = metric
	r.metricsMu.Unlock()
}

func (r *cloudWatchLogsRuntime) metricsSnapshot() map[cloudWatchLogSourceID]cloudWatchLogsSourceMetrics {
	out := make(map[cloudWatchLogSourceID]cloudWatchLogsSourceMetrics, len(cloudWatchLogSourceCatalog))
	if r == nil {
		return out
	}
	r.metricsMu.RLock()
	defer r.metricsMu.RUnlock()
	for source := range cloudWatchLogSourceCatalog {
		out[source] = r.metrics[source]
	}
	return out
}
