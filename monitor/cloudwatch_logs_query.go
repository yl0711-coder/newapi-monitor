package monitor

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

type cloudWatchFixedQueryKind string

const (
	cwQueryCloudFrontRequestID   cloudWatchFixedQueryKind = "cloudfront_request_id"
	cwQueryCloudFrontPath        cloudWatchFixedQueryKind = "cloudfront_path"
	cwQueryWorkerRequestID       cloudWatchFixedQueryKind = "worker_request_id"
	cwQueryWorkerPath            cloudWatchFixedQueryKind = "worker_path"
	cwQueryWorkerFixedErrors     cloudWatchFixedQueryKind = "worker_fixed_errors"
	cwQueryWorkerShadowNginx     cloudWatchFixedQueryKind = "worker_shadow_nginx"
	cwQueryWorkerShadowReject    cloudWatchFixedQueryKind = "worker_shadow_reject"
	cwQueryWorkerPreRouteReject  cloudWatchFixedQueryKind = "worker_preroute_reject"
	cwQueryWorkerNginxContinuous cloudWatchFixedQueryKind = "worker_nginx_continuous"
)

type cloudWatchInsightsRequest struct {
	Kind  cloudWatchFixedQueryKind
	From  time.Time
	To    time.Time
	Value string
	Limit int
}

type cloudWatchInsightsResult struct {
	Source         cloudWatchLogSourceID
	Rows           []map[string]string
	Status         cwlogtypes.QueryStatus
	Truncated      bool
	BytesScanned   uint64
	RecordsMatched uint64
	RecordsScanned uint64
}

// cloudWatchPreRouteRejectPattern is deliberately a closed vocabulary. The
// NewAPI log group also contains errors returned by an upstream provider; a
// broad query such as `rate limit` would pull those rows into the rejection
// lane and make the alert page report a route rejection that never happened.
// Keep this list in sync with the parser's rejection rules below. The
// case-insensitive flag covers the production `Invalid token`/`invalid token`
// split.
//
// ★ 长短语必须排在它的子串之前 ★ 正则交替是最左优先：把「该令牌无权访问模型」
// 放在「无权访问模型」之后，前者永远匹配不到，词表看着有、实际是死条目。
// 这里保留生产原话「该令牌无权访问模型」，它是 NewAPI 实际写出的措辞。
//
// 另有一类语序把模型名夹在措辞中间（`model gpt-5 is forbidden`、`模型 gpt-5 不存在`），
// 紧邻式词条匹配不到，因此单列带界定符的变体。模型名段用 [^\s|,，;；()（）]{1,80}
// 限长并排除分隔符，避免跨字段吞掉整行把上游错误也捞进这条拒绝车道。
const cloudWatchPreRouteRejectPattern = `(?i)(?:no available channel|no available channels|without available channel|no channel available|无可用渠道|没有可用渠道|无可用的渠道|没有可用的渠道|invalid token|token is invalid|unauthorized token|token invalid|无效令牌|无效的令牌|令牌无效|token disabled|token is disabled|令牌已禁用|令牌已被禁用|令牌被禁用|token model forbidden|model forbidden|model is not allowed|model not allowed|该令牌无权访问模型|该令牌无权限访问模型|无权访问模型|无权限访问模型|模型无权限|模型权限不足|model\s+[^\s|,，;；()（）]{1,80}\s+is\s+(?:forbidden|not\s+allowed)|模型\s*[^\s|,，;；()（）]{1,80}\s*(?:无权限|无权访问|权限不足)|model not found|model does not exist|unknown model|模型不存在|模型未找到|找不到模型|未知模型|model\s+[^\s|,，;；()（）]{1,80}\s+is\s+not\s+found|model\s+[^\s|,，;；()（）]{1,80}\s+does\s+not\s+exist|模型\s*[^\s|,，;；()（）]{1,80}\s*(?:不存在|未找到)|quota insufficient|insufficient quota|insufficient user quota|insufficient token quota|user quota insufficient|user quota is insufficient|user quota is not enough|token quota insufficient|token quota is insufficient|token quota is not enough|quota is insufficient|quota is not enough|not enough quota|pre consume failed|pre-consume failed|pre consume quota failed|pre-consume quota failed|too little quota|用户额度不足|账户额度不足|余额不足|额度不足|令牌额度不足|预扣费失败|预扣费额度失败|rate limit|rate_limit|rate limited|too many requests|concurrency limited|限流|请求过于频繁|超过速率限制|并发限制|并发数超限)`

// These markers identify an error returned by a selected upstream/channel,
// rather than a request rejected before routing. They are intentionally
// conservative: a row containing one is still useful evidence, but must not
// enter rejection_samples. The parser applies the same guard because query
// filters are not a security boundary and can be changed by AWS-side data.
const cloudWatchPreRouteUpstreamMarkerPattern = `(?i)(?:(?:status[_ ]?code|http[_ ]?status|response\s+status(?:\s+code)?)\s*[:=]|(?:upstream|provider|channel)\s+(?:returned|return|response|error|request|rate|limit|model|no|5\d\d)|bad response status|上游(?:返回|错误|限流|模型)|供应商(?:返回|错误|模型)|渠道(?:\s+[^\r\n:：,，]{1,80})?\s*(?:返回错误|错误|限流))`

func buildCloudWatchInsightsQuery(req cloudWatchInsightsRequest) (cloudWatchLogSource, string, int, error) {
	maximum := cloudWatchLogsHardLimit
	if req.Kind == cwQueryWorkerPreRouteReject {
		maximum = cloudWatchLogsPreRouteLimit
	} else if req.Kind == cwQueryWorkerNginxContinuous {
		maximum = cloudWatchLogsNginxLimit
	}
	limit, err := validateCloudWatchLogRangeLimit(req.From, req.To, req.Limit, maximum)
	if err != nil {
		return cloudWatchLogSource{}, "", 0, err
	}
	var sourceID cloudWatchLogSourceID
	var query string
	switch req.Kind {
	case cwQueryCloudFrontRequestID:
		value, err := validateCloudWatchQueryValue(req.Value, 256)
		if err != nil {
			return cloudWatchLogSource{}, "", 0, err
		}
		sourceID = cwSourceCloudFrontAccess
		query = fmt.Sprintf("filter `x-edge-request-id` = %s | fields @timestamp, `x-edge-request-id`, `sc-status`, `cs-method`, `cs-uri-stem`, `x-edge-result-type`, `x-edge-detailed-result-type`, `time-taken`, `time-to-first-byte`, `origin-fbl`, `origin-lbl`", cloudWatchQueryLiteral(value))
	case cwQueryCloudFrontPath:
		value, err := validateCloudWatchQueryValue(req.Value, 1024)
		if err != nil || !strings.HasPrefix(value, "/") {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "path", err)
		}
		sourceID = cwSourceCloudFrontAccess
		query = fmt.Sprintf("filter `cs-uri-stem` = %s | fields @timestamp, `x-edge-request-id`, `sc-status`, `cs-method`, `cs-uri-stem`, `time-taken`, `time-to-first-byte`, `origin-fbl`, `x-edge-detailed-result-type`", cloudWatchQueryLiteral(value))
	case cwQueryWorkerRequestID:
		value, err := validateCloudWatchQueryValue(req.Value, 256)
		if err != nil {
			return cloudWatchLogSource{}, "", 0, err
		}
		sourceID = cwSourceWorkerNginx
		query = fmt.Sprintf("filter @logStream like /^nginx\\// | filter oneapi_request_id = %s | fields @timestamp, @logStream, nginx_request_id, oneapi_request_id, request_method, uri, status, request_time, upstream_status, upstream_response_time, upstream_connect_time, upstream_header_time, bytes_sent, request_completion", cloudWatchQueryLiteral(value))
	case cwQueryWorkerPath:
		value, err := validateCloudWatchQueryValue(req.Value, 1024)
		if err != nil || !strings.HasPrefix(value, "/") {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "path", err)
		}
		sourceID = cwSourceWorkerNginx
		query = fmt.Sprintf("filter @logStream like /^nginx\\// | filter uri = %s | fields @timestamp, @logStream, nginx_request_id, oneapi_request_id, request_method, uri, status, request_time, upstream_status, upstream_response_time, upstream_connect_time, upstream_header_time, bytes_sent, request_completion", cloudWatchQueryLiteral(value))
	case cwQueryWorkerFixedErrors:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "fixed_query_value", nil)
		}
		sourceID = cwSourceWorkerNewAPI
		query = "filter @logStream like /^new-api\\// | filter @message like /No available channel|panic|fatal|timeout|broken pipe/ | fields @timestamp, @logStream, @message"
	case cwQueryWorkerShadowNginx:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "shadow_query_value", nil)
		}
		sourceID = cwSourceWorkerNginx
		// Shadow 只比较结构化 access 事实。同一 nginx/ 前缀还会混有错误和
		// 运行日志；若不在查询侧排除，它们既不是 access 请求，也没有完整的
		// access 字段，会把本来完整的窗口误标为解析失败。
		// 不使用任意用户提供的查询语句，也不把原始日志正文作为持久化结果。
		query = "filter @logStream like /^nginx\\// | filter ispresent(request_method) and ispresent(uri) and ispresent(status) and ispresent(request_time) | fields @timestamp, @logStream, @ptr, nginx_request_id, oneapi_request_id, request_method, uri, status, request_time, upstream_status, upstream_response_time, upstream_connect_time, upstream_header_time, bytes_sent, request_completion"
	case cwQueryWorkerShadowReject, cwQueryWorkerPreRouteReject:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "shadow_query_value", nil)
		}
		sourceID = cwSourceWorkerNewAPI
		// 过滤只覆盖前置拒绝闭集；普通 GIN/上游错误不进入 rejection lane。
		// 第二个 filter 是 defense-in-depth，避免上游明确带 status_code、
		// provider/upstream 来源标记的错误被“rate limit”等通用短语选进来。
		query = "filter @logStream like /^new-api\\// | filter @message like /" + cloudWatchPreRouteRejectPattern + "/ | filter @message not like /" + cloudWatchPreRouteUpstreamMarkerPattern + "/ | filter @message not like /(?i)^\\s*\\[gin\\]/ | fields @timestamp, @logStream, @ptr, @message, user_id, userId, uid, model, model_name, group, grp"
	case cwQueryWorkerNginxContinuous:
		if strings.TrimSpace(req.Value) != "" {
			return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "continuous_query_value", nil)
		}
		sourceID = cwSourceWorkerNginx
		// 只允许结构化 access 和标准 error.log 行进入解析器。nginx/ 前缀下
		// 的启动/运行噪声既不是请求事实也不是错误分类，必须在 AWS 查询侧排除。
		query = "filter @logStream like /^nginx\\// | filter (ispresent(request_method) and ispresent(uri) and ispresent(status) and ispresent(request_time)) or @message like /\\[(emerg|alert|crit|error|warn|notice)\\]/ | fields @timestamp, @logStream, @ptr, @message, nginx_request_id, oneapi_request_id, request_method, uri, status, request_time, upstream_status, upstream_response_time, upstream_connect_time, upstream_header_time, bytes_sent, request_completion"
	default:
		return cloudWatchLogSource{}, "", 0, newCloudWatchLogsError(cwLogsErrInvalid, "query_kind", nil)
	}
	source, err := cloudWatchLogSourceByID(sourceID)
	if err != nil {
		return cloudWatchLogSource{}, "", 0, err
	}
	return source, fmt.Sprintf("%s | sort @timestamp asc | limit %d", query, limit), limit, nil
}

func cloudWatchQueryLiteral(value string) string {
	value = strings.ReplaceAll(value, `\`, `\\`)
	value = strings.ReplaceAll(value, `"`, `\"`)
	return `"` + value + `"`
}

func (r *cloudWatchLogsRuntime) insights(ctx context.Context, req cloudWatchInsightsRequest) (result cloudWatchInsightsResult, err error) {
	source, query, limit, err := buildCloudWatchInsightsQuery(req)
	if err != nil {
		return result, err
	}
	started := time.Now()
	result.Source = source.ID
	r.recordStart(source.ID)
	defer func() { r.recordInsightsFinish(source.ID, result, started, err) }()

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
	startCtx, startCancel := context.WithTimeout(totalCtx, cloudWatchLogsCallTimeout)
	startedQuery, startErr := client.StartQuery(startCtx, &cloudwatchlogs.StartQueryInput{
		LogGroupName: aws.String(source.LogGroup), QueryString: aws.String(query),
		StartTime: aws.Int64(req.From.Unix()), EndTime: aws.Int64(req.To.Unix()), Limit: aws.Int32(int32(limit)),
	})
	startCancel()
	if startErr != nil {
		return result, newCloudWatchLogsError(classifyCloudWatchLogsError(startErr), "start_query", startErr)
	}
	if startedQuery == nil {
		return result, newCloudWatchLogsError(cwLogsErrFailed, "start_query", nil)
	}
	queryID := strings.TrimSpace(aws.ToString(startedQuery.QueryId))
	if queryID == "" {
		return result, newCloudWatchLogsError(cwLogsErrFailed, "start_query", nil)
	}
	stopNeeded := true
	defer func() {
		if stopNeeded {
			stopCloudWatchQuery(client, queryID)
		}
	}()

	for {
		getCtx, getCancel := context.WithTimeout(totalCtx, cloudWatchLogsCallTimeout)
		output, getErr := client.GetQueryResults(getCtx, &cloudwatchlogs.GetQueryResultsInput{QueryId: aws.String(queryID)})
		getCancel()
		if getErr != nil {
			return result, newCloudWatchLogsError(classifyCloudWatchLogsError(getErr), "get_query_results", getErr)
		}
		if output == nil {
			return result, newCloudWatchLogsError(cwLogsErrFailed, "get_query_results", nil)
		}
		result.Status = output.Status
		if output.Statistics != nil {
			result.BytesScanned = nonNegativeUint64(output.Statistics.BytesScanned)
			result.RecordsMatched = nonNegativeUint64(output.Statistics.RecordsMatched)
			result.RecordsScanned = nonNegativeUint64(output.Statistics.RecordsScanned)
		}
		switch output.Status {
		case cwlogtypes.QueryStatusScheduled, cwlogtypes.QueryStatusRunning:
			if pollErr := r.poll(totalCtx); pollErr != nil {
				return result, newCloudWatchLogsError(classifyCloudWatchLogsError(pollErr), "poll_query", pollErr)
			}
			continue
		case cwlogtypes.QueryStatusComplete:
			stopNeeded = false
			result.Rows = cloudWatchQueryRows(output.Results, limit)
			result.Truncated = len(output.Results) > len(result.Rows) || strings.TrimSpace(aws.ToString(output.NextToken)) != "" || result.RecordsMatched > uint64(len(result.Rows))
			return result, nil
		case cwlogtypes.QueryStatusFailed:
			stopNeeded = false
			return result, newCloudWatchLogsError(cwLogsErrFailed, "query_status", nil)
		case cwlogtypes.QueryStatusCancelled:
			stopNeeded = false
			return result, newCloudWatchLogsError(cwLogsErrCancelled, "query_status", nil)
		case cwlogtypes.QueryStatusTimeout:
			stopNeeded = false
			return result, newCloudWatchLogsError(cwLogsErrTimeout, "query_status", nil)
		default:
			return result, newCloudWatchLogsError(cwLogsErrUnavailable, "query_status", nil)
		}
	}
}

func stopCloudWatchQuery(client cloudWatchLogsAPI, queryID string) {
	ctx, cancel := context.WithTimeout(context.Background(), cloudWatchLogsStopTimeout)
	defer cancel()
	_, _ = client.StopQuery(ctx, &cloudwatchlogs.StopQueryInput{QueryId: aws.String(queryID)})
}

func cloudWatchQueryRows(rows [][]cwlogtypes.ResultField, limit int) []map[string]string {
	if len(rows) > limit {
		rows = rows[:limit]
	}
	out := make([]map[string]string, 0, len(rows))
	for _, fields := range rows {
		row := make(map[string]string, len(fields))
		for _, field := range fields {
			name := strings.TrimSpace(aws.ToString(field.Field))
			if name != "" {
				row[name] = aws.ToString(field.Value)
			}
		}
		out = append(out, row)
	}
	return out
}

func nonNegativeUint64(value float64) uint64 {
	if value <= 0 {
		return 0
	}
	return uint64(value)
}

func parseCloudWatchResultTime(value string) (time.Time, bool) {
	value = strings.TrimSpace(value)
	for _, layout := range []string{time.RFC3339Nano, "2006-01-02 15:04:05.000", "2006-01-02 15:04:05"} {
		if at, err := time.ParseInLocation(layout, value, time.UTC); err == nil {
			return at, true
		}
	}
	return time.Time{}, false
}

func (r *cloudWatchLogsRuntime) recordInsightsFinish(source cloudWatchLogSourceID, result cloudWatchInsightsResult, started time.Time, err error) {
	if r == nil {
		return
	}
	r.metricsMu.Lock()
	metric := r.metrics[source]
	metric.Observed = true
	metric.LastDurationNanos = time.Since(started).Nanoseconds()
	metric.BytesScanned += result.BytesScanned
	if err != nil {
		metric.Failures++
		metric.LastError = cloudWatchLogsErrorKindOf(err)
		if metric.LastError == cwLogsErrThrottled {
			metric.Throttled++
		}
	} else {
		metric.Successes++
		metric.Events += uint64(len(result.Rows))
		if result.Truncated {
			metric.Truncated++
		}
		metric.LastSuccessUnix = time.Now().Unix()
		metric.LastError = ""
		for _, row := range result.Rows {
			if at, ok := parseCloudWatchResultTime(row["@timestamp"]); ok && at.UnixMilli() > metric.LastEventUnixMS {
				metric.LastEventUnixMS = at.UnixMilli()
			}
		}
	}
	r.metrics[source] = metric
	r.metricsMu.Unlock()
}
