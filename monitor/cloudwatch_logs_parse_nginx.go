package monitor

import (
	"regexp"
	"strconv"
	"strings"
)

var cwNginxErrorPrefix = regexp.MustCompile(`^(\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}) \[([A-Za-z]+)\]`)

func (p *cloudWatchEvidenceParser) parseNginx(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	fields, err := cwInputFields(in)
	if err == nil && (cwField(fields, "request_method", "method") != "" || cwField(fields, "status") != "") {
		return p.parseNginxAccess(in, fields)
	}
	return p.parseNginxError(in)
}

func (p *cloudWatchEvidenceParser) parseNginxAccess(in cloudWatchEvidenceInput, fields map[string]string) (cloudWatchStructuredEvidence, error) {
	eventMS, err := cwTimestampMS(in, fields, "", "msec")
	if err != nil {
		eventMS, err = cwTimestampMS(in, fields, "", "ts")
	}
	method, path := cwField(fields, "request_method", "method"), cwField(fields, "uri", "path")
	status, statusErr := cwStatus(cwField(fields, "status"), false)
	requestMS, requestErr := cwDurationMS(cwField(fields, "request_time"))
	if err != nil || statusErr != nil || requestErr != nil || method == "" || path == "" || status == nil || requestMS == nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	upstreamStatuses, err := cwStatuses(cwField(fields, "upstream_status"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	upstreamMS, err := cwDurationMS(cwField(fields, "upstream_response_time"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	connectMS, err := cwDurationMS(cwField(fields, "upstream_connect_time"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	headerMS, err := cwDurationMS(cwField(fields, "upstream_header_time"))
	if err != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out := p.base(in)
	out.Kind = cwEvidenceNginxAccess
	out.EventMS = eventMS
	out.Method = cwMethod(method)
	out.Route = cwRoute(path)
	out.Status = status
	out.UpstreamStatuses = upstreamStatuses
	out.RequestMS = requestMS
	out.UpstreamMS = upstreamMS
	out.ConnectMS = connectMS
	out.HeaderMS = headerMS
	if bytes, present, parseErr := cwParseInt64(cwField(fields, "bytes_sent"), 0, 16<<30); parseErr != nil {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	} else if present {
		out.BytesSent = cwInt64Pointer(bytes)
	}
	out.Completion = cwCompletion(cwField(fields, "request_completion"))
	nginxHMAC, ok := p.optionalOpaqueHMAC("nginx-request-id", cwField(fields, "nginx_request_id", "request_id"))
	if !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out.NginxIDHMAC = nginxHMAC
	oneAPIHMAC, ok := p.optionalOpaqueHMAC("oneapi-request-id", cwField(fields, "oneapi_request_id"))
	if !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out.OneAPIIDHMAC = oneAPIHMAC
	detail := ""
	if len(upstreamStatuses) > 0 {
		detail = strconv.Itoa(upstreamStatuses[len(upstreamStatuses)-1])
	}
	out.FaultClass = cwHTTPFault(*status, detail)
	out.Category = "request"
	out.Summary = cwHTTPSummary("Nginx", *status)
	return out, nil
}

func (p *cloudWatchEvidenceParser) parseNginxError(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	message := strings.TrimSpace(in.Message)
	if in.Fields != nil {
		message = cwField(in.Fields, "@message", "message")
	}
	match := cwNginxErrorPrefix.FindStringSubmatch(message)
	if len(match) != 3 {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
	severity := strings.ToLower(match[2])
	switch severity {
	case "emerg", "alert", "crit", "error", "warn", "notice":
	default:
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
	eventMS := in.TimestampMS
	if eventMS <= 0 {
		if parsed, ok := cwParseLogTime(match[1], "2006/01/02 15:04:05"); ok {
			eventMS = parsed
		}
	}
	if eventMS <= 0 {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	category := cwNginxErrorCategory(message)
	out := p.base(in)
	out.Kind = cwEvidenceNginxError
	out.EventMS = eventMS
	out.Category = category
	out.Severity = severity
	out.FaultClass = cwNginxErrorFault(category)
	out.Summary = cwNginxErrorSummary(category)
	return out, nil
}

func cwNginxErrorCategory(message string) string {
	text := strings.ToLower(message)
	switch {
	case strings.Contains(text, "upstream timed out") || strings.Contains(text, "upstream timeout"):
		return "upstream_timeout"
	case strings.Contains(text, "connect() failed") && strings.Contains(text, "upstream"), strings.Contains(text, "no live upstreams"):
		return "upstream_connect_failed"
	case strings.Contains(text, "upstream prematurely closed connection"), strings.Contains(text, "upstream sent no valid"):
		return "upstream_closed"
	case strings.Contains(text, "upstream") && (strings.Contains(text, "ssl_do_handshake() failed") || strings.Contains(text, "certificate verify failed")):
		return "upstream_tls"
	case strings.Contains(text, "client prematurely closed connection"), strings.Contains(text, "client closed connection"):
		return "client_closed"
	case strings.Contains(text, "worker_connections are not enough"), strings.Contains(text, "too many open files"), strings.Contains(text, "cannot allocate memory"):
		return "worker_capacity"
	case strings.Contains(text, "host not found in upstream"), strings.Contains(text, "could not be resolved"), strings.Contains(text, "resolver") && strings.Contains(text, "failed"):
		return "resolver"
	case strings.Contains(text, "limiting requests"), strings.Contains(text, "limiting connections"):
		return "rate_limited"
	case strings.Contains(text, "client intended to send too large body"), strings.Contains(text, "client timed out") && strings.Contains(text, "request body"):
		return "request_body"
	default:
		return "other_error"
	}
}

func cwNginxErrorFault(category string) string {
	switch category {
	case "upstream_timeout":
		return "transport_timeout"
	case "upstream_connect_failed", "upstream_tls", "resolver":
		return "transport_connect"
	case "client_closed":
		return "client_gone"
	case "rate_limited", "worker_capacity":
		return "rate_limit_capacity"
	default:
		return "unknown"
	}
}

func cwNginxErrorSummary(category string) string {
	labels := map[string]string{"upstream_timeout": "Nginx 上游响应超时", "upstream_connect_failed": "Nginx 上游连接失败", "upstream_closed": "Nginx 上游提前关闭", "upstream_tls": "Nginx 上游 TLS 失败", "client_closed": "Nginx 记录到客户端提前关闭", "worker_capacity": "Nginx Worker 容量不足", "resolver": "Nginx 上游解析失败", "rate_limited": "Nginx 触发限流", "request_body": "Nginx 请求体处理异常", "other_error": "Nginx 记录到其他错误"}
	return labels[category]
}
