package monitor

// Customer troubleshooting only. The shared SQL classification and portal log
// schemas deliberately do not consume this catalogue or these admin fields.
import (
	_ "embed"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

//go:embed logchain_diagnostic_rules.json
var logChainDiagnosticRulesJSON []byte

type logChainDiagnosticRule struct {
	ID             string   `json:"id"`
	Version        string   `json:"version"`
	DocumentLines  []int    `json:"document_lines"`
	Status         int      `json:"status"`
	Pattern        string   `json:"pattern"`
	Priority       int      `json:"priority"`
	Profile        string   `json:"profile"`
	Reason         string   `json:"reason"`
	Responsibility string   `json:"responsibility"`
	Action         string   `json:"action"`
	RequiredFields []string `json:"required_fields"`
	CauseKnown     bool     `json:"cause_known"`
	re             *regexp.Regexp
}

var logChainDiagnosticRules = func() []logChainDiagnosticRule {
	var rules []logChainDiagnosticRule
	if err := json.Unmarshal(logChainDiagnosticRulesJSON, &rules); err != nil {
		panic(err)
	}
	for i := range rules {
		rules[i].re = regexp.MustCompile(rules[i].Pattern)
	}
	return rules
}()

type LogChainDiagnosticField struct {
	Value      string `json:"value"`
	Source     string `json:"source"`
	ObservedAt int64  `json:"observed_at"`
	Scope      string `json:"scope"` // request or current_config; never interchangeable
}

type LogChainDiagnosticError struct {
	Message    string `json:"message,omitempty"`
	Code       string `json:"code,omitempty"`
	Type       string `json:"type,omitempty"`
	Param      string `json:"param,omitempty"`
	Status     int    `json:"status,omitempty"`
	Source     string `json:"source"`
	Path       string `json:"path"`
	ObservedAt int64  `json:"observed_at"`
	AttemptID  string `json:"attempt_id,omitempty"`
	Origin     string `json:"origin,omitempty"`
}

type LogChainDiagnosticFacts struct {
	Fields              map[string]LogChainDiagnosticField `json:"fields"`
	Errors              []LogChainDiagnosticError          `json:"errors,omitempty"`
	Truncated           bool                               `json:"truncated,omitempty"`
	RequestIDs          []LogChainDiagnosticRequestID      `json:"request_ids,omitempty"`
	RequestIDsTruncated bool                               `json:"request_ids_truncated,omitempty"`
}

type LogChainDiagnosticMissing struct {
	Field         string                           `json:"field"`
	Reason        string                           `json:"reason"`
	RecordAt      string                           `json:"record_at"`
	Sources       []cloudWatchLogSourceID          `json:"sources,omitempty"`
	SourceResults map[cloudWatchLogSourceID]string `json:"source_results,omitempty"`
	Status        string                           `json:"status,omitempty"`
	AvailableFrom string                           `json:"available_from,omitempty"`
}

type LogChainDiagnosis struct {
	RuleID             string                      `json:"rule_id"`
	Version            string                      `json:"version"`
	DocumentLines      []int                       `json:"document_lines"`
	Reason             string                      `json:"reason"`
	CauseKnown         bool                        `json:"cause_known"`
	Confidence         string                      `json:"confidence,omitempty"`
	Responsibility     string                      `json:"responsibility"`
	ResponsibleParties []string                    `json:"responsible_parties"`
	ErrorLayer         string                      `json:"error_layer"`
	Action             string                      `json:"action"`
	ActionOwners       []string                    `json:"action_owners"`
	Basis              string                      `json:"basis"`
	Missing            []LogChainDiagnosticMissing `json:"missing"`
	Variables          map[string]string           `json:"variables,omitempty"`
	Profile            string                      `json:"profile"`
}

const logChainDiagnosticMaxText = 16 * 1024

var logChainDiagnosticSecret = regexp.MustCompile(`(?i)(authorization|cookie|set-cookie|password|api[-_ ]?key|access[-_ ]?token|refresh[-_ ]?token|secret)["']?\s*[:=]\s*["']?(?:bearer\s+)?[^\s,;"'}]+`)
var logChainDiagnosticPayload = regexp.MustCompile(`(?is)(?:prompt|input|messages|completion|response_text)["']?\s*[:=]\s*(?:"[^"\r\n]*"|'[^'\r\n]*'|\[[^\r\n]*|\{[^\r\n]*)`)
var logChainDiagnosticReflectedSchema = regexp.MustCompile(`(?is)(unexpected constant value:)\s*[\[{].*`)
var logChainDiagnosticURL = regexp.MustCompile(`https?://[^\s"'<>]+`)
var logChainDiagnosticStatus = regexp.MustCompile(`(?i)(?:status_code\s*=|status\s*=|HTTP\s+|status code\s+)([1-5][0-9]{2})`)
var logChainDiagnosticPath = regexp.MustCompile(`(?i)(?:path=|(?:^|[\s,:]))((?:messages|input|tools|response_format)(?:\[[0-9]+\]|\.[a-zA-Z_0-9]+)+)`)
var logChainDiagnosticNumbers = []*regexp.Regexp{
	regexp.MustCompile(`(?i)cpu overloaded.{0,40}current\s*[:=]?\s*(?P<resource_current>[0-9.]+)%?[,; ]+threshold\s*[:=]?\s*(?P<resource_limit>[0-9.]+)%?`),
	regexp.MustCompile(`(?:用户)?剩余额度[:： ]*[$＄]?\s*(?P<balance>-?[0-9.]+)`),
	regexp.MustCompile(`(?:需要)?预扣费额度[:： ]*[$＄]?\s*(?P<precharge>[0-9.]+)`),
	regexp.MustCompile(`(?i)\breset\s+(?P<reset_at>[0-9T:.Z+-]+)`),
	regexp.MustCompile(`请求体\s*(?P<request_body_bytes>[0-9]+)\s*字节`),
}

func logChainDiagnosticSafeText(s string, max int) string {
	if len(s) > logChainDiagnosticMaxText {
		s = s[:logChainDiagnosticMaxText]
	}
	s = strings.ToValidUTF8(s, "")
	// Strip URL credentials before the email/secret patterns can corrupt the
	// authority. The remaining host and path are needed for route diagnosis.
	s = logChainDiagnosticURL.ReplaceAllStringFunc(s, func(raw string) string {
		u, err := url.Parse(raw)
		if err != nil {
			return "<url>"
		}
		u.User, u.RawQuery, u.Fragment = nil, "", ""
		return u.String()
	})
	s = logChainDiagnosticSecret.ReplaceAllString(s, "$1=<redacted>")
	s = problemBearerPattern.ReplaceAllString(s, "Bearer <redacted>")
	s = problemKeyPattern.ReplaceAllString(s, "<credential>")
	s = problemEmailPattern.ReplaceAllString(s, "<email>")
	s = problemIPv4Pattern.ReplaceAllString(s, "<ip>")
	s = logChainDiagnosticPayload.ReplaceAllString(s, "<request-content-redacted>")
	s = logChainDiagnosticReflectedSchema.ReplaceAllString(s, "$1 <schema-value-redacted>")
	runes := []rune(s)
	if len(runes) > max {
		s = string(runes[:max]) + "…"
	}
	return s
}

// Only allowlisted metadata is retained. No request/response content bodies,
// headers, credentials or arbitrary JSON objects are copied into admin fields.
var logChainDiagnosticAliases = map[string]string{
	"request_path": "request_path", "path": "request_path", "request_method": "request_method", "method": "request_method",
	"upstream_request_id": "upstream_request_id", "provider_request_id": "model_provider_request_id",
	"model_provider_request_id": "model_provider_request_id", "upstream_relay_request_id": "upstream_relay_request_id",
	"forward_path": "forward_path", "upstream_path": "forward_path", "actual_forward_path": "forward_path",
	"upstream_host": "actual_upstream_host", "actual_upstream_host": "actual_upstream_host", "upstream_url": "actual_upstream_url",
	"location": "location", "model_mapping": "model_mapping", "upstream_model_name": "upstream_model", "model": "model", "model_name": "model",
	"group": "group", "account_capability": "account_capability", "supported_endpoints": "supported_endpoints", "supported_models": "supported_models",
	"request_length": "request_length", "request_body_bytes": "request_body_bytes", "body_bytes": "request_body_bytes", "request_body_limit": "request_body_limit",
	"error_source": "error_source", "error_param": "error_param", "error_code": "error_code", "error_type": "error_type",
	"parameter_values": "parameter_values", "parameter_limits": "parameter_limits", "schema_constraints": "schema_constraints", "tool_pairing": "tool_pairing",
	"original_account": "original_account", "original_route": "original_route", "previous_response_id": "continuation_id", "continuation_id": "continuation_id",
	"limit_subject": "limit_subject", "account_id": "account_id", "balance": "balance", "user_quota": "balance", "precharge": "precharge", "pre_consume_quota": "precharge",
	"limit": "limit", "reset_at": "reset_at", "retry_after": "reset_at", "input_tokens": "input_tokens", "limit_tokens": "limit_tokens",
	"connect_ms": "connect_ms", "first_byte_ms": "first_byte_ms", "frt": "first_byte_ms", "total_ms": "total_ms", "end_reason": "stream_end_reason",
	"retry_attempt": "retry_attempt", "attempt_id": "attempt_id", "node": "node", "policy_rule": "policy_rule", "resource_current": "resource_current", "resource_limit": "resource_limit",
	"route_config_error": "route_config_error", "status_code": "layer_statuses",
}

func logChainDiagnosticScalar(v any) string {
	switch v := v.(type) {
	case string:
		return v
	case json.Number:
		return string(v)
	case bool:
		return strconv.FormatBool(v)
	}
	return ""
}

func (f *LogChainDiagnosticFacts) put(key, value, source string, at int64, scope string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	if f.Fields == nil {
		f.Fields = map[string]LogChainDiagnosticField{}
	}
	if old, exists := f.Fields[key]; exists && !(old.Scope == "current_config" && scope == "request") {
		return
	}
	if key == "request_path" || key == "forward_path" || key == "location" {
		if u, err := url.Parse(value); err == nil {
			u.User, u.RawQuery, u.Fragment = nil, "", ""
			value = u.String()
		}
	}
	f.Fields[key] = LogChainDiagnosticField{Value: logChainDiagnosticSafeText(value, 1024), Source: source, ObservedAt: at, Scope: scope}
}

func logChainDiagnosticMetadata(key string, v any) string {
	switch key {
	case "supported_endpoints", "supported_models":
		if values, ok := v.([]any); ok {
			var out []string
			for i, value := range values {
				if i >= 32 {
					break
				}
				if s := logChainDiagnosticScalar(value); s != "" {
					out = append(out, logChainDiagnosticSafeText(s, 128))
				}
			}
			return strings.Join(out, ",")
		}
	case "model_mapping", "parameter_limits", "schema_constraints", "tool_pairing", "parameter_values":
		if values, ok := v.(map[string]any); ok {
			keys := make([]string, 0, len(values))
			for k := range values {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			var out []string
			for _, k := range keys {
				if len(out) >= 16 {
					break
				}
				if containsAny(strings.ToLower(k), "content", "prompt", "message", "secret", "password", "token_key", "authorization", "cookie") {
					continue
				}
				if key == "parameter_values" && !containsAny(k, "thinking", "temperature", "max_tokens", "top_p", "reasoning_effort", "output_config.effort", "parallel_tool_calls") {
					continue
				}
				if s := logChainDiagnosticScalar(values[k]); s != "" {
					out = append(out, logChainDiagnosticSafeText(k, 128)+"="+logChainDiagnosticSafeText(s, 128))
				}
			}
			return strings.Join(out, "; ")
		}
	}
	return ""
}

func (f *LogChainDiagnosticFacts) value(key string) string {
	if f == nil {
		return ""
	}
	v := f.Fields[key]
	if v.Scope != "request" {
		return ""
	}
	return v.Value
}

func logChainParseDiagnosticFacts(content, other, source string, at int64) *LogChainDiagnosticFacts {
	return logChainParseDiagnosticFactsMode(content, other, source, at, false)
}

// Extra delivery payloads are administrator troubleshooting evidence. Keep
// the shared customer-maintenance classification parser's input boundary.
func logChainParseDiagnosticAcquisitionFacts(content, other, source string, at int64) *LogChainDiagnosticFacts {
	return logChainParseDiagnosticFactsMode(content, other, source, at, true)
}

func logChainParseDiagnosticFactsMode(content, other, source string, at int64, acquisition bool) *LogChainDiagnosticFacts {
	f := &LogChainDiagnosticFacts{Fields: map[string]LogChainDiagnosticField{}}
	var visit func(any, string, int, logChainDiagnosticContext)
	var parseText func(string, string, int, logChainDiagnosticContext)
	parseText = func(text, path string, depth int, ctx logChainDiagnosticContext) {
		if depth > 16 || len(f.Errors) >= 24 {
			f.Truncated = true
			f.RequestIDsTruncated = true
			return
		}
		if len(text) > logChainDiagnosticMaxText {
			text = text[:logChainDiagnosticMaxText]
			f.Truncated = true
			f.RequestIDsTruncated = true
		}
		text = strings.TrimSpace(text)
		if ctx.owner == "unknown" && logChainFaultFromUpstream(text) {
			ctx.owner = "upstream"
		}
		// Decode JSON strings/wrappers, including JSON after a status/log prefix.
		candidates := []string{text}
		if start := strings.IndexByte(text, '{'); start > 0 {
			candidates = append(candidates, text[start:])
		}
		for _, candidate := range candidates {
			dec := json.NewDecoder(strings.NewReader(candidate))
			dec.UseNumber()
			var v any
			if dec.Decode(&v) == nil {
				if candidate != text {
					// Preserve the wrapper before its nested message, without
					// copying arbitrary JSON request bodies into diagnostic facts.
					prefix := strings.TrimSpace(text[:len(text)-len(candidate)])
					f.Errors = append(f.Errors, LogChainDiagnosticError{Message: logChainDiagnosticSafeText(prefix, 4096), Source: source, Path: path, AttemptID: ctx.attempt, Origin: ctx.owner})
					f.parseParentheticalIDs(prefix, source+":"+path, at, ctx)
				}
				visit(v, path, depth+1, ctx)
				if tail := strings.TrimSpace(candidate[dec.InputOffset():]); tail != "" {
					parseText(tail, path+".suffix", depth+1, ctx)
				}
				return
			}
		}
		// Escaped JSON/text may have been wrapped by more than one relay.
		if strings.Contains(text, `\"`) || strings.Contains(text, `\u`) {
			if unquoted, err := strconv.Unquote(`"` + text + `"`); err == nil && unquoted != text {
				parseText(unquoted, path, depth+1, ctx)
				return
			}
			text = strings.NewReplacer(`\"`, `"`, `\u003e`, ">", `\u003c`, "<", `\n`, " ", `\t`, " ").Replace(text)
		}
		if text != "" {
			f.parseParentheticalIDs(text, source+":"+path, at, ctx)
			f.Errors = append(f.Errors, LogChainDiagnosticError{Message: logChainDiagnosticSafeText(text, 4096), Source: source, Path: path, AttemptID: ctx.attempt, Origin: ctx.owner})
		}
	}
	visit = func(v any, path string, depth int, ctx logChainDiagnosticContext) {
		if depth > 16 || len(f.Errors) >= 24 {
			f.Truncated = true
			f.RequestIDsTruncated = true
			return
		}
		switch x := v.(type) {
		case string:
			parseText(x, path, depth+1, ctx)
		case map[string]any:
			if a := logChainDiagnosticScalar(x["attempt_id"]); a != "" {
				ctx.attempt = a
			}
			if owner := logChainDiagnosticIDOwner(logChainDiagnosticScalar(x["error_source"])); owner != "unknown" {
				ctx.owner = owner
			}
			e := LogChainDiagnosticError{Source: source, Path: path, AttemptID: ctx.attempt, Origin: ctx.owner}
			keys := make([]string, 0, len(x))
			for k := range x {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				v := x[k]
				key := strings.ToLower(k)
				scalar := logChainDiagnosticScalar(v)
				if owner, identity := logChainDiagnosticStructuredIDOwner(key, path, source, ctx.owner); identity {
					f.addRequestID(scalar, owner, source+":"+path+"."+k, at, ctx.attempt, key != "request_id" && key != "requestid")
				}
				if field := logChainDiagnosticAliases[key]; field != "" {
					if key == "path" && !strings.HasPrefix(scalar, "/") {
						field = "error_param"
					}
					if scalar == "" {
						scalar = logChainDiagnosticMetadata(key, v)
					}
					// A nested retry's attempt must not become the row's attempt.
					currentAttempt := f.value("attempt_id")
					if (currentAttempt == "" || ctx.attempt == "" || ctx.attempt == currentAttempt) && (field != "attempt_id" || path == "content" || path == "other") {
						f.put(field, scalar, source+":"+path+"."+k, at, "request")
					}
				}
				switch key {
				case "code", "error_code":
					e.Code = logChainDiagnosticSafeText(scalar, 128)
				case "type", "error_type":
					e.Type = logChainDiagnosticSafeText(scalar, 128)
				case "param", "error_param":
					e.Param = logChainDiagnosticSafeText(scalar, 256)
					if current := f.value("attempt_id"); current == "" || ctx.attempt == "" || current == ctx.attempt {
						f.put("error_param", scalar, source+":"+path+"."+k, at, "request")
					}
				case "statuscode", "status_code", "status":
					e.Status, _ = strconv.Atoi(scalar)
				case "message", "msg", "error_message":
					if scalar != "" {
						parseText(scalar, path+"."+k, depth+1, ctx)
					}
				case "end_error", "errors":
					if acquisition {
						visit(v, path+"."+k, depth+1, ctx)
					}
				case "error", "exterror", "cause", "inner_error", "error_response", "response_body", "response", "body", "data", "other", "admin_info", "diagnostic", "stream_status", "route_snapshot", "request_snapshot":
					if _, structured := v.(map[string]any); structured && (key == "error" || key == "error_response" || key == "response_body") {
						f.put("error_response", "已记录结构化错误对象，详见错误证据", source+":"+path+"."+k, at, "request")
					}
					visit(v, path+"."+k, depth+1, ctx)
				}
			}
			if e.Code != "" || e.Type != "" || e.Param != "" || e.Status > 0 {
				f.Errors = append(f.Errors, e)
			}
		case []any:
			for i, v := range x {
				if i >= 8 {
					f.Truncated = true
					f.RequestIDsTruncated = true
					break
				}
				visit(v, fmt.Sprintf("%s[%d]", path, i), depth+1, ctx)
			}
		}
	}
	ctx := logChainDiagnosticContext{owner: "unknown"}
	if source == "upstream_error_logs" || logChainFaultFromUpstream(strings.TrimSpace(content)) {
		ctx.owner = "upstream"
	}
	// Root metadata binds content to the current attempt regardless of JSON key order.
	if len(other) <= logChainDiagnosticMaxText {
		var meta map[string]any
		if json.Unmarshal([]byte(other), &meta) == nil {
			ctx.attempt = logChainDiagnosticScalar(meta["attempt_id"])
			if owner := logChainDiagnosticIDOwner(logChainDiagnosticScalar(meta["error_source"])); owner != "unknown" {
				ctx.owner = owner
			}
		}
	}
	f.put("attempt_id", ctx.attempt, source+":other.attempt_id", at, "request")
	parseText(content, "content", 0, ctx)
	if strings.TrimSpace(other) != "" {
		var x any
		dec := json.NewDecoder(strings.NewReader(other))
		dec.UseNumber()
		if len(other) <= logChainDiagnosticMaxText && dec.Decode(&x) == nil {
			visit(x, "other", 0, ctx)
		} else {
			f.Truncated = true
			f.RequestIDsTruncated = true
		}
	}
	for i := range f.Errors {
		f.Errors[i].ObservedAt = at
		e := f.Errors[i]
		if current := f.value("attempt_id"); current != "" && e.AttemptID != "" && current != e.AttemptID {
			continue
		}
		f.put("error_code", e.Code, e.Source+":"+e.Path, at, "request")
		for _, re := range logChainDiagnosticNumbers {
			if values := re.FindStringSubmatch(e.Message); values != nil {
				for i, key := range re.SubexpNames() {
					if i > 0 && key != "" {
						f.put(key, values[i], source+":"+e.Path, at, "request")
					}
				}
			}
		}
		f.put("error_type", e.Type, e.Source+":"+e.Path, at, "request")
		if m := logChainDiagnosticPath.FindStringSubmatch(e.Message); len(m) > 1 {
			f.put("error_param", m[1], source+":"+e.Path, at, "request")
		}
	}
	if v := f.value("actual_upstream_url"); v != "" {
		if u, err := url.Parse(v); err == nil && u.Host != "" {
			f.put("actual_upstream_host", u.Host, source+":upstream_url", at, "request")
			f.put("forward_path", u.Path, source+":upstream_url", at, "request")
		}
	}
	f.resolveRequestIDs(f.value("request_id"))
	return f
}

func logChainRowDiagnosticFacts(r LogChainRow) *LogChainDiagnosticFacts {
	f := r.DiagnosticFacts
	if f == nil {
		f = logChainParseDiagnosticFacts(r.Content, "", "logs.content", r.CreatedAt)
	}
	// Return a copy: investigation retries and list responses can share snapshots.
	out := logChainCloneDiagnosticFacts(f)
	if len(out.Errors) == 0 && r.Content != "" {
		extra := logChainParseDiagnosticFacts(r.Content, "", "logs.content", r.CreatedAt)
		out.Errors = extra.Errors
		out.mergeRequestIDs(extra)
	}
	if r.EndError != "" {
		if len(out.Errors) < 48 {
			extra := logChainParseDiagnosticFacts(r.EndError, "", "logs.other.stream_status.end_error", r.CreatedAt)
			seen := make(map[LogChainDiagnosticError]bool, len(out.Errors))
			for _, e := range out.Errors {
				seen[e] = true
			}
			for _, e := range extra.Errors {
				if !seen[e] && len(out.Errors) < 48 {
					out.Errors = append(out.Errors, e)
					seen[e] = true
				}
			}
			out.mergeRequestIDs(extra)
			for k, v := range extra.Fields {
				if _, exists := out.Fields[k]; !exists {
					out.Fields[k] = v
				}
			}
		}
	}
	out.put("request_path", r.RequestPath, "logs.other.request_path", r.CreatedAt, "request")
	out.put("model", r.ModelName, "logs.model_name", r.CreatedAt, "request")
	out.put("group", r.Group, "logs.group", r.CreatedAt, "request")
	out.put("upstream_model", r.UpstreamModelName, "logs.other.upstream_model_name", r.CreatedAt, "request")
	if r.IsModelMapped && r.ModelName != "" && r.UpstreamModelName != "" {
		out.put("model_mapping", r.ModelName+" -> "+r.UpstreamModelName, "logs.model_name+logs.other.upstream_model_name", r.CreatedAt, "request")
	}
	out.put("error_code", r.UpstreamErrorCode, "logs.other.error_code", r.CreatedAt, "request")
	out.put("error_type", r.UpstreamErrorType, "logs.other.error_type", r.CreatedAt, "request")
	out.put("stream_end_reason", r.EndReason, "logs.other.stream_status", r.CreatedAt, "request")
	if r.UseTimeKnown {
		out.put("total_ms", strconv.FormatInt(r.UseTime*1000, 10), "logs.use_time", r.CreatedAt, "request")
	}
	if r.FirstByteMs > 0 {
		out.put("first_byte_ms", strconv.FormatInt(r.FirstByteMs, 10), "logs.other.frt", r.CreatedAt, "request")
	}
	out.addRequestID(r.RequestID, "platform", "logs.request_id", r.CreatedAt, out.value("attempt_id"), true)
	out.resolveRequestIDs(r.RequestID)
	return out
}

func logChainDiagnosticParties(label string) []string {
	var out []string
	if strings.Contains(label, "客户") {
		out = append(out, "downstream")
	}
	if strings.Contains(label, "平台") {
		out = append(out, "ours")
	}
	if strings.Contains(label, "上游") {
		out = append(out, "upstream")
	}
	if containsAny(strings.ToLower(label), "网络", "cdn", "网关") {
		out = append(out, "network")
	}
	if len(out) == 0 {
		out = append(out, "unknown")
	}
	return out
}

func logChainDiagnosticRecordAt(field string) string {
	switch field {
	case "actual_upstream_host", "forward_path", "model_mapping", "account_capability", "supported_endpoints", "supported_models", "original_account", "original_route", "account_id", "retry_attempt", "attempt_id", "route_config_error":
		return "NewAPI 转发/重试层：按 Request ID 和 attempt_id 记录请求发生时的 " + field + " 快照（去除凭据）"
	case "error_response", "error_code", "error_type", "error_param", "upstream_request_id", "location", "parameter_limits", "schema_constraints", "reset_at":
		return "NewAPI 上游响应适配层：按 Request ID 和 attempt_id 记录脱敏的 " + field + "；保留内部错误结构与上游请求标识"
	case "request_body_bytes", "request_body_limit", "parameter_values", "tool_pairing", "continuation_id", "input_tokens", "limit_tokens":
		return "客户端/入口校验层：记录 " + field + " 的诊断摘要与限制值；不记录提示词、工具正文或密钥"
	case "balance", "precharge", "limit", "limit_subject":
		return "计费/限流决策层：记录当次 " + field + "，并区分客户、平台账号与上游账号"
	case "request_length", "connect_ms", "first_byte_ms", "total_ms", "layer_statuses", "node":
		return "Nginx 与 NewAPI 各跳分别记录 " + field + "、层名和时间；request_length 是入口整包字节，不是请求体字节"
	default:
		return "NewAPI 请求日志 other 中按 Request ID 和 attempt_id 记录 " + field + " 及采集时间"
	}
}

func logChainDiagnose(r LogChainRow) *LogChainDiagnosis {
	if r.Type != 5 && len(r.AnomalyTags) == 0 {
		return nil
	}
	f := logChainRowDiagnosticFacts(r)
	var texts []string
	statuses := map[int]bool{}
	if r.UpstreamStatusCode > 0 {
		statuses[r.UpstreamStatusCode] = true
	}
	for _, m := range logChainDiagnosticStatus.FindAllStringSubmatch(r.Content, 8) {
		n, _ := strconv.Atoi(m[1])
		statuses[n] = true
	}
	for _, e := range f.Errors {
		if attempt := f.value("attempt_id"); attempt != "" && e.AttemptID != "" && attempt != e.AttemptID {
			continue
		}
		texts = append(texts, e.Message, e.Code, e.Type)
		if e.Status > 0 {
			statuses[e.Status] = true
		}
	}
	texts = append(texts, f.value("error_code"), f.value("error_type"))
	text := strings.ReplaceAll(strings.Join(texts, "\n"), "`", "")
	var best *logChainDiagnosticRule
	var captures []string
	score := -1
	for i := range logChainDiagnosticRules {
		rule := &logChainDiagnosticRules[i]
		if rule.Priority < 100 && len(statuses) > 0 && !statuses[rule.Status] {
			continue
		}
		match := rule.re.FindStringSubmatch(text)
		if match == nil {
			continue
		}
		s := rule.Priority*1000 + len(rule.Pattern)
		if statuses[rule.Status] {
			s += 500
		}
		if s > score {
			best, captures, score = rule, match, s
		}
	}
	// No catalogue match: retain the existing source-gated message/code path.
	if best == nil {
		for _, rule := range logChainFaultMessageRules {
			if rule.pattern.MatchString(r.Content) && (rule.requireUpstream == nil || *rule.requireUpstream == logChainFaultFromUpstream(r.Content)) {
				return nil
			}
		}
		return logChainDiagnosticSpecificCode(r, f)
	}
	// Specific catalogue semantics retain precedence. A generic wrapper must
	// not hide a reliable, same-attempt error code already understood by us.
	if !best.CauseKnown || best.Priority < 100 {
		if specific := logChainDiagnosticSpecificCode(r, f); specific != nil {
			return specific
		}
	}
	d := &LogChainDiagnosis{RuleID: best.ID, Version: best.Version, DocumentLines: append([]int(nil), best.DocumentLines...), Reason: best.Reason, CauseKnown: best.CauseKnown, Responsibility: best.Responsibility, ResponsibleParties: logChainDiagnosticParties(best.Responsibility), Action: best.Action, Profile: best.Profile, Variables: map[string]string{}, Missing: []LogChainDiagnosticMissing{}, ErrorLayer: "日志未明确记录错误产生层"}
	for i, key := range best.re.SubexpNames() {
		if i > 0 && key != "" && i < len(captures) && captures[i] != "" {
			value := captures[i]
			if best.ID == "LC-119" && key == "forward_path" {
				value = logChainDiagnosticActualPath(value)
			}
			if value != "" {
				d.Variables[key] = value
				f.put(key, value, "logs.error_text:"+best.ID, r.CreatedAt, "request")
			}
		}
	}
	if best.ID == "LC-119" {
		if path := d.Variables["forward_path"]; path != "" {
			d.Reason = "通道不支持 " + path
		}
	}
	if containsAny(text, "advisor_tool_result") && best.DocumentLines[0] == 7 {
		d.Reason = "消息块类型 advisor_tool_result 不在接口支持的类型集合内"
		d.Profile = "parameter"
	}
	if source := f.value("error_source"); source != "" {
		d.ErrorLayer = source
	} else if strings.HasPrefix(r.Content, "status_code=") {
		d.ErrorLayer = "NewAPI 记录的上游响应（不等同于责任方）"
	}
	if best.Profile == "platform" && f.value("error_source") == "" && !strings.HasPrefix(r.Content, "status_code=") {
		d.ErrorLayer = "平台网关/运行或计费服务"
	}
	if strings.Contains(strings.ToLower(text), "cloudflare") {
		d.ErrorLayer = "上游源站与 CDN 之间"
	}
	d.ActionOwners = append([]string(nil), d.ResponsibleParties...)
	if len(d.ActionOwners) == 1 && d.ActionOwners[0] == "unknown" {
		d.ActionOwners = []string{"ours"}
	}
	if containsAny(best.Action, "切流", "配置", "查该", "核对上游", "平台侧充值", "补账号", "扩容", "联系上游") {
		d.ActionOwners = []string{"ours"}
	}
	if strings.Contains(best.Action, "联系上游") {
		d.ActionOwners = append(d.ActionOwners, "upstream")
	}
	// Values come from this request, never the examples in the document.
	for _, key := range []string{"balance", "precharge", "resource_current", "resource_limit", "reset_at", "request_body_bytes", "error_param"} {
		if value := f.value(key); value != "" {
			d.Variables[key] = value
		}
	}
	if len(d.Variables) > 0 {
		keys := make([]string, 0, len(d.Variables))
		for k := range d.Variables {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		var vals []string
		for _, k := range keys {
			if best.ID == "LC-119" && k == "forward_path" {
				continue
			}
			vals = append(vals, k+"="+d.Variables[k])
		}
		if len(vals) > 0 {
			d.Reason += "（" + strings.Join(vals, "，") + "）"
		}
	}
	if best.Status == 404 && best.Priority < 100 {
		logChainRefine404(d, f)
	}
	d.Basis = fmt.Sprintf("清单 %s，原始行 %v，规则版本 %s；命中错误语义：%s", best.ID, best.DocumentLines, best.Version, logChainDiagnosticSafeText(captures[0], 400))
	for _, key := range best.RequiredFields {
		if key == "error_response" && len(f.Errors) > 0 && d.CauseKnown {
			continue
		}
		if f.value(key) != "" || (key == "upstream_request_id" && f.hasUpstreamIdentity()) {
			continue
		}
		reason := "当前请求日志及已关联证据未记录；不能用当前配置补作历史事实"
		if key == "upstream_request_id" {
			reason = f.identityMissingReason()
		}
		d.Missing = append(d.Missing, LogChainDiagnosticMissing{Field: key, Reason: reason, RecordAt: logChainDiagnosticRecordAt(key)})
	}
	return d
}

func logChainDiagnosticActualPath(raw string) string {
	raw = strings.TrimRight(raw, "\"'`.,;:)]}，。；：）】")
	u, err := url.Parse(raw)
	if err != nil || !strings.HasPrefix(raw, "/") || strings.HasPrefix(raw, "//") || u.Host != "" || u.Path == "" {
		return ""
	}
	return logChainDiagnosticSafeText(u.EscapedPath(), 1024)
}

func logChainRefine404(d *LogChainDiagnosis, f *LogChainDiagnosticFacts) {
	host, path := f.value("actual_upstream_host"), f.value("forward_path")
	if host == "" || path == "" {
		return
	}
	if issue := f.value("route_config_error"); issue != "" && issue != "false" {
		d.Reason = "实际转发地址配置错误：" + issue
		d.Responsibility = "平台侧"
		d.ResponsibleParties = []string{"ours"}
		d.CauseKnown = true
		d.ActionOwners = []string{"ours"}
		d.Action = "按请求时的实际上游主机和转发路径修正渠道地址或路径映射，再核验该端点"
		return
	}
	if endpoints := f.value("supported_endpoints"); endpoints != "" {
		found := false
		for _, p := range strings.FieldsFunc(endpoints, func(r rune) bool { return r == ',' || r == ';' || r == ' ' }) {
			if p == path {
				found = true
			}
		}
		if !found {
			d.Reason = "实际上游不支持本次转发接口：" + host + path
			d.Responsibility = "平台侧/上游"
			d.ResponsibleParties = []string{"ours", "upstream"}
			d.CauseKnown = true
			d.ActionOwners = []string{"ours"}
			d.Action = "将该请求路由到支持此端点的渠道/账号，或按已记录的能力列表修正接口路径"
		}
	}
	notFound := containsAny(strings.ToLower(f.value("error_code")), "model_not_found", "resource_not_found")
	for _, e := range f.Errors {
		notFound = notFound || containsAny(strings.ToLower(e.Code), "model_not_found", "resource_not_found")
	}
	if !d.CauseKnown && notFound {
		d.Reason = "实际上游未找到请求的模型或资源：" + host + path
		d.CauseKnown = true
		d.Responsibility = "待查"
		d.ResponsibleParties = []string{"unknown"}
		d.ActionOwners = []string{"ours", "upstream"}
		d.Action = "核对请求时的模型映射、资源标识与上游资源清单；原因已知，配置责任仍需核对"
	}
}

func logChainDiagnosisFault(d *LogChainDiagnosis) logChainFault {
	fault := strings.Join(d.ResponsibleParties, "+")
	confidence := faultConfHigh
	if d.Confidence != "" {
		confidence = d.Confidence
	}
	if !d.CauseKnown {
		confidence = faultConfNone
	}
	return logChainFault{Fault: fault, Confidence: confidence, Why: d.Basis, Reason: d.Reason, Diagnosis: d}
}

func logChainApplyFault(r *LogChainRow, f logChainFault) {
	r.Fault, r.FaultConfidence, r.FaultWhy, r.FaultReason = f.Fault, f.Confidence, f.Why, f.Reason
	r.Diagnosis = f.Diagnosis
	r.DiagnosticFacts = logChainRowDiagnosticFacts(*r)
	if r.Diagnosis != nil {
		// A row may retain the same diagnosis in the list and investigation
		// snapshots. Acquisition state belongs to this row only.
		diagnosis := *r.Diagnosis
		diagnosis.Missing = append([]LogChainDiagnosticMissing(nil), diagnosis.Missing...)
		r.Diagnosis = &diagnosis
		for i := range r.Diagnosis.Missing {
			gap := &r.Diagnosis.Missing[i]
			gap.Sources = logChainDiagnosticFieldSources(gap.Field)
			gap.SourceResults = nil
			gap.Status = "not_queried"
			gap.AvailableFrom = "现有日志先精确补查；源端未记录的字段须部署对应日志后，从新请求开始获取"
		}
	}
}

func logChainDiagnosticJoinKey(r LogChainRow) string {
	f := logChainRowDiagnosticFacts(r)
	if f.identityConflict() {
		return ""
	}
	keys := map[string]bool{}
	for _, id := range f.RequestIDs {
		if id.Owner == "platform" {
			continue
		}
		if id.Status != "bound" {
			return ""
		}
		keys[id.Value] = true
	}
	if len(keys) == 1 {
		for key := range keys {
			if f.identityNamespaceCollision(key) {
				return ""
			}
			return key
		}
	}
	return ""
}

func logChainWithExactUpstreamDiagnostic(r LogChainRow) LogChainRow {
	m := r.UpstreamMatch
	if m == nil || m.Confidence != correlateExact || m.JoinKey == "" || logChainDiagnosticJoinKey(r) != m.JoinKey {
		return r
	}
	f := logChainRowDiagnosticFacts(r)
	extra := logChainParseDiagnosticFacts(m.UpstreamContent, "", "upstream_error_logs", m.UpstreamCreatedAt)
	if a, b := f.value("attempt_id"), extra.value("attempt_id"); a != "" && b != "" && a != b {
		return r
	}
	if _, conflict := logChainDiagnosticIdentityRelation(f, extra); conflict {
		return r
	}
	if m.UpstreamErrorCode != "" {
		extra.Errors = append(extra.Errors, LogChainDiagnosticError{Code: m.UpstreamErrorCode, Type: m.UpstreamErrorType, Source: "upstream_error_logs", Path: "other.error_code", Origin: "upstream", ObservedAt: m.UpstreamCreatedAt, AttemptID: extra.value("attempt_id")})
	}
	seen := make(map[LogChainDiagnosticError]bool, len(f.Errors))
	for _, e := range f.Errors {
		seen[e] = true
	}
	for _, e := range extra.Errors {
		if seen[e] {
			continue
		}
		if len(f.Errors) >= 64 {
			f.Truncated = true
			break
		}
		f.Errors = append(f.Errors, e)
		seen[e] = true
	}
	for key, v := range extra.Fields {
		if old, ok := f.Fields[key]; !ok || (old.Scope == "current_config" && v.Scope == "request") {
			f.Fields[key] = v
		}
	}
	f.mergeRequestIDs(extra)
	// The relay's own request ID is not the provider ID used as JoinKey.
	f.addRequestID(m.UpstreamRequestID, "upstream_relay", "upstream_error_logs.request_id", m.UpstreamCreatedAt, f.value("attempt_id"), true)
	f.resolveRequestIDs(r.RequestID)
	r.DiagnosticFacts = f
	return r
}
