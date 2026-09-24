package monitor

import (
	"regexp"
	"strconv"
	"strings"
)

var (
	// NewAPI 日志前缀有两种真实形态（2026-09-15 生产实测）：
	//
	//   [ERR] 2026/09/14 - 13:55:20 | <RequestID> | user 102 | No available channel...
	//   [GIN] 2026/09/14 - 13:55:20 | relay | <RequestID> | 503 | 5.8ms | <IP> | POST /v1/responses
	//
	// GIN 行第二段是固定标签 relay 而不是 Request ID。原来只取第二段，
	// 会把 relay 当成 ID，字符集校验后整行判为解析失败——实测该请求两条日志
	// 里有一条就这样丢了，页面上表现为"有事件但格式无法识别"。
	//
	// 因此改为在前若干段里找**形如 Request ID 的那一段**：纯字母数字且长度 >= 16。
	// 不放宽到任意非空段：那会把 relay、503、IP 也当成候选 ID。
	cwNewAPIPipeContext      = regexp.MustCompile(`^\[[A-Za-z]+\]\s+\d{4}/\d{2}/\d{2}\s+-\s+\d{2}:\d{2}:\d{2}\s+\|(.*)$`)
	cwNewAPITimestamp        = regexp.MustCompile(`^\[[A-Za-z]+\]\s+(\d{4}/\d{2}/\d{2}\s+-\s+\d{2}:\d{2}:\d{2})\b`)
	cwNewAPIRequestIDSeg     = regexp.MustCompile(`^[A-Za-z0-9]{16,128}$`)
	cwNewAPIUserSeg          = regexp.MustCompile(`(?i)^(?:user|用户)\s*[:=：]?\s*([0-9]{1,18})$`)
	cwNewAPIUserKV           = regexp.MustCompile(`(?i)(?:^|[\s|,，;；])(?:user(?:[_ ]?id)?|uid)\s*[:=：]?\s*([0-9]{1,18})(?:$|[\s|,，;；])`)
	cwNoChannelContext       = regexp.MustCompile(`(?i)No\s+(?:available\s+channel(?:s)?|channel\s+available)\s+for\s+(?:the\s+)?model\s+([^\s|,，;；()（）]+)\s+(?:under|in)\s+(?:the\s+)?(?:current\s+)?group\s+([^\s|,，;；()（）]+)`)
	cwNoChannelContextAlt    = regexp.MustCompile(`(?i)No\s+(?:available\s+channel(?:s)?|channel\s+available)\s+(?:under|in)\s+(?:the\s+)?(?:current\s+)?group\s+([^\s|,，;；()（）]+)\s+for\s+(?:the\s+)?model\s+([^\s|,，;；()（）]+)`)
	cwNoChannelContextZH     = regexp.MustCompile(`分组\s*(.+?)\s*下(?:的)?模型\s*(.+?)\s*(?:无|没有)可用(?:的)?渠道`)
	cwNoChannelContextZHAlt  = regexp.MustCompile(`模型\s*(.+?)\s*(?:在|于)\s*分组\s*(.+?)\s*(?:下|中)\s*(?:无|没有)可用(?:的)?渠道`)
	cwModelForbiddenContext  = regexp.MustCompile(`(?i)(?:token\s+model\s+forbidden|model\s+forbidden|model\s+is\s+not\s+allowed|model\s+not\s+allowed|无权访问模型|无权限访问模型|模型无权限|模型权限不足)\s*[:：]?\s*([^\s|,，;；()（）]+)`)
	cwModelForbiddenAfter    = regexp.MustCompile(`(?i)\bmodel\s+([^\s|,，;；()（）]+)\s+(?:is\s+)?(?:forbidden|not\s+allowed)\b`)
	cwModelForbiddenAfterZH  = regexp.MustCompile(`模型\s*([^\s|,，;；()（）]+)\s*(?:无权限|无权访问|权限不足)`)
	cwModelNotFoundContext   = regexp.MustCompile(`(?i)(?:model\s+not\s+found|model\s+does\s+not\s+exist|unknown\s+model|模型不存在|模型未找到|找不到模型|未知模型)\s*[:：]?\s*([^\s|,，;；()（）]+)`)
	cwModelNotFoundAfter     = regexp.MustCompile(`(?i)\bmodel\s+([^\s|,，;；()（）]+)\s+(?:is\s+)?(?:not\s+found|does\s+not\s+exist)\b`)
	cwModelNotFoundAfterZH   = regexp.MustCompile(`模型\s*([^\s|,，;；()（）]+)\s*(?:不存在|未找到|找不到)`)
	cwUpstream5xx            = regexp.MustCompile(`(?i)(?:(?:status[_ ]?code|http[_ ]?status|response\s+status(?:\s+code)?)\s*[:=]\s*|upstream[^0-9]{0,24})(5[0-9]{2})\b`)
	cwPreRouteUpstreamMarker = regexp.MustCompile(cloudWatchPreRouteUpstreamMarkerPattern)
)

var cwNewAPIRequestCompletedRule = cwNewAPIClassRule{category: "request_completed", summary: "NewAPI 记录到该请求已处理（GIN 访问日志）"}

type cwNewAPIClassRule struct {
	category string
	fault    string
	summary  string
	all      []string
	any      []string
}

var cwNewAPIClassRules = []cwNewAPIClassRule{
	{category: "route_no_channel", fault: "route_no_channel", summary: "NewAPI 没有可用渠道", any: []string{"no available channel", "no available channels", "without available channel", "no channel available", "无可用渠道", "没有可用渠道", "无可用的渠道", "没有可用的渠道"}},
	{category: "invalid_token", fault: "auth_quota_account", summary: "NewAPI 拒绝了无效凭证", any: []string{"invalid token", "token is invalid", "token invalid", "unauthorized token", "无效令牌", "无效的令牌", "令牌无效"}},
	{category: "token_disabled", fault: "auth_quota_account", summary: "NewAPI 拒绝了已禁用凭证", any: []string{"token is disabled", "token disabled", "令牌已禁用", "令牌已被禁用", "令牌被禁用"}},
	{category: "model_forbidden", fault: "model_not_found", summary: "NewAPI 拒绝了无权限模型", any: []string{"token model forbidden", "model forbidden", "model is not allowed", "model not allowed", "无权访问模型", "无权限访问模型", "模型无权限", "模型权限不足"}},
	{category: "model_not_found", fault: "model_not_found", summary: "NewAPI 未找到请求模型", any: []string{"model not found", "model does not exist", "unknown model", "模型不存在", "模型未找到", "找不到模型", "未知模型"}},
	{category: "quota_account", fault: "auth_quota_account", summary: "NewAPI 因账户或额度拒绝请求", any: []string{"quota insufficient", "insufficient quota", "insufficient user quota", "insufficient token quota", "user quota insufficient", "user quota is insufficient", "user quota is not enough", "token quota insufficient", "token quota is insufficient", "token quota is not enough", "quota is insufficient", "quota is not enough", "not enough quota", "pre consume failed", "pre-consume failed", "pre consume quota failed", "pre-consume quota failed", "too little quota", "用户额度不足", "账户额度不足", "余额不足", "额度不足", "令牌额度不足", "预扣费失败", "预扣费额度失败"}},
	{category: "rate_limited", fault: "rate_limit_capacity", summary: "NewAPI 记录到限流或容量不足", any: []string{"rate limit", "rate_limit", "rate limited", "too many requests", "concurrency limited", "限流", "请求过于频繁", "超过速率限制", "并发限制", "并发数超限"}},
	{category: "database_error", fault: "unknown", summary: "NewAPI 记录到数据库异常", any: []string{"database error", "sql error", "too many connections", "deadlock", "server has gone away", "lost connection to mysql"}},
	{category: "runtime_panic", fault: "unknown", summary: "NewAPI 记录到运行时 panic", any: []string{"panic:", "panic recovered", "runtime panic"}},
	{category: "runtime_fatal", fault: "unknown", summary: "NewAPI 记录到 fatal 异常", any: []string{"fatal:", "fatal error"}},
	{category: "client_gone", fault: "client_gone", summary: "NewAPI 记录到连接提前断开", any: []string{"broken pipe", "client_gone", "client gone"}},
	// ★ 必须排在 transport_timeout 之前 ★
	// 生产原文是 "timeout waiting for goroutines to exit"，含 timeout 字样。
	// 若让 transport_timeout 先命中，我方进程内的收尾等待会被显示成"上游超时"，
	// 排障据此会去查上游渠道，而问题根本不在那里。
	{category: "worker_drain_timeout", fault: "", summary: "NewAPI 记录到协程收尾等待超时", any: []string{"waiting for goroutines"}},
	{category: "transport_timeout", fault: "transport_timeout", summary: "NewAPI 记录到处理超时", any: []string{"context deadline exceeded", "i/o timeout", "request timeout", "upstream timeout", "timed out"}},
	{category: "context_cancelled", fault: "unknown", summary: "NewAPI 记录到上下文取消", any: []string{"context canceled", "context cancelled"}},
	// scanner error 是 new-api 写下的流读取故障，既有排障口径已把它算作流故障
	// （见 logchain.go 的 end_reason 判据）。这里保持同一语义，不另立一套。
	{category: "stream_error", fault: "unknown", summary: "NewAPI 记录到流读取故障", any: []string{"scanner error"}},
	// 上游未返回可计费用量。只陈述事实，不升格成确定根因——
	// 既有 logchain_fault.go 对同一情形也刻意保持待判。
	{category: "billing_anomaly", fault: "billing_anomaly", summary: "NewAPI 记录到用量为 0，无法计费", any: []string{"total tokens is 0"}},
	// ★ 以下是"请求已到达应用"的正常事实，不是错误 ★
	// 2026-09-15 用生产 new-api/ 日志实测 300 条：236 条带 Request ID，但按
	// 「只认错误短语」的旧规则一条都识别不了。后果不是少显示几条，而是按需查询
	// 会把整个来源标成"格式无法识别 → 数据源不可用"，于是排障最需要回答的
	// 「这个请求到底到没到应用」反而答不了，还会被读成数据源故障。
	//
	// 这些行 FaultClass 必须留空：它们是事实，不是故障，给了归因会凭空造出责任方。
	{category: "request_completed", fault: "", summary: "NewAPI 记录到该请求已处理（GIN 访问日志）", any: []string{"[gin]"}},
	{category: "stream_ended", fault: "", summary: "NewAPI 记录到流式响应结束", any: []string{"stream ended"}},
	{category: "billing_recorded", fault: "", summary: "NewAPI 记录到用量与计费", any: []string{"record consume log", "预扣费", "补扣费"}},
	{category: "quota_precheck", fault: "", summary: "NewAPI 完成额度预检", any: []string{"额度充足", "funding=", "预扣费"}},
}

func (p *cloudWatchEvidenceParser) parseNewAPI(in cloudWatchEvidenceInput) (cloudWatchStructuredEvidence, error) {
	message := strings.TrimSpace(in.Message)
	if in.Fields != nil {
		message = cwField(in.Fields, "@message", "message")
	}
	if message == "" {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	lower := strings.ToLower(message)
	// GIN is an access record, not an application error. It can echo a
	// status_code/429/rate_limit token; prioritize this fact before the generic
	// upstream-5xx fallback so an access row cannot be mislabeled as an error.
	trimmedLower := strings.TrimSpace(lower)
	rule, ok := cwNewAPIClassRule{}, false
	if strings.HasPrefix(trimmedLower, "[gin]") {
		rule, ok = cwNewAPIRequestCompletedRule, true
	} else {
		rule, ok = cwMatchNewAPIRule(lower)
		if !ok && cwUpstream5xx.MatchString(message) {
			rule, ok = cwNewAPIClassRule{category: "upstream_5xx", fault: "upstream_5xx", summary: "NewAPI 记录到上游 5xx"}, true
		}
	}
	if !ok {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseUnsupported, in.Source)
	}
	// The same new-api log stream contains both pre-route decisions and
	// responses copied from a selected upstream.  A phrase such as
	// "No available channel" or "rate limit" is not sufficient by itself:
	// production upstream rows may carry it after `status_code=` or an
	// explicit provider/upstream marker.  Keep those rows as evidence, but put
	// them outside the closed rejection category set so the pre-route sampler
	// cannot count them as requests rejected before routing.
	rule = cwGuardPreRouteRuleAgainstUpstream(rule, lower, message)
	eventMS := in.TimestampMS
	if eventMS <= 0 && in.Fields != nil {
		if at, valid := parseCloudWatchResultTime(cwField(in.Fields, "@timestamp")); valid {
			eventMS = at.UnixMilli()
		}
	}
	if eventMS <= 0 {
		if match := cwNewAPITimestamp.FindStringSubmatch(message); len(match) == 2 {
			if parsed, valid := cwParseLogTime(match[1], "2006/01/02 - 15:04:05"); valid {
				eventMS = parsed
			}
		}
	}
	if eventMS <= 0 {
		return cloudWatchStructuredEvidence{}, newCloudWatchEvidenceParseError(cwParseMalformed, in.Source)
	}
	out := p.base(in)
	out.EventMS = eventMS
	out.Kind = cwEvidenceNewAPIError
	if in.Source == cwSourceMaster {
		out.Kind = cwEvidenceMasterError
	}
	out.Category, out.FaultClass, out.Summary = rule.category, rule.fault, rule.summary
	if err := p.addNewAPIContext(&out, message, in.Fields); err != nil {
		return cloudWatchStructuredEvidence{}, err
	}
	return out, nil
}

func cwMatchNewAPIRule(lower string) (cwNewAPIClassRule, bool) {
	for _, rule := range cwNewAPIClassRules {
		matched := true
		for _, fragment := range rule.all {
			if !strings.Contains(lower, fragment) {
				matched = false
				break
			}
		}
		if !matched {
			continue
		}
		if len(rule.any) > 0 {
			matched = false
			for _, fragment := range rule.any {
				if strings.Contains(lower, fragment) {
					matched = true
					break
				}
			}
		}
		if matched {
			return rule, true
		}
	}
	return cwNewAPIClassRule{}, false
}

func cwIsPreRouteClass(category string) bool {
	switch category {
	case "route_no_channel", "invalid_token", "token_disabled", "model_forbidden", "model_not_found", "quota_account", "rate_limited":
		return true
	default:
		return false
	}
}

func cwGuardPreRouteRuleAgainstUpstream(rule cwNewAPIClassRule, lower, message string) cwNewAPIClassRule {
	if !cwIsPreRouteClass(rule.category) {
		return rule
	}
	// GIN access rows can echo arbitrary query/error text (including
	// "rate_limit"), but they are proof that the request reached the
	// application rather than a pre-route rejection.  The fixed query filters
	// these rows too; keep the parser fail-closed when called independently.
	if strings.HasPrefix(strings.TrimSpace(lower), "[gin]") {
		return cwNewAPIClassRule{category: "request_completed", summary: "NewAPI 记录到该请求已处理（GIN 访问日志）"}
	}
	if !cwPreRouteUpstreamMarker.MatchString(lower) {
		return rule
	}
	if cwUpstream5xx.MatchString(message) {
		return cwNewAPIClassRule{category: "upstream_5xx", fault: "upstream_5xx", summary: "NewAPI 记录到上游 5xx"}
	}
	return cwNewAPIClassRule{category: "upstream_error", fault: "unknown", summary: "NewAPI 记录到上游错误"}
}

func (p *cloudWatchEvidenceParser) addNewAPIContext(out *cloudWatchStructuredEvidence, message string, fields map[string]string) error {
	requestID := cwField(fields, "request_id", "oneapi_request_id")
	if match := cwNewAPIPipeContext.FindStringSubmatch(message); len(match) == 2 {
		// 只扫描前 4 段：Request ID 与 user 段都在日志头部，再往后是耗时、IP、
		// 路径和自由文本，继续扫会把它们误当成标识。
		segments := strings.Split(match[1], "|")
		if len(segments) > 4 {
			segments = segments[:4]
		}
		for _, segment := range segments {
			segment = strings.TrimSpace(segment)
			if requestID == "" && cwNewAPIRequestIDSeg.MatchString(segment) {
				requestID = segment
				continue
			}
			if out.UserID == nil {
				if userMatch := cwNewAPIUserSeg.FindStringSubmatch(segment); len(userMatch) == 2 {
					userID, err := strconv.ParseInt(userMatch[1], 10, 64)
					if err != nil || userID < 0 {
						return newCloudWatchEvidenceParseError(cwParseMalformed, out.Source)
					}
					// 前置鉴权失败发生在用户身份建立之前，生产日志会明确写
					// "user 0"。0 表示未知用户，不是损坏数据；保留 nil 可避免
					// 把拒绝错误归因给一个并不存在的客户。
					if userID > 0 {
						out.UserID = cwInt64Pointer(userID)
					}
				}
			}
		}
	}
	var ok bool
	out.OneAPIIDHMAC, ok = p.optionalOpaqueHMAC("oneapi-request-id", requestID)
	if !ok {
		return newCloudWatchEvidenceParseError(cwParseMalformed, out.Source)
	}
	if out.UserID == nil {
		if userMatch := cwNewAPIUserKV.FindStringSubmatch(message); len(userMatch) == 2 {
			userID, err := strconv.ParseInt(userMatch[1], 10, 64)
			if err != nil || userID < 0 {
				return newCloudWatchEvidenceParseError(cwParseMalformed, out.Source)
			}
			if userID > 0 {
				out.UserID = cwInt64Pointer(userID)
			}
		}
	}
	if out.UserID == nil {
		rawUserID := cwField(fields, "user_id", "userId", "uid")
		if rawUserID != "" {
			userID, present, err := cwParseInt64(rawUserID, 0, 1<<62)
			if err != nil {
				return newCloudWatchEvidenceParseError(cwParseMalformed, out.Source)
			}
			// Insights can materialize an absent structured field as "-".
			// That is a missing identity, not a malformed event; only a
			// present positive value can be associated with a customer.
			if present && userID > 0 {
				out.UserID = cwInt64Pointer(userID)
			}
		}
	}
	model, group := cwField(fields, "model", "model_name"), cwField(fields, "group", "grp")
	if parsedModel, parsedGroup, matched := cwNewAPINoChannelContext(message); matched {
		model, group = parsedModel, parsedGroup
	} else if parsedModel := cwNewAPIModelContext(message); parsedModel != "" {
		// Error text is the authoritative request context when fields are
		// absent (the usual CloudWatch Insights shape for these rows).  Keep a
		// structured field only as a fallback for formats that omit the model
		// from the message.
		model = parsedModel
	}
	out.Model, ok = cwBusinessLabel(model, 128)
	if !ok {
		return newCloudWatchEvidenceParseError(cwParseMalformed, out.Source)
	}
	out.Group, ok = cwBusinessLabel(group, 64)
	if !ok {
		return newCloudWatchEvidenceParseError(cwParseMalformed, out.Source)
	}
	return nil
}

func cwNewAPINoChannelContext(message string) (model, group string, ok bool) {
	if match := cwNoChannelContext.FindStringSubmatch(message); len(match) == 3 {
		return strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), true
	}
	if match := cwNoChannelContextAlt.FindStringSubmatch(message); len(match) == 3 {
		// Some NewAPI builds put the group before the model in the English
		// sentence: "... under group G for model M".  Keep the return order
		// stable for the caller (model, group).
		return strings.TrimSpace(match[2]), strings.TrimSpace(match[1]), true
	}
	if match := cwNoChannelContextZH.FindStringSubmatch(message); len(match) == 3 {
		// Chinese production form is "分组 G 下模型 M 无可用渠道" (group
		// comes before model, unlike the English form).
		return strings.TrimSpace(match[2]), strings.TrimSpace(match[1]), true
	}
	if match := cwNoChannelContextZHAlt.FindStringSubmatch(message); len(match) == 3 {
		// The other Chinese form starts with the model: "模型 M 在分组 G
		// 下无可用渠道".
		return strings.TrimSpace(match[1]), strings.TrimSpace(match[2]), true
	}
	return "", "", false
}

func cwNewAPIModelContext(message string) string {
	for _, pattern := range []*regexp.Regexp{
		cwModelForbiddenContext, cwModelForbiddenAfter, cwModelForbiddenAfterZH,
		cwModelNotFoundContext, cwModelNotFoundAfter, cwModelNotFoundAfterZH,
	} {
		if match := pattern.FindStringSubmatch(message); len(match) == 2 {
			return strings.TrimSpace(match[1])
		}
	}
	return ""
}
