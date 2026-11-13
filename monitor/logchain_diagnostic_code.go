package monitor

import "strings"

// This adapter is used only by customer troubleshooting. The shared code map
// and SQL stability classification retain their existing semantics and gates.
func logChainDiagnosticSpecificCode(r LogChainRow, f *LogChainDiagnosticFacts) *LogChainDiagnosis {
	if f.Truncated {
		return nil
	}
	attempt := f.value("attempt_id")
	attempts := map[string]bool{}
	for _, e := range f.Errors {
		if e.AttemptID != "" {
			attempts[e.AttemptID] = true
		}
	}
	if attempt == "" && len(attempts) > 1 {
		return nil
	}
	codes := map[string]string{}
	add := func(code, source, origin, recordedAttempt string) {
		code = strings.ToLower(strings.TrimSpace(code))
		if _, ok := logChainFaultByErrorCode[code]; !ok {
			return
		}
		if origin != "upstream" && origin != "upstream_relay" && origin != "model_provider" {
			return
		}
		if attempt != "" && recordedAttempt != "" && attempt != recordedAttempt {
			return
		}
		if len(attempts) > 1 && recordedAttempt == "" {
			return
		}
		codes[code] = source
	}
	if logChainFaultFromUpstream(r.Content) {
		origin := logChainDiagnosticIDOwner(f.value("error_source"))
		if origin == "unknown" {
			origin = "upstream"
		}
		add(r.UpstreamErrorCode, "logs.other.error_code", origin, attempt)
	}
	for _, e := range f.Errors {
		add(e.Code, e.Source+":"+e.Path, e.Origin, e.AttemptID)
	}
	if len(codes) != 1 {
		return nil
	}
	for code, source := range codes {
		rule := logChainFaultByErrorCode[code]
		party := map[string]string{faultOurs: "平台侧", faultUpstream: "上游侧", faultDownstream: "客户侧"}[rule.fault]
		d := &LogChainDiagnosis{RuleID: "LC-CODE-" + code, Version: "2026-11-08.2", Reason: rule.why,
			CauseKnown: true, Confidence: rule.confidence, Responsibility: party, ResponsibleParties: []string{rule.fault},
			ErrorLayer: "本次请求的上游错误响应（不等同于责任方）", Profile: "upstream",
			Action:       "平台人员按本次请求的路由和尝试记录向上游核查，保留错误响应与分层耗时",
			ActionOwners: []string{faultOurs, faultUpstream}, Variables: map[string]string{"error_code": code},
			Basis:   rule.why + "；来源：" + source + "；已通过上游来源及请求尝试校验；具体错误码优先于通用包装",
			Missing: []LogChainDiagnosticMissing{}}
		if reason := logChainHumanErrorCodeReason(code, rule.fault); reason != "" {
			d.Reason = reason
		}
		required := []string{"upstream_request_id", "attempt_id", "actual_upstream_host"}
		switch rule.fault {
		case faultDownstream:
			d.Profile, d.Action, d.ActionOwners = "parameter", "客户按接口约束修正报错参数；平台人员补记字段路径、当次值及限制值以便定位", []string{faultDownstream, faultOurs}
			required = append(required, "error_param", "parameter_limits")
		case faultOurs:
			d.Action, d.ActionOwners = "平台人员核查请求发生时的上游账号额度及限制，按记录充值、降并发或申请提额", []string{faultOurs}
			required = append(required, "account_id", "limit_subject", "balance", "limit")
		}
		switch code {
		case "array_above_max_length":
			d.Reason = "客户请求中的数组长度超限"
		case "upstream_http2_stream_error":
			d.Reason = "上游 HTTP/2 流错误"
		}
		for _, key := range []string{"error_param", "parameter_limits"} {
			if v := f.value(key); v != "" {
				d.Variables[key] = v
				d.Reason += "（" + key + "=" + v + "）"
			}
		}
		for _, key := range required {
			if f.value(key) != "" || (key == "upstream_request_id" && f.hasUpstreamIdentity()) {
				continue
			}
			reason := "当前请求日志及已关联证据未记录；用于进一步定位，不撤销已明确原因"
			if key == "upstream_request_id" {
				reason = f.identityMissingReason()
			}
			d.Missing = append(d.Missing, LogChainDiagnosticMissing{Field: key, Reason: reason, RecordAt: logChainDiagnosticRecordAt(key)})
		}
		return d
	}
	return nil
}
