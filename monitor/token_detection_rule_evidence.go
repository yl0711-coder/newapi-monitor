package monitor

import (
	"strconv"
	"strings"
)

func tokenRuleOperands(in tokenDetectionInput, rule tokenDetectionRule, result tokenDetectionResult) map[string]string {
	values := map[string]string{}
	switch rule.Name {
	case "usage_format":
		for _, k := range []string{"prompt_tokens", "input_tokens_total", "completion_tokens", "quota", "cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h"} {
			f := in.Usage.Fields[k]
			values[k] = f.State + ":" + f.Value
		}
		if in.Usage.TotalInput != nil {
			values["normalized_total_input"] = strconv.FormatInt(*in.Usage.TotalInput, 10)
		}
	case "historical_price_difference":
		if result.ActualQuota != nil {
			values["actual_quota"] = *result.ActualQuota
		}
		if result.Price.Supported {
			values["expected_quota"] = result.Price.Quota
		}
		if result.FormulaDifference != nil {
			values["difference_quota"] = *result.FormulaDifference
		}
	default:
		if in.Usage.TotalInput != nil {
			values["actual_input"] = strconv.FormatInt(*in.Usage.TotalInput, 10)
		}
		if result.Reference.Upper != nil {
			values["reference_upper"] = strconv.FormatInt(*result.Reference.Upper, 10)
		}
		values["reference_samples"] = strconv.Itoa(result.Reference.SampleCount)
	}
	return values
}

type tokenRuleEvidenceGap struct {
	Rule   string `json:"rule"`
	Name   string `json:"name"`
	Reason string `json:"reason"`
	Source string `json:"source"`
	Status string `json:"status"`
	Action string `json:"action"`
}

func tokenRuleMissingEvidence(in tokenDetectionInput, rule tokenDetectionRule, ref tokenReference) []tokenRuleEvidenceGap {
	missing := []tokenRuleEvidenceGap{}
	add := func(name, why, source, status, action string) {
		missing = append(missing, tokenRuleEvidenceGap{Rule: rule.Name, Name: name, Reason: why, Source: source, Status: status, Action: action})
	}
	fields := func(names ...string) {
		for _, name := range names {
			f := tokenMeteringOperand(in.Usage, name)
			if !tokenMeteringFieldUsable(f) {
				source, status := "newapi.logs.other."+name, "source_not_recorded"
				if name == "prompt_tokens" || name == "completion_tokens" || name == "quota" || name == "is_stream" {
					source = "newapi.logs." + name
				}
				if f.State == "invalid" && strings.Contains(f.Reason, "not_supported") {
					status = "source_field_not_adapted"
				} else if f.State != "missing" && f.State != "" && f.State != "null" {
					status = "source_value_unusable"
				}
				action := "在请求完成的用量日志记录该字段及其计量语义；源端部署后仅新请求可取，历史请求不能用当前配置补填"
				if tokenMeteringNeedsSourceReplay(in.Fact, in.Usage) && (name == "expr_b64" || name == "request_rules" || name == "response_model") {
					status, action = "source_replay_required", "本次检测提交后自动进入已有源窗口重放；成功提交原始字段后按事实版本自动重检"
				}
				add(name, tokenMeteringMissingReason(f, "source_missing"), source, status, action)
			}
		}
	}
	switch rule.Name {
	case "usage_format":
		fields("prompt_tokens", "completion_tokens", "quota", "is_stream")
	case "historical_price_difference":
		if in.Usage.Adapter != "newapi-rc26" && in.Usage.Contract == nil {
			add("pricing_adapter", rule.Reason, "newapi.logs.other.pricing_adapter", "source_contract_unverified", "补记请求时的计价实现版本和生效时间，核验后适配；不依据当前配置推定")
		}
		fields("billing_source", "currency", "quota_per_unit", "rounding_mode", "effective_group_ratio", "billing_mode", "request_rules", "image", "is_task")
		fields("prompt_tokens", "completion_tokens", "quota")
		if in.Usage.TotalInput == nil {
			add("usage_semantic", in.Usage.SemanticReason, "newapi.logs.other.usage_semantic", "source_semantic_unverified", "补记请求发生时的输入、缓存包含关系及计量协议；按实际语义复算")
		}
		switch tokenMeteringOperand(in.Usage, "billing_mode").Value {
		case "ratio":
			if in.Usage.Adapter != "newapi-rc48" || in.Usage.Fields["model_price"].State != "sentinel" {
				fields("model_price")
			}
			fields("completion_ratio", "cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h")
			for _, pair := range [][2]string{{"cache_tokens", "cache_ratio"}, {"cache_creation_tokens", "cache_creation_ratio"}, {"cache_creation_tokens_5m", "cache_creation_ratio_5m"}, {"cache_creation_tokens_1h", "cache_creation_ratio_1h"}, {"cache_creation_tokens_12h", "cache_creation_ratio_12h"}} {
				count := tokenMeteringOperandInt(in.Usage, pair[0])
				if count == nil || *count > 0 {
					fields(pair[1])
				}
			}
			if f := in.Usage.Fields["effective_model_ratio"]; f.State == "missing" || f.State == "" {
				fields("model_ratio")
			} else {
				fields("effective_model_ratio")
			}
		case "tiered_expr":
			fields("expr_b64", "matched_tier", "cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h")
		}
	case "input_inflation", "cache_structure":
		f := in.Fact
		if f.LinkState != "unique" || f.EvidenceLevel != "exact" {
			add("exact_entry_evidence", f.LinkReason, "nginx access request_id/event_id", "association_"+f.LinkState, "后台按精确请求身份补关联；不使用附近请求")
		}
		if f.RequestLength == nil || f.LengthState != "valid" {
			status, action := "awaiting_exact_entry_evidence", "后台按精确请求身份补关联入口整包字节数；关联版本更新后自动重检，不换算为真实 Token"
			if f.LinkState == "unique" {
				status, action = "source_not_recorded_or_unusable", "入口证据已唯一关联，但大小未记录或不可用；入口日志补记 request_length、字段版本及时间，部署后仅新请求可取"
			}
			add("request_length", f.LengthState, "nginx access request_length", status, action)
		}
		if in.Usage.TotalInput == nil {
			add("input_tokens", in.Usage.SemanticReason, "newapi.logs usage_semantic + raw usage", "source_semantic_unverified", "记录计量协议和缓存是否包含在输入中；不能按模型名猜测")
		}
		if !tokenMeteringFieldUsable(in.Usage.Fields["request_path"]) && f.RequestPath == "" {
			fields("request_path")
		}
		fields("is_stream")
		if rule.Name == "cache_structure" {
			fields("cache_tokens", "cache_creation_tokens")
			if in.Usage.Adapter == "newapi-rc48" && tokenMeteringOperand(in.Usage, "billing_mode").Value == "tiered_expr" {
				fields("expr_b64", "matched_tier", "request_rules")
			} else {
				for _, pair := range [][2]string{{"cache_tokens", "cache_ratio"}, {"cache_creation_tokens", "cache_creation_ratio"}, {"cache_creation_tokens_5m", "cache_creation_ratio_5m"}, {"cache_creation_tokens_1h", "cache_creation_ratio_1h"}, {"cache_creation_tokens_12h", "cache_creation_ratio_12h"}} {
					if count := tokenMeteringOperandInt(in.Usage, pair[0]); count == nil || *count > 0 {
						fields(pair[1])
					}
				}
			}
		}
		if ref.Status != "statistical" {
			status := "comparable_samples_unavailable"
			if len(ref.MissingComparable) > 0 {
				status = "comparable_fields_missing"
			}
			add("comparable_reference", ref.Reason, "local retained comparable facts", status, "按已保存的排除原因补齐字段或等待同客户、模型、计量语义及协议的合格样本；不能合并不同转换凑样本")
			for _, name := range ref.MissingComparable {
				add(name, "comparable_field_missing", "source request + exact entry evidence", "needs_evidence", "补齐对应请求的来源字段后自动重检；原日志未记录时保留缺口")
			}
		}
	}
	if len(missing) == 0 {
		add(rule.Name, rule.Reason, "recorded rule operands", "unsupported_or_unconfigured", "核对该规则标出的限制或缺失项，保留其他规则已完成的结论")
	}
	return missing
}
