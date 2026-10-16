package monitor

import (
	"fmt"
	"math/big"
)

type portalBillingCheck struct {
	baseUSD  *big.Rat // internal exact subtotal before group ratio; never serialized
	State    string   `json:"state"` // matched / incomplete / unsupported / mismatch
	Message  string   `json:"message"`
	Formula  string   `json:"formula,omitempty"`
	Readable string   `json:"readable,omitempty"`
}

// This is a bounded explanation, not a billing engine. Only plain-text and
// per-call text logs and a restricted linear tier grammar are supported.
// The integer quota from the source is authoritative and is never modified.
func checkPortalBilling(r LogRow, e portalLogEvidence) portalBillingCheck {
	unavailable := func(state, message string) portalBillingCheck {
		return portalBillingCheck{State: state, Message: message}
	}
	if !r.billingFactsValid || r.billingQuota < 0 || r.PromptTokens < 0 || r.CompletionTokens < 0 || e.State != portalEvidenceValid {
		return unavailable("incomplete", "计费原始记录不足，暂不能复核公式；费用仍以实际扣费为准。")
	}
	if e.Mode == "tiered_expr" {
		return checkPortalTierBilling(r, e)
	}
	if e.Mode != "ratio" && e.Mode != "per_request" {
		return unavailable("unsupported", "此计费类型的完整公式尚未支持；费用以实际扣费为准。")
	}
	source := e.Strings["billing_source"]
	if e.ExtraBilling || e.ToolsState != portalEvidenceMissing || (source.State != portalEvidenceMissing && (source.State != portalEvidenceValid || source.Text != "wallet")) || e.Strings["task_id"].State != portalEvidenceMissing {
		return unavailable("unsupported", "日志包含附加计费或结算信息，暂不套用普通文本公式。")
	}
	for _, key := range []string{"image", "audio", "ws", "is_task", "web_search", "file_search", portalAudioInputSeparatePriceKey, "image_generation_call"} {
		f := e.Flags[key]
		if f.State != portalEvidenceMissing && (f.State != portalEvidenceValid || f.Text != "false") {
			return unavailable("unsupported", "复杂计费记录暂不套用普通文本公式。")
		}
	}
	for _, key := range []string{"audio_input", "audio_output", "text_input", "text_output", "image_output", "web_search_call_count", "file_search_call_count", "audio_input_token_count", "image_generation_call_count"} {
		f := e.Numbers[key]
		if f.State == portalEvidenceMissing {
			continue
		}
		v, ok := new(big.Rat).SetString(f.Text)
		if f.State != portalEvidenceValid || !ok || v.Sign() != 0 {
			return unavailable("unsupported", "缓存、工具或多模态分项尚需独立核验，暂不展示简化公式。")
		}
	}
	for _, key := range []string{"web_search_price", "file_search_price", "audio_input_price", "image_generation_call_price"} {
		if e.Numbers[key].State != portalEvidenceMissing {
			return unavailable("unsupported", "附加单价记录尚需独立核验，暂不展示简化公式。")
		}
	}
	path := e.Strings["request_path"]
	if path.State != portalEvidenceMissing && (path.State != portalEvidenceValid || (path.Text != "/v1/chat/completions" && path.Text != "/v1/responses" && path.Text != "/v1/messages")) {
		return unavailable("unsupported", "该请求类型尚未纳入公式核验。")
	}
	if e.Mode == "per_request" && path.State != portalEvidenceValid {
		return unavailable("incomplete", "按次记录缺少文本请求类型依据，暂不复核公式。")
	}
	if r.PromptTokens == 0 && r.CompletionTokens == 0 {
		return unavailable("incomplete", "零 Token 记录需结合额外结算依据，暂不复核公式。")
	}
	get := func(key string) *big.Rat {
		f := e.Numbers[key]
		if f.State != portalEvidenceValid {
			return nil
		}
		v, ok := new(big.Rat).SetString(f.Text)
		if !ok || v.Sign() < 0 {
			return nil
		}
		return v
	}
	model, group := get("model_ratio"), get("group_ratio")
	if model == nil || group == nil {
		return unavailable("incomplete", "日志缺少有效模型倍率或分组倍率，暂不复核公式。")
	}
	u := e.Numbers["user_group_ratio"]
	if u.State != portalEvidenceMissing {
		v, ok := new(big.Rat).SetString(u.Text)
		if u.State != portalEvidenceValid || !ok || (v.Cmp(big.NewRat(-1, 1)) != 0 && v.Cmp(group) != 0) {
			return unavailable("incomplete", "个人倍率与实际分组倍率的关系待核验，不叠乘或猜测历史倍率。")
		}
	}
	var amount *big.Rat
	var expression string
	var displayTerms []portalBillingTerm
	if e.Mode == "per_request" {
		if portalNeedsCacheRatioCheck(e) {
			return unavailable("unsupported", "按次记录包含缓存分项，暂不套用普通按次公式。")
		}
		price := get("model_price")
		if price == nil {
			return unavailable("incomplete", "日志缺少有效按次单价。")
		}
		amount = new(big.Rat).Mul(price, big.NewRat(500000, 1))
		amount.Mul(amount, group)
		displayTerms = []portalBillingTerm{{label: "本次调用", count: big.NewRat(1, 1), price: price, perCall: true}}
		expression = fmt.Sprintf("按次单价 $%s × 分组倍率 %s × 500000", e.Numbers["model_price"].Text, e.Numbers["group_ratio"].Text)
	} else {
		completion := get("completion_ratio")
		if completion == nil {
			return unavailable("incomplete", "日志缺少输出倍率，暂不复核公式。")
		}
		amount = new(big.Rat).Mul(big.NewRat(r.CompletionTokens, 1), completion)
		amount.Add(amount, big.NewRat(r.PromptTokens, 1)).Mul(amount, model).Mul(amount, group)
		inputPrice := new(big.Rat).Mul(model, big.NewRat(2, 1))
		displayTerms = []portalBillingTerm{{label: "普通输入", count: big.NewRat(r.PromptTokens, 1), price: inputPrice}, {label: "输出", count: big.NewRat(r.CompletionTokens, 1), price: new(big.Rat).Mul(inputPrice, completion)}}
		expression = fmt.Sprintf("（输入 %d + 输出 %d × 输出倍率 %s）× 模型倍率 %s × 分组倍率 %s", r.PromptTokens, r.CompletionTokens, e.Numbers["completion_ratio"].Text, e.Numbers["model_ratio"].Text, e.Numbers["group_ratio"].Text)
		if portalNeedsCacheRatioCheck(e) {
			displayTerms = nil
			weighted, cacheExpression, failure := portalCachedRatioTokens(r, e, &displayTerms)
			if failure != nil {
				return *failure
			}
			amount = weighted.Mul(weighted, model).Mul(weighted, group)
			expression = cacheExpression + fmt.Sprintf(" × 模型倍率 %s × 分组倍率 %s", e.Numbers["model_ratio"].Text, e.Numbers["group_ratio"].Text)
		}
	}
	// rc26 rounds nonnegative decimal quota half away from zero, then applies
	// one quota unit for billable text when the model*group ratio is nonzero.
	rounded := new(big.Rat).Add(amount, big.NewRat(1, 2))
	quota := new(big.Int).Quo(rounded.Num(), rounded.Denom())
	minimum := quota.Sign() == 0 && model.Sign() != 0 && group.Sign() != 0
	if minimum {
		quota.SetInt64(1)
	}
	if !quota.IsInt64() || quota.Int64() > 2147483647 {
		return unavailable("unsupported", "金额达到额度边界，暂不套用普通公式。")
	}
	if quota.Int64() != r.billingQuota {
		return unavailable("mismatch", "现有日志字段的复算结果与实际扣费不一致，可能缺少计费依据；暂不展示推算公式，实际扣费未改动。")
	}
	amountUSD := new(big.Rat).SetFrac(quota, big.NewInt(500000)).FloatString(6)
	rounding := "按整数额度四舍五入"
	if minimum {
		rounding += "，适用最小扣费 1 额度"
	}
	return portalBillingCheck{State: "matched", Message: "按已记录字段复核，与本条实际扣费一致；不代表原始计费记录已完整审计。",
		baseUSD:  portalBillingBaseUSD(displayTerms),
		Readable: portalReadableBilling(displayTerms, group, quota.Int64(), minimum),
		Formula:  fmt.Sprintf("%s\n%s = %s 额度\n实际扣费：%s ÷ 500000 = $%s（沿用平台美元展示口径）", expression, rounding, quota.String(), quota.String(), amountUSD)}
}
