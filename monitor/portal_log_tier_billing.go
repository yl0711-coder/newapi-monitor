package monitor

import (
	"fmt"
	"math"
	"math/big"
	"strings"
)

// Arithmetic over a small, fully parsed linear grammar, never execution of an
// upstream expression. The original quota remains the authoritative charge.
func checkPortalTierBilling(r LogRow, e portalLogEvidence) portalBillingCheck {
	fail := func(state, message string) portalBillingCheck {
		return portalBillingCheck{State: state, Message: message + "；费用仍以实际扣费为准。"}
	}
	p, ok := portalRecordedTierPrices(e)
	if !ok {
		return fail("unsupported", "阶梯记录不完整或表达式尚未支持，未推测单价或费用")
	}
	source := e.Strings["billing_source"]
	if e.ExtraBilling || e.ToolsState != portalEvidenceMissing || e.Strings["task_id"].State != portalEvidenceMissing || (source.State != portalEvidenceMissing && (source.State != portalEvidenceValid || source.Text != "wallet")) {
		return fail("unsupported", "包含附加计费、订阅或边界处理记录，暂不套用文本阶梯公式")
	}
	for _, k := range []string{"image", "audio", "ws", "is_task", "web_search", "file_search", portalAudioInputSeparatePriceKey, "image_generation_call"} {
		f := e.Flags[k]
		if f.State != portalEvidenceMissing && (f.State != portalEvidenceValid || f.Text != "false") {
			return fail("unsupported", "多模态或工具计费尚需独立核验")
		}
	}
	for _, k := range []string{"audio_input", "audio_output", "text_input", "text_output", "image_output", "web_search_call_count", "file_search_call_count", "audio_input_token_count", "image_generation_call_count", "web_search_price", "file_search_price", "audio_input_price", "image_generation_call_price"} {
		if e.Numbers[k].State != portalEvidenceMissing {
			return fail("unsupported", "含额外计费分项，暂不套用文本阶梯公式")
		}
	}
	for _, k := range []string{"img", "img_o", "ai", "ao"} {
		if p.Used[k] {
			return fail("unsupported", "表达式使用多模态变量，尚未完整复核")
		}
	}
	path := e.Strings["request_path"]
	if path.State != portalEvidenceValid || (path.Text != "/v1/responses" && path.Text != "/v1/chat/completions" && path.Text != "/v1/messages") {
		return fail("incomplete", "缺少可核验的文本请求路径")
	}
	if r.PromptTokens == 0 && r.CompletionTokens == 0 {
		return fail("incomplete", "零 Token 记录需结合额外结算依据")
	}
	get := func(k string, optional bool) (*big.Rat, bool) {
		f := e.Numbers[k]
		if optional && f.State == portalEvidenceMissing {
			return new(big.Rat), true
		}
		v, ok := new(big.Rat).SetString(f.Text)
		return v, f.State == portalEvidenceValid && ok && v.Sign() >= 0
	}
	group, ok := get("group_ratio", false)
	if !ok {
		return fail("incomplete", "缺少有效的历史分组倍率")
	}
	if u := e.Numbers["user_group_ratio"]; u.State != portalEvidenceMissing {
		v, ok := new(big.Rat).SetString(u.Text)
		if u.State != portalEvidenceValid || !ok || (v.Cmp(big.NewRat(-1, 1)) != 0 && v.Cmp(group) != 0) {
			return fail("incomplete", "个人倍率与实际分组倍率的关系待核验，不重复叠乘")
		}
	}
	read, ok := get("cache_tokens", !p.Used["cr"])
	if !ok {
		return fail("incomplete", "缓存读取数缺失或异常")
	}
	write, ok := get("cache_creation_tokens", true)
	if !ok {
		return fail("incomplete", "缓存创建数异常")
	}
	five, ok := get("cache_creation_tokens_5m", true)
	if !ok {
		return fail("incomplete", "5 分钟缓存创建数异常")
	}
	hour, ok := get("cache_creation_tokens_1h", true)
	if !ok {
		return fail("incomplete", "1 小时缓存创建数异常")
	}
	cache := portalCacheWriteEvidence(e)
	if cache.State == portalEvidenceInvalid || cache.TotalSource == "split_only" {
		return fail("incomplete", "缓存创建总量与分项关系无法核验")
	}
	semantic := e.Strings["usage_semantic"]
	anthropic := semantic.State == portalEvidenceValid && semantic.Text == "anthropic"
	defaultSemantic := semantic.State == portalEvidenceMissing
	if !defaultSemantic && !anthropic && (semantic.State != portalEvidenceValid || semantic.Text != "openai") {
		return fail("incomplete", "无法确定缓存计量口径")
	}
	if defaultSemantic {
		flag := e.Flags["claude"]
		if flag.State != portalEvidenceMissing && (flag.State != portalEvidenceValid || flag.Text != "false") {
			return fail("incomplete", "旧 Claude 标志不足以确定新版缓存计量口径")
		}
		// rc26 explicitly records anthropic; other text logs use the OpenAI
		// default. Never infer this from the model name or the inbound path.
	}
	input := big.NewRat(r.PromptTokens, 1)
	output := big.NewRat(r.CompletionTokens, 1)
	length := new(big.Rat).Set(input)
	cc, cc1h := write, new(big.Rat)
	if anthropic {
		if new(big.Rat).Add(five, hour).Cmp(write) != 0 {
			return fail("incomplete", "Anthropic 缓存创建总量存在未拆分部分，不能猜测 TTL 计价")
		}
		cc, cc1h = five, hour
		length.Add(length, read).Add(length, write)
	} else {
		if five.Sign() != 0 || hour.Sign() != 0 {
			return fail("incomplete", "输入含缓存口径出现 TTL 拆分，需额外证据核验")
		}
		if p.Used["cr"] {
			input.Sub(input, read)
		}
		if p.Used["cc"] {
			input.Sub(input, cc)
		}
		if input.Sign() < 0 {
			input.SetInt64(0)
		}
	}
	values := map[string]*big.Rat{"p": input, "c": output, "cr": read, "cc": cc, "cc1h": cc1h}
	// NewAPI's evaluator uses float64. Refuse token magnitudes that cannot be
	// represented exactly, rather than pretending decimal arithmetic proves it.
	limit := big.NewRat(1<<53, 1)
	for _, v := range []*big.Rat{big.NewRat(r.PromptTokens, 1), output, read, write, five, hour, length} {
		if v.Cmp(limit) > 0 {
			return fail("unsupported", "Token 数超过精确复核范围")
		}
	}
	index := 0
	for i, b := range p.Branches {
		match := b.Op == ""
		if !match {
			bound, _ := new(big.Rat).SetString(b.Threshold.Text)
			bf, _ := bound.Float64()
			lf, _ := length.Float64()
			cmp := length.Cmp(bound)
			switch b.Op {
			case "<":
				match = cmp < 0
			case "<=":
				match = cmp <= 0
			case ">":
				match = cmp > 0
			case ">=":
				match = cmp >= 0
			}
			floatMatch := false
			switch b.Op {
			case "<":
				floatMatch = lf < bf
			case "<=":
				floatMatch = lf <= bf
			case ">":
				floatMatch = lf > bf
			case ">=":
				floatMatch = lf >= bf
			}
			if match != floatMatch {
				return fail("unsupported", "档位阈值位于浮点精度边界")
			}
		}
		if match {
			index = i + 1
			break
		}
	}
	if index != p.Index {
		return fail("mismatch", "日志命中档位与记录的上下文长度不一致，暂不展示推算公式")
	}
	amount := new(big.Rat)
	floatAmount := float64(0)
	var terms []string
	var displayTerms []portalBillingTerm
	labels := map[string]string{"p": "计价输入", "c": "输出", "cr": "缓存读取", "cc": "缓存创建", "cc1h": "1 小时缓存创建"}
	for _, key := range p.Branches[index-1].Order {
		price, _ := new(big.Rat).SetString(p.Prices[key].Text)
		amount.Add(amount, new(big.Rat).Mul(values[key], price))
		vf, _ := values[key].Float64()
		pf, _ := price.Float64()
		product := vf * pf
		floatAmount += product
		terms = append(terms, fmt.Sprintf("%s %s × %s", labels[key], values[key].Num().String(), p.Prices[key].Text))
		label := labels[key]
		if key == "p" {
			label = "普通输入"
		}
		displayTerms = append(displayTerms, portalBillingTerm{label: label, count: values[key], price: price})
	}
	amount.Mul(amount, group).Quo(amount, big.NewRat(2, 1)) // $/1M * 500000 quota/$
	groupFloat, _ := group.Float64()
	floatQuota := floatAmount / 1000000 * 500000 * groupFloat
	if math.IsNaN(floatQuota) || math.IsInf(floatQuota, 0) || floatQuota < 0 || floatQuota > 2147483647 {
		return fail("unsupported", "金额达到数值或额度边界")
	}
	rounded := new(big.Rat).Add(amount, big.NewRat(1, 2))
	quota := new(big.Int).Quo(rounded.Num(), rounded.Denom())
	if !quota.IsInt64() || quota.Int64() != int64(math.Round(floatQuota)) {
		return fail("unsupported", "金额位于浮点舍入边界，暂不输出确定公式")
	}
	if quota.Int64() != r.billingQuota {
		return fail("mismatch", "现有日志字段复算与实际扣费不一致，可能缺少计费依据，未修改实际扣费")
	}
	semantics := "输入含缓存：仅扣除整个阶梯表达式中单独计价的缓存项；重叠余量最低为 0"
	if defaultSemantic {
		semantics = "按 NewAPI 默认输入含缓存口径：仅扣除表达式中单独计价的缓存项；日志未记录的缓存创建项按 0 处理"
	}
	if anthropic {
		semantics = "日志明确为 Anthropic 口径：输入不再扣除缓存，缓存按独立项计价"
	}
	normalization := fmt.Sprintf("原始输入 %d", r.PromptTokens)
	if !anthropic {
		if p.Used["cr"] {
			normalization += " − 缓存读取 " + read.Num().String()
		}
		if p.Used["cc"] {
			normalization += " − 缓存创建 " + cc.Num().String()
		}
		normalization = "max(0, " + normalization + ")"
	}
	semantics += "\n计价输入：" + normalization + " = " + input.Num().String()
	usd := new(big.Rat).SetFrac(quota, big.NewInt(500000)).FloatString(6)
	return portalBillingCheck{State: "matched", Message: "按日志档位、Token、单价和分组倍率复核，与本条实际扣费一致；不代表计费证据已完整审计。", Readable: portalReadableBilling(displayTerms, group, quota.Int64(), false), Formula: fmt.Sprintf("%s\n上下文长度 %s，核验命中第 %d 档\n（%s）÷ 1000000 × 分组倍率 %s × 500000\n按整数额度四舍五入 = %s 额度（阶梯不套用普通倍率最低 1 额度）\n实际扣费：%s ÷ 500000 = $%s", semantics, length.Num().String(), index, strings.Join(terms, " + "), e.Numbers["group_ratio"].Text, quota.String(), quota.String(), usd)}
}
