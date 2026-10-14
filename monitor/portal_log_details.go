package monitor

import (
	"math/big"
	"strconv"
)

// Customer output is a separate allowlist. Never serialize portalLogEvidence:
// expressions, rule conditions, arbitrary tool names and admin fields stay out.
type portalLogDetailItem struct {
	Label string              `json:"label"`
	Value string              `json:"value"`
	State portalEvidenceState `json:"state"`
}

// Presence is separate from old numeric fields for backwards-compatible JSON.
type portalLogSourceFacts struct {
	PromptPresent     bool `json:"prompt_present"`
	CompletionPresent bool `json:"completion_present"`
	QuotaPresent      bool `json:"quota_present"`
}

type portalLogDetails struct {
	Billing             *portalBillingCheck   `json:"billing,omitempty"`
	CacheRead           portalCacheDisplay    `json:"cache_read"`
	CacheWrite          portalCacheDisplay    `json:"cache_write"`
	Items               []portalLogDetailItem `json:"items"`
	Notice              string                `json:"notice,omitempty"`
	LegacyFormulaUnsafe bool                  `json:"legacy_formula_unsafe,omitempty"`
}

func buildPortalLogDetails(logType int, raw string) *portalLogDetails {
	return portalLogDetailsFromEvidence(logType, parsePortalLogEvidence(raw))
}

func portalLogDetailsFromEvidence(logType int, e portalLogEvidence) *portalLogDetails {
	if logType != 2 {
		return nil
	}
	if e.State == portalEvidenceMissing || e.State == portalEvidenceNull {
		return nil
	}
	read, write := portalCacheDisplays(e)
	d := &portalLogDetails{Items: []portalLogDetailItem{}, CacheRead: read, CacheWrite: write}
	if e.State != portalEvidenceValid {
		d.Notice = "计费详情记录异常；费用仍以原始实际扣费为准。"
		d.LegacyFormulaUnsafe = true
		return d
	}
	add := func(label string, f portalEvidenceField) {
		if f.State == portalEvidenceMissing {
			return
		}
		value := f.Text
		if f.State == portalEvidenceNull {
			value = "未记录"
		}
		if f.State != portalEvidenceValid && f.State != portalEvidenceNull {
			value = "记录异常"
		}
		if f.State != portalEvidenceValid {
			d.LegacyFormulaUnsafe = true
		}
		d.Items = append(d.Items, portalLogDetailItem{label, value, f.State})
	}
	if e.Mode == "tiered_expr" {
		add("计费方式", portalEvidenceField{State: portalEvidenceValid, Text: "阶梯计费"})
		add("实际分组倍率（日志原值）", e.Numbers["group_ratio"])
		d.Items = append(d.Items, portalTierPriceItems(e)...)
	}
	for _, field := range []struct{ key, label string }{
		{"cache_tokens", "缓存读取 Tokens（日志原值）"},
		{"cache_creation_tokens", "缓存创建总量（日志原值）"},
		{"cache_creation_tokens_5m", "5 分钟缓存创建 Tokens"},
		{"cache_creation_tokens_1h", "1 小时缓存创建 Tokens"},
	} {
		add(field.label, e.Numbers[field.key])
	}
	cache := portalCacheWriteEvidence(e)
	if cache.State == portalEvidenceInvalid {
		d.Notice = "缓存总量或分项记录异常，未自动修正；费用仍以原始实际扣费为准。"
		d.LegacyFormulaUnsafe = true
	} else if cache.TotalSource == "split_only" {
		d.Notice = "日志未记录缓存创建总量，仅展示已有分项；不能据此认定分项完整。"
		d.LegacyFormulaUnsafe = true
	} else if cache.Unsplit.State == portalEvidenceValid && cache.Unsplit.Text != "0" &&
		(e.Numbers["cache_creation_tokens_5m"].State == portalEvidenceValid || e.Numbers["cache_creation_tokens_1h"].State == portalEvidenceValid) {
		add("未拆分缓存创建 Tokens", cache.Unsplit)
		d.LegacyFormulaUnsafe = true
	}
	// -1 is NewAPI's sentinel for no personal override; a real zero is retained.
	u := e.Numbers["user_group_ratio"]
	if u.State == portalEvidenceValid {
		n, _ := new(big.Rat).SetString(u.Text)
		if n.Cmp(big.NewRat(-1, 1)) == 0 {
			u.State = portalEvidenceMissing
		} else if n.Sign() < 0 {
			u = portalEvidenceField{State: portalEvidenceInvalid}
		}
	}
	add("个人分组倍率（日志原值）", u)
	if u.State == portalEvidenceValid {
		n, _ := new(big.Rat).SetString(u.Text)
		if n.Sign() == 0 {
			d.LegacyFormulaUnsafe = true
		}
	} else if u.State == portalEvidenceInvalid || u.State == portalEvidenceNull {
		d.LegacyFormulaUnsafe = true
	}
	if items := portalMediaDetailItems(e); len(items) != 0 {
		d.Items = append(d.Items, items...)
		d.LegacyFormulaUnsafe = true
	}
	if e.ToolsState != portalEvidenceMissing {
		// Structured fields take display precedence. Never add legacy and new
		// tool records together; this layer does not calculate any money.
		d.LegacyFormulaUnsafe = true
		if e.ToolsState != portalEvidenceValid {
			add("工具附加费记录", portalEvidenceField{State: e.ToolsState})
		} else {
			for i, tool := range e.Tools {
				label := "工具调用"
				switch tool.Name.Text {
				case "web_search":
					label = "网页搜索"
				case "web_search_preview":
					label = "网页搜索（预览）"
				case "google_search":
					label = "Google 搜索"
				case "file_search":
					label = "文件搜索"
				case "image_generation":
					label = "图片生成"
				}
				// Keep repeated/unknown names distinguishable without exposing
				// arbitrary customer-defined tool names or merging line items.
				if len(e.Tools) > 1 {
					label += "（记录 " + strconv.Itoa(i+1) + "）"
				}
				add(label+"次数", tool.Count)
				add(label+"单价（$/千次，未乘分组倍率）", tool.Price)
			}
		}
	} else {
		for _, tool := range []struct{ name, count, price string }{
			{"网页搜索", "web_search_call_count", "web_search_price"},
			{"文件搜索", "file_search_call_count", "file_search_price"},
		} {
			add(tool.name+"次数", e.Numbers[tool.count])
			add(tool.name+"单价（$/千次，未乘分组倍率）", e.Numbers[tool.price])
			if e.Numbers[tool.count].State != portalEvidenceMissing || e.Numbers[tool.price].State != portalEvidenceMissing {
				d.LegacyFormulaUnsafe = true
			}
		}
		// Legacy image generation was priced per call, not per 1000 calls.
		// Never synthesize a count from the boolean flag or a nonzero price.
		add("图片生成次数（日志原值）", e.Numbers["image_generation_call_count"])
		add("图片生成单价（$/次，未乘分组倍率）", e.Numbers["image_generation_call_price"])
	}
	// Existing formula code does not understand these newer settlement paths.
	if e.Mode == "unknown" || e.Mode == "tiered_expr" || e.Mode == "per_request" || e.Strings["usage_semantic"].State != portalEvidenceMissing {
		d.LegacyFormulaUnsafe = true
	}
	if e.Mode == "ratio" {
		// The old formatter silently substitutes zero/one for absent rates.
		// Do not advertise that as a complete formula for a historical charge.
		if e.Numbers["completion_ratio"].State != portalEvidenceValid ||
			(u.State != portalEvidenceValid && e.Numbers["group_ratio"].State != portalEvidenceValid) {
			d.LegacyFormulaUnsafe = true
		}
		for _, pair := range [][2]string{{"cache_tokens", "cache_ratio"}, {"cache_creation_tokens", "cache_creation_ratio"}, {"cache_creation_tokens_5m", "cache_creation_ratio_5m"}, {"cache_creation_tokens_1h", "cache_creation_ratio_1h"}} {
			f := e.Numbers[pair[0]]
			if f.State == portalEvidenceNull || f.State == portalEvidenceInvalid {
				d.LegacyFormulaUnsafe = true
			}
			if f.State == portalEvidenceValid {
				n, _ := new(big.Rat).SetString(f.Text)
				if n.Sign() > 0 && e.Numbers[pair[1]].State != portalEvidenceValid {
					d.LegacyFormulaUnsafe = true
				}
				if n.Sign() > 0 && pair[0] != "cache_tokens" && e.Flags["claude"].Text != "true" {
					d.LegacyFormulaUnsafe = true
				}
			}
		}
	}
	// Known specialized charges are not represented by the legacy text formula.
	for _, key := range []string{"audio_input", "audio_output", "audio_input_token_count", "audio_input_price", "image_output", "image_generation_call_price", "image_generation_call_count"} {
		if e.Numbers[key].State != portalEvidenceMissing {
			d.LegacyFormulaUnsafe = true
		}
	}
	for _, key := range []string{"audio", "image", "ws", "audio_input_seperate_price", "image_generation_call", "web_search", "file_search"} {
		if f := e.Flags[key]; f.State == portalEvidenceInvalid || f.State == portalEvidenceNull || f.Text == "true" {
			d.LegacyFormulaUnsafe = true
		}
	}
	if d.LegacyFormulaUnsafe && d.Notice == "" {
		d.Notice = "当前日志的完整计费公式暂未展示；以下为日志记录，费用以原始实际扣费为准。"
	}
	if len(d.Items) == 0 && d.Notice == "" {
		return nil
	}
	return d
}

// Shared by API and export queries. This changes derived display fields only;
// raw prompt/completion tokens and charged quota are never recomputed.
func populatePortalLogEvidence(r *LogRow, raw string) {
	populatePortalCustomerOperation(r, raw)
	e := parsePortalLogEvidence(raw)
	populatePortalSubscription(r, e)
	if portalLogHasRecordedAmount(r.Type) {
		r.HasToolSurcharge = portalHasToolSurcharge(e)
	}
	if r.Type != 2 && r.Type != 5 {
		return
	}
	r.RequestDetails = portalRequestDetails(*r, e)
	r.FirstByteEvidence = portalFirstByteEvidence(*r, e)
	if r.Type != 2 {
		return
	}
	read, write := portalCacheDisplays(e)
	r.CacheReadTokens, r.CacheWriteTokens = portalCacheDisplayInt(read), portalCacheDisplayInt(write)
	r.UsageDetails = portalLogDetailsFromEvidence(r.Type, e)
	if r.UsageDetails == nil {
		r.UsageDetails = &portalLogDetails{Items: []portalLogDetailItem{}, CacheRead: read, CacheWrite: write}
	}
	check := checkPortalBilling(*r, e)
	r.UsageDetails.Billing = &check
	r.UsageDetails.LegacyFormulaUnsafe = check.State != "matched"
	r.BillingProcess = check.Formula
	// The verification message supersedes the generic staging note, but never
	// removes a concrete cache-integrity warning.
	if r.UsageDetails.Notice == "当前日志的完整计费公式暂未展示；以下为日志记录，费用以原始实际扣费为准。" {
		r.UsageDetails.Notice = ""
	}
}
