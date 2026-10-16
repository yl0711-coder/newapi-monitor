package monitor

import (
	"errors"
	"math/big"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const portalBillFormat = "bill-v1"

func portalBillHeader() []string {
	return []string{"消费时间（北京时间）", "账户名", "令牌名", "分组名", "模型", "Request ID", "类型", "普通输入 Token", "输出 Token", "缓存读取 Token", "缓存写入 Token", "官方原价（美元）", "折扣前价格（元）", "折后价格（元）", "折扣比例"}
}

// Optional historical pricing metadata must not erase recorded consumption.
// Explicit subscription deductions require a separate customer-facing contract.
func portalBillRecord(r LogRow) ([]string, error) {
	quotaPresent := r.billingFactsValid
	if r.SourceFacts != nil {
		quotaPresent = r.SourceFacts.QuotaPresent
	}
	if r.Type != 2 || !quotaPresent || r.billingQuota < 0 {
		return nil, errors.New("消费记录缺少有效的原始扣费金额；未生成部分账单")
	}
	e := portalLogEvidence{}
	if r.billEvidence != nil {
		e = *r.billEvidence
	}
	if portalBillHasSubscription(e, r.BillingSource) {
		return nil, errors.New("所选消费包含订阅额度抵扣，不能按钱包收费导出；未生成部分账单")
	}
	actual := new(big.Rat).SetFrac(big.NewInt(r.billingQuota), big.NewInt(500000))
	row := []string{time.Unix(r.CreatedAt, 0).In(usageCST).Format("2006-01-02 15:04:05"), r.Member, r.TokenName, r.Group, r.ModelName, r.RequestID, portalBillRequestType(e), "", "", "", "", "", "", "¥" + actual.FloatString(6), ""}
	if r.CompletionTokens >= 0 && (r.SourceFacts == nil || r.SourceFacts.CompletionPresent) {
		row[8] = strconv.FormatInt(r.CompletionTokens, 10)
	}
	read, readOK := portalBillCount(e, "cache_tokens", false)
	write, writeOK := portalBillCount(e, "cache_creation_tokens", false)
	// Token evidence is independent of monetary verification. A rounding
	// boundary or recorded zero usage must not hide otherwise proven counts.
	defaultTier := portalBillDefaultTierTokenContract(r, e)
	if defaultTier && e.Numbers["cache_creation_tokens"].State == portalEvidenceMissing {
		write, writeOK = 0, true
	}
	if readOK {
		row[9] = strconv.FormatInt(read, 10)
	}
	if writeOK {
		row[10] = strconv.FormatInt(write, 10)
	}
	semantic := e.Strings["usage_semantic"]
	if defaultTier {
		semantic = portalEvidenceField{State: portalEvidenceValid, Text: "openai"}
	}
	if r.PromptTokens >= 0 && (r.SourceFacts == nil || r.SourceFacts.PromptPresent) && semantic.State == portalEvidenceValid {
		switch semantic.Text {
		case "anthropic":
			row[7] = strconv.FormatInt(r.PromptTokens, 10)
		case "openai":
			// Subtract sequentially to avoid int64 overflow. Unknown or
			// inconsistent counts remain blank, never clamp a negative to 0.
			if readOK && writeOK && read <= r.PromptTokens && write <= r.PromptTokens-read {
				row[7] = strconv.FormatInt(r.PromptTokens-read-write, 10)
			}
		}
	}
	if original, ok := portalBillRecordedReference(r, e); ok {
		// Reference is exact CNY, not the already-rounded CSV display. Keep
		// the existing recorded-price checks and the agreed fixed factor 7.
		row[11] = "$" + new(big.Rat).Quo(original, big.NewRat(7, 1)).FloatString(6)
		row[12] = "¥" + original.FloatString(6)
		if original.Sign() > 0 {
			row[14] = strings.TrimRight(strings.TrimRight(new(big.Rat).Quo(actual, original).FloatString(3), "0"), ".")
		}
	}
	for i := range row {
		row[i] = portalBillSafeText(row[i])
	}
	return row, nil
}

func portalBillDefaultTierTokenContract(r LogRow, e portalLogEvidence) bool {
	if e.State != portalEvidenceValid || e.Mode != "tiered_expr" ||
		e.Strings["usage_semantic"].State != portalEvidenceMissing ||
		r.BillingSource != "wallet" {
		return false
	}
	// Only the recognized text-log contract permits zero omission. Do not
	// infer it from model names, deployment dates or a price matching by chance.
	p, ok := portalRecordedTierPrices(e)
	if !ok || e.ToolsState != portalEvidenceMissing || e.Strings["task_id"].State != portalEvidenceMissing {
		return false
	}
	path := e.Strings["request_path"]
	if path.State != portalEvidenceValid || (path.Text != "/v1/responses" && path.Text != "/v1/chat/completions" && path.Text != "/v1/messages") {
		return false
	}
	for _, key := range []string{"claude", "image", "audio", "ws", "is_task", "web_search", "file_search", "image_generation_call", portalAudioInputSeparatePriceKey} {
		f := e.Flags[key]
		if f.State != portalEvidenceMissing && (f.State != portalEvidenceValid || f.Text != "false") {
			return false
		}
	}
	for _, key := range []string{"audio_input", "audio_output", "text_input", "text_output", "image_output", "web_search_call_count", "file_search_call_count", "audio_input_token_count", "image_generation_call_count", "web_search_price", "file_search_price", "audio_input_price", "image_generation_call_price"} {
		if e.Numbers[key].State != portalEvidenceMissing {
			return false
		}
	}
	for _, key := range []string{"img", "img_o", "ai", "ao"} {
		if p.Used[key] {
			return false
		}
	}
	// Prompt presence is required for normalization, not completion presence,
	// a positive quota, or successful reconstruction of historical prices.
	if (r.SourceFacts != nil && !r.SourceFacts.PromptPresent) || (r.SourceFacts == nil && !r.billingFactsValid) {
		return false
	}
	read, readOK := portalBillCount(e, "cache_tokens", false)
	write, writeOK := portalBillCount(e, "cache_creation_tokens", true)
	five, fiveOK := portalBillCount(e, "cache_creation_tokens_5m", true)
	hour, hourOK := portalBillCount(e, "cache_creation_tokens_1h", true)
	if !readOK || !writeOK || !fiveOK || !hourOK || five != 0 || hour != 0 ||
		r.PromptTokens < 0 || read > r.PromptTokens || write > r.PromptTokens-read {
		return false
	}
	// Redundant UI counts are checks, not additional billable token buckets.
	for key, want := range map[string]int64{"input_tokens_total": r.PromptTokens, "cache_write_tokens": write} {
		if e.Numbers[key].State != portalEvidenceMissing {
			v, valid := portalBillCount(e, key, false)
			if !valid || v != want {
				return false
			}
		}
	}
	return true
}

func portalBillHasSubscription(e portalLogEvidence, source string) bool {
	if source == "subscription" || e.Strings["billing_source"].Text == "subscription" {
		return true
	}
	for _, key := range []string{"subscription_id", "subscription_plan_id", "subscription_consumed", "subscription_pre_consumed", "subscription_total"} {
		f, exists := e.Numbers[key]
		if !exists || f.State == portalEvidenceMissing {
			continue
		}
		v, ok := portalBillCount(e, key, false)
		if !ok || v > 0 {
			return true
		}
	}
	return false
}

// Request category is separate from NewAPI's log type (consumption == 2).
// Do not label unknown history or future media requests as text by default.
func portalBillRequestType(e portalLogEvidence) string {
	for _, key := range []string{"audio", "ws", "image_generation_call", "is_task", portalAudioInputSeparatePriceKey} {
		f := e.Flags[key]
		if f.State != portalEvidenceMissing && (f.State != portalEvidenceValid || f.Text != "false") {
			return "未确认"
		}
	}
	if e.Strings["task_id"].State != portalEvidenceMissing {
		return "未确认"
	}
	p := e.Strings["request_path"]
	if p.State == portalEvidenceValid {
		switch p.Text {
		case "/v1/responses", "/v1/chat/completions", "/v1/messages":
			return "文本"
		}
	}
	return "未确认"
}

func portalBillCount(e portalLogEvidence, key string, omittedZero bool) (int64, bool) {
	f := e.Numbers[key]
	if omittedZero && f.State == portalEvidenceMissing {
		return 0, true
	}
	v, ok := new(big.Rat).SetString(f.Text)
	if f.State != portalEvidenceValid || !ok || !v.IsInt() || !v.Num().IsInt64() || v.Sign() < 0 {
		return 0, false
	}
	return v.Num().Int64(), true
}

// User-approved reference: use THIS log's recorded base prices and billing
// variables, before group ratio, then the agreed USD-to-CNY reference factor 7.
// This is not an independently sourced official price catalogue. Never use
// current prices, model-name guesses or reverse-divide rounded actual charges.
func portalBillRecordedReference(r LogRow, e portalLogEvidence) (*big.Rat, bool) {
	if !r.billingFactsValid || r.billingQuota < 0 || r.PromptTokens < 0 || r.CompletionTokens < 0 ||
		e.State != portalEvidenceValid || portalBillHasSubscription(e, r.BillingSource) {
		return nil, false
	}
	if r.SourceFacts != nil && (!r.SourceFacts.PromptPresent || !r.SourceFacts.CompletionPresent || !r.SourceFacts.QuotaPresent) {
		return nil, false
	}
	var check portalBillingCheck
	if e.Mode == "tiered_expr" {
		check = checkPortalTierBillingMode(r, e, true)
	} else {
		check = checkPortalBilling(r, e)
	}
	if check.State != "matched" || check.baseUSD == nil || check.baseUSD.Sign() < 0 {
		return nil, false
	}
	return new(big.Rat).Mul(check.baseUSD, big.NewRat(7, 1)), true
}

func portalExportFormatValid(format string) bool { return format == "" || format == portalBillFormat }

func portalBillSafeText(s string) string {
	trimmed := strings.TrimLeftFunc(s, unicode.IsSpace)
	if trimmed != "" && strings.ContainsRune("=+-@", rune(trimmed[0])) {
		return "'" + s
	}
	return csvSafe(s)
}
