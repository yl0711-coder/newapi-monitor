package monitor

import (
	"fmt"
	"math"
	"math/big"
	"strconv"
	"strings"
)

func portalNeedsCacheRatioCheck(e portalLogEvidence) bool {
	for _, key := range []string{"cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h"} {
		f := e.Numbers[key]
		if f.State == portalEvidenceMissing {
			continue
		}
		n, ok := new(big.Rat).SetString(f.Text)
		if f.State != portalEvidenceValid || !ok || n.Sign() != 0 {
			return true
		}
	}
	// Even a recorded zero split cannot establish an absent write total.
	return portalCacheWriteEvidence(e).TotalSource == "split_only"
}

// Only the shared rc.4/rc.26 text-cache arithmetic is accepted. The caller
// already excludes tools, media, tasks, other ratios and subscription billing.
// This function explains weighted tokens; only the caller's exact comparison
// with the original integer quota can publish the completed formula.
func portalCachedRatioTokens(r LogRow, e portalLogEvidence, displayTerms *[]portalBillingTerm) (*big.Rat, string, *portalBillingCheck) {
	fail := func(state, message string) (*big.Rat, string, *portalBillingCheck) {
		return nil, "", &portalBillingCheck{State: state, Message: message + "；费用仍以实际扣费为准。"}
	}
	cache := portalCacheWriteEvidence(e)
	if cache.State == portalEvidenceInvalid || cache.TotalSource == "split_only" {
		return fail("incomplete", "缓存创建总量与分项关系无法核验")
	}
	counts := make(map[string]*big.Rat)
	for _, key := range []string{"cache_tokens", "cache_creation_tokens", "cache_creation_tokens_5m", "cache_creation_tokens_1h"} {
		f := e.Numbers[key]
		// GenerateTextOtherInfo always writes cache_tokens (including zero).
		// Missing creation fields are omitted zero counts in the supported
		// generators; explicit null/malformed values are never treated as zero.
		if f.State == portalEvidenceMissing && key != "cache_tokens" {
			counts[key] = new(big.Rat)
			continue
		}
		n, ok := new(big.Rat).SetString(f.Text)
		if f.State != portalEvidenceValid || !ok {
			return fail("incomplete", "缓存读取或创建数量缺失、异常")
		}
		counts[key] = n
	}
	read, write := counts["cache_tokens"], counts["cache_creation_tokens"]
	five, hour := counts["cache_creation_tokens_5m"], counts["cache_creation_tokens_1h"]
	semantic := e.Strings["usage_semantic"]
	flag := e.Flags["claude"]
	if flag.State == portalEvidenceInvalid || flag.State == portalEvidenceNull {
		return fail("incomplete", "缓存口径标记异常")
	}
	anthropic := semantic.State == portalEvidenceValid && semantic.Text == "anthropic"
	defaultSemantic := semantic.State == portalEvidenceMissing
	if !anthropic && !defaultSemantic && (semantic.State != portalEvidenceValid || semantic.Text != "openai") {
		return fail("incomplete", "缓存计量口径未知")
	}
	if !anthropic && flag.Text == "true" {
		// claude marks the final request format, not necessarily usage semantics.
		return fail("incomplete", "仅凭旧 Claude 标记无法确定缓存是否已包含在输入中")
	}
	if !anthropic && (five.Sign() != 0 || hour.Sign() != 0) {
		return fail("incomplete", "输入含缓存口径出现 TTL 拆分，不能推测历史转换方式")
	}
	rates := make(map[string]*big.Rat)
	getRate := func(key string, required bool) bool {
		f := e.Numbers[key]
		if f.State == portalEvidenceMissing && !required {
			rates[key] = new(big.Rat)
			return true
		}
		if f.State != portalEvidenceValid {
			return false
		}
		n, ok := new(big.Rat).SetString(f.Text)
		if !ok || n.Sign() < 0 {
			return false
		}
		// Source code reads JSON rates as float64 then uses NewFromFloat.
		// Do not certify extra decimal precision the source couldn't preserve.
		v, err := strconv.ParseFloat(f.Text, 64)
		if err != nil || math.IsInf(v, 0) || math.IsNaN(v) {
			return false
		}
		roundTrip, ok := new(big.Rat).SetString(strconv.FormatFloat(v, 'g', -1, 64))
		if !ok || roundTrip.Cmp(n) != 0 {
			return false
		}
		rates[key] = n
		return true
	}
	remaining := new(big.Rat).Sub(write, new(big.Rat).Add(five, hour))
	for _, field := range []struct {
		key    string
		needed bool
	}{
		{"model_ratio", true}, {"group_ratio", true}, {"completion_ratio", true},
		{"cache_ratio", read.Sign() != 0},
		{"cache_creation_ratio", remaining.Sign() != 0},
		{"cache_creation_ratio_5m", five.Sign() != 0}, {"cache_creation_ratio_1h", hour.Sign() != 0},
	} {
		if !getRate(field.key, field.needed) {
			return fail("incomplete", "历史缓存倍率缺失、异常或数值精度无法复核")
		}
	}
	base := big.NewRat(r.PromptTokens, 1)
	meaning := "日志明确为输入不含缓存口径，缓存读取与创建分项另计"
	if !anthropic {
		base.Sub(base, read).Sub(base, write)
		if base.Sign() < 0 {
			// rc.4 permitted negative remainders; rc.26 clamps them. Do not
			// guess the writer version or choose a formula because quota matches.
			return fail("unsupported", "缓存分项超过输入总量，旧新版本处理不同，暂不推算公式")
		}
		meaning = fmt.Sprintf("输入含缓存口径：原始输入 %d − 缓存读取 %s − 缓存创建 %s = 普通输入 %s", r.PromptTokens, read.Num(), write.Num(), base.Num())
		if defaultSemantic {
			meaning = "按兼容生成器默认输入含缓存口径（日志未单独标记）：" + meaning
		}
	}
	weighted := new(big.Rat).Set(base)
	inputPrice := new(big.Rat).Mul(rates["model_ratio"], big.NewRat(2, 1))
	*displayTerms = append(*displayTerms, portalBillingTerm{label: "普通输入", count: base, price: inputPrice})
	parts := []string{fmt.Sprintf("普通输入 %s", base.Num())}
	for _, part := range []struct {
		label, rate string
		count       *big.Rat
	}{
		{"缓存读取", "cache_ratio", read}, {"未按 TTL 拆分的缓存创建", "cache_creation_ratio", remaining},
		{"5 分钟缓存创建", "cache_creation_ratio_5m", five}, {"1 小时缓存创建", "cache_creation_ratio_1h", hour},
	} {
		if part.count.Sign() != 0 {
			label := part.label
			if label == "未按 TTL 拆分的缓存创建" {
				label = "缓存创建"
			}
			*displayTerms = append(*displayTerms, portalBillingTerm{label: label, count: part.count, price: new(big.Rat).Mul(inputPrice, rates[part.rate])})
			weighted.Add(weighted, new(big.Rat).Mul(part.count, rates[part.rate]))
			parts = append(parts, fmt.Sprintf("%s %s × 倍率 %s", part.label, part.count.Num(), e.Numbers[part.rate].Text))
		}
	}
	weighted.Add(weighted, new(big.Rat).Mul(big.NewRat(r.CompletionTokens, 1), rates["completion_ratio"]))
	*displayTerms = append(*displayTerms, portalBillingTerm{label: "输出", count: big.NewRat(r.CompletionTokens, 1), price: new(big.Rat).Mul(inputPrice, rates["completion_ratio"])})
	parts = append(parts, fmt.Sprintf("输出 %d × 输出倍率 %s", r.CompletionTokens, e.Numbers["completion_ratio"].Text))
	return weighted, meaning + "\n缓存创建缺省分项按生成器零值省略约定处理；不修改原始 Token。\n（" + strings.Join(parts, " + ") + "）", nil
}
