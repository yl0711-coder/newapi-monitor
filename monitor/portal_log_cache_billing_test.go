package monitor

import (
	"fmt"
	"strings"
	"testing"
)

func TestPortalCacheRatioBillingCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, fields    string
		p, c, quota     int64
		state, fragment string
	}{
		{"default read", `"cache_tokens":20,"cache_ratio":0.1`, 100, 10, 122, "matched", "普通输入 80"},
		{"explicit openai", `"usage_semantic":"openai","cache_tokens":20,"cache_ratio":0.1`, 100, 10, 122, "matched", "缓存读取 20"},
		{"total write", `"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25`, 100, 10, 132, "matched", "普通输入 40"},
		{"anthropic", `"usage_semantic":"anthropic","claude":true,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25`, 100, 10, 192, "matched", "输入不含缓存"},
		{"anthropic TTL remainder", `"usage_semantic":"anthropic","cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":20,"cache_creation_ratio":1.25,"cache_creation_ratio_5m":1.25,"cache_creation_ratio_1h":2`, 100, 10, 207, "matched", "未按 TTL 拆分的缓存创建 10"},
		{"TTL complete no unused rate", `"usage_semantic":"anthropic","cache_tokens":0,"cache_creation_tokens":30,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":20,"cache_creation_ratio_5m":1.25,"cache_creation_ratio_1h":2`, 100, 10, 193, "matched", "1 小时缓存创建 20"},
		{"unknown Claude", `"claude":true,"cache_tokens":20,"cache_ratio":0.1`, 100, 10, 142, "incomplete", ""},
		{"conflicting format", `"usage_semantic":"openai","claude":true,"cache_tokens":20,"cache_ratio":0.1`, 100, 10, 122, "incomplete", ""},
		{"invalid flag", `"claude":null,"cache_tokens":20,"cache_ratio":0.1`, 100, 10, 122, "incomplete", ""},
		{"unknown semantic", `"usage_semantic":"other","cache_tokens":20,"cache_ratio":0.1`, 100, 10, 122, "incomplete", ""},
		{"missing read", `"cache_creation_tokens":20,"cache_creation_ratio":1.25`, 100, 10, 145, "incomplete", ""},
		{"null count", `"cache_tokens":null`, 100, 10, 140, "incomplete", ""},
		{"negative count", `"cache_tokens":-20,"cache_ratio":0.1`, 100, 10, 140, "incomplete", ""},
		{"missing rate", `"cache_tokens":20`, 100, 10, 120, "incomplete", ""},
		{"null rate", `"cache_tokens":20,"cache_ratio":null`, 100, 10, 120, "incomplete", ""},
		{"zero cache rate is real", `"cache_tokens":20,"cache_ratio":0`, 100, 10, 120, "matched", "倍率 0"},
		{"fractional token invalid", `"cache_tokens":1.5,"cache_ratio":0.1`, 100, 10, 139, "incomplete", ""},
		{"split total missing", `"usage_semantic":"anthropic","cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens_5m":10,"cache_creation_ratio_5m":1.25`, 100, 10, 155, "incomplete", ""},
		{"zero split does not prove total", `"cache_tokens":0,"cache_creation_tokens_5m":0`, 100, 10, 140, "incomplete", ""},
		{"split exceeds total", `"usage_semantic":"anthropic","cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":5,"cache_creation_tokens_5m":10,"cache_creation_ratio_5m":1.25`, 100, 10, 155, "incomplete", ""},
		{"default TTL ambiguous", `"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":10,"cache_creation_tokens_5m":10,"cache_creation_ratio_5m":1.25`, 100, 10, 155, "incomplete", ""},
		{"overlap version difference", `"cache_tokens":90,"cache_ratio":0.1,"cache_creation_tokens":20,"cache_creation_ratio":1.25`, 100, 10, 74, "unsupported", ""},
		{"exactly full cache", `"cache_tokens":100,"cache_ratio":0.1`, 100, 10, 50, "matched", "普通输入 0"},
		{"zero priced cache minimum", `"cache_tokens":100,"cache_ratio":0`, 100, 0, 1, "matched", "最小扣费 1"},
		{"different charge", `"cache_tokens":20,"cache_ratio":0.1`, 100, 10, 123, "mismatch", ""},
		{"extra decimal precision", `"cache_tokens":20,"cache_ratio":0.100000000000000001`, 100, 10, 122, "incomplete", ""},
		{"special rules", `"cache_tokens":20,"cache_ratio":0.1,"other_ratios":{"rate":2}`, 100, 10, 244, "unsupported", ""},
		{"tool mixed", `"cache_tokens":20,"cache_ratio":0.1,"tool_surcharges":[]`, 100, 10, 122, "unsupported", ""},
		{"per call mixed", `"cache_tokens":20,"cache_ratio":0.1,"model_price":0.1,"request_path":"/v1/responses"`, 100, 10, 50000, "unsupported", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			raw := `{"model_ratio":1,"group_ratio":1,"completion_ratio":4,` + tc.fields + `}`
			r := LogRow{Type: 2, PromptTokens: tc.p, CompletionTokens: tc.c, CostUSD: float64(tc.quota) / quotaPerUSD, billingQuota: tc.quota, billingFactsValid: true}
			populatePortalLogEvidence(&r, raw)
			got := r.UsageDetails.Billing
			if got.State != tc.state || (tc.fragment != "" && !strings.Contains(got.Formula, tc.fragment)) {
				t.Fatalf("%+v", got)
			}
			if tc.state != "matched" && got.Formula != "" {
				t.Fatal("unverified formula published")
			}
			if r.PromptTokens != tc.p || r.CompletionTokens != tc.c || r.billingQuota != tc.quota || r.CostUSD != float64(tc.quota)/quotaPerUSD {
				t.Fatal("changed original facts")
			}
			csv := portalCSVRecord(r)
			if csv[portalCSVBillingColumn] != portalCSVReadableBilling(r) || (got.State != "matched" && !strings.Contains(csv[portalCSVBillingColumn], got.Message)) {
				t.Fatal("API/CSV diverged")
			}
		})
	}
}

func BenchmarkPortalCacheRatioBilling(b *testing.B) {
	raw := `{"model_ratio":1,"group_ratio":1,"completion_ratio":4,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25}`
	b.ReportAllocs()
	for b.Loop() {
		r := LogRow{Type: 2, PromptTokens: 100, CompletionTokens: 10, billingQuota: 132, billingFactsValid: true}
		populatePortalLogEvidence(&r, raw)
		if r.UsageDetails.Billing.State != "matched" {
			b.Fatal("formula regression")
		}
	}
}

func TestPortalCacheRatioIntegerArithmetic(t *testing.T) {
	// Independent integer numerators: read 1/10, write 5/4, completion 4,
	// model 1/2 and actual group 3/2, rounded once at the end.
	for p := int64(1); p <= 24; p++ {
		for read := int64(0); read <= p; read++ {
			write := (p - read) / 2
			if read+write == 0 {
				continue
			}
			for _, anthropic := range []bool{false, true} {
				base := p - read - write
				semantic := "openai"
				if anthropic {
					base, semantic = p, "anthropic"
				}
				numerator := (base*20 + read*2 + write*25 + 2*4*20) * 3
				quota := (numerator + 40) / 80
				if quota == 0 {
					quota = 1
				}
				raw := fmt.Sprintf(`{"model_ratio":0.5,"group_ratio":1.5,"user_group_ratio":1.5,"completion_ratio":4,"usage_semantic":%q,"cache_tokens":%d,"cache_ratio":0.1,"cache_creation_tokens":%d,"cache_creation_ratio":1.25}`, semantic, read, write)
				got := checkPortalBilling(LogRow{PromptTokens: p, CompletionTokens: 2, billingQuota: quota, billingFactsValid: true}, parsePortalLogEvidence(raw))
				if got.State != "matched" {
					t.Fatalf("p=%d read=%d write=%d semantic=%s q=%d: %+v", p, read, write, semantic, quota, got)
				}
			}
		}
	}
}
