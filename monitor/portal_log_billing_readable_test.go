package monitor

import (
	"encoding/base64"
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
)

func TestPortalReadableBillingUsesVerifiedComponentsOnly(t *testing.T) {
	tier := base64.StdEncoding.EncodeToString([]byte(`tier("short",p*2+c*10+cr*0.1+cc*2.5)`))
	for _, tc := range []struct {
		name, raw string
		p, c, q   int64
		fragments []string
	}{
		{"plain", `{"model_ratio":2,"completion_ratio":3,"group_ratio":1.5}`, 100, 20, 480, []string{"输入价格：$4 / 1M tokens", "输出价格：$12 / 1M tokens", "（输入 100 tokens / 1M tokens × $4 + 输出 20 tokens / 1M tokens × $12）× 分组倍率 1.5 = $0.000960", "实际扣费：$0.000960"}},
		{"per-call", `{"model_price":0.2,"model_ratio":1,"group_ratio":1.5,"request_path":"/v1/responses"}`, 100, 20, 150000, []string{"模型价格：$0.2 / 次", "（$0.2 / 次 × 1 次）× 分组倍率 1.5 = $0.300000", "实际扣费：$0.300000"}},
		{"cache", `{"model_ratio":1,"completion_ratio":4,"group_ratio":1,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25}`, 100, 10, 132, []string{"输入价格：$2 / 1M tokens", "输出价格：$8 / 1M tokens", "缓存读取价格：$0.2 / 1M tokens", "缓存创建价格：$2.5 / 1M tokens", "输入 40 tokens", "缓存读取 20 tokens", "缓存创建 40 tokens", "输出 10 tokens", "实际扣费：$0.000264"}},
		{"anthropic-split", `{"usage_semantic":"anthropic","model_ratio":1,"completion_ratio":4,"group_ratio":1,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":20,"cache_creation_ratio":1.25,"cache_creation_ratio_5m":1.25,"cache_creation_ratio_1h":2}`, 100, 10, 207, []string{"输入 100 tokens", "缓存创建 10 tokens", "5m缓存创建 10 tokens", "1h缓存创建 20 tokens", "实际扣费：$0.000414"}},
		{"tier-real-values", fmt.Sprintf(`{"billing_mode":"tiered_expr","expr_b64":%q,"matched_tier":"short","group_ratio":0.35,"cache_tokens":52480,"request_path":"/v1/responses"}`, tier), 52696, 1831, 4198, []string{"输入价格：$2 / 1M tokens", "输出价格：$10 / 1M tokens", "缓存读取价格：$0.1 / 1M tokens", "（输入 216 tokens / 1M tokens × $2 + 缓存读取 52480 tokens / 1M tokens × $0.1 + 输出 1831 tokens / 1M tokens × $10）× 分组倍率 0.35 ≈ $0.008396", "实际扣费：$0.008396"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, PromptTokens: tc.p, CompletionTokens: tc.c, CostUSD: float64(tc.q) / quotaPerUSD, billingQuota: tc.q, billingFactsValid: true}
			before := r
			check := checkPortalBilling(r, parsePortalLogEvidence(tc.raw))
			if check.State != "matched" || !reflect.DeepEqual(r, before) {
				t.Fatalf("verification/facts changed: %+v", check)
			}
			for _, s := range tc.fragments {
				if !strings.Contains(check.Readable, s) {
					t.Fatalf("missing %q in %s", s, check.Readable)
				}
			}
			for _, s := range []string{"500000", "额度", "生成器", "max(", "TTL"} {
				if strings.Contains(check.Readable, s) {
					t.Fatalf("internal detail in customer explanation: %s", check.Readable)
				}
			}
			if !strings.HasSuffix(check.Readable, "仅供参考，以实际扣费为准。") || strings.Contains(check.Readable, "→") {
				t.Fatal("RC4-style structure not retained")
			}
			populatePortalLogEvidence(&r, tc.raw)
			csv := portalCSVRecord(r)
			if csv[portalCSVBillingColumn] != check.Readable || strings.Contains(csv[portalCSVBillingColumn], "详细核验依据") || csv[9] != fmt.Sprintf("%.6f", before.CostUSD) {
				t.Fatal("CSV description or original charge diverged")
			}
			wrong := before
			wrong.billingQuota++
			if got := checkPortalBilling(wrong, parsePortalLogEvidence(tc.raw)); got.Readable != "" {
				t.Fatal("unverified summary published")
			}
		})
	}
}

func TestPortalReadableBillingDisplayRoundingIsExplicit(t *testing.T) {
	for _, tc := range []struct{ value, want string }{{"0", "0"}, {"2", "2"}, {"1/10", "0.1"}, {"1/1000000000", "<0.00000001"}, {"1/3", "≈0.33333333"}} {
		n, _ := new(big.Rat).SetString(tc.value)
		before := n.RatString()
		if got := portalBillingDisplayNumber(n); got != tc.want {
			t.Fatalf("%s -> %s", tc.value, got)
		}
		if n.RatString() != before {
			t.Fatal("display modified exact input")
		}
	}
}

func TestPortalReadableBillingRC4MinimumAndZero(t *testing.T) {
	for _, tc := range []struct {
		raw      string
		q        int64
		fragment string
	}{
		{`{"model_ratio":0.49,"completion_ratio":1,"group_ratio":1}`, 1, "≈ $0.000002\n实际扣费：$0.000002（已按平台最低计费单位处理）"},
		{`{"model_ratio":1,"completion_ratio":1,"group_ratio":0}`, 0, "× 分组倍率 0 = $0.000000\n实际扣费：$0.000000"},
		{`{"model_ratio":0,"completion_ratio":1,"group_ratio":1}`, 0, "输入价格：$0 / 1M tokens"},
	} {
		r := LogRow{Type: 2, PromptTokens: 1, billingQuota: tc.q, billingFactsValid: true}
		check := checkPortalBilling(r, parsePortalLogEvidence(tc.raw))
		if check.State != "matched" || !strings.Contains(check.Readable, tc.fragment) {
			t.Fatalf("zero/minimum display lost: %+v", check)
		}
	}
}
