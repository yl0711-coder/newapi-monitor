package monitor

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"testing"
)

func TestPortalTierBillingBoundaries(t *testing.T) {
	const linear = `tier("base", p * 2 + c * 10 + cr * 0.2 + cc * 2.5)`
	for _, tc := range []struct {
		name, expr      string
		fields          map[string]any
		p, c, q         int64
		state, fragment string
	}{
		{"default cache", linear, nil, 100, 10, 67, "matched", "max(0, 原始输入 100 − 缓存读取 20 − 缓存创建 10) = 70"},
		{"explicit openai", linear, map[string]any{"usage_semantic": "openai"}, 100, 10, 67, "matched", "计价输入 70"},
		{"no double personal", linear, map[string]any{"user_group_ratio": 0.5}, 100, 10, 67, "matched", "分组倍率 0.5"},
		{"wrong personal", linear, map[string]any{"user_group_ratio": 0.7}, 100, 10, 67, "incomplete", ""},
		{"free group", linear, map[string]any{"group_ratio": 0}, 100, 10, 0, "matched", "$0.000000"},
		{"missing read", linear, map[string]any{"cache_tokens": nil}, 100, 10, 67, "incomplete", ""},
		{"null write", linear, map[string]any{"cache_creation_tokens": nil}, 100, 10, 67, "incomplete", ""},
		{"cache overlap", linear, map[string]any{"cache_tokens": 90, "cache_creation_tokens": 20}, 100, 10, 42, "matched", "计价输入 0"},
		{"zero usage", linear, nil, 0, 0, 0, "incomplete", ""},
		{"wrong charge", linear, nil, 100, 10, 68, "mismatch", ""},
		{"old claude ambiguous", linear, map[string]any{"claude": true}, 100, 10, 67, "incomplete", ""},
		{"unknown semantic", linear, map[string]any{"usage_semantic": "future"}, 100, 10, 67, "incomplete", ""},
		{"null semantic", linear, map[string]any{"usage_semantic": nil}, 100, 10, 67, "incomplete", ""},
		{"anthropic unsplit", linear, map[string]any{"usage_semantic": "anthropic"}, 100, 10, 67, "incomplete", ""},
		{"anthropic split", `tier("base", p * 2 + c * 10 + cr * 0.2 + cc * 2.5 + cc1h * 4)`, map[string]any{"usage_semantic": "anthropic", "cache_creation_tokens_5m": 6, "cache_creation_tokens_1h": 4}, 100, 10, 84, "matched", "上下文长度 130"},
		{"openai split", linear, map[string]any{"cache_creation_tokens_5m": 10}, 100, 10, 67, "incomplete", ""},
		{"split exceeds total", linear, map[string]any{"cache_creation_tokens_5m": 11}, 100, 10, 67, "incomplete", ""},
		{"media variable", `tier("base", p * 2 + img * 0)`, nil, 100, 10, 67, "unsupported", ""},
		{"subscription", linear, map[string]any{"billing_source": "subscription"}, 100, 10, 67, "unsupported", ""},
		{"tools", linear, map[string]any{"tool_surcharges": []any{}}, 100, 10, 67, "unsupported", ""},
		{"request rule", linear, map[string]any{"request_rules": []any{}}, 100, 10, 67, "unsupported", ""},
		{"nested saturation", linear, map[string]any{"admin_info": map[string]any{"quota_saturation": map[string]any{"private": "secret"}}}, 100, 10, 67, "unsupported", ""},
		{"task path", linear, map[string]any{"request_path": "/v1/videos"}, 100, 10, 67, "incomplete", ""},
		{"integer precision", linear, nil, (1 << 53) + 1, 10, 67, "unsupported", ""},
		{"quota bound", `tier("base", p * 1e308)`, nil, 100, 10, 2147483647, "unsupported", ""},
		{"float half boundary", `tier("base", p * 8.2)`, map[string]any{"group_ratio": 1}, 15, 0, 61, "unsupported", ""},
		{"float threshold boundary", `len <= 9007199254740991.5 ? tier("short", p * 0) : tier("base", p * 0)`, nil, 1 << 53, 0, 0, "unsupported", ""},
		{"no minimum", `tier("base", p * 0.1)`, nil, 1, 0, 0, "matched", "阶梯不套用普通倍率最低"},
		{"all branches affect input", `len <= 100 ? tier("base", p * 2 + c * 10) : tier("long", p * 4 + c * 20 + cr * 0.4)`, nil, 100, 10, 65, "matched", "计价输入 80"},
		{"wrong tier", `len < 100 ? tier("base", p * 2) : tier("long", p * 2)`, nil, 100, 0, 50, "mismatch", ""},
		{"long tier", `len <= 100 ? tier("short", p * 2) : tier("base", p * 4)`, nil, 101, 0, 101, "matched", "第 2 档"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]any{"billing_mode": "tiered_expr", "expr_b64": base64.StdEncoding.EncodeToString([]byte(tc.expr)), "matched_tier": "base", "group_ratio": 0.5, "user_group_ratio": -1, "model_ratio": 0, "completion_ratio": 0, "cache_tokens": 20, "cache_creation_tokens": 10, "request_path": "/v1/responses", "billing_source": "wallet"}
			for k, v := range tc.fields {
				fields[k] = v
			}
			raw, _ := json.Marshal(fields)
			r := LogRow{Type: 2, PromptTokens: tc.p, CompletionTokens: tc.c, billingQuota: tc.q, billingFactsValid: true, CostUSD: float64(tc.q) / quotaPerUSD}
			populatePortalLogEvidence(&r, string(raw))
			got := r.UsageDetails.Billing
			if got.State != tc.state || (tc.fragment != "" && !strings.Contains(got.Formula, tc.fragment)) {
				t.Fatalf("got %+v, want %s / %s", got, tc.state, tc.fragment)
			}
			if tc.state != "matched" && got.Formula != "" {
				t.Fatal("unverified formula exposed")
			}
			if r.billingQuota != tc.q || r.CostUSD != float64(tc.q)/quotaPerUSD || r.PromptTokens != tc.p || r.CompletionTokens != tc.c {
				t.Fatal("changed original charge or tokens")
			}
			if portalCSVRecord(r)[portalCSVBillingColumn] != portalCSVReadableBilling(r) {
				t.Fatal("CSV formula diverged")
			}
			out, _ := json.Marshal(r.UsageDetails)
			if strings.Contains(string(out), "secret") {
				t.Fatal("private clamp metadata leaked")
			}
		})
	}
}

func TestPortalTierCacheArithmeticAgainstIntegerOracle(t *testing.T) {
	// Independent integer arithmetic: half of p*2 + c*10 + cr*0.2.
	// A cache variable in the expression removes it from p even at zero price.
	for prompt := int64(1); prompt <= 40; prompt++ {
		for cached := int64(0); cached <= prompt; cached++ {
			q := ((prompt-cached)*10 + int64(3)*50 + cached + 5) / 10
			expr := `tier("base", p * 2 + c * 10 + cr * 0.2)`
			raw := fmt.Sprintf(`{"billing_mode":"tiered_expr","expr_b64":%q,"matched_tier":"base","group_ratio":1,"cache_tokens":%d,"request_path":"/v1/responses"}`, base64.StdEncoding.EncodeToString([]byte(expr)), cached)
			// q is rounded half(expression), hence weighted tenths above.
			r := LogRow{Type: 2, PromptTokens: prompt, CompletionTokens: 3, billingQuota: q, billingFactsValid: true}
			got := checkPortalBilling(r, parsePortalLogEvidence(raw))
			if got.State != "matched" && !(got.State == "unsupported" && strings.Contains(got.Message, "浮点舍入")) {
				t.Fatalf("p=%d cr=%d q=%d result=%+v", prompt, cached, q, got)
			}
		}
	}
}

func TestPortalTierAbsentCacheCreationIsExplicitDefault(t *testing.T) {
	raw := `{"billing_mode":"tiered_expr","expr_b64":"` + base64.StdEncoding.EncodeToString([]byte(`tier("base", p * 2 + cr * 0.2)`)) + `","matched_tier":"base","cache_tokens":20,"group_ratio":1,"request_path":"/v1/responses"}`
	r := LogRow{Type: 2, PromptTokens: 100, billingQuota: 82, billingFactsValid: true}
	got := checkPortalBilling(r, parsePortalLogEvidence(raw))
	if got.State != "matched" || !strings.Contains(got.Formula, "未记录的缓存创建项按 0") {
		t.Fatalf("%+v", got)
	}
}
