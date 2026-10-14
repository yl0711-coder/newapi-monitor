package monitor

import (
	"encoding/base64"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func portalTierTestEvidence(expr, matched string) portalLogEvidence {
	raw, _ := json.Marshal(map[string]any{"billing_mode": "tiered_expr", "expr_b64": base64.StdEncoding.EncodeToString([]byte(expr)), "matched_tier": matched, "group_ratio": 0.35, "model_ratio": 0, "completion_ratio": 0})
	return parsePortalLogEvidence(string(raw))
}

func TestPortalRecordedTierPrices(t *testing.T) {
	const chain = `len <= 272000 ? tier("short", p * 2 + c * 12 + cr * 0.2 + cc * 2.5) : tier("long", p * 4 + c * 18 + cr * 0.4 + cc * 5)`
	for _, tc := range []struct {
		name, expr, matched string
		want                bool
		index               int
	}{
		{"first", chain, "short", true, 1},
		{"second", chain, "long", true, 2},
		{"version", "v1: " + chain, "short", true, 1},
		{"flat zero", `tier("base", p * 0 + c * 1e1)`, "base", true, 1},
		{"unknown version", "v2:" + chain, "short", false, 0},
		{"missing match", chain, "", false, 0},
		{"unknown match", chain, "other", false, 0},
		{"case differs", chain, "SHORT", false, 0},
		{"extra multiplier", chain + " * 2", "short", false, 0},
		{"rule", chain + `|||when(header("secret") == "secret") * 2`, "short", false, 0},
		{"unknown function", `max(tier("base", p * 2), 1)`, "base", false, 0},
		{"duplicate variable", `tier("base", p * 2 + p * 3)`, "base", false, 0},
		{"subtraction", `tier("base", p * 2 - c * 3)`, "base", false, 0},
		{"trailing operator", `tier("base", p * 2 +)`, "base", false, 0},
		{"missing else", `len < 10 ? tier("base", p * 2)`, "base", false, 0},
		{"duplicate label", `len < 10 ? tier("base", p * 2) : tier("base", p * 3)`, "base", false, 0},
		{"invalid other branch", `len < 10 ? tier("base", p * 2) : tier("long", p * 3 * 2)`, "base", false, 0},
		{"oversized number", `tier("base", p * 1e309)`, "base", false, 0},
		{"html label", `tier("<script>", p * 1)`, "<script>", false, 0},
		{"unknown variable", `tier("base", secret * 2)`, "base", false, 0},
		{"size limit", strings.Repeat(" ", 8193) + chain, "short", false, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			p, ok := portalRecordedTierPrices(portalTierTestEvidence(tc.expr, tc.matched))
			if ok != tc.want || (ok && p.Index != tc.index) {
				t.Fatalf("accepted=%v index=%d", ok, p.Index)
			}
		})
	}
}

func FuzzPortalTierPrices(f *testing.F) {
	f.Add(`len <= 272000 ? tier("short", p * 2 + c * 12 + cr * 0.2 + cc * 2.5) : tier("long", p * 4 + c * 18 + cr * 0.4 + cc * 5)`, "short")
	f.Add(`tier("base", p * 0 + c * 1e1)`, "base")
	f.Fuzz(func(t *testing.T, expr, label string) {
		e := portalTierTestEvidence(expr, label)
		prices, ok := portalRecordedTierPrices(e)
		if ok && (prices.Index < 1 || prices.Index > 16 || len(prices.Prices) == 0 || len(prices.Prices) > 9) {
			t.Fatal("unbounded/empty result")
		}
		_ = portalTierPriceItems(e)
		e.Strings["request_path"] = portalEvidenceField{State: portalEvidenceValid, Text: "/v1/responses"}
		e.Numbers["cache_tokens"] = portalEvidenceField{State: portalEvidenceValid, Text: "20"}
		r := LogRow{Type: 2, PromptTokens: 100, CompletionTokens: 10, billingQuota: 123, billingFactsValid: true}
		got := checkPortalBilling(r, e)
		if got.State != "matched" && got.Formula != "" {
			t.Fatal("unverified formula escaped")
		}
	})
}

func TestPortalTierPricesDoNotClaimChargeVerification(t *testing.T) {
	for _, expr := range []string{`tier("base", p * 0 + c * 10 + cr * 0.2)`, `tier("base", p * 2) * 2`} {
		e := portalTierTestEvidence(expr, "base")
		raw, _ := json.Marshal(map[string]any{"billing_mode": "tiered_expr", "expr_b64": base64.StdEncoding.EncodeToString([]byte(expr)), "matched_tier": "base", "group_ratio": 0.35, "model_ratio": 0, "completion_ratio": 0})
		r := LogRow{Type: 2, PromptTokens: 100, CompletionTokens: 10, billingQuota: 123, billingFactsValid: true, CostUSD: 123.0 / 500000}
		before := r
		populatePortalLogEvidence(&r, string(raw))
		if r.UsageDetails.Billing.State == "matched" || r.BillingProcess != "" || !r.UsageDetails.LegacyFormulaUnsafe {
			t.Fatal("tier treated as verified/free")
		}
		if r.billingQuota != before.billingQuota || r.CostUSD != before.CostUSD || r.PromptTokens != before.PromptTokens || r.CompletionTokens != before.CompletionTokens {
			t.Fatal("changed source money/tokens")
		}
		encoded, _ := json.Marshal(r.UsageDetails)
		if strings.Contains(string(encoded), "tier(") || strings.Contains(string(encoded), "expr_b64") {
			t.Fatal("raw expression leaked")
		}
		csv := portalCSVRecord(r)
		if !strings.Contains(csv[portalCSVSupplementColumn], "阶梯计费") || !strings.Contains(csv[portalCSVBillingColumn], r.UsageDetails.Billing.Message) || strings.Contains(csv[portalCSVBillingColumn], "tier(") {
			t.Fatal("CSV/display disagree")
		}
		prices := portalTierPriceItems(e)
		if len(prices) > 0 && !strings.Contains(csv[portalCSVSupplementColumn], "未乘分组倍率") {
			t.Fatal("unqualified base price")
		}
		if !reflect.DeepEqual(prices, portalTierPriceItems(e)) {
			t.Fatal("unstable item order")
		}
	}
}
