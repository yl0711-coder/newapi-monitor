package monitor

import (
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
)

// Synthetic fixtures only: no production identities, keys, prompts or content.
func TestPortalLogEvidenceCompatibility(t *testing.T) {
	for _, tc := range []struct{ name, raw, mode string }{
		{"legacy ratio", `{"model_ratio":2.5,"completion_ratio":4,"group_ratio":1.2}`, "ratio"},
		{"legacy sentinel", `{"model_price":-1,"model_ratio":2.5}`, "ratio"},
		{"zero ratio", `{"model_ratio":0}`, "ratio"},
		{"per request", `{"model_price":0.1}`, "per_request"},
		{"claude", `{"claude":true,"model_ratio":2.5,"cache_tokens":10}`, "ratio"},
		{"rc26 tier", `{"billing_mode":"tiered_expr","model_ratio":0,"model_price":0}`, "tiered_expr"},
		{"future mode", `{"billing_mode":"future","model_ratio":1}`, "unknown"},
		{"bad mode", `{"billing_mode":17,"model_ratio":1}`, "unknown"},
		{"null mode", `{"billing_mode":null,"model_ratio":1}`, "unknown"},
		{"bad price", `{"model_price":"1","model_ratio":1}`, "unknown"},
		{"negative price", `{"model_price":-2,"model_ratio":1}`, "unknown"},
		{"task", `{"is_task":true,"model_price":-1,"model_ratio":1}`, "unknown"},
		{"task ID", `{"task_id":"synthetic-task","model_price":-1,"model_ratio":1}`, "unknown"},
		{"subscription", `{"billing_source":"subscription","model_ratio":1}`, "ratio"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := parsePortalLogEvidence(tc.raw)
			if e.State != portalEvidenceValid || e.Mode != tc.mode {
				t.Fatalf("state=%s mode=%s", e.State, e.Mode)
			}
		})
	}
}

func TestPortalLogEvidenceRequestRules(t *testing.T) {
	e := parsePortalLogEvidence(`{"billing_mode":"tiered_expr","request_rules":[{"cond":"service_tier == priority","multiplier":1.5,"matched":false,"admin_info":"secret"}]}`)
	if e.RulesState != portalEvidenceValid || len(e.Rules) != 1 || e.Rules[0].Matched.Text != "false" || e.Rules[0].Multiplier.Text != "1.5" {
		t.Fatalf("rules: %+v", e)
	}
	for _, rules := range []string{`{}`, `[null]`, `[{"cond":"x","multiplier":-1,"matched":true}]`, `[{"cond":"x","multiplier":1}]`, `[{"cond":"x","cond":"y"}]`} {
		e = parsePortalLogEvidence(`{"billing_mode":"tiered_expr","request_rules":` + rules + `}`)
		if e.RulesState != portalEvidenceInvalid || e.Mode != "tiered_expr" {
			t.Fatalf("bad rules accepted: %s", rules)
		}
	}
}

func TestPortalLogEvidencePresenceAndPrecision(t *testing.T) {
	e := parsePortalLogEvidence(`{"cache_tokens":0,"cache_ratio":null,"cache_creation_tokens":"17","cache_creation_tokens_5m":9007199254740993,"cache_creation_tokens_1h":2.5,"group_ratio":0.000000123456789123456789,"subscription_post_delta":-5,"model_price":1e-7}`)
	for key, state := range map[string]portalEvidenceState{
		"cache_tokens": portalEvidenceValid, "cache_ratio": portalEvidenceNull,
		"cache_creation_tokens": portalEvidenceInvalid, "cache_creation_tokens_5m": portalEvidenceValid,
		"cache_creation_tokens_1h": portalEvidenceInvalid, "model_ratio": portalEvidenceMissing,
		"subscription_post_delta": portalEvidenceValid, "model_price": portalEvidenceValid,
	} {
		if e.Numbers[key].State != state {
			t.Fatalf("%s: %+v", key, e.Numbers[key])
		}
	}
	if e.Numbers["cache_creation_tokens_5m"].Text != "9007199254740993" || e.Numbers["group_ratio"].Text != "0.000000123456789123456789" {
		t.Fatal("precision lost")
	}
}

func TestPortalLogEvidenceMalformedIsolation(t *testing.T) {
	for _, raw := range []string{`[]`, `true`, `{"model_ratio":1`, `{} {}`, `{"model_ratio":1,"model_ratio":2}`, strings.Repeat("x", portalEvidenceMaxBytes+1)} {
		if parsePortalLogEvidence(raw).State != portalEvidenceInvalid {
			t.Fatalf("accepted malformed input length=%d", len(raw))
		}
	}
	for _, raw := range []string{`-1`, `1.2`, `9223372036854775808`, `1e99999999`, `"1"`, `true`, `{}`} {
		e := parsePortalLogEvidence(`{"cache_tokens":` + raw + `,"group_ratio":1.2}`)
		if e.Numbers["cache_tokens"].State != portalEvidenceInvalid || e.Numbers["group_ratio"].Text != "1.2" {
			t.Fatal("bad field contaminated valid sibling")
		}
	}
	if parsePortalLogEvidence("").State != portalEvidenceMissing || parsePortalLogEvidence("null").State != portalEvidenceNull {
		t.Fatal("missing/null distinction lost")
	}
}

func TestPortalLogEvidenceExpressionAndAllowlist(t *testing.T) {
	expr := `tier("short", p <= 1000, 2 * p + 5 * c)`
	encoded := base64.StdEncoding.EncodeToString([]byte(expr))
	e := parsePortalLogEvidence(fmt.Sprintf(`{"billing_mode":"tiered_expr","expr_b64":%q,"matched_tier":"short","admin_info":{"api_key":"secret"},"channel_name":"secret","request_rules":[{"arbitrary":"secret"}],"quota":999}`, encoded))
	if e.Expression.State != portalEvidenceValid || e.Expression.Text != expr {
		t.Fatal("historical expression lost")
	}
	if strings.Contains(fmt.Sprintf("%+v", e), "secret") {
		t.Fatal("non-allowlisted data retained")
	}
	for _, expr := range []string{"", "invalid base64", "/w=="} {
		e = parsePortalLogEvidence(fmt.Sprintf(`{"billing_mode":"tiered_expr","expr_b64":%q}`, expr))
		if e.Mode != "tiered_expr" || e.Expression.State != portalEvidenceInvalid {
			t.Fatal("invalid expression became standard billing")
		}
	}
}

func FuzzPortalLogEvidence(f *testing.F) {
	f.Add(`{"model_ratio":1,"group_ratio":1,"completion_ratio":4,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25}`)
	f.Add(`{"model_ratio":1,"group_ratio":1,"completion_ratio":4,"usage_semantic":"anthropic","cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":20,"cache_creation_ratio":1.25,"cache_creation_ratio_5m":1.25,"cache_creation_ratio_1h":2}`)
	f.Add(`{"audio":true,"audio_input":0,"audio_output":9007199254740993,"text_input":null,"audio_ratio":4}`)
	f.Add(`{"image":true,"image_output":50,"image_ratio":1.5,"image_generation_call_price":0.04}`)
	f.Add(`{"tool_surcharges":[{"name":"image_generation","count":2,"price":40},{"name":"private-tool","count":3,"price":10}],"image_generation_call_price":0.04}`)
	f.Add(`{"frt":0,"stream_status":{"status":"error","end_reason":"client_gone","error_count":1,"end_error":"synthetic-private-error"},"is_system_prompt_overwritten":true}`)
	for _, raw := range []string{"", "null", "{}", `{"model_ratio":1}`, `{"billing_mode":"tiered_expr","expr_b64":"eA=="}`, `{"cache_creation_tokens":100,"cache_creation_tokens_5m":60}`, `{"tool_surcharges":[{"name":"web_search","count":2,"price":10}]}`, `{"model_ratio":1,"group_ratio":1,"completion_ratio":1}`, `{"model_ratio":1,"group_ratio":1,"model_price":0.2,"request_path":"/v1/responses"}`} {
		f.Add(raw)
	}
	f.Fuzz(func(t *testing.T, raw string) {
		e := parsePortalLogEvidence(raw)
		if e.Mode == "" || e.State == "" {
			t.Fatal("undefined parse state")
		}
		cache := portalCacheWriteEvidence(e)
		if cache.State == "" || cache.TotalSource == "" {
			t.Fatal("undefined cache evidence state")
		}
		_ = buildPortalLogDetails(2, raw) // customer projection must tolerate every parser input
		r := LogRow{Type: 2, IsStream: true, CostUSD: 0.25, PromptTokens: 123, CompletionTokens: 45, billingQuota: 125000, billingFactsValid: true}
		populatePortalLogEvidence(&r, raw)
		if r.CostUSD != 0.25 || r.PromptTokens != 123 || r.CompletionTokens != 45 || len(portalCSVRecord(r)) != len(portalCSVHeader()) {
			t.Fatal("evidence changed original charge/tokens or export schema")
		}
	})
}

func BenchmarkPortalLogEvidence(b *testing.B) {
	raw := `{"model_price":-1,"model_ratio":2.5,"group_ratio":1.2,"completion_ratio":4,"cache_tokens":100,"cache_ratio":0.1}`
	b.ReportAllocs()
	for b.Loop() {
		parsePortalLogEvidence(raw)
	}
}
