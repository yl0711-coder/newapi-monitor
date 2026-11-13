package monitor

import (
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"strings"
	"testing"
)

func tokenMeteringRawFixture(other string) tokenMeteringRaw {
	return tokenMeteringRaw{ID: 1, CreatedAt: 1_800_000_000, UserID: 2, ChannelID: 3, Model: "synthetic-model", Group: "actual-group",
		Input: sql.NullString{String: "100", Valid: true}, Output: sql.NullString{String: "10", Valid: true}, Quota: sql.NullString{String: "9007199254740993", Valid: true},
		Stream: sql.NullString{String: "1", Valid: true}, Other: sql.NullString{String: other, Valid: true}, OtherBytes: int64(len(other)), RequestID: "synthetic-request"}
}
func tokenMeteringConfig() Settings {
	return Settings{TokenFactsEnabled: true, TokenFactsSourceEpoch: "source-a", TokenFactsSourceMode: "complete", TokenFactsUsageAdapter: "newapi-rc26",
		TokenFactsSemanticBindings: `{"synthetic-openai":"openai_inclusive","synthetic-anthropic":"anthropic_native"}`,
		NginxEvidenceHMACKey:       strings.Repeat("k", 32), NginxEvidenceHMACKeyID: "key-1", NginxEvidenceMode: "verified", BackgroundSourceMinStartIntervalMS: 1}
}
func tokenMeteringDecode(t *testing.T, f TokenMeteringFact) tokenMeteringParsed {
	t.Helper()
	var p tokenMeteringParsed
	if err := json.Unmarshal([]byte(f.SourceFacts), &p); err != nil {
		t.Fatal(err)
	}
	return p
}
func TestTokenMeteringProtocolSemanticsAndMissing(t *testing.T) {
	tests := []struct {
		name, other, adapter, semantic, reason string
		total                                  *int64
	}{
		{"anthropic", `{"usage_semantic":"anthropic","claude":true,"cache_tokens":20,"cache_creation_tokens":30,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":15}`, "newapi-rc26", "anthropic_native", "explicit_source_semantic", ptrTokenMetering(150)},
		{"openai", `{"usage_semantic":"synthetic-openai","cache_tokens":20,"cache_creation_tokens":30}`, "unknown", "openai_inclusive", "explicit_bound_semantic", ptrTokenMetering(100)},
		{"unverified-claude", `{"claude":true,"cache_tokens":0,"cache_creation_tokens":0}`, "unknown", "unsupported", "adapter_unknown", nil},
		{"version-does-not-prove-semantic", `{"claude":true,"cache_tokens":20,"cache_creation_tokens":30}`, "newapi-rc26", "unsupported", "adapter_unknown", nil},
		{"explicit-openai-with-claude-protocol", `{"usage_semantic":"openai","claude":true,"cache_tokens":20,"cache_creation_tokens":30}`, "newapi-rc26", "openai_inclusive", "explicit_source_semantic", ptrTokenMetering(100)},
		{"false-format", `{"claude":false}`, "newapi-rc26", "unsupported", "adapter_unknown", nil},
		{"explicit-unknown", `{"usage_semantic":"future","claude":true,"cache_tokens":0,"cache_creation_tokens":0}`, "newapi-rc26", "unsupported", "explicit_semantic_unsupported", nil},
		{"missing-cache", `{"usage_semantic":"anthropic","claude":true}`, "newapi-rc26", "anthropic_native", "cache_fields_unusable", nil},
		{"null-cache", `{"usage_semantic":"anthropic","claude":true,"cache_tokens":null,"cache_creation_tokens":0}`, "newapi-rc26", "anthropic_native", "cache_fields_unusable", nil},
		{"ttl-conflict", `{"usage_semantic":"anthropic","claude":true,"cache_tokens":20,"cache_creation_tokens":10,"cache_creation_tokens_5m":10,"cache_creation_tokens_1h":10}`, "newapi-rc26", "anthropic_native", "cache_ttl_conflict", nil},
		{"redundant-write", `{"usage_semantic":"anthropic","cache_tokens":20,"cache_creation_tokens":10,"cache_write_tokens":10}`, "unknown", "anthropic_native", "explicit_source_semantic", ptrTokenMetering(130)},
		{"write-conflict", `{"usage_semantic":"anthropic","cache_tokens":20,"cache_creation_tokens":10,"cache_write_tokens":11}`, "unknown", "anthropic_native", "cache_total_conflict", nil},
		{"openai-overlapping-cache", `{"usage_semantic":"openai","cache_tokens":90,"cache_creation_tokens":11}`, "unknown", "openai_inclusive", "explicit_source_semantic", ptrTokenMetering(100)},
		{"openai-cache-overflow", `{"usage_semantic":"openai","cache_tokens":101}`, "unknown", "openai_inclusive", "cache_exceeds_input", nil},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			cfg := tokenMeteringConfig()
			cfg.TokenFactsUsageAdapter = tc.adapter
			f, _ := parseTokenMetering(cfg, tokenMeteringRawFixture(tc.other), 1_800_000_100)
			p := tokenMeteringDecode(t, f)
			if p.Semantic != tc.semantic || p.SemanticReason != tc.reason || !sameTokenMeteringLength(p.TotalInput, tc.total) {
				t.Fatalf("%+v", p)
			}
			if tc.name == "anthropic" && (p.UnassignedCacheWrite == nil || *p.UnassignedCacheWrite != 5) {
				t.Fatal("TTL remainder discarded")
			}
			if tc.name == "openai-overlapping-cache" {
				if rule := tokenFormatRule(p); !rule.Eligible || rule.Hit {
					t.Fatal("valid overlapping OpenAI cache fields became an anomaly", rule)
				}
			}
			if p.Fields["quota"].Value != "9007199254740993" {
				t.Fatal("quota lost integer precision")
			}
		})
	}
}
func ptrTokenMetering(v int64) *int64 { return &v }
func TestTokenMeteringWhitelistStrictPricingAndTiered(t *testing.T) {
	safe := base64.StdEncoding.EncodeToString([]byte("prompt_tokens * 0.1 + completion_tokens * 2"))
	other := `{"claude":true,"cache_tokens":"0","cache_creation_tokens":0,"model_ratio":"0.1","user_group_ratio":null,"billing_mode":"tiered_expr","expr_b64":"` + safe + `","request_rules":[],"matched_tier":0,"credit_usage":-2,"kiro_billable_input_tokens":-4,"authorization":"DO-NOT-PERSIST","prompt":"PRIVATE"}`
	f, refs := parseTokenMetering(tokenMeteringConfig(), tokenMeteringRawFixture(other), 1_800_000_100)
	p := tokenMeteringDecode(t, f)
	if strings.Contains(f.SourceFacts, "DO-NOT-PERSIST") || strings.Contains(f.SourceFacts, "PRIVATE") || strings.Contains(f.SourceFacts, "synthetic-request") {
		t.Fatal("unsafe payload retained")
	}
	if p.PricingSupport != "tiered_arithmetic_snapshot_only" || p.Fields["model_ratio"].Value != "1/10" || p.Fields["user_group_ratio"].State != "null" || p.Fields["credit_usage"].Value != "-2" {
		t.Fatalf("%+v", p)
	}
	if len(refs) != 1 || refs[0].Hash != nginxEvidenceIDHMAC(strings.Repeat("k", 32), "oneapi-request-id", "synthetic-request") {
		t.Fatal("HMAC differs")
	}
	unsafe := base64.StdEncoding.EncodeToString([]byte(`headers["Authorization"] == "DO-NOT-PERSIST"`))
	f, _ = parseTokenMetering(tokenMeteringConfig(), tokenMeteringRawFixture(`{"billing_mode":"tiered_expr","expr_b64":"`+unsafe+`","request_rules":[{"body":"PRIVATE"}]}`), 1)
	p = tokenMeteringDecode(t, f)
	if p.PricingSupport != "tiered_unsupported" || strings.Contains(f.SourceFacts, "PRIVATE") || strings.Contains(f.SourceFacts, unsafe) {
		t.Fatal("unsafe tiered retained or accepted")
	}
}
func TestTokenMeteringMalformedOtherAndScalarIsolation(t *testing.T) {
	for _, raw := range []string{`{"cache_tokens":1,"cache_tokens":0}`, `{"a":{"x":1,"x":2}}`, `{"cache_tokens":1.5}`, `{"cache_tokens":"SECRET"}`} {
		f, _ := parseTokenMetering(tokenMeteringConfig(), tokenMeteringRawFixture(raw), 1)
		p := tokenMeteringDecode(t, f)
		if p.Fields["quota"].State != "valid" || p.TotalInput != nil || strings.Contains(f.SourceFacts, "SECRET") {
			t.Fatalf("%+v", p)
		}
	}
	r := tokenMeteringRawFixture("{}")
	r.OtherBytes = tokenMeteringOtherBytes + 1
	f, _ := parseTokenMetering(tokenMeteringConfig(), r, 1)
	if p := tokenMeteringDecode(t, f); p.OtherState != "size_limit" {
		t.Fatal(p.OtherState)
	}
}
func TestTokenMeteringConfigClosedDefaultsAndKeyRotation(t *testing.T) {
	t.Setenv("MONITOR_TOKEN_FACTS_ENABLED", "")
	if LoadSettings().TokenFactsEnabled {
		t.Fatal("enabled by default")
	}
	cfg := tokenMeteringConfig()
	cfg.NginxEvidencePreviousHMACKeyID = "old"
	cfg.NginxEvidencePreviousHMACKey = strings.Repeat("p", 32)
	if err := validateTokenMeteringSettings(cfg); err != nil {
		t.Fatal(err)
	}
	_, refs := parseTokenMetering(cfg, tokenMeteringRawFixture("{}"), 1)
	if len(refs) != 2 {
		t.Fatal("previous key missing")
	}
	cfg.CloudWatchEvidenceHMACKeyID = "key-1"
	cfg.CloudWatchEvidenceHMACKey = strings.Repeat("x", 32)
	if validateTokenMeteringSettings(cfg) == nil {
		t.Fatal("same key id different secret accepted")
	}
	cfg = tokenMeteringConfig()
	cfg.TokenFactsSourceEpoch = ""
	if validateTokenMeteringSettings(cfg) == nil {
		t.Fatal("implicit source epoch")
	}
}
