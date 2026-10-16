package monitor

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestPortalCacheWriteCompatibility(t *testing.T) {
	for _, tc := range []struct {
		name, raw, total, unsplit, source, issue string
		state                                    portalEvidenceState
	}{
		{"missing", `{}`, "", "", "unknown", "", portalEvidenceMissing},
		{"old total", `{"cache_creation_tokens":100}`, "100", "100", "recorded", "", portalEvidenceValid},
		{"zero", `{"cache_creation_tokens":0}`, "0", "0", "recorded", "", portalEvidenceValid},
		{"split exact", `{"cache_creation_tokens":100,"cache_creation_tokens_5m":60,"cache_creation_tokens_1h":40}`, "100", "0", "recorded", "", portalEvidenceValid},
		{"split remainder", `{"cache_creation_tokens":100,"cache_creation_tokens_5m":60,"cache_creation_tokens_1h":30}`, "100", "10", "recorded", "", portalEvidenceValid},
		{"split partial", `{"cache_creation_tokens_5m":60}`, "60", "", "split_only", "cache_write_total_not_recorded", portalEvidenceValid},
		{"over total", `{"cache_creation_tokens":50,"cache_creation_tokens_5m":60}`, "", "", "unknown", "cache_write_split_exceeds_total", portalEvidenceInvalid},
		{"null", `{"cache_creation_tokens":null}`, "", "", "unknown", "cache_write_field_invalid", portalEvidenceInvalid},
		{"bad split", `{"cache_creation_tokens":50,"cache_creation_tokens_5m":"10"}`, "", "", "unknown", "cache_write_field_invalid", portalEvidenceInvalid},
		{"overflow", `{"cache_creation_tokens_5m":9223372036854775807,"cache_creation_tokens_1h":1}`, "", "", "unknown", "cache_write_sum_overflow", portalEvidenceInvalid},
		{"exponent", `{"cache_creation_tokens":1e2,"cache_creation_tokens_5m":6e1}`, "1e2", "40", "recorded", "", portalEvidenceValid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			e := parsePortalLogEvidence(tc.raw)
			before := fmt.Sprintf("%+v", e)
			r := portalCacheWriteEvidence(e)
			if r.State != tc.state || r.Total.Text != tc.total || r.Unsplit.Text != tc.unsplit || r.TotalSource != tc.source || r.Issue != tc.issue {
				t.Fatalf("unexpected cache evidence: %+v", r)
			}
			if before != fmt.Sprintf("%+v", e) {
				t.Fatal("modified original token evidence")
			}
		})
	}
}

func TestPortalToolEvidenceCompatibility(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		state portalEvidenceState
		count int
	}{
		{`{}`, portalEvidenceMissing, 0},
		{`{"web_search":true,"web_search_call_count":2,"web_search_price":10}`, portalEvidenceMissing, 0},
		{`{"tool_surcharges":null}`, portalEvidenceNull, 0},
		{`{"tool_surcharges":[]}`, portalEvidenceValid, 0},
		{`{"tool_surcharges":[{"name":"web_search","count":2,"price":10}]}`, portalEvidenceValid, 1},
		{`{"tool_surcharges":[{"name":"web_search","count":0,"price":0}]}`, portalEvidenceValid, 1},
		{`{"tool_surcharges":[{"name":"web_search","count":-1,"price":10}]}`, portalEvidenceInvalid, 1},
		{`{"tool_surcharges":[{"name":" ","count":1,"price":10}]}`, portalEvidenceInvalid, 1},
		{`{"tool_surcharges":[{"name":"web_search","count":1,"price":"10"}]}`, portalEvidenceInvalid, 1},
		{`{"tool_surcharges":[null]}`, portalEvidenceInvalid, 0},
		{`{"tool_surcharges":[{"name":"x","name":"y","count":1,"price":1}]}`, portalEvidenceInvalid, 0},
	} {
		e := parsePortalLogEvidence(tc.raw)
		if e.ToolsState != tc.state || len(e.Tools) != tc.count {
			t.Fatalf("unexpected tool evidence: %+v", e)
		}
	}
	item := `{"name":"x","count":1,"price":1}`
	raw := `{"tool_surcharges":[` + strings.Repeat(item+",", 128) + item + `]}`
	if parsePortalLogEvidence(raw).ToolsState != portalEvidenceInvalid {
		t.Fatal("accepted unbounded tool list")
	}
	e := parsePortalLogEvidence(`{"tool_surcharges":[{"name":"web_search","count":2,"price":10,"authorization":"secret"}],"web_search":true,"web_search_call_count":2,"web_search_price":10}`)
	if strings.Contains(fmt.Sprintf("%+v", e), "secret") {
		t.Fatal("retained unknown sensitive tool field")
	}
	// Both representations remain independent evidence; the parser must not sum
	// or infer duplicates without a verified settlement/display policy.
	if !reflect.DeepEqual(e.Tools[0].Count, e.Numbers["web_search_call_count"]) {
		t.Fatal("lost mixed-format evidence")
	}
}

func TestPortalLogSemanticFieldPreserved(t *testing.T) {
	e := parsePortalLogEvidence(`{"usage_semantic":"anthropic","claude":false,"subscription_post_delta":-10,"subscription_used":0,"wallet_quota_deducted":0}`)
	if e.Strings["usage_semantic"].Text != "anthropic" || e.Flags["claude"].Text != "false" || e.Numbers["subscription_used"].Text != "0" || e.Numbers["subscription_post_delta"].Text != "-10" {
		t.Fatal("lost separate semantic or subscription evidence")
	}
}
