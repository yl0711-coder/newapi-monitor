package monitor

import (
	"encoding/base64"
	"encoding/json"
	"math/big"
	"strings"
	"testing"
)

func TestPortalBillRecordedReferencePricesAndGroups(t *testing.T) {
	for _, tc := range []struct {
		name, raw, reference  string
		prompt, output, quota int64
	}{
		{"legacy ratio", `{"model_ratio":2,"completion_ratio":3,"group_ratio":1.5}`, "0.004480", 100, 20, 480},
		{"personal ratio not multiplied twice", `{"model_ratio":2,"completion_ratio":3,"group_ratio":1.5,"user_group_ratio":1.5}`, "0.004480", 100, 20, 480},
		{"different recorded base price", `{"model_ratio":4,"completion_ratio":3,"group_ratio":0.75}`, "0.008960", 100, 20, 480},
		{"per call", `{"model_price":0.2,"model_ratio":1,"group_ratio":1.5,"request_path":"/v1/responses"}`, "1.400000", 100, 20, 150000},
		{"cached ratio", `{"model_ratio":1,"completion_ratio":4,"group_ratio":1,"cache_tokens":20,"cache_ratio":0.1,"cache_creation_tokens":40,"cache_creation_ratio":1.25}`, "0.001848", 100, 10, 132},
		{"free group", `{"model_ratio":1,"completion_ratio":1,"group_ratio":0}`, "0.000014", 1, 0, 0},
		{"minimum quota", `{"model_ratio":0.49,"completion_ratio":1,"group_ratio":1}`, "0.000007", 1, 0, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, ModelName: "any-recorded-model", CreatedAt: 1700000000, PromptTokens: tc.prompt, CompletionTokens: tc.output, billingQuota: tc.quota, billingFactsValid: true}
			populatePortalLogEvidence(&r, tc.raw)
			before, _ := json.Marshal(r.billEvidence)
			row, err := portalBillRecord(r)
			if err != nil || strings.TrimPrefix(row[12], "¥") != tc.reference || strings.TrimPrefix(row[13], "¥") != big.NewRat(tc.quota, 500000).FloatString(6) {
				t.Fatalf("recorded reference: %v %v", row, err)
			}
			after, _ := json.Marshal(r.billEvidence)
			if string(before) != string(after) {
				t.Fatal("source evidence changed")
			}
			out, _ := json.Marshal(r.UsageDetails)
			if strings.Contains(string(out), "baseUSD") {
				t.Fatal("internal subtotal leaked")
			}
		})
	}
}

func TestPortalBillRecordedTierReferenceBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name, expr, tier, extra, reference string
		prompt, output, quota              int64
	}{
		{"short boundary", `len <= 100 ? tier("short", p*2+c*10+cr*0.2) : tier("long",p*4+c*20+cr*0.4)`, "short", "", "0.001848", 100, 10, 66},
		{"long boundary", `len <= 100 ? tier("short", p*2+c*10+cr*0.2) : tier("long",p*4+c*20+cr*0.4)`, "long", "", "0.003724", 101, 10, 133},
		{"wrong recorded tier", `len <= 100 ? tier("short", p*2+c*10+cr*0.2) : tier("long",p*4+c*20+cr*0.4)`, "short", "", "", 101, 10, 133},
		{"all branches used vars", `len <= 100 ? tier("short",p*2+c*10) : tier("long",p*4+c*20+cr*0.4)`, "short", "", "0.001820", 100, 10, 65},
		{"zero usage", `tier("short",p*2+c*10)`, "short", `,"cache_tokens":0`, "0.000000", 0, 0, 0},
		{"unverified charge", `tier("short",p*2+c*10)`, "short", "", "", 100, 10, 76},
		{"tools unavailable", `tier("short",p*2+c*10)`, "short", `,"tool_surcharges":null`, "", 100, 10, 75},
		{"personal ratio not doubled", `tier("short",p*2+c*10)`, "short", `,"user_group_ratio":0.5`, "0.002100", 100, 10, 75},
		{"conflicting personal ratio", `tier("short",p*2+c*10)`, "short", `,"user_group_ratio":0.7`, "", 100, 10, 75},
		{"unsupported expression", `tier("short",max(p,1)*2+c*10)`, "short", "", "", 100, 10, 75},
		{"invalid split", `tier("short",p*2+c*10)`, "short", `,"usage_semantic":"anthropic","cache_creation_tokens":10,"cache_creation_tokens_5m":11`, "", 100, 10, 75},
		{"unknown source", `tier("short",p*2+c*10)`, "short", `,"billing_source":"unknown"`, "", 100, 10, 75},
		{"multimodal variable", `tier("short",p*2+c*10+img*0)`, "short", "", "", 100, 10, 75},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fields := map[string]any{"billing_mode": "tiered_expr", "expr_b64": base64.StdEncoding.EncodeToString([]byte(tc.expr)), "matched_tier": tc.tier, "group_ratio": 0.5, "cache_tokens": 20, "request_path": "/v1/responses", "billing_source": "wallet"}
			if tc.extra != "" {
				var extra map[string]any
				if err := json.Unmarshal([]byte("{"+strings.TrimPrefix(tc.extra, ",")+"}"), &extra); err != nil {
					t.Fatal(err)
				}
				for k, v := range extra {
					fields[k] = v
				}
			}
			raw, _ := json.Marshal(fields)
			r := LogRow{Type: 2, ModelName: "any-model", PromptTokens: tc.prompt, CompletionTokens: tc.output, billingQuota: tc.quota, billingFactsValid: true}
			populatePortalLogEvidence(&r, string(raw))
			row, err := portalBillRecord(r)
			if err != nil || strings.TrimPrefix(row[12], "¥") != tc.reference || strings.TrimPrefix(row[13], "¥") != big.NewRat(tc.quota, 500000).FloatString(6) {
				t.Fatalf("%v %v", row, err)
			}
		})
	}
}

func TestPortalBillRecordedReferenceFloatRoundingAndMissingFacts(t *testing.T) {
	raw := `{"billing_mode":"tiered_expr","expr_b64":"` + base64.StdEncoding.EncodeToString([]byte(`tier("base", p*8.2)`)) + `","matched_tier":"base","group_ratio":1,"request_path":"/v1/responses"}`
	r := LogRow{Type: 2, PromptTokens: 15, billingQuota: 61, billingFactsValid: true}
	populatePortalLogEvidence(&r, raw)
	if r.UsageDetails.Billing.State != "unsupported" {
		t.Fatal("ordinary details float boundary gate changed")
	}
	row, err := portalBillRecord(r)
	if err != nil || strings.TrimPrefix(row[12], "¥") != "0.000861" || strings.TrimPrefix(row[13], "¥") != "0.000122" {
		t.Fatalf("source rounding or exact base reference differs: %v %v", row, err)
	}
	// Decimal arithmetic would round to 62; this is NOT the source evaluator's
	// result. Do not accept it by allowing a one-unit tolerance.
	r.billingQuota = 62
	row, err = portalBillRecord(r)
	if err != nil || strings.TrimPrefix(row[12], "¥") != "" || strings.TrimPrefix(row[13], "¥") != "0.000124" {
		t.Fatal("rounding mismatch was tolerated")
	}
	r.billingQuota = 61
	for _, facts := range []portalLogSourceFacts{
		{PromptPresent: false, CompletionPresent: true, QuotaPresent: true},
		{PromptPresent: true, CompletionPresent: false, QuotaPresent: true},
	} {
		r.SourceFacts = &facts
		row, err = portalBillRecord(r)
		if err != nil || strings.TrimPrefix(row[12], "¥") != "" || row[14] != "" || strings.TrimPrefix(row[13], "¥") != "0.000122" {
			t.Fatal("missing source token became zero or charge lost")
		}
	}
}

func TestPortalBillRecordedAnthropicUnsplitPreservesContextAndDetails(t *testing.T) {
	r := billSampleRow(t, 0, 202, 0, 237815, 0, 0, 2424)
	r.ModelName = "not-the-sample-model"
	r.CreatedAt = 1700000000
	r.billEvidence.Expression.Text = `len <= 100000 ? tier("short", p*4+c*20+cr*0.2+cc*5+cc1h*8) : tier("long",p*8+c*40+cr*0.4+cc*10+cc1h*16)`
	r.billEvidence.Strings["matched_tier"] = portalEvidenceField{State: portalEvidenceValid, Text: "long"}
	before, _ := json.Marshal(r.billEvidence)
	if checkPortalBilling(r, *r.billEvidence).State == "matched" {
		t.Fatal("ordinary log explanation unexpectedly relaxed")
	}
	row, err := portalBillRecord(r)
	if err != nil || strings.TrimPrefix(row[12], "¥") != "0.056560" || strings.TrimPrefix(row[13], "¥") != "0.004848" || row[10] != "237815" {
		t.Fatalf("lost actual context or invented cache price: %v %v", row, err)
	}
	after, _ := json.Marshal(r.billEvidence)
	if string(before) != string(after) {
		t.Fatal("source changed")
	}
	r.billEvidence.Strings["matched_tier"] = portalEvidenceField{State: portalEvidenceValid, Text: "short"}
	row, err = portalBillRecord(r)
	if err != nil || strings.TrimPrefix(row[12], "¥") != "" {
		t.Fatal("unsplit write omitted from tier selection")
	}
}
