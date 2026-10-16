package monitor

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestPortalMediaDetailsStatesAndUnits(t *testing.T) {
	for _, tc := range []struct {
		name, raw, label, value string
		state                   portalEvidenceState
	}{
		{"audio zero", `{"audio":true,"audio_input":0}`, "音频输入 Tokens（分项原值）", "0", portalEvidenceValid},
		{"realtime missing", `{"ws":true}`, "音频输入 Tokens（分项原值）", "未记录", portalEvidenceMissing},
		{"null", `{"audio_input":null}`, "音频输入 Tokens（分项原值）", "未记录", portalEvidenceNull},
		{"negative", `{"audio_input":-1}`, "音频输入 Tokens（分项原值）", "记录异常", portalEvidenceInvalid},
		{"fractional count", `{"audio_input":0.1}`, "音频输入 Tokens（分项原值）", "记录异常", portalEvidenceInvalid},
		{"large integer", `{"audio_output":9007199254740993}`, "音频输出 Tokens（分项原值）", "9007199254740993", portalEvidenceValid},
		{"bad string", `{"audio_ratio":"secret-key"}`, "音频输入倍率（日志原值，非单价）", "记录异常", portalEvidenceInvalid},
		{"zero rate", `{"audio_completion_ratio":0}`, "音频输出倍率（日志原值，非单价）", "0", portalEvidenceValid},
		{"audio price", `{"audio_input_seperate_price":true,"audio_input_price":7.5}`, "独立音频输入单价（$/百万 Tokens，未乘分组倍率）", "7.5", portalEvidenceValid}, //nolint:misspell // Original upstream wire fixture, deliberately independent of the constant.
		{"missing audio price", `{"audio_input_seperate_price":true}`, "独立音频输入单价（$/百万 Tokens，未乘分组倍率）", "未记录", portalEvidenceMissing},               //nolint:misspell // Original upstream wire fixture.
		{"image input historical key", `{"image":true,"image_output":321}`, "图片 Tokens（历史字段原值）", "321", portalEvidenceValid},
		{"old image call", `{"image_generation_call":true,"image_generation_call_price":0.04}`, "图片生成单价（$/次，未乘分组倍率）", "0.04", portalEvidenceValid},
	} {
		t.Run(tc.name, func(t *testing.T) {
			d := buildPortalLogDetails(2, tc.raw)
			found := false
			for _, item := range d.Items {
				if item.Label == tc.label {
					found = true
					if item.Value != tc.value || item.State != tc.state {
						t.Fatalf("unexpected evidence: %+v", item)
					}
				}
			}
			if !found || !d.LegacyFormulaUnsafe {
				t.Fatalf("missing evidence or unsafe formula allowed: %+v", d)
			}
			b, _ := json.Marshal(d)
			if strings.Contains(string(b), "secret-key") || strings.Contains(string(b), "图片输出 Tokens") {
				t.Fatalf("unsafe media presentation: %s", b)
			}
		})
	}
	for _, raw := range []string{`{}`, `{"audio":false,"ws":false,"image":false}`, `null`, `invalid`} {
		if items := portalMediaDetailItems(parsePortalLogEvidence(raw)); len(items) != 0 {
			t.Fatalf("invented media evidence for %s: %+v", raw, items)
		}
	}
}

func TestPortalMediaAndToolDetailExportPreservesOriginalCharge(t *testing.T) {
	for _, raw := range []string{
		`{"audio":true,"audio_input":20,"audio_output":0,"text_input":30,"text_output":0,"audio_ratio":4,"audio_completion_ratio":8}`,
		`{"image":true,"image_output":99,"image_ratio":1.5}`,
		`{"audio_input_seperate_price":true,"audio_input_token_count":45,"audio_input_price":6.25}`, //nolint:misspell // Original upstream wire fixture.
		`{"image_generation_call":true,"image_generation_call_price":0.04}`,
		`{"tool_surcharges":[{"name":"web_search_preview","count":2,"price":10},{"name":"google_search","count":0,"price":0},{"name":"private-secret-name","count":3,"price":5}]}`,
	} {
		r := LogRow{Type: 2, PromptTokens: 50, CompletionTokens: 10, CostUSD: 0.123456, billingQuota: 61728, billingFactsValid: true}
		populatePortalLogEvidence(&r, raw)
		if r.PromptTokens != 50 || r.CompletionTokens != 10 || r.CostUSD != 0.123456 || r.billingQuota != 61728 || r.BillingProcess != "" || r.UsageDetails.Billing.State == "matched" {
			t.Fatal("modified original charge/tokens or invented formula")
		}
		record := portalCSVRecord(r)
		if record[9] != "0.123456" || len(record) != portalCSVColumnCount {
			t.Fatal("modified export shape or charge")
		}
		for _, item := range r.UsageDetails.Items {
			if !strings.Contains(record[portalCSVSupplementColumn], item.Label) || !strings.Contains(record[portalCSVSupplementColumn], item.Value) {
				t.Fatalf("API/CSV mismatch: %+v", item)
			}
		}
		b, _ := json.Marshal(r)
		if strings.Contains(string(b)+strings.Join(record, " "), "private-secret-name") {
			t.Fatal("raw tool name disclosed")
		}
		for _, typ := range []int{1, 3, 4, 5, 6} {
			other := LogRow{Type: typ}
			populatePortalLogEvidence(&other, raw)
			if other.UsageDetails != nil || strings.Contains(portalCSVRecord(other)[portalCSVSupplementColumn], "音频") || strings.Contains(portalCSVRecord(other)[portalCSVSupplementColumn], "单价") {
				t.Fatal("media evidence leaked into non-consumption log")
			}
		}
	}
}

func TestPortalToolsNewOldPrecedenceAndDistinctRecords(t *testing.T) {
	for _, structured := range []string{
		`[{"name":"image_generation","count":2,"price":40}]`,
		`[]`, `null`, `[{"name":"image_generation","count":-1,"price":40}]`,
	} {
		d := buildPortalLogDetails(2, `{"tool_surcharges":`+structured+`,"image_generation_call_price":0.987,"image_generation_call_count":987,"web_search_call_count":987}`)
		b, _ := json.Marshal(d)
		if strings.Contains(string(b), "987") || strings.Contains(string(b), "$/次") {
			t.Fatalf("legacy tool fields counted alongside structured record: %s", b)
		}
	}
	d := buildPortalLogDetails(2, `{"tool_surcharges":[{"name":"image_generation","count":2,"price":40},{"name":"image_generation","count":3,"price":30},{"name":"private","count":4,"price":20}]}`)
	want := map[string]string{"图片生成（记录 1）次数": "2", "图片生成（记录 2）次数": "3", "工具调用（记录 3）次数": "4", "图片生成（记录 1）单价（$/千次，未乘分组倍率）": "40"}
	for _, item := range d.Items {
		if value, ok := want[item.Label]; ok {
			if value != item.Value {
				t.Fatalf("merged distinct tool records: %+v", item)
			}
			delete(want, item.Label)
		}
	}
	if len(want) != 0 {
		t.Fatalf("missing tool records: %v", want)
	}
}
