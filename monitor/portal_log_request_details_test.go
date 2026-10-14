package monitor

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func TestPortalFirstByteEvidencePreservesZeroAndMissing(t *testing.T) {
	for _, tc := range []struct {
		raw   string
		state portalEvidenceState
		value string
	}{
		{`{"frt":0}`, portalEvidenceValid, "0"},
		{`{"frt":0.25}`, portalEvidenceValid, "0.25"},
		{`{}`, portalEvidenceMissing, ""},
		{`{"frt":null}`, portalEvidenceNull, ""},
		{`{"frt":-1}`, portalEvidenceInvalid, ""},
		{`{"frt":"secret"}`, portalEvidenceInvalid, ""},
		{`{`, portalEvidenceInvalid, ""},
	} {
		for _, typ := range []int{2, 5} {
			r := LogRow{Type: typ, IsStream: true, FirstByteMs: 99}
			populatePortalLogEvidence(&r, tc.raw)
			if r.FirstByteEvidence == nil || r.FirstByteEvidence.State != tc.state || r.FirstByteEvidence.Value != tc.value || r.FirstByteMs != 99 {
				t.Fatalf("raw=%s evidence=%+v legacy=%d", tc.raw, r.FirstByteEvidence, r.FirstByteMs)
			}
		}
	}
	for _, r := range []LogRow{{Type: 2}, {Type: 2, IsStream: true, streamFlagUnknown: true}, {Type: 6, IsStream: true}} {
		populatePortalLogEvidence(&r, `{"frt":0}`)
		if r.FirstByteEvidence != nil {
			t.Fatal("unexpected first-byte evidence")
		}
	}
}

func TestPortalRequestDetailsSafeStates(t *testing.T) {
	for _, tc := range []struct {
		name, raw, want, absent string
		stream                  bool
	}{
		{"legacy missing", "{}", "未记录，不能判定是否完整结束", "正常（日志状态）", true},
		{"missing other", "", "未记录或无法解析", "正常（日志状态）", true},
		{"malformed", "{", "未记录或无法解析", "正常（日志状态）", true},
		{"normal", `{"frt":0,"stream_status":{"status":"ok","end_reason":"done","error_count":0}}`, `"value":"ok"`, "流结束原因", true},
		{"fraction frt", `{"frt":0.25,"stream_status":{"status":"error","end_reason":"timeout"}}`, "0.25", "未记录的首字", true},
		{"negative frt", `{"frt":-1}`, "记录异常", "-1", true},
		{"null frt", `{"frt":null}`, "未记录", "毫秒，日志原值", true},
		{"client gone", `{"stream_status":{"status":"error","end_reason":"client_gone","error_count":2,"end_error":"context canceled","errors":["Authorization: Bearer local-test-secret"]}}`, "context canceled", "local-test-secret", true},
		{"unknown reason", `{"stream_status":{"status":"error","end_reason":"new_reason"}}`, "new_reason", "未知类型", true},
		{"unknown status", `{"stream_status":{"status":"partial"}}`, "partial", "未知或记录异常", true},
		{"conflict", `{"stream_status":{"status":"ok","end_reason":"timeout"}}`, `"value":"ok"`, "冲突", true},
		{"error count conflict", `{"stream_status":{"status":"ok","end_reason":"done","error_count":1e1}}`, `"value":"ok"`, "冲突", true},
		{"zero exponent", `{"stream_status":{"status":"ok","end_reason":"done","error_count":0e2}}`, `"value":"ok"`, "冲突", true},
		{"duplicate status", `{"stream_status":{"status":"ok","status":"error"}}`, "记录异常，不能判定", "正常（日志状态）", true},
		{"null stream", `{"stream_status":null}`, "未记录，不能判定", "正常（日志状态）", true},
		{"not stream", `{"frt":100,"stream_status":{"status":"ok"}}`, "标记不一致", "毫秒，日志原值", false},
		{"nonstream null", `{"stream_status":null}`, "非流式", "不一致", false},
		{"prompt marker", `{"is_system_prompt_overwritten":true,"system_prompt":"secret-prompt","request_conversion":["secret-proxy"]}`, "已替换（不展示内容）", "secret", false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, IsStream: tc.stream, PromptTokens: 10, CompletionTokens: 5, CostUSD: 0.25, billingQuota: 125000}
			populatePortalLogEvidence(&r, tc.raw)
			data, _ := json.Marshal(r.RequestDetails)
			if !strings.Contains(string(data), tc.want) || (tc.absent != "" && strings.Contains(string(data), tc.absent)) {
				t.Fatalf("%s", data)
			}
			if r.PromptTokens != 10 || r.CompletionTokens != 5 || r.CostUSD != 0.25 || r.billingQuota != 125000 {
				t.Fatal("changed source values")
			}
			for _, item := range r.RequestDetails {
				if item.Label == "请求方式" || strings.HasPrefix(item.Label, "首字延迟") || item.State == portalEvidenceMissing || item.State == portalEvidenceNull {
					continue
				}
				if !strings.Contains(portalCSVRecord(r)[portalCSVSupplementColumn], item.Label+"："+item.Value) {
					t.Fatal("page/CSV metadata differs")
				}
			}
		})
	}
}

func TestPortalRequestDetailsCustomerTypeAndQuery(t *testing.T) {
	raw := `{"stream_status":{"status":"error","end_reason":"scanner_error","end_error":"failed https://upstream.invalid/secret-url"},"admin_info":{"secret":"secret-admin"},"channel_name":"secret-channel","request_conversion":["secret-route"]}`
	if strings.Contains(fmt.Sprintf("%+v", parsePortalLogEvidence(raw)), "secret") {
		t.Fatal("raw private errors retained in evidence")
	}
	for _, typ := range []int{1, 3, 4, 6, 7} {
		r := LogRow{Type: typ, IsStream: true}
		populatePortalLogEvidence(&r, raw)
		if len(r.RequestDetails) > 0 {
			t.Fatal("request evidence on unrelated log type")
		}
	}
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	_, err := m.prodDB.Exec(`INSERT INTO logs (id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,is_stream,other) VALUES (1,10,1000,5,0,0,0,1,?)`, raw)
	if err != nil {
		t.Fatal(err)
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{10}, 0, 2000, 0, 5, "", "", "", "", "", 0, 100)
	if err != nil || len(rows) != 1 {
		t.Fatalf("%v rows=%d", err, len(rows))
	}
	r := rows[0]
	data, _ := json.Marshal(r)
	if strings.Contains(string(data), "secret") || !strings.Contains(string(data), "scanner_error") || r.UsageDetails != nil {
		t.Fatalf("unsafe or incomplete response %s", data)
	}
	if portalCSVRecord(r)[9] != "0.000000" {
		t.Fatal("original zero error quota lost")
	}
	if !strings.Contains(portalCSVRecord(r)[portalCSVSupplementColumn], "不能据此判断是否另有消费扣费") {
		t.Fatal("missing error charge explanation")
	}
}

func TestPortalCSVExpandedCustomerFields(t *testing.T) {
	r := LogRow{Type: 2, RequestPath: "/v1/responses", IsStream: true, LogContent: "日志摘要", OtherContent: "含逗号,引号\"\n换行", ReasoningEffort: "high", IsModelMapped: true, ModelName: "requested-model", UpstreamModelName: "actual-model", ParamOverride: []string{"temperature=1"}, SubPlan: "plan", SubInstance: "instance", SubSettlement: "预扣：2\n最终抵扣：1", SubRemain: "9/10", BillingSource: "subscription"}
	row := portalCSVRecord(r)
	for _, s := range []string{"流式", "日志摘要", r.OtherContent, "high", "actual-model", "temperature=1", "plan", "instance", r.SubSettlement, "9/10", "不等同于钱包现金扣款"} {
		if !strings.Contains(strings.Join(row, "\n"), s) {
			t.Fatalf("missing %s", s)
		}
	}
	if strings.Contains(strings.Join(row, "\n"), r.RequestPath) {
		t.Fatal("admin-only request path exported to customer")
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.WriteAll([][]string{portalCSVHeader(), row}); err != nil {
		t.Fatal(err)
	}
	got, err := csv.NewReader(&buf).ReadAll()
	if err != nil || !reflect.DeepEqual(got, [][]string{portalCSVHeader(), row}) {
		t.Fatal("CSV multiline roundtrip failed")
	}
	r = LogRow{Type: 6, TaskID: "task-1", RefundReason: "task failure"}
	row = portalCSVRecord(r)
	if row[9] != "0.000000" || !strings.Contains(row[portalCSVSupplementColumn], "task-1") || !strings.Contains(row[portalCSVSupplementColumn], "task failure") {
		t.Fatal("refund details wrong")
	}
	r = LogRow{Type: 2, RequestDetails: []portalLogDetailItem{{Label: "=formula", Value: "danger", State: portalEvidenceValid}}}
	if !strings.HasPrefix(portalCSVRecord(r)[portalCSVSupplementColumn], "'") {
		t.Fatal("CSV formula injection")
	}
}

func TestPortalRequestNullStreamFlagIsNotNonStreaming(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	for i, flag := range []string{"NULL", "0", "1", "-1"} {
		_, err := m.prodDB.Exec(fmt.Sprintf(`INSERT INTO logs (id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,is_stream,other) VALUES (%d,10,1000,2,1,1,1,%s,'{}')`, i+1, flag))
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{10}, 0, 2000, 0, 2, "", "", "", "", "", 0, 100)
	if err != nil || len(rows) != 4 {
		t.Fatalf("%v rows=%d", err, len(rows))
	}
	for _, r := range rows {
		record := portalCSVRecord(r)
		if r.ID == 1 || r.ID == 4 {
			if record[portalCSVModeColumn] != "" || record[portalCSVFRTColumn] != "" {
				t.Fatal("unknown flag became known")
			}
		}
		if r.ID == 2 && record[portalCSVModeColumn] != "非流式" {
			t.Fatal("explicit false lost")
		}
		if r.ID == 3 && record[portalCSVModeColumn] != "流式" {
			t.Fatal("explicit true lost")
		}
	}
}
