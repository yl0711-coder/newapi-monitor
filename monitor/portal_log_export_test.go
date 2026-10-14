package monitor

import (
	"bytes"
	"context"
	"encoding/csv"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestPortalCacheSummaryDetailExportParity(t *testing.T) {
	for _, tc := range []struct {
		name, raw, value, status string
		count                    int64
	}{
		{"recorded plus remainder", `{"cache_creation_tokens":100,"cache_creation_tokens_5m":60,"cache_creation_tokens_1h":30}`, "100", "已记录总量", 100},
		{"old total", `{"cache_creation_tokens":100}`, "100", "已记录总量", 100},
		{"zero", `{"cache_creation_tokens":0}`, "0", "已记录总量", 0},
		{"missing", `{}`, "", "", 0},
		{"missing other", ``, "", "", 0},
		{"split only", `{"cache_creation_tokens_5m":60,"cache_creation_tokens_1h":30}`, "90", "仅分项小计（总量未记录）", 90},
		{"split greater than total", `{"cache_creation_tokens":50,"cache_creation_tokens_5m":60}`, "", "记录异常", 0},
		{"bad split", `{"cache_creation_tokens":100,"cache_creation_tokens_5m":"secret"}`, "", "记录异常", 0},
		{"null", `{"cache_creation_tokens":null}`, "", "记录异常", 0},
		{"exponent", `{"cache_creation_tokens":1e2}`, "100", "已记录总量", 100},
		{"large integer", `{"cache_creation_tokens":9007199254740993}`, "9007199254740993", "已记录总量", 9007199254740993},
		{"overflow", `{"cache_creation_tokens_5m":9223372036854775807,"cache_creation_tokens_1h":1}`, "", "记录异常", 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: 2, CostUSD: 0.123456, PromptTokens: 987, CompletionTokens: 123, CacheWriteTokens: 999, BillingProcess: "old formula"}
			populatePortalLogEvidence(&r, tc.raw)
			if r.CostUSD != 0.123456 || r.PromptTokens != 987 || r.CompletionTokens != 123 {
				t.Fatal("changed charge or original tokens")
			}
			if r.CacheWriteTokens != tc.count {
				t.Fatalf("summary=%d want=%d", r.CacheWriteTokens, tc.count)
			}
			csvRow := portalCSVRecord(r)
			if len(csvRow) != len(portalCSVHeader()) || csvRow[13] != tc.value || csvRow[14] != tc.status || csvRow[9] != "0.123456" {
				t.Fatalf("CSV mismatch: %v", csvRow)
			}
			if r.UsageDetails != nil && r.UsageDetails.CacheWrite.State == portalEvidenceValid && r.UsageDetails.CacheWrite.Value != tc.value {
				t.Fatal("detail and export differ")
			}
		})
	}
}

func TestPortalCSVClarityAndOriginalAmountSemantics(t *testing.T) {
	wantHeader := []string{"时间", "成员", "令牌", "分组", "类型", "模型", "用时(秒)", "输入tokens", "输出tokens", "费用(美元)", "详情", "Request ID",
		"缓存读取tokens", "缓存创建tokens（总量或分项小计）", "缓存创建数据状态", "请求方式", "首字延迟(毫秒)", "金额性质", "计费来源", "费用计算说明", "补充详情"}
	if !reflect.DeepEqual(portalCSVHeader(), wantHeader) || len(wantHeader) != portalCSVColumnCount {
		t.Fatal("customer export layout changed without updating its contract")
	}
	for _, tc := range []struct {
		name, raw, kind, source string
		typ                     int
	}{
		{"wallet", `{"billing_source":"wallet"}`, "消费", "钱包", 2},
		{"unknown source", `{}`, "消费", "", 2},
		{"subscription", `{"billing_source":"subscription","subscription_consumed":75000}`, "订阅抵扣", "订阅", 2},
		{"refund", `{}`, "退款", "", 6},
		{"error", `{}`, "错误记录", "", 5},
		{"login", `{}`, "", "", 7},
		{"manage", `{}`, "", "", 3},
		{"system", `{}`, "", "", 4},
		{"topup", `{}`, "", "", 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := LogRow{Type: tc.typ, CostUSD: .15, PromptTokens: 100, CompletionTokens: 10, UseTime: 2, IsStream: true,
				RequestID: "request", RequestPath: "/v1/responses", UpstreamRequestID: "=up-id", IP: "192.0.2.1", SourceFacts: &portalLogSourceFacts{true, true, true}}
			populatePortalLogEvidence(&r, tc.raw)
			r.FirstByteEvidence = &portalDurationEvidence{State: portalEvidenceValid, Value: "125.5"}
			before, _ := json.Marshal(r)
			row := portalCSVRecord(r)
			if row[portalCSVAmountKindColumn] != tc.kind || row[portalCSVSourceColumn] != tc.source || row[11] != r.RequestID {
				t.Fatal("structured fields or amount nature/source incorrect")
			}
			if len(row) != 21 || strings.Contains(strings.Join(row, "\n"), r.IP) || strings.Contains(strings.Join(row, "\n"), r.UpstreamRequestID) || strings.Contains(strings.Join(row, "\n"), r.RequestPath) {
				t.Fatal("customer CSV must not export IP/upstream ID fields")
			}
			if tc.typ == 0 || tc.typ == 2 || tc.typ == 5 || tc.typ == 6 {
				if row[9] != "0.150000" || row[7] != "100" || row[8] != "10" {
					t.Fatal("recorded amount/tokens changed")
				}
			} else if row[9] != "" || row[7] != "" || row[8] != "" {
				t.Fatal("non-usage row has made-up request amounts or tokens")
			}
			if tc.typ == 2 || tc.typ == 5 {
				if row[6] != "2" || row[portalCSVModeColumn] != "流式" || row[portalCSVFRTColumn] != "125.5" {
					t.Fatal("request timing differs")
				}
			} else if row[6] != "" || row[portalCSVModeColumn] != "" || row[portalCSVFRTColumn] != "" {
				t.Fatal("non-request row has timing or stream metadata")
			}
			after, _ := json.Marshal(r)
			if !bytes.Equal(before, after) {
				t.Fatal("export changed source DTO")
			}
		})
	}
}

func TestPortalCSVShortFormulaAndOriginalContentWithoutOther(t *testing.T) {
	r := LogRow{Type: 2, Detail: "generated price summary", OtherContent: "原始说明,含引号\"\n下一行", BillingProcess: "INTERNAL_ONLY 500000 额度 生成器", UsageDetails: &portalLogDetails{
		Billing: &portalBillingCheck{State: "matched", Readable: "输入价格：$2 / 1M tokens\n（输入 100 tokens / 1M tokens × $2）× 分组倍率 1 = $0.000200\n实际扣费：$0.000200\n仅供参考，以实际扣费为准。"},
	}}
	row := portalCSVRecord(r)
	if row[10] != r.OtherContent || row[portalCSVBillingColumn] != r.UsageDetails.Billing.Readable || strings.Contains(strings.Join(row, "\n"), "INTERNAL_ONLY") || strings.Contains(strings.Join(row, "\n"), "generated price summary") {
		t.Fatal("customer formula duplicated or replaced by internal arithmetic")
	}
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	content := "历史原始说明,不依赖 other JSON\n下一行"
	if _, err := m.prodDB.Exec(`INSERT INTO logs(id,user_id,created_at,type,quota,prompt_tokens,completion_tokens,content,other) VALUES(1,101,1000,2,100,1,0,?,NULL)`, content); err != nil {
		t.Fatal(err)
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{101}, 0, 2000, 0, 0, "", "", "", "", "", 0, 10)
	if err != nil || len(rows) != 1 || portalCSVRecord(rows[0])[10] != content || rows[0].OtherContent != content {
		t.Fatal("historical content was lost when pricing metadata was absent")
	}
}

func TestPortalCSVFirstByteApplicability(t *testing.T) {
	for _, tc := range []struct {
		state       portalEvidenceState
		value, want string
	}{
		{portalEvidenceValid, "0", ""}, {portalEvidenceValid, "0.25", "0.25"}, {portalEvidenceValid, "125.5", "125.5"},
		{portalEvidenceMissing, "", ""}, {portalEvidenceNull, "", ""}, {portalEvidenceInvalid, "", ""},
	} {
		r := LogRow{Type: 2, IsStream: true, FirstByteMs: 999, FirstByteEvidence: &portalDurationEvidence{State: tc.state, Value: tc.value}}
		if portalCSVRecord(r)[portalCSVFRTColumn] != tc.want {
			t.Fatal("FRT display differs from official positive-only semantics")
		}
		r.streamFlagUnknown = true
		if portalCSVRecord(r)[portalCSVFRTColumn] != "" {
			t.Fatal("unknown request type acquired FRT")
		}
		r.streamFlagUnknown, r.IsStream = false, false
		if portalCSVRecord(r)[portalCSVFRTColumn] != "" {
			t.Fatal("non-stream acquired FRT")
		}
	}
}

func TestPortalCSVAppendedDetailsRoundTripAndSafety(t *testing.T) {
	wantPrefix := []string{"时间", "成员", "令牌", "分组", "类型", "模型", "用时(秒)", "输入tokens", "输出tokens", "费用(美元)", "详情", "Request ID"}
	header := portalCSVHeader()
	if !reflect.DeepEqual(header[:12], wantPrefix) {
		t.Fatal("existing columns changed")
	}
	r := LogRow{Type: 2, Member: "=untrusted", BillingProcess: "=untrusted formula", UsageDetails: &portalLogDetails{
		CacheRead:  portalCacheDisplay{State: portalEvidenceValid, Value: "9007199254740993", Source: "recorded"},
		CacheWrite: portalCacheDisplay{State: portalEvidenceMissing},
		Items:      []portalLogDetailItem{{Label: "=untrusted", Value: "comma,quote\"newline\n", State: portalEvidenceValid}},
		Notice:     "@untrusted note",
	}}
	row := portalCSVRecord(r)
	if len(row) != len(header) || row[12] != "9007199254740993" {
		t.Fatal("columns or precision changed")
	}
	for _, idx := range []int{1, portalCSVBillingColumn, portalCSVSupplementColumn} {
		if !strings.HasPrefix(row[idx], "'") {
			t.Fatalf("unescaped CSV formula: column %d", idx)
		}
	}
	if strings.Contains(strings.Join(row, "\n"), "untrusted formula") {
		t.Fatal("technical formula exported")
	}
	var buf bytes.Buffer
	w := csv.NewWriter(&buf)
	if err := w.WriteAll([][]string{header, row}); err != nil {
		t.Fatal(err)
	}
	got, err := csv.NewReader(&buf).ReadAll()
	if err != nil || !reflect.DeepEqual(got, [][]string{header, row}) {
		t.Fatalf("CSV round trip: %v", err)
	}
	for _, typ := range []int{1, 3, 4, 5, 6} {
		r.Type = typ
		for _, v := range portalCSVRecord(r)[12:15] {
			if v != "" {
				t.Fatal("consumption-only fields leaked into other types")
			}
		}
		if portalCSVRecord(r)[portalCSVBillingColumn] != "" || strings.Contains(portalCSVRecord(r)[portalCSVSupplementColumn], "=untrusted") {
			t.Fatal("consumption evidence exported for unrelated type")
		}
	}
}
