package monitor

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
)

func TestPortalLogDetailsCustomerAllowlist(t *testing.T) {
	raw := `{"model_ratio":1,"group_ratio":1,"cache_tokens":0,"cache_creation_tokens":100,"cache_creation_tokens_5m":60,"cache_creation_tokens_1h":30,"tool_surcharges":[{"name":"secret-tool-sk-secret","count":2,"price":10}],"channel_id":7,"admin_info":{"secret":"admin-secret"},"billing_mode":"tiered_expr","expr_b64":"c2VjcmV0LWV4cHI=","request_rules":[{"cond":"secret-condition","multiplier":1,"matched":true}]}`
	d := buildPortalLogDetails(2, raw)
	b, err := json.Marshal(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{"secret", "channel_id", "admin_info", "expr_b64", "request_rules"} {
		if strings.Contains(string(b), forbidden) {
			t.Fatalf("customer data leak: %s", forbidden)
		}
	}
	want := map[string]string{"缓存读取 Tokens（日志原值）": "0", "缓存创建总量（日志原值）": "100", "未拆分缓存创建 Tokens": "10", "工具调用次数": "2"}
	for _, item := range d.Items {
		if value, ok := want[item.Label]; ok {
			if item.Value != value {
				t.Fatalf("%s=%s want %s", item.Label, item.Value, value)
			}
			delete(want, item.Label)
		}
	}
	if len(want) != 0 || !d.LegacyFormulaUnsafe {
		t.Fatalf("missing evidence: %v", want)
	}
	for _, typ := range []int{0, 1, 3, 4, 5, 6} {
		if buildPortalLogDetails(typ, raw) != nil {
			t.Fatal("consumption evidence applied to other log type")
		}
	}
}

func TestPortalLogDetailsSentinelAndPartialEvidence(t *testing.T) {
	for _, raw := range []string{"", "null"} {
		if buildPortalLogDetails(2, raw) != nil {
			t.Fatal("missing raw log became zero evidence")
		}
	}
	for _, tc := range []struct {
		raw, contains, absent string
		unsafe                bool
	}{
		{`{"model_ratio":1,"group_ratio":1,"completion_ratio":2,"user_group_ratio":-1}`, "", "个人分组倍率", false},
		{`{"model_ratio":1,"group_ratio":1,"completion_ratio":2,"user_group_ratio":-1e0}`, "", "个人分组倍率", false},
		{`{"model_ratio":1,"group_ratio":1}`, "暂未展示", "个人分组倍率", true},
		{`{"model_ratio":1,"group_ratio":1,"completion_ratio":2,"cache_tokens":100}`, "暂未展示", "缓存创建", true},
		{`{"model_ratio":1,"group_ratio":1,"completion_ratio":2,"cache_creation_tokens":100,"cache_creation_ratio":1.25}`, "暂未展示", "未拆分缓存创建", true},
		{`{"model_ratio":1,"group_ratio":1,"completion_ratio":2,"cache_tokens":null}`, "未记录", `"value":"0"`, true},
		{`{"model_ratio":1,"user_group_ratio":0}`, `"value":"0"`, "记录异常", true},
		{`{"model_ratio":1,"user_group_ratio":-2}`, "记录异常", `"value":"-2"`, true},
		{`{"cache_creation_tokens_5m":60}`, "仅展示已有分项", "未拆分缓存创建", true},
		{`{"cache_creation_tokens":20,"cache_creation_tokens_5m":60}`, "未自动修正", "未拆分缓存创建", true},
		{`{"cache_tokens":null}`, "未记录", `"value":"0"`, true},
		{`{"cache_tokens":"secret"}`, "记录异常", "secret", true},
		{`{"tool_surcharges":null,"web_search_call_count":2}`, "工具附加费记录", "网页搜索次数", true},
		{`{"tool_surcharges":[{"name":"web_search","count":2,"price":10}],"web_search_call_count":99}`, "网页搜索次数", `"value":"99"`, true},
		{`{"web_search_call_count":2,"web_search_price":10}`, "网页搜索次数", "工具调用", true},
	} {
		d := buildPortalLogDetails(2, tc.raw)
		b, _ := json.Marshal(d)
		if !strings.Contains(string(b), tc.contains) || (tc.absent != "" && strings.Contains(string(b), tc.absent)) {
			t.Fatalf("unexpected details for %s: %s", tc.raw, b)
		}
		if (d != nil && d.LegacyFormulaUnsafe) != tc.unsafe {
			t.Fatalf("unsafe for %s: %s", tc.raw, b)
		}
	}
}

func TestPortalLogDetailsQueryKeepsChargeScopeAndSource(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	raw := `{"model_ratio":1,"group_ratio":1,"cache_creation_tokens":100,"cache_creation_tokens_5m":60,"tool_surcharges":[{"name":"web_search","count":2,"price":10}],"admin_info":{"secret":"private"}}`
	for _, uid := range []int{10, 20} {
		_, err := m.prodDB.Exec("INSERT INTO logs (id,user_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,other) VALUES (?,?,1000,2,'model',123456,200,30,?)", uid, uid, raw)
		if err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.queryGroupLogs(context.Background(), []int64{10}, 0, 2000, 0, 2, "", "", "", "", "", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ID != 10 || rows[0].CostUSD != float64(123456)/quotaPerUSD || rows[0].PromptTokens != 200 || rows[0].CompletionTokens != 30 || rows[0].UsageDetails == nil {
		t.Fatalf("changed original charge or scope: %+v", rows)
	}
	b, _ := json.Marshal(rows)
	if strings.Contains(string(b), "private") {
		t.Fatal("raw other leaked")
	}
	if rows[0].BillingProcess != "" {
		t.Fatal("unsupported legacy formula exposed via API")
	}
	var quota int64
	var stored string
	if err := m.prodDB.QueryRow("SELECT quota,other FROM logs WHERE id=10").Scan(&quota, &stored); err != nil {
		t.Fatal(err)
	}
	if quota != 123456 || stored != raw {
		t.Fatal("mutated source logs")
	}
	// Original twelve columns stay in place; cache evidence is appended.
	if record := portalCSVRecord(rows[0]); len(record) != len(portalCSVHeader()) || record[9] != "0.246912" || record[13] != "100" || rows[0].CacheWriteTokens != 100 {
		t.Fatalf("CSV amount or columns changed: %v", record)
	}
}

func BenchmarkPortalLogDetails(b *testing.B) {
	raw := `{"model_ratio":1,"group_ratio":1,"completion_ratio":2,"cache_tokens":10,"cache_ratio":0.1,"cache_creation_tokens":100,"cache_creation_tokens_5m":60,"tool_surcharges":[{"name":"web_search","count":2,"price":10}]}`
	b.ReportAllocs()
	for b.Loop() {
		buildPortalLogDetails(2, raw)
	}
}
