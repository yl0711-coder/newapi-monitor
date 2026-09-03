package monitor

// 上游错误日志采集的行为约束。
//
// 最要紧的三条：
//  1. 只对 NewAPI 生效——另两家的端点是聚合/计价语义，硬拉会得到错的东西
//  2. 缺字段不丢整条——错误日志是排障线索，有 id+时间就值得存
//  3. 窗口没读完必须标 Truncated——不然会被当成「上游那段只有这些错误」

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
)

// TestDecodeUpstreamErrorLogItemKeepsPartialRows 缺可选字段不丢整条。
//
// 与 decodeNewAPIUsageItem 的容错原则相反：计价少一个 quota 必须硬失败（算钱错不得），
// 错误日志少个 model_name 仍值得存——没有这条记录，排障就少一条线索。
func TestDecodeUpstreamErrorLogItemKeepsPartialRows(t *testing.T) {
	item, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":123,"created_at":1787000000,"content":"status_code=502, bad gateway"}`))
	if err != nil {
		t.Fatalf("只有 id/created_at/content 也应解出: %v", err)
	}
	if item.ID != 123 || item.CreatedAt != 1787000000 {
		t.Errorf("主键字段解错: id=%d created=%d", item.ID, item.CreatedAt)
	}
	if item.Content != "status_code=502, bad gateway" {
		t.Errorf("错误原文丢了: %q", item.Content)
	}
	// 可选字段空着是允许的。
	if item.ModelName != "" || item.TokenName != "" {
		t.Errorf("缺失的可选字段应为空: model=%q token=%q", item.ModelName, item.TokenName)
	}
}

// TestDecodeUpstreamErrorLogItemRejectsBadKey id 与 created_at 必须硬校验。
//
// id 是主键：错了会让两条不同日志互相覆盖，那是静默的数据损坏。
func TestDecodeUpstreamErrorLogItemRejectsBadKey(t *testing.T) {
	cases := []struct {
		name string
		raw  string
	}{
		{"缺 id", `{"created_at":1787000000,"content":"x"}`},
		{"id 为 0", `{"id":0,"created_at":1787000000}`},
		{"id 为负", `{"id":-5,"created_at":1787000000}`},
		{"id 非整数", `{"id":1.5,"created_at":1787000000}`},
		{"缺 created_at", `{"id":1}`},
		{"created_at 为 0", `{"id":1,"created_at":0}`},
		{"整条不是 JSON", `not-json`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodeUpstreamErrorLogItem(json.RawMessage(tc.raw)); err == nil {
				t.Error("主键字段无效必须整条拒绝，不能让它覆盖别的记录")
			}
		})
	}
}

// TestDecodeUpstreamErrorLogItemTriesAlternateNames 字段名有候选，命中任一即可。
//
// ★ 本组最要紧的一条 ★
// 上游响应的字段名**没有全部核实过**：content / token_name 是照我方 logs 表
// 列名推的，零证据。定型结构体对猜错的名字是静默变空，而错误原文正是这张表
// 存在的理由——空了等于白拉。所以一个字段试多个候选名。
func TestDecodeUpstreamErrorLogItemTriesAlternateNames(t *testing.T) {
	cases := []struct {
		name    string
		raw     string
		wantMsg string
	}{
		{"content", `{"id":1,"created_at":1,"content":"c"}`, "c"},
		{"message", `{"id":1,"created_at":1,"message":"m"}`, "m"},
		{"error", `{"id":1,"created_at":1,"error":"e"}`, "e"},
		{"detail", `{"id":1,"created_at":1,"detail":"d"}`, "d"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			item, err := decodeUpstreamErrorLogItem(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatalf("decode: %v", err)
			}
			if item.Content != tc.wantMsg {
				t.Errorf("错误原文未从 %s 取到: got=%q want=%q", tc.name, item.Content, tc.wantMsg)
			}
		})
	}
	// 驼峰写法也要认——上游若换 JSON 命名风格不该整列变空。
	item, _ := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":1,"created_at":1,"modelName":"gpt-x","upstreamRequestId":"u1"}`))
	if item.ModelName != "gpt-x" {
		t.Errorf("modelName 驼峰未认: %q", item.ModelName)
	}
	if item.UpstreamUpstreamRequestID != "u1" {
		t.Errorf("upstreamRequestId 驼峰未认: %q", item.UpstreamUpstreamRequestID)
	}
}

// TestDecodeUpstreamErrorLogItemRecordsUnresolvedFields 一个候选名都没命中要登记。
//
// 这是「字段名猜错」的唯一观测出口。若上线后 content 长期出现在这里，
// 说明名字错了——那时照 RawJSON 就地重解，不必重新向上游拉（上游有保留期）。
func TestDecodeUpstreamErrorLogItemRecordsUnresolvedFields(t *testing.T) {
	item, err := decodeUpstreamErrorLogItem(json.RawMessage(`{"id":1,"created_at":1}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	got := strings.Join(item.UnresolvedFields, ",")
	for _, want := range []string{"content", "model_name", "token_name", "group", "request_id"} {
		if !strings.Contains(got, want) {
			t.Errorf("未登记缺失字段 %q（列表: %s）", want, got)
		}
	}
	// 命中了就不该登记。样本必须**连 other 一起给全**，否则 other 会被登记。
	full, _ := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":1,"created_at":1,"content":"c","model_name":"m","token_name":"t",` +
			`"group":"g","request_id":"r","upstream_request_id":"u",` +
			`"other":{"channel_name":"ch","channel_id":1,"status_code":500,` +
			`"error_code":"ec","error_type":"et","request_path":"/p"}}`))
	if len(full.UnresolvedFields) != 0 {
		t.Errorf("字段齐全时不应登记: %v", full.UnresolvedFields)
	}
}

// TestDecodeUpstreamErrorLogItemKeepsRawJSON 必须留原文。
//
// 上游日志有保留期，过期了再也拉不回来。字段名将来核准后要能就地重解，
// 前提是原文还在。这是本轮字段名未核实情况下的关键保险。
func TestDecodeUpstreamErrorLogItemKeepsRawJSON(t *testing.T) {
	raw := `{"id":9,"created_at":1787000000,"weird_field":"未来才认识的字段"}`
	item, err := decodeUpstreamErrorLogItem(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if item.Raw != raw {
		t.Errorf("原文未留存，将来无法就地重解: got=%q", item.Raw)
	}
	if !strings.Contains(item.Raw, "weird_field") {
		t.Error("原文应含我们当前不认识的字段")
	}
}

// TestJSONRawToStringHandlesNonStringScalars 上游可能把字段发成数字。
// 只认字符串会让数字型字段静默变空（契约 fixture 里 quota 就有字符串/数字两种形态）。
func TestJSONRawToStringHandlesNonStringScalars(t *testing.T) {
	cases := []struct{ raw, want string }{
		{`"abc"`, "abc"},
		{`123`, "123"},
		{`null`, ""},
		{`{}`, ""},
		{`{"a":1}`, ""}, // 结构化值不当标量用，否则整个对象会塞进 model_name 这类列
		{`[]`, ""},
	}
	for _, tc := range cases {
		if got := jsonRawToString(json.RawMessage(tc.raw)); got != tc.want {
			t.Errorf("jsonRawToString(%s) = %q, want %q", tc.raw, got, tc.want)
		}
	}
}

// TestDecodeUpstreamErrorLogItemReadsOtherNested ★ 本组最要紧的一条 ★
//
// other 里那五个字段是**我方 logs 表完全没有的信息**：我方只知道「打某个上游失败了」，
// 而这里能知道「上游用它自己的哪个渠道去打、对方返回什么状态码和错误类型」。
// 没有它们，这张表只剩一份错误原文，价值大打折扣。
//
// 样本形状取自 2026-08-28 生产实测（963 条真实 type=5 行，other 顶层键固定为
// admin_info / channel_id / error_code / error_type / status_code /
// channel_name / channel_type / request_path）。
func TestDecodeUpstreamErrorLogItemReadsOtherNested(t *testing.T) {
	raw := `{"id":91,"created_at":1787000000,"content":"status_code=502, bad gateway",
		"use_time":125,"channel_name":"",
		"other":{"admin_info":{"use_channel":["66"]},"channel_id":66,
			"channel_name":"kpzhu_gpt_pro","channel_type":1,
			"error_code":"bad_response_status_code","error_type":"new_api_error",
			"status_code":502,"request_path":"/v1/chat/completions"}}`
	item, err := decodeUpstreamErrorLogItem(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	// ★ 渠道名必须从 other 顶层取，不是 other.admin_info.channel_name ★
	// 后者实测恒为 NULL；顶层 channel_name 也是空字符串（用户已在真实响应上确认）。
	if item.UpstreamChannelName != "kpzhu_gpt_pro" {
		t.Errorf("上游渠道名应从 other.channel_name 取: got=%q", item.UpstreamChannelName)
	}
	if item.UpstreamChannelID != 66 {
		t.Errorf("上游渠道 ID: got=%d want=66", item.UpstreamChannelID)
	}
	if item.StatusCode != 502 {
		t.Errorf("HTTP 状态码: got=%d want=502", item.StatusCode)
	}
	if item.ErrorCode != "bad_response_status_code" || item.ErrorType != "new_api_error" {
		t.Errorf("错误分类丢失: code=%q type=%q", item.ErrorCode, item.ErrorType)
	}
	if item.RequestPath != "/v1/chat/completions" {
		t.Errorf("请求路径: got=%q", item.RequestPath)
	}
	// use_time 在顶层，2026-08-28 实测可用。
	if item.UseTime != 125 {
		t.Errorf("耗时应取顶层 use_time: got=%d want=125", item.UseTime)
	}
	// 本样本只带了 other 相关字段（顶层 model_name/token_name 等不在样本里，
	// 被登记是对的）。这里只断言 **other.* 一个都不该未命中**——
	// 那是本用例要钉的东西。
	for _, f := range item.UnresolvedFields {
		if strings.HasPrefix(f, "other.") {
			t.Errorf("other 内的字段未命中: %s（全部: %v）", f, item.UnresolvedFields)
		}
	}
}

// TestDecodeUpstreamErrorLogItemOtherAsEscapedString other 是转义字符串时也要解。
//
// ★ 两种形态都真实存在 ★
// 契约 fixture（channel_upstream_pricing_ledger_test.go）里 other 是**转义的
// JSON 字符串**，我方生产库里是 JSON 对象。只认一种，另一种会静默解不出，
// 而这些字段正是本表的核心价值——静默失败最难发现。
func TestDecodeUpstreamErrorLogItemOtherAsEscapedString(t *testing.T) {
	// 注意 other 的值是字符串，里面才是 JSON。
	raw := `{"id":92,"created_at":1787000001,
		"other":"{\"channel_name\":\"jikesoft_claude_max_1.1\",\"status_code\":429,\"error_type\":\"rate_limit\"}"}`
	item, err := decodeUpstreamErrorLogItem(json.RawMessage(raw))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if item.UpstreamChannelName != "jikesoft_claude_max_1.1" {
		t.Errorf("转义字符串形态的 other 未解出渠道名: got=%q", item.UpstreamChannelName)
	}
	if item.StatusCode != 429 || item.ErrorType != "rate_limit" {
		t.Errorf("转义形态下状态码/类型丢失: %d %q", item.StatusCode, item.ErrorType)
	}
}

// TestDecodeUpstreamErrorLogItemOtherMissingIsRecorded other 缺失或非 JSON 要登记。
// 不登记就无法区分「上游没给这些字段」与「我们解析写错了」。
func TestDecodeUpstreamErrorLogItemOtherMissingIsRecorded(t *testing.T) {
	for _, tc := range []struct{ name, raw string }{
		{"完全没有 other", `{"id":1,"created_at":1}`},
		{"other 是 null", `{"id":1,"created_at":1,"other":null}`},
		{"other 是空串", `{"id":1,"created_at":1,"other":""}`},
		{"other 是坏 JSON 串", `{"id":1,"created_at":1,"other":"{不是JSON"}`},
	} {
		t.Run(tc.name, func(t *testing.T) {
			item, err := decodeUpstreamErrorLogItem(json.RawMessage(tc.raw))
			if err != nil {
				t.Fatalf("整条不该失败（有 id 与 created_at 就值得存）: %v", err)
			}
			if !strings.Contains(strings.Join(item.UnresolvedFields, ","), "other") {
				t.Errorf("other 不可用时须登记: %v", item.UnresolvedFields)
			}
		})
	}
}

// TestDecodeUpstreamErrorLogItemReadsBothRequestIDs 两个 request id 都要取。
//
// request_id 是上游那条日志自己的；upstream_request_id 是上游记录的**它的**上游的。
// 两者语义不同，混淆会让对账串错人。
func TestDecodeUpstreamErrorLogItemReadsBothRequestIDs(t *testing.T) {
	item, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":7,"created_at":1787000000,"request_id":"own-123","upstream_request_id":"theirs-456"}`))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if item.UpstreamRequestID != "own-123" {
		t.Errorf("request_id 应存进 UpstreamRequestID: %q", item.UpstreamRequestID)
	}
	if item.UpstreamUpstreamRequestID != "theirs-456" {
		t.Errorf("upstream_request_id 应存进 UpstreamUpstreamRequestID: %q", item.UpstreamUpstreamRequestID)
	}
}

func TestUpstreamErrorEventKeyIgnoresPageLocalIDOnly(t *testing.T) {
	first, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":1,"created_at":1787000000,"request_id":"same-request","content":"same"}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"content":"same","request_id":"same-request","created_at":1787000000,"id":87}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.EventKey == "" || first.EventKey != second.EventKey {
		t.Fatalf("同一事件只改页内 id 时身份应稳定: %q != %q", first.EventKey, second.EventKey)
	}
	third, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":1,"created_at":1787000000,"request_id":"another-request","content":"same"}`))
	if err != nil {
		t.Fatal(err)
	}
	if third.EventKey == first.EventKey {
		t.Fatal("真实请求证据变化后不得被去重成同一事件")
	}
}

func TestUpstreamErrorEventKeyCanonicalizesNestedObjects(t *testing.T) {
	first, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"id":1,"created_at":1787000000,"other":{"status_code":502,"meta":{"b":2,"a":1}}}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := decodeUpstreamErrorLogItem(json.RawMessage(
		`{"other":{"meta":{"a":1,"b":2},"status_code":502},"created_at":1787000000,"id":99}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.EventKey != second.EventKey {
		t.Fatalf("嵌套对象仅字段顺序变化不应改变事件身份: %q != %q", first.EventKey, second.EventKey)
	}
}

// TestBoundedUpstreamErrorFieldKeepsUTF8Intact 截断不得切坏 UTF-8。
//
// 中文错误原文很常见（"无效的令牌，数据库查询出错" 这类）。按字节截到一半会产生
// 非法编码，存进去再读出来是乱码，而乱码的错误原文对排障毫无价值。
func TestBoundedUpstreamErrorFieldKeepsUTF8Intact(t *testing.T) {
	// "错误" 是 6 字节；截到 4 会落在第二个字符中间。
	got := boundedUpstreamErrorField("错误", 4)
	if !isValidUTF8(got) {
		t.Errorf("截断产生非法 UTF-8: %q", got)
	}
	if got != "错" {
		t.Errorf("应退到字符边界得到「错」: %q", got)
	}
	// 不超长时原样返回（去首尾空白）。
	if got := boundedUpstreamErrorField("  abc  ", 100); got != "abc" {
		t.Errorf("未超长应去空白后原样返回: %q", got)
	}
}

func isValidUTF8(s string) bool {
	for _, r := range s {
		if r == 0xFFFD {
			return false
		}
	}
	return true
}

// TestSyncUpstreamErrorLogWindowRejectsNonNewAPI 另两家必须显式拒绝。
//
// ★ 本文件最要紧的一条 ★
// sub2api 的 /api/v1/usage 是聚合查询（只返回 total_requests/total_tokens/
// total_actual_cost），aicodewith 的 /usage/details 是计价明细。
// 硬拿它们当日志接口用，会得到「格式不对」或更糟——静默解出一堆空记录。
func TestSyncUpstreamErrorLogWindowRejectsNonNewAPI(t *testing.T) {
	for _, provider := range []string{upstreamProviderSub2API, upstreamProviderAICodeWith, "", "unknown"} {
		row := ChannelUpstreamAccount{Domain: "d.example", Provider: provider}
		_, err := syncUpstreamErrorLogWindow(context.Background(), http.DefaultClient,
			row, newAPICredential{AccessToken: "t"}, 1000, 2000,
			newUpstreamUsageRequestPacer(10, 0), 1787000000)
		if err == nil {
			t.Errorf("provider=%q 无日志接口，必须拒绝而不是硬拉", provider)
		}
	}
}

// TestSyncUpstreamErrorLogWindowRequestsType5 必须真的请求 type=5。
//
// 拉成 type=2 会得到消费日志——那是渠道管理已有的东西，且量级差两个数量级
// （我方一天 4.4 万条 type=2 vs 几百条 type=5），会把上游接口打满。
func TestSyncUpstreamErrorLogWindowRequestsType5(t *testing.T) {
	var gotType, gotUser, gotAuth string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/log/self" {
			t.Errorf("端点错了: %s（/api/log/ 是管理员接口，普通凭据会 403）", r.URL.Path)
		}
		gotType = r.URL.Query().Get("type")
		gotUser = r.Header.Get("New-Api-User")
		gotAuth = r.Header.Get("Authorization")
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[],"total":0}}`))
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31}
	_, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787000000)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if gotType != "5" {
		t.Errorf("必须请求 type=5（错误日志），got=%q", gotType)
	}
	if gotUser != "31" || gotAuth != "Bearer tok" {
		t.Errorf("认证头不对: user=%q auth=%q", gotUser, gotAuth)
	}
}

// TestSyncUpstreamErrorLogWindowKeepsPerRowDetail 必须保留逐条明细，不聚合。
//
// 这是本功能与渠道管理用量同步的**根本差别**：那边拿到逐条后立刻折进小时桶
// （bucket.Requests++），把时间、模型、原文全丢了。这里丢了就等于没做。
func TestSyncUpstreamErrorLogWindowKeepsPerRowDetail(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[
			{"id":11,"created_at":1787000001,"model_name":"gpt-5.4","content":"status_code=502, bad gateway","token_name":"tk","group":"g","request_id":"r1"},
			{"id":12,"created_at":1787000002,"model_name":"claude-sonnet-4-6","content":"status_code=429, rate limited","request_id":"r2","upstream_request_id":"u2"}
		],"total":2}}`))
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31}
	res, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787009999)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(res.Rows) != 2 {
		t.Fatalf("应保留 2 条明细，got=%d（聚合掉了就等于没做）", len(res.Rows))
	}
	first := res.Rows[0]
	if first.UpstreamID != 11 || first.ModelName != "gpt-5.4" ||
		!strings.Contains(first.Content, "502") || first.TokenName != "tk" {
		t.Errorf("明细字段丢失: %+v", first)
	}
	if first.Domain != "d.example" || first.FetchedAt != 1787009999 {
		t.Errorf("域名/抓取时刻未落: domain=%q fetched=%d", first.Domain, first.FetchedAt)
	}
	if res.Rows[1].UpstreamUpstreamRequestID != "u2" {
		t.Errorf("第二条的上游的上游 request id 丢了: %+v", res.Rows[1])
	}
}

// TestSyncUpstreamErrorLogWindowDeduplicatesAcrossPages 翻页期间的重复条目要去重。
//
// 上游在我方翻页时仍在写入新日志，同一条可能在两页都出现。主键能兜住写入，
// 但返回的 Rows 若含重复，调用方统计条数会偏大。
func TestSyncUpstreamErrorLogWindowDeduplicatesAcrossPages(t *testing.T) {
	page := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		page++
		p := r.URL.Query().Get("p")
		// 101 条才会真正翻两页。服务端每页都把 id 从 1 重编，
		// 模拟未修改 NewAPI 的真实行为；事件身份必须不依赖这个 id。
		if p == "1" {
			items := make([]string, 0, 100)
			for i := 0; i < 100; i++ {
				items = append(items, fmt.Sprintf(`{"id":%d,"created_at":1787000001,"request_id":"r-%03d"}`, i+1, i))
			}
			_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[%s],"total":101}}`, strings.Join(items, ","))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[
			{"id":1,"created_at":1787000002,"request_id":"r-100"}],"total":101}}`))
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31}
	res, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787000000)
	if err != nil {
		t.Fatalf("sync: %v", err)
	}
	if len(res.Rows) != 101 {
		t.Fatalf("页内 id 重编不得把不同事件合并，got=%d want=101", len(res.Rows))
	}
	seen := map[string]int{}
	for _, r := range res.Rows {
		seen[r.EventKey]++
	}
	for key, n := range seen {
		if n != 1 {
			t.Errorf("event_key=%s 出现 %d 次", key, n)
		}
	}
	if page != 3 { // 首页 + 第 2 页 + 首页稳定性复核
		t.Errorf("应做完整扫描后的首页复核，requests=%d", page)
	}
}

func TestSyncUpstreamErrorLogWindowRejectsEventKeyCollision(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		// NewAPI rewrites id as a page-local ordinal. If two reported events are
		// otherwise identical, their distinctness cannot be proved safely.
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[
			{"id":1,"created_at":1787000001,"request_id":"same"},
			{"id":2,"created_at":1787000001,"request_id":"same"}],"total":2}}`))
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	res, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787000000)
	if err == nil || !strings.Contains(err.Error(), "事件键碰撞或分页漂移") {
		t.Fatalf("事件键碰撞必须 fail closed: err=%v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("碰撞窗口不得返回部分证据: rows=%d", len(res.Rows))
	}
}

func TestSyncUpstreamErrorLogWindowRejectsDuplicateAcrossPages(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("p") == "1" {
			items := make([]string, 100)
			for i := range items {
				items[i] = fmt.Sprintf(`{"id":%d,"created_at":1787000001,"request_id":"r-%d"}`, i+1, i)
			}
			_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[%s],"total":101}}`, strings.Join(items, ","))
			return
		}
		// Page two repeats an event from page one while still reporting 101.
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[
			{"id":1,"created_at":1787000001,"request_id":"r-99"}],"total":101}}`))
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	res, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787000000)
	if err == nil || !strings.Contains(err.Error(), "事件键碰撞或分页漂移") {
		t.Fatalf("跨页重复造成 unique<total 必须 fail closed: err=%v", err)
	}
	if len(res.Rows) != 0 {
		t.Fatalf("跨页漂移窗口不得返回部分证据: rows=%d", len(res.Rows))
	}
}

// TestSyncUpstreamErrorLogWindowMarksTruncated 过密窗口必须缩窗重试。
//
// ★ 这条关系到「缺失绝不显示为零」★
// 读到一半就停而不告知，运营会以为「上游那段时间只有这些错误」。
// 部分页不能落库，否则进水位后会永久丢失未读页。
func TestSyncUpstreamErrorLogWindowMarksTruncated(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// total 恒为 1000，永远读不完，必然撞预算。
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[
			{"id":` + r.URL.Query().Get("p") + `,"created_at":1787000001}],"total":1000}}`))
	}))
	defer server.Close()

	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31}
	res, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(3, 0), 1787000000) // 预算只有 3 次
	if err != nil {
		t.Fatalf("预算耗尽不该报错，应返回已读部分: %v", err)
	}
	if !res.Truncated {
		t.Error("窗口未读完必须标 Truncated，否则会被当成「上游只有这些错误」")
	}
	if len(res.Rows) != 0 {
		t.Error("过密窗口不得返回部分页供落库")
	}
	if res.SuggestedTo <= 1000 || res.SuggestedTo >= 2000 {
		t.Errorf("应给出可持久化的缩窗右边界: %d", res.SuggestedTo)
	}
}

func TestSyncUpstreamErrorLogWindowRejectsTotalDrift(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("p") == "1" {
			items := make([]string, 100)
			for i := range items {
				items[i] = fmt.Sprintf(`{"id":%d,"created_at":1787000001,"request_id":"r-%d"}`, i+1, i)
			}
			_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[%s],"total":101}}`, strings.Join(items, ","))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[{"id":1,"created_at":1787000002,"request_id":"r-100"}],"total":102}}`))
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	_, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787000000)
	if err == nil || !strings.Contains(err.Error(), "total 变化") {
		t.Fatalf("扫描期间 total 变化必须放弃本窗口: %v", err)
	}
}

func TestSyncUpstreamErrorLogWindowRejectsFirstPageDrift(t *testing.T) {
	firstPageCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Query().Get("p") == "1" {
			firstPageCalls++
			items := make([]string, 100)
			for i := range items {
				requestID := fmt.Sprintf("r-%d", i)
				if firstPageCalls > 1 && i == 0 {
					requestID = "newly-inserted"
				}
				items[i] = fmt.Sprintf(`{"id":%d,"created_at":1787000001,"request_id":"%s"}`, i+1, requestID)
			}
			_, _ = fmt.Fprintf(w, `{"success":true,"data":{"items":[%s],"total":101}}`, strings.Join(items, ","))
			return
		}
		_, _ = w.Write([]byte(`{"success":true,"data":{"items":[{"id":1,"created_at":1787000002,"request_id":"r-100"}],"total":101}}`))
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "d.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 31}
	_, err := syncUpstreamErrorLogWindow(context.Background(), server.Client(), row,
		newAPICredential{AccessToken: "tok"}, 1000, 2000,
		newUpstreamUsageRequestPacer(10, 0), 1787000000)
	if err == nil || !strings.Contains(err.Error(), "首页已变化") {
		t.Fatalf("首页指纹变化必须放弃本窗口: %v", err)
	}
}

// TestPersistUpstreamErrorLogsIsIdempotent 重复落库不产生重复行。
//
// 这张表要能被反复回填（同一窗口重跑），所以主键取
// (domain, event_key) 而非页内伪 upstream_id 或自增值。
func TestPersistUpstreamErrorLogsIsIdempotent(t *testing.T) {
	m := newTestMonitor(t)
	rows := []ChannelUpstreamErrorLog{
		{Domain: "d.example", EventKey: "event-1", UpstreamID: 1, CreatedAt: 100, Content: "第一次", FetchedAt: 1},
		{Domain: "d.example", EventKey: "event-2", UpstreamID: 2, CreatedAt: 200, Content: "另一条", FetchedAt: 1},
	}
	if err := m.persistUpstreamErrorLogs(context.Background(), rows); err != nil {
		t.Fatalf("首次落库: %v", err)
	}
	// 同一条再来一次，内容变了（上游补全了原文）。
	rows[0].Content = "第二次·已补全"
	rows[0].FetchedAt = 2
	if err := m.persistUpstreamErrorLogs(context.Background(), rows); err != nil {
		t.Fatalf("重复落库: %v", err)
	}

	var count int64
	if err := m.storeDB.Model(&ChannelUpstreamErrorLog{}).Count(&count).Error; err != nil {
		t.Fatalf("计数: %v", err)
	}
	if count != 2 {
		t.Errorf("重复落库应 upsert 不应翻倍，got=%d want=2", count)
	}
	var got ChannelUpstreamErrorLog
	if err := m.storeDB.First(&got, "domain = ? AND event_key = ?", "d.example", "event-1").Error; err != nil {
		t.Fatalf("回读: %v", err)
	}
	if got.Content != "第二次·已补全" || got.FetchedAt != 2 {
		t.Errorf("upsert 应更新内容与抓取时刻: %+v", got)
	}
}

// TestPersistUpstreamErrorLogsKeepsRawJSON 原文必须真的进库。
//
// decoder 留了原文但落库丢掉，等于没留——而这是字段名核准后能否就地重解的前提。
// 两者各自的用例都会绿，只有这条能发现断点。
func TestPersistUpstreamErrorLogsKeepsRawJSON(t *testing.T) {
	m := newTestMonitor(t)
	raw := `{"id":5,"created_at":700,"unknown_future_field":"x"}`
	if err := m.persistUpstreamErrorLogs(context.Background(), []ChannelUpstreamErrorLog{
		{Domain: "d", EventKey: "event-5", UpstreamID: 5, CreatedAt: 700, RawJSON: raw},
	}); err != nil {
		t.Fatalf("落库: %v", err)
	}
	var got ChannelUpstreamErrorLog
	if err := m.storeDB.First(&got, "domain = ? AND event_key = ?", "d", "event-5").Error; err != nil {
		t.Fatalf("回读: %v", err)
	}
	if got.RawJSON != raw {
		t.Errorf("原文未落库，将来无法就地重解: got=%q", got.RawJSON)
	}
}

// TestPruneUpstreamErrorLogsRespectsCutoff 清理只删早于截止时间的。
// 这张表按条存，不清理会无界增长；但删多了就把还有用的排障线索删了。
func TestPruneUpstreamErrorLogsRespectsCutoff(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.persistUpstreamErrorLogs(context.Background(), []ChannelUpstreamErrorLog{
		{Domain: "d", EventKey: "event-1", UpstreamID: 1, CreatedAt: 100},
		{Domain: "d", EventKey: "event-2", UpstreamID: 2, CreatedAt: 500},
	}); err != nil {
		t.Fatalf("落库: %v", err)
	}
	if err := m.storeDB.Create(&[]UpstreamErrorLogSyncState{
		{Domain: "d", CoverageFrom: 100, SyncedUntil: 600, LastSuccessAt: 600, Status: upstreamStatusOK},
		{Domain: "expired", CoverageFrom: 100, SyncedUntil: 200, LastSuccessAt: 200, Status: upstreamStatusOK},
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneUpstreamErrorLogs(context.Background(), 300); err != nil {
		t.Fatalf("清理: %v", err)
	}
	var ids []int64
	if err := m.storeDB.Model(&ChannelUpstreamErrorLog{}).Pluck("upstream_id", &ids).Error; err != nil {
		t.Fatalf("回读: %v", err)
	}
	if len(ids) != 1 || ids[0] != 2 {
		t.Errorf("应只留 created_at>=300 的那条，got=%v", ids)
	}
	var active, expired UpstreamErrorLogSyncState
	if err := m.storeDB.First(&active, "domain = ?", "d").Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&expired, "domain = ?", "expired").Error; err != nil {
		t.Fatal(err)
	}
	if active.CoverageFrom != 300 || active.SyncedUntil != 600 ||
		expired.CoverageFrom != 0 || expired.SyncedUntil != 0 ||
		expired.WindowFrom != 0 || expired.WindowTo != 0 {
		t.Fatalf("保留期清理必须收缩有效覆盖并重置完全过期水位: active=%+v expired=%+v",
			active, expired)
	}
	// before<=0 是「不清理」，不能当成「删全部」。
	if err := m.pruneUpstreamErrorLogs(context.Background(), 0); err != nil {
		t.Fatalf("清理(0): %v", err)
	}
	var count int64
	_ = m.storeDB.Model(&ChannelUpstreamErrorLog{}).Count(&count).Error
	if count != 1 {
		t.Errorf("before<=0 应是空操作，got=%d", count)
	}
}

// TestUpstreamErrorLogTableIsRegisteredForMigration 新表必须进 AutoMigrate 模型集，
// 且 plan ID 必须已 bump——两者缺一，生产上会出现「表不存在」或「回滚点被剪掉」。
func TestUpstreamErrorLogTableIsRegisteredForMigration(t *testing.T) {
	// 读源码而非反射：AutoMigrate 的模型集是个字面量列表，反射拿不到
	// 「有没有被列进去」。与 logchain_radius_wiring_test.go 读 logchain.go 同一做法。
	srcBytes, err := os.ReadFile("store.go")
	if err != nil {
		t.Fatalf("读 store.go: %v", err)
	}
	if !strings.Contains(string(srcBytes), "&ChannelUpstreamErrorLog{}") {
		t.Error("新表未注册进 AutoMigrate 模型集，生产上会「表不存在」")
	}
	if !strings.Contains(string(srcBytes), "&ChannelUpstreamUsageArchive{}") ||
		!strings.Contains(string(srcBytes), "&ChannelUpstreamErrorLogArchive{}") {
		t.Error("上游身份切换归档表未完整注册进 AutoMigrate 模型集")
	}
	planBytes, err := os.ReadFile("store_migration_backup.go")
	if err != nil {
		t.Fatalf("读 store_migration_backup.go: %v", err)
	}
	plan := string(planBytes)
	// 旧 plan ID 不得残留：模型集变了却不 bump，会让新镜像复用旧快照，
	// 旧镜像的回滚点被当成本次的迁移前快照。
	if strings.Contains(plan, `"main-facts-schema-20260825-v18-pricing-adapters"`) {
		t.Error("AutoMigrate 模型集已变但 preMigrationPlanID 未 bump")
	}
	if strings.Contains(plan, `preMigrationPlanID = "main-facts-schema-20260831-v27-upstream-errorlog-event-key-coverage"`) {
		t.Error("新增身份归档表后仍复用 v27 迁移计划")
	}
	if !strings.Contains(plan, "upstream-errorlog") {
		t.Error("preMigrationPlanID 未标出上游错误日志变更")
	}
	if !strings.Contains(plan, "identity-archive") {
		t.Error("preMigrationPlanID 未标出上游身份归档变更")
	}
}
