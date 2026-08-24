package monitor

// logchain_test.go:客户排障链路的行为约束测试。
// 重点不是"字段能查出来",而是三件容易在后续改动中被悄悄破坏的事:
//  1. content 必须绕过 scrubContent(否则最有用的上游错误原文正好全空)
//  2. 时间跨度/limit 必须收敛,且收敛结果要回显
//  3. 渠道快照查不到时必须标 unresolved,不能留空字段冒充"没有上游域名"

import (
	"context"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

// newLogChainCtx 造一个只带 query 的 gin.Context,用于解析层单测。
func newLogChainCtx(rawQuery string) *gin.Context {
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/logchain/requests?"+rawQuery, nil)
	c.Request.URL.RawQuery = rawQuery
	return c
}

// TestScrubContentWouldBlankUpstreamErrors 证明"绕过 scrubContent"这个决定不是多余的:
// new-api 的上游错误原文常含"渠道"二字,一旦过 scrubContent 就整段变空。
// 这个测试是给后续改动者的护栏——若有人把 logchain 的 content 接回 scrubContent,
// 排障功能会静默失效(有行、有时间、错误原文全空白),这里先把风险钉住。
func TestScrubContentWouldBlankUpstreamErrors(t *testing.T) {
	upstreamErr := "渠道 gpt-relay (#12) 返回错误：status_code=429 rate limit exceeded"
	if got := scrubContent(upstreamErr); got != "" {
		t.Fatalf("前提变了:scrubContent 不再清空含“渠道”的内容,got=%q", got)
	}
	// 反向确认:不含"渠道"的错误原文本来就不会被清空,所以问题只出在这一类上。
	plain := "status_code=500 upstream internal error"
	if got := scrubContent(plain); got != plain {
		t.Fatalf("scrubContent 误伤了不含“渠道”的内容:got=%q", got)
	}
}

func TestParseLogChainScopeClampsSpanAndLimit(t *testing.T) {
	now := time.Date(2026, 8, 19, 15, 30, 0, 0, cstLocation)

	s, err := parseLogChainScope(newLogChainCtx("days=9999&limit=99999"), now)
	if err != nil {
		t.Fatalf("parseLogChainScope: %v", err)
	}
	if s.Limit != logChainMaxLimit {
		t.Errorf("limit 未收敛到上限: got=%d want=%d", s.Limit, logChainMaxLimit)
	}
	span := time.Unix(s.ToTs, 0).Sub(time.Unix(s.FromTs, 0))
	maxSpan := time.Duration(logChainMaxDays) * 24 * time.Hour
	if span > maxSpan {
		t.Errorf("时间跨度未收敛: got=%v max=%v", span, maxSpan)
	}
	// 默认应含"现在"这一秒,否则刚发生的请求查不到。
	if s.ToTs <= now.Unix() {
		t.Errorf("to_ts 未包含当前秒: to=%d now=%d", s.ToTs, now.Unix())
	}
}

func TestParseLogChainScopeRejectsContradictions(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, cstLocation)
	cases := []struct {
		name  string
		query string
	}{
		{"只给 from", "from=2026-08-01"},
		{"只给 to", "to=2026-08-05"},
		{"to 早于 from", "from=2026-08-10&to=2026-08-01"},
		{"日期格式错", "from=08/01/2026&to=2026-08-05"},
		{"error_only 与 type 冲突", "error_only=true&type=2"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseLogChainScope(newLogChainCtx(tc.query), now); err == nil {
				t.Fatal("期望报错,实际通过")
			}
		})
	}
	// error_only + type=5 不矛盾,应放行。
	if _, err := parseLogChainScope(newLogChainCtx("error_only=true&type=5"), now); err != nil {
		t.Fatalf("error_only=true&type=5 应放行: %v", err)
	}
}

func TestParseLogChainScopeExplicitRangeTruncatesFromHead(t *testing.T) {
	now := time.Date(2026, 8, 19, 12, 0, 0, 0, cstLocation)
	// 跨度 200 天,应被截成最近 logChainMaxDays 天(保留 to 端,砍 from 端)。
	s, err := parseLogChainScope(newLogChainCtx("from=2026-01-01&to=2026-07-19"), now)
	if err != nil {
		t.Fatalf("parseLogChainScope: %v", err)
	}
	span := time.Unix(s.ToTs, 0).Sub(time.Unix(s.FromTs, 0))
	if span > time.Duration(logChainMaxDays)*24*time.Hour {
		t.Fatalf("显式区间未截断: span=%v", span)
	}
	wantTo := time.Date(2026, 7, 20, 0, 0, 0, 0, cstLocation).Unix() // to 当天整日纳入
	if s.ToTs != wantTo {
		t.Errorf("截断应保留 to 端: got=%d want=%d", s.ToTs, wantTo)
	}
}

// TestLogChainWhereIncludesErrorLogs 钉住最关键的口径:排障必须能看到 type=5。
// 事实表是 type IN (2,6),照抄过来就会把错误日志全滤掉,这正是本接口不建在事实表上的原因。
func TestLogChainWhereIncludesErrorLogs(t *testing.T) {
	base := logChainScope{FromTs: 100, ToTs: 200, Limit: 50}

	where, _ := logChainWhere(base, nil)
	if !strings.Contains(where, "type IN (2,5)") {
		t.Errorf("默认应含错误日志(type IN (2,5)): %s", where)
	}
	if strings.Contains(where, "type IN (2,6)") {
		t.Error("误用了事实表口径 type IN (2,6),会滤掉全部错误日志")
	}

	errOnly := base
	errOnly.ErrorOnly = true
	if where, _ := logChainWhere(errOnly, nil); !strings.Contains(where, "type = 5") {
		t.Errorf("error_only 应筛 type=5: %s", where)
	}

	// 渠道测试流量必须排除,否则自测请求混进客户排障结果。
	if !strings.Contains(where, "NOT (") {
		t.Errorf("未排除渠道测试流量: %s", where)
	}
}

// TestLogChainWhereParameterizesUserInput 所有用户可控值必须走占位符。
// 直接把 keyword/token_name 拼进 SQL 是注入,且 LIKE 通配符不转义会拖慢生产库查询。
func TestLogChainWhereParameterizesUserInput(t *testing.T) {
	s := logChainScope{
		FromTs: 100, ToTs: 200, Limit: 50,
		UserID: 7, ChannelID: 12, Model: "gpt-4o", Group: "vip",
		TokenName: "50%_off", RequestID: "req-1", Keyword: "'; DROP TABLE logs; --",
		BeforeTs: 150, BeforeID: 999,
	}
	where, args := logChainWhere(s, []int64{3, 4})

	if strings.Contains(where, "DROP TABLE") || strings.Contains(where, "gpt-4o") {
		t.Fatalf("用户输入被拼进 SQL: %s", where)
	}
	if n := strings.Count(where, "?"); n != len(args) {
		t.Errorf("占位符与参数数量不匹配: 占位符=%d 参数=%d\nSQL: %s", n, len(args), where)
	}
	// LIKE 值必须带转义:未转义的 % 和 _ 会变成泛匹配。
	foundEscaped := false
	for _, a := range args {
		if str, ok := a.(string); ok && strings.Contains(str, "50!%!_off") {
			foundEscaped = true
		}
	}
	if !foundEscaped {
		t.Errorf("token_name 的 LIKE 通配符未转义: args=%v", args)
	}
	if !strings.Contains(where, "ESCAPE '!'") {
		t.Errorf("LIKE 缺少 ESCAPE 子句: %s", where)
	}
	if !strings.Contains(where, "created_at < ? OR (created_at = ? AND id < ?)") {
		t.Errorf("复合游标条件缺失: %s", where)
	}
}

// TestLogChainOrdersByOccurrenceTime 排序必须按发生时间，不能按 id。
//
// new-api 在请求**完成时**写日志：一个耗时 60s 的超时请求会比后发起、快速失败的
// 请求更晚写入，因此 id 序 ≠ 发生时间序。用户要求"按发生错误的时间顺序排列"，
// 这条在 fixture 实测中真实暴露过（15:40、14:02、09:13 排在 13:22 之前）。
func TestLogChainOrdersByOccurrenceTime(t *testing.T) {
	// queryLogChain 在 prodDB 为 nil 时提前返回，拿不到完整 SQL；
	// 故直接断言排序子句本身——实现与测试共用 logChainOrderBySQL 这一份字面量，
	// 不会出现"改了 SQL 但测试还在断言旧写法"的漂移。
	sql := logChainOrderBySQL(false)
	if !strings.Contains(sql, "created_at DESC") {
		t.Errorf("必须按 created_at 倒序: %s", sql)
	}
	if !strings.Contains(sql, "id DESC") {
		t.Errorf("同秒多条需用 id 破平以保证顺序稳定: %s", sql)
	}
	if strings.HasPrefix(strings.TrimSpace(sql), "ORDER BY id DESC") {
		t.Error("不得以 id 为首要排序键：id 序不等于发生时间序")
	}
	// 正序方向同样要以 created_at 为首要键。
	if asc := logChainOrderBySQL(true); !strings.Contains(asc, "created_at ASC") ||
		!strings.Contains(asc, "id ASC") {
		t.Errorf("正序也须按 created_at 为首要键并用 id 破平: %s", asc)
	}
}

// TestLogChainCursorFollowsSortDirection 游标比较方向必须跟随排序方向：
// 倒序取更早的(<)，正序取更晚的(>)。方向写死会让"加载更多"在正序下往回翻，
// 重复吐出已经看过的行——这类 bug 首页看不出来，只在翻第二页时才暴露。
func TestLogChainCursorFollowsSortDirection(t *testing.T) {
	base := logChainScope{FromTs: 100, ToTs: 200, Limit: 50, BeforeTs: 150, BeforeID: 9}

	// 必须只比对"游标带来的增量"，不能拿整串 WHERE 去搜片段：
	// 时间窗上界本身就是 `created_at < ?`，与排序方向无关且恒存在，
	// 直接搜会把它当成游标条件，做出假判断。
	cursorClause := func(s logChainScope) string {
		withCursor, _ := logChainWhere(s, nil)
		noCursor := s
		noCursor.BeforeTs, noCursor.BeforeID = 0, 0
		base, _ := logChainWhere(noCursor, nil)
		return strings.TrimPrefix(withCursor, base)
	}

	desc := base
	descCur := cursorClause(desc)
	if !strings.Contains(descCur, "created_at < ?") || !strings.Contains(descCur, "id < ?") {
		t.Errorf("倒序游标应取更早的(<): %s", descCur)
	}
	if strings.Contains(descCur, ">") {
		t.Errorf("倒序游标不应出现 >: %s", descCur)
	}

	asc := base
	asc.Asc = true
	ascCur := cursorClause(asc)
	if !strings.Contains(ascCur, "created_at > ?") || !strings.Contains(ascCur, "id > ?") {
		t.Errorf("正序游标应取更晚的(>): %s", ascCur)
	}
	if strings.Contains(ascCur, "<") {
		t.Errorf("正序游标不应出现 <: %s", ascCur)
	}

	// 方向变了但参数个数与占位符仍须匹配，否则 driver 直接报错。
	ascWhere, ascArgs := logChainWhere(asc, nil)
	if n := strings.Count(ascWhere, "?"); n != len(ascArgs) {
		t.Errorf("占位符与参数数量不匹配: 占位符=%d 参数=%d", n, len(ascArgs))
	}
}

// TestParseLogChainScopeOrderParam order 只认 "asc"，其余一律默认倒序。
// 不把用户字符串带进 SQL（只转 bool），因此不存在排序注入。
func TestParseLogChainScopeOrderParam(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, cstLocation)
	cases := map[string]bool{
		"":                 false, // 默认倒序
		"order=asc":        true,
		"order=ASC":        true, // 大小写不敏感
		"order=desc":       false,
		"order=created_at": false, // 非法值走默认，不报错也不进 SQL
		"order=id+DESC--":  false, // 注入尝试同样只落成 false
		"order=%20asc%20":  true,  // 前后空格应被 TrimSpace 吃掉
	}
	for q, wantAsc := range cases {
		s, err := parseLogChainScope(newLogChainCtx(q), now)
		if err != nil {
			t.Fatalf("%q 不应报错: %v", q, err)
		}
		if s.Asc != wantAsc {
			t.Errorf("%q: Asc=%v want=%v", q, s.Asc, wantAsc)
		}
	}
}

// TestLogChainCursorRequiresBothParts 游标是 (created_at, id) 复合键，
// 只给一半无法定位续查位置。静默忽略会让"加载更多"从头再来、出现重复行。
func TestLogChainCursorRequiresBothParts(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, cstLocation)
	for _, q := range []string{"before_id=100", "before_ts=1700000000"} {
		if _, err := parseLogChainScope(newLogChainCtx(q), now); err == nil {
			t.Errorf("只提供半个游标应报错: %s", q)
		}
	}
	s, err := parseLogChainScope(newLogChainCtx("before_ts=1700000000&before_id=100"), now)
	if err != nil {
		t.Fatalf("成对提供游标应放行: %v", err)
	}
	if s.BeforeTs != 1700000000 || s.BeforeID != 100 {
		t.Errorf("游标解析错误: ts=%d id=%d", s.BeforeTs, s.BeforeID)
	}
}

// TestLogChainAnomalyRejectsConflicts 三类冲突必须显式报错，不静默忽略。
// 静默的后果是人拿着错的结果当真：与 error_only 同传必然 0 行（交集为空）、
// 与 type≠2 同传同理、取值拼错会退化成"全部请求"而人以为在看异常清单。
func TestLogChainAnomalyRejectsConflicts(t *testing.T) {
	now := time.Date(2026, 8, 20, 12, 0, 0, 0, cstLocation)
	for _, q := range []string{
		"anomaly=stream&error_only=true",
		"anomaly=billing&type=5",
		"anomaly=bogus",
		"anomaly=STREAM", // 取值大小写敏感，避免与后端 SQL 常量不一致
	} {
		if _, err := parseLogChainScope(newLogChainCtx(q), now); err == nil {
			t.Errorf("%q 应被拒绝", q)
		}
	}
	// 合法组合必须放行，且 type=2 与异常不冲突（异常判据本身就限定 type=2）。
	for _, q := range []string{
		"anomaly=stream", "anomaly=billing", "anomaly=billing_unpaid",
		"anomaly=billing_free", "anomaly=all", "anomaly=stream&type=2",
	} {
		if _, err := parseLogChainScope(newLogChainCtx(q), now); err != nil {
			t.Errorf("%q 应放行: %v", q, err)
		}
	}
}

// TestLogChainStreamAnomalyUsesExclusion 流状态必须用排除法而非枚举法。
// 枚举漏掉的新取值会被静默吞掉 —— new-api 升级新增 end_reason 时，
// 枚举写法会假装它不存在，而排障最怕"没见过的情况被藏起来"。
func TestLogChainStreamAnomalyUsesExclusion(t *testing.T) {
	sql := logChainAnomalySQL(anomalyStream)
	// 断言"用了 NOT IN 排除法"这个性质，而不是某种拼接写法：
	// 名单加成员（如 2026-08-21 加入 done）时字面量会变，但行为没变。
	// 绑死字面量的断言会在这种情况下变红，属测试过度绑定。
	if !strings.Contains(sql, "NOT IN (") {
		t.Errorf("必须用 NOT IN 排除法: %s", sql)
	}
	// 名单里每个正常值都必须出现在排除列表里。空串代表非流式请求
	// （other 里没有 stream_status），漏掉它会让全部非流式请求被判成异常。
	for _, normal := range logChainNormalEndReasons {
		if !strings.Contains(sql, "'"+normal+"'") {
			t.Errorf("排除列表缺正常取值 %q: %s", normal, sql)
		}
	}
	// 出现具体**故障**值的枚举，说明退回了枚举法——那会让新取值被静默吞掉。
	//
	// 注意 client_gone 不在这个名单里：它已独立成一档（anomalyClientGone），
	// 因此**合法地出现在排除列表**（NOT IN）中。这与"枚举故障值"是两件事：
	//   枚举法（错）：end_reason IN ('client_gone','timeout',...)  ← 漏掉的新值被吞
	//   排除法（对）：end_reason NOT IN ('','eof','done','client_gone') ← 新值仍会落进来
	for _, enumerated := range []string{"'timeout'", "'scanner_error'", "'panic'", "'ping_fail'"} {
		if strings.Contains(sql, enumerated) {
			t.Errorf("不得枚举具体故障值(%s)，新取值会被吞掉: %s", enumerated, sql)
		}
	}
	// 排除法的核心性质：必须是 NOT IN 而不是 IN。写成 IN 就是枚举法了。
	if strings.Contains(sql, anomalyEndReasonSQL+" IN (") {
		t.Errorf("流故障判据必须用 NOT IN（排除法），出现正向 IN 说明退回枚举: %s", sql)
	}
	// client_gone 必须被排除出「流故障」这一档——它已独立成档。
	if !strings.Contains(sql, "'"+logChainClientGoneEndReason+"'") {
		t.Errorf("流故障判据应把 client_gone 排除在外（它已独立成档）: %s", sql)
	}
	// 流状态只在消费日志上有意义；不限定会与「错误」筛选重叠、双重计数。
	if !strings.Contains(sql, "type = 2") {
		t.Errorf("流异常须限定 type=2: %s", sql)
	}
}

// TestLogChainNormalEndReasonsSharedBySQLAndTags 正常结束名单必须两侧共用。
//
// 背景：done 曾被误判成"流未正常结束"。2026-08-21 在生产真实数据上发现——
// 当天 20 条 done 全部真交付（平均 741 输出 token、31 秒），与 eof 无实质差别。
// 这个误报只能靠真数据发现：代码、注释、文档里都没有 done 这个取值。
//
// 修法不是"在两处各加一个值"，而是把名单收成单一事实源，让漂移不可能发生。
func TestLogChainNormalEndReasonsSharedBySQLAndTags(t *testing.T) {
	// 名单里的每个取值：SQL 侧必须排除，Go 侧必须判为正常，两者不得分歧。
	for _, normal := range logChainNormalEndReasons {
		if !logChainIsNormalEndReason(normal) {
			t.Errorf("Go 侧未把名单成员 %q 判为正常", normal)
		}
		if !strings.Contains(logChainStreamAnomalySQL(), "'"+normal+"'") {
			t.Errorf("SQL 侧未排除名单成员 %q", normal)
		}
		// 标签侧同样不得给它打 stream 标签。
		row := LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: normal, CompletionTokens: 10}
		for _, tag := range logChainAnomalyTags(row, 100) {
			if tag == "stream" {
				t.Errorf("正常取值 %q 不应被打 stream 标签", normal)
			}
		}
	}
	// done 必须在名单里——这是本次修复的实质内容，写成显式断言防止被回退。
	if !logChainIsNormalEndReason("done") {
		t.Error("done 是正常结束（生产实测 20/20 真交付），不得判为流异常")
	}
	// 反向：没见过的新取值仍必须算异常，排除法的意义就在这里。
	if logChainIsNormalEndReason("brand_new_reason_v9") {
		t.Error("未知取值必须算异常，否则 new-api 新增取值会被静默吞掉")
	}
	// client_gone 不在正常名单里：排障页要看它（客户的实际体验是"回答没出来"）。
	if logChainIsNormalEndReason("client_gone") {
		t.Error("client_gone 必须算异常——排障页要的恰恰是它")
	}
}

// TestLogChainBillingAnomalyBothDirections 消费异常两个方向都要，且各有必须的排除项。
func TestLogChainBillingAnomalyBothDirections(t *testing.T) {
	unpaid := logChainAnomalySQL(anomalyBillingUnpaid)
	free := logChainAnomalySQL(anomalyBillingFree)

	// 扣费未交付：客户付了钱没拿到内容。
	if !strings.Contains(unpaid, "quota > 0") || !strings.Contains(unpaid, "completion_tokens = 0") {
		t.Errorf("扣费未交付判据错: %s", unpaid)
	}
	// 交付未扣费：方向相反，亏的是我方。
	if !strings.Contains(free, "quota = 0") || !strings.Contains(free, "completion_tokens > 0") {
		t.Errorf("交付未扣费判据错: %s", free)
	}
	// 两个方向都必须排除天然无输出模型，否则 embedding 类被整类误判。
	// 按名单里的关键词逐个查，而不是断言某种拼接写法——SQL 从 REGEXP 改成
	// LOWER(...) NOT LIKE 链（为了能在 SQLite 假生产源上真执行）时，
	// 断言写法的测试会红而行为并未改变，那属于测试过度绑定字面量。
	for name, sql := range map[string]string{"扣费未交付": unpaid, "交付未扣费": free} {
		for _, kw := range logChainNoOutputModelKeywords {
			if !strings.Contains(sql, kw) {
				t.Errorf("%s 未排除天然无输出模型 %q: %s", name, kw, sql)
			}
		}
	}
	// 订阅计费的 quota 恒为 0 属正常。不排除会把所有订阅客户整批误报成漏计费。
	if !strings.Contains(free, "billing_source") || !strings.Contains(free, "<> 'subscription'") {
		t.Errorf("交付未扣费必须排除订阅计费: %s", free)
	}
	// billing 聚合必须真的把两个方向都包含进去。
	both := logChainAnomalySQL(anomalyBilling)
	if !strings.Contains(both, "quota > 0") || !strings.Contains(both, "quota = 0") {
		t.Errorf("billing 应含两个方向: %s", both)
	}
	// 判"是否真交付"只能用 completion_tokens：frt 只证明上游开口（任何 data: 行都置位）。
	if strings.Contains(both, "$.frt") {
		t.Error("不得用 frt 判断是否交付")
	}
}

// TestLogChainAnomalyNeverReadsEndError end_error 是自由文本，可能含 "panic" 等词，
// 参与判定会误命中。它只能出现在展示路径，不能进任何判定 SQL。
func TestLogChainAnomalyNeverReadsEndError(t *testing.T) {
	for _, kind := range []string{anomalyStream, anomalyBilling, anomalyBillingUnpaid, anomalyBillingFree, anomalyAll} {
		if strings.Contains(logChainAnomalySQL(kind), "end_error") {
			t.Errorf("%s 的判定 SQL 不得读 end_error（自由文本会误命中）", kind)
		}
	}
}

// TestLogChainAnomalyUnknownKindFailsClosed 未知 kind 必须返回恒假而非恒真。
// 万一将来有人绕过解析层校验调进来，宁可查不到，也不要把全部请求当异常吐出去。
func TestLogChainAnomalyUnknownKindFailsClosed(t *testing.T) {
	if got := logChainAnomalySQL("never-valid"); got != "1 = 0" {
		t.Errorf("未知 kind 应 fail-closed 返回 1 = 0，实际: %s", got)
	}
}

// TestLogChainDoesNotAlterStabilityPredicates 排障页不得改动 expandAnomalyPredicates。
// 那套服务稳定性报表，**故意排除 client_gone**（客户断连不算我方故障，
// 否则客户关标签页会拉低渠道评分）。改它会让历史稳定性数据的判定标准变化，
// 属破坏既有功能。排障页用自己的组合，两者目标不同。
func TestLogChainDoesNotAlterStabilityPredicates(t *testing.T) {
	got := expandAnomalyPredicates("SUM({{STREAMBAD}}) AS s")
	if strings.Contains(got, "client_gone") {
		t.Error("稳定性口径不得包含 client_gone：客户断连不是我方故障")
	}
	// 反向确认它仍在用枚举法（与排障页的排除法不同，这是有意的差异）。
	for _, want := range []string{"'timeout'", "'scanner_error'", "'panic'", "'ping_fail'"} {
		if !strings.Contains(got, want) {
			t.Errorf("稳定性口径缺少 %s，既有判定被改动了: %s", want, got)
		}
	}
}

// TestLogChainAnomalyTagsMatchSQL 标签侧与 SQL 侧口径必须一致。
// 两处各写一份是有意的（SQL 在库里筛，标签给已捞回的行打标记），
// 但不一致会产生"筛出来了却没标签"这种自相矛盾的结果。
func TestLogChainAnomalyTagsMatchSQL(t *testing.T) {
	cases := []struct {
		name  string
		row   LogChainRow
		quota int64
		want  []string
	}{
		{"正常结束不打标签", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "eof", CompletionTokens: 10}, 100, nil},
		{"非流式无 stream_status", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "", CompletionTokens: 10}, 100, nil},
		// 客户断连独立成档，标签是 client_gone 而非 stream。
		// 2026-08-24 实测：当天 1594 条 client_gone 里 92% 已真交付内容，
		// 与 timeout/panic 混在一档会让 25 条真故障被淹掉。
		{"客户端断连独立成档", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "client_gone", CompletionTokens: 5}, 100, []string{"client_gone"}},
		// 断连且流内有错误计数 → 按流故障处理（真出过错比"客户走了"更要紧）。
		{"断连但流内出过错按故障算", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "client_gone", StreamErrorCount: 1, CompletionTokens: 5}, 100, []string{"stream"}},
		{"未见过的新取值也算流故障", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "brand_new_reason", CompletionTokens: 5}, 100, []string{"stream"}},
		{"流错误计数>0", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "eof", StreamErrorCount: 2, CompletionTokens: 5}, 100, []string{"stream"}},
		{"扣费未交付", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "eof", CompletionTokens: 0}, 100, []string{"billing_unpaid"}},
		{"交付未扣费", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "eof", CompletionTokens: 8}, 0, []string{"billing_free"}},
		{"订阅计费不算漏收", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "eof", CompletionTokens: 8, BillingSource: "subscription"}, 0, nil},
		{"embedding 天然无输出不算异常", LogChainRow{Type: 2, ModelName: "text-embedding-3-small", EndReason: "eof", CompletionTokens: 0}, 100, nil},
		{"rerank 同理", LogChainRow{Type: 2, ModelName: "bge-reranker-v2", EndReason: "eof", CompletionTokens: 0}, 100, nil},
		{"可同时命中两类", LogChainRow{Type: 2, ModelName: "gpt-4o", EndReason: "client_gone", CompletionTokens: 0}, 100, []string{"client_gone", "billing_unpaid"}},
		{"错误日志不打异常标签", LogChainRow{Type: 5, ModelName: "gpt-4o", EndReason: "client_gone"}, 0, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := logChainAnomalyTags(tc.row, tc.quota)
			if len(got) != len(tc.want) {
				t.Fatalf("标签数不符: got=%v want=%v", got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("标签[%d]: got=%q want=%q", i, got[i], tc.want[i])
				}
			}
		})
	}
}

// TestLogChainNoOutputModelListMatchesSQL 无输出模型名单两侧必须同步。
//
// 名单本身已收成单一事实源（logChainNoOutputModelKeywords），SQL 与 Go 侧都从它生成，
// 所以"漂移"在结构上已不可能。这个测试改为验两件仍可能出错的事：
//  1. SQL 生成确实把每个关键词都用上了（漏拼一个不会编译报错）
//  2. Go 侧的匹配语义是"子串命中"，与 SQL 的 LIKE '%kw%' 一致
//
// 真正的两侧一致性由 TestLogChainAnomalySQLMatchesTagsOnRealRows 用执行验证，
// 那个测试会把 SQL 筛出来的行再过一遍标签函数逐行比对。
func TestLogChainNoOutputModelListMatchesSQL(t *testing.T) {
	sql := logChainNoOutputModelSQL()
	for _, kw := range logChainNoOutputModelKeywords {
		if !strings.Contains(sql, kw) {
			t.Errorf("SQL 名单缺 %q: %s", kw, sql)
		}
		if !logChainNoOutputModel("prefix-" + kw + "-suffix") {
			t.Errorf("Go 侧未识别 %q", kw)
		}
		// 大小写不敏感：SQL 侧用 LOWER(model_name)，Go 侧用 strings.ToLower，
		// 两者都不得依赖库的 collation 恰好是 _ci。
		if !logChainNoOutputModel("PREFIX-" + strings.ToUpper(kw) + "-SUFFIX") {
			t.Errorf("Go 侧对大写 %q 应同样识别", kw)
		}
	}
	if logChainNoOutputModel("gpt-4o") || logChainNoOutputModel("claude-sonnet-4") {
		t.Error("常规对话模型不应被判为天然无输出")
	}
}

func TestAttachChannelSnapsMarksUnresolved(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.replaceChannelSnaps([]ChannelSnap{
		{ID: 1, Name: "relay-a", Vendor: "OpenAI", BaseDomain: "a.example", Status: 1, UpdatedAt: 100},
		{ID: 2, Name: "gone", Vendor: "Anthropic", BaseDomain: "b.example", Status: 1, UpdatedAt: 100},
	}, 100); err != nil {
		t.Fatalf("replaceChannelSnaps: %v", err)
	}
	// 渠道 2 已从 new-api 删除:快照保留,排障仍要能显示主域名。
	if err := m.replaceChannelSnaps([]ChannelSnap{
		{ID: 1, Name: "relay-a", Vendor: "OpenAI", BaseDomain: "a.example", Status: 1, UpdatedAt: 200},
	}, 200); err != nil {
		t.Fatalf("replaceChannelSnaps 二次: %v", err)
	}

	rows := []LogChainRow{
		{ID: 10, ChannelID: 1},
		{ID: 11, ChannelID: 2},   // 已删除但有快照
		{ID: 12, ChannelID: 777}, // 快照没覆盖
		{ID: 13, ChannelID: 0},   // 未打到渠道
	}
	if err := m.attachChannelSnaps(context.Background(), rows); err != nil {
		t.Fatalf("attachChannelSnaps: %v", err)
	}

	if rows[0].UpstreamDomain != "a.example" || rows[0].ChannelName != "relay-a" {
		t.Errorf("正常渠道未补全: %+v", rows[0])
	}
	if rows[0].ChannelUnresolved {
		t.Error("正常渠道被误标 unresolved")
	}
	if rows[1].UpstreamDomain != "b.example" {
		t.Errorf("已删除渠道应仍能显示主域名: %+v", rows[1])
	}
	if !rows[1].ChannelDeleted {
		t.Error("已删除渠道未标 ChannelDeleted")
	}
	if !rows[2].ChannelUnresolved {
		t.Error("快照缺失的渠道必须标 unresolved,不能留空字段冒充“没有上游域名”")
	}
	if rows[2].UpstreamDomain != "" {
		t.Errorf("快照缺失时不应编造域名: %+v", rows[2])
	}
	// channel_id=0 是"未打到渠道",既不该补全也不该标 unresolved(它本就没有渠道)。
	if rows[3].ChannelUnresolved {
		t.Error("channel_id=0 不应标 unresolved:它的语义是未打到渠道,不是快照缺失")
	}
}

func TestResolveDomainChannelIDs(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.replaceChannelSnaps([]ChannelSnap{
		{ID: 1, Name: "a1", BaseDomain: "shared.example", Status: 1, UpdatedAt: 100},
		{ID: 2, Name: "a2", BaseDomain: "shared.example", Status: 2, UpdatedAt: 100},
		{ID: 3, Name: "b1", BaseDomain: "other.example", Status: 1, UpdatedAt: 100},
	}, 100); err != nil {
		t.Fatalf("replaceChannelSnaps: %v", err)
	}
	ids, truncated, err := m.resolveDomainChannelIDs(context.Background(), "shared.example")
	if err != nil {
		t.Fatalf("resolveDomainChannelIDs: %v", err)
	}
	if truncated {
		t.Error("3 个渠道不应触发截断")
	}
	if len(ids) != 2 {
		t.Fatalf("应反查到 2 个渠道(含已禁用): got=%v", ids)
	}
	// 空域名不查库,直接空集。
	if ids, _, err := m.resolveDomainChannelIDs(context.Background(), ""); err != nil || len(ids) != 0 {
		t.Errorf("空域名应返回空集: ids=%v err=%v", ids, err)
	}
}

// TestLogChainGateTimeoutDoesNotStarveExistingFeatures 排障接口与客户 Portal 的日志
// 计数/分页共用 usageDetailGate,而该泳道容量为 1(monitor.go:115)。本接口每多占 1 秒,
// 就是客户查自己日志时多排队 1 秒。
//
// 既有调用方 countGroupLogs / queryGroupLogs 都用 15s。本接口不得放长——
// 排障是内部功能,不能挤占客户功能。谁把 logChainGateTimeout 改大,这个测试就红。
func TestLogChainGateTimeoutDoesNotStarveExistingFeatures(t *testing.T) {
	const existingLaneTimeout = 15 * time.Second // usage.go countGroupLogs / queryGroupLogs
	if logChainGateTimeout > existingLaneTimeout {
		t.Fatalf("排障闸门超时(%v)超过既有调用方(%v):容量 1 的泳道会被内部功能挤占,"+
			"客户查日志将多排队 %v", logChainGateTimeout, existingLaneTimeout,
			logChainGateTimeout-existingLaneTimeout)
	}
	// 生产库单条 SQL 的 MAX_EXECUTION_TIME 也必须落在闸门超时之内,
	// 否则闸门先超时释放、SQL 仍在生产库上跑,等于绕过了并发控制。
	if time.Duration(logChainQueryTimeoutMS)*time.Millisecond >= logChainGateTimeout {
		t.Errorf("MAX_EXECUTION_TIME(%dms) 应小于闸门超时(%v)",
			logChainQueryTimeoutMS, logChainGateTimeout)
	}
}

// TestServeLogChainRequestsGuardsLocalSnapshotOnly 本地快照模式下 prodDB 为 nil,
// 必须返回可读错误而不是 panic。8202 验收环境正是这个模式。
func TestServeLogChainRequestsGuardsLocalSnapshotOnly(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.LocalSnapshotOnly = true // prodDB 保持 nil

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/logchain/requests?"+url.Values{"days": {"1"}}.Encode(), nil)

	m.serveLogChainRequests(c)

	if w.Code == 0 || w.Code == 200 {
		t.Fatalf("prodDB 为 nil 时不应返回成功: code=%d body=%s", w.Code, w.Body.String())
	}
	if !strings.Contains(w.Body.String(), "生产库未连接") {
		t.Errorf("错误信息应说明生产库未连接: %s", w.Body.String())
	}
}
