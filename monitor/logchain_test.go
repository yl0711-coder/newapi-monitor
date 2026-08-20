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
	sql := logChainOrderBySQL()
	if !strings.Contains(sql, "created_at DESC") {
		t.Errorf("必须按 created_at 倒序: %s", sql)
	}
	if !strings.Contains(sql, "id DESC") {
		t.Errorf("同秒多条需用 id 破平以保证顺序稳定: %s", sql)
	}
	if strings.HasPrefix(strings.TrimSpace(sql), "ORDER BY id DESC") {
		t.Error("不得以 id 为首要排序键：id 序不等于发生时间序")
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
