package monitor

import (
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

// alertsWiringMarkers 是新增 tab 必须同改的位置。
var alertsWiringMarkers = []string{
	"tab-alerts",
	"alTableBody",
	"/alerts.js",
	// 与客户排障同一套外观：工具栏、筛选栏、卡片、lc-table。
	"lc-toolbar",
	"lc-filterbar",
	"lc-card",
}

// 「问题预警」新增 tab 要改多处，漏任一处都是静默失效。
func TestAlertsTabWired(t *testing.T) {
	html := pageHTML
	for _, w := range alertsWiringMarkers {
		if !strings.Contains(html, w) {
			t.Errorf("页面缺 %q", w)
		}
	}
	if !strings.Contains(html, `tab==='alerts'?'alerts':'usage'`) {
		t.Error("刷新 #tab=alerts 时未恢复问题预警页")
	}
}

// 0 条不等于没有问题：采集器没跑时表也是空的。
func TestZeroMeansNotCollected(t *testing.T) {
	js := string(alertsJS)
	if !strings.Contains(js, "coverage_note") {
		t.Error("前端未读 coverage_note：会把未采集显示成没有问题")
	}
	if !strings.Contains(alertsNoDataNote, "采集器") {
		t.Error("空数据文案未提示去查采集器")
	}
}

// 未路由请求从未到达渠道，问题预警页必须明确说明不计入渠道稳定率。
// 稳定性报表本身使用 main，不在本测试中锁定其汇总口径。
func TestRejectedPageLabelsChannelBoundary(t *testing.T) {
	js := string(alertsJS)
	if !strings.Contains(js, "不计入渠道稳定率") {
		t.Error("问题预警页未说明未路由请求不计入渠道稳定率")
	}
}

// user_id 必须进两张表的主键。不进主键时入库按主键累加,
// 同一分钟/小时多客户会被并成一行,错误就对应到错的客户。
func TestRejectUserIDInPrimaryKey(t *testing.T) {
	for _, f := range []string{"store.go", "stability_store.go"} {
		src, err := readMonitorSource(f)
		if err != nil {
			t.Fatal(err)
		}
		if !strings.Contains(src, "primaryKey;autoIncrement:false;column:user_id") {
			t.Errorf("%s: user_id 不在主键里,多客户会被合并", f)
		}
	}
}

// 明细表要有时间/客户/分组/模型/错误原因五列，且不得出现令牌/渠道两列——
// 日志里没有令牌名，前置拒绝也从未到达渠道，摆出来就是空列骗人。
func TestAlertsDetailColumns(t *testing.T) {
	// 表头在 page.html 的 alTable 里，与客户排障同为两行式 lc-th/lc-th-sub。
	html := pageHTML
	for _, w := range []string{
		`<span class="lc-th">时间</span>`,
		`<span class="lc-th">客户</span>`,
		`<span class="lc-th">分组</span>`,
		`<span class="lc-th">模型</span>`,
		`<span class="lc-th">错误原因</span>`,
	} {
		if !strings.Contains(html, w) {
			t.Errorf("明细表缺列 %q", w)
		}
	}
	// 令牌/渠道不该出现在本页表头：日志无令牌名，前置拒绝也从未到达渠道。
	// 只在问题预警那段里找，避免命中客户排障自己的表头。
	i := strings.Index(html, `id="tab-alerts"`)
	if i < 0 {
		t.Fatal("找不到 tab-alerts")
	}
	seg := html[i:]
	if j := strings.Index(seg, `id="tab-`); j > 0 {
		seg = seg[:j]
	}
	for _, w := range []string{`lc-th">令牌`, `lc-th">渠道`} {
		if strings.Contains(seg, w) {
			t.Errorf("问题预警不该有 %q：源头无此数据，空列会让人以为采集坏了", w)
		}
	}
}

// reason 必须翻成中文；六种真实 reason 一个都不能漏。
func TestAlertsReasonLabels(t *testing.T) {
	js := string(alertsJS)
	for _, r := range []string{
		"no_available_channel", "invalid_token", "token_model_forbidden",
		"user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed",
	} {
		if !strings.Contains(js, r+":") {
			t.Errorf("reason %q 未映射中文，页面会露出原始代码", r)
		}
	}
}

// 缓存查不到名字时必须显示 ID，不能是空白：
// 空白分不清「没这个客户」还是「没查到名字」。
func TestAlertsFallsBackToUserID(t *testing.T) {
	js := string(alertsJS)
	if !strings.Contains(js, "x.username?esc(x.username)") {
		t.Error("客户列未按「有名字用名字、否则用 ID」渲染")
	}
	if !strings.Contains(js, "未鉴权") {
		t.Error("user_id=0 未显示为未鉴权")
	}
}

// 无效令牌根据 user_id 细分显示，但筛选仍保持底层 invalid_token 一档。
func TestInvalidTokenDisplayDistinguishesCustomerIdentity(t *testing.T) {
	js := string(alertsJS)
	for _, want := range []string{
		`r==="invalid_token"`, "无效令牌 · 可定位客户", "无效令牌 · 未识别用户", "rcell(x)",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("无效令牌显示细分缺少 %q", want)
		}
	}
	if strings.Contains(js, `x.reason="invalid_token_identified"`) ||
		strings.Contains(js, `x.reason="invalid_token_unidentified"`) {
		t.Error("不应改写底层 reason：筛选必须仍是 invalid_token 一档")
	}
}

// 身份缺失必须拆成两档：invalid_token 的 user 0 是未鉴权；
// 其他 user 0 是日志没有 user 段，不能误判成鉴权失败。
func TestAlertIdentityMissingStatsAreSeparated(t *testing.T) {
	m := newTestMonitor(t)
	const ts int64 = 1_788_500_000
	rows := []RejectionSample{
		{BucketTs: ts, Node: "master", Reason: "invalid_token", Model: "unknown", Grp: "", UserID: 0, Count: 3},
		{BucketTs: ts, Node: "master", Reason: "user_quota_insufficient", Model: "unknown", Grp: "", UserID: 0, Count: 2},
		{BucketTs: ts, Node: "master", Reason: "pre_consume_failed", Model: "unknown", Grp: "", UserID: 0, Count: 4},
		{BucketTs: ts, Node: "master", Reason: "token_quota_insufficient", Model: "unknown", Grp: "", UserID: 0, Count: 5},
		{BucketTs: ts, Node: "master", Reason: "future_reason", Model: "unknown", Grp: "", UserID: 0, Count: 6},
		{BucketTs: ts, Node: "master", Reason: "token_model_forbidden", Model: "gpt-5", Grp: "default", UserID: 16, Count: 7},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}

	filter := alertRejectFilter{Limit: 100}
	got, truncated, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: ts, ToTs: ts + 60}, filter)
	if err != nil {
		t.Fatal(err)
	}
	if truncated || len(got) != 6 {
		t.Fatalf("truncated=%v rows=%d, want false/6", truncated, len(got))
	}
	if stats.Total != 27 || stats.UnauthCount != 3 || stats.UnknownCustomerCount != 17 ||
		stats.UnknownUserQuotaCount != 2 || stats.UnknownPreConsumeCount != 4 ||
		stats.UnknownTokenQuotaCount != 5 || stats.UnknownCustomerOtherCount != 6 {
		t.Fatalf("stats=%+v, want total=27 unauth=3 unknown=17 (user=2/pre=4/token=5/other=6)", stats)
	}
}

func TestAlertRejectPageFilterAndCursor(t *testing.T) {
	m := newTestMonitor(t)
	const ts int64 = 1_788_500_000
	rows := []RejectionSample{
		{BucketTs: ts + 60, Node: "master", Reason: "invalid_token", Model: "unknown", UserID: 0, Count: 3},
		{BucketTs: ts, Node: "master", Reason: "invalid_token", Model: "unknown", UserID: 0, Count: 4},
		{BucketTs: ts, Node: "master", Reason: "invalid_token", Model: "gpt-5", Grp: "default", UserID: 16, Count: 7},
		{BucketTs: ts, Node: "master", Reason: "pre_consume_failed", Model: "unknown", UserID: 0, Count: 5},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: ts, ToTs: ts + 120}
	zero := int64(0)
	filter := alertRejectFilter{Reason: "invalid_token", UserID: &zero, Limit: 1}
	first, more, cursor, stats, err := m.queryRejectPage(scope, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || !more || cursor == "" || stats.RowTotal != 2 || stats.Total != 7 || stats.UnauthCount != 7 || stats.UnknownCustomerCount != 0 {
		t.Fatalf("first=%+v more=%v cursor=%q stats=%+v", first, more, cursor, stats)
	}
	decoded, err := decodeAlertRejectCursor(cursor)
	if err != nil {
		t.Fatal(err)
	}
	filter.Cursor = &decoded
	second, more, next, stats2, err := m.queryRejectPage(scope, filter)
	if err != nil {
		t.Fatal(err)
	}
	if len(second) != 1 || more || next != "" || second[0].Ts >= first[0].Ts || stats2 != stats {
		t.Fatalf("second=%+v more=%v next=%q stats=%+v want stats=%+v", second, more, next, stats2, stats)
	}
}

func TestParseAlertRejectFilterAcceptsZeroAndRejectsBadCursor(t *testing.T) {
	gin.SetMode(gin.TestMode)
	ctx, _ := gin.CreateTestContext(httptest.NewRecorder())
	ctx.Request = httptest.NewRequest("GET", "/alerts/rejections?user_id=0&reason=invalid_token&limit=17", nil)
	filter, err := parseAlertRejectFilter(ctx)
	if err != nil || filter.UserID == nil || *filter.UserID != 0 || filter.Limit != 17 || filter.Reason != "invalid_token" {
		t.Fatalf("filter=%+v err=%v", filter, err)
	}
	badCtx, _ := gin.CreateTestContext(httptest.NewRecorder())
	badCtx.Request = httptest.NewRequest("GET", "/alerts/rejections?cursor=not-base64!", nil)
	if _, err := parseAlertRejectFilter(badCtx); err == nil {
		t.Fatal("坏 cursor 应返回参数错误")
	}
}

func TestAlertRejectCursorMustMatchScopeAndFilter(t *testing.T) {
	uid := int64(7)
	filter := alertRejectFilter{Reason: "invalid_token", UserID: &uid, Cursor: &alertRejectCursor{
		Version: 1, FromTs: 100, ToTs: 200, FilterReason: "invalid_token", FilterUserID: &uid,
		Ts: 150, Reason: "invalid_token", UserID: 7,
	}}
	if err := validateAlertRejectCursor(stabilityScope{FromTs: 100, ToTs: 200}, filter); err != nil {
		t.Fatal(err)
	}
	badReason := filter
	badReason.Cursor = &alertRejectCursor{Version: 1, FromTs: 100, ToTs: 200, FilterReason: "other", FilterUserID: &uid, Ts: 150, Reason: "invalid_token", UserID: 7}
	if err := validateAlertRejectCursor(stabilityScope{FromTs: 100, ToTs: 200}, badReason); err == nil {
		t.Fatal("cursor reason 与当前筛选不一致应拒绝")
	}
	badDay := filter
	badDay.Cursor = &alertRejectCursor{Version: 1, FromTs: 90, ToTs: 100, FilterReason: "invalid_token", FilterUserID: &uid, Ts: 99, Reason: "invalid_token", UserID: 7}
	if err := validateAlertRejectCursor(stabilityScope{FromTs: 100, ToTs: 200}, badDay); err == nil {
		t.Fatal("跨日期 cursor 应拒绝")
	}
	removedFilters := alertRejectFilter{Cursor: filter.Cursor}
	if err := validateAlertRejectCursor(stabilityScope{FromTs: 100, ToTs: 200}, removedFilters); err == nil {
		t.Fatal("去掉筛选后复用 cursor 应拒绝")
	}
}

// 未鉴权与客户未知在页面上必须分开显示。
func TestAlertsIdentityMissingLabelsAreSeparated(t *testing.T) {
	js := string(alertsJS)
	for _, want := range []string{
		`x.reason==="invalid_token"`, "无效令牌 · 未识别用户",
		"客户未知", "日志无用户 ID", "unknown_customer_count",
		"用户额度不足", "预扣费失败", "令牌额度不足",
		"unknown_user_quota_count", "unknown_pre_consume_count", "unknown_token_quota_count",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("前端缺少身份分档标记 %q", want)
		}
	}
}

// 明细读分钟表:小时表给不出「几点几分」,这一页要显示时间。
func TestAlertRowsReadMinuteTable(t *testing.T) {
	src, err := readMonitorSource("alerts_query.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(src, "FROM rejection_samples") {
		t.Error("明细未读 rejection_samples：小时表没有分钟精度")
	}
	if !strings.Contains(src, "LIMIT ?") {
		t.Error("明细查询无上限：故障爆量时会把页面拖死")
	}
}

// 未鉴权那档(user_id=0)不得展示分组/模型:那批请求连是谁都不知道。
//
// 原先靠「汇总表里 anon 行把分组/模型渲染成 —」来守。现在汇总表已不含
// 涉及分组/涉及模型两列（那是 GROUP_CONCAT 出来的聚合值，把多个无关请求的
// 分组并给「未鉴权」本身就是误导），改为断言这两列确实不在汇总里。
func TestAlertsAnonRowHidesDetail(t *testing.T) {
	js := string(alertsJS)
	if !strings.Contains(js, "未鉴权") {
		t.Error("user_id=0 未显示为未鉴权")
	}
	if !strings.Contains(js, "无用户身份") {
		t.Error("未鉴权那档未说明「无用户身份」")
	}
	// 明细行的分组/模型来自该行自身的日志，缺失时必须显示 —，不能留空白。
	if !strings.Contains(js, `x.grp?esc(x.grp):'<span class="lc-sub">—</span>'`) {
		t.Error("明细行缺分组时未显示 —")
	}
}

// 这一页只保留明细表，不再做「按错误类型」的汇总——
// 那是稳定性报表的职责，两处各算一遍口径必然漂移。
func TestAlertsHasNoSummaryTable(t *testing.T) {
	js := string(alertsJS)
	// 查渲染代码，不查注释——注释里正解释「为什么不做汇总」。
	for _, w := range []string{"share_pct", "占比</th>", "d.reasons"} {
		if strings.Contains(js, w) {
			t.Errorf("汇总表残留 %q", w)
		}
	}
	src, err := readMonitorSource("alerts_query.go")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(src, "queryRejectReasons") || strings.Contains(src, "AlertReason") {
		t.Error("后端仍在算汇总：表已删，那次查询是白查")
	}
	// 总数与明细必须同源,否则会出现「明细 10 行、总数 0」。
	// 改成 CTE 一次查出后,检查 CTE 读的是分钟表。
	if !strings.Contains(src, "FROM rejection_samples") || !strings.Contains(src, `where := "bucket_ts >= ? AND bucket_ts < ?"`) {
		t.Error("总数未与明细同读分钟表或缺少同一时间范围")
	}
	// 确认已合并成分页查询(不再有单独的 countRejectTotal)。
	if strings.Contains(src, "func (m *Monitor) countRejectTotal") {
		t.Error("仍有单独的 countRejectTotal:应已合并到 queryRejectPage")
	}
}

// ★ 新错误类型必须显式标成待分类，不能把原始码当正常标签混进中文里 ★
//
// 后端对 reason 无白名单（server.go 只 clip 到 64 字符），采集器认出什么就存什么。
// 所以这一页迟早会遇到没见过的码。规范要求它们默认进入「未知/待分类」。
func TestAlertsUnknownReasonMarkedPending(t *testing.T) {
	js := string(alertsJS)
	// 兜底不能是「原样吐出原始码」。
	if strings.Contains(js, "REASON[r]||r") {
		t.Error("未知码被原样显示：页面会出现 snake_case 英文混在中文标签里")
	}
	for _, w := range []string{"待分类", "采集器未能识别", "isUnclassified"} {
		if !strings.Contains(js, w) {
			t.Errorf("缺 %q", w)
		}
	}
	// 两种未知要分开：other=改采集器正则，未知码=补本页映射。
	if !strings.Contains(js, "COLLECTOR_UNMATCHED") {
		t.Error("未区分「采集器没匹配上」与「本页缺映射」，两者要修的地方不同")
	}
	// 必须主动报出来，否则没人知道该去补规则。
	if !strings.Contains(js, "需补采集器的正则") || !strings.Contains(js, "需补映射") {
		t.Error("出现未分类原因时未在提示条说明该怎么修")
	}
	// 原始码要显示出来，否则不知道给哪个码加映射。
	if !strings.Contains(js, `esc(r||"(空)")`) {
		t.Error("未分类行未显示原始码")
	}
}

// 后端不得对 reason 做白名单过滤：原始码是证据，留着才能事后重算与补映射。
func TestRejectReasonStoredVerbatim(t *testing.T) {
	src, err := readMonitorSource("server.go")
	if err != nil {
		t.Fatal(err)
	}
	i := strings.Index(src, "func (m *Monitor) ingestRejections")
	if i < 0 {
		t.Fatal("找不到 ingestRejections")
	}
	seg := src[i:]
	if j := strings.Index(seg, "\nfunc "); j > 0 {
		seg = seg[:j]
	}
	if !strings.Contains(seg, "clip(s.Reason, 64)") {
		t.Error("reason 入库方式变了，请确认仍原样保留原始码")
	}
	// 不该出现按已知集合丢弃的逻辑。
	for _, w := range []string{"knownReasons", "validReasons", "reasonWhitelist"} {
		if strings.Contains(seg, w) {
			t.Errorf("出现 %q：白名单会把新错误类型静默丢掉", w)
		}
	}
}

// 客户列必须与客户排障同形：上行用户名、下行 ID。
func TestAlertsCustomerCellMatchesLogChain(t *testing.T) {
	js := string(alertsJS)
	if !strings.Contains(js, `class="lc-cust"`) || !strings.Contains(js, `class="lc-sub">ID `) {
		t.Error("客户列未用排障页的两行式(名字在上、ID 在下)")
	}
	if !strings.Contains(js, `("#"+uid)`) {
		t.Error("缓存无名字时未退回 #ID")
	}
}
