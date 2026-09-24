package monitor

import (
	"net/http/httptest"
	"strings"
	"testing"
	"time"

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
	if !strings.Contains(html, `|alerts|`) {
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
		// CloudWatch canonical reason。
		"route_no_channel", "token_disabled", "model_forbidden",
		"model_not_found", "quota_account", "rate_limited",
		// 旧 reject-collector reason，历史行和旧推送仍会出现。
		"no_available_channel", "invalid_token", "token_model_forbidden",
		"user_quota_insufficient", "token_quota_insufficient", "pre_consume_failed",
	} {
		if !strings.Contains(js, r+":") {
			t.Errorf("reason %q 未映射中文，页面会露出原始代码", r)
		}
	}
}

func TestAlertsShowsPartialCloudWatchCoverageWithRows(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	m.cfg.CloudWatchPreRouteLookbackHours = 168
	now := time.Now()
	m.cloudWatchPreRouteFrom.Store(now.Add(-24*time.Hour).Unix() / 60 * 60)
	m.cloudWatchPreRouteThrough.Store(now.Add(-2*time.Hour).Unix() / 60 * 60)
	m.cloudWatchPreRouteLastSuccess.Store(now.Add(-2 * time.Hour).Unix())
	m.cloudWatchPreRouteLastFailure.Store(now.Add(-90 * time.Minute).Unix())
	if note := m.alertsCoverageNote(stabilityScope{FromTs: now.Add(-3 * time.Hour).Unix(), ToTs: now.Unix()}, now); note == "" {
		t.Fatal("部分 CloudWatch 覆盖时，即使有明细也必须返回覆盖提示")
	}
	// A historical day already fully behind the durable through watermark is
	// complete even if a later poll failed; do not append a misleading warning
	// to every older date while the current window is recovering.
	if note := m.alertsCoverageNote(stabilityScope{FromTs: now.Add(-4 * time.Hour).Unix(), ToTs: now.Add(-3 * time.Hour).Unix()}, now); note != "" {
		t.Fatalf("已完全覆盖的历史范围不应报当前缺口: %q", note)
	}
	if note := m.alertsCoverageNote(stabilityScope{FromTs: now.Add(-200 * time.Hour).Unix(), ToTs: now.Add(-169 * time.Hour).Unix()}, now); note != "" {
		t.Fatalf("完全早于直采保留起点不应报当前缺口: %q", note)
	}
	// A range straddling the direct lane's retained start still depends on the
	// compatibility collector for its older prefix; do not present it as fully
	// covered merely because the newer suffix caught up.
	coverageFrom := m.cloudWatchPreRouteFrom.Load()
	if note := m.alertsCoverageNote(stabilityScope{FromTs: coverageFrom - 3600, ToTs: coverageFrom + 3600}, now); note == "" {
		t.Fatal("跨越直采保留起点的范围必须提示兼容采集器依赖")
	}
	if !strings.Contains(string(alertsJS), "if(lastMeta.note)note+=\" \"+lastMeta.note") {
		t.Fatal("有明细时前端未展示 coverage_note")
	}
}

// 问题预警是证据页，哪怕只落后一整分钟也必须显式提示；20 分钟的
// readiness 噪声容忍不能让页面把尚未发布的最近错误当成完整结果。
func TestAlertsCoverageNoteDoesNotHideNearThresholdGap(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	m.cfg.CloudWatchPreRouteLookbackHours = 168
	now := time.Date(2026, 9, 22, 12, 34, 45, 0, time.UTC)
	_, target := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	m.cloudWatchPreRouteFrom.Store(target - 24*3600)
	m.cloudWatchPreRouteThrough.Store(target - 60)
	m.cloudWatchPreRouteLastSuccess.Store(now.Unix() - 60)
	if note := m.alertsCoverageNote(stabilityScope{FromTs: target - 3600, ToTs: target}, now); note == "" {
		t.Fatal("直采只落后一整分钟时仍必须提示覆盖缺口")
	}
	m.cloudWatchPreRouteThrough.Store(target)
	if note := m.alertsCoverageNote(stabilityScope{FromTs: target - 3600, ToTs: target}, now); note != "" {
		t.Fatalf("直采追平精确 requiredThrough 后不应提示缺口: %q", note)
	}
}

// canonical 与旧 collector reason 必须共享同一套身份文案：user_id=0
// 只有 invalid_token 能确定为未鉴权，其余（包括 canonical reason）都是
// 日志未提供用户 ID 的客户未知，不能在页面上误称为未鉴权。
func TestAlertsCanonicalReasonsKeepUnknownCustomerIdentity(t *testing.T) {
	js := string(alertsJS)
	for _, want := range []string{
		"const isUnauthenticated=x=>!+x.user_id&&canonicalReasonKey(x.reason)===\"invalid_token\"",
		"const canonicalReasonKey=r=>",
		`replace(/[.\- ]/g,"_")`,
		"const unknownCustomerReason=x=>",
		"lastMeta.reason_options",
		"客户未知",
		"日志无用户 ID",
		"账户或额度不足",
		"限流或容量不足",
		"令牌已禁用",
		"unknown_quota_account_count",
	} {
		if !strings.Contains(js, want) {
			t.Errorf("canonical/user_id=0 文案或判定缺少 %q", want)
		}
	}
}

func TestAlertCanonicalIdentityStats(t *testing.T) {
	m := newTestMonitor(t)
	const ts int64 = 1_788_500_000
	rows := []RejectionSample{
		{BucketTs: ts, Node: "cloudwatch-direct", Reason: "route_no_channel", Model: "m", Grp: "g", UserID: 0, Count: 2},
		{BucketTs: ts, Node: "cloudwatch-direct", Reason: "token_disabled", Model: "m", Grp: "g", UserID: 0, Count: 3},
		{BucketTs: ts, Node: "cloudwatch-direct", Reason: "model_forbidden", Model: "m", Grp: "g", UserID: 0, Count: 4},
		{BucketTs: ts, Node: "cloudwatch-direct", Reason: "model_not_found", Model: "m", Grp: "g", UserID: 0, Count: 5},
		{BucketTs: ts, Node: "cloudwatch-direct", Reason: "quota_account", Model: "m", Grp: "g", UserID: 0, Count: 6},
		{BucketTs: ts, Node: "cloudwatch-direct", Reason: "rate_limited", Model: "m", Grp: "g", UserID: 0, Count: 7},
		// 大小写/空格容错必须与页面的 reasonKey 判定一致。
		{BucketTs: ts, Node: "legacy", Reason: " INVALID_TOKEN ", Model: "unknown", Grp: "", UserID: 0, Count: 8},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	_, truncated, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: ts, ToTs: ts + 60}, alertRejectFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if truncated {
		t.Fatal("canonical identity fixture unexpectedly truncated")
	}
	if stats.Total != 35 || stats.UnauthCount != 8 || stats.UnknownCustomerCount != 27 || stats.UnknownQuotaAccountCount != 6 || stats.UnknownCustomerOtherCount != 21 {
		t.Fatalf("stats=%+v, want total=35 unauth=8 unknown=27 quota=6 other=21", stats)
	}
}

// Historical collectors used punctuation/case variants for the same reason
// codes.  Identity buckets must canonicalize those aliases too; otherwise a
// token_invalid row is reported as an unknown customer while the direct
// invalid_token row is reported as unauthenticated.
func TestAlertCanonicalIdentityAliasesAreClassifiedConsistently(t *testing.T) {
	m := newTestMonitor(t)
	const ts int64 = 1_788_500_000
	rows := []RejectionSample{
		{BucketTs: ts, Node: "legacy", Reason: " TOKEN-INVALID ", Model: "unknown", UserID: 0, Count: 2},
		{BucketTs: ts, Node: "legacy", Reason: "user.quota-insufficient", Model: "unknown", UserID: 0, Count: 3},
		{BucketTs: ts, Node: "legacy", Reason: "pre-consume-failed", Model: "unknown", UserID: 0, Count: 4},
		{BucketTs: ts, Node: "legacy", Reason: "token-quota-insufficient", Model: "unknown", UserID: 0, Count: 5},
		{BucketTs: ts, Node: "legacy", Reason: "insufficient.quota", Model: "unknown", UserID: 0, Count: 6},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	_, truncated, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: ts, ToTs: ts + 60}, alertRejectFilter{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	if truncated || stats.Total != 20 || stats.UnauthCount != 2 || stats.UnknownCustomerCount != 18 ||
		stats.UnknownUserQuotaCount != 3 || stats.UnknownPreConsumeCount != 4 ||
		stats.UnknownTokenQuotaCount != 5 || stats.UnknownQuotaAccountCount != 6 || stats.UnknownCustomerOtherCount != 0 {
		t.Fatalf("canonical alias stats=%+v, want total=20 unauth=2 unknown=18 quota subclasses 3/4/5/account=6", stats)
	}
}

// CloudWatch 直采覆盖窗口内，旧 collector 的重叠行不能再次计数；窗口外
// 的历史行仍应保留，避免连续水位尚未追平时页面突然丢数据。
func TestAlertRejectPageUsesCloudWatchPrecedence(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from + 60,
		ThroughTs: from + 60, TargetThroughTs: from + 120,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "running",
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []RejectionSample{
		// 同一覆盖分钟的新旧来源只取直采行。
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "m", Grp: "g", UserID: 7, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "route_no_channel", Model: "m", Grp: "g", UserID: 7, Count: 99},
		// 旧采集器的标点别名也必须与 canonical reason 对齐，不能因
		// 归一化不完整而在迁移窗口内双算。
		{BucketTs: from, Node: "legacy", Reason: "no.channel", Model: "m", Grp: "g", UserID: 7, Count: 77},
		// 水位外仍回退旧来源。
		{BucketTs: from + 60, Node: "legacy", Reason: "route_no_channel", Model: "m", Grp: "g", UserID: 7, Count: 5},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	got, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 120}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(got) != 2 || stats.Total != 8 {
		t.Fatalf("rows=%+v more=%v stats=%+v, want 2 rows/8 total", got, more, stats)
	}
}

// Reason filters must use the same canonical vocabulary as overlap
// reconciliation.  Otherwise selecting a legacy alias (for example
// no.channel) suppresses the legacy duplicate and also filters out the
// authoritative route_no_channel row, making a real rejection appear absent.
func TestAlertRejectPageReasonFilterMatchesCanonicalAliases(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from + 60,
		ThroughTs: from + 60, TargetThroughTs: from + 60,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "m", Grp: "g", UserID: 7, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no.channel", Model: "m", Grp: "g", UserID: 7, Count: 77},
	}).Error; err != nil {
		t.Fatal(err)
	}
	got, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{
		Reason: "no.channel", Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(got) != 1 || got[0].Reason != "route_no_channel" || got[0].Count != 3 || stats.Total != 3 {
		t.Fatalf("legacy alias filter must return canonical direct row only: rows=%+v more=%v stats=%+v", got, more, stats)
	}
}

func TestAlertRejectPageReasonFilterKeepsLegacyQuotaSubcategories(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from + 60,
		ThroughTs: from + 60, TargetThroughTs: from + 60,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		// Direct CloudWatch only has the canonical quota class.
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "quota_account", Model: "direct", Grp: "g", UserID: 7, Count: 10},
		{BucketTs: from, Node: "legacy", Reason: "user_quota_insufficient", Model: "user", Grp: "g", UserID: 7, Count: 2},
		{BucketTs: from, Node: "legacy", Reason: "token-quota-insufficient", Model: "token", Grp: "g", UserID: 7, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "pre.consume.failed", Model: "pre", Grp: "g", UserID: 7, Count: 4},
		// Same dimensions as a broad direct quota row.  The broad filter
		// deduplicates it; a fine legacy filter must still retain it because
		// direct quota_account cannot reveal its subtype.
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "quota_account", Model: "same", Grp: "g", UserID: 7, Count: 8},
		{BucketTs: from, Node: "legacy", Reason: "user_quota_insufficient", Model: "same", Grp: "g", UserID: 7, Count: 6},
	}).Error; err != nil {
		t.Fatal(err)
	}
	fine, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{
		Reason: "user_quota_insufficient", Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(fine) != 2 || stats.Total != 8 {
		t.Fatalf("细分额度筛选不应扩成整个 quota_account: rows=%+v more=%v stats=%+v", fine, more, stats)
	}
	for _, row := range fine {
		if row.Reason != "user_quota_insufficient" {
			t.Fatalf("细分筛选混入了其它额度原因: %+v", fine)
		}
	}
	all, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{
		Reason: "quota_account", Limit: 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(all) != 5 || stats.Total != 27 {
		t.Fatalf("额度大类筛选应包含 direct 与旧细分: rows=%+v more=%v stats=%+v", all, more, stats)
	}
}

func TestAlertRejectBaseWhereCompatibilityDoesNotRequireCloudWatchCursor(t *testing.T) {
	uid := int64(7)
	where, args := alertRejectBaseWhere(
		stabilityScope{FromTs: 100, ToTs: 200},
		alertRejectFilter{Reason: "route_no_channel", UserID: &uid}, true,
	)
	if strings.Contains(where, "cloud_watch_pre_route_cursors") || strings.Contains(where, "cloudwatch-direct") {
		t.Fatalf("compatibility helper unexpectedly enabled direct-lane coverage: %s", where)
	}
	if len(args) != 4 || args[0] != int64(100) || args[1] != int64(200) || args[2] != int64(7) || args[3] != "route_no_channel" {
		t.Fatalf("where args=%v, want time/user/reason only", args)
	}
}

func TestAlertRejectPageIgnoresStaleCloudWatchCoverageWhenDisabled(t *testing.T) {
	m := newTestMonitor(t)
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from + 60,
		ThroughTs: from + 60, TargetThroughTs: from + 120,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{
		BucketTs: from, Node: "legacy", Reason: "future_reason", Model: "m", Grp: "g", UserID: 7, Count: 4,
	}).Error; err != nil {
		t.Fatal(err)
	}
	got, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(got) != 1 || stats.Total != 4 {
		t.Fatalf("disabled direct lane must keep legacy rows: rows=%+v more=%v stats=%+v", got, more, stats)
	}
}

// An older local snapshot can briefly run with the new setting before the
// cursor-table migration has completed.  The page must fall back to legacy
// facts instead of failing with "no such table" from the NOT EXISTS clause.
func TestAlertRejectPageFallsBackWhenCloudWatchCursorTableMissing(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	const from int64 = 1_788_500_000
	if err := m.storeDB.Migrator().DropTable(&CloudWatchPreRouteCursor{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{
		BucketTs: from, Node: "legacy", Reason: "no_channel", Model: "legacy-only", Grp: "g", UserID: 7, Count: 4,
	}).Error; err != nil {
		t.Fatal(err)
	}
	got, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(got) != 1 || got[0].Count != 4 || stats.Total != 4 {
		t.Fatalf("missing cursor table must fall back to legacy facts: rows=%+v more=%v stats=%+v", got, more, stats)
	}
}

func TestAlertRejectPageExcludesNonPositiveHistoricalCounts(t *testing.T) {
	m := newTestMonitor(t)
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: from, Node: "legacy", Reason: "no_channel", Model: "zero", Grp: "g", UserID: 7, Count: 0},
		{BucketTs: from, Node: "legacy", Reason: "no_channel", Model: "negative", Grp: "g", UserID: 7, Count: -2},
		{BucketTs: from, Node: "legacy", Reason: "no_channel", Model: "valid", Grp: "g", UserID: 7, Count: 3},
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(rows) != 1 || rows[0].Model != "valid" || stats.Total != 3 {
		t.Fatalf("non-positive historical counts must be ignored: rows=%+v more=%v stats=%+v", rows, more, stats)
	}
}

// Turning the direct lane off must not make a residual direct row and its
// legacy copy count twice.  The fallback is deliberately narrow: an
// unmatched legacy dimension remains visible, and user_id=0 is fail-open.
// No CloudWatch cursor is needed for this transition path.
func TestAlertRejectPageDeduplicatesResidualDirectWhenDisabled(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = false
	const from int64 = 1_788_500_000
	if err := m.storeDB.Migrator().DropTable(&CloudWatchPreRouteCursor{}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "same", Grp: "g", UserID: 7, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no_available_channel", Model: "same", Grp: "g", UserID: 7, Count: 99},
		{BucketTs: from, Node: "legacy", Reason: "no_channel", Model: "missing", Grp: "g", UserID: 7, Count: 5},
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "unknown", Grp: "g", UserID: 0, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no_available_channel", Model: "unknown", Grp: "g", UserID: 0, Count: 99},
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(rows) != 4 || stats.Total != 110 {
		t.Fatalf("disabled transition dedupe mismatch: rows=%+v more=%v stats=%+v", rows, more, stats)
	}
	var same, missing, unknown int
	for _, row := range rows {
		switch row.Model {
		case "same":
			if row.Count != 3 {
				t.Fatalf("known duplicate should keep direct count only: %+v", row)
			}
			same++
		case "missing":
			if row.Count != 5 {
				t.Fatalf("unmatched legacy row was hidden: %+v", row)
			}
			missing++
		case "unknown":
			unknown++
		}
	}
	if same != 1 || missing != 1 || unknown != 2 {
		t.Fatalf("unexpected residual rows: same=%d missing=%d unknown=%d rows=%+v", same, missing, unknown, rows)
	}
}

func TestAlertRejectPageKeepsUnknownLegacyReasonInsideCloudWatchCoverage(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from + 60,
		ThroughTs: from + 60, TargetThroughTs: from + 60,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{
		BucketTs: from, Node: "legacy", Reason: "future_reason", Model: "m", Grp: "g", UserID: 0, Count: 4,
	}).Error; err != nil {
		t.Fatal(err)
	}
	got, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(got) != 1 || stats.Total != 4 {
		t.Fatalf("unknown legacy reason must not be hidden by direct coverage: rows=%+v more=%v stats=%+v", got, more, stats)
	}
}

// user_id=0 is an unknown identity, not a stable customer key.  Even when a
// direct and legacy row share every other dimension, the two rows cannot be
// proven to describe the same requests; fail-open and retain both facts.
func TestAlertRejectPageKeepsUnknownIdentityOverlap(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.CloudWatchPreRouteEnabled = true
	const from int64 = 1_788_500_000
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from, NextTs: from + 60,
		ThroughTs: from + 60, TargetThroughTs: from + 60,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create([]RejectionSample{
		{BucketTs: from, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Model: "m", Grp: "g", UserID: 0, Count: 3},
		{BucketTs: from, Node: "legacy", Reason: "no_available_channel", Model: "m", Grp: "g", UserID: 0, Count: 99},
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows, more, _, stats, err := m.queryRejectPage(stabilityScope{FromTs: from, ToTs: from + 60}, alertRejectFilter{Limit: 20})
	if err != nil {
		t.Fatal(err)
	}
	if more || len(rows) != 2 || stats.Total != 102 {
		t.Fatalf("unknown identity overlap must remain fail-open: rows=%+v more=%v stats=%+v", rows, more, stats)
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
		`canonicalReasonKey(r)==="invalid_token"`, "无效令牌 · 可定位客户", "无效令牌 · 未识别用户", "rcell(x)",
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

func TestAlertRejectCursorContinuationKeepsCurrentDaySnapshot(t *testing.T) {
	uid := int64(7)
	filter := alertRejectFilter{Reason: "invalid_token", UserID: &uid, Cursor: &alertRejectCursor{
		Version: 1, FromTs: 100, ToTs: 200, FilterReason: "invalid_token", FilterUserID: &uid,
		Ts: 150, Reason: "invalid_token", UserID: 7,
	}}
	// The second request arrived later, so the live upper bound moved forward;
	// the cursor's original [100,200) snapshot is still a valid continuation.
	if err := validateAlertRejectCursorForContinuation(stabilityScope{FromTs: 100, ToTs: 240}, filter); err != nil {
		t.Fatalf("current-day upper-bound drift should be accepted: %v", err)
	}
	// A clock/range moving backwards must not make the cursor query outside the
	// requested scope.
	if err := validateAlertRejectCursorForContinuation(stabilityScope{FromTs: 100, ToTs: 190}, filter); err == nil {
		t.Fatal("cursor ahead of the current range should be rejected")
	}
	if err := validateAlertRejectCursorForContinuation(stabilityScope{FromTs: 90, ToTs: 240}, filter); err == nil {
		t.Fatal("cursor from another lower bound should be rejected")
	}
}

// 未鉴权与客户未知在页面上必须分开显示。
func TestAlertsIdentityMissingLabelsAreSeparated(t *testing.T) {
	js := string(alertsJS)
	for _, want := range []string{
		`canonicalReasonKey(x.reason)==="invalid_token"`, "无效令牌 · 未识别用户",
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
	if !strings.Contains(src, "FROM rejection_samples") || !strings.Contains(src, "bucket_ts >= ? AND bucket_ts < ?") {
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
