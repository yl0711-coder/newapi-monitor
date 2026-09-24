package monitor

import (
	"context"
	"database/sql/driver"
	"fmt"
	"math"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"

	glebarezsqlite "github.com/glebarez/go-sqlite"
)

func init() {
	// 生产 MySQL 原生支持 REGEXP；测试使用的纯 Go SQLite 驱动默认没有注册
	// 这个运算符。仅在测试进程给新连接补同语义函数，才能真执行成品判据。
	glebarezsqlite.MustRegisterDeterministicScalarFunction(
		"regexp", 2,
		func(_ *glebarezsqlite.FunctionContext, args []driver.Value) (driver.Value, error) {
			matched, err := regexp.MatchString(fmt.Sprint(args[0]), fmt.Sprint(args[1]))
			if err != nil {
				return nil, err
			}
			if matched {
				return int64(1), nil
			}
			return int64(0), nil
		},
	)
}

// chTestNow 固定一个 CST 时刻，避免测试随真实时间漂移。
func chTestNow() time.Time { return time.Date(2026, 9, 15, 14, 30, 0, 0, cstLocation) }

// chSeedCompany 建一家公司并挂上成员。
func chSeedCompany(t *testing.T, m *Monitor, name string, userIDs ...int64) int64 {
	t.Helper()
	g := CustomerHealthGroup{Name: name, CreatedAt: 1}
	if err := m.storeDB.Create(&g).Error; err != nil {
		t.Fatal(err)
	}
	for _, id := range userIDs {
		u := CustomerHealthMember{UserID: id, Username: name + "-u", GroupID: g.ID, AddedAt: 1}
		if err := m.storeDB.Create(&u).Error; err != nil {
			t.Fatal(err)
		}
	}
	return g.ID
}

// 一家公司多个用户名必须合并成一行统计：用户明确要求"统一按一个算"。
// 若分开统计，同一家公司会占两行，"这家公司稳不稳"就答不了。
func TestCustomerHealthMergesAllUsersOfOneCompany(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	chSeedCompany(t, m, "甲公司", 101, 102)
	rows := []CapacityUserMinuteSample{
		{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "m", Grp: "g", Success: 80, Failed: 10, CustomerHealthFailed: 10},
		{BucketTs: from + 120, UserID: 102, ChannelID: 7, ModelName: "m", Grp: "g", Success: 10, Anomaly: 0},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("一家公司必须只占一行: %+v", report.Rows)
	}
	row := report.Rows[0]
	if row.Total != 100 || row.Success != 90 || row.Failed != 10 || len(row.Members) != 2 {
		t.Fatalf("多个用户名未合并: %+v", row)
	}
	if row.StabilityPct == nil || math.Abs(*row.StabilityPct-90) > .001 {
		t.Fatalf("稳定率应为 90%%: %+v", row.StabilityPct)
	}
	if len(row.PrimaryModels) != 1 || row.PrimaryModels[0].Name != "m" || row.PrimaryModels[0].Requests != 100 {
		t.Fatalf("主要模型必须合并公司全部用户: %+v", row.PrimaryModels)
	}
}

func TestCustomerHealthPrimaryModelAggregatesMembersAndUsesFortyPercentPairRule(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	chSeedCompany(t, m, "模型公司", 101, 102)
	rows := []CapacityUserMinuteSample{
		{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "gpt-major", Grp: "g", Success: 30},
		{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "other", Grp: "g", Success: 20},
		{BucketTs: from + 120, UserID: 102, ChannelID: 8, ModelName: "gpt-major", Grp: "g", Success: 21},
		{BucketTs: from + 120, UserID: 102, ChannelID: 8, ModelName: "other", Grp: "g", Success: 29},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 || len(report.Rows[0].PrimaryModels) != 2 {
		t.Fatalf("两个模型均超过 40%% 时应同时展示: %+v", report.Rows)
	}
	primary := report.Rows[0].PrimaryModels[0]
	if primary.Name != "gpt-major" || primary.Requests != 51 || math.Abs(primary.SharePct-51) > .001 {
		t.Fatalf("公司成员模型合并错误: %+v", primary)
	}
	if second := report.Rows[0].PrimaryModels[1]; second.Name != "other" || second.Requests != 49 || math.Abs(second.SharePct-49) > .001 {
		t.Fatalf("第二个超过 40%% 的模型未展示: %+v", second)
	}
	if got := customerHealthPrimaryModels(map[string]int64{"a": 50, "b": 50}, 100); len(got) != 2 {
		t.Fatalf("两个模型均超过 40%% 时都应展示: %+v", got)
	}
	if got := customerHealthPrimaryModels(map[string]int64{"a": 40, "b": 40, "c": 20}, 100); len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("恰好 40%% 不满足双模型规则，应稳定选择最多模型中的一个: %+v", got)
	}
	if got := customerHealthPrimaryModels(map[string]int64{"a": 39, "b": 35, "c": 26}, 100); len(got) != 1 || got[0].Name != "a" {
		t.Fatalf("没有两个模型超过 40%% 时应展示使用最多的模型: %+v", got)
	}
	if got := customerHealthPrimaryModels(nil, 0); len(got) != 0 {
		t.Fatalf("无请求时主要模型必须为空: %+v", got)
	}
}

// ★ 金额取不到时必须是 nil，绝不能是 0 ★
//
// 本地事实层未启用（newTestMonitor 的默认状态，也是 8204 验收栈的真实状态）时，
// 今日消耗无从得知。若补 0，页面会显示 $0.00，被读成"这家公司今天没花钱"——
// 而真相是"我们不知道"。这是「缺失绝不显示为零」在金额字段上的形态。
func TestCustomerHealthSpendUnknownIsNeverZero(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	chSeedCompany(t, m, "甲公司", 101)
	rows := []CapacityUserMinuteSample{
		{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "m", Grp: "g", Success: 50},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("应有一行: %+v", report.Rows)
	}
	row := report.Rows[0]
	if row.TodaySpendUSD != nil {
		t.Fatalf("事实层未启用时金额必须为 nil，不得补 0，实际 %v", *row.TodaySpendUSD)
	}
	if row.SpendState != customerHealthSpendUnavailable {
		t.Fatalf("状态应为 %q，实际 %q", customerHealthSpendUnavailable, row.SpendState)
	}
	// 必须说明为什么取不到。只给个空值会让人以为是页面坏了。
	if strings.TrimSpace(row.SpendNote) == "" {
		t.Fatal("金额取不到时必须给出原因说明")
	}
	for _, mb := range row.Members {
		if mb.TodaySpendUSD != nil {
			t.Fatalf("成员金额同样不得补 0: user_id=%d", mb.UserID)
		}
	}
}

func TestCustomerHealthCollectedSpendUsesContinuousLocalCoverage(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	m.cfg.CustomerHealthSourceEnabled = true
	m.customerHealthSourceRunning.Store(true)
	m.customerHealthSourceFrom.Store(from)
	m.customerHealthSourceThrough.Store(from + 3600)
	chSeedCompany(t, m, "采集公司", 101, 102)
	rows := []CapacityUserMinuteSample{
		{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "m", Grp: "g",
			Success: 5, Quota: 750000, RefundQuota: 250000},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if !report.Collection.Ready || report.Collection.Mode != "logchain_only" || report.Collection.ThroughTs != from+3600 {
		t.Fatalf("collection status does not prove continuous coverage: %+v", report.Collection)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("rows=%+v", report.Rows)
	}
	row := report.Rows[0]
	if row.SpendState != customerHealthSpendSampled || row.TodaySpendUSD == nil || math.Abs(*row.TodaySpendUSD-1) > 1e-9 {
		t.Fatalf("collected company spend mismatch: %+v", row)
	}
	if row.Members[0].TodaySpendUSD == nil || math.Abs(*row.Members[0].TodaySpendUSD-1) > 1e-9 {
		t.Fatalf("collected member spend mismatch: %+v", row.Members)
	}
	if row.Members[1].TodaySpendUSD == nil || *row.Members[1].TodaySpendUSD != 0 {
		t.Fatalf("continuous coverage must prove an absent member is zero: %+v", row.Members)
	}
}

func TestCustomerHealthCollectedSpendRefusesUnprovenCoverage(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	m.cfg.CustomerHealthSourceEnabled = true
	m.customerHealthSourceFrom.Store(from + 3600)
	m.customerHealthSourceThrough.Store(from + 7200)
	chSeedCompany(t, m, "缺口公司", 101)
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	row := report.Rows[0]
	if row.MetricsReady {
		t.Fatalf("unproven coverage must keep request metrics unavailable: %+v", row)
	}
	if row.TodaySpendUSD != nil || row.SpendState != customerHealthSpendUnavailable {
		t.Fatalf("unproven coverage must not become zero spend: %+v", row)
	}
}

func TestCustomerHealthIndependentSourceCapsMetricsAtContinuousWatermark(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	m.cfg.CustomerHealthSourceEnabled = true
	m.customerHealthSourceFrom.Store(from)
	m.customerHealthSourceThrough.Store(from + 3600)
	chSeedCompany(t, m, "水位公司", 101)
	rows := []CapacityUserMinuteSample{
		{BucketTs: from + 60, UserID: 101, ChannelID: 7, ModelName: "m", Grp: "g", Success: 2},
		// 模拟重启前遗留、但本轮尚未重新证明覆盖到的未来本地行。
		{BucketTs: from + 7200, UserID: 101, ChannelID: 7, ModelName: "m", Grp: "g", Success: 99},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 || !report.Rows[0].MetricsReady || report.Rows[0].Total != 2 {
		t.Fatalf("request metrics escaped continuous watermark: %+v", report.Rows)
	}
}

func TestCustomerHealthSourceRangeUsesCSTAndClosedMinute(t *testing.T) {
	now := time.Date(2026, 9, 16, 14, 30, 45, 0, cstLocation)
	from, through := customerHealthSourceRange(now)
	wantFrom := time.Date(2026, 9, 16, 0, 0, 0, 0, cstLocation).Unix()
	wantThrough := time.Date(2026, 9, 16, 14, 28, 0, 0, cstLocation).Unix()
	if from != wantFrom || through != wantThrough {
		t.Fatalf("range=(%d,%d), want=(%d,%d)", from, through, wantFrom, wantThrough)
	}
	if got := customerHealthSourceNext(from, from+7200); got != from+3600 {
		t.Fatalf("source slice is not hourly: %d", got-from)
	}
}

func TestCustomerHealthSourceClosesPreviousDayBeforeSwitching(t *testing.T) {
	dayStart := time.Date(2026, 9, 16, 0, 0, 0, 0, cstLocation).Unix()
	nextDay := dayStart + 24*3600
	// Just after midnight the two-minute finalization target is still inside
	// the previous day, so the worker must keep that day active.
	switchDay, target := customerHealthSourceDayTransition(dayStart, nextDay, nextDay-90, nextDay-20*60)
	if switchDay || target != nextDay-90 {
		t.Fatalf("previous day should remain active while its tail is settling: switch=%v target=%d", switchDay, target)
	}
	// Once the old cursor reaches the natural day end, the next loop may
	// atomically move the durable cursor to the new day.
	switchDay, target = customerHealthSourceDayTransition(dayStart, nextDay, nextDay+60, nextDay)
	if !switchDay || target != nextDay+60 {
		t.Fatalf("worker did not switch after closing previous day: switch=%v target=%d", switchDay, target)
	}
}

func TestCustomerHealthSourceCursorSurvivesRestartAndReplaysOnlyTail(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	through := from + 8*3600
	if err := m.saveCustomerHealthSourceCursor(from, through); err != nil {
		t.Fatal(err)
	}
	cursor, covered, err := m.loadCustomerHealthSourceCursor(from, through+3600)
	if err != nil {
		t.Fatal(err)
	}
	if covered != through || cursor != through-customerHealthSourceReplaySeconds {
		t.Fatalf("restart cursor=(%d,%d), want=(%d,%d)", cursor, covered,
			through-customerHealthSourceReplaySeconds, through)
	}

	nextDay := from + 24*3600
	cursor, covered, err = m.loadCustomerHealthSourceCursor(nextDay, nextDay+3600)
	if err != nil {
		t.Fatal(err)
	}
	if cursor != nextDay || covered != nextDay {
		t.Fatalf("new day cursor=(%d,%d), want day start %d", cursor, covered, nextDay)
	}
}

func TestCustomerHealthSourceCursorRewindsTodayWhenPolicyChanges(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	if err := m.storeDB.Save(&CustomerHealthSourceCursor{
		ID: 1, DayTs: from, ThroughTs: from + 8*3600,
		SemanticsVersion: customerHealthStabilityPolicyVersion - 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	cursor, covered, err := m.loadCustomerHealthSourceCursor(from, from+9*3600)
	if err != nil {
		t.Fatal(err)
	}
	if cursor != from || covered != from {
		t.Fatalf("旧稳定性口径必须从当天 00:00 重采，got=(%d,%d) want=%d", cursor, covered, from)
	}
	var saved CustomerHealthSourceCursor
	if err := m.storeDB.First(&saved, 1).Error; err != nil {
		t.Fatal(err)
	}
	if saved.SemanticsVersion != customerHealthStabilityPolicyVersion || saved.ThroughTs != from {
		t.Fatalf("重置后的持久水位口径错误: %+v", saved)
	}
}

func TestCustomerHealthStandardSourceBackfillsNewPolicyAcrossToday(t *testing.T) {
	m := newTestMonitor(t)
	from, target := customerHealthSourceRange(chTestNow())
	if err := m.storeDB.Save(&CustomerHealthSourceCursor{
		ID: 1, DayTs: from, ThroughTs: from + 8*3600,
		SemanticsVersion: customerHealthStabilityPolicyVersion - 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	type span struct{ from, to int64 }
	var spans []span
	err := m.backfillCustomerHealthPolicyTodayWith(context.Background(), chTestNow(),
		func(_ context.Context, sliceFrom, sliceTo int64) (int, error) {
			spans = append(spans, span{sliceFrom, sliceTo})
			return 1, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	if len(spans) == 0 || spans[0].from != from || spans[len(spans)-1].to != target {
		t.Fatalf("新口径未连续回算当天: spans=%+v want=[%d,%d)", spans, from, target)
	}
	for i, current := range spans {
		if current.to <= current.from || current.to-current.from > customerHealthSourceSliceSeconds {
			t.Fatalf("回算分片越界: %+v", current)
		}
		if i > 0 && spans[i-1].to != current.from {
			t.Fatalf("回算范围有缺口: prev=%+v current=%+v", spans[i-1], current)
		}
	}
	var saved CustomerHealthSourceCursor
	if err := m.storeDB.First(&saved, 1).Error; err != nil {
		t.Fatal(err)
	}
	if saved.ThroughTs != target || saved.SemanticsVersion != customerHealthStabilityPolicyVersion {
		t.Fatalf("回算完成未发布当前口径水位: %+v", saved)
	}
}

func TestCustomerHealthSourceCursorNeverRegressesWithinDay(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	high := from + 8*3600
	if err := m.saveCustomerHealthSourceCursor(from, high); err != nil {
		t.Fatal(err)
	}
	// 模拟系统时间回拨后，本轮 target 比已经持久化的可信水位更早。
	if err := m.saveCustomerHealthSourceCursor(from, high-30*60); err != nil {
		t.Fatal(err)
	}
	cursor, covered, err := m.loadCustomerHealthSourceCursor(from, high+3600)
	if err != nil {
		t.Fatal(err)
	}
	if covered != high || cursor != high-customerHealthSourceReplaySeconds {
		t.Fatalf("same-day cursor regressed: cursor=(%d,%d), want=(%d,%d)",
			cursor, covered, high-customerHealthSourceReplaySeconds, high)
	}
}

// 折算必须用仓库既有的 quotaPerUSD，不得另立一个换算系数。
// 两处各写一份，同一家公司在客户维护页与用户用量页会显示不同金额。
func TestCustomerHealthSpendUsesSharedQuotaPerUSD(t *testing.T) {
	row := CustomerHealthRow{Members: []CustomerHealthMember{{UserID: 1}, {UserID: 2}}}
	applyCustomerHealthSpend(&row, map[int64]int64{1: 500000, 2: 250000}, customerHealthSpendLive, "note")
	if row.TodaySpendUSD == nil {
		t.Fatal("金额不应为 nil")
	}
	// 750000 quota / 500000 = 1.5 USD
	if math.Abs(*row.TodaySpendUSD-1.5) > 1e-9 {
		t.Fatalf("公司合计折算错误: %v", *row.TodaySpendUSD)
	}
	if row.Members[0].TodaySpendUSD == nil || math.Abs(*row.Members[0].TodaySpendUSD-1.0) > 1e-9 {
		t.Fatalf("成员一折算错误: %v", row.Members[0].TodaySpendUSD)
	}
	if row.Members[1].TodaySpendUSD == nil || math.Abs(*row.Members[1].TodaySpendUSD-0.5) > 1e-9 {
		t.Fatalf("成员二折算错误: %v", row.Members[1].TodaySpendUSD)
	}
	if row.SpendState != customerHealthSpendLive {
		t.Fatalf("状态未写入: %q", row.SpendState)
	}
}

// ★ 有任何成员取不到，公司合计必须为 nil ★
//
// 本测试的前一版认可"把取到的几个加起来当公司合计"，那个契约是错的：
// 本页承诺"该公司所有用户今日消耗"，部分合计会给出一个看起来合理但偏小的
// 数字，页面上还无从察觉它不完整。典型触发场景是某成员刚加进名单、
// 用量事实尚未为它发布完成。
func TestCustomerHealthSpendRefusesPartialCompanyTotal(t *testing.T) {
	row := CustomerHealthRow{Members: []CustomerHealthMember{{UserID: 1}, {UserID: 2}}}
	applyCustomerHealthSpend(&row, map[int64]int64{1: 500000}, customerHealthSpendFinalized, "note")

	if row.TodaySpendUSD != nil {
		t.Fatalf("缺成员时公司合计必须为 nil，不得给部分合计，实际 %v", *row.TodaySpendUSD)
	}
	if row.SpendState != customerHealthSpendPartial {
		t.Fatalf("状态应为 %q，实际 %q", customerHealthSpendPartial, row.SpendState)
	}
	// 已取到的成员金额仍要展示——那部分是真实的，藏起来等于丢信息。
	if row.Members[0].TodaySpendUSD == nil || math.Abs(*row.Members[0].TodaySpendUSD-1.0) > 1e-9 {
		t.Fatalf("已取到的成员金额应保留: %v", row.Members[0].TodaySpendUSD)
	}
	// 取不到的成员保持 nil，不补 0。
	if row.Members[1].TodaySpendUSD != nil {
		t.Fatalf("未取到的成员必须保持 nil，实际 %v", *row.Members[1].TodaySpendUSD)
	}
	// 说明里必须给出缺了几个，只说"不完整"无法判断严重程度。
	if !strings.Contains(row.SpendNote, "2 个成员中有 1 个") {
		t.Fatalf("说明必须写清缺失数量，实际: %q", row.SpendNote)
	}
}

// 全部成员都取到时才给合计，且状态沿用传入值。
func TestCustomerHealthSpendGivesTotalOnlyWhenAllMembersResolved(t *testing.T) {
	row := CustomerHealthRow{Members: []CustomerHealthMember{{UserID: 1}, {UserID: 2}}}
	applyCustomerHealthSpend(&row, map[int64]int64{1: 500000, 2: 250000}, customerHealthSpendFinalized, "note")
	if row.TodaySpendUSD == nil || math.Abs(*row.TodaySpendUSD-1.5) > 1e-9 {
		t.Fatalf("全部取到时应给合计 1.5，实际 %v", row.TodaySpendUSD)
	}
	if row.SpendState != customerHealthSpendFinalized {
		t.Fatalf("状态应沿用传入值 %q，实际 %q", customerHealthSpendFinalized, row.SpendState)
	}
}

// 完整发布的成员即使今天没有事实行，也应得到可信的 0，而不是 partial。
// usage_hour_facts 是稀疏表，零请求/零退款不会制造占位行；是否可补 0 必须由
// published membership 证明，不能由聚合 map 是否有 key 猜测。
func TestCustomerHealthFinalizedSpendPublishedZeroIsNotPartial(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	from, to, _ := customerHealthDayRange(chTestNow())
	groupID := chSeedCompany(t, m, "零消费公司", 101)
	finalizedThrough := from + usageFactHourSeconds
	seedPublishedUsageFactsForTest(t, m, []int64{101}, from, finalizedThrough)
	m.setUsageFactsReadiness(true, finalizedThrough)

	row := CustomerHealthRow{
		GroupID: groupID, Company: "零消费公司",
		Members: []CustomerHealthMember{{UserID: 101, Username: "零消费成员"}},
	}
	m.attachCustomerHealthSpend(context.Background(), &row, from, to, chTestNow())
	if row.SpendState != customerHealthSpendFinalized {
		t.Fatalf("完整发布的零消费成员应为 finalized，不应是 partial: %+v", row)
	}
	if row.TodaySpendUSD == nil || *row.TodaySpendUSD != 0 {
		t.Fatalf("完整发布且无事实行应得到可信的公司 0，实际 %v", row.TodaySpendUSD)
	}
	if row.Members[0].TodaySpendUSD == nil || *row.Members[0].TodaySpendUSD != 0 {
		t.Fatalf("完整发布且无事实行应得到可信的成员 0，实际 %v", row.Members[0].TodaySpendUSD)
	}
}

// 回归：旧成员已发布、新成员尚未发布时，公司合计不得给部分和。测试构造真实
// control/publication 状态，并故意给未发布成员塞入候选事实，确保读取面不会泄漏它。
func TestCustomerHealthSpendNewMemberNotPublishedYieldsPartial(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	from, to, _ := customerHealthDayRange(chTestNow())
	groupID := chSeedCompany(t, m, "甲公司", 101, 999)
	finalizedThrough := from + usageFactHourSeconds
	seedPublishedUsageFactsForTest(t, m, []int64{101}, from, finalizedThrough)
	m.setUsageFactsReadiness(true, finalizedThrough)
	if err := m.usageFactsStore().Create(&[]UsageHourFact{
		{HourTs: from, DayTs: from, UserID: 101, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, ConsumeQuota: 1000000},
		{HourTs: from, DayTs: from, UserID: 999, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1, ConsumeQuota: 9000000},
	}).Error; err != nil {
		t.Fatal(err)
	}
	row := CustomerHealthRow{
		GroupID: groupID, Company: "甲公司",
		Members: []CustomerHealthMember{
			{UserID: 101, Username: "老成员"},
			{UserID: 999, Username: "新成员"},
		},
	}
	m.attachCustomerHealthSpend(context.Background(), &row, from, to, chTestNow())

	if row.TodaySpendUSD != nil {
		t.Fatalf("新成员未发布时不得给公司合计，实际 %v", *row.TodaySpendUSD)
	}
	if row.SpendState == customerHealthSpendFinalized {
		t.Fatal("不得标成 finalized：那会让缺人的合计看起来是完整的封口金额")
	}
	if row.SpendState != customerHealthSpendPartial {
		t.Fatalf("状态应为 %q，实际 %q", customerHealthSpendPartial, row.SpendState)
	}
	if row.Members[0].TodaySpendUSD == nil || math.Abs(*row.Members[0].TodaySpendUSD-2.0) > 1e-9 {
		t.Fatalf("老成员金额应为 2.0，实际 %v", row.Members[0].TodaySpendUSD)
	}
	if row.Members[1].TodaySpendUSD != nil {
		t.Fatal("新成员金额必须为 nil")
	}
}

// 前端必须把 partial 与 unavailable 显示成不同的话。
// 合并成一句"不可知"会让"缺几个成员"被读成"整个数据源不可用"，
// 后者会让人白跑一趟去排查采集与开关。
func TestCustomerHealthSpendPartialLabelDiffersFromUnavailable(t *testing.T) {
	src, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(src)
	if !strings.Contains(js, "partial:'部分成员缺'") {
		t.Fatal("partial 必须有区别于 unavailable 的标签")
	}
	if !strings.Contains(js, "sampled:'本地采集'") || !strings.Contains(js, "collection?.note") {
		t.Fatal("独立采集金额和采集水位必须在页面显式展示")
	}
	// 金额格必须按 spend_state 取标签，不能写死"不可知"。
	if strings.Contains(js, `<span class="ch-spend-state">不可知</span>`) {
		t.Fatal("金额格不得写死「不可知」，必须按 spend_state 区分 partial/unavailable")
	}
}

// 前端不得把取不到的金额渲染成 $0.00。
//
// money() 是唯一的格式化入口，它对 null/undefined 必须返回 null，
// 由调用方决定显示什么。若它直接 (+v).toFixed(2)，null 会变成 "$0.00"，
// 后端再怎么小心地返回 nil 都会在最后一步被抹掉。
func TestCustomerHealthSpendFrontendNeverRendersZeroForUnknown(t *testing.T) {
	src, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(src)
	if !strings.Contains(js, "const money=v=>(v===null||v===undefined)?null:") {
		t.Fatal("money() 必须对 null/undefined 返回 null，否则取不到的金额会显示成 $0.00")
	}
	// 表格格与展开区都必须走「—」分支。
	if !strings.Contains(js, "ch-unknown") {
		t.Fatal("取不到的金额必须有 ch-unknown 样式标记")
	}
}

// 「今日已封口金额」只能有一份实现。
//
// 客户维护的退路与实时投影都要算这个数。两处各写一份 SQL，同一家公司在
// 两个页面上会显示不同金额，而且是"看起来都合理"的差异，极难发现。
func TestCustomerHealthSpendSharesFinalizedNetFormula(t *testing.T) {
	q, err := os.ReadFile("customer_health_query.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(q), "usageFactsTodayFinalizedNetByUser(") {
		t.Fatal("客户维护必须调用共享的 usageFactsTodayFinalizedNetByUser，不得自写净额 SQL")
	}
	// 净额公式（消费 - 退款）在全仓库只能出现一次。
	pattern := regexp.MustCompile(`COALESCE\(SUM\(consume_quota\),0\)-COALESCE\(SUM\(refund_quota\),0\)`)
	entries, err := os.ReadDir(".")
	if err != nil {
		t.Fatal(err)
	}
	hits := []string{}
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
			continue
		}
		b, err := os.ReadFile(e.Name())
		if err != nil {
			t.Fatal(err)
		}
		if pattern.Match(b) {
			hits = append(hits, e.Name())
		}
	}
	if len(hits) != 1 || hits[0] != "usage_facts_live_projection.go" {
		t.Fatalf("今日已封口净额公式必须只在 usage_facts_live_projection.go 出现一次，实际: %v", hits)
	}
}

// 客户维护绝不能走那条直查生产库的回退路径。
//
// 用户用量页在事实层未启用时会退回 computeUsageMatrix 直扫生产 logs，
// 那条路占 usageGate（容量 1）、与客户 Portal 同泳道。本页是常开概览页，
// 每次刷新都那么干会让客户查自己日志排队。
func TestCustomerHealthSpendNeverFallsBackToProductionDB(t *testing.T) {
	q, err := os.ReadFile("customer_health_query.go")
	if err != nil {
		t.Fatal(err)
	}
	src := stripGoLineComments(string(q))
	for _, banned := range []string{"computeUsageMatrix", "acquireInteractiveUsageGate", "acquireUsageGate", "m.prodDB"} {
		if strings.Contains(src, banned) {
			t.Fatalf("客户维护取数不得触碰 %q：那会挤占客户 Portal 的泳道", banned)
		}
	}
}

// ═══════════ 解散公司：前端只发一次原子请求 ═══════════

// 逐个调用用户删除接口会在中途失败时留下半删状态。前端必须把整个动作交给
// /customer-health/groups/dissolve，由后端事务统一处理成员和公司删除。
func TestCustomerHealthDissolveUsesOneAtomicRequest(t *testing.T) {
	src, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	js := stripJSLineCommentsForCH(string(src))
	start := strings.Index(js, "async function deleteCompany")
	if start < 0 {
		t.Fatal("找不到 deleteCompany 实现")
	}
	end := strings.Index(js[start:], "async function saveCompany")
	if end < 0 {
		t.Fatal("找不到 deleteCompany 结束边界")
	}
	body := js[start : start+end]
	if !strings.Contains(body, "/customer-health/groups/dissolve") || !strings.Contains(body, "ch-group-dissolve:") {
		t.Fatal("解散公司必须调用带稳定幂等键的原子 dissolve 接口")
	}
	if strings.Contains(body, "/usage/users/delete") || strings.Contains(body, "for(const mb") {
		t.Fatal("解散公司不得再由浏览器逐个删除成员")
	}
	if strings.Contains(body, "/usage/users/group") || strings.Contains(body, "group_id:0") {
		t.Fatal("解散公司不得把成员改成未分组")
	}
}

// 按钮与确认框文案必须与真实行为一致。
// 这条守的是"文案承诺了一件代码不做的事"这类缺陷——它编译能过、测试也不报，
// 只有用户真点下去才发现，本轮已因此返工一次。
func TestCustomerHealthDeleteButtonTextMatchesRealBehavior(t *testing.T) {
	src, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(src)
	if !strings.Contains(js, "解散公司（连") {
		t.Fatal("按钮必须说清它会连成员一起移除")
	}
	if !strings.Contains(js, "客户维护这边的人和公司一起消失") || !strings.Contains(js, "用户用量里的客户分组和用户名单不受影响") {
		t.Fatal("确认框必须说明只删除客户维护名单，用户用量不受影响")
	}
	// 两版旧文案都不得回归：
	// 1) 直接删组却声称成员回到未分组；
	// 2) 声称把成员改为未分组（本页无此状态）。
	for _, stale := range []string{
		"这会解散该公司分组，",
		"个成员逐个改为「未分组」",
		"客户维护与用户用量两边都不再显示",
	} {
		if strings.Contains(js, stale) {
			t.Fatalf("过时文案不得回归: %q", stale)
		}
	}
}

func TestCustomerHealthSingleUserDeleteExplainsMaintenanceOnlyScope(t *testing.T) {
	src, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	js := string(src)
	for _, required := range []string{"删除用户", "仅删除维护名单记录", "不影响主站账号", "不删除历史用量事实或请求日志"} {
		if !strings.Contains(js, required) {
			t.Fatalf("单用户删除文案没有说明真实边界: %q", required)
		}
	}
}

// ═══════════ 写操作必须走幂等封装 ═══════════

// 所有写操作必须经 window.usageMutationPost：它带稳定 Idempotency-Key 与
// 请求体 request_id，且在 5xx/网络失败时保留键供重试；裸 fetch POST 会绕过
// 页面统一的重复提交保护。
func TestCustomerHealthWritesUseIdempotentWrapper(t *testing.T) {
	src, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	js := stripJSLineCommentsForCH(string(src))
	if !strings.Contains(js, "window.usageMutationPost") {
		t.Fatal("写操作必须复用 window.usageMutationPost，不得自写幂等逻辑")
	}
	// 不得出现裸 POST。
	if strings.Contains(js, "method:'POST'") || strings.Contains(js, `method: 'POST'`) {
		t.Fatal("不得直接 fetch POST：会绕过幂等键与审计重放")
	}
	// 五个写动作都要有稳定 actionKey。新增客户已合并成一条原子写，不能再
	// 使用旧的“先建公司”键，否则前端很可能又退回两次请求。
	for _, key := range []string{
		"ch-customer-add:", "ch-member-add:", "ch-member-del:",
		"ch-group-rename:", "ch-group-dissolve:",
	} {
		if !strings.Contains(js, key) {
			t.Fatalf("缺少稳定 actionKey: %s", key)
		}
	}
	if !strings.Contains(js, "chMutate('/customer-health/customers'") || strings.Contains(js, "ch-group-create:") || strings.Contains(js, "ensureCompany") {
		t.Fatal("新增客户必须只调用原子 /customer-health/customers，不得恢复先建公司再加成员")
	}
}

// page.html 必须把 usageMutationPost 暴露到 window，否则外置 JS 拿不到，
// 只能自己再写一份——那就有两套重试语义。
func TestUsageMutationPostIsExposedForPageLevelJS(t *testing.T) {
	src, err := os.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), "window.usageMutationPost=usageMutationPost") {
		t.Fatal("usageMutationPost 必须挂到 window 供页面级 JS 复用")
	}
}

// ═══════════ 移动端入口与刷新恢复 ═══════════

// 小屏隐藏侧栏，移动端导航漏一项等于该页在手机上完全进不去。
func TestCustomerHealthHasMobileNavEntry(t *testing.T) {
	src, err := os.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	start := strings.Index(body, "monitor-mobile-tabs")
	if start < 0 {
		t.Fatal("找不到移动端导航")
	}
	end := strings.Index(body[start:], "</nav>")
	if end < 0 {
		t.Fatal("移动端导航未闭合")
	}
	nav := body[start : start+end]
	if !strings.Contains(nav, `data-tab="customer-health"`) {
		t.Fatal("移动端导航缺少客户维护入口")
	}
}

// 刷新 #tab=customer-health 必须回到本页，不能跳回用户用量。
func TestCustomerHealthSurvivesHashRestore(t *testing.T) {
	src, err := os.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	// 复用 group_governance_test.go 的 hashWhitelistContains：只判断"在名单里"，
	// 不绑它在名单中的位置——绑位置会让下一个加 tab 的人无故踩红。
	if !hashWhitelistContains(string(src), "customer-health") {
		t.Fatal("hash 白名单缺少 customer-health，刷新 #tab=customer-health 会跳回用户用量")
	}
}

// ═══════════ 前端不分角色，后端鉴权保留 ═══════════

// 该系统日常只有一个维护者使用，客户维护页不再维护两套角色 UI。权限边界仍由
// rootUsage 的服务端鉴权负责，不能因为前端统一展示而开放匿名写接口。
func TestCustomerHealthDoesNotCarryRedundantRoleUI(t *testing.T) {
	js, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	body := string(js)
	for _, redundant := range []string{"ch.isRoot", "customerHealthSetRole", "applyRoleToForm"} {
		if strings.Contains(body, redundant) {
			t.Fatalf("客户维护前端不应再保留角色分支: %s", redundant)
		}
	}
	if !strings.Contains(body, "data-ch-jump") {
		t.Fatal("去排障按钮必须保留")
	}
	if !strings.Contains(body, "preset:'customer_diagnosis'") {
		t.Fatal("去排障必须携带客户诊断预设，不能继承排障页旧筛选")
	}

	page, err := os.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	pageBody := string(page)
	if !strings.Contains(pageBody, `id="chAddForm"`) || strings.Contains(pageBody, `id="chAddForm" hidden`) {
		t.Fatal("客户维护新增表单应直接展示，不再按前端角色切换")
	}
	for _, redundant := range []string{"chRootHint", "customerHealthSetRole"} {
		if strings.Contains(pageBody, redundant) {
			t.Fatalf("page.html 仍有客户维护角色冗余代码: %s", redundant)
		}
	}
}

// 后端权限不得只靠前端隐藏。写路由必须仍在 root 组下。
func TestCustomerHealthWriteRoutesStayRootOnly(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	body := string(src)
	if !strings.Contains(body, `rootUsage := r.Group("/usage", m.requireRole(roleRoot))`) {
		t.Fatal("写接口必须留在 roleRoot 组：前端展示方式不是权限边界")
	}
	for _, route := range []string{
		`rootUsage.POST("/users"`, `rootUsage.POST("/users/delete"`,
		`rootUsage.POST("/users/group"`, `rootUsage.POST("/groups"`,
		`rootUsage.POST("/groups/update"`, `rootUsage.POST("/groups/delete"`,
		`rootUsage.POST("/groups/dissolve"`,
	} {
		if !strings.Contains(body, route) {
			t.Fatalf("写路由不得移出 root 组: %s", route)
		}
	}
	if !strings.Contains(body, `rootCustomerHealth := r.Group("/customer-health", m.requireRole(roleRoot))`) {
		t.Fatal("客户维护独立写接口必须留在 roleRoot 组")
	}
	for _, route := range []string{
		`rootCustomerHealth.POST("/customers"`,
		`rootCustomerHealth.POST("/groups/update"`,
		`rootCustomerHealth.POST("/groups/dissolve"`,
		`rootCustomerHealth.POST("/members"`,
		`rootCustomerHealth.POST("/members/delete"`,
		`rootCustomerHealth.POST("/members/update"`,
	} {
		if !strings.Contains(body, route) {
			t.Fatalf("客户维护写路由不得移出 roleRoot 组: %s", route)
		}
	}
}

// ═══════════ 敏感报表禁止缓存 ═══════════

// 响应含公司名、用户名、user_id、消耗金额与故障归因，属客户可识别信息。
func TestCustomerHealthReportIsNoStore(t *testing.T) {
	src, err := os.ReadFile("server.go")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(src), `view.GET("/customer-health/report", noStoreSensitive,`) {
		t.Fatal("报表路由必须挂 noStoreSensitive")
	}
	js, err := os.ReadFile("customer_health.js")
	if err != nil {
		t.Fatal(err)
	}
	body := string(js)
	if !strings.Contains(body, "cache:'no-store'") {
		t.Fatal("前端报表请求必须带 cache:'no-store'")
	}
	// 报表与分组列表都含公司名，两处都要。
	if strings.Count(body, "cache:'no-store'") < 2 {
		t.Fatalf("含公司名的 GET 都要 no-store，当前只有 %d 处", strings.Count(body, "cache:'no-store'"))
	}
}

// stripJSLineCommentsForCH 去掉 JS 行注释，避免"解释为什么不用某写法"的
// 注释被禁用类断言误命中（交接文档 29.5.2 记过这个坑）。
func stripJSLineCommentsForCH(s string) string {
	out := make([]string, 0, 128)
	for _, line := range strings.Split(s, "\n") {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "//") {
			continue
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// stripGoLineComments 去掉 // 行注释，避免"解释为什么不用某写法"的注释
// 被上面那条禁用断言误命中。这个坑本仓库踩过（见交接文档 29.5.2）。
func stripGoLineComments(s string) string {
	out := make([]string, 0, 64)
	for _, line := range strings.Split(s, "\n") {
		if i := strings.Index(line, "//"); i >= 0 {
			line = line[:i]
		}
		out = append(out, line)
	}
	return strings.Join(out, "\n")
}

// 今天没有请求时稳定率必须是 nil，绝不能是 0。
// 0% 会被读成"全挂了"，而实际是"今天还没用"——这是本仓库的硬约束。
func TestCustomerHealthMissingDataIsNotZero(t *testing.T) {
	m := newTestMonitor(t)
	chSeedCompany(t, m, "乙公司", 201)
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 {
		t.Fatalf("rows=%+v", report.Rows)
	}
	row := report.Rows[0]
	if row.StabilityPct != nil {
		t.Fatalf("无请求时稳定率必须为 nil，不得为 0: %v", *row.StabilityPct)
	}
	if row.Total != 0 || !strings.Contains(row.Reason, "还没有") {
		t.Fatalf("无请求的说明不明确: %+v", row)
	}
	if stability := customerHealthStability(0, 0, 0); stability != nil {
		t.Fatalf("零请求必须返回 nil: %v", *stability)
	}
}

func TestCustomerHealthOldPolicyFactsFailClosed(t *testing.T) {
	m := newTestMonitor(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	chSeedCompany(t, m, "旧口径公司", 201)
	row := CapacityUserMinuteSample{
		BucketTs: from + 60, UserID: 201, ChannelID: 7, ModelName: "old-model", Grp: "g",
		Success: 9, Failed: 1, CustomerHealthFailed: 1,
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).
		Where("bucket_ts = ? AND user_id = ?", row.BucketTs, row.UserID).
		Update("customer_health_version", 0).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if report.Collection.Ready || len(report.Rows) != 1 || report.Rows[0].MetricsReady {
		t.Fatalf("旧口径事实必须 fail-closed: collection=%+v rows=%+v", report.Collection, report.Rows)
	}
	got := report.Rows[0]
	if got.Total != 0 || got.Success != 0 || got.Anomaly != 0 || got.Failed != 0 ||
		got.StabilityAnomaly != 0 || got.StabilityFailed != 0 || got.StabilityPct != nil || len(got.PrimaryModels) != 0 {
		t.Fatalf("fail-closed 响应不得泄露看似可用的旧口径指标: %+v", got)
	}
}

// 空公司不出现在本页：展示出来会被读成"这家公司今天没请求"，
// 而实际是还没加人，两者处置完全不同。
func TestCustomerHealthSkipsCompaniesWithoutMembers(t *testing.T) {
	m := newTestMonitor(t)
	chSeedCompany(t, m, "空公司")
	chSeedCompany(t, m, "有人公司", 301)
	report, err := m.buildCustomerHealthReport(context.Background(), chTestNow())
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Rows) != 1 || report.Rows[0].Company != "有人公司" {
		t.Fatalf("空公司不应出现: %+v", report.Rows)
	}
}

// 稳定率必须与客户排障共用断连边界：正常取消不降低，零输出超过 3 秒降低。
func TestCustomerHealthStabilityUsesActionableClientGoneBoundary(t *testing.T) {
	if !strings.Contains(logChainStreamAnomalySQL(), logChainClientGoneEndReason) {
		t.Fatal("流异常判据未提及 client_gone，无法确认它被排除")
	}
	sql := logChainStreamAnomalySQL()
	if !strings.Contains(sql, "NOT IN") {
		t.Fatalf("client_gone 必须以排除方式出现: %s", sql)
	}
	healthSQL := customerHealthAttributedAnomalySQL()
	for _, want := range []string{"completion_tokens = 0", "COALESCE(use_time,0) > 3", logChainClientGoneEndReason,
		"JSON_EXTRACT(other,'$.frt')", "> 3000"} {
		if !strings.Contains(healthSQL, want) {
			t.Fatalf("客户维护遗漏断连异常边界 %q: %s", want, healthSQL)
		}
	}
	// 口径自洽：总记录 100，计入稳定性的异常 5、错误 5 → 90%。
	pct := customerHealthStability(100, 5, 5)
	if pct == nil || math.Abs(*pct-90) > .001 {
		t.Fatalf("稳定率算法与既有口径不一致: %v", pct)
	}
}

// 归因必须复用既有 fault 判据，且置信度要降档。
//
// 降档原因：那些 confidence 值是为"看着一条请求的原文"设计的，本页看到的是
// 渠道级聚合。声称 high 会让人过度相信一个推断出来的结论。
func TestCustomerHealthAttributionReusesFaultRulesAndDowngrades(t *testing.T) {
	cases := []struct {
		problem customerHealthProblem
		fault   string
	}{
		// 带 status_code 前缀（code 非空）→ 上游说的。
		{customerHealthProblem{code: "503", message: "bad response status code 503", count: 5}, faultUpstream},
		{customerHealthProblem{code: "400", message: "Invalid 'input[60].id'", count: 3}, faultDownstream},
		// 原文措辞明确且来自上游：优先于状态码泛规则。
		{customerHealthProblem{code: "503", message: "No available channel for model gpt-5", count: 9}, faultUpstream},
		// 无前缀的同样措辞 → 我方路由失败。
		{customerHealthProblem{code: "", message: "No available channel for model gpt-5", count: 2}, faultOurs},
		// 401/403 原文不含判别信息，必须保持待判，不能猜。
		{customerHealthProblem{code: "401", message: "bad response status code 401", count: 4}, faultUnknown},
	}
	for _, tc := range cases {
		got := customerHealthAttributeProblem(tc.problem)
		if got.fault != tc.fault {
			t.Fatalf("message=%q code=%q fault=%s want %s", tc.problem.message, tc.problem.code, got.fault, tc.fault)
		}
		if got.reason == "" {
			t.Fatalf("归因必须给出依据说明: %+v", got)
		}
	}
	if customerHealthDowngradeConfidence(faultConfHigh) != faultConfMid {
		t.Fatal("high 必须降为 mid")
	}
	if customerHealthDowngradeConfidence(faultConfNone) != faultConfNone {
		t.Fatal("none 不参与降档")
	}
}

// unknown 不得凭数量胜出：那一档的意义是"别瞎猜"，让它赢会使页面永远显示
// "需人工判断"，等于没做归因。
func TestCustomerHealthUnknownNeverWinsOverDefiniteFault(t *testing.T) {
	verdict := customerHealthPickFault([]customerHealthProblem{
		{code: "500", message: "internal error", count: 1000}, // unknown，量极大
		{code: "502", message: "bad response status code 502", count: 1},
	})
	if verdict.fault != faultUpstream {
		t.Fatalf("确定档必须优先于 unknown: %+v", verdict)
	}
	// 只有 unknown 时才允许 unknown 胜出。
	onlyUnknown := customerHealthPickFault([]customerHealthProblem{
		{code: "500", message: "internal error", count: 3},
	})
	if onlyUnknown.fault != faultUnknown {
		t.Fatalf("全是待判时应为 unknown: %+v", onlyUnknown)
	}
}

// 同渠道横向比较必须按失败占比差值判断，并保持无对比数据的状态分离。
func TestCustomerHealthChannelScopeSeparatesFourStates(t *testing.T) {
	mine := map[int64]struct{}{101: {}}
	// 只有自己在用：不能说成"只有这家有问题"——没有比较对象。
	soleUsage := &customerHealthUsageIndex{
		byUser:       map[int64]*customerHealthUserFacts{101: {channels: map[int]customerHealthChannelFacts{7: {failed: 5, healthFailed: 5}}}},
		channelUsers: map[int]map[int64]struct{}{7: {101: {}}},
	}
	if scope, note := customerHealthChannelScope(7, soleUsage, mine); scope != customerHealthScopeSoleUser || note == "" {
		t.Fatalf("独占渠道应为 sole_user: %s", scope)
	}
	// 双方稳定率都高于 90%，失败率只差 4 个百分点 → 更像渠道问题。
	sharedUsage := &customerHealthUsageIndex{
		byUser: map[int64]*customerHealthUserFacts{
			101: {channels: map[int]customerHealthChannelFacts{7: {success: 95, failed: 5, healthFailed: 5}}},
			999: {channels: map[int]customerHealthChannelFacts{7: {success: 99, failed: 1, healthFailed: 1}}},
		},
		channelUsers: map[int]map[int64]struct{}{7: {101: {}, 999: {}}},
	}
	if scope, _ := customerHealthChannelScope(7, sharedUsage, mine); scope != customerHealthScopeShared {
		t.Fatalf("失败率差值不足 7 个百分点应为 all_customers: %s", scope)
	}
	// 当前公司失败率比其他账号高超过 7 个百分点 → 更像该公司问题。
	isolatedUsage := &customerHealthUsageIndex{
		byUser: map[int64]*customerHealthUserFacts{
			101: {channels: map[int]customerHealthChannelFacts{7: {success: 92, failed: 8, healthFailed: 8}}},
			999: {channels: map[int]customerHealthChannelFacts{7: {success: 1000, failed: 1, healthFailed: 1}}},
		},
		channelUsers: map[int]map[int64]struct{}{7: {101: {}, 999: {}}},
	}
	if scope, _ := customerHealthChannelScope(7, isolatedUsage, mine); scope != customerHealthScopeOnlyThis {
		t.Fatalf("当前公司失败率高出至少 7 个百分点应为 only_this_customer: %s", scope)
	}
	// 边界正好 7 个百分点也归为当前公司问题。
	boundaryUsage := &customerHealthUsageIndex{
		byUser: map[int64]*customerHealthUserFacts{
			101: {channels: map[int]customerHealthChannelFacts{7: {success: 93, failed: 7, healthFailed: 7}}},
			999: {channels: map[int]customerHealthChannelFacts{7: {success: 100}}},
		},
		channelUsers: map[int]map[int64]struct{}{7: {101: {}, 999: {}}},
	}
	if scope, _ := customerHealthChannelScope(7, boundaryUsage, mine); scope != customerHealthScopeOnlyThis {
		t.Fatalf("失败率正好高出 7 个百分点应为 only_this_customer: %s", scope)
	}
	// 无本地事实 → 必须是 unknown，不能默认成"只有这家有问题"。
	empty := &customerHealthUsageIndex{byUser: map[int64]*customerHealthUserFacts{}, channelUsers: map[int]map[int64]struct{}{}}
	if scope, _ := customerHealthChannelScope(7, empty, mine); scope != customerHealthScopeUnknown {
		t.Fatalf("无事实时应为 unknown: %s", scope)
	}
}

// 有失败计数但没采到错误原文时，必须说清是"证据缺失"，不能显示成没问题。
func TestCustomerHealthReportsMissingEvidenceInsteadOfHealthy(t *testing.T) {
	usage := &customerHealthUsageIndex{
		byUser: map[int64]*customerHealthUserFacts{101: {
			failed: 8, healthFailed: 8,
			channels: map[int]customerHealthChannelFacts{7: {failed: 8, healthFailed: 8}},
		}},
		channelUsers: map[int]map[int64]struct{}{7: {101: {}}},
	}
	problems := &customerHealthProblemIndex{byChannel: map[int][]customerHealthProblem{}}
	company := customerHealthCompany{groupID: 1, name: "丙公司", members: []CustomerHealthMember{{UserID: 101}}}
	row := buildCustomerHealthRow(company, usage, problems)
	if row.Fault != faultUnknown || !strings.Contains(row.Reason, "未采到") {
		t.Fatalf("证据缺失必须显式说明: %+v", row)
	}
	// 全部成功时才允许显示"今天没问题"。
	healthyUsage := &customerHealthUsageIndex{
		byUser:       map[int64]*customerHealthUserFacts{101: {success: 50, channels: map[int]customerHealthChannelFacts{7: {success: 50}}}},
		channelUsers: map[int]map[int64]struct{}{7: {101: {}}},
	}
	healthy := buildCustomerHealthRow(company, healthyUsage, problems)
	if healthy.ChannelScope != customerHealthScopeHealthy || healthy.Fault != "" {
		t.Fatalf("无失败时不得给出责任方: %+v", healthy)
	}
}

// 责任方证据必须精确命中“渠道＋模型＋分组”，而且必须来自该公司自己的
// type=5 错误范围。只同渠道、只有异常 type=2 都不足以借用问题签名定责。
func TestCustomerHealthAttributionRequiresOwnExactFailedScope(t *testing.T) {
	company := customerHealthCompany{groupID: 1, name: "范围公司", members: []CustomerHealthMember{{UserID: 101}}}
	exactScope := customerHealthProblemScope{modelName: "wanted-model", grp: "wanted-group"}
	problem := customerHealthProblem{channelID: 7, modelName: exactScope.modelName, grp: exactScope.grp,
		code: "502", message: "bad response status code 502", count: 3}

	failedUsage := func(scopes map[customerHealthProblemScope]int64) *customerHealthUsageIndex {
		return &customerHealthUsageIndex{
			byUser: map[int64]*customerHealthUserFacts{101: {
				failed:       3,
				healthFailed: 3,
				channels:     map[int]customerHealthChannelFacts{7: {failed: 3, healthFailed: 3, failedScopes: scopes}},
			}},
			channelUsers: map[int]map[int64]struct{}{7: {101: {}}},
		}
	}
	tests := []struct {
		name     string
		usage    *customerHealthUsageIndex
		problems []customerHealthProblem
		want     string
	}{
		{"同渠道不同模型不能借用", failedUsage(map[customerHealthProblemScope]int64{{modelName: "other", grp: exactScope.grp}: 3}), []customerHealthProblem{problem}, faultUnknown},
		{"同渠道同模型不同分组不能借用", failedUsage(map[customerHealthProblemScope]int64{{modelName: exactScope.modelName, grp: "other"}: 3}), []customerHealthProblem{problem}, faultUnknown},
		{"计入稳定性的流异常按共同判据归上游", &customerHealthUsageIndex{
			byUser: map[int64]*customerHealthUserFacts{101: {
				anomaly: 3, healthAnomaly: 3,
				channels: map[int]customerHealthChannelFacts{7: {anomaly: 3, healthAnomaly: 3}},
			}},
			channelUsers: map[int]map[int64]struct{}{7: {101: {}}},
		}, []customerHealthProblem{problem}, faultUpstream},
		{"完全匹配仍可定责", failedUsage(map[customerHealthProblemScope]int64{exactScope: 3}), []customerHealthProblem{problem}, faultUpstream},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			row := buildCustomerHealthRow(company, tc.usage,
				&customerHealthProblemIndex{byChannel: map[int][]customerHealthProblem{7: tc.problems}})
			if row.Fault != tc.want {
				t.Fatalf("fault=%q want=%q row=%+v", row.Fault, tc.want, row)
			}
		})
	}
}

func TestCustomerHealthSamplingAppliesResponsibilityPolicy(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	from, _, _ := customerHealthDayRange(chTestNow())
	type sourceRow struct {
		id, typ, completion, useTime int
		content, other               string
	}
	rows := []sourceRow{
		// 明确客户/下游错误：不降低稳定率。
		{1, 5, 0, 0, "status_code=400, invalid input", `{}`},
		{2, 5, 0, 0, "status_code=400, invalid input", `{"error_code":"array_above_max_length"}`},
		{3, 5, 0, 0, "status_code=413, body too large", `{}`},
		// 上游、我方和待判错误：都降低稳定率。
		{4, 5, 0, 0, "status_code=503, unavailable", `{}`},
		{5, 5, 0, 0, "status_code=401, unauthorized", `{}`},
		{6, 5, 0, 0, "status_code=400, No available channel for model gpt-test", `{}`},
		{7, 5, 0, 0, "No available channel for model gpt-test", `{}`},
		{8, 5, 0, 0, "status_code=400, upstream dispatch failed", `{"error_code":"do_request_failed"}`},
		// 原文规则优先于状态码/error_code，仍应计入。
		{9, 5, 0, 0, "status_code=400, No available channel for model gpt-test", `{"error_code":"array_above_max_length"}`},
		// 真实流异常和零输出超过 3 秒的断连降低稳定率；普通零输出待判不计入。
		{10, 2, 20, 10, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"timeout","error_count":0}}`},
		{11, 2, 0, 10, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"eof","error_count":0}}`},
		// 已产出内容的断连，以及零输出但恰好 3 秒内取消，均按正常请求。
		{12, 2, 20, 20, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"client_gone","error_count":0}}`},
		{13, 2, 20, 10, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"eof","error_count":2}}`},
		{14, 2, 0, 3, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"client_gone","error_count":0}}`},
		{15, 2, 0, 4, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"client_gone","error_count":0}}`},
		// 零输出且不是客户 3 秒内取消：FRT 严格超过 3 秒才计入稳定性；
		// 恰好 3 秒和缺失 FRT 都保留为待判，不降低稳定率。
		{16, 2, 0, 10, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"eof","error_count":0},"frt":3001}`},
		{17, 2, 0, 10, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"eof","error_count":0},"frt":3000}`},
		{18, 2, 0, 10, "", `{"request_path":"/v1/chat/completions","stream_status":{"end_reason":"eof","error_count":0}}`},
	}
	for _, row := range rows {
		if _, err := m.prodDB.Exec(`INSERT INTO logs
			(id,user_id,channel_id,created_at,type,model_name,quota,prompt_tokens,completion_tokens,use_time,`+"`group`"+`,username,content,other)
			VALUES (?,101,7,?,?,?,?,10,?,?,'g','alice',?,?)`,
			row.id, from+int64(row.id), row.typ, "gpt-test", 0, row.completion, row.useTime, row.content, row.other); err != nil {
			t.Fatal(err)
		}
	}
	// 生产查询使用 MySQL 的 DIV；SQLite 假来源只为真执行判据，把这一处等价替换
	// 成整数除法。其余完整 SELECT、JSON、REGEXP 和列顺序均执行原始成品 SQL。
	query := strings.Replace(sampleWindowUserSQL(), "(created_at DIV 60)*60", "CAST(created_at / 60 AS INTEGER)*60", 1)
	result, err := m.prodDB.QueryContext(context.Background(), query, from, from+60)
	if err != nil {
		t.Fatal(err)
	}
	defer result.Close()
	columns, err := result.Columns()
	if err != nil {
		t.Fatal(err)
	}
	wantPrefix := []string{"bucket", "channel_id", "model_name", "grp", "user_id", "username", "success", "anomaly", "failed", "customer_health_anomaly", "customer_health_failed"}
	if len(columns) < len(wantPrefix) || strings.Join(columns[:len(wantPrefix)], ",") != strings.Join(wantPrefix, ",") {
		t.Fatalf("用户采样列顺序漂移，sampler Scan 会错位: %v", columns)
	}
	if !result.Next() {
		t.Fatalf("成品用户采样 SQL 没有返回聚合行: %v", result.Err())
	}
	values := make([]any, len(columns))
	dest := make([]any, len(columns))
	for i := range values {
		dest[i] = &values[i]
	}
	if err := result.Scan(dest...); err != nil {
		t.Fatal(err)
	}
	asInt := func(name string) int64 {
		t.Helper()
		idx := -1
		for i, column := range columns {
			if column == name {
				idx = i
				break
			}
		}
		if idx < 0 {
			t.Fatalf("缺少采样列 %s: %v", name, columns)
		}
		n, err := strconv.ParseInt(fmt.Sprint(values[idx]), 10, 64)
		if err != nil {
			t.Fatalf("采样列 %s=%v 不是整数: %v", name, values[idx], err)
		}
		return n
	}
	if raw, filtered := asInt("failed"), asInt("customer_health_failed"); raw != 9 || filtered != 6 {
		t.Fatalf("错误责任方筛选错误: raw=%d stability=%d", raw, filtered)
	}
	if raw, filtered := asInt("anomaly"), asInt("customer_health_anomaly"); raw != 7 || filtered != 4 {
		t.Fatalf("异常责任方筛选错误: raw=%d stability=%d", raw, filtered)
	}
	if success := asInt("success"); success != 2 {
		t.Fatalf("已产出断连和 3 秒边界取消都应计为正常: success=%d", success)
	}
}

// 旧的全局 LIMIT 2000 会被一个高噪声范围吃光。排名必须按精确归因范围
// 独立计算；同时旧判据版本与 nginx 旁路记录不得混进 NewAPI 错误归因。
func TestCustomerHealthProblemsRanksEachExactScopeIndependently(t *testing.T) {
	m := newTestMonitor(t)
	from, to, _ := customerHealthDayRange(chTestNow())
	rows := make([]StabilityProblemSample, 0, customerHealthMaxProblemsPerScope+8)
	for i := 0; i < customerHealthMaxProblemsPerScope+5; i++ {
		rows = append(rows, StabilityProblemSample{
			BucketTs: from + 60, Source: "newapi", SignatureHash: fmt.Sprintf("hot-%03d", i),
			ChannelID: 7, ModelName: "hot-model", Grp: "hot-group", Code: "503",
			Message: fmt.Sprintf("hot failure %03d", i), Count: int64(1000 - i),
		})
	}
	rows = append(rows,
		StabilityProblemSample{BucketTs: from + 60, Source: "newapi", SignatureHash: "wanted",
			ChannelID: 7, ModelName: "wanted-model", Grp: "wanted-group", Code: "502", Message: "wanted", Count: 1},
		StabilityProblemSample{BucketTs: from + 60, Source: "nginx_error", SignatureHash: "wrong-source",
			ChannelID: 7, ModelName: "wanted-model", Grp: "wanted-group", Code: "504", Message: "wrong-source", Count: 99999},
		StabilityProblemSample{BucketTs: from + 60, TrafficClassVersion: stabilityTrafficClassificationVersion + 1,
			Source: "newapi", SignatureHash: "wrong-version", ChannelID: 7, ModelName: "wanted-model",
			Grp: "wanted-group", Code: "500", Message: "wrong-version", Count: 99999},
	)
	if err := m.storeDB.CreateInBatches(rows, 40).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.customerHealthProblems(context.Background(), from, to, []int64{7})
	if err != nil {
		t.Fatal(err)
	}
	var wanted []customerHealthProblem
	for _, p := range got.byChannel[7] {
		if p.modelName == "wanted-model" && p.grp == "wanted-group" {
			wanted = append(wanted, p)
		}
	}
	if len(wanted) != 1 || wanted[0].message != "wanted" {
		t.Fatalf("低频精确范围应保留且只能使用当前 newapi 判据，got=%+v", wanted)
	}
}

// 批量金额读取后仍按公司独立 fail-closed：一家公司有未发布成员，不得让
// 另一家已经完整发布的公司也变成不可知。
func TestCustomerHealthSpendBatchKeepsCompaniesIndependent(t *testing.T) {
	m := newTestMonitor(t)
	m.prodDB = newFakeProdDB(t)
	enableUsageFactsForTest(m)
	from, to, _ := customerHealthDayRange(chTestNow())
	readyGroup := chSeedCompany(t, m, "已就绪公司", 101)
	pendingGroup := chSeedCompany(t, m, "待发布公司", 201)
	finalizedThrough := from + usageFactHourSeconds
	seedPublishedUsageFactsForTest(t, m, []int64{101}, from, finalizedThrough)
	m.setUsageFactsReadiness(true, finalizedThrough)
	if err := m.usageFactsStore().Create(&UsageHourFact{
		HourTs: from, DayTs: from, UserID: 101, ChannelID: 1, Grp: "g", ModelName: "m", TokenID: 1,
		ConsumeQuota: 500000,
	}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []CustomerHealthRow{
		{GroupID: readyGroup, Company: "已就绪公司", Members: []CustomerHealthMember{{UserID: 101}}},
		{GroupID: pendingGroup, Company: "待发布公司", Members: []CustomerHealthMember{{UserID: 201}}},
	}
	m.attachCustomerHealthSpendBatch(context.Background(), rows, from, to, chTestNow())
	if rows[0].TodaySpendUSD == nil || math.Abs(*rows[0].TodaySpendUSD-1) > 1e-9 || rows[0].SpendState != customerHealthSpendFinalized {
		t.Fatalf("已发布公司不应被另一家公司拖累: %+v", rows[0])
	}
	if rows[1].TodaySpendUSD != nil || rows[1].SpendState != customerHealthSpendPartial {
		t.Fatalf("未发布公司必须独立保持 partial: %+v", rows[1])
	}
}

func TestCustomerHealthAutoRefreshAndRecordWordingContract(t *testing.T) {
	js := string(customerHealthJS)
	for _, want := range []string{"refreshTimer", "setTimeout(async()=>", "60000", "visibilitychange", "document.hidden", "customerHealthDeactivate"} {
		if !strings.Contains(js, want) {
			t.Errorf("客户维护自动刷新契约缺少 %q", want)
		}
	}
	start := strings.Index(pageHTML, `id="tab-customer-health"`)
	if start < 0 {
		t.Fatal("找不到客户维护页面起点")
	}
	end := strings.Index(pageHTML[start:], `id="tab-alerts"`)
	if end < 0 {
		t.Fatal("找不到客户维护页面区间")
	}
	section := pageHTML[start : start+end]
	for _, want := range []string{"今日日志记录", "正常 / 异常 / 错误记录", "记录稳定率", "日志条数口径", "主要模型", "两个均超 40% 时都显示"} {
		if !strings.Contains(section, want) {
			t.Errorf("日志记录口径文案缺少 %q", want)
		}
	}
	for _, want := range []string{"colspan=\"7\"", "计入稳定性的异常", "计入稳定性的错误", "暂无可识别模型"} {
		if !strings.Contains(section+string(customerHealthJS), want) {
			t.Errorf("客户维护新口径展示缺少 %q", want)
		}
	}
	for _, forbidden := range []string{"今日请求</span>", "请求稳定率"} {
		if strings.Contains(section, forbidden) {
			t.Errorf("客户维护不得回退到会被理解成去重请求量的文案 %q", forbidden)
		}
	}
	if !strings.Contains(string(stabilityJS), "'customer-health':{title:'客户维护'") {
		t.Error("客户维护必须在 ST_HEADERS 注册独立页头，不能回退显示为用户用量")
	}
	if !strings.Contains(string(stabilityJS), "heart:'<svg") {
		t.Error("客户维护页头缺少 heart 图标注册")
	}
}

// ★ 每个 tabpane 必须有对应的 hidden 控制行 ★
//
// 2026-09-15 实测事故：客户维护面板加进了 page.html，但漏了 switchTab 里的
//
//	document.getElementById('tab-xxx').hidden=(name!=='xxx')
//
// 结果该面板永远不隐藏，切到任何其它页签时它都摊在页面上盖住内容——
// 表现为"所有功能点击都没反应"，整页失效。这是"新增功能不得妨碍既有功能"
// 被违反的典型形态：新面板本身没错，错在没接进既有的显示/隐藏机制。
//
// 本测试对**所有**面板生效，不只客户维护：以后任何人加页签漏了这一行都会被挡住。
func TestEveryTabPaneHasHiddenControl(t *testing.T) {
	page, err := os.ReadFile("page.html")
	if err != nil {
		t.Fatal(err)
	}
	html := string(page)
	paneIDs := regexp.MustCompile(`id="(tab-[a-z-]+)"`).FindAllStringSubmatch(html, -1)
	if len(paneIDs) < 10 {
		t.Fatalf("只找到 %d 个 tabpane，页面结构可能已变，请核对本测试", len(paneIDs))
	}
	seen := map[string]bool{}
	for _, match := range paneIDs {
		id := match[1]
		if seen[id] {
			continue
		}
		seen[id] = true
		// 控制行形如 getElementById('tab-xxx').hidden=(name!=='xxx')
		control := `getElementById('` + id + `').hidden=`
		if !strings.Contains(html, control) {
			t.Errorf("面板 %s 没有 hidden 控制行：切到其它页签时它不会隐藏，会盖住整页", id)
		}
	}
}

// ★ 页面级 JS 必须整体包在 IIFE 里 ★
//
// 2026-09-15 实测事故：customer_health.js 在顶层写了 const esc=...，
// 而 page.html 的内联脚本已在全局声明过同名 const。同一全局作用域两次 const
// 声明会抛 SyntaxError，**整个页面的 JS 全部不执行**——表现为进去停在默认页签、
// 所有页签点击都没反应。静态检查全绿也照样发生，只有真点一下才看得见。
//
// 本测试对所有页面级 JS 生效：以后谁新增页签脚本忘了包 IIFE 都会被挡住。
func TestPageLevelJSFilesAreIIFEWrapped(t *testing.T) {
	files := []string{
		"logchain.js", "alerts.js", "stability.js", "capacity.js",
		"channel_management.js", "group_governance.js", "customer_health.js",
	}
	for _, name := range files {
		body, err := os.ReadFile(name)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		// 取首个非空、非注释行。
		var first string
		for _, line := range strings.Split(string(body), "\n") {
			trimmed := strings.TrimSpace(line)
			if trimmed == "" || strings.HasPrefix(trimmed, "//") {
				continue
			}
			first = trimmed
			break
		}
		if !strings.HasPrefix(first, "(function(") {
			t.Errorf("%s 未包在 IIFE 里（首行 %q）：顶层声明会与 page.html 的全局同名 const "+
				"冲突并抛 SyntaxError，导致整页 JS 不执行", name, first)
		}
	}
}

// 长篇能力边界说明已从客户维护页移除；接口字段保留为空以兼容旧客户端。
func TestCustomerHealthNotesStateBoundaries(t *testing.T) {
	if notes := customerHealthNotes(); len(notes) != 0 {
		t.Fatalf("客户维护不应再下发长篇说明: %v", notes)
	}
	if strings.Contains(pageHTML, `id="chNotes"`) || strings.Contains(string(customerHealthJS), "renderNotes") {
		t.Fatal("客户维护页面仍保留已移除的说明渲染入口")
	}
}
