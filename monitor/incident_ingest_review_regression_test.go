package monitor

// B3 复核回归（2026-10-12 复核报告的 9 项 + 2 个留痕缺口）。
//
// 每个用例都对应一个**已复现**的缺陷，不是推测。断言写的是修复后的正确
// 行为，并在注释里记下原来错在哪，避免同一个坑被改回去。

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

const reviewRegBase int64 = 1700000100

func reviewRegFacts() incidentWindowFacts {
	return incidentWindowFacts{ChannelID: 777, ModelName: "reg-model", Grp: "reg-group",
		Success: 10, Anomaly: 90}
}

// reviewRegDecision 构造一个判定结果；gap=true 时覆盖凭证为空（缺口）。
func reviewRegDecision(t *testing.T, core IncidentConfig, ingest IncidentIngestConfig,
	from int64, gap bool) incidentWindowDecision {
	t.Helper()
	proof := coverageComplete(from, from+300)
	if gap {
		proof = CustomerHealthHistoricalCoverage{}
	}
	d, err := decideIncidentWindow(core, ingest, reviewRegFacts(), true, proof, from+600, from, from+300)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}
	return d
}

func reviewRegSeedFacts(t *testing.T, m *Monitor, from, success, anomaly int64) {
	t.Helper()
	f := reviewRegFacts()
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: from, UserID: 1, ChannelID: f.ChannelID,
		ModelName: f.ModelName, Grp: f.Grp, Success: success, Anomaly: anomaly,
	})
}

func reviewRegState(t *testing.T, m *Monitor, key string, to int64) IncidentIngestWindowState {
	t.Helper()
	var row IncidentIngestWindowState
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?", key, to).
		Take(&row).Error; err != nil {
		t.Fatalf("读窗口状态失败(key=%s to=%d): %v", key, to, err)
	}
	return row
}

// 1a. 窗口级失败必须真正重扫，不能只加重试次数。
//
// 原缺陷：覆盖查询失败后写占位行并推进 scan，而 retryIncidentWindowLevelFailure
// 只自增 attempts 与退避时刻，从不重新聚合——次数耗尽即永久漏掉该窗口。
func TestIncidentReviewWindowFailureActuallyRescans(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	reviewRegSeedFacts(t, m, reviewRegBase, 10, 90)
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)

	// 注入一次性的覆盖读取失败。
	const cb = "reg:transient_coverage_read"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(cb, func(tx *gorm.DB) {
		if tx.Statement.Table == "customer_health_day_coverages" {
			_ = tx.AddError(fmt.Errorf("注入的覆盖读取失败"))
		}
	}); err != nil {
		t.Fatalf("注册回调失败: %v", err)
	}
	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: reviewRegBase}
	now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
	if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatalf("扫描失败: %v", err)
	}
	if err := m.storeDB.Callback().Query().Remove(cb); err != nil {
		t.Fatalf("移除回调失败: %v", err)
	}

	ph := reviewRegState(t, m, incidentIngestWindowErrorKey, reviewRegBase+300)
	if ph.ProcessState != incidentProcessFailed {
		t.Fatalf("前置条件：应留下 failed 占位行，实际 %s", ph.ProcessState)
	}
	if ph.LastError == "" {
		t.Error("失败原因必须持久化，不能只写日志")
	}

	// 来源恢复后推进队列：必须真的重扫并完成真实聚合键。
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now+3600); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	key, err := incidentKeyForFacts(reviewRegFacts())
	if err != nil {
		t.Fatalf("映射聚合键失败: %v", err)
	}
	real := reviewRegState(t, m, key.DedupKey(), reviewRegBase+300)
	if real.ProcessState != incidentProcessDone {
		t.Errorf("重扫后真实聚合键应完成，实际 %s（err=%q）", real.ProcessState, real.LastError)
	}
	// 占位行收尾为完整，不再阻挡水位，但记录保留（曾失败过是事实）。
	after := reviewRegState(t, m, incidentIngestWindowErrorKey, reviewRegBase+300)
	if after.ProcessState != incidentProcessDone || !after.DataComplete {
		t.Errorf("重扫成功后占位行应收尾为 done+完整，实际 %s/%v",
			after.ProcessState, after.DataComplete)
	}
	if after.Attempts == 0 {
		t.Error("重试次数应保留，便于排查曾经失败过")
	}
}

// 1b. 必须用真实 sentinel 判定不可归属，不做字符串匹配。
func TestIncidentReviewUsesRealUnattributableSentinel(t *testing.T) {
	wrapped := fmt.Errorf("提交观测失败: %w", ErrObservationUnattributed)
	if !incidentIsUnattributableError(wrapped) {
		t.Fatalf("必须用 errors.Is 认 ErrObservationUnattributed，实际未识别: %v", wrapped)
	}
	// 自造的「看起来像」的文本不得被误判为该硬错误。
	if incidentIsUnattributableError(fmt.Errorf("旧窗口无法归属到任何分段")) {
		t.Error("不得靠字符串匹配判定：那会把普通失败误判成不可归属而跳过重试")
	}
	if incidentIsUnattributableError(nil) {
		t.Error("nil 不是错误")
	}
}

// 2a. 主扫描也必须遵守同键时间顺序。
//
// 原缺陷：顺序门禁只在来源队列里做，主扫描直接 commit——旧窗未完成时
// 新窗已被送进引擎建单，旧窗补齐后就会被判迟到或无法归属。
func TestIncidentReviewMainScanRespectsEarlierWindow(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	// 同键的更早窗口仍未终结。
	early := windowState(d.DedupKey, reviewRegBase, reviewRegBase+300,
		incidentProcessAwaitingSource, incidentWindowStateMixedVersion, false)
	early.CanonicalKey = d.Canonical
	seedWindowState(t, m, early)

	// 更晚的窗口来源已满足。
	reviewRegSeedFacts(t, m, reviewRegBase+300, 10, 90)
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+600)

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: reviewRegBase + 300}
	if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor,
		reviewRegBase+600+int64(ingest.FinalizeLagSeconds)); err != nil {
		t.Fatalf("扫描失败: %v", err)
	}

	late := reviewRegState(t, m, d.DedupKey, reviewRegBase+600)
	if late.ProcessState != incidentProcessAwaitingSource {
		t.Errorf("同键旧窗未终结时，主扫描的晚窗必须排队，实际 %s", late.ProcessState)
	}
	if late.SampleTotal != nil {
		t.Error("排队窗口不得带判定输入快照（它还没冻结输入）")
	}
	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 0 {
		t.Errorf("乱序窗口不得送引擎建单，实际 %d 条事故", incidents)
	}
}

// 2b. 轮转必须发生在 SQL 取数阶段。
//
// 原缺陷：先按 dedup_key 排序取 PendingBatch*2 行，再在内存轮转；字典序
// 靠前的大键吃满 LIMIT，靠后的键根本进不了结果集，设 LastScheduledDedupKey
// 也无效。
func TestIncidentReviewQueueIncludesKeysBeyondSQLLimit(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ingest.PendingBatch = 20
	m := openIncidentTestStore(t)

	k1, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 101, ModelName: "a", Grp: "g"})
	k2, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 202, ModelName: "b", Grp: "g"})
	// 固定方向：k1 字典序在前，让它成为「吃满 LIMIT 的大键」。
	if k1.DedupKey() > k2.DedupKey() {
		k1, k2 = k2, k1
	}
	for i := int64(0); i < 50; i++ {
		r := windowState(k1.DedupKey(), reviewRegBase+i*300, reviewRegBase+(i+1)*300,
			incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
		r.CanonicalKey = k1.Canonical()
		seedWindowState(t, m, r)
	}
	rowB := windowState(k2.DedupKey(), reviewRegBase, reviewRegBase+300,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	rowB.CanonicalKey = k2.Canonical()
	seedWindowState(t, m, rowB)

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID,
		LastScheduledDedupKey: k1.DedupKey()}
	if err := m.advanceIncidentPendingWindows(context.Background(), core, ingest,
		&cursor, reviewRegBase+3600); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	after := reviewRegState(t, m, k2.DedupKey(), reviewRegBase+300)
	if after.SourceCheckAttempts == 0 {
		t.Errorf("字典序靠后的键必须被调度到：大键 50 行、batch=20，上轮停在 %s",
			cursor.LastScheduledDedupKey)
	}
}

// 2c. 反方向也要成立：大键在字典序之后时同样不能把小键挤掉。
func TestIncidentReviewQueueRotatesBothKeyOrders(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ingest.PendingBatch = 10
	m := openIncidentTestStore(t)

	ka, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 303, ModelName: "a", Grp: "g"})
	kb, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 404, ModelName: "b", Grp: "g"})
	small, big := ka, kb
	if small.DedupKey() > big.DedupKey() {
		small, big = big, small
	}
	// 大量行放在字典序靠后的键上。
	for i := int64(0); i < 40; i++ {
		r := windowState(big.DedupKey(), reviewRegBase+i*300, reviewRegBase+(i+1)*300,
			incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
		r.CanonicalKey = big.Canonical()
		seedWindowState(t, m, r)
	}
	one := windowState(small.DedupKey(), reviewRegBase, reviewRegBase+300,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	one.CanonicalKey = small.Canonical()
	seedWindowState(t, m, one)

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID}
	if err := m.advanceIncidentPendingWindows(context.Background(), core, ingest,
		&cursor, reviewRegBase+3600); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	after := reviewRegState(t, m, small.DedupKey(), reviewRegBase+300)
	if after.SourceCheckAttempts == 0 {
		t.Error("少量窗口的键必须也被调度，不能被另一个键的 40 行挤掉")
	}
}

// 3a. 没有覆盖凭证、没有事实时，完整水位不得前进。
//
// 原缺陷：没有聚合键就不落任何待处理记录，scan 一过 complete 跟着越过，
// 之后事实补到该窗也没有状态行可复核。那不等于「覆盖完整且确实无流量」。
func TestIncidentReviewEmptyUncoveredWindowDoesNotClaimComplete(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: reviewRegBase}
	if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor,
		reviewRegBase+300+int64(ingest.FinalizeLagSeconds)); err != nil {
		t.Fatalf("扫描失败: %v", err)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatalf("算水位失败: %v", err)
	}
	if cursor.CompleteThroughTs > reviewRegBase {
		t.Errorf("无覆盖凭证又无事实时完整水位不得前进，实际 %d（scan=%d）",
			cursor.CompleteThroughTs, cursor.ScanThroughTs)
	}
	// 必须留下可被继续推进的阻塞记录。
	var n int64
	m.storeDB.Model(&IncidentIngestWindowState{}).
		Where("dedup_key = ? AND data_complete = ?", incidentIngestWindowCoverageKey, false).
		Count(&n)
	if n == 0 {
		t.Error("必须留下覆盖未采齐的阻塞记录，否则事实补齐后无从复核")
	}
}

// 3b. 覆盖补齐且确实无流量后，阻塞记录收尾为 no_sample 并放行水位。
func TestIncidentReviewCoverageBlockResolvesToNoSample(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: reviewRegBase}
	now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
	if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatalf("扫描失败: %v", err)
	}
	// 覆盖补齐，但该窗确实没有任何事实行。
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)
	if err := m.processIncidentWindow(ctx, core, ingest,
		reviewRegBase, reviewRegBase+300, now); err != nil {
		t.Fatalf("重新处理失败: %v", err)
	}
	row := reviewRegState(t, m, incidentIngestWindowCoverageKey, reviewRegBase+300)
	if row.WindowState != incidentWindowStateNoSample {
		t.Errorf("覆盖补齐且确认无流量应收尾为 no_sample，实际 %s", row.WindowState)
	}
	if !row.DataComplete {
		t.Error("确认无流量后应标为完整，不再阻挡水位")
	}
}

// 3c. 实时游标已越过缺口时，不得误判为 wait_coverage。
//
// 原缺陷：全局前沿取的是「从目标日起点开始的连续拼接」，前一天缺覆盖就
// 停在缺口前，于是实时游标明明已越过本窗，本窗仍被判成「还会继续推进」。
func TestIncidentReviewGapWithAdvancedLiveCursorIsGapObserved(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	day := customerHealthDayStart(reviewRegBase)
	from := day - 300 // 落在前一天最后一个窗口
	reviewRegSeedFacts(t, m, from, 10, 90)

	// 只有当天的实时游标，且已越过目标窗；前一天没有覆盖凭证。
	live := CustomerHealthSourceCursor{ID: 1, DayTs: day, ThroughTs: reviewRegBase + 600,
		SemanticsVersion: customerHealthStabilityPolicyVersion}
	if err := m.storeDB.Save(&live).Error; err != nil {
		t.Fatalf("写实时游标失败: %v", err)
	}

	if err := m.processIncidentWindow(context.Background(), core, ingest,
		from, day, reviewRegBase+1800); err != nil {
		t.Fatalf("处理失败: %v", err)
	}
	key, _ := incidentKeyForFacts(reviewRegFacts())
	after := reviewRegState(t, m, key.DedupKey(), day)
	if after.WindowState != incidentWindowStateGapObserved {
		t.Errorf("实时覆盖已越过缺失日，应判 gap_observed，实际 %s", after.WindowState)
	}
	if after.DataComplete {
		t.Error("确认缺口的窗口 data_complete 必须为 false")
	}
}

// 4. 已定稿缺口补齐后只记修订，不得重新进引擎建单。
func TestIncidentReviewGapBackfillDoesNotCreateRetroactiveIncident(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 缺口窗：无活跃事故 → 引擎 skipped，无归属。
	d := reviewRegDecision(t, core, ingest, reviewRegBase, true)
	now := reviewRegBase + 300 + int64(ingest.FinalizeLagSeconds)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if row.ProcessState != incidentProcessDone || row.IncidentID != "" {
		t.Fatalf("前置条件：应为无归属的已定稿缺口，实际 %s/%q", row.ProcessState, row.IncidentID)
	}

	// 补齐：90% 异常。
	reviewRegSeedFacts(t, m, reviewRegBase, 10, 90)
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)
	if err := m.advanceOneIncidentWindow(ctx, core, ingest, row, now+60); err != nil {
		t.Fatalf("推进失败: %v", err)
	}

	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 0 {
		t.Errorf("补齐不得追溯建单，实际新增 %d 条历史事故", incidents)
	}
	after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	// 原判定依据不回改。
	if after.WindowState != incidentWindowStateGapObserved {
		t.Errorf("原判定依据应保持 gap_observed，实际 %s", after.WindowState)
	}
	// 最新实测值另存。
	if after.MeasuredSampleTotal == nil || *after.MeasuredSampleTotal != 100 {
		t.Errorf("最新实测样本数应为 100，实际 %v", after.MeasuredSampleTotal)
	}
	if after.MeasuredWindowState != incidentWindowStateAnomaly {
		t.Errorf("最新实测窗口态应为 anomalous，实际 %q", after.MeasuredWindowState)
	}
	// 提示人工复核，不自动定级。
	if after.PendingReason != incidentPendingReasonGapBackfilledAnomalous {
		t.Errorf("应记 gap_backfilled_anomalous 待人工复核，实际 %q", after.PendingReason)
	}
	var revs int64
	m.storeDB.Model(&IncidentIngestRevision{}).
		Where("dedup_key = ? AND kind = ?", d.DedupKey, "gap_backfilled_anomalous").Count(&revs)
	if revs == 0 {
		t.Error("必须留下 gap_backfilled_anomalous 修订记录")
	}
}

// 5a. 来源无匹配行时必须保留原聚合键，完整覆盖的无样本窗要能正常结束。
//
// 原缺陷：查不到原键就用零值 facts 重新算键，得到 unknown/空模型/空分组的
// 新键，于是原窗只留 key_mismatch，无法结束为 no_sample。
func TestIncidentReviewMissingFactsRetainOriginalKey(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	row := windowState(d.DedupKey, d.WindowFromTs, d.WindowToTs,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	row.CanonicalKey = d.Canonical
	seedWindowState(t, m, row)
	// 覆盖完整，但来源里**没有**该键的任何行。
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)

	if err := m.advanceOneIncidentWindow(context.Background(), core, ingest,
		row, reviewRegBase+1800); err != nil {
		t.Fatalf("推进失败: %v", err)
	}
	after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if after.WindowState != incidentWindowStateNoSample {
		t.Errorf("完整覆盖且无样本应结束为 no_sample，实际 %s", after.WindowState)
	}
	if after.ProcessState != incidentProcessDone || !after.DataComplete {
		t.Errorf("应流程终结且数据完整，实际 %s/%v", after.ProcessState, after.DataComplete)
	}
	// 不得产生 key_mismatch 修订（那意味着身份被弄丢了）。
	var mismatch int64
	m.storeDB.Model(&IncidentIngestRevision{}).
		Where("kind = ?", "key_mismatch").Count(&mismatch)
	if mismatch != 0 {
		t.Errorf("来源消失不得改变事故身份，实际留下 %d 条 key_mismatch", mismatch)
	}
}

// 5b. 到期缺口终结必须归属原事故。
func TestIncidentReviewExpiredGapBelongsToOriginalIncident(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 先建一个有归属的事故。
	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, reviewRegBase+1800); err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	original := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if original.IncidentID == "" {
		t.Fatalf("前置条件：应建出事故，实际 action=%s", original.Action)
	}

	// 同键的后续窗口一直等不到来源，过补采期后到期终结。
	row := windowState(d.DedupKey, reviewRegBase+300, reviewRegBase+600,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	row.CanonicalKey = d.Canonical
	seedWindowState(t, m, row)
	if err := m.advanceOneIncidentWindow(ctx, core, ingest, row,
		row.WindowToTs+8*86400); err != nil {
		t.Fatalf("推进失败: %v", err)
	}

	after := reviewRegState(t, m, row.DedupKey, row.WindowToTs)
	if after.IncidentID != original.IncidentID || after.IncidentID == "" {
		t.Errorf("到期缺口应推进原事故 %s，实际 %q（action=%s）",
			original.IncidentID, after.IncidentID, after.Action)
	}
	if after.DataComplete {
		t.Error("到期缺口的 data_complete 必须为 false")
	}
	if !after.GapRecheckClosed {
		t.Error("到期应标 gap_recheck_closed（能力边界）")
	}
}

// 6. 迟到修订要比较完整输入：总数不变但异常构成变化也必须被发现。
func TestIncidentReviewAnomalyOnlyRevisionIsDetected(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, reviewRegBase+1800); err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if row.SampleTotal == nil || *row.SampleTotal != 100 {
		t.Fatalf("前置条件：判定快照样本数应为 100，实际 %v", row.SampleTotal)
	}

	// 总数仍是 100，但异常数 90 → 10。
	reviewRegSeedFacts(t, m, reviewRegBase, 90, 10)
	seedDayCoverage(t, m, reviewRegBase, reviewRegBase+300)
	if err := m.recheckOneIncidentWindow(ctx, core, ingest, row, reviewRegBase+1900); err != nil {
		t.Fatalf("复核失败: %v", err)
	}

	var revs []IncidentIngestRevision
	m.storeDB.Where("dedup_key = ? AND kind = ?", d.DedupKey, "revised_after_finalize").
		Find(&revs)
	if len(revs) == 0 {
		t.Fatal("总数 100 不变、异常 90→10 必须被记为修订（只比总数会漏报）")
	}
	rv := revs[0]
	if rv.OldAnomalyTotal == nil || *rv.OldAnomalyTotal != 90 {
		t.Errorf("应记录原异常数 90，实际 %v", rv.OldAnomalyTotal)
	}
	if rv.NewAnomalyTotal == nil || *rv.NewAnomalyTotal != 10 {
		t.Errorf("应记录新异常数 10，实际 %v", rv.NewAnomalyTotal)
	}

	// 原判定快照不得被回改；最新实测值另存。
	after := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if after.AnomalyTotal == nil || *after.AnomalyTotal != 90 {
		t.Errorf("原判定依据的异常数不得回改，应仍为 90，实际 %v", after.AnomalyTotal)
	}
	if after.MeasuredAnomalyTotal == nil || *after.MeasuredAnomalyTotal != 10 {
		t.Errorf("最新实测异常数应为 10，实际 %v", after.MeasuredAnomalyTotal)
	}
}

// 7a. 清理窗口不得抹掉存续事故的待确认结论。
func TestIncidentReviewPrunePreservesPendingScenario(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, reviewRegBase+1800); err != nil {
		t.Fatalf("提交失败: %v", err)
	}
	row := reviewRegState(t, m, d.DedupKey, d.WindowToTs)
	if row.IncidentID == "" {
		t.Fatalf("前置条件：应有事故归属")
	}
	pending, _, err := m.incidentScenarioPendingByID(ctx, row.IncidentID)
	if err != nil || !pending {
		t.Fatalf("前置条件：应为待确认，实际 %v（err=%v）", pending, err)
	}

	// 过保留期清理：窗口行被删，结论必须仍在。
	if err := m.pruneIncidentIngest(ctx, reviewRegBase+10*86400); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	pending, reason, err := m.incidentScenarioPendingByID(ctx, row.IncidentID)
	if err != nil {
		t.Fatalf("读待确认状态失败: %v", err)
	}
	if !pending {
		t.Error("存续事故的 scenario_pending 不得因窗口清理变成 false")
	}
	if reason != incidentPendingReasonNoFallback {
		t.Errorf("待确认原因应保留，实际 %q", reason)
	}
}

// 7b. 归档不得抹掉不完整语义。
func TestIncidentReviewArchiveKeepsPartialFlag(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	// 一个有样本、一个是缺口（样本为 nil）→ 混合，Partial 必须为 true。
	d1 := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	d2 := reviewRegDecision(t, core, ingest, reviewRegBase+300, true)
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := m.writeIncidentImpactWindows(tx, "reg-inc", "reg-seg", d1, reviewRegBase+1800); err != nil {
			return err
		}
		return m.writeIncidentImpactWindows(tx, "reg-inc", "reg-seg", d2, reviewRegBase+1800)
	}); err != nil {
		t.Fatalf("写影响面失败: %v", err)
	}
	var before IncidentImpact
	if err := m.storeDB.Where("incident_id = ? AND dimension = ?",
		"reg-inc", incidentImpactDimModel).Take(&before).Error; err != nil {
		t.Fatalf("读汇总失败: %v", err)
	}
	if !before.Partial {
		t.Fatal("前置条件：已知样本与缺口混合时 Partial 应为 true")
	}

	// 归档后明细被删，不完整语义必须由归档行继承。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.archiveIncidentImpactWindows(tx, "reg-inc", reviewRegBase+600, reviewRegBase+10*86400)
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	var after IncidentImpact
	if err := m.storeDB.Where("incident_id = ? AND dimension = ?",
		"reg-inc", incidentImpactDimModel).Take(&after).Error; err != nil {
		t.Fatalf("读汇总失败: %v", err)
	}
	if !after.Partial {
		t.Errorf("归档不得把 Partial 抹成 false（coverage_complete=%v，字段语义会自相矛盾）",
			after.CoverageComplete)
	}
}

// 8. 接口不得把查询失败显示成零。
func TestIncidentReviewHealthReportsQueryFailure(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	seedWindowState(t, m, windowState("reg-pending", reviewRegBase, reviewRegBase+300,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false))

	const cb = "reg:fail_health_counts"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(cb, func(tx *gorm.DB) {
		if tx.Statement.Table == "incident_ingest_window_states" {
			_ = tx.AddError(fmt.Errorf("注入的计数读取失败"))
		}
	}); err != nil {
		t.Fatalf("注册回调失败: %v", err)
	}
	defer func() { _ = m.storeDB.Callback().Query().Remove(cb) }()

	rec := httptest.NewRecorder()
	gin.SetMode(gin.TestMode)
	c, _ := gin.CreateTestContext(rec)
	c.Request = httptest.NewRequest("GET", "/incidents/health", nil)
	m.serveIncidentIngestHealth(c)

	if rec.Code == 200 {
		t.Fatalf("计数查询失败时不得返回 200，实际 body=%s", rec.Body.String())
	}
	var body map[string]any
	if err := json.Unmarshal(rec.Body.Bytes(), &body); err != nil {
		t.Fatalf("解析响应失败: %v", err)
	}
	if status, _ := body["data_status"].(string); status != "unavailable" {
		t.Errorf("应显式标为 unavailable，实际 %q", status)
	}
	if field, _ := body["failed_field"].(string); field == "" {
		t.Error("应指明哪个子查询失败")
	}
}

// 9a. 并发闸门必须真正执行配置容量。
func TestIncidentReviewQueryGateHonorsCapacity(t *testing.T) {
	cfg := DefaultIncidentConfig()
	cfg.QueryGateCapacity = 1
	cfg.GateTimeoutSeconds = 0 // 走默认，但下面用 ctx 超时控制

	ctx, cancel := context.WithTimeout(context.Background(), 80*time.Millisecond)
	defer cancel()
	release1, ok := acquireIncidentQueryGate(ctx, cfg)
	if !ok {
		t.Fatal("第一个查询应获准")
	}
	defer release1()

	release2, ok := acquireIncidentQueryGate(ctx, cfg)
	if ok {
		release2()
		t.Fatal("容量配成 1 时不得同时放进两个查询")
	}

	// 释放后应能再次获准，证明不是永久卡死。
	release1()
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Second)
	defer cancel2()
	release3, ok := acquireIncidentQueryGate(ctx2, cfg)
	if !ok {
		t.Fatal("释放后应可再次获准")
	}
	release3()
}

// 9b. 列表与分段要能真正翻页，且相同时间戳排序稳定。
func TestIncidentReviewListAndSegmentPagination(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	// 5 条事故，其中 3 条 last_seen_ts 完全相同——考验稳定排序。
	ids := []string{"i1", "i2", "i3", "i4", "i5"}
	for i, id := range ids {
		ts := int64(1000)
		if i >= 3 {
			ts = int64(2000 + i)
		}
		if err := m.storeDB.Save(&Incident{
			ID: id, DedupKey: "dk-" + id, ActiveMarker: int64(incidentActiveMarker + i),
			Status: "open", PrimaryFaultClass: "unknown",
			FirstSeenTs: 1, LastSeenTs: ts,
		}).Error; err != nil {
			t.Fatalf("写事故失败: %v", err)
		}
	}

	// 每页 2 条，逐页遍历必须不重不漏地取到全部 5 条。
	seen := map[string]int{}
	cursorParam := ""
	for page := 0; page < 10; page++ {
		path := "/incidents?limit=2"
		if cursorParam != "" {
			path += "&cursor=" + cursorParam
		}
		body := doIncidentRequest(t, m, path, m.serveIncidentList)
		items, _ := body["incidents"].([]any)
		for _, raw := range items {
			it := raw.(map[string]any)
			seen[it["id"].(string)]++
		}
		more, _ := body["has_more"].(bool)
		next, _ := body["next_cursor"].(string)
		if !more {
			break
		}
		if next == "" {
			t.Fatal("has_more=true 时必须给出 next_cursor，否则无法遍历全部记录")
		}
		cursorParam = next
	}
	if len(seen) != len(ids) {
		t.Errorf("逐页遍历应取到全部 %d 条，实际 %d 条: %v", len(ids), len(seen), seen)
	}
	for id, n := range seen {
		if n != 1 {
			t.Errorf("事故 %s 被取到 %d 次，翻页必须不重复", id, n)
		}
	}

	// 分段翻页。
	for seq := int64(1); seq <= 5; seq++ {
		if err := m.storeDB.Save(&IncidentSegment{
			ID: fmt.Sprintf("seg-%d", seq), IncidentID: "i1", SeqNo: seq,
			StartTs: seq * 100,
		}).Error; err != nil {
			t.Fatalf("写分段失败: %v", err)
		}
	}
	segSeen := map[string]int{}
	segCursor := ""
	for page := 0; page < 10; page++ {
		path := "/incidents/detail?id=i1&limit=2"
		if segCursor != "" {
			path += "&segments_cursor=" + segCursor
		}
		body := doIncidentRequest(t, m, path, m.serveIncidentDetail)
		segs, _ := body["segments"].([]any)
		for _, raw := range segs {
			s := raw.(map[string]any)
			segSeen[fmt.Sprintf("%v", s["ID"])]++
		}
		more, _ := body["segments_has_more"].(bool)
		if !more {
			break
		}
		next, _ := body["segments_next_cursor"].(float64)
		if next == 0 {
			t.Fatal("segments_has_more=true 时必须给出 segments_next_cursor")
		}
		segCursor = fmt.Sprintf("%d", int64(next))
	}
	if len(segSeen) != 5 {
		t.Errorf("分段逐页遍历应取到 5 条，实际 %d 条: %v", len(segSeen), segSeen)
	}
}

// 留痕缺口 1：来源过期必须真的被标记。
func TestIncidentReviewSourceFactsExpiredIsMarked(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	row := windowState("reg-expire", reviewRegBase, reviewRegBase+300,
		incidentProcessFailedPermanent, incidentWindowStateAnomaly, false)
	row.ReplayableUntil = reviewRegBase + 600
	seedWindowState(t, m, row)

	// 未到期：不标。
	if err := m.markExpiredIncidentSourceFacts(ctx, reviewRegBase+400); err != nil {
		t.Fatalf("标记失败: %v", err)
	}
	if got := reviewRegState(t, m, "reg-expire", reviewRegBase+300); got.SourceFactsExpired {
		t.Error("未到重放期不得标记来源过期")
	}
	// 已到期：必须标，且记录不删。
	if err := m.markExpiredIncidentSourceFacts(ctx, reviewRegBase+700); err != nil {
		t.Fatalf("标记失败: %v", err)
	}
	got := reviewRegState(t, m, "reg-expire", reviewRegBase+300)
	if !got.SourceFactsExpired {
		t.Error("过重放期必须标记 source_facts_expired，不能恒为 false")
	}
	if got.ProcessState != incidentProcessFailedPermanent {
		t.Errorf("只标记，不得改判定，实际 %s", got.ProcessState)
	}
}

// 留痕缺口 2：已归档窗口重放必须留下 replay_after_archive 修订。
func TestIncidentReviewReplayAfterArchiveIsRecorded(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	d := reviewRegDecision(t, core, ingest, reviewRegBase, false)
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, "reg-inc2", "reg-seg", d, reviewRegBase+1800)
	}); err != nil {
		t.Fatalf("写影响面失败: %v", err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.archiveIncidentImpactWindows(tx, "reg-inc2", d.WindowToTs, reviewRegBase+10*86400)
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	before := impactSampleTotal(t, m, "reg-inc2", incidentImpactDimModel, d.Key.Model)

	// 重放已归档窗口。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, "reg-inc2", "reg-seg", d, reviewRegBase+2000)
	}); err != nil {
		t.Fatalf("重放失败: %v", err)
	}
	after := impactSampleTotal(t, m, "reg-inc2", incidentImpactDimModel, d.Key.Model)
	if after != before {
		t.Errorf("重放不得改变汇总：%d → %d", before, after)
	}
	// 关键：必须有审计留痕，不能只靠「计数没翻倍」。
	var n int64
	m.storeDB.Model(&IncidentIngestRevision{}).
		Where("dedup_key = ? AND kind = ?", d.DedupKey, "replay_after_archive").Count(&n)
	if n == 0 {
		t.Error("已归档窗口重放必须写 replay_after_archive 修订")
	}
	// 反复重放只留一条，不把审计表刷爆。
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, "reg-inc2", "reg-seg", d, reviewRegBase+2100)
	}); err != nil {
		t.Fatalf("二次重放失败: %v", err)
	}
	m.storeDB.Model(&IncidentIngestRevision{}).
		Where("dedup_key = ? AND kind = ?", d.DedupKey, "replay_after_archive").Count(&n)
	if n != 1 {
		t.Errorf("同一窗口反复重放应只留一条留痕，实际 %d 条", n)
	}
}
