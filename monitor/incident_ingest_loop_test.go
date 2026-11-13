package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"
)

// 核心关闭时：循环不启动、不扫描、不写任何游标。
func TestIncidentIngestWritesNothingWhenCoreDisabled(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "false")
	t.Setenv("MONITOR_INCIDENT_INGEST_ENABLED", "true")
	m := openIncidentTestStore(t)

	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	done := make(chan struct{})
	go func() {
		m.startIncidentIngest(ctx)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("核心关闭时 startIncidentIngest 必须立即返回")
	}

	var cursors, states int64
	m.storeDB.Model(&IncidentIngestCursor{}).Count(&cursors)
	m.storeDB.Model(&IncidentIngestWindowState{}).Count(&states)
	if cursors != 0 {
		t.Errorf("核心关闭时不得写游标，实际 %d 行", cursors)
	}
	if states != 0 {
		t.Errorf("核心关闭时不得写窗口状态，实际 %d 行", states)
	}
}

// 引擎返回 skipped_disabled 绝不当成功消费。
func TestIncidentSkippedDisabledIsNotSuccess(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(1_500_000) / winSec * winSec
	facts := incidentWindowFacts{ChannelID: 606, ModelName: "m", Grp: "g",
		Success: 10, Anomaly: 90}
	d, err := decideIncidentWindow(core, ingest, facts, true,
		coverageComplete(base, base+winSec), base+10*winSec, base, base+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}

	// 处理过程中核心被关闭：引擎返回 skipped_disabled。
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "false")
	err = m.commitIncidentWindowDecision(ctx, core, ingest, d, base+100)
	if err == nil {
		t.Fatal("核心关闭时必须报错，不得把 skipped_disabled 当成功消费")
	}

	var row IncidentIngestWindowState
	if e := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		d.DedupKey, d.WindowToTs).Take(&row).Error; e != nil {
		t.Fatalf("读状态失败: %v", e)
	}
	// 必须停在 input_frozen，下轮重来；绝不能标 done 或 data_complete。
	if row.ProcessState != incidentProcessInputFrozen {
		t.Errorf("应停在 input_frozen 以便下轮重试，实际 %s", row.ProcessState)
	}
	if row.DataComplete {
		t.Error("未真正处理的窗口 data_complete 必须为 false")
	}
}

// 循环必须响应 ctx 取消。
func TestIncidentIngestLoopIsCancellable(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		m.runIncidentIngestLoop(ctx)
		close(done)
	}()
	time.Sleep(200 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("ctx 取消后循环必须及时返回")
	}
}

// 单窗口失败不阻断其他窗口，失败状态持久化且重启后仍在。
func TestIncidentWindowFailureIsIsolatedAndPersisted(t *testing.T) {
	_, ingest := incidentIngestTestEnv(t)
	ingest.MaxAttempts = 2
	m := openIncidentTestStore(t)
	ctx := context.Background()

	row := windowState("dk-fail", 0, 300, incidentProcessInputFrozen,
		incidentWindowStateAnomaly, false)
	row.CreatedAt = 100

	// 第一次失败 → failed，带退避。
	if err := m.markIncidentWindowFailure(ctx, row, errTestInjected, ingest, 1000); err != nil {
		t.Fatalf("记录失败状态出错: %v", err)
	}
	var after1 IncidentIngestWindowState
	m.storeDB.Where("dedup_key = ?", "dk-fail").Take(&after1)
	if after1.ProcessState != incidentProcessFailed {
		t.Errorf("首次失败应为 failed，实际 %s", after1.ProcessState)
	}
	if after1.NextRetryAt <= 1000 {
		t.Error("失败后必须设置退避时刻")
	}
	if after1.LastError == "" {
		t.Error("失败原因必须持久化，不能只记日志")
	}

	// 达上限 → failed_permanent，且 data_complete 仍为 false。
	if err := m.markIncidentWindowFailure(ctx, after1, errTestInjected, ingest, 2000); err != nil {
		t.Fatalf("记录失败状态出错: %v", err)
	}
	var after2 IncidentIngestWindowState
	m.storeDB.Where("dedup_key = ?", "dk-fail").Take(&after2)
	if after2.ProcessState != incidentProcessFailedPermanent {
		t.Errorf("达上限应为 failed_permanent，实际 %s", after2.ProcessState)
	}
	if after2.DataComplete {
		t.Error("永久失败的 data_complete 必须为 false")
	}
	// 重启后记录仍在（真实重开同一文件在迁移测试里覆盖；这里断言未被清理）。
	if err := m.pruneIncidentIngest(ctx, 3000); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	var left int64
	m.storeDB.Model(&IncidentIngestWindowState{}).Where("dedup_key = ?", "dk-fail").Count(&left)
	if left != 1 {
		t.Error("未过补齐期的永久失败记录不得被清理")
	}
}

// 无法归属的硬错误单独归类且不重试。
func TestIncidentUnattributableIsNotRetried(t *testing.T) {
	_, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	row := windowState("dk-unattr", 0, 300, incidentProcessInputFrozen,
		incidentWindowStateAnomaly, false)
	row.CreatedAt = 100
	if err := m.markIncidentWindowFailure(ctx, row, errTestUnattributable, ingest, 1000); err != nil {
		t.Fatalf("记录失败出错: %v", err)
	}
	var after IncidentIngestWindowState
	m.storeDB.Where("dedup_key = ?", "dk-unattr").Take(&after)
	if after.ProcessState != incidentProcessUnattributable {
		t.Errorf("无法归属应单独归类，实际 %s", after.ProcessState)
	}
	if after.Attempts != 0 {
		t.Errorf("无法归属不得进入重试计数，实际 attempts=%d", after.Attempts)
	}
	if after.NextRetryAt != 0 {
		t.Error("无法归属不得设置重试时刻")
	}
	// 它也不占待处理队列。
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if backlog != 0 {
		t.Errorf("无法归属不得占待处理队列，实际 %d", backlog)
	}
}

// 已处理的缺口窗口仍在推进队列中，补齐后完成闭环。
func TestIncidentGapObservedStaysInQueueAndBackfills(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(1_600_000) / winSec * winSec
	key, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 707, ModelName: "gpt-4o", Grp: "default"})

	// 一个已处理完的缺口窗（done + data_complete=false）。
	row := windowState(key.DedupKey(), base, base+winSec,
		incidentProcessDone, incidentWindowStateGapObserved, false)
	row.CanonicalKey = key.Canonical()
	row.CreatedAt = base
	row.FinalizedAt = base
	seedWindowState(t, m, row)

	// 它必须仍被算作待推进（否则与「持续复核 7 天」矛盾）。
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if backlog != 1 {
		t.Fatalf("已处理但未补齐的缺口窗必须仍在待推进队列，实际 %d", backlog)
	}

	// 缺口补齐：写入事实与完整覆盖凭证。
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: base, UserID: 1, ChannelID: 707, ModelName: "gpt-4o", Grp: "default",
		Success: 70, Anomaly: 30,
	})
	seedDayCoverage(t, m, base, base+winSec)

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, BacklogMode: "normal"}
	now := base + winSec + int64(ingest.FinalizeLagSeconds) + 60
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatalf("推进失败: %v", err)
	}

	var after IncidentIngestWindowState
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		key.DedupKey(), base+winSec).Take(&after).Error; err != nil {
		t.Fatalf("读状态失败: %v", err)
	}
	if !after.DataComplete {
		t.Errorf("补齐后 data_complete 应为 true，实际 false（state=%s/%s err=%q）",
			after.ProcessState, after.WindowState, after.LastError)
	}
	// ★ 原判定依据不回改，最新实测值另存 ★
	//
	// WindowState 保持 gap_observed：那是当时证据下的判定依据。补齐后实测
	// 到的窗口态记在 MeasuredWindowState，供展示「最新实测」。
	if after.WindowState != incidentWindowStateGapObserved {
		t.Errorf("原判定依据不得回改，WindowState 应仍为 gap_observed，实际 %s", after.WindowState)
	}
	if after.MeasuredWindowState == "" || after.MeasuredWindowState == incidentWindowStateGapObserved {
		t.Errorf("补齐后的实测窗口态应另存于 MeasuredWindowState，实际 %q", after.MeasuredWindowState)
	}
	if after.MeasuredSampleTotal == nil || *after.MeasuredSampleTotal != 100 {
		t.Errorf("最新实测样本数应为 100，实际 %v", after.MeasuredSampleTotal)
	}
	// 必须留下补齐修订记录。本例 30/100 = 30% 超过 20% 阈值，所以实测为
	// anomalous，按约定记 gap_backfilled_anomalous 提示人工复核，
	// 且**不自动追溯建单/定级**。
	var revs []IncidentIngestRevision
	m.storeDB.Where("dedup_key = ? AND kind IN ?", key.DedupKey(),
		[]string{"gap_backfilled", "gap_backfilled_anomalous"}).Find(&revs)
	if len(revs) == 0 {
		t.Fatal("缺口补齐必须写 gap_backfilled* 修订记录")
	}
	if revs[0].Kind != "gap_backfilled_anomalous" {
		t.Errorf("补齐后实测异常应记 gap_backfilled_anomalous，实际 %q", revs[0].Kind)
	}
	// 异常构成必须留痕，不能只记总数。
	if revs[0].NewAnomalyTotal == nil || *revs[0].NewAnomalyTotal != 30 {
		t.Errorf("修订应记录异常数 30，实际 %v", revs[0].NewAnomalyTotal)
	}
	// 不得追溯建单：该窗原本无事故归属。
	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 0 {
		t.Errorf("缺口补齐不得追溯建单，实际新增 %d 条事故", incidents)
	}
	// 待人工复核的显式原因。
	if after.PendingReason != incidentPendingReasonGapBackfilledAnomalous {
		t.Errorf("应显式记录待复核原因，实际 %q", after.PendingReason)
	}
	// 完整水位随之推进。
	cursor.ScanThroughTs = base + 10*winSec
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatalf("算水位失败: %v", err)
	}
	if cursor.CompleteThroughTs <= base {
		t.Errorf("补齐后完整水位应推进，实际 %d", cursor.CompleteThroughTs)
	}
}

// 迟到修订复核：范围内被发现并记录；超范围标 recheck_closed 后不再复核。
func TestIncidentRecheckBoundedAndClosable(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ingest.RecheckMaxAttempts = 2
	ingest.RecheckWindowSeconds = 7200
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	now := int64(2_000_000)
	inRange := (now - 3600) / winSec * winSec
	outRange := (now - 100_000) / winSec * winSec

	key, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 808, ModelName: "gpt-4o", Grp: "default"})

	mk := func(from int64, sample int64) IncidentIngestWindowState {
		r := windowState(key.DedupKey(), from, from+winSec,
			incidentProcessDone, incidentWindowStateNormal, true)
		r.CanonicalKey = key.Canonical()
		r.CreatedAt = from
		r.FinalizedAt = from + winSec
		s := sample
		r.SampleTotal = &s
		return r
	}
	seedWindowState(t, m, mk(inRange, 50))
	seedWindowState(t, m, mk(outRange, 50))

	// 来源数据变化（迟到）。
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: inRange, UserID: 1, ChannelID: 808, ModelName: "gpt-4o", Grp: "default",
		Success: 80, Anomaly: 20,
	})

	// This test concerns a complete late revision, so provide its coverage proof.
	seedDayCoverage(t, m, inRange, inRange+winSec)

	if err := m.recheckIncidentFinalizedWindows(ctx, core, ingest, now); err != nil {
		t.Fatalf("复核失败: %v", err)
	}

	// 范围内：修订被发现并记录，且带发现延迟。
	var revs []IncidentIngestRevision
	m.storeDB.Where("kind = ?", "revised_after_finalize").Find(&revs)
	if len(revs) == 0 {
		t.Fatal("范围内的迟到修订必须被发现并记录")
	}
	if revs[0].RevisionDetectLagSeconds <= 0 {
		t.Error("应记录修订发现延迟")
	}
	if revs[0].NewSampleTotal == nil || *revs[0].NewSampleTotal != 100 {
		t.Errorf("修订应记录新值 100，实际 %v", revs[0].NewSampleTotal)
	}

	// 超范围：标 recheck_closed（能力边界），不再复核。
	var out IncidentIngestWindowState
	m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		key.DedupKey(), outRange+winSec).Take(&out)
	if !out.RecheckClosed {
		t.Error("超出复核范围的窗口应标 recheck_closed")
	}
	// 它仍是完整数据，不进积压、不阻挡水位。
	if !out.DataComplete {
		t.Error("recheck_closed 是能力边界，不得把 data_complete 改成 false")
	}
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if backlog != 0 {
		t.Errorf("recheck_closed 不得计入积压，实际 %d", backlog)
	}
}

var (
	errTestInjected = &incidentHTTPError{"注入的处理失败"}
	// 必须用引擎真实返回的 sentinel 包装，不能自造一段「看起来像」的文本。
	// 旧实现靠 strings.Contains(err, "无法归属") 判定，而真实错误是
	// ErrObservationUnattributed（「观测窗口无法确定归属分段」），
	// 根本不含该子串——用自造文本的测试正好把这个缺陷掩盖掉了。
	errTestUnattributable = fmt.Errorf("提交观测失败: %w", ErrObservationUnattributed)
)
