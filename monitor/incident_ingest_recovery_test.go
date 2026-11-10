package monitor

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

// ★ 引擎已提交、engine_done 未保存就崩溃 ★
//
// 恢复必须回到同一事故、同一窗口归属，且用**原判定输入**补齐记录——
// 不能把期间变化的来源数据冒充原判定依据。
func TestIncidentRecoveryAfterEngineCommitUsesSnapshot(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(600_000) / winSec * winSec
	facts := incidentWindowFacts{ChannelID: 11, ModelName: "gpt-4o", Grp: "default",
		Success: 60, Anomaly: 40}

	d, err := decideIncidentWindow(core, ingest, facts, true,
		coverageComplete(base, base+winSec), base+10*winSec, base, base+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}

	// 模拟崩溃点：先落 input_frozen + 快照，再调引擎成功，但**不写** engine_done。
	frozen := incidentWindowStateRowFrom(d, 7, base)
	frozen.ProcessState = incidentProcessInputFrozen
	frozen.FinalizedAt = base
	if err := m.saveIncidentWindowState(ctx, frozen); err != nil {
		t.Fatalf("落冻结状态失败: %v", err)
	}
	res, err := m.IngestIncidentObservation(ctx, d.observationInputFor(core.MinSample))
	if err != nil {
		t.Fatalf("引擎提交失败: %v", err)
	}
	originalIncident, originalSegment := res.IncidentID, res.SegmentID
	if originalIncident == "" {
		t.Fatal("引擎应建出事故")
	}
	obsBefore := incidentObservationCount(t, m, originalIncident)

	// 崩溃后来源数据被改动（补采/回放），恢复绝不能用新值。
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: base, UserID: 99, ChannelID: 11, ModelName: "gpt-4o", Grp: "default",
		Success: 9999, Anomaly: 1,
	})

	// 重启恢复。
	var stored IncidentIngestWindowState
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		d.DedupKey, d.WindowToTs).Take(&stored).Error; err != nil {
		t.Fatalf("读状态失败: %v", err)
	}
	if stored.ProcessState != incidentProcessInputFrozen {
		t.Fatalf("前置条件：状态应为 input_frozen，实际 %s", stored.ProcessState)
	}
	if err := m.recoverIncidentFrozenWindow(ctx, core, ingest, stored, base+100); err != nil {
		t.Fatalf("崩溃恢复失败: %v", err)
	}

	// 1. 必须回到同一事故、同一分段，不新建。
	var after IncidentIngestWindowState
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		d.DedupKey, d.WindowToTs).Take(&after).Error; err != nil {
		t.Fatalf("读状态失败: %v", err)
	}
	if after.IncidentID != originalIncident {
		t.Errorf("应恢复到同一事故 %s，实际 %s", originalIncident, after.IncidentID)
	}
	if after.SegmentID != originalSegment {
		t.Errorf("应恢复到同一分段 %s，实际 %s", originalSegment, after.SegmentID)
	}
	if after.ProcessState != incidentProcessDone {
		t.Errorf("恢复后应终结为 done，实际 %s", after.ProcessState)
	}
	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 1 {
		t.Errorf("恢复不得新建事故，实际共 %d 条", incidents)
	}
	if got := incidentObservationCount(t, m, originalIncident); got != obsBefore {
		t.Errorf("恢复不得多出观测行: %d → %d", obsBefore, got)
	}

	// 2. ★ 必须用快照原值，不得用改动后的来源数据 ★
	if after.SampleTotal == nil || *after.SampleTotal != 100 {
		t.Errorf("恢复必须使用原判定输入快照（100），不得冒充为改动后的新值，实际 %v",
			after.SampleTotal)
	}
	// 影响面同样用原值。
	if got := impactSampleTotal(t, m, originalIncident, incidentImpactDimChannel, "11"); got != 100 {
		t.Errorf("影响面应按快照原值 100 写入，实际 %d", got)
	}
}

// 引擎未提交就崩溃：恢复用快照重放，仍只产生一个事故。
func TestIncidentRecoveryReplaysWhenEngineNeverCommitted(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(700_000) / winSec * winSec
	facts := incidentWindowFacts{ChannelID: 12, ModelName: "gpt-4o", Grp: "default",
		Success: 30, Anomaly: 70}
	d, err := decideIncidentWindow(core, ingest, facts, true,
		coverageComplete(base, base+winSec), base+10*winSec, base, base+winSec)
	if err != nil {
		t.Fatalf("判定失败: %v", err)
	}

	frozen := incidentWindowStateRowFrom(d, 7, base)
	frozen.ProcessState = incidentProcessInputFrozen
	frozen.FinalizedAt = base
	if err := m.saveIncidentWindowState(ctx, frozen); err != nil {
		t.Fatalf("落冻结状态失败: %v", err)
	}
	// 引擎从未被调用。恢复应重放。
	if err := m.recoverIncidentFrozenWindow(ctx, core, ingest, frozen, base+100); err != nil {
		t.Fatalf("恢复失败: %v", err)
	}
	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 1 {
		t.Errorf("应重放出 1 个事故，实际 %d", incidents)
	}
	var after IncidentIngestWindowState
	m.storeDB.Where("dedup_key = ? AND window_to_ts = ?", d.DedupKey, d.WindowToTs).Take(&after)
	if after.ProcessState != incidentProcessDone || after.IncidentID == "" {
		t.Errorf("恢复后应 done 且有事故归属，实际 %s/%q", after.ProcessState, after.IncidentID)
	}

	// 重复恢复不得产生第二个事故或重复观测。
	obs := incidentObservationCount(t, m, after.IncidentID)
	if err := m.recoverIncidentFrozenWindow(ctx, core, ingest, after, base+200); err != nil {
		t.Fatalf("二次恢复失败: %v", err)
	}
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 1 {
		t.Errorf("二次恢复不得新建事故，实际 %d", incidents)
	}
	if got := incidentObservationCount(t, m, after.IncidentID); got != obs {
		t.Errorf("二次恢复不得多出观测行: %d → %d", obs, got)
	}
}

// ★ 等待来源的窗口重启后绝不进引擎 ★
//
// wait_coverage / mixed_version 也是未终结状态，但它们是「等来源满足条件」，
// 不是「输入已冻结待提交」。混在一起会绕过前面的禁止调用条件。
func TestIncidentAwaitingSourceNeverEntersEngineOnRestart(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(800_000) / winSec * winSec

	for i, ws := range []string{incidentWindowStateWaitCoverage, incidentWindowStateMixedVersion} {
		from := base + int64(i)*winSec
		row := windowState("kw"+ws, from, from+winSec,
			incidentProcessAwaitingSource, ws, false)
		// 等待类窗口不带快照（与 input_frozen 的区分依据）。
		row.CreatedAt = from
		seedWindowState(t, m, row)
	}

	before := int64(0)
	m.storeDB.Model(&Incident{}).Count(&before)

	// 推进队列：覆盖仍未满足、仍在 7 天补齐期内 → 只退避，不调引擎。
	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, BacklogMode: "normal"}
	now := base + winSec*2
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatalf("推进失败: %v", err)
	}

	var after int64
	m.storeDB.Model(&Incident{}).Count(&after)
	if after != before {
		t.Errorf("等待来源的窗口不得调用引擎建单: %d → %d", before, after)
	}
	var obs int64
	m.storeDB.Model(&IncidentObservation{}).Count(&obs)
	if obs != 0 {
		t.Errorf("等待来源的窗口不得产生观测行，实际 %d", obs)
	}

	// 状态必须仍是 awaiting_source，不得被误升为 input_frozen。
	var rows []IncidentIngestWindowState
	m.storeDB.Where("process_state <> ?", incidentProcessDone).Find(&rows)
	for _, r := range rows {
		if r.ProcessState == incidentProcessInputFrozen {
			t.Errorf("窗口 %s@%d 被误升为 input_frozen", r.WindowState, r.WindowToTs)
		}
		if r.SampleTotal != nil {
			t.Errorf("等待来源的窗口不得带判定输入快照，实际 %v", r.SampleTotal)
		}
	}
}

// 来源推进队列不得与迟到修订复核队列共用次数上限。
func TestIncidentSourceQueueNotLimitedByRecheckAttempts(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ingest.RecheckMaxAttempts = 6
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(900_000) / winSec * winSec

	// 用真实聚合键：假键会走 key_mismatch 分支，测不到本意。
	facts := incidentWindowFacts{ChannelID: 5, ModelName: "gpt-4o", Grp: "default",
		Success: 50, Anomaly: 50}
	key, err := incidentKeyForFacts(facts)
	if err != nil {
		t.Fatalf("映射聚合键失败: %v", err)
	}
	row := windowState(key.DedupKey(), base, base+winSec,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	row.CanonicalKey = key.Canonical()
	// 已被检查远超 RecheckMaxAttempts(6) 次，且窗口早于 2 小时复核范围。
	row.SourceCheckAttempts = 50
	row.RecheckAttempts = 99
	row.CreatedAt = base
	seedWindowState(t, m, row)

	// 让该窗口的来源条件变为满足。
	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: base, UserID: 1, ChannelID: 5, ModelName: "gpt-4o", Grp: "default",
		Success: 50, Anomaly: 50,
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
	// 关键：次数超过 RecheckMaxAttempts 且超出 2 小时复核范围后，
	// 来源队列仍把它推进到了终结，没有被另一条队列的限制掐断。
	if after.ProcessState == incidentProcessAwaitingSource {
		t.Errorf("来源条件已满足，应被推进出 awaiting_source，实际仍为 %s（attempts=%d）",
			after.ProcessState, after.SourceCheckAttempts)
	}
	if after.ProcessState != incidentProcessDone {
		t.Errorf("应推进到 done，实际 %s（last_error=%q）", after.ProcessState, after.LastError)
	}
	if after.WindowState != incidentWindowStateAnomaly {
		t.Errorf("窗口态应更新为 anomalous，实际 %s", after.WindowState)
	}
	// RecheckAttempts 不得被来源队列消耗（两条队列各用自己的计数器）。
	if after.RecheckAttempts != 99 {
		t.Errorf("来源队列不得改动迟到修订复核计数，应为 99，实际 %d", after.RecheckAttempts)
	}
}

// seedDayCoverage 用真实持久化路径写覆盖凭证。
func seedDayCoverage(t *testing.T, m *Monitor, from, through int64) {
	t.Helper()
	day := customerHealthDayStart(from)
	state := CustomerHealthSourceCursor{
		ID: 1, DayTs: day, ThroughTs: through,
		SemanticsVersion:      customerHealthStabilityPolicyVersion,
		TTFTSemanticsVersion:  ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:    day,
		TTFTCoverageThroughTs: through,
		UpdatedAt:             through,
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return m.persistCustomerHealthDayCoverageTx(tx, state)
	}); err != nil {
		t.Fatalf("写覆盖凭证失败: %v", err)
	}
}
