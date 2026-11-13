package monitor

import (
	"context"
	"testing"
)

// ★ 单个聚合键的问题不得拖住其他键 ★
func TestIncidentSchedulingFairnessAcrossKeys(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ingest.PendingBatch = 20
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(1_000_000) / winSec * winSec

	// 键 A：500 个长期无法推进的旧窗口。
	keyA, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 101, ModelName: "a", Grp: "g"})
	for i := int64(0); i < 500; i++ {
		row := windowState(keyA.DedupKey(), base+i*winSec, base+(i+1)*winSec,
			incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
		row.CanonicalKey = keyA.Canonical()
		row.CreatedAt = base
		seedWindowState(t, m, row)
	}
	// 键 B：一个新窗口。注意 dedup_key 排序上要可能排在 A 之后，
	// 轮转调度必须保证它仍被取到。
	keyB, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 202, ModelName: "b", Grp: "g"})
	rowB := windowState(keyB.DedupKey(), base, base+winSec,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	rowB.CanonicalKey = keyB.Canonical()
	rowB.CreatedAt = base
	seedWindowState(t, m, rowB)

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, BacklogMode: "normal"}
	now := base + 600*winSec
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now); err != nil {
		t.Fatalf("推进失败: %v", err)
	}

	// 键 B 必须在这一轮被处理过（计数增长或状态推进）。
	var after IncidentIngestWindowState
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		keyB.DedupKey(), base+winSec).Take(&after).Error; err != nil {
		t.Fatalf("读键 B 状态失败: %v", err)
	}
	if after.SourceCheckAttempts == 0 && after.ProcessState == incidentProcessAwaitingSource {
		t.Error("键 B 的任务被键 A 的 500 个旧窗口挤掉了：轮转调度未生效")
	}
}

// 轮转函数本身：单键不得吃满整个配额。
func TestIncidentRotatePendingRowsSpreadsAcrossKeys(t *testing.T) {
	rows := []IncidentIngestWindowState{}
	for i := 0; i < 100; i++ {
		rows = append(rows, IncidentIngestWindowState{DedupKey: "aaa", WindowToTs: int64(i)})
	}
	rows = append(rows, IncidentIngestWindowState{DedupKey: "zzz", WindowToTs: 1})

	got := incidentRotatePendingRows(rows, "", 10)
	if len(got) != 10 {
		t.Fatalf("应取 10 条，实际 %d", len(got))
	}
	sawZ := false
	for _, r := range got {
		if r.DedupKey == "zzz" {
			sawZ = true
		}
	}
	if !sawZ {
		t.Error("少量窗口的键必须也被取到，不能被大键吃满配额")
	}
}

// ★ 没有可重试 failed、只有超期等待窗口时，degraded 也必须能恢复 ★
func TestIncidentDegradedRecoversWithOnlyExpiredAwaitingSource(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	ingest.BacklogWarnRows, ingest.BacklogMaxRows = 3, 5
	ingest.PendingBatch = 100
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(1_200_000) / winSec * winSec

	// 全部是超期的 awaiting_source，零个 failed。
	key, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 303, ModelName: "m", Grp: "g"})
	for i := int64(0); i < 10; i++ {
		row := windowState(key.DedupKey(), base+i*winSec, base+(i+1)*winSec,
			incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
		row.CanonicalKey = key.Canonical()
		row.CreatedAt = base
		seedWindowState(t, m, row)
	}

	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if backlog != 10 {
		t.Fatalf("前置条件：积压应为 10，实际 %d", backlog)
	}
	var failed int64
	m.storeDB.Model(&IncidentIngestWindowState{}).
		Where("process_state = ?", incidentProcessFailed).Count(&failed)
	if failed != 0 {
		t.Fatalf("前置条件：不应有可重试 failed，实际 %d", failed)
	}
	mode := incidentBacklogMode("normal", backlog, ingest)
	if mode != "degraded" {
		t.Fatalf("超过上限应降级，实际 %s", mode)
	}

	// degraded 期间仍推进非终结行。now 已过 7 天补采窗 → 全部到期终结。
	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, BacklogMode: mode}
	now := base + 10*winSec + customerHealthHistoryDays*24*3600 + 3600
	// 同键必须按时间顺序推进，所以需要多轮。
	for round := 0; round < 15; round++ {
		if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now); err != nil {
			t.Fatalf("第 %d 轮推进失败: %v", round, err)
		}
		b, err := m.countIncidentPendingBacklog(ctx)
		if err != nil {
			t.Fatalf("统计积压失败: %v", err)
		}
		cursor.BacklogMode = incidentBacklogMode(cursor.BacklogMode, b, ingest)
		if cursor.BacklogMode == "normal" {
			break
		}
	}

	final, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if final >= int64(ingest.BacklogWarnRows) {
		t.Errorf("到期终结应把积压清到告警线以下，实际 %d", final)
	}
	if cursor.BacklogMode != "normal" {
		t.Errorf("积压消退后应自动回到 normal（不得永久空转），实际 %s", cursor.BacklogMode)
	}
	// 终结后它们是不完整的缺口，必须计入历史缺口并阻挡完整水位。
	terminal, err := m.countIncidentTerminalGaps(ctx)
	if err != nil {
		t.Fatalf("统计历史缺口失败: %v", err)
	}
	if terminal == 0 {
		t.Error("到期终结的窗口应计入历史缺口，不得当成成功")
	}
}

// 到期终结：超过 7 天补采窗的等待窗口按 gap_observed 终结并标能力边界。
func TestIncidentAwaitingSourceClosesAfterBackfillWindow(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(1_300_000) / winSec * winSec

	for _, ws := range []string{incidentWindowStateWaitCoverage, incidentWindowStateMixedVersion} {
		key, _ := incidentKeyForFacts(incidentWindowFacts{
			ChannelID: 404, ModelName: "m-" + ws, Grp: "g"})
		row := windowState(key.DedupKey(), base, base+winSec,
			incidentProcessAwaitingSource, ws, false)
		row.CanonicalKey = key.Canonical()
		row.CreatedAt = base
		seedWindowState(t, m, row)

		now := base + winSec + customerHealthHistoryDays*24*3600 + 3600
		if err := m.advanceOneIncidentWindow(ctx, core, ingest, row, now); err != nil {
			t.Fatalf("%s 到期终结失败: %v", ws, err)
		}
		var after IncidentIngestWindowState
		if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
			key.DedupKey(), base+winSec).Take(&after).Error; err != nil {
			t.Fatalf("读状态失败: %v", err)
		}
		if after.ProcessState == incidentProcessAwaitingSource {
			t.Errorf("%s 超过补采窗后应终结，实际仍为 awaiting_source", ws)
		}
		if after.DataComplete {
			t.Errorf("%s 到期终结后数据不完整，data_complete 必须为 false", ws)
		}
		if !after.GapRecheckClosed {
			t.Errorf("%s 应标 gap_recheck_closed（能力边界）", ws)
		}
		if ws == incidentWindowStateMixedVersion && !after.MixedVersionClosed {
			t.Error("混版到期应标 mixed_version_closed")
		}
	}
}

// 同一聚合键必须保持时间顺序，不得乱序送引擎。
func TestIncidentSameKeyPreservesWindowOrder(t *testing.T) {
	core, ingest := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()

	const winSec = int64(IncidentWindowSeconds)
	base := int64(1_400_000) / winSec * winSec
	key, _ := incidentKeyForFacts(incidentWindowFacts{ChannelID: 505, ModelName: "m", Grp: "g"})

	// 早窗未终结。
	early := windowState(key.DedupKey(), base, base+winSec,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	early.CanonicalKey = key.Canonical()
	early.CreatedAt = base
	seedWindowState(t, m, early)

	// 晚窗来源已满足。
	late := windowState(key.DedupKey(), base+winSec, base+2*winSec,
		incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	late.CanonicalKey = key.Canonical()
	late.CreatedAt = base
	seedWindowState(t, m, late)

	seedCapacitySample(t, m, CapacityUserMinuteSample{
		BucketTs: base + winSec, UserID: 1, ChannelID: 505, ModelName: "m", Grp: "g",
		Success: 10, Anomaly: 90,
	})
	seedDayCoverage(t, m, base, base+2*winSec)

	now := base + 2*winSec + int64(ingest.FinalizeLagSeconds) + 60
	if err := m.advanceOneIncidentWindow(ctx, core, ingest, late, now); err != nil {
		t.Fatalf("推进晚窗失败: %v", err)
	}

	// 晚窗必须仍在排队，没有抢在早窗之前送引擎。
	var after IncidentIngestWindowState
	if err := m.storeDB.Where("dedup_key = ? AND window_to_ts = ?",
		key.DedupKey(), base+2*winSec).Take(&after).Error; err != nil {
		t.Fatalf("读状态失败: %v", err)
	}
	if after.ProcessState != incidentProcessAwaitingSource {
		t.Errorf("同键存在更早未终结窗口时，晚窗必须排队，实际 %s", after.ProcessState)
	}
	var incidents int64
	m.storeDB.Model(&Incident{}).Count(&incidents)
	if incidents != 0 {
		t.Errorf("乱序窗口不得送引擎建单，实际 %d 条事故", incidents)
	}
}
