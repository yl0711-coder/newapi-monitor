package monitor

import (
	"context"
	"testing"

	"gorm.io/gorm"
)

// seedWindowState 直接落一行状态，便于构造各种水位与积压场景。
func seedWindowState(t *testing.T, m *Monitor, row IncidentIngestWindowState) {
	t.Helper()
	if row.CreatedAt == 0 {
		row.CreatedAt = 1000
	}
	row.UpdatedAt = row.CreatedAt
	if err := m.storeDB.Save(&row).Error; err != nil {
		t.Fatalf("写窗口状态失败: %v", err)
	}
}

func windowState(dedupKey string, from, to int64, process, window string, complete bool) IncidentIngestWindowState {
	return IncidentIngestWindowState{
		DedupKey: dedupKey, WindowToTs: to, WindowFromTs: from,
		ProcessState: process, WindowState: window, DataComplete: complete,
		CanonicalKey: "fc=unknown\x1fch=unknown:\x1fmodel=m\x1fgrp=g" +
			"\x1fiproto=ingress_unknown\x1froute=route_unknown\x1fcfg=",
	}
}

// 流程完成 ≠ 数据完整：两个水位必须分开推进。
func TestIncidentWatermarksSeparateProcessFromCompleteness(t *testing.T) {
	m := openIncidentTestStore(t)
	ctx := context.Background()

	// 窗 1 完整完成；窗 2 是缺口（流程终结但数据不完整）；窗 3 完整完成。
	seedWindowState(t, m, windowState("k1", 0, 300, incidentProcessDone, incidentWindowStateNormal, true))
	seedWindowState(t, m, windowState("k1", 300, 600, incidentProcessDone, incidentWindowStateGapObserved, false))
	seedWindowState(t, m, windowState("k1", 600, 900, incidentProcessDone, incidentWindowStateNormal, true))

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: 900, BacklogMode: "normal"}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatalf("算水位失败: %v", err)
	}
	if cursor.CompleteThroughTs != 300 {
		t.Errorf("完整水位应停在缺口起点 300，实际 %d", cursor.CompleteThroughTs)
	}
	if cursor.ScanThroughTs != 900 {
		t.Errorf("扫描水位应越过缺口到 900，实际 %d", cursor.ScanThroughTs)
	}

	// 缺口补齐 → 完整水位推进。
	if err := m.storeDB.Model(&IncidentIngestWindowState{}).
		Where("dedup_key = ? AND window_to_ts = ?", "k1", int64(600)).
		Updates(map[string]any{"data_complete": true, "window_state": incidentWindowStateNormal}).Error; err != nil {
		t.Fatalf("补齐失败: %v", err)
	}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatalf("算水位失败: %v", err)
	}
	if cursor.CompleteThroughTs != 900 {
		t.Errorf("补齐后完整水位应推进到 900，实际 %d", cursor.CompleteThroughTs)
	}
}

// 永久失败：推进 scan、绝不推进 complete，且折叠成摘要后仍阻挡水位。
func TestIncidentFailedPermanentBlocksCompleteEvenAfterSummarized(t *testing.T) {
	m := openIncidentTestStore(t)
	ctx := context.Background()
	now := int64(100 * 86400)

	// 一个很久以前的永久失败窗（满足归档的时间条件）。
	old := windowState("k2", 300, 600, incidentProcessFailedPermanent, incidentWindowStateAnomaly, false)
	old.CreatedAt = 1000
	seedWindowState(t, m, old)
	seedWindowState(t, m, windowState("k2", 600, 900, incidentProcessDone, incidentWindowStateNormal, true))

	cursor := IncidentIngestCursor{ID: incidentIngestCursorID, ScanThroughTs: 900}
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatalf("算水位失败: %v", err)
	}
	if cursor.CompleteThroughTs != 300 {
		t.Fatalf("永久失败必须阻挡完整水位，应为 300，实际 %d", cursor.CompleteThroughTs)
	}

	// 折叠进缺口摘要并删明细。
	if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		_, err := m.archiveIncidentGapWindows(tx, 7, now)
		return err
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	var remain int64
	m.storeDB.Model(&IncidentIngestWindowState{}).
		Where("dedup_key = ? AND window_to_ts = ?", "k2", int64(600)).Count(&remain)
	if remain != 0 {
		t.Fatalf("满足条件的明细应被折叠删除，实际仍有 %d 行", remain)
	}
	var summaries []IncidentIngestGapSummary
	m.storeDB.Find(&summaries)
	if len(summaries) != 1 {
		t.Fatalf("应产生 1 条缺口摘要，实际 %d", len(summaries))
	}
	if summaries[0].FailedPermanentCount != 1 {
		t.Errorf("摘要应记 1 个永久失败，实际 %d", summaries[0].FailedPermanentCount)
	}

	// ★ 关键：明细被删除后，摘要必须等效阻挡完整水位 ★
	if err := m.recomputeIncidentCompleteWatermark(ctx, &cursor); err != nil {
		t.Fatalf("算水位失败: %v", err)
	}
	if cursor.CompleteThroughTs != 300 {
		t.Errorf("摘要必须等效阻挡完整水位（不得因删明细而跳过显示成功），应为 300，实际 %d",
			cursor.CompleteThroughTs)
	}
}

// ★ 历史失败不得占待处理队列、不得触发降级 ★
//
// 否则大量永久失败一旦达到阈值就会永久暂停新窗口处理且无法自动恢复。
func TestIncidentTerminalFailuresDoNotBlockNewData(t *testing.T) {
	m := openIncidentTestStore(t)
	ctx := context.Background()
	ingest := DefaultIncidentIngestConfig()
	ingest.BacklogWarnRows, ingest.BacklogMaxRows = 5, 10

	for i := int64(0); i < 50; i++ {
		seedWindowState(t, m, windowState("kf", i*300, (i+1)*300,
			incidentProcessFailedPermanent, incidentWindowStateAnomaly, false))
	}
	for i := int64(0); i < 20; i++ {
		seedWindowState(t, m, windowState("ku", 100_000+i*300, 100_000+(i+1)*300,
			incidentProcessUnattributable, incidentWindowStateAnomaly, false))
	}

	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if backlog != 0 {
		t.Errorf("终结的历史失败不得计入待处理积压，实际 %d", backlog)
	}
	if mode := incidentBacklogMode("normal", backlog, ingest); mode != "normal" {
		t.Errorf("无可推进任务时不得降级，实际 %s", mode)
	}
	terminal, err := m.countIncidentTerminalGaps(ctx)
	if err != nil {
		t.Fatalf("统计历史缺口失败: %v", err)
	}
	if terminal < 70 {
		t.Errorf("历史缺口应被单独计数（至少 70），实际 %d", terminal)
	}
}

// 本期全场景待确认不产生积压。
func TestIncidentScenarioPendingDoesNotCountAsBacklog(t *testing.T) {
	m := openIncidentTestStore(t)
	ctx := context.Background()
	for i := int64(0); i < 1000; i++ {
		row := windowState("kp", i*300, (i+1)*300, incidentProcessDone, incidentWindowStateNormal, true)
		row.ScenarioPending = true
		row.PendingReason = incidentPendingReasonNoFallback
		row.BelowMinSample = i%2 == 0
		row.RecheckClosed = i%3 == 0
		seedWindowState(t, m, row)
	}
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		t.Fatalf("统计积压失败: %v", err)
	}
	if backlog != 0 {
		t.Errorf("能力边界标记不得计入积压，实际 %d", backlog)
	}
	ingest := DefaultIncidentIngestConfig()
	ingest.BacklogMaxRows, ingest.BacklogWarnRows = 10, 5
	if mode := incidentBacklogMode("normal", backlog, ingest); mode != "normal" {
		t.Errorf("全场景待确认不得降级，实际 %s", mode)
	}
	// 这些行可被正常 prune（data_complete=true）。
	now := int64(1000 * 86400)
	if err := m.pruneIncidentIngest(ctx, now); err != nil {
		t.Fatalf("清理失败: %v", err)
	}
	var left int64
	m.storeDB.Model(&IncidentIngestWindowState{}).Count(&left)
	if left != 0 {
		t.Errorf("已完成且完整的待确认窗口应被清理，实际剩 %d", left)
	}
}

// 降级有恢复路径，且阈值滞回正确。
func TestIncidentBacklogModeHasRecoveryPath(t *testing.T) {
	ingest := DefaultIncidentIngestConfig()
	ingest.BacklogWarnRows, ingest.BacklogMaxRows = 5, 10

	if got := incidentBacklogMode("normal", 9, ingest); got != "normal" {
		t.Errorf("未达上限不应降级，实际 %s", got)
	}
	if got := incidentBacklogMode("normal", 10, ingest); got != "degraded" {
		t.Errorf("达上限应降级，实际 %s", got)
	}
	// 滞回：降级后要降到 warn 以下才恢复，避免阈值附近抖动。
	if got := incidentBacklogMode("degraded", 6, ingest); got != "degraded" {
		t.Errorf("未降到告警线以下应保持降级，实际 %s", got)
	}
	if got := incidentBacklogMode("degraded", 4, ingest); got != "normal" {
		t.Errorf("降到告警线以下应自动恢复，实际 %s", got)
	}
	// 上限低于告警线时必须被纠正，否则一进 degraded 永远回不去。
	t.Setenv("MONITOR_INCIDENT_BACKLOG_WARN_ROWS", "100")
	t.Setenv("MONITOR_INCIDENT_BACKLOG_MAX_ROWS", "10")
	c := LoadIncidentIngestConfig()
	if c.BacklogMaxRows < c.BacklogWarnRows {
		t.Errorf("上限 %d 不得低于告警线 %d", c.BacklogMaxRows, c.BacklogWarnRows)
	}
}

// 归档前置条件：仍需处理的任务一律不得被删。
func TestIncidentGapArchivableGuardsUnfinishedWork(t *testing.T) {
	now := int64(100 * 86400)
	oldWindow := int64(300)

	cases := []struct {
		name string
		row  IncidentIngestWindowState
		want bool
	}{
		{"等待来源不得归档", windowState("k", 0, oldWindow,
			incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false), false},
		{"输入冻结不得归档", windowState("k", 0, oldWindow,
			incidentProcessInputFrozen, incidentWindowStateAnomaly, false), false},
		{"崩溃恢复中不得归档", windowState("k", 0, oldWindow,
			incidentProcessEngineDone, incidentWindowStateAnomaly, false), false},
		{"待重试不得归档", windowState("k", 0, oldWindow,
			incidentProcessFailed, incidentWindowStateAnomaly, false), false},
		{"数据完整不需归档", windowState("k", 0, oldWindow,
			incidentProcessDone, incidentWindowStateNormal, true), false},
		{"永久失败可归档", windowState("k", 0, oldWindow,
			incidentProcessFailedPermanent, incidentWindowStateAnomaly, false), true},
		{"无法归属可归档", windowState("k", 0, oldWindow,
			incidentProcessUnattributable, incidentWindowStateAnomaly, false), true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := incidentGapArchivable(tc.row, 7, now); got != tc.want {
				t.Errorf("归档判定应为 %v，实际 %v", tc.want, got)
			}
		})
	}

	// 缺口窗复核未关闭 → 不得归档（可能被补采消退）。
	open := windowState("k", 0, oldWindow, incidentProcessDone, incidentWindowStateGapObserved, false)
	if incidentGapArchivable(open, 7, now) {
		t.Error("复核未关闭的缺口窗不得归档")
	}
	closed := open
	closed.GapRecheckClosed = true
	if !incidentGapArchivable(closed, 7, now) {
		t.Error("复核已关闭且过期的缺口窗应可归档")
	}
	// 仍在 7 天补齐期内 → 不得归档。
	fresh := closed
	fresh.WindowToTs = now - 3600
	if incidentGapArchivable(fresh, 7, now) {
		t.Error("仍在 7 天补齐期内的窗口不得归档")
	}
}

// 缺口摘要：右边界重复归档不得重复计数；非连续缺口不得合并成一条。
func TestIncidentGapSummaryBoundaryAndContinuity(t *testing.T) {
	m := openIncidentTestStore(t)
	ctx := context.Background()
	now := int64(100 * 86400)

	mk := func(from, to int64) IncidentIngestWindowState {
		r := windowState("kg", from, to, incidentProcessFailedPermanent, incidentWindowStateAnomaly, false)
		r.CreatedAt = 1000
		return r
	}

	// 场景 1：窗口 [0,300) 与摘要完全同界时重复归档不得累加。
	seedWindowState(t, m, mk(0, 300))
	for i := 0; i < 3; i++ {
		if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
			_, err := m.archiveIncidentGapWindows(tx, 7, now)
			return err
		}); err != nil {
			t.Fatalf("归档失败: %v", err)
		}
		// 第二、三次归档时明细已被删，再塞回同一窗口模拟重放后再归档。
		if i < 2 {
			seedWindowState(t, m, mk(0, 300))
		}
	}
	var s1 []IncidentIngestGapSummary
	m.storeDB.Where("dedup_key = ?", "kg").Find(&s1)
	if len(s1) != 1 {
		t.Fatalf("应只有 1 条摘要，实际 %d", len(s1))
	}
	if s1[0].WindowCount != 1 {
		t.Errorf("同界窗口重复归档不得重复计数（右边界判重），应为 1，实际 %d", s1[0].WindowCount)
	}
	if s1[0].GapFromTs != 0 || s1[0].GapThroughTs != 300 {
		t.Errorf("摘要区间应为 [0,300)，实际 [%d,%d)", s1[0].GapFromTs, s1[0].GapThroughTs)
	}

	// 场景 2：非连续缺口。[0,300) 已归档，再归档 [600,900)，
	// 中间 [300,600) 尚未满足条件 → 必须是两条摘要，不得跨过空洞。
	seedWindowState(t, m, mk(600, 900))
	// 中间窗口仍需处理（等待来源），不满足归档条件。
	mid := windowState("kg", 300, 600, incidentProcessAwaitingSource, incidentWindowStateWaitCoverage, false)
	mid.CreatedAt = 1000
	seedWindowState(t, m, mid)

	if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		_, err := m.archiveIncidentGapWindows(tx, 7, now)
		return err
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	var s2 []IncidentIngestGapSummary
	m.storeDB.Where("dedup_key = ?", "kg").Order("gap_from_ts").Find(&s2)
	if len(s2) != 2 {
		t.Fatalf("非连续缺口应产生 2 条摘要（不得跨过未归档窗口），实际 %d: %+v", len(s2), s2)
	}
	if s2[0].GapThroughTs != 300 || s2[1].GapFromTs != 600 {
		t.Errorf("两段区间应为 [0,300) 与 [600,900)，实际 [%d,%d) 与 [%d,%d)",
			s2[0].GapFromTs, s2[0].GapThroughTs, s2[1].GapFromTs, s2[1].GapThroughTs)
	}
	// 中间窗口必须还在（未被误删）。
	var midLeft int64
	m.storeDB.Model(&IncidentIngestWindowState{}).
		Where("dedup_key = ? AND window_to_ts = ?", "kg", int64(600)).Count(&midLeft)
	if midLeft != 1 {
		t.Error("仍需处理的中间窗口不得被归档删除")
	}

	// 场景 3：中间窗口后来也满足条件 → 被正确计数（不漏），三段合并为一条。
	if err := m.storeDB.Model(&IncidentIngestWindowState{}).
		Where("dedup_key = ? AND window_to_ts = ?", "kg", int64(600)).
		Updates(map[string]any{
			"process_state": incidentProcessFailedPermanent,
			"window_state":  incidentWindowStateAnomaly,
		}).Error; err != nil {
		t.Fatalf("更新中间窗口失败: %v", err)
	}
	if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		_, err := m.archiveIncidentGapWindows(tx, 7, now)
		return err
	}); err != nil {
		t.Fatalf("归档失败: %v", err)
	}
	var s3 []IncidentIngestGapSummary
	m.storeDB.Where("dedup_key = ?", "kg").Order("gap_from_ts").Find(&s3)
	total := int64(0)
	for _, s := range s3 {
		total += s.WindowCount
	}
	if total != 3 {
		t.Errorf("三个窗口都应被计数一次（不重不漏），实际总计 %d: %+v", total, s3)
	}
	// 合并后区间应覆盖 [0,900)。
	minFrom, maxThrough := s3[0].GapFromTs, s3[0].GapThroughTs
	for _, s := range s3 {
		if s.GapFromTs < minFrom {
			minFrom = s.GapFromTs
		}
		if s.GapThroughTs > maxThrough {
			maxThrough = s.GapThroughTs
		}
	}
	if minFrom != 0 || maxThrough != 900 {
		t.Errorf("合并后应覆盖 [0,900)，实际 [%d,%d)", minFrom, maxThrough)
	}
}
