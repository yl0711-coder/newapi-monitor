package monitor

// Incident 采集后台循环（MON-003 B3）。
//
// ★ 独立、可取消、有界重试 ★
//
// 不进主采样 tick：那条 tick 在 8204 的 logchain-only 模式下根本不跑
// （startSampler 不启动），挂上去等于永不执行。本循环由 startSourceEpoch
// 的两个分支分别挂载，用既有 group.Go 保证可取消。
//
// ★ 单窗口失败不阻断其他窗口与现有采集 ★
//
// 每个「聚合键 × 窗口」是独立处理单元，失败只影响该单元与完整水位，
// 不阻止其他键继续处理、不阻止扫描前进、不让循环退出。

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// startIncidentIngest 是挂载入口。
//
// 总开关：核心关闭时直接返回，不扫描、不写任何游标。
func (m *Monitor) startIncidentIngest(ctx context.Context) {
	if _, _, ok := incidentIngestActive(); !ok {
		return
	}
	if m.storeDB == nil {
		return
	}
	m.runIncidentIngestLoop(ctx)
}

// runIncidentIngestLoop 主循环。
func (m *Monitor) runIncidentIngestLoop(ctx context.Context) {
	timer := time.NewTimer(0)
	defer timer.Stop()
	failures := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			if ctx.Err() != nil {
				return
			}
			core, ingest, ok := incidentIngestActive()
			if !ok {
				// 运行中被关闭：停止处理，但不写游标、不推进水位。
				return
			}
			err := m.runIncidentIngestTurn(ctx, core, ingest, time.Now().Unix())
			if ctx.Err() != nil {
				return
			}
			if err != nil {
				if failures < 6 {
					failures++
				}
			} else {
				failures = 0
			}
			timer.Reset(incidentIngestDelay(ingest.PollSeconds, failures))
		}
	}
}

func incidentIngestDelay(pollSeconds, failures int) time.Duration {
	base := time.Duration(pollSeconds) * time.Second
	if failures <= 0 {
		return base
	}
	d := base
	for i := 0; i < failures && d < 30*time.Minute; i++ {
		d *= 2
	}
	if d > 30*time.Minute {
		d = 30 * time.Minute
	}
	return d
}

// runIncidentIngestTurn 执行一轮：推进等待/失败行 → 扫描新窗口 → 复核修订 → 归档。
//
// 顺序是刻意的：先推进积压，再扫新窗口。degraded 模式下只跳过「扫描新窗口」
// 这一步，其余照常——所以积压能靠推进与到期终结自然消退并自动恢复，
// 不会永久空转。
func (m *Monitor) runIncidentIngestTurn(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, now int64) error {

	cursor, err := m.loadIncidentIngestCursor(ctx)
	if err != nil {
		return err
	}
	cursor.TickCount++

	var errs []string

	// 1. 推进来源队列（等待来源 / 缺口未关闭 / 待重试 / 崩溃恢复）。
	if err := m.advanceIncidentPendingWindows(ctx, core, ingest, &cursor, now); err != nil {
		errs = append(errs, err.Error())
	}

	// 2. 扫描新窗口（degraded 时跳过）。
	backlog, err := m.countIncidentPendingBacklog(ctx)
	if err != nil {
		errs = append(errs, err.Error())
	} else {
		cursor.BacklogMode = incidentBacklogMode(cursor.BacklogMode, backlog, ingest)
	}
	if cursor.BacklogMode != "degraded" {
		if err := m.scanIncidentNewWindows(ctx, core, ingest, &cursor, now); err != nil {
			errs = append(errs, err.Error())
		}
	}

	// 3. 迟到修订复核（独立队列，受 2h / 6 次限制）。
	if ingest.RecheckEveryTicks > 0 && cursor.TickCount%int64(ingest.RecheckEveryTicks) == 0 {
		if err := m.recheckIncidentFinalizedWindows(ctx, core, ingest, now); err != nil {
			errs = append(errs, err.Error())
		}
	}

	// 4. 归档（影响面贡献 + 缺口摘要）。
	if err := m.pruneIncidentIngest(ctx, now); err != nil {
		errs = append(errs, err.Error())
	}

	// 5. 重算完整水位。必须在归档之后：摘要与明细等效阻挡。
	completeErr := m.recomputeIncidentCompleteWatermark(ctx, &cursor)
	if completeErr != nil {
		errs = append(errs, completeErr.Error())
	}

	cursor.UpdatedAt = now
	var saveErr error
	if completeErr != nil {
		// Revisions may have lowered the persisted watermark during this turn.
		// A failed recomputation cannot authorize writing the old in-memory value.
		saveErr = m.saveIncidentIngestProgress(ctx, cursor)
	} else {
		saveErr = m.saveIncidentIngestCursor(ctx, cursor)
	}
	if saveErr != nil {
		errs = append(errs, saveErr.Error())
	}
	if len(errs) > 0 {
		return errors.New(strings.Join(errs, "; "))
	}
	return nil
}

// incidentBacklogMode 决定积压模式。
//
// 只看 pending_backlog（仍可被推进的行）。终结的历史失败不参与——否则
// 大量永久失败一旦达到阈值就会永久暂停新窗口处理且无法自动恢复。
//
// 滞回：超过 Max 进 degraded，降到 Warn 以下才回 normal。
func incidentBacklogMode(current string, backlog int64, ingest IncidentIngestConfig) string {
	if current == "degraded" {
		if backlog < int64(ingest.BacklogWarnRows) {
			return "normal"
		}
		return "degraded"
	}
	if backlog >= int64(ingest.BacklogMaxRows) {
		return "degraded"
	}
	return "normal"
}

// countIncidentPendingBacklog 统计**仍可被推进**的行。
//
// 刻意不含 failed_permanent / unattributable / 已关闭复核的不完整窗口：
// 那些是「已终结的历史失败」，只做告警与水位阻挡，不占待处理队列。
func (m *Monitor) countIncidentPendingBacklog(ctx context.Context) (int64, error) {
	var n int64
	err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Where(`process_state IN (?,?,?,?)
			OR (process_state = ? AND data_complete = ?
			    AND gap_recheck_closed = ? AND mixed_version_closed = ?)`,
			incidentProcessAwaitingSource, incidentProcessInputFrozen,
			incidentProcessEngineDone, incidentProcessFailed,
			incidentProcessDone, false, false, false).
		Count(&n).Error
	if err != nil {
		return 0, fmt.Errorf("统计事故采集积压失败: %w", err)
	}
	return n, nil
}

// countIncidentTerminalGaps 统计已终结的历史失败与缺口（含摘要窗口数）。
func (m *Monitor) countIncidentTerminalGaps(ctx context.Context) (int64, error) {
	var rows, summarized int64
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Where(`process_state IN (?,?) OR (data_complete = ? AND (gap_recheck_closed = ? OR mixed_version_closed = ?))`,
			incidentProcessFailedPermanent, incidentProcessUnattributable,
			false, true, true).
		Count(&rows).Error; err != nil {
		return 0, fmt.Errorf("统计事故历史缺口失败: %w", err)
	}
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestGapSummary{}).
		Select("COALESCE(SUM(window_count), 0)").Scan(&summarized).Error; err != nil {
		return 0, fmt.Errorf("统计缺口摘要失败: %w", err)
	}
	return rows + summarized, nil
}

// loadIncidentIngestCursor 读单例游标；不存在则返回初始值（**不写库**）。
//
// 刻意只读：核心关闭时也可能走到读取路径（如健康接口），不能顺手建行。
func (m *Monitor) loadIncidentIngestCursor(ctx context.Context) (IncidentIngestCursor, error) {
	c := IncidentIngestCursor{ID: incidentIngestCursorID, BacklogMode: "normal"}
	res := m.storeDB.WithContext(ctx).Where("id = ?", incidentIngestCursorID).Limit(1).Find(&c)
	if res.Error != nil {
		return c, fmt.Errorf("读事故采集游标失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return IncidentIngestCursor{ID: incidentIngestCursorID, BacklogMode: "normal"}, nil
	}
	if c.BacklogMode == "" {
		c.BacklogMode = "normal"
	}
	return c, nil
}

func (m *Monitor) saveIncidentIngestCursor(ctx context.Context, c IncidentIngestCursor) error {
	c.ID = incidentIngestCursorID
	if err := m.storeDB.WithContext(ctx).Save(&c).Error; err != nil {
		return fmt.Errorf("写事故采集游标失败: %w", err)
	}
	return nil
}

// saveIncidentIngestProgress preserves the stored completeness watermark while
// retaining durable scan/scheduling progress after a failed recomputation.
// If no cursor exists, completeness starts unknown (zero), never from a stale copy.
func (m *Monitor) saveIncidentIngestProgress(ctx context.Context, c IncidentIngestCursor) error {
	c.ID = incidentIngestCursorID
	c.CompleteThroughTs = 0
	if err := m.storeDB.WithContext(ctx).Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "id"}},
		DoUpdates: clause.AssignmentColumns([]string{
			"scan_through_ts", "backlog_mode", "last_scheduled_dedup_key", "tick_count", "updated_at",
		}),
	}).Create(&c).Error; err != nil {
		return fmt.Errorf("write incident scan progress: %w", err)
	}
	return nil
}

// recomputeIncidentCompleteWatermark 重算完整处理水位。
//
// 水位 = 「最早一个未完整起点」之前。未完整包括：
//   - 任何未终结行（awaiting_source / input_frozen / engine_done / failed）
//   - 任何 data_complete=false 的明细（含 failed_permanent / unattributable）
//   - 任何缺口摘要区间
//
// 摘要与明细等效，所以把明细折叠成摘要再删除**不会**让水位跳过并显示成功。
func (m *Monitor) recomputeIncidentCompleteWatermark(ctx context.Context, cursor *IncidentIngestCursor) error {
	var blocker *int64
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Select("MIN(window_from_ts)").
		Where("data_complete = ? OR process_state IN (?,?,?,?)",
			false, incidentProcessAwaitingSource, incidentProcessInputFrozen,
			incidentProcessEngineDone, incidentProcessFailed).
		Scan(&blocker).Error; err != nil {
		return fmt.Errorf("算事故完整水位失败: %w", err)
	}
	var gapBlocker *int64
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestGapSummary{}).
		Select("MIN(gap_from_ts)").Scan(&gapBlocker).Error; err != nil {
		return fmt.Errorf("算缺口摘要水位失败: %w", err)
	}

	limit := cursor.ScanThroughTs
	if blocker != nil && *blocker > 0 && *blocker < limit {
		limit = *blocker
	}
	if gapBlocker != nil && *gapBlocker > 0 && *gapBlocker < limit {
		limit = *gapBlocker
	}
	// 单调：水位只前进不后退，除非阻塞点真的更早（那是如实回退，必须允许）。
	cursor.CompleteThroughTs = limit
	return nil
}

// incidentIngestTx 在本地库事务里执行，带超时预算。
func (m *Monitor) incidentIngestTx(ctx context.Context, fn func(tx *gorm.DB) error) error {
	c, cancel := context.WithTimeout(ctx, incidentWriteBudget)
	defer cancel()
	return m.storeDB.WithContext(c).Transaction(fn)
}
