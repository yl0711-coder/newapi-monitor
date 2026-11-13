package monitor

// 窗口处理主体：扫描、推进、引擎提交、崩溃恢复（MON-003 B3）。

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// scanIncidentNewWindows 扫描尚未处理的新窗口。
//
// 处理条件两条都要满足（见 incidentFinalizeFrontier）：
// 延迟已过 **且** 请求覆盖已越过窗口右界。
func (m *Monitor) scanIncidentNewWindows(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, cursor *IncidentIngestCursor, now int64) error {

	windowSec := int64(core.WindowSeconds)
	if windowSec <= 0 {
		windowSec = IncidentWindowSeconds
	}
	frontier := AlignIncidentWindow(incidentFinalizeFrontier(ingest, now), windowSec).FromTs

	from := cursor.ScanThroughTs
	if from <= 0 {
		// 首次启动：从延迟前沿往回一个复核窗起步，不做历史全量回填。
		from = AlignIncidentWindow(frontier-int64(ingest.RecheckWindowSeconds), windowSec).FromTs
		if from < 0 {
			from = 0
		}
		// 固定首次扫描的左界；首窗未能落库时，下轮也从这里重试，
		// 不能随着时间推进重新计算回看起点而跳过尚未消费的窗口。
		cursor.ScanThroughTs = from
	}

	processed := 0
	for winFrom := from; winFrom+windowSec <= frontier; winFrom += windowSec {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if processed >= ingest.RecheckMaxWindows {
			break
		}
		winTo := winFrom + windowSec
		if err := m.processIncidentWindow(ctx, core, ingest, winFrom, winTo, now); err != nil {
			// 只有失败记录可靠落库后才能隔离该窗，留给来源队列重试。
			// 落库也失败时停止扫描，不能消费一个重启后无从恢复的窗口。
			if writeErr := m.reportIncidentIngestWindowError(ctx, winFrom, winTo, err, ingest, now); writeErr != nil {
				return fmt.Errorf("窗口 [%d,%d) 失败且无法持久化重试记录: %w", winFrom, winTo, errors.Join(err, writeErr))
			}
		}
		// 成功处理或持久化失败/阻塞记录后才推进扫描；完整水位另行计算。
		cursor.ScanThroughTs = winTo
		processed++
	}
	return nil
}

// processIncidentWindow 处理一个窗口内的全部聚合键。
func (m *Monitor) processIncidentWindow(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, winFrom, winTo, now int64) error {
	_, err := m.processIncidentWindowWithResult(ctx, core, ingest, winFrom, winTo, now)
	return err
}

// incidentWindowProcessResult 区分重扫成功后的持久化去向，不是窗口判定状态。
// 两个标记都为 false 时，各真实聚合键的状态已落库，由它们承担完整性阻塞。
type incidentWindowProcessResult struct {
	CoveragePending bool
	NoSample        bool
	CoverageProof   string
}

func (m *Monitor) processIncidentWindowWithResult(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, winFrom, winTo, now int64) (incidentWindowProcessResult, error) {

	var result incidentWindowProcessResult
	proof, err := m.incidentRequestCoverage(ctx, winFrom, winTo)
	if err != nil {
		return result, err
	}
	result.CoverageProof = incidentCoverageProof(proof)
	frontier, err := m.incidentCoverageFrontier(ctx, winFrom, winTo)
	if err != nil {
		return result, err
	}
	facts, err := m.aggregateIncidentWindowFacts(ctx, winFrom, winTo)
	if err != nil {
		return result, err
	}

	// 覆盖完整但完全没有事实行：仍要为已知的活跃聚合键落 no_sample 记录，
	// 否则「异常后无流量」就没有任何痕迹。
	if len(facts) == 0 {
		keys, err := m.incidentActiveKeysForNoSample(ctx)
		if err != nil {
			return result, err
		}
		facts = append(facts, keys...)
	}

	// ★ 没有聚合键 ≠ 覆盖完整且确实无流量 ★
	//
	// 覆盖凭证不完整、又没有任何事实行与活跃键时，这个窗口既没有「无流量」
	// 的凭证，也没有任何待处理记录；扫描水位一过，完整水位就跟着越过它，
	// 之后事实补进来也没有状态行可供复核。所以必须留一条可被来源队列继续
	// 推进的窗口级阻塞行，由它承担「数据尚未采齐」的语义。
	if len(facts) == 0 && !proof.RequestsComplete {
		result.CoveragePending = true
		return result, m.markIncidentWindowAwaitingCoverage(ctx, ingest, winFrom, winTo, proof, frontier, now)
	}
	// 覆盖已完整且确实没有任何聚合键：这是可以如实宣告的「无流量」。
	// 把遗留的阻塞占位行收尾为 no_sample + 完整，不再阻挡水位。
	if len(facts) == 0 {
		result.NoSample = true
		return result, m.resolveIncidentWindowCoverageBlock(ctx, winFrom, winTo, proof, now)
	}

	var errs []string
	for _, f := range facts {
		d, err := decideIncidentWindow(core, ingest, f, f.sampleTotal() > 0, proof, frontier, winFrom, winTo)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		// ★ 顺序门禁必须覆盖主扫描 ★
		//
		// 同键存在更早的未终结窗口时，主扫描也不得抢先把更晚的窗口送进引擎：
		// 乱序提交会让旧窗补齐后被判迟到或无法归属。只排队，不建单。
		blocked, err := m.incidentKeyHasEarlierUnfinished(ctx, d.DedupKey, d.WindowToTs)
		if err != nil {
			errs = append(errs, err.Error())
			continue
		}
		if blocked {
			if err := m.deferIncidentWindowDecision(ctx, ingest, d, now); err != nil {
				errs = append(errs, fmt.Sprintf("%s@%d: %v", d.DedupKey[:8], d.WindowToTs, err))
			}
			continue
		}
		if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
			errs = append(errs, fmt.Sprintf("%s@%d: %v", d.DedupKey[:8], d.WindowToTs, err))
		}
	}
	if len(errs) > 0 {
		return result, fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return result, nil
}

// incidentCoverageFrontier 返回全局请求覆盖的已证明前沿。
//
// ★ 这里要的是「全局已推进到哪」，不是「本窗连续覆盖证明」★
//
// customerHealthHistoricalCoverage 返回的 RequestThroughTs 是**从目标日起点
// 开始的连续拼接**：前一天缺覆盖时它停在缺口之前，于是即使实时游标早已越过
// 本窗，本窗也会被误判成 wait_coverage（还会继续推进）而不是 gap_observed
// （确认存在缺口）。所以再取一次只读的实时来源游标前沿，两者取大。
//
// 只读 CustomerHealthSourceCursor 表，不调 loadCustomerHealthSourceCursor
// ——后者会初始化水位、在语义迁移时开事务回退 FRT 水位并持锁，有写副作用。
func (m *Monitor) incidentCoverageFrontier(ctx context.Context, winFrom, winTo int64) (int64, error) {
	dayStart := customerHealthDayStart(winFrom)
	proof, err := m.customerHealthHistoricalCoverage(ctx, dayStart, winTo+24*3600)
	if err != nil {
		return 0, err
	}
	frontier := proof.RequestThroughTs

	live, err := m.incidentLiveCoverageFrontier(ctx)
	if err != nil {
		return 0, err
	}
	if live > frontier {
		frontier = live
	}
	return frontier, nil
}

// incidentLiveCoverageFrontier 只读实时来源游标的最大已采时刻。
//
// 口径版本不符的游标行不算证据：那部分数据会被重采，不能用来证明「已越过」。
func (m *Monitor) incidentLiveCoverageFrontier(ctx context.Context) (int64, error) {
	var through *int64
	if err := m.storeDB.WithContext(ctx).Model(&CustomerHealthSourceCursor{}).
		Select("MAX(through_ts)").
		Where("semantics_version = ?", customerHealthStabilityPolicyVersion).
		Scan(&through).Error; err != nil {
		return 0, fmt.Errorf("读实时覆盖前沿失败: %w", err)
	}
	if through == nil {
		return 0, nil
	}
	return *through, nil
}

// markIncidentWindowAwaitingCoverage 为「无任何聚合键且覆盖不完整」的窗口
// 留一条可推进的阻塞记录。
//
// 没有聚合键不能推定完整：这条占位行让完整水位停在该窗之前，并由来源队列
// 按窗口整体重扫，直到覆盖补齐（或到期按缺口终结）。
func (m *Monitor) markIncidentWindowAwaitingCoverage(ctx context.Context, ingest IncidentIngestConfig,
	winFrom, winTo int64, proof CustomerHealthHistoricalCoverage, frontier, now int64) error {

	state := incidentWindowStateWaitCoverage
	if winTo <= frontier {
		state = incidentWindowStateGapObserved
	}
	var existing IncidentIngestWindowState
	res := m.storeDB.WithContext(ctx).
		Where("dedup_key = ? AND window_to_ts = ?", incidentIngestWindowCoverageKey, winTo).
		Limit(1).Find(&existing)
	if res.Error != nil {
		return fmt.Errorf("查覆盖阻塞占位行失败: %w", res.Error)
	}
	row := IncidentIngestWindowState{
		DedupKey:      incidentIngestWindowCoverageKey,
		WindowToTs:    winTo,
		WindowFromTs:  winFrom,
		WindowState:   state,
		CanonicalKey:  incidentIngestWindowCoverageCanonical,
		ProcessState:  incidentProcessAwaitingSource,
		DataComplete:  false,
		CoverageProof: incidentCoverageProof(proof),
		CreatedAt:     now,
		UpdatedAt:     now,
	}
	if res.RowsAffected > 0 {
		row.CreatedAt = existing.CreatedAt
		row.SourceCheckAttempts = existing.SourceCheckAttempts
		row.Attempts = existing.Attempts
	}
	row.SourceCheckAttempts++
	row.NextSourceCheckAt = incidentSourceCheckBackoff(row.SourceCheckAttempts, ingest.PollSeconds, now)
	return m.saveIncidentWindowState(ctx, row)
}

// resolveIncidentWindowCoverageBlock 覆盖补齐且确认无流量后收尾阻塞占位行。
//
// 收尾为 no_sample + data_complete=true，而不是删除：这一窗曾因覆盖未采齐
// 被阻塞过，之后才确认「确实无流量」，这个区别要留痕。
// 没有占位行（从未被阻塞过）时什么都不做。
func (m *Monitor) resolveIncidentWindowCoverageBlock(ctx context.Context, winFrom, winTo int64,
	proof CustomerHealthHistoricalCoverage, now int64) error {

	var row IncidentIngestWindowState
	res := m.storeDB.WithContext(ctx).
		Where("dedup_key = ? AND window_to_ts = ?", incidentIngestWindowCoverageKey, winTo).
		Limit(1).Find(&row)
	if res.Error != nil {
		return fmt.Errorf("查覆盖阻塞占位行失败: %w", res.Error)
	}
	if res.RowsAffected == 0 {
		return nil
	}
	row.WindowState = incidentWindowStateNoSample
	row.ProcessState = incidentProcessDone
	row.DataComplete = true
	row.CoverageProof = incidentCoverageProof(proof)
	row.NextSourceCheckAt = 0
	row.NextRetryAt = 0
	row.UpdatedAt = now
	return m.saveIncidentWindowState(ctx, row)
}

// deferIncidentWindowDecision 同键有更早未终结窗口时只排队，不调引擎。
//
// 落 awaiting_source 并带退避，由来源队列在旧窗终结后按顺序推进。
// 不写判定输入快照：此时还没有冻结输入，崩溃恢复不得把它当已冻结窗重放。
func (m *Monitor) deferIncidentWindowDecision(ctx context.Context, ingest IncidentIngestConfig,
	d incidentWindowDecision, now int64) error {

	var existing IncidentIngestWindowState
	res := m.storeDB.WithContext(ctx).
		Where("dedup_key = ? AND window_to_ts = ?", d.DedupKey, d.WindowToTs).
		Limit(1).Find(&existing)
	if res.Error != nil {
		return fmt.Errorf("查窗口状态失败: %w", res.Error)
	}
	if res.RowsAffected > 0 && existing.ProcessState != incidentProcessAwaitingSource {
		// 已经进入冻结/完成/放弃态的行不回退，交由各自路径处理。
		return nil
	}
	row := incidentWindowStateRowFrom(d, m.incidentIngestRetentionDays(), now)
	row.ProcessState = incidentProcessAwaitingSource
	row.DataComplete = false
	row.SampleTotal, row.AnomalyTotal, row.SuccessTotal = nil, nil, nil
	if res.RowsAffected > 0 {
		row.CreatedAt = existing.CreatedAt
		row.SourceCheckAttempts = existing.SourceCheckAttempts
		row.Attempts = existing.Attempts
	}
	row.SourceCheckAttempts++
	row.NextSourceCheckAt = incidentSourceCheckBackoff(row.SourceCheckAttempts, ingest.PollSeconds, now)
	return m.saveIncidentWindowState(ctx, row)
}

// incidentActiveKeysForNoSample 返回仍活跃的事故聚合键，用于无样本窗记录。
//
// 只读 incidents 表的维度列重建 facts 壳（计数为 0），不读生产库。
func (m *Monitor) incidentActiveKeysForNoSample(ctx context.Context) ([]incidentWindowFacts, error) {
	var rows []Incident
	if err := m.storeDB.WithContext(ctx).
		Where("active_marker = ?", incidentActiveMarker).
		Limit(1000).Find(&rows).Error; err != nil {
		return nil, fmt.Errorf("读活跃事故聚合键失败: %w", err)
	}
	out := make([]incidentWindowFacts, 0, len(rows))
	for _, r := range rows {
		f := incidentWindowFacts{ModelName: r.Model, Grp: r.UserGroup}
		if r.ChannelID != nil {
			f.ChannelID = int(*r.ChannelID)
		}
		out = append(out, f)
	}
	return out, nil
}

// commitIncidentWindowDecision 落状态行，必要时调引擎与写影响面。
//
// ★ 状态流转 ★
//
//	awaiting_source --(来源满足)--> input_frozen --(引擎提交)--> engine_done --> done
//	awaiting_source --(no_sample / 无需调引擎)--> done
//
// 写 input_frozen 的那次提交同时落判定输入快照，所以「有快照」与
// input_frozen 等价，不会出现带快照的 awaiting_source 行。
func (m *Monitor) commitIncidentWindowDecision(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, d incidentWindowDecision, now int64) error {

	row := incidentWindowStateRowFrom(d, m.incidentIngestRetentionDays(), now)

	// 已存在且已终结为完整的窗口不再重复处理（幂等）。
	var existing IncidentIngestWindowState
	res := m.storeDB.WithContext(ctx).
		Where("dedup_key = ? AND window_to_ts = ?", d.DedupKey, d.WindowToTs).
		Limit(1).Find(&existing)
	if res.Error != nil {
		return fmt.Errorf("查窗口状态失败: %w", res.Error)
	}
	if res.RowsAffected > 0 {
		// Every entry, including whole-window retries, must finish the frozen input.
		// Changed source facts belong to the separate, audited revision path.
		if existing.ProcessState == incidentProcessInputFrozen || existing.ProcessState == incidentProcessEngineDone {
			return m.recoverIncidentFrozenWindow(ctx, core, ingest, existing, now)
		}
		if existing.ProcessState == incidentProcessDone && existing.DataComplete {
			return nil
		}
		// 整窗重扫也会到这里，不能绕过来源队列的已定稿缺口保护。
		// 覆盖补齐只追加修订，仍缺失则等待；两种情况都不重进 B2 引擎。
		if existing.ProcessState == incidentProcessDone && !existing.DataComplete {
			if existing.GapRecheckClosed || existing.MixedVersionClosed {
				return nil
			}
			return m.reviseBackfilledIncidentGap(ctx, ingest, existing, d, true, now)
		}
		if existing.ProcessState == incidentProcessFailedPermanent ||
			existing.ProcessState == incidentProcessUnattributable {
			// 已放弃：不自动重试，也不因覆盖后来补齐就改成成功。
			return nil
		}
		row.CreatedAt = existing.CreatedAt
		row.Attempts = existing.Attempts
		row.RecheckAttempts = existing.RecheckAttempts
		row.SourceCheckAttempts = existing.SourceCheckAttempts
		row.IncidentID = existing.IncidentID
		row.SegmentID = existing.SegmentID
	}

	if !d.SendToEngine {
		switch d.WindowState {
		case incidentWindowStateWaitCoverage, incidentWindowStateMixedVersion:
			row.ProcessState = incidentProcessAwaitingSource
			row.SourceCheckAttempts++
			row.NextSourceCheckAt = incidentSourceCheckBackoff(
				row.SourceCheckAttempts, ingest.PollSeconds, now)
		default:
			// no_sample：流程终结、数据完整，但**不调引擎**。
			row.ProcessState = incidentProcessDone
			row.DataComplete = true
		}
		return m.saveIncidentWindowState(ctx, row)
	}

	// 冻结输入后再调引擎。两次独立提交之间崩溃由恢复流程处理。
	row.ProcessState = incidentProcessInputFrozen
	row.FinalizedAt = now
	if err := m.saveIncidentWindowState(ctx, row); err != nil {
		return err
	}

	in := d.observationInputFor(core.MinSample)
	result, err := m.IngestIncidentObservation(ctx, in)
	if err != nil {
		return m.markIncidentWindowFailure(ctx, row, err, ingest, now)
	}
	if result.Action == "skipped_disabled" {
		// 核心被关闭：绝不当成功消费。保持 input_frozen，下轮重来。
		return fmt.Errorf("事故核心开关已关闭，窗口未处理")
	}

	row.ProcessState = incidentProcessEngineDone
	row.IncidentID = result.IncidentID
	row.SegmentID = result.SegmentID
	row.Severity = result.Severity
	row.Outcome = result.Outcome
	row.Action = result.Action
	if err := m.saveIncidentWindowState(ctx, row); err != nil {
		return err
	}
	return m.finishIncidentImpactStage(ctx, row, d, now)
}

// finishIncidentImpactStage 写影响面并把状态推到 done。
//
// ★ 判定是否该写影响面看 IncidentID，不看 Action 白名单 ★
//
// 用 {created, updated, segment_appended} 白名单会漏掉 evidence_only
// （重复/乱序窗口的正常返回，同样带 IncidentID/SegmentID），
// 于是「引擎成功、影响面失败」的重试那一轮就永远不写影响面。
func (m *Monitor) finishIncidentImpactStage(ctx context.Context, row IncidentIngestWindowState,
	d incidentWindowDecision, now int64) error {

	if row.IncidentID == "" {
		// 无事故归属（skipped_no_anomaly 等）：无影响面可写，直接终结。
		row.ProcessState = incidentProcessDone
		row.DataComplete = d.DataComplete
		return m.saveIncidentWindowState(ctx, row)
	}
	err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		return m.writeIncidentImpactWindows(tx, row.IncidentID, row.SegmentID, d, now)
	})
	if err != nil {
		// 保持 engine_done：游标不推进完整水位，重试时不再调引擎。
		return fmt.Errorf("写影响面失败: %w", err)
	}
	row.ProcessState = incidentProcessDone
	row.DataComplete = d.DataComplete
	row.UpdatedAt = now
	return m.saveIncidentWindowState(ctx, row)
}

func (m *Monitor) saveIncidentWindowState(ctx context.Context, row IncidentIngestWindowState) error {
	return m.saveIncidentWindowStateTx(m.storeDB.WithContext(ctx), row)
}

func (m *Monitor) saveIncidentWindowStateTx(tx *gorm.DB, row IncidentIngestWindowState) error {
	if row.CreatedAt == 0 {
		row.CreatedAt = row.UpdatedAt
	}
	// 显式 upsert 而不是 Save：复合主键里 window_to_ts 可以是 0，
	// GORM 会把零值主键当新记录走 INSERT 并撞唯一约束。
	if err := tx.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "dedup_key"}, {Name: "window_to_ts"}},
		UpdateAll: true,
	}).Create(&row).Error; err != nil {
		return fmt.Errorf("写窗口状态失败: %w", err)
	}
	// 待确认结论同步到事故级摘要，使它不随窗口清理消失。
	// 放在这里是因为所有 pending 的产生路径最终都经过这一个写入口。
	return m.persistIncidentPendingSummaryTx(tx, row)
}

// persistIncidentPendingSummaryTx 把窗口上的待确认结论提升到事故级。
//
// 只做「置为待确认」，不在这里清除：确认与否是人工动作，不能因为某个
// 窗口不再 pending 就推定整个事故已确认。
func (m *Monitor) persistIncidentPendingSummaryTx(tx *gorm.DB, row IncidentIngestWindowState) error {
	if !row.ScenarioPending || row.IncidentID == "" {
		return nil
	}
	sum := IncidentPendingSummary{
		IncidentID:      row.IncidentID,
		ScenarioPending: true,
		PendingReason:   row.PendingReason,
		FirstPendingAt:  row.UpdatedAt,
		UpdatedAt:       row.UpdatedAt,
	}
	if err := tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{{Name: "incident_id"}},
		// FirstPendingAt 不覆盖：保留最早一次待确认的时刻。
		DoUpdates: clause.AssignmentColumns([]string{"scenario_pending", "pending_reason", "updated_at"}),
	}).Create(&sum).Error; err != nil {
		return fmt.Errorf("写事故待确认摘要失败: %w", err)
	}
	return nil
}

// markIncidentWindowFailure 记录失败并决定是否放弃。
//
// isStaleUnattributedWindow 的硬错误单独归类为 unattributable 且**不重试**：
// 那是 B2 刻意保留的「无法归属即整体回滚」契约，重试不会改变结果。
func (m *Monitor) markIncidentWindowFailure(ctx context.Context, row IncidentIngestWindowState,
	cause error, ingest IncidentIngestConfig, now int64) error {

	row.LastError = incidentIngestErrorText(cause)
	row.UpdatedAt = now
	row.DataComplete = false
	if incidentIsUnattributableError(cause) {
		row.ProcessState = incidentProcessUnattributable
		return m.saveIncidentWindowState(ctx, row)
	}
	row.Attempts++
	if row.Attempts >= ingest.MaxAttempts {
		row.ProcessState = incidentProcessFailedPermanent
	} else {
		row.ProcessState = incidentProcessFailed
		row.NextRetryAt = incidentRetryBackoff(row.Attempts, ingest.PollSeconds, now)
	}
	return m.saveIncidentWindowState(ctx, row)
}

// incidentIsUnattributableError 识别引擎的旧窗口归属硬错误。
//
// 必须用 errors.Is 认 sentinel，不能做字符串匹配：真实错误是
// ErrObservationUnattributed（文本「观测窗口无法确定归属分段」），
// 匹配「无法归属」这个子串根本认不到它，于是真实硬错误会被当成普通
// 失败进入无意义的有界重试——重试不会改变归属结果。
func incidentIsUnattributableError(err error) bool {
	return errors.Is(err, ErrObservationUnattributed)
}

// incidentIngestErrorText 截断并去除可能的敏感内容。
func incidentIngestErrorText(err error) string {
	if err == nil {
		return ""
	}
	s, _ := stabilityProblemText(err.Error())
	if len(s) > 480 {
		s = s[:480]
	}
	return s
}

// reportIncidentIngestWindowError 窗口级失败兜底记录。
//
// 聚合键无法确定时（如覆盖查询失败）落一条以窗口为单位的占位失败行，
// 保证失败不只写日志、游标推进后不会永久漏掉。
func (m *Monitor) reportIncidentIngestWindowError(ctx context.Context, winFrom, winTo int64,
	cause error, ingest IncidentIngestConfig, now int64) error {

	row := IncidentIngestWindowState{
		DedupKey:     incidentIngestWindowErrorKey,
		WindowToTs:   winTo,
		WindowFromTs: winFrom,
		WindowState:  incidentWindowStateWaitCoverage,
		CanonicalKey: "window_level_failure",
		CreatedAt:    now,
		UpdatedAt:    now,
	}
	return m.markIncidentWindowFailure(ctx, row, cause, ingest, now)
}

// incidentIngestWindowErrorKey 是窗口级失败的占位聚合键。
//
// 用固定串而不是空串：空串会与「真实聚合键算出空值」混淆。
const incidentIngestWindowErrorKey = "window_level_failure_placeholder_key_000000000000000000000000000"

// incidentIngestWindowCoverageKey 是「窗口内无任何聚合键且覆盖未采齐」的占位键。
//
// 与 incidentIngestWindowErrorKey 分开：那条代表**读取失败**（要重试），
// 这条代表**数据尚未采齐**（要等覆盖补齐），两种未知不能互相冒充。
const incidentIngestWindowCoverageKey = "window_level_coverage_placeholder_key_00000000000000000000000000"

const incidentIngestWindowCoverageCanonical = "window_level_coverage_pending"
