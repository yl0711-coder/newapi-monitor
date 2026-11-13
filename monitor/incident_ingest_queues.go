package monitor

// 两条独立队列与归档调度（MON-003 B3）。
//
// ★ 为什么必须是两条队列 ★
//
//	来源推进队列    处理「仍可被推进或仍需确认缺口是否补齐」的行。
//	                不受 2h / 6 次限制，调度持续到窗口终结。
//	迟到修订复核队列 只处理**已完成**窗口的定稿后修订。
//	                受 RecheckWindowSeconds / RecheckMaxAttempts 限制。
//
// 共用一条队列会冲突：缺口要「持续等待至 7 天后终结」，但复核队列最多
// 查 6 次、范围只有 2 小时，次数一耗尽就不再调度，到期判断也就停了。
// 所以两条队列用两个计数器（SourceCheckAttempts / RecheckAttempts）。

import (
	"context"
	"fmt"
	"sort"
	"strings"

	"gorm.io/gorm"
)

// advanceIncidentPendingWindows 推进来源队列。
//
// Finalized windows can lose completeness during late review. Keep any done,
// incomplete window in the source queue until backfilled or explicitly closed.
// Permanent failures remain excluded by the process-state predicate.
func (m *Monitor) advanceIncidentPendingWindows(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, cursor *IncidentIngestCursor, now int64) error {

	// ★ 轮转必须发生在 SQL 取数阶段 ★
	//
	// 以前的做法是「按 dedup_key 排序取 PendingBatch*2 行，再在内存里轮转」。
	// 单个键有大量待推进旧窗时，它就把 LIMIT 吃满，排在它之后的键根本进不了
	// 结果集，内存轮转只能在已被截断的那一个键里打转——其他键永远排不上队，
	// 设置 LastScheduledDedupKey 也无效。
	//
	// 所以先只查**有哪些键**（轻量 DISTINCT），按游标轮转选出本轮参与的键，
	// 再按这些键取行。
	base := func() *gorm.DB {
		return m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
			Where(`(process_state IN (?,?,?,?))
				OR (process_state = ? AND data_complete = ?
				    AND gap_recheck_closed = ? AND mixed_version_closed = ?)`,
				incidentProcessAwaitingSource, incidentProcessInputFrozen,
				incidentProcessEngineDone, incidentProcessFailed,
				incidentProcessDone, false, false, false).
			// 退避过滤：到点的才取。没有它每轮都会取最旧的同一批，
			// 其他聚合键的可处理任务永远排不上。
			Where("(next_source_check_at <= ? OR next_source_check_at = 0) AND (next_retry_at <= ? OR next_retry_at = 0)", now, now)
	}

	var keys []string
	if err := base().Distinct().Order("dedup_key").Pluck("dedup_key", &keys).Error; err != nil {
		return fmt.Errorf("读事故采集待推进聚合键失败: %w", err)
	}
	if len(keys) == 0 {
		cursor.LastScheduledDedupKey = ""
		return nil
	}
	keys = incidentRotateDedupKeys(keys, cursor.LastScheduledDedupKey, ingest.PendingBatch)

	// 逐键取行，每键独立 LIMIT。
	//
	// 不能用 `dedup_key IN (...)` 加一个总 LIMIT：那样 ORDER BY dedup_key 又会
	// 让字典序靠前的大键把配额吃光，靠后的键再次被截断——正是要修的问题。
	//
	// 每键配额 = 批量 / 参与键数（至少 1）：既保证每个键都有份，也把一轮读入
	// 的总行数压在 PendingBatch 量级，不会退化成 键数 × PendingBatch。
	perKey := ingest.PendingBatch / len(keys)
	if perKey < 1 {
		perKey = 1
	}
	var rows []IncidentIngestWindowState
	for _, k := range keys {
		var keyRows []IncidentIngestWindowState
		if err := base().Where("dedup_key = ?", k).
			Order("window_to_ts").Limit(perKey).Find(&keyRows).Error; err != nil {
			return fmt.Errorf("读事故采集待推进窗口失败: %w", err)
		}
		rows = append(rows, keyRows...)
	}
	if len(rows) == 0 {
		cursor.LastScheduledDedupKey = ""
		return nil
	}
	rows = incidentRotatePendingRows(rows, cursor.LastScheduledDedupKey, ingest.PendingBatch)

	var errs []string
	for _, selected := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		cursor.LastScheduledDedupKey = selected.DedupKey
		// A preceding whole-window retry can finish or defer this selected task.
		// Reload both its state and eligibility; never execute the preloaded copy.
		var row IncidentIngestWindowState
		res := base().Where("dedup_key = ? AND window_to_ts = ?", selected.DedupKey, selected.WindowToTs).
			Limit(1).Find(&row)
		if res.Error != nil {
			errs = append(errs, fmt.Sprintf("reload incident pending window: %v", res.Error))
			continue
		}
		if res.RowsAffected == 0 {
			continue
		}
		if err := m.advanceOneIncidentWindow(ctx, core, ingest, row, now); err != nil {
			errs = append(errs, err.Error())
		}
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// incidentRotateDedupKeys 选出本轮参与调度的聚合键，从上轮停下的键之后开始。
//
// keys 必须已按字典序升序。返回值保持「从起点开始绕回」的轮转顺序，
// 所以字典序靠后的键也能在上一轮之后拿到配额。
//
// 不做数量截断（最多 limit 个键，每键至少一行，正好够 PendingBatch）：
// 键数少于 limit 时全部参与。
func incidentRotateDedupKeys(keys []string, lastKey string, limit int) []string {
	if len(keys) == 0 {
		return keys
	}
	start := incidentRotationStart(keys, lastKey)
	out := make([]string, 0, len(keys))
	for i := 0; i < len(keys); i++ {
		out = append(out, keys[(start+i)%len(keys)])
		if limit > 0 && len(out) >= limit {
			break
		}
	}
	return out
}

// incidentRotationStart 返回轮转起点下标：第一个严格大于 lastKey 的键，
// 没有则绕回 0。
func incidentRotationStart(keys []string, lastKey string) int {
	for i, k := range keys {
		if k > lastKey {
			return i
		}
	}
	return 0
}

// incidentRotatePendingRows 按键轮转取 limit 条。
//
// 从 lastKey 之后的键开始，绕回头部，再按顺序取。单个聚合键有大量
// 长期无法推进的旧窗口时，其他键的任务不会被挤掉。
func incidentRotatePendingRows(rows []IncidentIngestWindowState, lastKey string, limit int) []IncidentIngestWindowState {
	if limit <= 0 || len(rows) <= limit {
		return rows
	}
	byKey := map[string][]IncidentIngestWindowState{}
	keys := []string{}
	for _, r := range rows {
		if _, ok := byKey[r.DedupKey]; !ok {
			keys = append(keys, r.DedupKey)
		}
		byKey[r.DedupKey] = append(byKey[r.DedupKey], r)
	}
	sort.Strings(keys)
	start := incidentRotationStart(keys, lastKey)
	out := make([]IncidentIngestWindowState, 0, limit)
	// 每轮每键先各取一个，再第二轮，避免单键吃满配额。
	for round := 0; len(out) < limit; round++ {
		added := false
		for i := 0; i < len(keys) && len(out) < limit; i++ {
			k := keys[(start+i)%len(keys)]
			if round < len(byKey[k]) {
				out = append(out, byKey[k][round])
				added = true
			}
		}
		if !added {
			break
		}
	}
	return out
}

// advanceOneIncidentWindow 推进单行。
//
// ★ 同一聚合键必须保持时间顺序 ★
//
// 该键存在更早的未终结窗口时，不处理更晚的窗口——乱序送入会触发引擎的
// 旧窗口归属硬错误或被判 stale_ignored。
func (m *Monitor) advanceOneIncidentWindow(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, row IncidentIngestWindowState, now int64) error {

	if row.DedupKey == incidentIngestWindowErrorKey ||
		row.DedupKey == incidentIngestWindowCoverageKey {
		// 窗口级占位行（读取失败 / 覆盖未采齐）：聚合键未知，只能整窗重扫。
		return m.retryIncidentWindowLevelFailure(ctx, core, ingest, row, now)
	}

	blocked, err := m.incidentKeyHasEarlierUnfinished(ctx, row.DedupKey, row.WindowToTs)
	if err != nil {
		return err
	}
	if blocked {
		row.SourceCheckAttempts++
		row.NextSourceCheckAt = incidentSourceCheckBackoff(row.SourceCheckAttempts, ingest.PollSeconds, now)
		row.UpdatedAt = now
		return m.saveIncidentWindowState(ctx, row)
	}

	// 崩溃恢复分支**只接受 input_frozen 与 engine_done**。
	// awaiting_source 一律重走覆盖/混版检查，绝不进引擎。
	if row.ProcessState == incidentProcessInputFrozen || row.ProcessState == incidentProcessEngineDone {
		return m.recoverIncidentFrozenWindow(ctx, core, ingest, row, now)
	}

	// 到期终结：超过 7 天补采窗仍未满足来源条件 → 按 gap_observed 终结。
	if incidentGapRecheckDeadline(row.WindowToTs) < now {
		return m.closeIncidentWindowAsGap(ctx, core, ingest, row, now)
	}

	// 重新判定来源条件。
	return m.reevaluateIncidentWindow(ctx, core, ingest, row, now)
}

// incidentKeyHasEarlierUnfinished 该键是否存在更早的未终结窗口。
func (m *Monitor) incidentKeyHasEarlierUnfinished(ctx context.Context, dedupKey string, windowToTs int64) (bool, error) {
	var n int64
	err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Where("dedup_key = ? AND window_to_ts < ? AND process_state IN (?,?,?,?)",
			dedupKey, windowToTs,
			incidentProcessAwaitingSource, incidentProcessInputFrozen,
			incidentProcessEngineDone, incidentProcessFailed).
		Count(&n).Error
	if err != nil {
		return false, fmt.Errorf("查同键更早未终结窗口失败: %w", err)
	}
	return n > 0, nil
}

// reevaluateIncidentWindow 重新聚合与判定一个等待/失败窗口。
func (m *Monitor) reevaluateIncidentWindow(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, row IncidentIngestWindowState, now int64) error {

	facts, err := m.aggregateIncidentWindowFacts(ctx, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return err
	}
	proof, err := m.incidentRequestCoverage(ctx, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return err
	}
	frontier, err := m.incidentCoverageFrontier(ctx, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return err
	}

	target, found := incidentFactsForDedupKey(facts, row.DedupKey)
	d, err := m.decideIncidentWindowForExistingRow(core, ingest, row, target, found,
		proof, frontier)
	if err != nil {
		return err
	}
	if d.DedupKey != row.DedupKey {
		// 聚合键算不回原值（例如渠道归属变了）：保留原行，记一条修订留痕，
		// 不改写原行的聚合键——那会破坏引擎侧已有的归属。
		//
		// 必须同时自增并退避，否则该行会被每一轮无限重取，挤占
		// PendingBatch 配额并让其他键排不上。
		if err := m.recordIncidentRevision(ctx, row, d, "key_mismatch", now); err != nil {
			return err
		}
		row.SourceCheckAttempts++
		row.NextSourceCheckAt = incidentSourceCheckBackoff(row.SourceCheckAttempts, ingest.PollSeconds, now)
		row.UpdatedAt = now
		return m.saveIncidentWindowState(ctx, row)
	}

	// ★ 已定稿缺口的补齐只记修订，不重新进引擎 ★
	//
	// done + data_complete=false 的行，其判定已经定稿（当时
	// 无活跃事故就是 skipped）。补到数据后若再走普通提交分支，引擎会拿着
	// 补齐后的异常样本**追溯建单**——那违反「修订不回改原判定」的边界。
	// 这里改走专门的修订路径：只记新旧值、更新影响面与完整性。
	if row.ProcessState == incidentProcessDone && !row.DataComplete {
		return m.reviseBackfilledIncidentGap(ctx, ingest, row, d, found, now)
	}

	// 先自增并落库，再交给 commit 处理。commit 会从库里读回 existing 并
	// 继承这些计数，所以顺序不能颠倒——否则 commit 重建的行会把自增盖掉。
	row.SourceCheckAttempts++
	row.NextSourceCheckAt = incidentSourceCheckBackoff(row.SourceCheckAttempts, ingest.PollSeconds, now)
	row.UpdatedAt = now
	if err := m.saveIncidentWindowState(ctx, row); err != nil {
		return err
	}
	return m.commitIncidentWindowDecision(ctx, core, ingest, d, now)
}

// decideIncidentWindowForExistingRow 为一条已存在的状态行重新判定，
// 并保证聚合键身份来自持久化的 CanonicalKey。
//
// ★ 来源行消失不得改变事故身份 ★
//
// 查不到原键的匹配行时，以前直接把零值 facts 交给 decideIncidentWindow，
// 于是它算出一个 unknown 渠道 / 空模型 / 空分组的**新**聚合键：
//   - 完整覆盖且确实无样本的窗口不能正常结束为 no_sample，只留 key_mismatch；
//   - 过补采期的缺口终结变成无归属的 skipped_no_anomaly，原事故没被推进。
//
// 来源消失只影响「样本/覆盖」，不影响「这是哪个维度的窗口」。所以从
// CanonicalKey 还原原维度，再把它写回 d.Key/DedupKey/Canonical 三者。
// 三者必须一致：只强填后两个会让引擎实际使用 d.Key 里的另一个键。
func (m *Monitor) decideIncidentWindowForExistingRow(core IncidentConfig, ingest IncidentIngestConfig,
	row IncidentIngestWindowState, target incidentWindowFacts, found bool,
	proof CustomerHealthHistoricalCoverage, frontier int64) (incidentWindowDecision, error) {

	d, err := decideIncidentWindow(core, ingest, target, found && target.sampleTotal() > 0,
		proof, frontier, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return incidentWindowDecision{}, err
	}
	if found {
		// 有匹配行：聚合键由事实算出，与原键是否相同交由调用方判断
		// （不同则走 key_mismatch 留痕，不在这里强行改写）。
		return d, nil
	}
	if row.CanonicalKey == "" {
		return d, nil
	}
	key, err := incidentKeyFromCanonical(row.CanonicalKey)
	if err != nil {
		// 还原失败就不猜：保持算出来的键，由 key_mismatch 分支留痕。
		return d, nil
	}
	d.Key = key
	d.DedupKey = key.DedupKey()
	d.Canonical = key.Canonical()
	return d, nil
}

// reviseBackfilledIncidentGap 已定稿缺口补齐后的修订路径。
//
// 只做三件事：记修订、更新影响面贡献与完整性、推进水位。
// **不重新调用引擎、不覆盖原判定快照**——原判定（含 severity/outcome/action）
// 是当时证据下的结论，补齐后的数据是「最新实测值」，两者分开保存。
//
// 补齐后发现异常时按 gap_backfilled_anomalous 记一条修订，提示人工复核；
// 不自动追溯建单或定级。
func (m *Monitor) reviseBackfilledIncidentGap(ctx context.Context, ingest IncidentIngestConfig,
	row IncidentIngestWindowState, d incidentWindowDecision, found bool, now int64) error {

	kind := "revised_after_finalize"
	if incidentDecisionHasCompleteData(d) {
		kind = "gap_backfilled"
		if d.WindowState == incidentWindowStateAnomaly {
			kind = "gap_backfilled_anomalous"
		}
	}
	return m.applyIncidentFinalizedRevision(ctx, ingest, row, d, kind, found, now)
}

// incidentFactsForDedupKey 在事实集合里找出属于该聚合键的那条。
func incidentFactsForDedupKey(facts []incidentWindowFacts, dedupKey string) (incidentWindowFacts, bool) {
	for _, f := range facts {
		k, err := incidentKeyForFacts(f)
		if err != nil {
			continue
		}
		if k.DedupKey() == dedupKey {
			return f, true
		}
	}
	return incidentWindowFacts{}, false
}

// closeIncidentWindowAsGap 到期终结一个等待窗口。
//
// 超过 7 天补采窗再等也不会被补齐（runCustomerHealthHistoryTurnWith 只回填
// 近 7 天）。按 gap_observed 走引擎的 DataPresent=false 路径终结，
// 并标相应的**能力边界**标记——不是失败状态。
func (m *Monitor) closeIncidentWindowAsGap(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, row IncidentIngestWindowState, now int64) error {

	// 已定稿缺口到期只结束来源复核，不能再次进入引擎改写当时的判定。
	if row.ProcessState == incidentProcessDone {
		row.GapRecheckClosed = true
		row.MixedVersionClosed = row.MixedVersionClosed || row.MeasuredWindowState == incidentWindowStateMixedVersion
		row.UpdatedAt = now
		return m.saveIncidentWindowState(ctx, row)
	}

	wasMixed := row.WindowState == incidentWindowStateMixedVersion
	facts, _ := m.aggregateIncidentWindowFacts(ctx, row.WindowFromTs, row.WindowToTs)
	target, found := incidentFactsForDedupKey(facts, row.DedupKey)

	// ★ 身份来自持久化的 CanonicalKey，不来自「现在还能不能查到来源行」★
	//
	// 过补采期终结时来源行往往已经不存在。拿零值 facts 算键会得到一个
	// unknown/空维度的新键，引擎据此走 skipped_no_anomaly，原事故的缺口
	// 根本没被推进。所以优先还原原维度；只有还原失败才回落到事实算键。
	key, err := incidentKeyFromCanonical(row.CanonicalKey)
	if err != nil {
		key, err = incidentKeyForFacts(target)
		if err != nil {
			return err
		}
	}
	_ = found
	level, method := incidentEvidenceForLocalFacts()
	// 三者必须同源一致：只强填 DedupKey/Canonical 会让引擎实际使用 Key 里的另一个键。
	d := incidentWindowDecision{
		Key: key, DedupKey: key.DedupKey(), Canonical: key.Canonical(),
		WindowFromTs: row.WindowFromTs, WindowToTs: row.WindowToTs,
		WindowState:    incidentWindowStateGapObserved,
		SendToEngine:   true,
		DataComplete:   false,
		CoverageProof:  row.CoverageProof,
		EvidenceLevel:  level,
		EvidenceMethod: method,
		Responsibility: incidentResponsibilityForLocalFacts(level),
	}
	if err := m.commitIncidentWindowDecision(ctx, core, ingest, d, now); err != nil {
		return err
	}
	// 回读并打上关闭标记。
	var cur IncidentIngestWindowState
	if err := m.storeDB.WithContext(ctx).
		Where("dedup_key = ? AND window_to_ts = ?", row.DedupKey, row.WindowToTs).
		Limit(1).Find(&cur).Error; err != nil {
		return fmt.Errorf("回读窗口状态失败: %w", err)
	}
	if cur.DedupKey == "" {
		return nil
	}
	if wasMixed {
		cur.MixedVersionClosed = true
	}
	cur.GapRecheckClosed = true
	cur.UpdatedAt = now
	return m.saveIncidentWindowState(ctx, cur)
}

// recoverIncidentFrozenWindow 崩溃恢复。
//
// ★ 恢复依据是快照，不是重新聚合 ★
//
// 引擎事务与状态行是两次独立提交，中间崩溃会留下「事故已存在、状态仍
// input_frozen」。此时重新聚合会把期间变化的来源数据冒充原判定依据，
// 所以必须用落在状态行里的输入快照重放。
//
// 先按确定性派生的 observation ID 探测引擎侧是否已落证据：
//   - 已有 → 引擎此前成功，直接补 engine_done 再走影响面
//   - 没有 → 引擎未提交，用快照重放
func (m *Monitor) recoverIncidentFrozenWindow(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, row IncidentIngestWindowState, now int64) error {

	d, err := incidentDecisionFromSnapshot(row)
	if err != nil {
		return err
	}

	if row.IncidentID != "" {
		// engine_done：只补影响面，不再调引擎。
		return m.finishIncidentImpactStage(ctx, row, d, now)
	}

	incidentID, segmentID, found, err := m.findIncidentObservationForWindow(ctx, row)
	if err != nil {
		return err
	}
	if found {
		row.ProcessState = incidentProcessEngineDone
		row.IncidentID, row.SegmentID = incidentID, segmentID
		row.UpdatedAt = now
		if err := m.saveIncidentWindowState(ctx, row); err != nil {
			return err
		}
		return m.finishIncidentImpactStage(ctx, row, d, now)
	}

	result, err := m.IngestIncidentObservation(ctx, d.observationInputFor(core.MinSample))
	if err != nil {
		return m.markIncidentWindowFailure(ctx, row, err, ingest, now)
	}
	row.ProcessState = incidentProcessEngineDone
	row.IncidentID, row.SegmentID = result.IncidentID, result.SegmentID
	row.Severity, row.Outcome, row.Action = result.Severity, result.Outcome, result.Action
	row.UpdatedAt = now
	if err := m.saveIncidentWindowState(ctx, row); err != nil {
		return err
	}
	return m.finishIncidentImpactStage(ctx, row, d, now)
}

// incidentDecisionFromSnapshot 从状态行的输入快照重建判定结果。
func incidentDecisionFromSnapshot(row IncidentIngestWindowState) (incidentWindowDecision, error) {
	key, err := incidentKeyFromCanonical(row.CanonicalKey)
	if err != nil {
		return incidentWindowDecision{}, err
	}
	level, method := incidentEvidenceForLocalFacts()
	d := incidentWindowDecision{
		Key: key, DedupKey: row.DedupKey, Canonical: row.CanonicalKey,
		WindowFromTs:   row.WindowFromTs,
		WindowToTs:     row.WindowToTs,
		WindowState:    row.WindowState,
		SendToEngine:   true,
		DataComplete:   row.WindowState != incidentWindowStateGapObserved,
		CoverageProof:  row.CoverageProof,
		EvidenceLevel:  level,
		EvidenceMethod: method,
		Responsibility: row.Responsibility,
		BelowMinSample: row.BelowMinSample,
		AnomalyRate:    row.AnomalyRate,
		MixedRows:      row.MixedRows,
	}
	if row.SampleTotal != nil {
		d.SampleTotal = *row.SampleTotal
	}
	if row.AnomalyTotal != nil {
		d.AnomalyTotal = *row.AnomalyTotal
	}
	if row.SuccessTotal != nil {
		d.SuccessTotal = *row.SuccessTotal
	}
	if d.Responsibility == "" {
		d.Responsibility = incidentResponsibilityForLocalFacts(level)
	}
	return d, nil
}

// findIncidentObservationForWindow 按确定性 observation ID 探测引擎侧证据。
func (m *Monitor) findIncidentObservationForWindow(ctx context.Context,
	row IncidentIngestWindowState) (string, string, bool, error) {

	var candidates []Incident
	if err := m.storeDB.WithContext(ctx).
		Where("dedup_key = ?", row.DedupKey).
		Order("last_seen_ts DESC").Limit(8).Find(&candidates).Error; err != nil {
		return "", "", false, fmt.Errorf("查同键事故失败: %w", err)
	}
	for _, inc := range candidates {
		var segs []IncidentSegment
		if err := m.storeDB.WithContext(ctx).
			Where("incident_id = ?", inc.ID).Find(&segs).Error; err != nil {
			return "", "", false, fmt.Errorf("查事故分段失败: %w", err)
		}
		for _, seg := range segs {
			var ob IncidentObservation
			res := m.storeDB.WithContext(ctx).
				Where("id = ?", incidentObservationID(seg.ID, row.WindowToTs)).
				Limit(1).Find(&ob)
			if res.Error != nil {
				return "", "", false, fmt.Errorf("查事故观测失败: %w", res.Error)
			}
			if res.RowsAffected > 0 {
				return inc.ID, seg.ID, true, nil
			}
		}
	}
	return "", "", false, nil
}

// retryIncidentWindowLevelFailure 对窗口级占位行做真正的整窗重扫。
//
// ★ 重试必须真的重扫，不能只加计数 ★
//
// 占位行的聚合键是未知的（覆盖/聚合查询本身失败，或窗口内暂无任何键），
// 所以无法按键推进，只能重新执行 processIncidentWindow 整窗处理。
// 以前这里只自增 attempts 与退避时刻，从不重新聚合，等次数耗尽就标
// failed_permanent——那等于永久漏掉该窗口。
//
// processIncidentWindow 对每个键走 commitIncidentWindowDecision，而后者对
// 「已 done 且完整」与「已 engine_done」都是幂等的，所以重扫只会补齐尚未
// 完成的键，不会重复建单。
//
// 只有覆盖已证明无流量，或已有其他持久状态承担阻塞时，才结束占位调度。
// 记录继续保留；重扫执行成功本身不证明来源完整。
func (m *Monitor) retryIncidentWindowLevelFailure(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, row IncidentIngestWindowState, now int64) error {

	if row.Attempts >= ingest.MaxAttempts && row.DedupKey == incidentIngestWindowErrorKey {
		// 读取失败类占位行有界放弃；覆盖未采齐类不受 MaxAttempts 限制，
		// 它要一直等到覆盖补齐或过 7 天补采期（与缺口复核同一边界）。
		row.ProcessState = incidentProcessFailedPermanent
		row.UpdatedAt = now
		return m.saveIncidentWindowState(ctx, row)
	}
	if row.DedupKey == incidentIngestWindowCoverageKey &&
		incidentGapRecheckDeadline(row.WindowToTs) < now {
		// 过补采期仍未采齐：如实终结为确认缺口，不再占用调度。
		row.WindowState = incidentWindowStateGapObserved
		row.ProcessState = incidentProcessDone
		row.DataComplete = false
		row.GapRecheckClosed = true
		row.UpdatedAt = now
		return m.saveIncidentWindowState(ctx, row)
	}

	result, rescanErr := m.processIncidentWindowWithResult(ctx, core, ingest, row.WindowFromTs, row.WindowToTs, now)
	if rescanErr == nil && result.CoveragePending && row.DedupKey == incidentIngestWindowCoverageKey {
		// 重扫已保存最新覆盖阻塞和退避，不能再用传入的旧行覆盖它。
		return nil
	}

	// 先无条件记一次尝试，保证失败路径一定有留痕与退避。
	row.Attempts++
	row.SourceCheckAttempts++
	row.NextRetryAt = incidentRetryBackoff(row.Attempts, ingest.PollSeconds, now)
	row.NextSourceCheckAt = row.NextRetryAt
	row.UpdatedAt = now
	if rescanErr != nil {
		row.LastError = incidentIngestErrorText(rescanErr)
		row.ProcessState = incidentProcessFailed
		return m.saveIncidentWindowState(ctx, row)
	}

	// 重扫已持久化真实键状态，或从读取失败转交给覆盖阻塞占位行，
	// 或已证明确实无流量。只有这些情况才结束当前占位行的调度。
	//
	// 这里把它标成 done + data_complete=true 而**不是删除**：曾经发生过
	// 窗口级读取失败、重试了几次、何时恢复，这些都是要留痕的事实。
	// data_complete=true 让它不再阻挡完整水位；真实键若仍不完整，会由
	// 它们自己的行继续阻挡，不靠这条占位行。
	row.ProcessState = incidentProcessDone
	row.DataComplete = true
	row.CoverageProof = result.CoverageProof
	row.LastError = ""
	row.NextSourceCheckAt = 0
	row.NextRetryAt = 0
	if result.NoSample {
		// 只有完整覆盖且确实没有聚合键才叫无样本；交给真实键不等于无样本。
		row.WindowState = incidentWindowStateNoSample
	}
	return m.saveIncidentWindowState(ctx, row)
}

// recordIncidentRevision 写一条只增的修订记录。
func (m *Monitor) recordIncidentRevision(ctx context.Context, row IncidentIngestWindowState,
	d incidentWindowDecision, kind string, now int64) error {
	return m.recordIncidentRevisionWithFacts(ctx, row, d, kind, true, now)
}

// recordIncidentRevisionWithFacts 写修订记录，并如实标注来源行是否还存在。
func (m *Monitor) recordIncidentRevisionWithFacts(ctx context.Context, row IncidentIngestWindowState,
	d incidentWindowDecision, kind string, sourceFound bool, now int64) error {
	return m.recordIncidentRevisionTx(m.storeDB.WithContext(ctx), row, d, kind, sourceFound, now)
}

func (m *Monitor) recordIncidentRevisionTx(tx *gorm.DB, row IncidentIngestWindowState,
	d incidentWindowDecision, kind string, sourceFound bool, now int64) error {

	seq := 0
	var existing int64
	if err := tx.Model(&IncidentIngestRevision{}).
		Where("dedup_key = ? AND window_to_ts = ?", row.DedupKey, row.WindowToTs).
		Count(&existing).Error; err != nil {
		return fmt.Errorf("查修订序号失败: %w", err)
	}
	seq = int(existing) + 1

	newSample, newAnomaly, newSuccess := d.SampleTotal, d.AnomalyTotal, d.SuccessTotal
	// 旧值取「已有的最新已知值」，与 incidentWindowInputsChanged 的基准一致。
	oldSample, oldAnomaly, oldSuccess := row.SampleTotal, row.AnomalyTotal, row.SuccessTotal
	oldRate := row.AnomalyRate
	oldState, oldProof := row.WindowState, row.CoverageProof
	if row.MeasuredAt > 0 {
		oldSample, oldAnomaly, oldSuccess = row.MeasuredSampleTotal, row.MeasuredAnomalyTotal, row.MeasuredSuccessTotal
		oldRate = row.MeasuredAnomalyRate
		if row.MeasuredWindowState != "" {
			oldState = row.MeasuredWindowState
		}
		if row.MeasuredCoverageProof != "" {
			oldProof = row.MeasuredCoverageProof
		}
	}
	rev := IncidentIngestRevision{
		ID:                incidentImpactWindowID(row.DedupKey, kind, "rev", "seq", int64(seq)*1_000_000+row.WindowToTs),
		DedupKey:          row.DedupKey,
		WindowToTs:        row.WindowToTs,
		WindowFromTs:      row.WindowFromTs,
		IncidentID:        row.IncidentID,
		RevisionSeq:       seq,
		Kind:              kind,
		OldSampleTotal:    oldSample,
		NewSampleTotal:    &newSample,
		OldAnomalyTotal:   oldAnomaly,
		NewAnomalyTotal:   &newAnomaly,
		OldSuccessTotal:   oldSuccess,
		NewSuccessTotal:   &newSuccess,
		OldAnomalyRate:    oldRate,
		NewAnomalyRate:    d.AnomalyRate,
		OldWindowState:    oldState,
		NewWindowState:    d.WindowState,
		OldCoverageProof:  oldProof,
		CoverageProof:     d.CoverageProof,
		SourceRowsMissing: !sourceFound,
		DetectedAt:        now,
	}
	// ★ 这是「修订发现延迟」，不是日志实际迟到时长 ★
	// 只表示定稿后多久被复核发现，受复核频率离散化、范围与次数截断，
	// 且只统计被发现的修订，是系统性低估。
	if row.FinalizedAt > 0 && now > row.FinalizedAt {
		rev.RevisionDetectLagSeconds = now - row.FinalizedAt
	}
	if err := tx.Save(&rev).Error; err != nil {
		return fmt.Errorf("写修订记录失败: %w", err)
	}
	return nil
}

// recheckIncidentFinalizedWindows 迟到修订复核队列。
//
// 只处理**已完成**窗口（done + data_complete）的定稿后修订。
// 受 RecheckWindowSeconds（范围）与 RecheckMaxAttempts（次数）限制，
// 耗尽标 recheck_closed——那是**能力边界**，明示「超出复核范围之后的
// 迟到数据不会被发现」，不假装覆盖全部。
func (m *Monitor) recheckIncidentFinalizedWindows(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, now int64) error {

	lowerBound := now - int64(ingest.RecheckWindowSeconds)
	var rows []IncidentIngestWindowState
	err := m.storeDB.WithContext(ctx).
		Where("process_state = ? AND data_complete = ? AND recheck_closed = ? AND window_to_ts >= ?",
			incidentProcessDone, true, false, lowerBound).
		Where("recheck_attempts < ?", ingest.RecheckMaxAttempts).
		Order("window_to_ts DESC").Limit(ingest.RecheckMaxWindows).Find(&rows).Error
	if err != nil {
		return fmt.Errorf("读待复核窗口失败: %w", err)
	}

	var errs []string
	for _, row := range rows {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := m.recheckOneIncidentWindow(ctx, core, ingest, row, now); err != nil {
			errs = append(errs, err.Error())
		}
	}
	// 超出范围或次数的窗口标能力边界。
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Where("process_state = ? AND data_complete = ? AND recheck_closed = ?",
			incidentProcessDone, true, false).
		Where("window_to_ts < ? OR recheck_attempts >= ?", lowerBound, ingest.RecheckMaxAttempts).
		Updates(map[string]any{"recheck_closed": true, "updated_at": now}).Error; err != nil {
		errs = append(errs, fmt.Sprintf("标记复核关闭失败: %v", err))
	}
	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// recheckOneIncidentWindow 复核单个已完成窗口是否有迟到修订。
//
// 发现变化时：写 revised_after_finalize 修订、替换影响面窗口贡献并重算汇总。
// **不改写引擎侧已判定的观测与严重度**（引擎的 OnConflict DoNothing 会丢弃
// 观测修订，且 B2 的连续性单调性不允许回改已判定窗口）。
func (m *Monitor) recheckOneIncidentWindow(ctx context.Context, core IncidentConfig,
	ingest IncidentIngestConfig, row IncidentIngestWindowState, now int64) error {

	facts, err := m.aggregateIncidentWindowFacts(ctx, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return err
	}
	target, found := incidentFactsForDedupKey(facts, row.DedupKey)

	row.RecheckAttempts++
	row.UpdatedAt = now

	proof, err := m.incidentRequestCoverage(ctx, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return err
	}
	frontier, err := m.incidentCoverageFrontier(ctx, row.WindowFromTs, row.WindowToTs)
	if err != nil {
		return err
	}
	d, err := m.decideIncidentWindowForExistingRow(core, ingest, row, target, found, proof, frontier)
	if err != nil {
		return err
	}

	return m.applyIncidentFinalizedRevision(ctx, ingest, row, d, "revised_after_finalize", found, now)
}

// incidentApplyMeasured 把最新实测值写进 Measured* 列。
//
// 绝不覆盖 SampleTotal/AnomalyTotal/SuccessTotal/AnomalyRate——那是引擎
// 当时的判定依据，回改后就无法回答「当初为什么这么判」。
func incidentApplyMeasured(row *IncidentIngestWindowState, d incidentWindowDecision, now int64) {
	sample, anomaly, success := d.SampleTotal, d.AnomalyTotal, d.SuccessTotal
	row.MeasuredSampleTotal = &sample
	row.MeasuredAnomalyTotal = &anomaly
	row.MeasuredSuccessTotal = &success
	row.MeasuredAnomalyRate = d.AnomalyRate
	row.MeasuredWindowState = d.WindowState
	row.MeasuredCoverageProof = d.CoverageProof
	row.MeasuredAt = now
}

// incidentWindowInputsChanged 比较「影响判定与展示的完整输入」。
//
// 对比基准是**已有的最新已知值**：有 Measured* 时用它（上一次复核的结论），
// 否则用引擎判定快照。这样连续多轮复核不会把同一个变化反复记成修订。
//
// 比较项：样本/异常/成功计数、异常率、窗口态、覆盖凭证、来源行是否消失。
// 只比 sample_total 会漏掉构成变化（总数不变、异常数大幅变动）。
func incidentWindowInputsChanged(row IncidentIngestWindowState, d incidentWindowDecision, found bool) bool {
	baseSample, baseAnomaly, baseSuccess := row.SampleTotal, row.AnomalyTotal, row.SuccessTotal
	baseRate := row.AnomalyRate
	baseState, baseProof := row.WindowState, row.CoverageProof
	if row.MeasuredAt > 0 {
		baseSample, baseAnomaly, baseSuccess = row.MeasuredSampleTotal, row.MeasuredAnomalyTotal, row.MeasuredSuccessTotal
		baseRate = row.MeasuredAnomalyRate
		if row.MeasuredWindowState != "" {
			baseState = row.MeasuredWindowState
		}
		if row.MeasuredCoverageProof != "" {
			baseProof = row.MeasuredCoverageProof
		}
	}

	// 来源行消失：原先有样本、现在查不到匹配行，这本身就是要留痕的变化。
	if !found && baseSample != nil && *baseSample > 0 {
		return true
	}
	if row.DataComplete != incidentDecisionHasCompleteData(d) {
		return true
	}
	if incidentInt64PtrChanged(baseSample, d.SampleTotal) ||
		incidentInt64PtrChanged(baseAnomaly, d.AnomalyTotal) ||
		incidentInt64PtrChanged(baseSuccess, d.SuccessTotal) {
		return true
	}
	if incidentFloatPtrChanged(baseRate, d.AnomalyRate) {
		return true
	}
	if baseState != d.WindowState {
		return true
	}
	// 覆盖凭证变化意味着口径/覆盖范围变了，展示与完整性都要跟着更新。
	return baseProof != d.CoverageProof
}

// incidentInt64PtrChanged：nil 基准视为「尚无已知值」，有新值即为变化。
func incidentInt64PtrChanged(base *int64, now int64) bool {
	if base == nil {
		return now > 0
	}
	return *base != now
}

func incidentFloatPtrChanged(base, now *float64) bool {
	if base == nil && now == nil {
		return false
	}
	if base == nil || now == nil {
		return true
	}
	// 浮点异常率用一个远小于展示精度的容差比较，避免纯表示误差被记成修订。
	diff := *base - *now
	if diff < 0 {
		diff = -diff
	}
	return diff > 1e-9
}

// pruneIncidentIngest 归档调度。
//
// 两种归档各自独立事务，不并入其他清理事务。
func (m *Monitor) pruneIncidentIngest(ctx context.Context, now int64) error {
	retention := m.incidentIngestRetentionDays()
	var errs []string

	// 1. 缺口摘要：折叠已终结、复核已关闭、过 7 天补齐期与保留期的不完整窗口。
	if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
		_, err := m.archiveIncidentGapWindows(tx, retention, now)
		return err
	}); err != nil {
		errs = append(errs, err.Error())
	}

	// 2. 已完成且完整的窗口行按保留期删除（它们不阻挡水位）。
	if retention > 0 {
		if err := m.storeDB.WithContext(ctx).
			Where("process_state = ? AND data_complete = ? AND window_to_ts < ?",
				incidentProcessDone, true, now-int64(retention)*86400).
			Delete(&IncidentIngestWindowState{}).Error; err != nil {
			errs = append(errs, fmt.Sprintf("清理已完成窗口失败: %v", err))
		}
	}

	// 3. 影响面窗口贡献：只归档**已终结且冷却已过**事故的过期贡献。
	//    存续事故的明细要支撑详情与迟到修订，不能动。
	if retention > 0 {
		if err := m.archiveResolvedIncidentImpacts(ctx, retention, now); err != nil {
			errs = append(errs, err.Error())
		}
	}

	// 4. 修订记录：其关联窗口已不存在时才清理。
	if err := m.pruneOrphanIncidentRevisions(ctx); err != nil {
		errs = append(errs, err.Error())
	}

	// 5. 来源过期标记。
	//
	// ★ 必须真的赋 true ★
	//
	// 这个字段原来只有定义与响应读取，没有任何处理会置它为 true，接口文字
	// 却承诺「会明确标记来源过期」——于是它恒为 0，看上去永远「都可重放」。
	// 过了 ReplayableUntil 就意味着 CapacityUserMinuteSample 源行已被清理、
	// 该窗口不再可重放，必须如实标记，不删记录。
	if err := m.markExpiredIncidentSourceFacts(ctx, now); err != nil {
		errs = append(errs, err.Error())
	}

	if len(errs) > 0 {
		return fmt.Errorf("%s", strings.Join(errs, "; "))
	}
	return nil
}

// markExpiredIncidentSourceFacts 标记来源事实已过重放期的窗口。
//
// 只置标记，不删窗口行、不改判定：窗口仍要如实保留它当时的结论与完整性，
// 只是从此不能再靠重放来补齐。ReplayableUntil 为 0 的行表示未设置过期，
// 不在此列。
func (m *Monitor) markExpiredIncidentSourceFacts(ctx context.Context, now int64) error {
	if err := m.storeDB.WithContext(ctx).Model(&IncidentIngestWindowState{}).
		Where("replayable_until > 0 AND replayable_until < ? AND source_facts_expired = ?", now, false).
		Updates(map[string]any{"source_facts_expired": true, "updated_at": now}).Error; err != nil {
		return fmt.Errorf("标记来源事实过期失败: %w", err)
	}
	return nil
}

// archiveResolvedIncidentImpacts 归档已终结事故的过期影响面贡献。
func (m *Monitor) archiveResolvedIncidentImpacts(ctx context.Context, retentionDays int, now int64) error {
	before := now - int64(retentionDays)*86400
	var ids []string
	if err := m.storeDB.WithContext(ctx).Model(&Incident{}).
		Select("id").
		Where("status = ? AND active_marker <> ? AND cooldown_until_ts < ?",
			"resolved", incidentActiveMarker, now).
		Limit(500).Find(&ids).Error; err != nil {
		return fmt.Errorf("查可归档事故失败: %w", err)
	}
	for _, id := range ids {
		if err := m.incidentIngestTx(ctx, func(tx *gorm.DB) error {
			return m.archiveIncidentImpactWindows(tx, id, before, now)
		}); err != nil {
			return err
		}
	}
	return nil
}

// pruneOrphanIncidentRevisions 清理关联窗口已不存在的修订记录。
func (m *Monitor) pruneOrphanIncidentRevisions(ctx context.Context) error {
	err := m.storeDB.WithContext(ctx).Exec(`
		DELETE FROM incident_ingest_revisions
		WHERE NOT EXISTS (
			SELECT 1 FROM incident_ingest_window_states s
			WHERE s.dedup_key = incident_ingest_revisions.dedup_key
			  AND s.window_to_ts = incident_ingest_revisions.window_to_ts
		) AND NOT EXISTS (
			SELECT 1 FROM incident_ingest_gap_summaries g
			WHERE g.dedup_key = incident_ingest_revisions.dedup_key
			  AND g.gap_from_ts <= incident_ingest_revisions.window_from_ts
			  AND g.gap_through_ts >= incident_ingest_revisions.window_to_ts
		)`).Error
	if err != nil {
		return fmt.Errorf("清理孤立修订记录失败: %w", err)
	}
	return nil
}
