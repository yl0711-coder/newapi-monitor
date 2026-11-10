package monitor

// Incident 引擎的段、观测、连续状态与四层状态初始化（MON-003/004 B2）。
//
// 从 incident_engine.go 拆出，便于分别测试。所有写入只走 m.storeDB。

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// windowAlreadyFinalized 判断某个窗口右界是否已被判定过。
//
// 用于在**开段与改生命周期之前**拦住重复与乱序窗口。
// 读持久化水位而不是内存状态，所以重启后依然有效。
func (m *Monitor) windowAlreadyFinalized(tx *gorm.DB, incidentID string, windowToTs int64) (bool, error) {
	var row IncidentContinuity
	err := tx.Where("incident_id = ?", incidentID).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("读连续水位失败: %w", err)
	}
	if row.LastCountedWindowToTs != 0 && windowToTs == row.LastCountedWindowToTs {
		return true, nil
	}
	// 早于或等于已判定水位 → 旧窗口。
	if row.LastFinalizedWindowToTs != 0 && windowToTs <= row.LastFinalizedWindowToTs {
		return true, nil
	}
	return false, nil
}

// lastSegmentID 取事故最后一个段（不论是否已结束），用于给旧窗口补证据。
func (m *Monitor) lastSegmentID(tx *gorm.DB, incidentID string) (string, error) {
	var seg IncidentSegment
	if err := tx.Where("incident_id = ?", incidentID).
		Order("seq_no DESC").Take(&seg).Error; err != nil {
		return "", fmt.Errorf("取末段失败: %w", err)
	}
	return seg.ID, nil
}

// openSegment 新开一个异常段。当前段证据从零起算，不继承上一段。
func (m *Monitor) openSegment(tx *gorm.DB, incidentID string,
	in IncidentObservationInput, now int64) (string, error) {

	// 取下一个序号。唯一索引 (incident_id, seq_no) 防并发追加同号。
	var maxSeq int64
	if err := tx.Model(&IncidentSegment{}).Where("incident_id = ?", incidentID).
		Select("COALESCE(MAX(seq_no), 0)").Scan(&maxSeq).Error; err != nil {
		return "", fmt.Errorf("取段序号失败: %w", err)
	}
	seq := maxSeq + 1
	segID := incidentSegmentID(incidentID, seq)
	seg := &IncidentSegment{
		ID:         segID,
		IncidentID: incidentID,
		SeqNo:      seq,
		StartTs:    in.WindowFromTs,
		// 当前段证据：首窗即按本次观测起算。
		HardSignal:           in.HardSignal,
		MultiSourceConfirmed: in.MultiSourceConfirmed,
		CreatedAt:            now,
		UpdatedAt:            now,
	}
	r := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(seg)
	if r.Error != nil {
		return "", fmt.Errorf("建段失败: %w", r.Error)
	}
	if r.RowsAffected == 0 {
		// 并发下别人已建同号段，复用它。
		var exist IncidentSegment
		if err := tx.Where("incident_id = ? AND seq_no = ?", incidentID, seq).
			Take(&exist).Error; err != nil {
			return "", fmt.Errorf("并发建段后查既有段失败: %w", err)
		}
		return exist.ID, nil
	}
	return segID, nil
}

// currentSegmentID 取事故当前未结束的段；没有则新开一个。
//
// 事故活跃但无开放段，出现在「重启后首次观测」等情形，必须能自愈。
func (m *Monitor) currentSegmentID(tx *gorm.DB, incidentID string,
	in IncidentObservationInput, now int64) (string, error) {

	var seg IncidentSegment
	err := tx.Where("incident_id = ? AND end_ts = 0", incidentID).
		Order("seq_no DESC").Take(&seg).Error
	if err == nil {
		return seg.ID, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", fmt.Errorf("查当前段失败: %w", err)
	}
	return m.openSegment(tx, incidentID, in, now)
}

// closeSegment 结束一个段。
//
// 段结束只代表「异常观测段结束」，不代表渠道已恢复健康
// （后者属 D 期恢复门禁）。
func (m *Monitor) closeSegment(tx *gorm.DB, segmentID string, now int64) error {
	if err := tx.Model(&IncidentSegment{}).
		Where("id = ? AND end_ts = 0", segmentID).
		Updates(map[string]any{"end_ts": now, "updated_at": now}).Error; err != nil {
		return fmt.Errorf("结束段失败: %w", err)
	}
	return nil
}

// advanceAndDecide 推进连续窗口状态并判定严重度。
//
// 顺序严格按用户要求：先判故障场景，再验窗口/样本/多源条件
// （DecideIncidentSeverity 内部保证），不按窗口数量机械升级。
// 返回 outcome：重复/乱序窗口必须让上层知道，不能继续改生命周期。
func (m *Monitor) advanceAndDecide(tx *gorm.DB, cfg IncidentConfig,
	in IncidentObservationInput, incidentID, segmentID string, now int64) (string, IncidentWindowOutcome, error) {

	// 读回持久化的连续状态；没有则从零开始。
	var row IncidentContinuity
	err := tx.Where("incident_id = ?", incidentID).Take(&row).Error
	if err != nil && !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", "", fmt.Errorf("读连续状态失败: %w", err)
	}
	fresh := errors.Is(err, gorm.ErrRecordNotFound)

	// 当前段证据来自段，历史证据来自连续状态行——两者不混。
	var seg IncidentSegment
	if err := tx.Where("id = ?", segmentID).Take(&seg).Error; err != nil {
		return "", "", fmt.Errorf("读段失败: %w", err)
	}

	st := IncidentContinuityState{
		DedupKey:                 "",
		ConsecutiveWindows:       seg.ConsecutiveWindows,
		LastCountedWindowToTs:    row.LastCountedWindowToTs,
		LastFinalizedWindowToTs:  row.LastFinalizedWindowToTs,
		GapWindows:               row.GapWindows,
		LowSampleWindows:         row.LowSampleWindows,
		LastWindowSample:         row.LastWindowSample,
		HardSignal:               seg.HardSignal,
		MultiSourceConfirmed:     seg.MultiSourceConfirmed,
		EverHardSignal:           row.EverHardSignal,
		EverMultiSourceConfirmed: row.EverMultiSourceConfirmed,
		SampleScope:              IncidentSampleScope(row.SampleScope),
		WindowSeconds:            row.WindowSeconds,
		// 必须恢复：漏掉 LowSample 会让重复扫描或采集缺口在没有新证据的
		// 情况下把低样本标记清掉并重新升级，绕过样本门槛。
		LowSample: seg.LowSample,
		// 峰值是事故级的，存在 incidents 表。这里不填：
		// RecordPeakSeverity 只负责把本次判定并入 st.PeakSeverity，
		// 事故行的峰值由上层按 incidentSeverityRank 比较后更新
		// （见 updateActiveIncident / appendSegmentToResolved）。
		// 刻意留空，确保历史峰值不会反过来抬高当前段判定。
	}

	sample := int64(0)
	if in.SampleTotal != nil {
		sample = *in.SampleTotal
	}
	obs := IncidentWindowObservation{
		Window:      IncidentWindow{FromTs: in.WindowFromTs, ToTs: in.WindowToTs},
		Scope:       IncidentSampleScope(row.SampleScope),
		SampleTotal: sample,
		DataPresent: in.DataPresent,
		Anomalous:   in.Anomalous,
		HardSignal:  in.HardSignal,
		SourceCount: int(in.SourceCount),
		Final:       true,
	}
	outcome := AdvanceIncidentContinuity(&st, obs, int64(cfg.MinSample))

	// 重复扫描与乱序旧窗口：连续状态未推进，严重度也不得重算。
	// 直接返回段上已有的判定，并把 outcome 交给上层——
	// 上层据此跳过生命周期、冷却与峰值的任何修改。
	if outcome == OutcomeDuplicateIgnored || outcome == OutcomeStaleIgnored {
		return seg.Severity, outcome, nil
	}

	sev := DecideIncidentSeverity(IncidentSeverityInput{
		Scenario:            IncidentScenario(in.Scenario),
		Continuity:          st,
		DualSourceConfirmed: in.MultiSourceConfirmed,
	})
	RecordPeakSeverity(&st, sev)

	// 回写：当前段证据进段，历史证据与窗口水位进连续状态行。
	if err := tx.Model(&IncidentSegment{}).Where("id = ?", segmentID).
		Updates(map[string]any{
			"consecutive_windows":    st.ConsecutiveWindows,
			"hard_signal":            st.HardSignal,
			"multi_source_confirmed": st.MultiSourceConfirmed,
			"low_sample":             st.LowSample,
			"severity":               sev,
			"updated_at":             now,
		}).Error; err != nil {
		return "", "", fmt.Errorf("回写段证据失败: %w", err)
	}

	cont := &IncidentContinuity{
		IncidentID:               incidentID,
		LastCountedWindowToTs:    st.LastCountedWindowToTs,
		LastFinalizedWindowToTs:  st.LastFinalizedWindowToTs,
		LastWindowSample:         st.LastWindowSample,
		GapWindows:               st.GapWindows,
		LowSampleWindows:         st.LowSampleWindows,
		EverHardSignal:           st.EverHardSignal,
		EverMultiSourceConfirmed: st.EverMultiSourceConfirmed,
		SampleScope:              string(st.SampleScope),
		WindowSeconds:            st.WindowSeconds,
		UpdatedAt:                now,
	}
	if fresh {
		if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(cont).Error; err != nil {
			return "", "", fmt.Errorf("建连续状态失败: %w", err)
		}
	} else if err := tx.Model(&IncidentContinuity{}).
		Where("incident_id = ?", incidentID).Updates(map[string]any{
		"last_counted_window_to_ts":   cont.LastCountedWindowToTs,
		"last_finalized_window_to_ts": cont.LastFinalizedWindowToTs,
		"last_window_sample":          cont.LastWindowSample,
		"gap_windows":                 cont.GapWindows,
		"low_sample_windows":          cont.LowSampleWindows,
		"ever_hard_signal":            cont.EverHardSignal,
		"ever_multi_source_confirmed": cont.EverMultiSourceConfirmed,
		"sample_scope":                cont.SampleScope,
		"window_seconds":              cont.WindowSeconds,
		"updated_at":                  now,
	}).Error; err != nil {
		return "", "", fmt.Errorf("更新连续状态失败: %w", err)
	}
	return sev, outcome, nil
}

// observationLowSample 判断单个窗口是否样本不足。
//
// 三态，不能压成两态：
//   - 样本未知（SampleTotal=nil）→ false，未知不是「低」，否则缺口会被
//     当成低样本证据；区别保留在 SampleTotal 本身仍为 nil。
//   - 样本已知且 < 门限（含真实零值 0）→ true
//   - 样本已知且 >= 门限 → false
//
// 真实零值 0 与未知的区别必须保住：0 是「确实没有样本」，要判低样本；
// nil 是「算不出来」，不做判断。
func observationLowSample(sampleTotal *int64, minSample int64) bool {
	if sampleTotal == nil {
		return false
	}
	if minSample <= 0 {
		return false
	}
	return *sampleTotal < minSample
}

// replayTarget 是一个旧窗口的原始归属。
type replayTarget struct {
	IncidentID string
	SegmentID  string
}

// locateReplayTarget 按窗口反查该窗口原本写在哪个事故的哪个段上。
//
// 为什么不能用当前段或最后一段：旧窗口重放时，当前段可能已经是
// 第二段（甚至冷却后已新建另一个事故），把证据写到那里会造成
// 同一窗口的证据重复、且归属到错误的段/事故。
//
// 这里以同一 dedup_key 下已落库的观测行为准——观测行自己就记着
// incident_id 与 segment_id，是该窗口归属的权威来源。命中即说明
// 是重放，按原归属幂等写回（主键 = segmentID+windowToTs，
// OnConflict DoNothing 保证不堆重复）。
//
// 返回 nil 表示这是个没见过的窗口，按正常路径处理。
func (m *Monitor) locateReplayTarget(tx *gorm.DB, dedupKey string,
	in IncidentObservationInput) (*replayTarget, error) {

	// 限定在同一 dedup_key 的事故范围内，避免跨渠道/跨场景误匹配。
	var ids []string
	if err := tx.Model(&Incident{}).Where("dedup_key = ?", dedupKey).
		Pluck("id", &ids).Error; err != nil {
		return nil, fmt.Errorf("查同键事故失败: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}

	// 情形一：该窗口已入库 → 直接按原归属幂等写回。
	var ob IncidentObservation
	err := tx.Where("incident_id IN ? AND window_from_ts = ? AND window_to_ts = ?",
		ids, in.WindowFromTs, in.WindowToTs).
		Order("created_at ASC").Take(&ob).Error
	if err == nil {
		return &replayTarget{IncidentID: ob.IncidentID, SegmentID: ob.SegmentID}, nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, fmt.Errorf("查原窗口归属失败: %w", err)
	}

	// 情形二：首次迟到、从未入库的旧窗口。
	//
	// 没有观测行可依据，就按历史分段的时间范围定位：段的 start_ts/end_ts
	// 与 window_*_ts 同在「窗口时间轴」上（引擎的 now 即 WindowToTs），
	// 口径一致可直接比对。
	//
	// 必须这样做而不是挂当前事故/最后一段：迟到窗口在时间上属于更早的
	// 分段，直接挂当前段会把证据归到错误分段，冷却到期后甚至会挂到
	// 另一个事故上。
	return m.locateSegmentByWindowTime(tx, ids, in)
}

// locateSegmentByWindowTime 按窗口时间落在哪个历史分段内来定位归属。
//
// 只匹配**已结束**的段（end_ts > 0）且窗口完整落在 [start_ts, end_ts) 内。
// 仍未结束的段不在此处匹配：那是当前段，走正常路径即可。
//
// 匹配不到（窗口落在段之间的空隙、或早于第一段）返回 nil，
// 由调用方按「无归属的迟到窗口」处理，同样不得改生命周期。
func (m *Monitor) locateSegmentByWindowTime(tx *gorm.DB, incidentIDs []string,
	in IncidentObservationInput) (*replayTarget, error) {

	var seg IncidentSegment
	err := tx.Where("incident_id IN ? AND end_ts > 0 AND start_ts <= ? AND end_ts >= ?",
		incidentIDs, in.WindowFromTs, in.WindowToTs).
		Order("start_ts DESC").Take(&seg).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("按时间定位历史分段失败: %w", err)
	}
	return &replayTarget{IncidentID: seg.IncidentID, SegmentID: seg.ID}, nil
}

// ErrObservationUnattributed 表示该观测窗口无法确定归属分段。
//
// 出现它说明窗口落在分段空隙、事故空隙，或早于首段——这些位置上
// 没有任何分段覆盖该时间段。此时不能猜：把证据挂到当前段或最后一段
// 会写出错误的 incident_id/segment_id，比缺一条证据更难排查。
//
// 与本项目既有的「未归属成本」契约一致：归属不明时保持未归属、
// 明确报出，不按时间就近或按当前活跃对象猜测。
var ErrObservationUnattributed = errors.New("观测窗口无法确定归属分段")

// isStaleUnattributedWindow 判断这是不是一个「属于过去、但没有任何分段
// 覆盖」的窗口。
//
// 判据：同 dedup_key 下存在起点晚于本窗口起点的事故，说明本窗口在时间上
// 属于更早的时期，却没被任何分段认领（否则 locateReplayTarget 已经命中）。
// 三类位置都会走到这里——分段之间的空隙、事故之间的空隙、早于首段。
//
// 返回非 nil 即表示应当报错中止，而不是当作新窗口去挂当前活跃事故。
// 返回 nil 表示它确实是推进中的新窗口，照正常路径处理。
func (m *Monitor) isStaleUnattributedWindow(tx *gorm.DB, dedupKey string,
	in IncidentObservationInput) (*error, error) {

	var incs []Incident
	if err := tx.Where("dedup_key = ?", dedupKey).
		Order("first_seen_ts ASC").Find(&incs).Error; err != nil {
		return nil, fmt.Errorf("查同键事故失败: %w", err)
	}
	if len(incs) == 0 {
		return nil, nil // 首个事故尚未建立，是新窗口
	}

	// 只要有任一事故的起点不早于本窗口起点，本窗口就属于过去。
	// 用 >= 而非 >：与某事故同起点的窗口若未被分段认领，同样是空隙。
	var latest int64
	stale := false
	for _, inc := range incs {
		if inc.FirstSeenTs >= in.WindowFromTs {
			stale = true
		}
		if inc.FirstSeenTs > latest {
			latest = inc.FirstSeenTs
		}
	}
	if !stale {
		return nil, nil
	}

	err := fmt.Errorf(
		"%w: 迟到窗口 [%d,%d) 落在无分段覆盖处（同键事故共 %d 个，最晚起点 %d）",
		ErrObservationUnattributed, in.WindowFromTs, in.WindowToTs, len(incs), latest)
	return &err, nil
}

// resolveEvidenceSegment 给一条只补证据的观测确定最终归属段。
//
// fallback 是调用方原本打算用的段（当前段或最后一段）。只有当窗口
// 确实落在该段的时间范围内时才采用；否则按同事故的历史分段时间重新
// 定位。两者都不匹配时返回 ErrObservationUnattributed，由调用方
// 连带回滚——绝不退回 fallback。
//
// 无论走哪条分支都只决定证据挂哪里，不触碰生命周期、严重度、冷却、
// 水位与治理状态。
func (m *Monitor) resolveEvidenceSegment(tx *gorm.DB, incidentID, fallbackSegID string,
	in IncidentObservationInput) (replayTarget, error) {

	fb := replayTarget{IncidentID: incidentID, SegmentID: fallbackSegID}

	var seg IncidentSegment
	if err := tx.Where("id = ?", fallbackSegID).Take(&seg).Error; err != nil {
		return replayTarget{}, fmt.Errorf("%w: 事故 %s 的候选段 %s 读取失败: %w",
			ErrObservationUnattributed, incidentID, fallbackSegID, err)
	}
	// 窗口起点不早于该段起点即属于它（段未结束时 end_ts=0 视作仍在延续）。
	if in.WindowFromTs >= seg.StartTs && (seg.EndTs == 0 || in.WindowToTs <= seg.EndTs) {
		return fb, nil
	}

	// 候选段不含该窗口 → 在本事故的历史分段里按时间找。
	t, err := m.locateSegmentByWindowTime(tx, []string{incidentID}, in)
	if err != nil {
		return replayTarget{}, err
	}
	if t != nil {
		return *t, nil
	}

	// 都没匹配上：窗口落在没有任何分段覆盖的位置。报错而不是乱挂，
	// 错误里带齐窗口与候选段信息，便于直接定位是哪一类空隙。
	return replayTarget{}, fmt.Errorf(
		"%w: 窗口 [%d,%d) 不在事故 %s 的任何分段内（候选段 %s 范围 [%d,%d)）",
		ErrObservationUnattributed, in.WindowFromTs, in.WindowToTs,
		incidentID, fallbackSegID, seg.StartTs, seg.EndTs)
}

// writeObservation 写一条观测证据。错误原文与标识先过脱敏。
//
// minSample 来自配置，用于给本窗口独立判定 low_sample——
// 不能沿用段上的标记：段标记是连续链的聚合结论，
// 单窗记录必须反映这一窗自己的样本情况。
func (m *Monitor) writeObservation(tx *gorm.DB, incidentID, segmentID string,
	in IncidentObservationInput, minSample int64, now int64) error {

	note, redacted := stabilityProblemText(in.Note)
	ob := &IncidentObservation{
		ID:                 incidentObservationID(segmentID, in.WindowToTs),
		IncidentID:         incidentID,
		SegmentID:          segmentID,
		WindowFromTs:       in.WindowFromTs,
		WindowToTs:         in.WindowToTs,
		DataPresent:        in.DataPresent,
		Anomalous:          in.Anomalous,
		SourceCount:        in.SourceCount,
		SourceIDs:          in.SourceIDs,
		HardSignal:         in.HardSignal,
		DistinctRequests:   in.DistinctRequests,
		VisibleAttempts:    in.VisibleAttempts,
		LogRows:            in.LogRows,
		RowsWithoutChannel: in.RowsWithoutChannel,
		UnlinkableRows:     in.UnlinkableRows,
		AttemptsExact:      in.AttemptsExact,
		SampleTotal:        in.SampleTotal,
		LowSample:          observationLowSample(in.SampleTotal, minSample),
		EvidenceLevel:      in.EvidenceLevel,
		EvidenceMethod:     in.EvidenceMethod,
		Note:               note,
		Truncated:          redacted,
		CreatedAt:          now,
	}
	// 同一段同一窗口重复扫描：幂等忽略，不堆重复证据。
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(ob).Error; err != nil {
		return fmt.Errorf("写观测证据失败: %w", err)
	}
	return nil
}

// initIncidentState 初始化四层状态行，只写前两层。
//
// approved 只能由人工审批写入，applied 只能由只读同步写入——
// 引擎绝不代写这两层，这是「已审批≠已执行」的实现保证。
func (m *Monitor) initIncidentState(tx *gorm.DB, incidentID, severity string,
	in IncidentObservationInput, now int64) error {

	observed := governanceStateForSeverity(severity)
	recommended := governanceStateRecommendedFor(severity, in.EvidenceLevel)
	// 自造枚举值会在这里被挡住，不会落库。
	if !validGovernanceState(observed) || !validGovernanceState(recommended) {
		return fmt.Errorf("治理状态非法: observed=%q recommended=%q", observed, recommended)
	}
	row := &IncidentState{
		IncidentID:               incidentID,
		ObservedState:            observed,
		ObservedAt:               now,
		RecommendedState:         recommended,
		RecommendedAt:            now,
		RecommendedReason:        fmt.Sprintf("severity=%s evidence=%s", severity, in.EvidenceLevel),
		RecommendedEvidenceLevel: in.EvidenceLevel,
		// 第三、四层留空：分别等人工审批与只读同步。
		AppliedSyncOK: false,
		Revision:      1,
		UpdatedAt:     now,
	}
	if err := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(row).Error; err != nil {
		return fmt.Errorf("建四层状态行失败: %w", err)
	}
	return nil
}

// refreshIncidentState 同步更新前两层。
//
// 事故升级（如 SEV3→SEV1）后观测与建议状态必须跟着变，否则页面与下游
// 看到的仍是初始值。必须与严重度更新在同一事务内。
//
// 审批层（approved_*）与实际配置层（applied_newapi_*）绝不触碰：
// 前者只能人工写，后者只能由只读同步写。
func (m *Monitor) refreshIncidentState(tx *gorm.DB, incidentID, severity string,
	evidenceLevel string, now int64) error {

	observed := governanceStateForSeverity(severity)
	recommended := governanceStateRecommendedFor(severity, evidenceLevel)
	if !validGovernanceState(observed) || !validGovernanceState(recommended) {
		return fmt.Errorf("治理状态非法: observed=%q recommended=%q", observed, recommended)
	}

	// 只列前两层字段，物理上排除误写审批层与实际配置层的可能。
	updates := map[string]any{
		"observed_state":             observed,
		"observed_at":                now,
		"recommended_state":          recommended,
		"recommended_at":             now,
		"recommended_reason":         fmt.Sprintf("severity=%s evidence=%s", severity, evidenceLevel),
		"recommended_evidence_level": evidenceLevel,
		"updated_at":                 now,
		"revision":                   gorm.Expr("revision + 1"),
	}
	r := tx.Model(&IncidentState{}).Where("incident_id = ?", incidentID).Updates(updates)
	if r.Error != nil {
		return fmt.Errorf("更新前两层状态失败: %w", r.Error)
	}
	if r.RowsAffected == 0 {
		// 状态行缺失（历史数据或重启中断）：补建，不静默跳过。
		return m.initIncidentState(tx, incidentID, severity,
			IncidentObservationInput{EvidenceLevel: evidenceLevel}, now)
	}
	return nil
}

// governanceStateForSeverity 把严重度映射到观测层治理状态（01.3 五种枚举之一）。
//
// 这是「观测到什么」，不是「该怎么处置」——后者是建议层。
func governanceStateForSeverity(severity string) string {
	switch severity {
	case "SEV0", "SEV1":
		return governanceStateDegraded
	case "SEV2":
		return governanceStateSuspect
	default:
		// SEV3 含低样本与盲区标记情形：有疑点但不足以称降级。
		return governanceStateSuspect
	}
}

// governanceStateRecommendedFor 产出建议层治理状态。
//
// 01.3 §5 给每种证据等级规定了处置上限，严重度再高也不能越过：
//
//	exact       → 可到 QUARANTINED
//	correlated  → 最多 DEGRADED（隔离前需补强证据）
//	inferred    → 只用于排查方向，最多 SUSPECT
//	unavailable → 不支持处置结论，只能 SUSPECT
//
// 所以这里是「严重度给出诉求，证据等级设上限」，取两者的较弱者。
// 不能像先前那样把 correlated 与 exact 并列当强证据。
func governanceStateRecommendedFor(severity, evidenceLevel string) string {
	// 严重度诉求。
	want := governanceStateSuspect
	switch severity {
	case "SEV0", "SEV1":
		want = governanceStateQuarantined
	case "SEV2":
		want = governanceStateDegraded
	}

	// 证据上限。非法等级一律按最弱处理（调用方已先行校验并拒绝，
	// 这里再兜一层，确保任何路径都不会越权建议）。
	cap := governanceStateSuspect
	switch evidenceLevel {
	case evidenceLevelExact:
		cap = governanceStateQuarantined
	case evidenceLevelCorrelated:
		cap = governanceStateDegraded
	case evidenceLevelInferred, evidenceLevelUnavailable:
		cap = governanceStateSuspect
	}

	if governanceRank(want) <= governanceRank(cap) {
		return want
	}
	return cap
}

// governanceRank 给建议层用的强弱序。
//
// 只用于「取较弱者」的比较，不是治理状态的全序——
// RECOVERY_PROBATION 不参与建议，排在最低。
func governanceRank(s string) int {
	switch s {
	case governanceStateQuarantined:
		return 3
	case governanceStateDegraded:
		return 2
	case governanceStateSuspect:
		return 1
	default:
		return 0
	}
}

func incidentSegmentID(incidentID string, seq int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|seg|%d", incidentID, seq)))
	return hex.EncodeToString(sum[:])[:32]
}

func incidentObservationID(segmentID string, windowToTs int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|obs|%d", segmentID, windowToTs)))
	return hex.EncodeToString(sum[:])[:32]
}
