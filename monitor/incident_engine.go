package monitor

// Incident 引擎：把既有纯函数判据接到真实持久化写入（MON-003 B2）。
//
// 本文件是事故的**唯一业务写入入口**。复发三态、并发去重、重启续接
// 都在这里决定，而不是靠数据库索引——索引只保证「同键最多一个活跃事故」，
// 分辨「已结束但冷却未到期」与「已结束且冷却到期」必须读 status 与
// cooldown_until_ts，那是本文件的职责（见 incident_store.go 文件头）。
//
// 只读旁路：全程只用 m.storeDB（本地 SQLite），绝不触碰 m.prodDB，
// 也绝不修改 NewAPI 配置。

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// incidentWriteBudget 单次事故写入的总预算（含 BUSY 重试）。
// 取 3 秒与 incidentQueryTimeoutMS 一致：事故写入不该比查询更能占住本地库。
const incidentWriteBudget = 3 * time.Second

// IncidentObservationInput 是一次窗口观测的输入，由采集侧产出。
//
// 计数与样本用 *int64：算不出来是 nil，不是 0（未知不能填零）。
type IncidentObservationInput struct {
	// 聚合键构成（复用既有 IncidentKey，不另造一套字段）。
	Key IncidentKey

	// 窗口左闭右开，必须已对齐（调用方先过 AlignIncidentWindow）。
	WindowFromTs int64
	WindowToTs   int64

	// DataPresent=false 表示这一窗没有采集数据，既非正常也非异常。
	DataPresent bool
	Anomalous   bool

	// Scenario 是故障场景，严重度必须先判场景再验窗口/样本。
	Scenario string

	HardSignal           bool
	MultiSourceConfirmed bool
	SourceCount          int64
	SourceIDs            string

	DistinctRequests *int64
	VisibleAttempts  *int64
	LogRows          *int64
	SampleTotal      *int64

	RowsWithoutChannel int64
	UnlinkableRows     int64
	AttemptsExact      bool

	// 责任与证据分别判断，不由 fault_class 固定映射。
	Responsibility string
	EvidenceLevel  string
	EvidenceMethod string

	// Note 原文，写入前由本文件过脱敏。
	Note string
}

// IncidentIngestResult 报告本次观测对持久化状态做了什么，便于测试与排障。
type IncidentIngestResult struct {
	IncidentID string
	SegmentID  string
	// Action: created / updated / reopened / segment_appended / skipped_disabled / skipped_no_anomaly
	Action   string
	Severity string
	// Reused=true 表示复用了已存在的事故（未新建）。
	Reused bool
	// Outcome 是连续窗口判定结果（duplicate_ignored / stale_ignored 等），
	// 便于排查「为什么这次观测没改变事故状态」。
	Outcome string
}

// IngestIncidentObservation 是事故的唯一业务写入入口。
//
// 复发三态在此决定：
//   - 活跃事故存在 → 更新它，冷却到期也不新建（ShouldReopenAfterCooldown 返回 false）
//   - 已结束且冷却未到期 → 复用原事故，追加新段
//   - 已结束且冷却到期 → 新建事故，原事故历史保留
func (m *Monitor) IngestIncidentObservation(ctx context.Context, in IncidentObservationInput) (IncidentIngestResult, error) {
	cfg := LoadIncidentConfig()
	if !cfg.Enabled {
		// 功能关闭：不启动引擎、不写业务数据。建表已在 openStore 完成，与此无关。
		return IncidentIngestResult{Action: "skipped_disabled"}, nil
	}
	if m.storeDB == nil {
		return IncidentIngestResult{}, fmt.Errorf("本地库未就绪，拒绝写入事故")
	}
	if in.WindowToTs <= in.WindowFromTs {
		return IncidentIngestResult{}, fmt.Errorf("事故观测窗口非法: [%d,%d)", in.WindowFromTs, in.WindowToTs)
	}

	if !in.Key.ChannelScope.Valid() {
		return IncidentIngestResult{}, fmt.Errorf("渠道归属 %q 非法", in.Key.ChannelScope)
	}
	// 证据等级必须是 01.3 §5 的四种之一。空串与自造值（indicative、
	// insufficient_evidence 等）一律拒绝，不能落库后再影响处置建议。
	if !validEvidenceLevel(in.EvidenceLevel) {
		return IncidentIngestResult{}, fmt.Errorf(
			"证据等级 %q 非法，须为 exact/correlated/inferred/unavailable 之一", in.EvidenceLevel)
	}
	dedupKey := in.Key.DedupKey()
	canonical := in.Key.Canonical()
	now := in.WindowToTs

	var res IncidentIngestResult
	err := m.incidentWrite(ctx, func(tx *gorm.DB) error {
		var err error
		res, err = m.ingestIncidentTx(tx, cfg, in, dedupKey, canonical, now)
		return err
	})
	return res, err
}

// ingestIncidentTx 在一个事务内完成「查活跃事故 → 定三态 → 写事故/段/观测」。
//
// 整段必须在同一事务内：读与写之间若被另一个 goroutine 插入新事故，
// 先查后写就会产生重复活跃事故。
func (m *Monitor) ingestIncidentTx(tx *gorm.DB, cfg IncidentConfig,
	in IncidentObservationInput, dedupKey, canonical string, now int64) (IncidentIngestResult, error) {

	// ★ 最先处理旧窗口重放 ★
	//
	// 该窗口若已落过库，按它**原本**的事故与段幂等写回，不碰当前段、
	// 不碰最后一段、也不碰任何生命周期字段。放在最前面是因为冷却后
	// 可能已经新建了另一个事故，此时连「取活跃事故」都会指向错误目标。
	if t, err := m.locateReplayTarget(tx, dedupKey, in); err != nil {
		return IncidentIngestResult{}, err
	} else if t != nil {
		if err := m.writeObservation(tx, t.IncidentID, t.SegmentID, in,
			int64(cfg.MinSample), now); err != nil {
			return IncidentIngestResult{}, err
		}
		var sev string
		var seg IncidentSegment
		if err := tx.Where("id = ?", t.SegmentID).Take(&seg).Error; err == nil {
			sev = seg.Severity
		}
		return IncidentIngestResult{IncidentID: t.IncidentID, SegmentID: t.SegmentID,
			Action: "evidence_only", Severity: sev, Reused: true,
			Outcome: string(OutcomeStaleIgnored)}, nil
	}

	// 旧窗口但找不到归属分段 → 明确报错，不让它落到正常路径。
	//
	// 否则下面会把它当新窗口处理，挂到当前活跃事故（甚至是起点远在
	// 该窗口之后的新事故），写出错误的 incident_id/segment_id。
	// 事务整体回滚，生命周期、水位、治理状态均不受影响。
	if stale, err := m.isStaleUnattributedWindow(tx, dedupKey, in); err != nil {
		return IncidentIngestResult{}, err
	} else if stale != nil {
		return IncidentIngestResult{}, *stale
	}

	// 取同键的活跃事故（最多一条，由唯一索引保证）。
	var active Incident
	activeErr := tx.Where("dedup_key = ? AND active_marker = ?", dedupKey, incidentActiveMarker).
		Take(&active).Error
	hasActive := activeErr == nil
	if activeErr != nil && !errors.Is(activeErr, gorm.ErrRecordNotFound) {
		return IncidentIngestResult{}, fmt.Errorf("查活跃事故失败: %w", activeErr)
	}

	// 没有活跃事故、本窗又不异常 → 不建单。
	// 采集缺口（DataPresent=false）同样不建单：未知不是异常。
	if !hasActive && (!in.Anomalous || !in.DataPresent) {
		return IncidentIngestResult{Action: "skipped_no_anomaly"}, nil
	}

	if hasActive {
		return m.updateActiveIncident(tx, cfg, in, &active, now)
	}

	// 无活跃事故：检查同键最近一条已结束事故，决定复用还是新建。
	var last Incident
	lastErr := tx.Where("dedup_key = ? AND active_marker <> ?", dedupKey, incidentActiveMarker).
		Order("last_seen_ts DESC").Take(&last).Error
	if lastErr != nil && !errors.Is(lastErr, gorm.ErrRecordNotFound) {
		return IncidentIngestResult{}, fmt.Errorf("查历史事故失败: %w", lastErr)
	}
	if lastErr == nil {
		// 已结束。冷却到期才新建；未到期则复用原事故并追加新段。
		if !ShouldReopenAfterCooldown(true, now, last.CooldownUntilTs) {
			return m.appendSegmentToResolved(tx, cfg, in, &last, now)
		}
	}
	return m.createIncident(tx, cfg, in, dedupKey, canonical, now)
}

// updateActiveIncident 更新仍活跃的事故。冷却到期**不**新建。
func (m *Monitor) updateActiveIncident(tx *gorm.DB, cfg IncidentConfig,
	in IncidentObservationInput, inc *Incident, now int64) (IncidentIngestResult, error) {

	segID, err := m.currentSegmentID(tx, inc.ID, in, now)
	if err != nil {
		return IncidentIngestResult{}, err
	}
	sev, outcome, err := m.advanceAndDecide(tx, cfg, in, inc.ID, segID, now)
	if err != nil {
		return IncidentIngestResult{}, err
	}

	// 重复扫描或乱序旧窗口：只补证据，不动生命周期、严重度与冷却。
	// 先前漏掉这一分支，导致「新异常之后收到旧正常窗口」会把仍活跃的
	// 事故误改成 resolved。
	if outcome == OutcomeDuplicateIgnored || outcome == OutcomeStaleIgnored {
		// 归属仍要按窗口时间核对：走到这里的窗口未入库过，
		// 若它其实早于当前段起点，就不能挂当前段。
		target, err := m.resolveEvidenceSegment(tx, inc.ID, segID, in)
		if err != nil {
			return IncidentIngestResult{}, err
		}
		if err := m.writeObservation(tx, target.IncidentID, target.SegmentID, in,
			int64(cfg.MinSample), now); err != nil {
			return IncidentIngestResult{}, err
		}
		return IncidentIngestResult{IncidentID: target.IncidentID, SegmentID: target.SegmentID,
			Action: "evidence_only", Severity: sev, Reused: true, Outcome: string(outcome)}, nil
	}

	updates := map[string]any{
		"last_seen_ts": gorm.Expr("MAX(last_seen_ts, ?)", now),
		"severity":     sev,
		"updated_at":   now,
		"revision":     gorm.Expr("revision + 1"),
		// 冷却期内继续检测与更新证据，不暂停处理：每次观测都把冷却推后。
		"cooldown_until_ts": cfg.CooldownUntil(now, sev),
	}
	if incidentSeverityRank(sev) > incidentSeverityRank(inc.PeakSeverity) {
		updates["peak_severity"] = sev
	}
	// 本窗判为正常 → 事故观测段结束。
	// 注意：这只表示「观测段结束」，不等于渠道已恢复健康——
	// 后者是 D 期恢复门禁的结论，本期不产出。
	if in.DataPresent && !in.Anomalous {
		updates["status"] = "resolved"
		updates["resolved_ts"] = now
		// 让出活跃槽位，marker 改为该事故自身派生值。
		updates["active_marker"] = incidentHistoryMarker(inc.ID)
		if err := m.closeSegment(tx, segID, now); err != nil {
			return IncidentIngestResult{}, err
		}
	}

	// 乐观并发：按读到的 revision 做 CAS，冲突则让调用方重试整个事务。
	r := tx.Model(&Incident{}).
		Where("id = ? AND revision = ?", inc.ID, inc.Revision).Updates(updates)
	if r.Error != nil {
		return IncidentIngestResult{}, fmt.Errorf("更新事故失败: %w", r.Error)
	}
	if r.RowsAffected == 0 {
		return IncidentIngestResult{}, fmt.Errorf("事故 %s 版本冲突（revision=%d），需重试", inc.ID, inc.Revision)
	}
	// 同一事务内同步前两层：升级后 observed/recommended 必须跟着变。
	if err := m.refreshIncidentState(tx, inc.ID, sev, in.EvidenceLevel, now); err != nil {
		return IncidentIngestResult{}, err
	}
	if err := m.writeObservation(tx, inc.ID, segID, in, int64(cfg.MinSample), now); err != nil {
		return IncidentIngestResult{}, err
	}
	return IncidentIngestResult{IncidentID: inc.ID, SegmentID: segID,
		Action: "updated", Severity: sev, Reused: true, Outcome: string(outcome)}, nil
}

// appendSegmentToResolved 复用已结束但冷却未到期的事故，追加新异常段。
func (m *Monitor) appendSegmentToResolved(tx *gorm.DB, cfg IncidentConfig,
	in IncidentObservationInput, inc *Incident, now int64) (IncidentIngestResult, error) {

	if !in.Anomalous || !in.DataPresent {
		// 冷却期内的正常窗口无需处理：事故已结束。
		return IncidentIngestResult{IncidentID: inc.ID, Action: "skipped_no_anomaly", Reused: true}, nil
	}

	// ★ 开段之前先核验水位 ★
	//
	// 必须在 openSegment 之前判断：重放一个已判定过的旧异常窗口
	// 不得让已结束的事故重新 open，也不得凭空追加第二段。
	// 先前把这个检查放在开段之后，导致「事故结束后重放最初的异常窗口」
	// 会把事故改回 open 并多出一个段。
	stale, err := m.windowAlreadyFinalized(tx, inc.ID, in.WindowToTs)
	if err != nil {
		return IncidentIngestResult{}, err
	}
	if stale {
		// 按窗口时间定位归属，不能直接用最后一段：
		// 迟到窗口可能属于更早的分段。
		lastSeg, err := m.lastSegmentID(tx, inc.ID)
		if err != nil {
			return IncidentIngestResult{}, err
		}
		target, err := m.resolveEvidenceSegment(tx, inc.ID, lastSeg, in)
		if err != nil {
			return IncidentIngestResult{}, err
		}
		if err := m.writeObservation(tx, target.IncidentID, target.SegmentID, in,
			int64(cfg.MinSample), now); err != nil {
			return IncidentIngestResult{}, err
		}
		return IncidentIngestResult{IncidentID: target.IncidentID, SegmentID: target.SegmentID,
			Action: "evidence_only", Severity: inc.Severity, Reused: true,
			Outcome: string(OutcomeStaleIgnored)}, nil
	}

	// 新段：当前段证据从零开始，不继承上一段。
	segID, err := m.openSegment(tx, inc.ID, in, now)
	if err != nil {
		return IncidentIngestResult{}, err
	}
	sev, outcome, err := m.advanceAndDecide(tx, cfg, in, inc.ID, segID, now)
	if err != nil {
		return IncidentIngestResult{}, err
	}

	// 重新激活：抢回活跃槽位。若此刻已有别人抢到，唯一索引会拒绝，
	// 调用方重试时会走到 updateActiveIncident 分支。
	reopen := map[string]any{
		"status":            "open",
		"active_marker":     incidentActiveMarker,
		"resolved_ts":       0,
		"last_seen_ts":      gorm.Expr("MAX(last_seen_ts, ?)", now),
		"severity":          sev,
		"cooldown_until_ts": cfg.CooldownUntil(now, sev),
		"updated_at":        now,
		"revision":          gorm.Expr("revision + 1"),
	}
	// 复发比原先更严重时要抬高历史峰值（如原 SEV3，冷却期内以 SEV1 复发）。
	// 反向不成立：峰值只记录历史，不能回头抬高当前段判定。
	if incidentSeverityRank(sev) > incidentSeverityRank(inc.PeakSeverity) {
		reopen["peak_severity"] = sev
	}
	r := tx.Model(&Incident{}).
		Where("id = ? AND revision = ?", inc.ID, inc.Revision).
		Updates(reopen)
	if r.Error != nil {
		return IncidentIngestResult{}, fmt.Errorf("复发重新激活事故失败: %w", r.Error)
	}
	if r.RowsAffected == 0 {
		return IncidentIngestResult{}, fmt.Errorf("事故 %s 版本冲突，需重试", inc.ID)
	}
	// 复发同样要同步前两层。
	if err := m.refreshIncidentState(tx, inc.ID, sev, in.EvidenceLevel, now); err != nil {
		return IncidentIngestResult{}, err
	}
	if err := m.writeObservation(tx, inc.ID, segID, in, int64(cfg.MinSample), now); err != nil {
		return IncidentIngestResult{}, err
	}
	return IncidentIngestResult{IncidentID: inc.ID, SegmentID: segID,
		Action: "segment_appended", Severity: sev, Reused: true, Outcome: string(outcome)}, nil
}

// createIncident 新建事故。原有历史事故保留，不覆盖。
func (m *Monitor) createIncident(tx *gorm.DB, cfg IncidentConfig,
	in IncidentObservationInput, dedupKey, canonical string, now int64) (IncidentIngestResult, error) {

	incID := incidentInstanceID(dedupKey, now)
	inc := &Incident{
		ID:                incID,
		DedupKey:          dedupKey,
		DedupKeyCanonical: canonical,
		ActiveMarker:      incidentActiveMarker,
		Status:            "open",
		PrimaryFaultClass: in.Key.PrimaryFaultClass,
		Scenario:          in.Scenario,
		Responsibility:    in.Responsibility,
		EvidenceLevel:     in.EvidenceLevel,
		ChannelScope:      string(in.Key.ChannelScope),
		ChannelID:         in.Key.ChannelID,
		Model:             in.Key.Model,
		UserGroup:         in.Key.UserGroup,
		IngressProtocol:   string(in.Key.IngressProtocol),
		Route:             in.Key.Route,
		ConfigChangeRef:   in.Key.ConfigChangeRef,
		FirstSeenTs:       in.WindowFromTs,
		LastSeenTs:        now,
		Revision:          1,
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	// 并发：同键只能成一条活跃事故，第二条由唯一索引拒绝。
	// 用 DoNothing 吸收冲突，再查出胜者复用——不先查后写。
	r := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(inc)
	if r.Error != nil {
		return IncidentIngestResult{}, fmt.Errorf("新建事故失败: %w", r.Error)
	}
	if r.RowsAffected == 0 {
		// 被别的 goroutine 抢先。复用胜者，不重复建单。
		var winner Incident
		if err := tx.Where("dedup_key = ? AND active_marker = ?", dedupKey, incidentActiveMarker).
			Take(&winner).Error; err != nil {
			return IncidentIngestResult{}, fmt.Errorf("并发冲突后查胜者失败: %w", err)
		}
		return m.updateActiveIncident(tx, cfg, in, &winner, now)
	}

	segID, err := m.openSegment(tx, incID, in, now)
	if err != nil {
		return IncidentIngestResult{}, err
	}
	sev, outcome, err := m.advanceAndDecide(tx, cfg, in, incID, segID, now)
	if err != nil {
		return IncidentIngestResult{}, err
	}
	_ = outcome // 新建事故时连续状态必为空，不会是 duplicate/stale。
	if err := tx.Model(&Incident{}).Where("id = ?", incID).
		Updates(map[string]any{"severity": sev, "peak_severity": sev,
			"cooldown_until_ts": cfg.CooldownUntil(now, sev),
			"revision":          gorm.Expr("revision + 1")}).Error; err != nil {
		return IncidentIngestResult{}, fmt.Errorf("写入初始严重度失败: %w", err)
	}

	// 四层状态行：只写前两层。approved 只能由人工写，applied 只能由只读同步写。
	if err := m.initIncidentState(tx, incID, sev, in, now); err != nil {
		return IncidentIngestResult{}, err
	}
	if err := m.writeObservation(tx, incID, segID, in, int64(cfg.MinSample), now); err != nil {
		return IncidentIngestResult{}, err
	}
	return IncidentIngestResult{IncidentID: incID, SegmentID: segID,
		Action: "created", Severity: sev}, nil
}

// incidentInstanceID 生成事故实例 ID。
//
// 含 firstSeen 时间戳，所以同一聚合键的多次复发得到不同 ID，
// 历史事故不会被覆盖。
func incidentInstanceID(dedupKey string, firstSeenTs int64) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s|%d", dedupKey, firstSeenTs)))
	return hex.EncodeToString(sum[:])[:32]
}

// incidentHistoryMarker 把事故 ID 映射成一个稳定的非 1 marker，
// 用于事故结束时让出活跃槽位。
//
// 必须对同一 ID 恒定：重启后重复结束同一事故要得到同样的 marker，
// 否则会在历史行里留下重复记录。
func incidentHistoryMarker(incidentID string) int64 {
	sum := sha256.Sum256([]byte(incidentID))
	var v int64
	for i := 0; i < 7; i++ {
		v = v<<8 | int64(sum[i])
	}
	if v <= incidentActiveMarker {
		v += incidentActiveMarker + 1
	}
	return v
}

// incidentWrite 统一的本地写入包装：超时预算 + WAL BUSY 重试。
//
// 重试必须重跑整个事务（含读），不能只重试最后那条 INSERT——
// 读到的 revision 在重试时可能已过期。
func (m *Monitor) incidentWrite(ctx context.Context, fn func(*gorm.DB) error) error {
	ctx, cancel := context.WithTimeout(ctx, incidentWriteBudget)
	defer cancel()
	return retryStabilityLocalWrite(ctx, func(attempt context.Context) error {
		return m.storeDB.WithContext(attempt).Transaction(fn)
	})
}
