package monitor

// 单窗口的态判定与处理（MON-003 B3）。
//
// ★ 窗口态完整列表，互不冒充 ★
//
//	wait_coverage  覆盖未到且仍可能推进        → awaiting_source，不送引擎
//	mixed_version  本窗该键存在非当前口径版本行  → awaiting_source，不送引擎
//	gap_observed   水位已过但本窗覆盖不完整      → 送引擎 DataPresent=false
//	no_sample      覆盖完整但该键无匹配行        → **不送引擎**
//	normal         覆盖完整 + 有样本 + 未超阈值   → 送引擎 Anomalous=false
//	anomalous      覆盖完整 + 有样本 + 超阈值     → 送引擎 Anomalous=true
//
// ★ 为什么 no_sample 绝不送引擎 ★
//
// 引擎的更新路径里有 `if in.DataPresent && !in.Anomalous { status=resolved;
// closeSegment }`。把无样本传成 DataPresent=true/Anomalous=false 会让已有
// 活跃事故直接改 resolved 并关闭观测段——那就是把「没有流量」当成「已恢复」。
// 也不能改标成 DataPresent=false，那是采集缺失，是另一回事。
//
// 代价要如实承认：异常后彻底无流量的事故会长期挂 open，而**冷却不会结束它**
// （cooldown 只决定已结束事故能否被新异常复用），唯一结束路径是收到
// 「有样本且正常」的窗口。本轮由人工或后续恢复门禁收尾。

import (
	"context"
	"fmt"
)

// incidentWindowDecision 是一个聚合键在一个窗口上的判定结果。
type incidentWindowDecision struct {
	Key          IncidentKey
	DedupKey     string
	Canonical    string
	WindowFromTs int64
	WindowToTs   int64

	WindowState string
	// SendToEngine 为 false 时绝不调用 IngestIncidentObservation。
	SendToEngine bool
	// DataComplete 是「来源数据完整」，与流程是否跑完无关。
	DataComplete bool

	SampleTotal  int64
	AnomalyTotal int64
	SuccessTotal int64
	AnomalyRate  *float64
	MixedRows    int64

	BelowMinSample bool
	CoverageProof  string

	EvidenceLevel  string
	EvidenceMethod string
	Responsibility string
}

// incidentCoverageProof 把覆盖凭证编成可追溯的来源引用串。
func incidentCoverageProof(proof CustomerHealthHistoricalCoverage) string {
	return fmt.Sprintf("capacity_user_minute_samples;tcv=%d;req_through=%d;req_complete=%t",
		stabilityTrafficClassificationVersion, proof.RequestThroughTs, proof.RequestsComplete)
}

// decideIncidentWindow 判定单个聚合键在该窗口的状态。
//
// coverageFrontier 是「全局已证明前沿」，用于区分「断点就是水位本身（还会继续
// 推进）」与「水位已越过本窗但本窗仍不完整（缺天/跨日断裂/口径版本不符）」。
//
// 之所以需要这个额外判据：RequestsComplete 的算法是
// `requestOpen && RequestFromTs == from && RequestThroughTs >= to`，
// 任何不连续都会先落 complete=false 并把 RequestThroughTs 停在断点，
// 所以「已覆盖但存在不连续缺口」这种分支根本不可达。
func decideIncidentWindow(cfg IncidentConfig, ingest IncidentIngestConfig,
	facts incidentWindowFacts, hasFacts bool,
	proof CustomerHealthHistoricalCoverage, coverageFrontier int64,
	from, to int64) (incidentWindowDecision, error) {

	key, err := incidentKeyForFacts(facts)
	if err != nil {
		return incidentWindowDecision{}, err
	}
	level, method := incidentEvidenceForLocalFacts()
	d := incidentWindowDecision{
		Key: key, DedupKey: key.DedupKey(), Canonical: key.Canonical(),
		WindowFromTs: from, WindowToTs: to,
		CoverageProof:  incidentCoverageProof(proof),
		EvidenceLevel:  level,
		EvidenceMethod: method,
		Responsibility: incidentResponsibilityForLocalFacts(level),
		SuccessTotal:   facts.Success,
		AnomalyTotal:   facts.Anomaly + facts.Failed,
		SampleTotal:    facts.sampleTotal(),
		MixedRows:      facts.MixedVersionRows,
	}

	if !proof.RequestsComplete {
		if to > coverageFrontier {
			// 断点就是水位本身，覆盖仍在向前推进 → 继续等。
			d.WindowState = incidentWindowStateWaitCoverage
			return d, nil
		}
		// 水位已越过本窗但本窗仍不完整。**「当前确认存在缺口」，不是永久缺失**：
		// runCustomerHealthHistoryTurnWith 会在近 7 天内逐日补采，且
		// SemanticsVersion 不符时把 ThroughTs 重置回 day 重新采集。
		d.WindowState = incidentWindowStateGapObserved
		d.SendToEngine = true
		d.DataComplete = false
		return d, nil
	}

	// 覆盖完整。先看口径版本：混版不得「排除旧行后把剩余部分当完整样本」。
	if facts.MixedVersionRows > 0 {
		d.WindowState = incidentWindowStateMixedVersion
		return d, nil
	}

	if !hasFacts || d.SampleTotal <= 0 {
		// 覆盖完整但该键无匹配行 → 无样本。不送引擎（见文件头）。
		d.WindowState = incidentWindowStateNoSample
		d.DataComplete = true
		return d, nil
	}

	rate, hasSample := facts.anomalyRate()
	if !hasSample {
		d.WindowState = incidentWindowStateNoSample
		d.DataComplete = true
		return d, nil
	}
	d.AnomalyRate = &rate
	d.SendToEngine = true
	d.DataComplete = true
	// BelowMinSample 与正常/异常正交：低样本窗同样算异常率，可能超也可能不超。
	// 窗口态仍按阈值判，样本数如实传给引擎，由它按 MinSample 决定 Outcome。
	d.BelowMinSample = d.SampleTotal < int64(cfg.MinSample)
	if rate >= ingest.AnomalyRate {
		d.WindowState = incidentWindowStateAnomaly
	} else {
		d.WindowState = incidentWindowStateNormal
	}
	return d, nil
}

// observationInputFor 把判定结果转成引擎输入。
//
// 只在 SendToEngine 为真时调用。
func (d incidentWindowDecision) observationInputFor(minSample int) IncidentObservationInput {
	sample := d.SampleTotal
	in := IncidentObservationInput{
		Key:          d.Key,
		WindowFromTs: d.WindowFromTs,
		WindowToTs:   d.WindowToTs,
		// gap_observed → DataPresent=false：引擎据此走「未知不是异常」路径，
		// 无活跃事故时不建单，有活跃事故时推进缺口观测且不误判恢复。
		DataPresent: d.WindowState != incidentWindowStateGapObserved,
		Anomalous:   d.WindowState == incidentWindowStateAnomaly,
		// ★ Scenario 留空：「场景待确认」★
		//
		// site_wide_or_security 需硬信号或双源；primary_no_fallback 与
		// degraded_with_fallback 都依赖「有无备用渠道」这一配置事实。
		// 本轮单侧数据都不具备，所以不塞虚假场景、不用非法枚举强行接通。
		// 待确认这一状态由 IncidentIngestWindowState.ScenarioPending 显式记录，
		// 不靠「severity 为空」反推。
		Scenario:    "",
		SourceCount: 1,
		SourceIDs:   "capacity_user_minute_samples",
		// DistinctRequests 置 nil：分钟聚合桶无法诚实导出独立请求数，
		// 不用桶数冒充。VisibleAttempts / LogRows 同理。
		DistinctRequests: nil,
		VisibleAttempts:  nil,
		LogRows:          nil,
		AttemptsExact:    false,
		Responsibility:   d.Responsibility,
		EvidenceLevel:    d.EvidenceLevel,
		EvidenceMethod:   d.EvidenceMethod,
	}
	if d.WindowState != incidentWindowStateGapObserved {
		in.SampleTotal = &sample
	}
	if d.Key.ChannelScope == ChannelScopeUnknown {
		// 渠道信息没采到 → 如实记录为「无渠道归属的行」，不写 0 渠道。
		in.RowsWithoutChannel = sample
	}
	return in
}

// incidentCoverageFrontier 返回全局请求覆盖的已证明前沿。
//
// 用**只读**的 customerHealthHistoricalCoverage，绝不调
// loadCustomerHealthSourceCursor——后者会初始化水位、在语义迁移时开事务
// 回退 FRT 水位、还持 customerHealthCursorMu，且返回值被 clamp 到 FRT
// 覆盖而非请求覆盖。Incident 不能把它当查询工具。
//
// 只取 Request 三项，不与 FRT 回算绑定。
func (m *Monitor) incidentRequestCoverage(ctx context.Context, from, to int64) (CustomerHealthHistoricalCoverage, error) {
	return m.customerHealthHistoricalCoverage(ctx, from, to)
}

// incidentFinalizeFrontier 返回「可以开始处理」的窗口右界上限。
//
// 两条都必须满足才处理一个窗口：
//  1. window_to + FinalizeLagSeconds <= now
//  2. 请求覆盖已越过 window_to
//
// FinalizeLagSeconds 默认 25 分钟，大于客户维护 lane 的 20 分钟回放窗。
// 但这只是**本期 shadow 处理策略，不是最终性保证**——20 分钟只说明那条
// lane 会主动回放最近 20 分钟，并不证明日志最多迟到 20 分钟（上游写入延迟、
// 补偿任务、时钟偏移都可能更久）。实际迟到由复核机制观测，见 loop。
func incidentFinalizeFrontier(ingest IncidentIngestConfig, now int64) int64 {
	return now - int64(ingest.FinalizeLagSeconds)
}

// incidentWindowStateRowFrom 由判定结果构造状态行（尚未调引擎）。
func incidentWindowStateRowFrom(d incidentWindowDecision, retentionDays int, now int64) IncidentIngestWindowState {
	row := IncidentIngestWindowState{
		DedupKey:     d.DedupKey,
		WindowToTs:   d.WindowToTs,
		CanonicalKey: d.Canonical,
		WindowFromTs: d.WindowFromTs,
		WindowState:  d.WindowState,
		DataComplete: false,
		// 本轮能证明的场景集合为空，所以一律待确认。显式写入，不反推。
		ScenarioPending: true,
		PendingReason:   incidentPendingReasonNoFallback,
		MixedRows:       d.MixedRows,
		CoverageProof:   d.CoverageProof,
		EvidenceLevel:   d.EvidenceLevel,
		Responsibility:  d.Responsibility,
		BelowMinSample:  d.BelowMinSample,
		AnomalyRate:     d.AnomalyRate,
		CreatedAt:       now,
		UpdatedAt:       now,
	}
	if d.WindowState != incidentWindowStateWaitCoverage &&
		d.WindowState != incidentWindowStateMixedVersion {
		// 冻结判定输入快照。崩溃恢复用它重放，不重新聚合。
		sample, anomaly, success := d.SampleTotal, d.AnomalyTotal, d.SuccessTotal
		row.SampleTotal, row.AnomalyTotal, row.SuccessTotal = &sample, &anomaly, &success
	}
	if retentionDays > 0 {
		row.ReplayableUntil = d.WindowToTs + int64(retentionDays)*86400
	}
	return row
}

// incidentIngestRetentionDays 返回采集侧留存天数。
//
// 沿用 Settings.RetentionDays（分钟级本地留存，默认 7）——重放依赖的源表
// CapacityUserMinuteSample 就按它清理，采集状态保留更久也无法重放。
// 未配置时回落到与 customerHealthHistoryDays 一致的 7 天，而不是 0：
// 0 会让 prune 判断「永不过期」，带 data_complete=false 的行无界增长。
func (m *Monitor) incidentIngestRetentionDays() int {
	if m != nil && m.cfg.RetentionDays > 0 {
		return m.cfg.RetentionDays
	}
	return int(customerHealthHistoryDays)
}

// incidentGapRecheckDeadline 是缺口/混版的复核截止时刻。
//
// 与历史补采能力对齐：runCustomerHealthHistoryTurnWith 只回填近
// customerHealthHistoryDays(7) 天，所以超过这个期限再等也不会被补齐。
func incidentGapRecheckDeadline(windowToTs int64) int64 {
	return windowToTs + customerHealthHistoryDays*24*3600
}

// incidentSourceCheckBackoff 返回下次来源检查时刻。
//
// 退避是调度公平性的关键：没有它，每轮都会取最旧的同一批等待行，
// 其他聚合键的可处理任务永远排不上。
func incidentSourceCheckBackoff(attempts int, pollSeconds int, now int64) int64 {
	step := int64(pollSeconds)
	if step <= 0 {
		step = incidentIngestDefaultPollSeconds
	}
	delay := step
	for i := 0; i < attempts && delay < 1800; i++ {
		delay *= 2
	}
	if delay > 1800 {
		delay = 1800
	}
	return now + delay
}

// incidentRetryBackoff 失败窗口的重试退避。
func incidentRetryBackoff(attempts int, pollSeconds int, now int64) int64 {
	return incidentSourceCheckBackoff(attempts, pollSeconds, now)
}
