package monitor

// 连续异常窗口判定。
//
// 窗口固定对齐到 5 分钟格栅（既有时间格栅：采样器 60 秒一桶、聚合到 hour_ts；
// 5 分钟 = 5 个分钟桶，不另造格栅）。判定门槛严格按 02.1 §8，不自行降低。
//
// 这个文件要同时守住六条，每条都靠结构而不是调用约定：
//  1. 同一窗口重复扫描只计一次（幂等）
//  2. 乱序到达的旧窗口不重复累计
//  3. 缺窗既不算异常达标、也不算恢复
//  4. 样本不足不算异常达标，但仍要展示、不得当作正常
//  5. 正常窗口打断异常连续计数
//  6. 重启后计数不丢（全部字段持久化，不放内存）

import "fmt"

// IncidentWindowSeconds 是窗口粒度默认值（秒）。
//
// 5 分钟的依据：1 分钟样本太少，大量窗口会被最小样本门拦下，连续判定几乎不成立；
// 1 小时则 SEV2 的「至少三个窗口」要 3 小时才确认，太慢。
// 可由 MONITOR_INCIDENT_WINDOW_SECONDS 覆盖，夹取 [60,3600] 且必须为 60 的整数倍
// （否则窗口边界会切开分钟桶）。
const IncidentWindowSeconds = 300

// IncidentMinSample 是统计类规则的最小样本默认值。
//
// 只约束**统计类**规则（比率、分位数一类需要样本量才有意义的判据）。
// 硬信号不受它约束——见 IncidentWindowVerdict 的 HardSignal 分支。
const IncidentMinSample = 20

// 连续窗口门槛，按 02.1 §8，不得下调。
const (
	// SEV1：连续两个窗口，或单一来源 + 其他来源佐证。
	incidentSev1ConsecutiveWindows = 2
	// SEV2：满足最小样本且持续至少三个窗口。
	incidentSev2ConsecutiveWindows = 3
)

// IncidentWindow 是一个已对齐的统计窗口，左闭右开 [FromTs, ToTs)。
type IncidentWindow struct {
	FromTs int64
	ToTs   int64
}

// AlignIncidentWindow 把任意时刻对齐到固定窗口格栅。
//
// 固定对齐（绝对时间整除）而不是「从首次异常起算」：后者会让同一事故在不同
// 副本、不同扫描时刻产生错位窗口，相邻性判断随之失效。
func AlignIncidentWindow(ts int64, windowSeconds int64) IncidentWindow {
	if windowSeconds <= 0 {
		windowSeconds = IncidentWindowSeconds
	}
	from := ts - ts%windowSeconds
	return IncidentWindow{FromTs: from, ToTs: from + windowSeconds}
}

// IncidentSampleScope 说明样本分母的口径。
//
// 必须显式携带，否则「最小样本 20」是对什么的 20 无法追溯。
type IncidentSampleScope string

const (
	// 分母 = 该窗口内落在同一聚合键上的请求总数（成功 + 平台失败 + 降级成功）。
	// 用户拒绝不进分母、未知单列——沿用既有平台稳定率口径。
	IncidentSampleScopeKeyedRequests IncidentSampleScope = "keyed_requests"
)

// IncidentWindowObservation 是单个窗口的观测输入。
type IncidentWindowObservation struct {
	Window IncidentWindow
	Scope  IncidentSampleScope

	// SampleTotal 是样本分母；AnomalyCount 是其中的异常数。
	SampleTotal  int64
	AnomalyCount int64

	// DataPresent=false 表示该窗口没有可用数据（采集缺口、源不可用）。
	// 注意它与 SampleTotal==0 不同：后者是「查成功了，确实零流量」。
	DataPresent bool

	// Anomalous 是该窗口是否判为异常（由上层统计规则给出）。
	Anomalous bool

	// HardSignal 为真时绕过最小样本门（02.1 §8：硬信号可立即触发）。
	HardSignal bool

	// SourceCount 是本窗口确认该异常的独立来源数，用于 SEV1 的多源佐证分支。
	SourceCount int

	// Final=false 表示窗口尚未收齐数据（右界未到或采集仍在追尾）。
	// 未完成的窗口不得当成最终结论。
	Final bool
}

// IncidentContinuityState 是持久化的连续判定状态。
//
// 全部字段落库（见 store 的 IncidentContinuity 表），不放内存——重启后计数不丢。
type IncidentContinuityState struct {
	DedupKey string

	ConsecutiveWindows int64
	// LastCountedWindowToTs 是最后一个**已计入**连续计数的窗口右界。
	// 幂等与相邻性全靠它，不靠扫描时刻。
	LastCountedWindowToTs int64

	// LastFinalizedWindowToTs 是最后一个已判定（含判为正常）的窗口右界，
	// 用于识别乱序到达的旧窗口。
	LastFinalizedWindowToTs int64

	GapWindows       int64
	LowSampleWindows int64

	BaselineValue  *float64
	BaselineSample int64

	LastWindowSample int64
	LowSample        bool

	// HardSignal / MultiSourceConfirmed 只描述**当前连续异常段**，
	// 正常窗口打断异常段时与 ConsecutiveWindows 一同重置。
	//
	// 不重置会让恢复后的新一段异常绕过自己的确认门槛：
	// 双源（或硬信号）异常 → 正常窗口 → 一个单源非硬信号异常窗口，
	// 此时 ConsecutiveWindows 已重算为 1，却仍因旧标记被判 SEV1。
	// 严重度判定只读这两个字段，不读下面的历史字段。
	HardSignal           bool
	MultiSourceConfirmed bool

	// EverHardSignal / EverMultiSourceConfirmed 是**历史**证据，跨异常段保留，
	// 供审计与排障参考。**不得**作为新一段异常的硬信号或多源确认。
	EverHardSignal           bool
	EverMultiSourceConfirmed bool

	// PeakSeverity 是本事故生命周期内达到过的最高严重度（SEV0 最高）。
	// 历史最高严重度与当前段的判定条件是两件事：它只用于展示与审计，
	// 不参与 DecideIncidentSeverity 的门槛判断。
	PeakSeverity string

	SampleScope   IncidentSampleScope
	WindowSeconds int64
}

// IncidentWindowOutcome 说明一次 Advance 调用对状态做了什么，便于审计与测试断言。
type IncidentWindowOutcome string

const (
	// 计入连续计数。
	OutcomeCounted IncidentWindowOutcome = "counted"
	// 同一窗口重复扫描，已忽略（幂等）。
	OutcomeDuplicateIgnored IncidentWindowOutcome = "duplicate_ignored"
	// 乱序到达的旧窗口，已忽略。
	OutcomeStaleIgnored IncidentWindowOutcome = "stale_ignored"
	// 窗口尚未收齐，不作最终判定。
	OutcomePendingNotFinal IncidentWindowOutcome = "pending_not_final"
	// 采集缺口：计数冻结，既不递增也不清零。
	OutcomeGapFrozen IncidentWindowOutcome = "gap_frozen"
	// 样本不足：不计入连续达标，但保留展示。
	OutcomeLowSampleHeld IncidentWindowOutcome = "low_sample_held"
	// 正常窗口：打断连续计数。
	OutcomeNormalReset IncidentWindowOutcome = "normal_reset"
	// 窗口不相邻（缺口之后的第一个窗口）：连续计数重新起算。
	OutcomeRestartedAfterGap IncidentWindowOutcome = "restarted_after_gap"
)

// AdvanceIncidentContinuity 用一个窗口观测推进连续判定状态。
//
// 返回 outcome 便于上层审计；state 原地更新。调用方必须把更新后的 state
// 与窗口进度在**同一个事务**里持久化（见 incident_store.go），
// 否则重启后会出现「计数已推进但窗口进度未落」的不一致。
func AdvanceIncidentContinuity(
	st *IncidentContinuityState,
	obs IncidentWindowObservation,
	minSample int64,
) IncidentWindowOutcome {
	if minSample <= 0 {
		minSample = IncidentMinSample
	}
	st.SampleScope = obs.Scope
	if obs.Window.ToTs > obs.Window.FromTs {
		st.WindowSeconds = obs.Window.ToTs - obs.Window.FromTs
	}

	// 窗口尚未收齐：不做任何最终判定，也不推进任何进度。
	// 迟到数据会在窗口 Final 之后再次送进来，那时才判定（见文件末尾说明）。
	if !obs.Final {
		return OutcomePendingNotFinal
	}

	// 同一窗口重复扫描：右界相等即幂等忽略。
	// 这里不能用「>=」合并下面的乱序分支——两者语义不同，审计要分得开。
	if obs.Window.ToTs == st.LastCountedWindowToTs ||
		(st.LastFinalizedWindowToTs != 0 && obs.Window.ToTs == st.LastFinalizedWindowToTs) {
		return OutcomeDuplicateIgnored
	}

	// 乱序到达的旧窗口：右界早于已判定水位，忽略，不重复累计。
	if st.LastFinalizedWindowToTs != 0 && obs.Window.ToTs < st.LastFinalizedWindowToTs {
		return OutcomeStaleIgnored
	}

	// 采集缺口：计数冻结。
	// 不递增（缺口不算异常达标），不清零（缺口不算恢复），
	// 且 LastCountedWindowToTs **不前移**——否则缺口后的窗口会被误判为相邻。
	if !obs.DataPresent {
		st.GapWindows++
		st.LastFinalizedWindowToTs = obs.Window.ToTs
		return OutcomeGapFrozen
	}

	st.LastWindowSample = obs.SampleTotal

	// 历史证据只增不减，跨异常段保留，供审计与排障。
	if obs.SourceCount >= 2 {
		st.EverMultiSourceConfirmed = true
	}
	if obs.HardSignal {
		st.EverHardSignal = true
	}

	// 窗口判为正常：打断连续计数，**当前段的证据标记一并清零**。
	// 放在最小样本门之前——「确实正常」是比「样本不足」更强的结论。
	//
	// 这里必须清 HardSignal/MultiSourceConfirmed：它们是当前段的确认条件，
	// 留着会让恢复后的新一段异常凭旧证据跳过自己的门槛。
	// 历史保留在 EverHardSignal/EverMultiSourceConfirmed 里，不丢。
	if !obs.Anomalous {
		st.ConsecutiveWindows = 0
		st.HardSignal = false
		st.MultiSourceConfirmed = false
		st.LastCountedWindowToTs = obs.Window.ToTs
		st.LastFinalizedWindowToTs = obs.Window.ToTs
		st.LowSample = obs.SampleTotal < minSample
		return OutcomeNormalReset
	}

	// 异常窗口：按本窗口观测置位当前段标记。
	// 放在正常分支之后，确保恢复后由新窗口自己重新确认。
	if obs.SourceCount >= 2 {
		st.MultiSourceConfirmed = true
	}
	if obs.HardSignal {
		st.HardSignal = true
	}

	// 异常但样本不足：不计入连续达标，但**保留展示**，绝不当作正常。
	// 硬信号不受此门约束（02.1 §8）。
	if obs.SampleTotal < minSample && !obs.HardSignal {
		st.LowSample = true
		st.LowSampleWindows++
		st.LastFinalizedWindowToTs = obs.Window.ToTs
		// LastCountedWindowToTs 不前移：低样本窗口未计入连续链，
		// 下一个窗口相对它不构成相邻，连续计数会重新起算。
		return OutcomeLowSampleHeld
	}

	st.LowSample = false

	// 相邻性判断：严格要求本窗口左界等于上次已计入窗口的右界。
	// 首次计入（LastCountedWindowToTs==0）直接起算。
	adjacent := st.LastCountedWindowToTs != 0 && obs.Window.FromTs == st.LastCountedWindowToTs
	if adjacent {
		st.ConsecutiveWindows++
		st.LastCountedWindowToTs = obs.Window.ToTs
		st.LastFinalizedWindowToTs = obs.Window.ToTs
		return OutcomeCounted
	}

	// 不相邻（缺口、低样本窗口或首次）：连续计数从 1 重新起算，不接续缺口前的计数。
	restarted := st.LastCountedWindowToTs != 0
	st.ConsecutiveWindows = 1
	st.LastCountedWindowToTs = obs.Window.ToTs
	st.LastFinalizedWindowToTs = obs.Window.ToTs
	if restarted {
		return OutcomeRestartedAfterGap
	}
	return OutcomeCounted
}

// IncidentScenario 是故障场景，决定**候选**严重度。
//
// 02.1 §174-177 的严重度首先取决于场景，不是窗口数：
//
//	SEV0 全站不可用、安全事件、严重错路由、大面积重复计费
//	SEV1 主渠道/主模型多用户持续失败、无备用
//	SEV2 单渠道退化但有备用、持续慢或 429
//	SEV3 单请求、低样本、采集延迟或配置漂移
//
// 所以不能「连续窗口多就升级」——第三个窗口不该让「有备用的单渠道退化」
// 变成「无备用的主渠道失败」。窗口条件只用于**验证**各场景自己的门槛。
type IncidentScenario string

const (
	// 全站不可用、安全事件、严重错路由、大面积重复计费。
	ScenarioSiteWideOrSecurity IncidentScenario = "site_wide_or_security"
	// 主渠道/主模型多用户持续失败，且无备用。
	ScenarioPrimaryNoFallback IncidentScenario = "primary_no_fallback"
	// 单渠道退化但有备用、持续慢或 429。
	ScenarioDegradedWithFallback IncidentScenario = "degraded_with_fallback"
	// 单请求、低样本、采集延迟或配置漂移。
	ScenarioIsolatedOrDrift IncidentScenario = "isolated_or_drift"
)

// IncidentSeverityInput 是严重度判定输入。场景与窗口状态分别给出。
type IncidentSeverityInput struct {
	Scenario IncidentScenario

	// Continuity 是连续窗口状态，用于验证场景各自的窗口门槛。
	Continuity IncidentContinuityState

	// DualSourceConfirmed 为真表示两个独立来源都确认。
	// SEV0 的非硬信号路径要求双源（02.1 §174）。
	DualSourceConfirmed bool
}

// DecideIncidentSeverity 先定候选严重度，再验证该场景自己的窗口条件。
//
// 返回空串表示窗口条件尚未满足、不构成该候选严重度的事故
// （低样本异常仍需展示，由调用方按 SEV3 + 数据盲区处理）。
//
// 关键：**不按窗口数跨场景升级**。连续窗口只验证当前场景的门槛，
// 验证不过时降到 SEV3（仍是事故、仍要展示），而不是升到更高级别。
func DecideIncidentSeverity(in IncidentSeverityInput) string {
	st := in.Continuity

	switch in.Scenario {
	case ScenarioSiteWideOrSecurity:
		// 硬信号可立即触发；否则需双源（02.1 §174）。
		if st.HardSignal || in.DualSourceConfirmed || st.MultiSourceConfirmed {
			return "SEV0"
		}
		// 场景够严重但佐证不足：不擅自定 SEV0，降为 SEV3 继续观察。
		return "SEV3"

	case ScenarioPrimaryNoFallback:
		// 02.1 §175：两个连续窗口，或一源 + Eval/另一源。
		// 硬信号按 §8 可立即触发，不要求连续窗口。
		if st.HardSignal {
			return "SEV1"
		}
		if st.ConsecutiveWindows >= incidentSev1ConsecutiveWindows || st.MultiSourceConfirmed {
			return "SEV1"
		}
		return "SEV3"

	case ScenarioDegradedWithFallback:
		// 02.1 §176：最小样本 **且** 至少三个窗口。
		// 两个条件都要满足；低样本时不得产出 SEV2（02.1 MON-004）。
		if st.LowSample {
			return "SEV3"
		}
		if st.ConsecutiveWindows >= incidentSev2ConsecutiveWindows {
			return "SEV2"
		}
		return "SEV3"

	case ScenarioIsolatedOrDrift:
		// 单请求、低样本、采集延迟或配置漂移本身就是 SEV3，无额外窗口门槛。
		if st.ConsecutiveWindows >= 1 || st.LowSample || st.HardSignal {
			return "SEV3"
		}
		return ""
	}

	return ""
}

// DescribeSampleScope 给页面一句可读的分母说明，避免「最小样本 20」无从追溯。
func DescribeSampleScope(st IncidentContinuityState, minSample int64) string {
	if minSample <= 0 {
		minSample = IncidentMinSample
	}
	scope := "该窗口内落在同一聚合键上的请求总数（成功 + 降级成功 + 平台失败；用户拒绝不计入）"
	return fmt.Sprintf("窗口 %d 秒；样本分母：%s；最小样本门 %d", st.WindowSeconds, scope, minSample)
}

// 迟到数据的处理规则（与 Final 标记配套）：
//
//  1. 窗口右界未到，或采集水位尚未越过右界时，上层必须以 Final=false 送入，
//     本函数返回 OutcomePendingNotFinal，不写任何判定结果。
//  2. 采集水位越过右界后，该窗口以 Final=true 送入，此时才产生最终判定。
//  3. 判定之后才到达的数据（真正的迟到）不会改写已判定窗口：
//     重复送入会被 OutcomeDuplicateIgnored 拦下，更早的窗口被 OutcomeStaleIgnored 拦下。
//     这是刻意的——允许回改已判定窗口会让连续计数失去单调性，
//     同一事故在不同时刻读出不同的连续窗口数。
//  4. 迟到数据仍会进入 IncidentObservation 作为证据留存（证据只增），
//     并体现在影响范围里；它影响「证据与影响面」，不影响「连续窗口判定」。
//     这与用户要求的「冷却期间仍要更新证据、影响范围和严重程度」一致。

// incidentSeverityRank 给严重度排序，SEV0 最高。空串表示尚未构成事故。
func incidentSeverityRank(sev string) int {
	switch sev {
	case "SEV0":
		return 4
	case "SEV1":
		return 3
	case "SEV2":
		return 2
	case "SEV3":
		return 1
	}
	return 0
}

// RecordPeakSeverity 把本次判定结果并入历史最高严重度。
//
// 历史最高只用于展示与审计（「这个事故最严重时到过 SEV1」），
// **不回灌给 DecideIncidentSeverity**——否则恢复后的新一段异常会直接继承
// 旧的高严重度，与重置 HardSignal/MultiSourceConfirmed 的目的相抵。
func RecordPeakSeverity(st *IncidentContinuityState, sev string) {
	if st == nil {
		return
	}
	if incidentSeverityRank(sev) > incidentSeverityRank(st.PeakSeverity) {
		st.PeakSeverity = sev
	}
}
