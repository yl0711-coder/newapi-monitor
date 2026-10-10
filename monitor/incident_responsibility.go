package monitor

// 责任归属判定。
//
// 核心原则（用户 2026-10-09 确认）：**故障分类、证据等级、责任归属必须分别判断**，
// 不能只按 fault_class 固定映射责任方。所以这里没有「fault_class → responsibility」
// 的静态表——那种表会把「发生了什么」和「该找谁」绑死，而后者取决于拿到了什么证据。
//
// 归因置信度（faultConfHigh/Mid/Low/None）与证据等级（exact/correlated/inferred/
// unavailable）**不是同一概念**，不得互相换算：
//   - 置信度回答「这条判据本身有多可靠」（取决于实测样本量）
//   - 证据等级回答「这个结论是怎么关联出来的」（取决于关联方式）
// 一条判据可以置信度 high 但证据等级只有 inferred（判据很准，但只是单侧日志推断）。

import "github.com/yl0711-coder/newapi-monitor/internal/observability"

// IncidentResponsibility 是责任归属。与 fault_class 严格分离。
type IncidentResponsibility string

const (
	RespUpstream   IncidentResponsibility = "upstream"
	RespPlatform   IncidentResponsibility = "platform"
	RespClient     IncidentResponsibility = "client"
	RespUserConfig IncidentResponsibility = "user_config"

	// RespUnknown：观察齐全，但确实判不出是谁。
	// 对应 01.3 的 unknown「有异常事实但仍无法分类」。
	RespUnknown IncidentResponsibility = "unknown"

	// RespInsufficientEvidence：证据不足，**无权归责任何一方**。
	// 与 RespUnknown 是两个值，不可互换：前者是「证据不够」，后者是「证据够但判不出」。
	RespInsufficientEvidence IncidentResponsibility = "insufficient_evidence"
)

// Valid 校验取值。
func (r IncidentResponsibility) Valid() bool {
	switch r {
	case RespUpstream, RespPlatform, RespClient, RespUserConfig,
		RespUnknown, RespInsufficientEvidence:
		return true
	}
	return false
}

// DisplayName 返回页面文案。
func (r IncidentResponsibility) DisplayName() string {
	switch r {
	case RespUpstream:
		return "上游"
	case RespPlatform:
		return "平台"
	case RespClient:
		return "客户端"
	case RespUserConfig:
		return "客户配置或额度"
	case RespUnknown:
		return "无法分类"
	case RespInsufficientEvidence:
		return "证据不足，责任待判"
	}
	return "证据不足，责任待判"
}

// AssignsBlame 说明该归属是否指向了某个具体责任方。
//
// unknown 与 insufficient_evidence 都不指向责任方，但原因不同——
// 调用方要据此决定「能不能据此处置」，而不是看 fault_class。
func (r IncidentResponsibility) AssignsBlame() bool {
	switch r {
	case RespUpstream, RespPlatform, RespClient, RespUserConfig:
		return true
	}
	return false
}

// IncidentCorrelation 说明证据是怎么取得的。
//
// 严格对应 01.3 §87-90 的四档定义，既不自动升级也不一律降级：
//
//	exact       同一 ID 或 Attempt 的直接事实，或确定性定向复现
//	correlated  多源在相同维度和窗口高度相关
//	inferred    启发式或弱相关推断
//	unavailable 没有必要观察点
type IncidentCorrelation string

const (
	// CorrelationDirectFact：同一 ID 或 Attempt 上**直接观测到该事实本身**。
	// 例如上游日志里该 Attempt 自己返回了 5xx——不是推断，是直接事实。
	// 这是 exact 的合法路径之一。
	CorrelationDirectFact IncidentCorrelation = "direct_fact"

	// CorrelationDeterministicReproduction：确定性定向复现。
	// 01.3 把它与直接事实并列为 exact。
	CorrelationDeterministicReproduction IncidentCorrelation = "deterministic_reproduction"

	// CorrelationMultiSource：多源在相同维度和窗口高度相关。
	// 关联出来的结论——不是直接事实，所以是 correlated 而非 exact。
	CorrelationMultiSource IncidentCorrelation = "multi_source"

	// CorrelationLocalOnly：只有我方单侧日志，属启发式推断。
	CorrelationLocalOnly IncidentCorrelation = "local_only"

	// CorrelationTimeWindowOnly：只有时间窗重合，弱相关。
	CorrelationTimeWindowOnly IncidentCorrelation = "time_window_only"

	// CorrelationObservationMissing：应有的观察点缺失。
	CorrelationObservationMissing IncidentCorrelation = "observation_missing"
)

// EvidenceLevelFor 按证据取得方式给出证据等级。
//
// 输入是取得方式，**不是归因置信度**——刻意不接受 faultConfidence 参数，
// 从签名上杜绝把 high/mid/low 换算成 exact/correlated/inferred。
// 01.3 §92 另有明示：API-evaluator 的 high/medium/low 是测试置信度，
// 不得代替 evidence level。
func EvidenceLevelFor(c IncidentCorrelation) observability.EvidenceLevel {
	switch c {
	case CorrelationDirectFact, CorrelationDeterministicReproduction:
		// 直接事实与确定性复现是 exact 的合法路径。
		// 「不能仅凭关联就升为 exact」不等于「禁止直接事实取得 exact」。
		return observability.EvidenceExact
	case CorrelationMultiSource:
		return observability.EvidenceCorrelated
	case CorrelationLocalOnly, CorrelationTimeWindowOnly:
		return observability.EvidenceInferred
	case CorrelationObservationMissing:
		return observability.EvidenceUnavailable
	}
	// 未知取得方式不得放行成任何可用等级。
	return observability.EvidenceUnavailable
}

// ResponsibilityInput 是责任判定的输入。三者分别给出，不互相推导。
type ResponsibilityInput struct {
	FaultClass    observability.FaultClass
	EvidenceLevel observability.EvidenceLevel

	// UpstreamEvidenceLinked 为真表示已可靠关联到对应请求的上游侧证据。
	// 仅凭错误文本包含 "upstream" 字样**不足以**置位——文本措辞不是证据。
	UpstreamEvidenceLinked bool

	// UpstreamEvidenceIndicatesUpstreamCause 为真表示那份上游证据确实指向
	// 上游侧成因（例如上游自己返回 429/5xx），而不只是「上游这一跳出现过」。
	UpstreamEvidenceIndicatesUpstreamCause bool

	// PlatformCauseConfirmed 为真表示已证实根因在我方平台链路
	// （例如记账或解析环节的确定性缺陷），而不只是「NewAPI 记了一条异常日志」。
	PlatformCauseConfirmed bool

	// ClientDirectTelemetry 为真表示拿到了经授权的客户端直接遥测。
	// 01.3 把 client_retry_loop / client_compaction 定义为「客户端直接证实」，
	// 没有这个就不能产出那两类。
	ClientDirectTelemetry bool
}

// DecideResponsibilityChecked 校验证据等级后给出责任归属。
//
// 缺失或非法的 evidence_level **不得给出确定责任**：那种输入是契约违规，
// 不是「证据弱」。原先用排除法只拦 inferred/unavailable，
// 空串与 "high"（置信度字符串）这类值会直接穿过天花板，
// 只要上游两个标记为 true 就返回 upstream。
//
// 返回的 IncidentResponsibility 在出错时为 RespInsufficientEvidence，
// 调用方即便忽略 error 也不会拿到确定责任。
func DecideResponsibilityChecked(in ResponsibilityInput) (IncidentResponsibility, error) {
	if err := in.EvidenceLevel.Validate(); err != nil {
		return RespInsufficientEvidence, err
	}
	if err := in.FaultClass.Validate(); err != nil {
		return RespInsufficientEvidence, err
	}
	return decideResponsibilityValidated(in), nil
}

// DecideResponsibility 是 DecideResponsibilityChecked 的便捷形式。
//
// 非法或缺失的证据等级一律得到 RespInsufficientEvidence——
// 需要区分「契约违规」与「证据确实不足」时请用 DecideResponsibilityChecked。
func DecideResponsibility(in ResponsibilityInput) IncidentResponsibility {
	resp, _ := DecideResponsibilityChecked(in)
	return resp
}

// decideResponsibilityValidated 在证据等级与分类都已校验合法后判定。
//
// 判定顺序是刻意的：先用证据等级设天花板，再看 fault_class 的具体情形。
// 这样「证据不足不能归责上游」由结构保证，不靠每个分支自觉。
func decideResponsibilityValidated(in ResponsibilityInput) IncidentResponsibility {
	// 证据等级天花板：白名单式——只有 exact/correlated 继续检查定责条件。
	// 其余（inferred、unavailable，以及校验已拦掉的非法值）一律无权归责。
	switch in.EvidenceLevel {
	case observability.EvidenceExact, observability.EvidenceCorrelated:
	default:
		return RespInsufficientEvidence
	}

	switch in.FaultClass {
	// 采集缺口：恒为证据不足。采集坏了不是上游的错。
	case observability.FaultTelemetryGap:
		return RespInsufficientEvidence

	// 限流：默认不归责。只有可靠关联到对应请求、且证据确实指向上游限流时才归上游。
	// 错误文本含 "upstream" 字样不作为依据。
	case observability.FaultRateLimitCapacity:
		if in.UpstreamEvidenceLinked && in.UpstreamEvidenceIndicatesUpstreamCause {
			return RespUpstream
		}
		return RespInsufficientEvidence

	// 空输出：仅有 completion_tokens=0 与扣费记录时**不足以定责**——
	// token 字段的启发式判断不能证明响应内容为空（既有判据
	// logchain_fault.go:859 同样保持待判）。
	// 但证据补齐后不禁止定责：可靠关联到对应请求、且证据确实指向上游成因时归上游。
	case observability.FaultEmptyOrTruncatedOutput:
		if in.UpstreamEvidenceLinked && in.UpstreamEvidenceIndicatesUpstreamCause {
			return RespUpstream
		}
		return RespInsufficientEvidence

	// 计费异常：total tokens is 0 只能证明 NewAPI 记录了零用量异常，
	// 不能单独证明根因在平台——也可能是上游 usage 缺失、解析或其它链路问题。
	// 根因证实后可归责：
	//   - 证据指向上游（如上游 usage 缺失）→ upstream
	//   - 证据指向我方记账/解析链路 → platform
	case observability.FaultBillingAnomaly:
		if in.UpstreamEvidenceLinked && in.UpstreamEvidenceIndicatesUpstreamCause {
			return RespUpstream
		}
		if in.PlatformCauseConfirmed {
			return RespPlatform
		}
		return RespInsufficientEvidence

	// 客户端直接证实的两类：没有经授权的客户端遥测就不产出（见 CanProduceFaultClass）。
	// 走到这里说明调用方已确认有客户端遥测。
	case observability.FaultClientRetryLoop, observability.FaultClientCompaction:
		if in.ClientDirectTelemetry {
			return RespClient
		}
		return RespInsufficientEvidence

	// 观察齐全但判不出。
	case observability.FaultUnknown:
		return RespUnknown
	}

	// 其余分类不在本轮确认范围内，一律保持待判而不是猜一个责任方。
	// 新增分类的归属需按「证据条件—故障分类—责任归属」单独确认后再加分支。
	return RespInsufficientEvidence
}

// FaultClassObservability 说明 Monitor 当前对某个 fault_class 的观察能力。
type FaultClassObservability string

const (
	// Monitor 自身有观察点，可以产出。
	FaultObservable FaultClassObservability = "observable"

	// 当前缺少必要证据，Monitor 不擅自输出。
	// **这不是永久禁止**：接入 Eval 的脱敏校验结果、故障事件与证据引用后即可产出。
	// 不采集正文/thinking signature/工具参数，不等于不能接收 Eval 的结论。
	FaultNeedsExternalEvidence FaultClassObservability = "needs_external_evidence"

	// 需要经授权的客户端直接遥测。仅接入 Eval 不能代替真实客户端遥测。
	FaultNeedsClientTelemetry FaultClassObservability = "needs_client_telemetry"
)

// FaultClassObservabilityOf 返回该分类当前的观察能力状态。
//
// 页面据此显示「无观察点／不可判定」，**绝不显示「没有此类故障」**——
// 查不到不等于没发生过。
func FaultClassObservabilityOf(fc observability.FaultClass) FaultClassObservability {
	switch fc {
	// 协议级与结构校验类：Monitor 无观察点，但 Eval 可提供脱敏校验结果。
	case observability.FaultProtocolInvalid,
		observability.FaultResponsesIncomplete,
		observability.FaultThinkingSignatureInvalid,
		observability.FaultToolCallInvalid:
		return FaultNeedsExternalEvidence

	// 01.3 定义为「客户端直接证实」。NewAPI 内部渠道重试与 Nginx→NewAPI 这一跳的
	// upstream_attempts 都不能证明客户端重试循环，补阈值也不能解决。
	case observability.FaultClientRetryLoop,
		observability.FaultClientCompaction:
		return FaultNeedsClientTelemetry
	}
	return FaultObservable
}

// CanProduceFaultClass 判断在给定证据条件下 Monitor 是否可以输出该分类。
//
// 测试保证的是「缺少必要证据时不擅自输出」，**不是永久禁止这些分类**——
// 否则会阻碍后续 Eval 联调与客户端遥测接入。
func CanProduceFaultClass(
	fc observability.FaultClass,
	hasExternalEvidence bool,
	hasClientTelemetry bool,
) bool {
	switch FaultClassObservabilityOf(fc) {
	case FaultNeedsExternalEvidence:
		return hasExternalEvidence
	case FaultNeedsClientTelemetry:
		return hasClientTelemetry
	}
	return true
}

// SupportsQuarantineSuggestion 判断该分类在当前责任与证据下是否可支持隔离建议。
//
// 01.3 §87-88：只有 exact「可支持隔离建议，仍须结合影响与审批」，
// correlated「最多支持 DEGRADED，隔离前需补强证据」。
// 所以这里要求**白名单式的合法 exact**，而不是排除 inferred/unavailable——
// 后者会让 correlated 以及空值、非法枚举一并放行。
//
// 返回 true 只表示「证据等级这一关过了」，**不代表可以跳过**其他治理条件
// 与人工审批：01.3 对 exact 的原文也是「仍须结合影响与审批」。
//
// 这里只回答「能不能产生隔离建议」，不回答「是否与渠道有关」——
// billing_anomaly 不凭单条日志产生隔离建议，但这不表示所有计费异常都与渠道无关。
func SupportsQuarantineSuggestion(
	fc observability.FaultClass,
	resp IncidentResponsibility,
	level observability.EvidenceLevel,
) bool {
	// 责任未指向具体一方时不支持任何处置结论。
	if !resp.AssignsBlame() {
		return false
	}
	// 白名单：只有 exact 放行。correlated 最多支持 DEGRADED，不支持隔离；
	// 空值与未定义枚举一并落到 default 被拒。
	switch level {
	case observability.EvidenceExact:
	default:
		return false
	}
	switch fc {
	// 01.3 明示 semantic_quality 不用于自动隔离。
	case observability.FaultSemanticQuality:
		return false
	// 计费异常不凭这类日志单独产生隔离建议。
	case observability.FaultBillingAnomaly:
		return false
	// 客户端断连不是渠道可靠性问题。
	case observability.FaultClientGone:
		return false
	}
	return true
}
