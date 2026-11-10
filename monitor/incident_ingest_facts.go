package monitor

// 本地事实 → 事故引擎输入的聚合层（MON-003 B3）。
//
// ★ 事实源为什么是 CapacityUserMinuteSample ★
//
// 8204 跑 logchain-only 模式：startSourceEpoch 在该模式下走独立分支并直接
// return，startSampler 整条不运行；logchain-only 下唯一的采样调用方还显式
// 传 includeMetrics=false（刻意的，避免制造「普通采样器曾运行」的假象）。
// 所以 MetricSample 在 8204 是死表。
//
// 而 upsertCapacityUserMinuteSamplesDB 在 includeMetrics 判断**之外**无条件
// 执行，由 startCustomerHealthSource lane 驱动（8204 已开该开关）。字段也齐备：
// BucketTs / ChannelID / ModelName / Grp + Success / Anomaly / Failed +
// TrafficClassVersion。它是 8204 唯一既 fresh 又有完整异常率分母的分钟级表。
//
// ★ 口径版本有两套，不可混用 ★
//
// 事实行的 traffic_class_version 写的是 stabilityTrafficClassificationVersion
// (= trafficclass.DeliveryCurrent = 7)。同一行里的 customer_health_version
// 才是 customerHealthStabilityPolicyVersion (= 3)。本轮异常口径用
// Success/Anomaly/Failed（交付分类口径），所以过滤必须用前者——用后者会把
// 当前数据全部排除。若将来改用 CustomerHealthAnomaly/Failed，必须同步换成
// customer_health_version 过滤。
//
// 覆盖凭证的版本校验是 customerHealthHistoricalCoverage 自己的职责
// （它内部对 SemanticsVersion 校验 customerHealthStabilityPolicyVersion），
// 适配层不另加判断，也不把两套版本相互替代。

import (
	"context"
	"fmt"
	"strconv"
	"strings"

	"github.com/yl0711-coder/newapi-monitor/internal/observability"
)

// incidentPendingReasonNoFallback 是本轮唯一的「场景待确认」原因。
//
// 取值集合固定，避免自由文本发散。本轮单侧聚合数据无法取得「有无备用渠道」
// 这一配置事实，而 primary_no_fallback / degraded_with_fallback 都依赖它，
// site_wide_or_security 又需要硬信号或双源——所以能证明的场景集合为空。
const incidentPendingReasonNoFallback = "no_fallback_config_fact"

// incidentPendingReasonGapBackfilledAnomalous 表示一个**已定稿**的缺口窗口
// 在补齐后呈现异常。
//
// 这类窗口按计划不自动追溯建单/定级（见 reviseBackfilledIncidentGap），
// 所以用显式待确认原因提示人工复核，而不是静默修订了事。
const incidentPendingReasonGapBackfilledAnomalous = "gap_backfilled_anomalous"

// incidentWindowFacts 是一个聚合键在一个窗口内的本地事实。
type incidentWindowFacts struct {
	ChannelID int
	ModelName string
	Grp       string

	Success int64
	Anomaly int64
	Failed  int64

	// MixedVersionRows 是该键本窗内**非当前口径版本**的行数。
	// > 0 时整窗判 mixed_version：不计算异常率、不送引擎，更不得当作
	// 「排除旧行后剩余部分是完整样本」去判正常或结束事故。
	MixedVersionRows int64
}

// sampleTotal 分母 = 三者之和，即「该窗落在同一聚合键上的请求总数」。
func (f incidentWindowFacts) sampleTotal() int64 {
	return f.Success + f.Anomaly + f.Failed
}

// anomalyRate 返回异常比例与「是否有样本」。
//
// 口径：(Anomaly + Failed) / (Success + Anomaly + Failed)。
// 这是**已有日志口径的异常比例，不代表客户端最终失败率**。
// 分母为 0 时返回 hasSample=false —— 调用方必须按「无样本」处理，
// 绝不能当成 0% 异常（那会被判为正常并结束事故）。
func (f incidentWindowFacts) anomalyRate() (rate float64, hasSample bool) {
	total := f.sampleTotal()
	if total <= 0 {
		return 0, false
	}
	return float64(f.Anomaly+f.Failed) / float64(total), true
}

// aggregateIncidentWindowFacts 聚合窗口 [from, to) 内的本地事实。
//
// 窗口左闭右开。按 (channel_id, model_name, grp) 分组——不含 user_id：
// 事故聚合键里没有用户维度，按用户分组会把同一事故拆成 N 条。
func (m *Monitor) aggregateIncidentWindowFacts(ctx context.Context, from, to int64) ([]incidentWindowFacts, error) {
	if m == nil || m.storeDB == nil {
		return nil, fmt.Errorf("本地库未就绪，无法聚合事故事实")
	}
	if to <= from {
		return nil, fmt.Errorf("聚合窗口非法: [%d,%d)", from, to)
	}

	type row struct {
		ChannelID int
		ModelName string
		Grp       string
		Success   int64
		Anomaly   int64
		Failed    int64
		MixedRows int64
	}
	var rows []row

	// 同一次查询里既算当前口径的计数，也数出混版行数：
	// 分两次查会在两次之间被补采改动，导致「分母用新数据、混版判断用旧数据」。
	err := m.storeDB.WithContext(ctx).
		Model(&CapacityUserMinuteSample{}).
		Select(`channel_id AS channel_id,
			model_name AS model_name,
			grp AS grp,
			COALESCE(SUM(CASE WHEN traffic_class_version = ? THEN success ELSE 0 END), 0) AS success,
			COALESCE(SUM(CASE WHEN traffic_class_version = ? THEN anomaly ELSE 0 END), 0) AS anomaly,
			COALESCE(SUM(CASE WHEN traffic_class_version = ? THEN failed  ELSE 0 END), 0) AS failed,
			COALESCE(SUM(CASE WHEN traffic_class_version <> ? THEN 1 ELSE 0 END), 0) AS mixed_rows`,
			stabilityTrafficClassificationVersion, stabilityTrafficClassificationVersion,
			stabilityTrafficClassificationVersion, stabilityTrafficClassificationVersion).
		Where("bucket_ts >= ? AND bucket_ts < ?", from, to).
		Group("channel_id, model_name, grp").
		Find(&rows).Error
	if err != nil {
		return nil, fmt.Errorf("聚合事故窗口事实失败: %w", err)
	}

	facts := make([]incidentWindowFacts, 0, len(rows))
	for _, r := range rows {
		facts = append(facts, incidentWindowFacts{
			ChannelID: r.ChannelID, ModelName: r.ModelName, Grp: r.Grp,
			Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed,
			MixedVersionRows: r.MixedRows,
		})
	}
	return facts, nil
}

// incidentKeyForFacts 把本地事实映射成事故聚合键。
//
// 全部用既有合法枚举值，不自造：
//   - ChannelID <= 0 → ChannelScopeUnknown（**不是** not_assigned）。
//     按 incident_dedup.go 的契约，not_assigned 是「已确知没有渠道」这一事实，
//     只在有 route_no_channel 之类独立证据时才可用；裸 0 是采集缺口。
//     此时 ChannelID 置 nil，绝不写 0。
//   - Route 用既有 NormalizeIncidentRoute("") → "route_unknown"，不写空串。
//   - IngressProtocol 用 DeriveIngressProtocol({OtherUsable:false}) → ingress_unknown，
//     那是「没采到」的合法值（区别于 unrecognized 新端点与 conflict 数据矛盾）。
//   - PrimaryFaultClass 一律 FaultUnknown。
//
// ★ 缺口窗口不得改用 FaultTelemetryGap ★
//
// PrimaryFaultClass 是 IncidentKey 成员，进 Canonical()/DedupKey()。把分类
// 从 unknown 换成 telemetry_gap 会算出另一个 dedup_key，引擎按
// WHERE dedup_key = ? 就找不到原活跃事故，于是无法推进原事故的缺口状态，
// 反而可能凭空建第二个事故。缺口这一数据质量事实由 IncidentIngestWindowState
// 的 window_state=gap_observed 表达，不靠改故障分类表达。
func incidentKeyForFacts(f incidentWindowFacts) (IncidentKey, error) {
	fc := observability.FaultUnknown
	if err := fc.Validate(); err != nil {
		return IncidentKey{}, fmt.Errorf("故障分类自检失败: %w", err)
	}

	key := IncidentKey{
		PrimaryFaultClass: string(fc),
		Model:             f.ModelName,
		// UserGroup 是 NewAPI logs.group（website_group），**不是** CustomerGroup.ID。
		UserGroup: f.Grp,
		// 无路由事实 → OtherUsable=false → ingress_unknown。
		IngressProtocol: DeriveIngressProtocol(IngressProtocolInput{OtherUsable: false}),
		Route:           NormalizeIncidentRoute(""),
		ConfigChangeRef: "",
	}
	if f.ChannelID > 0 {
		id := int64(f.ChannelID)
		key.ChannelScope = ChannelScopeAssigned
		key.ChannelID = &id
	} else {
		key.ChannelScope = ChannelScopeUnknown
	}
	if !key.ChannelScope.Valid() {
		return IncidentKey{}, fmt.Errorf("渠道归属 %q 非法", key.ChannelScope)
	}
	return key, nil
}

// incidentKeyFromCanonical 由规范串还原聚合键。
//
// 崩溃恢复要用快照重放，而快照里存的是 canonical_key。这里做精确逆向：
// Canonical() 用固定顺序的七个 "名=值" 片段加 \x1f 分隔，且分隔符刻意选了
// 模型名/分组名里不会出现的字符，所以逆向是无损的。
//
// 任何片段缺失或前缀不符都返回错误——宁可让该窗口转失败并保留证据，
// 也不要拿一个猜出来的聚合键去调引擎（那会挂错事故）。
func incidentKeyFromCanonical(canonical string) (IncidentKey, error) {
	parts := strings.Split(canonical, incidentKeySep)
	if len(parts) != 7 {
		return IncidentKey{}, fmt.Errorf("聚合键规范串片段数 %d，期望 7", len(parts))
	}
	prefixes := []string{"fc=", "ch=", "model=", "grp=", "iproto=", "route=", "cfg="}
	vals := make([]string, 7)
	for i, p := range parts {
		if !strings.HasPrefix(p, prefixes[i]) {
			return IncidentKey{}, fmt.Errorf("聚合键规范串第 %d 段前缀不符", i+1)
		}
		vals[i] = strings.TrimPrefix(p, prefixes[i])
	}

	key := IncidentKey{
		PrimaryFaultClass: vals[0],
		Model:             vals[2],
		UserGroup:         vals[3],
		IngressProtocol:   IngressProtocol(vals[4]),
		Route:             vals[5],
		ConfigChangeRef:   vals[6],
	}
	scope, id, found := strings.Cut(vals[1], ":")
	if !found {
		return IncidentKey{}, fmt.Errorf("聚合键渠道段格式非法: %q", vals[1])
	}
	key.ChannelScope = IncidentChannelScope(scope)
	if !key.ChannelScope.Valid() {
		return IncidentKey{}, fmt.Errorf("聚合键渠道归属 %q 非法", scope)
	}
	if key.ChannelScope == ChannelScopeAssigned {
		n, err := strconv.ParseInt(id, 10, 64)
		if err != nil {
			return IncidentKey{}, fmt.Errorf("聚合键渠道 ID 非法: %w", err)
		}
		key.ChannelID = &n
	}
	// 自检：还原出的键必须算回同一个规范串，否则不得使用。
	if key.Canonical() != canonical {
		return IncidentKey{}, fmt.Errorf("聚合键还原自检失败")
	}
	return key, nil
}

// incidentEvidenceForLocalFacts 返回单侧本地事实对应的证据等级与方式。
//
// 复用 EvidenceLevelFor，不手写字面量：单侧日志是 local_only → inferred。
// 证据等级与事故严重度是两件事，inferred 不意味着必须是 SEV3。
func incidentEvidenceForLocalFacts() (level string, method string) {
	return string(EvidenceLevelFor(CorrelationLocalOnly)), string(CorrelationLocalOnly)
}

// incidentResponsibilityForLocalFacts 按既有判定函数给出责任归属。
//
// 不手动指定：inferred 证据在 decideResponsibilityValidated 的天花板里
// 一律得到 insufficient_evidence，这是规则的结论，不该由适配层自己写死。
func incidentResponsibilityForLocalFacts(evidenceLevel string) string {
	resp := DecideResponsibility(ResponsibilityInput{
		FaultClass:    observability.FaultUnknown,
		EvidenceLevel: observability.EvidenceLevel(evidenceLevel),
	})
	return string(resp)
}
