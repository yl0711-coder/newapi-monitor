package monitor

// Incident 四层状态与审计轨迹（MON-004 B1 建表部分）。
//
// ★ 四层不是自动推进的状态机 ★（用户 2026-10-10 明确）
//
//	observed_state        ← 监控观测产出
//	recommended_state     ← 判定规则产出（exact 证据也只到这一层）
//	approved_state        ← **只来自人工审批**，系统不得自行写入
//	applied_newapi_state  ← **只读反映 NewAPI 实际配置**，由只读采集得出
//
// 四列同在一行、各自独立来源，没有「下一层」的概念。
// 「已审批」≠「已执行」：approved 与 applied 可以长期不一致，
// 这种偏离必须如实记录并在页面显示，不能假定审批后就生效。
//
// Monitor 永不修改 NewAPI 配置。审批只记录 Monitor 内的决定。

// 治理状态唯一枚举（01.3 §6）。前三层只能取这五个值。
//
// 第四层 applied_newapi_* 不用这套枚举：按 01.3「NewAPI 的 enabled/disabled
// 不能替代治理状态」，它记录的是 NewAPI 实际启停/分组/优先级/权重，
// 是配置快照而非治理结论。因此也不能拿前三层的治理状态字符串
// 去和第四层的启停字符串直接比较来证明漂移。
const (
	governanceStateHealthy           = "HEALTHY"
	governanceStateSuspect           = "SUSPECT"
	governanceStateDegraded          = "DEGRADED"
	governanceStateQuarantined       = "QUARANTINED"
	governanceStateRecoveryProbation = "RECOVERY_PROBATION"
)

// 证据等级唯一枚举（01.3 §5）。只有这四种，治理用途各不相同：
//
//	exact       同一 ID/Attempt 的直接事实 → 可支持隔离建议
//	correlated  多源同维同窗高度相关       → 最多支持 DEGRADED，隔离前需补强
//	inferred    启发式或弱相关推断         → 只用于排查方向，不支持处置
//	unavailable 没有必要观察点             → 不支持健康或处置结论
//
// 注意 API-evaluator 的 high/medium/low 是测试置信度，不得代替证据等级。
const (
	evidenceLevelExact       = "exact"
	evidenceLevelCorrelated  = "correlated"
	evidenceLevelInferred    = "inferred"
	evidenceLevelUnavailable = "unavailable"
)

// validEvidenceLevel 写入前校验。空串、indicative、insufficient_evidence
// 一类自造值必须被挡住，不能落库。
func validEvidenceLevel(s string) bool {
	switch s {
	case evidenceLevelExact, evidenceLevelCorrelated,
		evidenceLevelInferred, evidenceLevelUnavailable:
		return true
	}
	return false
}

// validGovernanceState 用于写入前校验，挡住自造枚举值。
func validGovernanceState(s string) bool {
	switch s {
	case governanceStateHealthy, governanceStateSuspect, governanceStateDegraded,
		governanceStateQuarantined, governanceStateRecoveryProbation:
		return true
	}
	return false
}

// IncidentState 是一个事故的四层状态行。一事故一行。
type IncidentState struct {
	IncidentID string `gorm:"primaryKey;size:64;column:incident_id"`

	// 第一层：观测状态，来自监控。取值限 governanceState* 五种。
	ObservedState string `gorm:"size:32;column:observed_state"`
	ObservedAt    int64  `gorm:"column:observed_at"`

	// 第二层：建议状态，来自判定规则。证据等级再高也只能到这层。
	RecommendedState  string `gorm:"size:32;column:recommended_state"`
	RecommendedAt     int64  `gorm:"column:recommended_at"`
	RecommendedReason string `gorm:"size:256;column:recommended_reason"`
	// RecommendedEvidenceLevel 记录建议当时的证据等级，供审批人核对。
	RecommendedEvidenceLevel string `gorm:"size:16;column:recommended_evidence_level"`

	// 第三层：人工审批状态。系统不得自行写入这三列。
	ApprovedState string `gorm:"size:32;column:approved_state"`
	ApprovedAt    int64  `gorm:"column:approved_at"`
	// ApprovedBy 是审批人标识（已脱敏）。空串表示尚无人审批。
	ApprovedBy string `gorm:"size:64;column:approved_by"`
	// ApprovalExpiresAt 限时人工覆盖的到期时刻。到期后须重新计算，
	// 不自动续期、不静默延长。0 表示不限时。
	ApprovalExpiresAt int64 `gorm:"column:approval_expires_at;index"`
	// ApprovalExpired 标记覆盖已到期且已重算过，供页面区分「从未审批」与「审批已过期」。
	ApprovalExpired bool `gorm:"column:approval_expired"`

	// 第四层：NewAPI 实际配置快照，只读同步得来（01.3 要求启停/分组/优先级/权重）。
	// 这一层**不是** Monitor 的决定，而是对外部事实的观测，
	// 也**不是**治理状态——不可与前三层的枚举互相比较或赋值。
	//
	// 四个字段全部可空：同步失败或该维度拿不到时为 nil，表示「实际配置未知」，
	// 页面必须显示未知而不是填一个默认值（false/0 都会被误读成真实配置）。
	AppliedNewapiEnabled  *bool   `gorm:"column:applied_newapi_enabled"`
	AppliedNewapiGroup    *string `gorm:"size:64;column:applied_newapi_group"`
	AppliedNewapiPriority *int64  `gorm:"column:applied_newapi_priority"`
	AppliedNewapiWeight   *int64  `gorm:"column:applied_newapi_weight"`

	AppliedSyncedAt int64 `gorm:"column:applied_synced_at"`
	// AppliedSyncOK=false 表示本次只读同步失败，上面四个快照字段整体不可信。
	AppliedSyncOK bool `gorm:"column:applied_sync_ok"`
	// AppliedSyncError 已脱敏的同步失败原因，供排查。
	AppliedSyncError string `gorm:"size:256;column:applied_sync_error"`

	// DriftDetected：审批决定与 NewAPI 实际配置不一致，如实记录，不自动纠正任一方
	// （01.3 §6：生成 configuration_drift 数据质量事件）。
	//
	// 判定**不是**字符串比较：治理状态与启停/分组/优先级/权重不同类，
	// 需要一条「该治理决定期望什么样的实际配置」的映射才能比。
	// 该映射与只读同步同属后续阶段，B1 只提供存储位置，不写入判定结果。
	DriftDetected bool   `gorm:"column:drift_detected;index"`
	DriftNote     string `gorm:"size:256;column:drift_note"`

	// Revision 乐观并发版本号。审批走 CAS：UPDATE ... WHERE revision = ?
	Revision int64 `gorm:"column:revision"`

	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (IncidentState) TableName() string { return "incident_states" }

// IncidentAudit 是审计轨迹，只增不改不删。
//
// 每条记录「谁、何时、把哪一层从什么改成什么、基于哪份证据」。
type IncidentAudit struct {
	ID         string `gorm:"primaryKey;size:64;column:id"`
	IncidentID string `gorm:"size:64;column:incident_id;index"`

	// Layer: observed / recommended / approved / applied_newapi
	Layer string `gorm:"size:24;column:layer;index"`
	// Action: observe / recommend / approve / reject / expire / sync / drift
	Action string `gorm:"size:24;column:action"`

	FromState string `gorm:"size:32;column:from_state"`
	ToState   string `gorm:"size:32;column:to_state"`

	// Actor 人工操作时是审批人（脱敏），系统动作时是来源标识。
	Actor string `gorm:"size:64;column:actor"`
	// ActorKind: human / system。审批必须是 human。
	ActorKind string `gorm:"size:16;column:actor_kind"`

	// EvidenceLevel / EvidenceRef 记录决定依据，供事后复核。
	EvidenceLevel string `gorm:"size:16;column:evidence_level"`
	EvidenceRef   string `gorm:"size:256;column:evidence_ref"`

	// IdempotencyKey 防重复审批：同一 key 第二次提交不产生第二条记录。
	IdempotencyKey string `gorm:"size:64;column:idempotency_key;uniqueIndex:idx_incident_audit_idem"`

	Note      string `gorm:"size:512;column:note"`
	CreatedAt int64  `gorm:"column:created_at;index"`
}

func (IncidentAudit) TableName() string { return "incident_audits" }
