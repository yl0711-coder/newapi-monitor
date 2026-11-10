package monitor

// Incident 持久化模型（MON-003 B1）。
//
// ★ 回滚点 ★
//
// 本次 schema 变更把 plan ID bump 到 v59-incident-core-v1。
// 正确的回滚快照是**执行本次 v59 迁移前生成的那一个**——它保存的是迁移前
// （即 v58 结构）的数据，但其 manifest 标记的目标计划是 v59。
//
// 不要去找"v58 当年迁移前的快照"：按 preMigrationReferenceName 的注释，
// 新计划发布自己的快照后会原子替换引用文件，保留策略随后可以淘汰
// 不再被钉住的旧计划快照，所以旧 v58 快照并不保证仍然存在。
//
// 回滚仍是人工三件套：旧镜像 + 上述 v59 迁移前快照 + 新卷，
// 禁止只换 tag 复用已迁移卷。
//
// ★ 稳定聚合键 vs 事故实例 ★
//
// dedup_key 是**稳定聚合键**：同一故障分类+渠道+模型+分组+协议+路由（+配置分段）
// 永远算出同一个值。它**不能**建全局唯一索引——那会让事故结束后的复发
// 永远建不了新单，或者被迫覆盖旧事故，两种都不可接受。
//
// 唯一约束只约束「同一聚合键下同时最多一个活跃事故」：
//
//	uniqueIndex:idx_incident_active_dedup  (dedup_key, active_marker)
//
// active_marker 对活跃事故恒为 1，对已结束事故写该事故自己的 ID。
// 于是活跃行最多一条（并发 INSERT 由数据库拒绝第二条），
// 已结束的历史行各自带不同 marker，可以无限累积、不互相冲突、不被覆盖。
//
// ★ 这个索引只提供「同键最多一个活跃事故」这一条存储约束 ★
//
// 复发三态（用户 2026-10-10 口径）需要的判断**不在**索引里：
//   - 持续未结束：冷却到期也不新建
//   - 已结束但冷却未到期：复用原事故，追加新异常段
//   - 已结束且冷却到期：新建事故，原事故历史保留
//
// 第二、三态都要读 status 与 cooldown_until_ts 才能分辨，索引管不了：
// 事故一旦结束就让出了活跃槽位，此时索引**允许**插入新事故，
// 是否该插入必须由 B2 的业务逻辑按冷却判断决定。
// B1 只验证存储约束，复发逻辑由 B2 经真实业务入口验收。

// incidentActiveMarker 是活跃事故在唯一索引里占的固定值。
//
// 取 1 而不是 0：0 容易与「未初始化的 int64 零值」混淆，
// 而这一列的语义是「我是活跃行」，写错会直接破坏去重。
const incidentActiveMarker = 1

// Incident 是事故实例。一个聚合键可以对应多条（一条活跃 + 任意多条历史）。
type Incident struct {
	ID string `gorm:"primaryKey;size:64;column:id"`

	// DedupKey 是稳定聚合键（SHA-256 hex，定长 64）。
	// 刻意**不单独唯一**：见文件头说明。
	DedupKey string `gorm:"size:64;column:dedup_key;index;uniqueIndex:idx_incident_active_dedup,priority:1"`
	// DedupKeyCanonical 是算出 DedupKey 的规范串，供人工核对。
	DedupKeyCanonical string `gorm:"size:512;column:dedup_key_canonical"`

	// ActiveMarker：活跃事故恒为 incidentActiveMarker(1)，
	// 事故结束时改写为该事故自己的数值 ID 派生值，从而让出活跃槽位。
	ActiveMarker int64 `gorm:"column:active_marker;uniqueIndex:idx_incident_active_dedup,priority:2"`

	// Status 生命周期：open / resolved。
	// 不设 "closed"：事故只有「仍在观测」与「观测段已结束」两态，
	// 「已恢复健康」是 D 期恢复门禁的结论，不由本期的观测段结束代替。
	Status string `gorm:"size:16;column:status;index"`

	PrimaryFaultClass string `gorm:"size:48;column:primary_fault_class;index"`
	Scenario          string `gorm:"size:48;column:scenario"`

	// Severity 是当前段判定结果；PeakSeverity 是历史最高，只用于展示与审计。
	Severity     string `gorm:"size:8;column:severity;index"`
	PeakSeverity string `gorm:"size:8;column:peak_severity"`

	// Responsibility / EvidenceLevel 分别判断，不互相换算。
	Responsibility string `gorm:"size:32;column:responsibility"`
	EvidenceLevel  string `gorm:"size:16;column:evidence_level"`

	// 渠道三态：assigned / not_assigned / unknown，不用 channel_id=0 兼表两义。
	ChannelScope string `gorm:"size:16;column:channel_scope"`
	ChannelID    *int64 `gorm:"column:channel_id;index"`

	Model           string `gorm:"size:128;column:model;index"`
	UserGroup       string `gorm:"size:64;column:user_group"`
	IngressProtocol string `gorm:"size:32;column:ingress_protocol"`
	Route           string `gorm:"size:128;column:route"`
	ConfigChangeRef string `gorm:"size:64;column:config_change_ref"`

	FirstSeenTs int64 `gorm:"column:first_seen_ts;index"`
	LastSeenTs  int64 `gorm:"column:last_seen_ts;index"`
	ResolvedTs  int64 `gorm:"column:resolved_ts"`

	// CooldownUntilTs 是冷却到期时刻。冷却期内**继续检测和更新证据**，
	// 不暂停处理；它只决定「已结束的事故能否被新异常复用」。
	CooldownUntilTs int64 `gorm:"column:cooldown_until_ts"`

	// Revision 乐观并发版本号，每次更新 +1。
	Revision int64 `gorm:"column:revision"`

	CreatedAt int64 `gorm:"column:created_at"`
	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (Incident) TableName() string { return "incidents" }

// IncidentSegment 是一个连续异常段。
//
// 事故结束后复发（冷却未到期）会追加新段而不是新建事故，
// 所以「段」才是当前段证据的归属单位——当前段的 HardSignal/MultiSourceConfirmed
// 存在这里，不存在 Incident 上，从结构上杜绝跨段继承。
type IncidentSegment struct {
	ID         string `gorm:"primaryKey;size:64;column:id"`
	IncidentID string `gorm:"size:64;column:incident_id;index;uniqueIndex:idx_incident_segment_seq,priority:1"`
	// SeqNo 在同一事故内从 1 递增。唯一索引防并发追加同号段。
	SeqNo int64 `gorm:"column:seq_no;uniqueIndex:idx_incident_segment_seq,priority:2"`

	StartTs int64 `gorm:"column:start_ts;index"`
	EndTs   int64 `gorm:"column:end_ts"`

	// 当前段证据：正常窗口打断后不跨段继承。
	ConsecutiveWindows   int64  `gorm:"column:consecutive_windows"`
	HardSignal           bool   `gorm:"column:hard_signal"`
	MultiSourceConfirmed bool   `gorm:"column:multi_source_confirmed"`
	LowSample            bool   `gorm:"column:low_sample"`
	Severity             string `gorm:"size:8;column:severity"`

	CreatedAt int64 `gorm:"column:created_at"`
	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (IncidentSegment) TableName() string { return "incident_segments" }

// IncidentContinuity 是连续窗口判定状态的持久化形态。
//
// 历史证据（Ever*）与当前段证据分开存：前者挂事故，后者挂段。
type IncidentContinuity struct {
	IncidentID string `gorm:"primaryKey;size:64;column:incident_id"`

	LastCountedWindowToTs   int64 `gorm:"column:last_counted_window_to_ts"`
	LastFinalizedWindowToTs int64 `gorm:"column:last_finalized_window_to_ts"`
	LastWindowSample        int64 `gorm:"column:last_window_sample"`
	GapWindows              int64 `gorm:"column:gap_windows"`
	LowSampleWindows        int64 `gorm:"column:low_sample_windows"`

	// 历史证据跨段保留，**不得**作为新段的确认依据。
	EverHardSignal           bool `gorm:"column:ever_hard_signal"`
	EverMultiSourceConfirmed bool `gorm:"column:ever_multi_source_confirmed"`

	SampleScope   string `gorm:"size:32;column:sample_scope"`
	WindowSeconds int64  `gorm:"column:window_seconds"`

	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (IncidentContinuity) TableName() string { return "incident_continuity" }
