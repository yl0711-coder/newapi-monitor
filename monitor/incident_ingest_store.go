package monitor

// Incident 采集侧持久化模型（MON-003 B3）。
//
// ★ 两个水位必须分开 ★
//
//	ScanThroughTs     扫描进度。只要窗口已落下某个状态行（含永久失败）即可越过。
//	                  保证循环不被单个无法处理的窗口永久卡死。
//	CompleteThroughTs 完整处理水位。要求其之前所有窗口 data_complete=true。
//	                  这是「来源数据完整」的对外凭证。
//
// 「流程执行完成」≠「来源数据完整」：process_state 表达前者，
// data_complete 表达后者。永久失败推进 ScanThroughTs，但**绝不**推进
// CompleteThroughTs；删除失败记录也不会让水位跳过——明细到期只能折叠进
// IncidentIngestGapSummary，摘要继续等效阻挡 CompleteThroughTs。

// 窗口态：来源事实对这一窗说明了什么。
const (
	// 覆盖还没到且仍可能推进。不送引擎。
	incidentWindowStateWaitCoverage = "wait_coverage"
	// 该聚合键本窗存在非当前口径版本的行。口径不可用，不送引擎、不算正常。
	incidentWindowStateMixedVersion = "mixed_version"
	// 水位已越过但本窗覆盖不完整。**当前确认存在缺口**，可被历史补采消退。
	incidentWindowStateGapObserved = "gap_observed"
	// 覆盖完整但该键无匹配行。**不是**恢复，也不是缺失。不送引擎。
	incidentWindowStateNoSample = "no_sample"
	incidentWindowStateNormal   = "normal"
	incidentWindowStateAnomaly  = "anomalous"
)

// 处理状态：流程走到哪一步。
const (
	// 等待来源满足条件（wait_coverage / mixed_version）。**引擎禁止调用**。
	incidentProcessAwaitingSource = "awaiting_source"
	// 判定输入已快照冻结，只等引擎提交。重启恢复只认这个与 engine_done。
	incidentProcessInputFrozen = "input_frozen"
	// 引擎已提交，待写影响面。
	incidentProcessEngineDone = "engine_done"
	// 流程终结（不论有无事故归属）。是否完整看 data_complete。
	incidentProcessDone = "done"
	// 失败，可重试。
	incidentProcessFailed = "failed"
	// 失败，有界重试后放弃。不再重试、不占待处理队列。
	incidentProcessFailedPermanent = "failed_permanent"
	// 引擎的旧窗口归属硬错误（isStaleUnattributedWindow）。不重试。
	incidentProcessUnattributable = "unattributable"
)

// IncidentIngestCursor 是单例全局游标。无行增长，无需清理。
type IncidentIngestCursor struct {
	ID int64 `gorm:"primaryKey;autoIncrement:false;column:id"`

	// ScanThroughTs / CompleteThroughTs 见文件头。两者差值即积压区间。
	ScanThroughTs     int64 `gorm:"column:scan_through_ts"`
	CompleteThroughTs int64 `gorm:"column:complete_through_ts"`

	// BacklogMode: normal / degraded。degraded 时暂停扫描新窗口，
	// 但**继续推进**所有非终结行，所以能自动恢复，不会永久空转。
	BacklogMode string `gorm:"size:16;column:backlog_mode"`

	// LastScheduledDedupKey 是来源推进队列的按键轮转起点。
	// 没有它，每轮都取最旧的同一批，其他键的可处理任务永远排不上。
	LastScheduledDedupKey string `gorm:"size:64;column:last_scheduled_dedup_key"`

	TickCount int64 `gorm:"column:tick_count"`
	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (IncidentIngestCursor) TableName() string { return "incident_ingest_cursors" }

// incidentIngestCursorID 单例主键。
const incidentIngestCursorID = 1

// IncidentIngestWindowState 是「一个聚合键 × 一个窗口」的处理状态。
//
// 主键 (dedup_key, window_to_ts) 让重复扫描天然幂等。
//
// 这张表同时承担：窗口态、处理状态机、失败重试、待判场景、复核计数、
// 判定输入快照。不另立失败窗口表——一个窗口一行，避免两套状态互相漂移。
type IncidentIngestWindowState struct {
	DedupKey   string `gorm:"primaryKey;size:64;column:dedup_key"`
	WindowToTs int64  `gorm:"primaryKey;autoIncrement:false;column:window_to_ts"`

	CanonicalKey string `gorm:"size:512;column:canonical_key"`
	WindowFromTs int64  `gorm:"column:window_from_ts;index"`

	WindowState  string `gorm:"size:24;column:window_state;index"`
	ProcessState string `gorm:"size:24;column:process_state;index"`

	// DataComplete 与 ProcessState 独立：流程跑完不代表来源数据完整。
	// 只有「覆盖完整 + 非混版 + 引擎与影响面均成功」才为 true。
	DataComplete bool `gorm:"column:data_complete;index"`

	// 引擎归属。写 input_frozen 时为空，engine_done 时落库。
	// 崩溃恢复靠它，不靠重新调用引擎去取。
	IncidentID string `gorm:"size:64;column:incident_id;index"`
	SegmentID  string `gorm:"size:64;column:segment_id"`
	// Severity / Outcome 是引擎返回值的副本，仅供展示与比对，不参与判定。
	Severity string `gorm:"size:8;column:severity"`
	Outcome  string `gorm:"size:32;column:outcome"`
	Action   string `gorm:"size:32;column:action"`

	// ★ 场景待确认是显式状态，不从 severity 反推 ★
	//
	// 引擎的更新路径不同步 scenario（incident_engine.go 的 updates map 里
	// 没有这一列），空 severity 也可能来自别的原因。所以接口的
	// scenario_pending 只能读这里，不得写 severity=="" → pending 这种推导。
	ScenarioPending bool   `gorm:"column:scenario_pending;index"`
	PendingReason   string `gorm:"size:48;column:pending_reason"`

	// ★ 判定输入快照 ★
	//
	// 在通过覆盖与混版检查后、调引擎**之前**随 input_frozen 一同落库。
	// 崩溃恢复用这份快照重放，绝不重新聚合——期间来源数据可能已变，
	// 那会把新数据冒充原判定依据。
	SampleTotal    *int64   `gorm:"column:sample_total"`
	AnomalyTotal   *int64   `gorm:"column:anomaly_total"`
	SuccessTotal   *int64   `gorm:"column:success_total"`
	AnomalyRate    *float64 `gorm:"column:anomaly_rate"`
	MixedRows      int64    `gorm:"column:mixed_version_rows"`
	CoverageProof  string   `gorm:"size:256;column:coverage_proof"`
	EvidenceLevel  string   `gorm:"size:16;column:evidence_level"`
	Responsibility string   `gorm:"size:32;column:responsibility"`

	// ★ 最新实测值与上面的判定快照分开保存 ★
	//
	// 上面那组是**引擎当时据以判定的输入**，定稿后不得回改——否则就无法再
	// 回答「当初为什么这么判」。迟到修订与缺口补齐发现的新数据落在这组
	// Measured* 列里，供展示「最新实测」与差异对比。
	// 两组都为 nil 表示从未冻结过输入；Measured* 为 nil 表示尚未发现修订。
	MeasuredSampleTotal   *int64   `gorm:"column:measured_sample_total"`
	MeasuredAnomalyTotal  *int64   `gorm:"column:measured_anomaly_total"`
	MeasuredSuccessTotal  *int64   `gorm:"column:measured_success_total"`
	MeasuredAnomalyRate   *float64 `gorm:"column:measured_anomaly_rate"`
	MeasuredWindowState   string   `gorm:"size:24;column:measured_window_state"`
	MeasuredCoverageProof string   `gorm:"size:256;column:measured_coverage_proof"`
	MeasuredAt            int64    `gorm:"column:measured_at"`

	// BelowMinSample 是能力边界标记，不是窗口态：样本数低与正常/异常正交。
	// 低样本窗同样算异常率，可能超阈值也可能不超，由引擎按 MinSample 决定 Outcome。
	BelowMinSample bool `gorm:"column:below_min_sample"`

	// 复核关闭标记。都是**能力边界**，不是失败状态。
	//   RecheckClosed      迟到修订复核次数耗尽（2h / 6 次）
	//   GapRecheckClosed   缺口超过 7 天补采窗仍未补齐
	//   MixedVersionClosed 混版超过 7 天仍未修复
	RecheckClosed      bool `gorm:"column:recheck_closed"`
	GapRecheckClosed   bool `gorm:"column:gap_recheck_closed"`
	MixedVersionClosed bool `gorm:"column:mixed_version_closed"`

	// RecheckAttempts 只归**迟到修订复核队列**用，上限 RecheckMaxAttempts。
	// SourceCheckAttempts 只归**来源推进队列**用，无上限、仅作观测与退避输入。
	// 两条队列不共用计数器：否则「持续等待至 7 天」会被 6 次上限提前掐断。
	RecheckAttempts     int `gorm:"column:recheck_attempts"`
	SourceCheckAttempts int `gorm:"column:source_check_attempts"`
	// NextSourceCheckAt 退避时刻。长期无法补齐的老键间隔被拉长，
	// 不会每轮都挤占 PendingBatch 配额。
	NextSourceCheckAt int64 `gorm:"column:next_source_check_at;index"`

	// 失败重试。
	Attempts    int    `gorm:"column:attempts"`
	NextRetryAt int64  `gorm:"column:next_retry_at;index"`
	LastError   string `gorm:"size:512;column:last_error"`

	// ReplayableUntil = window_to_ts + RetentionDays*86400。
	// 重放依赖 CapacityUserMinuteSample，源行被清理后窗口即不可重放。
	// 到期不删记录，只置 SourceFactsExpired。
	ReplayableUntil    int64 `gorm:"column:replayable_until"`
	SourceFactsExpired bool  `gorm:"column:source_facts_expired"`

	FinalizedAt int64 `gorm:"column:finalized_at"`
	CreatedAt   int64 `gorm:"column:created_at"`
	UpdatedAt   int64 `gorm:"column:updated_at"`
}

func (IncidentIngestWindowState) TableName() string { return "incident_ingest_window_states" }

// IncidentIngestRevision 是只增不改的修订轨迹。
//
// 引擎的 writeObservation 用 OnConflict DoNothing，已写窗口的修订进不去；
// 且 B2 的连续性单调性不允许回改已判定窗口。所以修订在这里留痕，
// 并更新影响面的窗口贡献，**不改写引擎侧的观测证据与严重度判定**。
// IncidentPendingSummary 是事故级的待确认结论。
//
// ★ 为什么需要事故级这一层 ★
//
// 待确认结论原本只存在窗口行上，而窗口行会被保留期清理删除；活跃事故的
// scenario_pending 于是从 true 变成 false、pending_reason 变空——结论被
// 清理抹掉了。按 dedup_key 回查窗口也不行：同键的其他事故会冒充本事故。
//
// 所以结论在产生时就按 IncidentID 落到这里，生命周期跟随事故本身，
// 不受窗口清理影响。
type IncidentPendingSummary struct {
	IncidentID      string `gorm:"primaryKey;size:64;column:incident_id"`
	ScenarioPending bool   `gorm:"column:scenario_pending;index"`
	PendingReason   string `gorm:"size:48;column:pending_reason"`
	// FirstPendingAt / UpdatedAt 便于判断结论存续多久仍未确认。
	FirstPendingAt int64 `gorm:"column:first_pending_at"`
	UpdatedAt      int64 `gorm:"column:updated_at"`
}

func (IncidentPendingSummary) TableName() string { return "incident_pending_summaries" }

type IncidentIngestRevision struct {
	ID string `gorm:"primaryKey;size:96;column:id"`

	DedupKey   string `gorm:"size:64;column:dedup_key;index:idx_incident_ingest_rev_win,priority:1"`
	WindowToTs int64  `gorm:"column:window_to_ts;index:idx_incident_ingest_rev_win,priority:2"`
	IncidentID string `gorm:"size:64;column:incident_id;index"`

	WindowFromTs int64 `gorm:"column:window_from_ts"`
	RevisionSeq  int   `gorm:"column:revision_seq"`

	// Kind: revised_after_finalize / gap_backfilled / gap_confirmed /
	//       replay_after_archive / scenario_resolvable
	Kind string `gorm:"size:32;column:kind;index"`

	OldSampleTotal *int64   `gorm:"column:old_sample_total"`
	NewSampleTotal *int64   `gorm:"column:new_sample_total"`
	OldAnomalyRate *float64 `gorm:"column:old_anomaly_rate"`
	NewAnomalyRate *float64 `gorm:"column:new_anomaly_rate"`
	OldWindowState string   `gorm:"size:24;column:old_window_state"`
	NewWindowState string   `gorm:"size:24;column:new_window_state"`

	// ★ 异常构成必须单独留痕 ★
	//
	// 只记 sample_total 时，「总数恒为 100、异常数 90→10」这种构成变化
	// 在修订记录里完全看不出来。所以异常/成功计数与来源行是否消失都记。
	OldAnomalyTotal *int64 `gorm:"column:old_anomaly_total"`
	NewAnomalyTotal *int64 `gorm:"column:new_anomaly_total"`
	OldSuccessTotal *int64 `gorm:"column:old_success_total"`
	NewSuccessTotal *int64 `gorm:"column:new_success_total"`
	// SourceRowsMissing 表示复核时该聚合键在来源里已查不到匹配行。
	SourceRowsMissing bool   `gorm:"column:source_rows_missing"`
	OldCoverageProof  string `gorm:"size:256;column:old_coverage_proof"`

	// RevisionDetectLagSeconds = detected_at - finalized_at。
	//
	// ★ 这不是日志实际迟到时长 ★
	// 它只表示「定稿后多久被复核发现」，受复核频率离散化、受复核范围与
	// 次数上限截断，且只统计**被发现的**修订——未被发现的迟到不在样本内，
	// 是系统性低估。不足以单独判断 FinalizeLagSeconds 是否够用。
	RevisionDetectLagSeconds int64 `gorm:"column:revision_detect_lag_seconds"`

	CoverageProof string `gorm:"size:256;column:coverage_proof"`
	Note          string `gorm:"size:256;column:note"`
	DetectedAt    int64  `gorm:"column:detected_at;index"`
}

func (IncidentIngestRevision) TableName() string { return "incident_ingest_revisions" }

// IncidentIngestGapSummary 是缺口摘要：已终结且不再需要处理的明细的折叠结果。
//
// ★ 为什么需要它 ★
//
// 水位正确性不能靠永久保留全部明细（会无界增长），也不能靠直接删除
// （删掉就会让 CompleteThroughTs 跳过并显示为成功）。折叠成摘要后，
// 摘要区间与明细**等效地**阻挡 CompleteThroughTs。
//
// ★ 只合并连续区间 ★
//
// 绝不跨过尚未归档的窗口：否则外层区间会把那个窗口误认为「已计数」，
// 它后来被归档时反而被跳过（漏计）。非连续缺口产生多条行。
type IncidentIngestGapSummary struct {
	DedupKey  string `gorm:"primaryKey;size:64;column:dedup_key"`
	GapFromTs int64  `gorm:"primaryKey;autoIncrement:false;column:gap_from_ts"`

	GapThroughTs int64 `gorm:"column:gap_through_ts;index"`

	WindowCount int64 `gorm:"column:window_count"`
	// 分类计数，便于区分「缺口」「永久失败」「无法归属」。
	GapObservedCount     int64 `gorm:"column:gap_observed_count"`
	FailedPermanentCount int64 `gorm:"column:failed_permanent_count"`
	UnattributableCount  int64 `gorm:"column:unattributable_count"`

	FirstDetectedAt int64 `gorm:"column:first_detected_at"`
	ArchivedAt      int64 `gorm:"column:archived_at"`
}

func (IncidentIngestGapSummary) TableName() string { return "incident_ingest_gap_summaries" }
