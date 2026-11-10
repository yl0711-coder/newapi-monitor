package monitor

// Incident 观测证据、影响面、四层状态与审计记录（MON-003 B1 续）。

// IncidentObservation 是一条观测证据，构成详情页的证据时间线。
//
// 一条观测属于某个段（SegmentID），不直接挂事故——
// 这样「当前段看到了什么」与「这个事故历史上看到过什么」天然分开。
type IncidentObservation struct {
	ID         string `gorm:"primaryKey;size:64;column:id"`
	IncidentID string `gorm:"size:64;column:incident_id;index"`
	SegmentID  string `gorm:"size:64;column:segment_id;index"`

	// WindowFromTs/WindowToTs 左闭右开。跨页核对必须用时间戳，不从日期反推。
	WindowFromTs int64 `gorm:"column:window_from_ts;index"`
	WindowToTs   int64 `gorm:"column:window_to_ts"`

	// DataPresent=false 表示采集缺口。缺口不等于正常，也不等于异常。
	DataPresent bool `gorm:"column:data_present"`
	Anomalous   bool `gorm:"column:anomalous"`

	// SourceCount 参与确认的来源数；SourceIDs 逗号分隔，供人工核对来源构成。
	SourceCount int64  `gorm:"column:source_count"`
	SourceIDs   string `gorm:"size:256;column:source_ids"`

	HardSignal bool `gorm:"column:hard_signal"`

	// 三个计数分别存，不合并：语义不同，合并会让「部分覆盖冒充全量」。
	// 同样用 *int64：算不出来是 nil，不是 0。
	DistinctRequests *int64 `gorm:"column:distinct_requests"`
	VisibleAttempts  *int64 `gorm:"column:visible_attempts"`
	LogRows          *int64 `gorm:"column:log_rows"`

	// 退化说明：取不到渠道、无法归并的行数，不静默。
	RowsWithoutChannel int64 `gorm:"column:rows_without_channel"`
	UnlinkableRows     int64 `gorm:"column:unlinkable_rows"`
	AttemptsExact      bool  `gorm:"column:attempts_exact"`

	// SampleTotal nil 表示样本量未知；LowSample 只在样本已知且低于门限时为真。
	SampleTotal *int64 `gorm:"column:sample_total"`
	LowSample   bool   `gorm:"column:low_sample"`

	// EvidenceLevel 由关联方式决定，不从置信度换算。
	EvidenceLevel  string `gorm:"size:16;column:evidence_level"`
	EvidenceMethod string `gorm:"size:32;column:evidence_method"`

	// Note 已脱敏（过 stabilityProblemText）。Truncated 记录是否因截断丢信息。
	Note      string `gorm:"size:512;column:note"`
	Truncated bool   `gorm:"column:truncated"`

	CreatedAt int64 `gorm:"column:created_at"`
}

func (IncidentObservation) TableName() string { return "incident_observations" }

// IncidentImpact 是事故影响面的一个维度条目。
//
// 六维（渠道/模型/入口协议/分组/节点/用户）用同一张表 + Dimension 区分，
// 而不是六张表：维度会增减，表结构不该跟着变。
type IncidentImpact struct {
	ID         string `gorm:"primaryKey;size:64;column:id"`
	IncidentID string `gorm:"size:64;column:incident_id;index;uniqueIndex:idx_incident_impact_dim,priority:1"`

	// Dimension: channel / model / ingress_protocol / user_group / node / user
	Dimension string `gorm:"size:32;column:dimension;uniqueIndex:idx_incident_impact_dim,priority:2"`
	// DimValue 已按需脱敏（user 维度尤其）。
	DimValue string `gorm:"size:256;column:dim_value;uniqueIndex:idx_incident_impact_dim,priority:3"`

	// 时间窗左闭右开。
	WindowFromTs int64 `gorm:"column:window_from_ts"`
	WindowToTs   int64 `gorm:"column:window_to_ts"`

	// 三个计数分别展示，不互相代替；nil 表示未知，不是 0。
	DistinctRequests *int64 `gorm:"column:distinct_requests"`
	VisibleAttempts  *int64 `gorm:"column:visible_attempts"`
	LogRows          *int64 `gorm:"column:log_rows"`

	// SourceIDs 这条影响面的来源构成。
	SourceIDs string `gorm:"size:256;column:source_ids"`

	// 覆盖缺口：未知与真实零值必须可区分，所以用 *int64 而不是 int64。
	//
	//	nil → 未知（没算出来 / 来源不可用）
	//	0   → 已知且确实没有缺口
	//	>0  → 已知的缺口秒数
	//
	// 用普通 int64 会让数据库 NULL 读成 0，再保存就把「未知」写成了
	// 「已知无缺口」——那正是「未知不能填零」要防的事。
	CoverageComplete bool   `gorm:"column:coverage_complete"`
	UncoveredSeconds *int64 `gorm:"column:uncovered_seconds"`
	PendingSeconds   *int64 `gorm:"column:pending_seconds"`
	// Partial=true 表示本条是分页/部分覆盖结果，不得当全量用。
	Partial bool `gorm:"column:partial"`

	SampleTotal *int64 `gorm:"column:sample_total"`

	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (IncidentImpact) TableName() string { return "incident_impacts" }
