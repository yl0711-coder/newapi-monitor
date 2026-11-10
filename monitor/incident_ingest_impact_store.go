package monitor

// 影响面的窗口贡献层（MON-003 B3）。
//
// ★ 为什么需要第二张表 ★
//
// IncidentImpact 的唯一索引只有 (incident_id, dimension, dim_value)，
// **不含窗口**。直接对它 upsert，第二个窗口会把第一个窗口的值覆盖掉：
// 窗 A 20 样本、窗 B 30 样本，结果是 30 而不是累计 50。
//
// 所以分两层：这张表按窗口存贡献（含窗口的唯一键 → 同窗重放是替换，
// 幂等，不重复计数），IncidentImpact 由它 SUM 出来并整行重写。
//
// ★ 归档行必须有独立身份 ★
//
// 归档基线如果沿用普通窗口的 window_to_ts，就会与其中一个真实窗口撞键，
// 正常 upsert 或重放会覆盖掉累计基线。所以唯一键里加 row_kind，
// 归档行恒 row_kind='archive' 且 window_to_ts 固定 0（它不代表某个窗口）。
// 区间信息另存 archive_from_ts / archive_through_ts。
//
// 由此而来的两条配套规则：
//  1. 汇总的时间范围必须按 row_kind 分别读取。只剩归档行时直接
//     MAX(window_to_ts) 会把事故结束时间算成 0。
//  2. ArchiveThroughTs 是归档水位。window_to_ts <= 水位的贡献不再写入
//     本层，只记一条 replay_after_archive 修订——否则重放会插回一行明细，
//     再被 SUM 进汇总，与已折叠进归档的同一窗口重复计数。

const (
	incidentImpactRowKindWindow  = "window"
	incidentImpactRowKindArchive = "archive"
)

// 本轮有事实的三个维度。ingress_protocol / node / user 无事实，不写。
const (
	incidentImpactDimChannel   = "channel"
	incidentImpactDimModel     = "model"
	incidentImpactDimUserGroup = "user_group"
)

// IncidentImpactWindow 是某事故某维度在某窗口的影响面贡献。
type IncidentImpactWindow struct {
	ID string `gorm:"primaryKey;size:96;column:id"`

	IncidentID string `gorm:"size:64;column:incident_id;uniqueIndex:idx_incident_impact_window,priority:1;index"`
	Dimension  string `gorm:"size:32;column:dimension;uniqueIndex:idx_incident_impact_window,priority:2"`
	DimValue   string `gorm:"size:256;column:dim_value;uniqueIndex:idx_incident_impact_window,priority:3"`
	// RowKind 进唯一键，使归档行与真实窗口永不撞键。
	RowKind string `gorm:"size:16;column:row_kind;uniqueIndex:idx_incident_impact_window,priority:4"`
	// 普通窗口为该窗右界；归档行恒为 0。
	WindowToTs int64 `gorm:"column:window_to_ts;uniqueIndex:idx_incident_impact_window,priority:5"`

	SegmentID    string `gorm:"size:64;column:segment_id"`
	WindowFromTs int64  `gorm:"column:window_from_ts"`

	// 归档行的区间。普通窗口为 0。
	ArchiveFromTs       int64 `gorm:"column:archive_from_ts"`
	ArchiveThroughTs    int64 `gorm:"column:archive_through_ts"`
	ArchivedWindowCount int64 `gorm:"column:archived_window_count"`

	// nil 表示未知，不是 0（宪法：缺失绝不显示为零）。
	SampleTotal      *int64 `gorm:"column:sample_total"`
	AnomalyTotal     *int64 `gorm:"column:anomaly_total"`
	DistinctRequests *int64 `gorm:"column:distinct_requests"`
	UncoveredSeconds *int64 `gorm:"column:uncovered_seconds"`
	PendingSeconds   *int64 `gorm:"column:pending_seconds"`

	CoverageComplete bool   `gorm:"column:coverage_complete"`
	WindowState      string `gorm:"size:24;column:window_state"`
	// Incomplete=true 表示本行数值由 nil 与非 nil 混合而来，汇总时不可当完整值。
	Incomplete bool `gorm:"column:incomplete"`

	UpdatedAt int64 `gorm:"column:updated_at"`
}

func (IncidentImpactWindow) TableName() string { return "incident_impact_windows" }
