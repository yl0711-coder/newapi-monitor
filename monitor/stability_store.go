package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"regexp"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// StabilityHourSample 是历史稳定性报表自己的长期维度汇总。
//
// 它由两条互不并发覆盖的路径生成：近期小时从本地 metric_samples 滚动汇总，
// 长期缺口由限速补数任务按单小时只读生产 logs 后写入。页面始终只读 Monitor
// 本地 SQLite；复合主键和小时完整性台账保证重复补数是完整替换而不是累加。
type StabilityHourSample struct {
	HourTs              int64  `gorm:"primaryKey;autoIncrement:false;index:idx_stability_hour;index:idx_stability_group_hour,priority:2;index:idx_stability_channel_hour,priority:2;index:idx_stability_model_hour,priority:2"`
	ChannelID           int    `gorm:"primaryKey;autoIncrement:false;index:idx_stability_channel_hour,priority:1"`
	ModelName           string `gorm:"primaryKey;size:128;column:model_name;index:idx_stability_model_hour,priority:1"`
	Grp                 string `gorm:"primaryKey;size:64;column:grp;index:idx_stability_group_hour,priority:1"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	Success             int64
	Anomaly             int64
	Failed              int64
	AnomalyBilled       int64 `gorm:"column:anomaly_billed"`
	AnomalyFree         int64 `gorm:"column:anomaly_free"`
	AnomalyStream       int64 `gorm:"column:anomaly_stream"`
	AnomalyQuota        int64 `gorm:"column:anomaly_quota"`
	SumUseTime          int64 `gorm:"column:sum_use_time"`
	MaxUseTime          int   `gorm:"column:max_use_time"`
	Tokens              int64
	Quota               int64
	Err4xx              int64 `gorm:"column:err_4xx"`
	Err5xx              int64 `gorm:"column:err_5xx"`
	ErrTimeout          int64 `gorm:"column:err_timeout"`
	ErrOther            int64 `gorm:"column:err_other"`
}

func (s *StabilityHourSample) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// ChannelTestHourSample 单独保存 NewAPI 渠道管理的内部模型测试。
// 这些请求真实消耗上游资源，因此保留请求、Token 和 quota 成本基数；但它们绝不
// 进入用户请求、用户侧消费或稳定性收入统计。未修改 NewAPI 不记录
// 手动/定时与单渠道/全渠道来源，所以 Scope 统一为 legacy；
// Origin 仅作为旧表复合主键中的成本口径分桶，不表示请求来源。
type ChannelTestHourSample struct {
	HourTs              int64  `gorm:"primaryKey;autoIncrement:false;index:idx_channel_test_hour;index:idx_channel_test_channel_hour,priority:2"`
	ChannelID           int    `gorm:"primaryKey;autoIncrement:false;index:idx_channel_test_channel_hour,priority:1"`
	ModelName           string `gorm:"primaryKey;size:128;column:model_name"`
	Grp                 string `gorm:"primaryKey;size:64;column:grp"`
	Origin              string `gorm:"primaryKey;size:16;column:origin"` // legacy_base / legacy_tiered cost bucket
	Scope               string `gorm:"size:16;column:scope"`             // legacy; additive and intentionally not part of the PK
	CostBasis           string `gorm:"size:24;column:cost_basis"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	Requests            int64
	Success             int64
	Anomaly             int64
	Failed              int64
	Tokens              int64
	Quota               int64 // 渠道测试结算的模型成本基数；不是用户收入
	SumUseTime          int64 `gorm:"column:sum_use_time"`
	MaxUseTime          int   `gorm:"column:max_use_time"`
}

func (s *ChannelTestHourSample) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	if s.CostBasis == "" {
		s.CostBasis = "legacy_assumed_base"
	}
	s.Origin = "legacy_base"
	if s.CostBasis == "legacy_after_group" {
		s.Origin = "legacy_tiered"
	}
	s.Scope = "legacy"
	if s.Requests > 0 && s.Success+s.Anomaly+s.Failed == 0 {
		s.Success = s.Requests - s.Anomaly
	}
	return nil
}

// StabilityRejectHour 把未进入真实渠道的前置拒绝长期保留。
// 这类请求会计入分组/模型的用户交付稳定性，但永远不归因给某个渠道。
type StabilityRejectHour struct {
	HourTs int64  `gorm:"primaryKey;autoIncrement:false;index:idx_stability_reject_hour;index:idx_stability_reject_group_hour,priority:2"`
	Node   string `gorm:"primaryKey;size:64"`
	Reason string `gorm:"primaryKey;size:64"`
	Model  string `gorm:"primaryKey;size:128;index:idx_stability_reject_model_hour,priority:1"`
	Grp    string `gorm:"primaryKey;size:64;column:grp;index:idx_stability_reject_group_hour,priority:1"`
	Count  int64
}

// StabilityProblemSample 按分钟保存原始错误签名的客观计数。
// Message 保留 logs.content 原文；SignatureHash 只用于稳定主键，不参与展示或归因。
// Source 为 newapi/nginx_access/nginx_error/pre_route，后两类为后续旁路采集预留。
type StabilityProblemSample struct {
	BucketTs            int64  `gorm:"primaryKey;autoIncrement:false;index:idx_stability_problem_bucket;index:idx_stability_problem_group_bucket,priority:2;index:idx_stability_problem_channel_bucket,priority:2;index:idx_stability_problem_model_bucket,priority:2"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	Source              string `gorm:"primaryKey;size:24;index:idx_stability_problem_source_bucket,priority:1"`
	SignatureHash       string `gorm:"primaryKey;size:64;column:signature_hash"`
	ChannelID           int    `gorm:"primaryKey;autoIncrement:false;index:idx_stability_problem_channel_bucket,priority:1"`
	ModelName           string `gorm:"primaryKey;size:128;column:model_name;index:idx_stability_problem_model_bucket,priority:1"`
	Grp                 string `gorm:"primaryKey;size:64;column:grp;index:idx_stability_problem_group_bucket,priority:1"`
	Node                string `gorm:"primaryKey;size:64"`
	Path                string `gorm:"primaryKey;size:160"`
	Code                string `gorm:"size:32"`
	Message             string `gorm:"type:text"`
	Count               int64
	FirstTs             int64 `gorm:"column:first_ts"`
	LastTs              int64 `gorm:"column:last_ts"`
	Truncated           bool
}

func (s *StabilityProblemSample) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// StabilityProblemIngestState 记录每个完整分钟的原始错误采集进度。
// 正常分钟一次完成；故障高峰超过单轮预算时保存游标，下轮继续，避免整窗丢弃。
type StabilityProblemIngestState struct {
	BucketTs            int64 `gorm:"primaryKey;autoIncrement:false"`
	TrafficClassVersion int   `gorm:"column:traffic_class_version;index"`
	LastCreatedAt       int64 `gorm:"column:last_created_at"`
	LastID              int64 `gorm:"column:last_id"`
	RowsScanned         int64 `gorm:"column:rows_scanned"`
	Complete            bool  `gorm:"index"`
	UpdatedAt           int64 `gorm:"index"`
	CompletedAt         int64 `gorm:"column:completed_at"`
}

// StabilityProblemClassificationMigration is the small durable cursor for
// rebuilding raw problem minutes after a traffic-classification change. Old
// problem rows remain intact until each bounded source window is replaced;
// ordinary startup never clears the historical tables.
type StabilityProblemClassificationMigration struct {
	ID                  uint   `gorm:"primaryKey;autoIncrement:false"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	FromTs              int64  `gorm:"column:from_ts"`
	ThroughTs           int64  `gorm:"column:through_ts"`
	NextTs              int64  `gorm:"column:next_ts;index"`
	Status              string `gorm:"size:16;index;column:status"`
	CurrentSpanMinutes  int    `gorm:"column:current_span_minutes"`
	HealthyWindows      int    `gorm:"column:healthy_windows"`
	Attempts            int    `gorm:"column:attempts"`
	NextRetryAt         int64  `gorm:"column:next_retry_at;index"`
	LastError           string `gorm:"size:512;column:last_error"`
	LastSuccessAt       int64  `gorm:"column:last_success_at"`
	LastFailureAt       int64  `gorm:"column:last_failure_at"`
	CreatedAt           int64  `gorm:"column:created_at"`
	UpdatedAt           int64  `gorm:"column:updated_at"`
	CompletedAt         int64  `gorm:"column:completed_at"`
}

// StabilityProblemLiveCursor is the independent durable recent-Tail
// watermark. It must not be inferred from the shared minute table because a
// cold classification migration writes that table too. Keeping target and
// next separately prevents restart, a >12-minute outage or a many-page hot
// minute from jumping across an unsampled live gap.
type StabilityProblemLiveCursor struct {
	ID                  uint   `gorm:"primaryKey;autoIncrement:false"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	NextTs              int64  `gorm:"column:next_ts;index"`
	TargetThroughTs     int64  `gorm:"column:target_through_ts"`
	Status              string `gorm:"size:16;index;column:status"`
	Attempts            int    `gorm:"column:attempts"`
	NextRetryAt         int64  `gorm:"column:next_retry_at;index"`
	LastError           string `gorm:"size:512;column:last_error"`
	LastSuccessAt       int64  `gorm:"column:last_success_at"`
	LastFailureAt       int64  `gorm:"column:last_failure_at"`
	UpdatedAt           int64  `gorm:"column:updated_at"`
}

func (s *StabilityProblemIngestState) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// StabilityProblemStage 是高峰分页期间的本地暂存聚合。只有对应分钟完整读完后，
// 才会原子替换到 StabilityProblemSample，页面不会把半截数据当成事实。
type StabilityProblemStage struct {
	BucketTs            int64  `gorm:"primaryKey;autoIncrement:false;index"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	Source              string `gorm:"primaryKey;size:24"`
	SignatureHash       string `gorm:"primaryKey;size:64;column:signature_hash"`
	ChannelID           int    `gorm:"primaryKey;autoIncrement:false"`
	ModelName           string `gorm:"primaryKey;size:128;column:model_name"`
	Grp                 string `gorm:"primaryKey;size:64;column:grp"`
	Node                string `gorm:"primaryKey;size:64"`
	Path                string `gorm:"primaryKey;size:160"`
	Code                string `gorm:"size:32"`
	Message             string `gorm:"type:text"`
	Count               int64
	FirstTs             int64 `gorm:"column:first_ts"`
	LastTs              int64 `gorm:"column:last_ts"`
	Truncated           bool
}

func (s *StabilityProblemStage) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

const maxStabilityProblemMessage = 4096

var statusCodePattern = regexp.MustCompile(`(?i)(?:status[_ ]?code|http[_ ]?status|response\s+status\s+code|unexpected\s+status|code)\s*(?:[=:]\s*|\s+)([0-9]{3})`)

func stabilityProblemCode(message string) string {
	if m := statusCodePattern.FindStringSubmatch(message); len(m) == 2 {
		return m[1]
	}
	return ""
}

// stabilityProblemText 只做存储安全截断，不重写、不翻译、不归类原始错误。
// 截断按 rune 进行，避免把 UTF-8 字符切坏；调用方必须展示 Truncated 标记。
func stabilityProblemText(message string) (string, bool) {
	message = strings.TrimSpace(message)
	runes := []rune(message)
	if len(runes) <= maxStabilityProblemMessage {
		return message, false
	}
	return string(runes[:maxStabilityProblemMessage]), true
}

func stabilityProblemHash(source, message string) string {
	sum := sha256.Sum256([]byte(source + "\x00" + message))
	return hex.EncodeToString(sum[:])
}

// rollupStabilityHours 从分钟表生成分组×渠道×模型小时表。查询和写入都发生在
// Monitor 本地 SQLite，调用频率不会增加 NewAPI 生产库压力。
func (m *Monitor) rollupStabilityHours(sinceTs int64) error {
	return m.storeDB.Exec(`INSERT INTO stability_hour_samples (
		hour_ts, channel_id, model_name, grp, success, anomaly, failed,
		anomaly_billed, anomaly_free, anomaly_stream, anomaly_quota,
		sum_use_time, max_use_time, tokens, quota, err_4xx, err_5xx, err_timeout, err_other,
		traffic_class_version)
		SELECT (bucket_ts/3600)*3600 AS hour_ts, channel_id, model_name, grp,
		  SUM(success), SUM(anomaly), SUM(failed),
		  SUM(anomaly_billed), SUM(anomaly_free), SUM(anomaly_stream), SUM(anomaly_quota),
		  SUM(sum_use_time), MAX(max_use_time), SUM(tokens), SUM(quota),
		  SUM(err_4xx), SUM(err_5xx), SUM(err_timeout), SUM(err_other), ?
		FROM metric_samples WHERE bucket_ts >= ? AND traffic_class_version = ?
		  AND NOT EXISTS (
		    SELECT 1 FROM stability_hour_ingest_states hs
		    WHERE hs.hour_ts=(metric_samples.bucket_ts/3600)*3600 AND hs.status='complete'
		      AND hs.traffic_class_version=?
		  )
		GROUP BY hour_ts, channel_id, model_name, grp
		ON CONFLICT(hour_ts, channel_id, model_name, grp) DO UPDATE SET
		  success=excluded.success, anomaly=excluded.anomaly, failed=excluded.failed,
		  anomaly_billed=excluded.anomaly_billed, anomaly_free=excluded.anomaly_free,
		  anomaly_stream=excluded.anomaly_stream, anomaly_quota=excluded.anomaly_quota,
		  sum_use_time=excluded.sum_use_time, max_use_time=excluded.max_use_time,
		  tokens=excluded.tokens, quota=excluded.quota,
		  err_4xx=excluded.err_4xx, err_5xx=excluded.err_5xx,
		  err_timeout=excluded.err_timeout, err_other=excluded.err_other,
		  traffic_class_version=excluded.traffic_class_version`,
		userTrafficClassificationVersion, sinceTs, userTrafficClassificationVersion, userTrafficClassificationVersion).Error
}

func (m *Monitor) rollupStabilityRejections(sinceTs int64) error {
	return m.storeDB.Exec(`INSERT INTO stability_reject_hours (hour_ts, node, reason, model, grp, count)
		SELECT (bucket_ts/3600)*3600 AS hour_ts, node, reason, model, grp, SUM(count)
		FROM rejection_samples WHERE bucket_ts >= ?
		GROUP BY hour_ts, node, reason, model, grp
		ON CONFLICT(hour_ts, node, reason, model, grp) DO UPDATE SET count=excluded.count`, sinceTs).Error
}

func (m *Monitor) upsertStabilityProblems(rows []StabilityProblemSample) error {
	if len(rows) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "bucket_ts"}, {Name: "source"}, {Name: "signature_hash"},
			{Name: "channel_id"}, {Name: "model_name"}, {Name: "grp"},
			{Name: "node"}, {Name: "path"},
		},
		UpdateAll: true,
	}).CreateInBatches(rows, 200).Error
}

func upsertStabilityProblemStages(tx *gorm.DB, rows []StabilityProblemStage) error {
	if len(rows) == 0 {
		return nil
	}
	return tx.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "bucket_ts"}, {Name: "source"}, {Name: "signature_hash"},
			{Name: "channel_id"}, {Name: "model_name"}, {Name: "grp"},
			{Name: "node"}, {Name: "path"},
		},
		DoUpdates: clause.Assignments(map[string]any{
			"count":     gorm.Expr("stability_problem_stages.count + excluded.count"),
			"first_ts":  gorm.Expr("MIN(stability_problem_stages.first_ts, excluded.first_ts)"),
			"last_ts":   gorm.Expr("MAX(stability_problem_stages.last_ts, excluded.last_ts)"),
			"truncated": gorm.Expr("stability_problem_stages.truncated OR excluded.truncated"),
		}),
	}).CreateInBatches(rows, 200).Error
}

func (m *Monitor) pruneStabilityOlderThan(cutoffTs int64) error {
	if err := m.storeDB.Where("hour_ts < ?", cutoffTs).Delete(&StabilityHourSample{}).Error; err != nil {
		return fmt.Errorf("清理稳定性小时汇总: %w", err)
	}
	if err := m.storeDB.Where("hour_ts < ?", cutoffTs).Delete(&ChannelTestHourSample{}).Error; err != nil {
		return fmt.Errorf("清理渠道内部测试小时汇总: %w", err)
	}
	if err := m.storeDB.Where("hour_ts < ?", cutoffTs).Delete(&StabilityRejectHour{}).Error; err != nil {
		return fmt.Errorf("清理稳定性拒绝汇总: %w", err)
	}
	if err := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&StabilityProblemSample{}).Error; err != nil {
		return fmt.Errorf("清理稳定性问题签名: %w", err)
	}
	if err := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&StabilityProblemStage{}).Error; err != nil {
		return fmt.Errorf("清理稳定性问题暂存: %w", err)
	}
	if err := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&StabilityProblemIngestState{}).Error; err != nil {
		return fmt.Errorf("清理稳定性问题采集状态: %w", err)
	}
	if err := m.storeDB.Where("hour_ts < ?", cutoffTs).Delete(&StabilityHourIngestState{}).Error; err != nil {
		return fmt.Errorf("清理稳定性小时覆盖状态: %w", err)
	}
	// 补数任务只保留审计摘要，不随小时明细无限增长。正在执行/等待续跑的任务不能删。
	jobCutoff := cutoffTs - 30*86400
	if err := m.storeDB.Where("updated_at < ? AND status IN ?", jobCutoff, []string{"complete", "paused"}).Delete(&StabilityBackfillJob{}).Error; err != nil {
		return fmt.Errorf("清理稳定性补数任务: %w", err)
	}
	return nil
}
