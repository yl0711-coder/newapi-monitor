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
// 它只由本地 metric_samples 滚动生成，不新增生产库查询；页面也只读这张
// Monitor 本地 SQLite 表。复合主键保证重复汇总是覆盖而不是累加。
type StabilityHourSample struct {
	HourTs        int64  `gorm:"primaryKey;autoIncrement:false;index:idx_stability_hour;index:idx_stability_group_hour,priority:2;index:idx_stability_channel_hour,priority:2;index:idx_stability_model_hour,priority:2"`
	ChannelID     int    `gorm:"primaryKey;autoIncrement:false;index:idx_stability_channel_hour,priority:1"`
	ModelName     string `gorm:"primaryKey;size:128;column:model_name;index:idx_stability_model_hour,priority:1"`
	Grp           string `gorm:"primaryKey;size:64;column:grp;index:idx_stability_group_hour,priority:1"`
	Success       int64
	Anomaly       int64
	Failed        int64
	AnomalyBilled int64 `gorm:"column:anomaly_billed"`
	AnomalyFree   int64 `gorm:"column:anomaly_free"`
	AnomalyStream int64 `gorm:"column:anomaly_stream"`
	AnomalyQuota  int64 `gorm:"column:anomaly_quota"`
	SumUseTime    int64 `gorm:"column:sum_use_time"`
	MaxUseTime    int   `gorm:"column:max_use_time"`
	Tokens        int64
	Quota         int64
	Err4xx        int64 `gorm:"column:err_4xx"`
	Err5xx        int64 `gorm:"column:err_5xx"`
	ErrTimeout    int64 `gorm:"column:err_timeout"`
	ErrOther      int64 `gorm:"column:err_other"`
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
	BucketTs      int64  `gorm:"primaryKey;autoIncrement:false;index:idx_stability_problem_bucket;index:idx_stability_problem_group_bucket,priority:2;index:idx_stability_problem_channel_bucket,priority:2;index:idx_stability_problem_model_bucket,priority:2"`
	Source        string `gorm:"primaryKey;size:24;index:idx_stability_problem_source_bucket,priority:1"`
	SignatureHash string `gorm:"primaryKey;size:64;column:signature_hash"`
	ChannelID     int    `gorm:"primaryKey;autoIncrement:false;index:idx_stability_problem_channel_bucket,priority:1"`
	ModelName     string `gorm:"primaryKey;size:128;column:model_name;index:idx_stability_problem_model_bucket,priority:1"`
	Grp           string `gorm:"primaryKey;size:64;column:grp;index:idx_stability_problem_group_bucket,priority:1"`
	Node          string `gorm:"primaryKey;size:64"`
	Path          string `gorm:"primaryKey;size:160"`
	Code          string `gorm:"size:32"`
	Message       string `gorm:"type:text"`
	Count         int64
	FirstTs       int64 `gorm:"column:first_ts"`
	LastTs        int64 `gorm:"column:last_ts"`
	Truncated     bool
}

// StabilityProblemIngestState 记录每个完整分钟的原始错误采集进度。
// 正常分钟一次完成；故障高峰超过单轮预算时保存游标，下轮继续，避免整窗丢弃。
type StabilityProblemIngestState struct {
	BucketTs      int64 `gorm:"primaryKey;autoIncrement:false"`
	LastCreatedAt int64 `gorm:"column:last_created_at"`
	LastID        int64 `gorm:"column:last_id"`
	RowsScanned   int64 `gorm:"column:rows_scanned"`
	Complete      bool  `gorm:"index"`
	UpdatedAt     int64 `gorm:"index"`
	CompletedAt   int64 `gorm:"column:completed_at"`
}

// StabilityProblemStage 是高峰分页期间的本地暂存聚合。只有对应分钟完整读完后，
// 才会原子替换到 StabilityProblemSample，页面不会把半截数据当成事实。
type StabilityProblemStage struct {
	BucketTs      int64  `gorm:"primaryKey;autoIncrement:false;index"`
	Source        string `gorm:"primaryKey;size:24"`
	SignatureHash string `gorm:"primaryKey;size:64;column:signature_hash"`
	ChannelID     int    `gorm:"primaryKey;autoIncrement:false"`
	ModelName     string `gorm:"primaryKey;size:128;column:model_name"`
	Grp           string `gorm:"primaryKey;size:64;column:grp"`
	Node          string `gorm:"primaryKey;size:64"`
	Path          string `gorm:"primaryKey;size:160"`
	Code          string `gorm:"size:32"`
	Message       string `gorm:"type:text"`
	Count         int64
	FirstTs       int64 `gorm:"column:first_ts"`
	LastTs        int64 `gorm:"column:last_ts"`
	Truncated     bool
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
		sum_use_time, max_use_time, tokens, quota, err_4xx, err_5xx, err_timeout, err_other)
		SELECT (bucket_ts/3600)*3600 AS hour_ts, channel_id, model_name, grp,
		  SUM(success), SUM(anomaly), SUM(failed),
		  SUM(anomaly_billed), SUM(anomaly_free), SUM(anomaly_stream), SUM(anomaly_quota),
		  SUM(sum_use_time), MAX(max_use_time), SUM(tokens), SUM(quota),
		  SUM(err_4xx), SUM(err_5xx), SUM(err_timeout), SUM(err_other)
		FROM metric_samples WHERE bucket_ts >= ?
		GROUP BY hour_ts, channel_id, model_name, grp
		ON CONFLICT(hour_ts, channel_id, model_name, grp) DO UPDATE SET
		  success=excluded.success, anomaly=excluded.anomaly, failed=excluded.failed,
		  anomaly_billed=excluded.anomaly_billed, anomaly_free=excluded.anomaly_free,
		  anomaly_stream=excluded.anomaly_stream, anomaly_quota=excluded.anomaly_quota,
		  sum_use_time=excluded.sum_use_time, max_use_time=excluded.max_use_time,
		  tokens=excluded.tokens, quota=excluded.quota,
		  err_4xx=excluded.err_4xx, err_5xx=excluded.err_5xx,
		  err_timeout=excluded.err_timeout, err_other=excluded.err_other`, sinceTs).Error
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
	return nil
}
