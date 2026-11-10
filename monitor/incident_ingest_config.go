package monitor

// Incident 采集侧配置（MON-003 B3）。
//
// 与已验收的 IncidentConfig 分开：那一份是引擎判定参数，这一份是
// 「本地事实 → 引擎」适配层的采集参数。引擎配置不因接采集而改动。
//
// ★ 核心开关是总开关 ★
//
// 仅当 MONITOR_INCIDENT_CORE_ENABLED && MONITOR_INCIDENT_INGEST_ENABLED
// 同时为 true 时，采集循环才启动。核心关闭时：不扫描、不写任何游标与
// 窗口状态行，也不把引擎返回的 skipped_disabled 当成「成功消费」而推进水位。

import "os"

const (
	// incidentIngestDefaultPollSeconds 主扫描间隔。
	incidentIngestDefaultPollSeconds = 60
	incidentIngestMinPollSeconds     = 10
	incidentIngestMaxPollSeconds     = 3600

	// incidentIngestDefaultAnomalyRate 是**待校准的 shadow 初值**。
	//
	// 口径：(Anomaly + Failed) / (Success + Anomaly + Failed)，即
	// 「已有日志口径的异常比例」，**不代表客户端最终失败率**。
	// 按 02.1 需至少观察 7 天后校准，期间不得据此对外宣称失败率。
	incidentIngestDefaultAnomalyRate = 0.20
	incidentIngestMinAnomalyRate     = 0.01
	incidentIngestMaxAnomalyRate     = 1.0

	// incidentIngestDefaultFinalizeLag 处理延迟（秒）。
	//
	// 必须 > customerHealthSourceReplaySeconds(20 分钟)：客户维护 lane 会
	// 持续回放最近 20 分钟，覆盖水位越过**不**等于该窗数据不再变化。
	// 这只是本期 shadow 处理策略，**不是最终性保证**——日志实际迟到时长
	// 无法由本地事实测量，见 incident_ingest_loop.go 的复核机制。
	// 下界必须**严格大于** customerHealthSourceReplaySeconds(1200)：
	// 恰好等于时，窗口右界刚好落在回放窗边缘，该窗仍可能被回放改写。
	incidentIngestDefaultFinalizeLag = 1500
	incidentIngestMinFinalizeLag     = 1320
	incidentIngestMaxFinalizeLag     = 7200

	// 迟到修订复核队列：只处理已完成窗口的定稿后修订。有界。
	incidentIngestDefaultRecheckWindow   = 7200
	incidentIngestMinRecheckWindow       = 1800
	incidentIngestMaxRecheckWindow       = 86400
	incidentIngestDefaultRecheckMaxWin   = 48
	incidentIngestDefaultRecheckEvery    = 5
	incidentIngestDefaultRecheckAttempts = 6

	// 来源推进队列：处理未终结行与未关闭复核的缺口窗口。每轮限量，但不限次数。
	incidentIngestDefaultPendingBatch = 200
	incidentIngestMinPendingBatch     = 10
	incidentIngestMaxPendingBatch     = 5000

	// 积压保护阈值。只统计 pending_backlog（仍可被推进的行），
	// 终结的历史失败不参与，否则永久失败会把新数据处理永久卡住。
	incidentIngestDefaultBacklogWarn = 5000
	incidentIngestDefaultBacklogMax  = 20000

	// 单窗口有界重试次数，超过标 failed_permanent。
	incidentIngestDefaultMaxAttempts = 5
)

// IncidentIngestConfig 是采集侧参数。
type IncidentIngestConfig struct {
	Enabled bool `json:"enabled"`

	PollSeconds        int     `json:"poll_seconds"`
	AnomalyRate        float64 `json:"anomaly_rate_threshold"`
	FinalizeLagSeconds int     `json:"finalize_lag_seconds"`

	RecheckWindowSeconds int `json:"recheck_window_seconds"`
	RecheckMaxWindows    int `json:"recheck_max_windows"`
	RecheckEveryTicks    int `json:"recheck_every_ticks"`
	RecheckMaxAttempts   int `json:"recheck_max_attempts"`

	PendingBatch int `json:"pending_batch"`

	BacklogWarnRows int `json:"backlog_warn_rows"`
	BacklogMaxRows  int `json:"backlog_max_rows"`

	MaxAttempts int `json:"max_attempts"`
}

// DefaultIncidentIngestConfig 返回全部默认值，Enabled 为 false。
func DefaultIncidentIngestConfig() IncidentIngestConfig {
	return IncidentIngestConfig{
		Enabled:              false,
		PollSeconds:          incidentIngestDefaultPollSeconds,
		AnomalyRate:          incidentIngestDefaultAnomalyRate,
		FinalizeLagSeconds:   incidentIngestDefaultFinalizeLag,
		RecheckWindowSeconds: incidentIngestDefaultRecheckWindow,
		RecheckMaxWindows:    incidentIngestDefaultRecheckMaxWin,
		RecheckEveryTicks:    incidentIngestDefaultRecheckEvery,
		RecheckMaxAttempts:   incidentIngestDefaultRecheckAttempts,
		PendingBatch:         incidentIngestDefaultPendingBatch,
		BacklogWarnRows:      incidentIngestDefaultBacklogWarn,
		BacklogMaxRows:       incidentIngestDefaultBacklogMax,
		MaxAttempts:          incidentIngestDefaultMaxAttempts,
	}
}

// LoadIncidentIngestConfig 读环境变量覆盖默认值并夹取。
func LoadIncidentIngestConfig() IncidentIngestConfig {
	c := DefaultIncidentIngestConfig()
	c.Enabled = os.Getenv("MONITOR_INCIDENT_INGEST_ENABLED") == "true"

	c.PollSeconds = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_INGEST_POLL_SECONDS", c.PollSeconds),
		incidentIngestMinPollSeconds, incidentIngestMaxPollSeconds, c.PollSeconds)

	c.AnomalyRate = clampIncidentIngestRate(
		envFloat("MONITOR_INCIDENT_ANOMALY_RATE_THRESHOLD", c.AnomalyRate))

	c.FinalizeLagSeconds = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_FINALIZE_LAG_SECONDS", c.FinalizeLagSeconds),
		incidentIngestMinFinalizeLag, incidentIngestMaxFinalizeLag, c.FinalizeLagSeconds)

	c.RecheckWindowSeconds = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_RECHECK_WINDOW_SECONDS", c.RecheckWindowSeconds),
		incidentIngestMinRecheckWindow, incidentIngestMaxRecheckWindow, c.RecheckWindowSeconds)

	c.RecheckMaxWindows = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_RECHECK_MAX_WINDOWS", c.RecheckMaxWindows),
		1, 1000, c.RecheckMaxWindows)

	c.RecheckEveryTicks = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_RECHECK_EVERY_TICKS", c.RecheckEveryTicks),
		1, 1000, c.RecheckEveryTicks)

	c.RecheckMaxAttempts = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_RECHECK_MAX_ATTEMPTS", c.RecheckMaxAttempts),
		1, 100, c.RecheckMaxAttempts)

	c.PendingBatch = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_PENDING_BATCH", c.PendingBatch),
		incidentIngestMinPendingBatch, incidentIngestMaxPendingBatch, c.PendingBatch)

	c.BacklogWarnRows = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_BACKLOG_WARN_ROWS", c.BacklogWarnRows),
		1, 10_000_000, c.BacklogWarnRows)

	c.BacklogMaxRows = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_BACKLOG_MAX_ROWS", c.BacklogMaxRows),
		1, 10_000_000, c.BacklogMaxRows)
	// 上限不得低于告警线，否则一进 degraded 就永远回不到 normal。
	if c.BacklogMaxRows < c.BacklogWarnRows {
		c.BacklogMaxRows = c.BacklogWarnRows
	}

	c.MaxAttempts = clampIncidentIngestInt(
		envInt("MONITOR_INCIDENT_INGEST_MAX_ATTEMPTS", c.MaxAttempts),
		1, 100, c.MaxAttempts)

	return c
}

func clampIncidentIngestInt(v, lo, hi, def int) int {
	if v < lo || v > hi {
		return def
	}
	return v
}

func clampIncidentIngestRate(v float64) float64 {
	if v < incidentIngestMinAnomalyRate || v > incidentIngestMaxAnomalyRate {
		return incidentIngestDefaultAnomalyRate
	}
	return v
}

// incidentIngestActive 是总开关判定。
//
// 核心关闭 → 整条采集链不启动。刻意不提供「核心关闭但采集开启」的组合：
// 那会让采集一直扫描并把引擎的 skipped_disabled 当成功消费，水位推过去后
// 这些窗口再也不会被处理。
func incidentIngestActive() (IncidentConfig, IncidentIngestConfig, bool) {
	core := LoadIncidentConfig()
	ingest := LoadIncidentIngestConfig()
	return core, ingest, core.Enabled && ingest.Enabled
}
