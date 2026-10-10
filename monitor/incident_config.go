package monitor

// Incident 处理配置。
//
// 冷却期在这里是**去重参数**，不是「暂停检测」的时间：冷却期内引擎照常处理每个窗口，
// 照常更新证据、影响范围和严重程度，只是不新建事故行而是更新原行。
// 这与既有告警的 CooldownMin（那个确实是「抑制发送」）语义不同，
// 两者互不影响——本文件不读、不写、不改 AlertConfig。

import "os"

// 冷却期默认值（分钟）。
//
// 量级依据既有告警冷却（monitor/alert.go：错误类 30、观察类 60、余额类 720），
// 按 SEV 分级：越严重的事故需要越快反映新进展。
const (
	incidentCooldownSev0Min = 15
	incidentCooldownSev1Min = 30
	incidentCooldownSev2Min = 60
	incidentCooldownSev3Min = 180
)

// 冷却期夹取边界。越界回落默认值而不是报错——与既有配置做法一致。
const (
	incidentCooldownMinBound = 5
	incidentCooldownMaxBound = 1440
)

// 窗口与样本夹取边界。
const (
	incidentWindowMinSeconds = 60
	incidentWindowMaxSeconds = 3600
	incidentMinSampleLow     = 5
	incidentMinSampleHigh    = 1000
)

// 查询门控默认值。
//
// 容量 4 而不是复用容量为 1 的 usageDetailGate：事故页读的是本地 SQLite
// 持久化结果（非生产库），WAL 支持多读并发；取 4 既允许多人同时查看，
// 又为总体数据库负载留住上限。
const (
	incidentQueryGateCapacity  = 4
	incidentGateTimeoutSeconds = 5
	incidentQueryTimeoutMS     = 3000
)

// IncidentConfig 是 B 期事故处理的全部可调参数。
type IncidentConfig struct {
	Enabled bool `json:"enabled"`

	CooldownSev0Min int `json:"cooldown_sev0_min"`
	CooldownSev1Min int `json:"cooldown_sev1_min"`
	CooldownSev2Min int `json:"cooldown_sev2_min"`
	CooldownSev3Min int `json:"cooldown_sev3_min"`

	WindowSeconds int `json:"window_seconds"`
	MinSample     int `json:"min_sample"`

	QueryGateCapacity  int `json:"query_gate_capacity"`
	GateTimeoutSeconds int `json:"gate_timeout_seconds"`
	QueryTimeoutMS     int `json:"query_timeout_ms"`
}

// DefaultIncidentConfig 返回全部默认值，Enabled 为 false。
//
// 默认关闭（02.1 §10 可回滚要求）：关闭时不起 worker、不写新表、页面不显示子视图，
// 但**不删除任何历史事故数据**。验收时显式开启，生产启用另行确认。
func DefaultIncidentConfig() IncidentConfig {
	return IncidentConfig{
		Enabled:            false,
		CooldownSev0Min:    incidentCooldownSev0Min,
		CooldownSev1Min:    incidentCooldownSev1Min,
		CooldownSev2Min:    incidentCooldownSev2Min,
		CooldownSev3Min:    incidentCooldownSev3Min,
		WindowSeconds:      IncidentWindowSeconds,
		MinSample:          IncidentMinSample,
		QueryGateCapacity:  incidentQueryGateCapacity,
		GateTimeoutSeconds: incidentGateTimeoutSeconds,
		QueryTimeoutMS:     incidentQueryTimeoutMS,
	}
}

// LoadIncidentConfig 读环境变量覆盖默认值，并做夹取。
func LoadIncidentConfig() IncidentConfig {
	c := DefaultIncidentConfig()
	c.Enabled = os.Getenv("MONITOR_INCIDENT_CORE_ENABLED") == "true"

	c.CooldownSev0Min = clampIncidentCooldown(
		envInt("MONITOR_INCIDENT_COOLDOWN_SEV0_MIN", c.CooldownSev0Min), incidentCooldownSev0Min)
	c.CooldownSev1Min = clampIncidentCooldown(
		envInt("MONITOR_INCIDENT_COOLDOWN_SEV1_MIN", c.CooldownSev1Min), incidentCooldownSev1Min)
	c.CooldownSev2Min = clampIncidentCooldown(
		envInt("MONITOR_INCIDENT_COOLDOWN_SEV2_MIN", c.CooldownSev2Min), incidentCooldownSev2Min)
	c.CooldownSev3Min = clampIncidentCooldown(
		envInt("MONITOR_INCIDENT_COOLDOWN_SEV3_MIN", c.CooldownSev3Min), incidentCooldownSev3Min)

	c.WindowSeconds = clampIncidentWindowSeconds(
		envInt("MONITOR_INCIDENT_WINDOW_SECONDS", c.WindowSeconds))

	c.MinSample = clampIncidentMinSample(
		envInt("MONITOR_INCIDENT_MIN_SAMPLE", c.MinSample))

	return c
}

// clampIncidentCooldown 夹取冷却期；越界回落该 SEV 的默认值。
func clampIncidentCooldown(v, def int) int {
	if v < incidentCooldownMinBound || v > incidentCooldownMaxBound {
		return def
	}
	return v
}

// clampIncidentWindowSeconds 夹取窗口粒度。
//
// 除了上下界，还要求是 60 的整数倍：窗口必须对齐到既有分钟桶格栅，
// 否则窗口边界会切开分钟桶，样本统计与桶数据错位。
func clampIncidentWindowSeconds(v int) int {
	if v < incidentWindowMinSeconds || v > incidentWindowMaxSeconds {
		return IncidentWindowSeconds
	}
	if v%60 != 0 {
		return IncidentWindowSeconds
	}
	return v
}

// clampIncidentMinSample 夹取最小样本门。
func clampIncidentMinSample(v int) int {
	if v < incidentMinSampleLow || v > incidentMinSampleHigh {
		return IncidentMinSample
	}
	return v
}

// CooldownMinutesFor 返回该严重度的冷却分钟数。
//
// 未知严重度按最长（SEV3）处理：宁可少建单也不要噪声刷事故。
func (c IncidentConfig) CooldownMinutesFor(severity string) int {
	switch severity {
	case "SEV0":
		return c.CooldownSev0Min
	case "SEV1":
		return c.CooldownSev1Min
	case "SEV2":
		return c.CooldownSev2Min
	case "SEV3":
		return c.CooldownSev3Min
	}
	return c.CooldownSev3Min
}

// CooldownUntil 按严重度算出冷却截止时刻（Unix 秒）。
//
// 落库到 Incident.cooldown_until_ts，不放内存——重启后冷却关系得以延续。
func (c IncidentConfig) CooldownUntil(nowTs int64, severity string) int64 {
	return nowTs + int64(c.CooldownMinutesFor(severity))*60
}

// ShouldReopenAfterCooldown 判断冷却到期后是否应当新建事故。
//
// 用户明确要求：持续未结束的同一事故，不能仅因冷却到期就重复建单。
// 所以冷却到期本身**不是**新建条件——只有在事故已判定结束
// （resolved，即出现过判为正常的窗口）之后再次出现异常，才新建。
//
//	resolved=false 且冷却到期 → 仍然更新原事故（延长冷却，刷新 last_seen）
//	resolved=true  且再次异常 → 新建事故
func ShouldReopenAfterCooldown(resolved bool, nowTs, cooldownUntilTs int64) bool {
	if !resolved {
		return false
	}
	return nowTs >= cooldownUntilTs
}
