package monitor

import (
	"os"
	"strconv"
)

// Settings 是监控服务的独立配置,全部从环境变量读取——不依赖任何外部 config 包。
type Settings struct {
	Addr              string // 监听地址,默认 :8090
	ProdDSN           string // NEWAPI_LOG_DSN:new-api 生产库【只读】DSN
	StorePath         string // 本地采样库(sqlite)路径,默认 monitor.db
	SampleSeconds     int    // 采样间隔秒,默认 60
	RetentionDays     int    // 分钟级本地留存天数,默认 7
	HourRetentionDays int    // 小时级汇总(rollup)留存天数,默认 90;支撑长期趋势 + 同比环比
	BackfillHours     int    // 启动回填小时数,默认 24

	// 登录鉴权:复用 new-api 用户身份(不改 new-api,只调其 API 验证)
	NewAPIBaseURL string // MONITOR_NEWAPI_BASE_URL,如 http://new-api:3000
	SessionSecret string // MONITOR_SESSION_SECRET,签发监控自己的会话;留空则启动时随机生成(重启需重新登录)

	// dead-man 心跳:每周期成功采样后向外部服务(如 healthchecks.io)打一次;留空=不启用。
	// 监控/采样若停了,外部服务收不到心跳即告警——"谁来监控监控"。
	HeartbeatURL string // MONITOR_HEARTBEAT_URL

	// 对外看板:站点名【兜底】。默认空——优先从主站 new-api 的 system_name 同步;
	// 仅当主站不可达时用此环境变量兜底;再为空则前端显通用名。
	SiteName string // MONITOR_SITE_NAME
}

// LoadSettings 从环境变量装载配置(可配合 .env)。
func LoadSettings() Settings {
	return Settings{
		Addr:              env("MONITOR_ADDR", ":8090"),
		ProdDSN:           env("NEWAPI_LOG_DSN", ""),
		StorePath:         env("MONITOR_STORE_PATH", "monitor.db"),
		SampleSeconds:     envInt("MONITOR_SAMPLE_SECONDS", 60),
		RetentionDays:     envInt("MONITOR_RETENTION_DAYS", 7),
		HourRetentionDays: envInt("MONITOR_HOUR_RETENTION_DAYS", 90),
		BackfillHours:     envInt("MONITOR_BACKFILL_HOURS", 24),
		NewAPIBaseURL:     env("MONITOR_NEWAPI_BASE_URL", ""),
		SessionSecret:     env("MONITOR_SESSION_SECRET", ""),
		HeartbeatURL:      env("MONITOR_HEARTBEAT_URL", ""),
		SiteName:          env("MONITOR_SITE_NAME", ""),
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envInt(k string, def int) int {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}
