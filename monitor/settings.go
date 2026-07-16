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

	// 客户端「用量报表」独立监听(portal.go):客户域名只指这个端口,上面不存在任何管理端路由。
	// 留空 = 关闭(默认);如 ":8092"。
	PortalAddr string // MONITOR_PORTAL_ADDR

	// dead-man 心跳:每周期成功采样后向外部服务(如 healthchecks.io)打一次;留空=不启用。
	// 监控/采样若停了,外部服务收不到心跳即告警——"谁来监控监控"。
	HeartbeatURL string // MONITOR_HEARTBEAT_URL

	// 对外看板:站点名【兜底】。默认空——优先从主站 new-api 的 system_name 同步;
	// 仅当主站不可达时用此环境变量兜底;再为空则前端显通用名。
	SiteName string // MONITOR_SITE_NAME

	// 被拒请求采集:接收各节点 newapi-reject-collector 推送的鉴权 token。
	// 留空 = 关闭接收接口(POST /internal/rejections 返回 503),不接受任何推送。
	// 同一 token 也用于 POST /internal/host(各节点主机 agent 推送 OS 内存/磁盘)。
	IngestToken string // MONITOR_INGEST_TOKEN

	// 服务端健康监控(实例/数据库/负载均衡):基于 AWS Lightsail 指标接口拉取。
	// 默认【关】——关时完全不调 AWS、不影响模型监控与现网行为。
	InfraEnabled       bool   // MONITOR_INFRA_ENABLED(=true 才启用)
	AWSRegion          string // AWS_REGION,如 us-west-2;AWS 凭证用 SDK 默认链(AWS_ACCESS_KEY_ID/_SECRET)
	InfraSampleSeconds int    // MONITOR_INFRA_SAMPLE_SECONDS,默认 300(AWS 指标本就 5min 分辨率)
	InfraRetentionDays int    // MONITOR_INFRA_RETENTION_DAYS,默认 7
	// MONITOR_INFRA_RESOURCES:逗号分隔,显式指定要监控的资源,留空=自动发现。
	// 格式 type:name,type∈ instance/database/lb,如 "instance:Master,database:DB-X,lb:LB-X"。
	InfraResources string

	// 服务端监控告急阈值(百分比)。可用内存/存储「低于」即黄/红;CPU「高于」即黄/红;突发额度「低于」即黄。
	InfraMemAvailWarnPct     float64 // MONITOR_INFRA_MEM_AVAIL_WARN_PCT,默认 25
	InfraMemAvailBadPct      float64 // MONITOR_INFRA_MEM_AVAIL_BAD_PCT,默认 15
	InfraStorageAvailWarnPct float64 // MONITOR_INFRA_STORAGE_AVAIL_WARN_PCT,默认 25
	InfraStorageAvailBadPct  float64 // MONITOR_INFRA_STORAGE_AVAIL_BAD_PCT,默认 15
	InfraCPUWarnPct          float64 // MONITOR_INFRA_CPU_WARN_PCT,默认 70
	InfraCPUBadPct           float64 // MONITOR_INFRA_CPU_BAD_PCT,默认 85
	InfraBurstWarnPct        float64 // MONITOR_INFRA_BURST_WARN_PCT,默认 20
	InfraDBConnWarn          float64 // MONITOR_INFRA_DB_CONN_WARN,数据库连接数「高于」即黄,默认 70
	InfraDBDiskQueueWarn     float64 // MONITOR_INFRA_DB_DISK_QUEUE_WARN,数据库磁盘队列深度「高于」即黄,默认 5
	InfraLBRespWarnMs        float64 // MONITOR_INFRA_LB_RESP_WARN_MS,负载均衡响应毫秒「高于」即黄,默认 2000

	// 端到端可用性探活:周期性对每个前端域名做 HTTPS 探活 + 读 TLS 证书剩余天数。
	// 受 MONITOR_INFRA_ENABLED 同一开关控制(关时不探活);探活只读公网,对生产零写入。
	ProbeDomains       string  // MONITOR_PROBE_DOMAINS,逗号分隔域名;默认见 LoadSettings
	ProbePath          string  // MONITOR_PROBE_PATH,探活路径,默认 /api/status
	ProbeSeconds       int     // MONITOR_PROBE_SECONDS,探活间隔秒,默认 60
	ProbeLatencyWarnMs float64 // MONITOR_PROBE_LATENCY_WARN_MS,默认 500
	ProbeLatencyBadMs  float64 // MONITOR_PROBE_LATENCY_BAD_MS,默认 1500
	ProbeCertWarnDays  float64 // MONITOR_PROBE_CERT_WARN_DAYS,默认 30
	ProbeCertBadDays   float64 // MONITOR_PROBE_CERT_BAD_DAYS,默认 7
	ProbeExpectCDN     bool    // MONITOR_PROBE_EXPECT_CDN,默认 true;断言四入口确实经 CloudFront(Via 头)

	// 源站锁完整性监控(F-5 看门狗):周期性直连各源站 nginx,不带 X-Origin-Verify 头,
	// 期望被拦 403。一旦变 200 说明锁失效/被回滚(如重建容器漏了 env),立刻红告警。
	// 走私网,只有部署到实例上才测得到;本地预览连不到私网会显示「无数据」(不误报)。
	OriginLockTargets string // MONITOR_ORIGIN_LOCK_TARGETS,逗号分隔源站端点(host:port),默认两台 nginx 私网;留空=关闭
	OriginLockHost    string // MONITOR_ORIGIN_LOCK_HOST,检查时带的 Host 头,默认 nexusapi.link
	OriginLockPath    string // MONITOR_ORIGIN_LOCK_PATH,检查路径,默认 /(/ 无内网豁免,无头必 403;勿用 /api/status)
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
		PortalAddr:        env("MONITOR_PORTAL_ADDR", ""),
		HeartbeatURL:      env("MONITOR_HEARTBEAT_URL", ""),
		SiteName:          env("MONITOR_SITE_NAME", ""),
		IngestToken:       env("MONITOR_INGEST_TOKEN", ""),

		InfraEnabled:       env("MONITOR_INFRA_ENABLED", "") == "true",
		AWSRegion:          env("AWS_REGION", "us-west-2"),
		InfraSampleSeconds: envInt("MONITOR_INFRA_SAMPLE_SECONDS", 300),
		InfraRetentionDays: envInt("MONITOR_INFRA_RETENTION_DAYS", 7),
		InfraResources:     env("MONITOR_INFRA_RESOURCES", ""),

		InfraMemAvailWarnPct:     envFloat("MONITOR_INFRA_MEM_AVAIL_WARN_PCT", 25),
		InfraMemAvailBadPct:      envFloat("MONITOR_INFRA_MEM_AVAIL_BAD_PCT", 15),
		InfraStorageAvailWarnPct: envFloat("MONITOR_INFRA_STORAGE_AVAIL_WARN_PCT", 25),
		InfraStorageAvailBadPct:  envFloat("MONITOR_INFRA_STORAGE_AVAIL_BAD_PCT", 15),
		InfraCPUWarnPct:          envFloat("MONITOR_INFRA_CPU_WARN_PCT", 70),
		InfraCPUBadPct:           envFloat("MONITOR_INFRA_CPU_BAD_PCT", 85),
		InfraBurstWarnPct:        envFloat("MONITOR_INFRA_BURST_WARN_PCT", 20),
		InfraDBConnWarn:          envFloat("MONITOR_INFRA_DB_CONN_WARN", 70),
		InfraDBDiskQueueWarn:     envFloat("MONITOR_INFRA_DB_DISK_QUEUE_WARN", 5),
		InfraLBRespWarnMs:        envFloat("MONITOR_INFRA_LB_RESP_WARN_MS", 2000),

		ProbeDomains:       env("MONITOR_PROBE_DOMAINS", "nexusapi.link,routepath.link,pathgo.link,us.nexusapi.link"),
		ProbePath:          env("MONITOR_PROBE_PATH", "/api/status"),
		ProbeSeconds:       envInt("MONITOR_PROBE_SECONDS", 60),
		ProbeLatencyWarnMs: envFloat("MONITOR_PROBE_LATENCY_WARN_MS", 500),
		ProbeLatencyBadMs:  envFloat("MONITOR_PROBE_LATENCY_BAD_MS", 1500),
		ProbeCertWarnDays:  envFloat("MONITOR_PROBE_CERT_WARN_DAYS", 30),
		ProbeCertBadDays:   envFloat("MONITOR_PROBE_CERT_BAD_DAYS", 7),
		ProbeExpectCDN:     env("MONITOR_PROBE_EXPECT_CDN", "true") == "true",

		OriginLockTargets: env("MONITOR_ORIGIN_LOCK_TARGETS", "172.26.0.20:80,172.26.10.97:80"),
		OriginLockHost:    env("MONITOR_ORIGIN_LOCK_HOST", "nexusapi.link"),
		OriginLockPath:    env("MONITOR_ORIGIN_LOCK_PATH", "/"),
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

func envFloat(k string, def float64) float64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil {
			return n
		}
	}
	return def
}
