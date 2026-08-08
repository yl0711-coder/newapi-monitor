package monitor

import (
	"os"
	"strconv"
	"strings"
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
	// 历史稳定性报表只使用 Monitor 本地汇总。开关关闭时不采集原始错误、
	// 不执行稳定性长期汇总，但不影响原有模型/用量/服务端监控。
	StabilityEnabled          bool // MONITOR_STABILITY_ENABLED,默认 true
	StabilityQueryMaxDays     int  // MONITOR_STABILITY_QUERY_MAX_DAYS,默认 90,页面最大查询范围
	StabilityRetentionDays    int  // MONITOR_STABILITY_RETENTION_DAYS,默认 181,至少覆盖两个最大查询周期
	StabilityProblemSampleSec int  // MONITOR_STABILITY_PROBLEM_SAMPLE_SECONDS,默认 300
	// 长期小时数据补数直接聚合生产 logs 的单个小时，不写分钟表。查询始终串行，
	// 片间延迟用于给主站数据库让路；自动修洞每轮最多处理一个已结束小时。
	StabilityBackfillDelayMS    int  // MONITOR_STABILITY_BACKFILL_DELAY_MS,默认 2000
	StabilityBackfillTimeoutSec int  // MONITOR_STABILITY_BACKFILL_TIMEOUT_SECONDS,默认 20
	StabilityBackfillEnabled    bool // MONITOR_STABILITY_BACKFILL_ENABLED,默认 true;关闭后禁止人工、自动及重启续跑
	StabilityAutoRepair         bool // MONITOR_STABILITY_AUTO_REPAIR,默认 true

	// Nginx 入口层旁路聚合。默认关闭；开启后只接收采集器已经脱敏、按分钟聚合的
	// 客观指标，不读取 access/error 原文，不采集 IP、Header、Key、请求体或响应体。
	NginxEnabled       bool     // MONITOR_NGINX_ENABLED,默认 false
	NginxRetentionDays int      // MONITOR_NGINX_RETENTION_DAYS,默认 7
	NginxAllowedNodes  []string // MONITOR_NGINX_ALLOWED_NODES,逗号分隔；启用 Nginx 采集时必填

	// 登录鉴权:复用 new-api 用户身份(不改 new-api,只调其 API 验证)
	NewAPIBaseURL string // MONITOR_NEWAPI_BASE_URL,如 http://new-api:3000
	SessionSecret string // MONITOR_SESSION_SECRET,签发监控自己的会话;留空则启动时随机生成(重启需重新登录)
	// 上游账户凭据只保存在 Monitor 本地 SQLite，使用该密钥经 AES-256-GCM 加密。
	// 留空时复用 MONITOR_SESSION_SECRET；生产必须至少固定配置二者之一，否则拒绝保存凭据。
	UpstreamCredentialSecret string // MONITOR_UPSTREAM_CREDENTIAL_SECRET
	UpstreamSyncMinutes      int    // MONITOR_UPSTREAM_SYNC_MINUTES,默认 5；失败时按账户退避，最长 60 分钟
	UpstreamSyncTimeoutSec   int    // MONITOR_UPSTREAM_SYNC_TIMEOUT_SECONDS,默认 15

	// 客户端「用量报表」独立监听(portal.go):客户域名只指这个端口,上面不存在任何管理端路由。
	// 留空 = 关闭(默认);如 ":8092"。
	PortalAddr string // MONITOR_PORTAL_ADDR

	// 客户端用量聚合结果的可选 Redis 缓存。留空地址时完全不连接 Redis，
	// 自动退化为有严格容量上限的进程内短缓存；Redis 不参与鉴权，也不保存原始日志。
	UsageRedisAddr     string // MONITOR_USAGE_REDIS_ADDR，如 172.26.4.11:6379
	UsageRedisUsername string // MONITOR_USAGE_REDIS_USERNAME，生产建议使用仅限 nxmon:* 的 ACL 用户
	UsageRedisPassword string // MONITOR_USAGE_REDIS_PASSWORD，只从环境变量读取
	UsageRedisDB       int    // MONITOR_USAGE_REDIS_DB，默认 0；权限隔离仍以 ACL/key prefix 为准
	UsageRedisPrefix   string // MONITOR_USAGE_REDIS_PREFIX，默认 nxmon:usage:v1
	// 仅信任这些反代来源提供的 X-Forwarded-For/X-Real-IP。留空时不信任任何转发头，
	// 登录限流按直连地址计算，避免外部请求伪造来源 IP 绕过限流。
	TrustedProxies []string // MONITOR_TRUSTED_PROXIES，逗号分隔 CIDR/IP

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
	// MONITOR_INFRA_EXCLUDE_RESOURCES:逗号分隔的资源名。用于暂时下线某台实例的
	// 监控：不再自动采样，也不在现有历史采样的快照、趋势或最近告警中展示。
	InfraExcludeResources []string

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

	// AlertsDisabled 本地/测试用的硬开关:=true 时 evaluateAlerts 直接返回,一封都不发。
	// 存在的理由是真实事故:本地测试实例连生产库、库里带真实 SMTP 与真实收件人,
	// 隧道抖动被误判成"采样器掉线",用真实凭据连发了 9 封骚扰邮件。
	// 不要用"默认配置里 Enabled=false"来代替这个开关——那是约定,这是断路器。
	AlertsDisabled bool // MONITOR_ALERTS_DISABLED
}

// LoadSettings 从环境变量装载配置(可配合 .env)。
func LoadSettings() Settings {
	return Settings{
		Addr:                        env("MONITOR_ADDR", ":8090"),
		ProdDSN:                     env("NEWAPI_LOG_DSN", ""),
		StorePath:                   env("MONITOR_STORE_PATH", "monitor.db"),
		SampleSeconds:               envInt("MONITOR_SAMPLE_SECONDS", 60),
		RetentionDays:               envInt("MONITOR_RETENTION_DAYS", 7),
		HourRetentionDays:           envInt("MONITOR_HOUR_RETENTION_DAYS", 90),
		BackfillHours:               envInt("MONITOR_BACKFILL_HOURS", 24),
		StabilityEnabled:            env("MONITOR_STABILITY_ENABLED", "true") == "true",
		StabilityQueryMaxDays:       envInt("MONITOR_STABILITY_QUERY_MAX_DAYS", 90),
		StabilityRetentionDays:      envInt("MONITOR_STABILITY_RETENTION_DAYS", 181),
		StabilityProblemSampleSec:   envInt("MONITOR_STABILITY_PROBLEM_SAMPLE_SECONDS", 300),
		StabilityBackfillDelayMS:    envInt("MONITOR_STABILITY_BACKFILL_DELAY_MS", 2000),
		StabilityBackfillTimeoutSec: envInt("MONITOR_STABILITY_BACKFILL_TIMEOUT_SECONDS", 20),
		StabilityBackfillEnabled:    env("MONITOR_STABILITY_BACKFILL_ENABLED", "true") == "true",
		StabilityAutoRepair:         env("MONITOR_STABILITY_AUTO_REPAIR", "true") == "true",
		NginxEnabled:                env("MONITOR_NGINX_ENABLED", "false") == "true",
		NginxRetentionDays:          envInt("MONITOR_NGINX_RETENTION_DAYS", 7),
		NginxAllowedNodes:           envCSV("MONITOR_NGINX_ALLOWED_NODES"),
		NewAPIBaseURL:               env("MONITOR_NEWAPI_BASE_URL", ""),
		SessionSecret:               env("MONITOR_SESSION_SECRET", ""),
		UpstreamCredentialSecret:    env("MONITOR_UPSTREAM_CREDENTIAL_SECRET", ""),
		UpstreamSyncMinutes:         envInt("MONITOR_UPSTREAM_SYNC_MINUTES", 5),
		UpstreamSyncTimeoutSec:      envInt("MONITOR_UPSTREAM_SYNC_TIMEOUT_SECONDS", 15),
		PortalAddr:                  env("MONITOR_PORTAL_ADDR", ""),
		UsageRedisAddr:              strings.TrimSpace(env("MONITOR_USAGE_REDIS_ADDR", "")),
		UsageRedisUsername:          strings.TrimSpace(env("MONITOR_USAGE_REDIS_USERNAME", "")),
		UsageRedisPassword:          env("MONITOR_USAGE_REDIS_PASSWORD", ""),
		UsageRedisDB:                envInt("MONITOR_USAGE_REDIS_DB", 0),
		UsageRedisPrefix:            strings.Trim(strings.TrimSpace(env("MONITOR_USAGE_REDIS_PREFIX", "nxmon:usage:v1")), ":"),
		TrustedProxies:              envCSV("MONITOR_TRUSTED_PROXIES"),
		HeartbeatURL:                env("MONITOR_HEARTBEAT_URL", ""),
		SiteName:                    env("MONITOR_SITE_NAME", ""),
		IngestToken:                 env("MONITOR_INGEST_TOKEN", ""),

		InfraEnabled:          env("MONITOR_INFRA_ENABLED", "") == "true",
		AWSRegion:             env("AWS_REGION", "us-west-2"),
		InfraSampleSeconds:    envInt("MONITOR_INFRA_SAMPLE_SECONDS", 300),
		InfraRetentionDays:    envInt("MONITOR_INFRA_RETENTION_DAYS", 7),
		InfraResources:        env("MONITOR_INFRA_RESOURCES", ""),
		InfraExcludeResources: envCSV("MONITOR_INFRA_EXCLUDE_RESOURCES"),

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

		AlertsDisabled: env("MONITOR_ALERTS_DISABLED", "") == "true",
	}
}

// stabilityQueryDays 与 stabilityStorageDays 把“页面可查多久”和“本地至少要存多久”
// 分开。90 天报表还要读取紧邻的上一 90 天做环比，因此留存少于 181 天会让
// 页面看似支持 90 天、实际却永远缺上一周期。即使旧部署仍配置 90，也在运行时
// 安全提升到所需下限；只增加 Monitor 本地小时汇总留存，不扩大页面查询范围。
func (s Settings) stabilityQueryDays() int {
	days := s.StabilityQueryMaxDays
	if days <= 0 || days > 365 {
		return 90
	}
	return days
}

func (s Settings) stabilityStorageDays() int {
	days := s.StabilityRetentionDays
	minimum := s.stabilityQueryDays()*2 + 1
	if days < minimum {
		return minimum
	}
	return days
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func envCSV(k string) []string {
	v := strings.TrimSpace(os.Getenv(k))
	if v == "" {
		return nil
	}
	parts := strings.Split(v, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
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
