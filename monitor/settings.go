package monitor

import (
	"context"
	"database/sql"
	"os"
	"strconv"
	"strings"
	"time"
)

// Settings 是监控服务的独立配置,全部从环境变量读取——不依赖任何外部 config 包。
type Settings struct {
	Addr      string // 监听地址,默认 :8090
	ProdDSN   string // NEWAPI_LOG_DSN:new-api 生产库【只读】DSN
	StorePath string // 本地采样库(sqlite)路径,默认 monitor.db
	// Monitor 本地 SQLite 是用量事实和运维历史的事实源。启动时始终
	// 先做 quick_check；在线备份使用 SQLite 一致性快照，不直接拷贝 WAL 运行库。
	StoreBackupEnabled       bool   // MONITOR_STORE_BACKUP_ENABLED，默认 true
	StoreBackupDir           string // MONITOR_STORE_BACKUP_DIR，默认 <StorePath 目录>/backups
	StoreBackupIntervalHours int    // MONITOR_STORE_BACKUP_INTERVAL_HOURS，默认 24
	StoreBackupRetention     int    // MONITOR_STORE_BACKUP_RETENTION，默认 7
	// 迁移前成套快照不受 StoreBackupEnabled 影响，独立限量保留，避免与日备份
	// 叠加后无提示占满数据卷。
	StoreMigrationBackupRetention int // MONITOR_STORE_MIGRATION_BACKUP_RETENTION，默认 3
	// LocalSnapshotOnly 只供本机验收使用：不建立任何 NewAPI 生产库连接，也不启动
	// 采样、回填、上游轮询等后台任务。页面只能读取已复制到本地 SQLite 的快照。
	// 生产环境必须保持 false；它不是“连接失败后的降级模式”。
	LocalSnapshotOnly bool // MONITOR_LOCAL_SNAPSHOT_ONLY，默认 false
	// LocalAuthBypass 只供完全离线的本机快照容器免登录验收。启动前会同时
	// 校验本地快照、无生产 DSN、无主站地址、无上游/AWS/心跳与告警任务；
	// 任意一项不满足都拒绝启动，生产默认永远关闭。
	LocalAuthBypass bool // MONITOR_LOCAL_AUTH_BYPASS，默认 false
	// 来源 worker 与 Web 进程分离：MySQL 短暂不可达时仍可从
	// SQLite 服务已发布数据。生产默认开启 worker，并通过 MySQL
	// advisory lock 保证同一来源只有一个实例采集。
	SourceWorkerEnabled bool   // MONITOR_SOURCE_WORKER_ENABLED，默认 true
	SourceLeaseRequired bool   // MONITOR_SOURCE_LEASE_REQUIRED，默认 true
	SourceLeaseName     string // MONITOR_SOURCE_LEASE_NAME，默认 newapi-monitor-source-worker-v1
	SampleSeconds       int    // 采样间隔秒,默认 60
	RetentionDays       int    // 分钟级本地留存天数,默认 7
	HourRetentionDays   int    // 小时级汇总(rollup)留存天数,默认 90;支撑长期趋势 + 同比环比
	BackfillHours       int    // 来源 epoch 启动缺口上限,默认且自动硬限 1 小时；更久缺口走维护回填
	// 历史稳定性报表只使用 Monitor 本地汇总。开关关闭时不采集原始错误、
	// 不执行稳定性长期汇总，但不影响原有模型/用量/服务端监控。
	StabilityEnabled          bool // MONITOR_STABILITY_ENABLED,默认 true
	StabilityQueryMaxDays     int  // MONITOR_STABILITY_QUERY_MAX_DAYS,默认 90,页面最大查询范围
	StabilityRetentionDays    int  // MONITOR_STABILITY_RETENTION_DAYS,默认 181,至少覆盖两个最大查询周期
	StabilityProblemSampleSec int  // MONITOR_STABILITY_PROBLEM_SAMPLE_SECONDS,默认 300
	// 长期小时数据补数直接聚合生产 logs 的单个小时，不写分钟表。查询始终串行，
	// 片间延迟和来源占用率共同给主站数据库让路。分类规则升级产生的大范围
	// 缺口只能通过显式 migration 开关自动创建持久任务，不能随普通修洞静默启动。
	BackgroundSourceMinStartIntervalMS      int  // MONITOR_BACKGROUND_SOURCE_MIN_START_INTERVAL_MS，默认 2000；所有后台来源查询共用
	StabilityBackfillDelayMS                int  // MONITOR_STABILITY_BACKFILL_DELAY_MS,默认 2000
	StabilityBackfillTimeoutSec             int  // MONITOR_STABILITY_BACKFILL_TIMEOUT_SECONDS,默认 20
	StabilityBackfillServerMaxExecutionMS   int  // MONITOR_STABILITY_BACKFILL_SERVER_MAX_EXECUTION_MS，默认且最大 8000
	StabilityBackfillSourceDutyPercent      int  // MONITOR_STABILITY_BACKFILL_SOURCE_DUTY_PERCENT，默认 20
	StabilityBackfillEnabled                bool // MONITOR_STABILITY_BACKFILL_ENABLED,默认 true;关闭后禁止人工、自动及重启续跑
	StabilityAutoRepair                     bool // MONITOR_STABILITY_AUTO_REPAIR,默认 true
	StabilityClassificationMigrationEnabled bool // MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED，默认 false；显式允许自动创建分类迁移任务

	// Nginx 入口层旁路聚合。默认关闭；开启后只接收采集器已经脱敏、按分钟聚合的
	// 客观指标，不读取 access/error 原文，不采集 IP、Header、Key、请求体或响应体。
	NginxEnabled       bool     // MONITOR_NGINX_ENABLED,默认 false
	NginxRetentionDays int      // MONITOR_NGINX_RETENTION_DAYS,默认 7
	NginxAllowedNodes  []string // MONITOR_NGINX_ALLOWED_NODES,逗号分隔；启用 Nginx 采集时必填
	NginxErrorEnabled  bool     // MONITOR_NGINX_ERROR_ENABLED，标准 error.log 的节点侧分类分钟聚合
	// Nginx 请求证据是独立灰度 lane。off 完全不打开证据库；pilot 只采集并
	// 验证覆盖率；verified 才允许客户排障按 Request ID 精确关联。
	NginxEvidenceMode              string // MONITOR_NGINX_EVIDENCE_MODE=off|pilot|verified，默认 off
	NginxEvidenceStorePath         string // MONITOR_NGINX_EVIDENCE_STORE_PATH，启用时必须显式配置独立卷路径
	NginxEvidenceRetentionHours    int    // MONITOR_NGINX_EVIDENCE_RETENTION_HOURS，默认 168
	NginxEvidenceHMACKey           string // MONITOR_NGINX_EVIDENCE_HMAC_KEY，独立于会话/上游凭据
	NginxEvidenceHMACKeyID         string // MONITOR_NGINX_EVIDENCE_HMAC_KEY_ID，轮换时用于区分历史证据
	NginxEvidenceMaxMiB            int    // MONITOR_NGINX_EVIDENCE_MAX_MIB，证据库硬上限
	NginxEvidencePreviousHMACKey   string // 上一把轮换密钥，仅过渡期配置
	NginxEvidencePreviousHMACKeyID string

	// 登录鉴权:复用 new-api 用户身份(不改 new-api,只调其 API 验证)
	NewAPIBaseURL string // MONITOR_NEWAPI_BASE_URL,如 http://new-api:3000
	SessionSecret string // MONITOR_SESSION_SECRET,签发监控自己的会话;留空则启动时随机生成(重启需重新登录)
	// 上游账户凭据只保存在 Monitor 本地 SQLite，使用该密钥经 AES-256-GCM 加密。
	// 留空时复用 MONITOR_SESSION_SECRET；生产必须至少固定配置二者之一，否则拒绝保存凭据。
	UpstreamCredentialSecret string // MONITOR_UPSTREAM_CREDENTIAL_SECRET
	// 关闭后禁止后台主动轮询已配置的上游账户余额。默认开启，保证已有生产行为不变；
	// 本地验收可显式关闭，避免启动页面时向任何上游站点发出请求。
	UpstreamSyncEnabled    bool // MONITOR_UPSTREAM_SYNC_ENABLED,默认 true
	UpstreamSyncMinutes    int  // MONITOR_UPSTREAM_SYNC_MINUTES,默认 5；失败时按账户退避，最长 60 分钟
	UpstreamSyncTimeoutSec int  // MONITOR_UPSTREAM_SYNC_TIMEOUT_SECONDS,默认 15
	// 上游使用日志与余额是两条独立同步链。日志全局开关默认关闭，
	// 只有全局开关与账户开关同时开启才会后台读取。页面访问绝不会触发上游请求。
	UpstreamUsageSyncEnabled  bool // MONITOR_UPSTREAM_USAGE_SYNC_ENABLED,默认 false；新功能灰度闸门
	UpstreamUsageSyncMinutes  int  // MONITOR_UPSTREAM_USAGE_SYNC_MINUTES,默认 20，最小 15
	UpstreamUsageBackfillDays int  // MONITOR_UPSTREAM_USAGE_BACKFILL_DAYS,默认 90，首次低频补齐范围
	// 默认 1 保持所有上游请求全局串行；生产观察达标后最多升到 2。
	// 同一 host 仍由 upstreamHostGuard 强制单并发，不能被此开关绕过。
	UpstreamMaxConcurrency int // MONITOR_UPSTREAM_MAX_CONCURRENCY,默认 1，范围 1～2
	// 上游计价账本与既有消费汇总使用独立灰度闸门和域名白名单。
	// 支持 NewAPI、Sub2API 和 AICodeWith；默认关闭且空白名单，迁移不会发起任何上游请求。
	UpstreamPricingLedgerEnabled       bool     // MONITOR_UPSTREAM_PRICING_LEDGER_ENABLED，默认 false
	UpstreamPricingLedgerDomains       []string // MONITOR_UPSTREAM_PRICING_LEDGER_DOMAINS，逗号分隔
	UpstreamPricingBackfillHoursPerRun int      // MONITOR_UPSTREAM_PRICING_BACKFILL_HOURS_PER_RUN，默认 1，最大 6
	// 渠道成本闭环是计价账本之上的独立影子层。第一阶段只消费已经拉取并核验的
	// NewAPI 小时日志，不增加上游请求、不修改人工倍率，也不改变现有页面/告警口径。
	ChannelCostClosureEnabled bool     // MONITOR_CHANNEL_COST_CLOSURE_ENABLED，默认 false
	ChannelCostClosureDomains []string // MONITOR_CHANNEL_COST_CLOSURE_DOMAINS，必须是计价账本白名单的子集
	ChannelCostHMACKey        string   // MONITOR_CHANNEL_COST_HMAC_KEY，独立固定密钥；仅用于匿名化上游来源 ID
	ChannelCostHMACKeyID      string   // MONITOR_CHANNEL_COST_HMAC_KEY_ID，写入证据供密钥轮换审计
	// 报表展示闸门与后台证据采集分离：先积累并核验 manifest，
	// 再显式开启页面，避免把部分数据误当成精确利润。
	ChannelEconomicsReportEnabled bool // MONITOR_CHANNEL_ECONOMICS_REPORT_ENABLED，默认 false

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
	// 用户用量事实层：采集阶段与页面切读阶段分开开关。上线时先只开采集，
	// 等本地小时覆盖率校验通过后再开 ReadEnabled；Redis 仅加速，不是事实源。
	UsageFactsStorePath   string // MONITOR_USAGE_FACTS_STORE_PATH，默认与主库同目录的 usage-facts.db
	UsageFactsEnabled     bool   // MONITOR_USAGE_FACTS_ENABLED，默认 false
	UsageFactsReadEnabled bool   // MONITOR_USAGE_FACTS_READ_ENABLED，默认 false；生产仅 FactsEnabled=true 时生效
	// UsageFactsLocalReadOnly 仅与 LocalSnapshotOnly 同时生效。它允许本机验收
	// 读取已验证的事实快照，但绝不允许访问来源库或启动事实采集。
	UsageFactsLocalReadOnly      bool // MONITOR_USAGE_FACTS_LOCAL_READ_ONLY，默认 false
	UsageFactsBackfillDays       int  // MONITOR_USAGE_FACTS_BACKFILL_DAYS，默认 366
	UsageFactsRetentionDays      int  // MONITOR_USAGE_FACTS_RETENTION_DAYS，默认 400
	UsageFactsHourRetentionDays  int  // MONITOR_USAGE_FACTS_HOUR_RETENTION_DAYS，默认 8
	UsageFactsSyncMinutes        int  // MONITOR_USAGE_FACTS_SYNC_MINUTES，默认 5
	UsageFactsProfileSyncMinutes int  // MONITOR_USAGE_FACTS_PROFILE_SYNC_MINUTES，默认 5
	UsageFactsBackfillDelayMS    int  // MONITOR_USAGE_FACTS_BACKFILL_DELAY_MS，默认 15000；后台只读回填的最低保护节流
	UsageFactsQueryTimeoutSec    int  // MONITOR_USAGE_FACTS_QUERY_TIMEOUT_SECONDS，默认 20
	UsageFactsLagMinutes         int  // MONITOR_USAGE_FACTS_LAG_MINUTES，默认 10；未闭合尾部不作为漏采
	// 全历史是显式灰度开关：事实层可先继续服务已签收的固定窗口，只有开启后
	// 才按成员真实来源边界创建持久日级任务。它绝不把 BackfillDays 当历史起点。
	UsageFactsFullHistoryEnabled        bool  // MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED，默认 false
	UsageFactsHistoryDelayMS            int   // MONITOR_USAGE_FACTS_HISTORY_DELAY_MS，默认 30000；成功 chunk 间隔
	UsageFactsHistoryDutyPercent        int   // MONITOR_USAGE_FACTS_HISTORY_SOURCE_DUTY_PERCENT，默认 20
	UsageFactsHistoryMaxDiskUsedPercent int   // MONITOR_USAGE_FACTS_HISTORY_MAX_DISK_USED_PERCENT，默认 80
	UsageFactsHistoryMinFreeBytes       int64 // MONITOR_USAGE_FACTS_HISTORY_MIN_FREE_BYTES，默认 2GiB
	// Full-history must be backed by a declared complete source. "complete"
	// means the selected logs source still contains every retained user-traffic
	// row since account creation; SourceEpoch changes whenever archival/routing
	// semantics change and forces all source proofs to be rebuilt.
	UsageFactsHistorySourceMode  string // MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE，默认 unverified
	UsageFactsHistorySourceEpoch string // MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH，显式稳定标识
	// RawPageImport switches full-history live/cold/repair work to the bounded
	// raw-log protocol: one member shard is read as cursor pages and aggregated
	// only in SQLite. It is the production default; false is an explicit
	// emergency compatibility rollback to the legacy source GROUP BY path.
	UsageFactsRawPageImportEnabled bool // MONITOR_USAGE_FACTS_RAW_PAGE_IMPORT_ENABLED，默认 true
	// 分类口径升级可能要求重签全部日志派生事实。普通启动绝不隐式清表；只有在
	// 已关闭页面切读、已签完整来源且开启全历史持久任务后，运维才能在维护窗口
	// 显式撤销旧发布授权并按成员逐日覆盖。旧事实保留，供回滚与分片校验。
	UsageFactsClassificationMigrationEnabled bool // MONITOR_USAGE_FACTS_CLASSIFICATION_MIGRATION_ENABLED，默认 false
	// 已完成小时也会低频轮换复核，用于修正晚到或事后变更的日志。
	// 每轮只读一个小时，且仍与前台查询共用串行闸门。
	UsageFactsReconcileMinutes int // MONITOR_USAGE_FACTS_RECONCILE_MINUTES，默认 30
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
	InfraEnabled bool // MONITOR_INFRA_ENABLED(=true 才启用主动采样/探测)
	// CapacityEnabled 只开放容量规划的本地读取页。它不启动 worker、
	// 不访问 NewAPI/MySQL/Nginx/AWS，只联合展示已落盘的脱敏事实。
	CapacityEnabled bool // MONITOR_CAPACITY_ENABLED，默认 false
	// InfraSnapshotReadOnly 只开放已落入 Monitor SQLite 的服务端快照和曲线。
	// 它不启动 AWS、域名、源站锁探测，也不评估/发送基础设施告警；
	// 仅用于本机验收和其他只读快照场景。
	InfraSnapshotReadOnly bool   // MONITOR_INFRA_SNAPSHOT_READ_ONLY，默认 false
	AWSRegion             string // AWS_REGION,如 us-west-2;AWS 凭证用 SDK 默认链(AWS_ACCESS_KEY_ID/_SECRET)
	InfraSampleSeconds    int    // MONITOR_INFRA_SAMPLE_SECONDS,默认 300(AWS 指标本就 5min 分辨率)
	InfraRetentionDays    int    // MONITOR_INFRA_RETENTION_DAYS,默认 7
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

	// 以下字段只用于包内单元测试注入可控时钟/连接。
	// LoadSettings 会显式标记 lifecycle 已配置；直接 Settings{}
	// 保持历史测试语义（worker 开、lease 关）。
	sourceLifecycleConfigured bool
	sourceOpen                func(string) (*sql.DB, error)
	sourceProbe               func(context.Context, *sql.DB) error
	sourceAcquireLease        func(context.Context, *sql.DB, string) (sourceLeaseHandle, bool, error)
	sourceRetryDelay          func(int) time.Duration
	sourceCheckInterval       time.Duration
	sourcePreflightInterval   time.Duration
	sourceDrainTimeout        time.Duration
	localProbeInterval        time.Duration
	sourceWorkerStart         func(context.Context, *Monitor)
}

// LoadSettings 从环境变量装载配置(可配合 .env)。
func LoadSettings() Settings {
	return Settings{
		Addr:                                     env("MONITOR_ADDR", ":8090"),
		ProdDSN:                                  env("NEWAPI_LOG_DSN", ""),
		StorePath:                                env("MONITOR_STORE_PATH", "monitor.db"),
		StoreBackupEnabled:                       env("MONITOR_STORE_BACKUP_ENABLED", "true") == "true",
		StoreBackupDir:                           strings.TrimSpace(env("MONITOR_STORE_BACKUP_DIR", "")),
		StoreBackupIntervalHours:                 envInt("MONITOR_STORE_BACKUP_INTERVAL_HOURS", 24),
		StoreBackupRetention:                     envInt("MONITOR_STORE_BACKUP_RETENTION", 7),
		StoreMigrationBackupRetention:            envInt("MONITOR_STORE_MIGRATION_BACKUP_RETENTION", 3),
		LocalSnapshotOnly:                        env("MONITOR_LOCAL_SNAPSHOT_ONLY", "false") == "true",
		LocalAuthBypass:                          env("MONITOR_LOCAL_AUTH_BYPASS", "false") == "true",
		SourceWorkerEnabled:                      env("MONITOR_SOURCE_WORKER_ENABLED", "true") == "true",
		SourceLeaseRequired:                      env("MONITOR_SOURCE_LEASE_REQUIRED", "true") == "true",
		SourceLeaseName:                          strings.TrimSpace(env("MONITOR_SOURCE_LEASE_NAME", "newapi-monitor-source-worker-v1")),
		sourceLifecycleConfigured:                true,
		SampleSeconds:                            envInt("MONITOR_SAMPLE_SECONDS", 60),
		RetentionDays:                            envInt("MONITOR_RETENTION_DAYS", 7),
		HourRetentionDays:                        envInt("MONITOR_HOUR_RETENTION_DAYS", 90),
		BackfillHours:                            envInt("MONITOR_BACKFILL_HOURS", 1),
		StabilityEnabled:                         env("MONITOR_STABILITY_ENABLED", "true") == "true",
		StabilityQueryMaxDays:                    envInt("MONITOR_STABILITY_QUERY_MAX_DAYS", 90),
		StabilityRetentionDays:                   envInt("MONITOR_STABILITY_RETENTION_DAYS", 181),
		StabilityProblemSampleSec:                envInt("MONITOR_STABILITY_PROBLEM_SAMPLE_SECONDS", 300),
		BackgroundSourceMinStartIntervalMS:       envInt("MONITOR_BACKGROUND_SOURCE_MIN_START_INTERVAL_MS", 2000),
		StabilityBackfillDelayMS:                 envInt("MONITOR_STABILITY_BACKFILL_DELAY_MS", 2000),
		StabilityBackfillTimeoutSec:              envInt("MONITOR_STABILITY_BACKFILL_TIMEOUT_SECONDS", 20),
		StabilityBackfillServerMaxExecutionMS:    envInt("MONITOR_STABILITY_BACKFILL_SERVER_MAX_EXECUTION_MS", 8000),
		StabilityBackfillSourceDutyPercent:       envInt("MONITOR_STABILITY_BACKFILL_SOURCE_DUTY_PERCENT", 20),
		StabilityBackfillEnabled:                 env("MONITOR_STABILITY_BACKFILL_ENABLED", "true") == "true",
		StabilityAutoRepair:                      env("MONITOR_STABILITY_AUTO_REPAIR", "true") == "true",
		StabilityClassificationMigrationEnabled:  env("MONITOR_STABILITY_CLASSIFICATION_MIGRATION_ENABLED", "false") == "true",
		NginxEnabled:                             env("MONITOR_NGINX_ENABLED", "false") == "true",
		NginxRetentionDays:                       envInt("MONITOR_NGINX_RETENTION_DAYS", 7),
		NginxAllowedNodes:                        envCSV("MONITOR_NGINX_ALLOWED_NODES"),
		NginxErrorEnabled:                        env("MONITOR_NGINX_ERROR_ENABLED", "false") == "true",
		NginxEvidenceMode:                        strings.ToLower(strings.TrimSpace(env("MONITOR_NGINX_EVIDENCE_MODE", "off"))),
		NginxEvidenceStorePath:                   strings.TrimSpace(env("MONITOR_NGINX_EVIDENCE_STORE_PATH", "")),
		NginxEvidenceRetentionHours:              envInt("MONITOR_NGINX_EVIDENCE_RETENTION_HOURS", 168),
		NginxEvidenceHMACKey:                     env("MONITOR_NGINX_EVIDENCE_HMAC_KEY", ""),
		NginxEvidenceHMACKeyID:                   strings.TrimSpace(env("MONITOR_NGINX_EVIDENCE_HMAC_KEY_ID", "")),
		NginxEvidenceMaxMiB:                      envInt("MONITOR_NGINX_EVIDENCE_MAX_MIB", 512),
		NginxEvidencePreviousHMACKey:             env("MONITOR_NGINX_EVIDENCE_PREVIOUS_HMAC_KEY", ""),
		NginxEvidencePreviousHMACKeyID:           strings.TrimSpace(env("MONITOR_NGINX_EVIDENCE_PREVIOUS_HMAC_KEY_ID", "")),
		NewAPIBaseURL:                            env("MONITOR_NEWAPI_BASE_URL", ""),
		SessionSecret:                            env("MONITOR_SESSION_SECRET", ""),
		UpstreamCredentialSecret:                 env("MONITOR_UPSTREAM_CREDENTIAL_SECRET", ""),
		UpstreamSyncEnabled:                      env("MONITOR_UPSTREAM_SYNC_ENABLED", "true") == "true",
		UpstreamSyncMinutes:                      envInt("MONITOR_UPSTREAM_SYNC_MINUTES", 5),
		UpstreamSyncTimeoutSec:                   envInt("MONITOR_UPSTREAM_SYNC_TIMEOUT_SECONDS", 15),
		UpstreamUsageSyncEnabled:                 env("MONITOR_UPSTREAM_USAGE_SYNC_ENABLED", "false") == "true",
		UpstreamUsageSyncMinutes:                 envInt("MONITOR_UPSTREAM_USAGE_SYNC_MINUTES", 20),
		UpstreamUsageBackfillDays:                envInt("MONITOR_UPSTREAM_USAGE_BACKFILL_DAYS", 90),
		UpstreamMaxConcurrency:                   envInt("MONITOR_UPSTREAM_MAX_CONCURRENCY", 1),
		UpstreamPricingLedgerEnabled:             env("MONITOR_UPSTREAM_PRICING_LEDGER_ENABLED", "false") == "true",
		UpstreamPricingLedgerDomains:             envCSV("MONITOR_UPSTREAM_PRICING_LEDGER_DOMAINS"),
		UpstreamPricingBackfillHoursPerRun:       envInt("MONITOR_UPSTREAM_PRICING_BACKFILL_HOURS_PER_RUN", 1),
		ChannelCostClosureEnabled:                env("MONITOR_CHANNEL_COST_CLOSURE_ENABLED", "false") == "true",
		ChannelCostClosureDomains:                envCSV("MONITOR_CHANNEL_COST_CLOSURE_DOMAINS"),
		ChannelCostHMACKey:                       env("MONITOR_CHANNEL_COST_HMAC_KEY", ""),
		ChannelCostHMACKeyID:                     strings.TrimSpace(env("MONITOR_CHANNEL_COST_HMAC_KEY_ID", "")),
		ChannelEconomicsReportEnabled:            env("MONITOR_CHANNEL_ECONOMICS_REPORT_ENABLED", "false") == "true",
		PortalAddr:                               env("MONITOR_PORTAL_ADDR", ""),
		UsageRedisAddr:                           strings.TrimSpace(env("MONITOR_USAGE_REDIS_ADDR", "")),
		UsageRedisUsername:                       strings.TrimSpace(env("MONITOR_USAGE_REDIS_USERNAME", "")),
		UsageRedisPassword:                       env("MONITOR_USAGE_REDIS_PASSWORD", ""),
		UsageRedisDB:                             envInt("MONITOR_USAGE_REDIS_DB", 0),
		UsageRedisPrefix:                         strings.Trim(strings.TrimSpace(env("MONITOR_USAGE_REDIS_PREFIX", "nxmon:usage:v1")), ":"),
		UsageFactsStorePath:                      strings.TrimSpace(env("MONITOR_USAGE_FACTS_STORE_PATH", "")),
		UsageFactsEnabled:                        env("MONITOR_USAGE_FACTS_ENABLED", "false") == "true",
		UsageFactsReadEnabled:                    env("MONITOR_USAGE_FACTS_READ_ENABLED", "false") == "true",
		UsageFactsLocalReadOnly:                  env("MONITOR_USAGE_FACTS_LOCAL_READ_ONLY", "false") == "true",
		UsageFactsBackfillDays:                   envInt("MONITOR_USAGE_FACTS_BACKFILL_DAYS", 366),
		UsageFactsRetentionDays:                  envInt("MONITOR_USAGE_FACTS_RETENTION_DAYS", 400),
		UsageFactsHourRetentionDays:              envInt("MONITOR_USAGE_FACTS_HOUR_RETENTION_DAYS", 8),
		UsageFactsSyncMinutes:                    envInt("MONITOR_USAGE_FACTS_SYNC_MINUTES", 5),
		UsageFactsProfileSyncMinutes:             envInt("MONITOR_USAGE_FACTS_PROFILE_SYNC_MINUTES", 5),
		UsageFactsBackfillDelayMS:                envInt("MONITOR_USAGE_FACTS_BACKFILL_DELAY_MS", 15000),
		UsageFactsQueryTimeoutSec:                envInt("MONITOR_USAGE_FACTS_QUERY_TIMEOUT_SECONDS", 20),
		UsageFactsLagMinutes:                     envInt("MONITOR_USAGE_FACTS_LAG_MINUTES", 10),
		UsageFactsFullHistoryEnabled:             env("MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED", "false") == "true",
		UsageFactsHistoryDelayMS:                 envInt("MONITOR_USAGE_FACTS_HISTORY_DELAY_MS", 30000),
		UsageFactsHistoryDutyPercent:             envInt("MONITOR_USAGE_FACTS_HISTORY_SOURCE_DUTY_PERCENT", 20),
		UsageFactsHistoryMaxDiskUsedPercent:      envInt("MONITOR_USAGE_FACTS_HISTORY_MAX_DISK_USED_PERCENT", 80),
		UsageFactsHistoryMinFreeBytes:            envInt64("MONITOR_USAGE_FACTS_HISTORY_MIN_FREE_BYTES", 2*1024*1024*1024),
		UsageFactsHistorySourceMode:              strings.ToLower(strings.TrimSpace(env("MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE", "unverified"))),
		UsageFactsHistorySourceEpoch:             strings.TrimSpace(env("MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH", "")),
		UsageFactsRawPageImportEnabled:           env("MONITOR_USAGE_FACTS_RAW_PAGE_IMPORT_ENABLED", "true") == "true",
		UsageFactsClassificationMigrationEnabled: env("MONITOR_USAGE_FACTS_CLASSIFICATION_MIGRATION_ENABLED", "false") == "true",
		UsageFactsReconcileMinutes:               envInt("MONITOR_USAGE_FACTS_RECONCILE_MINUTES", 30),
		TrustedProxies:                           envCSV("MONITOR_TRUSTED_PROXIES"),
		HeartbeatURL:                             env("MONITOR_HEARTBEAT_URL", ""),
		SiteName:                                 env("MONITOR_SITE_NAME", ""),
		IngestToken:                              env("MONITOR_INGEST_TOKEN", ""),
		InfraEnabled:                             env("MONITOR_INFRA_ENABLED", "") == "true",
		CapacityEnabled:                          env("MONITOR_CAPACITY_ENABLED", "false") == "true",
		InfraSnapshotReadOnly:                    env("MONITOR_INFRA_SNAPSHOT_READ_ONLY", "false") == "true",
		AWSRegion:                                env("AWS_REGION", "us-west-2"),
		InfraSampleSeconds:                       envInt("MONITOR_INFRA_SAMPLE_SECONDS", 300),
		InfraRetentionDays:                       envInt("MONITOR_INFRA_RETENTION_DAYS", 7),
		InfraResources:                           env("MONITOR_INFRA_RESOURCES", ""),
		InfraExcludeResources:                    envCSV("MONITOR_INFRA_EXCLUDE_RESOURCES"),

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

func envInt64(k string, def int64) int64 {
	if v := os.Getenv(k); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
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
