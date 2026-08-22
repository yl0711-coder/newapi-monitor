// Package monitor 是一个【独立的】上游稳定性监控服务,完全自包含:
// 自带配置(settings.go,读环境变量)、自带本地采样库(store.go,独立 sqlite)、
// 自带页面(server.go + page.html)。运行时只按配置连接只读生产库、可选 Redis、
// 上游公开面板 API 及基础设施 API；入口见 main.go。
//
// 架构(关键:不给生产库带来负担):
//   - 采样器(sampler.go)每 N 秒对 new-api 生产 MySQL 做有界小窗口只读聚合，
//     按"分钟桶 × 渠道 × 模型 × 分组"写入本地 SQLite；原始错误低频增量采样。
//   - 监控页面只读本地库,与访问量/刷新/窗口解耦；页面刷新不会触发生产查询。
//   - 全程只读、不改 new-api;并本地留存历史,扛日志清理、为后续告警备数据。
//
// 状态由 Monitor 持有(无包级全局):用 New 创建、Start 起采样、RegisterRoutes 挂页面。
//
// logs 表关键列(对照 new-api model/log.go):type 2=成功 5=错误;channel_id/model_name/
// `group`/use_time(整秒)/prompt_tokens/completion_tokens/quota(1USD=500000)/created_at(unix,有索引)。
package monitor

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/go-sql-driver/mysql" // MySQL 驱动(database/sql 注册用)
	"gorm.io/gorm"
)

const quotaPerUSD = 500000.0

// 稳定性样本不足阈值:窗口内请求数低于此值标"样本不足"而非红灯,避免偶发请求误报。
const minSample = 20

// Monitor 是一个监控实例:持有配置、生产库只读连接、本地采样库与采样心跳。
// 用 New 创建,Start 启动后台采样,RegisterRoutes 挂载页面与接口。零包级全局,可多实例、易测。
type Monitor struct {
	cfg     Settings
	prodDB  *sql.DB  // new-api 生产库【只读】连接(采样器周期查询 + 用户用量按需查询);nil = 未连接
	storeDB *gorm.DB // 本地采样库
	// usageFactsDB 独立承载高增长的用量小时/日事实、同步水位和资料快照。
	// 生产通过 New 创建时默认与 storeDB 分文件，避免补数、WAL 膨胀、损坏或
	// 写锁把告警配置、渠道配置和其他 Monitor 页面一起拖垮。测试直接调用
	// openStore 且未配置路径时仍可共用 storeDB，保持轻量构造兼容。
	usageFactsDB *gorm.DB

	lastRun                   atomic.Int64 // 采样心跳:最近一次成功采样的 Unix 秒(0=从未)
	problemLastSuccess        atomic.Int64 // 原始错误采集器最近一次成功执行
	problemLastFailure        atomic.Int64 // 原始错误采集器最近一次失败
	problemLiveThrough        atomic.Int64 // 原始错误实时 lane 已确认到的分钟右水位
	stabilityBackfillRunning  atomic.Bool  // 长期小时补数串行闸门；人工任务与自动修洞共用
	usageFactsHistoryRestarts atomic.Int64 // 全历史持久 worker panic/意外退出后的守护重启次数
	ctxMu                     sync.RWMutex
	backgroundCtx             context.Context // Start 注入；后台任务不绑定浏览器请求生命周期
	shutdownInitOnce          sync.Once
	closeOnce                 sync.Once
	portalGCOnce              sync.Once
	shutdown                  chan struct{}
	processStartedAt          atomic.Int64
	shuttingDown              atomic.Bool
	// 来源库生命周期与 Web/SQLite 服务解耦。这些状态只由
	// supervisor 写、健康接口原子读，绝不在 /ready 请求中探测 MySQL。
	sourceLifecycleInitialized atomic.Bool
	sourceState                atomic.Int32
	sourceLastSuccessAt        atomic.Int64
	sourceLastFailureAt        atomic.Int64
	sourceNextRetryAt          atomic.Int64
	sourceFailureStreak        atomic.Int64
	sourceRuntimeFailureClass  atomic.Int32 // 0=none,1=transient,2=permanent；permanent 不会被覆盖
	sourceFailureNotify        chan struct{}
	sourceLeaseHeld            atomic.Bool
	sourceLeaseName            string // 由配置名 + DBName 稳定摘要组成，不含 host/凭据
	sourceWorkerRunning        atomic.Bool
	sourceSupervisorOnce       sync.Once
	sourceSupervisorCancelMu   sync.Mutex
	sourceSupervisorCancel     context.CancelFunc
	sourceSupervisorDone       chan struct{}
	sourceEpochMu              sync.RWMutex
	sourceEpochCtx             context.Context
	sourceQuarantineMu         sync.Mutex
	sourceQuarantinedLease     sourceLeaseHandle
	sourceQuarantineDone       <-chan struct{}
	sourceDrainTimedOut        atomic.Bool
	// 本地探针在后台低频更新。/ready 本身只读这些原子值。
	localProbeOnce    sync.Once
	localStoreProbeOK atomic.Bool
	localStoreProbeAt atomic.Int64
	localFactsProbeOK atomic.Bool
	localFactsProbeAt atomic.Int64
	// 稳定性覆盖/问题采集也由后台本地探针归纳，避免
	// /ready 为了观测自己去扫 SQLite 或来源库。
	stabilityCoverageCheckedAt atomic.Int64
	stabilityCoverageBPS       atomic.Int64 // 0..10000
	stabilityBackfillStalled   atomic.Bool
	stabilityProblemCoverageTo atomic.Int64
	stabilityProblemPending    atomic.Int64
	// Raw problem classification history has its own durable cold cursor.  It
	// must remain visible to /ready even when the execution flag is switched
	// off; otherwise a paused/incomplete v5 migration can be hidden behind a
	// healthy recent live cursor.
	stabilityProblemMigrationIncomplete atomic.Bool

	chMu    sync.RWMutex
	chNames map[string]string // 渠道 id->name 映射缓存

	snapMu    sync.Mutex
	snapCache map[int]cachedSnap // 按窗口缓存快照(短 TTL),去重并发请求、给 slave 减负

	usageGateOnce         sync.Once // 聚合/后台来源查询泳道，容量 1
	usageGate             chan struct{}
	usageDetailGateOnce   sync.Once // 浏览器日志分页/计数泳道，容量 1
	usageDetailGate       chan struct{}
	usageExportGateOnce   sync.Once // CSV 流式导出泳道，容量 1
	usageExportGate       chan struct{}
	usageSourceBudgetOnce sync.Once // 三条泳道共享的来源库预算，容量 2；连接池另留采样/身份查询余量
	usageSourceBudget     chan struct{}
	usageSourceInUse      atomic.Int64
	// 所有来源库后台重查询共用单槽：分钟采样、facts、稳定性补数不会
	// 同时占连接/IO。交互查询仍使用独立预算并优先获得服务。
	// 后台来源查询除了单并发，还共享全局最小启动间隔。Stability range
	// 结束后按 SQL 占用时长另外推进 low-only duty 水位；Tail/sampler 可在
	// 下一个全局间隔获得槽位，不被批量补数的 2–32s cooldown 饿死。
	backgroundSourceScheduleMu  sync.Mutex
	stabilitySourceNotBefore    time.Time // Stability low lane duty waterline; high-priority Tail/sampler bypass it.
	backgroundSourceActive      bool
	backgroundSourceNotify      chan struct{}
	backgroundSourceHighWaiters int
	backgroundSourceLowWaiters  int
	backgroundSourceLastStart   atomic.Int64
	backgroundSourceStarts      atomic.Uint64
	// 正在等待任一来源库泳道/预算的交互请求数；后台事实采集见到后主动让路。
	usageInteractiveWaiters atomic.Int64
	usageAggregateMetrics   usageQueryLaneMetrics
	usageDetailMetrics      usageQueryLaneMetrics
	usageExportMetrics      usageQueryLaneMetrics
	usageDayExpr            string // 日桶 SQL 表达式覆盖(仅测试用;生产走 MySQL 默认,见 usage.go dayExpr)
	// 用量事实同步与读取开关相互独立：先后台慢速补齐/校验，再显式切读本地事实。
	// syncMu 让近期修订、历史补数、资料快照严格串行；每次实际生产库读取还会
	// 进入 usageGate，避免与前台旧读路径并发扫描来源库。
	usageFactsSyncMu          sync.Mutex
	usageFactsLeaseSeq        atomic.Uint64
	usageFactsRevision        atomic.Int64
	usageFactsServingRevision atomic.Int64
	// 来源库保护状态只影响后台 MySQL→SQLite 同步。调度采用单向抖动，
	// 连续来源错误采用全局指数退避，避免固定节拍或故障期重试命中上游风控。
	usageFactsScheduleSeq         atomic.Uint64
	usageFactsAdaptiveTurn        atomic.Uint64
	usageFactsSourceFailureStreak atomic.Int64
	usageFactsSourceBackoffUntil  atomic.Int64 // UnixNano；0 表示未退避
	// 页面冷缓存查询不再读生产 logs，但不同键仍可同时扫描大量
	// 本地事实。两槽预算限制 SQLite/JSON 的 CPU 与 RSS 峰值，指标暴露在
	// /usage/cache-stats，方便区分“来源库等待”和“本地事实等待”。
	usageFactsReadGateOnce sync.Once
	usageFactsReadGate     chan struct{}
	usageFactsReadMetrics  usageFactsReadBudgetMetrics
	// 只有已通过完整性校验并原子发布的事实快照才置 true。它不是配置
	// 开关：名单或回填窗口变化时，新版在候选区继续补数，页面仍读上一份
	// 已发布快照；候选版完整后再一次性切换，不把整个用量页关闭。
	usageFactsReadReady atomic.Bool
	// ReadyFrom/ReadyThrough 是已发布快照的左闭、右开小时边界。左边界
	// 防止在只回填 2 天时把“近 7 天”前 5 天悄悄当成零；右边界用于提示
	// 最新已闭合小时是否尚在同步。两者都不会使页面退回生产 logs。
	usageFactsReadyFrom    atomic.Int64
	usageFactsReadyThrough atomic.Int64
	// 完整窗口的缺口自愈审计是 O(成员×小时) 的本地操作，不应在
	// 后台空闲循环中每分钟重复。该时间戳只控制最长一小时一次的分片审计。
	usageFactsGapAuditAt atomic.Int64
	// proof 缺口审计按成员分片轮转，避免每小时扫整个
	// 200/400×366 天台账。游标只是性能调度，不作为业务正确性证明。
	usageFactsGapAuditNextUser atomic.Int64
	// 日事实语义审计重新计算持久化业务行的内容指纹，能发现 SQLite
	// quick_check 无法识别的逻辑删除/误迁移。发布前强制执行；已发布服务版
	// 至少每小时复核一次。失败会立即关闭本地读许可且绝不回扫来源 logs。
	usageFactsSemanticAuditAt        atomic.Int64
	usageFactsSemanticAuditFailureAt atomic.Int64
	usageFactsSemanticAuditOK        atomic.Bool
	// A proven local/source mismatch closes reads before it can wait on the
	// publication mutex. The counter stays non-zero until a durable repair or
	// repair-hold marker exists, preventing a concurrent publisher refresh from
	// reopening the brief serialization window.
	usageFactsRepairHoldPending    atomic.Int64
	usageFactsSemanticAuditNextDay atomic.Int64
	usageFactsHistoryDiskBlocked   atomic.Bool
	usageFactsHistoryDiskLevel     atomic.Int64
	usageFactsHistoryDiskFreeBytes atomic.Int64
	usageFactsHistoryDiskUsedBPS   atomic.Int64 // 0..10000

	// 本地 SQLite 是 Monitor 的事实源。完整性、备份与事实同步守护状态只写
	// 原子值，健康接口读取时不会争用采样/页面查询锁。
	storeIntegrityCheckedAt      atomic.Int64
	storeIntegrityOK             atomic.Bool
	storeMaintenanceOnce         sync.Once
	storeBackupMu                sync.Mutex
	storeBackupLastSuccess       atomic.Int64
	storeBackupLastFailure       atomic.Int64
	storeBackupBytes             atomic.Int64
	storeManualBackupRunning     atomic.Bool
	usageFactsIntegrityCheckedAt atomic.Int64
	usageFactsIntegrityOK        atomic.Bool
	usageFactsBackupLastSuccess  atomic.Int64
	usageFactsBackupLastFailure  atomic.Int64
	usageFactsBackupBytes        atomic.Int64
	storeBackupSetLastSuccess    atomic.Int64
	storeBackupSetLastFailure    atomic.Int64
	storeBackupSetVerified       atomic.Bool
	usageFactsLoopHeartbeat      atomic.Int64
	usageFactsRestarts           atomic.Int64

	usageCache *usageResultCache // 用量昂贵聚合结果缓存：Redis 主缓存 + 有界本机应急缓存
	portalLim  *portalLimiter    // 客户端登录限流
	adminLim   *portalLimiter    // 管理端登录限流(按来源 IP)
	exportLim  *exportLimiter    // 客户端日志导出限流(每组织账号 1 次/5min,仅计成功下载)

	// 上游账户余额同步只访问各上游公开面板 API，结果和加密凭据均落 Monitor 本地库。
	// 全局串行锁同时保护 Sub2API 的旋转 refresh token，避免后台同步与人工刷新抢用旧 token。
	upstreamSyncMu               sync.Mutex
	upstreamClient               *http.Client
	upstreamCredentialPersistent bool
	upstreamBalanceAlertLastEval atomic.Int64 // 动态余额评估最多与余额同步同频，避免每分钟重复扫本地小时汇总
}

// cachedSnap 是一次快照的缓存项。
type cachedSnap struct {
	snap *Snapshot
	at   int64 // 计算时刻(unix 秒)
}

// usageQueryLaneMetrics 只记录调度耗时与计数，不保存 SQL、用户、筛选或结果。
// 每个泳道容量为 1，因此 holdStartedAt 只会对应一个当前持有者。
type usageQueryLaneMetrics struct {
	acquired      atomic.Uint64
	failed        atomic.Uint64
	waitNanos     atomic.Int64
	maxWaitNanos  atomic.Int64
	holdNanos     atomic.Int64
	maxHoldNanos  atomic.Int64
	holdStartedAt atomic.Int64
	active        atomic.Int64
}

type usageFactsReadBudgetMetrics struct {
	acquired     atomic.Uint64
	failed       atomic.Uint64
	completed    atomic.Uint64
	waiters      atomic.Int64
	active       atomic.Int64
	waitNanos    atomic.Int64
	maxWaitNanos atomic.Int64
	runNanos     atomic.Int64
	maxRunNanos  atomic.Int64
}

// snapCacheTTL 快照缓存有效期(秒)。小于采样间隔,既减负又不显著影响新鲜度。
const snapCacheTTL = 15

// New 创建监控实例:打开本地采样库;若配置了生产 DSN,则连库并校验连通。
// 不自动启动采样器——需调用 Start 才开始后台采样。
func New(s Settings) (*Monitor, error) {
	if strings.TrimSpace(s.UsageFactsStorePath) == "" && storeUsesFile(s.StorePath) {
		s.UsageFactsStorePath = filepath.Join(filepath.Dir(s.StorePath), "usage-facts.db")
	}
	if err := validateUsageFactsSettings(s); err != nil {
		return nil, err
	}
	credentialSecretConfigured := strings.TrimSpace(s.UpstreamCredentialSecret) != "" || strings.TrimSpace(s.SessionSecret) != ""
	if err := validateNginxSettings(s); err != nil {
		return nil, err
	}
	if s.SessionSecret == "" {
		s.SessionSecret = randomSecret() // 未配置则随机生成,重启后需重新登录
		slog.Warn("未设置 MONITOR_SESSION_SECRET,已临时随机生成;重启后所有登录失效,生产建议固定配置一个长随机串")
	}
	if s.NewAPIBaseURL == "" {
		slog.Warn("未设置 MONITOR_NEWAPI_BASE_URL,登录将无法验证身份;生产必须配成 new-api 地址,如 http://new-api:3000")
	}

	m := &Monitor{
		cfg:                          s,
		chNames:                      map[string]string{},
		snapCache:                    map[int]cachedSnap{},
		usageCache:                   newUsageResultCache(s),
		upstreamClient:               newUpstreamHTTPClient(upstreamSyncTimeout(s)),
		upstreamCredentialPersistent: credentialSecretConfigured,
		sourceFailureNotify:          make(chan struct{}, 1),
	}
	m.processStartedAt.Store(time.Now().Unix())
	if err := m.openStore(s.StorePath); err != nil {
		return nil, err
	}
	m.probeLocalStores(context.Background())
	m.sourceLifecycleInitialized.Store(true)
	initialized := false
	defer func() {
		if !initialized {
			m.Close()
		}
	}()
	// 本机验收必须能在完全没有 NEWAPI_LOG_DSN 的情况下启动。这个模式不是
	// “连接失败时退回本地”：它是显式隔离，确保页面验收不会意外读线上库。
	if s.LocalSnapshotOnly {
		m.setSourceState(sourceStateDisabled)
		slog.Info("本地快照只读模式已启用，已隔离生产日志库和后台采集")
		initialized = true
		return m, nil
	}
	if err := m.initializeSource(); err != nil {
		return nil, err
	}
	initialized = true
	return m, nil
}

// validateUsageFactsSettings 把“页面已经要求只读本地事实”的配置意图设为
// fail-closed。若采集器未启用却允许进程继续启动，读取包装器会被迫回到旧的
// 生产 logs 路径；这类拼错开关不能靠运维观察才能发现。
func validateUsageFactsSettings(s Settings) error {
	if s.UsageFactsLocalReadOnly && !s.LocalSnapshotOnly {
		return errors.New("MONITOR_USAGE_FACTS_LOCAL_READ_ONLY 只能与 MONITOR_LOCAL_SNAPSHOT_ONLY=true 一起使用")
	}
	if s.UsageFactsFullHistoryEnabled {
		onlineWorker := s.UsageFactsEnabled && !s.LocalSnapshotOnly
		offlineSnapshot := s.LocalSnapshotOnly && s.UsageFactsLocalReadOnly && s.UsageFactsReadEnabled
		if !onlineWorker && !offlineSnapshot {
			return errors.New("MONITOR_USAGE_FACTS_FULL_HISTORY_ENABLED 需要在线 facts worker，或显式的本地全历史只读快照模式")
		}
	}
	if s.UsageFactsFullHistoryEnabled {
		if strings.ToLower(strings.TrimSpace(s.UsageFactsHistorySourceMode)) != "complete" {
			return errors.New("全历史 facts 必须显式确认 MONITOR_USAGE_FACTS_HISTORY_SOURCE_MODE=complete；来源归档不完整时拒绝启动")
		}
		epoch := strings.TrimSpace(s.UsageFactsHistorySourceEpoch)
		if epoch == "" || len(epoch) > 64 {
			return errors.New("全历史 facts 必须配置 1～64 字节的 MONITOR_USAGE_FACTS_HISTORY_SOURCE_EPOCH")
		}
	}
	if s.UsageFactsClassificationMigrationEnabled {
		if !s.UsageFactsEnabled || !s.UsageFactsFullHistoryEnabled || s.LocalSnapshotOnly {
			return errors.New("MONITOR_USAGE_FACTS_CLASSIFICATION_MIGRATION_ENABLED 只能在在线全历史 facts worker 模式开启")
		}
		if s.UsageFactsReadEnabled {
			return errors.New("分类迁移维护窗口必须先关闭 MONITOR_USAGE_FACTS_READ_ENABLED，拒绝边服务边改写共享事实")
		}
	}
	if !s.UsageFactsReadEnabled {
		return nil
	}
	if s.LocalSnapshotOnly {
		if !s.UsageFactsLocalReadOnly {
			return errors.New("本地快照切读必须同时设置 MONITOR_USAGE_FACTS_LOCAL_READ_ONLY=true")
		}
		return nil
	}
	if !s.UsageFactsEnabled {
		return errors.New("MONITOR_USAGE_FACTS_READ_ENABLED=true 时必须同时启用 MONITOR_USAGE_FACTS_ENABLED，已拒绝静默回扫生产 logs")
	}
	return nil
}

// Start 启动后台采样(生产库未连接则空操作)。ctx 取消时采样器退出。
func (m *Monitor) Start(ctx context.Context) {
	m.ctxMu.Lock()
	m.backgroundCtx = ctx
	m.ctxMu.Unlock()
	// 备份只读取 Monitor 自己的 SQLite，与是否连接 NewAPI 来源库无关。
	// 本机验收可用 MONITOR_STORE_BACKUP_ENABLED=false 显式关闭。
	m.startStoreMaintenance(ctx)
	m.startLocalStoreProbe(ctx)
	go func() {
		select {
		case <-ctx.Done():
		case <-m.shutdownSignal():
		}
		m.shuttingDown.Store(true)
	}()
	if m.cfg.LocalSnapshotOnly {
		// 只核验已经挂载到本地的事实快照是否可读；不启动任何会访问主站、上游、
		// AWS 或公网的后台任务。这样本地容器可以作为真正的离线验收环境。
		m.startUsageFactsSync(ctx)
		return
	}
	// 已发布 facts 的可读性只依赖本地 SQLite；即使此刻
	// MySQL 拒绝连接，也应先恢复本地读水位。
	if m.cfg.UsageFactsEnabled || m.cfg.UsageFactsReadEnabled {
		m.refreshUsageFactsReadiness(ctx, time.Now())
	}
	if m.cfg.NginxEnabled {
		m.startNginxMaintenance(ctx)
	}
	if m.cfg.InfraEnabled {
		go m.startInfra(ctx)
	}
	m.startSourceSupervisor(ctx)
}

func (m *Monitor) taskContext() context.Context {
	m.ctxMu.RLock()
	ctx := m.backgroundCtx
	m.ctxMu.RUnlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

type backgroundSourcePriority uint8

const (
	backgroundSourceHigh backgroundSourcePriority = iota
	backgroundSourceLow
)

// acquireBackgroundSource is the default high-priority lane used by sampler
// and Usage facts Tail. Stability migration must call the explicit low lane.
func (m *Monitor) acquireBackgroundSource(ctx context.Context) (func(), error) {
	return m.acquireBackgroundSourcePriority(ctx, backgroundSourceHigh)
}

func (m *Monitor) acquireBackgroundSourceLow(ctx context.Context) (func(), error) {
	return m.acquireBackgroundSourcePriority(ctx, backgroundSourceLow)
}

func (m *Monitor) acquireBackgroundSourcePriority(ctx context.Context, priority backgroundSourcePriority) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if !m.sourceAccessAllowed() {
		return nil, errSourceNotReady
	}
	m.backgroundSourceScheduleMu.Lock()
	m.ensureBackgroundSourceNotifyLocked()
	if priority == backgroundSourceHigh {
		m.backgroundSourceHighWaiters++
	} else {
		m.backgroundSourceLowWaiters++
	}
	m.signalBackgroundSourceLocked()
	m.backgroundSourceScheduleMu.Unlock()
	registered := true
	unregister := func() {
		if !registered {
			return
		}
		m.backgroundSourceScheduleMu.Lock()
		if priority == backgroundSourceHigh {
			m.backgroundSourceHighWaiters--
		} else {
			m.backgroundSourceLowWaiters--
		}
		registered = false
		m.signalBackgroundSourceLocked()
		m.backgroundSourceScheduleMu.Unlock()
	}

	for {
		m.backgroundSourceScheduleMu.Lock()
		m.ensureBackgroundSourceNotifyLocked()
		if err := ctx.Err(); err != nil {
			if priority == backgroundSourceHigh {
				m.backgroundSourceHighWaiters--
			} else {
				m.backgroundSourceLowWaiters--
			}
			registered = false
			m.signalBackgroundSourceLocked()
			m.backgroundSourceScheduleMu.Unlock()
			return nil, err
		}
		if !m.sourceAccessAllowed() {
			if priority == backgroundSourceHigh {
				m.backgroundSourceHighWaiters--
			} else {
				m.backgroundSourceLowWaiters--
			}
			registered = false
			m.signalBackgroundSourceLocked()
			m.backgroundSourceScheduleMu.Unlock()
			return nil, errSourceNotReady
		}
		now := time.Now()
		startAt := m.backgroundSourceStartAtLocked(priority)
		canClaim := !m.backgroundSourceActive && !now.Before(startAt)
		if priority == backgroundSourceLow && m.backgroundSourceHighWaiters > 0 {
			canClaim = false
		}
		if canClaim {
			m.backgroundSourceActive = true
			if priority == backgroundSourceHigh {
				m.backgroundSourceHighWaiters--
			} else {
				m.backgroundSourceLowWaiters--
			}
			registered = false
			m.backgroundSourceLastStart.Store(now.UnixNano())
			m.backgroundSourceStarts.Add(1)
			m.signalBackgroundSourceLocked()
			m.backgroundSourceScheduleMu.Unlock()

			var once sync.Once
			return func() {
				once.Do(func() {
					m.backgroundSourceScheduleMu.Lock()
					m.backgroundSourceActive = false
					m.signalBackgroundSourceLocked()
					m.backgroundSourceScheduleMu.Unlock()
				})
			}, nil
		}
		notify := m.backgroundSourceNotify
		wait := time.Until(startAt)
		m.backgroundSourceScheduleMu.Unlock()

		if err := waitForBackgroundSourceChange(ctx, notify, wait); err != nil {
			unregister()
			return nil, err
		}
	}
}

func (m *Monitor) backgroundSourceMinStartInterval() time.Duration {
	ms := m.cfg.BackgroundSourceMinStartIntervalMS
	// 直接构造 Settings{} 的单元测试保持零等待；LoadSettings 为真实进程
	// 提供 2000ms 默认值。负值同样只作为测试中的显式禁用值。
	if ms <= 0 {
		return 0
	}
	if ms > 60000 {
		ms = 60000
	}
	return time.Duration(ms) * time.Millisecond
}

func (m *Monitor) backgroundSourceStartAtLocked(priority backgroundSourcePriority) time.Time {
	var startAt time.Time
	if last := m.backgroundSourceLastStart.Load(); last > 0 {
		startAt = time.Unix(0, last).Add(m.backgroundSourceMinStartInterval())
	}
	// Stability's source-duty cooldown protects the source from bulk traffic,
	// but it must not make a fresh Tail/sampler query wait for a 2–32 second
	// low-priority cooldown. Both priorities still obey the global start spacing.
	if priority == backgroundSourceLow && m.stabilitySourceNotBefore.After(startAt) {
		startAt = m.stabilitySourceNotBefore
	}
	return startAt
}

func waitForBackgroundSourceChange(ctx context.Context, notify <-chan struct{}, wait time.Duration) error {
	if wait <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-notify:
			return nil
		}
	}
	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-notify:
		return nil
	case <-timer.C:
		return nil
	}
}

func (m *Monitor) ensureBackgroundSourceNotifyLocked() {
	if m.backgroundSourceNotify == nil {
		m.backgroundSourceNotify = make(chan struct{})
	}
}

func (m *Monitor) signalBackgroundSourceLocked() {
	m.ensureBackgroundSourceNotifyLocked()
	close(m.backgroundSourceNotify)
	m.backgroundSourceNotify = make(chan struct{})
}

// deferBackgroundSourceStart is called by Stability before releasing the
// single source gate. The duty window applies only to low-priority bulk work;
// Tail and sampler may run at the next global min-start-spacing window.
func (m *Monitor) deferBackgroundSourceStart(delay time.Duration) {
	if delay <= 0 {
		return
	}
	m.backgroundSourceScheduleMu.Lock()
	next := time.Now().Add(delay)
	if next.After(m.stabilitySourceNotBefore) {
		m.stabilitySourceNotBefore = next
		m.signalBackgroundSourceLocked()
	}
	m.backgroundSourceScheduleMu.Unlock()
}

func (m *Monitor) backgroundSourceWaiterCounts() (high, low int) {
	m.backgroundSourceScheduleMu.Lock()
	high, low = m.backgroundSourceHighWaiters, m.backgroundSourceLowWaiters
	m.backgroundSourceScheduleMu.Unlock()
	return
}

// Enabled 报告生产库是否已连通。
func (m *Monitor) Enabled() bool { return m.prodDB != nil }

// Close 释放来源库、本地两份 SQLite、可选缓存与 HTTP 空闲连接。
// 独立事实库与主库可能指向同一连接（测试/兼容模式），因此只关闭一次。
func (m *Monitor) Close() {
	m.closeOnce.Do(func() {
		m.shuttingDown.Store(true)
		close(m.shutdownSignal())
		if !m.stopAndWaitSource() {
			// 有缺陷的 worker 忽略 cancel 时，不关闭它仍可能读写的
			// DB/HTTP 对象，也不释放 lease。容器将在 stop grace 内退出，
			// OS 统一回收；这比在进程尚活时制造跨 epoch/跨实例重叠安全。
			return
		}
		if m.usageCache != nil {
			m.usageCache.Close()
		}
		if m.upstreamClient != nil {
			m.upstreamClient.CloseIdleConnections()
		}
		m.sourceQuarantineMu.Lock()
		if m.sourceQuarantinedLease != nil {
			_ = m.sourceQuarantinedLease.Release()
			m.sourceQuarantinedLease = nil
		}
		m.sourceQuarantineMu.Unlock()
		if m.prodDB != nil {
			_ = m.prodDB.Close()
		}
		if m.usageFactsDB != nil && m.usageFactsDB != m.storeDB {
			if db, err := m.usageFactsDB.DB(); err == nil {
				_ = db.Close()
			}
		}
		if m.storeDB != nil {
			if db, err := m.storeDB.DB(); err == nil {
				_ = db.Close()
			}
		}
	})
}

func (m *Monitor) shutdownSignal() chan struct{} {
	m.shutdownInitOnce.Do(func() { m.shutdown = make(chan struct{}) })
	return m.shutdown
}

// InfraEnabled 报告服务端监控页是否可读。生产主动采样由
// MONITOR_INFRA_ENABLED 单独控制；本机可只开放已落盘快照，不因页面验收
// 而启动 AWS/域名探测。所有采样与告警路径仍只检查 cfg.InfraEnabled。
func (m *Monitor) InfraEnabled() bool {
	return m.cfg.InfraEnabled || m.cfg.InfraSnapshotReadOnly
}

// ---- 页面数据结构 ----

// Summary 是某时间窗口的总体指标汇总(成功率、时延分位、错误构成等)。
type Summary struct {
	WindowMinutes int     `json:"window_minutes"`
	Total         int64   `json:"total"`
	Success       int64   `json:"success"`      // 干净成功
	Anomaly       int64   `json:"anomaly"`      // 异常(客户端断开等)
	Failed        int64   `json:"failed"`       // 错误(type=5)
	SuccessRate   float64 `json:"success_rate"` // 干净成功率(异常、错误都不算)
	AnomalyRate   float64 `json:"anomaly_rate"`
	ErrorRate     float64 `json:"error_rate"`
	// 交付异常明细(B 类)
	AnomalyBilled  int64   `json:"anomaly_billed"`
	AnomalyFree    int64   `json:"anomaly_free"`
	AnomalyStream  int64   `json:"anomaly_stream"`
	AnomalyCostUSD float64 `json:"anomaly_cost_usd"`
	AnomalyAvgWait float64 `json:"anomaly_avg_wait"`
	QPS            float64 `json:"qps"`
	AvgLatency     float64 `json:"avg_latency"`
	MaxLatency     int     `json:"max_latency"`
	P50            float64 `json:"p50"`
	P95            float64 `json:"p95"`
	P99            float64 `json:"p99"`
	TtftP50        float64 `json:"ttft_p50"` // 首字延迟 p50(秒)
	TtftP95        float64 `json:"ttft_p95"` // 首字延迟 p95(秒)
	TokPerSec      float64 `json:"tok_per_sec"`
	Tokens         int64   `json:"tokens"`
	CostUSD        float64 `json:"cost_usd"`
	Err4xx         int64   `json:"err_4xx"`
	Err5xx         int64   `json:"err_5xx"`
	ErrTimeout     int64   `json:"err_timeout"`
	ErrOther       int64   `json:"err_other"`
	LatHist        []int64 `json:"lat_hist"`  // 总延迟分布:≤1/≤2/≤5/≤10/≤30/≤60/>60 秒
	TtftHist       []int64 `json:"ttft_hist"` // 首字延迟分布:≤.5/≤1/≤2/≤5/≤10/>10 秒
}

// Row 是某维度取值(分组 / 渠道 / 模型)在窗口内的指标行,含迷你趋势与健康色标。
type Row struct {
	Key         string  `json:"key"`
	Label       string  `json:"label"`
	Total       int64   `json:"total"`
	Success     int64   `json:"success"`
	Anomaly     int64   `json:"anomaly"`
	Failed      int64   `json:"failed"`
	SuccessRate float64 `json:"success_rate"`
	AnomalyRate float64 `json:"anomaly_rate"`
	ErrorRate   float64 `json:"error_rate"`
	// 交付异常明细(B 类):三项互斥、之和 = Anomaly;金额只含 B1(零输出却已扣费)。
	AnomalyBilled  int64       `json:"anomaly_billed"`
	AnomalyFree    int64       `json:"anomaly_free"`
	AnomalyStream  int64       `json:"anomaly_stream"`
	AnomalyCostUSD float64     `json:"anomaly_cost_usd"`
	AnomalyAvgWait float64     `json:"anomaly_avg_wait"` // 秒;用户白等多久
	QPS            float64     `json:"qps"`
	AvgLatency     float64     `json:"avg_latency"`
	MaxLatency     int         `json:"max_latency"`
	P50            float64     `json:"p50"`
	P95            float64     `json:"p95"`
	P99            float64     `json:"p99"`
	TtftP50        float64     `json:"ttft_p50"`
	TtftP95        float64     `json:"ttft_p95"`
	TokPerSec      float64     `json:"tok_per_sec"`
	Tokens         int64       `json:"tokens"`
	CostUSD        float64     `json:"cost_usd"`
	Err4xx         int64       `json:"err_4xx"`
	Err5xx         int64       `json:"err_5xx"`
	ErrTimeout     int64       `json:"err_timeout"`
	ErrOther       int64       `json:"err_other"`
	Health         string      `json:"health"`
	AnomalyBurst   bool        `json:"anomaly_burst"` // 异常成簇(连续/突增),需要关注
	Spark          []TimePoint `json:"spark"`         // 该维度最近若干分钟桶的成功/异常/失败,供迷你趋势
}

// TimePoint 是某分钟桶的成功 / 异常 / 失败计数,用于趋势与迷你图(sparkline)。
type TimePoint struct {
	Ts      int64 `json:"ts"`
	Success int64 `json:"success"`
	Anomaly int64 `json:"anomaly"`
	Failed  int64 `json:"failed"`
}

// TokenRow 是某令牌(API Key)在窗口内的指标行,回答"谁在制造错误 / 烧配额"。
type TokenRow struct {
	Key         string  `json:"key"`
	Total       int64   `json:"total"`
	Success     int64   `json:"success"`
	Anomaly     int64   `json:"anomaly"`
	Failed      int64   `json:"failed"`
	SuccessRate float64 `json:"success_rate"`
	AnomalyRate float64 `json:"anomaly_rate"`
	ErrorRate   float64 `json:"error_rate"`
	QPS         float64 `json:"qps"`
	Tokens      int64   `json:"tokens"`
	CostUSD     float64 `json:"cost_usd"`
	Health      string  `json:"health"`
}

// RejectionRow 是「前置拒绝」按 (原因 × 模型 × 分组) 聚合的一行,供「被拒请求」面板展示。
// 数据来自旁路采集器推送的 rejection_samples(new-api logs 表的盲区,如"无可用渠道")。
type RejectionRow struct {
	Reason string `json:"reason" gorm:"column:reason"`
	Model  string `json:"model" gorm:"column:model"`
	Group  string `json:"group" gorm:"column:group"`
	Count  int64  `json:"count" gorm:"column:count"`
}

// HourPoint 小时级序列点(长期趋势图)。
type HourPoint struct {
	Ts      int64 `json:"ts"`
	Success int64 `json:"success"`
	Anomaly int64 `json:"anomaly"`
	Failed  int64 `json:"failed"`
}

// PeriodStat 一个时间段的总览统计(同比环比用)。
type PeriodStat struct {
	Total       int64   `json:"total"`
	Failed      int64   `json:"failed"`
	SuccessRate float64 `json:"success_rate"`
	CostUSD     float64 `json:"cost_usd"`
}

// CompareStat 同比环比:近 24h vs 前 24h(环比) vs 上周同期(同比)。
type CompareStat struct {
	Now      PeriodStat `json:"now"`       // 近 24h
	Prev     PeriodStat `json:"prev"`      // 前 24h(环比基)
	LastWeek PeriodStat `json:"last_week"` // 上周同期(同比基)
}

// Snapshot 是一次完整看板快照:总览 + 分组 / 渠道 / 模型 / 令牌明细 + 趋势 + SLO + 同比环比。
type Snapshot struct {
	WindowMinutes  int            `json:"window_minutes"`
	GeneratedAt    string         `json:"generated_at"`
	SamplingActive bool           `json:"sampling_active"`
	DataAgeSec     int64          `json:"data_age_sec"`
	Summary        Summary        `json:"summary"`
	ByGroup        []Row          `json:"by_group"`
	ByChannel      []Row          `json:"by_channel"`
	ByModel        []Row          `json:"by_model"`
	ByToken        []TokenRow     `json:"by_token"`
	Trend          []TimePoint    `json:"trend"`
	SLO            SLOStatus      `json:"slo"`
	Compare        CompareStat    `json:"compare"`
	Rejections     []RejectionRow `json:"rejections"`     // 前置拒绝(采集器旁路采集,logs 盲区)
	RejectEnabled  bool           `json:"reject_enabled"` // 超管是否开启「被拒请求」面板
}

// attachSpark 给每行挂上对应维度取值的分钟桶时序(失败则静默跳过)。
func (m *Monitor) attachSpark(rows []Row, dimCol string, since int64, windowMinutes int) {
	series, err := m.storeDimSeries(dimCol, since, windowMinutes)
	if err != nil {
		return
	}
	for i := range rows {
		if s := series[rows[i].Key]; s != nil {
			rows[i].Spark = s
			rows[i].AnomalyBurst = anomalyBurst(s, 3)
			// 异常成簇 → 至少升到"关注"(黄);错误已驱动的 bad 不降级。
			if rows[i].AnomalyBurst && rows[i].Health == "good" {
				rows[i].Health = "warn"
			}
		}
	}
}

// anomalyBurst 判断异常是否"成簇/连续":连续 ≥n 个采样桶都有异常。
// 单个/零散异常(网络抖动)不算;持续多桶出现才算需要关注。看板用默认 3,报警用配置值。
func anomalyBurst(spark []TimePoint, n int) bool {
	if n < 1 {
		n = 3
	}
	run := 0
	for _, p := range spark {
		if p.Anomaly > 0 {
			run++
			if run >= n {
				return true
			}
		} else {
			run = 0
		}
	}
	return false
}

func health(total int64, r float64) string {
	if total < minSample {
		return "nosample"
	}
	switch {
	case r >= 99:
		return "good"
	case r >= 95:
		return "warn"
	default:
		return "bad"
	}
}

func rate(success, total int64) float64 {
	if total == 0 {
		return 0
	}
	return float64(success) / float64(total) * 100
}

// GetSnapshot 从本地库聚合一次完整看板数据(零生产负担)。
// GetSnapshot 返回看板快照;带短 TTL 缓存(按窗口),去重并发请求、减少重复重算,给 slave 减负。
func (m *Monitor) GetSnapshot(windowMinutes int, nowUnix int64) (*Snapshot, error) {
	if windowMinutes <= 0 {
		windowMinutes = 60
	}
	m.snapMu.Lock()
	defer m.snapMu.Unlock()
	if m.snapCache == nil {
		m.snapCache = map[int]cachedSnap{}
	}
	if c, ok := m.snapCache[windowMinutes]; ok && nowUnix-c.at < snapCacheTTL {
		return c.snap, nil
	}
	snap, err := m.computeSnapshot(windowMinutes, nowUnix)
	if err != nil {
		return nil, err
	}
	m.snapCache[windowMinutes] = cachedSnap{snap: snap, at: nowUnix}
	return snap, nil
}

func (m *Monitor) computeSnapshot(windowMinutes int, nowUnix int64) (*Snapshot, error) {
	if windowMinutes <= 0 {
		windowMinutes = 60
	}
	since := nowUnix - int64(windowMinutes)*60
	windowSec := float64(windowMinutes) * 60

	sum, err := m.storeSummary(since, windowSec)
	if err != nil {
		return nil, err
	}
	grp, err := m.storeDim("grp", since, windowSec)
	if err != nil {
		return nil, err
	}
	ch, err := m.storeDim("channel_id", since, windowSec)
	if err != nil {
		return nil, err
	}
	chMap := m.channelNames()
	for i := range ch {
		if name := chMap[ch[i].Key]; name != "" {
			ch[i].Label = "#" + ch[i].Key + " " + name
		} else {
			ch[i].Label = "#" + ch[i].Key
		}
	}
	md, err := m.storeDim("model_name", since, windowSec)
	if err != nil {
		return nil, err
	}
	trend, err := m.storeTrend(since, windowMinutes)
	if err != nil {
		return nil, err
	}
	// 给每行挂上迷你趋势(sparkline)序列
	m.attachSpark(grp, "grp", since, windowMinutes)
	m.attachSpark(ch, "channel_id", since, windowMinutes)
	m.attachSpark(md, "model_name", since, windowMinutes)

	tokens, terr := m.storeTokens(since, windowSec)
	if terr != nil {
		tokens = nil // token 维度失败不影响主看板
	}
	ac := m.loadAlertConfig()
	slo := m.computeSLO(ac, nowUnix)
	compare := m.storeCompare(nowUnix)
	var rejections []RejectionRow
	if ac.RejectPanelEnabled { // 关闭时不查、不下发,面板隐藏
		rejections = m.storeRejections(nowUnix - int64(windowMinutes)*60)
	}

	lastBucket := m.storeFreshness()
	age := int64(-1)
	if lastBucket > 0 {
		age = nowUnix - (lastBucket + 60)
		if age < 0 {
			age = 0
		}
	}

	return &Snapshot{
		WindowMinutes:  windowMinutes,
		GeneratedAt:    time.Unix(nowUnix, 0).Format("2006-01-02 15:04:05"),
		SamplingActive: m.LastSampleRun() > nowUnix-int64(m.cfg.SampleSeconds)*3,
		DataAgeSec:     age,
		Summary:        *sum,
		ByGroup:        grp,
		ByChannel:      ch,
		ByModel:        md,
		ByToken:        tokens,
		Trend:          trend,
		SLO:            slo,
		Compare:        compare,
		Rejections:     rejections,
		RejectEnabled:  ac.RejectPanelEnabled,
	}, nil
}
