// Package monitor 是一个【独立的】上游稳定性监控服务,完全自包含:
// 自带配置(settings.go,读环境变量)、自带本地采样库(store.go,独立 sqlite)、
// 自带页面(server.go + page.html),无外部依赖。入口见 main.go。
//
// 架构(关键:不给生产库带来负担):
//   - 采样器(sampler.go)每 N 秒对 new-api 生产 MySQL 做【一条】小窗口聚合查询,
//     按"分钟桶 × 渠道 × 模型 × 分组"写入本地 sqlite。
//   - 页面只读本地库,与访问量/刷新/窗口完全解耦——生产库永远只承担"每周期一条小查询"。
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
	"fmt"
	"log/slog"
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

	lastRun atomic.Int64 // 采样心跳:最近一次成功采样的 Unix 秒(0=从未)

	chMu    sync.RWMutex
	chNames map[string]string // 渠道 id->name 映射缓存

	snapMu    sync.Mutex
	snapCache map[int]cachedSnap // 按窗口缓存快照(短 TTL),去重并发请求、给 slave 减负

	usageMu      sync.Mutex // 「用户用量」聚合串行闸:同一时刻最多一条按需聚合在生产库上跑(usage.go)
	usageDayExpr string     // 日桶 SQL 表达式覆盖(仅测试用;生产走 MySQL 默认,见 usage.go dayExpr)

	portalCache *ttlCache      // 客户端组级数据缓存(portal.go;RegisterPortalRoutes 时初始化)
	portalLim   *portalLimiter // 客户端登录限流
	exportLim   *exportLimiter // 客户端日志导出限流(每组织账号 1 次/5min,仅计成功下载)
}

// cachedSnap 是一次快照的缓存项。
type cachedSnap struct {
	snap *Snapshot
	at   int64 // 计算时刻(unix 秒)
}

// snapCacheTTL 快照缓存有效期(秒)。小于采样间隔,既减负又不显著影响新鲜度。
const snapCacheTTL = 15

// New 创建监控实例:打开本地采样库;若配置了生产 DSN,则连库并校验连通。
// 不自动启动采样器——需调用 Start 才开始后台采样。
func New(s Settings) (*Monitor, error) {
	if s.SessionSecret == "" {
		s.SessionSecret = randomSecret() // 未配置则随机生成,重启后需重新登录
		slog.Warn("未设置 MONITOR_SESSION_SECRET,已临时随机生成;重启后所有登录失效,生产建议固定配置一个长随机串")
	}
	if s.NewAPIBaseURL == "" {
		slog.Warn("未设置 MONITOR_NEWAPI_BASE_URL,登录将无法验证身份;生产必须配成 new-api 地址,如 http://new-api:3000")
	}

	m := &Monitor{cfg: s, chNames: map[string]string{}, snapCache: map[int]cachedSnap{}}
	if err := m.openStore(s.StorePath); err != nil {
		return nil, err
	}
	if s.ProdDSN == "" {
		return nil, fmt.Errorf("未设置 NEWAPI_LOG_DSN(只读日志库),监控无数据来源")
	}
	conn, err := sql.Open("mysql", s.ProdDSN)
	if err != nil {
		return nil, fmt.Errorf("打开生产库失败: %w", err)
	}
	// 3 = 采样器(周期) + 用量聚合(按需,usageMu 已串行化) + 用户解析/SMTP 同步(偶发)。
	// 曾为 2(仅采样器时代),用量功能加入后两连接会在三方碰撞时把 5s 的解析请求饿到假超时。
	conn.SetMaxOpenConns(3)
	conn.SetMaxIdleConns(1)
	conn.SetConnMaxLifetime(5 * time.Minute)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = conn.PingContext(ctx); err != nil {
		return nil, fmt.Errorf("连接生产库失败: %w", err)
	}
	m.prodDB = conn
	return m, nil
}

// Start 启动后台采样(生产库未连接则空操作)。ctx 取消时采样器退出。
func (m *Monitor) Start(ctx context.Context) { m.startSampler(ctx) }

// Enabled 报告生产库是否已连通。
func (m *Monitor) Enabled() bool { return m.prodDB != nil }

// InfraEnabled 报告服务端健康监控(实例/DB/LB)是否启用(MONITOR_INFRA_ENABLED=true)。
// 关闭时:不调 AWS、/infra 返回 enabled:false、不影响模型监控。
func (m *Monitor) InfraEnabled() bool { return m.cfg.InfraEnabled }

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
	QPS           float64 `json:"qps"`
	AvgLatency    float64 `json:"avg_latency"`
	MaxLatency    int     `json:"max_latency"`
	P50           float64 `json:"p50"`
	P95           float64 `json:"p95"`
	P99           float64 `json:"p99"`
	TtftP50       float64 `json:"ttft_p50"` // 首字延迟 p50(秒)
	TtftP95       float64 `json:"ttft_p95"` // 首字延迟 p95(秒)
	TokPerSec     float64 `json:"tok_per_sec"`
	Tokens        int64   `json:"tokens"`
	CostUSD       float64 `json:"cost_usd"`
	Err4xx        int64   `json:"err_4xx"`
	Err5xx        int64   `json:"err_5xx"`
	ErrTimeout    int64   `json:"err_timeout"`
	ErrOther      int64   `json:"err_other"`
	LatHist       []int64 `json:"lat_hist"`  // 总延迟分布:≤1/≤2/≤5/≤10/≤30/≤60/>60 秒
	TtftHist      []int64 `json:"ttft_hist"` // 首字延迟分布:≤.5/≤1/≤2/≤5/≤10/>10 秒
}

// Row 是某维度取值(分组 / 渠道 / 模型)在窗口内的指标行,含迷你趋势与健康色标。
type Row struct {
	Key          string      `json:"key"`
	Label        string      `json:"label"`
	Total        int64       `json:"total"`
	Success      int64       `json:"success"`
	Anomaly      int64       `json:"anomaly"`
	Failed       int64       `json:"failed"`
	SuccessRate  float64     `json:"success_rate"`
	AnomalyRate  float64     `json:"anomaly_rate"`
	ErrorRate    float64     `json:"error_rate"`
	QPS          float64     `json:"qps"`
	AvgLatency   float64     `json:"avg_latency"`
	MaxLatency   int         `json:"max_latency"`
	P50          float64     `json:"p50"`
	P95          float64     `json:"p95"`
	P99          float64     `json:"p99"`
	TtftP50      float64     `json:"ttft_p50"`
	TtftP95      float64     `json:"ttft_p95"`
	TokPerSec    float64     `json:"tok_per_sec"`
	Tokens       int64       `json:"tokens"`
	CostUSD      float64     `json:"cost_usd"`
	Err4xx       int64       `json:"err_4xx"`
	Err5xx       int64       `json:"err_5xx"`
	ErrTimeout   int64       `json:"err_timeout"`
	ErrOther     int64       `json:"err_other"`
	Health       string      `json:"health"`
	AnomalyBurst bool        `json:"anomaly_burst"` // 异常成簇(连续/突增),需要关注
	Spark        []TimePoint `json:"spark"`         // 该维度最近若干分钟桶的成功/异常/失败,供迷你趋势
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
