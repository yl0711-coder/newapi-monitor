package monitor

import (
	"fmt"
	"log/slog"
	"strings"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// store.go:监控自带的本地采样库(独立 sqlite,不碰 new-api 的库)。
// 页面只读这里;唯一写入者是采样器。所有存储函数挂在 Monitor 上,用 m.storeDB。

// MetricSample 按【分钟桶 × 渠道 × 模型 × 分组】聚合的一行采样。
// 复合主键使采样可幂等 UPSERT,自愈小的采集间隙。
type MetricSample struct {
	BucketTs  int64  `gorm:"primaryKey;autoIncrement:false;index:idx_metric_bucket"`
	ChannelID int    `gorm:"primaryKey;autoIncrement:false"`
	ModelName string `gorm:"primaryKey;size:128"`
	Grp       string `gorm:"primaryKey;size:64;column:grp"`

	Success    int64 // 干净成功(type=2 且正常结束)
	Anomaly    int64 // 异常(type=2 但 client_gone/scanner_error/panic/ping_fail)
	Failed     int64 // 错误(type=5,上游返回的错误)
	SumUseTime int64
	MaxUseTime int
	Tokens     int64
	Quota      int64
	// 失败粗分类(GORM 默认不在数字前加下划线,故对数字字段显式列名)
	Err4xx     int64 `gorm:"column:err_4xx"`
	Err5xx     int64 `gorm:"column:err_5xx"`
	ErrTimeout int64
	ErrOther   int64

	// 成功请求的总延迟直方图(各档【非累计】计数,单位秒),用于近似 p50/p95/p99。
	Lat1   int64 `gorm:"column:lat_1"`   // (0,1]
	Lat2   int64 `gorm:"column:lat_2"`   // (1,2]
	Lat5   int64 `gorm:"column:lat_5"`   // (2,5]
	Lat10  int64 `gorm:"column:lat_10"`  // (5,10]
	Lat30  int64 `gorm:"column:lat_30"`  // (10,30]
	Lat60  int64 `gorm:"column:lat_60"`  // (30,60]
	LatInf int64 `gorm:"column:lat_inf"` // (60,+∞)

	// 出字速度用:成功请求的输出 token 数之和(tok/s = CompletionTokens / SumUseTime)。
	CompletionTokens int64 `gorm:"column:completion_tokens"`

	// 首字延迟 TTFT 直方图(成功且 frt>0,单位【毫秒】),用于近似 p50/p95。
	Ttft500   int64 `gorm:"column:ttft_500"`    // (0,500ms]
	Ttft1k    int64 `gorm:"column:ttft_1k"`     // (500,1000]
	Ttft2k    int64 `gorm:"column:ttft_2k"`     // (1000,2000]
	Ttft5k    int64 `gorm:"column:ttft_5k"`     // (2000,5000]
	Ttft10k   int64 `gorm:"column:ttft_10k"`    // (5000,10000]
	TtftInf   int64 `gorm:"column:ttft_inf"`    // (10000,+∞)
	TtftMaxMs int   `gorm:"column:ttft_max_ms"` // 最大 frt(ms),用于分位末档收尾
}

// TokenSample 按【分钟桶 × 令牌(API Key)】聚合,用于"谁在制造错误 / 烧配额"维度。
// 故意比 MetricSample 轻(不交叉渠道/模型)以控制基数。
type TokenSample struct {
	BucketTs  int64  `gorm:"primaryKey;autoIncrement:false;index"`
	TokenName string `gorm:"primaryKey;size:128;column:token_name"`
	Success   int64
	Anomaly   int64
	Failed    int64
	Tokens    int64
	Quota     int64
}

// HourSample 小时级汇总(rollup):每小时一行总览,长期留存(默认 90 天),支撑长期趋势 + 同比环比。
// 由分钟级 metric_samples 周期性汇总而来;存储只随时间增长(每小时 1 行),与请求量无关。
type HourSample struct {
	HourTs     int64 `gorm:"primaryKey;autoIncrement:false"`
	Success    int64
	Anomaly    int64
	Failed     int64
	Tokens     int64
	Quota      int64
	SumUseTime int64
}

// ChannelSnap 渠道健康快照:采样器周期性从生产 channels 表读入(id/状态/分组/模型),
// 供对外看板(public 包)派生"某线路×模型有无可用渠道"。仅存路由与状态,【无任何密钥】。
type ChannelSnap struct {
	ID           int    `gorm:"primaryKey;autoIncrement:false"`
	Status       int    // new-api: 1启用 / 2手动禁用 / 3自动禁用
	Groups       string `gorm:"size:512"`  // 逗号分隔分组
	Models       string `gorm:"type:text"` // 逗号分隔模型
	EnabledSince int64  // 当前这段"启用"的起始 Unix 秒;禁用=0;0 也表示"自始启用"(算全量历史);重启用刷新为重启用时刻
	UpdatedAt    int64  `gorm:"index"`
}

// enabledChanFilter 把"已知被禁用 / 在其启用时刻之前"的渠道流量排除出稳定性聚合:
// 禁用渠道(手动/熔断)的旧账不计入;重新启用的渠道从 enabled_since 重新计。
// 用 NOT EXISTS(反向)而非 EXISTS(正向):**没有渠道快照的流量默认保留**(fail-open)——
// 避免"新部署首刷前 channel_snaps 为空 → 全被排除"的空窗;只排除明确已知该排除的。
// 仅用于"跨渠道聚合"(总览/分组/模型/趋势 + 看板);按渠道明细(channel_id)不加,排障仍能看到禁用渠道。
const enabledChanFilter = ` AND NOT EXISTS (SELECT 1 FROM channel_snaps c ` +
	`WHERE c.id = metric_samples.channel_id AND (c.status <> 1 OR metric_samples.bucket_ts < c.enabled_since))`

// channelDim 是"按渠道"维度的列名;该维度不施加 enabledChanFilter / selectableFilter。
const channelDim = "channel_id"

// SelectablePair 是"用户真能选到"的 (分组, 模型) 对:该分组在 /api/pricing 可见,且有启用渠道配置了它。
// 采样器每周期重算(可见分组 ∩ 启用渠道配置)。监控的稳定性聚合只统计在此表里的对——
// 不可选的(误路由 / 全禁用 / 只在不可选分组)不计入监控与报警("都不能选了报什么警")。
type SelectablePair struct {
	Grp   string `gorm:"primaryKey;size:64;column:grp"`
	Model string `gorm:"primaryKey;size:128"`
}

// selectableFilter 把"不可选的 (分组,模型)"排除出监控聚合;
// 表为空(未拉到 /api/pricing / 新部署首刷前)时 fail-open 不过滤,避免空窗。
// 仅用于跨(分组/模型)聚合(总览/分组/模型/趋势);按渠道明细不加,排障仍能看误路由等异常。
const selectableFilter = ` AND (NOT EXISTS (SELECT 1 FROM selectable_pairs) OR ` +
	`EXISTS (SELECT 1 FROM selectable_pairs sp WHERE sp.grp = metric_samples.grp AND sp.model = metric_samples.model_name))`

// splitList 拆逗号分隔串(去空白、去空项),解析渠道的 groups/models 字段。
func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

// RejectionSample 是「前置拒绝」按分钟聚合的计数,由各节点的旁路采集器
// (newapi-reject-collector)POST 推来。这类拒绝(如"无可用渠道")不进 new-api 的 logs 表,
// 是 logs 维度监控的盲区,这里单列。复合主键使重复推送幂等(同键累加)。
type RejectionSample struct {
	BucketTs int64  `gorm:"primaryKey;autoIncrement:false;index"`
	Node     string `gorm:"primaryKey;size:64"`  // 来源节点(master/slave)
	Reason   string `gorm:"primaryKey;size:64"`  // no_available_channel 等
	Model    string `gorm:"primaryKey;size:128"` // 被拒模型
	Grp      string `gorm:"primaryKey;size:64;column:grp"`
	Count    int64
}

// InfraSample 是「服务端健康」长格式时序采样:一行 = 某资源某指标在某分钟桶的取值。
// 长格式(resource × metric)适配实例/数据库/负载均衡/主机的不同指标集,新增指标无需改表。
// 来源两类:AWS Lightsail 指标接口(rtype=instance/database/lb,采样器拉)、各节点主机 agent
// 推送(rtype=host,POST /internal/host)。复合主键使重复写入幂等(同键覆盖)。
// 存储单位已归一(见 infra.go 注释):内存/swap=MB、存储=GB、网络=KB/s、CPU/突发=%、其余原值。
type InfraSample struct {
	BucketTs int64  `gorm:"primaryKey;autoIncrement:false;index:idx_infra_bucket"`
	Resource string `gorm:"primaryKey;size:128"` // 资源名,如 Database-NexusAPI
	RType    string `gorm:"primaryKey;size:16;column:rtype"`
	Metric   string `gorm:"primaryKey;size:48"`
	Value    float64
}

// 直方图档位上界。lat 单位秒,ttft 单位毫秒。
var (
	latEdges  = []int{1, 2, 5, 10, 30, 60}
	ttftEdges = []int{500, 1000, 2000, 5000, 10000}
)

// openStore 打开本地采样库并迁移表结构。
func (m *Monitor) openStore(path string) error {
	// busy_timeout:采样写入与页面读取并发时,等锁而非立刻报 SQLITE_BUSY;WAL:提升读写并发。
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return fmt.Errorf("打开本地采样库失败: %w", err)
	}
	if err := db.AutoMigrate(&MetricSample{}, &TokenSample{}, &HourSample{}, &ChannelSnap{}, &RejectionSample{}, &SelectablePair{}, &InfraSample{}, &AlertConfig{}, &AlertLog{}, &TrackedUser{}, &CustomerGroup{}); err != nil {
		return fmt.Errorf("表迁移失败: %w", err)
	}
	m.storeDB = db
	slog.Info("本地采样库就绪", "path", path)
	return nil
}

func (m *Monitor) upsertSamples(rows []MetricSample) error {
	if len(rows) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "bucket_ts"}, {Name: "channel_id"}, {Name: "model_name"}, {Name: "grp"},
		},
		UpdateAll: true,
	}).CreateInBatches(rows, 200).Error
}

func (m *Monitor) upsertTokenSamples(rows []TokenSample) error {
	if len(rows) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "bucket_ts"}, {Name: "token_name"}},
		UpdateAll: true,
	}).CreateInBatches(rows, 200).Error
}

// nextEnabledSince 计算渠道本轮的 enabled_since:
//   - 禁用 → 0;
//   - 上轮也启用 → 保持原值(含 0:0 表示"自始启用、算全量历史"——升级首刷时既有启用渠道保持 0,
//     不因一次监控部署把所有渠道的稳定性历史清掉);
//   - 新建 / 从禁用重新启用 → 记为 now(从启用时刻起算)。
func nextEnabledSince(status, prevStatus int, prevSince, now int64) int64 {
	if status != 1 {
		return 0
	}
	if prevStatus == 1 {
		return prevSince
	}
	return now
}

// chanPrev 是某渠道上一轮的状态与启用起始时刻,供刷新时判断"禁用→启用"跳变。
type chanPrev struct {
	status int
	since  int64
}

// channelEnabledState 返回当前 channel_snaps 里每个渠道的上一轮状态,
// 供刷新时正确维护 enabled_since(渠道不存在时取零值,即按"新建"处理)。
func (m *Monitor) channelEnabledState() map[int]chanPrev {
	var rows []struct {
		ID, Status   int
		EnabledSince int64
	}
	m.storeDB.Raw("SELECT id, status, enabled_since FROM channel_snaps").Scan(&rows)
	out := make(map[int]chanPrev, len(rows))
	for _, r := range rows {
		out[r.ID] = chanPrev{status: r.Status, since: r.EnabledSince}
	}
	return out
}

// replaceChannelSnaps 用本轮读到的渠道快照覆盖本地表:幂等 UPSERT + 删除本轮未出现的(已删渠道)。
func (m *Monitor) replaceChannelSnaps(rows []ChannelSnap, now int64) error {
	if len(rows) == 0 {
		return nil
	}
	if err := m.storeDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "id"}},
		UpdateAll: true,
	}).CreateInBatches(rows, 200).Error; err != nil {
		return err
	}
	return m.storeDB.Where("updated_at < ?", now).Delete(&ChannelSnap{}).Error
}

// replaceSelectablePairs 全量替换可选 (分组,模型) 对表(数量不大,清空+批量插简单可靠)。
func (m *Monitor) replaceSelectablePairs(pairs []SelectablePair) error {
	if err := m.storeDB.Where("1 = 1").Delete(&SelectablePair{}).Error; err != nil {
		return err
	}
	if len(pairs) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{DoNothing: true}).CreateInBatches(pairs, 300).Error
}

func (m *Monitor) pruneOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&MetricSample{})
	m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&TokenSample{}) // token 维度一并清理
	return r.RowsAffected, r.Error
}

// upsertRejections 累加写入采集器推来的拒绝计数(同键累加,重复/分批推送幂等)。
func (m *Monitor) upsertRejections(rows []RejectionSample) error {
	if len(rows) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "bucket_ts"}, {Name: "node"}, {Name: "reason"}, {Name: "model"}, {Name: "grp"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"count": gorm.Expr("rejection_samples.count + excluded.count"),
		}),
	}).CreateInBatches(rows, 200).Error
}

// storeRejections 取窗口内按 (原因 × 模型 × 分组) 聚合的拒绝计数,按次数降序(Top 100)。
func (m *Monitor) storeRejections(since int64) []RejectionRow {
	var rows []RejectionRow
	m.storeDB.Raw(`SELECT reason, model, grp AS `+"`group`"+`, COALESCE(SUM(count),0) AS count
		FROM rejection_samples WHERE bucket_ts >= ?
		GROUP BY reason, model, grp ORDER BY count DESC LIMIT 100`, since).Scan(&rows)
	return rows
}

func (m *Monitor) pruneRejectionsOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&RejectionSample{})
	return r.RowsAffected, r.Error
}

// aggRow 承接聚合结果。
type aggRow struct {
	K          string
	Success    int64
	Anomaly    int64
	Failed     int64
	SumUseTime int64
	MaxUseTime int
	Tokens     int64
	Quota      int64
	Err4xx     int64 `gorm:"column:err_4xx"`
	Err5xx     int64 `gorm:"column:err_5xx"`
	ErrTimeout int64
	ErrOther   int64
	Lat1       int64 `gorm:"column:lat_1"`
	Lat2       int64 `gorm:"column:lat_2"`
	Lat5       int64 `gorm:"column:lat_5"`
	Lat10      int64 `gorm:"column:lat_10"`
	Lat30      int64 `gorm:"column:lat_30"`
	Lat60      int64 `gorm:"column:lat_60"`
	LatInf     int64 `gorm:"column:lat_inf"`

	CompletionTokens int64 `gorm:"column:completion_tokens"`
	Ttft500          int64 `gorm:"column:ttft_500"`
	Ttft1k           int64 `gorm:"column:ttft_1k"`
	Ttft2k           int64 `gorm:"column:ttft_2k"`
	Ttft5k           int64 `gorm:"column:ttft_5k"`
	Ttft10k          int64 `gorm:"column:ttft_10k"`
	TtftInf          int64 `gorm:"column:ttft_inf"`
	TtftMaxMs        int   `gorm:"column:ttft_max_ms"`
}

const aggCols = `
  COALESCE(SUM(success),0)      AS success,
  COALESCE(SUM(anomaly),0)      AS anomaly,
  COALESCE(SUM(failed),0)       AS failed,
  COALESCE(SUM(sum_use_time),0) AS sum_use_time,
  COALESCE(MAX(max_use_time),0) AS max_use_time,
  COALESCE(SUM(tokens),0)       AS tokens,
  COALESCE(SUM(quota),0)        AS quota,
  COALESCE(SUM(err_4xx),0)      AS err_4xx,
  COALESCE(SUM(err_5xx),0)      AS err_5xx,
  COALESCE(SUM(err_timeout),0)  AS err_timeout,
  COALESCE(SUM(err_other),0)    AS err_other,
  COALESCE(SUM(lat_1),0)  AS lat_1,  COALESCE(SUM(lat_2),0)  AS lat_2,  COALESCE(SUM(lat_5),0)  AS lat_5,
  COALESCE(SUM(lat_10),0) AS lat_10, COALESCE(SUM(lat_30),0) AS lat_30, COALESCE(SUM(lat_60),0) AS lat_60,
  COALESCE(SUM(lat_inf),0) AS lat_inf,
  COALESCE(SUM(completion_tokens),0) AS completion_tokens,
  COALESCE(SUM(ttft_500),0) AS ttft_500, COALESCE(SUM(ttft_1k),0) AS ttft_1k, COALESCE(SUM(ttft_2k),0) AS ttft_2k,
  COALESCE(SUM(ttft_5k),0) AS ttft_5k, COALESCE(SUM(ttft_10k),0) AS ttft_10k, COALESCE(SUM(ttft_inf),0) AS ttft_inf,
  COALESCE(MAX(ttft_max_ms),0) AS ttft_max_ms`

func (a aggRow) fill(r *Row, windowSec float64) {
	typ2 := a.Success + a.Anomaly // 所有计费请求(干净成功 + 异常)
	total := typ2 + a.Failed
	r.Total, r.Success, r.Anomaly, r.Failed = total, a.Success, a.Anomaly, a.Failed
	r.SuccessRate = rate(a.Success, total) // 干净成功率(异常、错误都不算成功)
	r.AnomalyRate = rate(a.Anomaly, total)
	r.ErrorRate = rate(a.Failed, total)
	r.QPS = float64(total) / windowSec
	if typ2 > 0 {
		r.AvgLatency = float64(a.SumUseTime) / float64(typ2) // 延迟覆盖全部 type=2(含异常的慢请求)
	}
	r.MaxLatency = a.MaxUseTime
	r.Tokens = a.Tokens
	r.CostUSD = float64(a.Quota) / quotaPerUSD
	r.Err4xx, r.Err5xx, r.ErrTimeout, r.ErrOther = a.Err4xx, a.Err5xx, a.ErrTimeout, a.ErrOther
	lat := []int64{a.Lat1, a.Lat2, a.Lat5, a.Lat10, a.Lat30, a.Lat60, a.LatInf}
	r.P50 = percentile(lat, latEdges, a.MaxUseTime, 50)
	r.P95 = percentile(lat, latEdges, a.MaxUseTime, 95)
	r.P99 = percentile(lat, latEdges, a.MaxUseTime, 99)

	// 出字速度 tok/s = 输出token之和 / 成功耗时之和
	if a.SumUseTime > 0 {
		r.TokPerSec = float64(a.CompletionTokens) / float64(a.SumUseTime)
	}
	// TTFT 首字延迟(直方图单位 ms,展示转秒)
	ttft := []int64{a.Ttft500, a.Ttft1k, a.Ttft2k, a.Ttft5k, a.Ttft10k, a.TtftInf}
	r.TtftP50 = percentile(ttft, ttftEdges, a.TtftMaxMs, 50) / 1000
	r.TtftP95 = percentile(ttft, ttftEdges, a.TtftMaxMs, 95) / 1000

	// 健康由【错误(type=5)】驱动——错误是重点,每条都关注。
	// 异常(client_gone 等)不在此驱动色标;其"成簇"判定在 GetSnapshot 里按时间序列另行升级为关注。
	r.Health = health(total, rate(typ2, total)) // 非错误率 = (成功+异常)/总
}

// percentile 从直方图近似分位数。hist 各档非累计计数,档上界为 edges(长度比 hist 少 1),
// 末档以观测到的 maxVal 收尾;桶内线性插值。单位由调用方决定(秒或毫秒)。
func percentile(hist []int64, edges []int, maxVal int, p float64) float64 {
	var total int64
	for _, c := range hist {
		total += c
	}
	if total == 0 {
		return 0
	}
	target := p / 100 * float64(total)
	var cum, lower float64
	for i, c := range hist {
		upper := float64(maxVal)
		if i < len(edges) {
			upper = float64(edges[i])
		}
		if upper < lower {
			upper = lower
		}
		if cum+float64(c) >= target {
			if c == 0 {
				return upper
			}
			return lower + (target-cum)/float64(c)*(upper-lower)
		}
		cum += float64(c)
		lower = upper
	}
	return lower
}

func (m *Monitor) storeSummary(since int64, windowSec float64) (*Summary, error) {
	var a aggRow
	if err := m.storeDB.Raw(`SELECT '' AS k, `+aggCols+` FROM metric_samples WHERE bucket_ts >= ?`+enabledChanFilter+selectableFilter, since).
		Scan(&a).Error; err != nil {
		return nil, fmt.Errorf("本地汇总失败: %w", err)
	}
	var r Row
	a.fill(&r, windowSec)
	return &Summary{
		Total: r.Total, Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed,
		SuccessRate: r.SuccessRate, AnomalyRate: r.AnomalyRate, ErrorRate: r.ErrorRate,
		QPS: r.QPS, AvgLatency: r.AvgLatency, MaxLatency: r.MaxLatency,
		P50: r.P50, P95: r.P95, P99: r.P99,
		TtftP50: r.TtftP50, TtftP95: r.TtftP95, TokPerSec: r.TokPerSec,
		Tokens: r.Tokens, CostUSD: r.CostUSD,
		Err4xx: r.Err4xx, Err5xx: r.Err5xx, ErrTimeout: r.ErrTimeout, ErrOther: r.ErrOther,
		LatHist:  []int64{a.Lat1, a.Lat2, a.Lat5, a.Lat10, a.Lat30, a.Lat60, a.LatInf},
		TtftHist: []int64{a.Ttft500, a.Ttft1k, a.Ttft2k, a.Ttft5k, a.Ttft10k, a.TtftInf},
	}, nil
}

// storeDimSeries 取每个维度取值的分钟桶时间序列(成功/失败),供前端画迷你趋势(sparkline)。
// 同样在 Go 内粗化,点数受控;返回 key -> 时序。
func (m *Monitor) storeDimSeries(dimCol string, since int64, windowMinutes int) (map[string][]TimePoint, error) {
	type row struct {
		K        string
		BucketTs int64
		Success  int64
		Anomaly  int64
		Failed   int64
	}
	f := enabledChanFilter + selectableFilter
	if dimCol == channelDim { // 按渠道明细不过滤,排障仍能看禁用渠道/误路由
		f = ""
	}
	q := fmt.Sprintf(`SELECT %s AS k, bucket_ts, COALESCE(SUM(success),0) AS success,
		COALESCE(SUM(anomaly),0) AS anomaly, COALESCE(SUM(failed),0) AS failed
		FROM metric_samples WHERE bucket_ts >= ?%s GROUP BY k, bucket_ts ORDER BY k, bucket_ts`, dimCol, f)
	var rows []row
	if err := m.storeDB.Raw(q, since).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("本地维度时序失败(%s): %w", dimCol, err)
	}
	bucketSec := int64(60)
	if windowMinutes > 60 {
		bucketSec = int64(windowMinutes) / 60 * 60
	}
	out := map[string][]TimePoint{}
	idx := map[string]map[int64]int{} // key -> bucket -> 下标
	for _, mr := range rows {
		key := mr.K
		if key == "" {
			key = "(空)"
		}
		b := (mr.BucketTs / bucketSec) * bucketSec
		if idx[key] == nil {
			idx[key] = map[int64]int{}
		}
		if i, ok := idx[key][b]; ok {
			out[key][i].Success += mr.Success
			out[key][i].Anomaly += mr.Anomaly
			out[key][i].Failed += mr.Failed
		} else {
			idx[key][b] = len(out[key])
			out[key] = append(out[key], TimePoint{Ts: b, Success: mr.Success, Anomaly: mr.Anomaly, Failed: mr.Failed})
		}
	}
	return out, nil
}

func (m *Monitor) storeDim(dimCol string, since int64, windowSec float64) ([]Row, error) {
	f := enabledChanFilter + selectableFilter
	if dimCol == channelDim { // 按渠道明细不过滤,排障仍能看禁用渠道/误路由
		f = ""
	}
	q := fmt.Sprintf(`SELECT %s AS k, %s FROM metric_samples
		WHERE bucket_ts >= ?%s GROUP BY %s
		ORDER BY failed DESC, (success+failed) DESC LIMIT 200`, dimCol, aggCols, f, dimCol)
	var rows []aggRow
	if err := m.storeDB.Raw(q, since).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("本地维度聚合失败(%s): %w", dimCol, err)
	}
	out := make([]Row, 0, len(rows))
	for _, a := range rows {
		key := a.K
		if key == "" {
			key = "(空)"
		}
		r := Row{Key: key, Label: key}
		a.fill(&r, windowSec)
		out = append(out, r)
	}
	return out, nil
}

func (m *Monitor) storeTrend(since int64, windowMinutes int) ([]TimePoint, error) {
	type minRow struct {
		BucketTs int64
		Success  int64
		Failed   int64
	}
	var rows []minRow
	if err := m.storeDB.Raw(`SELECT bucket_ts, COALESCE(SUM(success),0) AS success, COALESCE(SUM(failed),0) AS failed
		FROM metric_samples WHERE bucket_ts >= ?`+enabledChanFilter+selectableFilter+` GROUP BY bucket_ts ORDER BY bucket_ts`, since).
		Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("本地趋势失败: %w", err)
	}
	bucketSec := int64(60)
	if windowMinutes > 60 {
		bucketSec = int64(windowMinutes) / 60 * 60
	}
	agg := map[int64]*TimePoint{}
	var order []int64
	for _, mr := range rows {
		b := (mr.BucketTs / bucketSec) * bucketSec
		p := agg[b]
		if p == nil {
			p = &TimePoint{Ts: b}
			agg[b] = p
			order = append(order, b)
		}
		p.Success += mr.Success
		p.Failed += mr.Failed
	}
	out := make([]TimePoint, 0, len(order))
	for _, b := range order {
		out = append(out, *agg[b])
	}
	return out, nil
}

// storeTokens 按令牌(API Key)聚合窗口内的成功/异常/失败/用量/成本,按 错误数→请求数 降序取 Top 100。
func (m *Monitor) storeTokens(since int64, windowSec float64) ([]TokenRow, error) {
	type tr struct {
		K       string
		Success int64
		Anomaly int64
		Failed  int64
		Tokens  int64
		Quota   int64
	}
	var rows []tr
	if err := m.storeDB.Raw(`SELECT token_name AS k,
		COALESCE(SUM(success),0) AS success, COALESCE(SUM(anomaly),0) AS anomaly,
		COALESCE(SUM(failed),0) AS failed, COALESCE(SUM(tokens),0) AS tokens, COALESCE(SUM(quota),0) AS quota
		FROM token_samples WHERE bucket_ts >= ? GROUP BY token_name
		ORDER BY failed DESC, (success+failed) DESC LIMIT 100`, since).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("本地 token 聚合失败: %w", err)
	}
	out := make([]TokenRow, 0, len(rows))
	for _, a := range rows {
		key := a.K
		if key == "" {
			key = "(无令牌名)"
		}
		total := a.Success + a.Anomaly + a.Failed
		out = append(out, TokenRow{
			Key: key, Total: total, Success: a.Success, Anomaly: a.Anomaly, Failed: a.Failed,
			SuccessRate: rate(a.Success, total), AnomalyRate: rate(a.Anomaly, total), ErrorRate: rate(a.Failed, total),
			QPS: float64(total) / windowSec, Tokens: a.Tokens, CostUSD: float64(a.Quota) / quotaPerUSD,
			Health: health(total, rate(a.Success+a.Anomaly, total)),
		})
	}
	return out, nil
}

// rollupHours 把【还有分钟数据的近段时间】按小时汇总进 hour_samples(幂等 UPSERT)。
// 关键:在分钟数据被清理前就已滚动写入小时表,故长期数据不丢失。
func (m *Monitor) rollupHours(sinceTs int64) error {
	return m.storeDB.Exec(`INSERT INTO hour_samples (hour_ts, success, anomaly, failed, tokens, quota, sum_use_time)
		SELECT (bucket_ts/3600)*3600 AS hour_ts,
		  SUM(success), SUM(anomaly), SUM(failed), SUM(tokens), SUM(quota), SUM(sum_use_time)
		FROM metric_samples WHERE bucket_ts >= ?
		GROUP BY hour_ts
		ON CONFLICT(hour_ts) DO UPDATE SET
		  success=excluded.success, anomaly=excluded.anomaly, failed=excluded.failed,
		  tokens=excluded.tokens, quota=excluded.quota, sum_use_time=excluded.sum_use_time`, sinceTs).Error
}

func (m *Monitor) pruneHoursOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("hour_ts < ?", cutoffTs).Delete(&HourSample{})
	return r.RowsAffected, r.Error
}

// storeHourSeries 取小时级序列(长期趋势图用),按时间升序。
func (m *Monitor) storeHourSeries(sinceTs int64) []HourPoint {
	var pts []HourPoint
	m.storeDB.Raw(`SELECT hour_ts AS ts, success, anomaly, failed FROM hour_samples WHERE hour_ts >= ? ORDER BY hour_ts`, sinceTs).Scan(&pts)
	return pts
}

// periodStat 取 [fromTs,toTs) 的小时级汇总统计(同比环比用)。
func (m *Monitor) periodStat(fromTs, toTs int64) PeriodStat {
	var r struct{ S, A, F, Q int64 }
	m.storeDB.Raw(`SELECT COALESCE(SUM(success),0) s, COALESCE(SUM(anomaly),0) a, COALESCE(SUM(failed),0) f, COALESCE(SUM(quota),0) q
		FROM hour_samples WHERE hour_ts >= ? AND hour_ts < ?`, fromTs, toTs).Scan(&r)
	total := r.S + r.A + r.F
	return PeriodStat{Total: total, Failed: r.F, SuccessRate: rate(r.S, total), CostUSD: float64(r.Q) / quotaPerUSD}
}

// storeCompare 同比环比:近 24h vs 前 24h(环比) vs 上周同期(同比),取小时表(7 天前也有数据)。
func (m *Monitor) storeCompare(nowUnix int64) CompareStat {
	const h = int64(3600)
	end := nowUnix / h * h // 对齐整点;小时表只含已完成的小时
	return CompareStat{
		Now:      m.periodStat(end-24*h, end),
		Prev:     m.periodStat(end-48*h, end-24*h),
		LastWeek: m.periodStat(end-192*h, end-168*h),
	}
}

func (m *Monitor) storeFreshness() (lastBucket int64) {
	var v struct{ M int64 }
	m.storeDB.Raw(`SELECT COALESCE(MAX(bucket_ts),0) AS m FROM metric_samples`).Scan(&v)
	return v.M
}

// ---- 服务端健康(infra)存储 ----

// upsertInfra 幂等写入一批 infra 采样(同键覆盖)。
func (m *Monitor) upsertInfra(rows []InfraSample) error {
	if len(rows) == 0 {
		return nil
	}
	return m.storeDB.Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "bucket_ts"}, {Name: "resource"}, {Name: "rtype"}, {Name: "metric"}},
		UpdateAll: true,
	}).CreateInBatches(rows, 200).Error
}

// infraLatestRow 是某资源某指标的最新取值(含其桶时刻,用于算数据新鲜度)。
type infraLatestRow struct {
	Resource string
	RType    string `gorm:"column:rtype"`
	Metric   string
	Value    float64
	BucketTs int64
}

// storeInfraLatest 返回每个 (资源,指标) 的最新一条取值。
func (m *Monitor) storeInfraLatest() []infraLatestRow {
	var rows []infraLatestRow
	// 取每个 (resource,metric) 的最大 bucket_ts 对应行。
	m.storeDB.Raw(`SELECT s.resource, s.rtype, s.metric, s.value, s.bucket_ts
		FROM infra_samples s
		JOIN (SELECT resource, metric, MAX(bucket_ts) AS mx FROM infra_samples GROUP BY resource, metric) t
		  ON s.resource=t.resource AND s.metric=t.metric AND s.bucket_ts=t.mx`).Scan(&rows)
	return rows
}

// InfraPoint 是 infra 指标的一个时间点(供趋势小图)。
type InfraPoint struct {
	Ts    int64   `json:"ts"`
	Value float64 `json:"value"`
}

// storeInfraSeries 返回某资源某指标自 since 起的时序(升序),供趋势小图(如 DB 内存/swap)。
func (m *Monitor) storeInfraSeries(resource, metric string, since int64) []InfraPoint {
	var pts []InfraPoint
	m.storeDB.Raw(`SELECT bucket_ts AS ts, value FROM infra_samples
		WHERE resource=? AND metric=? AND bucket_ts >= ? ORDER BY bucket_ts`, resource, metric, since).Scan(&pts)
	return pts
}

func (m *Monitor) pruneInfraOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&InfraSample{})
	return r.RowsAffected, r.Error
}
