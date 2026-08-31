package monitor

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/glebarez/sqlite"
	"github.com/yl0711-coder/newapi-monitor/internal/trafficclass"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"
	"gorm.io/gorm/logger"
)

// store.go:监控自带的本地采样库(独立 sqlite,不碰 new-api 的库)。
// 页面只读这里;唯一写入者是采样器。所有存储函数挂在 Monitor 上,用 m.storeDB。

// userTrafficClassificationVersion 标记“已经排除渠道内部测试”的聚合口径。
// v2 同时识别旧版批量/定时测试产生的无 request_id 错误日志。旧版本数据可能
// 混有测试流量，读取时必须 fail-closed，等待后台按小时重算；不能把旧聚合继续
// 伪装成用户流量。
const userTrafficClassificationVersion = trafficclass.Current

var (
	currentMetricTrafficFilter = fmt.Sprintf(" AND metric_samples.traffic_class_version = %d", userTrafficClassificationVersion)
	currentTokenTrafficFilter  = fmt.Sprintf(" AND token_samples.traffic_class_version = %d", userTrafficClassificationVersion)
	currentHourTrafficFilter   = fmt.Sprintf(" AND hour_samples.traffic_class_version = %d", userTrafficClassificationVersion)
)

// warnReadErr:读路径统一 fail-open——出错按"无数据"返回,页面显示空而不中断监控;
// 但必须留痕,否则真实的库损坏/schema 漂移会被静默成"没数据",误导排障。
func warnReadErr(op string, tx *gorm.DB) {
	if tx.Error != nil {
		slog.Warn("本地库读取失败(按无数据处理)", "op", op, "err", tx.Error)
	}
}

// MetricSample 按【分钟桶 × 渠道 × 模型 × 分组】聚合的一行采样。
// 复合主键使采样可幂等 UPSERT,自愈小的采集间隙。
type MetricSample struct {
	BucketTs            int64  `gorm:"primaryKey;autoIncrement:false;index:idx_metric_bucket"`
	ChannelID           int    `gorm:"primaryKey;autoIncrement:false"`
	ModelName           string `gorm:"primaryKey;size:128"`
	Grp                 string `gorm:"primaryKey;size:64;column:grp"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`

	Success int64 // 干净成功(type=2 且已交付:completion_tokens>0 且流正常结束)
	// Anomaly 交付异常(B 类):上游没明确拒绝,但用户没拿到东西。判据见 sampler.go 的 ANOM。
	// 列名保留 anomaly(历史兼容:sparkline/告警/看板多处引用),界面显示为「交付异常」。
	Anomaly int64
	Failed  int64 // 错误(type=5,上游返回的错误)
	// 交付异常明细(互斥,三者之和 = Anomaly)
	AnomalyBilled int64 `gorm:"column:anomaly_billed"` // B1 零输出且已扣费——唯一直接产生对客损失的一类
	AnomalyFree   int64 `gorm:"column:anomaly_free"`   // B2 零输出未扣费(上游连 usage 都没返回)
	AnomalyStream int64 `gorm:"column:anomaly_stream"` // B3/B4 流异常结束或流内错误(可能已产出部分内容)
	// 严重度补充:B1 已扣配额之和(算金额)、异常请求耗时之和(算平均等待)
	AnomalyQuota   int64 `gorm:"column:anomaly_quota"`
	AnomalySumTime int64 `gorm:"column:anomaly_sum_time"`
	SumUseTime     int64
	MaxUseTime     int
	Tokens         int64
	Quota          int64
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

func (s *MetricSample) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// TokenSample 按【分钟桶 × 令牌(API Key)】聚合,用于"谁在制造错误 / 烧配额"维度。
// 故意比 MetricSample 轻(不交叉渠道/模型)以控制基数。
type TokenSample struct {
	BucketTs            int64  `gorm:"primaryKey;autoIncrement:false;index"`
	TokenName           string `gorm:"primaryKey;size:128;column:token_name"`
	Success             int64
	Anomaly             int64
	Failed              int64
	Tokens              int64
	Quota               int64
	TrafficClassVersion int `gorm:"column:traffic_class_version;index"`
}

func (s *TokenSample) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// HourSample 小时级汇总(rollup):每小时一行总览,长期留存(默认 90 天),支撑长期趋势 + 同比环比。
// 由分钟级 metric_samples 周期性汇总而来;存储只随时间增长(每小时 1 行),与请求量无关。
type HourSample struct {
	HourTs              int64 `gorm:"primaryKey;autoIncrement:false"`
	Success             int64
	Anomaly             int64
	Failed              int64
	Tokens              int64
	Quota               int64
	SumUseTime          int64
	TrafficClassVersion int `gorm:"column:traffic_class_version;index"`
}

func (s *HourSample) BeforeCreate(_ *gorm.DB) error {
	if s.TrafficClassVersion == 0 {
		s.TrafficClassVersion = userTrafficClassificationVersion
	}
	return nil
}

// ChannelSnap 渠道健康快照:采样器周期性从生产 channels 表读入(id/状态/分组/模型),
// 供对外看板(public 包)派生"某线路×模型有无可用渠道"。仅存路由与状态,【无任何密钥】。
type ChannelSnap struct {
	ID           int    `gorm:"primaryKey;autoIncrement:false"`
	Name         string `gorm:"size:192"` // 展示名；只读同步，不含 key 等敏感字段
	Type         int    // NewAPI 官方渠道类型；只用于客观展示渠道厂商/类型，不按名称猜测
	Vendor       string `gorm:"size:64"`                           // 由 Type 按 NewAPI 官方 ChannelTypeNames 映射
	BaseDomain   string `gorm:"size:253;column:base_domain;index"` // 仅保存 base_url 的可注册主域名；不保存完整 URL/路径/凭据
	BaseHost     string `gorm:"size:253;column:base_host;index"`   // 仅保存脱敏主机名；用于核对自动归并依据
	Status       int    // new-api: 1启用 / 2手动禁用 / 3自动禁用
	Groups       string `gorm:"size:512"`  // 逗号分隔分组
	Models       string `gorm:"type:text"` // 逗号分隔模型
	EnabledSince int64  // 当前这段"启用"的起始 Unix 秒;禁用=0;0 也表示"自始启用"(算全量历史);重启用刷新为重启用时刻
	UpdatedAt    int64  `gorm:"index"`
	// DeletedAt 只表示该渠道已不在 NewAPI 当前渠道表中。删除后的最后快照必须保留，
	// 供稳定性和渠道用量历史继续展示名称、厂商与主域名；0 表示当前仍存在。
	DeletedAt int64 `gorm:"column:deleted_at;index"`
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
// 是 logs 维度监控的盲区,这里单列。样本按不同批次累加；同一批次重试由
// RejectionIngestBatch 台账去重，不能仅靠样本复合主键判断重试。
type RejectionSample struct {
	BucketTs int64  `gorm:"primaryKey;autoIncrement:false;index"`
	Node     string `gorm:"primaryKey;size:64"`  // 来源节点(master/slave)
	Reason   string `gorm:"primaryKey;size:64"`  // no_available_channel 等
	Model    string `gorm:"primaryKey;size:128"` // 被拒模型
	Grp      string `gorm:"primaryKey;size:64;column:grp"`
	Count    int64
}

// RejectionIngestBatch 是前置拒绝采集的幂等台账。同一节点的同一批次只会
// 累加一次；PayloadHash 用于发现采集器错误复用 batch_id，而不是静默吞数据。
type RejectionIngestBatch struct {
	Node        string `gorm:"primaryKey;size:64"`
	BatchID     string `gorm:"primaryKey;size:64;column:batch_id"`
	PayloadHash string `gorm:"size:64;column:payload_hash"`
	Rows        int
	TotalCount  int64 `gorm:"column:total_count"`
	ReceivedAt  int64 `gorm:"index;column:received_at"`
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

// HostContainerSnapshot 保存 HostAgent 显式白名单内的当前容器状态。
// 这是覆盖式快照而非高频历史，避免本地库随时间增长；不含任何 Docker 敏感配置。
type HostContainerSnapshot struct {
	Node         string `gorm:"primaryKey;size:128"`
	Name         string `gorm:"primaryKey;size:128"`
	State        string `gorm:"size:16"`
	Health       string `gorm:"size:16"`
	RestartCount int
	LastSeen     int64 `gorm:"index"`
}

// NginxMinuteSample 是采集器在节点侧先脱敏、再按分钟聚合后的入口事实。
// 主键维度均为有限集合，存储量与请求量解耦；表内不存在请求级原文或身份信息。
type NginxMinuteSample struct {
	BucketTs          int64  `gorm:"primaryKey;autoIncrement:false;index:idx_nginx_minute_bucket"`
	Node              string `gorm:"primaryKey;size:64"`
	Route             string `gorm:"primaryKey;size:48"`
	Method            string `gorm:"primaryKey;size:8"`
	Status            int    `gorm:"primaryKey;autoIncrement:false"`
	UpstreamStatus    int    `gorm:"primaryKey;autoIncrement:false"`
	Count             int64
	RequestTimeSumMS  int64
	RequestTimeMaxMS  int64
	UpstreamTimeSumMS int64
	UpstreamTimeCount int64
	BytesSent         int64
	RequestIDPresent  int64
}

// NginxIngestBatch 使采集器断线重试保持幂等，避免同一批次重复累计。
type NginxIngestBatch struct {
	Node        string `gorm:"primaryKey;size:64"`
	BatchID     string `gorm:"primaryKey;size:64"`
	PayloadHash string `gorm:"size:64;not null"`
	FirstTs     int64
	LastTs      int64
	Rows        int
	ReceivedAt  int64 `gorm:"index"`
}

// NginxSourceState 是入口数据源健康状态；只保留每节点最后进度，不随时间增长。
type NginxSourceState struct {
	Node                      string `gorm:"primaryKey;size:64"`
	LastEventTs               int64
	LastIngestTs              int64
	LastBatchID               string `gorm:"size:64"`
	AcceptedRows              int64
	AcceptedCount             int64
	BacklogBytes              int64
	BacklogKnown              bool
	CursorDiscontinuities     int64
	LastCursorDiscontinuityAt int64
	DiscardedLines            int64
	LastDiscardedAt           int64
}

// UsageHourFact 是用户用量的本地小时事实。它只保存消费/退款的聚合数字，
// 不保存原始日志、请求内容、Key 或错误详情；既能把页面读取从生产 logs 表移开，
// 也为后续按渠道核对成本保留必要的 channel_id 维度。
//
// DayTs 是该小时所属的 CST 自然日零点。完整自然日会再汇总成 UsageDailyFact；
// 未完成的当天/缺口日仍直接从小时事实读取，因此不会把缺口错误显示成零流量。
type UsageHourFact struct {
	HourTs    int64  `gorm:"primaryKey;autoIncrement:false;index:idx_usage_hour_fact_hour_user,priority:1"`
	UserID    int64  `gorm:"primaryKey;autoIncrement:false;index:idx_usage_hour_fact_hour_user,priority:2"`
	ChannelID int64  `gorm:"primaryKey;autoIncrement:false"`
	Grp       string `gorm:"primaryKey;size:128;column:grp"`
	ModelName string `gorm:"primaryKey;size:255;column:model_name"`
	TokenID   int64  `gorm:"primaryKey;autoIncrement:false"`
	DayTs     int64  `gorm:"index:idx_usage_hour_fact_day;column:day_ts"`
	TokenName string `gorm:"size:255;column:token_name"` // 日志已有的展示名；不含 token key

	Requests         int64 `gorm:"column:requests"`
	RefundRecords    int64 `gorm:"column:refund_records"`
	PromptTokens     int64 `gorm:"column:prompt_tokens"`
	CompletionTokens int64 `gorm:"column:completion_tokens"`
	ConsumeQuota     int64 `gorm:"column:consume_quota"`
	RefundQuota      int64 `gorm:"column:refund_quota"`
}

// UsageDailyFact 是已完整采到的 CST 自然日聚合。查询优先走它，只有尚未闭合的
// 当天或存在采集缺口的日期才回退读取该日的小时事实，兼顾历史查询性能与数据诚实性。
type UsageDailyFact struct {
	DateTs    int64  `gorm:"primaryKey;autoIncrement:false;index:idx_usage_daily_fact_date_user,priority:1"`
	UserID    int64  `gorm:"primaryKey;autoIncrement:false;index:idx_usage_daily_fact_date_user,priority:2"`
	ChannelID int64  `gorm:"primaryKey;autoIncrement:false"`
	Grp       string `gorm:"primaryKey;size:128;column:grp"`
	ModelName string `gorm:"primaryKey;size:255;column:model_name"`
	TokenID   int64  `gorm:"primaryKey;autoIncrement:false"`
	TokenName string `gorm:"size:255;column:token_name"`

	Requests         int64 `gorm:"column:requests"`
	RefundRecords    int64 `gorm:"column:refund_records"`
	PromptTokens     int64 `gorm:"column:prompt_tokens"`
	CompletionTokens int64 `gorm:"column:completion_tokens"`
	ConsumeQuota     int64 `gorm:"column:consume_quota"`
	RefundQuota      int64 `gorm:"column:refund_quota"`
}

// UsageFactMemberDayState 是日事实的内容证明。小时明细默认只保留近期 8 天，
// 更早历史无法再用 24 小时明细现场重算；因此每次日事实从已核验小时原子重建时，
// 同事务保存成员×自然日的稳定内容指纹。发布前和周期语义审计会重新读取日事实
// 与该指纹比对，防止 quick_check 正常但业务行被误删/误迁移后仍继续提供错误数据。
// 空流量日同样保存空集合指纹，能区分“确实为零”和“从未构建”。
type UsageFactMemberDayState struct {
	UserID                int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	DateTs                int64  `gorm:"primaryKey;autoIncrement:false;index:idx_usage_fact_member_day_date,priority:1;column:date_ts"`
	Status                string `gorm:"size:16;index:idx_usage_fact_member_day_status;column:status"`
	Rows                  int    `gorm:"column:rows"` // legacy fact-row count; retained for rollback compatibility
	SourceRows            int64  `gorm:"column:source_rows"`
	Requests              int64  `gorm:"column:requests"`
	RefundRecords         int64  `gorm:"column:refund_records"`
	Tokens                int64  `gorm:"column:tokens"` // legacy prompt+completion total
	PromptTokens          int64  `gorm:"column:prompt_tokens"`
	CompletionTokens      int64  `gorm:"column:completion_tokens"`
	ConsumeQuota          int64  `gorm:"column:consume_quota"`
	RefundQuota           int64  `gorm:"column:refund_quota"`
	ContentHash           string `gorm:"size:64;column:content_hash"` // legacy alias used by the current semantic audit
	SourceResultHash      string `gorm:"size:64;column:source_result_hash"`
	FactContentHash       string `gorm:"size:64;column:fact_content_hash"`
	ClassificationVersion int    `gorm:"column:classification_version"`
	QuerySemanticsVersion int    `gorm:"column:query_semantics_version"`
	SourceEpoch           string `gorm:"size:64;column:source_epoch"`
	SourceCheckedAt       int64  `gorm:"column:source_checked_at"`
	CompletedAt           int64  `gorm:"column:completed_at"`
	JobID                 string `gorm:"size:80;index;column:job_id"`
	Attempts              int    `gorm:"column:attempts"`
	LastError             string `gorm:"size:512;column:last_error"`
	UpdatedAt             int64  `gorm:"index;column:updated_at"`
}

// UsageHourIngestState 是每个小时的本地采集水位。即使该小时没有任何消费日志，
// 成功读取也会写一条 complete 状态，避免把“零流量”误判为“漏采”。
type UsageHourIngestState struct {
	HourTs      int64  `gorm:"primaryKey;autoIncrement:false;column:hour_ts"`
	Status      string `gorm:"size:16;index:idx_usage_hour_ingest_status;column:status"` // complete / failed / queued
	Rows        int    `gorm:"column:rows"`
	Requests    int64  `gorm:"column:requests"`
	Tokens      int64  `gorm:"column:tokens"`
	ContentHash string `gorm:"size:64;column:content_hash"` // 小时聚合内容指纹，用于低频补漏与幂等校验
	Attempts    int    `gorm:"column:attempts"`
	UpdatedAt   int64  `gorm:"index;column:updated_at"`
	CompletedAt int64  `gorm:"column:completed_at"`
	LastError   string `gorm:"size:512;column:last_error"`
}

// UsageFactMemberState 为每个关注用户维护独立的历史回填游标。成员增删时只会
// 新建/停用对应行，不再清空所有小时台账：新增用户只补自己的历史，已有用户
// 继续服务已发布事实，删除用户则立即从权限交集移除。
type UsageFactMemberState struct {
	UserID              int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	Active              bool   `gorm:"index:idx_usage_fact_member_active_cursor,priority:1;column:active"`
	TrackedRevision     int64  `gorm:"column:tracked_revision"`
	BackfillWindowDays  int    `gorm:"column:backfill_window_days"`
	RangeStart          int64  `gorm:"column:range_start"`
	NextBackfillHour    int64  `gorm:"index:idx_usage_fact_member_active_cursor,priority:2;column:next_backfill_hour"`
	SourceFloorHour     *int64 `gorm:"column:source_floor_hour"`
	SourceFirstLogHour  *int64 `gorm:"column:source_first_log_hour"`
	SourceCeilingHour   *int64 `gorm:"column:source_ceiling_hour"`
	CoverageThroughHour *int64 `gorm:"column:coverage_through_hour"`
	TailThroughHour     *int64 `gorm:"column:tail_through_hour"`
	// LiveThroughHour is an independent, continuous recent-data cursor. It is
	// never inferred from cold coverage and never jumps across a failed hour.
	LiveFromHour      *int64 `gorm:"column:live_from_hour"`
	LiveThroughHour   *int64 `gorm:"index:idx_usage_fact_member_live_cursor;column:live_through_hour"`
	LiveTargetHour    *int64 `gorm:"column:live_target_hour"`
	LiveSpanHours     int    `gorm:"column:live_span_hours"`
	LiveStatus        string `gorm:"size:24;index;column:live_status"`
	LiveAttempts      int    `gorm:"column:live_attempts"`
	LiveNextRetryAt   int64  `gorm:"index;column:live_next_retry_at"`
	LiveLastServedSeq int64  `gorm:"index:idx_usage_fact_member_live_cursor;column:live_last_served_seq"`
	LiveLastServedAt  int64  `gorm:"index:idx_usage_fact_member_live_cursor;column:live_last_served_at"`
	LiveLastSuccessAt int64  `gorm:"column:live_last_success_at"`
	LiveLastFailureAt int64  `gorm:"column:live_last_failure_at"`
	LiveLastError     string `gorm:"size:512;column:live_last_error"`
	// Recent bridge expands the already-fresh live publication backwards to
	// the normal seven-day UI window without making the current Tail wait for
	// deep archive import. It is a second continuous cursor: publication only
	// moves LiveFromHour after the whole bridge has reached its fixed target.
	RecentFromHour        *int64 `gorm:"column:recent_from_hour"`
	RecentThroughHour     *int64 `gorm:"index:idx_usage_fact_member_recent_cursor;column:recent_through_hour"`
	RecentTargetHour      *int64 `gorm:"column:recent_target_hour"`
	RecentSpanHours       int    `gorm:"column:recent_span_hours"`
	RecentStatus          string `gorm:"size:24;index;column:recent_status"`
	RecentAttempts        int    `gorm:"column:recent_attempts"`
	RecentNextRetryAt     int64  `gorm:"index;column:recent_next_retry_at"`
	RecentLastServedSeq   int64  `gorm:"index:idx_usage_fact_member_recent_cursor;column:recent_last_served_seq"`
	RecentLastServedAt    int64  `gorm:"index:idx_usage_fact_member_recent_cursor;column:recent_last_served_at"`
	RecentLastSuccessAt   int64  `gorm:"column:recent_last_success_at"`
	RecentLastFailureAt   int64  `gorm:"column:recent_last_failure_at"`
	RecentLastError       string `gorm:"size:512;column:recent_last_error"`
	VerifyNextHour        *int64 `gorm:"column:verify_next_hour"`
	VerifiedThroughHour   *int64 `gorm:"column:verified_through_hour"`
	VerificationStatus    string `gorm:"size:24;index;column:verification_status"`
	VerifiedAt            int64  `gorm:"column:verified_at"`
	SourceFloorCheckedAt  int64  `gorm:"column:source_floor_checked_at"`
	SourceHistoryStatus   string `gorm:"size:32;column:source_history_status"`
	CoverageStatus        string `gorm:"size:24;index;column:coverage_status"`
	LastSyncAt            int64  `gorm:"column:last_sync_at"`
	LastSuccessAt         int64  `gorm:"column:last_success_at"`
	LastFailureAt         int64  `gorm:"column:last_failure_at"`
	LastError             string `gorm:"size:512;column:last_error"`
	ClassificationVersion int    `gorm:"column:classification_version"`
	QuerySemanticsVersion int    `gorm:"column:query_semantics_version"`
	SourceEpoch           string `gorm:"size:64;column:source_epoch"`
	// RawPageSpanHours is the adaptive cold-import width selected only from
	// observed page density. Zero is the upgrade-safe alias for the default
	// width; it is never a customer/user-specific setting.
	RawPageSpanHours int   `gorm:"column:raw_page_span_hours"`
	UpdatedAt        int64 `gorm:"index;column:updated_at"`
}

// UsageFactMemberHourState 是“该用户的该小时已经成功读取”的持久证明。即使
// 没有任何日志也会保存空集合指纹。LeaseToken/LeaseUntil 让慢 SQL 在本地锁外
// 执行：进程崩溃或查询超时后，过期租约可由下一轮安全重领。
type UsageFactMemberHourState struct {
	UserID      int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	HourTs      int64  `gorm:"primaryKey;autoIncrement:false;index:idx_usage_fact_member_hour_status,priority:1;column:hour_ts"`
	Status      string `gorm:"size:16;index:idx_usage_fact_member_hour_status,priority:2;column:status"`
	Rows        int    `gorm:"column:rows"`
	Requests    int64  `gorm:"column:requests"`
	Tokens      int64  `gorm:"column:tokens"`
	ContentHash string `gorm:"size:64;column:content_hash"`
	SourceEpoch string `gorm:"size:64;column:source_epoch"`
	Attempts    int    `gorm:"column:attempts"`
	UpdatedAt   int64  `gorm:"index;column:updated_at"`
	CompletedAt int64  `gorm:"column:completed_at"`
	LastError   string `gorm:"size:512;column:last_error"`
	LeaseToken  string `gorm:"size:80;column:lease_token"`
	LeaseUntil  int64  `gorm:"index;column:lease_until"`
}

// UsageFactPageIngestState is the durable, per-member cursor for the
// page-oriented importer.  Unlike the legacy source-side GROUP BY path, a
// page contains a bounded number of raw log events and the cursor advances in
// the same SQLite transaction as the local aggregation.  It is intentionally
// member scoped: one high-volume customer can need many pages without making
// another member's waterline wait.
//
// The importer is introduced behind a worker switch.  Keeping its state in
// the facts database now lets the implementation be verified independently
// before it replaces any serving path.
type UsageFactPageIngestState struct {
	UserID int64 `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	HourTs int64 `gorm:"primaryKey;autoIncrement:false;column:hour_ts"`
	// ThroughTs is exclusive. Legacy rows have zero and therefore mean exactly
	// one hour. A value wider than one hour exists only while an adaptive shard
	// is in progress; successful finalization expands it into hourly proofs.
	ThroughTs              int64  `gorm:"column:through_ts"`
	SourceEpoch            string `gorm:"size:64;column:source_epoch"`
	CursorCreatedAt        int64  `gorm:"column:cursor_created_at"`
	CursorType             int    `gorm:"column:cursor_type"`
	CursorID               int64  `gorm:"column:cursor_id"`
	Status                 string `gorm:"size:16;index:idx_usage_fact_page_ingest_status;column:status"`
	Pages                  int64  `gorm:"column:pages"`
	SourceRows             int64  `gorm:"column:source_rows"`
	Requests               int64  `gorm:"column:requests"`
	RefundRecords          int64  `gorm:"column:refund_records"`
	PromptTokens           int64  `gorm:"column:prompt_tokens"`
	CompletionTokens       int64  `gorm:"column:completion_tokens"`
	ConsumeQuota           int64  `gorm:"column:consume_quota"`
	RefundQuota            int64  `gorm:"column:refund_quota"`
	RawHash                string `gorm:"size:64;column:raw_hash"`
	VerifyCursorCreatedAt  int64  `gorm:"column:verify_cursor_created_at"`
	VerifyCursorType       int    `gorm:"column:verify_cursor_type"`
	VerifyCursorID         int64  `gorm:"column:verify_cursor_id"`
	VerifyPages            int64  `gorm:"column:verify_pages"`
	VerifySourceRows       int64  `gorm:"column:verify_source_rows"`
	VerifyRequests         int64  `gorm:"column:verify_requests"`
	VerifyRefundRecords    int64  `gorm:"column:verify_refund_records"`
	VerifyPromptTokens     int64  `gorm:"column:verify_prompt_tokens"`
	VerifyCompletionTokens int64  `gorm:"column:verify_completion_tokens"`
	VerifyConsumeQuota     int64  `gorm:"column:verify_consume_quota"`
	VerifyRefundQuota      int64  `gorm:"column:verify_refund_quota"`
	VerifyRawHash          string `gorm:"size:64;column:verify_raw_hash"`
	ContentHash            string `gorm:"size:64;column:content_hash"`
	LastError              string `gorm:"size:512;column:last_error"`
	UpdatedAt              int64  `gorm:"index;column:updated_at"`
	CompletedAt            int64  `gorm:"column:completed_at"`
}

// UsageUserSnapshot 和 UsageTokenSnapshot 是资料快照，不参与日志事实汇总。
// 余额/累计消耗或令牌资料同步失败时，用量图表仍能从事实表返回；前端只会得到
// 上一次成功快照或空值，而不会因一条 users/tokens 查询失败整页不可用。
type UsageUserSnapshot struct {
	UserID       int64  `gorm:"primaryKey;autoIncrement:false"`
	Username     string `gorm:"size:255"`
	Email        string `gorm:"size:255"`
	BalanceQuota int64  `gorm:"column:balance_quota"`
	UsedQuota    int64  `gorm:"column:used_quota"`
	Exists       bool   `gorm:"column:exists"`
	CapturedAt   int64  `gorm:"index;column:captured_at"`
}

// UsageUserQuotaWatermark keeps a short, local history of the cumulative
// users.used_quota watermark. It lets the read path anchor an unfinalized tail
// to a nearby finalized hour without re-reading source logs or assuming the
// cumulative counter has matched historical facts since account creation.
type UsageUserQuotaWatermark struct {
	UserID     int64 `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	CapturedAt int64 `gorm:"primaryKey;autoIncrement:false;index:idx_usage_user_quota_watermark_captured;column:captured_at"`
	UsedQuota  int64 `gorm:"column:used_quota"`
	Exists     bool  `gorm:"column:exists"`
}

type UsageTokenSnapshot struct {
	TokenID    int64  `gorm:"primaryKey;autoIncrement:false"`
	UserID     int64  `gorm:"index:idx_usage_token_snapshot_user;column:user_id"`
	Name       string `gorm:"size:255"`
	MaskedKey  string `gorm:"size:64;column:masked_key"` // 永远是脱敏串，绝不落 token 明文
	Grp        string `gorm:"size:128;column:grp"`
	UsedQuota  int64  `gorm:"column:used_quota"`
	Deleted    bool   `gorm:"column:deleted"`
	CapturedAt int64  `gorm:"index;column:captured_at"`
}

// UsageFactPublishedMember 是已通过完整性校验、当前允许页面读取的成员快照。
// 表中只保存 user_id；用户名、邮箱和客户组始终与当前 TrackedUser 取交集，
// 因此删除/移组会立即生效，不会因为保留上一版事实而越权显示旧成员。
// 新成员只有在候选回填完整并原子发布后才会进入读取面，避免显示半份历史数据。
type UsageFactPublishedMember struct {
	UserID                int64  `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	TrackedRevision       int64  `gorm:"column:tracked_revision"`
	SourceEpoch           string `gorm:"size:64;column:source_epoch"`
	ClassificationVersion int    `gorm:"column:classification_version"`
	QuerySemanticsVersion int    `gorm:"column:query_semantics_version"`
	SourceFloorHour       int64  `gorm:"column:source_floor_hour"`
	VerifiedThroughHour   int64  `gorm:"column:verified_through_hour"`
	PublishedAt           int64  `gorm:"index;column:published_at"`
}

// UsageFactJob is the persistent control record for full-history coverage,
// verification, rolling audit, and exact repair work.
type UsageFactJob struct {
	ID             string  `gorm:"primaryKey;size:80;column:id"`
	IdempotencyKey *string `gorm:"uniqueIndex;size:96;column:idempotency_key"`
	Kind           string  `gorm:"size:32;index:idx_usage_fact_job_queue,priority:1;column:kind"`
	Priority       int     `gorm:"index:idx_usage_fact_job_queue,priority:2;column:priority"`
	UserID         *int64  `gorm:"index;column:user_id"`
	// TrackedRevision fences work leased before remove/rejoin. A worker may
	// finish staging old facts, but it cannot advance coverage or publish unless
	// this revision still equals the authoritative main-store control row.
	TrackedRevision int64  `gorm:"column:tracked_revision"`
	SourceEpoch     string `gorm:"size:64;column:source_epoch"`
	FromTs          int64  `gorm:"column:from_ts"`
	ThroughTs       int64  `gorm:"column:through_ts"`
	NextHour        int64  `gorm:"index;column:next_hour"`
	VerifyNextHour  int64  `gorm:"index;column:verify_next_hour"`
	TotalHours      int64  `gorm:"column:total_hours"`
	CompletedHours  int64  `gorm:"column:completed_hours"`
	VerifiedHours   int64  `gorm:"column:verified_hours"`
	FailedHours     int64  `gorm:"column:failed_hours"`
	FailedHourList  string `gorm:"type:text;column:failed_hour_list"`
	Reason          string `gorm:"size:500;column:reason"`
	Status          string `gorm:"size:16;index:idx_usage_fact_job_queue,priority:3;column:status"`
	Attempts        int    `gorm:"column:attempts"`
	NextRetryAt     int64  `gorm:"index;column:next_retry_at"`
	LeaseOwner      string `gorm:"size:80;column:lease_owner"`
	LeaseUntil      int64  `gorm:"index;column:lease_until"`
	// LastServedSeq is a durable, monotonically increasing scheduler ticket.
	// Wall-clock seconds are not a safe fairness key: several bounded turns can
	// complete in one second and clocks can move backwards. Raw-page lanes sort
	// never-served/least-recently-served jobs by this sequence instead.
	LastServedSeq int64  `gorm:"index;column:last_served_seq"`
	CreatedAt     int64  `gorm:"index;column:created_at"`
	UpdatedAt     int64  `gorm:"index;column:updated_at"`
	StartedAt     int64  `gorm:"column:started_at"`
	HeartbeatAt   int64  `gorm:"index;column:heartbeat_at"`
	CompletedAt   int64  `gorm:"column:completed_at"`
	LastError     string `gorm:"size:512;column:last_error"`
	RequestedBy   string `gorm:"size:64;column:requested_by"`
	ApprovedBy    string `gorm:"size:64;column:approved_by"`
}

// UsageFactRepairRequest is the durable HTTP idempotency receipt for one
// administrator-requested member-day repair. The repair job itself is unique
// by member revision/day so a concurrent rolling audit and a manual request
// converge on one worker; this receipt prevents a retried HTTP request from
// re-opening that completed job later.
type UsageFactRepairRequest struct {
	RequestID       string `gorm:"primaryKey;size:96;column:request_id"`
	JobID           string `gorm:"size:80;index;column:job_id"`
	UserID          int64  `gorm:"index;column:user_id"`
	TrackedRevision int64  `gorm:"column:tracked_revision"`
	SourceEpoch     string `gorm:"size:64;column:source_epoch"`
	DayTs           int64  `gorm:"index;column:day_ts"`
	Reason          string `gorm:"size:500;column:reason"`
	RequestedBy     string `gorm:"size:64;column:requested_by"`
	CreatedAt       int64  `gorm:"index;column:created_at"`
}

// UsageFactRepairMember 是当前唯一受控修复任务的持久目标清单。修复可能只涉及
// 候选快照中的部分成员，不能继续用“全部已发布成员”推算进度。ResumeBackfillHour
// 保存任务前的连续水位；目标自然日完成后原子恢复该水位，避免为了回到候选尾部
// 再逐周扫描已经验证过的本地 proof。新任务开始时会整体替换此表。
type UsageFactRepairMember struct {
	UserID             int64 `gorm:"primaryKey;autoIncrement:false;column:user_id"`
	RequestedAt        int64 `gorm:"index;column:requested_at"`
	ResumeBackfillHour int64 `gorm:"column:resume_backfill_hour"`
}

// UsageFactSyncState 只保留一行全局运行状态。MemberFingerprint /
// BackfillWindowDays 描述正在回填的“候选版”；Published* 描述上一份
// 已通过完整性校验的“服务版”。候选版失败、扩展时间窗口或成员变化时，
// 页面继续读服务版，不回扫主站 logs，也不把整页变成“统计不可用”。
// Generation 跟踪候选事实变化；ServingGeneration 只在当前发布成员/
// 窗口的可见数据改变时递增，用作 Redis/L1 缓存世代号。两者分离
// 可避免新成员几百天候选回填每 15 秒把旧服务版缓存全部换键。
type UsageFactSyncState struct {
	ID                  uint   `gorm:"primaryKey;autoIncrement:false"`
	TrafficClassVersion int    `gorm:"column:traffic_class_version;index"`
	Generation          int64  `gorm:"column:generation"`
	ServingGeneration   int64  `gorm:"column:serving_generation"`
	MemberFingerprint   string `gorm:"size:32;column:member_fingerprint"`
	// BackfillWindowDays 是创建当前回填游标时的配置窗口。它让运维后续扩展
	// MONITOR_USAGE_FACTS_BACKFILL_DAYS 时，能识别“游标已到尾部、但新增历史
	// 范围尚未补齐”的情况，并安全地从新窗口起点恢复，而不是误判为已完成。
	BackfillWindowDays int `gorm:"column:backfill_window_days"`
	// NextBackfillHour 是历史回填游标。只前移，不参与页面缓存世代；服务重启后
	// 可从上次未完成小时继续，避免每轮从保留窗口起点重复扫描本地台账。
	NextBackfillHour int64 `gorm:"column:next_backfill_hour"`
	// 历史复核与首次回填使用不同游标：首次回填只负责补齐缺口；复核游标在
	// 已发布窗口内低频轮转，用于发现晚到、事后修订或曾经静默漏掉的数据。
	// 两者分开后，复核失败不会阻塞正常 Tail，也不会把完整率改成失败。
	NextReconcileHour      int64  `gorm:"column:next_reconcile_hour"`
	LastReconciledHour     int64  `gorm:"column:last_reconciled_hour"`
	LastReconcileAt        int64  `gorm:"column:last_reconcile_at"`
	LastReconcileFailureAt int64  `gorm:"column:last_reconcile_failure_at"`
	ReconcileCorrections   int64  `gorm:"column:reconcile_corrections"`
	PublishedFingerprint   string `gorm:"size:32;column:published_fingerprint"`
	PublishedWindowDays    int    `gorm:"column:published_window_days"`
	PublishedRangeStart    int64  `gorm:"column:published_range_start"`
	PublishedThrough       int64  `gorm:"column:published_through"`
	PublishedAt            int64  `gorm:"column:published_at"`
	// Repair* 记录一次由超级管理员显式发起的历史自然日受控补数。
	// 补数只删除本地 member-hour 完成证明并回退已发布成员的候选游标，
	// 然后复用现有串行小时同步逐日重建事实；旧服务版在新日完整前仍可读。
	RepairFrom                  int64  `gorm:"column:repair_from"`
	RepairThrough               int64  `gorm:"column:repair_through"`
	RepairMode                  string `gorm:"size:32;column:repair_mode"`
	RepairMembershipFingerprint string `gorm:"size:32;column:repair_membership_fingerprint"`
	RepairTargetMembers         int64  `gorm:"column:repair_target_members"`
	RepairRequestedAt           int64  `gorm:"column:repair_requested_at"`
	RepairCompletedAt           int64  `gorm:"column:repair_completed_at"`
	RepairLastFailureAt         int64  `gorm:"column:repair_last_failure_at"`
	RepairLastError             string `gorm:"size:512;column:repair_last_error"`
	RepairTotalMemberHours      int64  `gorm:"column:repair_total_member_hours"`
	RepairCompletedMemberHours  int64  `gorm:"column:repair_completed_member_hours"`
	LastFactSyncAt              int64  `gorm:"column:last_fact_sync_at"`
	LastProfileSyncAt           int64  `gorm:"column:last_profile_sync_at"`
	LastFactFailureAt           int64  `gorm:"column:last_fact_failure_at"`
	LastProfileFailureAt        int64  `gorm:"column:last_profile_failure_at"`
	LastPrunedAt                int64  `gorm:"column:last_pruned_at"`
	// Full-history cold source circuit is durable so a restart cannot turn a
	// timed-out minimum chunk into another immediate burst. Tail and local reads
	// do not use this gate.
	HistoryBulkCircuitState      string `gorm:"size:16;column:history_bulk_circuit_state"`
	HistoryBulkOpenedUntil       int64  `gorm:"index;column:history_bulk_opened_until"`
	HistoryBulkSlowStreak        int    `gorm:"column:history_bulk_slow_streak"`
	HistoryBulkFailureStreak     int    `gorm:"column:history_bulk_failure_streak"`
	HistoryBulkHalfOpenSuccesses int    `gorm:"column:history_bulk_half_open_successes"`
	HistoryBulkLastQueryMS       int64  `gorm:"column:history_bulk_last_query_ms"`
	HistoryBulkLastQueryAt       int64  `gorm:"column:history_bulk_last_query_at"`
	HistoryBulkLastError         string `gorm:"size:512;column:history_bulk_last_error"`
}

// 直方图档位上界。lat 单位秒,ttft 单位毫秒。
var (
	latEdges  = []int{1, 2, 5, 10, 30, 60}
	ttftEdges = []int{500, 1000, 2000, 5000, 10000}
)

// openStore 打开本地采样库并迁移表结构。
func (m *Monitor) openStore(path string) error {
	m.cfg.StorePath = path
	// A runtime restore publishes two SQLite files separately, guarded by a
	// durable IN_PROGRESS -> READY -> ACTIVATED protocol. Validate/activate that
	// pair before the ordinary migration snapshot can mistake a crash-partial
	// restore for a fresh facts database.
	if err := m.preflightStoreRestoreActivation(context.Background(), path, m.cfg.UsageFactsStorePath); err != nil {
		return fmt.Errorf("运行期备份集恢复门禁拒绝启动: %w", err)
	}
	// 必须在主库任何 Migrator 探测/AutoMigrate 之前，同时锁定并备份主库与
	// facts 库。任意一库的 WAL 快照、quick_check、表计数或 manifest 发布
	// 失败都会在这里终止启动，两库都不会进入迁移。
	snapshot, err := m.createPreMigrationSnapshot(context.Background(), path, m.cfg.UsageFactsStorePath, time.Now())
	prechecked := snapshot.checkedPath(path)
	m.storeIntegrityCheckedAt.Store(time.Now().Unix())
	m.storeIntegrityOK.Store(err == nil && prechecked)
	factsPath := strings.TrimSpace(m.cfg.UsageFactsStorePath)
	factsPrechecked := snapshot.checkedPath(factsPath)
	if factsPath != "" && !sameStorePath(path, factsPath) {
		m.usageFactsIntegrityCheckedAt.Store(time.Now().Unix())
		m.usageFactsIntegrityOK.Store(err == nil && factsPrechecked)
	}
	if err != nil {
		return fmt.Errorf("迁移前双库快照失败，已保留原文件且拒绝执行任何 AutoMigrate: %w", err)
	}
	if snapshot.SnapshotDir != "" {
		if snapshot.Reused {
			slog.Info("已复核并复用当前 migration plan 的固定迁移前快照", "snapshot", filepath.Base(snapshot.SnapshotDir))
		} else {
			slog.Info("迁移前双库快照已校验并原子发布", "snapshot", filepath.Base(snapshot.SnapshotDir))
		}
	}
	// 文件在快照闸门执行时不存在，却在闸门释放后出现，说明仍有另一个
	// 进程在操作该卷。宁可拒绝启动，也不能迁移一份未进入 manifest 的库。
	if !prechecked && storeUsesFile(path) {
		appeared, checkErr := preflightStoreIntegrity(path)
		if checkErr != nil {
			return fmt.Errorf("迁移闸门后本地采样库检查失败，拒绝迁移: %w", checkErr)
		}
		if appeared {
			return errors.New("迁移闸门后本地采样库才出现，疑似仍有旧进程运行；拒绝未备份迁移")
		}
	}
	// busy_timeout:采样写入与页面读取并发时,等锁而非立刻报 SQLITE_BUSY;WAL:提升读写并发。
	dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
	db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		return fmt.Errorf("打开本地采样库失败: %w", err)
	}
	openedSuccessfully := false
	defer func() {
		if openedSuccessfully {
			return
		}
		if sqlDB, dbErr := db.DB(); dbErr == nil {
			_ = sqlDB.Close()
		}
		if m.storeDB == db {
			m.storeDB = nil
		}
		if m.usageFactsDB == db {
			m.usageFactsDB = nil
		}
	}()
	// 新建数据库在打开前不存在，需在建表前补做一次检查；已有库已经由上面的
	// 只读连接检查过，避免对大库重复扫描两次。
	if !prechecked {
		if err := checkGORMStoreIntegrity(db); err != nil {
			m.storeIntegrityOK.Store(false)
			if sqlDB, dbErr := db.DB(); dbErr == nil {
				_ = sqlDB.Close()
			}
			return fmt.Errorf("新建本地采样库完整性检查失败: %w", err)
		}
		m.storeIntegrityCheckedAt.Store(time.Now().Unix())
		m.storeIntegrityOK.Store(true)
	}
	// 栏目开关列是否已存在(迁移前探测):不存在=本次 AutoMigrate 会新建 → 老库存量配置行需补置 true,
	// 保证从旧版本升级后报警行为不变。字段本身不带 gorm default(布尔 false 会被 default 顶掉,见 AlertConfig 注释)。
	hadCategoryToggles := db.Migrator().HasColumn(&AlertConfig{}, "server_alerts_enabled")
	// 交付异常告警列同理:老库升级时这些列刚建出来是零值(开关 false、阈值 0 = 规则不启用),
	// 需按 defaultAlertConfig 补一次,否则升级后交付异常告警静默失效。
	hadAnomalyAlerts := db.Migrator().HasColumn(&AlertConfig{}, "anomaly_alerts_enabled")
	hadUpstreamBalanceAlerts := db.Migrator().HasColumn(&AlertConfig{}, "upstream_balance_alerts_enabled")
	if err := db.AutoMigrate(
		&MetricSample{}, &TokenSample{}, &HourSample{}, &ChannelSnap{}, &RejectionSample{}, &RejectionIngestBatch{}, &SelectablePair{},
		&StabilityHourSample{}, &ChannelTestHourSample{}, &StabilityRejectHour{}, &StabilityProblemSample{},
		&StabilityProblemIngestState{}, &StabilityProblemStage{}, &StabilityProblemClassificationMigration{}, &StabilityProblemLiveCursor{},
		&StabilityHourIngestState{}, &StabilityBackfillJob{},
		&ChannelFinanceSetting{}, &ChannelSaleGroupRate{}, &WebsiteGroupCatalog{}, &ChannelDomainCost{}, &ChannelDomainGroupCost{}, &ChannelFinanceChannelCost{}, &ChannelFinanceVersion{},
		&ChannelUpstreamAccount{}, &ChannelUpstreamUsageHour{}, &ChannelUpstreamErrorLog{}, &UpstreamErrorLogSyncState{}, &NewAPIUsageBackfillCheckpoint{}, &AICodeWithKeySyncState{}, &AICodeWithUsageStage{}, &AICodeWithUsageRound{}, &UpstreamHostCircuit{},
		&ChannelUpstreamPricingHourEvidence{}, &ChannelUpstreamPricingHourState{}, &ChannelUpstreamPricingObservedState{}, &ChannelUpstreamPricingChangeEvent{}, &ChannelUpstreamPricingSyncState{}, &ChannelUpstreamPricingPageCheckpoint{}, &AICodeWithPricingCheckpoint{},
		&InfraSample{}, &HostContainerSnapshot{}, &NginxMinuteSample{}, &NginxIngestBatch{}, &NginxSourceState{},
		&AlertConfig{}, &AlertLog{}, &TrackedUser{}, &CustomerGroup{}, &UsageMemberControl{}, &UsageMemberAudit{}, &UsageMemberControlMigration{}, &FollowUpLog{}, &UsageSettings{},
	); err != nil {
		return fmt.Errorf("表迁移失败: %w", err)
	}
	if err := migrateUsageMemberControls(db); err != nil {
		return fmt.Errorf("用量成员控制层迁移失败: %w", err)
	}
	if err := migrateLegacyChannelFinanceVersions(db); err != nil {
		return fmt.Errorf("倍率版本迁移失败: %w", err)
	}
	if !hadCategoryToggles {
		if err := db.Model(&AlertConfig{}).Where("id = 1").Updates(map[string]any{
			"model_alerts_enabled":  true,
			"server_alerts_enabled": true,
		}).Error; err != nil {
			return fmt.Errorf("初始化报警分类开关失败: %w", err)
		}
	}
	if !hadAnomalyAlerts {
		d := defaultAlertConfig()
		if err := db.Model(&AlertConfig{}).Where("id = 1").Updates(map[string]any{
			"anomaly_alerts_enabled": d.AnomalyAlertsEnabled,
			"anomaly_rate_pct":       d.AnomalyRatePct,
			"anomaly_cooldown_min":   d.AnomalyCooldownMin,
			"anomaly_billed_usd":     d.AnomalyBilledUSD,
			"anomaly_min_count":      d.AnomalyMinCount,
		}).Error; err != nil {
			return fmt.Errorf("初始化交付异常报警配置失败: %w", err)
		}
	}
	if !hadUpstreamBalanceAlerts {
		d := defaultAlertConfig()
		if err := db.Model(&AlertConfig{}).Where("id = 1").Updates(map[string]any{
			"upstream_balance_alerts_enabled": d.UpstreamBalanceAlertsEnabled,
			"upstream_balance_runway_days":    d.UpstreamBalanceRunwayDays,
			"upstream_balance_lookback_days":  d.UpstreamBalanceLookbackDays,
			"upstream_balance_min_coverage":   d.UpstreamBalanceMinCoverage,
			"upstream_balance_cooldown_min":   d.UpstreamBalanceCooldownMin,
		}).Error; err != nil {
			return fmt.Errorf("初始化上游余额报警配置失败: %w", err)
		}
	}
	m.storeDB = db
	if err := m.migrateLegacyUpstreamCredentialEncryption(); err != nil {
		return fmt.Errorf("上游凭据加密密钥迁移失败: %w", err)
	}
	if err := m.migrateAICodeWithCredentialSlots(); err != nil {
		return fmt.Errorf("AICodeWith Key 槽位迁移失败: %w", err)
	}
	if err := m.migrateAICodeWithContractLedgerUnit(); err != nil {
		return fmt.Errorf("AICodeWith 账面单位迁移失败: %w", err)
	}
	if err := m.openUsageFactsStore(m.cfg.UsageFactsStorePath, factsPrechecked); err != nil {
		factsPath := strings.TrimSpace(m.cfg.UsageFactsStorePath)
		separateFactsStore := factsPath != "" && factsPath != m.cfg.StorePath
		// 现有文件的损坏/备份失败已经由上面的双库迁移闸门拦截。通过闸门后，
		// facts 独立打开/建新库仍可能遇到瞬时磁盘故障：write-only/shadow 可
		// 隔离该功能；已显式切读时必须 fail closed，绝不能回扫生产 logs。
		if separateFactsStore && !m.cfg.UsageFactsReadEnabled && !m.cfg.UsageFactsLocalReadOnly {
			m.usageFactsDB = nil
			slog.Error("用量事实库不可用，已隔离该功能并继续启动 Monitor 主库", "err", err)
		} else {
			return err
		}
	}
	slog.Info("本地采样库就绪", "path", path)
	openedSuccessfully = true
	return nil
}

// usageFactsStore 返回用量事实专库。测试/迁移工具没有显式配置专库时允许
// 与主库共用连接；生产 New 会自动填充独立路径，因此不会走这个兼容分支。
func (m *Monitor) usageFactsStore() *gorm.DB {
	if m.usageFactsDB != nil {
		return m.usageFactsDB
	}
	path := strings.TrimSpace(m.cfg.UsageFactsStorePath)
	if path == "" || path == m.cfg.StorePath {
		return m.storeDB
	}
	return nil
}

func (m *Monitor) openUsageFactsStore(path string, prechecked bool) error {
	path = strings.TrimSpace(path)
	openedSeparate := false
	openedSuccessfully := false
	defer func() {
		if !openedSeparate || openedSuccessfully || m.usageFactsDB == nil {
			return
		}
		m.usageFactsIntegrityOK.Store(false)
		if sqlDB, err := m.usageFactsDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
		m.usageFactsDB = nil
	}()
	if path == "" || path == m.cfg.StorePath {
		m.usageFactsDB = m.storeDB
	} else {
		m.usageFactsIntegrityCheckedAt.Store(time.Now().Unix())
		m.usageFactsIntegrityOK.Store(prechecked)
		if !prechecked {
			appeared, err := preflightStoreIntegrity(path)
			if err != nil {
				return fmt.Errorf("迁移闸门后用量事实库检查失败，拒绝迁移: %w", err)
			}
			if appeared {
				return errors.New("迁移闸门后用量事实库才出现，疑似仍有旧进程运行；拒绝未备份迁移")
			}
		}
		dsn := path + "?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)"
		db, err := gorm.Open(sqlite.Open(dsn), &gorm.Config{Logger: logger.Default.LogMode(logger.Silent)})
		if err != nil {
			m.usageFactsIntegrityOK.Store(false)
			return fmt.Errorf("打开用量事实库失败: %w", err)
		}
		if !prechecked {
			if err := checkGORMStoreIntegrity(db); err != nil {
				m.usageFactsIntegrityOK.Store(false)
				if sqlDB, dbErr := db.DB(); dbErr == nil {
					_ = sqlDB.Close()
				}
				return fmt.Errorf("新建用量事实库完整性检查失败: %w", err)
			}
			m.usageFactsIntegrityCheckedAt.Store(time.Now().Unix())
			m.usageFactsIntegrityOK.Store(true)
		}
		m.cfg.UsageFactsStorePath = path
		m.usageFactsDB = db
		openedSeparate = true
	}

	factsDB := m.usageFactsStore()
	if factsDB == nil {
		return errors.New("用量事实库未初始化")
	}
	if m.usageFactsDB == m.storeDB {
		m.usageFactsIntegrityCheckedAt.Store(m.storeIntegrityCheckedAt.Load())
		m.usageFactsIntegrityOK.Store(m.storeIntegrityOK.Load())
	}
	if err := factsDB.AutoMigrate(
		&UsageHourFact{}, &UsageDailyFact{}, &UsageFactMemberDayState{}, &UsageHourIngestState{}, &UsageFactMemberState{}, &UsageFactMemberHourState{}, &UsageUserSnapshot{}, &UsageUserQuotaWatermark{},
		&UsageFactPageIngestState{},
		&UsageTokenSnapshot{}, &UsageFactPublishedMember{}, &UsageFactRepairMember{}, &UsageFactJob{}, &UsageFactRepairRequest{}, &UsageFactSyncState{},
	); err != nil {
		return fmt.Errorf("用量事实表迁移失败: %w", err)
	}
	for _, stmt := range []string{
		"CREATE INDEX IF NOT EXISTS idx_usage_daily_fact_date_group ON usage_daily_facts(date_ts, grp)",
		"CREATE INDEX IF NOT EXISTS idx_usage_daily_fact_date_model ON usage_daily_facts(date_ts, model_name)",
		"CREATE INDEX IF NOT EXISTS idx_usage_daily_fact_date_token ON usage_daily_facts(date_ts, user_id, token_id)",
		// 组织矩阵会按日期扫全部成员，单用户/令牌下钻则相反：先锁定用户再取长日期窗。
		// 两种方向都保留，避免 90/366 天单用户详情扫描同窗口内所有成员的本地事实。
		"CREATE INDEX IF NOT EXISTS idx_usage_daily_fact_user_date ON usage_daily_facts(user_id, date_ts)",
		"CREATE INDEX IF NOT EXISTS idx_usage_daily_fact_user_token_date ON usage_daily_facts(user_id, token_id, date_ts)",
		"CREATE INDEX IF NOT EXISTS idx_usage_hour_fact_hour_token ON usage_hour_facts(hour_ts, user_id, token_id)",
		"CREATE INDEX IF NOT EXISTS idx_usage_hour_fact_day_user ON usage_hour_facts(day_ts, user_id)",
		"CREATE INDEX IF NOT EXISTS idx_usage_hour_fact_user_day ON usage_hour_facts(user_id, day_ts)",
	} {
		if err := factsDB.Exec(stmt).Error; err != nil {
			return fmt.Errorf("创建用量事实索引失败: %w", err)
		}
	}
	// 预建唯一的同步状态行并载入缓存世代。这样服务重启不会把 Redis/L1
	// 看成同一批事实，避免命中旧数据；若初始化失败则宁可启动失败，不把
	// “本地事实模式”的正确性建立在不确定状态上。
	var usageState UsageFactSyncState
	if err := factsDB.First(&usageState, 1).Error; errors.Is(err, gorm.ErrRecordNotFound) {
		usageState = UsageFactSyncState{ID: 1, TrafficClassVersion: userTrafficClassificationVersion}
		if err := factsDB.Create(&usageState).Error; err != nil {
			return fmt.Errorf("初始化用量事实状态失败: %w", err)
		}
	} else if err != nil {
		return fmt.Errorf("读取用量事实状态失败: %w", err)
	}
	migratedState, rebuilt, migrateErr := migrateUsageFactsTrafficClassification(factsDB, m.cfg.UsageFactsClassificationMigrationEnabled)
	if migrateErr != nil {
		return fmt.Errorf("用量事实分类版本迁移失败: %w", migrateErr)
	}
	usageState = migratedState
	if rebuilt {
		slog.Warn("用量事实分类口径已显式进入维护迁移：旧事实保留，旧发布已撤销，等待按新分类逐成员重签",
			"traffic_class_version", userTrafficClassificationVersion)
	}
	if usageState.ServingGeneration == 0 && usageState.PublishedAt > 0 {
		// 旧快照没有独立服务世代列；首次升级以当前 Generation
		// 作为安全起点，并且必须持久化。只改内存会让 Portal 的
		// durable-generation fence 在每次重启后永久看到 DB=0/atomic=N。
		servingGeneration := usageState.Generation
		if servingGeneration <= 0 {
			servingGeneration = 1
		}
		updates := map[string]any{"serving_generation": servingGeneration}
		if usageState.Generation <= 0 {
			updates["generation"] = servingGeneration
		}
		result := factsDB.Model(&UsageFactSyncState{}).
			Where("id = ? AND serving_generation = ?", usageState.ID, 0).Updates(updates)
		if result.Error != nil {
			return fmt.Errorf("持久化旧用量事实服务世代失败: %w", result.Error)
		}
		if err := factsDB.First(&usageState, usageState.ID).Error; err != nil {
			return fmt.Errorf("重读用量事实服务世代失败: %w", err)
		}
	}
	m.publishUsageFactGenerations(usageState.Generation, usageState.ServingGeneration)
	if path != "" && path != m.cfg.StorePath {
		slog.Info("用量事实库就绪", "path", path)
	}
	openedSuccessfully = true
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
	status    int
	since     int64
	deletedAt int64
}

// channelEnabledState 返回当前 channel_snaps 里每个渠道的上一轮状态,
// 供刷新时正确维护 enabled_since(渠道不存在时取零值,即按"新建"处理)。
func (m *Monitor) channelEnabledState() map[int]chanPrev {
	var rows []struct {
		ID, Status   int
		EnabledSince int64
		DeletedAt    int64
	}
	warnReadErr("channelEnabledState", m.storeDB.Raw("SELECT id, status, enabled_since, deleted_at FROM channel_snaps").Scan(&rows))
	out := make(map[int]chanPrev, len(rows))
	for _, r := range rows {
		out[r.ID] = chanPrev{status: r.Status, since: r.EnabledSince, deletedAt: r.DeletedAt}
	}
	return out
}

// replaceChannelSnaps 用本轮完整读取到的渠道快照更新本地表。
// 同 ID 的名称/厂商/地址等直接同步最新值；本轮未出现的渠道只标记 deleted_at，
// 永不物理删除其最后快照，以便历史报表仍能解释旧 channel_id。
func (m *Monitor) replaceChannelSnaps(rows []ChannelSnap, now int64) error {
	if len(rows) == 0 {
		// 空结果可能是上游读取失败或异常，绝不能据此把所有渠道标成已删除。
		return nil
	}
	return m.replaceChannelSnapsAuthoritative(rows, now)
}

// replaceChannelSnapsAuthoritative 只能在 channels 整表查询已成功读完时调用。
// 与 replaceChannelSnaps 不同，此处的空集合是可信事实，表示当前已没有任何渠道。
// 删除判断按本轮 ID 集合而不是 updated_at，避免同一秒内连续刷新时漏标。
func (m *Monitor) replaceChannelSnapsAuthoritative(rows []ChannelSnap, now int64) error {
	ids := make([]int, 0, len(rows))
	for i := range rows {
		rows[i].DeletedAt = 0
		ids = append(ids, rows[i].ID)
	}
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		if len(rows) > 0 {
			if err := tx.Clauses(clause.OnConflict{
				Columns:   []clause.Column{{Name: "id"}},
				UpdateAll: true,
			}).CreateInBatches(rows, 200).Error; err != nil {
				return err
			}
		}
		missing := tx.Model(&ChannelSnap{}).Where("deleted_at = 0")
		if len(ids) > 0 {
			missing = missing.Where("id NOT IN ?", ids)
		}
		return missing.Update("deleted_at", now).Error
	})
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

// upsertRejections 累加一个已经确认是“新批次”的拒绝计数。HTTP 重试幂等由
// ingestRejectionBatch 的批次台账保证；不同批次落在同一分钟时必须继续累加。
func (m *Monitor) upsertRejections(rows []RejectionSample) error {
	return upsertRejectionsDB(m.storeDB, rows)
}

func upsertRejectionsDB(db *gorm.DB, rows []RejectionSample) error {
	if len(rows) == 0 {
		return nil
	}
	return db.Clauses(clause.OnConflict{
		Columns: []clause.Column{
			{Name: "bucket_ts"}, {Name: "node"}, {Name: "reason"}, {Name: "model"}, {Name: "grp"},
		},
		DoUpdates: clause.Assignments(map[string]interface{}{
			"count": gorm.Expr("rejection_samples.count + excluded.count"),
		}),
	}).CreateInBatches(rows, 200).Error
}

var errRejectionBatchConflict = errors.New("rejection batch id reused with different payload")

func rejectionBatchPayloadHash(rows []RejectionSample) string {
	canonical := append([]RejectionSample(nil), rows...)
	sort.Slice(canonical, func(i, j int) bool {
		a, b := canonical[i], canonical[j]
		if a.BucketTs != b.BucketTs {
			return a.BucketTs < b.BucketTs
		}
		if a.Reason != b.Reason {
			return a.Reason < b.Reason
		}
		if a.Model != b.Model {
			return a.Model < b.Model
		}
		if a.Grp != b.Grp {
			return a.Grp < b.Grp
		}
		return a.Count < b.Count
	})
	payload, _ := json.Marshal(canonical) // 字段均为基础类型，不存在编码失败路径。
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// ingestRejectionBatch 在同一个 SQLite 事务内先登记批次、再累加样本。
// 返回 duplicate=true 表示服务端已经完整接收过同一批，调用方可安全丢弃重试。
func (m *Monitor) ingestRejectionBatch(node, batchID string, rows []RejectionSample, receivedAt int64) (duplicate bool, err error) {
	hash := rejectionBatchPayloadHash(rows)
	var total int64
	for _, row := range rows {
		total += row.Count
	}
	err = m.storeDB.Transaction(func(tx *gorm.DB) error {
		batch := RejectionIngestBatch{Node: node, BatchID: batchID, PayloadHash: hash, Rows: len(rows), TotalCount: total, ReceivedAt: receivedAt}
		created := tx.Clauses(clause.OnConflict{DoNothing: true}).Create(&batch)
		if created.Error != nil {
			return created.Error
		}
		if created.RowsAffected == 0 {
			var existing RejectionIngestBatch
			if err := tx.First(&existing, "node = ? AND batch_id = ?", node, batchID).Error; err != nil {
				return err
			}
			if existing.PayloadHash != hash {
				return errRejectionBatchConflict
			}
			duplicate = true
			return nil
		}
		return upsertRejectionsDB(tx, rows)
	})
	return duplicate, err
}

// storeRejections 取窗口内按 (原因 × 模型 × 分组) 聚合的拒绝计数,按次数降序(Top 100)。
func (m *Monitor) storeRejections(since int64) []RejectionRow {
	var rows []RejectionRow
	warnReadErr("storeRejections", m.storeDB.Raw(`SELECT reason, model, grp AS `+"`group`"+`, COALESCE(SUM(count),0) AS count
		FROM rejection_samples WHERE bucket_ts >= ?
		GROUP BY reason, model, grp ORDER BY count DESC LIMIT 100`, since).Scan(&rows))
	return rows
}

func (m *Monitor) pruneRejectionsOlderThan(cutoffTs int64) (int64, error) {
	var deleted int64
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		r := tx.Where("bucket_ts < ?", cutoffTs).Delete(&RejectionSample{})
		if r.Error != nil {
			return r.Error
		}
		deleted = r.RowsAffected
		return tx.Where("received_at < ?", cutoffTs).Delete(&RejectionIngestBatch{}).Error
	})
	return deleted, err
}

// aggRow 承接聚合结果。
type aggRow struct {
	K              string
	Success        int64
	Anomaly        int64
	Failed         int64
	AnomalyBilled  int64 `gorm:"column:anomaly_billed"`
	AnomalyFree    int64 `gorm:"column:anomaly_free"`
	AnomalyStream  int64 `gorm:"column:anomaly_stream"`
	AnomalyQuota   int64 `gorm:"column:anomaly_quota"`
	AnomalySumTime int64 `gorm:"column:anomaly_sum_time"`
	SumUseTime     int64
	MaxUseTime     int
	Tokens         int64
	Quota          int64
	Err4xx         int64 `gorm:"column:err_4xx"`
	Err5xx         int64 `gorm:"column:err_5xx"`
	ErrTimeout     int64
	ErrOther       int64
	Lat1           int64 `gorm:"column:lat_1"`
	Lat2           int64 `gorm:"column:lat_2"`
	Lat5           int64 `gorm:"column:lat_5"`
	Lat10          int64 `gorm:"column:lat_10"`
	Lat30          int64 `gorm:"column:lat_30"`
	Lat60          int64 `gorm:"column:lat_60"`
	LatInf         int64 `gorm:"column:lat_inf"`

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
  COALESCE(SUM(anomaly_billed),0)   AS anomaly_billed,
  COALESCE(SUM(anomaly_free),0)     AS anomaly_free,
  COALESCE(SUM(anomaly_stream),0)   AS anomaly_stream,
  COALESCE(SUM(anomaly_quota),0)    AS anomaly_quota,
  COALESCE(SUM(anomaly_sum_time),0) AS anomaly_sum_time,
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
	// 交付异常明细:拆分 + 严重度(金额、平均等待)。
	// 金额只算 B1(零输出却已扣费)——这是唯一直接产生对客损失的一类。
	r.AnomalyBilled, r.AnomalyFree, r.AnomalyStream = a.AnomalyBilled, a.AnomalyFree, a.AnomalyStream
	r.AnomalyCostUSD = float64(a.AnomalyQuota) / quotaPerUSD
	if a.Anomaly > 0 {
		r.AnomalyAvgWait = float64(a.AnomalySumTime) / float64(a.Anomaly)
	}
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
	if err := m.storeDB.Raw(`SELECT '' AS k, `+aggCols+` FROM metric_samples WHERE bucket_ts >= ?`+currentMetricTrafficFilter+enabledChanFilter+selectableFilter, since).
		Scan(&a).Error; err != nil {
		return nil, fmt.Errorf("本地汇总失败: %w", err)
	}
	var r Row
	a.fill(&r, windowSec)
	return &Summary{
		Total: r.Total, Success: r.Success, Anomaly: r.Anomaly, Failed: r.Failed,
		SuccessRate: r.SuccessRate, AnomalyRate: r.AnomalyRate, ErrorRate: r.ErrorRate,
		AnomalyBilled: r.AnomalyBilled, AnomalyFree: r.AnomalyFree, AnomalyStream: r.AnomalyStream,
		AnomalyCostUSD: r.AnomalyCostUSD, AnomalyAvgWait: r.AnomalyAvgWait,
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
// dimColOK:dimCol 是全库唯一被 fmt.Sprintf 进 SQL 的字符串,只允许三个内部常量列名。
// 白名单在入口卡死——防未来有人把请求参数误传进来,把这里变成注入面。
func dimColOK(dimCol string) bool {
	switch dimCol {
	case "grp", channelDim, "model_name":
		return true
	}
	return false
}

func (m *Monitor) storeDimSeries(dimCol string, since int64, windowMinutes int) (map[string][]TimePoint, error) {
	if !dimColOK(dimCol) {
		return nil, fmt.Errorf("非法维度列: %q", dimCol)
	}
	type row struct {
		K        string
		BucketTs int64
		Success  int64
		Anomaly  int64
		Failed   int64
	}
	f := currentMetricTrafficFilter + enabledChanFilter + selectableFilter
	if dimCol == channelDim { // 按渠道明细不过滤,排障仍能看禁用渠道/误路由
		f = currentMetricTrafficFilter
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
	if !dimColOK(dimCol) {
		return nil, fmt.Errorf("非法维度列: %q", dimCol)
	}
	f := currentMetricTrafficFilter + enabledChanFilter + selectableFilter
	if dimCol == channelDim { // 按渠道明细不过滤,排障仍能看禁用渠道/误路由
		f = currentMetricTrafficFilter
	}
	q := fmt.Sprintf(`SELECT %s AS k, %s FROM metric_samples
		WHERE bucket_ts >= ?%s GROUP BY %s
		ORDER BY quota DESC, (success+anomaly+failed) DESC, k ASC LIMIT 200`, dimCol, aggCols, f, dimCol)
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
		FROM metric_samples WHERE bucket_ts >= ?`+currentMetricTrafficFilter+enabledChanFilter+selectableFilter+` GROUP BY bucket_ts ORDER BY bucket_ts`, since).
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

// storeTokens 按令牌(API Key)聚合窗口内的成功/异常/失败/用量/成本，
// 按用户侧消费→请求数→名称排序取 Top 100，与模型监控另外三个维度保持一致。
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
		FROM token_samples WHERE bucket_ts >= ?`+currentTokenTrafficFilter+` GROUP BY token_name
		ORDER BY quota DESC, (success+anomaly+failed) DESC, k ASC LIMIT 100`, since).Scan(&rows).Error; err != nil {
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
	return m.storeDB.Exec(`INSERT INTO hour_samples (hour_ts, success, anomaly, failed, tokens, quota, sum_use_time, traffic_class_version)
		SELECT (bucket_ts/3600)*3600 AS hour_ts,
		  SUM(success), SUM(anomaly), SUM(failed), SUM(tokens), SUM(quota), SUM(sum_use_time), ?
		FROM metric_samples WHERE bucket_ts >= ? AND traffic_class_version = ?
		GROUP BY hour_ts
		ON CONFLICT(hour_ts) DO UPDATE SET
		  success=excluded.success, anomaly=excluded.anomaly, failed=excluded.failed,
		  tokens=excluded.tokens, quota=excluded.quota, sum_use_time=excluded.sum_use_time,
		  traffic_class_version=excluded.traffic_class_version`,
		userTrafficClassificationVersion, sinceTs, userTrafficClassificationVersion).Error
}

func (m *Monitor) pruneHoursOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("hour_ts < ?", cutoffTs).Delete(&HourSample{})
	return r.RowsAffected, r.Error
}

// storeHourSeries 取小时级序列(长期趋势图用),按时间升序。
func (m *Monitor) storeHourSeries(sinceTs int64) []HourPoint {
	var pts []HourPoint
	warnReadErr("storeHourSeries", m.storeDB.Raw(`SELECT hour_ts AS ts, success, anomaly, failed FROM hour_samples WHERE hour_ts >= ?`+currentHourTrafficFilter+` ORDER BY hour_ts`, sinceTs).Scan(&pts))
	return pts
}

// periodStat 取 [fromTs,toTs) 的小时级汇总统计(同比环比用)。
func (m *Monitor) periodStat(fromTs, toTs int64) PeriodStat {
	var r struct{ S, A, F, Q int64 }
	warnReadErr("periodStat", m.storeDB.Raw(`SELECT COALESCE(SUM(success),0) s, COALESCE(SUM(anomaly),0) a, COALESCE(SUM(failed),0) f, COALESCE(SUM(quota),0) q
		FROM hour_samples WHERE hour_ts >= ? AND hour_ts < ?`+currentHourTrafficFilter, fromTs, toTs).Scan(&r))
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
	warnReadErr("storeFreshness", m.storeDB.Raw(`SELECT COALESCE(MAX(bucket_ts),0) AS m FROM metric_samples WHERE traffic_class_version = ?`, userTrafficClassificationVersion).Scan(&v))
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
	warnReadErr("storeInfraLatest", m.storeDB.Raw(`SELECT s.resource, s.rtype, s.metric, s.value, s.bucket_ts
		FROM infra_samples s
		JOIN (SELECT resource, metric, MAX(bucket_ts) AS mx FROM infra_samples GROUP BY resource, metric) t
		  ON s.resource=t.resource AND s.metric=t.metric AND s.bucket_ts=t.mx`).Scan(&rows))
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
	warnReadErr("storeInfraSeries", m.storeDB.Raw(`SELECT bucket_ts AS ts, value FROM infra_samples
		WHERE resource=? AND metric=? AND bucket_ts >= ? ORDER BY bucket_ts`, resource, metric, since).Scan(&pts))
	return pts
}

func (m *Monitor) pruneInfraOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&InfraSample{})
	return r.RowsAffected, r.Error
}
