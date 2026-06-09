package monitor

import (
	"fmt"
	"log"

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
	ChannelId int    `gorm:"primaryKey;autoIncrement:false"`
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
	if err := db.AutoMigrate(&MetricSample{}, &TokenSample{}, &AlertConfig{}, &AlertLog{}); err != nil {
		return fmt.Errorf("表迁移失败: %w", err)
	}
	m.storeDB = db
	log.Printf("[Monitor] 本地采样库就绪: %s", path)
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

func (m *Monitor) pruneOlderThan(cutoffTs int64) (int64, error) {
	r := m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&MetricSample{})
	m.storeDB.Where("bucket_ts < ?", cutoffTs).Delete(&TokenSample{}) // token 维度一并清理
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
	if err := m.storeDB.Raw(`SELECT '' AS k, `+aggCols+` FROM metric_samples WHERE bucket_ts >= ?`, since).
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
	q := fmt.Sprintf(`SELECT %s AS k, bucket_ts, COALESCE(SUM(success),0) AS success,
		COALESCE(SUM(anomaly),0) AS anomaly, COALESCE(SUM(failed),0) AS failed
		FROM metric_samples WHERE bucket_ts >= ? GROUP BY k, bucket_ts ORDER BY k, bucket_ts`, dimCol)
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
	q := fmt.Sprintf(`SELECT %s AS k, %s FROM metric_samples
		WHERE bucket_ts >= ? GROUP BY %s
		ORDER BY failed DESC, (success+failed) DESC LIMIT 200`, dimCol, aggCols, dimCol)
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
		FROM metric_samples WHERE bucket_ts >= ? GROUP BY bucket_ts ORDER BY bucket_ts`, since).
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

func (m *Monitor) storeFreshness() (lastBucket int64) {
	var v struct{ M int64 }
	m.storeDB.Raw(`SELECT COALESCE(MAX(bucket_ts),0) AS m FROM metric_samples`).Scan(&v)
	return v.M
}
