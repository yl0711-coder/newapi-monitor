package monitor

// usage.go:「用户用量」——盯一组指定的 new-api 用户(按邮箱 / 用户ID 添加),
// 按需对生产库 logs 表做窗口化聚合:消费矩阵(用户×日)与单用户详情(每日/分组/模型),费用=quota/500000 美元。
//
// 与采样器的边界:采样器是【常驻周期】查询,这里是【按需】查询——只在打开页面 / 重选日期时执行。
// 可重新计算的聚合结果会进入有界缓存，余额/用户资料/原始日志始终实时读取。源查询全部限定时间范围，
// 并由可取消的单槽闸门控制同一时刻最多一条重查询；具体索引利用情况需以
// 生产库 EXPLAIN 为准，不能仅因 WHERE 中存在索引列就假定一定命中。生产库全程只读。
// 名单存本地 sqlite(tracked_users);鉴权沿用全站约定:管理员可看,仅超管可改名单。

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// usageTZOffsetSec 天粒度按东八区(CST)切日,与团队运营时区一致(日志本身是 unix 秒,无时区)。
	usageTZOffsetSec = 8 * 3600
	maxUsageDays     = 366 // 单次查询时间范围上限;支持近一年(含闰年),仍由分页/导出上限控制返回量
	maxUsageDimRows  = 300 // 分组/模型维度返回上限
	// 前端本来就不渲染超过 2 万格的矩阵。服务端同样在计算与
	// JSON 编码前拒绝该明细域，避免“页面不展示，却照样查询/占内存”。
	usageMatrixMaxCells = 20_000
)

var usageCST = time.FixedZone("CST", usageTZOffsetSec)

const statusClientClosedRequest = 499 // Nginx 约定：客户端主动断开；用于访问日志区分真实服务端 5xx

// acquireUsageSemaphore 是可取消的单个 semaphore 获取操作。取得槽位后再次检查
// context，防止 Done 与空闲槽位同时就绪时把已经取消的请求随机放进数据库。
func acquireUsageSemaphore(ctx context.Context, once *sync.Once, gate *chan struct{}, capacity int) error {
	// ctx 已经结束时不能让 select 在“空闲槽位”和 Done 同时就绪时随机取得槽位。
	if err := ctx.Err(); err != nil {
		return err
	}
	once.Do(func() { *gate = make(chan struct{}, capacity) })
	select {
	case *gate <- struct{}{}:
		// 取消可能与取得槽位同时发生；再次确认并归还自己刚取得的槽位，
		// 不能让已取消请求进入 SQL，也不能误释放其他请求持有的槽位。
		if err := ctx.Err(); err != nil {
			<-*gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// acquireUsageLane 先领取类别泳道，再领取所有来源查询共享的两槽预算。
// 类别泳道避免同类慢请求堆叠；共享预算确保聚合、明细和导出最多并发两条，
// 与生产连接池的第三条采样连接互不争抢。
func updateUsageMetricMax(dst *atomic.Int64, value int64) {
	for old := dst.Load(); value > old && !dst.CompareAndSwap(old, value); old = dst.Load() {
	}
}

func (m *Monitor) acquireUsageLane(ctx context.Context, once *sync.Once, gate *chan struct{}, metrics *usageQueryLaneMetrics) error {
	started := time.Now()
	if err := acquireUsageSemaphore(ctx, once, gate, 1); err != nil {
		metrics.failed.Add(1)
		waited := time.Since(started).Nanoseconds()
		metrics.waitNanos.Add(waited)
		updateUsageMetricMax(&metrics.maxWaitNanos, waited)
		return err
	}
	if err := acquireUsageSemaphore(ctx, &m.usageSourceBudgetOnce, &m.usageSourceBudget, 2); err != nil {
		<-*gate
		metrics.failed.Add(1)
		waited := time.Since(started).Nanoseconds()
		metrics.waitNanos.Add(waited)
		updateUsageMetricMax(&metrics.maxWaitNanos, waited)
		return err
	}
	waited := time.Since(started).Nanoseconds()
	metrics.waitNanos.Add(waited)
	updateUsageMetricMax(&metrics.maxWaitNanos, waited)
	metrics.acquired.Add(1)
	m.usageSourceInUse.Add(1)
	metrics.active.Add(1)
	metrics.holdStartedAt.Store(time.Now().UnixNano())
	return nil
}

func (m *Monitor) releaseUsageLane(gate chan struct{}, metrics *usageQueryLaneMetrics) {
	if started := metrics.holdStartedAt.Swap(0); started > 0 {
		held := time.Now().UnixNano() - started
		if held > 0 {
			metrics.holdNanos.Add(held)
			updateUsageMetricMax(&metrics.maxHoldNanos, held)
		}
	}
	metrics.active.Add(-1)
	m.usageSourceInUse.Add(-1)
	<-m.usageSourceBudget
	<-gate
}

// acquireUsageGate 是聚合/后台事实来源查询泳道。保留稳定名称供既有调用和
// 并发测试使用；日志分页与 CSV 导出分别走独立泳道。
func (m *Monitor) acquireUsageGate(ctx context.Context) error {
	return m.acquireUsageLane(ctx, &m.usageGateOnce, &m.usageGate, &m.usageAggregateMetrics)
}

func (m *Monitor) releaseUsageGate() { m.releaseUsageLane(m.usageGate, &m.usageAggregateMetrics) }

func (m *Monitor) acquireUsageDetailGate(ctx context.Context) error {
	return m.acquireUsageLane(ctx, &m.usageDetailGateOnce, &m.usageDetailGate, &m.usageDetailMetrics)
}

func (m *Monitor) releaseUsageDetailGate() {
	m.releaseUsageLane(m.usageDetailGate, &m.usageDetailMetrics)
}

func (m *Monitor) acquireUsageExportGate(ctx context.Context) error {
	return m.acquireUsageLane(ctx, &m.usageExportGateOnce, &m.usageExportGate, &m.usageExportMetrics)
}

func (m *Monitor) releaseUsageExportGate() {
	m.releaseUsageLane(m.usageExportGate, &m.usageExportMetrics)
}

type usageQueryLaneStats struct {
	Acquired    uint64 `json:"acquired"`
	Failed      uint64 `json:"failed"`
	Active      int64  `json:"active"`
	TotalWaitMS int64  `json:"total_wait_ms"`
	MaxWaitMS   int64  `json:"max_wait_ms"`
	TotalHoldMS int64  `json:"total_hold_ms"`
	MaxHoldMS   int64  `json:"max_hold_ms"`
}

type usageSourceBudgetStats struct {
	Capacity           int                 `json:"capacity"`
	InUse              int                 `json:"in_use"`
	InteractiveWaiters int64               `json:"interactive_waiters"`
	Aggregate          usageQueryLaneStats `json:"aggregate"`
	Detail             usageQueryLaneStats `json:"detail"`
	Export             usageQueryLaneStats `json:"export"`
}

func usageLaneStats(metrics *usageQueryLaneMetrics) usageQueryLaneStats {
	return usageQueryLaneStats{
		Acquired:    metrics.acquired.Load(),
		Failed:      metrics.failed.Load(),
		Active:      metrics.active.Load(),
		TotalWaitMS: time.Duration(metrics.waitNanos.Load()).Milliseconds(),
		MaxWaitMS:   time.Duration(metrics.maxWaitNanos.Load()).Milliseconds(),
		TotalHoldMS: time.Duration(metrics.holdNanos.Load()).Milliseconds(),
		MaxHoldMS:   time.Duration(metrics.maxHoldNanos.Load()).Milliseconds(),
	}
}

func (m *Monitor) usageSourceStats() usageSourceBudgetStats {
	return usageSourceBudgetStats{
		Capacity:           2,
		InUse:              int(m.usageSourceInUse.Load()),
		InteractiveWaiters: m.usageInteractiveWaiters.Load(),
		Aggregate:          usageLaneStats(&m.usageAggregateMetrics),
		Detail:             usageLaneStats(&m.usageDetailMetrics),
		Export:             usageLaneStats(&m.usageExportMetrics),
	}
}

// acquireInteractiveUsageGate 为页面上的重查询登记一个短暂等待标记。标记只覆盖
// “等待拿到闸门”的阶段；一旦页面已持有闸门，后台自然无法并发进入。这样后台
// 事实采集即便正好在前台请求前醒来，也会在查询前检测到用户排队并让路。
func (m *Monitor) acquireInteractiveUsageGate(ctx context.Context) error {
	m.usageInteractiveWaiters.Add(1)
	defer m.usageInteractiveWaiters.Add(-1)
	return m.acquireUsageGate(ctx)
}

func (m *Monitor) acquireInteractiveUsageDetailGate(ctx context.Context) error {
	m.usageInteractiveWaiters.Add(1)
	defer m.usageInteractiveWaiters.Add(-1)
	return m.acquireUsageDetailGate(ctx)
}

func (m *Monitor) acquireInteractiveUsageExportGate(ctx context.Context) error {
	m.usageInteractiveWaiters.Add(1)
	defer m.usageInteractiveWaiters.Add(-1)
	return m.acquireUsageExportGate(ctx)
}

func isCanceledUsageRequest(c *gin.Context, err error) bool {
	return errors.Is(err, context.Canceled) || errors.Is(c.Request.Context().Err(), context.Canceled)
}

// abortCanceledUsageRequest 把浏览器主动取消与真正的服务端故障分开。
// 内部查询超时仍返回 false，由调用方按真实错误记录和响应。
func abortCanceledUsageRequest(c *gin.Context, err error) bool {
	if !isCanceledUsageRequest(c, err) {
		return false
	}
	c.Status(statusClientClosedRequest)
	c.Writer.WriteHeaderNow()
	return true
}

// usageDayExprMySQL 把 created_at(unix 秒)折算成 CST 日序号(自 epoch 起第几天)。
// MySQL 整除用 DIV;测试里(sqlite)用 usageDayExpr 字段覆盖为 '/'(sqlite 整型相除即整除)。
const usageDayExprMySQL = "(created_at + 28800) DIV 86400"

// ---- 聚合查询(生产库,只读、窗口化、走索引) ----

// dayExpr 返回日桶 SQL 表达式;测试用 usageDayExpr 覆盖成 sqlite 兼容写法。
func (m *Monitor) dayExpr() string {
	if m.usageDayExpr != "" {
		return m.usageDayExpr
	}
	return usageDayExprMySQL
}

// UsageBilling 是所有用量聚合共用的计费口径。
// CostUSD 保留原接口语义，始终表示消费毛额；退款与净消费分别显式返回，避免旧前端被静默改义。
// 聚合、排序和相加全部使用整数 quota，美元只作为最终展示值，避免浮点累计误差。
type UsageBilling struct {
	Requests      int64   `json:"requests"`       // 消费请求数(type=2)，保持现有请求数口径
	RefundRecords int64   `json:"refund_records"` // 退款日志数(type=6)，不混入请求数
	ConsumeQuota  int64   `json:"consume_quota"`
	RefundQuota   int64   `json:"refund_quota"`
	NetQuota      int64   `json:"net_quota"`
	CostUSD       float64 `json:"cost_usd"` // 兼容字段：消费毛额
	RefundUSD     float64 `json:"refund_usd"`
	NetUSD        float64 `json:"net_usd"`
}

func (b *UsageBilling) finalize() {
	b.NetQuota = b.ConsumeQuota - b.RefundQuota
	b.CostUSD = float64(b.ConsumeQuota) / quotaPerUSD
	b.RefundUSD = float64(b.RefundQuota) / quotaPerUSD
	b.NetUSD = float64(b.NetQuota) / quotaPerUSD
}

func (b *UsageBilling) add(other UsageBilling) {
	b.Requests += other.Requests
	b.RefundRecords += other.RefundRecords
	b.ConsumeQuota += other.ConsumeQuota
	b.RefundQuota += other.RefundQuota
	b.finalize()
}

// UsageDaily 某 CST 自然日的合计。
type UsageDaily struct {
	UsageBilling
	Date             string `json:"date"`
	PromptTokens     int64  `json:"prompt_tokens"`
	CompletionTokens int64  `json:"completion_tokens"`
	Tokens           int64  `json:"tokens"`
}

// UsageDailyModel 某 CST 自然日、某模型的合计(供每日消耗按模型堆叠展示用)。
type UsageDailyModel struct {
	UsageBilling
	Date   string `json:"date"`
	Model  string `json:"model"`
	Other  bool   `json:"other,omitempty"` // true=非 Top 模型的完整归并桶，不是名为“其他”的真实模型
	Tokens int64  `json:"tokens"`
}

// UsageDim 某维度取值(分组 / 模型 / 用户)的合计。
type UsageDim struct {
	UsageBilling
	Key    string `json:"key"`
	Tokens int64  `json:"tokens"`
}

// UsageStats 一次用户用量查询的完整结果(详情页专用:单用户的每日/分组/模型)。
type UsageStats struct {
	From             string            `json:"from"`
	To               string            `json:"to"`
	Summary          UsageDim          `json:"summary"`
	Daily            []UsageDaily      `json:"daily"`
	DailyByModel     []UsageDailyModel `json:"daily_by_model"`
	ByGroup          []UsageDim        `json:"by_group"`
	ByModel          []UsageDim        `json:"by_model"`
	ByGroupTruncated bool              `json:"by_group_truncated"`
	ByModelTruncated bool              `json:"by_model_truncated"`
}

// newUsageStatsRange 构造详情统计的空范围骨架。它只提供前端需要的日期边界，
// 不把尚未读取到的范围统计伪装成零值；用于正常聚合初始化，以及聚合暂不可用时的
// 受限降级响应。
func newUsageStatsRange(fromTs, toTs int64) *UsageStats {
	return &UsageStats{
		From:         time.Unix(fromTs, 0).In(usageCST).Format("2006-01-02"),
		To:           time.Unix(toTs-1, 0).In(usageCST).Format("2006-01-02"),
		Daily:        []UsageDaily{},
		DailyByModel: []UsageDailyModel{},
		ByGroup:      []UsageDim{},
		ByModel:      []UsageDim{},
	}
}

// usageIn 生成 "<col> IN (?,?,…)" 片段与参数(ids 已由调用方保证非空;col 只传代码内常量,勿传用户输入)。
func usageIn(col string, ids []int64) (string, []any) {
	ph := make([]string, len(ids))
	args := make([]any, len(ids))
	for i, id := range ids {
		ph[i] = "?"
		args[i] = id
	}
	return col + " IN (" + strings.Join(ph, ",") + ")", args
}

// computeUsageStats 对 [fromTs, toTs) 内、指定用户集合的消费与退款日志(type=2/6)做三路聚合。
// tokenID>0 时再按令牌过滤(单用户详情下钻单令牌;与 user_id 双条件,隔离不依赖 token 归属校验)。
// 串行化(usageGate):同一时刻最多一条聚合在生产库上跑，等待也计入 20 秒总时限。
func (m *Monitor) computeUsageStats(ctx context.Context, ids []int64, fromTs, toTs, tokenID int64) (*UsageStats, error) {
	if len(ids) == 0 {
		return &UsageStats{}, nil // 名单为空不该走到这;防御:不拼 "IN ()" 非法 SQL
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.acquireInteractiveUsageGate(cctx); err != nil {
		return nil, fmt.Errorf("等待用量查询槽位失败: %w", err)
	}
	defer m.releaseUsageGate()

	inSQL, inArgs := usageIn("user_id", ids)
	where := "type IN (2,6) AND created_at >= ? AND created_at < ? AND " + inSQL +
		" AND NOT (" + m.channelTestSourcePredicateSQL() + ")"
	args := append([]any{fromTs, toTs}, inArgs...)
	if tokenID > 0 {
		where += " AND token_id = ?"
		args = append(args, tokenID)
	}

	st := newUsageStatsRange(fromTs, toTs)

	// 1) 每日:日桶 = CST 日序号,回来再折成日期文本。
	dailyQ := "SELECT " + m.dayExpr() + " AS day_idx," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN 1 ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=6 THEN 1 ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(prompt_tokens,0) ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)" +
		" FROM logs WHERE " + where + " GROUP BY day_idx ORDER BY day_idx"
	rows, err := m.prodDB.QueryContext(cctx, dailyQ, args...)
	if err != nil {
		return nil, fmt.Errorf("按日聚合失败: %w", err)
	}
	for rows.Next() {
		var dayIdx int64
		var d UsageDaily
		if err := rows.Scan(&dayIdx, &d.Requests, &d.RefundRecords, &d.PromptTokens, &d.CompletionTokens, &d.ConsumeQuota, &d.RefundQuota); err != nil {
			rows.Close()
			return nil, err
		}
		d.Date = time.Unix(dayIdx*86400-usageTZOffsetSec, 0).In(usageCST).Format("2006-01-02")
		d.Tokens = d.PromptTokens + d.CompletionTokens
		d.finalize()
		st.Daily = append(st.Daily, d)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	for _, d := range st.Daily { // 汇总卡直接由日聚合累加,不再多查一遍
		st.Summary.Requests += d.Requests
		st.Summary.RefundRecords += d.RefundRecords
		st.Summary.Tokens += d.Tokens
		st.Summary.ConsumeQuota += d.ConsumeQuota
		st.Summary.RefundQuota += d.RefundQuota
	}
	st.Summary.finalize()

	// 2/3) 按分组 / 模型。列名 group 是保留字,必须反引号。
	// (曾有第三路 GROUP BY user_id:前端改成矩阵+单用户详情后无人消费,纯耗生产库,已删。)
	dims := []struct {
		col       string
		dst       *[]UsageDim
		truncated *bool
		desc      string
	}{
		{"COALESCE(`group`,'')", &st.ByGroup, &st.ByGroupTruncated, "按分组"},
		{"COALESCE(model_name,'')", &st.ByModel, &st.ByModelTruncated, "按模型"},
	}
	for _, dim := range dims {
		q := "SELECT " + dim.col + " AS k," +
			" CAST(COALESCE(SUM(CASE WHEN type=2 THEN 1 ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=6 THEN 1 ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(prompt_tokens,0)+COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)" +
			" FROM logs WHERE " + where +
			" GROUP BY k ORDER BY SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END) DESC, k" +
			" LIMIT " + strconv.Itoa(maxUsageDimRows+1)
		rows, err := m.prodDB.QueryContext(cctx, q, args...)
		if err != nil {
			return nil, fmt.Errorf("%s聚合失败: %w", dim.desc, err)
		}
		for rows.Next() {
			var r UsageDim
			if err := rows.Scan(&r.Key, &r.Requests, &r.RefundRecords, &r.Tokens, &r.ConsumeQuota, &r.RefundQuota); err != nil {
				rows.Close()
				return nil, err
			}
			r.finalize()
			*dim.dst = append(*dim.dst, r)
		}
		rows.Close()
		if err := rows.Err(); err != nil {
			return nil, err
		}
		if len(*dim.dst) > maxUsageDimRows {
			*dim.truncated = true
			*dim.dst = (*dim.dst)[:maxUsageDimRows]
		}
	}

	// 1b) 按日×模型。先由完整区间模型排名确定 Top 6，其余模型在 SQL 内归并为单一“其他”桶。
	// 因此返回行数天然不超过 天数×7，不再用固定 LIMIT 截掉某些日期/模型而让趋势少算。
	const topDailyModels = 6
	const otherModelSentinel = "__newapi_monitor_other_models__"
	topModels := make([]string, 0, topDailyModels)
	for _, r := range st.ByModel {
		if len(topModels) == topDailyModels {
			break
		}
		topModels = append(topModels, r.Key)
	}
	if len(topModels) > 0 {
		ph := make([]string, len(topModels))
		dmArgs := make([]any, 0, len(topModels)+len(args))
		for i, model := range topModels {
			ph[i] = "?"
			dmArgs = append(dmArgs, model)
		}
		dmArgs = append(dmArgs, args...)
		modelExpr := "CASE WHEN COALESCE(model_name,'') IN (" + strings.Join(ph, ",") + ")" +
			" THEN COALESCE(model_name,'') ELSE '" + otherModelSentinel + "' END"
		dailyModelQ := "SELECT " + m.dayExpr() + " AS day_idx, " + modelExpr + " AS model_bucket," +
			" CAST(COALESCE(SUM(CASE WHEN type=2 THEN 1 ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=6 THEN 1 ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(prompt_tokens,0)+COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)," +
			" CAST(COALESCE(SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)" +
			" FROM logs WHERE " + where + " GROUP BY day_idx, model_bucket ORDER BY day_idx, model_bucket"
		dmRows, err := m.prodDB.QueryContext(cctx, dailyModelQ, dmArgs...)
		if err != nil {
			return nil, fmt.Errorf("按日按模型聚合失败: %w", err)
		}
		for dmRows.Next() {
			var dayIdx int64
			var dm UsageDailyModel
			if err := dmRows.Scan(&dayIdx, &dm.Model, &dm.Requests, &dm.RefundRecords, &dm.Tokens, &dm.ConsumeQuota, &dm.RefundQuota); err != nil {
				dmRows.Close()
				return nil, err
			}
			dm.Date = time.Unix(dayIdx*86400-usageTZOffsetSec, 0).In(usageCST).Format("2006-01-02")
			if dm.Model == otherModelSentinel {
				dm.Model, dm.Other = "其他", true
			}
			dm.finalize()
			st.DailyByModel = append(st.DailyByModel, dm)
		}
		dmRows.Close()
		if err := dmRows.Err(); err != nil {
			return nil, err
		}
	}
	return st, nil
}

// ---- 列表页矩阵数据(前端渲染为 行=用户 × 列=日期,格=当日消费) ----

// UsageMatrixUser 矩阵列头(一个被盯用户)+ 区间合计。
type UsageMatrixUser struct {
	UserID         int64    `json:"user_id"`
	Username       string   `json:"username"`
	Email          string   `json:"email"`
	GroupID        int64    `json:"group_id"`
	GroupName      string   `json:"group_name"`
	Note           string   `json:"note"`
	ConsumeQuota   int64    `json:"consume_quota"`
	RefundQuota    int64    `json:"refund_quota"`
	NetQuota       int64    `json:"net_quota"`
	TotalUSD       float64  `json:"total_usd"` // 兼容字段：所选范围消费毛额
	RefundUSD      float64  `json:"refund_usd"`
	NetUSD         float64  `json:"net_usd"`
	BalanceQuota   *int64   `json:"balance_quota"`
	TotalUsedQuota *int64   `json:"total_used_quota"`
	BalanceUSD     *float64 `json:"balance_usd"`    // 主站当前余额(users.quota 折美元);null=主站已删/取不到
	TotalUsedUSD   *float64 `json:"total_used_usd"` // 主站累计总消耗(users.used_quota 折美元;终身值);null=已删/取不到
}

// UsageMatrixCell 稀疏格:某用户某天的消费(无消费的天不出格)。
type UsageMatrixCell struct {
	UsageBilling
	UserID int64  `json:"user_id"`
	Date   string `json:"date"`
}

// UsageMatrix 列表页数据:days 连续日期(新→旧)+ 用户(按累计总消耗降序,稳定)+ 稀疏格。
type UsageMatrix struct {
	From  string            `json:"from"`
	To    string            `json:"to"`
	Days  []string          `json:"days"`
	Users []UsageMatrixUser `json:"users"`
	Cells []UsageMatrixCell `json:"cells"`
}

// newUsageMatrixRange 构造一个只包含日期轴的矩阵骨架。它不读取主站日志，既用于
// 正常查询的初始化，也用于“资料可读、范围消费聚合暂不可用”的受限降级响应。这样前端
// 可以继续展示成员、余额和累计消耗，同时不会把未知的范围消费误显示为 0。
func newUsageMatrixRange(fromTs, toTs int64) *UsageMatrix {
	if toTs <= fromTs {
		day := time.Unix(fromTs, 0).In(usageCST).Format("2006-01-02")
		return &UsageMatrix{From: day, To: day}
	}
	mx := &UsageMatrix{
		From: time.Unix(fromTs, 0).In(usageCST).Format("2006-01-02"),
		To:   time.Unix(toTs-1, 0).In(usageCST).Format("2006-01-02"),
	}
	// Facts readiness may clamp the right edge to an hour in the current CST
	// day.  Build the display axis from calendar-day boundaries rather than
	// assuming toTs is next-day midnight; otherwise today's returned cells have
	// no matching column.
	firstDay := usageFactDayStart(fromTs)
	lastDay := usageFactDayStart(toTs - 1)
	for ts := lastDay; ts >= firstDay; ts -= usageFactDaySeconds {
		mx.Days = append(mx.Days, time.Unix(ts, 0).In(usageCST).Format("2006-01-02"))
	}
	return mx
}

// usageMatrixExceedsCellBudget 按成员数×自然日数实施返回量护栏。
// parseUsageRange 产生的区间是 CST 自然日左闭右开，可按固定日秒数计算。
func usageMatrixExceedsCellBudget(memberCount int, fromTs, toTs int64) bool {
	if memberCount <= 0 || toTs <= fromTs {
		return false
	}
	days := (toTs - fromTs) / usageFactDaySeconds
	return days > 0 && int64(memberCount) > int64(usageMatrixMaxCells)/days
}

func usageMatrixCellBudgetMessage(memberCount int, fromTs, toTs int64) string {
	days := (toTs - fromTs) / usageFactDaySeconds
	return fmt.Sprintf("每日消费明细未加载：%d 人 × %d 天超过 %d 格上限，请缩短日期范围或查看单个公司/成员", memberCount, days, usageMatrixMaxCells)
}

// computeUsageMatrix 一条 GROUP BY user_id×日 的聚合,窗口与索引约束同 computeUsageStats。
func (m *Monitor) computeUsageMatrix(ctx context.Context, ids []int64, fromTs, toTs int64) (*UsageMatrix, error) {
	mx := newUsageMatrixRange(fromTs, toTs)
	if len(ids) == 0 {
		return mx, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	if err := m.acquireInteractiveUsageGate(cctx); err != nil {
		return nil, fmt.Errorf("等待用量查询槽位失败: %w", err)
	}
	defer m.releaseUsageGate()

	inSQL, inArgs := usageIn("user_id", ids)
	q := "SELECT user_id, " + m.dayExpr() + " AS day_idx," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN 1 ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=6 THEN 1 ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)" +
		" FROM logs WHERE type IN (2,6) AND created_at >= ? AND created_at < ? AND " + inSQL +
		" AND NOT (" + m.channelTestSourcePredicateSQL() + ")" +
		" GROUP BY user_id, day_idx"
	rows, err := m.prodDB.QueryContext(cctx, q, append([]any{fromTs, toTs}, inArgs...)...)
	if err != nil {
		return nil, fmt.Errorf("矩阵聚合失败: %w", err)
	}
	defer rows.Close()
	for rows.Next() {
		var uid, dayIdx int64
		var billing UsageBilling
		if err := rows.Scan(&uid, &dayIdx, &billing.Requests, &billing.RefundRecords, &billing.ConsumeQuota, &billing.RefundQuota); err != nil {
			return nil, err
		}
		billing.finalize()
		mx.Cells = append(mx.Cells, UsageMatrixCell{
			UsageBilling: billing,
			UserID:       uid,
			Date:         time.Unix(dayIdx*86400-usageTZOffsetSec, 0).In(usageCST).Format("2006-01-02"),
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return mx, nil // 用户列(含合计与排序)由 handler 结合名单组装
}

// parseUsageRange 只接受 CST 自然日。返回左闭右开区间：
// [开始日 00:00:00, 结束日次日 00:00:00)，避免 23:59:59/小数秒边界遗漏。
// 空值默认近 7 个自然日(今天及前 6 天)。
func parseUsageRange(fromStr, toStr string, now time.Time) (fromTs, toTs int64, err error) {
	today := now.In(usageCST).Truncate(0)
	y, mo, d := today.Date()
	todayStart := time.Date(y, mo, d, 0, 0, 0, 0, usageCST)

	parseDay := func(raw, label string) (time.Time, error) {
		t, e := time.ParseInLocation("2006-01-02", strings.TrimSpace(raw), usageCST)
		if e != nil {
			return time.Time{}, fmt.Errorf("%s格式应为 YYYY-MM-DD", label)
		}
		return t, nil
	}

	to := todayStart
	if toStr != "" {
		if to, err = parseDay(toStr, "结束日期"); err != nil {
			return 0, 0, err
		}
	}
	from := to.AddDate(0, 0, -6) // 默认近 7 天(含今天)
	if fromStr != "" {
		if from, err = parseDay(fromStr, "开始日期"); err != nil {
			return 0, 0, err
		}
	}
	if from.After(to) {
		from, to = to, from
	}
	// 含两端点共 N 天 ⇔ 零点差 (N-1)*24h；用 >= 卡住“比上限多一天”的范围。
	if to.Sub(from) >= time.Duration(maxUsageDays)*24*time.Hour {
		return 0, 0, fmt.Errorf("时间范围过大,最长 %d 天", maxUsageDays)
	}
	return from.Unix(), to.AddDate(0, 0, 1).Unix(), nil // to 含当天 → 上界取次日 0 点(开区间)
}

// adminUsageAggregateKey 只标识可重算的数字聚合。用户集合取服务端 ID 指纹，
// 日期/令牌/统计类型均入键；用户名、邮箱、余额等实时字段不进键也不进值。
func adminUsageAggregateKey(kind, memberFP string, uid, tokenID, fromTs, toTs int64) string {
	return fmt.Sprintf("agg:%s:admin:m:%s:r:%d:%d:u:%d:t:%d:%s",
		usageAggregateKeyVersion, memberFP, fromTs, toTs, uid, tokenID, kind)
}

func usageRefreshRequested(c *gin.Context) bool {
	return c.Query("refresh") == "1"
}

func (m *Monitor) loadUsageAggregateJSON(
	ctx context.Context,
	key string,
	ttl time.Duration,
	forceFresh bool,
	dst any,
	fill func() (any, error),
) error {
	if forceFresh {
		return m.usageCache.DoJSONFresh(ctx, key, ttl, dst, fill)
	}
	return m.usageCache.DoJSON(ctx, key, ttl, dst, fill)
}

// loadUsageAggregateJSONStaleIfError 只服务于页面的核心汇总数据域。它先按正常
// 缓存口径查询；仅当新鲜结果已失效且本次源查询失败时，才回退到同键最近成功的本机
// 结果，并由调用方把 stale 状态明确回传给页面。权限、原始日志和配置写入不得使用它。
func (m *Monitor) loadUsageAggregateJSONStaleIfError(
	ctx context.Context,
	key string,
	ttl time.Duration,
	toTs int64,
	forceFresh bool,
	dst any,
	fill func() (any, error),
) (stale bool, err error) {
	staleGrace := usageAggregateStaleGrace(toTs, time.Now())
	if forceFresh {
		return m.usageCache.DoJSONFreshStaleIfError(ctx, key, ttl, staleGrace, dst, fill)
	}
	return m.usageCache.DoJSONStaleIfError(ctx, key, ttl, staleGrace, dst, fill)
}

// usageAggregateAuthorizationGuard gives administrator aggregates the same
// publication fence as Portal.  The handler body (including cache lookup) is
// buffered until the durable ServingGeneration, member fingerprint and read
// bounds are unchanged.  This covers both DB-commit -> atomic-publication and
// revoke -> repair -> republish ABA windows.
func (m *Monitor) usageAggregateAuthorizationGuard(next gin.HandlerFunc) gin.HandlerFunc {
	return func(c *gin.Context) {
		if !m.usageFactsReadRequested() || m.usageFactsLocalSnapshotReadOnly() {
			// A mounted read-only snapshot has no publication writer and therefore
			// no DB->atomic hand-off window. Its startup checkpoint is the authority;
			// legacy snapshots may intentionally predate PublishedMember rows.
			next(c)
			return
		}
		for attempt := 0; attempt < 2; attempt++ {
			before, err := m.loadUsageFactServingReadSnapshot(c.Request.Context())
			if err != nil {
				c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地用量事实正在切换，请稍后重试"})
				return
			}

			original := c.Writer
			buffered := newPortalBufferedResponseWriter(original)
			func() {
				c.Writer = buffered
				defer func() { c.Writer = original }()
				next(c)
			}()
			if buffered.Status() < 200 || buffered.Status() >= 300 {
				buffered.commitTo(original)
				return
			}

			after, err := m.loadUsageFactServingReadSnapshot(c.Request.Context())
			if err == nil && before.equal(after) {
				buffered.commitTo(original)
				return
			}
			if attempt == 0 {
				continue
			}
			c.JSON(http.StatusServiceUnavailable, gin.H{"error": "本地用量事实发布状态已变化，请稍后重试"})
			return
		}
	}
}

// ---- HTTP 处理器 ----

// serveUsageMatrix GET /usage/matrix?from=&to=(管理员):列表页矩阵数据(前端渲染为 行=用户 × 列=日期)。
// 用户 label 取邮箱(缺则用户名/#id)并按【累计总消耗】降序(稳定,不随日期区间变);零消费用户仍保留。
func (m *Monitor) serveUsageMatrix(c *gin.Context) {
	if !m.usageReadServingEnabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	tracked, err := m.listTrackedForUsageRead(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	tracked, balances, usedTotals := m.refreshTrackedLabelsForRead(c.Request.Context(), tracked) // 身份标签校准 + 取当前余额与累计总消耗
	if err := c.Request.Context().Err(); errors.Is(err, context.Canceled) {
		abortCanceledUsageRequest(c, err)
		return
	}
	memberFP := portalMemberFingerprint(tracked)
	forceFresh := usageRefreshRequested(c)
	readRange := m.resolveUsageAggregateReadRange(fromTs, toTs)
	requestedRange := newUsageMatrixRange(fromTs, toTs)
	requestedFrom, requestedTo := requestedRange.From, requestedRange.To
	fromTs, toTs = readRange.From, readRange.To
	// 成员资料/余额与范围消费矩阵是不同的数据域。矩阵暂不可读时仍返回资料列，
	// 并由前端把范围消费明确标成“不可用”，不能把整页变成错误页或显示假 0。
	mx := requestedRange
	matrixAvailable := readRange.Available
	dataStale := false
	matrixMessage := readRange.Message
	if matrixAvailable {
		mx = newUsageMatrixRange(fromTs, toTs)
	}
	if matrixAvailable && len(tracked) > 0 && usageMatrixExceedsCellBudget(len(tracked), fromTs, toTs) {
		matrixAvailable = false
		matrixMessage = usageMatrixCellBudgetMessage(len(tracked), fromTs, toTs)
	} else if matrixAvailable && len(tracked) > 0 {
		mx = &UsageMatrix{}
		dataStale, err = m.loadUsageAggregateJSONStaleIfError(
			c.Request.Context(),
			m.usageFactCacheKey(adminUsageAggregateKey("matrix", memberFP, 0, 0, fromTs, toTs)),
			usageAggregateTTL(toTs, time.Now()),
			toTs,
			forceFresh,
			mx,
			func() (any, error) {
				result, err := m.computeUsageMatrixForRead(c.Request.Context(), idsOf(tracked), fromTs, toTs)
				if result != nil {
					// 管理端身份、余额与分组字段在响应阶段实时组装，不进 Redis。
					result.Users = nil
				}
				return result, err
			},
		)
		if err != nil {
			if abortCanceledUsageRequest(c, err) {
				return
			}
			matrixAvailable = false
			readRange.Partial = false
			matrixMessage = m.usageFactsUnavailableMessage(err, "每日消费明细")
			mx = requestedRange
			slog.Warn("用户用量矩阵聚合失败，保留资料快照", "err", err)
		}
	}
	totals := map[int64]UsageBilling{}
	for _, cell := range mx.Cells {
		t := totals[cell.UserID]
		t.add(cell.UsageBilling)
		totals[cell.UserID] = t
	}
	gm := m.groupNameMap()
	for _, u := range tracked {
		t := totals[u.UserID]
		mu := UsageMatrixUser{UserID: u.UserID, Username: u.Username, Email: u.Email,
			GroupID: u.GroupID, GroupName: gm[u.GroupID], Note: u.Note,
			ConsumeQuota: t.ConsumeQuota, RefundQuota: t.RefundQuota, NetQuota: t.NetQuota,
			TotalUSD: t.CostUSD, RefundUSD: t.RefundUSD, NetUSD: t.NetUSD}
		if b, ok := balances[u.UserID]; ok {
			bq := b
			bv := float64(bq) / quotaPerUSD
			mu.BalanceQuota, mu.BalanceUSD = &bq, &bv
		}
		if uq, ok := usedTotals[u.UserID]; ok {
			usedQ := uq
			uv := float64(usedQ) / quotaPerUSD
			mu.TotalUsedQuota, mu.TotalUsedUSD = &usedQ, &uv
		}
		mx.Users = append(mx.Users, mu)
	}
	// 排序按【累计总消耗】降序(终身值,与所选日期区间无关)——切换时间范围顺序不变,大客户恒在前;
	// 同值(如都为0/已删)按用户名兜底,保证顺序完全稳定。
	usedOf := func(u UsageMatrixUser) int64 {
		if u.TotalUsedQuota != nil {
			return *u.TotalUsedQuota
		}
		return 0
	}
	sort.SliceStable(mx.Users, func(i, j int) bool {
		ui, uj := usedOf(mx.Users[i]), usedOf(mx.Users[j])
		if ui != uj {
			return ui > uj
		}
		return mx.Users[i].Username < mx.Users[j].Username
	})
	// 只有管理员重新选择日期的主动刷新才失效客户端同范围结果。
	// 普通打开/返回列表不再每次删缓存，避免客户端随后又扫一次生产库。
	if forceFresh && matrixAvailable {
		m.invalidatePortalAggregates(tracked, fromTs, toTs)
	}
	dataMessage := ""
	if matrixAvailable {
		dataStale, dataMessage = m.usageDataStaleness(dataStale, fromTs, toTs, time.Now())
	}
	resp := gin.H{
		"enabled":          true,
		"matrix":           mx,
		"empty":            len(tracked) == 0,
		"matrix_available": matrixAvailable,
		"data_stale":       dataStale,
		"range_partial":    matrixAvailable && readRange.Partial,
		"requested_from":   requestedFrom,
		"requested_to":     requestedTo,
	}
	if matrixAvailable && readRange.Partial {
		resp["range_message"] = readRange.Message
	} else if !matrixAvailable {
		resp["matrix_message"] = matrixMessage
	} else if dataStale {
		resp["data_message"] = dataMessage
	}
	c.JSON(http.StatusOK, resp)
}

func quotaUSDPtr(quota *int64) *float64 {
	if quota == nil {
		return nil
	}
	v := float64(*quota) / quotaPerUSD
	return &v
}

// userLiveUsage 用一次 users 主键查询取回单用户的展示名、当前余额和累计总消耗。
// 用户不存在/查询失败时保留既有降级语义：owner=#ID，两项金额为 nil(前端显示 —)。
func (m *Monitor) userLiveUsage(ctx context.Context, id int64) (owner string, balance, used *int64) {
	owner = fmt.Sprintf("#%d", id)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var username, email string
	var balanceQ, usedQ int64
	err := m.prodDB.QueryRowContext(cctx,
		"SELECT COALESCE(username,''), COALESCE(email,''), COALESCE(quota,0), COALESCE(used_quota,0) FROM users WHERE id = ?", id,
	).Scan(&username, &email, &balanceQ, &usedQ)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Warn("查询用户实时资料失败", "err", err, "user_id", id)
		}
		return owner, nil, nil
	}
	if username != "" {
		owner = username
	} else if email != "" {
		owner = email
	}
	return owner, &balanceQ, &usedQ
}

// TokenUsage 单个令牌在时间范围内的用量。MaskedKey 永远是脱敏串,服务端绝不返回明文 key。
type TokenUsage struct {
	UsageBilling
	TokenID        int64    `json:"token_id"` // 主站 tokens.id;0=老日志无token_id(不可下钻)
	Owner          string   `json:"owner"`    // 令牌所属用户(展示名:用户名/邮箱/#ID)
	Name           string   `json:"name"`
	MaskedKey      string   `json:"masked_key"`
	Group          string   `json:"group"` // 令牌绑定的分组(计价档);空=跟随用户默认分组/已删
	Tokens         int64    `json:"tokens"`
	TotalCostQuota *int64   `json:"total_cost_quota"`
	TotalCostUSD   *float64 `json:"total_cost_usd"` // 累计总消耗(tokens.used_quota 折美元;创建至今终身值,不受日期范围影响);null=令牌已不可查(硬删/老日志无token_id)
	Deleted        bool     `json:"deleted"`        // 已删除令牌(软删有消费仍显示/硬删兜底行);前端沉底+标记,与现存令牌分区
}

// tokenUsageAggregate 是可进 Redis 的按令牌日志聚合：只保留计费数字、token_id 和
// 硬删令牌需要的日志名回退。当前令牌名/分组/脱敏 key/删除状态/累计消耗不进缓存。
type tokenUsageAggregate struct {
	UsageBilling
	TokenID int64  `json:"token_id"`
	LogName string `json:"log_name,omitempty"`
	Tokens  int64  `json:"tokens"`
}

// tokenMetaOf 取单令牌元数据(名称/脱敏key/分组/累计/是否已删),强制归属校验(id+user_id 双条件)。
// 查不到(硬删/不属于该用户)返回 nil,不报错——令牌详情页此时只展示日志侧数据。
func (m *Monitor) tokenMetaOf(ctx context.Context, uid, tokenID int64) *TokenUsage {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var name, key, group string
	var used int64
	var deleted bool
	q := "SELECT COALESCE(name,''), COALESCE(`key`,''), COALESCE(`group`,''), CAST(COALESCE(used_quota,0) AS SIGNED), (deleted_at IS NOT NULL)" +
		" FROM tokens WHERE id = ? AND user_id = ?"
	if err := m.prodDB.QueryRowContext(cctx, q, tokenID, uid).Scan(&name, &key, &group, &used, &deleted); err != nil {
		return nil
	}
	total := float64(used) / quotaPerUSD
	return &TokenUsage{TokenID: tokenID, Name: name, MaskedKey: maskTokenKey(key), Group: group, TotalCostQuota: &used, TotalCostUSD: &total, Deleted: deleted}
}

// maskTokenKey 与 new-api 的 MaskTokenKey 同风格。tokens.key 不含 sk- 前缀,
// 客户实际用的是 sk-<key>,故脱敏串带 sk- 前缀以便客户辨认,同时绝不暴露完整 key。
func maskTokenKey(key string) string {
	n := len(key)
	switch {
	case n == 0:
		return ""
	case n <= 4:
		return strings.Repeat("*", n)
	case n <= 8:
		return "sk-" + key[:2] + "****" + key[n-2:]
	default:
		return "sk-" + key[:4] + "**********" + key[n-4:]
	}
}

// computeUserTokenUsage 列出某用户的全部现存令牌(即使范围内零用量),叠加 [fromTs,toTs) 消费日志的按令牌聚合;
// 已删除但范围内有用量的令牌也保留一行(名称/key 尽量回查,查不到回退日志名)。
// 每行带累计总消耗(tokens.used_quota,创建至今终身值);生产库只读;key 只在服务端脱敏后返回,明文永不出库。
// 排序:现存令牌在前、已删除沉底,区内按范围费用降序。
func (m *Monitor) computeUserTokenUsage(ctx context.Context, uid, fromTs, toTs int64) ([]TokenUsage, error) {
	aggregates, err := m.computeUserTokenAggregates(ctx, uid, fromTs, toTs)
	if err != nil {
		return nil, err
	}
	return m.hydrateUserTokenUsage(ctx, uid, aggregates, nil)
}

// computeUserTokenAggregates 只扫时间窗内 logs，结果不含 users/tokens 表的当前资料。
func (m *Monitor) computeUserTokenAggregates(ctx context.Context, uid, fromTs, toTs int64) ([]tokenUsageAggregate, error) {
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m.acquireInteractiveUsageGate(cctx); err != nil {
		return nil, fmt.Errorf("等待令牌查询槽位失败: %w", err)
	}
	defer m.releaseUsageGate()

	q := "SELECT token_id, COALESCE(MAX(token_name),'')," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN 1 ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=6 THEN 1 ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(prompt_tokens,0)+COALESCE(completion_tokens,0) ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=2 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)," +
		" CAST(COALESCE(SUM(CASE WHEN type=6 THEN COALESCE(quota,0) ELSE 0 END),0) AS SIGNED)" +
		" FROM logs WHERE type IN (2,6) AND user_id = ? AND created_at >= ? AND created_at < ?" +
		" AND NOT (" + m.channelTestSourcePredicateSQL() + ")" +
		" GROUP BY token_id"
	rows, err := m.prodDB.QueryContext(cctx, q, uid, fromTs, toTs)
	if err != nil {
		return nil, fmt.Errorf("按令牌聚合失败: %w", err)
	}
	out := make([]tokenUsageAggregate, 0)
	for rows.Next() {
		var a tokenUsageAggregate
		if err := rows.Scan(&a.TokenID, &a.LogName, &a.Requests, &a.RefundRecords, &a.Tokens, &a.ConsumeQuota, &a.RefundQuota); err != nil {
			rows.Close()
			return nil, err
		}
		a.UsageBilling.finalize()
		out = append(out, a)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// hydrateUserTokenUsage 用当前 users/tokens 主键数据补齐令牌列表。这些查询不扫 logs，
// 因此可在每次响应时执行，既保持实时性又不给日志库增加聚合压力。
// 响应层已同时取回 users 资料时传入 ownerOverride，避免重复查询 users；
// 传 nil 时保持 computeUserTokenUsage 等独立调用的原有行为。
func (m *Monitor) hydrateUserTokenUsage(ctx context.Context, uid int64, aggregates []tokenUsageAggregate, ownerOverride *string) ([]TokenUsage, error) {
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	owner := fmt.Sprintf("#%d", uid)
	if ownerOverride != nil {
		if *ownerOverride != "" {
			owner = *ownerOverride
		}
	} else {
		owner = m.usageOwnerLabel(cctx, uid)
	}
	byTok := make(map[int64]tokenUsageAggregate, len(aggregates))
	ids := make([]int64, 0, len(aggregates))
	for _, a := range aggregates {
		byTok[a.TokenID] = a
		if a.TokenID > 0 {
			ids = append(ids, a.TokenID)
		}
	}

	// tokens 表:该用户全部现存令牌(零用量也要展示)+ 范围内出现过的已删令牌(软删,名称/key 仍可回查)。
	// used_quota = 令牌创建至今累计消耗;deleted_at 判定软删;key、group 是保留字,MySQL 需反引号。
	type tokInfo struct {
		name, mask, group string
		usedQuota         int64
		deleted           bool
	}
	infoByID := map[int64]*tokInfo{}
	kq := "SELECT id, COALESCE(name,''), COALESCE(`key`,''), COALESCE(`group`,''), CAST(COALESCE(used_quota,0) AS SIGNED), (deleted_at IS NOT NULL)" +
		" FROM tokens WHERE user_id = ? AND (deleted_at IS NULL"
	kargs := []any{uid}
	if len(ids) > 0 {
		inSQL, inArgs := usageIn("id", ids)
		kq += " OR " + inSQL
		kargs = append(kargs, inArgs...)
	}
	kq += ")"
	krows, err := m.prodDB.QueryContext(cctx, kq, kargs...)
	if err != nil {
		return nil, fmt.Errorf("查询令牌信息失败: %w", err)
	}
	for krows.Next() {
		var id, used int64
		var name, key, group string
		var deleted bool
		if err := krows.Scan(&id, &name, &key, &group, &used, &deleted); err != nil {
			krows.Close()
			return nil, err
		}
		infoByID[id] = &tokInfo{name: name, mask: maskTokenKey(key), group: group, usedQuota: used, deleted: deleted}
	}
	krows.Close()
	if err := krows.Err(); err != nil {
		return nil, err
	}

	out := make([]TokenUsage, 0, len(infoByID)+len(byTok))
	// tokens 表里的每个令牌都出一行(现存令牌零用量补零;软删且范围内有用量的也在此列)
	for tid, info := range infoByID {
		a := byTok[tid]
		delete(byTok, tid)
		name := info.name
		if name == "" {
			name = a.LogName
		}
		if name == "" {
			name = "(未命名)"
		}
		total := float64(info.usedQuota) / quotaPerUSD
		out = append(out, TokenUsage{
			UsageBilling:   a.UsageBilling,
			TokenID:        tid,
			Owner:          owner,
			Name:           name,
			MaskedKey:      info.mask,
			Group:          info.group,
			Tokens:         a.Tokens,
			TotalCostQuota: &info.usedQuota,
			TotalCostUSD:   &total,
			Deleted:        info.deleted,
		})
	}
	// 剩下的是 tokens 表查不到的:硬删令牌/老日志 token_id=0 → 回退日志名,key/分组/累计留空,归入已删除区
	for tid, a := range byTok {
		name := a.LogName
		if name == "" {
			name = "(未命名)"
		}
		out = append(out, TokenUsage{
			UsageBilling: a.UsageBilling,
			TokenID:      tid,
			Owner:        owner,
			Name:         name,
			Tokens:       a.Tokens,
			Deleted:      true,
		})
	}
	sort.SliceStable(out, func(i, j int) bool {
		if out[i].Deleted != out[j].Deleted {
			return !out[i].Deleted // 现存令牌在前,已删除沉底(前端按此分区渲染)
		}
		if out[i].ConsumeQuota != out[j].ConsumeQuota {
			return out[i].ConsumeQuota > out[j].ConsumeQuota
		}
		var ti, tj int64
		if out[i].TotalCostQuota != nil {
			ti = *out[i].TotalCostQuota
		}
		if out[j].TotalCostQuota != nil {
			tj = *out[j].TotalCostQuota
		}
		if ti != tj {
			return ti > tj
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

// usageOwnerLabel 只读主站 users 主键行，用于在响应边界给已缓存的令牌聚合补回实时展示名。
// 用户名/邮箱不进入 Redis；查询失败时保留既有 #ID 降级语义。
func (m *Monitor) usageOwnerLabel(ctx context.Context, uid int64) string {
	owner := fmt.Sprintf("#%d", uid)
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var username, email string
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COALESCE(username,''), COALESCE(email,'') FROM users WHERE id = ?", uid).Scan(&username, &email); err != nil {
		return owner
	}
	if username != "" {
		return username
	}
	if email != "" {
		return email
	}
	return owner
}

// LogRow 一条日志(逐条明细,给客户端「使用日志」查看/导出用);不含请求/响应正文。
// 错误日志的 detail 按中转站口径保留 logs.content 原文，便于按原始 status/错误文本排障。
type LogRow struct {
	ID                int64    `json:"id"`
	CreatedAt         int64    `json:"created_at"`
	Member            string   `json:"member"` // 成员用户名(日志写入时记录)
	Type              int      `json:"type"`   // 1充值 2消费 3管理 4系统 5错误 6退款
	TokenName         string   `json:"token_name"`
	ModelName         string   `json:"model_name"`
	Group             string   `json:"group"`
	PromptTokens      int64    `json:"prompt_tokens"`
	CompletionTokens  int64    `json:"completion_tokens"`
	UseTime           int64    `json:"use_time"`                      // 总耗时(秒)
	IsStream          bool     `json:"is_stream"`                     // 流式请求
	FirstByteMs       int64    `json:"first_byte_ms"`                 // 首字延迟(毫秒);仅流式且有值时>0
	CostUSD           float64  `json:"cost_usd"`                      // 费用(美元);仅消费(type=2)有值,其它类型 quota 恒为0不代表费用,置0且前端/CSV留空
	Detail            string   `json:"detail"`                        // 详情摘要(消费=计价摘要/退款=退款文案/其它=content)
	RequestID         string   `json:"request_id"`                    // new-api logs.request_id,同官方客户端使用日志展示;无则空
	CacheReadTokens   int64    `json:"cache_read_tokens"`             // 缓存读 tokens(other.cache_tokens);供 tokens 列下方展示,同 new-api
	CacheWriteTokens  int64    `json:"cache_write_tokens"`            // 缓存写 tokens(优先取 5m/1h 拆分之和,否则 other.cache_creation_tokens)
	LogContent        string   `json:"log_content,omitempty"`         // 对齐 new-api 展开区"日志详情"(renderLogContent,一句逗号连接的话);仅消费且非阶梯计费有值
	OtherContent      string   `json:"other_content,omitempty"`       // 对齐 new-api 展开区"其他详情"(logs[i].content,仅消费(type=2)且非空时展示;已过 scrubContent 纵深防御)
	BillingProcess    string   `json:"billing_process,omitempty"`     // 对齐 new-api 展开区"计费过程"(renderModelPrice,含具体计算公式);见 buildBillingProcess 覆盖范围
	RequestPath       string   `json:"request_path,omitempty"`        // other.request_path,new-api 展开区可见字段(非渠道/内部信息)
	TaskID            string   `json:"task_id,omitempty"`             // 退款(type=6)/异步任务消费(type=2)关联任务ID
	RefundReason      string   `json:"refund_reason,omitempty"`       // 退款(type=6)失败原因
	ReasoningEffort   string   `json:"reasoning_effort,omitempty"`    // 推理强度,new-api 展开区可见字段
	IsModelMapped     bool     `json:"is_model_mapped,omitempty"`     // 是否发生了模型映射;为真时前端加"请求并计费模型"(=model_name)/"实际模型"两行
	UpstreamModelName string   `json:"upstream_model_name,omitempty"` // 映射后实际请求的上游模型名
	ParamOverride     []string `json:"param_override,omitempty"`      // 参数覆盖审计行(见 logOther.PO)
	SubPlan           string   `json:"sub_plan,omitempty"`            // 订阅套餐:"#planId planTitle"
	SubInstance       string   `json:"sub_instance,omitempty"`        // 订阅实例:"#subscriptionId"
	SubSettlement     string   `json:"sub_settlement,omitempty"`      // 订阅结算:预扣/结算差额/最终抵扣三行(\n 分隔)
	SubRemain         string   `json:"sub_remain,omitempty"`          // 订阅剩余:"remain/total 额度"
	BillingSource     string   `json:"billing_source,omitempty"`      // other.billing_source,=="subscription" 时前端加"订阅说明"固定提示行
}

// logTypeName 日志类型码 → 中文名(与 new-api LogType 常量一致)。
func logTypeName(t int) string {
	switch t {
	case 1:
		return "充值"
	case 2:
		return "消费"
	case 3:
		return "管理"
	case 4:
		return "系统"
	case 5:
		return "错误"
	case 6:
		return "退款"
	default:
		return "其它"
	}
}

// logOther 日志 other JSON 里【仅】我们要用的安全字段:首字延迟 + 计价摘要所需的价格/倍率。
// 渠道等内部字段(channel_id/channel_name/admin_info…)不在此结构 → 天然不解析、不外传。
type logOther struct {
	FRT                     float64  `json:"frt"`
	ModelPrice              *float64 `json:"model_price"`
	ModelRatio              *float64 `json:"model_ratio"`
	GroupRatio              *float64 `json:"group_ratio"`
	UserGroupRatio          *float64 `json:"user_group_ratio"`
	CacheTokens             float64  `json:"cache_tokens"`
	CacheRatio              *float64 `json:"cache_ratio"`
	CacheCreationTokens     float64  `json:"cache_creation_tokens"`
	CacheCreationRatio      *float64 `json:"cache_creation_ratio"`
	CacheCreationTokens5m   float64  `json:"cache_creation_tokens_5m"`
	CacheCreationRatio5m    *float64 `json:"cache_creation_ratio_5m"`
	CacheCreationTokens1h   float64  `json:"cache_creation_tokens_1h"`
	CacheCreationRatio1h    *float64 `json:"cache_creation_ratio_1h"`
	Image                   bool     `json:"image"`
	ImageRatio              *float64 `json:"image_ratio"`
	ViolationFeeCode        string   `json:"violation_fee_code"`
	BillingMode             string   `json:"billing_mode"`        // "tiered_expr"=阶梯计费(此时 model_ratio/model_price 均为0,不能当标准单价展示)
	CompletionRatio         *float64 `json:"completion_ratio"`    // 输出倍率,new-api 展开区"计费过程"计算输出价格要用
	RequestPath             string   `json:"request_path"`        // 请求路径,普通用户可见字段(非渠道/内部信息)
	TaskID                  string   `json:"task_id"`             // 退款(type=6)关联的异步任务ID / 消费(type=2)异步任务日志的关联任务ID
	Reason                  string   `json:"reason"`              // 退款(type=6)失败原因,普通用户可见
	IsModelMapped           bool     `json:"is_model_mapped"`     // 是否发生了模型映射(展开区"请求并计费模型"/"实际模型"两行)
	UpstreamModelName       string   `json:"upstream_model_name"` // 映射后实际请求的上游模型名
	ReasoningEffort         string   `json:"reasoning_effort"`    // 推理强度,new-api 展开区可见字段
	IsTask                  bool     `json:"is_task"`             // 异步任务日志(与 task_id 任一命中即算任务日志,决定"计费过程"是否走任务预扣费文案)
	Claude                  bool     `json:"claude"`              // 该请求最终以 Claude Messages 格式转发给上游;日志详情/计费过程走 claude 专用公式(缓存读取价格恒展示+缓存创建含5m/1h拆分)
	BillingSource           string   `json:"billing_source"`      // "subscription" 表示本次由订阅额度结算,非钱包 quota
	SubscriptionID          *int64   `json:"subscription_id"`
	SubscriptionPlanID      *int64   `json:"subscription_plan_id"`
	SubscriptionPlanTitle   string   `json:"subscription_plan_title"`
	SubscriptionPreConsumed *int64   `json:"subscription_pre_consumed"`
	SubscriptionPostDelta   *int64   `json:"subscription_post_delta"`
	SubscriptionConsumed    *int64   `json:"subscription_consumed"`
	SubscriptionRemain      *int64   `json:"subscription_remain"`
	SubscriptionTotal       *int64   `json:"subscription_total"`
	PO                      []string `json:"po"` // 参数覆盖审计行(仅覆盖了审计白名单路径时非空),展开区"参数覆盖"按钮/内容用
}

func parseLogOther(s string) *logOther {
	if s == "" {
		return nil
	}
	var o logOther
	// 容错:单个字段类型漂移(如上游改版把 frt 发成字符串)时 Unmarshal 报错但已解析的字段仍有效,
	// 保留部分结果而不是整行降级;完全不是 JSON 时得到零值结构,行为与 nil 等价(详情回退 content)。
	_ = json.Unmarshal([]byte(s), &o)
	return &o
}

// fmtPriceUSD/trimNum:与 new-api(线上 classic 主题 formatCompactDisplayPrice)一致——美元符号+去尾零数字。
func fmtPriceUSD(v float64) string { return "$" + trimNum(v, 6) }
func trimNum(v float64, digits int) string {
	s := strconv.FormatFloat(v, 'f', digits, 64)
	if strings.Contains(s, ".") {
		s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	}
	return s
}

// buildLogContent 对齐 new-api web/classic renderLogContent/renderClaudeLogContent(price 模式,
// 我们没有 ratio 模式的用户偏好开关故只实现这一种):一句中文逗号连接的话——"输入价格 $X / 1M
// tokens，输出价格 $Y / 1M tokens[，缓存读取价格 $Z / 1M tokens][，图片输入价格...]，分组倍率/专属倍率 Wx"。
// 仅消费(type=2)且非阶梯计费(tiered_expr,此时 model_ratio/model_price 恒为0不能当单价展示)时有值。
func buildLogContent(logType int, o *logOther) string {
	if logType != 2 || o == nil || o.BillingMode == "tiered_expr" {
		return ""
	}
	// 异步任务日志(model_price=-1,按任务/时长计费)没有"每 1M tokens 多少钱"这回事:
	// 此时 model_ratio 往往是占位的 1,照公式算会展示成"输入价格 $2 / 1M tokens、输出价格 $0",
	// 对客户是错的。与 buildBillingProcess 同一判据,这一行整段不出。
	if (o.IsTask || o.TaskID != "") && o.ModelPrice != nil && *o.ModelPrice == -1 {
		return ""
	}
	ratioText := groupRatioText(o)
	if o.ModelPrice != nil && *o.ModelPrice > 0 {
		return "模型价格 " + fmtPriceUSD(*o.ModelPrice) + " / 次，" + ratioText
	}
	if o.ModelRatio == nil {
		return ""
	}
	in := *o.ModelRatio * 2.0
	parts := []string{
		"输入价格 " + fmtPriceUSD(in) + " / 1M tokens",
		"输出价格 " + fmtPriceUSD(in*completionRatioOf(o)) + " / 1M tokens",
	}
	if o.Claude {
		// renderClaudeLogContent:缓存读取价格恒展示(cache_ratio 默认1.0,同样套 in*ratio 公式);
		// 缓存创建价格按 5m/1h 拆分优先,否则回退未拆分字段——三者最多展示其中的 1 或 2 行。
		cacheRatio := 1.0
		if o.CacheRatio != nil {
			cacheRatio = *o.CacheRatio
		}
		parts = append(parts, "缓存读取价格 "+fmtPriceUSD(in*cacheRatio)+" / 1M tokens")
		hasSplit := o.CacheCreationTokens5m > 0 || o.CacheCreationTokens1h > 0
		if hasSplit {
			if o.CacheCreationTokens5m > 0 && o.CacheCreationRatio5m != nil {
				parts = append(parts, "5m缓存创建价格 "+fmtPriceUSD(in**o.CacheCreationRatio5m)+" / 1M tokens")
			}
			if o.CacheCreationTokens1h > 0 && o.CacheCreationRatio1h != nil {
				parts = append(parts, "1h缓存创建价格 "+fmtPriceUSD(in**o.CacheCreationRatio1h)+" / 1M tokens")
			}
		} else if o.CacheCreationRatio != nil {
			parts = append(parts, "缓存创建价格 "+fmtPriceUSD(in**o.CacheCreationRatio)+" / 1M tokens")
		}
	} else {
		if o.CacheRatio != nil && *o.CacheRatio != 1.0 {
			parts = append(parts, "缓存读取价格 "+fmtPriceUSD(in**o.CacheRatio)+" / 1M tokens")
		}
		if o.Image && o.ImageRatio != nil {
			parts = append(parts, "图片输入价格 "+fmtPriceUSD(in**o.ImageRatio)+" / 1M tokens")
		}
	}
	parts = append(parts, ratioText)
	return strings.Join(parts, "，")
}

// groupRatioText 对齐 getGroupRatioText:"专属倍率 Xx"(user_group_ratio 有效时)或"分组倍率 Xx"。
func groupRatioText(o *logOther) string {
	if o.UserGroupRatio != nil && *o.UserGroupRatio > 0 {
		return "专属倍率 " + trimNum(*o.UserGroupRatio, 6) + "x"
	}
	if o.GroupRatio != nil {
		return "分组倍率 " + trimNum(*o.GroupRatio, 6) + "x"
	}
	return "分组倍率 0x"
}

// completionRatioOf 输出倍率;other.completion_ratio 缺失时按 new-api 默认值 0 处理
// (线上老日志/未落这个字段的记录,renderLogContent 里 _completionRatio ?? 0 同一兜底)。
func completionRatioOf(o *logOther) float64 {
	if o.CompletionRatio != nil {
		return *o.CompletionRatio
	}
	return 0
}

// buildBillingProcess 对齐 new-api web/classic renderModelPrice/renderClaudeModelPrice/
// renderTaskBillingProcess(按量计费主路径,即 model_price==-1 时的分支):算出实际扣费的公式文本,如
// "(输入 1000 tokens / 1M tokens * $2 + 缓存 500 tokens / 1M tokens * $0.2) 输出 200 tokens / 1M
// tokens * $4) * 分组倍率 1.4 = $0.0034"。只覆盖最常见路径(纯文本/Claude 输入输出+缓存读+可选
// 5m/1h拆分缓存创建、异步任务日志);音频/图片/web搜索/文件搜索等边缘计费(我们实际数据里几乎不出现)
// 不做,那些场景仍回退到 buildLogDetail 的摘要行。promptTokens/completionTokens 来自 LogRow(数据库列)。
func buildBillingProcess(logType int, o *logOther, promptTokens, completionTokens int64, rawContent string) string {
	if logType != 2 || o == nil || o.BillingMode == "tiered_expr" {
		return ""
	}
	// 对齐 renderTaskBillingProcess:异步任务日志(is_task 或带 task_id)且尚未按实际用量结算
	// (model_price 恰好等于 -1,即调用方未传具体单价)时——有 task_id 用原始 content(无免责声明行,
	// content 本身就是任务完成后的真实扣费文案);否则(纯 is_task 标记但无 task_id)用固定预扣费文案。
	// 已结算(model_price!=-1 或缺省)的任务日志走下面正常的按量/按次计费路径,不特殊处理。
	if (o.IsTask || o.TaskID != "") && o.ModelPrice != nil && *o.ModelPrice == -1 {
		if o.TaskID != "" {
			return rawContent
		}
		return "任务预扣费（将在任务完成后按实际token重算）\n仅供参考，以实际扣费为准"
	}
	if o.ModelRatio == nil {
		return ""
	}
	if o.ModelPrice != nil && *o.ModelPrice > 0 {
		return "" // 按次计费:new-api 走另一条更简单的分支,我们暂不复刻(极少见,回退摘要行足够)
	}
	groupRatio := 0.0
	ratioText := groupRatioText(o)
	if o.UserGroupRatio != nil && *o.UserGroupRatio > 0 {
		groupRatio = *o.UserGroupRatio
	} else if o.GroupRatio != nil {
		groupRatio = *o.GroupRatio
	}
	inputRatioPrice := *o.ModelRatio * 2.0
	completionRatioPrice := inputRatioPrice * completionRatioOf(o)
	cacheTokens := int64(o.CacheTokens)

	if o.Claude {
		return buildClaudeBillingProcess(o, promptTokens, completionTokens, inputRatioPrice, completionRatioPrice, groupRatio, ratioText)
	}

	var inputDesc string
	if cacheTokens > 0 && o.CacheRatio != nil {
		cachePrice := inputRatioPrice * *o.CacheRatio
		inputDesc = fmt.Sprintf("(输入 %d tokens / 1M tokens * %s + 缓存 %d tokens / 1M tokens * %s",
			promptTokens-cacheTokens, fmtPriceUSD(inputRatioPrice), cacheTokens, fmtPriceUSD(cachePrice))
	} else {
		inputDesc = fmt.Sprintf("(输入 %d tokens / 1M tokens * %s", promptTokens, fmtPriceUSD(inputRatioPrice))
	}
	outputDesc := fmt.Sprintf("输出 %d tokens / 1M tokens * %s) * %s",
		completionTokens, fmtPriceUSD(completionRatioPrice), ratioText)

	textInputTokens := promptTokens - cacheTokens
	if textInputTokens < 0 {
		textInputTokens = 0
	}
	price := float64(textInputTokens)/1e6*inputRatioPrice*groupRatio +
		float64(completionTokens)/1e6*completionRatioPrice*groupRatio
	if cacheTokens > 0 && o.CacheRatio != nil {
		price += float64(cacheTokens) / 1e6 * inputRatioPrice * *o.CacheRatio * groupRatio
	}

	lines := []string{
		"输入价格：" + fmtPriceUSD(inputRatioPrice) + " / 1M tokens",
		"输出价格：" + fmtPriceUSD(completionRatioPrice) + " / 1M tokens",
	}
	if cacheTokens > 0 && o.CacheRatio != nil {
		lines = append(lines, "缓存读取价格："+fmtPriceUSD(inputRatioPrice**o.CacheRatio)+" / 1M tokens")
	}
	lines = append(lines, inputDesc+" + "+outputDesc+" = "+fmtPriceUSD(price))
	lines = append(lines, "仅供参考，以实际扣费为准")
	return strings.Join(lines, "\n")
}

// buildClaudeBillingProcess 对齐 renderClaudeModelPrice 的按量计费分支:与标准分支的区别——Claude
// 语义下 prompt_tokens 已在写入时排除缓存 tokens(见 new-api summary.PromptTokens -= cache*),所以
// effectiveInputTokens 是【累加】缓存/缓存创建换算后的 tokens,而不是从 prompt 里扣除再折算。
func buildClaudeBillingProcess(o *logOther, promptTokens, completionTokens int64, inputRatioPrice, completionRatioPrice, groupRatio float64, ratioText string) string {
	cacheTokens := int64(o.CacheTokens)
	cacheRatio := 1.0
	if o.CacheRatio != nil {
		cacheRatio = *o.CacheRatio
	}
	cacheCreationRatio := 1.0
	if o.CacheCreationRatio != nil {
		cacheCreationRatio = *o.CacheCreationRatio
	}
	cacheCreationTokens5m := int64(o.CacheCreationTokens5m)
	cacheCreationRatio5m := 1.0
	if o.CacheCreationRatio5m != nil {
		cacheCreationRatio5m = *o.CacheCreationRatio5m
	}
	cacheCreationTokens1h := int64(o.CacheCreationTokens1h)
	cacheCreationRatio1h := 1.0
	if o.CacheCreationRatio1h != nil {
		cacheCreationRatio1h = *o.CacheCreationRatio1h
	}
	hasSplit := cacheCreationTokens5m > 0 || cacheCreationTokens1h > 0
	legacyCacheCreationTokens := int64(0)
	if !hasSplit {
		legacyCacheCreationTokens = int64(o.CacheCreationTokens)
	}

	effectiveInputTokens := float64(promptTokens) +
		float64(cacheTokens)*cacheRatio +
		float64(legacyCacheCreationTokens)*cacheCreationRatio +
		float64(cacheCreationTokens5m)*cacheCreationRatio5m +
		float64(cacheCreationTokens1h)*cacheCreationRatio1h
	price := effectiveInputTokens/1e6*inputRatioPrice*groupRatio + float64(completionTokens)/1e6*completionRatioPrice*groupRatio

	breakdown := []string{fmt.Sprintf("提示 %d tokens / 1M tokens * %s", promptTokens, fmtPriceUSD(inputRatioPrice))}
	if cacheTokens > 0 {
		breakdown = append(breakdown, fmt.Sprintf("缓存 %d tokens / 1M tokens * %s", cacheTokens, fmtPriceUSD(inputRatioPrice*cacheRatio)))
	}
	if !hasSplit && legacyCacheCreationTokens > 0 {
		breakdown = append(breakdown, fmt.Sprintf("缓存创建 %d tokens / 1M tokens * %s", legacyCacheCreationTokens, fmtPriceUSD(inputRatioPrice*cacheCreationRatio)))
	}
	if hasSplit && cacheCreationTokens5m > 0 {
		breakdown = append(breakdown, fmt.Sprintf("5m缓存创建 %d tokens / 1M tokens * %s", cacheCreationTokens5m, fmtPriceUSD(inputRatioPrice*cacheCreationRatio5m)))
	}
	if hasSplit && cacheCreationTokens1h > 0 {
		breakdown = append(breakdown, fmt.Sprintf("1h缓存创建 %d tokens / 1M tokens * %s", cacheCreationTokens1h, fmtPriceUSD(inputRatioPrice*cacheCreationRatio1h)))
	}
	breakdown = append(breakdown, fmt.Sprintf("补全 %d tokens / 1M tokens * %s", completionTokens, fmtPriceUSD(completionRatioPrice)))

	lines := []string{
		"输入价格：" + fmtPriceUSD(inputRatioPrice) + " / 1M tokens",
		"输出价格：" + fmtPriceUSD(completionRatioPrice) + " / 1M tokens",
	}
	if cacheTokens > 0 {
		lines = append(lines, "缓存读取价格："+fmtPriceUSD(inputRatioPrice*cacheRatio)+" / 1M tokens")
	}
	if !hasSplit && legacyCacheCreationTokens > 0 {
		lines = append(lines, "缓存创建价格："+fmtPriceUSD(inputRatioPrice*cacheCreationRatio)+" / 1M tokens")
	}
	if hasSplit && cacheCreationTokens5m > 0 {
		lines = append(lines, "5m缓存创建价格："+fmtPriceUSD(inputRatioPrice*cacheCreationRatio5m)+" / 1M tokens")
	}
	if hasSplit && cacheCreationTokens1h > 0 {
		lines = append(lines, "1h缓存创建价格："+fmtPriceUSD(inputRatioPrice*cacheCreationRatio1h)+" / 1M tokens")
	}
	lines = append(lines, strings.Join(breakdown, " + ")+" * "+ratioText+" = "+fmtPriceUSD(price))
	lines = append(lines, "仅供参考，以实际扣费为准")
	return strings.Join(lines, "\n")
}

// buildLogDetail 拼「详情」,逐行对齐 new-api 线上(classic 主题 renderPriceSimpleCore segments 模式):
// 消费=多行(首行 分组/专属倍率,再 输入价、缓存读、5m/1h/缓存创建、图片输入;【不含输出价】,与线上一致);
// 退款=固定文案;错误=原始 content;其余类型及无价格信息的消费=回退到原始 content。行以 \n 分隔,前端首行深色其余灰。
func buildLogDetail(logType int, o *logOther, content string) string {
	if logType == 6 {
		return "异步任务退款"
	}
	if logType == 5 {
		return content // 对齐中转站使用日志：不归类/改写上游错误，保留原始排障信息
	}
	if logType != 2 || o == nil {
		return scrubContent(content) // 充值/管理/系统回退 content:先剔除内部信息(纵深防御)
	}
	if o.ViolationFeeCode != "" {
		return "违规费 " + o.ViolationFeeCode
	}
	// 阶梯计费:model_ratio/model_price 均为 0,按标准单价展示会显示"$0/1M"误导;
	// 我们不复刻线上阶梯专用渲染,回退 content(无则标注),避免展示错误单价。
	if o.BillingMode == "tiered_expr" {
		if content != "" {
			return content
		}
		if o.UserGroupRatio != nil && *o.UserGroupRatio > 0 {
			return "阶梯计费 · 专属倍率 " + trimNum(*o.UserGroupRatio, 6) + "x"
		}
		if o.GroupRatio != nil {
			return "阶梯计费 · 分组倍率 " + trimNum(*o.GroupRatio, 6) + "x"
		}
		return "阶梯计费"
	}
	var lines []string
	// 首行:专属倍率(user_group_ratio 有效时)优先,否则分组倍率
	if o.UserGroupRatio != nil && *o.UserGroupRatio > 0 {
		lines = append(lines, "专属倍率 "+trimNum(*o.UserGroupRatio, 6)+"x")
	} else if o.GroupRatio != nil {
		lines = append(lines, "分组倍率 "+trimNum(*o.GroupRatio, 6)+"x")
	}
	switch {
	case o.ModelPrice != nil && *o.ModelPrice > 0: // 按次计费
		lines = append(lines, "模型价格 "+fmtPriceUSD(*o.ModelPrice))
	case o.ModelRatio != nil: // 按量:倍率1 = $2/1M
		in := *o.ModelRatio * 2.0
		lines = append(lines, "输入 "+fmtPriceUSD(in)+" / 1M tokens")
		if o.CacheTokens != 0 && o.CacheRatio != nil {
			lines = append(lines, "缓存读 "+fmtPriceUSD(in**o.CacheRatio)+" / 1M tokens")
		}
		hasSplit := o.CacheCreationTokens5m > 0 || o.CacheCreationTokens1h > 0
		if hasSplit && o.CacheCreationTokens5m > 0 && o.CacheCreationRatio5m != nil {
			lines = append(lines, "5m缓存创建 "+fmtPriceUSD(in**o.CacheCreationRatio5m)+" / 1M tokens")
		}
		if hasSplit && o.CacheCreationTokens1h > 0 && o.CacheCreationRatio1h != nil {
			lines = append(lines, "1h缓存创建 "+fmtPriceUSD(in**o.CacheCreationRatio1h)+" / 1M tokens")
		}
		if !hasSplit && o.CacheCreationTokens != 0 && o.CacheCreationRatio != nil {
			lines = append(lines, "缓存创建 "+fmtPriceUSD(in**o.CacheCreationRatio)+" / 1M tokens")
		}
		if o.Image && o.ImageRatio != nil {
			lines = append(lines, "图片输入 "+fmtPriceUSD(in**o.ImageRatio)+" / 1M tokens")
		}
	}
	if len(lines) == 0 {
		return scrubContent(content) // 无价格信息的消费(老格式)→ 原始 content(同样先剔内部信息)
	}
	return strings.Join(lines, "\n")
}

// scrubContent 纵深防御:回退展示的 new-api 日志 content 里若含"渠道"字样(如系统日志
// "查看渠道密钥信息 (渠道ID: N)"),整条隐去——正常客户日志不会有这类内部文案,
// 唯一来源是误把管理员账号加进客户组;宁可少显也绝不把渠道信息漏给客户。
//
// 注意:这是【关键字黑名单】,只用于充值/管理/系统/消费这类由 new-api 自己生成、措辞可控的 content。
// 错误日志(type=5)按中转站使用日志约定直接展示原始 content，不经过本函数。
func scrubContent(content string) string {
	if strings.Contains(content, "渠道") {
		return ""
	}
	return content
}

// populateExpandFields 填充 LogRow 里"行展开区"要用的全部字段,严格对齐 new-api web/classic
// useUsageLogsData.jsx setLogsFormat 的【非管理员】分支字段列表与出现顺序(管理员专属字段——渠道信息/
// 拦截原因/流状态/请求转换/计费模式/订单支付方式等——普通客户端本就看不到,此处天然不产出)。
func populateExpandFields(r *LogRow, o *logOther, content string) {
	if o == nil {
		return
	}
	r.LogContent = buildLogContent(r.Type, o)
	if r.Type == 2 && content != "" {
		r.OtherContent = scrubContent(content) // 与 Detail 回退口径一致,先剔"渠道"内部字样纵深防御
	}
	if r.Type == 2 {
		if o.IsModelMapped && o.UpstreamModelName != "" {
			r.IsModelMapped = true
			r.UpstreamModelName = o.UpstreamModelName
		}
		r.BillingProcess = buildBillingProcess(r.Type, o, r.PromptTokens, r.CompletionTokens, content)
		// 已结算的任务日志,计费过程就是 content 原文 → 与"其他详情"完全同一句话,
		// 展开区连着显示两遍同样的内容。留计费过程(那一栏才是讲钱的),去掉重复的其他详情。
		if r.BillingProcess == r.OtherContent {
			r.OtherContent = ""
		}
		if o.ReasoningEffort != "" {
			r.ReasoningEffort = o.ReasoningEffort
		}
	}
	if r.Type == 6 {
		r.TaskID = o.TaskID
		// 与中转站一致，退款失败原因也保留原始文本，避免二次解释影响排障。
		if o.Reason != "" {
			r.RefundReason = o.Reason
		}
	}
	r.RequestPath = o.RequestPath
	if len(o.PO) > 0 {
		r.ParamOverride = o.PO
	}
	if o.BillingSource == "subscription" {
		r.BillingSource = o.BillingSource
		if o.SubscriptionPlanID != nil && *o.SubscriptionPlanID != 0 {
			r.SubPlan = strings.TrimSpace(fmt.Sprintf("#%d %s", *o.SubscriptionPlanID, o.SubscriptionPlanTitle))
		}
		if o.SubscriptionID != nil && *o.SubscriptionID != 0 {
			r.SubInstance = fmt.Sprintf("#%d", *o.SubscriptionID)
		}
		pre := int64(0)
		if o.SubscriptionPreConsumed != nil {
			pre = *o.SubscriptionPreConsumed
		}
		postDelta := int64(0)
		if o.SubscriptionPostDelta != nil {
			postDelta = *o.SubscriptionPostDelta
		}
		finalConsumed := pre + postDelta
		if o.SubscriptionConsumed != nil {
			finalConsumed = *o.SubscriptionConsumed
		}
		deltaSign := ""
		if postDelta > 0 {
			deltaSign = "+"
		}
		r.SubSettlement = fmt.Sprintf("预扣：%d 额度\n结算差额：%s%d 额度\n最终抵扣：%d 额度", pre, deltaSign, postDelta, finalConsumed)
		if o.SubscriptionRemain != nil && o.SubscriptionTotal != nil {
			r.SubRemain = fmt.Sprintf("%d/%d 额度", *o.SubscriptionRemain, *o.SubscriptionTotal)
		}
	}
}

// logFilterWhere 拼日志筛选的公共 WHERE(不含游标/排序/上限);全部用户可控值参数化,无注入。
// 查看(queryGroupLogs)与计数(countGroupLogs)共用,保证两者筛选口径完全一致。logType=0 表示全部类型。
// 类型口径对齐 new-api 官方客户端使用日志(model.GetUserLogs):普通用户可见全部 6 种类型,
// 含错误(5)/退款(6)——官方不在查询层屏蔽,靠写入时已脱敏的 content(见 buildLogDetail/scrubContent)+
// 不回传渠道信息兜底,故此处不再排除 5/6。
func (m *Monitor) logFilterWhere(ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName, detailKw, requestID string) (string, []any) {
	inSQL, inArgs := usageIn("user_id", ids)
	where := "created_at >= ? AND created_at < ? AND " + inSQL +
		" AND NOT (" + m.channelTestSourcePredicateSQL() + ")"
	args := append([]any{fromTs, toTs}, inArgs...)
	if logType > 0 { // 仅看某类型(1-6 全部开放,同 new-api 官方客户端使用日志)
		where += " AND type = ?"
		args = append(args, logType)
	}
	if memberUID > 0 { // 仅看某成员
		where += " AND user_id = ?"
		args = append(args, memberUID)
	}
	if model != "" { // 仅看某模型(精确匹配,与聚合的 by_model key 一致)
		where += " AND model_name = ?"
		args = append(args, model)
	}
	if group != "" { // 仅看某分组(精确匹配,与聚合的 by_group key 一致)
		where += " AND `group` = ?"
		args = append(args, group)
	}
	if tokenName != "" { // 令牌名模糊匹配(参数化+通配符转义,防注入/防 %_ 泛匹配拖慢查询)
		where += " AND token_name LIKE ? ESCAPE '!'"
		args = append(args, "%"+escapeLike(tokenName)+"%")
	}
	if requestID != "" { // request_id 精确匹配,同 new-api GetUserLogs;logs.request_id 有独立索引(idx_logs_request_id)
		where += " AND request_id = ?"
		args = append(args, requestID)
	}
	if detailKw != "" { // 详情关键字模糊搜索:只匹配 DB 原始 content(详情列里由 other 现算的倍率/单价文本不在 content 里,搜不到——可接受,费用列本身已给准确金额)
		pat := "%" + escapeLike(detailKw) + "%"
		if strings.Contains(detailKw, "违规费") {
			// 违规费是 buildLogDetail 用 other.violation_fee_code 现算的展示文案,content 里没有这几个字;
			// 但 other 原始 JSON 里的英文字段名 violation_fee_code 是唯一暴露"这条被判违规扣款"的标记,
			// 客户搜"违规费"时额外把它捞出来(不然这类记录在搜索里完全隐形,无法通过其它列定位)。
			where += " AND (content LIKE ? ESCAPE '!' OR other LIKE '%violation_fee_code%')"
		} else {
			where += " AND content LIKE ? ESCAPE '!'"
		}
		args = append(args, pat)
	}
	return where, args
}

// escapeLike 转义 LIKE 模式里的通配符,使用户输入按字面匹配。
// ESCAPE 字符选 '!'(非反斜杠):反斜杠在 MySQL 与 sqlite 的字符串字面量语义不同,'!' 两边行为一致。
func escapeLike(s string) string {
	return strings.NewReplacer("!", "!!", "%", "!%", "_", "!_").Replace(s)
}

// countGroupLogs 数一组成员在当前筛选下的日志总条数(供前端算总页数)。只在翻页首页调用一次,翻页时前端复用。
func (m *Monitor) countGroupLogs(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName, detailKw, requestID string) (int64, error) {
	if len(ids) == 0 {
		return 0, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m.acquireInteractiveUsageDetailGate(cctx); err != nil {
		return 0, fmt.Errorf("等待日志计数槽位失败: %w", err)
	}
	defer m.releaseUsageDetailGate()
	where, args := m.logFilterWhere(ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID)
	var n int64
	if err := m.prodDB.QueryRowContext(cctx, "SELECT COUNT(*) FROM logs WHERE "+where, args...).Scan(&n); err != nil {
		return 0, fmt.Errorf("日志计数失败: %w", err)
	}
	return n, nil
}

// countGroupLogsSnapshot 为浏览器原生 CSV 下载取回“有界总数 + 当前最大 ID”。
// 导出最多只会读取 portalExportCap 行，因此预检也只探测 cap+1 条：达到该值就
// 足以触发确认，不再为了展示一个精确大数字扫描全部匹配日志。后续分页从
// maxID+1 开始，预检完成后新到的日志不会混入本次文件。
func (m *Monitor) countGroupLogsSnapshot(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName, detailKw, requestID string) (total, startCursor int64, err error) {
	if len(ids) == 0 {
		return 0, 0, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 15*time.Second)
	defer cancel()
	if err := m.acquireInteractiveUsageExportGate(cctx); err != nil {
		return 0, 0, fmt.Errorf("等待日志计数槽位失败: %w", err)
	}
	defer m.releaseUsageExportGate()
	where, args := m.logFilterWhere(ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID)
	var maxID int64
	// ORDER BY id DESC 同真实导出顺序；LIMIT 使用编译期常量，不接收用户输入。
	// 少于等于上限时 total 精确，超过时固定返回 cap+1 作为“至少超限”的哨兵。
	q := "SELECT COUNT(*), COALESCE(MAX(id), 0) FROM (SELECT id FROM logs WHERE " + where +
		" ORDER BY id DESC LIMIT " + strconv.Itoa(portalExportCap+1) + ") AS bounded_export_logs"
	if err := m.prodDB.QueryRowContext(cctx, q, args...).Scan(&total, &maxID); err != nil {
		return 0, 0, fmt.Errorf("日志计数失败: %w", err)
	}
	if maxID == int64(^uint64(0)>>1) {
		return 0, 0, errors.New("日志 ID 超出可导出范围")
	}
	// queryGroupLogs 使用 id < cursor；+1 才包含当前最大 ID。空结果时 maxID=0、
	// cursor=1，确保预检后新到的正数自增 ID 也不会混入这次空快照。
	startCursor = maxID + 1
	return total, startCursor, nil
}

// queryGroupLogs 查一组成员的日志,按 id 倒序游标分页；普通页面走明细泳道。
// 全部用户可控值参数化;memberUID 需调用方已校验属本组;limit 由调用方控上限(分页 pageSize+1 / 导出 cap,超限判定在导出侧用 COUNT 探测)。
// 取 content+other 拼「详情」与首字(only 安全字段);花费/首字/详情按 new-api 的可展示/计时类型口径填。
func (m *Monitor) queryGroupLogs(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName, detailKw, requestID string, beforeID int64, limit int) ([]LogRow, error) {
	return m.queryGroupLogsWithLane(ctx, ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID, beforeID, limit, false)
}

// queryGroupLogsForExport 与页面明细口径完全一致，但使用独立 CSV 泳道。
// 一个慢下载不会占住普通日志页或聚合页的类别锁；共享来源预算仍限制总并发。
func (m *Monitor) queryGroupLogsForExport(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName, detailKw, requestID string, beforeID int64, limit int) ([]LogRow, error) {
	return m.queryGroupLogsWithLane(ctx, ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID, beforeID, limit, true)
}

func (m *Monitor) queryGroupLogsWithLane(ctx context.Context, ids []int64, fromTs, toTs, memberUID int64, logType int, model, group, tokenName, detailKw, requestID string, beforeID int64, limit int, export bool) ([]LogRow, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 25*time.Second) // 导出可能取到 5 万行,给足超时
	defer cancel()
	if export {
		if err := m.acquireInteractiveUsageExportGate(cctx); err != nil {
			return nil, fmt.Errorf("等待日志导出槽位失败: %w", err)
		}
		defer m.releaseUsageExportGate()
	} else if err := m.acquireInteractiveUsageDetailGate(cctx); err != nil {
		return nil, fmt.Errorf("等待日志查询槽位失败: %w", err)
	} else {
		defer m.releaseUsageDetailGate()
	}

	where, args := m.logFilterWhere(ids, fromTs, toTs, memberUID, logType, model, group, tokenName, detailKw, requestID)
	if beforeID > 0 { // 游标:取比上次末尾更早的(id 近似时间序,倒序翻页,不用深 OFFSET)
		where += " AND id < ?"
		args = append(args, beforeID)
	}
	// NewAPI 历史版本与迁移数据可能让计数/耗时/流式标记保留 NULL。
	// 这些字段在客户日志语义上都等价于 0；直接 Scan 到 int64/int 会使
	// 整个分页返回 500。查询层统一归零，既兼容历史数据，也不改变筛选口径。
	q := "SELECT id, created_at, COALESCE(username,''), COALESCE(token_name,''), COALESCE(model_name,''), COALESCE(`group`,'')," +
		" COALESCE(prompt_tokens,0), COALESCE(completion_tokens,0), COALESCE(use_time,0), COALESCE(quota,0), COALESCE(type,0), COALESCE(is_stream,0)," +
		" COALESCE(content,''), COALESCE(other,''), COALESCE(request_id,'')" +
		" FROM logs WHERE " + where + " ORDER BY id DESC LIMIT " + strconv.Itoa(limit)
	rows, err := m.prodDB.QueryContext(cctx, q, args...)
	if err != nil {
		return nil, fmt.Errorf("日志查询失败: %w", err)
	}
	defer rows.Close()
	out := make([]LogRow, 0, limit)
	for rows.Next() {
		var r LogRow
		var quota int64
		var isStream int
		var content, other string
		if err := rows.Scan(&r.ID, &r.CreatedAt, &r.Member, &r.TokenName, &r.ModelName, &r.Group, &r.PromptTokens, &r.CompletionTokens, &r.UseTime, &quota, &r.Type, &isStream, &content, &other, &r.RequestID); err != nil {
			return nil, err
		}
		r.IsStream = isStream != 0
		o := parseLogOther(other)
		// 费用仅消费(type=2)有意义:充值/管理/系统在 new-api 里 quota 恒为 0(金额只写在 content),
		// 折美元会得 $0.00 误导客户对账,故非消费不给 CostUSD,前端/CSV 费用列留空。
		if r.Type == 2 {
			r.CostUSD = float64(quota) / quotaPerUSD
		}
		if r.IsStream && o != nil && o.FRT > 0 {
			r.FirstByteMs = int64(o.FRT)
		}
		if o != nil {
			r.CacheReadTokens = int64(o.CacheTokens)
			// 缓存写:5m/1h 拆分优先(有拆分即用其和),否则回退未拆分的 cache_creation_tokens——
			// 同 buildLogDetail 的 hasSplit 判断口径,避免写 tokens 展示与计价明细数字不一致
			cw5m, cw1h := int64(o.CacheCreationTokens5m), int64(o.CacheCreationTokens1h)
			if cw5m > 0 || cw1h > 0 {
				r.CacheWriteTokens = cw5m + cw1h
			} else {
				r.CacheWriteTokens = int64(o.CacheCreationTokens)
			}
		}
		r.Detail = buildLogDetail(r.Type, o, content)
		populateExpandFields(&r, o, content)
		out = append(out, r)
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	return out, nil
}

// serveUsageStats GET /usage/stats?from=&to=&user_id=[&token_id=](管理员):对名单(或其中一人/其单个令牌)做每日/分组/模型聚合。
func (m *Monitor) serveUsageStats(c *gin.Context) {
	if !m.usageReadServingEnabled() {
		c.JSON(http.StatusOK, gin.H{"enabled": false})
		return
	}
	tracked, err := m.listTrackedForUsageRead(c.Request.Context())
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
		return
	}
	trackedByID := map[int64]TrackedUser{}
	for _, u := range tracked {
		trackedByID[u.UserID] = u
	}
	selected := append([]TrackedUser(nil), tracked...)
	isGroup := false
	var tokenID, selectedUID int64
	// 可选其一:user_id=单用户详情;group_id=公司详情(聚合整组成员,0=未分组成员)
	if f := strings.TrimSpace(c.Query("user_id")); f != "" {
		id, err := strconv.ParseInt(f, 10, 64)
		member, ok := trackedByID[id]
		if err != nil || !ok {
			c.JSON(http.StatusBadRequest, gin.H{"error": "user_id 不在名单内"})
			return
		}
		selectedUID = id
		selected = []TrackedUser{member}
		// 令牌详情:仅在单用户详情下有效;聚合强制 user_id+token_id 双条件,越权令牌只会查出空
		if t := strings.TrimSpace(c.Query("token_id")); t != "" {
			tokenID, err = strconv.ParseInt(t, 10, 64)
			if err != nil || tokenID <= 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "token_id 不合法"})
				return
			}
		}
	} else if g := strings.TrimSpace(c.Query("group_id")); g != "" {
		gid, err := strconv.ParseInt(g, 10, 64)
		if err != nil || gid < 0 {
			c.JSON(http.StatusBadRequest, gin.H{"error": "group_id 不合法"})
			return
		}
		isGroup = true
		selected = selected[:0]
		for _, u := range tracked {
			if u.GroupID == gid {
				selected = append(selected, u)
			}
		}
	}
	ids := idsOf(selected)
	if len(ids) == 0 { // 名单为空:不查生产库,直接空结果
		c.JSON(http.StatusOK, gin.H{"enabled": true, "stats": &UsageStats{}, "empty": true})
		return
	}
	fromTs, toTs, err := parseUsageRange(c.Query("from"), c.Query("to"), time.Now())
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	readRange := m.resolveUsageAggregateReadRange(fromTs, toTs)
	requestedRange := newUsageStatsRange(fromTs, toTs)
	requestedFrom, requestedTo := requestedRange.From, requestedRange.To
	fromTs, toTs = readRange.From, readRange.To
	memberFP := portalMemberFingerprint(selected)
	forceFresh := usageRefreshRequested(c)
	cacheTTL := usageAggregateTTL(toTs, time.Now())
	st := requestedRange
	var dataStale bool
	if readRange.Available {
		st = newUsageStatsRange(fromTs, toTs)
		dataStale, err = m.loadUsageAggregateJSONStaleIfError(
			c.Request.Context(),
			m.usageFactCacheKey(adminUsageAggregateKey("stats", memberFP, selectedUID, tokenID, fromTs, toTs)),
			cacheTTL,
			toTs,
			forceFresh,
			st,
			func() (any, error) {
				return m.computeUsageStatsForRead(c.Request.Context(), ids, fromTs, toTs, tokenID)
			},
		)
	} else {
		err = errUsageFactsNotReady
	}
	if err != nil {
		if abortCanceledUsageRequest(c, err) {
			return
		}
		slog.Warn("用户用量详情聚合失败", "err", err)
		// 用量聚合是详情中的一个数据域，不应把余额、累计消耗和成员资料一并打成失败。
		// 空骨架明确表示“未知”，前端会显示 —，不会把未知范围误当成零消费。
		st = requestedRange
	}
	statsAvailable := err == nil
	statsMessage := ""
	dataMessage := ""
	if statsAvailable {
		dataStale, dataMessage = m.usageDataStaleness(dataStale, fromTs, toTs, time.Now())
	} else {
		dataStale = false
		statsMessage = readRange.Message
		if statsMessage == "" {
			statsMessage = "范围统计暂不可用，余额和累计消耗仍可查看"
		}
	}
	resp := gin.H{
		"enabled":         true,
		"stats":           st,
		"stats_available": statsAvailable,
		"stats_message":   statsMessage,
		"data_stale":      dataStale,
		"range_partial":   statsAvailable && readRange.Partial,
		"requested_from":  requestedFrom,
		"requested_to":    requestedTo,
	}
	if statsAvailable && readRange.Partial {
		resp["range_message"] = readRange.Message
	}
	if dataStale {
		resp["data_message"] = dataMessage
	}
	if isGroup { // 公司详情:成员数 + 余额合计 + 累计总消耗合计(主键 IN 的 SUM)
		balanceQ, usedQ := m.sumUsageLiveQuotasForRead(c.Request.Context(), ids)
		resp["members"] = len(ids)
		resp["balance_quota"] = balanceQ
		resp["balance_usd"] = quotaUSDPtr(balanceQ)
		resp["total_used_quota"] = usedQ
		resp["total_used_usd"] = quotaUSDPtr(usedQ)
	} else if tokenID > 0 { // 令牌详情:元数据(名称/脱敏key/分组/累计;硬删查不到则为 null,前端用点击时的名字兜底)
		resp["token"] = m.tokenMetaOfForRead(c.Request.Context(), ids[0], tokenID)
	} else if len(ids) == 1 { // 单用户详情:个人余额 + 累计总消耗(按当前读取模式取值;null=已删/取不到)+ 各令牌用量
		owner, balanceQ, usedQ := m.userLiveUsageForRead(c.Request.Context(), ids[0])
		resp["balance_quota"] = balanceQ
		resp["balance_usd"] = quotaUSDPtr(balanceQ)
		resp["total_used_quota"] = usedQ
		resp["total_used_usd"] = quotaUSDPtr(usedQ)
		// 令牌明细独立于主统计。它临时不可用时仍返回已经取得的余额、累计消耗、
		// 范围汇总和趋势，避免一个可选下钻拖垮整个“用户用量”详情页。
		tokenDataAvailable := statsAvailable
		tokenDataMessage := ""
		tokenAggregates := make([]tokenUsageAggregate, 0)
		if !statsAvailable {
			tokenDataMessage = "令牌明细未加载：范围统计暂不可用"
		} else {
			err := m.loadUsageAggregateJSON(
				c.Request.Context(),
				m.usageFactCacheKey(adminUsageAggregateKey("tokens", memberFP, ids[0], 0, fromTs, toTs)),
				cacheTTL,
				forceFresh,
				&tokenAggregates,
				func() (any, error) {
					return m.computeUserTokenAggregatesForRead(c.Request.Context(), ids[0], fromTs, toTs)
				},
			)
			if err != nil {
				if abortCanceledUsageRequest(c, err) {
					return
				}
				slog.Warn("单用户令牌用量聚合失败，保留主统计", "err", err, "user_id", ids[0])
				tokenDataAvailable = false
				tokenDataMessage = "令牌明细暂不可用，其他统计正常"
			}
		}
		toks := make([]TokenUsage, 0)
		if tokenDataAvailable {
			toks, err = m.hydrateUserTokenUsageForRead(c.Request.Context(), ids[0], tokenAggregates, &owner)
			if err != nil {
				if abortCanceledUsageRequest(c, err) {
					return
				}
				slog.Warn("查询令牌实时元数据失败，保留主统计", "err", err, "user_id", ids[0])
				tokenDataAvailable = false
				tokenDataMessage = "令牌明细暂不可用，其他统计正常"
				toks = []TokenUsage{}
			}
		}
		resp["by_token"] = toks
		resp["token_data_available"] = tokenDataAvailable
		resp["token_data_message"] = tokenDataMessage
	}
	if forceFresh && statsAvailable {
		if isGroup {
			m.invalidatePortalAggregates(selected, fromTs, toTs)
		} else if selectedUID > 0 {
			m.invalidatePortalUserAggregates(tracked, selectedUID, tokenID, fromTs, toTs)
		}
	}
	c.JSON(http.StatusOK, resp)
}

// sumUsageLiveQuotas 用一次 users 聚合同时取回一组用户的当前余额和累计总消耗。
// 空组/查询失败时两项均返回 nil；有成员但主站已无匹配行时 COALESCE 保留既有的 0 值语义。
func (m *Monitor) sumUsageLiveQuotas(ctx context.Context, ids []int64) (balance, used *int64) {
	if len(ids) == 0 {
		return nil, nil
	}
	cctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	inSQL, args := usageIn("id", ids)
	var balanceQ, usedQ int64
	err := m.prodDB.QueryRowContext(cctx,
		"SELECT COALESCE(SUM(quota),0), COALESCE(SUM(used_quota),0) FROM users WHERE "+inSQL, args...,
	).Scan(&balanceQ, &usedQ)
	if err != nil {
		if !errors.Is(err, context.Canceled) {
			slog.Warn("查询分组实时金额合计失败", "err", err)
		}
		return nil, nil
	}
	return &balanceQ, &usedQ
}
