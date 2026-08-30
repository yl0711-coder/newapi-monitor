package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/trafficclass"
)

const sourceEpochStartupMaxLookbackSec int64 = 3600

// boundedSourceEpochStartupLookback prevents every lease reacquisition or
// transient reconnect from replaying an operator-sized historical window on
// the high-priority sampler lane. The durable local watermark reduces normal
// restarts to a small overlap; gaps beyond one hour require the explicitly
// throttled maintenance backfill instead of a surprise 24-hour GROUP BY.
func boundedSourceEpochStartupLookback(configuredHours int, now, latestBucket int64) int64 {
	if configuredHours <= 0 {
		return 0
	}
	limit := sourceEpochStartupMaxLookbackSec
	if configuredHours < 1 {
		return 0
	}
	if configuredHours == 1 {
		limit = int64(configuredHours) * 3600
	}
	if limit > sourceEpochStartupMaxLookbackSec {
		limit = sourceEpochStartupMaxLookbackSec
	}
	if latestBucket <= 0 {
		return limit
	}
	lookback := now - latestBucket + 120 // overlap two minute buckets
	if lookback < 180 {
		lookback = 180
	}
	if lookback > limit {
		lookback = limit
	}
	return lookback
}

func (m *Monitor) sourceEpochStartupLookbacks(now int64) (int64, int64) {
	if m.cfg.BackfillHours <= 0 {
		return 0, 0
	}
	var metricLatest, tokenLatest int64
	if err := m.storeDB.Raw(`SELECT COALESCE(MAX(bucket_ts),0) FROM metric_samples WHERE traffic_class_version = ?`,
		userTrafficClassificationVersion).Scan(&metricLatest).Error; err != nil {
		metricLatest = 0
	}
	if err := m.storeDB.Raw(`SELECT COALESCE(MAX(bucket_ts),0) FROM token_samples WHERE traffic_class_version = ?`,
		userTrafficClassificationVersion).Scan(&tokenLatest).Error; err != nil {
		tokenLatest = 0
	}
	return boundedSourceEpochStartupLookback(m.cfg.BackfillHours, now, metricLatest),
		boundedSourceEpochStartupLookback(m.cfg.BackfillHours, now, tokenLatest)
}

// sampler.go:唯一访问生产库的组件。每周期对 logs 表做有界小窗口聚合，
// 错误原文按默认 5 分钟周期另取一个完整分钟小窗口；结果均写入本地 SQLite。
// 采样心跳(m.lastRun)与渠道名缓存(m.chNames)都挂在 Monitor 上。

// LastSampleRun 返回采样器最近一次成功运行时刻(0=从未)。
func (m *Monitor) LastSampleRun() int64 { return m.lastRun.Load() }

func (m *Monitor) channelNames() map[string]string {
	m.chMu.RLock()
	defer m.chMu.RUnlock()
	cp := make(map[string]string, len(m.chNames))
	for k, v := range m.chNames {
		cp[k] = v
	}
	return cp
}

// startSampler 启动后台采样(prodDB 未配置则不启动)。
func (m *Monitor) startSampler(ctx context.Context) {
	if m.prodDB == nil {
		return
	}
	if m.cfg.StabilityEnabled {
		if err := m.resetStaleStabilityProblemClassification(); err != nil {
			slog.Warn("重置旧版稳定性问题分类失败，问题页将 fail-closed 隐藏旧数据", "err", err)
			m.problemLastFailure.Store(time.Now().Unix())
		}
	}
	_ = m.refreshChannelsContext(ctx)

	if metricLookback, tokenLookback := m.sourceEpochStartupLookbacks(time.Now().Unix()); metricLookback > 0 || tokenLookback > 0 {
		if n, err := m.sampleWindow(ctx, metricLookback); err != nil {
			slog.Warn("历史回填失败(忽略)", "err", err)
		} else {
			slog.Info("来源 epoch 启动缺口补齐完成", "configured_hours", m.cfg.BackfillHours,
				"effective_seconds", metricLookback, "rows", n)
		}
		if tokenLookback > 0 {
			if err := m.sampleTokens(ctx, tokenLookback); err != nil {
				slog.Warn("token 维度启动缺口补齐失败(忽略,不影响主监控)", "err", err)
			}
		}
		if err := m.rollupHours(time.Now().Unix() - int64(m.cfg.RetentionDays)*86400); err != nil {
			slog.Warn("启动小时汇总失败(忽略)", "err", err)
		}
	}
	// 稳定性历史表只从已经存在的本地分钟桶生成。即使关闭了生产历史回填，
	// 也把本地现有留存转成长期维度表；不会因此多查一次生产库。
	if m.cfg.StabilityEnabled {
		since := time.Now().Unix() - int64(m.cfg.RetentionDays)*86400
		if err := m.rollupStabilityHours(since); err != nil {
			slog.Warn("启动稳定性维度汇总失败(忽略,不影响原监控)", "err", err)
		}
		if err := m.rollupStabilityRejections(since); err != nil {
			slog.Warn("启动稳定性拒绝汇总失败(忽略,不影响原监控)", "err", err)
		}
	}

	interval := time.Duration(m.cfg.SampleSeconds) * time.Second
	if interval < 10*time.Second {
		interval = 10 * time.Second
	}
	// lastRun/dead-man 只能由一次真实成功的来源采样更新。
	// 启动、持有 lease 或本地回填完成都不能伪装新鲜度。
	goSourceEpoch(ctx, func(loopCtx context.Context) { m.loop(loopCtx, interval) })
	slog.Info("采样器已启动", "interval", interval.String(), "note", "生产库仅执行有界小窗口只读查询")
	if m.cfg.StabilityEnabled {
		m.startStabilityBackfillMaintenance(ctx)
		if m.cfg.StabilityClassificationMigrationEnabled && m.stabilityProblemClassificationMigrationActive() {
			goSourceEpoch(ctx, m.runStabilityProblemMigrationLoop)
		}
	}

}

func (m *Monitor) loop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lookback := int64(interval.Seconds())*3 + 60
	var ticks int
	var nextProblemSample int64
	var nextStabilityRollup int64
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now().Unix()
			mainSampleOK := true
			if _, err := m.sampleWindow(ctx, lookback); err != nil {
				slog.Error("采样失败(下周期重试)", "err", err)
				mainSampleOK = false
			} else {
				m.lastRun.Store(now)
				m.heartbeat() // 成功采样后向外部 dead-man 服务打心跳
				if err := m.sampleTokens(ctx, lookback); err != nil {
					slog.Warn("token 维度采样失败(忽略,不影响主监控)", "err", err)
				}
				_ = m.refreshChannelsContext(ctx) // 每周期同步渠道开关；与来源 epoch 一起取消
				m.refreshSelectable()             // 每周期重算"可选(分组,模型)对",监控只统计用户能选到的模型
			}
			if m.cfg.StabilityEnabled {
				// 稳定性是历史报表而非秒级看板：每 5 分钟重算最近两小时已足够
				// 覆盖迟到日志，同时避免每分钟重复扫描本地维度表。
				if mainSampleOK && now >= nextStabilityRollup {
					if err := m.rollupStabilityHours(now - 2*3600); err != nil {
						slog.Warn("稳定性维度汇总失败(忽略,不影响原监控)", "err", err)
					}
					if err := m.rollupStabilityRejections(now - 2*3600); err != nil {
						slog.Warn("稳定性拒绝汇总失败(忽略,不影响原监控)", "err", err)
					}
					nextStabilityRollup = now + 300
				}
				problemEvery := stabilityProblemIntervalSeconds(m.cfg.StabilityProblemSampleSec)
				if now >= nextProblemSample {
					// 延迟 10 分钟再确认完整分钟，覆盖 360 秒长请求和日志落库抖动；高峰积压时
					// 采集器按本地游标续跑，不会把超限窗口直接丢掉。
					problemTargetTo := now - stabilityProblemFinalizeDelaySec
					if _, err := m.sampleStabilityProblems(ctx, problemTargetTo-2*problemEvery-120, problemTargetTo); err != nil {
						m.problemLastFailure.Store(now)
						slog.Warn("稳定性原始错误采样失败(忽略,不影响主采样)", "err", err)
					} else {
						m.problemLastSuccess.Store(now)
					}
					liveFrom := problemTargetTo - 2*problemEvery - 120
					if m.stabilityProblemPendingCountInRange(liveFrom/60*60, problemTargetTo/60*60) > 0 ||
						m.stabilityProblemNeedsCatchup(problemTargetTo) {
						nextProblemSample = now + 60 // 有积压时加快追赶，但每轮读取预算仍固定。
					} else {
						nextProblemSample = now + problemEvery
					}
				}
			}
			// 主维度采样失败也必须继续尝试 problem live；两类业务查询
			// 各自记录水位。只有真实来源生命周期故障才由共享 gate 阻断。
			m.evaluateAlerts(now)
			ticks++
			if !mainSampleOK {
				continue
			}
			if ticks%(int(600/interval.Seconds())+1) == 0 {
				if d := m.cfg.RetentionDays; d > 0 {
					cutoff := time.Now().Unix() - int64(d)*86400
					if n, err := m.pruneOlderThan(cutoff); err == nil && n > 0 {
						slog.Info("清理过期采样", "rows", n)
					}
					if err := m.rollupHours(cutoff); err != nil { // 分钟数据被清前,先滚动汇总进小时表
						slog.Warn("小时汇总失败(忽略)", "err", err)
					}
					if n, err := m.pruneRejectionsOlderThan(cutoff); err == nil && n > 0 {
						slog.Info("清理过期被拒采样", "rows", n)
					}
				}
				if hd := m.cfg.HourRetentionDays; hd > 0 {
					if n, err := m.pruneHoursOlderThan(time.Now().Unix() - int64(hd)*86400); err == nil && n > 0 {
						slog.Info("清理过期小时汇总", "rows", n)
					}
				}
				if m.cfg.StabilityEnabled {
					days := m.cfg.stabilityStorageDays()
					if err := m.pruneStabilityOlderThan(stabilityRetentionCutoff(time.Now().Unix(), days)); err != nil {
						slog.Warn("清理稳定性历史失败(忽略)", "err", err)
					}
				}
			}
		}
	}
}

// 交付异常(B 类)SQL 判据 —— 口径见 文档/NexusAPI/12-上游渠道监控/09。
//
// 为什么用 completion_tokens 作为"是否交付"的唯一信号:
//   - frt(首字延迟)不行:stream_scanner 在任何 data: 行(含 Claude 的 message_start)
//     都会置首响应时间,它只证明上游开口了,不证明用户拿到内容。
//   - prompt_tokens 也不行:上游不返 usage 时 new-api 会本地估算输入并照此扣费,
//     有输入 token 不代表上游真的处理了。
//
// 三个易错点(都实测踩过):
//   - end_reason 必须走 JSON_EXTRACT。other 里另有 end_error 自由文本字段,内容可能含
//     "panic" 等词,对整串做正则会误命中——这正是旧口径误报的来源之一。
//   - anomalyZeroSQL 必须排除天然无输出模型(embedding/rerank/图像生成),
//     否则这些模型会被整类误判成 B1。当前生产零命中,是防御项。
//   - REGEXP 里的字符串字面量是 coercible 的,会跟随列的 utf8mb4_unicode_ci;
//     但换成会话变量(带显式 collation)会抛 Illegal mix of collations。勿改写成变量。
//
// 两个 JSON 取值必须用 COALESCE 兜成非 NULL —— 这是踩过的坑:
// 非流式请求的 other 里没有 stream_status,JSON_EXTRACT 返回 NULL,而
// `NULL IN (...)` = NULL、`FALSE OR NULL` = NULL,于是整个 ANOM 变 NULL,
// `NOT ANOM` 也是 NULL,SUM 会跳过该行 —— 结果 success 恒为 0(异常侧因
// `TRUE OR NULL` = TRUE 反而正常,所以只丢成功数,极难察觉)。
const (
	anomalyZeroSQL = "(completion_tokens = 0 AND model_name NOT REGEXP 'embed|rerank|bge-|m3e|image|seedream|seedance')"
	// REPLACE(CAST(JSON_EXTRACT ... AS CHAR),'\"','') is deliberately used
	// instead of MySQL-only JSON_UNQUOTE.  The extracted values below are
	// closed enum fields, so stripping the JSON string quotes is lossless and
	// keeps the same SQL executable against the SQLite fake-production DB used
	// by local acceptance.
	anomalyEndReasonSQL = "COALESCE(CASE WHEN JSON_VALID(other) THEN REPLACE(CAST(JSON_EXTRACT(other,'$.stream_status.end_reason') AS CHAR),'\"','') END,'')"
	anomalyErrCountSQL  = "COALESCE(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.stream_status.error_count') AS SIGNED) END,0)"
)

// channelTestJSONEnumSQL extracts a closed-enum string from logs.other using
// syntax shared by MySQL and SQLite.  Callers only pass compile-time JSON
// paths; this helper must never receive user input.
func channelTestJSONEnumSQL(path string) string {
	return `COALESCE(CASE WHEN JSON_VALID(other) THEN REPLACE(CAST(JSON_EXTRACT(other,'` + path + `') AS CHAR),'"','') ELSE '' END,'')`
}

// channelTestLogPredicateSQL 只使用未修改 NewAPI 已经持久化的稳定旧标记。
// 两个旧文本字段必须同时命中，避免用户碰巧把普通令牌命名成“模型测试”时被误排除。
// 旧版批量/定时测试失败会经 processChannelError 写 type=5：合成上下文固定为
// root、无 token、无 request_id。正常 HTTP 用户请求会经过鉴权和 request-id 中间件，
// 因此用这组完整特征兼容旧错误日志，而不能只凭 internal 分组猜测。
// 旧日志没有手动/定时与单渠道/全渠道标记，Monitor 统一记为 legacy，
// 不会从调度时间或数量反推出一个无法审计的类别。
// 所有生产 logs 聚合必须复用这个谓词，不能在不同报表里各写一套分类规则。
func channelTestLogPredicateSQL() string {
	return trafficclass.SourceExclusionPredicateSQL
}

// channelTestSourcePredicateSQL is the single source-read boundary used by
// direct usage, facts and problem sampling. The legacy predicate is portable
// across production MySQL and the SQLite fake source used by acceptance tests.
func (m *Monitor) channelTestSourcePredicateSQL() string {
	return channelTestLogPredicateSQL()
}

func channelTestOriginSQL(testPredicate string) string {
	return `CASE WHEN ` + testPredicate + ` THEN 'legacy' ELSE '' END`
}

func channelTestScopeSQL(testPredicate string) string {
	return `CASE WHEN ` + testPredicate + ` THEN 'legacy' ELSE '' END`
}

// channelTestSeriesSQL 保留既有复合主键形状。CostBasis 不在旧表主键中，
// 因此用两个明确的 legacy 系列避免普通和 tiered 成本行互相覆盖；
// 它们是成本口径分桶，不表示手动/定时来源。
func channelTestSeriesSQL(testPredicate string) string {
	billingMode := channelTestJSONEnumSQL("$.billing_mode")
	return `CASE WHEN ` + testPredicate + ` THEN CASE ` +
		`WHEN ` + billingMode + `='tiered_expr' THEN 'legacy_tiered' ` +
		`ELSE 'legacy_base' END ELSE '' END`
}

func channelTestResultSQL(testPredicate string) string {
	return `CASE WHEN ` + testPredicate + ` THEN CASE ` +
		`WHEN type=5 THEN 'failed' WHEN {{ANOM}} THEN 'anomaly' ELSE 'success' END ELSE '' END`
}

func channelTestCostBasisSQL(testPredicate string) string {
	billingMode := channelTestJSONEnumSQL("$.billing_mode")
	return `CASE WHEN ` + testPredicate + ` THEN CASE ` +
		`WHEN ` + billingMode + `='tiered_expr' THEN 'legacy_after_group' ` +
		`ELSE 'legacy_assumed_base' END ELSE '' END`
}

// expandAnomalyPredicates 把 {{ZERO}} / {{STREAMBAD}} / {{ANOM}} 占位符展开成 SQL。
// 占位符用 {{}} 包裹是必要的:裸 ANOM 是 anomaly_billed 等列别名的前缀,直接替换会误伤别名。
// sampleWindow 与 sampleTokens 共用本函数,保证两处口径同源、不会各改一半。
func expandAnomalyPredicates(q string) string {
	streamBad := "(" + anomalyEndReasonSQL + " IN ('timeout','scanner_error','panic','ping_fail') OR " + anomalyErrCountSQL + " > 0)"
	anom := "(" + anomalyZeroSQL + " OR " + streamBad + ")"
	q = strings.ReplaceAll(q, "{{ANOM}}", anom)
	q = strings.ReplaceAll(q, "{{STREAMBAD}}", streamBad)
	q = strings.ReplaceAll(q, "{{ZERO}}", anomalyZeroSQL)
	return q
}

// sampleWindow 查询生产库最近 lookbackSec 秒日志,按"分钟桶×渠道×模型×分组"聚合并写本地。
// 这是全程唯一打到生产库的查询。
func (m *Monitor) sampleWindow(ctx context.Context, lookbackSec int64) (int, error) {
	now := time.Now().Unix()
	// +60 上界留一分钟余量,避免边界那一秒的日志正好落在两次采样之间被漏掉
	// (桶是幂等 UPSERT,重叠采样只会覆盖同一桶,不会重复累加)。
	return m.sampleRange(ctx, now-lookbackSec, now+60)
}

// sampleRange 采集 [fromTs, toTs) 区间的日志并写入本地桶。
// 常规采样与历史回填共用同一条 SQL,保证两者口径绝不会各改一半。
func (m *Monitor) sampleRange(ctx context.Context, fromTs, toTs int64) (int, error) {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	release, err := m.acquireBackgroundSource(cctx)
	if err != nil {
		return 0, err
	}
	defer release()

	rows, err := m.prodDB.QueryContext(cctx, sampleWindowSQL(), fromTs, toTs)
	if err != nil {
		m.reportSourceQueryError(err)
		return 0, err
	}
	defer rows.Close()

	var batch []MetricSample
	for rows.Next() {
		var (
			s           MetricSample
			grp         sql.NullString
			e4, e5, eto int64
		)
		if err := rows.Scan(&s.BucketTs, &s.ChannelID, &s.ModelName, &grp,
			&s.Success, &s.Anomaly, &s.Failed,
			&s.AnomalyBilled, &s.AnomalyFree, &s.AnomalyStream, &s.AnomalyQuota, &s.AnomalySumTime,
			&s.SumUseTime, &s.MaxUseTime, &s.Tokens, &s.Quota, &s.RefundRecords, &s.RefundQuota,
			&e4, &e5, &eto,
			&s.Lat1, &s.Lat2, &s.Lat5, &s.Lat10, &s.Lat30, &s.Lat60, &s.LatInf,
			&s.CompletionTokens,
			&s.Ttft500, &s.Ttft1k, &s.Ttft2k, &s.Ttft5k, &s.Ttft10k, &s.TtftInf, &s.TtftMaxMs); err != nil {
			return 0, err
		}
		s.Grp = grp.String
		s.TrafficClassVersion = userTrafficClassificationVersion
		s.Err4xx, s.Err5xx, s.ErrTimeout = e4, e5, eto
		if other := s.Failed - e4 - e5 - eto; other > 0 {
			s.ErrOther = other
		}
		batch = append(batch, s)
	}
	if err := rows.Err(); err != nil {
		m.reportSourceQueryError(err)
		return 0, err
	}
	if err := m.upsertSamples(batch); err != nil {
		return 0, err
	}
	return len(batch), nil
}

// sampleWindowSQL 组装采样查询。拆成独立函数是为了能在测试里渲染出成品 SQL
// 拿到真实 MySQL 上验证语法与 collation——判据里有 REGEXP 和 JSON_EXTRACT,
// 光靠 Go 侧字符串断言盖不住"打到生产库才报错"这类问题。
func sampleWindowSQL() string {
	// MySQL SUM/布尔聚合返回 DECIMAL,需 CAST 成 SIGNED 才能 Scan 进 int64。
	// 错误分类互斥(优先级:超时 > 5xx > 4xx),四类之和不超过失败数。
	// FRT = 首字延迟(ms),取自 other JSON 的 frt;非法 JSON 或缺失则计 0(被 frt>0 过滤掉)。
	//
	// 交付异常判据见 expandAnomalyPredicates。
	const frt = "(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)"
	q := `
SELECT /*+ MAX_EXECUTION_TIME(8000) */
  (created_at DIV 60)*60 AS bucket,
  channel_id, model_name, ` + "`group`" + ` AS grp,
  CAST(COALESCE(SUM(type=2 AND NOT {{ANOM}}),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(type=2 AND {{ANOM}}),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(type=5),0) AS SIGNED) AS failed,
  CAST(COALESCE(SUM(type=2 AND {{ZERO}} AND prompt_tokens > 0),0) AS SIGNED) AS anomaly_billed,
  CAST(COALESCE(SUM(type=2 AND {{ZERO}} AND prompt_tokens = 0),0) AS SIGNED) AS anomaly_free,
  CAST(COALESCE(SUM(type=2 AND {{STREAMBAD}} AND NOT {{ZERO}}),0) AS SIGNED) AS anomaly_stream,
  CAST(COALESCE(SUM(CASE WHEN type=2 AND {{ZERO}} AND prompt_tokens > 0 THEN quota END),0) AS SIGNED) AS anomaly_quota,
  CAST(COALESCE(SUM(CASE WHEN type=2 AND {{ANOM}} THEN use_time END),0) AS SIGNED) AS anomaly_sum_time,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS sum_use_time,
  CAST(COALESCE(MAX(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS max_use_time,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END),0) AS SIGNED) AS tokens,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN quota END),0) AS SIGNED) AS quota,
  CAST(COALESCE(SUM(type=6),0) AS SIGNED) AS refund_records,
  CAST(COALESCE(SUM(CASE WHEN type=6 THEN quota END),0) AS SIGNED) AS refund_quota,
  CAST(COALESCE(SUM(type=5 AND content REGEXP 'status_code=4'
        AND content NOT LIKE '%timeout%' AND content NOT LIKE '%deadline%'),0) AS SIGNED) AS err_4xx,
  CAST(COALESCE(SUM(type=5 AND content REGEXP 'status_code=5'
        AND content NOT LIKE '%timeout%' AND content NOT LIKE '%deadline%'),0) AS SIGNED) AS err_5xx,
  CAST(COALESCE(SUM(type=5 AND (content LIKE '%timeout%' OR content LIKE '%deadline%')),0) AS SIGNED) AS err_timeout,
  CAST(COALESCE(SUM(type=2 AND use_time<=1),0) AS SIGNED)                 AS lat_1,
  CAST(COALESCE(SUM(type=2 AND use_time>1  AND use_time<=2),0) AS SIGNED) AS lat_2,
  CAST(COALESCE(SUM(type=2 AND use_time>2  AND use_time<=5),0) AS SIGNED) AS lat_5,
  CAST(COALESCE(SUM(type=2 AND use_time>5  AND use_time<=10),0) AS SIGNED) AS lat_10,
  CAST(COALESCE(SUM(type=2 AND use_time>10 AND use_time<=30),0) AS SIGNED) AS lat_30,
  CAST(COALESCE(SUM(type=2 AND use_time>30 AND use_time<=60),0) AS SIGNED) AS lat_60,
  CAST(COALESCE(SUM(type=2 AND use_time>60),0) AS SIGNED)                 AS lat_inf,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN completion_tokens END),0) AS SIGNED) AS completion_tokens,
  CAST(COALESCE(SUM(type=2 AND FRT>0    AND FRT<=500),0)   AS SIGNED) AS ttft_500,
  CAST(COALESCE(SUM(type=2 AND FRT>500  AND FRT<=1000),0)  AS SIGNED) AS ttft_1k,
  CAST(COALESCE(SUM(type=2 AND FRT>1000 AND FRT<=2000),0)  AS SIGNED) AS ttft_2k,
  CAST(COALESCE(SUM(type=2 AND FRT>2000 AND FRT<=5000),0)  AS SIGNED) AS ttft_5k,
  CAST(COALESCE(SUM(type=2 AND FRT>5000 AND FRT<=10000),0) AS SIGNED) AS ttft_10k,
  CAST(COALESCE(SUM(type=2 AND FRT>10000),0)               AS SIGNED) AS ttft_inf,
  CAST(COALESCE(MAX(CASE WHEN type=2 AND FRT>0 THEN FRT END),0) AS SIGNED) AS ttft_max_ms
FROM logs
WHERE created_at >= ? AND created_at < ? AND type IN (2,5,6)
  AND NOT (` + channelTestLogPredicateSQL() + `)
GROUP BY bucket, channel_id, model_name, grp`
	q = expandAnomalyPredicates(q)
	return strings.ReplaceAll(q, "FRT", frt)
}

// BackfillResult 回填结果,回给管理接口。
type BackfillResult struct {
	Hours    int   `json:"hours"`
	Slices   int   `json:"slices"`
	Rows     int   `json:"rows"`
	Failed   int   `json:"failed_slices"`
	ElapsedS int64 `json:"elapsed_sec"`
}

// backfillRunning 保证同一时刻只有一次回填在跑:回填要打 168 次生产库,并发跑会放大压力。
var backfillRunning atomic.Bool

// BackfillHours 用当前判据重算最近 hours 小时的历史桶。
//
// 为什么需要:本地库存的是【算好的结果】而非原始日志。判据一改,只影响此后新采的桶,
// 已存的旧桶仍是旧口径,同一张图里两套口径混着——趋势上出现假台阶,
// 且跨分界点的告警窗口分子分母来自两套口径,阈值会失真。
// 生产 logs 保留期远长于本地分钟级留存,所以旧桶可以重算。
//
// 为什么按小时切片:生产 logs 约 14.5 万行,单条全窗口 GROUP BY 既压生产库又会撞 20 秒超时
// (实测全 7 天查询 >20s)。切成一小时一片后每片都在 1 秒内,压力与常规采样同量级。
// 片间 sleep 让出时间,避免连续 168 次查询把生产库打满。
//
// 覆盖是安全的:upsertSamples 按【分钟桶 × 渠道 × 模型 × 分组】幂等 UPSERT,重算即替换,不累加。
// 但覆盖【不可逆】——旧口径的数值会被冲掉;真要退回需回滚镜像后用旧代码再回填一次。
func (m *Monitor) BackfillHours(ctx context.Context, hours int) (*BackfillResult, error) {
	if m.prodDB == nil {
		return nil, fmt.Errorf("未配置生产库(只读),无法回填")
	}
	if hours <= 0 {
		return nil, fmt.Errorf("hours 需大于 0")
	}
	if max := m.cfg.RetentionDays * 24; max > 0 && hours > max {
		// 超过分钟级留存的部分回填了也会被清理任务删掉,白打生产库。
		return nil, fmt.Errorf("hours 不能超过分钟级留存 %d 小时(RetentionDays=%d)", max, m.cfg.RetentionDays)
	}
	if !backfillRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("已有回填正在进行,请等待其结束")
	}
	defer backfillRunning.Store(false)

	start := time.Now()
	now := start.Unix()
	res := &BackfillResult{Hours: hours}
	slog.Info("开始历史回填", "hours", hours, "note", "只读生产库,按小时切片")

	for i := hours; i >= 1; i-- {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}
		from, to := now-int64(i)*3600, now-int64(i-1)*3600
		res.Slices++
		n, err := m.sampleRange(ctx, from, to)
		if err != nil {
			res.Failed++
			slog.Warn("回填分片失败(跳过)", "from", from, "to", to, "err", err)
		} else {
			res.Rows += n
		}
		// 令牌维度跟着一起补,否则回填完主维度对了、令牌页仍是旧口径的旧数。
		// 与主采样同样的隔离原则:它失败只记日志,不算整体失败。
		if err := m.sampleTokensRange(ctx, from, to); err != nil {
			slog.Warn("回填分片令牌维度失败(忽略)", "from", from, "to", to, "err", err)
		}
		if i > 1 {
			time.Sleep(500 * time.Millisecond) // 让生产库喘口气
		}
	}
	// 小时级汇总(90 天留存,长期趋势用)也是从分钟桶算出来的,必须跟着重算,
	// 否则假台阶会在长期趋势图上留三个月。
	if err := m.rollupHours(now - int64(m.cfg.RetentionDays)*86400); err != nil {
		slog.Warn("回填后小时汇总失败", "err", err)
	}
	if m.cfg.StabilityEnabled {
		if err := m.rollupStabilityHours(now - int64(m.cfg.RetentionDays)*86400); err != nil {
			slog.Warn("回填后稳定性维度汇总失败(忽略)", "err", err)
		}
		if err := m.rollupStabilityRejections(now - int64(m.cfg.RetentionDays)*86400); err != nil {
			slog.Warn("回填后稳定性拒绝汇总失败(忽略)", "err", err)
		}
	}
	res.ElapsedS = int64(time.Since(start).Seconds())
	slog.Info("历史回填完成", "hours", hours, "slices", res.Slices, "rows", res.Rows,
		"failed", res.Failed, "elapsed_sec", res.ElapsedS)
	return res, nil
}

// sampleTokens 按【分钟桶 × 令牌】聚合最近 lookbackSec 秒日志,写本地 token_samples。
// 与主采样隔离:它失败由调用方记日志后继续,绝不影响主监控。
func (m *Monitor) sampleTokens(ctx context.Context, lookbackSec int64) error {
	now := time.Now().Unix()
	return m.sampleTokensRange(ctx, now-lookbackSec, now+60)
}

// sampleTokensRange 采集 [fromTs, toTs) 区间的令牌维度。与 sampleRange 成对,
// 两者都必须是区间式,否则回填时令牌维度会悄悄只补最近一段(主维度补齐、令牌维度错位)。
func (m *Monitor) sampleTokensRange(ctx context.Context, fromTs, toTs int64) error {
	cctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	release, err := m.acquireBackgroundSource(cctx)
	if err != nil {
		return err
	}
	defer release()
	rows, err := m.prodDB.QueryContext(cctx, sampleTokenSQL(), fromTs, toTs)
	if err != nil {
		m.reportSourceQueryError(err)
		return err
	}
	defer rows.Close()
	var batch []TokenSample
	for rows.Next() {
		var s TokenSample
		var tn sql.NullString
		if err := rows.Scan(&s.BucketTs, &tn, &s.Success, &s.Anomaly, &s.Failed, &s.Tokens, &s.Quota); err != nil {
			return err
		}
		s.TokenName = tn.String
		s.TrafficClassVersion = userTrafficClassificationVersion
		batch = append(batch, s)
	}
	if err := rows.Err(); err != nil {
		m.reportSourceQueryError(err)
		return err
	}
	return m.upsertTokenSamples(batch)
}

// sampleTokenSQL remains a separate renderable function so tests can lock the
// server-side execution ceiling as well as the shared classification predicate.
func sampleTokenSQL() string {
	// 判据与 sampleWindow 保持一致(口径必须同源),但按令牌维度不拆明细,控制基数。
	q := `
SELECT /*+ MAX_EXECUTION_TIME(8000) */ (created_at DIV 60)*60 AS bucket, token_name,
  CAST(COALESCE(SUM(type=2 AND NOT {{ANOM}}),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(type=2 AND {{ANOM}}),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(type=5),0) AS SIGNED) AS failed,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END),0) AS SIGNED) AS tokens,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN quota END),0) AS SIGNED) AS quota
FROM logs
WHERE created_at >= ? AND created_at < ? AND type IN (2,5)
  AND NOT (` + channelTestLogPredicateSQL() + `)
GROUP BY bucket, token_name`
	return expandAnomalyPredicates(q)
}

// refreshChannels 刷新渠道 id->name 映射,并把渠道健康快照(类型/状态/分组/模型)写入本地库,
// 供对外看板派生"无可用渠道"。低频、失败保留旧值。仅读非密字段(无 key/凭证)。
func (m *Monitor) refreshChannels() { _ = m.refreshChannelsContext(context.Background()) }

func (m *Monitor) refreshChannelsContext(parent context.Context) error {
	if m.prodDB == nil {
		return errSourceNotReady
	}
	cctx, cancel := context.WithTimeout(parent, 10*time.Second)
	defer cancel()
	release, err := m.acquireBackgroundSource(cctx)
	if err != nil {
		return err
	}
	defer release()
	// type 按 NewAPI 官方映射展示厂商。base_url 只在内存中提取可注册主域名，
	// 本地快照不保存完整 URL/路径，更不读取 key。这只是在原有渠道小查询上多取一列。
	rows, err := m.prodDB.QueryContext(cctx, "SELECT id, name, type, status, `group`, models, base_url FROM channels")
	if err != nil {
		m.reportSourceQueryError(err)
		return err
	}
	defer rows.Close()
	names := map[string]string{}
	var snaps []ChannelSnap
	now := time.Now().Unix()
	prev := m.channelEnabledState() // 上一轮各渠道 (status, enabled_since)
	for rows.Next() {
		var id, channelType, status int
		var name, grp, models, baseURL sql.NullString
		if err := rows.Scan(&id, &name, &channelType, &status, &grp, &models, &baseURL); err != nil {
			return err
		}
		names[strconv.Itoa(id)] = name.String
		p := prev[id] // 不存在或曾被删除都按新建处理，重新出现时从本轮重新计算启用起点。
		if p.deletedAt > 0 {
			p.status, p.since = 0, 0
		}
		enabledSince := nextEnabledSince(status, p.status, p.since, now)
		snaps = append(snaps, ChannelSnap{
			ID: id, Name: name.String, Type: channelType, Vendor: newAPIChannelTypeName(channelType), Status: status,
			BaseDomain: normalizeChannelBaseDomain(baseURL.String), BaseHost: normalizeChannelBaseHost(baseURL.String),
			Groups: grp.String, Models: models.String,
			EnabledSince: enabledSince, UpdatedAt: now,
		})
	}
	if err := rows.Err(); err != nil {
		m.reportSourceQueryError(err)
		return err
	}
	// rows 已成功读到 EOF，因此即使为空也是可信的当前状态。
	// 同步清空内存名称映射，并将本地旧渠道软删除，但保留最后快照。
	m.chMu.Lock()
	m.chNames = names
	m.chMu.Unlock()
	if err := m.replaceChannelSnapsAuthoritative(snaps, now); err != nil {
		slog.Warn("渠道健康快照写入失败(忽略,不影响监控)", "err", err)
	}
	return nil
}

// fetchUsableGroups 从 new-api 的 /api/pricing(匿名可读)取可见分组(用户创建令牌时能选的分组)。
func (m *Monitor) fetchUsableGroups() []string {
	base := strings.TrimRight(m.cfg.NewAPIBaseURL, "/")
	if base == "" {
		return nil
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get(base + "/api/pricing")
	if err != nil {
		return nil
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		return nil
	}
	var body struct {
		UsableGroup map[string]string `json:"usable_group"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&body); err != nil {
		return nil
	}
	out := make([]string, 0, len(body.UsableGroup))
	for k := range body.UsableGroup {
		if k != "" {
			out = append(out, k)
		}
	}
	return out
}

// refreshSelectable 重算"可选 (分组,模型) 对" = 可见分组(/api/pricing) ∩ 启用渠道配置(channel_snaps),
// 写入 selectable_pairs。拉不到可见分组则不动旧表(避免误清空导致监控全过滤为空)。
func (m *Monitor) refreshSelectable() {
	groups := m.fetchUsableGroups()
	if len(groups) == 0 {
		return
	}
	visible := make(map[string]bool, len(groups))
	for _, g := range groups {
		visible[g] = true
	}
	var rows []struct{ Groups, Models string }
	m.storeDB.Raw("SELECT groups, models FROM channel_snaps WHERE status = 1 AND deleted_at = 0").Scan(&rows)
	set := map[[2]string]bool{}
	for _, r := range rows {
		for _, g := range splitList(r.Groups) {
			if !visible[g] {
				continue
			}
			for _, md := range splitList(r.Models) {
				if md != "" && md != "*" {
					set[[2]string{g, md}] = true
				}
			}
		}
	}
	pairs := make([]SelectablePair, 0, len(set))
	for k := range set {
		pairs = append(pairs, SelectablePair{Grp: k[0], Model: k[1]})
	}
	if err := m.replaceSelectablePairs(pairs); err != nil {
		slog.Warn("可选模型对刷新失败(忽略,沿用上次)", "err", err)
	}
}

// heartbeat 向外部 dead-man 服务(如 healthchecks.io)打一次心跳。
// fire-and-forget:5 秒超时、失败忽略,绝不影响采样。未配置 MONITOR_HEARTBEAT_URL 则空操作。
func (m *Monitor) heartbeat() {
	if m.cfg.HeartbeatURL == "" {
		return
	}
	cl := &http.Client{Timeout: 5 * time.Second}
	resp, err := cl.Get(m.cfg.HeartbeatURL)
	if err != nil {
		return // 失败忽略,绝不影响监控主流程
	}
	resp.Body.Close()
}
