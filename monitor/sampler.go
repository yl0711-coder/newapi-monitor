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
	"gorm.io/gorm"
)

const sourceEpochStartupMaxLookbackSec int64 = 3600

const (
	// The realtime sampler stays deliberately small. A separate closed-window
	// pass waits an hour, then advances in bounded ten-minute slices so requests
	// that took tens of minutes to finish are eventually included.
	metricFinalizeDelaySec          int64 = 60 * 60
	metricFinalizeSliceSec          int64 = 10 * 60
	metricFinalizeInitialOverlapSec int64 = 15 * 60
	metricFinalizeRunEverySec       int64 = 5 * 60
	metricMigrationLookbackSec      int64 = 24 * 60 * 60
)

type metricRangeSampler func(context.Context, int64, int64) (int, error)
type tokenRangeSampler func(context.Context, int64, int64) error

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
		stabilityTrafficClassificationVersion).Scan(&metricLatest).Error; err != nil {
		metricLatest = 0
	}
	if err := m.storeDB.Raw(`SELECT COALESCE(MAX(bucket_ts),0) FROM token_samples WHERE traffic_class_version = ?`,
		stabilityTrafficClassificationVersion).Scan(&tokenLatest).Error; err != nil {
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
	// 治理快照是低优先级审计任务；必须在核心采样启动和启动缺口
	// 补齐之后才挂载，避免首次整表用户读取延迟实时采样。
	if m.cfg.GroupGovernanceEnabled {
		goSourceEpoch(ctx, m.runGroupGovernanceLoop)
	}

}

func (m *Monitor) loop(ctx context.Context, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	lookback := int64(interval.Seconds())*3 + 60
	var ticks int
	var nextProblemSample int64
	var nextStabilityRollup int64
	var nextMetricFinalize int64
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
			// Do not widen the every-minute source query to accommodate long
			// requests. Re-read a small, already closed range on a durable cursor
			// instead. Failure leaves the cursor untouched, so restart/retry cannot
			// silently turn a source outage into a permanent chart gap.
			if mainSampleOK && now >= nextMetricFinalize {
				err := m.runMetricFinalizeTurn(ctx, now)
				if err != nil {
					slog.Warn("模型监控迟到日志定稿失败(保留水位重试)", "err", err)
					nextMetricFinalize = now + 60
				} else {
					nextMetricFinalize = now + metricFinalizeRunEverySec
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
					// 必须先把即将越过保留线的分钟事实汇总成功，再删除原始数据。
					// 汇总失败时保留分钟事实供下轮重试，避免维护任务主动制造永久缺口。
					if err := m.rollupHours(cutoff); err != nil {
						slog.Warn("小时汇总失败(忽略)", "err", err)
					} else if n, err := m.pruneOlderThan(cutoff); err != nil {
						slog.Warn("清理过期采样失败(保留下轮重试)", "err", err)
					} else if n > 0 {
						slog.Info("清理过期采样", "rows", n)
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

func metricFinalizeTarget(now int64) int64 {
	target := now - metricFinalizeDelaySec
	if target <= 0 {
		return 0
	}
	return target / 60 * 60
}

func metricFinalizeRetryDelay(attempts int) int64 {
	if attempts < 1 {
		attempts = 1
	}
	if attempts > 5 {
		attempts = 5
	}
	return min(int64(60)<<uint(attempts-1), int64(10*60))
}

func (m *Monitor) loadOrExtendMetricFinalizeState(now int64) (*MetricFinalizeState, error) {
	target := metricFinalizeTarget(now)
	start := target - metricFinalizeInitialOverlapSec
	var legacyCount int64
	if err := m.storeDB.Model(&MetricSample{}).Where("traffic_class_version <> ?", stabilityTrafficClassificationVersion).Limit(1).Count(&legacyCount).Error; err != nil {
		return nil, err
	}
	if legacyCount > 0 {
		start = target - metricMigrationLookbackSec
	}
	if start < 0 {
		start = 0
	}
	start = start / 60 * 60
	hourStart := ((start + 3599) / 3600) * 3600
	defaults := MetricFinalizeState{
		ID: 1, NextTs: start, TargetThroughTs: target, CoverageFromTs: start,
		SemanticsVersion:   stabilityTrafficClassificationVersion,
		HourCoverageFromTs: hourStart, HourCoverageToTs: hourStart,
		HourSemanticsVersion: stabilityTrafficClassificationVersion,
		Status:               "queued", UpdatedAt: now,
	}
	var state MetricFinalizeState
	if err := m.storeDB.Where("id = ?", 1).Attrs(defaults).FirstOrCreate(&state).Error; err != nil {
		return nil, err
	}
	updates := map[string]any{}
	if state.SemanticsVersion != stabilityTrafficClassificationVersion {
		state.NextTs = start
		state.TargetThroughTs = target
		state.CoverageFromTs = start
		state.SemanticsVersion = stabilityTrafficClassificationVersion
		state.Status = "queued"
		state.Attempts = 0
		state.NextRetryAt = 0
		state.LastError = ""
		updates = map[string]any{
			"next_ts": start, "target_through_ts": target, "coverage_from_ts": start,
			"semantics_version": stabilityTrafficClassificationVersion, "status": "queued",
			"attempts": 0, "next_retry_at": 0, "last_error": "",
		}
	}
	if state.HourSemanticsVersion != stabilityTrafficClassificationVersion {
		state.HourCoverageFromTs = hourStart
		state.HourCoverageToTs = hourStart
		state.HourSemanticsVersion = stabilityTrafficClassificationVersion
		updates["hour_coverage_from_ts"] = hourStart
		updates["hour_coverage_to_ts"] = hourStart
		updates["hour_semantics_version"] = stabilityTrafficClassificationVersion
	}
	if state.NextTs <= 0 && target > 0 {
		state.NextTs = start
		updates["next_ts"] = start
	}
	if state.CoverageFromTs <= 0 {
		state.CoverageFromTs = state.NextTs
		updates["coverage_from_ts"] = state.CoverageFromTs
	}
	if target > state.TargetThroughTs {
		state.TargetThroughTs = target
		updates["target_through_ts"] = target
	}
	if len(updates) > 0 {
		updates["updated_at"] = now
		if err := m.storeDB.Model(&MetricFinalizeState{}).Where("id = ?", 1).Updates(updates).Error; err != nil {
			return nil, err
		}
	}
	m.metricFinalizeThrough.Store(state.NextTs)
	m.metricFinalizeTarget.Store(state.TargetThroughTs)
	m.metricFinalizeLastSuccess.Store(state.LastSuccessAt)
	m.metricFinalizeLastFailure.Store(state.LastFailureAt)
	return &state, nil
}

func (m *Monitor) metricWindowCoverage(fromTs, now int64) (bool, int64, int64) {
	target := metricFinalizeTarget(now)
	var state MetricFinalizeState
	if err := m.storeDB.First(&state, "id = ?", 1).Error; err != nil {
		return false, 0, target
	}
	complete := state.SemanticsVersion == stabilityTrafficClassificationVersion &&
		state.CoverageFromTs > 0 && state.CoverageFromTs <= fromTs && state.NextTs >= target
	return complete, state.CoverageFromTs, min(state.NextTs, target)
}

// metricHourWindowCoverage proves long-range hour_samples independently from
// the much shorter minute retention. This lets operators rebuild a 30-day
// trend without retaining 30 days of high-cardinality minute rows.
func (m *Monitor) metricHourWindowCoverage(fromTs, now int64) (bool, int64, int64) {
	target := metricFinalizeTarget(now) / 3600 * 3600
	var state MetricFinalizeState
	if err := m.storeDB.First(&state, "id = ?", 1).Error; err != nil {
		return false, 0, target
	}
	complete := state.HourSemanticsVersion == stabilityTrafficClassificationVersion &&
		state.HourCoverageFromTs > 0 && state.HourCoverageFromTs <= fromTs &&
		state.HourCoverageToTs >= target
	return complete, state.HourCoverageFromTs, min(state.HourCoverageToTs, target)
}

func (m *Monitor) publishMetricBackfillCoverage(stateID uint, minuteFrom, finalizeTarget, hourlyFrom, hourlyUntil, finishedAt int64) error {
	return m.storeDB.Model(&MetricFinalizeState{}).Where("id = ?", stateID).Updates(map[string]any{
		// Only union intervals that actually overlap. A long backfill may finish
		// after the live finalizer has started a newer, disjoint interval; treating
		// MIN(left)..MAX(right) as continuous would manufacture evidence for the
		// gap. In that case keep whichever interval is newer, and fail closed until
		// a later catch-up/retry joins the two ranges.
		"coverage_from_ts": gorm.Expr(`CASE
			WHEN semantics_version=? AND coverage_from_ts>0 AND next_ts>coverage_from_ts AND next_ts>=? AND ?>=coverage_from_ts THEN MIN(coverage_from_ts,?)
			WHEN semantics_version=? AND coverage_from_ts>0 AND next_ts>coverage_from_ts AND ?>next_ts THEN ?
			WHEN semantics_version=? AND coverage_from_ts>0 AND next_ts>coverage_from_ts THEN coverage_from_ts
			WHEN next_ts>? THEN next_ts
			ELSE ? END`, stabilityTrafficClassificationVersion, minuteFrom, finalizeTarget, minuteFrom,
			stabilityTrafficClassificationVersion, minuteFrom, minuteFrom,
			stabilityTrafficClassificationVersion, finalizeTarget, minuteFrom),
		"next_ts":           gorm.Expr("MAX(next_ts,?)", finalizeTarget),
		"target_through_ts": gorm.Expr("MAX(target_through_ts,?)", finalizeTarget),
		"semantics_version": stabilityTrafficClassificationVersion,
		"status":            gorm.Expr(`CASE WHEN MAX(next_ts,?)>=target_through_ts THEN 'caught_up' ELSE status END`, finalizeTarget),
		"hour_coverage_from_ts": gorm.Expr(`CASE
			WHEN hour_semantics_version=? AND hour_coverage_from_ts>0 AND hour_coverage_to_ts>hour_coverage_from_ts AND hour_coverage_to_ts>=? AND ?>=hour_coverage_from_ts THEN MIN(hour_coverage_from_ts,?)
			WHEN hour_semantics_version=? AND hour_coverage_from_ts>0 AND hour_coverage_to_ts>hour_coverage_from_ts AND ?>hour_coverage_to_ts THEN ?
			WHEN hour_semantics_version=? AND hour_coverage_from_ts>0 AND hour_coverage_to_ts>hour_coverage_from_ts THEN hour_coverage_from_ts
			ELSE ? END`, stabilityTrafficClassificationVersion, hourlyFrom, hourlyUntil, hourlyFrom,
			stabilityTrafficClassificationVersion, hourlyFrom, hourlyFrom,
			stabilityTrafficClassificationVersion, hourlyFrom),
		"hour_coverage_to_ts": gorm.Expr(`CASE
			WHEN hour_semantics_version=? AND hour_coverage_from_ts>0 AND hour_coverage_to_ts>hour_coverage_from_ts AND hour_coverage_to_ts>=? AND ?>=hour_coverage_from_ts THEN MAX(hour_coverage_to_ts,?)
			WHEN hour_semantics_version=? AND hour_coverage_from_ts>0 AND hour_coverage_to_ts>hour_coverage_from_ts AND ?>hour_coverage_to_ts THEN ?
			WHEN hour_semantics_version=? AND hour_coverage_from_ts>0 AND hour_coverage_to_ts>hour_coverage_from_ts THEN hour_coverage_to_ts
			ELSE ? END`, stabilityTrafficClassificationVersion, hourlyFrom, hourlyUntil, hourlyUntil,
			stabilityTrafficClassificationVersion, hourlyFrom, hourlyUntil,
			stabilityTrafficClassificationVersion, hourlyUntil),
		"hour_semantics_version": stabilityTrafficClassificationVersion,
		"updated_at":             finishedAt,
	}).Error
}

func (m *Monitor) recordMetricFinalizeFailure(state *MetricFinalizeState, cause error, now int64) {
	if state == nil || cause == nil {
		return
	}
	attempts := state.Attempts + 1
	if err := m.storeDB.Model(&MetricFinalizeState{}).Where("id = ? AND next_ts = ?", state.ID, state.NextTs).Updates(map[string]any{
		"status": "retry", "attempts": attempts, "next_retry_at": now + metricFinalizeRetryDelay(attempts),
		"last_failure_at": now, "last_error": clip(cause.Error(), 512), "updated_at": now,
	}).Error; err == nil {
		m.metricFinalizeLastFailure.Store(now)
	}
}

// runMetricFinalizeTurnWith executes at most one bounded source slice. The
// cursor advances only after both the model and token projections plus their
// local hour rollup succeed. Replaying a partly written slice is safe because
// both minute tables use replace-style UPSERTs.
func (m *Monitor) runMetricFinalizeTurnWith(ctx context.Context, now int64, metric metricRangeSampler, token tokenRangeSampler) error {
	state, err := m.loadOrExtendMetricFinalizeState(now)
	if err != nil {
		return err
	}
	if state.NextRetryAt > now {
		return nil
	}
	if state.NextTs >= state.TargetThroughTs {
		return m.storeDB.Model(&MetricFinalizeState{}).Where("id = ?", state.ID).Updates(map[string]any{
			"status": "caught_up", "attempts": 0, "next_retry_at": 0, "last_error": "", "updated_at": now,
		}).Error
	}
	from, to := state.NextTs, min(state.NextTs+metricFinalizeSliceSec, state.TargetThroughTs)
	if _, err := metric(ctx, from, to); err != nil {
		m.recordMetricFinalizeFailure(state, err, now)
		return err
	}
	if err := token(ctx, from, to); err != nil {
		m.recordMetricFinalizeFailure(state, err, now)
		return err
	}
	if err := m.rollupHours(from / 3600 * 3600); err != nil {
		m.recordMetricFinalizeFailure(state, err, now)
		return err
	}
	status := "queued"
	if to >= state.TargetThroughTs {
		status = "caught_up"
	}
	updates := map[string]any{
		"next_ts": to, "target_through_ts": state.TargetThroughTs, "status": status,
		"attempts": 0, "next_retry_at": 0, "last_success_at": now,
		"last_failure_at": 0, "last_error": "", "updated_at": now,
	}
	completedHourTo := to / 3600 * 3600
	if completedHourTo > state.HourCoverageToTs {
		updates["hour_coverage_to_ts"] = completedHourTo
		updates["hour_semantics_version"] = stabilityTrafficClassificationVersion
	}
	result := m.storeDB.Model(&MetricFinalizeState{}).
		Where("id = ? AND next_ts = ?", state.ID, from).Updates(updates)
	if result.Error != nil {
		return result.Error
	}
	if result.RowsAffected != 1 {
		return fmt.Errorf("模型监控迟到日志水位并发更新冲突")
	}
	m.metricFinalizeThrough.Store(to)
	m.metricFinalizeTarget.Store(state.TargetThroughTs)
	m.metricFinalizeLastSuccess.Store(now)
	m.metricFinalizeLastFailure.Store(0)
	return nil
}

func (m *Monitor) runMetricFinalizeTurn(ctx context.Context, now int64) error {
	return m.runMetricFinalizeTurnWith(ctx, now, m.sampleRange, m.sampleTokensRange)
}

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
	return m.sampleRangeWithPriority(ctx, fromTs, toTs, false, m.cfg.CapacityEnabled)
}

func (m *Monitor) sampleRangeLow(ctx context.Context, fromTs, toTs int64) (int, error) {
	return m.sampleRangeWithPriority(ctx, fromTs, toTs, true, m.cfg.CapacityEnabled)
}

func (m *Monitor) sampleMetricRangeLow(ctx context.Context, fromTs, toTs int64) (int, error) {
	return m.sampleRangeWithPriority(ctx, fromTs, toTs, true, false)
}

func (m *Monitor) sampleRangeWithPriority(ctx context.Context, fromTs, toTs int64, lowPriority, includeCapacity bool) (int, error) {
	gateCtx := ctx
	var cctx context.Context
	var cancel context.CancelFunc
	if !lowPriority {
		cctx, cancel = context.WithTimeout(ctx, 20*time.Second)
		gateCtx = cctx
	}
	var release func()
	var err error
	if lowPriority {
		release, err = m.acquireBackgroundSourceLow(gateCtx)
	} else {
		release, err = m.acquireBackgroundSource(gateCtx)
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return 0, err
	}
	defer release()
	if lowPriority {
		cctx, cancel = context.WithTimeout(ctx, 20*time.Second)
	}
	defer cancel()

	query := sampleWindowSQL()
	if includeCapacity {
		query = sampleWindowUserSQL()
	}
	rows, err := m.prodDB.QueryContext(cctx, query, fromTs, toTs)
	if err != nil {
		m.reportSourceQueryError(err)
		return 0, err
	}
	defer rows.Close()

	type metricKey struct {
		bucketTs  int64
		channelID int
		modelName string
		grp       string
	}
	metricByKey := map[metricKey]*MetricSample{}
	var userBatch []CapacityUserMinuteSample
	for rows.Next() {
		var (
			s           MetricSample
			grp         sql.NullString
			username    sql.NullString
			userID      int64
			e4, e5, eto int64
		)
		scanArgs := []any{&s.BucketTs, &s.ChannelID, &s.ModelName, &grp}
		if includeCapacity {
			scanArgs = append(scanArgs, &userID, &username)
		}
		scanArgs = append(scanArgs, &s.Success, &s.Anomaly, &s.Failed,
			&s.AnomalyBilled, &s.AnomalyFree, &s.AnomalyStream, &s.AnomalyQuota, &s.AnomalySumTime,
			&s.SumUseTime, &s.MaxUseTime, &s.Tokens, &s.Quota, &s.RefundRecords, &s.RefundQuota,
			&e4, &e5, &eto,
			&s.Lat1, &s.Lat2, &s.Lat5, &s.Lat10, &s.Lat30, &s.Lat60, &s.LatInf,
			&s.CompletionTokens,
			&s.Ttft500, &s.Ttft1k, &s.Ttft2k, &s.Ttft5k, &s.Ttft10k, &s.TtftInf, &s.TtftMaxMs)
		if err := rows.Scan(scanArgs...); err != nil {
			return 0, err
		}
		s.Grp = grp.String
		s.TrafficClassVersion = stabilityTrafficClassificationVersion
		s.Err4xx, s.Err5xx, s.ErrTimeout = e4, e5, eto
		if other := s.Failed - e4 - e5 - eto; other > 0 {
			s.ErrOther = other
		}
		key := metricKey{bucketTs: s.BucketTs, channelID: s.ChannelID, modelName: s.ModelName, grp: s.Grp}
		aggregated := metricByKey[key]
		if aggregated == nil {
			aggregated = &MetricSample{BucketTs: s.BucketTs, ChannelID: s.ChannelID, ModelName: s.ModelName,
				Grp: s.Grp, TrafficClassVersion: stabilityTrafficClassificationVersion}
			metricByKey[key] = aggregated
		}
		mergeMetricSample(aggregated, s)
		if includeCapacity && (s.Success+s.Anomaly+s.Failed > 0 || s.Tokens > 0) {
			userBatch = append(userBatch, CapacityUserMinuteSample{
				BucketTs: s.BucketTs, UserID: userID, Username: username.String, ChannelID: s.ChannelID,
				ModelName: s.ModelName, Grp: s.Grp, TrafficClassVersion: stabilityTrafficClassificationVersion,
				Success: s.Success, Anomaly: s.Anomaly, Failed: s.Failed, Tokens: s.Tokens,
			})
		}
	}
	if err := rows.Err(); err != nil {
		m.reportSourceQueryError(err)
		return 0, err
	}
	batch := make([]MetricSample, 0, len(metricByKey))
	for _, sample := range metricByKey {
		batch = append(batch, *sample)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := upsertSamplesDB(tx, batch); err != nil {
			return err
		}
		return upsertCapacityUserMinuteSamplesDB(tx, userBatch)
	}); err != nil {
		return 0, err
	}
	return len(batch), nil
}

func mergeMetricSample(dst *MetricSample, src MetricSample) {
	dst.Success += src.Success
	dst.Anomaly += src.Anomaly
	dst.Failed += src.Failed
	dst.AnomalyBilled += src.AnomalyBilled
	dst.AnomalyFree += src.AnomalyFree
	dst.AnomalyStream += src.AnomalyStream
	dst.AnomalyQuota += src.AnomalyQuota
	dst.AnomalySumTime += src.AnomalySumTime
	dst.SumUseTime += src.SumUseTime
	if src.MaxUseTime > dst.MaxUseTime {
		dst.MaxUseTime = src.MaxUseTime
	}
	dst.Tokens += src.Tokens
	dst.Quota += src.Quota
	dst.RefundRecords += src.RefundRecords
	dst.RefundQuota += src.RefundQuota
	dst.Err4xx += src.Err4xx
	dst.Err5xx += src.Err5xx
	dst.ErrTimeout += src.ErrTimeout
	dst.ErrOther += src.ErrOther
	dst.Lat1 += src.Lat1
	dst.Lat2 += src.Lat2
	dst.Lat5 += src.Lat5
	dst.Lat10 += src.Lat10
	dst.Lat30 += src.Lat30
	dst.Lat60 += src.Lat60
	dst.LatInf += src.LatInf
	dst.CompletionTokens += src.CompletionTokens
	dst.Ttft500 += src.Ttft500
	dst.Ttft1k += src.Ttft1k
	dst.Ttft2k += src.Ttft2k
	dst.Ttft5k += src.Ttft5k
	dst.Ttft10k += src.Ttft10k
	dst.TtftInf += src.TtftInf
	if src.TtftMaxMs > dst.TtftMaxMs {
		dst.TtftMaxMs = src.TtftMaxMs
	}
}

// sampleWindowSQL 组装采样查询。拆成独立函数是为了能在测试里渲染出成品 SQL
// 拿到真实 MySQL 上验证语法与 collation——判据里有 REGEXP 和 JSON_EXTRACT,
// 光靠 Go 侧字符串断言盖不住"打到生产库才报错"这类问题。
func sampleWindowSQL() string { return sampleWindowSQLWithUser(false) }

func sampleWindowUserSQL() string { return sampleWindowSQLWithUser(true) }

func sampleWindowSQLWithUser(includeUser bool) string {
	// MySQL SUM/布尔聚合返回 DECIMAL,需 CAST 成 SIGNED 才能 Scan 进 int64。
	// 错误分类互斥(优先级:超时 > 5xx > 4xx),四类之和不超过失败数。
	// FRT = 首字延迟(ms),取自 other JSON 的 frt;非法 JSON 或缺失则计 0(被 frt>0 过滤掉)。
	//
	// 交付异常判据见 expandAnomalyPredicates。
	const frt = "(CASE WHEN JSON_VALID(other) THEN CAST(JSON_EXTRACT(other,'$.frt') AS SIGNED) ELSE 0 END)"
	userColumns, userGroup := "", ""
	if includeUser {
		userColumns = "  user_id, MAX(COALESCE(username,'')) AS username,\n"
		userGroup = ", user_id"
	}
	q := `
SELECT /*+ MAX_EXECUTION_TIME(8000) */
  (created_at DIV 60)*60 AS bucket,
  channel_id, model_name, ` + "`group`" + ` AS grp,
` + userColumns + `
  CAST(COALESCE(SUM(type=2 AND NOT {{ANOM}}),0) AS SIGNED) AS success,
  CAST(COALESCE(SUM(type=2 AND {{ANOM}}),0) AS SIGNED) AS anomaly,
  CAST(COALESCE(SUM(type=5),0) AS SIGNED) AS failed,
  CAST(COALESCE(SUM(type=2 AND {{ZERO}} AND quota > 0),0) AS SIGNED) AS anomaly_billed,
  CAST(COALESCE(SUM(type=2 AND {{ZERO}} AND quota = 0),0) AS SIGNED) AS anomaly_free,
  CAST(COALESCE(SUM(type=2 AND {{STREAMBAD}} AND NOT {{ZERO}}),0) AS SIGNED) AS anomaly_stream,
  CAST(COALESCE(SUM(CASE WHEN type=2 AND {{ZERO}} AND quota > 0 THEN quota END),0) AS SIGNED) AS anomaly_quota,
  CAST(COALESCE(SUM(CASE WHEN type=2 AND {{ANOM}} THEN use_time END),0) AS SIGNED) AS anomaly_sum_time,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS sum_use_time,
  CAST(COALESCE(MAX(CASE WHEN type=2 THEN use_time END),0) AS SIGNED) AS max_use_time,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN prompt_tokens+completion_tokens END),0) AS SIGNED) AS tokens,
  CAST(COALESCE(SUM(CASE WHEN type=2 THEN quota END),0) AS SIGNED) AS quota,
  CAST(COALESCE(SUM(type=6),0) AS SIGNED) AS refund_records,
  CAST(COALESCE(SUM(CASE WHEN type=6 THEN quota END),0) AS SIGNED) AS refund_quota,
  CAST(COALESCE(SUM(type=5 AND {{ERR4XX}}),0) AS SIGNED) AS err_4xx,
  CAST(COALESCE(SUM(type=5 AND {{ERR5XX}}),0) AS SIGNED) AS err_5xx,
  CAST(COALESCE(SUM(type=5 AND {{ERRTIMEOUT}}),0) AS SIGNED) AS err_timeout,
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
GROUP BY bucket, channel_id, model_name, grp` + userGroup
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

type MetricBackfillStatus struct {
	Status     string          `json:"status"`
	Hours      int             `json:"hours"`
	StartedAt  int64           `json:"started_at,omitempty"`
	FinishedAt int64           `json:"finished_at,omitempty"`
	Error      string          `json:"error,omitempty"`
	Result     *BackfillResult `json:"result,omitempty"`
}

func (m *Monitor) setMetricBackfillStatus(status MetricBackfillStatus) {
	m.metricBackfillMu.Lock()
	m.metricBackfillStatus = status
	m.metricBackfillMu.Unlock()
}

func (m *Monitor) getMetricBackfillStatus() MetricBackfillStatus {
	m.metricBackfillMu.RLock()
	defer m.metricBackfillMu.RUnlock()
	return m.metricBackfillStatus
}

// backfillRunning 保证同一时刻只有一次回填在跑：30 天回填要按小时
// 访问 720 个来源分片，并发执行会放大生产库压力。
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
	return m.backfillHoursWith(ctx, hours, m.sampleRangeLow, m.sampleMetricRangeLow, m.sampleTokensRangeLow, m.rollupHourRange)
}

func (m *Monitor) backfillHoursWith(ctx context.Context, hours int, metric, historicalMetric metricRangeSampler, token tokenRangeSampler, rollup func(int64, int64) error) (*BackfillResult, error) {
	if err := m.validateMetricBackfill(hours); err != nil {
		return nil, err
	}
	if !backfillRunning.CompareAndSwap(false, true) {
		return nil, fmt.Errorf("已有回填正在进行,请等待其结束")
	}
	return m.backfillHoursLocked(ctx, hours, metric, historicalMetric, token, rollup)
}

func (m *Monitor) validateMetricBackfill(hours int) error {
	if m.prodDB == nil {
		return fmt.Errorf("未配置生产库(只读),无法回填")
	}
	if hours <= 0 {
		return fmt.Errorf("hours 需大于 0")
	}
	if max := m.cfg.HourRetentionDays * 24; max > 0 && hours > max {
		return fmt.Errorf("hours 不能超过小时级留存 %d 小时(HourRetentionDays=%d)", max, m.cfg.HourRetentionDays)
	}
	return nil
}

// backfillHoursLocked runs after the global maintenance lease has already
// been acquired. Both synchronous tests/tools and the async HTTP job use the
// same implementation, so there is only one coverage publication path.
func (m *Monitor) backfillHoursLocked(ctx context.Context, hours int, metric, historicalMetric metricRangeSampler, token tokenRangeSampler, rollup func(int64, int64) error) (*BackfillResult, error) {
	defer backfillRunning.Store(false)

	start := time.Now()
	// 小时历史以已定稿整点为右边界；分钟覆盖再单独补到定稿水位。
	// 这样 720 小时回填会精确产生 720 个完整小时，不会缺首尾半小时。
	now := start.Unix() / 60 * 60
	finalizeTarget := metricFinalizeTarget(now)
	hourlyUntil := finalizeTarget / 3600 * 3600
	hourlyFrom := hourlyUntil - int64(hours)*3600
	minuteCutoff := now - int64(m.cfg.RetentionDays)*86400
	res := &BackfillResult{Hours: hours}
	slog.Info("开始历史回填", "hours", hours, "note", "只读生产库,按小时切片")

	for from := hourlyFrom; from < hourlyUntil; from += 3600 {
		select {
		case <-ctx.Done():
			return res, ctx.Err()
		default:
		}
		to := from + 3600
		res.Slices++
		sliceFailed := false
		keepMinutes := to > minuteCutoff
		metricSampler := historicalMetric
		if keepMinutes {
			metricSampler = metric
		}
		n, err := metricSampler(ctx, from, to)
		if err != nil {
			sliceFailed = true
			slog.Warn("回填分片失败(跳过)", "from", from, "to", to, "err", err)
		} else {
			res.Rows += n
		}
		// 令牌是分钟级功能，只回填仍在分钟留存线内的分片。
		// 更旧分片仅用于小时趋势/同比，不制造无法保留的令牌明细。
		if keepMinutes {
			if err := token(ctx, from, to); err != nil {
				sliceFailed = true
				slog.Warn("回填分片令牌维度失败", "from", from, "to", to, "err", err)
			}
		}
		if !sliceFailed {
			if err := rollup(from, to); err != nil {
				sliceFailed = true
				slog.Warn("回填分片小时汇总失败", "from", from, "to", to, "err", err)
			}
		}
		if !sliceFailed && !keepMinutes {
			if err := m.pruneMetricRange(from, to); err != nil {
				sliceFailed = true
				slog.Warn("回填临时分钟事实清理失败", "from", from, "to", to, "err", err)
			}
		}
		if sliceFailed {
			res.Failed++
		}
		if to < hourlyUntil {
			time.Sleep(500 * time.Millisecond) // 让生产库喘口气
		}
	}
	// 整点至定稿水位的尾段只服务分钟看板，不能宣布为完整小时。
	if finalizeTarget > hourlyUntil {
		res.Slices++
		if n, err := metric(ctx, hourlyUntil, finalizeTarget); err != nil {
			res.Failed++
			slog.Warn("回填分钟尾段失败", "from", hourlyUntil, "to", finalizeTarget, "err", err)
		} else {
			res.Rows += n
			if err := token(ctx, hourlyUntil, finalizeTarget); err != nil {
				res.Failed++
				slog.Warn("回填分钟尾段令牌维度失败", "from", hourlyUntil, "to", finalizeTarget, "err", err)
			}
		}
	}
	if m.cfg.StabilityEnabled {
		stabilityFrom := max(hourlyFrom, minuteCutoff)
		if err := m.rollupStabilityHours(stabilityFrom); err != nil {
			slog.Warn("回填后稳定性维度汇总失败(忽略)", "err", err)
		}
		if err := m.rollupStabilityRejections(now - int64(m.cfg.RetentionDays)*86400); err != nil {
			slog.Warn("回填后稳定性拒绝汇总失败(忽略)", "err", err)
		}
	}
	if res.Failed == 0 {
		// 长周期小时覆盖与短周期分钟覆盖分别发布。
		// 小时回填可扩展到 HourRetentionDays，分钟水位仍严格受 RetentionDays 限制。
		state, stateErr := m.loadOrExtendMetricFinalizeState(now)
		if stateErr != nil {
			res.Failed++
			slog.Warn("回填完整性水位读取失败", "err", stateErr)
		} else {
			minuteFrom := max(hourlyFrom, minuteCutoff)
			minuteFrom = minuteFrom / 60 * 60
			finishedAt := time.Now().Unix()
			if err := m.publishMetricBackfillCoverage(state.ID, minuteFrom, finalizeTarget, hourlyFrom, hourlyUntil, finishedAt); err != nil {
				res.Failed++
				slog.Warn("回填完整性水位保存失败", "err", err)
			}
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
	return m.sampleTokensRangeWithPriority(ctx, fromTs, toTs, false)
}

func (m *Monitor) sampleTokensRangeLow(ctx context.Context, fromTs, toTs int64) error {
	return m.sampleTokensRangeWithPriority(ctx, fromTs, toTs, true)
}

func (m *Monitor) sampleTokensRangeWithPriority(ctx context.Context, fromTs, toTs int64, lowPriority bool) error {
	gateCtx := ctx
	var cctx context.Context
	var cancel context.CancelFunc
	if !lowPriority {
		cctx, cancel = context.WithTimeout(ctx, 20*time.Second)
		gateCtx = cctx
	}
	var release func()
	var err error
	if lowPriority {
		release, err = m.acquireBackgroundSourceLow(gateCtx)
	} else {
		release, err = m.acquireBackgroundSource(gateCtx)
	}
	if err != nil {
		if cancel != nil {
			cancel()
		}
		return err
	}
	defer release()
	if lowPriority {
		cctx, cancel = context.WithTimeout(ctx, 20*time.Second)
	}
	defer cancel()
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
		s.TrafficClassVersion = stabilityTrafficClassificationVersion
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
		return err
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
