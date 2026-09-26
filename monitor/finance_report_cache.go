package monitor

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"gorm.io/gorm"
)

const (
	// 经营核算是按闭合小时生成的管理报表，不需要像实时监控一样每次请求重算。
	// 五分钟新鲜期减少重复 SQLite 扫描；其后的十分钟只用于“立即展示旧结果、
	// 后台更新”，页面会显示 report.generated_at；仅旧读取模式手动刷新绕过缓存。
	financeReportCacheTTL        = 5 * time.Minute
	financeReportCacheStaleGrace = 10 * time.Minute
	financeReportCacheMaxEntries = 16
	financeReportCacheMaxBytes   = 12 << 20
	financeReportBuildTimeout    = 30 * time.Second
	// A full-range report changes its cache key every closed hour. Keep a
	// verified prior-range snapshot long enough to cover a quiet day or a
	// restart, but never present it as current-period data.
	financeReportPersistentStale = 48 * time.Hour
)

type financeReportRequest struct {
	from              time.Time
	to                time.Time
	snapshotAsOf      int64
	snapshotClamped   bool
	configurationHash string
	sourceFingerprint string
}

func financeDBPoolStats(db *gorm.DB) sql.DBStats {
	if db == nil {
		return sql.DBStats{}
	}
	pool, err := db.DB()
	if err != nil {
		return sql.DBStats{}
	}
	return pool.Stats()
}

func logFinanceReadStageTiming(stage string, started time.Time, err error) {
	elapsed := time.Since(started)
	if elapsed < 2*time.Second && err == nil {
		return
	}
	slog.Warn("经营核算读取阶段耗时诊断", "stage", stage, "elapsed_ms", elapsed.Milliseconds(), "failed", err != nil)
}

// A slow report may spend most of its deadline waiting for the single facts
// SQLite connection rather than executing its query. Emit only aggregate pool
// counters, never SQL parameters, account IDs or report amounts.
func (m *Monitor) logFinanceBuildTiming(started time.Time, mainBefore, factsBefore, readBefore sql.DBStats, err error) {
	elapsed := time.Since(started)
	if elapsed < 5*time.Second && err == nil {
		return
	}
	mainAfter := financeDBPoolStats(m.storeDB)
	factsAfter := financeDBPoolStats(m.usageFactsDB)
	readAfter := financeDBPoolStats(m.financeFactsReadDB.Load())
	slog.Warn("经营核算构建耗时诊断",
		"elapsed_ms", elapsed.Milliseconds(), "failed", err != nil,
		"main_pool_wait_count", mainAfter.WaitCount-mainBefore.WaitCount,
		"main_pool_wait_ms", (mainAfter.WaitDuration - mainBefore.WaitDuration).Milliseconds(),
		"facts_pool_wait_count", factsAfter.WaitCount-factsBefore.WaitCount,
		"facts_pool_wait_ms", (factsAfter.WaitDuration - factsBefore.WaitDuration).Milliseconds(),
		"facts_pool_in_use", factsAfter.InUse, "facts_pool_open", factsAfter.OpenConnections,
		"finance_read_pool_wait_count", readAfter.WaitCount-readBefore.WaitCount,
		"finance_read_pool_wait_ms", (readAfter.WaitDuration - readBefore.WaitDuration).Milliseconds(),
		"finance_read_pool_in_use", readAfter.InUse, "finance_read_pool_open", readAfter.OpenConnections)
}

// Legacy explicit refresh bypasses both layers. The async queue revalidates
// source versions and keeps unaffected historical month components reusable.
type financeForceRebuildKey struct{}

var errFinanceFactsChanged = errors.New("经营核算事实在报表生成期间已变更，请重试")

func (r financeReportRequest) lastGoodKey() string {
	return "last-good:" + r.logicalKey()
}

func (r financeReportRequest) logicalKey() string {
	// Version the report projection separately from source facts. Changes to
	// month arithmetic must also bump financePeriodCacheSchema.
	return fmt.Sprintf("accounting-delivery-compat-v1:%d:%d:%d:%t:%s", r.from.Unix(), r.to.Unix(), r.snapshotAsOf, r.snapshotClamped, r.configurationHash)
}

func (r financeReportRequest) cacheKey() string {
	return r.logicalKey() + ":" + r.sourceFingerprint
}

func (m *Monitor) getFinanceReportCache() *boundedByteCache {
	m.financeReportCacheOnce.Do(func() {
		m.financeReportCache = newBoundedByteCache(financeReportCacheMaxEntries, financeReportCacheMaxBytes)
	})
	return m.financeReportCache
}

func (m *Monitor) buildFinanceReportPayload(ctx context.Context, request financeReportRequest) ([]byte, error) {
	report, err := m.buildFinanceOperatingReport(ctx, request.from, request.to)
	if err != nil {
		return nil, err
	}
	// 管理员可能在报表读取期间修改内部账号或业务分组。
	// 不得把新旧口径混合的结果写入任一配置的缓存键。
	if request.configurationHash != "" {
		currentHash, hashErr := m.financeReportConfigurationHash(ctx)
		if hashErr != nil {
			return nil, fmt.Errorf("复核经营核算配置: %w", hashErr)
		}
		if currentHash != request.configurationHash {
			return nil, fmt.Errorf("经营核算配置在报表生成期间已变更，请重试")
		}
	}
	if request.sourceFingerprint != "" {
		currentFingerprint, fingerprintErr := m.financeReportSourceFingerprint(ctx, request.from.Unix(), request.to.Unix())
		if fingerprintErr != nil {
			return nil, fmt.Errorf("复核经营核算事实版本: %w", fingerprintErr)
		}
		if currentFingerprint != request.sourceFingerprint {
			return nil, errFinanceFactsChanged
		}
	}
	if request.snapshotAsOf > 0 {
		report.DataAsOf = request.snapshotAsOf
	}
	if request.snapshotClamped {
		loc, _ := time.LoadLocation("Asia/Shanghai")
		notice := fmt.Sprintf("本机预览使用静态快照，数据截至 %s；查询结束时间已限制在快照边界。",
			time.Unix(request.snapshotAsOf, 0).In(loc).Format("2006-01-02 15:04"))
		report.Notices = append([]string{notice}, report.Notices...)
	}
	payload, err := json.Marshal(report)
	if err != nil {
		return nil, fmt.Errorf("序列化经营核算报表: %w", err)
	}
	return payload, nil
}

// Retry only a publication race, at most once and under the original deadline.
// The accepted version is returned separately so it can never be stored under
// the fingerprint of the rejected attempt. Configuration changes still fail.
func (m *Monitor) buildFinanceReportWithRetry(ctx context.Context, request financeReportRequest) ([]byte, financeReportRequest, error) {
	if request.sourceFingerprint == "" && m.cfg.FinanceEnabled {
		fingerprint, err := m.financeReportSourceFingerprint(ctx, request.from.Unix(), request.to.Unix())
		if err != nil {
			return nil, request, err
		}
		request.sourceFingerprint = fingerprint
	}
	for attempt := 0; attempt < 2; attempt++ {
		payload, err := m.buildFinanceReportPayload(ctx, request)
		if !errors.Is(err, errFinanceFactsChanged) || attempt == 1 || ctx.Err() != nil {
			return payload, request, err
		}
		fingerprint, err := m.financeReportSourceFingerprint(ctx, request.from.Unix(), request.to.Unix())
		if err != nil {
			return nil, request, err
		}
		request.sourceFingerprint = fingerprint
		// Each component checks its own source version. The async read lane
		// can reuse unchanged months after a publication race.
		if !m.cfg.FinanceFastSnapshotEnabled {
			ctx = context.WithValue(ctx, financeForceRebuildKey{}, true)
		}
	}
	return nil, request, errFinanceFactsChanged
}

func (m *Monitor) rememberFinanceReport(request financeReportRequest, payload []byte, now time.Time) {
	cache := m.getFinanceReportCache()
	cache.PutWithStale(request.cacheKey(), payload, financeReportCacheTTL, financeReportCacheStaleGrace, now)
	// Both keys share the existing byte/entry budget; no unbounded stale store.
	cache.PutWithStale(request.lastGoodKey(), payload, financeReportCacheTTL, financeReportCacheStaleGrace, now)
}

// financeReportPayload returns a bounded in-process cache result. It never
// writes SQLite and never calls NewAPI/upstream/AWS. Stale results are served
// only inside the short grace window while one coalesced background refresh
// rebuilds the exact same range.
func (m *Monitor) financeReportPayload(ctx context.Context, request financeReportRequest, forceFresh bool) ([]byte, string, error) {
	if request.sourceFingerprint == "" && m.cfg.FinanceEnabled {
		// Resolve a missing version before lookup as well as before building,
		// so a successfully retried probe uses the same cache key on later reads.
		if fingerprint, err := m.financeReportSourceFingerprint(ctx, request.from.Unix(), request.to.Unix()); err == nil {
			request.sourceFingerprint = fingerprint
		}
	}
	cache := m.getFinanceReportCache()
	key := request.cacheKey()
	now := time.Now()
	if !forceFresh {
		if payload, ok := cache.Get(key, now); ok {
			return payload, "hit", nil
		}
		if payload, ok := cache.GetStale(key, now); ok {
			m.refreshFinanceReportAsync(request)
			return payload, "stale-refreshing", nil
		}
		if payload, ok := cache.GetStale(request.lastGoodKey(), now); ok {
			m.refreshFinanceReportAsync(request)
			return payload, "stale-refreshing", nil
		}
		payload, _, snapshotState, ok, snapshotErr := m.loadFinanceReportSnapshot(request, now)
		if snapshotErr != nil {
			slog.Warn("经营核算持久快照读取失败，回退实时计算", "err", snapshotErr)
		} else if ok {
			if snapshotState == "fresh" {
				// The matching source fingerprint proves this payload still reflects
				// the current local facts. Warm L1 from now instead of inheriting an
				// already elapsed file-age deadline.
				m.rememberFinanceReport(request, payload, now)
				return payload, "persistent-hit", nil
			}
			m.refreshFinanceReportAsync(request)
			return payload, "persistent-stale-refreshing", nil
		}
		// The default "since launch" range advances hourly. After a restart,
		// there may be no snapshot for this exact hour even though the last
		// completed report is still available. Its own, earlier range remains
		// in the payload and the UI must label it as an old interval.
		if previous, ok, priorErr := m.loadPriorFinanceReportSnapshot(request, now); priorErr != nil {
			slog.Warn("经营核算上一区间快照读取失败，回退实时计算", "err", priorErr)
		} else if ok {
			m.refreshFinanceReportAsync(request)
			return previous, "persistent-prior-stale-refreshing", nil
		}
	}

	flightKey := key
	if force, _ := ctx.Value(financeForceRebuildKey{}).(bool); force {
		flightKey += ":force-rebuild"
	}
	payload, err := m.financeReportFlight.Do(ctx, flightKey, func() ([]byte, error) {
		// A concurrent request may have completed between the first lookup and
		// winning the flight. Normal reads reuse it; explicit refreshes rebuild.
		if !forceFresh {
			if cached, ok := cache.Get(key, time.Now()); ok {
				return cached, nil
			}
		}
		started := time.Now()
		mainBefore := financeDBPoolStats(m.storeDB)
		factsBefore := financeDBPoolStats(m.usageFactsDB)
		readBefore := financeDBPoolStats(m.financeFactsReadDB.Load())
		built, accepted, buildErr := m.buildFinanceReportWithRetry(ctx, request)
		m.logFinanceBuildTiming(started, mainBefore, factsBefore, readBefore, buildErr)
		if buildErr != nil {
			return nil, buildErr
		}
		m.rememberFinanceReport(accepted, built, time.Now())
		m.persistFinanceReportSnapshotShadowAsync(accepted, built)
		return built, nil
	})
	if err != nil {
		if errors.Is(err, errFinanceFactsChanged) || errors.Is(err, context.DeadlineExceeded) {
			slog.Warn("经营核算更新暂未完成，尝试保留已校验结果", "from", request.from.Unix(), "to", request.to.Unix(), "err", err)
			if previous, ok := cache.GetStale(request.lastGoodKey(), time.Now()); ok {
				return previous, "stale-retry", nil
			}
			if previous, _, _, ok, readErr := m.loadFinanceReportSnapshot(request, time.Now()); readErr == nil && ok {
				return previous, "persistent-stale-retry", nil
			}
			if previous, ok, readErr := m.loadPriorFinanceReportSnapshot(request, time.Now()); readErr == nil && ok {
				return previous, "persistent-prior-stale-retry", nil
			}
		}
		return nil, "miss", err
	}
	if forceFresh {
		return payload, "refresh", nil
	}
	return payload, "miss", nil
}

func (m *Monitor) persistFinanceReportSnapshotShadowAsync(request financeReportRequest, payload []byte) {
	if !m.cfg.FinanceReportSnapshotShadowEnabled {
		return
	}
	payload = append([]byte(nil), payload...)
	go func() {
		m.financeSnapshotWriteMu.Lock()
		defer m.financeSnapshotWriteMu.Unlock()
		if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), request.sourceFingerprint, payload, time.Now()); err != nil {
			slog.Warn("经营核算持久快照影子写入失败，继续使用现有内存缓存", "err", err)
		}
	}()
}

func (m *Monitor) refreshFinanceReportAsync(request financeReportRequest) {
	if m.taskContext().Err() != nil || !m.financeReportRefreshRunning.CompareAndSwap(false, true) {
		return
	}
	go func() {
		defer m.financeReportRefreshRunning.Store(false)
		ctx, cancel := context.WithTimeout(m.taskContext(), financeReportBuildTimeout)
		defer cancel()
		m.refreshFinanceReport(ctx, request)
	}()
}

// A failed refresh never replaces a usable cached report. Keep the failure
// observable without adding any writes to the accounting database.
func (m *Monitor) refreshFinanceReport(ctx context.Context, request financeReportRequest) {
	started := time.Now()
	_, _, err := m.financeReportPayload(ctx, request, true)
	if err != nil && !errors.Is(err, context.Canceled) {
		slog.Warn("经营核算后台刷新失败，保留上次结果", "from", request.from.Unix(), "to", request.to.Unix(),
			"elapsed_ms", time.Since(started).Milliseconds(), "err", err)
	}
}
