package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"time"
)

const (
	// 经营核算是按闭合小时生成的管理报表，不需要像实时监控一样每次请求重算。
	// 五分钟新鲜期减少重复 SQLite 扫描；其后的十分钟只用于“立即展示旧结果、
	// 后台更新”，页面会显示 report.generated_at，手动刷新始终绕过缓存。
	financeReportCacheTTL        = 5 * time.Minute
	financeReportCacheStaleGrace = 10 * time.Minute
	financeReportCacheMaxEntries = 16
	financeReportCacheMaxBytes   = 12 << 20
	financeReportBuildTimeout    = 12 * time.Second
)

type financeReportRequest struct {
	from            time.Time
	to              time.Time
	snapshotAsOf    int64
	snapshotClamped bool
}

func (r financeReportRequest) cacheKey() string {
	return fmt.Sprintf("%d:%d:%d:%t", r.from.Unix(), r.to.Unix(), r.snapshotAsOf, r.snapshotClamped)
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

// financeReportPayload returns a bounded in-process cache result. It never
// writes SQLite and never calls NewAPI/upstream/AWS. Stale results are served
// only inside the short grace window while one coalesced background refresh
// rebuilds the exact same range.
func (m *Monitor) financeReportPayload(ctx context.Context, request financeReportRequest, forceFresh bool) ([]byte, string, error) {
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
	}

	payload, err := m.financeReportFlight.Do(ctx, key, func() ([]byte, error) {
		// A concurrent request may have completed between the first lookup and
		// winning the flight. Normal reads reuse it; explicit refreshes rebuild.
		if !forceFresh {
			if cached, ok := cache.Get(key, time.Now()); ok {
				return cached, nil
			}
		}
		built, buildErr := m.buildFinanceReportPayload(ctx, request)
		if buildErr != nil {
			return nil, buildErr
		}
		cache.PutWithStale(key, built, financeReportCacheTTL, financeReportCacheStaleGrace, time.Now())
		return built, nil
	})
	if err != nil {
		return nil, "miss", err
	}
	if forceFresh {
		return payload, "refresh", nil
	}
	return payload, "miss", nil
}

func (m *Monitor) refreshFinanceReportAsync(request financeReportRequest) {
	go func() {
		ctx, cancel := context.WithTimeout(m.taskContext(), financeReportBuildTimeout)
		defer cancel()
		_, _, _ = m.financeReportPayload(ctx, request, true)
	}()
}
