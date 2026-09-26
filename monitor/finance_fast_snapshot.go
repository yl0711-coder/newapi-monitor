package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	// This is a display-only shortcut for an already verified report, not a
	// substitute for the source fingerprint used by a new financial build.
	financeFastSnapshotMaxAge = financeReportPersistentStale
)

// financeFastSnapshotPayload avoids a full historical SQLite fingerprint on
// repeated page opens. The payload is always labeled as a dated snapshot;
// the queue verifies the source fingerprint before publishing a new report.
// A changed configuration has a
// different logical key and can never reuse the old snapshot.
func (m *Monitor) financeFastSnapshotPayload(request financeReportRequest, now time.Time) ([]byte, string, bool) {
	if m == nil || !m.cfg.FinanceFastSnapshotEnabled || request.configurationHash == "" {
		return nil, "", false
	}
	if payload, ok := m.getFinanceReportCache().GetStale(request.lastGoodKey(), now); ok && validFinanceSnapshotBounds(payload, request, now) {
		return payload, "fast-snapshot-stale", true
	}
	payload, storedAt, _, ok, err := m.loadFinanceReportSnapshot(request, now)
	if err != nil {
		slog.Warn("经营核算快速快照读取失败，回退事实核验", "err", err)
		return nil, "", false
	}
	if !ok || storedAt.After(now) || now.Sub(storedAt) > financeFastSnapshotMaxAge {
		return nil, "", false
	}
	if !validFinanceSnapshotBounds(payload, request, storedAt) {
		slog.Warn("经营核算快速快照范围或生成时间无效，回退后台核验")
		return nil, "", false
	}
	return payload, "fast-snapshot-stale", true
}

func validFinanceSnapshotBounds(payload []byte, request financeReportRequest, verifiedAt time.Time) bool {
	var bounds struct {
		From        int64 `json:"from"`
		To          int64 `json:"to"`
		GeneratedAt int64 `json:"generated_at"`
	}
	return json.Unmarshal(payload, &bounds) == nil && bounds.From == request.from.Unix() &&
		bounds.To == request.to.Unix() && bounds.GeneratedAt > 0 && bounds.GeneratedAt <= verifiedAt.Add(time.Minute).Unix()
}

// This path never computes source fingerprints on the HTTP goroutine.
func (m *Monitor) serveFinanceQueuedReport(c *gin.Context, request financeReportRequest) {
	payload, status, available := m.financeFastSnapshotPayload(request, time.Now())
	if !available {
		m.financeAsyncQueue.forgetMissingResult(request.logicalKey())
	}
	state := m.financeAsyncQueue.submit(m.taskContext(), request.logicalKey(), c.Query("fresh") == "1", func(ctx context.Context) error {
		return m.refreshFinanceFastSnapshot(ctx, request)
	})
	if state == "succeeded" {
		// The worker may have published between the initial read and submit.
		payload, status, available = m.financeFastSnapshotPayload(request, time.Now())
	}
	c.Header("X-Monitor-Finance-Update", state)
	if available {
		c.Header("X-Monitor-Finance-Cache", status)
		c.Data(http.StatusOK, "application/json; charset=utf-8", payload)
		return
	}
	if payload, ok, err := m.loadPriorFinanceReportSnapshot(request, time.Now()); err == nil && ok {
		c.Header("X-Monitor-Finance-Cache", "persistent-prior-stale-queued")
		c.Data(http.StatusOK, "application/json; charset=utf-8", payload)
		return
	}
	c.Header("Retry-After", "5")
	code := http.StatusAccepted
	message := "报表正在后台生成，请稍后查看"
	if state == "failed" || state == "stopped" {
		code = http.StatusServiceUnavailable
		message = "报表更新暂未完成，请稍后重试；详情见数据同步状态"
	} else if state == "busy" {
		message = "报表计算繁忙，请稍后重试"
	}
	c.JSON(code, gin.H{"status": state, "message": message, "error": message, "from": request.from.Unix(), "to": request.to.Unix(), "retry_after_seconds": 5})
}

// An unchanged version warms memory without rewriting the report file.
// Changed facts retain the existing before/after consistency fence.
func (m *Monitor) refreshFinanceFastSnapshot(ctx context.Context, request financeReportRequest) (err error) {
	started := time.Now()
	defer func() {
		if err != nil {
			slog.Warn("经营核算后台核验失败，保留旧结果", "elapsed_ms", time.Since(started).Milliseconds(), "err", err)
		}
	}()
	config, err := m.financeReportConfigurationHash(ctx)
	if err != nil {
		return err
	}
	if config != request.configurationHash {
		return fmt.Errorf("经营核算配置已变化，跳过旧配置任务")
	}
	fingerprint, err := m.financeReportSourceFingerprint(ctx, request.from.Unix(), request.to.Unix())
	if err != nil {
		return err
	}
	request.sourceFingerprint = fingerprint
	if payload, ok := m.getFinanceReportCache().Get(request.cacheKey(), time.Now()); ok {
		m.rememberFinanceReport(request, payload, time.Now())
		return nil
	}
	payload, _, state, ok, readErr := m.loadFinanceReportSnapshot(request, time.Now())
	if readErr != nil {
		slog.Warn("经营核算快速快照复核读取失败，回退完整计算", "err", readErr)
	}
	if readErr == nil && ok && state == "fresh" && validFinanceSnapshotBounds(payload, request, time.Now()) {
		m.rememberFinanceReport(request, payload, time.Now())
		return nil
	}
	buildStarted := time.Now()
	mainBefore, factsBefore, readBefore := financeDBPoolStats(m.storeDB), financeDBPoolStats(m.usageFactsDB), financeDBPoolStats(m.financeFactsReadDB.Load())
	built, accepted, err := m.buildFinanceReportWithRetry(ctx, request)
	m.logFinanceBuildTiming(buildStarted, mainBefore, factsBefore, readBefore, err)
	if err != nil {
		return err
	}
	m.rememberFinanceReport(accepted, built, time.Now())
	m.financeSnapshotWriteMu.Lock()
	err = m.persistFinanceReportSnapshotShadow(accepted.logicalKey(), accepted.sourceFingerprint, built, time.Now())
	m.financeSnapshotWriteMu.Unlock()
	return err
}
