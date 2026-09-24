package monitor

import (
	"context"
	"encoding/json"
	"log/slog"
	"time"
)

const (
	// This is a display-only shortcut for an already verified report, not a
	// substitute for the source fingerprint used by a new financial build.
	financeFastSnapshotMaxAge       = 3 * time.Minute
	financeFastSnapshotRefreshAfter = time.Minute
)

// financeFastSnapshotPayload avoids a full historical SQLite fingerprint on
// repeated page opens. The payload is always labeled as a dated snapshot;
// a fresh=1 request and every background rebuild still verify the exact source
// fingerprint before publishing a new report. A changed configuration has a
// different logical key and can never reuse the old snapshot.
func (m *Monitor) financeFastSnapshotPayload(request financeReportRequest, now time.Time) ([]byte, string, bool) {
	if m == nil || !m.cfg.FinanceFastSnapshotEnabled || request.configurationHash == "" {
		return nil, "", false
	}
	payload, storedAt, _, ok, err := m.loadFinanceReportSnapshot(request, now)
	if err != nil {
		slog.Warn("经营核算快速快照读取失败，回退事实核验", "err", err)
		return nil, "", false
	}
	if !ok || storedAt.After(now) || now.Sub(storedAt) > financeFastSnapshotMaxAge {
		return nil, "", false
	}
	var bounds struct {
		From        int64 `json:"from"`
		To          int64 `json:"to"`
		GeneratedAt int64 `json:"generated_at"`
	}
	if err := json.Unmarshal(payload, &bounds); err != nil || bounds.From != request.from.Unix() ||
		bounds.To != request.to.Unix() || bounds.GeneratedAt <= 0 ||
		bounds.GeneratedAt > storedAt.Add(time.Minute).Unix() {
		slog.Warn("经营核算快速快照范围或生成时间无效，回退事实核验")
		return nil, "", false
	}
	if now.Sub(storedAt) >= financeFastSnapshotRefreshAfter {
		m.scheduleFinanceFastSnapshotRefresh(request, now)
	}
	return payload, "fast-snapshot-stale", true
}

// One Monitor process runs at most one fast-snapshot-triggered version check in
// a one-minute interval. This limit is global on purpose: each check scans
// the same SQLite stores, regardless of which browser or date range asked.
func (m *Monitor) scheduleFinanceFastSnapshotRefresh(request financeReportRequest, now time.Time) {
	for {
		next := m.financeFastRefreshAfter.Load()
		if now.UnixNano() < next {
			return
		}
		if m.financeFastRefreshAfter.CompareAndSwap(next, now.Add(financeFastSnapshotRefreshAfter).UnixNano()) {
			if m.taskContext().Err() != nil || !m.financeReportRefreshRunning.CompareAndSwap(false, true) {
				return
			}
			go func() {
				defer m.financeReportRefreshRunning.Store(false)
				ctx, cancel := context.WithTimeout(m.taskContext(), financeReportBuildTimeout)
				defer cancel()
				m.refreshFinanceFastSnapshot(ctx, request)
			}()
			return
		}
	}
}

// An unchanged source version only renews the small file snapshot. Changed
// source facts go through the ordinary full-build consistency fence.
func (m *Monitor) refreshFinanceFastSnapshot(ctx context.Context, request financeReportRequest) {
	started := time.Now()
	fingerprint, err := m.financeReportSourceFingerprint(ctx, request.from.Unix(), request.to.Unix())
	if err != nil {
		slog.Warn("经营核算快速快照事实核验失败，保留旧快照", "elapsed_ms", time.Since(started).Milliseconds(), "err", err)
		return
	}
	request.sourceFingerprint = fingerprint
	payload, _, state, ok, readErr := m.loadFinanceReportSnapshot(request, time.Now())
	if readErr != nil {
		slog.Warn("经营核算快速快照复核读取失败，回退完整计算", "err", readErr)
	}
	if readErr == nil && ok && state == "fresh" {
		m.financeSnapshotWriteMu.Lock()
		err = m.persistFinanceReportSnapshotShadow(request.logicalKey(), fingerprint, payload, time.Now())
		m.financeSnapshotWriteMu.Unlock()
		if err != nil {
			slog.Warn("经营核算快速快照续期失败，保留原快照", "err", err)
		}
		return
	}
	m.refreshFinanceReport(ctx, request)
}
