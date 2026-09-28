package monitor

import (
	"errors"
	"fmt"
	"time"
)

// Only the immediate pre-delivery-upgrade projection is eligible. Its money
// units and configuration hash are unchanged. This is a dated display fallback,
// never a component of a current calculation or a fresh/source-verified cache.
// Do not expand this allowlist when changing accounting arithmetic.
func financePreviousProjectionKey(request financeReportRequest) string {
	return fmt.Sprintf("daily-internal-diagnostics-v1:%d:%d:%d:%t:%s", request.from.Unix(), request.to.Unix(),
		request.snapshotAsOf, request.snapshotClamped, request.configurationHash)
}

func (m *Monitor) loadFinanceUpgradeSnapshot(request financeReportRequest, now time.Time) ([]byte, bool, error) {
	// A future projection/classification change must opt in after its own
	// accounting review; it must not inherit this compatibility implicitly.
	if financeReportProjection != "accounting-delivery-compat-v1" || stabilityTrafficClassificationVersion != 7 ||
		!m.cfg.FinanceFastSnapshotEnabled || !m.cfg.FinanceReportSnapshotReadEnabled ||
		request.configurationHash == "" || request.snapshotAsOf != 0 || request.snapshotClamped {
		return nil, false, nil
	}
	// Check the exact old range, then earlier closed-hour ranges. The existing
	// retention, hash and configuration checks still apply. No DB queries, writes,
	// source calls, refreshed timestamps or promotion into current cache keys.
	for hours := time.Duration(0); hours <= financeReportPersistentStale/time.Hour; hours++ {
		candidate := request
		candidate.to = request.to.Add(-hours * time.Hour)
		candidate.sourceFingerprint = ""
		if !candidate.from.Before(candidate.to) {
			break
		}
		payload, storedAt, _, ok, err := m.loadFinanceReportSnapshotKey(candidate, financePreviousProjectionKey(candidate), now)
		if err != nil {
			return nil, false, err
		}
		if !ok {
			continue
		}
		if !validFinanceSnapshotBounds(payload, candidate, storedAt) {
			return nil, false, errors.New("经营核算升级前快照范围或生成时间无效")
		}
		return payload, true, nil
	}
	return nil, false, nil
}
