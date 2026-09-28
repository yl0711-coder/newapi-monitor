package monitor

import (
	"errors"
	"fmt"
	"time"
)

// Only explicitly reviewed projections are eligible for dated display while
// rebuilding. Retain the previously supported deployed v1 snapshot as well as
// v2; neither is current proof or eligible for promotion into the current key.
func financePreviousProjectionKey(request financeReportRequest) string {
	return financeUpgradeProjectionKey(request, "accounting-amount-evidence-v2")
}

func financeUpgradeProjectionKey(request financeReportRequest, projection string) string {
	return fmt.Sprintf("%s:%d:%d:%d:%t:%s", projection, request.from.Unix(), request.to.Unix(),
		request.snapshotAsOf, request.snapshotClamped, request.configurationHash)
}

func (m *Monitor) loadFinanceUpgradeSnapshot(request financeReportRequest, now time.Time) ([]byte, bool, error) {
	// A future projection/classification change must opt in after its own
	// accounting review; it must not inherit this compatibility implicitly.
	if financeReportProjection != "accounting-partial-correction-v3" || stabilityTrafficClassificationVersion != 7 ||
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
		for _, projection := range []string{"accounting-amount-evidence-v2", "accounting-delivery-compat-v1"} {
			payload, storedAt, _, ok, err := m.loadFinanceReportSnapshotKey(candidate, financeUpgradeProjectionKey(candidate, projection), now)
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
	}
	return nil, false, nil
}
