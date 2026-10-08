package monitor

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// Only explicitly reviewed projections are eligible for dated display while
// rebuilding. They are not current proof or eligible for promotion into the
// current key. v4 retains known amounts but invalidates old exact contribution
// claims, whose domain-count check did not prove complete hourly pairing.
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
	if financeReportProjection != "accounting-pairing-hour-diagnostics-v8" || stabilityTrafficClassificationVersion != 7 ||
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
		for _, projection := range []string{"accounting-recharge-zero-proof-v7", "accounting-platform-recharge-v6", "accounting-gift-horizon-v5", "accounting-pairing-proof-v4", "accounting-partial-correction-v3", "accounting-amount-evidence-v2", "accounting-delivery-compat-v1"} {
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
			// v8 adds read-only hour diagnostics, with no monetary or period
			// formula change. Keep v7's dated money intact while rebuilding the
			// report shell; unchanged monthly components remain reusable.
			if projection == "accounting-recharge-zero-proof-v7" {
				return payload, true, nil
			}
			// v6 already adopted native TokenForce face values. Older projections
			// need the provider gate before any amount can be displayed.
			if projection != "accounting-platform-recharge-v6" {
				compatible, err := financePriorCostPolicyCompatible(payload)
				if err != nil {
					return nil, false, err
				}
				if !compatible {
					continue // Prior TokenForce currency-converted amounts need rebuilding.
				}
			}
			payload, err = invalidateFinancePriorContribution(payload)
			if err != nil {
				return nil, false, err
			}
			return payload, true, nil
		}
	}
	return nil, false, nil
}

// Keep unknown/legacy fields and integer precision intact. Only the aggregate
// exact contribution and its margin need the new v4 completeness proof. Known
// amounts remain available as explicitly dated display while rebuilding.
func invalidateFinancePriorContribution(payload []byte) ([]byte, error) {
	var report map[string]json.RawMessage
	if err := json.Unmarshal(payload, &report); err != nil {
		return nil, err
	}
	clearStatement := func(container map[string]json.RawMessage) (bool, error) {
		raw, ok := container["statement"]
		if !ok || bytes.Equal(bytes.TrimSpace(raw), []byte("null")) {
			return false, nil
		}
		var statement map[string]json.RawMessage
		if err := json.Unmarshal(raw, &statement); err != nil {
			return false, err
		}
		changed := false
		for _, key := range []string{"contribution_profit", "contribution_margin_percent"} {
			if value, ok := statement[key]; ok && !bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
				statement[key], changed = json.RawMessage("null"), true
			}
		}
		if !changed {
			return false, nil
		}
		value, err := json.Marshal(statement)
		container["statement"] = value
		return true, err
	}
	changed, err := clearStatement(report)
	if err != nil {
		return nil, err
	}
	for _, key := range []string{"periods", "days"} {
		var periods []map[string]json.RawMessage
		if raw, ok := report[key]; ok {
			if err := json.Unmarshal(raw, &periods); err != nil {
				return nil, err
			}
		}
		periodsChanged := false
		for _, period := range periods {
			cleared, err := clearStatement(period)
			if err != nil {
				return nil, err
			}
			periodsChanged = periodsChanged || cleared
		}
		if periodsChanged {
			value, err := json.Marshal(periods)
			if err != nil {
				return nil, err
			}
			report[key], changed = value, true
		}
	}
	if !changed {
		return payload, nil
	}
	return json.Marshal(report)
}
