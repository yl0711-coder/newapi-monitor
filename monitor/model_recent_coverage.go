package monitor

import (
	"context"
	"gorm.io/gorm"
)

type modelMinuteInterval struct{ From, Through int64 }

func (v modelMinuteInterval) valid() bool {
	return v.From > 0 && v.Through > v.From && v.From%60 == 0 && v.Through%60 == 0
}
func (v modelMinuteInterval) covers(from, to int64) bool {
	return v.valid() && v.From <= from && v.Through >= to
}

// Select a certified interval, merging only overlap/adjacency. A disjoint live
// tail can move the report's right edge, but can never fill a historical gap.
func modelMergeMinuteProof(old, recent modelMinuteInterval, to int64) modelMinuteInterval {
	if !recent.valid() {
		return old
	}
	if !old.valid() {
		return recent
	}
	if old.From <= recent.Through && recent.From <= old.Through {
		return modelMinuteInterval{min(old.From, recent.From), max(old.Through, recent.Through)}
	}
	if to > 0 && to <= old.Through {
		return old
	}
	if recent.Through > old.Through {
		return recent
	}
	return old
}

// The ordinary sampler commits both request and FRT projections atomically.
// Reuse that proof without changing Capacity/Stability's finalization policy.
// Row semantics, exact sums and projection equality are checked separately.
func (m *Monitor) modelMinuteProof(ctx context.Context, to int64) (requests, frt modelMinuteInterval, err error) {
	var historical MetricFinalizeState
	if err = m.storeDB.WithContext(ctx).Where("id=1").Limit(1).Find(&historical).Error; err != nil {
		return
	}
	if historical.SemanticsVersion == stabilityTrafficClassificationVersion {
		requests = modelMinuteInterval{historical.CoverageFromTs, historical.NextTs}
	}
	if historical.TTFTSemanticsVersion == ttftCoverageSemanticsVersion {
		frt = modelMinuteInterval{historical.TTFTCoverageFromTs, historical.TTFTCoverageThroughTs}
	}
	var count int64
	if err = m.storeDB.WithContext(ctx).Raw("SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='group_governance_minute_coverages'").Scan(&count).Error; err != nil {
		return
	}
	if count == 0 {
		return
	} // Read-only pre-upgrade snapshots remain supported.
	var recent GroupGovernanceMinuteCoverage
	if err = m.storeDB.WithContext(ctx).Where("id=1").Limit(1).Find(&recent).Error; err != nil {
		return
	}
	if recent.Version == stabilityTrafficClassificationVersion {
		tail := modelMinuteInterval{recent.FromTs, recent.ThroughTs}
		requests = modelMergeMinuteProof(requests, tail, to)
		frt = modelMergeMinuteProof(frt, tail, to)
	}
	return
}

func publishModelMinuteCoverage(tx *gorm.DB, from, to, now int64) error {
	// Kept in the existing table: no additional collector, lease or watermark.
	return publishGGMinuteCoverage(tx, from, to, now)
}
