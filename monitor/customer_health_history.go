package monitor

// Cross-day coverage for the logchain-only customer-health source. A nonempty
// capacity table is not a coverage proof: each day's prefix is certified only
// after its source query and local fact write have succeeded.

import (
	"context"
	"fmt"
	"time"

	"gorm.io/gorm"
)

const customerHealthHistoryDays = int64(7)

// CustomerHealthDayCoverage is a continuous [day_ts, through_ts) certificate.
// FRT has an independent prefix because an upgraded deployment can replay
// first-data-event measurements without revoking already proven request facts.
type CustomerHealthDayCoverage struct {
	DayTs                 int64 `gorm:"primaryKey;autoIncrement:false;column:day_ts"`
	ThroughTs             int64 `gorm:"column:through_ts"`
	SemanticsVersion      int   `gorm:"column:semantics_version"`
	TTFTSemanticsVersion  int   `gorm:"column:ttft_semantics_version"`
	TTFTCoverageFromTs    int64 `gorm:"column:ttft_coverage_from_ts"`
	TTFTCoverageThroughTs int64 `gorm:"column:ttft_coverage_through_ts"`
	UpdatedAt             int64 `gorm:"column:updated_at"`
}

type CustomerHealthHistoricalCoverage struct {
	RequestFromTs    int64
	RequestThroughTs int64
	RequestsComplete bool
	FRTFromTs        int64
	FRTThroughTs     int64
	FRTComplete      bool
}

// persistCustomerHealthDayCoverageTx mirrors the current-day cursor in the
// same transaction as that cursor's update. It intentionally allows an FRT
// watermark to move backwards on a semantics migration; retaining the old
// high-water mark would certify FRT that has not been replayed yet. The caller
// holds customerHealthCursorMu.
func (m *Monitor) persistCustomerHealthDayCoverageTx(tx *gorm.DB, state CustomerHealthSourceCursor) error {
	if tx == nil || state.DayTs <= 0 || state.ThroughTs < state.DayTs ||
		state.ThroughTs > state.DayTs+24*3600 {
		return fmt.Errorf("invalid customer-health day cursor")
	}
	row := CustomerHealthDayCoverage{
		DayTs: state.DayTs, ThroughTs: state.ThroughTs,
		SemanticsVersion:      state.SemanticsVersion,
		TTFTSemanticsVersion:  state.TTFTSemanticsVersion,
		TTFTCoverageFromTs:    state.TTFTCoverageFromTs,
		TTFTCoverageThroughTs: state.TTFTCoverageThroughTs,
		UpdatedAt:             state.UpdatedAt,
	}
	return tx.Save(&row).Error
}

func customerHealthDayStart(ts int64) int64 {
	local := time.Unix(ts, 0).In(cstLocation)
	return time.Date(local.Year(), local.Month(), local.Day(), 0, 0, 0, 0, cstLocation).Unix()
}

// customerHealthHistoricalCoverage stitches consecutive certified day prefixes.
// The live cursor takes precedence over a same-day ledger row so a just-reset
// FRT replay cannot be masked by a stale high certificate in the ledger.
func (m *Monitor) customerHealthHistoricalCoverage(ctx context.Context, from, to int64) (CustomerHealthHistoricalCoverage, error) {
	proof := CustomerHealthHistoricalCoverage{}
	if m == nil || m.storeDB == nil || from <= 0 || to <= from {
		return proof, nil
	}
	firstDay, lastDay := customerHealthDayStart(from), customerHealthDayStart(to-1)
	var rows []CustomerHealthDayCoverage
	var live CustomerHealthSourceCursor
	var liveFound bool
	err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("day_ts >= ? AND day_ts <= ?", firstDay, lastDay).Find(&rows).Error; err != nil {
			return err
		}
		result := tx.Where("id = ?", 1).Limit(1).Find(&live)
		if result.Error != nil {
			return result.Error
		}
		liveFound = result.RowsAffected > 0
		return nil
	})
	if err != nil {
		return proof, err
	}
	byDay := make(map[int64]CustomerHealthDayCoverage, len(rows)+1)
	for _, row := range rows {
		byDay[row.DayTs] = row
	}
	if liveFound && live.DayTs >= firstDay && live.DayTs <= lastDay {
		byDay[live.DayTs] = CustomerHealthDayCoverage{
			DayTs: live.DayTs, ThroughTs: live.ThroughTs,
			SemanticsVersion:      live.SemanticsVersion,
			TTFTSemanticsVersion:  live.TTFTSemanticsVersion,
			TTFTCoverageFromTs:    live.TTFTCoverageFromTs,
			TTFTCoverageThroughTs: live.TTFTCoverageThroughTs,
		}
	}
	requestOpen, frtOpen := true, true
	for day := firstDay; day <= lastDay; day += 24 * 3600 {
		row, exists := byDay[day]
		segmentFrom, segmentTo := max(from, day), min(to, day+24*3600)
		if requestOpen {
			if !exists || row.SemanticsVersion != customerHealthStabilityPolicyVersion ||
				row.ThroughTs < segmentFrom || row.ThroughTs > day+24*3600 {
				requestOpen = false
			} else {
				if proof.RequestFromTs == 0 {
					proof.RequestFromTs = segmentFrom
				}
				proof.RequestThroughTs = min(segmentTo, row.ThroughTs)
				if proof.RequestThroughTs < segmentTo {
					requestOpen = false
				}
			}
		}
		if frtOpen {
			if !exists || row.TTFTSemanticsVersion != ttftCoverageSemanticsVersion ||
				row.TTFTCoverageFromTs != day || row.TTFTCoverageThroughTs < segmentFrom ||
				row.TTFTCoverageThroughTs > day+24*3600 {
				frtOpen = false
			} else {
				if proof.FRTFromTs == 0 {
					proof.FRTFromTs = segmentFrom
				}
				proof.FRTThroughTs = min(segmentTo, row.TTFTCoverageThroughTs)
				if proof.FRTThroughTs < segmentTo {
					frtOpen = false
				}
			}
		}
	}
	proof.RequestsComplete = requestOpen && proof.RequestFromTs == from && proof.RequestThroughTs >= to
	proof.FRTComplete = frtOpen && proof.FRTFromTs == from && proof.FRTThroughTs >= to
	return proof, nil
}

// runCustomerHealthHistoryTurnWith fills at most one historical hour, nearest
// missing day first. It runs on the existing low-priority source lane after
// today's cursor has caught up. Failed slices leave the durable day prefix
// untouched, so a restart resumes from the same interval.
func (m *Monitor) runCustomerHealthHistoryTurnWith(ctx context.Context, now time.Time, sample metricRangeSampler) (worked bool, err error) {
	if m == nil || m.storeDB == nil || sample == nil {
		return false, nil
	}
	if now.IsZero() {
		now = time.Now()
	}
	today, _ := customerHealthSourceRange(now)
	// The current-day range clamps its target to today's midnight. Historical
	// days must use the raw finalized watermark: at 00:00:30 the previous
	// day's final two minutes are still open and cannot be certified yet.
	closedTarget := customerHealthSourceFinalizedThrough(now)
	for age := int64(1); age <= customerHealthHistoryDays; age++ {
		day := today - age*24*3600
		target := min(day+24*3600, closedTarget)
		if target <= day {
			continue
		}
		var row CustomerHealthDayCoverage
		query := m.storeDB.WithContext(ctx).Where("day_ts = ?", day).Limit(1).Find(&row)
		if query.Error != nil {
			return false, query.Error
		}
		requestThrough, frtThrough := day, day
		if query.RowsAffected > 0 && row.SemanticsVersion == customerHealthStabilityPolicyVersion &&
			row.ThroughTs >= day && row.ThroughTs <= day+24*3600 {
			requestThrough = row.ThroughTs
		}
		if query.RowsAffected > 0 && row.TTFTSemanticsVersion == ttftCoverageSemanticsVersion &&
			row.TTFTCoverageFromTs == day && row.TTFTCoverageThroughTs >= day &&
			row.TTFTCoverageThroughTs <= day+24*3600 {
			frtThrough = row.TTFTCoverageThroughTs
		}
		if requestThrough >= target && frtThrough >= target {
			continue
		}
		from := min(requestThrough, frtThrough)
		to := customerHealthSourceNext(from, target)
		if _, err := sample(ctx, from, to); err != nil {
			return false, err
		}
		m.customerHealthCursorMu.Lock()
		err := m.storeDB.WithContext(ctx).Transaction(func(tx *gorm.DB) error {
			var current CustomerHealthDayCoverage
			result := tx.Where("day_ts = ?", day).Limit(1).Find(&current)
			if result.Error != nil {
				return result.Error
			}
			if result.RowsAffected == 0 {
				current = CustomerHealthDayCoverage{DayTs: day}
			}
			if current.SemanticsVersion != customerHealthStabilityPolicyVersion || current.ThroughTs < day || current.ThroughTs > day+24*3600 {
				current.ThroughTs = day
			}
			if current.TTFTSemanticsVersion != ttftCoverageSemanticsVersion || current.TTFTCoverageFromTs != day ||
				current.TTFTCoverageThroughTs < day || current.TTFTCoverageThroughTs > day+24*3600 {
				current.TTFTCoverageFromTs, current.TTFTCoverageThroughTs = day, day
			}
			// Both prefixes can advance only across the sampled interval. If a
			// different writer moved a prefix ahead, retain that greater proof.
			if from <= current.ThroughTs && to > current.ThroughTs {
				current.ThroughTs = to
			}
			if from <= current.TTFTCoverageThroughTs && to > current.TTFTCoverageThroughTs {
				current.TTFTCoverageThroughTs = to
			}
			current.SemanticsVersion = customerHealthStabilityPolicyVersion
			current.TTFTSemanticsVersion = ttftCoverageSemanticsVersion
			current.UpdatedAt = time.Now().Unix()
			return tx.Save(&current).Error
		})
		m.customerHealthCursorMu.Unlock()
		if err != nil {
			return false, err
		}
		return true, nil
	}
	// Prune only when no historical slice needs work, at most once per local
	// day. An active replay may take days; a DELETE transaction per 2-second
	// slice would contend with the sampler without improving retention.
	retainedDays := max(customerHealthHistoryDays, int64(m.cfg.RetentionDays))
	oldestDay := today - retainedDays*24*3600
	if m.customerHealthHistoryPrunedBefore.Load() != oldestDay {
		if err := m.pruneCustomerHealthHistoryBefore(oldestDay); err != nil {
			return false, err
		}
		m.customerHealthHistoryPrunedBefore.Store(oldestDay)
	}
	return false, nil
}

// Keep the oldest entire CST day that a rolling seven-day window may touch.
// Do not call pruneOlderThan here: the logchain-only lane does not own metric
// or token facts, and their retention lifecycle is separate.
func (m *Monitor) pruneCustomerHealthHistoryBefore(oldestDay int64) error {
	m.customerHealthCursorMu.Lock()
	defer m.customerHealthCursorMu.Unlock()
	return m.storeDB.Transaction(func(tx *gorm.DB) error {
		if err := tx.Where("bucket_ts < ?", oldestDay).Delete(&CapacityUserMinuteSample{}).Error; err != nil {
			return err
		}
		return tx.Where("day_ts < ?", oldestDay).Delete(&CustomerHealthDayCoverage{}).Error
	})
}
