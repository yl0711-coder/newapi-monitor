package monitor

import (
	"context"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// An independent proof of committed, closed request minutes. It is published
// with the existing minute facts, never from MAX(bucket_ts), and does not move
// the one-hour finalization watermark used by the other reports.
type GroupGovernanceMinuteCoverage struct {
	ID                uint `gorm:"primaryKey;autoIncrement:false"`
	FromTs, ThroughTs int64
	Version           int
	UpdatedAt         int64
}

func publishGGMinuteCoverage(tx *gorm.DB, from, to, now int64) error {
	through := min(to, now/60*60) / 60 * 60
	if from%60 != 0 || through <= from {
		return nil
	}
	var old GroupGovernanceMinuteCoverage
	if err := tx.Where("id=1").Limit(1).Find(&old).Error; err != nil {
		return err
	}
	if old.Version == stabilityTrafficClassificationVersion && old.ThroughTs > old.FromTs {
		if from <= old.ThroughTs && through >= old.FromTs {
			from, through = min(from, old.FromTs), max(through, old.ThroughTs)
		} else if through < old.ThroughTs {
			return nil // a disjoint historical replay cannot replace the live tail
		}
	}
	row := GroupGovernanceMinuteCoverage{ID: 1, FromTs: from, ThroughTs: through, Version: stabilityTrafficClassificationVersion, UpdatedAt: now}
	return tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&row).Error
}

func (m *Monitor) groupGovernanceWindow(ctx context.Context, now time.Time) (target, end, from int64, err error) {
	if m.cfg.CustomerHealthSourceEnabled {
		target = customerHealthSourceFinalizedThrough(now)
		end, err = m.modelStatisticsAvailableEnd(ctx, target, false)
		return
	}
	target = now.Unix() / 60 * 60
	var proof GroupGovernanceMinuteCoverage
	err = m.storeDB.WithContext(ctx).Where("id=1").Limit(1).Find(&proof).Error
	if err == nil && m.cfg.CapacityEnabled && proof.Version == stabilityTrafficClassificationVersion && proof.FromTs > 0 && proof.ThroughTs > proof.FromTs {
		from, end = proof.FromTs, min(target, proof.ThroughTs)
	}
	return
}
