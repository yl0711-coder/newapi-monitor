package monitor

import (
	"context"
	"errors"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errInfraHostGeneration = errors.New("sample cannot be attributed to the current host generation")

// Only a unique first cloud generation may inherit a legacy alias. Existing
// explicit cloud actions take precedence. No older cloud tombstone is copied.
func linkInfraLegacyHost(tx *gorm.DB, node string, now int64) error {
	var clouds []InfraAsset
	if err := tx.Where("resource = ? AND kind = ?", node, "instance").Limit(2).Find(&clouds).Error; err != nil {
		return err
	}
	if len(clouds) != 1 {
		return nil
	}
	var legacy InfraAsset
	err := tx.First(&legacy, "identity = ?", "host-name:"+node).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	if legacy.State == "linked" {
		return nil
	}
	cloud := clouds[0]
	updates := map[string]any{"last_report": max(cloud.LastReport, legacy.LastReport), "last_sample_at": max(cloud.LastSampleAt, legacy.LastSampleAt)}
	if cloud.Revision == 1 {
		updates["state"], updates["archived_at"], updates["removed_at"] = legacy.State, legacy.ArchivedAt, legacy.RemovedAt
	}
	revision := cloud.Revision + 1
	updates["revision"] = revision
	if err := tx.Model(&cloud).Updates(updates).Error; err != nil {
		return err
	}
	if err := tx.Model(&legacy).Updates(map[string]any{"state": "linked", "revision": legacy.Revision + 1}).Error; err != nil {
		return err
	}
	return tx.Create(&InfraAssetAudit{AssetID: cloud.ID, Action: "link", Actor: "discovery", At: now, Revision: revision}).Error
}

// Identity resolution, registration and telemetry writes share one rollback-
// safe transaction. WAL upgrade conflicts retry the entire resolution, never
// only the final INSERT against a stale name lookup.
func (m *Monitor) acceptInfraHostReport(ctx context.Context, node string, now, sampleAt int64, write func(*gorm.DB) error) error {
	if sampleAt <= 0 || sampleAt > now || now-sampleAt > m.infraMetricFreshnessSec() {
		sampleAt = 0
	}
	return m.infraAssetWrite(ctx, 3*time.Second, func(tx *gorm.DB) error {
		var clouds []InfraAsset
		if err := tx.Where("resource = ? AND kind = ?", node, "instance").Order("last_discovered DESC, first_seen DESC").Limit(infraAssetLimit + 1).Find(&clouds).Error; err != nil {
			return err
		}
		multiple := len(clouds) > 1
		if multiple {
			if len(clouds) > infraAssetLimit {
				return errInfraHostGeneration
			}
			var current []InfraAsset
			for _, a := range clouds {
				if a.CloudState != "missing" && a.State != "removed" {
					current = append(current, a)
				}
			}
			if len(current) != 1 || sampleAt <= current[0].FirstSeen {
				return errInfraHostGeneration
			}
			clouds = current
		}
		if len(clouds) == 1 {
			if err := linkInfraLegacyHost(tx, node, now); err != nil {
				return err
			}
			if err := tx.Model(&InfraAsset{}).Where("id = ?", clouds[0].ID).Updates(map[string]any{"last_report": gorm.Expr("MAX(last_report, ?)", now), "last_sample_at": gorm.Expr("MAX(last_sample_at, ?)", sampleAt)}).Error; err != nil {
				return err
			}
		} else {
			identity := "host-name:" + node
			a := InfraAsset{ID: infraAssetID(identity), Identity: identity, Scope: "host", Resource: node, Kind: "host", Platform: "Host agent", State: "active", CloudState: "unknown", FirstSeen: now, LastReport: now, LastSampleAt: sampleAt, Revision: 1}
			if err := tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "id"}}, DoUpdates: clause.Assignments(map[string]any{"last_report": gorm.Expr("MAX(last_report, ?)", now), "last_sample_at": gorm.Expr("MAX(last_sample_at, ?)", sampleAt)})}).Create(&a).Error; err != nil {
				return err
			}
		}
		if write != nil {
			return write(tx)
		}
		return nil
	})
}

// Name-keyed legacy buckets cannot distinguish two incarnations within one
// minute. Exclude the entire boundary minute rather than invent ownership.
func infraGenerationStart(firstSeen int64) int64 { return (firstSeen/60 + 1) * 60 }
