package monitor

import (
	"context"
	"gorm.io/gorm"
)

func initTokenDetectionCleanup(tx *gorm.DB) error {
	for _, sql := range []string{
		`CREATE TABLE IF NOT EXISTS token_detection_cleanup_v1(source_epoch TEXT PRIMARY KEY,cursor_at INTEGER NOT NULL DEFAULT 0,cursor_id INTEGER NOT NULL DEFAULT 0,next_at INTEGER NOT NULL DEFAULT 0,last_reason TEXT NOT NULL DEFAULT '',updated_at INTEGER NOT NULL DEFAULT 0)`,
		`CREATE INDEX IF NOT EXISTS idx_token_detection_cleanup_v1 ON token_detection_currents(source_epoch,expires_at,log_id)`,
		`CREATE TABLE IF NOT EXISTS token_reference_cleanup_v1(id INTEGER PRIMARY KEY,cursor TEXT NOT NULL DEFAULT '',next_at INTEGER NOT NULL DEFAULT 0,deleted_rows INTEGER NOT NULL DEFAULT 0,deleted_bytes INTEGER NOT NULL DEFAULT 0)`,
		`INSERT OR IGNORE INTO token_reference_cleanup_v1(id) VALUES(1)`,
	} {
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
	}
	return nil
}

// Expiry alone is not deletion permission. Preserve every referenced payload,
// all four-day detail, and the earliest calibration identity in each bucket.
// Cursor movement and accounting commit with the examined bounded prefix.
func (m *Monitor) cleanupTokenReferences(ctx context.Context, now int64) error {
	var position struct {
		Cursor string
		NextAt int64
	}
	if err := m.tokenReadStore().WithContext(ctx).Table("token_reference_cleanup_v1").Where("id=1").Take(&position).Error; err != nil {
		return err
	}
	if position.NextAt > now || m.tokenLocalWriteMu.collectionQueued() {
		return nil
	}
	var rows []TokenDetectionReference
	if err := m.tokenReadStore().WithContext(ctx).Select("id").Where("id>?", position.Cursor).Order("id").Limit(32).Find(&rows).Error; err != nil {
		return err
	}
	return m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		var backup bool
		if err := tx.Raw(`SELECT EXISTS(SELECT 1 FROM token_metering_states WHERE source_epoch=? AND cleanup_backup_id<>'' AND cleanup_verified_at>0 AND cleanup_stable_since>0 AND cleanup_verified_at-cleanup_stable_since>=1800)
   AND EXISTS(SELECT 1 FROM token_revision_members_checkpoint_v1 WHERE id=1 AND ready=1)
   AND NOT EXISTS(SELECT 1 FROM token_revision_members_dirty_v1 LIMIT 1)`, m.cfg.TokenFactsSourceEpoch).Scan(&backup).Error; err != nil {
			return err
		}
		if !backup {
			return nil
		}
		var deleted, bytes int64
		cursor := position.Cursor
		for _, row := range rows {
			var ref TokenDetectionReference
			found := tx.Where(`id=? AND expires_at<=? AND recorded_at<?
    AND NOT EXISTS(SELECT 1 FROM token_revision_members_v1 m WHERE m.reference_id=token_detection_references.id)
    AND NOT EXISTS(SELECT 1 FROM token_detection_currents c WHERE c.bucket=token_detection_references.bucket)
    AND NOT EXISTS(SELECT 1 FROM token_reference_members_invalid_v1 i WHERE i.reference_id=token_detection_references.id)`, row.ID, now, now-4*86400).Limit(1).Find(&ref)
			if found.Error != nil {
				return found.Error
			}
			cursor = row.ID
			if found.RowsAffected == 0 {
				continue
			}
			var anchor string
			if err := tx.Model(&TokenDetectionReference{}).Select("id").Where("bucket=?", ref.Bucket).Order("through,id").Limit(1).Scan(&anchor).Error; err != nil {
				return err
			}
			if anchor == ref.ID {
				if ref.InvalidReason == "anchor_expired_requires_calibration" {
					continue
				}
				body := `{"status":"unavailable","reason":"anchor_expired_requires_calibration","samples":[]}`
				if err := tx.Model(&TokenDetectionReference{}).Where("id=?", ref.ID).Updates(map[string]any{"snapshot_json": body, "stored_bytes": len(body), "invalid_reason": "anchor_expired_requires_calibration"}).Error; err != nil {
					return err
				}
				bytes += ref.StoredBytes - int64(len(body))
			} else {
				if err := tx.Where("id=?", ref.ID).Delete(&TokenDetectionReference{}).Error; err != nil {
					return err
				}
				deleted++
				bytes += ref.StoredBytes
			}
		}
		next := int64(0)
		if len(rows) < 32 {
			cursor = ""
			next = now + 300
		}
		return tx.Table("token_reference_cleanup_v1").Where("id=1 AND cursor=?", position.Cursor).Updates(map[string]any{"cursor": cursor, "next_at": next, "deleted_rows": gorm.Expr("deleted_rows+?", deleted), "deleted_bytes": gorm.Expr("deleted_bytes+?", bytes)}).Error
	})
}

// An orphan is not evidence that work completed. This predicate requires a
// verified backup, a stable/reconciled boundary, a final result and no retained
// work or dependency. Unknown legacy heads deliberately remain protected.
const tokenDetectionCleanupEligible = `
	expires_at<? AND created_at<? AND detection_state IN ('not_hit','local_formula_consistent')
	AND source_complete=1 AND stage>=3 AND COALESCE(next_at,0)=0
	AND EXISTS (SELECT 1 FROM token_metering_states s WHERE s.source_epoch=token_detection_currents.source_epoch
	 AND s.cleanup_backup_id<>'' AND s.cleanup_safe_before>token_detection_currents.created_at
	 AND s.cleanup_verified_at>=token_detection_currents.evaluated_at AND s.cleanup_stable_since>0
	 AND s.cleanup_verified_at-s.cleanup_stable_since>=1800 AND s.cleanup_next_at<=?)
	AND EXISTS (SELECT 1 FROM token_reference_members_checkpoint_v1 WHERE id=1 AND ready=1)
	AND EXISTS (SELECT 1 FROM token_revision_members_checkpoint_v1 WHERE id=1 AND ready=1)
	AND NOT EXISTS (SELECT 1 FROM token_reference_members_invalid_v1 LIMIT 1)
	AND NOT EXISTS (SELECT 1 FROM token_revision_members_dirty_v1 LIMIT 1)
	AND NOT EXISTS (SELECT 1 FROM token_metering_facts f WHERE f.source_epoch=token_detection_currents.source_epoch AND f.log_id=token_detection_currents.log_id)
	AND NOT EXISTS (SELECT 1 FROM token_detection_retries r WHERE r.source_epoch=token_detection_currents.source_epoch AND r.log_id=token_detection_currents.log_id)
	AND NOT EXISTS (SELECT 1 FROM token_reference_members_v1 r WHERE r.source_epoch=token_detection_currents.source_epoch AND r.log_id=token_detection_currents.log_id)
	AND NOT EXISTS (SELECT 1 FROM token_revision_members_v1 r WHERE r.source_epoch=token_detection_currents.source_epoch AND r.log_id=token_detection_currents.log_id)
	AND NOT EXISTS (SELECT 1 FROM token_detection_revisions r WHERE r.source_epoch=token_detection_currents.source_epoch AND r.log_id=token_detection_currents.log_id AND r.expires_at>=?)`
