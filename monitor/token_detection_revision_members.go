package monitor

import (
	"encoding/json"
	"errors"

	"gorm.io/gorm"
)

func initTokenRevisionMembers(tx *gorm.DB) error {
	for _, sql := range []string{
		`CREATE TABLE IF NOT EXISTS token_revision_members_v1(owner_epoch TEXT NOT NULL,owner_id INTEGER NOT NULL,revision INTEGER NOT NULL,source_epoch TEXT NOT NULL,log_id INTEGER NOT NULL,reference_id TEXT NOT NULL,PRIMARY KEY(owner_epoch,owner_id,revision,source_epoch,log_id,reference_id))`,
		`CREATE INDEX IF NOT EXISTS idx_token_revision_member_fact_v1 ON token_revision_members_v1(source_epoch,log_id,owner_epoch,owner_id)`,
		`CREATE INDEX IF NOT EXISTS idx_token_revision_member_reference_v1 ON token_revision_members_v1(reference_id,owner_epoch,owner_id)`,
		`CREATE TABLE IF NOT EXISTS token_revision_members_dirty_v1(source_epoch TEXT NOT NULL,log_id INTEGER NOT NULL,revision INTEGER NOT NULL,PRIMARY KEY(source_epoch,log_id,revision))`,
		`CREATE TABLE IF NOT EXISTS token_revision_members_checkpoint_v1(id INTEGER PRIMARY KEY,cursor_epoch TEXT NOT NULL,cursor_id INTEGER NOT NULL,cursor_revision INTEGER NOT NULL,ready INTEGER NOT NULL)`,
		`INSERT OR IGNORE INTO token_revision_members_checkpoint_v1 VALUES(1,'',0,0,NOT EXISTS(SELECT 1 FROM token_detection_revisions LIMIT 1))`,
		`CREATE TRIGGER IF NOT EXISTS token_revision_members_insert_v1 AFTER INSERT ON token_detection_revisions BEGIN INSERT OR IGNORE INTO token_revision_members_dirty_v1 VALUES(NEW.source_epoch,NEW.log_id,NEW.revision); END`,
		`CREATE TRIGGER IF NOT EXISTS token_revision_members_update_v1 AFTER UPDATE OF frozen_json ON token_detection_revisions BEGIN INSERT OR IGNORE INTO token_revision_members_dirty_v1 VALUES(NEW.source_epoch,NEW.log_id,NEW.revision); END`,
		`CREATE TRIGGER IF NOT EXISTS token_revision_members_delete_v1 AFTER DELETE ON token_detection_revisions BEGIN DELETE FROM token_revision_members_v1 WHERE owner_epoch=OLD.source_epoch AND owner_id=OLD.log_id AND revision=OLD.revision; DELETE FROM token_revision_members_dirty_v1 WHERE source_epoch=OLD.source_epoch AND log_id=OLD.log_id AND revision=OLD.revision; END`,
	} {
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
	}
	return nil
}

func saveTokenRevisionMembers(tx *gorm.DB, row TokenDetectionRevision) error {
	var f tokenDetectionFrozen
	if json.Unmarshal([]byte(row.FrozenJSON), &f) != nil || f.Version != tokenDetectionVersion {
		return errors.New("token revision dependency evidence invalid")
	}
	if err := tx.Exec("DELETE FROM token_revision_members_v1 WHERE owner_epoch=? AND owner_id=? AND revision=?", row.SourceEpoch, row.LogID, row.Revision).Error; err != nil {
		return err
	}
	for _, s := range f.Result.Reference.Samples {
		if s.Epoch == "" || s.LogID <= 0 {
			return errors.New("token reference sample identity missing")
		}
		if err := tx.Exec("INSERT OR IGNORE INTO token_revision_members_v1 VALUES(?,?,?,?,?,?)", row.SourceEpoch, row.LogID, row.Revision, s.Epoch, s.LogID, "").Error; err != nil {
			return err
		}
	}
	for _, id := range []string{f.Result.Reference.ID, f.Result.Reference.ParentID} {
		if id != "" {
			if err := tx.Exec("INSERT OR IGNORE INTO token_revision_members_v1 VALUES(?,?,?,?,?,?)", row.SourceEpoch, row.LogID, row.Revision, "", 0, id).Error; err != nil {
				return err
			}
		}
	}
	return tx.Exec("DELETE FROM token_revision_members_dirty_v1 WHERE source_epoch=? AND log_id=? AND revision=?", row.SourceEpoch, row.LogID, row.Revision).Error
}

func migrateTokenRevisionMembers(tx *gorm.DB) (bool, error) {
	var checkpoint struct {
		CursorEpoch              string
		CursorID, CursorRevision int64
		Ready                    bool
	}
	if err := tx.Table("token_revision_members_checkpoint_v1").Where("id=1").Take(&checkpoint).Error; err != nil {
		return false, err
	}
	var rows []TokenDetectionRevision
	if checkpoint.Ready {
		if err := tx.Raw(`WITH dirty AS MATERIALIZED (SELECT source_epoch,log_id,revision FROM token_revision_members_dirty_v1 ORDER BY source_epoch,log_id,revision LIMIT 16)
		 SELECT r.* FROM dirty d CROSS JOIN token_detection_revisions r ON r.source_epoch=d.source_epoch AND r.log_id=d.log_id AND r.revision=d.revision
		 ORDER BY d.source_epoch,d.log_id,d.revision`).Find(&rows).Error; err != nil {
			return false, err
		}
	} else {
		if err := tx.Where("(source_epoch,log_id,revision)>(?,?,?)", checkpoint.CursorEpoch, checkpoint.CursorID, checkpoint.CursorRevision).
			Order("source_epoch,log_id,revision").Limit(16).Find(&rows).Error; err != nil {
			return false, err
		}
	}
	for _, row := range rows {
		if err := saveTokenRevisionMembers(tx, row); err != nil {
			return false, err
		}
		checkpoint.CursorEpoch, checkpoint.CursorID, checkpoint.CursorRevision = row.SourceEpoch, row.LogID, row.Revision
	}
	if !checkpoint.Ready {
		if err := tx.Table("token_revision_members_checkpoint_v1").Where("id=1").Updates(map[string]any{"cursor_epoch": checkpoint.CursorEpoch, "cursor_id": checkpoint.CursorID, "cursor_revision": checkpoint.CursorRevision, "ready": len(rows) < 16}).Error; err != nil {
			return false, err
		}
	}
	return len(rows) == 0, nil
}
