package monitor

import "gorm.io/gorm"

// A narrow reverse index protects facts used by frozen references. Keeping it
// transactional avoids reparsing every retained reference in the cleanup writer.
func initTokenReferenceMembers(tx *gorm.DB) error {
	validityExisted := tx.Migrator().HasTable("token_reference_members_invalid_v1")
	for _, sql := range []string{
		`CREATE TABLE IF NOT EXISTS token_reference_members_v1 (reference_id TEXT NOT NULL,source_epoch TEXT NOT NULL,log_id INTEGER NOT NULL,PRIMARY KEY(reference_id,source_epoch,log_id))`,
		`CREATE INDEX IF NOT EXISTS idx_token_reference_member_identity_v1 ON token_reference_members_v1(source_epoch,log_id,reference_id)`,
		`CREATE TABLE IF NOT EXISTS token_reference_members_checkpoint_v1 (id INTEGER PRIMARY KEY,cursor TEXT NOT NULL,ready INTEGER NOT NULL)`,
		`CREATE TABLE IF NOT EXISTS token_reference_members_invalid_v1(reference_id TEXT PRIMARY KEY)`,
		`INSERT OR IGNORE INTO token_reference_members_checkpoint_v1 VALUES(1,'',NOT EXISTS(SELECT 1 FROM token_detection_references LIMIT 1))`,
		`CREATE TRIGGER IF NOT EXISTS token_reference_members_insert_v1 AFTER INSERT ON token_detection_references BEGIN
		 INSERT OR IGNORE INTO token_reference_members_v1 SELECT NEW.id,json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch'),json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id')
		 FROM json_each(CASE WHEN json_valid(NEW.snapshot_json) THEN NEW.snapshot_json ELSE '{}' END,'$.samples') j
		 WHERE json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch') IS NOT NULL AND json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id') IS NOT NULL; END`,
		`CREATE TRIGGER IF NOT EXISTS token_reference_members_update_v1 AFTER UPDATE OF snapshot_json ON token_detection_references WHEN NEW.snapshot_json IS NOT OLD.snapshot_json BEGIN
		 DELETE FROM token_reference_members_v1 WHERE reference_id=OLD.id;
		 INSERT OR IGNORE INTO token_reference_members_v1 SELECT NEW.id,json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch'),json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id')
		 FROM json_each(CASE WHEN json_valid(NEW.snapshot_json) THEN NEW.snapshot_json ELSE '{}' END,'$.samples') j
		 WHERE json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch') IS NOT NULL AND json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id') IS NOT NULL; END`,
		`CREATE TRIGGER IF NOT EXISTS token_reference_members_delete_v1 AFTER DELETE ON token_detection_references BEGIN DELETE FROM token_reference_members_v1 WHERE reference_id=OLD.id; END`,
		`CREATE TRIGGER IF NOT EXISTS token_reference_members_invalid_insert_v1 AFTER INSERT ON token_detection_references BEGIN
		 INSERT OR IGNORE INTO token_reference_members_invalid_v1 SELECT NEW.id WHERE NOT json_valid(NEW.snapshot_json)
		 OR COALESCE(json_type(CASE WHEN json_valid(NEW.snapshot_json) THEN NEW.snapshot_json ELSE '{}' END,'$.samples'),'missing') NOT IN ('array','null')
		 OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(NEW.snapshot_json) THEN NEW.snapshot_json ELSE '{}' END,'$.samples') j
		 WHERE j.type<>'object' OR COALESCE(json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch'),'')='' OR COALESCE(json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id'),0)<=0); END`,
		`CREATE TRIGGER IF NOT EXISTS token_reference_members_invalid_update_v1 AFTER UPDATE OF snapshot_json ON token_detection_references BEGIN
		 DELETE FROM token_reference_members_invalid_v1 WHERE reference_id=NEW.id;
		 INSERT OR IGNORE INTO token_reference_members_invalid_v1 SELECT NEW.id WHERE NOT json_valid(NEW.snapshot_json)
		 OR COALESCE(json_type(CASE WHEN json_valid(NEW.snapshot_json) THEN NEW.snapshot_json ELSE '{}' END,'$.samples'),'missing') NOT IN ('array','null')
		 OR EXISTS (SELECT 1 FROM json_each(CASE WHEN json_valid(NEW.snapshot_json) THEN NEW.snapshot_json ELSE '{}' END,'$.samples') j
		 WHERE j.type<>'object' OR COALESCE(json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch'),'')='' OR COALESCE(json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id'),0)<=0); END`,
		`CREATE TRIGGER IF NOT EXISTS token_reference_members_invalid_delete_v1 AFTER DELETE ON token_detection_references BEGIN DELETE FROM token_reference_members_invalid_v1 WHERE reference_id=OLD.id; END`,
	} {
		if err := tx.Exec(sql).Error; err != nil {
			return err
		}
	}
	if !validityExisted {
		return tx.Exec(`UPDATE token_reference_members_checkpoint_v1 SET ready=NOT EXISTS(SELECT 1 FROM token_detection_references LIMIT 1),cursor='' WHERE id=1`).Error
	}
	return nil
}

func migrateTokenReferenceMembers(tx *gorm.DB) (bool, error) {
	var checkpoint struct {
		Cursor string
		Ready  bool
	}
	if err := tx.Table("token_reference_members_checkpoint_v1").Where("id=1").Take(&checkpoint).Error; err != nil {
		return false, err
	}
	if checkpoint.Ready {
		return true, nil
	}
	var ids []string
	if err := tx.Model(&TokenDetectionReference{}).Where("id>?", checkpoint.Cursor).Order("id").Limit(32).Pluck("id", &ids).Error; err != nil {
		return false, err
	}
	for _, id := range ids {
		// Fire both dependency and validity projections for this bounded prefix.
		if err := tx.Exec(`UPDATE token_detection_references SET snapshot_json=snapshot_json WHERE id=?`, id).Error; err != nil {
			return false, err
		}
		if err := tx.Exec(`INSERT OR IGNORE INTO token_reference_members_v1
		 SELECT r.id,json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch'),json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id') FROM token_detection_references r,
		 json_each(CASE WHEN json_valid(r.snapshot_json) THEN r.snapshot_json ELSE '{}' END,'$.samples') j
		 WHERE r.id=? AND json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.epoch') IS NOT NULL AND json_extract(CASE WHEN j.type='object' THEN j.value ELSE '{}' END,'$.log_id') IS NOT NULL`, id).Error; err != nil {
			return false, err
		}
		checkpoint.Cursor = id
	}
	ready := len(ids) < 32
	return ready, tx.Table("token_reference_members_checkpoint_v1").Where("id=1").Updates(map[string]any{"cursor": checkpoint.Cursor, "ready": ready}).Error
}
