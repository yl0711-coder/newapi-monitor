package monitor

import (
	"strings"

	"gorm.io/gorm"
)

const tokenPendingMigrationTable = "token_report_pending_migration_v1"

// Reinstalling triggers does not reset an in-flight migration after a restart.
func initTokenPendingMigration(tx *gorm.DB, required bool) error {
	// The first versioned checkpoint also upgrades an intermediate projection
	// whose columns already exist but whose trigger did not classify empty heads.
	required = required || !tx.Migrator().HasTable(tokenPendingMigrationTable)
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenPendingMigrationTable + `(id INTEGER PRIMARY KEY,
	 cursor_epoch TEXT NOT NULL DEFAULT '',cursor_id INTEGER NOT NULL DEFAULT 0,ready INTEGER NOT NULL DEFAULT 1)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`INSERT OR IGNORE INTO ` + tokenPendingMigrationTable + `(id) VALUES(1)`).Error; err != nil {
		return err
	}
	if required {
		if err := tx.Exec(`UPDATE ` + tokenPendingMigrationTable + ` SET cursor_epoch='',cursor_id=0,ready=0 WHERE id=1`).Error; err != nil {
			return err
		}
	}
	_, _, err := migrateTokenPendingBatch(tx)
	return err
}

func tokenPendingProjectionReady(tx *gorm.DB) (bool, error) {
	if !tx.Migrator().HasTable(tokenPendingMigrationTable) {
		return true, nil
	}
	var ready bool
	err := tx.Table(tokenPendingMigrationTable).Select("ready").Where("id=1").Scan(&ready).Error
	return ready, err
}

func migrateTokenPendingBatch(tx *gorm.DB) (int, bool, error) {
	var checkpoint struct {
		CursorEpoch string
		CursorID    int64
		Ready       bool
	}
	if err := tx.Table(tokenPendingMigrationTable).Where("id=1").Take(&checkpoint).Error; err != nil {
		return 0, false, err
	}
	if checkpoint.Ready {
		return 0, true, nil
	}
	var ids []struct {
		SourceEpoch string
		LogID       int64
	}
	if err := tx.Model(&TokenMeteringFact{}).Select("source_epoch,log_id").
		Where("(source_epoch,log_id)>(?,?)", checkpoint.CursorEpoch, checkpoint.CursorID).
		Order("source_epoch,log_id").Limit(32).Scan(&ids).Error; err != nil {
		return 0, false, err
	}
	projection := `INSERT INTO ` + tokenReportPendingMembers + `
	 (source_epoch,log_id,created_at,user_id,channel_id,model,user_group,start_at,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,fact_revision,link_revision,retry_present,head_present,next_at,group_key)
	 ` + tokenReportPendingProjection(tx)
	for _, id := range ids {
		if err := tx.Exec(`DELETE FROM `+tokenReportPendingMembers+` WHERE source_epoch=? AND log_id=?`, id.SourceEpoch, id.LogID).Error; err != nil {
			return 0, false, err
		}
		if err := tx.Exec(projection+` WHERE f.source_epoch=? AND f.log_id=?) v`, id.SourceEpoch, id.LogID).Error; err != nil {
			return 0, false, err
		}
		checkpoint.CursorEpoch, checkpoint.CursorID = id.SourceEpoch, id.LogID
	}
	ready := len(ids) < 32
	err := tx.Table(tokenPendingMigrationTable).Where("id=1").Updates(map[string]any{
		"cursor_epoch": checkpoint.CursorEpoch, "cursor_id": checkpoint.CursorID, "ready": ready,
	}).Error
	return len(ids), ready, err
}

func tokenReportPendingProjection(tx *gorm.DB) string {
	groupFields := strings.Split("source_epoch,start_at,user_id,channel_id,model,user_group,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,retry_present,head_present", ",")
	for i := range groupFields {
		groupFields[i] = "v." + groupFields[i]
	}
	return `SELECT v.*,json_array(` + strings.Join(groupFields, ",") + `) AS group_key FROM (SELECT
	 f.source_epoch,f.log_id,f.created_at,f.user_id,f.channel_id,f.model,f.user_group,(f.created_at/300)*300 AS start_at,
	 COALESCE(c.log_id IS NULL OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR COALESCE(c.detection_state,'')='',0) AS base_pending,
	 c.policy_hash,c.reference_bucket,c.reference_generation,c.source_complete,f.revision AS fact_revision,f.link_revision,
	 (r.log_id IS NOT NULL) AS retry_present,(c.log_id IS NOT NULL) AS head_present,c.next_at
	 FROM token_metering_facts f ` + tokenDetectionHeadJoin(tx) + ` LEFT JOIN token_detection_retries r ON r.source_epoch=f.source_epoch AND r.log_id=f.log_id`
}
