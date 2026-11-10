package monitor

import "gorm.io/gorm"

func initTokenReportStateMigration(tx *gorm.DB, required bool) error {
	if !required && !tx.Migrator().HasTable("token_report_state_migration_v1") {
		if err := tx.Raw("SELECT EXISTS(SELECT 1 FROM token_detection_currents LIMIT 1)").Scan(&required).Error; err != nil {
			return err
		}
	}
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS token_report_state_migration_v1(id INTEGER PRIMARY KEY,cursor_epoch TEXT NOT NULL,cursor_id INTEGER NOT NULL,ready INTEGER NOT NULL)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`INSERT OR IGNORE INTO token_report_state_migration_v1 VALUES(1,'',0,1)`).Error; err != nil {
		return err
	}
	if required {
		return tx.Exec(`UPDATE token_report_state_migration_v1 SET cursor_epoch='',cursor_id=0,ready=0 WHERE id=1`).Error
	}
	return nil
}

func migrateTokenReportStates(tx *gorm.DB) (bool, error) {
	var state struct {
		CursorEpoch string
		CursorID    int64
		Ready       bool
	}
	if err := tx.Table("token_report_state_migration_v1").Where("id=1").Take(&state).Error; err != nil {
		return false, err
	}
	if state.Ready {
		return true, nil
	}
	var heads []TokenDetectionCurrent
	if err := tx.Select("source_epoch,log_id,detection_state").Where("(source_epoch,log_id)>(?,?)", state.CursorEpoch, state.CursorID).
		Order("source_epoch,log_id").Limit(32).Find(&heads).Error; err != nil {
		return false, err
	}
	for _, h := range heads {
		if err := tx.Exec(`DELETE FROM `+tokenReportSummaryTable+` WHERE source_epoch=? AND log_id=?`, h.SourceEpoch, h.LogID).Error; err != nil {
			return false, err
		}
		if err := tx.Exec(`INSERT INTO `+tokenReportSummaryTable+` (`+tokenReportSummaryFields+`) SELECT `+tokenReportSummaryProjection("c")+` FROM token_detection_currents c WHERE c.source_epoch=? AND c.log_id=?`, h.SourceEpoch, h.LogID).Error; err != nil {
			return false, err
		}
		if err := tx.Exec(`DELETE FROM `+tokenReportIndexTable+` WHERE source_epoch=? AND log_id=?`, h.SourceEpoch, h.LogID).Error; err != nil {
			return false, err
		}
		if err := tx.Exec(`INSERT INTO `+tokenReportIndexTable+` SELECT `+tokenReportEntryProjection("c")+` FROM token_detection_currents c WHERE c.source_epoch=? AND c.log_id=?`, h.SourceEpoch, h.LogID).Error; err != nil {
			return false, err
		}
		state.CursorEpoch, state.CursorID = h.SourceEpoch, h.LogID
	}
	return len(heads) < 32, tx.Table("token_report_state_migration_v1").Where("id=1").Updates(map[string]any{"cursor_epoch": state.CursorEpoch, "cursor_id": state.CursorID, "ready": len(heads) < 32}).Error
}
