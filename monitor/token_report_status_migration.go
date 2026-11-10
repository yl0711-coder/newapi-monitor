package monitor

import "gorm.io/gorm"

// Only the committed identity prefix contributes to counters while rebuilding.
// Triggers use the same checkpoint, so concurrent inserts, moves and deletes
// neither double count an unvisited row nor lose changes behind the cursor.
func migrateTokenStatusCounts(tx *gorm.DB) (int, bool, error) {
	var state struct {
		Generation, CursorID int64
		CursorEpoch          string
		Ready                bool
	}
	if err := tx.Table(tokenReportStatusCheckpoint).Where("id=1").Take(&state).Error; err != nil {
		return 0, false, err
	}
	if state.Ready {
		return 0, true, nil
	}
	var rows []struct {
		SourceEpoch, Status string
		LogID               int64
		CreatedAt           *int64
	}
	if err := tx.Table(tokenReportIndexTable).Where("(source_epoch,log_id)>(?,?)", state.CursorEpoch, state.CursorID).Order("source_epoch,log_id").Limit(32).Scan(&rows).Error; err != nil {
		return 0, false, err
	}
	for _, row := range rows {
		if row.CreatedAt != nil {
			if err := tx.Exec(`INSERT INTO `+tokenReportStatusCounts+` VALUES(?,?,?,?,1) ON CONFLICT(generation,source_epoch,status,start_at) DO UPDATE SET requests=requests+1`, state.Generation, row.SourceEpoch, row.Status, *row.CreatedAt/300*300).Error; err != nil {
				return 0, false, err
			}
		}
		state.CursorEpoch, state.CursorID = row.SourceEpoch, row.LogID
	}
	entriesReady, err := tokenReportEntriesReady(tx)
	if err != nil {
		return 0, false, err
	}
	ready := len(rows) < 32 && entriesReady
	err = tx.Table(tokenReportStatusCheckpoint).Where("id=1").Updates(map[string]any{"cursor_epoch": state.CursorEpoch, "cursor_id": state.CursorID, "ready": ready}).Error
	return len(rows), ready, err
}

func tokenReportProjectionsReady(tx *gorm.DB) (bool, error) {
	for _, table := range []string{tokenPendingMigrationTable, "token_report_state_migration_v1", tokenReportStatusCheckpoint} {
		if !tx.Migrator().HasTable(table) {
			continue
		}
		var ready bool
		if err := tx.Table(table).Where("id=1").Select("ready").Scan(&ready).Error; err != nil {
			return false, err
		}
		if !ready {
			return false, nil
		}
	}
	return true, nil
}
