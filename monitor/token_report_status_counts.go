package monitor

import (
	"errors"
	"strings"

	"gorm.io/gorm"
)

const tokenReportStatusCounts = "token_report_status_counts_v2"
const tokenReportStatusCheckpoint = "token_report_status_checkpoint_v2"
const tokenReportCountLimit = 20000

var errTokenReportCountNotReady = errors.New("status_count_budget_exhausted")

// A complete counter requires BOTH migrations. Live triggers maintain new
// entries immediately, but cannot prove that older heads have been projected.
func tokenReportEntriesReady(db *gorm.DB) (bool, error) {
	const table = "token_report_state_migration_v1"
	probe := db.Session(&gorm.Session{NewDB: true})
	probe.Config.DryRun = false
	if !probe.Migrator().HasTable(table) {
		return true, nil
	}
	var ready bool
	err := probe.Table(table).Select("ready").Where("id=1").Scan(&ready).Error
	return ready, err
}

func tokenReportUseEntryIndex(db *gorm.DB) bool {
	if !tokenReportHasIndexTable(db) {
		return false
	}
	ready, err := tokenReportEntriesReady(db)
	if err != nil {
		db.AddError(err)
	}
	return err == nil && ready
}

func tokenReportBoundedStatusCount(db *gorm.DB, q tokenDetectionQuery, status string, entriesReady bool) (int64, error) {
	var count, examined int64
	if entriesReady {
		rows := tokenReportIdentityRange(db.Table(tokenReportIndexTable), q).Where("status=?", status).Select("1").Limit(tokenReportCountLimit + 1)
		if err := db.Raw("SELECT COUNT(*) FROM (?)", rows).Scan(&count).Error; err != nil {
			return 0, err
		}
		examined = count
	} else {
		// The authoritative head time index bounds this fallback even while the
		// entry projection is incomplete. Classify the same snapshot as the list.
		rows := tokenReportIdentityRange(db.Model(&TokenDetectionCurrent{}), q).
			Select(tokenReportEntryProjection("token_detection_currents") + " AS status").Limit(tokenReportCountLimit + 1)
		var result struct{ Count, Examined int64 }
		if err := db.Raw("SELECT COALESCE(SUM(CASE WHEN status=? THEN 1 ELSE 0 END),0) AS count,COUNT(*) AS examined FROM (?)", status, rows).Scan(&result).Error; err != nil {
			return 0, err
		}
		count, examined = result.Count, result.Examined
	}
	if examined > tokenReportCountLimit {
		return 0, errTokenReportCountNotReady
	}
	return count, nil
}

// A whole-range coverage count must not walk every retained result on every
// refresh. Keep only five-minute status totals here. Identity filters and both
// partial boundary buckets still use the authoritative compact entry index.
// Entry changes, counters and the originating head share one transaction.
func initTokenReportStatusCounts(tx *gorm.DB, entriesRebuilt bool) error {
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportStatusCounts + ` (
		generation INTEGER NOT NULL,source_epoch TEXT NOT NULL,status TEXT NOT NULL,start_at INTEGER NOT NULL,requests INTEGER NOT NULL,
		PRIMARY KEY(generation,source_epoch,status,start_at)) WITHOUT ROWID`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportStatusCheckpoint + `(id INTEGER PRIMARY KEY,generation INTEGER NOT NULL DEFAULT 1,cursor_epoch TEXT NOT NULL DEFAULT '',cursor_id INTEGER NOT NULL DEFAULT 0,ready INTEGER NOT NULL DEFAULT 0)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`INSERT OR IGNORE INTO ` + tokenReportStatusCheckpoint + `(id) VALUES(1)`).Error; err != nil {
		return err
	}
	if entriesRebuilt {
		// A new generation avoids clearing a large derived table under the
		// startup writer. Retained generations are excluded from active counts.
		if err := tx.Exec(`UPDATE ` + tokenReportStatusCheckpoint + ` SET generation=generation+1,cursor_epoch='',cursor_id=0,ready=0 WHERE id=1`).Error; err != nil {
			return err
		}
	}
	active := `(SELECT generation FROM ` + tokenReportStatusCheckpoint + ` WHERE id=1)`
	counted := func(alias string) string {
		return `EXISTS(SELECT 1 FROM ` + tokenReportStatusCheckpoint + ` p WHERE p.id=1 AND (p.ready=1 OR (` + alias + `.source_epoch,` + alias + `.log_id)<=(p.cursor_epoch,p.cursor_id)))`
	}
	remove := `UPDATE ` + tokenReportStatusCounts + ` SET requests=requests-1
		WHERE generation=` + active + ` AND source_epoch=OLD.source_epoch AND status=OLD.status AND start_at=(OLD.created_at/300)*300 AND ` + counted("OLD") + `;
		DELETE FROM ` + tokenReportStatusCounts + ` WHERE generation=` + active + ` AND source_epoch=OLD.source_epoch AND status=OLD.status AND start_at=(OLD.created_at/300)*300 AND requests=0;`
	add := `INSERT INTO ` + tokenReportStatusCounts + ` SELECT ` + active + `,NEW.source_epoch,NEW.status,(NEW.created_at/300)*300,1 WHERE NEW.created_at IS NOT NULL AND ` + counted("NEW") + `
		ON CONFLICT(generation,source_epoch,status,start_at) DO UPDATE SET requests=requests+1;`
	for _, event := range []string{"insert", "delete", "update"} {
		if err := tx.Exec(`DROP TRIGGER IF EXISTS token_report_status_count_` + event + `_v1`).Error; err != nil {
			return err
		}
	}
	for _, statement := range []string{
		`CREATE TRIGGER IF NOT EXISTS token_report_status_count_insert_v2 AFTER INSERT ON ` + tokenReportIndexTable + ` BEGIN ` + add + ` END`,
		`CREATE TRIGGER IF NOT EXISTS token_report_status_count_delete_v2 AFTER DELETE ON ` + tokenReportIndexTable + ` BEGIN ` + remove + ` END`,
		`CREATE TRIGGER IF NOT EXISTS token_report_status_count_update_v2 AFTER UPDATE OF source_epoch,log_id,status,created_at ON ` + tokenReportIndexTable + `
		WHEN NEW.source_epoch IS NOT OLD.source_epoch OR NEW.log_id IS NOT OLD.log_id OR NEW.status IS NOT OLD.status OR NEW.created_at IS NOT OLD.created_at BEGIN ` + remove + add + ` END`,
	} {
		if err := tx.Exec(statement).Error; err != nil {
			return err
		}
	}
	return nil
}

func tokenReportStatusCount(db *gorm.DB, q tokenDetectionQuery, status string) (int64, error) {
	var count int64
	entriesReady, err := tokenReportEntriesReady(db)
	if err != nil {
		return 0, err
	}
	var countersReady bool
	if entriesReady && db.Migrator().HasTable(tokenReportStatusCheckpoint) {
		if err := db.Table(tokenReportStatusCheckpoint).Where("id=1").Select("ready").Scan(&countersReady).Error; err != nil {
			return 0, err
		}
	}
	innerFrom, innerTo := (q.From+299)/300*300, q.To/300*300
	if q.UserSet || q.UserID > 0 || q.ChannelSet || q.ChannelID > 0 || q.ModelSet || q.Model != "" || q.GroupSet || q.Group != "" ||
		q.From < 0 || innerFrom >= innerTo || !countersReady || !db.Migrator().HasTable(tokenReportStatusCounts) {
		return tokenReportBoundedStatusCount(db, q, status, entriesReady)
	}
	parts := []string{`(SELECT COALESCE(SUM(requests),0) FROM ` + tokenReportStatusCounts + ` WHERE generation=(SELECT generation FROM ` + tokenReportStatusCheckpoint + ` WHERE id=1) AND source_epoch=? AND status=? AND start_at>=? AND start_at<?)`}
	args := []any{q.SourceEpoch, status, innerFrom, innerTo}
	for _, edge := range [][2]int64{{q.From, innerFrom}, {innerTo, q.To}} {
		if edge[0] >= edge[1] {
			continue
		}
		parts = append(parts, `(SELECT COUNT(*) FROM `+tokenReportIndexTable+` WHERE source_epoch=? AND status=? AND created_at>=? AND created_at<?)`)
		args = append(args, q.SourceEpoch, status, edge[0], edge[1])
	}
	err = db.Raw("SELECT "+strings.Join(parts, " + "), args...).Scan(&count).Error
	return count, err
}
