package monitor

import (
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const tokenReportIndexTable = "token_detection_report_entries_v1"

func tokenReportEntryProjection(alias string) string {
	return fmt.Sprintf(`%[1]s.source_epoch,%[1]s.log_id,%[1]s.created_at,%[1]s.user_id,%[1]s.channel_id,%[1]s.model,%[1]s.user_group,
	 CASE WHEN COALESCE(%[1]s.detection_state,'')='' THEN 'pending'
	 WHEN %[1]s.candidate=1 THEN 'candidate' WHEN %[1]s.candidate=0 THEN
	 CASE WHEN %[1]s.source_complete=0 THEN 'insufficient'
	 WHEN NOT json_valid(%[1]s.result_json) THEN 'invalid'
	 WHEN EXISTS (SELECT 1 FROM json_each(%[1]s.result_json,'$.rules') WHERE json_extract(value,'$.eligible')=0) THEN 'insufficient'
	 WHEN %[1]s.source_complete=1 THEN 'not_hit' ELSE 'unknown' END ELSE 'unknown' END`, alias)
}

func tokenReportHasIndexTable(db *gorm.DB) bool {
	// Building an EXPLAIN statement may use a DryRun session. The schema probe
	// still needs real read-only metadata; keep that setting off the caller.
	probe := db.Session(&gorm.Session{NewDB: true})
	probe.Config.DryRun = false
	return probe.Migrator().HasTable(tokenReportIndexTable)
}

// The old status predicate parsed every result JSON for every summary count,
// even when the requested page contained only 25 rows. This compact derived
// index preserves that predicate and its identity filters. Triggers maintain
// it in the head's transaction, including raw writes, rollback and cleanup.
func initTokenDetectionReportIndex(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := initTokenDetectionStateIndexes(tx); err != nil {
			return err
		}
		if err := initTokenDetectionCleanup(tx); err != nil {
			return err
		}
		if err := initTokenDetectionClaims(tx); err != nil {
			return err
		}
		if err := initTokenDetectionReferenceIndex(tx); err != nil {
			return err
		}
		if err := initTokenReferenceMembers(tx); err != nil {
			return err
		}
		if err := initTokenRevisionMembers(tx); err != nil {
			return err
		}
		existed := tx.Migrator().HasTable(tokenReportIndexTable)
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportIndexTable + ` (
			source_epoch TEXT NOT NULL, log_id INTEGER NOT NULL, created_at INTEGER,
			user_id INTEGER, channel_id INTEGER, model TEXT, user_group TEXT, status TEXT NOT NULL,
			PRIMARY KEY(source_epoch,log_id))`).Error; err != nil {
			return err
		}
		if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_entry_scope_v1 ON ` + tokenReportIndexTable +
			` (source_epoch,status,created_at,log_id,user_id,channel_id,model,user_group)`).Error; err != nil {
			return err
		}
		// Whole-range coverage counts do not need the potentially wide model
		// and group strings from the identity-filter index. Keep that common
		// scan compact as detection fills the retained fact population.
		if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_entry_count_v1 ON ` + tokenReportIndexTable +
			` (source_epoch,status,created_at)`).Error; err != nil {
			return err
		}
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			fields := []string{"source_epoch", "log_id", "created_at", "user_id", "channel_id", "model", "user_group", "candidate", "source_complete", "result_json"}
			if event == "UPDATE" {
				fields = append(fields, "detection_state")
			}
			updateOf, when := "", ""
			if event == "UPDATE" {
				updateOf = " OF " + strings.Join(fields, ",")
				changed := make([]string, len(fields))
				for i, field := range fields {
					changed[i] = "OLD." + field + " IS NOT NEW." + field
				}
				when = " WHEN " + strings.Join(changed, " OR ")
			}
			removeOld := ""
			if event == "UPDATE" {
				removeOld = `DELETE FROM ` + tokenReportIndexTable + ` WHERE source_epoch=OLD.source_epoch AND log_id=OLD.log_id
					AND (OLD.source_epoch IS NOT NEW.source_epoch OR OLD.log_id IS NOT NEW.log_id);`
			}
			for _, version := range []string{"v1", "v2"} {
				if err := tx.Exec("DROP TRIGGER IF EXISTS token_report_entry_" + event + "_" + version).Error; err != nil {
					return err
				}
			}
			if event == "DELETE" {
				if err := tx.Exec(`CREATE TRIGGER token_report_entry_DELETE_v2 AFTER DELETE ON token_detection_currents BEGIN
					DELETE FROM ` + tokenReportIndexTable + ` WHERE source_epoch=OLD.source_epoch AND log_id=OLD.log_id; END`).Error; err != nil {
					return err
				}
				continue
			}
			values, differences := []string{}, []string{}
			for _, field := range []string{"created_at", "user_id", "channel_id", "model", "user_group", "status"} {
				values = append(values, field+"=excluded."+field)
				differences = append(differences, field+" IS NOT excluded."+field)
			}
			// Rechecking a frozen result often changes only scheduling or evidence.
			// Preserve the compact entry and status counters when their projection
			// is identical, instead of a DELETE/INSERT for every head Save.
			statement := `CREATE TRIGGER token_report_entry_` + event + `_v2 AFTER ` + event + updateOf + ` ON token_detection_currents` + when + ` BEGIN ` + removeOld +
				` INSERT INTO ` + tokenReportIndexTable + ` SELECT ` + tokenReportEntryProjection("NEW") + ` WHERE 1 ON CONFLICT(source_epoch,log_id) DO UPDATE SET ` +
				strings.Join(values, ",") + ` WHERE ` + strings.Join(differences, " OR ") + `; END`
			if err := tx.Exec(statement).Error; err != nil {
				return err
			}
		}
		if err := initTokenReportStatusCounts(tx, !existed); err != nil {
			return err
		}
		if err := initTokenReportSummaryStore(tx); err != nil {
			return err
		}
		if err := initTokenReportStateMigration(tx, !existed); err != nil {
			return err
		}
		if _, err := migrateTokenReportStates(tx); err != nil {
			return err
		}
		if _, _, err := migrateTokenStatusCounts(tx); err != nil {
			return err
		}
		return initTokenReportPendingIndex(tx)
	})
}

func tokenReportIdentityRange(db *gorm.DB, q tokenDetectionQuery) *gorm.DB {
	db = db.Where(tokenReportSummaryColumn(db, "source_epoch")+" = ? AND "+tokenReportSummaryColumn(db, "created_at")+" >= ? AND "+tokenReportSummaryColumn(db, "created_at")+" < ?", q.SourceEpoch, q.From, q.To)
	if q.UserID > 0 || q.UserSet {
		db = db.Where(tokenReportSummaryColumn(db, "user_id")+" = ?", q.UserID)
	}
	if q.ChannelID > 0 || q.ChannelSet {
		db = db.Where(tokenReportSummaryColumn(db, "channel_id")+" = ?", q.ChannelID)
	}
	if q.Model != "" || q.ModelSet {
		db = db.Where(tokenReportSummaryColumn(db, "model")+" = ?", q.Model)
	}
	if q.Group != "" || q.GroupSet {
		db = db.Where(tokenReportSummaryColumn(db, "user_group")+" = ?", q.Group)
	}
	return db
}

func tokenReportInsufficientCount(db *gorm.DB, q tokenDetectionQuery) (int64, error) {
	var count int64
	if !tokenReportHasIndexTable(db) {
		err := tokenDetectionRange(db, q).Count(&count).Error
		return count, err
	}
	// Invalid legacy JSON must remain an explicit query failure, rather than
	// silently disappearing from the evidence coverage count.
	var invalid int64
	invalid, err := tokenReportStatusCount(db, q, "invalid")
	if err != nil {
		return 0, err
	}
	if invalid > 0 {
		return 0, errors.New("local_result_invalid")
	}
	return tokenReportStatusCount(db, q, "insufficient")
}

// Prove an empty outcome selection from the existing covering classification
// index. Otherwise two head scans can traverse all retained rows merely to
// return an empty page. This runs in the report's read transaction, and does not
// suppress the separate whole-scope coverage/pending/invalid-result checks.
func tokenReportStatusEmpty(db *gorm.DB, q tokenDetectionQuery) (bool, error) {
	if q.Status != "candidate" && q.Status != "insufficient" && q.Status != "not_hit" || !tokenReportUseEntryIndex(db) {
		return false, nil
	}
	var matches []int64
	err := tokenReportIdentityRange(db.Table(tokenReportIndexTable), q).Where("status=?", q.Status).Select("log_id").Limit(1).Scan(&matches).Error
	return err == nil && len(matches) == 0, err
}
