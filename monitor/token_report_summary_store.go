package monitor

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const tokenReportSummaryTable = "token_detection_report_summaries_v1"
const tokenReportSummaryStatusJoinSetting = "token_report_summary_status_join"
const tokenReportSummaryFields = "source_epoch,created_at,log_id,user_id,channel_id,model,user_group,candidate,source_complete,result_json,detection_state"

func tokenReportSummaryProjection(alias string) string {
	paths := []string{"reference", "reference_price", "version", "quarantined", "cache_signal", "gaps", "price.supported", "price.method", "price.raw_quota", "price.quota", "price.tier", "price.input_scale"}
	for i := 0; i < 16; i++ {
		paths = append(paths, fmt.Sprintf("rules[%d].reason", i), fmt.Sprintf("rules[%d].fault_class", i))
	}
	for i, p := range paths {
		paths[i] = "'$." + p + "'"
	}
	return alias + `.source_epoch,COALESCE(` + alias + `.created_at,0),` + alias + `.log_id,` +
		alias + `.user_id,` + alias + `.channel_id,` + alias + `.model,` + alias + `.user_group,` +
		alias + `.candidate,` + alias + `.source_complete,CASE WHEN json_valid(` + alias + `.result_json) THEN json_remove(` + alias + `.result_json,` + strings.Join(paths, ",") + `) ELSE ` + alias + `.result_json END,COALESCE(` + alias + `.detection_state,'')`
}

// The list summary reads up to 5,001 results on every page. Fetching their
// scattered full heads dominates the read budget while workers write. Keep the
// fields consumed by summary/analysis/filters together in event-time order.
// This is a transactionally maintained projection, never a timed response cache.
// Full current results and frozen detail evidence remain authoritative.
func initTokenReportSummaryStore(tx *gorm.DB) error {
	existed := tx.Migrator().HasTable(tokenReportSummaryTable)
	stateUpgrade := existed && !tx.Migrator().HasColumn(tokenReportSummaryTable, "detection_state")
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportSummaryTable + ` (
		source_epoch TEXT NOT NULL,created_at INTEGER NOT NULL,log_id INTEGER NOT NULL,
		user_id INTEGER,channel_id INTEGER,model TEXT,user_group TEXT,
		candidate INTEGER,source_complete INTEGER,result_json TEXT,
		detection_state TEXT NOT NULL DEFAULT '',
		PRIMARY KEY(source_epoch,created_at,log_id)) WITHOUT ROWID`).Error; err != nil {
		return err
	}
	if stateUpgrade {
		if err := tx.Exec(`ALTER TABLE ` + tokenReportSummaryTable + ` ADD COLUMN detection_state TEXT NOT NULL DEFAULT ''`).Error; err != nil {
			return err
		}
	}
	if err := initTokenReportStateMigration(tx, !existed || stateUpgrade); err != nil {
		return err
	}
	if err := tx.Exec(`CREATE UNIQUE INDEX IF NOT EXISTS idx_token_report_summary_identity_v1 ON ` + tokenReportSummaryTable + ` (source_epoch,log_id)`).Error; err != nil {
		return err
	}
	var candidateIndexSQL string
	if err := tx.Raw(`SELECT COALESCE(sql,'') FROM sqlite_master WHERE type='index' AND name='idx_token_report_summary_candidate_v1'`).Scan(&candidateIndexSQL).Error; err != nil {
		return err
	}
	if candidateIndexSQL != "" && !strings.Contains(strings.ToLower(candidateIndexSQL), "detection_state") {
		if err := tx.Exec(`DROP INDEX idx_token_report_summary_candidate_v1`).Error; err != nil {
			return err
		}
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_summary_candidate_v1 ON ` + tokenReportSummaryTable + ` (source_epoch,candidate,detection_state,created_at,log_id)`).Error; err != nil {
		return err
	}
	// Remove only fields neither summary nor any supported filter consumes.
	// json_remove preserves JSON types, absent/null operands, monetary strings
	// and malformed legacy shapes. Invalid JSON stays invalid and still fails
	// the query; the projection must never turn corrupt evidence into all-clear.
	fields := []string{"source_epoch", "log_id", "created_at", "user_id", "channel_id", "model", "user_group", "candidate", "source_complete", "result_json", "detection_state"}
	changed := make([]string, len(fields))
	for i, field := range fields {
		changed[i] = "NEW." + field + " IS NOT OLD." + field
	}
	for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
		update, when, body := "", "", ""
		if event != "INSERT" {
			body = `DELETE FROM ` + tokenReportSummaryTable + ` WHERE source_epoch=OLD.source_epoch AND created_at=COALESCE(OLD.created_at,0) AND log_id=OLD.log_id`
			if event == "UPDATE" {
				body += ` AND (OLD.source_epoch IS NOT NEW.source_epoch OR COALESCE(OLD.created_at,0) IS NOT COALESCE(NEW.created_at,0) OR OLD.log_id IS NOT NEW.log_id)`
			}
			body += `;`
		}
		if event == "UPDATE" {
			update = " OF " + strings.Join(fields, ",")
			when = " WHEN " + strings.Join(changed, " OR ")
		}
		if event != "DELETE" {
			// A staged recheck can change frozen evidence without changing any
			// summary/filter operand. Avoid rewriting that unchanged projection.
			values, differences := []string{}, []string{}
			for _, field := range fields[3:] {
				values = append(values, field+"=excluded."+field)
				differences = append(differences, field+" IS NOT excluded."+field)
			}
			body += ` INSERT INTO ` + tokenReportSummaryTable + ` (` + tokenReportSummaryFields + `) SELECT ` + tokenReportSummaryProjection("NEW") + ` WHERE 1
			ON CONFLICT(source_epoch,created_at,log_id) DO UPDATE SET ` + strings.Join(values, ",") + ` WHERE ` + strings.Join(differences, " OR ") + `;`
		}
		for _, version := range []string{"v1", "v2"} {
			if err := tx.Exec(`DROP TRIGGER IF EXISTS token_report_summary_` + event + `_` + version).Error; err != nil {
				return err
			}
		}
		if err := tx.Exec(`CREATE TRIGGER token_report_summary_` + event + `_v2 AFTER ` + event + update + ` ON token_detection_currents` + when + ` BEGIN ` + body + ` END`).Error; err != nil {
			return err
		}
	}
	return initTokenReportSummaryVersions(tx)
}

func tokenReportSummaryRange(db *gorm.DB, q tokenDetectionQuery) *gorm.DB {
	ready, err := tokenReportEntriesReady(db)
	if err != nil {
		db.AddError(err)
	}
	if ready && db.Migrator().HasTable(tokenReportSummaryTable) {
		// Keep the existing filter predicates, including their head alias, exact.
		table := tokenReportSummaryTable + " AS token_detection_currents"
		// The summary projection is ordered by event time. SQLite therefore
		// prefers its primary key for the default ORDER BY, even when the
		// selected status is `candidate`. On the retained production store that
		// plan walks every summary row (and its JSON) before finding a few
		// candidates, which can exceed the report's bounded read deadline. The
		// candidate index has the same event-time order after the status key and
		// only visits the selected rows. Force it whenever the filter implies a
		// candidate-only result; the legacy table path remains unchanged.
		candidateOnly := q.Status == "candidate" || q.Method != ""
		if candidateOnly && db.Migrator().HasIndex(tokenReportSummaryTable, "idx_token_report_summary_candidate_v1") {
			table += " INDEXED BY idx_token_report_summary_candidate_v1"
		}
		db = db.Table(table)
		// The compact projection intentionally omits worker-only head fields. A
		// caller that scans into TokenDetectionCurrent without an explicit Select
		// must therefore receive only the columns this table actually stores;
		// otherwise a status join makes GORM expand the full head schema and
		// SQLite reports missing columns such as reference_bucket.
		db = db.Select("token_detection_currents.source_epoch,token_detection_currents.created_at,token_detection_currents.log_id,token_detection_currents.user_id,token_detection_currents.channel_id,token_detection_currents.model,token_detection_currents.user_group,token_detection_currents.candidate,token_detection_currents.source_complete,token_detection_currents.result_json")
		// Insufficient/not-hit summaries are classified in the compact report
		// entry index, not in the wide JSON projection. Drive those reads from
		// that ordered status index and look up only the selected summary rows;
		// otherwise an empty not-hit query can scan the entire retained summary
		// history before returning. The range/filter helpers qualify summary
		// columns while this join is active.
		if (q.Status == "insufficient" || q.Status == "not_hit") &&
			db.Migrator().HasIndex(tokenReportIndexTable, "idx_token_report_entry_scope_v1") {
			// Merge the identity columns with USING so ordinary callers can keep
			// their `created_at,log_id` order without SQLite ambiguity, while the
			// qualified entry columns remain available to the indexed read plan.
			entries := "(SELECT source_epoch,created_at,log_id FROM " + tokenReportIndexTable + " INDEXED BY idx_token_report_entry_scope_v1 WHERE source_epoch=? AND status=? AND created_at>=? AND created_at<?) AS token_report_entries USING(source_epoch,created_at,log_id)"
			db = db.Joins("JOIN "+entries, q.SourceEpoch, q.Status, q.From, q.To).
				Set(tokenReportSummaryStatusJoinSetting, true)
		}
	}
	return tokenDetectionRange(db, q)
}

func tokenReportSummaryStatusJoined(db *gorm.DB) bool {
	_, ok := db.Get(tokenReportSummaryStatusJoinSetting)
	return ok
}

func tokenReportSummaryColumn(db *gorm.DB, column string) string {
	if tokenReportSummaryStatusJoined(db) {
		return "token_detection_currents." + column
	}
	return column
}
