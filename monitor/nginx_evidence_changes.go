package monitor

import (
	"errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

var errNginxEvidenceEventConflict = errors.New("evidence event identity conflict")

// A replay may enrich the same event after the collector learned a missing
// field. Keep the original observed values; incompatible identities or two
// different valid lengths must fail the batch instead of overwriting evidence.
func saveNginxEvidenceRows(tx *gorm.DB, rows []NginxRequestEvidence) error {
	ids := make([]string, 0, len(rows))
	for _, row := range rows {
		ids = append(ids, row.EventID)
	}
	var existing []NginxRequestEvidence
	if err := tx.Where("event_id IN ?", ids).Find(&existing).Error; err != nil {
		return err
	}
	byID := make(map[string]NginxRequestEvidence, len(existing)+len(rows))
	for _, row := range existing {
		byID[row.EventID] = row
	}
	for i, row := range rows {
		if old, ok := byID[row.EventID]; ok {
			if old.Node != row.Node || old.EventMS != row.EventMS {
				return errNginxEvidenceEventConflict
			}
			for _, pair := range [][2]string{{old.Route, row.Route}, {old.Method, row.Method}, {old.OneAPIRequestID, row.OneAPIRequestID}} {
				if pair[0] != "" && pair[1] != "" && pair[0] != pair[1] {
					return errNginxEvidenceEventConflict
				}
			}
			if old.HMACKeyID == row.HMACKeyID && old.OneAPIIDHMAC != "" && row.OneAPIIDHMAC != "" && old.OneAPIIDHMAC != row.OneAPIIDHMAC {
				return errNginxEvidenceEventConflict
			}
			if old.RequestLengthState == "valid" && old.RequestLength != nil && row.RequestLengthState == "valid" && row.RequestLength != nil && *old.RequestLength != *row.RequestLength {
				return errNginxEvidenceEventConflict
			}
			if old.Route == "" {
				old.Route = row.Route
			}
			if old.Method == "" {
				old.Method = row.Method
			}
			if old.OneAPIIDHMAC == "" {
				old.OneAPIIDHMAC, old.HMACKeyID = row.OneAPIIDHMAC, row.HMACKeyID
			}
			// Accepted key rotation can replay an identical event. Preserve its
			// original HMAC pair while merging independent missing metadata.
			if old.OneAPIRequestID == "" {
				old.OneAPIRequestID = row.OneAPIRequestID
			}
			if row.RequestLengthState == "valid" && row.RequestLength != nil && (old.RequestLengthState != "valid" || old.RequestLength == nil) {
				old.RequestLength, old.RequestLengthState, old.RequestLengthVersion = row.RequestLength, row.RequestLengthState, row.RequestLengthVersion
			} else if old.RequestLengthVersion == "" && old.RequestLength != nil && row.RequestLength != nil && *old.RequestLength == *row.RequestLength {
				old.RequestLengthVersion = row.RequestLengthVersion
			}
			row = old
		}
		rows[i], byID[row.EventID] = row, row
	}
	return tx.Clauses(clause.OnConflict{Columns: []clause.Column{{Name: "event_id"}}, DoUpdates: clause.AssignmentColumns([]string{"route", "method", "hmac_key_id", "oneapi_id_hmac", "oneapi_request_id", "request_length", "request_length_state", "request_length_version"})}).CreateInBatches(rows, 200).Error
}

// Keep one compact change identity per retained event. Updating an event moves
// its sequence forward; it never appends copies of the evidence payload. The
// clock and identity commit with ingestion, so a restarted consumer can resume
// without relying on wall-clock timestamps or a process-local notification.
func initNginxEvidenceChanges(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		for _, stmt := range []string{
			`CREATE TABLE IF NOT EXISTS nginx_evidence_change_clock_v1(id INTEGER PRIMARY KEY,source_id TEXT NOT NULL,generation INTEGER NOT NULL)`,
			`INSERT OR IGNORE INTO nginx_evidence_change_clock_v1 VALUES(1,lower(hex(randomblob(16))),0)`,
			`CREATE TABLE IF NOT EXISTS nginx_evidence_changes_v1(event_id TEXT PRIMARY KEY,generation INTEGER NOT NULL)`,
			`CREATE INDEX IF NOT EXISTS idx_nginx_evidence_changes_v1 ON nginx_evidence_changes_v1(generation,event_id)`,
		} {
			if err := tx.Exec(stmt).Error; err != nil {
				return err
			}
		}
		body := `UPDATE nginx_evidence_change_clock_v1 SET generation=generation+1 WHERE id=1;
		 INSERT INTO nginx_evidence_changes_v1(event_id,generation)
		 SELECT NEW.event_id,generation FROM nginx_evidence_change_clock_v1 WHERE id=1
		 ON CONFLICT(event_id) DO UPDATE SET generation=excluded.generation;`
		if err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS nginx_evidence_change_insert_v1 AFTER INSERT ON nginx_request_evidences BEGIN ` + body + ` END`).Error; err != nil {
			return err
		}
		if err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS nginx_evidence_change_update_v1 AFTER UPDATE ON nginx_request_evidences
		 WHEN OLD.oneapi_request_id IS NOT NEW.oneapi_request_id OR OLD.hmac_key_id IS NOT NEW.hmac_key_id
		 OR OLD.oneapi_id_hmac IS NOT NEW.oneapi_id_hmac OR OLD.event_ms IS NOT NEW.event_ms
		 OR OLD.node IS NOT NEW.node OR OLD.route IS NOT NEW.route OR OLD.method IS NOT NEW.method OR OLD.status IS NOT NEW.status
		 OR OLD.request_length IS NOT NEW.request_length OR OLD.request_length_state IS NOT NEW.request_length_state
		 OR OLD.request_length_version IS NOT NEW.request_length_version BEGIN ` + body + ` END`).Error; err != nil {
			return err
		}
		return tx.Exec(`CREATE TRIGGER IF NOT EXISTS nginx_evidence_change_delete_v1 AFTER DELETE ON nginx_request_evidences
		 BEGIN DELETE FROM nginx_evidence_changes_v1 WHERE event_id=OLD.event_id; END`).Error
	})
}
