package monitor

import "gorm.io/gorm"

const tokenReferenceCandidateIndex = "idx_token_reference_candidates_v1"

// The caller already selected at most 200 identities (64 for a frozen
// anchor). Ordering their payloads by event time must not make SQLite scan
// the entire epoch through its time index. Seek the table's existing compound
// primary key, then sort only these selected rows; no new index is required.
func tokenReferenceFactIdentities(tx *gorm.DB, epoch string, ids []int64) *gorm.DB {
	table := "token_metering_facts AS f"
	if tx.Dialector.Name() == "sqlite" {
		table += " INDEXED BY sqlite_autoindex_token_metering_facts_1"
	}
	return tx.Table(table).Where("f.source_epoch=? AND f.log_id IN ?", epoch, ids)
}

// Keep candidate eligibility and event ordering in a narrow index. The older
// user/model/time index required a wide fact fetch for every rejected channel,
// size or association. In a busy user/model range that can hold the writer for
// seconds even when no control qualifies. Payloads are read only after the
// bounded identity page is known to fit the reference cap.
func initTokenDetectionReferenceIndex(db *gorm.DB) error {
	if db.Dialector.Name() != "sqlite" || !db.Migrator().HasTable(&TokenMeteringFact{}) {
		return nil
	}
	return db.Exec(`CREATE INDEX IF NOT EXISTS ` + tokenReferenceCandidateIndex + `
		ON token_metering_facts(source_epoch,user_id,model,user_group,created_at,log_id,channel_id,request_length)
		WHERE link_state='unique' AND evidence_level='exact' AND length_state='valid'`).Error
}
