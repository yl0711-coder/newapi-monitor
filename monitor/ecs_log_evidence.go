package monitor

import "gorm.io/gorm"

// Legacy evidence IDs are globally unique. Until that store has a separately
// reviewed composite-key migration, a malformed ECS source must not silently
// deduplicate against another task's event and receive a successful ACK.
// Called within the existing evidence transaction; no check/write race.
func validateECSEvidenceEventOwnership(tx *gorm.DB, node string, rows []NginxRequestEvidence) error {
	ids := make([]string, 0, len(rows))
	seen := make(map[string]bool, len(rows))
	for _, row := range rows {
		if seen[row.EventID] {
			return errNginxEvidenceBatchConflict
		}
		seen[row.EventID] = true
		ids = append(ids, row.EventID)
	}
	// Each batch contains at most 1,000 events. Chunk queries also work with
	// SQLite builds whose bind-parameter limit is below that size.
	for start := 0; start < len(ids); start += 200 {
		var count int64
		if err := tx.Model(&NginxRequestEvidence{}).Where("event_id IN ? AND node <> ?", ids[start:min(start+200, len(ids))], node).Count(&count).Error; err != nil {
			return err
		}
		if count != 0 {
			return errNginxEvidenceBatchConflict
		}
	}
	return nil
}
