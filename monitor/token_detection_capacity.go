package monitor

import (
	"context"
	"fmt"

	"gorm.io/gorm"
)

// SQLite triggers keep global capacity exact for every writer, including
// cleanup, retries and a different source epoch. Checking this single row in
// the fenced write transaction avoids scanning all frozen history per request.
type tokenDetectionCapacity struct {
	ID             int
	CurrentRows    int64
	RevisionBytes  int64
	ReferenceBytes int64
	HeadBytes      int64
	ReferenceRows  int64
}

func (tokenDetectionCapacity) TableName() string { return "token_detection_capacity" }

const tokenDetectionActiveReferencePredicate = "snapshot_json <> '' AND (invalid_reason = '' OR invalid_reason IS NULL)"

func tokenDetectionActiveReferences(db *gorm.DB, now int64) *gorm.DB {
	// Keep the literal predicate identical to the partial index. It contains
	// only active snapshot identities, so counting does not reread every large
	// frozen JSON payload while holding the writer. Legacy read-only stores
	// without the index retain the same query and NULL/empty-field semantics.
	return db.Model(&TokenDetectionReference{}).Where(tokenDetectionActiveReferencePredicate).Where("expires_at > ?", now)
}

func (m *Monitor) ensureTokenDetectionCapacity(ctx context.Context) error {
	if m.tokenDetectionCapacityReady.Load() {
		return nil
	}
	err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		if err := tx.Exec(`CREATE TABLE IF NOT EXISTS token_detection_capacity (
			id INTEGER PRIMARY KEY CHECK(id=1),current_rows INTEGER NOT NULL,
			revision_bytes INTEGER NOT NULL,reference_bytes INTEGER NOT NULL,
			head_bytes INTEGER NOT NULL,reference_rows INTEGER NOT NULL)`).Error; err != nil {
			return err
		}
		// Initialization and trigger installation hold the same write transaction.
		// A restart must never overwrite an already maintained ledger.
		if err := tx.Exec(`INSERT INTO token_detection_capacity
			SELECT 1,(SELECT COUNT(*) FROM token_detection_currents),
			(SELECT COALESCE(SUM(stored_bytes),0) FROM token_detection_revisions),
			(SELECT COALESCE(SUM(stored_bytes),0) FROM token_detection_references),
			(SELECT COALESCE(SUM(length(CAST(result_json AS BLOB))),0) FROM token_detection_currents),
			(SELECT COUNT(*) FROM token_detection_references)
			WHERE NOT EXISTS(SELECT 1 FROM token_detection_capacity WHERE id=1)`).Error; err != nil {
			return err
		}
		for _, spec := range []struct{ table, column, bytes, count string }{
			{"token_detection_currents", "result_json", "head_bytes", "current_rows"},
			{"token_detection_revisions", "stored_bytes", "revision_bytes", ""},
			{"token_detection_references", "stored_bytes", "reference_bytes", "reference_rows"},
		} {
			value := func(prefix string) string {
				if spec.column == "result_json" {
					return "COALESCE(length(CAST(" + prefix + ".result_json AS BLOB)),0)"
				}
				return "COALESCE(" + prefix + ".stored_bytes,0)"
			}
			for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
				delta := value("NEW")
				countDelta := "+1"
				triggerEvent, condition := event, ""
				if event == "DELETE" {
					delta = "-" + value("OLD")
					countDelta = "-1"
				} else if event == "UPDATE" {
					delta = value("NEW") + "-" + value("OLD")
					triggerEvent = "UPDATE OF " + spec.column
					condition = " WHEN " + value("NEW") + " <> " + value("OLD")
				}
				// Replace both known generations atomically. IF NOT EXISTS alone
				// preserves an unconditional legacy UPDATE trigger, while a stray
				// v2 would make every write update the ledger twice.
				for _, version := range []string{"v1", "v2"} {
					if err := tx.Exec(fmt.Sprintf("DROP TRIGGER IF EXISTS %s_capacity_%s_%s", spec.table, event, version)).Error; err != nil {
						return err
					}
				}
				set := spec.bytes + "=" + spec.bytes + "+(" + delta + ")"
				if spec.count != "" && event != "UPDATE" {
					set += "," + spec.count + "=" + spec.count + countDelta
				}
				query := fmt.Sprintf("CREATE TRIGGER IF NOT EXISTS %s_capacity_%s_v1 AFTER %s ON %s%s BEGIN UPDATE token_detection_capacity SET %s WHERE id=1; END", spec.table, event, triggerEvent, spec.table, condition, set)
				if err := tx.Exec(query).Error; err != nil {
					return err
				}
			}
		}
		return tx.Exec("CREATE INDEX IF NOT EXISTS idx_token_detection_active_reference_v1 ON token_detection_references(expires_at) WHERE " + tokenDetectionActiveReferencePredicate).Error
	})
	if err == nil {
		m.tokenDetectionCapacityReady.Store(true)
	}
	return err
}
