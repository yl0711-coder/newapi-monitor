package monitor

import (
	"errors"
	"fmt"
	"time"

	"gorm.io/gorm"
)

var errUsageFactsClassificationMigrationRequired = errors.New("usage facts classification migration required")

// usageFactsContainDerivedTraffic proves whether a version upgrade has any
// log-derived state to invalidate. v5 fixes a NULL predicate that can affect
// every user identity, so the old v4 optimization that checked only synthetic
// root traffic is no longer safe.
func usageFactsContainDerivedTraffic(tx *gorm.DB) (bool, error) {
	// COUNT(*) still scans the whole table even with LIMIT 1. Startup needs only
	// an existence proof, so keep every probe index/row bounded.
	tables := []string{
		"usage_hour_facts",
		"usage_daily_facts",
		"usage_fact_member_day_states",
		"usage_hour_ingest_states",
		"usage_fact_member_states",
		"usage_fact_member_hour_states",
		"usage_fact_published_members",
	}
	for _, table := range tables {
		var exists int
		if err := tx.Raw("SELECT EXISTS(SELECT 1 FROM " + table + " LIMIT 1)").Scan(&exists).Error; err != nil {
			return false, err
		}
		if exists == 1 {
			return true, nil
		}
	}
	return false, nil
}

// migrateUsageFactsTrafficClassification upgrades the global semantic marker.
//
// Normal startup is deliberately read-only when log-derived rows exist: an
// application upgrade must never translate into an implicit full-table DELETE,
// long SQLite write lock or WAL spike.  An explicitly authorised maintenance
// window only revokes the small serving/control surface.  Hour/day facts and
// proofs remain on disk and the durable full-history worker replaces them per
// member/day under independent source control; until then no v5 publication is
// possible. Profile snapshots are not derived from logs and remain reusable.
func migrateUsageFactsTrafficClassification(db *gorm.DB, maintenanceEnabled bool) (UsageFactSyncState, bool, error) {
	var final UsageFactSyncState
	invalidated := false
	err := db.Transaction(func(tx *gorm.DB) error {
		if err := tx.First(&final, 1).Error; err != nil {
			return err
		}
		if final.TrafficClassVersion == userTrafficClassificationVersion {
			return nil
		}
		impacted, err := usageFactsContainDerivedTraffic(tx)
		if err != nil {
			return fmt.Errorf("检查旧版 facts 测试账号影响: %w", err)
		}
		if !impacted {
			final.TrafficClassVersion = userTrafficClassificationVersion
			return tx.Save(&final).Error
		}
		if !maintenanceEnabled {
			return fmt.Errorf("%w: current=%d required=%d; set MONITOR_USAGE_FACTS_CLASSIFICATION_MIGRATION_ENABLED=true only in an approved full-history maintenance window",
				errUsageFactsClassificationMigrationRequired, final.TrafficClassVersion, userTrafficClassificationVersion)
		}

		// This is intentionally bounded by the tracked/published/job cardinality.
		// The potentially large fact/proof tables are never cleared at startup.
		if err := tx.Where("1 = 1").Delete(&UsageFactPublishedMember{}).Error; err != nil {
			return err
		}
		if err := tx.Model(&UsageFactJob{}).
			Where("status NOT IN ?", []string{usageFactHistoryJobComplete, usageFactHistoryJobCancelled}).
			Updates(map[string]any{"status": usageFactHistoryJobCancelled, "lease_owner": "", "lease_until": 0,
				"last_error": "traffic classification changed", "completed_at": time.Now().Unix(), "updated_at": time.Now().Unix()}).Error; err != nil {
			return err
		}
		// Keep the old member semantic signature. Reconcile sees the version
		// mismatch and creates durable v5 discovery/backfill/verify work. Never
		// relabel old member/day proofs as current without reading the source.
		// Preserve profile freshness; clear only candidate/serving authorization.
		final = UsageFactSyncState{
			ID:                   1,
			TrafficClassVersion:  userTrafficClassificationVersion,
			Generation:           final.Generation + 1,
			ServingGeneration:    final.ServingGeneration + 1,
			LastProfileSyncAt:    final.LastProfileSyncAt,
			LastProfileFailureAt: final.LastProfileFailureAt,
		}
		if err := tx.Save(&final).Error; err != nil {
			return err
		}
		invalidated = true
		return nil
	})
	return final, invalidated, err
}
