package monitor

import "gorm.io/gorm"

// currentEconomicsPublicationQuery retains both current-pointer checks and
// the manifest's authoritative account epoch. In SQLite, CROSS JOIN fixes
// mc before mp without changing inner-join semantics (the ON clause remains).
// Otherwise range propagation from p.hour_ts can make the planner scan every
// manifest in the requested date range for EACH publication instead of looking
// up the one current manifest by ID. Start from current heads, resolve mp by
// primary key, then seek publications at that exact hour using existing indexes.
// Pin the existing publication hour index as well: a domain filter otherwise
// makes SQLite choose the domain-only index and rescan that domain's history
// for every hour. AutoMigrate already maintains this index; no new index or
// schema migration is introduced here.
// Keep range/domain predicates, projections, ordering and row limits at callers.
func currentEconomicsPublicationQuery(db *gorm.DB) *gorm.DB {
	return db.Table("channel_economics_hour_manifest_current mc").
		Joins("CROSS JOIN channel_economics_hour_manifest_publications mp ON mp.manifest_id=mc.manifest_id").
		Joins("JOIN channel_economics_hour_publications p INDEXED BY idx_channel_economics_hour_publications_hour_ts ON p.domain=mp.domain AND p.hour_ts=mp.hour_ts AND p.account_epoch=mp.authoritative_epoch AND p.semantics_version=mp.semantics_version").
		Joins("JOIN channel_economics_hour_current c ON c.publication_id=p.publication_id")
}
