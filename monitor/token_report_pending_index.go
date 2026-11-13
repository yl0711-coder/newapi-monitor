package monitor

import (
	"fmt"
	"strings"

	"gorm.io/gorm"
)

const tokenReportPendingMembers = "token_report_pending_members_v1"
const tokenReportPendingGroups = "token_report_pending_groups_v1"

// Materialize only request/head identity validity. Window completeness and
// reference generations stay authoritative in their original tables and are
// checked at read time. This avoids rewriting peers whenever a bucket changes.
// Both membership and group counters change inside the fact/head transaction.
func initTokenReportPendingIndex(tx *gorm.DB) error {
	existed := tx.Migrator().HasTable(tokenReportPendingMembers)
	upgrade := existed && !tx.Migrator().HasColumn(tokenReportPendingMembers, "head_present")
	scheduleUpgrade := existed && !tx.Migrator().HasColumn(tokenReportPendingMembers, "next_at")
	columns := `source_epoch TEXT NOT NULL,log_id INTEGER NOT NULL,created_at INTEGER,
		user_id INTEGER,channel_id INTEGER,model TEXT,user_group TEXT,start_at INTEGER,
		base_pending INTEGER NOT NULL,policy_hash TEXT,reference_bucket TEXT,
		reference_generation INTEGER,source_complete INTEGER,fact_revision INTEGER,link_revision INTEGER,
		retry_present INTEGER NOT NULL,head_present INTEGER NOT NULL,next_at INTEGER,group_key TEXT NOT NULL`
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportPendingMembers + ` (` + columns + `,PRIMARY KEY(source_epoch,log_id))`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_pending_member_range_v1 ON ` + tokenReportPendingMembers + ` (source_epoch,created_at,log_id)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_pending_member_group_v1 ON ` + tokenReportPendingMembers + ` (group_key,log_id,source_epoch)`).Error; err != nil {
		return err
	}
	groupColumns := `group_key TEXT PRIMARY KEY,source_epoch TEXT NOT NULL,start_at INTEGER,
		user_id INTEGER,channel_id INTEGER,model TEXT,user_group TEXT,base_pending INTEGER NOT NULL,
		policy_hash TEXT,reference_bucket TEXT,reference_generation INTEGER,source_complete INTEGER,retry_present INTEGER NOT NULL,head_present INTEGER NOT NULL,requests INTEGER NOT NULL`
	if err := tx.Exec(`CREATE TABLE IF NOT EXISTS ` + tokenReportPendingGroups + ` (` + groupColumns + `)`).Error; err != nil {
		return err
	}
	if upgrade {
		// Only the derived index is rebuilt. Readers see an explicit rebuilding
		// gap until the durable migration has committed every identity.
		for _, table := range []string{tokenReportPendingMembers, tokenReportPendingGroups} {
			if !tx.Migrator().HasColumn(table, "head_present") {
				if err := tx.Exec(`ALTER TABLE ` + table + ` ADD COLUMN head_present INTEGER NOT NULL DEFAULT 0`).Error; err != nil {
					return err
				}
			}
		}
		for _, name := range []string{"member_insert", "member_delete", "fact_INSERT", "fact_UPDATE", "fact_DELETE", "head_INSERT", "head_UPDATE", "head_DELETE", "retry_INSERT", "retry_UPDATE", "retry_DELETE"} {
			if err := tx.Exec(`DROP TRIGGER IF EXISTS token_report_pending_` + name + `_v1`).Error; err != nil {
				return err
			}
		}
		// Existing members and their group counts remain paired. The durable
		// migration below replaces only a bounded identity prefix per turn.
	}
	if scheduleUpgrade {
		if err := tx.Exec(`ALTER TABLE ` + tokenReportPendingMembers + ` ADD COLUMN next_at INTEGER`).Error; err != nil {
			return err
		}
		// Reinstall the projection for an existing intermediate schema too.
		for _, source := range []string{"fact", "head", "retry"} {
			for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
				if err := tx.Exec(`DROP TRIGGER IF EXISTS token_report_pending_` + source + `_` + event + `_v1`).Error; err != nil {
					return err
				}
			}
		}
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_discovery_v1 ON ` + tokenReportPendingMembers + `
	 (source_epoch,created_at,log_id,next_at,retry_present,head_present,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,start_at,fact_revision,link_revision)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_changed_group_v1 ON ` + tokenReportPendingGroups + `(source_epoch,retry_present,head_present,start_at) WHERE base_pending=1`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_changed_identity_v1 ON ` + tokenReportPendingMembers + `(source_epoch,created_at,log_id) WHERE base_pending=1 AND head_present=1`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_retry_schedule_v1 ON token_detection_retries(source_epoch,next_at,log_id)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_pending_no_head_v1 ON ` + tokenReportPendingMembers + `(source_epoch,created_at,log_id,user_id,channel_id,model,user_group) WHERE head_present=0`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_pending_fresh_v1 ON ` + tokenReportPendingMembers + `
		(source_epoch,retry_present,head_present,created_at,log_id)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_pending_group_v1 ON ` + tokenReportPendingGroups + `
		(source_epoch,retry_present,head_present,start_at,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,requests,group_key)`).Error; err != nil {
		return err
	}
	// Global queue classification needs validity and totals, not the large JSON
	// group key used to expand sparse work. A separate narrow covering index
	// avoids rereading that key for every retained group on each worker turn.
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_pending_count_v1 ON ` + tokenReportPendingGroups + `
		(source_epoch,retry_present,head_present,start_at,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,requests)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_pending_due_v1 ON ` + tokenReportPendingMembers + `
		(source_epoch,retry_present,head_present,next_at,created_at,log_id,group_key,fact_revision,link_revision)`).Error; err != nil {
		return err
	}
	// The detector's corrected prefix tests copied head/window fields for
	// every retained member. Keep that bounded scan covering so a large stale
	// queue does not fetch the full identity row before the validity predicate
	// can reject it.
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_detection_pending_scan_v1 ON ` + tokenReportPendingMembers + `
		(source_epoch,retry_present,head_present,created_at,log_id,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,start_at,fact_revision,link_revision)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_pending_group_range_v1 ON ` + tokenReportPendingGroups + ` (source_epoch,start_at)`).Error; err != nil {
		return err
	}
	// Unfiltered range counts only need validity and totals. Cover these
	// operands without fetching each group's larger identity/key row.
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_pending_count_v1 ON ` + tokenReportPendingGroups + `
		(source_epoch,start_at,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,requests)`).Error; err != nil {
		return err
	}
	if err := tx.Exec(`CREATE INDEX IF NOT EXISTS idx_token_report_pending_health_v1 ON ` + tokenReportPendingGroups + `
		(source_epoch,retry_present,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,start_at,group_key)`).Error; err != nil {
		return err
	}
	groupFields := []string{"source_epoch", "start_at", "user_id", "channel_id", "model", "user_group", "base_pending", "policy_hash", "reference_bucket", "reference_generation", "source_complete", "retry_present", "head_present"}
	memberFields := "source_epoch,log_id,created_at,user_id,channel_id,model,user_group,start_at,base_pending,policy_hash,reference_bucket,reference_generation,source_complete,fact_revision,link_revision,retry_present,head_present,next_at,group_key"
	groupInsert := `INSERT INTO ` + tokenReportPendingGroups + ` (group_key,` + strings.Join(groupFields, ",") + `,requests)`
	memberInsert := `INSERT INTO ` + tokenReportPendingMembers + ` (` + memberFields + `)`
	qualified := func(alias string) string {
		fields := make([]string, len(groupFields))
		for i, field := range groupFields {
			fields[i] = alias + "." + field
		}
		return strings.Join(fields, ",")
	}
	projection := tokenReportPendingProjection(tx)
	// Reinstall all projection triggers on every schema open.  Existing
	// databases may still carry the pre-DetectionState definitions behind an
	// IF NOT EXISTS guard; leaving those in place would keep future heads out of
	// the queue even though the projection expression has been upgraded.
	for _, source := range []string{"fact", "head", "retry"} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			if err := tx.Exec(`DROP TRIGGER IF EXISTS token_report_pending_` + source + `_` + event + `_v1`).Error; err != nil {
				return err
			}
		}
	}
	// The head update trigger gained DetectionState as a queue input. Replace
	// the old definition on existing databases; IF NOT EXISTS alone would keep
	// the stale trigger forever and empty legacy states would never requeue.
	if err := tx.Exec(`DROP TRIGGER IF EXISTS token_report_pending_head_UPDATE_v1`).Error; err != nil {
		return err
	}
	statements := []string{
		`CREATE TRIGGER IF NOT EXISTS token_report_pending_member_insert_v1 AFTER INSERT ON ` + tokenReportPendingMembers + ` BEGIN
		` + groupInsert + ` VALUES (NEW.group_key,` + qualified("NEW") + `,1)
		ON CONFLICT(group_key) DO UPDATE SET requests=requests+1; END`,
		`CREATE TRIGGER IF NOT EXISTS token_report_pending_member_delete_v1 AFTER DELETE ON ` + tokenReportPendingMembers + ` BEGIN
		UPDATE ` + tokenReportPendingGroups + ` SET requests=requests-1 WHERE group_key=OLD.group_key;
		DELETE FROM ` + tokenReportPendingGroups + ` WHERE group_key=OLD.group_key AND requests=0; END`,
	}
	for _, statement := range statements {
		if err := tx.Exec(statement).Error; err != nil {
			return err
		}
	}
	// Scheduling alone does not change membership or group counters. Keep its
	// narrow index current without deleting/reinserting a whole projection.
	if err := tx.Exec(`CREATE TRIGGER IF NOT EXISTS token_report_pending_schedule_v1 AFTER UPDATE OF next_at ON token_detection_currents
		WHEN NEW.next_at IS NOT OLD.next_at BEGIN UPDATE ` + tokenReportPendingMembers + ` SET next_at=NEW.next_at WHERE source_epoch=NEW.source_epoch AND log_id=NEW.log_id; END`).Error; err != nil {
		return err
	}
	for _, source := range []struct {
		name, table string
		fields      []string
	}{
		{"fact", "token_metering_facts", []string{"source_epoch", "log_id", "created_at", "user_id", "channel_id", "model", "user_group", "revision", "link_revision"}},
		{"head", "token_detection_currents", []string{"source_epoch", "log_id", "fact_revision", "link_revision", "policy_hash", "reference_bucket", "reference_generation", "source_complete", "detection_state"}},
		{"retry", "token_detection_retries", []string{"source_epoch", "log_id"}},
	} {
		for _, event := range []string{"INSERT", "UPDATE", "DELETE"} {
			identities := []string{"NEW"}
			if event == "DELETE" {
				identities = []string{"OLD"}
			} else if event == "UPDATE" {
				identities = []string{"OLD", "NEW"}
			}
			where := func(alias string) string {
				parts := make([]string, len(identities))
				for i, identity := range identities {
					parts[i] = fmt.Sprintf("(%ssource_epoch=%s.source_epoch AND %slog_id=%s.log_id)", alias, identity, alias, identity)
				}
				return strings.Join(parts, " OR ")
			}
			updateOf, when := "", ""
			if event == "UPDATE" {
				updateOf = " OF " + strings.Join(source.fields, ",")
				changed := make([]string, len(source.fields))
				for i, field := range source.fields {
					changed[i] = "NEW." + field + " IS NOT OLD." + field
				}
				when = " WHEN " + strings.Join(changed, " OR ")
			}
			statement := `CREATE TRIGGER IF NOT EXISTS token_report_pending_` + source.name + `_` + event + `_v1 AFTER ` + event + updateOf + ` ON ` + source.table + when + ` BEGIN
			DELETE FROM ` + tokenReportPendingMembers + ` WHERE ` + where("") + `;
			` + memberInsert + ` ` + projection + ` WHERE ` + where("f.") + `) v; END`
			if err := tx.Exec(statement).Error; err != nil {
				return err
			}
		}
	}
	return initTokenPendingMigration(tx, !existed || upgrade || scheduleUpgrade)
}

func tokenReportPendingScope(db *gorm.DB, q tokenDetectionQuery, alias string) *gorm.DB {
	db = db.Where(alias+"source_epoch=?", q.SourceEpoch)
	if q.UserID > 0 || q.UserSet {
		db = db.Where(alias+"user_id=?", q.UserID)
	}
	if q.ChannelID > 0 || q.ChannelSet {
		db = db.Where(alias+"channel_id=?", q.ChannelID)
	}
	if q.Model != "" || q.ModelSet {
		db = db.Where(alias+"model=?", q.Model)
	}
	if q.Group != "" || q.GroupSet {
		db = db.Where(alias+"user_group=?", q.Group)
	}
	return db
}

// Known stale identities do not need bucket/window lookups. Keep SQL NULL
// semantics for legacy nullable heads, including callers that negate this
// expression to select current results. The only parameter is the policy hash.
func tokenReportPendingPredicate(alias string) string {
	return fmt.Sprintf(`CASE WHEN %[1]s.base_pending=1 OR %[1]s.policy_hash!=? THEN 1 ELSE
		(CASE WHEN %[1]s.policy_hash IS NULL THEN NULL ELSE 0 END
		OR %[1]s.reference_generation!=COALESCE((SELECT b.generation FROM token_detection_buckets b WHERE b.id=%[1]s.reference_bucket),0)
		OR %[1]s.source_complete!=COALESCE((SELECT CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END
		FROM token_metering_windows w WHERE w.source_epoch=%[1]s.source_epoch AND w.start_at=%[1]s.start_at),0)) END`, alias)
}

// The joined equivalent preserves SQL NULL semantics while sharing the two
// dependency lookups across aggregates. Only use it with the b/w LEFT JOINs.
func tokenReportPendingJoinedPredicate(alias string) string {
	return fmt.Sprintf(`%[1]s.base_pending=1 OR %[1]s.policy_hash!=? OR %[1]s.reference_generation!=COALESCE(b.generation,0)
		OR %[1]s.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END`, alias)
}

func tokenReportPendingCount(db *gorm.DB, q tokenDetectionQuery, hash string) (int64, error) {
	if !db.Migrator().HasTable(tokenReportPendingMembers) {
		return tokenReportLegacyPendingCount(db, q, hash)
	}
	query := func(table string) *gorm.DB {
		return tokenReportPendingScope(db.Table(table+" AS g"), q, "g.").
			Joins("LEFT JOIN token_detection_buckets b ON b.id=g.reference_bucket").
			Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=g.source_epoch AND w.start_at=g.start_at").
			Where("("+tokenReportPendingJoinedPredicate("g")+")", hash)
	}
	// Full five-minute windows use transactional group totals. Only the two
	// partial boundary windows need individual membership rows; this also keeps
	// arbitrary one-minute queries and all identity filters exact.
	left, right := (q.From+299)/300*300, q.To/300*300
	var pending int64
	if left >= right {
		err := query(tokenReportPendingMembers).Where("g.created_at>=? AND g.created_at<?", q.From, q.To).Count(&pending).Error
		return pending, err
	}
	interior := query(tokenReportPendingGroups).Where("g.start_at>=? AND g.start_at<?", left, right).Select("COALESCE(SUM(g.requests),0)")
	// Empty edge predicates joined with OR can still scan every retained
	// member. Omit empty edges and give each remaining edge its own range seek.
	parts, args := []string{"(?)"}, []any{interior}
	for _, edge := range [][2]int64{{q.From, left}, {right, q.To}} {
		if edge[0] < edge[1] {
			parts = append(parts, "(?)")
			args = append(args, query(tokenReportPendingMembers).Where("g.created_at>=? AND g.created_at<?", edge[0], edge[1]).Select("COUNT(*)"))
		}
	}
	err := db.Raw("SELECT "+strings.Join(parts, " + "), args...).Scan(&pending).Error
	return pending, err
}

func tokenReportLegacyPendingCount(db *gorm.DB, q tokenDetectionQuery, hash string) (int64, error) {
	var pending int64
	query := tokenReportPendingScope(db.Table("token_metering_facts AS f"), q, "f.").
		Joins(tokenDetectionHeadJoin(db)).
		Joins("LEFT JOIN token_detection_buckets b ON b.id=c.reference_bucket").
		Joins("LEFT JOIN token_metering_windows w ON w.source_epoch=f.source_epoch AND w.start_at=(f.created_at/300)*300").
		Where("f.created_at>=? AND f.created_at<?", q.From, q.To).
		Where(`c.log_id IS NULL OR COALESCE(c.detection_state,'')='' OR c.fact_revision!=f.revision OR c.link_revision!=f.link_revision OR c.policy_hash!=? OR c.reference_generation!=COALESCE(b.generation,0) OR c.source_complete!=CASE WHEN w.done=1 AND w.source_complete=1 AND w.replay=0 THEN 1 ELSE 0 END`, hash)
	err := query.Count(&pending).Error
	return pending, err
}

func tokenReportPendingSample(db *gorm.DB, epoch, hash string, now int64, limit int) ([]TokenMeteringFact, error) {
	var rows []TokenMeteringFact
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		rows, err = tokenReportPendingSampleSnapshot(tx, epoch, hash, now, limit)
		return err
	})
	return rows, err
}

func tokenReportPendingSampleSnapshot(db *gorm.DB, epoch, hash string, now int64, limit int) ([]TokenMeteringFact, error) {
	// Health uses the same bounded durable lanes. An empty sample never scans
	// every retained group merely to prove that there is no currently due work.
	return tokenDetectionBoundedQueueJobs(db, epoch, hash, now, limit, 15, nil)
}
