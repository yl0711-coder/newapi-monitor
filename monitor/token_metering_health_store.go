package monitor

import (
	"errors"
	"fmt"
	"strings"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

// The resumable rebuild changes how the ledger is maintained, not its stored
// dimensions or meaning. Retain the deployed v1 marker so a healthy retained
// ledger is usable immediately instead of rewriting every fact on upgrade.
// Inconsistent or missing ledgers still enter the bounded background rebuild.
const tokenMeteringHealthSchemaVersion = "v1"

const tokenMeteringHealthRebuildRows = 128

var errTokenHealthCountsInconsistent = errors.New("token health counters inconsistent")

// The cursor and ledger are committed together. Triggers maintain only the
// scanned prefix while a rebuild is active, so concurrent inserts, updates and
// retention cannot double count or lose rows between batches or after restart.
type tokenMeteringHealthRebuild struct {
	ID          int `gorm:"primaryKey;autoIncrement:false"`
	Active      bool
	CursorEpoch string
	CursorID    int64
}

type tokenHealthDimension struct{ name, value, amount string }

func tokenHealthDimensions() []tokenHealthDimension {
	dims := []tokenHealthDimension{{"records", "'all'", "1"}, {"stored_bytes", "'all'", "COALESCE(f.stored_bytes,0)"},
		{"pending_links", "'all'", "CASE WHEN f.link_next_at>0 THEN 1 ELSE 0 END"}, {"closed_link_reviews", "'all'", "CASE WHEN f.link_next_at=0 THEN 1 ELSE 0 END"}}
	for _, name := range []string{"link_state", "link_reason", "length_state", "semantic_reason"} {
		dims = append(dims, tokenHealthDimension{name, "COALESCE(NULLIF(f." + name + ",''),'missing')", "1"})
	}
	jsonValue := func(path string) string {
		return "COALESCE(json_extract(CASE WHEN json_valid(f.source_facts) THEN f.source_facts ELSE '{\"other_state\":\"invalid\"}' END,'$." + path + "'),'missing')"
	}
	for _, name := range []string{"pricing_support", "other_state"} {
		dims = append(dims, tokenHealthDimension{name, jsonValue(name), "1"})
	}
	for _, name := range tokenHealthFields() {
		dims = append(dims, tokenHealthDimension{"field:" + name, jsonValue("fields." + name + ".state"), "1"})
	}
	return dims
}

func tokenHealthFields() []string {
	return []string{"prompt_tokens", "completion_tokens", "quota", "cache_tokens", "cache_creation_tokens", "model_ratio", "quota_per_unit", "expr_b64"}
}

type tokenMeteringHealthCount struct {
	SourceEpoch string `gorm:"primaryKey"`
	Dimension   string `gorm:"primaryKey"`
	Value       string `gorm:"primaryKey"`
	Count       int64
}

// The isolated all-customer store keeps health counts in the same transactions
// as facts. Health reads do not repeatedly parse days of JSON while holding the
// writer connection. Legacy main stores retain their existing read path.
// ensureTokenMeteringHealthCountsSchema creates only the small counter table
// and its maintenance triggers.  It deliberately does not scan the retained
// facts table.  The latter can contain millions of JSON rows and must never
// run before the HTTP listener is available.
func ensureTokenMeteringHealthCountsSchema(db *gorm.DB) error {
	return db.Transaction(func(tx *gorm.DB) error {
		if err := tx.AutoMigrate(&tokenMeteringHealthCount{}, &tokenMeteringHealthRebuild{}); err != nil {
			return err
		}
		return createTokenMeteringHealthCountTriggers(tx)
	})
}

func createTokenMeteringHealthCountTriggers(tx *gorm.DB) error {
	dims := tokenHealthDimensions()
	covered := func(prefix string) string {
		return "NOT EXISTS (SELECT 1 FROM token_metering_health_rebuilds WHERE id=1 AND active=1 AND (" + prefix + ".source_epoch," + prefix + ".log_id)>(cursor_epoch,cursor_id))"
	}
	// Replace every prior definition in the same transaction. CREATE TRIGGER IF
	// NOT EXISTS would leave a v1/v2 definition in place after a binary upgrade,
	// causing both stale and new ledgers to run for every association update.
	for _, name := range []string{"token_health_INSERT_v1", "token_health_INSERT_v2", "token_health_DELETE_v1", "token_health_DELETE_v2", "token_health_UPDATE_v1"} {
		if err := tx.Exec("DROP TRIGGER IF EXISTS " + name).Error; err != nil {
			return err
		}
	}
	for i := range dims {
		for _, version := range []string{"v1", "v2"} {
			name := fmt.Sprintf("token_health_UPDATE_%s_%d", version, i)
			if err := tx.Exec("DROP TRIGGER IF EXISTS " + name).Error; err != nil {
				return err
			}
		}
	}
	for _, event := range []string{"INSERT", "DELETE"} {
		values := []string{}
		for _, d := range dims {
			for _, prefix := range []string{"OLD", "NEW"} {
				if event == "INSERT" && prefix == "OLD" || event == "DELETE" && prefix == "NEW" {
					continue
				}
				amount := strings.ReplaceAll(d.amount, "f.", prefix+".")
				if prefix == "OLD" {
					amount = "-(" + amount + ")"
				}
				values = append(values, fmt.Sprintf("(%s.source_epoch,'%s',%s,%s)", prefix, d.name, strings.ReplaceAll(d.value, "f.", prefix+"."), amount))
			}
		}
		prefix := "NEW"
		if event == "DELETE" {
			prefix = "OLD"
		}
		query := fmt.Sprintf("CREATE TRIGGER token_health_%s_v2 AFTER %s ON token_metering_facts WHEN %s BEGIN INSERT INTO token_metering_health_counts(source_epoch,dimension,value,count) VALUES %s ON CONFLICT(source_epoch,dimension,value) DO UPDATE SET count=count+excluded.count; END", event, event, covered(prefix), strings.Join(values, ","))
		if err := tx.Exec(query).Error; err != nil {
			return err
		}
	}
	for i, d := range dims {
		oldKey, newKey := strings.ReplaceAll(d.value, "f.", "OLD."), strings.ReplaceAll(d.value, "f.", "NEW.")
		oldAmount, newAmount := strings.ReplaceAll(d.amount, "f.", "OLD."), strings.ReplaceAll(d.amount, "f.", "NEW.")
		changed := fmt.Sprintf("OLD.source_epoch IS NOT NEW.source_epoch OR (%s) IS NOT (%s) OR (%s) IS NOT (%s)", oldKey, newKey, oldAmount, newAmount)
		if strings.Contains(d.value, "json_extract") {
			// The cheap comparison avoids parsing retained source JSON during
			// link bookkeeping. Epoch changes still transfer every dimension.
			changed = "OLD.source_epoch IS NOT NEW.source_epoch OR (OLD.source_facts IS NOT NEW.source_facts AND (" + oldKey + ") IS NOT (" + newKey + "))"
		}
		changed = "((" + changed + ") OR OLD.log_id IS NOT NEW.log_id) AND ((" + covered("OLD") + ") OR (" + covered("NEW") + "))"
		oldAmount = "CASE WHEN " + covered("OLD") + " THEN " + oldAmount + " ELSE 0 END"
		newAmount = "CASE WHEN " + covered("NEW") + " THEN " + newAmount + " ELSE 0 END"
		query := fmt.Sprintf(`CREATE TRIGGER token_health_UPDATE_v2_%d AFTER UPDATE OF source_epoch,log_id,source_facts,stored_bytes,link_state,link_reason,length_state,semantic_reason,link_next_at ON token_metering_facts
			WHEN %s BEGIN INSERT INTO token_metering_health_counts(source_epoch,dimension,value,count)
			VALUES (OLD.source_epoch,'%s',%s,-(%s)),(NEW.source_epoch,'%s',%s,%s)
			ON CONFLICT(source_epoch,dimension,value) DO UPDATE SET count=count+excluded.count; END`, i, changed, d.name, oldKey, oldAmount, d.name, newKey, newAmount)
		if err := tx.Exec(query).Error; err != nil {
			return err
		}
	}
	return nil
}

// Closed-store initialization uses the same resumable batches as maintenance.
// Runtime maintenance reserves the fair writer separately for each batch.
func initTokenMeteringHealthCounts(db *gorm.DB) error {
	if err := ensureTokenMeteringHealthCountsSchema(db); err != nil {
		return err
	}
	for {
		done := false
		err := db.Transaction(func(tx *gorm.DB) error {
			var err error
			done, err = rebuildTokenMeteringHealthCountsBatch(tx, false)
			return err
		})
		if err != nil || done {
			return err
		}
	}
}

func rebuildTokenMeteringHealthCountsBatch(tx *gorm.DB, force bool) (bool, error) {
	var progress tokenMeteringHealthRebuild
	if err := tx.Where("id=1").Limit(1).Find(&progress).Error; err != nil {
		return false, err
	}
	if !progress.Active {
		ready, err := tokenMeteringHealthCountsReady(tx)
		if err != nil {
			return false, err
		}
		if ready && !force {
			return true, nil
		}
		if err := tx.Where("1=1").Delete(&tokenMeteringHealthCount{}).Error; err != nil {
			return false, err
		}
		progress = tokenMeteringHealthRebuild{ID: 1, Active: true}
		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&progress).Error; err != nil {
			return false, err
		}
	}
	var page []TokenMeteringFact
	if err := tx.Select("source_epoch,log_id").Where("(source_epoch,log_id)>(?,?)", progress.CursorEpoch, progress.CursorID).
		Order("source_epoch,log_id").Limit(tokenMeteringHealthRebuildRows).Find(&page).Error; err != nil {
		return false, err
	}
	if len(page) > 0 {
		last := page[len(page)-1]
		for _, d := range tokenHealthDimensions() {
			query := fmt.Sprintf(`INSERT INTO token_metering_health_counts(source_epoch,dimension,value,count)
				SELECT f.source_epoch,'%s',%s,SUM(%s) FROM token_metering_facts f
				WHERE (f.source_epoch,f.log_id)>(?,?) AND (f.source_epoch,f.log_id)<=(?,?) GROUP BY f.source_epoch,%s
				ON CONFLICT(source_epoch,dimension,value) DO UPDATE SET count=count+excluded.count`, d.name, d.value, d.amount, d.value)
			if err := tx.Exec(query, progress.CursorEpoch, progress.CursorID, last.SourceEpoch, last.LogID).Error; err != nil {
				return false, err
			}
		}
		progress.CursorEpoch, progress.CursorID = last.SourceEpoch, last.LogID
	}
	done := len(page) < tokenMeteringHealthRebuildRows
	progress.Active = !done
	if err := tx.Save(&progress).Error; err != nil {
		return false, err
	}
	if done {
		if err := tx.Clauses(clause.OnConflict{UpdateAll: true}).Create(&tokenMeteringHealthCount{Dimension: "schema", Value: tokenMeteringHealthSchemaVersion, Count: 1}).Error; err != nil {
			return false, err
		}
	}
	return done, nil
}

func tokenMeteringHealthCountsReady(tx *gorm.DB) (bool, error) {
	var count int64
	if err := tx.Model(&tokenMeteringHealthCount{}).
		Where("source_epoch='' AND dimension='schema' AND value=? AND count=1", tokenMeteringHealthSchemaVersion).Count(&count).Error; err != nil {
		return false, err
	}
	return count > 0, nil
}

func readTokenMeteringHealthCounts(tx *gorm.DB, epoch string, h *tokenMeteringHealth) error {
	var rows []tokenMeteringHealthCount
	if err := tx.Where("source_epoch=?", epoch).Find(&rows).Error; err != nil {
		return err
	}
	for _, row := range rows {
		if row.Count < 0 {
			return errTokenHealthCountsInconsistent
		}
		if row.Count == 0 {
			continue
		}
		var target map[string]int64
		switch row.Dimension {
		case "records":
			h.Facts = row.Count
		case "stored_bytes":
			h.StoredBytes = row.Count
		case "pending_links":
			h.PendingLinks = row.Count
		case "closed_link_reviews":
			h.ClosedLinkReviews = row.Count
		case "link_state":
			target = h.LinkStates
		case "link_reason":
			target = h.LinkReasons
		case "length_state":
			target = h.LengthStates
		case "semantic_reason":
			target = h.SemanticReasons
		case "pricing_support":
			target = h.PricingSupport
		case "other_state":
			target = h.OtherStates
		default:
			if strings.HasPrefix(row.Dimension, "field:") {
				name := strings.TrimPrefix(row.Dimension, "field:")
				if h.FieldStates[name] == nil {
					h.FieldStates[name] = map[string]int64{}
				}
				target = h.FieldStates[name]
			}
		}
		if target != nil {
			target[row.Value] = row.Count
		}
	}
	return nil
}

func emptyTokenMeteringHealthCounts() tokenMeteringHealth {
	return tokenMeteringHealth{
		LinkReasons:     map[string]int64{},
		LinkStates:      map[string]int64{},
		LengthStates:    map[string]int64{},
		SemanticReasons: map[string]int64{},
		PricingSupport:  map[string]int64{},
		OtherStates:     map[string]int64{},
		FieldStates:     map[string]map[string]int64{},
	}
}

// A ready marker alone is not a proof that a copied/partially upgraded ledger
// still describes the retained facts.  This bounded read checks the current
// epoch's derived dimensions without scanning the JSON fact table.  A mismatch
// is handled by the asynchronous full rebuild, while the HTTP endpoint remains
// explicitly unavailable during that interval.
func tokenMeteringHealthCountsConsistentForEpoch(tx *gorm.DB, epoch string) (bool, error) {
	h := emptyTokenMeteringHealthCounts()
	if err := readTokenMeteringHealthCounts(tx, epoch, &h); err != nil {
		if errors.Is(err, errTokenHealthCountsInconsistent) {
			return false, nil
		}
		return false, err
	}
	if !tokenMeteringHealthCountsConsistent(&h) {
		return false, nil
	}
	// A wholly missing ledger also has internally consistent zero dimensions.
	// Count the identity index only in this periodic background probe, never
	// rescan source JSON or mistake a failed read for a reason to reset progress.
	var facts int64
	if err := tx.Model(&TokenMeteringFact{}).Where("source_epoch=?", epoch).Count(&facts).Error; err != nil {
		return false, err
	}
	return h.Facts == facts, nil
}

// A ready counter ledger is useful only when its independent projections can
// still explain the same fact population. Keep this check cheap and local;
// rebuilding a mismatched ledger is preferable to presenting a plausible
// zero/partial health result while association is still incomplete.
func tokenMeteringHealthCountsConsistent(h *tokenMeteringHealth) bool {
	if h == nil {
		return false
	}
	if h.Facts < 0 || h.StoredBytes < 0 || h.PendingLinks < 0 || h.ClosedLinkReviews < 0 ||
		h.PendingLinks+h.ClosedLinkReviews != h.Facts {
		return false
	}
	// Every fact contributes exactly one value to each of these dimensions.
	// Checking all of them catches a trigger left behind by an older schema or
	// a partially rebuilt ledger before a plausible-looking zero is exposed.
	for _, values := range []map[string]int64{h.LinkStates, h.LinkReasons, h.LengthStates, h.SemanticReasons, h.PricingSupport, h.OtherStates} {
		if healthCountSum(values) != h.Facts {
			return false
		}
	}
	for _, field := range tokenHealthFields() {
		if healthCountSum(h.FieldStates[field]) != h.Facts {
			return false
		}
	}
	return true
}

func healthCountSum(values map[string]int64) int64 {
	var total int64
	for _, n := range values {
		if n < 0 {
			return -1
		}
		total += n
	}
	return total
}
