package monitor

import (
	"context"
	"errors"
	"strings"
	"time"

	"gorm.io/gorm"
)

const tokenEvidenceCursorTable = "token_metering_evidence_cursor_v1"

type tokenEvidenceCursor struct {
	SourceEpoch, SourceID                  string
	Generation, PendingGeneration, OwnerID int64
	BootstrapEvent, BootstrapPending       string
	BootstrapOwner                         int64
	BootstrapDone                          bool
	Version                                int64
}

type tokenEvidenceChange struct {
	EventID, RequestID, KeyID, Hash string
	Generation                      int64
}

func initTokenMeteringEvidenceCursor(tx *gorm.DB) error {
	for _, stmt := range []string{
		`CREATE TABLE IF NOT EXISTS ` + tokenEvidenceCursorTable + `(source_epoch TEXT PRIMARY KEY,source_id TEXT NOT NULL,
		 generation INTEGER NOT NULL DEFAULT 0,pending_generation INTEGER NOT NULL DEFAULT 0,owner_id INTEGER NOT NULL DEFAULT 0,
		 bootstrap_event TEXT NOT NULL DEFAULT '',bootstrap_pending TEXT NOT NULL DEFAULT '',bootstrap_owner INTEGER NOT NULL DEFAULT 0,
		 bootstrap_done INTEGER NOT NULL DEFAULT 0,version INTEGER NOT NULL DEFAULT 0)`,
		`CREATE INDEX IF NOT EXISTS idx_token_fact_request_owner_v1 ON token_metering_facts(request_id,source_epoch,log_id)`,
		`CREATE INDEX IF NOT EXISTS idx_token_fact_event_owner_v1 ON token_metering_facts(entry_event_id,source_epoch,log_id)`,
		`CREATE INDEX IF NOT EXISTS idx_token_ref_exact_owner_v1 ON token_metering_refs(key_id,hash,source_epoch,log_id)`,
	} {
		if err := tx.Exec(stmt).Error; err != nil {
			return err
		}
	}
	return nil
}

// A closed retry schedule must not hide later evidence. Incremental changes
// and an initial, durable bootstrap each get half of a bounded read allowance.
// Only exact identities are scheduled. Association remains the sole writer of
// link content/revisions; it wakes detection after that content commits.
func (m *Monitor) refreshTokenMeteringEvidence(parent context.Context, now int64) (bool, error) {
	if m.nginxEvidenceDB == nil || nginxEvidenceMode(m.cfg.NginxEvidenceMode) == "off" {
		return false, nil
	}
	ctx, cancel := context.WithTimeout(parent, time.Second)
	defer cancel()
	evidence := m.nginxEvidenceDB.WithContext(ctx)
	if !evidence.Migrator().HasTable("nginx_evidence_changes_v1") {
		return false, nil // Read-only legacy/test stores retain ordinary retries.
	}
	var sourceID string
	if err := evidence.Table("nginx_evidence_change_clock_v1").Select("source_id").Where("id=1").Scan(&sourceID).Error; err != nil {
		return false, err
	}
	var cursor tokenEvidenceCursor
	if err := m.tokenReadStore().WithContext(ctx).Table(tokenEvidenceCursorTable).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Limit(1).Find(&cursor).Error; err != nil {
		return false, err
	}
	if cursor.SourceEpoch == "" || cursor.SourceID != sourceID {
		err := m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
			return tx.Exec(`INSERT INTO `+tokenEvidenceCursorTable+`(source_epoch,source_id) VALUES(?,?)
			 ON CONFLICT(source_epoch) DO UPDATE SET source_id=excluded.source_id,generation=0,pending_generation=0,owner_id=0,
			 bootstrap_event='',bootstrap_pending='',bootstrap_owner=0,bootstrap_done=0,version=version+1
			 WHERE source_id<>excluded.source_id`, m.cfg.TokenFactsSourceEpoch, sourceID).Error
		})
		if err != nil {
			return true, err
		}
		if err := m.tokenReadStore().WithContext(ctx).Table(tokenEvidenceCursorTable).Where("source_epoch=?", m.cfg.TokenFactsSourceEpoch).Take(&cursor).Error; err != nil {
			return true, err
		}
	}
	more := false
	for _, bootstrap := range []bool{false, true} {
		if bootstrap && cursor.BootstrapDone {
			continue
		}
		var changes []tokenEvidenceChange
		var query *gorm.DB
		if bootstrap {
			query = evidence.Table("nginx_request_evidences e").Select("e.event_id,e.oneapi_request_id AS request_id,e.hmac_key_id AS key_id,e.oneapi_id_hmac AS hash").
				Where("e.event_id>?", cursor.BootstrapEvent).Order("e.event_id")
		} else {
			query = evidence.Table("nginx_evidence_changes_v1 c INDEXED BY idx_nginx_evidence_changes_v1").
				Select("c.generation,e.event_id,e.oneapi_request_id AS request_id,e.hmac_key_id AS key_id,e.oneapi_id_hmac AS hash").
				Joins("JOIN nginx_request_evidences e ON e.event_id=c.event_id").Where("c.generation>?", cursor.Generation).Order("c.generation,c.event_id")
		}
		if err := query.Limit(32).Scan(&changes).Error; err != nil {
			return true, err
		}
		more = more || len(changes) == 32
		// Empty bootstrap still needs a durable completion checkpoint.
		if bootstrap && len(changes) == 0 {
			next := cursor
			next.BootstrapDone = true
			if err := m.commitTokenEvidenceCursor(ctx, cursor, next, nil, now); err != nil {
				return true, err
			}
			continue
		}
		committed := cursor
		owners := make([]int64, 0, 32)
		flush := func() error {
			if err := m.commitTokenEvidenceCursor(ctx, committed, cursor, owners, now); err != nil {
				return err
			}
			cursor.Version = committed.Version + 1
			committed = cursor
			owners = owners[:0]
			return nil
		}
		for i, change := range changes {
			if m.tokenLocalWriteMu.collectionQueued() || ctx.Err() != nil {
				return true, ctx.Err()
			}
			after := int64(0)
			if bootstrap && cursor.BootstrapPending == change.EventID {
				after = cursor.BootstrapOwner
			} else if !bootstrap && cursor.PendingGeneration == change.Generation {
				after = cursor.OwnerID
			}
			var ids []int64
			err := m.tokenBackgroundRead(ctx, func(reader *gorm.DB) error {
				var err error
				ids, err = tokenEvidenceOwners(reader, cursor.SourceEpoch, change, after)
				return err
			})
			if err != nil {
				return true, err
			}
			available := 32 - len(owners)
			partial := len(ids) > available
			ids = ids[:min(len(ids), available)]
			next := cursor
			if bootstrap {
				if partial {
					next.BootstrapPending, next.BootstrapOwner = change.EventID, ids[len(ids)-1]
				} else {
					next.BootstrapEvent, next.BootstrapPending, next.BootstrapOwner = change.EventID, "", 0
					next.BootstrapDone = i == len(changes)-1 && len(changes) < 32
				}
			} else if partial {
				next.PendingGeneration, next.OwnerID = change.Generation, ids[len(ids)-1]
			} else {
				next.Generation, next.PendingGeneration, next.OwnerID = change.Generation, 0, 0
			}
			cursor = next
			owners = append(owners, ids...)
			// Batch both unmatched evidence progress and up to 32 affected facts.
			// One WAL commit per event otherwise throttles a busy evidence stream.
			if len(owners) == 32 || partial || i == len(changes)-1 {
				if err := flush(); err != nil {
					return true, err
				}
			}
			if partial {
				more = true
				break
			}
		}
	}
	return more, nil
}

func tokenEvidenceOwners(tx *gorm.DB, epoch string, e tokenEvidenceChange, after int64) ([]int64, error) {
	queries := []string{`SELECT log_id FROM token_metering_refs WHERE key_id='event-id' AND hash=? AND source_epoch=? AND log_id>?`,
		`SELECT log_id FROM token_metering_facts WHERE entry_event_id=? AND source_epoch=? AND log_id>?`}
	args := []any{e.EventID, epoch, after, e.EventID, epoch, after}
	if e.RequestID != "" {
		queries = append(queries, `SELECT log_id FROM token_metering_facts WHERE request_id=? AND source_epoch=? AND log_id>?`)
		args = append(args, e.RequestID, epoch, after)
	}
	if e.KeyID != "" && e.Hash != "" {
		queries = append(queries, `SELECT log_id FROM token_metering_refs WHERE key_id=? AND hash=? AND source_epoch=? AND log_id>?`)
		args = append(args, e.KeyID, e.Hash, epoch, after)
	}
	var ids []int64
	for i, query := range queries {
		queries[i] = `SELECT log_id FROM (` + query + ` ORDER BY log_id LIMIT 33)`
	}
	err := tx.Raw(strings.Join(queries, " UNION ")+` ORDER BY log_id LIMIT 33`, args...).Scan(&ids).Error
	return ids, err
}

func (m *Monitor) commitTokenEvidenceCursor(ctx context.Context, old, next tokenEvidenceCursor, ids []int64, now int64) error {
	return m.tokenLocalWriteTransaction(ctx, func(tx *gorm.DB) error {
		result := tx.Table(tokenEvidenceCursorTable).Where("source_epoch=? AND source_id=? AND version=?", old.SourceEpoch, old.SourceID, old.Version).
			Updates(map[string]any{"generation": next.Generation, "pending_generation": next.PendingGeneration, "owner_id": next.OwnerID,
				"bootstrap_event": next.BootstrapEvent, "bootstrap_pending": next.BootstrapPending, "bootstrap_owner": next.BootstrapOwner,
				"bootstrap_done": next.BootstrapDone, "version": old.Version + 1})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected != 1 {
			return errors.New("token evidence cursor changed")
		}
		if len(ids) == 0 {
			return nil
		}
		return tx.Model(&TokenMeteringFact{}).Where("source_epoch=? AND log_id IN ?", old.SourceEpoch, ids).
			Updates(map[string]any{"link_next_at": gorm.Expr("CASE WHEN link_next_at>0 AND link_next_at<=? THEN link_next_at ELSE ? END", now, now),
				"link_attempts": gorm.Expr("CASE WHEN link_next_at=0 THEN 0 ELSE link_attempts END"),
				"link_deadline": gorm.Expr("MAX(COALESCE(link_deadline,0),?)", now+tokenMeteringRepairSeconds)}).Error
	})
}
