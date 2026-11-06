package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"reflect"
	"strings"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
)

var ErrGenerationArchived = errors.New("generation payload archived; explicit verified restoration required")

const originalGenerationDeleteGuard = `CREATE TRIGGER monthly_generation_no_delete BEFORE DELETE ON portal_monthly_generation_inputs
BEGIN SELECT RAISE(ABORT,'generation retention requires a separate reviewed archive operation'); END`

const archivedGenerationDeleteGuard = `CREATE TRIGGER monthly_generation_no_delete BEFORE DELETE ON portal_monthly_generation_inputs
WHEN NOT EXISTS(SELECT 1 FROM portal_monthly_generation_archives WHERE id=OLD.id)
OR NOT EXISTS(SELECT 1 FROM portal_monthly_generation_release_permits WHERE id=OLD.id)
OR NOT EXISTS(SELECT 1 FROM portal_monthly_draft_tasks WHERE json_extract(CAST(descriptor AS TEXT),'$.input_sha256')=OLD.id)
OR EXISTS(SELECT 1 FROM portal_monthly_draft_tasks WHERE json_extract(CAST(descriptor AS TEXT),'$.input_sha256')=OLD.id AND NOT(state='invalidated' OR (state='failed' AND attempts=3)))
BEGIN SELECT RAISE(ABORT,'verified archive and terminal task barriers required'); END`

// InitGenerationRetention explicitly opts a LOCAL receiver into v1 retention.
// Normal InitGenerationReceiver does NOT enable deletion. Unknown delete guards
// are rejected, not silently replaced. No production startup calls this method.
func InitGenerationRetention(ctx context.Context, db *sql.DB) error {
	return initGenerationRetention(ctx, db, false)
}

func initGenerationRetention(ctx context.Context, db *sql.DB, completed bool) error {
	return generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		var guard string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='trigger' AND name='monthly_generation_no_delete'`).Scan(&guard); err != nil {
			return err
		}
		guard = strings.ReplaceAll(guard, "IF NOT EXISTS ", "")
		if guard != originalGenerationDeleteGuard && guard != archivedGenerationDeleteGuard && guard != completedGenerationDeleteGuard {
			return ErrGenerationIntegrity
		}
		target := archivedGenerationDeleteGuard
		if completed || guard == completedGenerationDeleteGuard {
			target = completedGenerationDeleteGuard // Never silently downgrade an explicit opt-in.
		}
		for _, q := range []string{
			`CREATE TABLE IF NOT EXISTS portal_monthly_generation_archives(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=64),version INTEGER NOT NULL CHECK(version=1),receipt BLOB NOT NULL CHECK(length(CAST(receipt AS BLOB))<=4096))`,
			`CREATE TABLE IF NOT EXISTS portal_monthly_generation_release_permits(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=64))`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_archives_no_update BEFORE UPDATE ON portal_monthly_generation_archives BEGIN SELECT RAISE(ABORT,'archive references are immutable'); END`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_archives_no_delete BEFORE DELETE ON portal_monthly_generation_archives BEGIN SELECT RAISE(ABORT,'archive references preserve history'); END`,
			`DROP TRIGGER monthly_generation_no_delete`,
			target,
		} {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
}

type archiveQuery interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func archiveReference(ctx context.Context, q archiveQuery, id string) ([]byte, error) {
	var exists int
	if err := q.QueryRowContext(ctx, `SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='portal_monthly_generation_archives'`).Scan(&exists); err != nil {
		return nil, err
	}
	if exists == 0 {
		return nil, nil // Older receivers have never opted in.
	}
	var raw []byte
	var version int
	err := q.QueryRowContext(ctx, `SELECT version,CASE WHEN length(CAST(receipt AS BLOB))<=4096 THEN receipt END FROM portal_monthly_generation_archives WHERE id=?`, id).Scan(&version, &raw)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var receipt ArchiveReceipt
	if version != 1 || json.Unmarshal(raw, &receipt) != nil || !generationHex(receipt.ManifestSHA256, 32) ||
		receipt.InputCount < 1 || receipt.InputCount > generationRecordLimit || receipt.PayloadBytes <= 0 || receipt.PayloadBytes > generationReceiverLimit {
		return nil, ErrGenerationIntegrity
	}
	canonical, _ := json.Marshal(receipt)
	if !bytes.Equal(canonical, raw) {
		return nil, ErrGenerationIntegrity
	}
	return raw, nil
}

// readEvictionArchive performs all filesystem work before acquiring a SQLite
// writer. Its bounded payload is compared against the receiver inside the TX.
func readEvictionArchive(ctx context.Context, receipt ArchiveReceipt, id string) (generationRetained, archiveEntry, error) {
	var s generationRetained
	var entry archiveEntry
	if !generationHex(id, 32) {
		return s, entry, ErrGenerationInput
	}
	root, err := openArchiveRoot(receipt.Directory)
	if err != nil {
		return s, entry, err
	}
	defer root.Close()
	verified, err := verifyArchive(ctx, root, "manifest.json", receipt.ManifestSHA256)
	if err != nil {
		return s, entry, err
	}
	if verified.InputCount != receipt.InputCount || verified.PayloadBytes != receipt.PayloadBytes {
		return s, entry, ErrGenerationIntegrity
	}
	raw, err := readArchiveFile(ctx, root, "manifest.json", archiveManifestLimit)
	if err != nil {
		return s, entry, err
	}
	var m archiveManifest
	if generationHash(raw) != receipt.ManifestSHA256 || json.Unmarshal(raw, &m) != nil {
		return s, entry, ErrGenerationIntegrity
	}
	for _, e := range m.Entries {
		if e.Item.InputSHA256 == id {
			entry = e
		}
	}
	if entry.Item.InputSHA256 != id {
		return s, entry, ErrGenerationInput
	}
	s.digest = entry.ArtifactSHA256
	if s.planRaw, err = readArchiveFile(ctx, root, id+".plan", generationPlanLimit); err != nil {
		return s, entry, err
	}
	if s.inputRaw, err = readArchiveFile(ctx, root, id+".preparation", 8<<20); err != nil {
		return s, entry, err
	}
	if s.digest != "" {
		if s.artifact, err = readArchiveFile(ctx, root, id+".artifact", generationArtifactLimit); err != nil {
			return s, entry, err
		}
	}
	if _, err := inspectRetainedInput(id, &s, time.UnixMilli(m.AsOfMS)); err != nil {
		return s, entry, err
	}
	if generationHash(s.inputRaw) != entry.PreparationSHA256 {
		return s, entry, ErrGenerationIntegrity
	}
	return s, entry, nil
}

// ChangeArchivedGenerationPayload is a LOCAL maintenance operation on ONE
// input. restore=false releases payload capacity; restore=true rehydrates exact
// bytes only. Both preserve ALL task metadata, invalidation, and archive refs.
// DraftReady (even superseded), active and retryable tasks are never released.
// No source/publication access, money calculation, VACUUM, or task activation.
func ChangeArchivedGenerationPayload(parent context.Context, db *sql.DB, receipt ArchiveReceipt, id string, asOf time.Time, restore bool) error {
	return changeArchivedGenerationPayload(parent, db, receipt, id, asOf, restore, false)
}

func changeArchivedGenerationPayload(parent context.Context, db *sql.DB, receipt ArchiveReceipt, id string, asOf time.Time, restore, completed bool) error {
	if db == nil || asOf.UnixMilli() <= 0 {
		return ErrGenerationInput
	}
	select {
	case archiveSlots <- struct{}{}:
		defer func() { <-archiveSlots }()
	default:
		return ErrGenerationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
	defer cancel()
	saved, entry, err := readEvictionArchive(ctx, receipt, id)
	if err != nil {
		return err
	}
	rawReceipt, err := json.Marshal(receipt)
	if err != nil || len(rawReceipt) > 4096 {
		return ErrGenerationInput
	}
	return generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		var guard string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE name='monthly_generation_no_delete' AND type='trigger'`).Scan(&guard); err != nil {
			return err
		}
		if (guard != archivedGenerationDeleteGuard && guard != completedGenerationDeleteGuard) || (completed && guard != completedGenerationDeleteGuard) {
			return ErrGenerationInput // Explicit opt-in required, not an implicit migration.
		}
		var permits int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_generation_release_permits`).Scan(&permits); err != nil {
			return err
		}
		if permits != 0 {
			return ErrGenerationIntegrity // Permits must never survive a committed operation.
		}
		all, err := monthlytasks.InspectReceiverTasks(ctx, tx)
		if err != nil {
			return err
		}
		var refs []monthlytasks.TaskObservation
		for _, t := range all {
			if t.Task.Request.InputSHA256 == id {
				if t.Task.State != monthlytasks.Invalidated && !(t.Task.State == monthlytasks.Failed && t.Task.Attempts == 3) && !(completed && t.Task.State == monthlytasks.DraftReady) {
					return ErrGenerationInput
				}
				refs = append(refs, t)
			}
		}
		if !reflect.DeepEqual(refs, entry.Tasks) { // Changed state, binding or missing task: stop.
			return ErrGenerationChanged
		}
		for _, t := range refs {
			if t.Task.UpdatedAtMS > asOf.UnixMilli() || asOf.UnixMilli()-t.Task.UpdatedAtMS < retentionReviewAge.Milliseconds() {
				return ErrGenerationInput
			}
		}
		reference, err := archiveReference(ctx, tx, id)
		if err != nil {
			return err
		}
		if reference != nil && !bytes.Equal(reference, rawReceipt) {
			return ErrGenerationChanged
		}
		var p, input, artifact []byte
		var digest string
		var artifactPresent bool
		err = tx.QueryRowContext(ctx, `SELECT CASE WHEN length(CAST(plan AS BLOB))<=2097152 THEN plan END,CASE WHEN length(CAST(preparation AS BLOB))<=8388608 THEN preparation END,CASE WHEN length(CAST(artifact AS BLOB))<=8388608 THEN artifact END,artifact_hash,artifact IS NOT NULL FROM portal_monthly_generation_inputs WHERE id=?`, id).Scan(&p, &input, &artifact, &digest, &artifactPresent)
		missing := errors.Is(err, sql.ErrNoRows)
		if err != nil && !missing {
			return err
		}
		if !missing && (!bytes.Equal(p, saved.planRaw) || !bytes.Equal(input, saved.inputRaw) || !bytes.Equal(artifact, saved.artifact) || digest != saved.digest || artifactPresent != (saved.artifact != nil)) {
			return ErrGenerationIntegrity
		}
		if restore {
			if reference == nil {
				return ErrGenerationInput // Never insert a foreign archived input.
			}
			if !missing {
				return nil
			}
			if err := generationCapacity(ctx, tx, 1, int64(len(saved.planRaw)+len(saved.inputRaw)+len(saved.artifact))); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_inputs(id,plan,preparation,artifact,artifact_hash) VALUES(?,?,?,?,?)`, id, saved.planRaw, saved.inputRaw, saved.artifact, saved.digest)
			return err
		}
		if missing {
			if reference == nil {
				return ErrGenerationIntegrity // Missing isn't evidence that eviction succeeded.
			}
			return nil
		}
		if completed {
			// Recompute retention under the SAME writer reservation. An old
			// archive receipt must not unpin the last intact hot draft after a
			// newer draft was revoked, corrupted or removed in the meantime.
			report, err := inspectRetentionSnapshot(ctx, tx, asOf)
			if err != nil {
				return err
			}
			eligible := false
			for _, item := range report.Entries {
				if item.InputSHA256 == id {
					eligible = item.Disposition == "archive_copy_candidate"
				}
			}
			if !eligible {
				return ErrGenerationInput
			}
		}
		if reference == nil {
			var count int
			if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_generation_archives`).Scan(&count); err != nil {
				return err
			}
			if count >= 256 {
				return ErrGenerationCapacity
			}
			if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_archives(id,version,receipt) VALUES(?,1,?)`, id, rawReceipt); err != nil {
				return err
			}
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_release_permits(id) VALUES(?)`, id); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM portal_monthly_generation_inputs WHERE id=?`, id)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			return ErrGenerationIntegrity
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM portal_monthly_generation_release_permits WHERE id=?`, id)
		return err
	})
}
