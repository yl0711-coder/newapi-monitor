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
	"gorm.io/gorm"
)

// The SQL barrier cannot audit a separate main DB. It enforces the immutable
// archive/journal/task binding and a transaction-local permit AFTER Go audits
// the customer publication and original sources. No permit survives commit.
const publishedGenerationDeleteGuard = `CREATE TRIGGER monthly_generation_no_delete BEFORE DELETE ON portal_monthly_generation_inputs
WHEN (SELECT COUNT(*) FROM portal_monthly_draft_tasks WHERE json_extract(CAST(descriptor AS TEXT),'$.input_sha256')=OLD.id)!=1
OR NOT EXISTS(SELECT 1 FROM portal_monthly_published_release_permits p
JOIN portal_monthly_generation_archives a ON a.id=p.id
JOIN portal_monthly_draft_tasks t ON t.id=p.task_id
JOIN portal_monthly_publication_jobs j ON j.task_id=t.id
WHERE p.id=OLD.id AND json_extract(CAST(a.receipt AS TEXT),'$.manifest_sha256')=p.manifest_sha256
AND json_extract(CAST(t.descriptor AS TEXT),'$.input_sha256')=OLD.id
AND t.state='draft_ready' AND t.result_sha256=OLD.artifact_hash AND t.result_sha256=p.result_hash
AND j.state='saved' AND j.task_key=t.task_key AND j.result_hash=t.result_sha256)
BEGIN SELECT RAISE(ABORT,'verified saved publication and exact archive permit required'); END`

// InitPublishedGenerationRetention is an explicit LOCAL opt-in only. Older
// maintenance initializers reject this guard rather than downgrade it. Normal
// default startup never calls it. Explicit maintenance setup may opt in, but
// no customer endpoint can grant a release permit.
func InitPublishedGenerationRetention(ctx context.Context, db *sql.DB) error {
	return generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		var guard string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='trigger' AND name='monthly_generation_no_delete'`).Scan(&guard); err != nil {
			return err
		}
		guard = strings.ReplaceAll(guard, "IF NOT EXISTS ", "")
		if guard != originalGenerationDeleteGuard && guard != archivedGenerationDeleteGuard && guard != completedGenerationDeleteGuard && guard != publishedGenerationDeleteGuard {
			return ErrGenerationIntegrity
		}
		if err := publicationVersion(ctx, tx); err != nil {
			return err
		}
		for _, q := range []string{
			`CREATE TABLE IF NOT EXISTS portal_monthly_generation_archives(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=64),version INTEGER NOT NULL CHECK(version=1),receipt BLOB NOT NULL CHECK(length(CAST(receipt AS BLOB))<=4096))`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_archives_no_update BEFORE UPDATE ON portal_monthly_generation_archives BEGIN SELECT RAISE(ABORT,'archive references are immutable'); END`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_archives_no_delete BEFORE DELETE ON portal_monthly_generation_archives BEGIN SELECT RAISE(ABORT,'archive references preserve history'); END`,
			`CREATE TABLE IF NOT EXISTS portal_monthly_published_release_permits(id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=64),task_id TEXT NOT NULL CHECK(length(task_id)=32),result_hash TEXT NOT NULL CHECK(length(result_hash)=64),manifest_sha256 TEXT NOT NULL CHECK(length(manifest_sha256)=64))`,
			`DROP TRIGGER monthly_generation_no_delete`,
			publishedGenerationDeleteGuard,
		} {
			if _, err := tx.ExecContext(ctx, q); err != nil {
				return err
			}
		}
		return nil
	})
}

func publishedArchiveManifest(ctx context.Context, receipt ArchiveReceipt) (archiveManifest, error) {
	var m archiveManifest
	root, err := openArchiveRoot(receipt.Directory)
	if err != nil {
		return m, err
	}
	defer root.Close()
	raw, err := readArchiveFile(ctx, root, "manifest.json", archiveManifestLimit)
	if err != nil {
		return m, err
	}
	if generationHash(raw) != receipt.ManifestSHA256 || json.Unmarshal(raw, &m) != nil || m.Version != 2 || m.Mode != publishedArchiveMode || m.Published == nil || len(m.Entries) != 1 {
		return archiveManifest{}, ErrGenerationIntegrity
	}
	return m, nil
}

// ChangePublishedGenerationPayload affects ONE receiver input, never the main
// bill or original source evidence. Restore rehydrates exact bytes; it neither
// activates tasks nor resets attempts, budgets or timestamps. Even the latest
// draft is eligible ONLY while its exact customer publication is still saved.
// Main/receiver are separate databases: a later withdrawal cannot be made
// atomic with this transaction, so the verified cold copy is always retained.
func (g *Generation) ChangePublishedGenerationPayload(parent context.Context, main *gorm.DB, receipt ArchiveReceipt, owner monthlytasks.Owner, taskID string, restore bool) error {
	ctx, cancel := context.WithTimeout(parent, 2*time.Minute)
	defer cancel()
	want, entry, binding, err := g.auditPublishedArchive(ctx, main, owner, taskID)
	if err != nil {
		return err
	}
	select {
	case archiveSlots <- struct{}{}:
		defer func() { <-archiveSlots }()
	default:
		return ErrGenerationBusy
	}
	id := entry.Item.InputSHA256
	saved, archived, err := readEvictionArchive(ctx, receipt, id)
	if err != nil {
		return err
	}
	m, err := publishedArchiveManifest(ctx, receipt)
	if err != nil {
		return err
	}
	if *m.Published != binding || !reflect.DeepEqual(archived, entry) || !bytes.Equal(saved.planRaw, want.planRaw) || !bytes.Equal(saved.inputRaw, want.inputRaw) || !bytes.Equal(saved.artifact, want.artifact) || saved.digest != want.digest {
		return ErrGenerationChanged
	}
	rawReceipt, err := json.Marshal(receipt)
	if err != nil || len(rawReceipt) > 4096 {
		return ErrGenerationInput
	}
	record, err := g.readPublishedRecord(ctx, main, entry.Tasks[0].Task.Request)
	if err != nil || publishedRecordHash(record) != binding.PublicationSHA256 {
		return errors.Join(ErrGenerationChanged, err)
	}
	if err := g.current(ctx, entry.Tasks[0].Task.Request); err != nil {
		return err
	}
	return generationWrite(ctx, g.receiver, func(ctx context.Context, tx *sql.Tx) error {
		var guard string
		if err := tx.QueryRowContext(ctx, `SELECT sql FROM sqlite_master WHERE type='trigger' AND name='monthly_generation_no_delete'`).Scan(&guard); err != nil {
			return err
		}
		if guard != publishedGenerationDeleteGuard {
			return ErrGenerationInput
		}
		var count int
		if err := tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM portal_monthly_published_release_permits`).Scan(&count); err != nil {
			return err
		}
		if count != 0 {
			return ErrGenerationIntegrity
		}
		current, err := inspectPublishedTask(ctx, tx, entry.Tasks[0].Task.Request, &binding)
		if err != nil {
			return err
		}
		if !reflect.DeepEqual(current, entry.Tasks) {
			return ErrGenerationChanged
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
		err = tx.QueryRowContext(ctx, `SELECT CASE WHEN length(CAST(plan AS BLOB))<=2097152 THEN plan END,CASE WHEN length(CAST(preparation AS BLOB))<=8388608 THEN preparation END,CASE WHEN length(CAST(artifact AS BLOB))<=8388608 THEN artifact END,artifact_hash FROM portal_monthly_generation_inputs WHERE id=?`, id).Scan(&p, &input, &artifact, &digest)
		missing := errors.Is(err, sql.ErrNoRows)
		if err != nil && !missing {
			return err
		}
		if !missing && (!bytes.Equal(p, saved.planRaw) || !bytes.Equal(input, saved.inputRaw) || !bytes.Equal(artifact, saved.artifact) || digest != saved.digest) {
			return ErrGenerationIntegrity
		}
		if restore {
			if reference == nil {
				return ErrGenerationInput
			}
			if !missing {
				return nil
			}
			if err := generationCapacity(ctx, tx, 1, entry.Item.PayloadBytes); err != nil {
				return err
			}
			_, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_inputs(id,plan,preparation,artifact,artifact_hash) VALUES(?,?,?,?,?)`, id, saved.planRaw, saved.inputRaw, saved.artifact, saved.digest)
			return err
		}
		if missing {
			if reference == nil {
				return ErrGenerationIntegrity
			}
			return nil
		}
		if reference == nil {
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
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_published_release_permits(id,task_id,result_hash,manifest_sha256) VALUES(?,?,?,?)`, id, binding.TaskID, binding.ResultSHA256, receipt.ManifestSHA256); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `DELETE FROM portal_monthly_generation_inputs WHERE id=?`, id)
		if err != nil {
			return err
		}
		if n, err := result.RowsAffected(); err != nil || n != 1 {
			return ErrGenerationIntegrity
		}
		_, err = tx.ExecContext(ctx, `DELETE FROM portal_monthly_published_release_permits WHERE id=?`, id)
		return err
	})
}
