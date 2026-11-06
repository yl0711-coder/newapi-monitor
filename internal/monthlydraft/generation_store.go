package monthlydraft

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlytasks"
	"github.com/yl0711-coder/newapi-monitor/monitor"
)

// InitGenerationReceiver is explicit LOCAL setup. No production entrypoint
// calls it. It creates only this feature's task, private draft and publication
// recovery tables, never source or customer wallet tables.
func InitGenerationReceiver(ctx context.Context, db *sql.DB) error {
	if err := monthlytasks.Init(ctx, db); err != nil {
		return err
	}
	if err := generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		for _, query := range []string{
			`CREATE TABLE IF NOT EXISTS portal_monthly_generation_inputs (
id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=64),
plan BLOB NOT NULL CHECK(length(plan)<=2097152),
preparation BLOB NOT NULL CHECK(length(preparation)<=8388608),
artifact BLOB CHECK(length(artifact)<=8388608),artifact_hash TEXT NOT NULL DEFAULT '')`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_no_replace BEFORE UPDATE ON portal_monthly_generation_inputs
WHEN NEW.id!=OLD.id OR NEW.plan!=OLD.plan OR NEW.preparation!=OLD.preparation OR OLD.artifact IS NOT NULL
BEGIN SELECT RAISE(ABORT,'generation inputs and completed drafts are immutable'); END`,
			`CREATE TRIGGER IF NOT EXISTS monthly_generation_no_delete BEFORE DELETE ON portal_monthly_generation_inputs
BEGIN SELECT RAISE(ABORT,'generation retention requires a separate reviewed archive operation'); END`,
		} {
			if _, err := tx.ExecContext(ctx, query); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	return InitAutomaticPublicationReceiver(ctx, db)
}

// Use the v1 task receiver's reservation row. Both packages belong to the same
// explicitly initialized receiver; unknown versions fail, never auto-migrate.
func generationWrite(ctx context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) error {
	err := monthlytasks.WriteReceiver(ctx, db, fn)
	if errors.Is(err, monthlytasks.ErrCorrupt) {
		return ErrGenerationIntegrity
	}
	return err
}

func generationCapacity(ctx context.Context, tx *sql.Tx, adding int, bytesToAdd int64) error {
	var count, size int64
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(length(plan)+length(preparation)+COALESCE(length(artifact),0)),0)
FROM portal_monthly_generation_inputs`).Scan(&count, &size); err != nil {
		return err
	}
	if count+int64(adding) > generationRecordLimit || size+bytesToAdd > generationReceiverLimit {
		return ErrGenerationCapacity
	}
	return nil
}

// Retain inputs in the TASK admission transaction, never a separate commit.
// Existing legacy orphan inputs may be reused only if their exact bytes match;
// an established task with missing inputs must fail closed, not repair itself.
func retainGenerationPlan(ctx context.Context, tx *sql.Tx, plan, preparation []byte, existingTask bool) error {
	var oldPlan, oldInput []byte
	err := tx.QueryRowContext(ctx, `SELECT CASE WHEN length(plan)<=2097152 THEN plan END,
CASE WHEN length(preparation)<=8388608 THEN preparation END FROM portal_monthly_generation_inputs WHERE id=?`, generationHash(plan)).Scan(&oldPlan, &oldInput)
	if err == nil {
		if !bytes.Equal(plan, oldPlan) || !bytes.Equal(preparation, oldInput) {
			return ErrGenerationIntegrity
		}
		return nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return err
	}
	if reference, err := archiveReference(ctx, tx, generationHash(plan)); err != nil {
		return err
	} else if reference != nil {
		return ErrGenerationArchived // Do not reconstruct archived inputs through admission.
	}
	if existingTask {
		return ErrGenerationIntegrity
	}
	if err := generationCapacity(ctx, tx, 1, int64(len(plan)+len(preparation))); err != nil {
		return err
	}
	_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_generation_inputs(id,plan,preparation) VALUES(?,?,?)`, generationHash(plan), plan, preparation)
	return err
}

type generationRetained struct {
	plan     GenerationPlan
	planRaw  []byte
	inputRaw []byte
	input    monitor.MonthlyPreparationInput
	artifact []byte
	digest   string
}

func loadGeneration(parent context.Context, db *sql.DB, request monthlytasks.Request, now time.Time) (generationRetained, error) {
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	var saved generationRetained
	err := db.QueryRowContext(ctx, `SELECT CASE WHEN length(plan)<=2097152 THEN plan END,
CASE WHEN length(preparation)<=8388608 THEN preparation END,CASE WHEN length(artifact)<=8388608 THEN artifact END,artifact_hash
FROM portal_monthly_generation_inputs WHERE id=?`, request.InputSHA256).Scan(&saved.planRaw, &saved.inputRaw, &saved.artifact, &saved.digest)
	if errors.Is(err, sql.ErrNoRows) {
		if reference, refErr := archiveReference(ctx, db, request.InputSHA256); refErr != nil {
			return saved, refErr
		} else if reference != nil {
			return saved, ErrGenerationArchived
		}
	}
	if err != nil {
		return saved, err
	}
	if generationHash(saved.planRaw) != request.InputSHA256 || json.Unmarshal(saved.planRaw, &saved.plan) != nil {
		return saved, ErrGenerationIntegrity
	}
	p, canonical, input, err := canonicalGeneration(saved.plan, saved.inputRaw, now)
	if err != nil || !bytes.Equal(canonical, saved.planRaw) || generationRequest(p, canonical) != request ||
		(len(saved.artifact) == 0) != (saved.digest == "") || (len(saved.artifact) > 0 && generationHash(saved.artifact) != saved.digest) {
		return saved, ErrGenerationIntegrity
	}
	saved.input = input
	return saved, nil
}

func saveGenerationArtifact(ctx context.Context, db *sql.DB, id string, raw []byte) error {
	if len(raw) == 0 || len(raw) > generationArtifactLimit {
		return ErrGenerationCapacity
	}
	return generationWrite(ctx, db, func(ctx context.Context, tx *sql.Tx) error {
		var existing []byte
		var digest string
		if err := tx.QueryRowContext(ctx, `SELECT CASE WHEN length(artifact)<=8388608 THEN artifact END,artifact_hash
FROM portal_monthly_generation_inputs WHERE id=?`, id).Scan(&existing, &digest); err != nil {
			return err
		}
		if existing != nil || digest != "" {
			if !bytes.Equal(existing, raw) || digest != generationHash(raw) {
				return ErrGenerationIntegrity
			}
			return nil
		}
		if err := generationCapacity(ctx, tx, 0, int64(len(raw))); err != nil {
			return err
		}
		result, err := tx.ExecContext(ctx, `UPDATE portal_monthly_generation_inputs SET artifact=?,artifact_hash=? WHERE id=? AND artifact IS NULL`, raw, generationHash(raw), id)
		if err != nil {
			return err
		}
		n, err := result.RowsAffected()
		if err != nil || n != 1 {
			return ErrGenerationIntegrity
		}
		return nil
	})
}
