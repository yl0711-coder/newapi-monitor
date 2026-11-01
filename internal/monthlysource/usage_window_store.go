package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// InitUsageWindowStore is explicit setup of a separate SQLite observation
// receiver. No application entrypoint calls it; it never changes source or
// customer bill tables. Retention/eviction require a separate reviewed workflow.
func InitUsageWindowStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return errors.New("usage observation receiver is required")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return err // Never attempt schema writes on a MySQL source.
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_usage_observations (
id TEXT PRIMARY KEY NOT NULL,source_identity TEXT NOT NULL,user_id INTEGER NOT NULL,
window_from INTEGER NOT NULL,window_to INTEGER NOT NULL,started_at_ms INTEGER NOT NULL,
completed_at_ms INTEGER NOT NULL,payload BLOB NOT NULL CHECK(length(payload)<=32768))`,
		`CREATE INDEX IF NOT EXISTS idx_monthly_usage_scope ON portal_monthly_usage_observations(source_identity,user_id,window_from,window_to,started_at_ms,id)`,
		`CREATE TRIGGER IF NOT EXISTS monthly_usage_no_update BEFORE UPDATE ON portal_monthly_usage_observations BEGIN SELECT RAISE(ABORT,'usage observations are append-only'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_usage_no_delete BEFORE DELETE ON portal_monthly_usage_observations BEGIN SELECT RAISE(ABORT,'usage observations are append-only'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SaveUsageWindowObservation retains only a completed capture, after its
// source transaction ended. Identical bytes are idempotent; a later observation
// gets a new digest instead of overwriting history. No implicit initialization.
func SaveUsageWindowObservation(parent context.Context, db *sql.DB, o UsageWindowObservation) (string, error) {
	if parent == nil || db == nil {
		return "", errors.New("usage observation receiver is required")
	}
	raw, err := usageWindowBytes(o)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	id := digest(raw)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_usage_observations
(id,source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?) ON CONFLICT(id) DO NOTHING`,
		id, o.Request.SourceIdentity, o.Request.UserID, o.Request.From, o.Request.To, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
		return "", err
	}
	stored, err := loadUsageWindow(ctx, tx, id)
	if err != nil || !bytes.Equal(raw, stored) {
		return "", errors.Join(errors.New("existing usage observation differs; not overwritten"), err)
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

func loadUsageWindow(ctx context.Context, db walletRowReader, id string) ([]byte, error) {
	if !validDigest(id) {
		return nil, errors.New("invalid usage observation ID")
	}
	var source string
	var user, from, to, started, completed int64
	var raw []byte
	if err := db.QueryRowContext(ctx, `SELECT source_identity,user_id,window_from,window_to,started_at_ms,completed_at_ms,payload
FROM portal_monthly_usage_observations WHERE id=? AND length(payload)<=32768`, id).Scan(&source, &user, &from, &to, &started, &completed, &raw); err != nil {
		return nil, err
	}
	var o UsageWindowObservation
	if digest(raw) != id || json.Unmarshal(raw, &o) != nil || source != o.Request.SourceIdentity || user != o.Request.UserID ||
		from != o.Request.From || to != o.Request.To || started != o.StartedAtMS || completed != o.CompletedAtMS {
		return nil, errors.New("usage observation integrity check failed")
	}
	canonical, err := usageWindowBytes(o)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.New("usage observation is not a valid complete canonical record")
	}
	return raw, nil
}

// LoadUsageWindowObservation validates the stored digest, canonical payload and
// indexed identity. Success is observation integrity, never bill approval.
func LoadUsageWindowObservation(parent context.Context, db *sql.DB, id string) (UsageWindowObservation, error) {
	var empty UsageWindowObservation
	if parent == nil || db == nil {
		return empty, errors.New("usage observation receiver is required")
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	raw, err := loadUsageWindow(ctx, db, id)
	if err != nil {
		return empty, err
	}
	var o UsageWindowObservation
	if err := json.Unmarshal(raw, &o); err != nil {
		return empty, err
	}
	return o, nil
}
