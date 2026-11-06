package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

const registeredWalletPayloadLimit = 65536

// Explicit PRIVATE receiver setup only. No caller on ordinary Monitor/Usage
// startup, no change to legacy wallet rows and no source-DB schema operation.
// Storage integrity does not establish a historical balance or approve a bill.
func InitRegisteredWalletObservationStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return errors.New("registered wallet receiver and context are required")
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	if err := registeredWalletSQLite(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_registered_wallet_observations (
		 id TEXT PRIMARY KEY NOT NULL, source_identity TEXT NOT NULL,
		 started_at_ms INTEGER NOT NULL, completed_at_ms INTEGER NOT NULL,
		 payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=65536))`,
		`CREATE INDEX IF NOT EXISTS idx_monthly_registered_wallet_source_time ON portal_monthly_registered_wallet_observations(source_identity,started_at_ms,id)`,
		`CREATE TRIGGER IF NOT EXISTS monthly_registered_wallet_no_update BEFORE UPDATE ON portal_monthly_registered_wallet_observations BEGIN SELECT RAISE(ABORT,'registered wallet observations are append-only'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_registered_wallet_no_delete BEFORE DELETE ON portal_monthly_registered_wallet_observations BEGIN SELECT RAISE(ABORT,'registered wallet observations are append-only'); END`,
		// REPLACE can bypass DELETE triggers with recursive_triggers=OFF.
		// Reject a changed duplicate before the conflicting row can be removed.
		`CREATE TRIGGER IF NOT EXISTS monthly_registered_wallet_no_replace BEFORE INSERT ON portal_monthly_registered_wallet_observations
		 WHEN EXISTS(SELECT 1 FROM portal_monthly_registered_wallet_observations WHERE id=NEW.id AND
		 (source_identity IS NOT NEW.source_identity OR started_at_ms IS NOT NEW.started_at_ms OR completed_at_ms IS NOT NEW.completed_at_ms OR payload IS NOT NEW.payload))
		 BEGIN SELECT RAISE(ABORT,'registered wallet observation cannot be replaced'); END`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func registeredWalletSQLite(ctx context.Context, db *sql.DB) error {
	var version string
	// Check before ANY writer transaction, even if a source handle was passed
	// accidentally. On MySQL this SELECT fails; no source DDL/DML is attempted.
	return db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version)
}

func registeredWalletBytes(o RegisteredWalletObservation) ([]byte, error) {
	if err := ValidateRegisteredWalletObservation(o); err != nil {
		return nil, err
	}
	raw, err := json.Marshal(o)
	if err != nil || len(raw) > registeredWalletPayloadLimit {
		return nil, errors.New("registered wallet observation exceeds its canonical storage budget")
	}
	return raw, nil
}

// Save stores the complete original metadata and wallet in ONE local row, only
// after source capture has ended. No implicit setup, retry, pruning or overwrite.
// A failed/uncertain commit can be retried with the SAME original observation;
// its content address remains the same, without another source capture.
func SaveRegisteredWalletObservation(parent context.Context, db *sql.DB, o RegisteredWalletObservation) (string, error) {
	if parent == nil || db == nil {
		return "", errors.New("registered wallet receiver and context are required")
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	raw, err := registeredWalletBytes(o)
	if err != nil {
		return "", err
	}
	if err := registeredWalletSQLite(ctx, db); err != nil {
		return "", err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	id, err := insertRegisteredWalletObservation(ctx, tx, o, raw)
	if err != nil {
		return "", err
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

// Shared row insertion for the capture checkpoint's SAME local transaction.
// No nested transaction or split commit of original and job reference.
func insertRegisteredWalletObservation(ctx context.Context, tx *sql.Tx, o RegisteredWalletObservation, raw []byte) (string, error) {
	id := digest(raw)
	if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_registered_wallet_observations(id,source_identity,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, id, o.SourceIdentity, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
		return "", err
	}
	stored, err := loadRegisteredWalletBytes(ctx, tx, id)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(raw, stored) {
		return "", errors.New("existing registered wallet source differs; not overwritten")
	}
	return id, nil
}

func loadRegisteredWalletBytes(ctx context.Context, db walletRowReader, id string) ([]byte, error) {
	var source string
	var started, completed int64
	var raw []byte
	if !validDigest(id) {
		return nil, errors.New("invalid registered wallet observation ID")
	}
	if err := db.QueryRowContext(ctx, `SELECT source_identity,started_at_ms,completed_at_ms,payload FROM portal_monthly_registered_wallet_observations WHERE id=? AND typeof(payload)='blob' AND length(payload)<=65536`, id).Scan(&source, &started, &completed, &raw); err != nil {
		return nil, err
	}
	var o RegisteredWalletObservation
	if digest(raw) != id || json.Unmarshal(raw, &o) != nil || o.SourceIdentity != source || o.StartedAtMS != started || o.CompletedAtMS != completed {
		return nil, errors.New("registered wallet source identity, bytes or timestamps differ")
	}
	canonical, err := registeredWalletBytes(o)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.New("registered wallet source is not its complete canonical original")
	}
	return raw, nil
}

// Load requires the expected source AND complete account roster in addition
// to the content address. A valid record for another source/subset is not an
// acceptable replacement. Neither reading nor validating publishes anything.
func LoadRegisteredWalletObservation(parent context.Context, db *sql.DB, id, source string, ids []int64) (RegisteredWalletObservation, error) {
	var empty RegisteredWalletObservation
	ordered, err := walletIDs(ids)
	if parent == nil || db == nil || err != nil || !validDigest(id) || !validDigest(source) {
		return empty, errors.New("invalid registered wallet read identity or scope")
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := registeredWalletSQLite(ctx, db); err != nil {
		return empty, err
	}
	raw, err := loadRegisteredWalletBytes(ctx, db, id)
	if err != nil {
		return empty, err
	}
	var o RegisteredWalletObservation
	if json.Unmarshal(raw, &o) != nil || o.SourceIdentity != source || len(o.Users) != len(ordered) {
		return empty, errors.New("registered wallet source belongs to another source or roster")
	}
	for i, user := range o.Users {
		if user.UserID != ordered[i] {
			return empty, errors.New("registered wallet source account roster changed")
		}
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return o, nil
}
