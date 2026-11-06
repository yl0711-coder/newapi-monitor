package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// InitWalletObservationStore is explicit setup for the receiver SQLite only.
// Nothing invokes it on Monitor startup. Observation storage is separate from
// usage_user_snapshots and customer publications. No retention/deletion here.
func InitWalletObservationStore(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("wallet observation store is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return err // do not attempt schema writes on a MySQL source
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, statement := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_wallet_observations (
		 id TEXT PRIMARY KEY NOT NULL, source_identity TEXT NOT NULL,
		 started_at_ms INTEGER NOT NULL, completed_at_ms INTEGER NOT NULL,
		 payload BLOB NOT NULL CHECK(length(payload) <= 65536))`,
		`CREATE INDEX IF NOT EXISTS idx_monthly_wallet_source_time ON portal_monthly_wallet_observations(source_identity,started_at_ms,id)`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_no_update BEFORE UPDATE ON portal_monthly_wallet_observations BEGIN SELECT RAISE(ABORT,'wallet observations are append-only'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_no_delete BEFORE DELETE ON portal_monthly_wallet_observations BEGIN SELECT RAISE(ABORT,'wallet observations are append-only'); END`,
	} {
		if _, err := tx.ExecContext(ctx, statement); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SaveWalletObservation never overwrites an existing observation. Repeating
// the same completed capture is idempotent; different times/values get new IDs.
// The source transaction has already ended before the local write begins.
func SaveWalletObservation(ctx context.Context, db *sql.DB, observation WalletObservation) (string, error) {
	if db == nil {
		return "", errors.New("wallet observation store is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, err := walletObservationBytes(observation)
	if err != nil {
		return "", err
	}
	if len(raw) > 65536 {
		return "", errors.New("wallet observation exceeds storage limit")
	}
	id := digest(raw)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	_, err = tx.ExecContext(ctx, `INSERT INTO portal_monthly_wallet_observations(id,source_identity,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, id, observation.SourceIdentity, observation.StartedAtMS, observation.CompletedAtMS, raw)
	if err != nil {
		return "", err
	}
	stored, err := loadWalletObservation(ctx, tx, id)
	if err != nil {
		return "", err
	}
	if !bytes.Equal(stored, raw) {
		return "", errors.New("existing wallet observation differs; not overwritten")
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

type walletRowReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func loadWalletObservation(ctx context.Context, db walletRowReader, id string) ([]byte, error) {
	if !validDigest(id) {
		return nil, errors.New("invalid wallet observation ID")
	}
	var source string
	var started, completed int64
	var raw []byte
	// The stored length is checked before returning a potentially damaged blob.
	err := db.QueryRowContext(ctx, `SELECT source_identity,started_at_ms,completed_at_ms,payload FROM portal_monthly_wallet_observations WHERE id=? AND length(payload)<=65536`, id).Scan(&source, &started, &completed, &raw)
	if err != nil {
		return nil, err
	}
	var observation WalletObservation
	if digest(raw) != id || json.Unmarshal(raw, &observation) != nil || ValidateWalletObservation(observation) != nil || observation.SourceIdentity != source || observation.StartedAtMS != started || observation.CompletedAtMS != completed {
		return nil, errors.New("wallet observation integrity check failed")
	}
	canonical, err := walletObservationBytes(observation)
	if err != nil || !bytes.Equal(raw, canonical) {
		return nil, errors.New("wallet observation is not the complete canonical record")
	}
	return raw, nil
}

func LoadWalletObservation(ctx context.Context, db *sql.DB, id string) (WalletObservation, error) {
	var observation WalletObservation
	if db == nil {
		return observation, errors.New("wallet observation store is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	raw, err := loadWalletObservation(ctx, db, id)
	if err != nil {
		return observation, err
	}
	err = json.Unmarshal(raw, &observation)
	return observation, err
}
