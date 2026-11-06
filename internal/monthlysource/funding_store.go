package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

// Explicit receiver-only setup; not called on startup. Pages contain private
// source audit fields and must never be served through customer endpoints.
func InitFundingPageStore(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return errors.New("funding receiver is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var version string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&version); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_funding_pages (
id TEXT PRIMARY KEY NOT NULL, scan_id TEXT NOT NULL, sequence INTEGER NOT NULL CHECK(sequence>0),
payload BLOB NOT NULL CHECK(length(payload)<=2097152), UNIQUE(scan_id,sequence))`,
		`CREATE TABLE IF NOT EXISTS portal_monthly_funding_records (
scan_id TEXT NOT NULL, log_id INTEGER NOT NULL, page_id TEXT NOT NULL, row_digest TEXT NOT NULL,
PRIMARY KEY(scan_id,log_id))`,
		`CREATE TRIGGER IF NOT EXISTS monthly_funding_pages_no_update BEFORE UPDATE ON portal_monthly_funding_pages BEGIN SELECT RAISE(ABORT,'funding pages are append-only'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_funding_pages_no_delete BEFORE DELETE ON portal_monthly_funding_pages BEGIN SELECT RAISE(ABORT,'funding pages are append-only'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_funding_records_no_update BEFORE UPDATE ON portal_monthly_funding_records BEGIN SELECT RAISE(ABORT,'funding records are append-only'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_funding_records_no_delete BEFORE DELETE ON portal_monthly_funding_records BEGIN SELECT RAISE(ABORT,'funding records are append-only'); END`,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// SaveFundingPage commits all source rows and the continuation as one local
// transaction. Same completed page is idempotent. Skips, forks, altered scope,
// a changed fence and duplicate source IDs within a traversal are rejected.
// Different scans retain observations independently; they must NOT be summed.
func SaveFundingPage(ctx context.Context, db *sql.DB, p FundingPage) (string, error) {
	if db == nil {
		return "", errors.New("funding receiver is required")
	}
	raw, err := fundingPageBytes(p)
	if err != nil {
		return "", err
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	id := digest(raw)
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return "", err
	}
	defer func() { _ = tx.Rollback() }()
	_, _, saved, err := loadFundingStored(ctx, tx, false, id)
	if err == nil {
		if !bytes.Equal(saved, raw) {
			return "", errors.New("funding page replay differs")
		}
		if err := tx.Commit(); err != nil {
			return "", err
		}
		return id, nil
	}
	if !errors.Is(err, sql.ErrNoRows) {
		return "", err
	}
	last, sequence, lastRaw, err := loadFundingStored(ctx, tx, true, p.Scan.ID)
	if errors.Is(err, sql.ErrNoRows) {
		if p.PreviousID != "" || p.After != (FundingCursor{}) {
			return "", errors.New("funding predecessor is not persisted")
		}
		sequence = 0
	} else if err != nil {
		return "", err
	} else if last.EndReached || digest(lastRaw) != p.PreviousID || !sameFundingScan(last.Scan, p.Scan) || p.After != last.Next || p.FenceID != last.FenceID || p.StartedAtMS < last.CompletedAtMS {
		return "", errors.New("funding continuation conflicts with persisted head")
	}
	if sequence >= 1000 {
		return "", errors.New("funding scan page budget exceeded")
	}
	if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_funding_pages(id,scan_id,sequence,payload) VALUES(?,?,?,?)`, id, p.Scan.ID, sequence+1, raw); err != nil {
		return "", err
	}
	for _, row := range p.Rows {
		rowRaw, err := json.Marshal(row)
		if err != nil {
			return "", err
		}
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_funding_records(scan_id,log_id,page_id,row_digest) VALUES(?,?,?,?)`, p.Scan.ID, row.ID, id, digest(rowRaw)); err != nil {
			return "", err
		}
	}
	if err := tx.Commit(); err != nil {
		return "", err
	}
	return id, nil
}

func loadFundingStored(ctx context.Context, db walletRowReader, latest bool, key string) (FundingPage, int64, []byte, error) {
	var p FundingPage
	var id, scanID string
	var sequence int64
	var raw []byte
	query := `SELECT id,scan_id,sequence,CASE WHEN length(payload)<=2097152 THEN payload ELSE NULL END FROM portal_monthly_funding_pages WHERE id=?`
	if latest {
		if !validFundingScanID(key) {
			return p, 0, nil, errors.New("invalid funding scan key")
		}
		query = `SELECT id,scan_id,sequence,CASE WHEN length(payload)<=2097152 THEN payload ELSE NULL END FROM portal_monthly_funding_pages WHERE scan_id=? ORDER BY sequence DESC LIMIT 1`
	} else if !validDigest(key) {
		return p, 0, nil, errors.New("invalid funding page key")
	}
	if err := db.QueryRowContext(ctx, query, key).Scan(&id, &scanID, &sequence, &raw); err != nil {
		return p, 0, nil, err
	}
	if digest(raw) != id || json.Unmarshal(raw, &p) != nil || p.Scan.ID != scanID || sequence <= 0 || (sequence == 1) != (p.PreviousID == "") {
		return FundingPage{}, 0, nil, errors.New("funding stored page integrity failure")
	}
	canonical, err := fundingPageBytes(p)
	if err != nil || !bytes.Equal(canonical, raw) {
		return FundingPage{}, 0, nil, errors.New("funding stored page is not a complete canonical record")
	}
	return p, sequence, raw, nil
}

// LoadLatestFundingPage is the durable resume point, including source/scope,
// fence and cursor. sql.ErrNoRows means no successful first page, not zero money.
func LoadLatestFundingPage(ctx context.Context, db *sql.DB, scanID string) (FundingPage, error) {
	if db == nil {
		return FundingPage{}, errors.New("funding receiver is required")
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	p, _, _, err := loadFundingStored(ctx, db, true, scanID)
	return p, err
}
