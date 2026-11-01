package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"sort"
	"time"
)

const walletTailPayloadLimit = 4 << 20
const walletTailStoreBytesLimit = 64 << 20
const walletTailStoreRowsLimit = 32 * WalletBatchLimit * 2
const walletTailRoundsLimit = 12

const walletTailInsertQuery = `INSERT INTO portal_monthly_wallet_tail_captures(job_key,plan_sha256,scope_sha256,boundary,user_id,ordinal,id,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?,?,?,?,?,?)`

var ErrWalletTailCaptureCapacity = errors.New("wallet tail capture storage capacity reached")

// One immutable row contains BOTH the complete original and its exact job,
// plan, roster, boundary and ordinal. There is no split original/ref commit.
// This is explicit private receiver setup, never ordinary startup/source DDL.
func InitWalletTailCaptureStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := registeredWalletSQLite(ctx, db); err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, query := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_wallet_tail_captures (
job_key TEXT NOT NULL CHECK(length(job_key)=64),plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
scope_sha256 TEXT NOT NULL CHECK(length(scope_sha256)=64),boundary INTEGER NOT NULL CHECK(boundary>0),
user_id INTEGER NOT NULL CHECK(user_id>0),ordinal INTEGER NOT NULL CHECK(ordinal IN (0,1)),
id TEXT NOT NULL CHECK(length(id)=64),started_at_ms INTEGER NOT NULL,completed_at_ms INTEGER NOT NULL,
payload BLOB NOT NULL CHECK(typeof(payload)='blob' AND length(payload)<=4194304),
PRIMARY KEY(job_key,boundary,user_id,ordinal))`,
		`CREATE INDEX IF NOT EXISTS idx_monthly_wallet_tail_id ON portal_monthly_wallet_tail_captures(id)`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_tail_no_update BEFORE UPDATE ON portal_monthly_wallet_tail_captures BEGIN SELECT RAISE(ABORT,'wallet tail originals are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_tail_no_delete BEFORE DELETE ON portal_monthly_wallet_tail_captures BEGIN SELECT RAISE(ABORT,'wallet tail originals are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_tail_no_replace BEFORE INSERT ON portal_monthly_wallet_tail_captures
WHEN EXISTS(SELECT 1 FROM portal_monthly_wallet_tail_captures WHERE job_key=NEW.job_key AND boundary=NEW.boundary AND user_id=NEW.user_id AND ordinal=NEW.ordinal AND
(plan_sha256 IS NOT NEW.plan_sha256 OR scope_sha256 IS NOT NEW.scope_sha256 OR id IS NOT NEW.id OR started_at_ms IS NOT NEW.started_at_ms OR completed_at_ms IS NOT NEW.completed_at_ms OR payload IS NOT NEW.payload))
BEGIN SELECT RAISE(ABORT,'wallet tail checkpoint cannot be replaced'); END`,
	} {
		if _, err := tx.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func ValidateWalletTailCaptureStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	var count int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM main.sqlite_master WHERE
(type='table' AND name='portal_monthly_wallet_tail_captures') OR
(type='index' AND name='idx_monthly_wallet_tail_id') OR
(type='trigger' AND name IN ('monthly_wallet_tail_no_update','monthly_wallet_tail_no_delete','monthly_wallet_tail_no_replace'))`).Scan(&count)
	if err != nil || count != 5 {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	return ctx.Err()
}

type CapturedWalletTailPair struct {
	Older WalletTailObservation
	Newer WalletTailObservation
}

type walletTailSet struct {
	boundary int64
	pairs    []CapturedWalletTailPair // complete pinned roster; missing views remain explicit
}

type walletTailReader interface {
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
}

func walletTailBytes(o WalletTailObservation) ([]byte, error) {
	if err := ValidateWalletTailObservation(o); err != nil {
		return nil, errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	raw, err := json.Marshal(o)
	if err != nil || len(raw) > walletTailPayloadLimit {
		return nil, errors.Join(ErrWalletTailCaptureCapacity, err)
	}
	return raw, nil
}

// Pick a fully committed round before considering a new current-month round.
// Incomplete cross-month rounds are RETAINED, never retimed or overwritten.
// A complete but corrupt round fails subsequent audit; it is not bypassed.
func chooseWalletTailRound(ctx context.Context, db walletTailReader, key, planSHA string, scope OrganizationMonthScope, now time.Time) (int64, error) {
	rows, err := db.QueryContext(ctx, `SELECT boundary,min(plan_sha256),max(plan_sha256),min(scope_sha256),max(scope_sha256),count(*),count(DISTINCT user_id)
FROM portal_monthly_wallet_tail_captures WHERE job_key=? GROUP BY boundary ORDER BY boundary LIMIT 13`, key)
	if err != nil {
		return 0, err
	}
	defer rows.Close()
	month, err := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil {
		return 0, err
	}
	current, selected, count := walletTailMonthStart(now.UnixMilli()), int64(0), 0
	currentExists := false
	for rows.Next() {
		var boundary int64
		var loPlan, hiPlan, loScope, hiScope string
		var records, users int
		if err := rows.Scan(&boundary, &loPlan, &hiPlan, &loScope, &hiScope, &records, &users); err != nil {
			return 0, err
		}
		count++
		if count > walletTailRoundsLimit || boundary > current || boundary < month.AddDate(0, 1, 0).Unix() ||
			walletTailMonthStart(boundary*1000) != boundary || loPlan != planSHA || hiPlan != planSHA ||
			loScope != registeredCaptureScopeSHA(scope) || hiScope != loScope || records < 1 || records > len(scope.Funding.UserIDs)*2 || users > len(scope.Funding.UserIDs) {
			return 0, ErrWalletMonthCaptureIntegrity
		}
		if boundary == current {
			currentExists = true
		}
		if selected == 0 && records == len(scope.Funding.UserIDs)*2 && users == len(scope.Funding.UserIDs) {
			selected = boundary
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return 0, err
	}
	if selected != 0 {
		return selected, nil
	}
	if !currentExists && count >= walletTailRoundsLimit {
		return 0, ErrWalletTailCaptureCapacity
	}
	return current, nil
}

// This also audits a sealed copy without its live funding-plan control table;
// expected key/hash/full scope must come from the pinned source selection.
func loadWalletTailRound(ctx context.Context, db walletTailReader, key, planSHA string, scope OrganizationMonthScope, boundary int64, now time.Time) (walletTailSet, error) {
	var empty walletTailSet
	if !validDigest(key) || !validDigest(planSHA) {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	if _, err := canonicalFundingCaptureScope(scope, now); err != nil {
		return empty, err
	}
	rows, err := db.QueryContext(ctx, `SELECT user_id,ordinal,plan_sha256,scope_sha256,id,started_at_ms,completed_at_ms,
CASE WHEN typeof(payload)='blob' AND length(payload)<=4194304 THEN payload END
FROM portal_monthly_wallet_tail_captures WHERE job_key=? AND boundary=? ORDER BY user_id,ordinal LIMIT 401`, key, boundary)
	if err != nil {
		return empty, err
	}
	defer rows.Close()
	set := walletTailSet{boundary: boundary, pairs: make([]CapturedWalletTailPair, len(scope.Funding.UserIDs))}
	lastUser, lastOrdinal, count := int64(0), -1, 0
	for rows.Next() {
		var userID, started, completed int64
		var ordinal int
		var pinnedPlan, pinnedScope, id string
		var raw []byte
		if err := rows.Scan(&userID, &ordinal, &pinnedPlan, &pinnedScope, &id, &started, &completed, &raw); err != nil {
			return empty, err
		}
		count++
		index := sort.Search(len(scope.Funding.UserIDs), func(i int) bool { return scope.Funding.UserIDs[i] >= userID })
		if index == len(scope.Funding.UserIDs) || scope.Funding.UserIDs[index] != userID || ordinal < 0 || ordinal > 1 ||
			count > len(scope.Funding.UserIDs)*2 || pinnedPlan != planSHA || pinnedScope != registeredCaptureScopeSHA(scope) ||
			!validDigest(id) || digest(raw) != id || completed > now.UnixMilli() ||
			(userID != lastUser && ordinal != 0) || (userID == lastUser && ordinal != lastOrdinal+1) {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		var o WalletTailObservation
		if json.Unmarshal(raw, &o) != nil || o.Request != (WalletTailRequest{SourceIdentity: scope.Funding.SourceIdentity, Rules: scope.Funding.Rules, UserID: userID, Boundary: boundary}) || o.StartedAtMS != started || o.CompletedAtMS != completed {
			return empty, ErrWalletMonthCaptureIntegrity
		}
		canonical, err := walletTailBytes(o)
		if err != nil || !bytes.Equal(canonical, raw) {
			return empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		if ordinal == 0 {
			set.pairs[index].Older = o
		} else {
			set.pairs[index].Newer = o
			if _, err := DeriveWalletTailBoundary(set.pairs[index].Older, o); err != nil {
				return empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
			}
		}
		lastUser, lastOrdinal = userID, ordinal
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return empty, err
	}
	return set, ctx.Err()
}

func saveWalletTailCapture(ctx context.Context, db *sql.DB, key, planSHA string, scope OrganizationMonthScope, ordinal int, o WalletTailObservation, now time.Time) error {
	if ordinal < 0 || ordinal > 1 || !validDigest(key) || !validDigest(planSHA) || !fundingScopeHas(scope.Funding.UserIDs, o.Request.UserID) ||
		o.Request.SourceIdentity != scope.Funding.SourceIdentity || o.Request.Rules != scope.Funding.Rules || o.CompletedAtMS > now.UnixMilli() {
		return ErrWalletMonthCaptureIntegrity
	}
	if _, err := canonicalFundingCaptureScope(scope, now); err != nil {
		return err
	}
	month, err := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil || o.Request.Boundary < month.AddDate(0, 1, 0).Unix() {
		return ErrWalletMonthCaptureIntegrity
	}
	raw, err := walletTailBytes(o)
	if err != nil {
		return err
	}
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var previous int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_wallet_tail_captures WHERE job_key=? AND boundary=? AND user_id=? AND ordinal=0`, key, o.Request.Boundary, o.Request.UserID).Scan(&previous); err != nil {
		return err
	}
	if ordinal == 1 && previous != 1 {
		return ErrWalletMonthCaptureIntegrity
	}
	var oldID string
	var oldRaw []byte
	err = tx.QueryRowContext(ctx, `SELECT id,CASE WHEN typeof(payload)='blob' AND length(payload)<=4194304 THEN payload END FROM portal_monthly_wallet_tail_captures WHERE job_key=? AND boundary=? AND user_id=? AND ordinal=?`, key, o.Request.Boundary, o.Request.UserID, ordinal).Scan(&oldID, &oldRaw)
	if err == nil {
		if oldID != digest(raw) || !bytes.Equal(oldRaw, raw) {
			return ErrWalletMonthCaptureIntegrity
		}
	} else if !errors.Is(err, sql.ErrNoRows) {
		return err
	} else {
		var count, size int64
		if err := tx.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(length(payload)),0) FROM portal_monthly_wallet_tail_captures`).Scan(&count, &size); err != nil {
			return err
		}
		if count >= walletTailStoreRowsLimit || size+int64(len(raw)) > walletTailStoreBytesLimit {
			return ErrWalletTailCaptureCapacity
		}
		if _, err := tx.ExecContext(ctx, walletTailInsertQuery,
			key, planSHA, registeredCaptureScopeSHA(scope), o.Request.Boundary, o.Request.UserID, ordinal, digest(raw), o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
			return err
		}
	}
	// Confirm the stored original/binding, including exact idempotent replay.
	var pinnedPlan, pinnedScope string
	var started, completed int64
	if err := tx.QueryRowContext(ctx, `SELECT plan_sha256,scope_sha256,started_at_ms,completed_at_ms FROM portal_monthly_wallet_tail_captures WHERE job_key=? AND boundary=? AND user_id=? AND ordinal=?`, key, o.Request.Boundary, o.Request.UserID, ordinal).Scan(&pinnedPlan, &pinnedScope, &started, &completed); err != nil {
		return err
	}
	if pinnedPlan != planSHA || pinnedScope != registeredCaptureScopeSHA(scope) || started != o.StartedAtMS || completed != o.CompletedAtMS {
		return ErrWalletMonthCaptureIntegrity
	}
	return tx.Commit()
}
