package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

var ErrWalletMonthCaptureIntegrity = errors.New("wallet capture differs from pinned organization scope")

type WalletMonthCaptureProgress struct {
	Mode           string   `json:"mode"`
	Completed      int      `json:"completed"`
	NextEligibleMS int64    `json:"next_eligible_unix_ms,omitempty"`
	Captured       bool     `json:"captured"`
	IDs            []string `json:"ids"`
}

// InitWalletMonthCaptureStore is explicit setup of the private unsealed
// capture SQLite. No application startup, source or customer bill is changed.
func InitWalletMonthCaptureStore(parent context.Context, receiver *sql.DB) error {
	if parent == nil || receiver == nil {
		return ErrWalletMonthCaptureIntegrity
	}
	if err := InitWalletObservationStore(parent, receiver); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := receiver.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_wallet_capture_slots (
job_key TEXT NOT NULL CHECK(length(job_key)=64),ordinal INTEGER NOT NULL CHECK(ordinal IN (0,1)),
observation_id TEXT NOT NULL CHECK(length(observation_id)=64),PRIMARY KEY(job_key,ordinal))`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_capture_no_update BEFORE UPDATE ON portal_monthly_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'wallet capture slots are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_wallet_capture_no_delete BEFORE DELETE ON portal_monthly_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'wallet capture slots are immutable'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

func capturedWalletForScope(ctx context.Context, db walletRowReader, id string, scope OrganizationMonthScope) (WalletObservation, error) {
	raw, err := loadWalletObservation(ctx, db, id)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return WalletObservation{}, errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		return WalletObservation{}, err
	}
	var o WalletObservation
	if json.Unmarshal(raw, &o) != nil || o.SourceIdentity != scope.Funding.SourceIdentity || len(o.Users) != len(scope.Funding.UserIDs) || o.CompletedAtMS > time.Now().UnixMilli() {
		return WalletObservation{}, ErrWalletMonthCaptureIntegrity
	}
	month, err := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil || o.CompletedAtMS/1000 < month.AddDate(0, 1, 0).Unix() {
		return WalletObservation{}, ErrWalletMonthCaptureIntegrity
	}
	for i, user := range o.Users {
		if user.UserID != scope.Funding.UserIDs[i] || !user.Exists || user.BalanceQuota == nil || user.UsedQuota == nil {
			return WalletObservation{}, ErrWalletMonthCaptureIntegrity
		}
	}
	return o, nil
}

// InspectWalletMonthCapture preserves actual observation times and counters.
// Different balances between reads are valid; neither is called a month end.
func InspectWalletMonthCapture(parent context.Context, receiver *sql.DB, key string, scope OrganizationMonthScope) (WalletMonthCaptureProgress, error) {
	out := WalletMonthCaptureProgress{Mode: "wallet_observations_not_historical_balances", IDs: []string{}}
	if parent == nil || receiver == nil {
		return out, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if _, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now()); err != nil {
		return out, err
	}
	rows, err := receiver.QueryContext(ctx, `SELECT ordinal,observation_id FROM portal_monthly_wallet_capture_slots WHERE job_key=? ORDER BY ordinal LIMIT 3`, key)
	if err != nil {
		return out, err
	}
	for rows.Next() {
		var ordinal int
		var id string
		if err := rows.Scan(&ordinal, &id); err != nil {
			_ = rows.Close()
			return out, err
		}
		if ordinal != len(out.IDs) || ordinal > 1 {
			_ = rows.Close()
			return out, ErrWalletMonthCaptureIntegrity
		}
		out.IDs = append(out.IDs, id)
	}
	err = rows.Err()
	closeErr := rows.Close()
	if err != nil || closeErr != nil {
		return out, errors.Join(err, closeErr)
	}
	var older WalletObservation
	for i, id := range out.IDs {
		o, err := capturedWalletForScope(ctx, receiver, id, scope)
		if err != nil {
			return WalletMonthCaptureProgress{}, err
		}
		if i == 0 {
			older = o
		} else if id == out.IDs[0] || o.StartedAtMS-older.CompletedAtMS < 300000 {
			return WalletMonthCaptureProgress{}, ErrWalletMonthCaptureIntegrity
		}
	}
	out.Completed = len(out.IDs)
	if out.Completed == 1 && older.CompletedAtMS+300000 > time.Now().UnixMilli() {
		out.NextEligibleMS = older.CompletedAtMS + 300000
	}
	return out, nil
}

// Original wallet and its stable slot commit together. No orphan observation
// is turned into another source read after a crash; exact replay is idempotent.
func saveWalletCaptureSlot(parent context.Context, receiver *sql.DB, plan FundingMonthCapturePlan, ordinal int, o WalletObservation) error {
	if ordinal < 0 || ordinal > 1 {
		return ErrWalletMonthCaptureIntegrity
	}
	raw, err := walletObservationBytes(o)
	if err != nil {
		return err
	}
	if len(raw) > 65536 || o.SourceIdentity != plan.Scope.Funding.SourceIdentity || len(o.Users) != len(plan.Scope.Funding.UserIDs) {
		return ErrWalletMonthCaptureIntegrity
	}
	for i, u := range o.Users {
		if u.UserID != plan.Scope.Funding.UserIDs[i] {
			return ErrWalletMonthCaptureIntegrity
		}
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := receiver.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	result, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_wallet_capture_slots(job_key,ordinal,observation_id) SELECT ?,?,? WHERE (SELECT count(*) FROM portal_monthly_wallet_capture_slots WHERE job_key=?)=? ON CONFLICT(job_key,ordinal) DO NOTHING`, plan.Key, ordinal, digest(raw), plan.Key, ordinal)
	if err != nil {
		return err
	}
	inserted, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if inserted == 1 {
		if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_wallet_observations(id,source_identity,started_at_ms,completed_at_ms,payload) VALUES(?,?,?,?,?) ON CONFLICT(id) DO NOTHING`, digest(raw), o.SourceIdentity, o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
			return err
		}
	}
	var id string
	if err := tx.QueryRowContext(ctx, `SELECT observation_id FROM portal_monthly_wallet_capture_slots WHERE job_key=? AND ordinal=?`, plan.Key, ordinal).Scan(&id); err != nil {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	stored, err := loadWalletObservation(ctx, tx, id)
	if err != nil || id != digest(raw) || !bytes.Equal(stored, raw) {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	return tx.Commit()
}

func CaptureNextWalletMonthView(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope) (WalletMonthCaptureProgress, error) {
	if parent == nil || source == nil || receiver == nil || source == receiver {
		return WalletMonthCaptureProgress{}, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 12*time.Second)
	defer cancel()
	progress, err := InspectWalletMonthCapture(ctx, receiver, key, scope)
	if err != nil || progress.Completed == 2 || progress.NextEligibleMS > time.Now().UnixMilli() {
		return progress, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return progress, err
	}
	if err := waitFundingInterval(ctx, time.Second); err != nil {
		return progress, err
	}
	connectCtx, connectCancel := context.WithTimeout(ctx, 4*time.Second)
	err = source.PingContext(connectCtx)
	connectCancel()
	if err != nil {
		return progress, err
	}
	o, err := CaptureWallet(ctx, source, scope.Funding.SourceIdentity, scope.Funding.UserIDs)
	if err != nil {
		return progress, err
	}
	if err := saveWalletCaptureSlot(ctx, receiver, plan, progress.Completed, o); err != nil {
		return progress, err
	}
	after, err := InspectWalletMonthCapture(ctx, receiver, key, scope)
	if err == nil {
		after.Captured = true
	}
	return after, err
}
