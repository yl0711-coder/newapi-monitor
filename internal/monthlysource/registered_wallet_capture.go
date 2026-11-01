package monthlysource

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"time"
)

var registeredWalletMonthSlots = make(chan struct{}, 1)
var errRegisteredWalletSlotMissing = errors.New("registered wallet checkpoint not captured")

type RegisteredWalletCaptureProgress struct {
	Completed int    `json:"completed"`
	Captured  bool   `json:"captured"`
	ID        string `json:"id,omitempty"`
}

// Explicit setup of a PRIVATE capture receiver, never normal startup or HTTP.
func InitRegisteredWalletMonthCaptureStore(parent context.Context, db *sql.DB) error {
	if err := InitRegisteredWalletObservationStore(parent, db); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	for _, q := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_registered_wallet_capture_slots (
job_key TEXT PRIMARY KEY NOT NULL CHECK(length(job_key)=64),plan_sha256 TEXT NOT NULL CHECK(length(plan_sha256)=64),
scope_sha256 TEXT NOT NULL CHECK(length(scope_sha256)=64),observation_id TEXT NOT NULL CHECK(length(observation_id)=64))`,
		`CREATE TRIGGER IF NOT EXISTS monthly_registered_capture_no_update BEFORE UPDATE ON portal_monthly_registered_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'registered wallet checkpoints are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_registered_capture_no_delete BEFORE DELETE ON portal_monthly_registered_wallet_capture_slots BEGIN SELECT RAISE(ABORT,'registered wallet checkpoints are immutable'); END`,
		`CREATE TRIGGER IF NOT EXISTS monthly_registered_capture_no_replace BEFORE INSERT ON portal_monthly_registered_wallet_capture_slots
WHEN EXISTS(SELECT 1 FROM portal_monthly_registered_wallet_capture_slots WHERE job_key=NEW.job_key AND
(plan_sha256 IS NOT NEW.plan_sha256 OR scope_sha256 IS NOT NEW.scope_sha256 OR observation_id IS NOT NEW.observation_id))
BEGIN SELECT RAISE(ABORT,'registered wallet checkpoint cannot be replaced'); END`,
	} {
		if _, err := tx.ExecContext(ctx, q); err != nil {
			return err
		}
	}
	return tx.Commit()
}

// Read-only preflight. A missing schema is not an empty checkpoint and must
// not cause source reads or implicit schema initialization at installation.
func ValidateRegisteredWalletMonthCaptureStore(parent context.Context, db *sql.DB) error {
	if parent == nil || db == nil {
		return ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	var found int
	err := db.QueryRowContext(ctx, `SELECT count(*) FROM sqlite_master WHERE name IN (
'portal_monthly_registered_wallet_observations','idx_monthly_registered_wallet_source_time',
'monthly_registered_wallet_no_update','monthly_registered_wallet_no_delete','monthly_registered_wallet_no_replace',
'portal_monthly_registered_wallet_capture_slots','monthly_registered_capture_no_update','monthly_registered_capture_no_delete','monthly_registered_capture_no_replace')`).Scan(&found)
	if err != nil || found != 9 {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	return ctx.Err()
}

func registeredCapturePlanSHA(plan FundingMonthCapturePlan) string {
	raw, _ := json.Marshal(plan)
	return digest(raw)
}

func registeredCaptureScopeSHA(scope OrganizationMonthScope) string {
	raw, _ := json.Marshal(scope)
	return digest(raw)
}

// This also works on a sealed copy WITHOUT funding-plan/control tables. The
// expected plan hash and full scope must come from its pinned source selection.
func loadRegisteredWalletCapture(ctx context.Context, db walletRowReader, key, planSHA string, scope OrganizationMonthScope) (string, RegisteredWalletObservation, error) {
	var empty RegisteredWalletObservation
	if !validDigest(key) || !validDigest(planSHA) {
		return "", empty, ErrWalletMonthCaptureIntegrity
	}
	if _, err := canonicalFundingCaptureScope(scope, time.Now()); err != nil {
		return "", empty, err
	}
	var pinnedPlan, pinnedScope, id string
	err := db.QueryRowContext(ctx, `SELECT plan_sha256,scope_sha256,observation_id FROM portal_monthly_registered_wallet_capture_slots WHERE job_key=?`, key).Scan(&pinnedPlan, &pinnedScope, &id)
	if errors.Is(err, sql.ErrNoRows) {
		return "", empty, errRegisteredWalletSlotMissing
	}
	if err != nil || pinnedPlan != planSHA || pinnedScope != registeredCaptureScopeSHA(scope) || !validDigest(id) {
		return "", empty, errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	raw, err := loadRegisteredWalletBytes(ctx, db, id)
	if err != nil {
		return "", empty, errors.Join(ErrWalletMonthCaptureIntegrity, err) // dangling is NOT uncaptured
	}
	var o RegisteredWalletObservation
	month, err := time.ParseInLocation("2006-01", scope.Funding.Month, fundingMonthZone)
	if err != nil || json.Unmarshal(raw, &o) != nil || o.SourceIdentity != scope.Funding.SourceIdentity || len(o.Users) != len(scope.Funding.UserIDs) || o.CompletedAtMS > time.Now().UnixMilli() || o.CompletedAtMS/1000 < month.AddDate(0, 1, 0).Unix() {
		return "", empty, ErrWalletMonthCaptureIntegrity
	}
	for i, u := range o.Users {
		if u.UserID != scope.Funding.UserIDs[i] {
			return "", empty, ErrWalletMonthCaptureIntegrity
		}
	}
	return id, o, ctx.Err()
}

func InspectRegisteredWalletMonthCapture(parent context.Context, db *sql.DB, key string, scope OrganizationMonthScope) (RegisteredWalletCaptureProgress, error) {
	var empty RegisteredWalletCaptureProgress
	if parent == nil || db == nil {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := ValidateRegisteredWalletMonthCaptureStore(ctx, db); err != nil {
		return empty, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, db, key, scope, time.Now())
	if err != nil {
		return empty, err
	}
	id, _, err := loadRegisteredWalletCapture(ctx, db, key, registeredCapturePlanSHA(plan), scope)
	if errors.Is(err, errRegisteredWalletSlotMissing) {
		return empty, nil
	}
	if err != nil {
		return empty, err
	}
	return RegisteredWalletCaptureProgress{Completed: 1, ID: id}, nil
}

func saveRegisteredWalletCapture(parent context.Context, db *sql.DB, plan FundingMonthCapturePlan, o RegisteredWalletObservation) error {
	raw, err := registeredWalletBytes(o)
	if err != nil || o.SourceIdentity != plan.Scope.Funding.SourceIdentity || len(o.Users) != len(plan.Scope.Funding.UserIDs) {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	for i, user := range o.Users {
		if user.UserID != plan.Scope.Funding.UserIDs[i] {
			return ErrWalletMonthCaptureIntegrity
		}
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	planSHA := registeredCapturePlanSHA(plan)
	if _, err := tx.ExecContext(ctx, `INSERT INTO portal_monthly_registered_wallet_capture_slots(job_key,plan_sha256,scope_sha256,observation_id) VALUES(?,?,?,?) ON CONFLICT(job_key) DO NOTHING`, plan.Key, planSHA, registeredCaptureScopeSHA(plan.Scope), digest(raw)); err != nil {
		return err
	}
	if _, err := insertRegisteredWalletObservation(ctx, tx, o, raw); err != nil {
		return err
	}
	id, current, err := loadRegisteredWalletCapture(ctx, tx, plan.Key, planSHA, plan.Scope)
	if err != nil {
		return err
	}
	canonical, err := registeredWalletBytes(current)
	if err != nil || id != digest(raw) || !bytes.Equal(canonical, raw) {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	return tx.Commit()
}

// One source view at most, with original and job reference in ONE local commit.
// Already committed checkpoints (including after an ambiguous return/crash)
// are verified and reused, never replaced by a newly captured wallet.
func CaptureNextRegisteredWalletMonthView(parent context.Context, source, receiver *sql.DB, key string, scope OrganizationMonthScope) (RegisteredWalletCaptureProgress, error) {
	var empty RegisteredWalletCaptureProgress
	if parent == nil || source == nil || receiver == nil || source == receiver {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 8*time.Second)
	defer cancel()
	select {
	case registeredWalletMonthSlots <- struct{}{}:
		defer func() { <-registeredWalletMonthSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	progress, err := InspectRegisteredWalletMonthCapture(ctx, receiver, key, scope)
	if err != nil || progress.Completed == 1 {
		return progress, err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, receiver, key, scope, time.Now())
	if err != nil {
		return empty, err
	}
	if err := waitFundingInterval(ctx, time.Second); err != nil {
		return empty, err
	}
	o, err := CaptureRegisteredWallet(ctx, source, scope.Funding.SourceIdentity, scope.Funding.UserIDs)
	if err != nil {
		return empty, err
	}
	if err := saveRegisteredWalletCapture(ctx, receiver, plan, o); err != nil {
		return empty, err
	}
	after, err := InspectRegisteredWalletMonthCapture(ctx, receiver, key, scope)
	if err == nil {
		after.Captured = true
	}
	return after, err
}
