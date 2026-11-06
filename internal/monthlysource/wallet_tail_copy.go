package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Source originals only, never a normalized historical balance or permission
// to publish. The caller must keep the sealed DB digest and exact selection.
type SelectedWalletTails struct {
	Boundary int64
	Pairs    []CapturedWalletTailPair
}

func ReadSelectedWalletTails(parent context.Context, db *sql.DB, selection []byte) (SelectedWalletTails, error) {
	var empty SelectedWalletTails
	if parent == nil || db == nil {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := ValidateWalletTailCaptureStore(ctx, db); err != nil {
		return empty, err
	}
	s, err := canonicalCapturedUsageIndex(selection, time.Now())
	if err != nil {
		return empty, err
	}
	now := time.Now()
	boundary, err := chooseWalletTailRound(ctx, db, s.Key, s.PlanSHA256, s.Scope, now)
	if err != nil {
		return empty, err
	}
	set, err := loadWalletTailRound(ctx, db, s.Key, s.PlanSHA256, s.Scope, boundary, now)
	if err != nil {
		return empty, err
	}
	progress := walletTailProgress(set, now)
	if progress.TotalViews == 0 || progress.Completed != progress.TotalViews {
		return empty, ErrWalletMonthCaptureIntegrity
	}
	return SelectedWalletTails{Boundary: boundary, Pairs: set.pairs}, ctx.Err()
}

// Extend only a freshly copied, exact core+registered seal. The live capture
// controls and unrelated jobs/accounts/old rounds do NOT enter the sealed DB.
// The core copier's physical SQLite page budget is preserved, never raised.
func CopySelectedWalletTails(parent context.Context, source, destination *sql.DB, selection []byte) (err error) {
	defer func() {
		var coded interface{ Code() int }
		if errors.As(err, &coded) && coded.Code()&255 == 13 {
			err = errors.Join(ErrTokenMonthCaptureBudget, err)
		}
	}()
	if parent == nil || source == nil || destination == nil || source == destination {
		return ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 5*time.Second)
	defer cancel()
	s, err := canonicalCapturedUsageIndex(selection, time.Now())
	if err != nil {
		return err
	}
	plan, err := loadFundingMonthCapturePlan(ctx, source, s.Key, s.Scope, time.Now())
	if err != nil || registeredCapturePlanSHA(plan) != s.PlanSHA256 {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	originals, err := ReadSelectedWalletTails(ctx, source, selection)
	if err != nil {
		return err
	}
	var tables, other int
	if err := destination.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(CASE WHEN name NOT IN ('portal_monthly_funding_pages','portal_monthly_funding_records','portal_monthly_usage_observations','portal_monthly_wallet_observations','portal_monthly_registered_wallet_observations','portal_monthly_registered_wallet_capture_slots') THEN 1 ELSE 0 END),0) FROM main.sqlite_master WHERE type='table'`).Scan(&tables, &other); err != nil {
		return err
	}
	if tables != 6 || other != 0 {
		return ErrWalletMonthCaptureIntegrity
	}
	if err := registeredCopyCoreReferences(ctx, destination, s); err != nil {
		return err
	}
	if _, _, err := ReadSelectedRegisteredWallet(ctx, destination, selection); err != nil {
		return err
	}
	if err := InitWalletTailCaptureStore(ctx, destination); err != nil {
		return err
	}
	if err := copyWalletTailRows(ctx, destination, s, originals); err != nil {
		return err
	}
	for _, db := range []*sql.DB{destination, source} {
		got, err := ReadSelectedWalletTails(ctx, db, selection)
		if err != nil || got.Boundary != originals.Boundary || len(got.Pairs) != len(originals.Pairs) {
			return errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		for i, pair := range got.Pairs {
			want, err := DeriveWalletTailBoundary(originals.Pairs[i].Older, originals.Pairs[i].Newer)
			if err != nil {
				return err
			}
			actual, err := DeriveWalletTailBoundary(pair.Older, pair.Newer)
			if err != nil || want != actual {
				return errors.Join(ErrWalletMonthCaptureIntegrity, err)
			}
		}
	}
	return ctx.Err()
}

// A single LOCAL transaction, not two fsyncs for each of up to 200 accounts.
// Failure rolls back the whole selected tail set. No source transaction is
// held, and neither the byte budget nor SQLite's physical page cap is raised.
func copyWalletTailRows(ctx context.Context, destination *sql.DB, s CapturedMonthSelection, originals SelectedWalletTails) error {
	if !validDigest(s.Key) || !validDigest(s.PlanSHA256) || len(originals.Pairs) != len(s.Scope.Funding.UserIDs) {
		return ErrWalletMonthCaptureIntegrity
	}
	if _, err := canonicalFundingCaptureScope(s.Scope, time.Now()); err != nil {
		return err
	}
	tx, err := destination.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	var count int
	if err := tx.QueryRowContext(ctx, `SELECT count(*) FROM portal_monthly_wallet_tail_captures`).Scan(&count); err != nil {
		return err
	}
	if count != 0 {
		return ErrWalletMonthCaptureIntegrity
	}
	size, scopeSHA := 0, registeredCaptureScopeSHA(s.Scope)
	for index, pair := range originals.Pairs {
		if _, err := DeriveWalletTailBoundary(pair.Older, pair.Newer); err != nil {
			return err
		}
		for ordinal, o := range []WalletTailObservation{pair.Older, pair.Newer} {
			if o.Request != (WalletTailRequest{SourceIdentity: s.Scope.Funding.SourceIdentity, Rules: s.Scope.Funding.Rules, UserID: s.Scope.Funding.UserIDs[index], Boundary: originals.Boundary}) || o.CompletedAtMS > time.Now().UnixMilli() {
				return ErrWalletMonthCaptureIntegrity
			}
			raw, err := walletTailBytes(o)
			if err != nil {
				return err
			}
			size += len(raw)
			if size > walletTailStoreBytesLimit {
				return ErrWalletTailCaptureCapacity
			}
			if _, err := tx.ExecContext(ctx, walletTailInsertQuery, s.Key, s.PlanSHA256, scopeSHA, originals.Boundary, o.Request.UserID, ordinal, digest(raw), o.StartedAtMS, o.CompletedAtMS, raw); err != nil {
				return err
			}
		}
	}
	return tx.Commit()
}
