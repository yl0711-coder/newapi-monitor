package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"time"
)

// Verify only the expected original/checkpoint, not historical wallet money.
// Core funding/token/usage verification remains mandatory in the common seal.
func ReadSelectedRegisteredWallet(parent context.Context, db *sql.DB, selection []byte) (string, RegisteredWalletObservation, error) {
	var empty RegisteredWalletObservation
	if parent == nil || db == nil {
		return "", empty, ErrWalletMonthCaptureIntegrity
	}
	ctx, cancel := context.WithTimeout(parent, 2*time.Second)
	defer cancel()
	if err := ValidateRegisteredWalletMonthCaptureStore(ctx, db); err != nil {
		return "", empty, err
	}
	s, err := canonicalCapturedUsageIndex(selection, time.Now())
	if err != nil {
		return "", empty, err
	}
	return loadRegisteredWalletCapture(ctx, db, s.Key, s.PlanSHA256, s.Scope)
}

// Add ONE exact original and its pinned request reference after the legacy
// core copier, before closing/fingerprinting the destination. Does NOT modify
// source selections, token archives, money, core rows or publication eligibility.
// Existing registered namespaces/foreign/control tables are refused, not merged.
func CopySelectedRegisteredWallet(parent context.Context, source, destination *sql.DB, selection []byte) (err error) {
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
	id, o, err := loadRegisteredWalletCapture(ctx, source, s.Key, s.PlanSHA256, s.Scope)
	if err != nil {
		return err
	}
	// A destination is the fresh core-only export, never the live capture DB,
	// business/task DB or an existing richer seal (including handle aliases).
	var tables, other int
	if err := destination.QueryRowContext(ctx, `SELECT count(*),coalesce(sum(CASE WHEN name NOT IN ('portal_monthly_funding_pages','portal_monthly_funding_records','portal_monthly_usage_observations','portal_monthly_wallet_observations') THEN 1 ELSE 0 END),0) FROM main.sqlite_master WHERE type='table'`).Scan(&tables, &other); err != nil {
		return err
	}
	if tables != 4 || other != 0 {
		return ErrWalletMonthCaptureIntegrity
	}
	if err := registeredCopyCoreReferences(ctx, destination, s); err != nil {
		return err
	}
	if err := InitRegisteredWalletMonthCaptureStore(ctx, destination); err != nil {
		return err
	}
	if err := saveRegisteredWalletCapture(ctx, destination, plan, o); err != nil {
		return err
	}
	got, restored, err := loadRegisteredWalletCapture(ctx, destination, s.Key, s.PlanSHA256, s.Scope)
	if err != nil || got != id {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	want, err := registeredWalletBytes(o)
	if err != nil {
		return err
	}
	actual, err := registeredWalletBytes(restored)
	if err != nil || digest(actual) != digest(want) {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	// Recheck the immutable source reference after destination work. Source
	// tampering never makes a partial copy a successful seal/handoff.
	after, _, err := loadRegisteredWalletCapture(ctx, source, s.Key, s.PlanSHA256, s.Scope)
	if err != nil || after != id {
		return errors.Join(ErrWalletMonthCaptureIntegrity, err)
	}
	return ctx.Err()
}

// Cheap exact-identity checks before extending a core export. This refuses a
// same-shaped EMPTY/other-job DB without duplicating the full financial audit,
// which the common completion-marker path still performs afterward.
func registeredCopyCoreReferences(ctx context.Context, db *sql.DB, s CapturedMonthSelection) error {
	if len(s.WalletIDs) != 2 || len(s.Funding) == 0 || len(s.Funding) > 31 {
		return ErrWalletMonthCaptureIntegrity
	}
	for _, id := range s.WalletIDs {
		if _, err := capturedWalletForScope(ctx, db, id, s.Scope); err != nil {
			return err
		}
	}
	for _, w := range []FundingMonthWindowVersion{s.Funding[0], s.Funding[len(s.Funding)-1]} {
		page, _, _, err := loadFundingStored(ctx, db, false, w.NewerHeadID)
		if err != nil || page.Scan.ID != w.NewerScanID || page.Scan.SourceIdentity != s.Scope.Funding.SourceIdentity || page.Scan.From != w.From || page.Scan.To != w.To || len(page.Scan.UserIDs) != len(s.Scope.Funding.UserIDs) {
			return errors.Join(ErrWalletMonthCaptureIntegrity, err)
		}
		for i, id := range page.Scan.UserIDs {
			if id != s.Scope.Funding.UserIDs[i] {
				return ErrWalletMonthCaptureIntegrity
			}
		}
	}
	return nil
}
