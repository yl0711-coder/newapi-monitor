package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strconv"
	"strings"
	"time"
)

const registeredWalletObservationBasis = "registered_database_observation_not_historical_balance"

// Original source fields from ONE users-table view. Registration is metadata;
// wallet values retain their observation time, not an inferred month boundary.
type RegisteredWalletUser struct {
	UserID       int64  `json:"user_id"`
	RegisteredAt int64  `json:"registered_at"`
	BalanceQuota *int64 `json:"balance_quota"`
	UsedQuota    *int64 `json:"used_quota"`
}

// No opening/closing balance, completeness assertion or permission to publish.
// This source collector is not yet connected to history assembly: independent
// historical usage/funding bridges remain necessary before preparing a bill.
type RegisteredWalletObservation struct {
	Version        int                    `json:"version"`
	Basis          string                 `json:"basis"`
	SourceIdentity string                 `json:"source_identity"`
	StartedAtMS    int64                  `json:"started_at_ms"`
	CompletedAtMS  int64                  `json:"completed_at_ms"`
	Users          []RegisteredWalletUser `json:"users"`
}

// CaptureRegisteredWallet shares the existing wallet capture lane and budget.
// Two SELECTs only: verify source identity, then read all four original columns
// for the explicit roster in the SAME repeatable-read READ ONLY transaction.
// It starts no collector/task, retries nothing and writes no source/local data.
func CaptureRegisteredWallet(parent context.Context, db *sql.DB, pin string, ids []int64) (RegisteredWalletObservation, error) {
	return captureRegisteredWallet(parent, db, pin, ids, time.Now)
}

func captureRegisteredWallet(parent context.Context, db *sql.DB, pin string, ids []int64, clock func() time.Time) (RegisteredWalletObservation, error) {
	var empty RegisteredWalletObservation
	ordered, err := walletIDs(ids)
	if parent == nil || db == nil || clock == nil || err != nil || !validDigest(pin) {
		return empty, errors.New("invalid registered wallet source or scope")
	}
	if err := parent.Err(); err != nil {
		return empty, err
	}
	select {
	case walletCaptureSlots <- struct{}{}:
		defer func() { <-walletCaptureSlots }()
	default:
		return empty, ErrWalletCaptureBusy
	}
	ctx, cancel := context.WithTimeout(parent, walletCaptureTimeout)
	defer cancel()
	started := clock().UnixMilli()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return empty, err
	}
	defer func() { _ = tx.Rollback() }()
	var uuid, database string
	if err := tx.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE()").Scan(&uuid, &database); err != nil {
		return empty, err
	}
	identity, err := WalletSourceIdentity(uuid, database)
	if err != nil || identity != pin {
		return empty, errors.New("registered wallet source identity changed")
	}
	args := make([]any, len(ordered))
	for i, id := range ordered {
		args[i] = id
	}
	query := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at,quota,used_quota FROM users WHERE id IN (%s) ORDER BY id LIMIT %d", strings.TrimSuffix(strings.Repeat("?,", len(args)), ","), WalletBatchLimit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return empty, err
	}
	users, err := readRegisteredWalletRows(rows, ordered)
	if err != nil {
		return empty, err
	}
	observation := RegisteredWalletObservation{Version: 1, Basis: registeredWalletObservationBasis, SourceIdentity: identity, StartedAtMS: started, CompletedAtMS: clock().UnixMilli(), Users: users}
	if err := ValidateRegisteredWalletObservation(observation); err != nil {
		return empty, err
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	if err := ctx.Err(); err != nil {
		return empty, err
	}
	return observation, nil
}

// Reject NULL, noncanonical decimals and all floating-point transport values.
// BIGINT arrives as int64 or its exact decimal bytes/text; no float conversion
// is acceptable even when a particular float happens to be an integer.
type registeredWalletInteger struct{ value int64 }

func (v *registeredWalletInteger) Scan(raw any) error {
	var text string
	switch value := raw.(type) {
	case int64:
		v.value = value
		return nil
	case string:
		text = value
	case []byte:
		text = string(value)
	default:
		return errors.New("registered wallet field is not an exact source integer")
	}
	value, err := strconv.ParseInt(text, 10, 64)
	if err != nil || strconv.FormatInt(value, 10) != text {
		return errors.New("registered wallet field is not a canonical int64")
	}
	v.value = value
	return nil
}

func readRegisteredWalletRows(rows *sql.Rows, ids []int64) ([]RegisteredWalletUser, error) {
	defer rows.Close()
	result := make([]RegisteredWalletUser, 0, len(ids))
	for rows.Next() {
		var id, registered, balance, used registeredWalletInteger
		if err := rows.Scan(&id, &registered, &balance, &used); err != nil {
			return nil, err
		}
		if len(result) >= len(ids) || id.value != ids[len(result)] || registered.value <= 0 || used.value < 0 {
			return nil, errors.New("registered wallet rows are missing, unordered or inconsistent")
		}
		result = append(result, RegisteredWalletUser{UserID: id.value, RegisteredAt: registered.value, BalanceQuota: &balance.value, UsedQuota: &used.value})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(result) != len(ids) {
		return nil, errors.New("registered wallet account missing; not a zero account")
	}
	return result, nil
}

func ValidateRegisteredWalletObservation(o RegisteredWalletObservation) error {
	if o.Version != 1 || o.Basis != registeredWalletObservationBasis || !validDigest(o.SourceIdentity) || o.StartedAtMS <= 0 || o.CompletedAtMS < o.StartedAtMS || o.CompletedAtMS-o.StartedAtMS > walletCaptureTimeout.Milliseconds() || len(o.Users) == 0 || len(o.Users) > WalletBatchLimit {
		return errors.New("invalid registered wallet observation version, source, timing or scope")
	}
	var last int64
	for _, user := range o.Users {
		if user.UserID <= last || user.RegisteredAt <= 0 || user.RegisteredAt > o.CompletedAtMS/1000 || user.BalanceQuota == nil || user.UsedQuota == nil || *user.UsedQuota < 0 {
			return errors.New("invalid registered wallet registration, identity or amount")
		}
		last = user.UserID
	}
	return nil
}
