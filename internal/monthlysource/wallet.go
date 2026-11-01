// Package monthlysource collects bounded, private billing source observations.
// It has no scheduler, HTTP routes, credentials or automatic database setup.
package monthlysource

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"
)

const WalletBatchLimit = 200
const walletCaptureTimeout = 3 * time.Second
const walletObservationBasis = "database_observation_not_month_boundary"

var ErrWalletCaptureBusy = errors.New("monthly wallet observation already running")
var walletCaptureSlots = make(chan struct{}, 1)

// Absent users remain explicit. A missing user or NULL quota is never a zero
// balance; negative balances are valid. No usernames, keys or email are read.
type WalletUser struct {
	UserID       int64  `json:"user_id"`
	Exists       bool   `json:"exists"`
	BalanceQuota *int64 `json:"balance_quota"`
	UsedQuota    *int64 `json:"used_quota"`
}

// Times bound the database read; neither time is a source-log cutoff. NewAPI
// may flush wallet counters asynchronously, even in a READ ONLY transaction.
// There is intentionally no Month, Complete, Reviewed or publishable field.
type WalletObservation struct {
	Version        int          `json:"version"`
	Basis          string       `json:"basis"`
	SourceIdentity string       `json:"source_identity"`
	StartedAtMS    int64        `json:"started_at_ms"`
	CompletedAtMS  int64        `json:"completed_at_ms"`
	Users          []WalletUser `json:"users"`
}

func digest(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func validDigest(value string) bool {
	if len(value) != 64 || strings.ToLower(value) != value {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}

// Pin the expected server AND database, not a freely supplied display label.
// A failover/restore to a different server must be explicitly revalidated.
func WalletSourceIdentity(serverUUID, database string) (string, error) {
	for _, value := range []string{serverUUID, database} {
		if strings.TrimSpace(value) != value || value == "" || len(value) > 128 || strings.ContainsAny(value, "\x00\r\n") {
			return "", errors.New("invalid wallet source identity")
		}
	}
	return digest([]byte("mysql-wallet-v1\x00" + serverUUID + "\x00" + database)), nil
}

func walletIDs(ids []int64) ([]int64, error) {
	if len(ids) == 0 || len(ids) > WalletBatchLimit {
		return nil, errors.New("wallet observation requires 1 to 200 explicit users")
	}
	result := append([]int64(nil), ids...)
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	for i, id := range result {
		if id <= 0 || (i > 0 && result[i-1] == id) {
			return nil, errors.New("invalid or duplicate wallet user")
		}
	}
	return result, nil
}

// CaptureWallet performs two bounded SELECTs, at most one capture at a time.
// It never falls back to a weaker transaction, expands scope, retries, writes
// the source, or treats a successful SELECT as financial completeness proof.
func CaptureWallet(parent context.Context, db *sql.DB, sourceIdentity string, ids []int64) (WalletObservation, error) {
	return captureWallet(parent, db, sourceIdentity, ids, time.Now)
}

func captureWallet(parent context.Context, db *sql.DB, sourceIdentity string, ids []int64, clock func() time.Time) (WalletObservation, error) {
	var empty WalletObservation
	ordered, err := walletIDs(ids)
	if err != nil || db == nil || !validDigest(sourceIdentity) {
		return empty, errors.New("invalid wallet capture source or scope")
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
	started := clock()
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
	if err != nil || identity != sourceIdentity {
		return empty, errors.New("wallet source identity differs from the pinned source")
	}
	args := make([]any, len(ordered))
	for i, id := range ordered {
		args[i] = id
	}
	query := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,quota,used_quota FROM users WHERE id IN (%s) ORDER BY id LIMIT %d", strings.TrimSuffix(strings.Repeat("?,", len(args)), ","), WalletBatchLimit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return empty, err
	}
	users, err := readWalletRows(rows, ordered)
	if err != nil {
		return empty, err
	}
	if err := tx.Commit(); err != nil {
		return empty, err
	}
	observation := WalletObservation{Version: 1, Basis: walletObservationBasis, SourceIdentity: identity, StartedAtMS: started.UnixMilli(), CompletedAtMS: clock().UnixMilli(), Users: users}
	if err := ValidateWalletObservation(observation); err != nil {
		return empty, err
	}
	return observation, nil
}

func readWalletRows(rows *sql.Rows, ids []int64) ([]WalletUser, error) {
	defer rows.Close()
	result := make([]WalletUser, len(ids))
	byID := make(map[int64]int, len(ids))
	for i, id := range ids {
		result[i].UserID, byID[id] = id, i
	}
	var last int64
	for rows.Next() {
		var id int64
		var balance, used sql.NullInt64
		if err := rows.Scan(&id, &balance, &used); err != nil {
			return nil, err
		}
		i, allowed := byID[id]
		if !allowed || id <= last || !balance.Valid || !used.Valid || used.Int64 < 0 {
			return nil, errors.New("wallet rows contain missing amounts, duplicate or out-of-scope users")
		}
		last = id
		result[i] = WalletUser{UserID: id, Exists: true, BalanceQuota: &balance.Int64, UsedQuota: &used.Int64}
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if err := rows.Close(); err != nil {
		return nil, err
	}
	return result, nil
}

func ValidateWalletObservation(o WalletObservation) error {
	if o.Version != 1 || o.Basis != walletObservationBasis || !validDigest(o.SourceIdentity) || o.StartedAtMS <= 0 || o.CompletedAtMS < o.StartedAtMS || o.CompletedAtMS-o.StartedAtMS > walletCaptureTimeout.Milliseconds() || len(o.Users) == 0 || len(o.Users) > WalletBatchLimit {
		return errors.New("invalid wallet observation version, source, time bounds or scope")
	}
	var last int64
	for _, u := range o.Users {
		if u.UserID <= last || (u.Exists && (u.BalanceQuota == nil || u.UsedQuota == nil || *u.UsedQuota < 0)) || (!u.Exists && (u.BalanceQuota != nil || u.UsedQuota != nil)) {
			return errors.New("invalid wallet observation user or missing amount")
		}
		last = u.UserID
	}
	return nil
}

func walletObservationBytes(o WalletObservation) ([]byte, error) {
	if err := ValidateWalletObservation(o); err != nil {
		return nil, err
	}
	return json.Marshal(o)
}
