package monthlysource

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"strings"
	"time"
)

// RegistrationTime is platform users.created_at metadata, NOT first traffic,
// money, a historical wallet boundary or permission to issue a bill.
type RegistrationTime struct {
	UserID int64
	At     int64
}

var ErrRegistrationBusy = errors.New("monthly registration lookup busy")
var registrationSlots = make(chan struct{}, 1)

// ReadRegistrationTimes performs bounded primary-key SELECTs only. Unknown,
// NULL, zero, future, duplicate or missing registrations fail as a whole: they
// cannot be replaced with Monitor enrollment or the first available log.
func ReadRegistrationTimes(parent context.Context, db *sql.DB, pin string, ids []int64) ([]RegistrationTime, error) {
	ordered, err := walletIDs(ids)
	if parent == nil || db == nil || err != nil || !validDigest(pin) {
		return nil, errors.New("invalid registration source or scope")
	}
	if err := parent.Err(); err != nil {
		return nil, err
	}
	select {
	case registrationSlots <- struct{}{}:
		defer func() { <-registrationSlots }()
	default:
		return nil, ErrRegistrationBusy
	}
	ctx, cancel := context.WithTimeout(parent, 3*time.Second)
	defer cancel()
	tx, err := db.BeginTx(ctx, &sql.TxOptions{ReadOnly: true, Isolation: sql.LevelRepeatableRead})
	if err != nil {
		return nil, err
	}
	defer func() { _ = tx.Rollback() }()
	var uuid, database string
	if err := tx.QueryRowContext(ctx, "SELECT @@server_uuid, DATABASE()").Scan(&uuid, &database); err != nil {
		return nil, err
	}
	identity, err := WalletSourceIdentity(uuid, database)
	if err != nil || identity != pin {
		return nil, errors.New("registration source identity changed")
	}
	args := make([]any, len(ordered))
	for i, id := range ordered {
		args[i] = id
	}
	query := fmt.Sprintf("SELECT /*+ MAX_EXECUTION_TIME(2000) */ id,created_at FROM users WHERE id IN (%s) ORDER BY id LIMIT %d", strings.TrimSuffix(strings.Repeat("?,", len(args)), ","), WalletBatchLimit+1)
	rows, err := tx.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	result := make([]RegistrationTime, 0, len(ordered))
	now := time.Now().Unix()
	for rows.Next() {
		var id int64
		var at sql.NullInt64
		if err := rows.Scan(&id, &at); err != nil {
			return nil, err
		}
		if len(result) >= len(ordered) || id != ordered[len(result)] || !at.Valid || at.Int64 <= 0 || at.Int64 > now {
			return nil, errors.New("registration metadata missing or inconsistent")
		}
		result = append(result, RegistrationTime{UserID: id, At: at.Int64})
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return nil, err
	}
	if len(result) != len(ordered) {
		return nil, errors.New("registration account missing")
	}
	if err := tx.Commit(); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	return result, nil
}
