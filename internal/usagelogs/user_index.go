package usagelogs

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"strings"
	"time"
)

// usageLogsUsernameUserID decides whether an exact username filter may be
// answered through user_id for COUNT/SUM queries, and returns that user_id
// (0 = keep filtering by the username column).
//
// Why: logs has only a single-column username index. COUNT/SUM over a time
// range with `username = ?` walks every log row the user ever produced and
// reads each row to test created_at. Production 2026-11-16: a full-day count
// for one active user scanned ~640k index entries and timed out at 25s, so the
// page lost its page numbers. The (user_id, created_at, type) index answers the
// same COUNT from the index alone in ~0.5s.
//
// It must not change results. logs.username is a snapshot taken when the row
// was written, and new-api lets an admin rename a user, so `username = X` and
// `user_id = id(X)` can select different rows. The switch is made only when
// all three hold, each answered by a single index entry:
//  1. exactly one users row (soft-deleted rows included) has username X;
//  2. that user's earliest log row carries username X, so the user has not
//     been renamed since its first log;
//  3. no log row with username X precedes that earliest row, so no other user
//     wrote logs under name X before this user took it.
//
// A user renamed away from X and later back to X is not detected; that case
// would include the rows written under the interim name.
func usageLogsUsernameUserID(ctx context.Context, db usageLogsQuerier, username string) (int64, error) {
	if username == "" || strings.Contains(username, "%") {
		return 0, nil
	}
	rows, err := db.QueryContext(ctx, "SELECT id FROM users WHERE username = ? LIMIT 2", username)
	if err != nil {
		return 0, err
	}
	var ids []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			rows.Close()
			return 0, err
		}
		ids = append(ids, id)
	}
	err = rows.Err()
	rows.Close()
	if err != nil {
		return 0, err
	}
	if len(ids) != 1 || ids[0] <= 0 {
		return 0, nil
	}
	userID := ids[0]

	// Every lookup is a MIN over a (key, id) index or a primary-key read, which
	// MySQL answers from the index alone. `ORDER BY id LIMIT 1` was not used:
	// on production the optimizer sometimes walked the primary key from the
	// start of the table instead (>8s for 3 of 10 active users, 2026-11-17).
	var firstID, nameFirstID sql.NullInt64
	if err := db.QueryRowContext(ctx, "SELECT MIN(id) FROM logs WHERE user_id = ?", userID).Scan(&firstID); err != nil {
		return 0, err
	}
	if err := db.QueryRowContext(ctx, "SELECT MIN(id) FROM logs WHERE username = ?", username).Scan(&nameFirstID); err != nil {
		return 0, err
	}
	if !firstID.Valid {
		// The user has no rows: user_id matches nothing, so the switch is exact
		// only if no row carries the name either.
		if nameFirstID.Valid {
			return 0, nil
		}
		return userID, nil
	}
	// 3. A row with this name before the user's first row belongs to someone else.
	if !nameFirstID.Valid || nameFirstID.Int64 < firstID.Int64 {
		return 0, nil
	}
	// 2. The user's earliest row must carry this name.
	var firstName string
	if err := db.QueryRowContext(ctx, "SELECT COALESCE(username,'') FROM logs WHERE id = ?", firstID.Int64).Scan(&firstName); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return 0, nil
		}
		return 0, err
	}
	if firstName != username {
		return 0, nil
	}
	return userID, nil
}

// usageLogsWithUserIndex returns f with UserID set when the username filter can
// be answered through user_id; otherwise f unchanged. Callers use it only for
// COUNT/SUM statements; page rows keep the username predicate.
func usageLogsWithUserIndex(ctx context.Context, db usageLogsQuerier, f Filter) (Filter, error) {
	userID, err := usageLogsUsernameUserID(ctx, db, f.Username)
	if err != nil {
		return f, err
	}
	f.userID = userID
	return f, nil
}

// usageLogsWarnFailure records a failed count/sum. Failures are otherwise only
// returned to the page as success=false, which left no server-side trace when
// the page lost its total (2026-11-16). Filter values are not logged.
func usageLogsWarnFailure(phase string, started time.Time, f Filter, err error) {
	if errors.Is(err, context.Canceled) {
		return
	}
	span := int64(0)
	if f.Start > 0 && f.End > f.Start {
		span = (f.End - f.Start) / 3600
	}
	slog.Warn("使用日志从库查询失败", "phase", phase, "elapsed_ms", time.Since(started).Milliseconds(), "range_hours", span,
		"username", f.Username != "", "user_index", f.userID > 0, "err", err)
}
