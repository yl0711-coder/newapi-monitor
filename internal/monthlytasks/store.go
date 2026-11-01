package monthlytasks

import (
	"bytes"
	"context"
	"database/sql"
	"database/sql/driver"
	"encoding/json"
	"errors"
	"time"
)

// Init is explicit receiver-only setup, not a production startup migration.
// The connection must use a bounded SQLite busy timeout no longer than the
// operation deadline. No upstream/network calls occur inside transactions.
func Init(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var sqliteVersion string
	if err := db.QueryRowContext(ctx, "SELECT sqlite_version()").Scan(&sqliteVersion); err != nil {
		return err
	}
	// CREATE IF NOT EXISTS can acquire a read lock before INSERT. Reserve the
	// writer first, including on an already initialized database, so concurrent
	// process starts don't deadlock while upgrading their read transactions.
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	if _, err := conn.ExecContext(ctx, "BEGIN IMMEDIATE"); err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			cleanup, stop := context.WithTimeout(context.Background(), time.Second)
			defer stop()
			if _, err := conn.ExecContext(cleanup, "ROLLBACK"); err != nil {
				// Don't return a connection with uncertain transaction state
				// to the shared pool, even if the original deadline has expired.
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
	}()
	for _, query := range []string{
		`CREATE TABLE IF NOT EXISTS portal_monthly_draft_task_control (id INTEGER PRIMARY KEY CHECK(id=1),version INTEGER NOT NULL)`,
		`INSERT OR IGNORE INTO portal_monthly_draft_task_control(id,version) VALUES(1,1)`,
		`CREATE TABLE IF NOT EXISTS portal_monthly_draft_tasks (
id TEXT PRIMARY KEY NOT NULL CHECK(length(id)=32),
task_key TEXT NOT NULL UNIQUE CHECK(length(task_key)=64),
group_id INTEGER NOT NULL CHECK(group_id>0),
descriptor BLOB NOT NULL CHECK(length(descriptor)<=2048),
state TEXT NOT NULL CHECK(state IN ('queued','running','draft_ready','failed','invalidated')),
attempts INTEGER NOT NULL CHECK(attempts BETWEEN 0 AND 3),
created_at INTEGER NOT NULL,updated_at INTEGER NOT NULL,
started_at INTEGER NOT NULL,lease_until INTEGER NOT NULL,lease_token TEXT NOT NULL CHECK(length(lease_token) IN (0,32)),
retry_at INTEGER NOT NULL,result_sha256 TEXT NOT NULL CHECK(length(result_sha256) IN (0,64)),
failure_code TEXT NOT NULL CHECK(length(failure_code)<=32))`,
		`CREATE UNIQUE INDEX IF NOT EXISTS monthly_draft_active_group ON portal_monthly_draft_tasks(group_id) WHERE state IN ('queued','running')`,
	} {
		if _, err := conn.ExecContext(ctx, query); err != nil {
			return err
		}
	}
	var version int
	if err := conn.QueryRowContext(ctx, `SELECT version FROM portal_monthly_draft_task_control WHERE id=1`).Scan(&version); err != nil {
		return err
	}
	if version != 1 {
		return ErrCorrupt
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if _, err := conn.ExecContext(ctx, "COMMIT"); err != nil {
		return err
	}
	committed = true
	return nil
}

// Acquire the SQLite write reservation BEFORE any capacity/state read. This
// serializes competing processes too; a Go mutex alone cannot fence workers
// after a restart or prevent a read-to-write upgrade race.
func (s *Store) write(ctx context.Context, fn func(context.Context, *sql.Tx, int64) error) error {
	return WriteReceiver(ctx, s.db, func(ctx context.Context, tx *sql.Tx) error {
		now := s.now().UnixMilli() // Read after waiting for the database reservation.
		if now <= 0 {
			return ErrClock
		}
		return fn(ctx, tx, now)
	})
}

type rowReader interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func scoped(ctx context.Context, db rowReader, owner Owner, id string) (record, error) {
	if !owner.valid() || !validHex(id, 16) {
		return record{}, ErrNotFound
	}
	r, err := load(ctx, db, "id", id)
	if errors.Is(err, sql.ErrNoRows) || (err == nil && r.Request.Owner != owner) {
		return record{}, ErrNotFound
	}
	return r, err
}

func load(ctx context.Context, db rowReader, column, value string) (record, error) {
	if column != "id" && column != "task_key" {
		return record{}, ErrInvalid
	}
	var r record
	var raw []byte
	var groupID int64
	err := db.QueryRowContext(ctx, `SELECT id,task_key,group_id,
CASE WHEN length(descriptor)<=2048 THEN descriptor ELSE NULL END,
state,attempts,created_at,updated_at,started_at,lease_until,lease_token,retry_at,result_sha256,failure_code
FROM portal_monthly_draft_tasks WHERE `+column+`=?`, value).Scan(
		&r.ID, &r.Key, &groupID, &raw, &r.State, &r.Attempts, &r.CreatedAtMS, &r.UpdatedAtMS,
		&r.started, &r.until, &r.token, &r.RetryAtMS, &r.ResultSHA256, &r.FailureCode)
	if err != nil {
		return record{}, err
	}
	if !validHex(r.ID, 16) || !validHex(r.Key, 32) || hash(raw) != r.Key || json.Unmarshal(raw, &r.Request) != nil {
		return record{}, ErrCorrupt
	}
	canonical, err := requestBytes(r.Request)
	if err != nil || !bytes.Equal(canonical, raw) || r.Request.Owner.GroupID != groupID || !validRecord(r) {
		return record{}, ErrCorrupt
	}
	r.descriptor = string(raw)
	return r, nil
}

func validRecord(r record) bool {
	if r.CreatedAtMS <= 0 || r.UpdatedAtMS < r.CreatedAtMS || r.Attempts < 0 || r.Attempts > maxAttempts {
		return false
	}
	if r.State == Running {
		return r.Attempts > 0 && r.started >= r.CreatedAtMS && r.UpdatedAtMS >= r.started &&
			r.until > r.UpdatedAtMS && r.until <= r.started+attemptLimit.Milliseconds() &&
			validHex(r.token, 16) && r.RetryAtMS == 0 && r.ResultSHA256 == "" && r.FailureCode == ""
	}
	if r.token != "" || r.started != 0 || r.until != 0 {
		return false
	}
	switch r.State {
	case Queued:
		return r.Attempts < maxAttempts && r.RetryAtMS == 0 && r.ResultSHA256 == "" && r.FailureCode == ""
	case DraftReady:
		return r.Attempts > 0 && r.RetryAtMS == 0 && validHex(r.ResultSHA256, 32) && r.FailureCode == ""
	case Failed:
		return r.Attempts > 0 && r.ResultSHA256 == "" &&
			((validFailure(r.FailureCode) && r.RetryAtMS == r.UpdatedAtMS+retryDelay.Milliseconds()) ||
				(r.FailureCode == "attempts_exhausted" && r.Attempts == maxAttempts && r.RetryAtMS == 0))
	case Invalidated:
		return r.RetryAtMS == 0 && r.ResultSHA256 == "" && r.FailureCode == "inputs_changed"
	}
	return false
}

func save(ctx context.Context, tx *sql.Tx, r record) error {
	if !validRecord(r) {
		return ErrCorrupt
	}
	updated, err := tx.ExecContext(ctx, `UPDATE portal_monthly_draft_tasks SET state=?,attempts=?,updated_at=?,
started_at=?,lease_until=?,lease_token=?,retry_at=?,result_sha256=?,failure_code=? WHERE id=? AND task_key=?`,
		r.State, r.Attempts, r.UpdatedAtMS, r.started, r.until, r.token, r.RetryAtMS, r.ResultSHA256, r.FailureCode, r.ID, r.Key)
	if err != nil {
		return err
	}
	n, err := updated.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrCorrupt
	}
	return nil
}

func activeCapacity(ctx context.Context, tx *sql.Tx, groupID int64) error {
	var total, inGroup int
	if err := tx.QueryRowContext(ctx, `SELECT COUNT(*),COALESCE(SUM(CASE WHEN group_id=? THEN 1 ELSE 0 END),0)
FROM portal_monthly_draft_tasks WHERE state IN ('queued','running')`, groupID).Scan(&total, &inGroup); err != nil {
		return err
	}
	if total >= maxActive || inGroup != 0 {
		return ErrBusy
	}
	return nil
}
