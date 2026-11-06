package monthlytasks

import (
	"context"
	"database/sql"
	"database/sql/driver"
)

// WriteReceiver runs bounded, trusted storage code in an explicitly initialized
// v1 LOCAL task receiver. It neither initializes nor owns the database. Callers
// must use tx, respect ctx and avoid network/filesystem work or money calculation
// in fn. Reserving the writer before reading also serializes separate processes.
//
// A pinned connection is essential: SQLite may leave a transaction open after
// COMMIT fails, although database/sql has already marked sql.Tx done. Never put
// that uncertain connection back into the pool for later reads or writes.
func WriteReceiver(parent context.Context, db *sql.DB, fn func(context.Context, *sql.Tx) error) error {
	if db == nil || fn == nil {
		return ErrInvalid
	}
	ctx, cancel := context.WithTimeout(parent, operationLimit)
	defer cancel()
	conn, err := db.Conn(ctx)
	if err != nil {
		return err
	}
	defer func() { _ = conn.Close() }()
	tx, err := conn.BeginTx(ctx, nil)
	if err != nil {
		// BEGIN can fail because an underlying transaction was already open.
		// Do not return a connection of uncertain state, even on early failure.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
		return err
	}
	committed := false
	defer func() {
		if !committed {
			if err := tx.Rollback(); err != nil {
				_ = conn.Raw(func(any) error { return driver.ErrBadConn })
			}
		}
	}()
	locked, err := tx.ExecContext(ctx, `UPDATE portal_monthly_draft_task_control SET version=version WHERE id=1 AND version=1`)
	if err != nil {
		return err
	}
	n, err := locked.RowsAffected()
	if err != nil {
		return err
	}
	if n != 1 {
		return ErrCorrupt
	}
	if err := fn(ctx, tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}
