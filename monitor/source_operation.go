package monitor

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"time"
)

// Stages are fixed labels, never SQL, credentials or driver error messages.
type sourceOperationError struct {
	stage   string
	elapsed time.Duration
	err     error
}

func (e *sourceOperationError) Error() string { return "source " + e.stage + ": " + e.err.Error() }
func (e *sourceOperationError) Unwrap() error { return e.err }
func sourceOperationStage(err error) string {
	var e *sourceOperationError
	if errors.As(err, &e) {
		return e.stage
	}
	return "unknown"
}
func sourceOperationFailure(stage string, started time.Time, err error) error {
	if err == nil {
		return nil
	}
	return &sourceOperationError{stage: stage, elapsed: time.Since(started), err: err}
}

func sourceProbeConnection(ctx context.Context, db *sql.DB, check func(*sql.Conn) error) error {
	start := time.Now()
	conn, err := db.Conn(ctx)
	if err != nil {
		return sourceOperationFailure("connection_acquire", start, err)
	}
	defer conn.Close()
	err = check(conn)
	if err != nil && (isSourceConnectionFailure(err) || errors.Is(err, context.DeadlineExceeded)) {
		// A failed health probe must not return a stale connection to the pool.
		// Do not close the shared pool or disturb the lease connection.
		_ = conn.Raw(func(any) error { return driver.ErrBadConn })
	}
	return err
}
