package monthlysource

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
)

// Deterministic transport double; the real read-only MySQL test is separate.
type walletDriverState struct {
	mu                                    sync.Mutex
	data                                  [][]driver.Value
	queries                               []string
	args                                  [][]driver.NamedValue
	options                               driver.TxOptions
	beginErr, queryErr, rowErr, commitErr error
	committed, rolledBack                 bool
	block                                 chan struct{}
	uuid, database                        string
}

type walletDriver struct{ s *walletDriverState }
type walletConn struct{ s *walletDriverState }
type walletTx struct{ s *walletDriverState }
type walletRows struct {
	columns []string
	data    [][]driver.Value
	n       int
	err     error
}

var walletDriverSequence atomic.Int64

func walletTestSource(t *testing.T, data [][]driver.Value) (*sql.DB, *walletDriverState, string) {
	t.Helper()
	s := &walletDriverState{data: data, uuid: "synthetic-server", database: "synthetic-source"}
	name := fmt.Sprintf("monthly-wallet-%d", walletDriverSequence.Add(1))
	sql.Register(name, &walletDriver{s})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	id, err := WalletSourceIdentity(s.uuid, s.database)
	if err != nil {
		t.Fatal(err)
	}
	return db, s, id
}

func (d *walletDriver) Open(string) (driver.Conn, error) { return &walletConn{d.s}, nil }
func (c *walletConn) Close() error                       { return nil }
func (c *walletConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("unexpected prepared statement")
}
func (c *walletConn) Begin() (driver.Tx, error) {
	return nil, errors.New("readonly transaction options required")
}
func (c *walletConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.s.mu.Lock()
	defer c.s.mu.Unlock()
	c.s.options = options
	if c.s.beginErr != nil {
		return nil, c.s.beginErr
	}
	return &walletTx{c.s}, nil
}
func (c *walletConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if _, ok := ctx.Deadline(); !ok {
		return nil, errors.New("unbounded source query")
	}
	c.s.mu.Lock()
	c.s.queries = append(c.s.queries, query)
	c.s.args = append(c.s.args, append([]driver.NamedValue(nil), args...))
	if query == "SELECT @@server_uuid, DATABASE()" {
		data := [][]driver.Value{{c.s.uuid, c.s.database}}
		c.s.mu.Unlock()
		return &walletRows{columns: []string{"uuid", "database"}, data: data}, nil
	}
	block, queryErr, rowErr, data := c.s.block, c.s.queryErr, c.s.rowErr, c.s.data
	c.s.mu.Unlock()
	if block != nil {
		close(block)
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if queryErr != nil {
		return nil, queryErr
	}
	if strings.Contains(query, "id,created_at FROM users") {
		return &walletRows{columns: []string{"id", "created_at"}, data: data, err: rowErr}, nil
	}
	if strings.Contains(query, "id,created_at,quota,used_quota FROM users") {
		return &walletRows{columns: []string{"id", "created_at", "quota", "used_quota"}, data: data, err: rowErr}, nil
	}
	return &walletRows{columns: []string{"id", "quota", "used_quota"}, data: data, err: rowErr}, nil
}
func (t *walletTx) Commit() error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.committed = t.s.commitErr == nil
	return t.s.commitErr
}
func (t *walletTx) Rollback() error {
	t.s.mu.Lock()
	defer t.s.mu.Unlock()
	t.s.rolledBack = true
	return nil
}
func (r *walletRows) Columns() []string { return r.columns }
func (r *walletRows) Close() error      { return nil }
func (r *walletRows) Next(dest []driver.Value) error {
	if r.n == len(r.data) {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	copy(dest, r.data[r.n])
	r.n++
	return nil
}
