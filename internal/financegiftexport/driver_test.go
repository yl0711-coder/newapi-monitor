package financegiftexport

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync/atomic"
	"testing"
)

// A small deterministic SQL transport double. No network or MySQL service is
// involved; integration with a real MySQL optimizer remains a separate gate.
type giftExportDB struct {
	data                  [][]driver.Value
	targetData            map[[2]int64][][]driver.Value
	queries               []string
	options               driver.TxOptions
	planType              string
	planRows              string
	commitErr, readErr    error
	committed, rolledBack bool
	waitForCancel         bool
	activeTx              bool
	transactions          int
	sourceUUID            string
}

type giftExportDriver struct{ db *giftExportDB }
type giftExportConn struct{ db *giftExportDB }
type giftExportTx struct{ db *giftExportDB }
type giftExportRows struct {
	columns []string
	data    [][]driver.Value
	index   int
	err     error
}

var giftExportDriverID atomic.Int64

func giftExportTestDB(t *testing.T, data [][]driver.Value) (*sql.DB, *giftExportDB) {
	t.Helper()
	state := &giftExportDB{data: data, planType: "range", planRows: "3000", sourceUUID: "synthetic-uuid"}
	name := fmt.Sprintf("gift-export-test-%d", giftExportDriverID.Add(1))
	sql.Register(name, &giftExportDriver{state})
	db, err := sql.Open(name, "")
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db, state
}

func (d *giftExportDriver) Open(string) (driver.Conn, error) { return &giftExportConn{d.db}, nil }
func (c *giftExportConn) Close() error                       { return nil }
func (c *giftExportConn) Prepare(string) (driver.Stmt, error) {
	return nil, errors.New("prepared statements are not expected")
}
func (c *giftExportConn) Begin() (driver.Tx, error) {
	return nil, errors.New("explicit readonly transaction options required")
}
func (c *giftExportConn) BeginTx(_ context.Context, options driver.TxOptions) (driver.Tx, error) {
	c.db.options = options
	c.db.activeTx = true
	c.db.transactions++
	return &giftExportTx{c.db}, nil
}
func (c *giftExportConn) QueryContext(ctx context.Context, query string, args []driver.NamedValue) (driver.Rows, error) {
	if query == "SELECT @@server_uuid, DATABASE(), CURRENT_USER()" {
		if _, ok := ctx.Deadline(); !ok || len(args) != 0 {
			return nil, errors.New("unbounded source identity query")
		}
		c.db.queries = append(c.db.queries, query)
		return &giftExportRows{columns: []string{"uuid", "database", "user"}, data: [][]driver.Value{{c.db.sourceUUID, "fixture", "monitor_ro@localhost"}}}, nil
	}
	largePage := strings.Contains(query, " AND id IN (")
	if _, ok := ctx.Deadline(); !ok || (!largePage && len(args) != 3) || (largePage && (len(args) <= 3 || len(args) > largeHourPageRows+3)) {
		return nil, errors.New("missing bounded query context or unexpected target")
	}
	user, userOK := args[0].Value.(int64)
	hour, hourOK := args[1].Value.(int64)
	data := c.db.data
	if c.db.targetData != nil {
		var found bool
		data, found = c.db.targetData[[2]int64{user, hour}]
		if !found {
			return nil, errors.New("unexpected batch target")
		}
	} else if user != 7 || hour != 3600 {
		return nil, errors.New("unexpected target")
	}
	if !userOK || !hourOK || args[2].Value != hour+3600 {
		return nil, errors.New("unexpected query bounds")
	}
	c.db.queries = append(c.db.queries, query)
	if strings.HasPrefix(query, "EXPLAIN ") {
		return &giftExportRows{columns: []string{"table", "type", "key", "rows"}, data: [][]driver.Value{{"logs", c.db.planType, "idx_user_time", c.db.planRows}}}, nil
	}
	if !strings.HasPrefix(query, "SELECT /*+ MAX_EXECUTION_TIME(2000) */") {
		return nil, errors.New("unexpected statement")
	}
	if c.db.waitForCancel {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	if largePage {
		ids := make(map[int64]bool)
		for _, arg := range args[3:] {
			id, ok := arg.Value.(int64)
			if !ok || ids[id] {
				return nil, errors.New("invalid pinned page IDs")
			}
			ids[id] = true
		}
		var filtered [][]driver.Value
		for _, row := range data {
			if ids[row[0].(int64)] {
				filtered = append(filtered, row)
			}
		}
		data = filtered
	}
	return &giftExportRows{columns: []string{"id", "user_id", "created_at", "type", "quota", "group"}, data: data, err: c.db.readErr}, nil
}
func (t *giftExportTx) Commit() error {
	t.db.activeTx = false
	t.db.committed = t.db.commitErr == nil
	return t.db.commitErr
}
func (t *giftExportTx) Rollback() error {
	t.db.activeTx = false
	t.db.rolledBack = true
	return nil
}
func (r *giftExportRows) Columns() []string { return r.columns }
func (r *giftExportRows) Close() error      { return nil }
func (r *giftExportRows) Next(dest []driver.Value) error {
	if r.index == len(r.data) {
		if r.err != nil {
			return r.err
		}
		return io.EOF
	}
	copy(dest, r.data[r.index])
	r.index++
	return nil
}
