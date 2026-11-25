package localresource

import (
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"syscall"
	"testing"
)

func TestTemporaryRejectsTextAndPermanentFailures(t *testing.T) {
	for _, err := range []error{nil, syscall.EIO, syscall.EACCES, syscall.EROFS, errors.New("database or disk is full"), errors.New("database is locked")} {
		if Temporary(err) {
			t.Fatalf("permanent or untyped error retried: %v", err)
		}
	}
	for _, err := range []error{syscall.ENOSPC, syscall.EDQUOT, fmt.Errorf("local commit: %w", syscall.ENOSPC)} {
		if !Temporary(err) {
			t.Fatalf("typed capacity error not recognized: %v", err)
		}
	}
}

func openClassificationDB(t *testing.T, path string) *sql.DB {
	t.Helper()
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	t.Cleanup(func() { _ = db.Close() })
	return db
}

func TestTemporaryDistinguishesRealSQLiteBusyFromConstraintAndReadOnly(t *testing.T) {
	path := filepath.Join(t.TempDir(), "classification.sqlite")
	a := openClassificationDB(t, path)
	b := openClassificationDB(t, path)
	for _, statement := range []string{"CREATE TABLE records(id INTEGER PRIMARY KEY)", "INSERT INTO records VALUES(1)"} {
		if _, err := a.Exec(statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := b.Exec("PRAGMA busy_timeout=1"); err != nil {
		t.Fatal(err)
	}
	tx, err := a.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec("INSERT INTO records VALUES(2)"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec("INSERT INTO records VALUES(3)"); err == nil || !Temporary(fmt.Errorf("local write: %w", err)) {
		t.Fatal("real SQLite writer contention not retryable", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec("INSERT INTO records VALUES(3)"); err != nil {
		t.Fatal("same connection failed after writer released", err)
	}
	if _, err := b.Exec("INSERT INTO records VALUES(1)"); err == nil || Temporary(err) {
		t.Fatal("constraint failure classified as resource wait", err)
	}
	if _, err := b.Exec("PRAGMA query_only=ON"); err != nil {
		t.Fatal(err)
	}
	if _, err := b.Exec("INSERT INTO records VALUES(4)"); err == nil || Temporary(err) {
		t.Fatal("read-only failure classified as resource wait", err)
	}
}
