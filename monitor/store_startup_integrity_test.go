package monitor

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestStartupIntegrityRetainsReadOnlyValidationAndParentCancellation(t *testing.T) {
	path := filepath.Join(t.TempDir(), "startup.db")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec("CREATE TABLE evidence(id INTEGER PRIMARY KEY, value TEXT); INSERT INTO evidence VALUES (1, 'retained')"); err != nil {
		_ = db.Close()
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	checked, err := preflightStartupStoreIntegrity(t.Context(), path)
	if err != nil || !checked {
		t.Fatalf("valid startup store rejected: checked=%v err=%v", checked, err)
	}
	after, err := os.ReadFile(path)
	if err != nil || sha256.Sum256(before) != sha256.Sum256(after) {
		t.Fatal("startup integrity probe modified its source")
	}
	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := preflightStartupStoreIntegrity(canceled, path); !errors.Is(err, context.Canceled) {
		t.Fatalf("startup check lost parent cancellation: %v", err)
	}
	expired, cancelDeadline := context.WithDeadline(t.Context(), time.Now().Add(-time.Second))
	defer cancelDeadline()
	if _, err := preflightStartupStoreIntegrity(expired, path); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("startup check extended parent deadline: %v", err)
	}
}

func TestStartupIntegrityRejectsCorruptionAndKeepsFiniteBudget(t *testing.T) {
	if storeStartupIntegrityTimeout <= storeIntegrityTimeout || storeStartupIntegrityTimeout >= storeBackupTimeout {
		t.Fatal("startup needs a separate finite budget within the backup operation limit")
	}
	path := filepath.Join(t.TempDir(), "corrupt.db")
	if err := os.WriteFile(path, []byte("not SQLite"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := preflightStartupStoreIntegrity(t.Context(), path); err == nil {
		t.Fatal("startup accepted corrupt evidence")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatal("startup must retain the corrupt file for recovery")
	}
}
