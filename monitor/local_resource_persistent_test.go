package monitor

import (
	"database/sql"
	"os"
	"path/filepath"
	"testing"
)

// This actor is intentionally a separate process: exit 23 means simulated
// abrupt termination, NOT a passing test. Only the subsequent verifier may
// certify the committed transaction and WAL recovery on the same disk volume.
func TestLocalResourcePersistentCrashWriter(t *testing.T) {
	if os.Getenv("MONITOR_TEST_PERSISTENCE_PHASE") != "crash-write" {
		t.Skip("isolated crash actor only")
	}
	if os.Getenv("MONITOR_TEST_PERSISTENCE_DIR") != "/data/recovery" {
		t.Fatal("unrecognized persistent target")
	}
	dir := "/data/recovery"
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "wal.db")
	if _, err := os.Lstat(path); !os.IsNotExist(err) {
		t.Fatal("refusing to overwrite existing evidence", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if _, err := db.Exec("PRAGMA journal_mode=WAL; PRAGMA synchronous=FULL; PRAGMA wal_autocheckpoint=0; CREATE TABLE recovery_probe(id INTEGER PRIMARY KEY, amount INTEGER); INSERT INTO recovery_probe VALUES(1,123456)"); err != nil {
		t.Fatal(err)
	}
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec("INSERT INTO recovery_probe VALUES(2,999999)"); err != nil {
		t.Fatal(err)
	}
	// Do not Close/Rollback: model process death while a transaction is open.
	os.Exit(23)
}

func TestLocalResourcePersistentCrashVerify(t *testing.T) {
	if os.Getenv("MONITOR_TEST_PERSISTENCE_PHASE") != "verify" {
		t.Skip("isolated disk restart verifier only")
	}
	if os.Getenv("MONITOR_TEST_PERSISTENCE_DIR") != "/data/recovery" {
		t.Fatal("unrecognized persistent target")
	}
	path := "/data/recovery/wal.db"
	if _, err := os.Stat(path); err != nil {
		t.Fatal("missing pre-crash database", err)
	}
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()
	var count, amount int64
	if err := db.QueryRow("SELECT count(*),COALESCE(sum(amount),0) FROM recovery_probe").Scan(&count, &amount); err != nil || count != 1 || amount != 123456 {
		t.Fatalf("lost commit or exposed uncommitted data: %d %d %v", count, amount, err)
	}
	var check string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&check); err != nil || check != "ok" {
		t.Fatal("WAL recovery integrity", check, err)
	}
	if _, err := db.Exec("INSERT INTO recovery_probe VALUES(3,42)"); err != nil {
		t.Fatal("post-restart write failed", err)
	}
	if err := db.QueryRow("SELECT count(*),sum(amount) FROM recovery_probe").Scan(&count, &amount); err != nil || count != 2 || amount != 123498 {
		t.Fatal("post-restart duplicate or missing value", count, amount, err)
	}
}
