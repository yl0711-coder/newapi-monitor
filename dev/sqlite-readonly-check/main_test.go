package main

import (
	"database/sql"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestImmutableStoppedStoreAndSidecarGuards(t *testing.T) {
	name := filepath.Join(t.TempDir(), "stopped.db")
	db, err := sql.Open("sqlite", name)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("PRAGMA journal_mode=WAL; CREATE TABLE facts(id INTEGER PRIMARY KEY); INSERT INTO facts VALUES(1)"); err != nil {
		t.Fatal(err)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	if err := check(name, time.Second, true); err != nil {
		t.Fatal(err)
	}
	for _, suffix := range []string{"-wal", "-shm", "-journal"} {
		if err := os.WriteFile(name+suffix, nil, 0600); err != nil {
			t.Fatal(err)
		}
		if err := check(name, time.Second, true); err == nil || !strings.Contains(err.Error(), suffix) {
			t.Fatalf("%s must be rejected, got %v", suffix, err)
		}
		if err := os.Remove(name + suffix); err != nil {
			t.Fatal(err)
		}
	}
}
