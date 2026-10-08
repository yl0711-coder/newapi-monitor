package monitor

import (
	"context"
	"database/sql"
	"errors"
	"path/filepath"
	"testing"
	"time"

	"gorm.io/gorm"
)

func directorySyncFixture(t *testing.T) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	prod, err := sql.Open("sqlite", filepath.Join(t.TempDir(), "directory-source.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = prod.Close() })
	if _, err := prod.Exec(`CREATE TABLE users (id INTEGER PRIMARY KEY, username TEXT, "group" TEXT); INSERT INTO users VALUES (7,'new','paid')`); err != nil {
		t.Fatal(err)
	}
	m.prodDB = prod
	if err := m.storeDB.Create(&UserDirectoryEntry{UserID: 7, Username: "old", SyncedAt: 1}).Error; err != nil {
		t.Fatal(err)
	}
	return m
}

func TestDirectorySyncReleasesSourceBeforeLocalWrite(t *testing.T) {
	m := directorySyncFixture(t)
	called := false
	err := m.storeDB.Callback().Create().Before("gorm:create").Register("test:source-released", func(tx *gorm.DB) {
		if tx.Statement.Table != "user_directory_entries" {
			return
		}
		called = true
		if m.prodDB.Stats().InUse != 0 {
			t.Error("source rows/connection still held")
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		release, err := m.acquireBackgroundSource(ctx)
		if err != nil {
			_ = tx.AddError(err)
			return
		}
		release()
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := m.syncUserDirectory(context.Background()); err != nil {
		t.Fatal(err)
	}
	if !called {
		t.Fatal("local persistence was not tested")
	}
	if got := lookupUserNames(m.storeDB, []int64{7})[7]; got != "new" {
		t.Fatalf("name=%q", got)
	}
}

func TestDirectorySyncFailureRetainsCache(t *testing.T) {
	for _, mode := range []string{"empty", "query_error", "write_error", "cancelled"} {
		t.Run(mode, func(t *testing.T) {
			m := directorySyncFixture(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			switch mode {
			case "empty":
				if _, err := m.prodDB.Exec("DELETE FROM users"); err != nil {
					t.Fatal(err)
				}
			case "query_error":
				if _, err := m.prodDB.Exec("DROP TABLE users"); err != nil {
					t.Fatal(err)
				}
			case "write_error":
				if err := m.storeDB.Callback().Create().Before("gorm:create").Register("test:write-error", func(tx *gorm.DB) { _ = tx.AddError(errors.New("local write failure")) }); err != nil {
					t.Fatal(err)
				}
			case "cancelled":
				cancel()
			}
			err := m.syncUserDirectory(ctx)
			if (err == nil) != (mode == "empty") {
				t.Fatalf("unexpected result: %v", err)
			}
			var entry UserDirectoryEntry
			if err := m.storeDB.First(&entry, "user_id = ?", 7).Error; err != nil {
				t.Fatal(err)
			}
			if entry.Username != "old" || entry.SyncedAt != 1 {
				t.Fatalf("old cache overwritten: %+v", entry)
			}
		})
	}
}
