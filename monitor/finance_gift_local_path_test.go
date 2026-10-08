//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinanceGiftLocalDatabasePathReopen(t *testing.T) {
	for _, name := range []string{"ascii", "中文", strings.Repeat("a", 100), strings.Repeat("中文", 40), "space # percent% query?"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), name)
			if err := os.Mkdir(dir, 0700); err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(dir, "receiver.db")
			if err := giftLocalWriteNew(path, nil); err != nil {
				t.Fatal(err)
			}
			for pass := 0; pass < 2; pass++ {
				db, closeDB, err := giftLocalDatabase(path)
				if err != nil {
					t.Fatalf("open %d: %v", pass, err)
				}
				if err := db.Exec("PRAGMA journal_mode=WAL").Error; err != nil {
					closeDB()
					t.Fatal(err)
				}
				if err := db.AutoMigrate(&financeGiftHandoffCommit{}); err != nil {
					closeDB()
					t.Fatalf("migrate %d: %v", pass, err)
				}
				closeDB()
			}
			cwd, err := os.Getwd()
			if err != nil {
				t.Fatal(err)
			}
			relative, err := filepath.Rel(cwd, path)
			if err != nil {
				t.Fatal(err)
			}
			rw, closeRW, err := giftLocalDatabase(relative)
			if err != nil {
				t.Fatalf("relative reopen: %v", err)
			}
			if err := rw.AutoMigrate(&financeGiftHandoffCommit{}); err != nil {
				closeRW()
				t.Fatal(err)
			}
			closeRW()
			ro, closeRO, err := giftLocalReadonlyDatabase(relative)
			if err != nil {
				t.Fatal(err)
			}
			defer closeRO()
			var count int64
			if err := ro.Raw("SELECT COUNT(*) FROM local_gift_handoff_commits").Scan(&count).Error; err != nil {
				t.Fatal(err)
			}
			connection, err := ro.DB()
			if err != nil {
				t.Fatal(err)
			}
			if _, err := connection.Exec("INSERT INTO local_gift_handoff_commits VALUES ('must-not-write',0,'')"); err == nil {
				t.Fatal("readonly database accepted write")
			}
		})
	}
}

func TestFinanceGiftHandoffRelativePaths(t *testing.T) {
	source, digest, receiver, _ := giftHandoffPreviewFixture(t)
	cwd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	relative := func(path string) string {
		t.Helper()
		result, err := filepath.Rel(cwd, path)
		if err != nil {
			t.Fatal(err)
		}
		return result
	}
	before := giftLocalFileHash(t, receiver)
	job := relative(filepath.Join(t.TempDir(), "handoff 中文 # % ?"))
	_, confirmation, err := PrepareFinanceGiftLocalHandoff(context.Background(), relative(source), digest, relative(receiver), job)
	if err != nil {
		t.Fatal(err)
	}
	noWait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	for attempt, wantRows := range []int{3, 0} {
		result, err := runFinanceGiftLocalHandoff(context.Background(), job, confirmation, noWait, nil)
		rows := 0
		for _, entry := range result.Entries {
			rows += entry.RowsUpdated
		}
		if err != nil || result.Status != "complete" || result.Remaining != 0 || rows != wantRows {
			t.Fatalf("attempt %d: %+v, rows=%d, err=%v", attempt, result, rows, err)
		}
	}
	preview, err := PreviewFinanceGiftLocalHandoff(context.Background(), relative(source), digest, filepath.Join(job, "usage-facts.db"))
	if err != nil || preview.MatchedTargets != 1 || preview.RowsToUpdate != 0 {
		t.Fatalf("preview %+v: %v", preview, err)
	}
	if giftLocalFileHash(t, receiver) != before {
		t.Fatal("handoff modified original receiver")
	}
}
