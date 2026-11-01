package monthlydraft

import (
	"context"
	"os"
	"path/filepath"
	"sync"
	"testing"
)

func TestReceiverFilePreviewCreateAndNeverOverwrite(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "receiver.sqlite")
	ctx := context.Background()
	if result, err := InitializeGenerationReceiverFile(ctx, path, false); err != nil || result.Applied {
		t.Fatal(result, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 0 {
		t.Fatal("preview created files", entries, err)
	}
	if result, err := InitializeGenerationReceiverFile(ctx, path, true); err != nil || !result.Applied {
		t.Fatal(result, err)
	}
	info, err := os.Stat(path)
	if err != nil || info.Mode().Perm() != 0600 {
		t.Fatal(info, err)
	}
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, apply := range []bool{false, true} {
		if _, err := InitializeGenerationReceiverFile(ctx, path, apply); err == nil {
			t.Fatal("existing receiver accepted")
		}
	}
	after, err := os.ReadFile(path)
	if err != nil || generationHash(before) != generationHash(after) {
		t.Fatal("existing receiver changed", err)
	}
	db := generationDB(t, path, true)
	for _, table := range []string{"portal_monthly_generation_inputs", "portal_monthly_generation_selections", "portal_monthly_draft_tasks"} {
		var n int
		if err := db.QueryRow("SELECT COUNT(*) FROM " + table).Scan(&n); err != nil || n != 0 {
			t.Fatal(table, n, err)
		}
	}
	var integrity string
	if err := db.QueryRow("PRAGMA integrity_check").Scan(&integrity); err != nil || integrity != "ok" {
		t.Fatal(integrity, err)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatal("temporary files left", entries, err)
	}
}

func TestReceiverFileConcurrentCreateAndFailure(t *testing.T) {
	dir := t.TempDir()
	if err := os.Chmod(dir, 0700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, "receiver.sqlite")
	var wg sync.WaitGroup
	results := make(chan bool, 2)
	for range 2 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			r, err := InitializeGenerationReceiverFile(context.Background(), path, true)
			results <- err == nil && r.Applied
		}()
	}
	wg.Wait()
	close(results)
	success := 0
	for ok := range results {
		if ok {
			success++
		}
	}
	if success != 1 {
		t.Fatal("expected one winner", success)
	}
	if entries, err := os.ReadDir(dir); err != nil || len(entries) != 1 {
		t.Fatal(entries, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := InitializeGenerationReceiverFile(canceled, filepath.Join(dir, "canceled.sqlite"), true); err == nil {
		t.Fatal("canceled init accepted")
	}
	link := filepath.Join(dir, "dangling.sqlite")
	if err := os.Symlink(filepath.Join(dir, "missing"), link); err != nil {
		t.Fatal(err)
	}
	if _, err := InitializeGenerationReceiverFile(context.Background(), link, true); err == nil {
		t.Fatal("symlink target accepted")
	}
	if _, err := os.Lstat(filepath.Join(dir, "missing")); !os.IsNotExist(err) {
		t.Fatal("symlink followed", err)
	}
}
