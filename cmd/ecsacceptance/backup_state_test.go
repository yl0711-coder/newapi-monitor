package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRestartWithMandatoryMigrationBackup(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	digest := strings.Repeat("a", 64)
	lock, err := prepareState(dir, digest)
	if err != nil {
		t.Fatal(err)
	}
	lock.Close()
	backup := filepath.Join(dir, "backups", "pre-migrate-test")
	if err := os.MkdirAll(backup, 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(backup, "monitor.db"), []byte("preserved"), 0600); err != nil {
		t.Fatal(err)
	}
	for range 3 {
		lock, err := prepareState(dir, digest)
		if err != nil {
			t.Fatal(err)
		}
		lock.Close()
	}
	if got, err := os.ReadFile(filepath.Join(backup, "monitor.db")); err != nil || string(got) != "preserved" {
		t.Fatal("backup changed on resume", err)
	}
}

func TestBackupResumeRejectsUnsafeTree(t *testing.T) {
	for _, kind := range []string{"root-link", "child-link", "public-directory", "unrelated-directory"} {
		t.Run(kind, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			digest := strings.Repeat("a", 64)
			lock, err := prepareState(dir, digest)
			if err != nil {
				t.Fatal(err)
			}
			lock.Close()
			backup := filepath.Join(dir, "backups")
			switch kind {
			case "root-link":
				err = os.Symlink(t.TempDir(), backup)
			case "child-link":
				err = os.Mkdir(backup, 0700)
				if err == nil {
					err = os.Symlink(t.TempDir(), filepath.Join(backup, "snapshot"))
				}
			case "public-directory":
				err = os.Mkdir(backup, 0755)
			case "unrelated-directory":
				err = os.Mkdir(filepath.Join(dir, "other"), 0700)
			}
			if err != nil {
				t.Fatal(err)
			}
			if lock, err := prepareState(dir, digest); err == nil {
				lock.Close()
				t.Fatal("unsafe backup tree accepted")
			}
		})
	}
}
