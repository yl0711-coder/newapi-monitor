package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestIsolationRejectsHardLinkedStateWithoutChangingOriginal(t *testing.T) {
	for _, name := range []string{"monitor.db", "monitor.db-wal", "evidence.db", "usage-facts.db", "backups/snapshot/monitor.db"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "receiver")
			digest := strings.Repeat("a", 64)
			lock, err := prepareState(dir, digest)
			if err != nil {
				t.Fatal(err)
			}
			if err := lock.Close(); err != nil {
				t.Fatal(err)
			}
			original := filepath.Join(t.TempDir(), "original.db")
			const sentinel = "do-not-touch-original-store"
			if err := os.WriteFile(original, []byte(sentinel), 0600); err != nil {
				t.Fatal(err)
			}
			alias := filepath.Join(dir, name)
			if err := os.MkdirAll(filepath.Dir(alias), 0700); err != nil {
				t.Fatal(err)
			}
			if err := os.Link(original, alias); err != nil {
				t.Fatal(err)
			}
			if resumed, err := prepareState(dir, digest); err == nil {
				_ = resumed.Close()
				t.Fatal("hard-linked store accepted")
			}
			b, err := os.ReadFile(original)
			if err != nil || string(b) != sentinel {
				t.Fatal("original store changed", err)
			}
		})
	}
}
