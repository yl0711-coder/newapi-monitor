package main

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"
)

func TestHandoffCommandRequiresPinnedMetadata(t *testing.T) {
	path := filepath.Join(t.TempDir(), "context.json")
	if err := os.WriteFile(path, []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	for _, args := range [][]string{
		{"handoff"},
		{"handoff", "-group-id=1", "-month=2026-09", "-context", path, "-export"},
		{"handoff", "-group-id=1", "-month=2026-09", "extra"},
		{"handoff", "-apply"},
	} {
		var out, stderr bytes.Buffer
		if err := run(context.Background(), args, &out, &stderr); err == nil || out.Len() != 0 {
			t.Fatal("invalid handoff accepted", args, err)
		}
	}
}
