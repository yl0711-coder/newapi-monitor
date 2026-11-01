package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/monthlydraft"
)

func TestReadInputBoundaries(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "input.json")
	if err := os.WriteFile(path, []byte(`{"version":1}`), 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := readInput(path, 64); err != nil || string(got) != `{"version":1}` {
		t.Fatal(string(got), err)
	}
	link := filepath.Join(dir, "link.json")
	if err := os.Symlink(path, link); err != nil {
		t.Fatal(err)
	}
	for _, bad := range []string{"relative.json", dir, link, filepath.Join(dir, "missing.json")} {
		if _, err := readInput(bad, 64); err == nil {
			t.Fatal("invalid input accepted", bad)
		}
	}
	if _, err := readInput(path, 4); err == nil {
		t.Fatal("oversized input accepted")
	}
}

func TestRunRequiresExplicitScopeAndHead(t *testing.T) {
	for _, args := range [][]string{nil, {"-apply"}, {"-group-id=1", "-month=2026-09"}, {"-expected-head=", "-group-id=1", "-month=2026-09", "positional"}} {
		var out, stderr bytes.Buffer
		if err := run(context.Background(), args, &out, &stderr); err == nil || out.Len() != 0 {
			t.Fatal("invalid arguments accepted", args, err)
		}
	}
}

func TestSafeErrorDoesNotExposePrivateEvidence(t *testing.T) {
	private := "secret key and account money"
	for _, err := range []error{errors.New(private), errors.Join(monthlydraft.ErrGenerationIntegrity, errors.New(private))} {
		if msg := safeError(err); msg == "" || strings.Contains(msg, private) {
			t.Fatal(msg)
		}
	}
}
