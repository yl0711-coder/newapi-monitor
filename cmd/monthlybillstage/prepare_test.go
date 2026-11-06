package main

import (
	"bytes"
	"context"
	"testing"
)

func TestPrepareCommandRequiresExplicitInputsAndNoActivation(t *testing.T) {
	for _, args := range [][]string{
		{"prepare"}, {"prepare", "-write"}, {"prepare", "-apply"},
		{"prepare", "-group-id=1", "-month=2026-09", "-output-root=/tmp"},
		{"prepare", "-group-id=1", "-month=2026-09", "extra"},
		{"prepare", "-group-id=1", "-month=2026-09", "-output-root=/tmp", "-scope-source=history", "-history=/private/history"},
		{"prepare", "-group-id=1", "-month=2026-09", "-output-root=/tmp", "-scope-source=history", "-organization-scope=/private/roster"},
	} {
		var out, stderr bytes.Buffer
		if err := run(context.Background(), args, &out, &stderr); err == nil || out.Len() != 0 {
			t.Fatal("invalid or activating command accepted", args, err)
		}
	}
}

func TestPrepareRetainedCommandRequiresExplicitInputsAndNoActivation(t *testing.T) {
	for _, args := range [][]string{
		{"prepare-retained"}, {"prepare-retained", "-write"}, {"prepare-retained", "-apply"},
		{"prepare-retained", "-group-id=1", "-month=2026-09", "-output-root=/tmp"},
		{"prepare-retained", "-group-id=1", "-month=2026-09", "-expected-head=", "-output-root=/tmp", "-material=/tmp/data"},
		{"prepare-retained", "-group-id=1", "-month=2026-09", "-expected-head=", "-output-root=/tmp", "extra"},
	} {
		var out, stderr bytes.Buffer
		if err := run(context.Background(), args, &out, &stderr); err == nil || out.Len() != 0 {
			t.Fatal("invalid or activating retained command accepted", args, err)
		}
	}
}
