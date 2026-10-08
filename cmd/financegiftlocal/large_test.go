//go:build unix

package main

import (
	"context"
	"testing"
)

func TestLargeActionsRejectMissingOrExtraneousFlags(t *testing.T) {
	for _, action := range []string{"large-read-plan", "large-plan", "large-run", "large-status"} {
		if !isLargeAction(action) {
			t.Fatal("missing routing")
		}
		if _, err := runLargeAction(context.Background(), action, largeOptions{}, nil); err == nil {
			t.Fatal("missing inputs accepted")
		}
		if _, err := runLargeAction(context.Background(), action, largeOptions{}, map[string]bool{"export-dir": true}); err == nil {
			t.Fatal("unrelated flag accepted")
		}
	}
	if isLargeAction("run") {
		t.Fatal("ordinary action intercepted")
	}
}
