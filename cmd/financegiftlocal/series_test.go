//go:build unix

package main

import (
	"context"
	"testing"
)

func TestSeriesFlagsRejectMissingAndUnrelated(t *testing.T) {
	for _, action := range []string{"series-check", "series-run"} {
		if _, err := runSeriesAction(context.Background(), action, "", "", nil); err == nil {
			t.Fatal("missing manifest accepted")
		}
		if _, err := runSeriesAction(context.Background(), action, "somewhere", "digest", map[string]bool{"backup": true}); err == nil {
			t.Fatal("unrelated flag accepted")
		}
	}
	if _, err := runSeriesAction(context.Background(), "series-check", "somewhere", "digest", nil); err == nil {
		t.Fatal("check accepted confirmation")
	}
}
