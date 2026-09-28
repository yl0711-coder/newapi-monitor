//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Inputs are closed private snapshots. Only the newly prepared temporary job
// is mutated; this test never connects to production or an upstream source.
func TestFinanceGiftHandoffLimitedRealSnapshot(t *testing.T) {
	source, digest, receiver := os.Getenv("MONITOR_GIFT_LIMIT_SOURCE_JOB"), os.Getenv("MONITOR_GIFT_LIMIT_SOURCE_SHA256"), os.Getenv("MONITOR_GIFT_LIMIT_RECEIVER")
	if source == "" || digest == "" || receiver == "" {
		t.Skip("requires private closed local source job and receiver snapshot")
	}
	paths := []string{filepath.Join(source, "usage-facts.db"), filepath.Join(source, "audit.jsonl"), receiver}
	hashes := make([]string, len(paths))
	for i, path := range paths {
		hashes[i] = giftLocalFileHash(t, path)
	}
	ctx := context.Background()
	before, err := PreviewFinanceGiftLocalHandoff(ctx, source, digest, receiver)
	if err != nil || before.BlockedTargets != 0 || before.ReadyTargets == 0 {
		t.Fatal(before, err)
	}
	job := filepath.Join(t.TempDir(), "handoff")
	_, confirmation, err := PrepareFinanceGiftLocalHandoff(ctx, source, digest, receiver, job)
	if err != nil {
		t.Fatal(err)
	}
	total, completed := 0, false
	for run := 0; run < financeGiftScopeBatchLimit; run++ {
		result, err := runFinanceGiftLocalHandoffLimited(ctx, job, confirmation, 2, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }, nil)
		if err != nil {
			t.Fatal(result, err)
		}
		hours, rows := 0, 0
		for _, entry := range result.Entries {
			rows += entry.RowsUpdated
			if entry.RowsUpdated > 0 {
				hours++
			}
		}
		if hours > 2 || (result.Status != "paused_limit" && result.Status != "complete") {
			t.Fatal("allowance exceeded", result)
		}
		total += rows
		t.Logf("run=%d repaired_hours=%d rows=%d remaining=%d status=%s", run+1, hours, rows, result.Remaining, result.Status)
		if result.Status == "complete" {
			completed = true
			break
		}
	}
	if !completed || total != before.RowsToUpdate {
		t.Fatal("incomplete or duplicated repair", total, before)
	}
	after, err := PreviewFinanceGiftLocalHandoff(ctx, source, digest, filepath.Join(job, "usage-facts.db"))
	if err != nil || after.Status != "already_matched" || after.RowsToUpdate != 0 || after.BlockedTargets != 0 {
		t.Fatal(after, err)
	}
	finalHash := giftLocalFileHash(t, filepath.Join(job, "usage-facts.db"))
	repeat, err := runFinanceGiftLocalHandoffLimited(ctx, job, confirmation, 1, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }, nil)
	if err != nil || repeat.Status != "complete" || repeat.Remaining != 0 {
		t.Fatal(repeat, err)
	}
	if finalHash != giftLocalFileHash(t, filepath.Join(job, "usage-facts.db")) {
		t.Fatal("completed replay changed database")
	}
	for i, path := range paths {
		if hashes[i] != giftLocalFileHash(t, path) {
			t.Fatal("original input changed")
		}
	}
	t.Logf("repaired %d records in new private copy; original snapshots and audit unchanged", total)
}
