//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestFinanceGiftHandoffLimitedResume(t *testing.T) {
	job, digest, receiver := giftHandoffRunFixture(t)
	before := giftLocalFileHash(t, receiver)
	for attempt := 0; attempt < 6; attempt++ {
		waits := 0
		result, err := runFinanceGiftLocalHandoffLimited(context.Background(), job, digest, 2, func(ctx context.Context, _ time.Duration) error {
			waits++
			return ctx.Err()
		}, nil)
		wantRows, wantWaits, wantRemaining, wantStatus := 2, 2, 8-2*attempt, "paused_limit"
		if attempt == 4 {
			wantStatus = "complete"
		}
		if attempt == 5 {
			wantRows, wantWaits, wantRemaining, wantStatus = 0, 1, 0, "complete"
		}
		rows := 0
		for _, entry := range result.Entries {
			rows += entry.RowsUpdated
		}
		if err != nil || result.Status != wantStatus || result.Remaining != wantRemaining || rows != wantRows || waits != wantWaits {
			t.Fatalf("attempt %d: result=%+v rows=%d waits=%d err=%v", attempt, result, rows, waits, err)
		}
		db, closeDB, err := giftLocalDatabase(filepath.Join(job, "usage-facts.db"))
		if err != nil {
			t.Fatal(err)
		}
		var commits []financeGiftHandoffCommit
		err = db.Order("\"index\"").Find(&commits).Error
		closeDB()
		wantCommits := (attempt + 1) * 2
		if wantCommits > 10 {
			wantCommits = 10
		}
		if err != nil || len(commits) != wantCommits {
			t.Fatalf("commits %d: %v", len(commits), err)
		}
		for i, commit := range commits {
			if commit.Job != digest || commit.Index != i {
				t.Fatal("resume changed durable target identity", commit)
			}
		}
	}
	if giftLocalFileHash(t, receiver) != before {
		t.Fatal("original receiver changed")
	}
}

func TestFinanceGiftHandoffLimitRejectsBeforeIO(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "must-not-exist")
	for _, limit := range []int{-1, 0, 11} {
		result, err := RunFinanceGiftLocalHandoffLimited(context.Background(), dir, "invalid", limit)
		if err == nil || result.Status != "rejected" || err.Error() != "handoff max-hours must be between 1 and 10" {
			t.Fatal("invalid limit accepted", limit, result, err)
		}
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatal("invalid limit accessed job", err)
	}
}

func TestFinanceGiftHandoffLimitedRecoversUnjournaledCommit(t *testing.T) {
	job, digest, _ := giftHandoffRunFixture(t)
	noWait := func(ctx context.Context, _ time.Duration) error { return ctx.Err() }
	failed, err := runFinanceGiftLocalHandoffLimited(context.Background(), job, digest, 1, noWait, func(financeGiftScopeBatchEntry) error {
		return errors.New("injected fsync failure")
	})
	if err == nil || failed.Status != "audit_failed" || failed.Entries[0].RowsUpdated != 1 {
		t.Fatal(failed, err)
	}
	resumed, err := runFinanceGiftLocalHandoffLimited(context.Background(), job, digest, 1, noWait, nil)
	if err != nil || resumed.Status != "paused_limit" || resumed.Remaining != 8 || len(resumed.Entries) != 2 || resumed.Entries[0].RowsUpdated != 0 || resumed.Entries[1].RowsUpdated != 1 {
		t.Fatal("committed replay consumed allowance or was duplicated", resumed, err)
	}
}

func TestFinanceGiftHandoffLimitCountsHoursNotRows(t *testing.T) {
	source, sourceDigest, receiver, _ := giftHandoffPreviewFixture(t)
	job := filepath.Join(t.TempDir(), "handoff")
	_, digest, err := PrepareFinanceGiftLocalHandoff(context.Background(), source, sourceDigest, receiver, job)
	if err != nil {
		t.Fatal(err)
	}
	result, err := runFinanceGiftLocalHandoffLimited(context.Background(), job, digest, 1, func(ctx context.Context, _ time.Duration) error { return ctx.Err() }, nil)
	if err != nil || result.Status != "complete" || result.Remaining != 0 || len(result.Entries) != 1 || result.Entries[0].RowsUpdated != 3 {
		t.Fatal("user-hour must remain atomic", result, err)
	}
}
