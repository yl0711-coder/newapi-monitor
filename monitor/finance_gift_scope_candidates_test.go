//go:build unix

package monitor

import (
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/yl0711-coder/newapi-monitor/internal/financegiftexport"
)

func giftCandidateJob(t *testing.T, backup string, evidence []string) (string, string) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "job")
	_, hash, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, evidence)
	if err != nil {
		t.Fatal(err)
	}
	return dir, hash
}

func giftReadCandidates(t *testing.T, dir, hash string) FinanceGiftLocalCandidates {
	t.Helper()
	before := giftLocalJobFileHashes(t, dir)
	got, err := SuggestFinanceGiftLocalCandidates(context.Background(), dir, hash)
	if err != nil || got.Mode != "offline_suggestion_only" || got.RowLimit != 3000 || got.TargetLimit != 10 {
		t.Fatalf("suggestion=%+v error=%v", got, err)
	}
	if !reflect.DeepEqual(before, giftLocalJobFileHashes(t, dir)) {
		t.Fatal("candidate suggestion changed local files")
	}
	plan, hash, planErr := PrepareFinanceGiftReadPlan(context.Background(), dir, hash)
	if got.Status == "ready" {
		if planErr != nil || plan.SourceEpoch != got.SourceEpoch || len(plan.Targets) != len(got.Entries) {
			t.Fatalf("read plan differs from suggestion: %+v %v", plan, planErr)
		}
		for i, target := range plan.Targets {
			e := got.Entries[i]
			if target.UserID != e.UserID || target.HourTs != e.HourTs || target.ExpectedRows != e.Rows || target.LocalContentHash != e.ContentHash {
				t.Fatal("read plan changed candidate identity or budget")
			}
		}
		confirmed, err := financegiftexport.BatchConfirmation(plan)
		if err != nil || confirmed != hash {
			t.Fatal("read plan confirmation invalid", err)
		}
	} else if planErr == nil || hash != "" {
		t.Fatal("blocked/empty candidate produced executable read plan")
	}
	if !reflect.DeepEqual(before, giftLocalJobFileHashes(t, dir)) {
		t.Fatal("read plan changed local files")
	}
	return got
}

func TestFinanceGiftScopeCandidatesBudgets(t *testing.T) {
	for _, tc := range []struct {
		name                     string
		counts                   []int
		planIndex, targets, rows int
		status, reason           string
	}{
		{"exact_rows", []int{1500, 1500}, 0, 2, 3000, "ready", "source_scope_exhausted"},
		{"do_not_skip_next_hour", []int{1500, 1501, 1}, 0, 1, 1500, "ready", "row_budget"},
		{"target_limit", []int{1, 1, 1, 1, 1, 1, 1, 1, 1, 1, 1}, 0, 10, 10, "ready", "target_limit"},
		{"oversize_first", []int{3001, 1}, 1, 0, 0, "blocked", "whole_hour_exceeds_row_limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backup, paths := giftLargeLocalFixture(t, tc.counts...)
			dir, hash := giftCandidateJob(t, backup, paths[tc.planIndex:tc.planIndex+1])
			got := giftReadCandidates(t, dir, hash)
			if got.Status != tc.status || got.StopReason != tc.reason || len(got.Entries) != tc.targets || got.Rows != tc.rows || got.UnknownRows != tc.rows {
				t.Fatalf("wrong bounded suggestion: %+v", got)
			}
			for i, e := range got.Entries {
				if e.UserID != int64(i+7) || e.Rows != tc.counts[i] || e.ContentHash == "" {
					t.Fatal("wrong order, incomplete hour, or missing local proof")
				}
			}
			if tc.status == "blocked" && (got.Blocked == nil || got.Blocked.UserID != 7) {
				t.Fatal("oversize target not identified")
			}
		})
	}
}

func TestFinanceGiftScopeCandidatesDeterministicAndExcludeCompleted(t *testing.T) {
	backup, paths, hour := giftMixedLocalFixture(t)
	dir, hash := giftCandidateJob(t, backup, paths)
	first := giftReadCandidates(t, dir, hash)
	if !reflect.DeepEqual(first, giftReadCandidates(t, dir, hash)) || first.Rows != 8 || len(first.Entries) != 4 {
		t.Fatal("candidate selection is not deterministic")
	}
	for i, e := range first.Entries {
		if e.UserID != int64(7+i%2) || e.HourTs != hour+int64(i/2)*3600 {
			t.Fatal("order must be hour, then user")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	waits := 0
	result, err := runFinanceGiftLocalJob(ctx, dir, hash, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits == 2 {
			cancel()
		}
		return ctx.Err()
	})
	cancel()
	if err == nil || result.Status != "paused" {
		t.Fatalf("expected pause: %+v %v", result, err)
	}
	remaining := giftReadCandidates(t, dir, hash)
	if len(remaining.Entries) != 3 || remaining.Rows != 5 || remaining.Entries[0].UserID != 8 {
		t.Fatalf("completed hour reselected: %+v", remaining)
	}
	_, err = runFinanceGiftLocalJob(context.Background(), dir, hash, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
	if err != nil {
		t.Fatal(err)
	}
	empty := giftReadCandidates(t, dir, hash)
	if empty.Status != "empty" || len(empty.Entries) != 0 || empty.Rows != 0 {
		t.Fatalf("not exhausted: %+v", empty)
	}
}

func TestFinanceGiftScopeCandidatesRejectBadNextTarget(t *testing.T) {
	for _, mutation := range []string{
		"UPDATE finance_user_hour_facts SET consume_quota=consume_quota+1 WHERE user_id=8",
		"UPDATE finance_gift_boundary_events SET quota=quota+1 WHERE user_id=8",
		"UPDATE finance_gift_boundary_states SET status='pending' WHERE user_id=8",
		"UPDATE finance_gift_boundary_states SET rows=0 WHERE user_id=8",
	} {
		t.Run(mutation, func(t *testing.T) {
			backup, paths, _ := giftMixedLocalFixture(t)
			dir, hash := giftCandidateJob(t, backup, paths[:1])
			db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			err = db.Exec(mutation).Error
			closeDB()
			if err != nil {
				t.Fatal(err)
			}
			before := giftLocalJobFileHashes(t, dir)
			got, err := SuggestFinanceGiftLocalCandidates(context.Background(), dir, hash)
			if err == nil || got.Status != "unverified" || len(got.Entries) != 0 {
				t.Fatalf("invalid suggestion: %+v %v", got, err)
			}
			if !reflect.DeepEqual(before, giftLocalJobFileHashes(t, dir)) {
				t.Fatal("rejection changed files")
			}
		})
	}
}

func TestFinanceGiftScopeCandidatesPinsEpoch(t *testing.T) {
	backup, paths, _ := giftMixedLocalFixture(t)
	dir, hash := giftCandidateJob(t, backup, paths[:1])
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	for _, table := range []string{"finance_gift_boundary_events", "finance_gift_boundary_states"} {
		if err := db.Table(table).Where("user_id=8").Update("source_epoch", "other-source").Error; err != nil {
			closeDB()
			t.Fatal(err)
		}
	}
	closeDB()
	got := giftReadCandidates(t, dir, hash)
	if len(got.Entries) != 2 || got.Rows != 5 || got.SourceEpoch != "v1" {
		t.Fatalf("mixed sources: %+v", got)
	}
	for _, e := range got.Entries {
		if e.UserID != 7 {
			t.Fatal("another epoch selected")
		}
	}
}

func TestFinanceGiftScopeCandidatesBudgetIncludesKnownRecords(t *testing.T) {
	backup, paths, hour := giftMixedLocalFixture(t)
	dir, hash := giftCandidateJob(t, backup, paths[:1])
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	prior, err := loadFinanceGiftScopeSnapshot(ctx, db, "v1", hour, 7)
	if err != nil {
		closeDB()
		t.Fatal(err)
	}
	prior.Events[0].Group, prior.Events[0].GroupKnown = "test", true
	prior.Events[0].EvidenceHash = financeGiftBoundaryEventHash(prior.Events[0])
	_, err = replaceFinanceGiftBoundaryUserHour(ctx, db, hour, 7, time.Now().Unix(), "v1", prior.Events)
	closeDB()
	if err != nil {
		t.Fatal(err)
	}
	got := giftReadCandidates(t, dir, hash)
	if got.Rows != 8 || got.UnknownRows != 7 || got.Entries[0].Rows != 3 || got.Entries[0].UnknownRows != 2 {
		t.Fatalf("partial hour budget discarded known records: %+v", got)
	}
}
