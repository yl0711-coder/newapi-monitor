//go:build unix

package monitor

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

func TestFinanceGiftPreparedHandoffRecoveryAndInterruptedRun(t *testing.T) {
	prior, priorHash, export, readHash, _ := giftContinuationFixture(t)
	next := filepath.Join(t.TempDir(), "next")
	plan, digest, err := PrepareFinanceGiftLocalContinuation(context.Background(), prior, priorHash, export, readHash, next)
	if err != nil {
		t.Fatal(err)
	}
	// The on-disk state immediately before completion publication is identical.
	if err := os.Remove(filepath.Join(prior, "continuation-complete.json")); err != nil {
		t.Fatal(err)
	}
	before := giftLocalJobFileHashes(t, next)
	recovered, recoveredDigest, err := PrepareFinanceGiftLocalContinuation(context.Background(), prior, priorHash, export, readHash, next)
	if err != nil || recoveredDigest != digest || !reflect.DeepEqual(recovered, plan) || !reflect.DeepEqual(before, giftLocalJobFileHashes(t, next)) {
		t.Fatalf("recovery changed the prepared child: %v", err)
	}
	db, closeDB, err := giftLocalReadonlyDatabase(filepath.Join(next, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	moneyBefore := giftMixedMonetarySnapshot(t, db)
	closeDB()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	waits := 0
	partial, err := runFinanceGiftLocalJob(ctx, next, digest, func(ctx context.Context, _ time.Duration) error {
		waits++
		if waits == 2 { // Startup passed; stop before reading the second target.
			cancel()
		}
		return ctx.Err()
	})
	if !errors.Is(err, context.Canceled) || partial.Status != "paused" || len(partial.Entries) != 1 || partial.Entries[0].RowsUpdated != plan.Rows[0] {
		t.Fatalf("unexpected interrupted result: %+v %v", partial, err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		result, err := runFinanceGiftLocalJob(context.Background(), next, digest, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
		if err != nil || result.Status != "complete" || result.Remaining != 0 || result.Entries[0].RowsUpdated != 0 {
			t.Fatalf("resume repeated committed work: %+v %v", result, err)
		}
		for i, entry := range result.Entries {
			want := 0
			if attempt == 0 && i > 0 {
				want = plan.Rows[i]
			}
			if entry.RowsUpdated != want {
				t.Fatal("incorrect resume update count")
			}
		}
	}
	db, closeDB, err = giftLocalReadonlyDatabase(filepath.Join(next, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	if giftMixedMonetarySnapshot(t, db) != moneyBefore {
		t.Fatal("handoff recovery or resumed execution altered monetary facts/cursors")
	}
	giftAssertMixedAllocation(t, &Monitor{usageFactsDB: db}, plan.Targets[0].HourTs)
}

func TestFinanceGiftPreparedHandoffRejectsAmbiguousRecovery(t *testing.T) {
	for _, scenario := range []string{"different_child", "missing_plan", "modified_database", "started_audit", "changed_evidence", "locked_child", "completed"} {
		t.Run(scenario, func(t *testing.T) {
			prior, priorHash, export, readHash, _ := giftContinuationFixture(t)
			next := filepath.Join(t.TempDir(), "next")
			if _, _, err := PrepareFinanceGiftLocalContinuation(context.Background(), prior, priorHash, export, readHash, next); err != nil {
				t.Fatal(err)
			}
			if scenario != "completed" {
				if err := os.Remove(filepath.Join(prior, "continuation-complete.json")); err != nil {
					t.Fatal(err)
				}
			}
			switch scenario {
			case "different_child":
				next = filepath.Join(t.TempDir(), "other")
			case "missing_plan":
				if err := os.Remove(filepath.Join(next, "plan.json")); err != nil {
					t.Fatal(err)
				}
			case "modified_database":
				db, closeDB, err := giftLocalDatabase(filepath.Join(next, "usage-facts.db"))
				if err != nil {
					t.Fatal(err)
				}
				err = db.Exec("PRAGMA user_version=999").Error
				closeDB()
				if err != nil {
					t.Fatal(err)
				}
			case "started_audit", "changed_evidence":
				name := "audit.jsonl"
				if scenario == "changed_evidence" {
					name = giftLocalEvidenceName(0)
				}
				if err := os.WriteFile(filepath.Join(next, name), []byte("changed"), 0600); err != nil {
					t.Fatal(err)
				}
			case "locked_child":
				lock, err := giftLocalLock(next)
				if err != nil {
					t.Fatal(err)
				}
				defer lock.Close()
			}
			before := giftLocalJobFileHashes(t, prior)
			if _, _, err := PrepareFinanceGiftLocalContinuation(context.Background(), prior, priorHash, export, readHash, next); err == nil {
				t.Fatal("ambiguous handoff recovered")
			}
			if !reflect.DeepEqual(before, giftLocalJobFileHashes(t, prior)) {
				t.Fatal("rejected recovery changed predecessor")
			}
		})
	}
}
