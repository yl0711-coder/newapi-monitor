//go:build unix

package monitor

import (
	"context"
	"errors"
	"path/filepath"
	"slices"
	"testing"
	"time"
)

func TestFinanceGiftScopeLocalMixedHoursAndRecovery(t *testing.T) {
	for _, mode := range []string{"ordered", "reversed", "paused", "committed_without_audit"} {
		t.Run(mode, func(t *testing.T) {
			backup, paths, hour := giftMixedLocalFixture(t)
			if mode != "ordered" {
				slices.Reverse(paths)
			}
			originalHash := giftLocalFileHash(t, backup)
			dir := filepath.Join(t.TempDir(), "job")
			ctx := context.Background()
			plan, hash, err := PrepareFinanceGiftLocalJob(ctx, backup, dir, paths)
			if err != nil {
				t.Fatal(err)
			}
			giftAssertLocalProgress(t, dir, hash, 4, 0, 8, 0)
			db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			moneyBefore := giftMixedMonetarySnapshot(t, db)
			m := &Monitor{usageFactsDB: db}
			before, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+7200, map[string]bool{"test": false}, nil)
			if err != nil || before.Coverage.Complete || before.Coverage.ScopeUnknownEvents != 8 {
				t.Fatalf("before=%+v %v", before, err)
			}
			closeDB()
			alreadyUpdated := 0
			if mode == "paused" {
				pausedCtx, cancel := context.WithCancel(ctx)
				defer cancel()
				waits := 0
				partial, err := runFinanceGiftLocalJob(pausedCtx, dir, hash, func(ctx context.Context, d time.Duration) error {
					waits++
					if waits == 2 {
						cancel()
					}
					return ctx.Err()
				})
				if !errors.Is(err, context.Canceled) || partial.Status != "paused" || partial.Remaining != 3 || len(partial.Entries) != 1 {
					t.Fatalf("partial=%+v %v", partial, err)
				}
				alreadyUpdated = partial.Entries[0].RowsUpdated
			}
			if mode == "committed_without_audit" {
				// Simulate process death after the DB commit but before the audit
				// append. Facts, not an audit row, must prevent double publication.
				var e financeGiftLocalEvidence
				if _, err = giftLocalReadJSON(filepath.Join(dir, giftLocalEvidenceName(0)), &e); err != nil {
					t.Fatal(err)
				}
				source, err := giftLocalSource(ctx, []financeGiftLocalEvidence{e})
				if err != nil {
					t.Fatal(err)
				}
				db, closeDB, err = giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
				if err != nil {
					t.Fatal(err)
				}
				target := plan.Targets[0]
				repaired, err := repairFinanceGiftBoundaryScope(ctx, db, source, target.SourceEpoch, target.HourTs, target.UserID, time.Now().Unix())
				source.Close()
				closeDB()
				if err != nil {
					t.Fatal(err)
				}
				alreadyUpdated = repaired.RowsUpdated
			}
			if alreadyUpdated > 0 {
				giftAssertLocalProgress(t, dir, hash, 4, 1, 8, alreadyUpdated)
				db, closeDB, err = giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
				if err != nil {
					t.Fatal(err)
				}
				partial, err := (&Monitor{usageFactsDB: db}).loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+7200, map[string]bool{"test": false}, nil)
				if err != nil || partial.Coverage.Complete || partial.Coverage.ScopeUnknownEvents != int64(8-alreadyUpdated) {
					t.Fatalf("partial published as complete: %+v %v", partial, err)
				}
				if giftMixedMonetarySnapshot(t, db) != moneyBefore {
					t.Fatal("partial repair changed money")
				}
				closeDB()
			}
			for attempt := 0; attempt < 2; attempt++ {
				waits := 0
				result, err := runFinanceGiftLocalJob(ctx, dir, hash, func(ctx context.Context, d time.Duration) error {
					waits++
					if d <= 0 || d > financeGiftScopeBatchInterval {
						t.Fatalf("invalid wait %v", d)
					}
					return ctx.Err()
				})
				if err != nil || result.Status != "complete" || result.Remaining != 0 || len(result.Entries) != 4 || waits != 4 {
					t.Fatalf("result=%+v waits=%d %v", result, waits, err)
				}
				updated := 0
				for _, entry := range result.Entries {
					updated += entry.RowsUpdated
				}
				want := 8 - alreadyUpdated
				if attempt == 1 {
					want = 0
				}
				if updated != want {
					t.Fatalf("updated=%d want=%d", updated, want)
				}
				if alreadyUpdated > 0 && result.Entries[0].Status != "unchanged" {
					t.Fatal("committed hour republished")
				}
				giftAssertLocalProgress(t, dir, hash, 4, 4, 8, 8)
			}
			db, closeDB, err = giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
			if err != nil {
				t.Fatal(err)
			}
			defer closeDB()
			if giftMixedMonetarySnapshot(t, db) != moneyBefore || giftLocalFileHash(t, backup) != originalHash {
				t.Fatal("monetary facts, cursor or original backup changed")
			}
			giftAssertMixedAllocation(t, &Monitor{usageFactsDB: db}, hour)
			if err = giftLocalValidateAudit(filepath.Join(dir, "audit.jsonl"), plan); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func giftAssertMixedAllocation(t *testing.T, m *Monitor, hour int64) {
	t.Helper()
	ctx := context.Background()
	business := map[string]bool{"test": false}
	for _, tc := range []struct {
		name      string
		from, to  int64
		policies  map[string]bool
		excluded  map[int64]bool
		net, gift int64
	}{
		{"business_whole", hour, hour + 7200, business, nil, 165, 95},
		{"business_first", hour, hour + 3600, business, nil, 110, 50},
		{"business_second", hour + 3600, hour + 7200, business, nil, 55, 45},
		{"internal_excluded", hour, hour + 7200, business, map[int64]bool{8: true}, 140, 70},
		{"test_included", hour, hour + 7200, nil, nil, 195, 125},
	} {
		result, err := m.loadFinanceGiftAllocationForScope(ctx, hour, tc.from, tc.to, tc.policies, tc.excluded)
		if err != nil || !result.Coverage.Complete || result.Coverage.ScopeUnknownEvents != 0 || result.Allocation.PeriodNetUsageMicroUSD != tc.net*1_000_000 || result.Allocation.PeriodGiftConsumptionMicroUSD != tc.gift*1_000_000 {
			t.Fatalf("%s: %+v %v", tc.name, result, err)
		}
	}
	whole, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+7200, business, nil)
	if err != nil {
		t.Fatal(err)
	}
	var net, gift int64
	for start := hour; start < hour+7200; start += 3600 {
		part, err := financeGiftSubrange(whole, start, start+3600)
		if err != nil {
			t.Fatal(err)
		}
		net += part.Allocation.PeriodNetUsageMicroUSD
		gift += part.Allocation.PeriodGiftConsumptionMicroUSD
	}
	if net != whole.Allocation.PeriodNetUsageMicroUSD || gift != whole.Allocation.PeriodGiftConsumptionMicroUSD {
		t.Fatal("subranges do not reconcile")
	}
}

func TestFinanceGiftScopeLocalMixedRejectsOneBadTargetBeforeAnyRepair(t *testing.T) {
	backup, paths, _ := giftMixedLocalFixture(t)
	dir := filepath.Join(t.TempDir(), "job")
	plan, hash, err := PrepareFinanceGiftLocalJob(context.Background(), backup, dir, paths)
	if err != nil {
		t.Fatal(err)
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	target := plan.Targets[len(plan.Targets)-1]
	// Damage only the final target after preview. Even the earlier valid
	// targets must remain untouched when preflight detects the mismatch.
	if err = db.Exec("UPDATE finance_gift_boundary_events SET quota=quota+1 WHERE user_id=? AND hour_ts=?", target.UserID, target.HourTs).Error; err != nil {
		t.Fatal(err)
	}
	closeDB()
	result, err := runFinanceGiftLocalJob(context.Background(), dir, hash, func(context.Context, time.Duration) error { return nil })
	if err == nil || result.Status != "rejected" || len(result.Entries) != 0 {
		t.Fatalf("result=%+v %v", result, err)
	}
	db, closeDB, err = giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	defer closeDB()
	var known int64
	if err = db.Model(&FinanceGiftBoundaryEvent{}).Where("group_known=?", true).Count(&known).Error; err != nil || known != 0 {
		t.Fatalf("earlier target mutated: %d %v", known, err)
	}
}
