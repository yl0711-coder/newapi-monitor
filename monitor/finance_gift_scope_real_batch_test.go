//go:build unix

package monitor

import (
	"context"
	"net/url"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

// Opt-in local acceptance: the manifest contains paths to already exported
// original evidence. This test never establishes a source connection.
func TestFinanceGiftScopeRealBatchCoverageAndCache(t *testing.T) {
	manifest := os.Getenv("MONITOR_GIFT_SCOPE_REPAIR_EVIDENCE_MANIFEST")
	backup := os.Getenv("MONITOR_GIFT_SCOPE_REPAIR_SNAPSHOT")
	mainPath := os.Getenv("MONITOR_GIFT_SCOPE_REPAIR_MAIN_SNAPSHOT")
	if manifest == "" || backup == "" || mainPath == "" {
		t.Skip("requires private evidence manifest and closed local snapshots")
	}
	var paths []string
	if _, err := giftLocalReadJSON(manifest, &paths); err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	dir := filepath.Join(t.TempDir(), "job")
	plan, confirmation, err := PrepareFinanceGiftLocalJob(ctx, backup, dir, paths)
	if err != nil {
		t.Fatal(err)
	}
	users, hours := map[int64]bool{}, map[int64]bool{}
	var expected int64
	var lastHour int64
	for i, target := range plan.Targets {
		users[target.UserID], hours[target.HourTs] = true, true
		expected += int64(plan.Rows[i])
		lastHour = max(lastHour, target.HourTs)
	}
	if len(users) < 2 || len(hours) < 2 {
		t.Fatal("real batch acceptance requires multiple users and hours")
	}
	db, closeDB, err := giftLocalDatabase(filepath.Join(dir, "usage-facts.db"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(closeDB)
	mainURI := (&url.URL{Scheme: "file", Path: mainPath, RawQuery: "mode=ro&immutable=1"}).String()
	mainDB, err := gorm.Open(sqlite.Open(mainURI), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := mainDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	m := &Monitor{storeDB: mainDB, usageFactsDB: db, cfg: Settings{FinanceStartDate: "2026-05-01"}}
	policies, err := loadChannelBusinessGroupPolicies(ctx, mainDB)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	excluded := map[int64]bool{}
	for _, a := range accounts {
		excluded[a.UserID] = true
	}
	// Repair includes zero-quota requests, but financial scope coverage only
	// counts monetary events of non-internal users when groups are excluded.
	var expectedScopeDelta int64
	if channelBusinessGroupsExcluded(policies) {
		for _, target := range plan.Targets {
			if excluded[target.UserID] {
				continue
			}
			var count int64
			if err := db.Model(&FinanceGiftBoundaryEvent{}).
				Where("source_epoch=? AND user_id=? AND hour_ts=? AND COALESCE(group_known,0)=0 AND quota<>0", target.SourceEpoch, target.UserID, target.HourTs).
				Count(&count).Error; err != nil {
				t.Fatal(err)
			}
			expectedScopeDelta += count
		}
	}
	seed, err := financeStartHour(m.cfg.FinanceStartDate)
	if err != nil {
		t.Fatal(err)
	}
	to := lastHour + 3600
	readCoverage := func() financeGiftAllocationResult {
		t.Helper()
		v, err := m.loadFinanceGiftAllocationForScope(ctx, seed, seed, to, policies, excluded)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	fingerprint := func(full bool) string {
		t.Helper()
		v, err := m.financeReportSourceFingerprintForScope(ctx, seed, to, full)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	before, moneyBefore := readCoverage(), giftMixedMonetarySnapshot(t, db)
	fullBefore, baseBefore := fingerprint(true), fingerprint(false)
	for attempt := 0; attempt < 2; attempt++ {
		result, err := runFinanceGiftLocalJob(ctx, dir, confirmation, func(ctx context.Context, _ time.Duration) error { return ctx.Err() })
		if err != nil || result.Status != "complete" || result.Remaining != 0 {
			t.Fatalf("batch=%+v error=%v", result, err)
		}
		var updated int64
		for _, e := range result.Entries {
			updated += int64(e.RowsUpdated)
		}
		want := expected
		if attempt > 0 {
			want = 0
		}
		if updated != want || giftMixedMonetarySnapshot(t, db) != moneyBefore {
			t.Fatal("unexpected repair count or changed money/cursor")
		}
	}
	after := readCoverage()
	if before.Coverage.ScopeUnknownEvents-after.Coverage.ScopeUnknownEvents != expectedScopeDelta {
		t.Fatalf("financial coverage delta differs from monetary scope evidence: expected=%d before=%+v after=%+v", expectedScopeDelta, before.Coverage, after.Coverage)
	}
	if after.Coverage.ScopeUnknownEvents > 0 && after.Coverage.Complete {
		t.Fatal("partial real repair published as complete")
	}
	if fingerprint(true) == fullBefore || fingerprint(false) != baseBefore {
		t.Fatal("gift repair must invalidate full report but preserve base components")
	}
	if after.Coverage.Complete {
		// A real batch can close a historical reporting window even though
		// later history remains incomplete. Verify daily gift allocations add
		// up without interpreting this as complete revenue/cost coverage.
		var dailyNet, dailyGift int64
		for from := seed; from < to; from += 24 * 3600 {
			part, err := financeGiftSubrange(after, from, min(from+24*3600, to))
			if err != nil || !part.Coverage.Complete {
				t.Fatalf("verified real daily gift allocation unavailable: %v", err)
			}
			if err := addEconomicsInt64(&dailyNet, part.Allocation.PeriodNetUsageMicroUSD); err != nil {
				t.Fatal(err)
			}
			if err := addEconomicsInt64(&dailyGift, part.Allocation.PeriodGiftConsumptionMicroUSD); err != nil {
				t.Fatal(err)
			}
		}
		if dailyNet != after.Allocation.PeriodNetUsageMicroUSD || dailyGift != after.Allocation.PeriodGiftConsumptionMicroUSD {
			t.Fatal("real daily gift allocations do not reconcile with whole window")
		}
		t.Log("verified real daily gift allocation totals reconcile; this is not complete profit coverage")
	}
	t.Logf("real users=%d hours=%d rows=%d; monetary scope delta=%d; unknown=%d -> %d; complete=%t; full cache invalidated; base cache preserved; retry updates=0", len(users), len(hours), expected, expectedScopeDelta, before.Coverage.ScopeUnknownEvents, after.Coverage.ScopeUnknownEvents, after.Coverage.Complete)
}
