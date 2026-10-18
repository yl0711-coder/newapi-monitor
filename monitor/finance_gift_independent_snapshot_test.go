//go:build unix

package monitor

import (
	"context"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

// Uses the same pinned, local cold backups as the cutoff acceptance. This is
// calculation only: immutable database handles, no migration/network/writers.
func TestFinanceGiftIndependentPeriodClosedSnapshot(t *testing.T) {
	mainPath, factsPath := os.Getenv("MONITOR_FINANCE_CUTOFF_MAIN"), os.Getenv("MONITOR_FINANCE_CUTOFF_FACTS")
	if mainPath == "" || factsPath == "" {
		t.Skip("requires explicitly selected closed local main/facts snapshots")
	}
	paths := []string{mainPath, factsPath}
	hashes := []string{giftLocalFileHash(t, mainPath), giftLocalFileHash(t, factsPath)}
	t.Cleanup(func() {
		for i, path := range paths {
			if giftLocalFileHash(t, path) != hashes[i] {
				t.Error("input snapshot changed", i)
			}
		}
	})
	mainDB, closeMain, err := giftLocalReadonlyDatabase(mainPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeMain()
	factsDB, closeFacts, err := giftLocalReadonlyDatabase(factsPath)
	if err != nil {
		t.Fatal(err)
	}
	defer closeFacts()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	m := &Monitor{storeDB: mainDB, usageFactsDB: factsDB, cfg: Settings{FinanceEnabled: true, FinanceStartDate: "2026-05-01", LocalSnapshotOnly: true,
		UsageFactsHistorySourceEpoch: "newapi-hotlogs-complete-20260817-v1", StorePath: filepath.Join(t.TempDir(), "cache-only.db")}}
	defer m.financeGiftEvidenceGuard.close()
	seed, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	end, err := financeStartHour("2026-10-08")
	if err != nil {
		t.Fatal(err)
	}
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
	full, err := m.loadFinanceGiftAllocationForScope(ctx, seed, seed, end, policies, excluded)
	if err != nil {
		t.Fatal(err)
	}
	full, err = excludeFinanceGiftUsers(full, excluded)
	if err != nil {
		t.Fatal(err)
	}
	if full.Coverage.Complete || full.Coverage.ScopeUnknownEvents == 0 || full.verifiedSuffix == nil {
		t.Fatalf("real historical gap lacks an independently verified suffix: %+v", full.Coverage)
	}
	suffixFrom := full.verifiedSuffix.Coverage.FromTs
	narrow, err := m.loadFinanceGiftAllocationForScope(ctx, seed, suffixFrom, end, policies, excluded)
	if err != nil {
		t.Fatal(err)
	}
	narrow, err = excludeFinanceGiftUsers(narrow, excluded)
	if err != nil || !narrow.Coverage.Complete || narrow.Allocation != full.verifiedSuffix.Allocation {
		t.Fatalf("real suffix differs from independent interval query: %+v %v", narrow.Coverage, err)
	}
	report, err := m.buildFinanceOperatingReport(ctx, time.Unix(seed, 0), time.Unix(end, 0))
	if err != nil {
		t.Fatal(err)
	}
	if report.Statement.OperatingRevenue != nil || report.GiftCoverage.Complete || report.Statement.OperatingProfit != nil {
		t.Fatal("historical unknowns promoted into complete revenue/profit")
	}
	verifiedDays, unknownDays := 0, 0
	for _, day := range report.Days {
		if day.From >= suffixFrom && day.To <= end {
			if day.Statement.UserConsumption == nil || day.Statement.OperatingRevenue == nil || day.Statement.RegistrationGiftConsumption == nil {
				t.Fatalf("verified later day still blank: %s", day.Date)
			}
			gift, err := financeGiftSubrange(narrow, day.From, day.To)
			if err != nil {
				t.Fatal(err)
			}
			wantGift := economicsMoney(gift.Allocation.PeriodGiftConsumptionMicroUSD)
			wantRevenue, err := financeSubtract(*day.Statement.UserConsumption, wantGift)
			if err != nil || day.Statement.OperatingRevenue.MicroUSD != wantRevenue.MicroUSD || day.Statement.RegistrationGiftConsumption.MicroUSD != wantGift.MicroUSD {
				t.Fatal("real daily revenue/gift do not reconcile", err)
			}
			verifiedDays++
		} else if day.Statement.OperatingRevenue == nil {
			unknownDays++
		}
	}
	if verifiedDays == 0 || unknownDays == 0 {
		t.Fatal("fixture did not cover both usable periods and real historical gaps")
	}
	verifiedMonths := 0
	for _, period := range report.Periods {
		if period.From < suffixFrom || period.To > end {
			continue
		}
		if period.Statement.OperatingRevenue == nil || period.Statement.RegistrationGiftConsumption == nil {
			t.Fatal("verified monthly subrange still blank")
		}
		var revenue, gifts int64
		for _, day := range report.Days {
			if day.From >= period.From && day.To <= period.To {
				if day.Statement.OperatingRevenue == nil || day.Statement.RegistrationGiftConsumption == nil {
					t.Fatal("complete month includes an unknown daily revenue")
				}
				if _, err := financeAddMoney(&revenue, *day.Statement.OperatingRevenue); err != nil {
					t.Fatal(err)
				}
				if _, err := financeAddMoney(&gifts, *day.Statement.RegistrationGiftConsumption); err != nil {
					t.Fatal(err)
				}
			}
		}
		if period.Statement.OperatingRevenue.MicroUSD != economicsMoney(revenue).MicroUSD || period.Statement.RegistrationGiftConsumption.MicroUSD != economicsMoney(gifts).MicroUSD {
			t.Fatal("real month/day revenue and gift sums differ")
		}
		verifiedMonths++
	}
	warm, err := m.buildFinanceOperatingReport(ctx, time.Unix(seed, 0), time.Unix(end, 0))
	if err != nil {
		t.Fatal(err)
	}
	// Monthly JSON caches intentionally omit this unexported build-only
	// scratch map. Compare every monetary, coverage and public field, excluding
	// only that map (the wire responses were also checked during diagnosis).
	coldDays := append([]financeDailyView(nil), report.Days...)
	warmDays := append([]financeDailyView(nil), warm.Days...)
	for i := range coldDays {
		coldDays[i].ledgerSourceCosts = nil
	}
	for i := range warmDays {
		warmDays[i].ledgerSourceCosts = nil
	}
	if !reflect.DeepEqual(warm.Statement, report.Statement) || !reflect.DeepEqual(warmDays, coldDays) || !reflect.DeepEqual(warm.Periods, report.Periods) {
		t.Fatal("warm cache changed real report money/coverage", err)
	}
	t.Logf("historical_unknown=%d remains; suffix_from=%d; newly_usable_closed_days=%d usable_month_subranges=%d historical_unknown_days=%d; cold/warm=equal; full_revenue/profit remain absent; snapshot hashes unchanged", full.Coverage.ScopeUnknownEvents, suffixFrom, verifiedDays, verifiedMonths, unknownDays)
}
