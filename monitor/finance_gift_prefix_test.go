//go:build unix

package monitor

import (
	"context"
	"net/url"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func TestFinanceGiftVerifiedPeriodsBeforeScopeGap(t *testing.T) {
	m, evidence, targets, hour := giftReportLocalFixture(t)
	ctx := context.Background()
	source, err := giftLocalSource(ctx, evidence)
	if err != nil {
		t.Fatal(err)
	}
	defer source.Close()
	policies := map[string]bool{"test": false}
	read := func(from int64, excluded map[int64]bool) financeGiftAllocationResult {
		t.Helper()
		r, err := m.loadFinanceGiftAllocationForScope(ctx, hour, from, hour+7200, policies, excluded)
		if err != nil {
			t.Fatal(err)
		}
		r, err = excludeFinanceGiftUsers(r, excluded)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	project := func(r financeGiftAllocationResult) *financeOperatingReport {
		t.Helper()
		statement := func() financeStatementView {
			return financeStatementView{UserConsumption: financeMoneyPointer(economicsMoney(110_000_000))}
		}
		report := &financeOperatingReport{
			Statement: statement(),
			Periods: []financePeriodView{
				{From: hour, To: hour + 3600, Statement: statement()},
				{From: hour, To: hour + 7200, Statement: statement()},
				{From: hour + 3600, To: hour + 7200, Statement: statement()},
			},
			Days: []financeDailyView{{From: hour, To: hour + 3600, Statement: statement()}, {From: hour, To: hour + 7200, Statement: statement()}},
		}
		if err := applyFinanceGiftAllocation(&report.Statement, r); err != nil {
			t.Fatal(err)
		}
		if err := applyFinanceGiftBreakdowns(report, r); err != nil {
			t.Fatal(err)
		}
		if r.Coverage.Complete || report.Statement.OperatingRevenue != nil {
			t.Fatal("scope gap published complete total")
		}
		for _, s := range []financeStatementView{report.Periods[1].Statement, report.Periods[2].Statement, report.Days[1].Statement} {
			if s.RegistrationGiftConsumption != nil || s.OperatingRevenue != nil {
				t.Fatal("period crossing gap published partial gift as complete")
			}
		}
		return report
	}
	beforeMoney := giftMixedMonetarySnapshot(t, m.usageFactsStore())
	if project(read(hour, nil)).Periods[0].Statement.OperatingRevenue != nil {
		t.Fatal("missing first hour was ignored")
	}
	for i, target := range targets[:2] {
		if _, err := repairFinanceGiftBoundaryScope(ctx, m.usageFactsStore(), source, target.SourceEpoch, target.HourTs, target.UserID, time.Now().Unix()); err != nil {
			t.Fatal(err)
		}
		if i == 0 && project(read(hour, nil)).Periods[0].Statement.OperatingRevenue != nil {
			t.Fatal("another user's gap in same hour was ignored")
		}
	}
	for _, tc := range []struct {
		name     string
		excluded map[int64]bool
		gift     string
	}{
		{"all", nil, "50000000"}, {"internal excluded", map[int64]bool{8: true}, "30000000"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			r := read(hour, tc.excluded)
			report := project(r)
			for _, s := range []financeStatementView{report.Periods[0].Statement, report.Days[0].Statement} {
				if s.RegistrationGiftConsumption == nil || s.RegistrationGiftConsumption.MicroUSD != tc.gift || s.OperatingRevenue == nil {
					t.Fatalf("verified prefix not published: %+v", s)
				}
			}
			if project(read(hour+3600, tc.excluded)).Periods[0].Statement.OperatingRevenue != nil {
				t.Fatal("unknown opening wallet was ignored")
			}
		})
	}
	if giftMixedMonetarySnapshot(t, m.usageFactsStore()) != beforeMoney {
		t.Fatal("projection changed money/cursors")
	}
	// A malformed hour proof is not merely a scope gap and cannot produce a prefix.
	if err := m.usageFactsStore().Model(&FinanceGiftBoundaryState{}).Where("hour_ts=?", hour+3600).Update("content_hash", "invalid").Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.loadFinanceGiftAllocationForScope(ctx, hour, hour, hour+7200, policies, nil); err == nil {
		t.Fatal("corrupt evidence accepted")
	}
}

// Opt-in real evidence test. Both inputs are already downloaded closed local
// databases; no source connection or repair is performed here.
func TestFinanceGiftVerifiedPeriodsRealSnapshot(t *testing.T) {
	mainPath := os.Getenv("MONITOR_GIFT_SCOPE_REPAIR_MAIN_SNAPSHOT")
	if mainPath == "" {
		t.Skip("requires closed local main snapshot")
	}
	db := copyGiftScopeSnapshotForTest(t)
	uri := (&url.URL{Scheme: "file", Path: mainPath, RawQuery: "mode=ro&immutable=1"}).String()
	mainDB, err := gorm.Open(sqlite.Open(uri), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := mainDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	m := &Monitor{storeDB: mainDB, usageFactsDB: db}
	ctx := context.Background()
	policies, err := loadChannelBusinessGroupPolicies(ctx, mainDB)
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := m.loadFinanceInternalAccounts(ctx)
	if err != nil {
		t.Fatal(err)
	}
	excluded := map[int64]bool{}
	for _, account := range accounts {
		excluded[account.UserID] = true
	}
	seed, err := financeStartHour("2026-05-01")
	if err != nil {
		t.Fatal(err)
	}
	to := int64(1783483200) // 2026-07-08 12:00 CST, fixed acceptance window.
	before := giftMixedMonetarySnapshot(t, db)
	r, err := m.loadFinanceGiftAllocationForScope(ctx, seed, seed, to, policies, excluded)
	if err != nil {
		t.Fatal(err)
	}
	r, err = excludeFinanceGiftUsers(r, excluded)
	if err != nil {
		t.Fatal(err)
	}
	if r.Coverage.Complete || r.Coverage.ScopeUnknownEvents != 20 || r.verifiedPrefix == nil {
		t.Fatalf("expected last batch's 20-event gap: %+v", r.Coverage)
	}
	end := r.verifiedPrefix.Coverage.ToTs
	standalone, err := m.loadFinanceGiftAllocationForScope(ctx, seed, seed, end, policies, excluded)
	if err != nil {
		t.Fatal(err)
	}
	standalone, err = excludeFinanceGiftUsers(standalone, excluded)
	if err != nil {
		t.Fatal(err)
	}
	if !standalone.Coverage.Complete || !reflect.DeepEqual(standalone.Allocation, r.verifiedPrefix.Allocation) {
		t.Fatal("prefix differs from independent closed-range query")
	}
	report := &financeOperatingReport{}
	for from := seed; from < to; from += 86400 {
		report.Days = append(report.Days, financeDailyView{From: from, To: min(from+86400, to)})
	}
	if err := applyFinanceGiftBreakdowns(report, r); err != nil {
		t.Fatal(err)
	}
	var published int
	var dayGift int64
	var closedTo int64
	for _, day := range report.Days {
		gift := day.Statement.RegistrationGiftConsumption
		if day.To > end {
			if gift != nil {
				t.Fatal("incomplete real day published")
			}
			continue
		}
		if gift == nil {
			t.Fatal("complete real day hidden")
		}
		value, ok := financeMoneyInt64(*gift)
		if !ok {
			t.Fatal("invalid gift amount")
		}
		if err := addEconomicsInt64(&dayGift, value); err != nil {
			t.Fatal(err)
		}
		published++
		closedTo = day.To
	}
	wholeClosedDays, err := financeGiftSubrange(standalone, seed, closedTo)
	if err != nil || dayGift != wholeClosedDays.Allocation.PeriodGiftConsumptionMicroUSD {
		t.Fatalf("closed-day reconciliation failed: %v", err)
	}
	if giftMixedMonetarySnapshot(t, db) != before {
		t.Fatal("read-only projection changed facts")
	}
	t.Logf("real complete days=%d; prefix through=%d; overall unknown=%d remains incomplete; daily gifts reconcile", published, end, r.Coverage.ScopeUnknownEvents)
}
