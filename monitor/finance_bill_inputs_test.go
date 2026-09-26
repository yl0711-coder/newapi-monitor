package monitor

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestFinanceBillInputsMatchIndependentWindowsAndDailyProjection(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	ctx, now := context.Background(), scope.ToTs+86400
	finance := channelFinanceSnapshot{domainCosts: map[string]ChannelDomainCost{
		"hour.example": {EffectiveAt: scope.FromTs, RechargePaid: 1, RechargeCredit: 3},
		"day.example":  {EffectiveAt: scope.FromTs, RechargePaid: 1, RechargeCredit: 7},
	}}
	for _, mode := range []string{"whole", "partial_ends"} {
		t.Run(mode, func(t *testing.T) {
			window := scope
			if mode == "partial_ends" {
				window.FromTs += 3600
				window.ToTs -= 3600
			}
			in, err := m.financePeriodBills(ctx, window, accounts, finance, nil)
			if err != nil {
				t.Fatal(err)
			}
			beforeRows := append([]ChannelUpstreamUsageHour(nil), in.rows...)
			for _, query := range []stabilityScope{window, {FromTs: scope.FromTs + 86400, ToTs: window.ToTs}} {
				wantMetrics, wantMoney, err := m.loadFinanceBillWindow(ctx, query, now, in.accounts, finance)
				if err != nil {
					t.Fatal(err)
				}
				metrics, money, err := in.window(query, now, in.accounts)
				if err != nil || !reflect.DeepEqual(metrics, wantMetrics) || !reflect.DeepEqual(money, wantMoney) {
					t.Fatalf("shared window changed precision or bucket checks: %v", err)
				}
			}
			correction := map[string]bool{"hour.example": true, "day.example": true}
			want, err := m.loadFinanceDailyBills(ctx, window, now, accounts, finance, correction)
			if err != nil {
				t.Fatal(err)
			}
			got, err := m.loadFinanceDailyBillsWithInputs(ctx, window, now, accounts, finance, correction, in)
			if err != nil || !reflect.DeepEqual(got, want) || !reflect.DeepEqual(in.rows, beforeRows) {
				t.Fatal("daily projection changed or mutated shared buckets")
			}
		})
	}
}

func TestFinanceBillInputsOnlyReadBucketsOncePerComponent(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	// Exercise closed-day fallback as well: it must use the same bucket input.
	scope.ToTs -= 3600
	counter := &financeAcceptanceSQLCounter{Interface: logger.Default.LogMode(logger.Silent)}
	m.storeDB = m.storeDB.Session(&gorm.Session{Logger: counter})
	build := func(ctx context.Context) bool {
		t.Helper()
		_, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "shared-bills", accounts,
			channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return hit
	}
	ctx := context.Background()
	if build(ctx) || counter.billScans.Load() != 1 || counter.rechargeReads.Load() != 1 || counter.activityScans.Load() != 2 {
		t.Fatalf("cold reads: bills=%d recharge=%d lifecycle=%d", counter.billScans.Load(), counter.rechargeReads.Load(), counter.activityScans.Load())
	}
	if !build(ctx) || counter.billScans.Load() != 1 || counter.rechargeReads.Load() != 1 || counter.activityScans.Load() != 3 {
		t.Fatal("cache hit reread buckets/versions or skipped lifecycle validation")
	}
	if build(context.WithValue(ctx, financeForceRebuildKey{}, true)) || counter.billScans.Load() != 2 || counter.rechargeReads.Load() != 2 {
		t.Fatal("forced build reused another build's bill inputs")
	}
}

func TestFinanceBillInputsRejectWrongScopeAndCancellation(t *testing.T) {
	m := &Monitor{}
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	in := &financeBillInputs{scope: scope}
	for _, bad := range []stabilityScope{{FromTs: 0, ToTs: 7200}, {FromTs: 3600, ToTs: 10800}} {
		if _, err := m.financePeriodBills(context.Background(), bad, nil, channelFinanceSnapshot{}, in); err == nil {
			t.Fatal("mismatched shared inputs accepted")
		}
		if _, _, err := in.window(bad, 10800, nil); err == nil {
			t.Fatal("out-of-bounds subrange accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.financePeriodBills(ctx, scope, nil, channelFinanceSnapshot{}, in); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled build accepted")
	}
}
