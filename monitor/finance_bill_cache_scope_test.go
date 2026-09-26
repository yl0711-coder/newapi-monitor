package monitor

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"gorm.io/gorm"
)

func financeBillHistoricalScopeFixture(t *testing.T) (*Monitor, stabilityScope, map[string]ChannelUpstreamAccountView, ChannelUpstreamUsageHour) {
	t.Helper()
	m, whole, accounts := dailyBillFixture(t)
	scope := stabilityScope{FromTs: whole.FromTs + 86400, ToTs: whole.ToTs}
	db := m.storeDB
	// Initially this supplier is first seen halfway through the selected day.
	if err := db.Where("hour_ts<?", scope.FromTs+12*3600).Delete(&StabilityHourSample{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Where("hour_ts<?", scope.FromTs+12*3600).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts=?", "hour.example", scope.FromTs+12*3600).
		Updates(map[string]any{"cost_usd": 1, "quota": quotaPerUSD}).Error; err != nil {
		t.Fatal(err)
	}
	prior := ChannelUpstreamUsageHour{Domain: "hour.example", HourTs: scope.FromTs - 3600,
		BucketSeconds: 3600, CostUSD: 1, Quota: quotaPerUSD, UnitPerUSD: quotaPerUSD, Provider: upstreamProviderNewAPI}
	return m, scope, accounts, prior
}

func TestFinanceBillCacheNoticesActivityBeforeSelectedMonth(t *testing.T) {
	m, scope, accounts, prior := financeBillHistoricalScopeFixture(t)
	db, ctx := m.storeDB, context.Background()
	build := func() (financePeriodComponent, bool) {
		t.Helper()
		r, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "history-scope", accounts,
			channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return r, hit
	}
	first, hit := build()
	if hit || first.Statement.UpstreamBilledCost == nil {
		t.Fatal("initial verified bill was not built")
	}
	before, err := m.financeReportSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil {
		t.Fatal(err)
	}
	// Historical backfill proves this account was already used before the range.
	// The missing first half-day is now a gap, even though in-range rows did not change.
	if err := db.Create(&prior).Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil {
		t.Fatal(err)
	}
	second, hit := build()
	if before == after {
		t.Error("whole report fingerprint ignored earlier activity")
	}
	if hit || second.Statement.UpstreamBilledCost != nil || second.UpstreamCoverage.CompleteDomains != 0 {
		t.Error("monthly cache retained complete coverage after historical backfill revealed a gap")
	}
	if err := db.Where("domain=? AND hour_ts=?", prior.Domain, prior.HourTs).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		t.Fatal(err)
	}
	restored, _ := build()
	if restored.Statement.UpstreamBilledCost == nil {
		t.Fatal("removing historical activity did not restore the original boundary")
	}
}

func TestFinanceBillCacheRejectsLifecycleChangeDuringBuild(t *testing.T) {
	m, scope, accounts, prior := financeBillHistoricalScopeFixture(t)
	var changed atomic.Bool
	name := "test:bill_lifecycle_race"
	if err := m.storeDB.Callback().Query().Before("gorm:query").Register(name, func(tx *gorm.DB) {
		if tx.Statement.Table == "channel_finance_versions" && changed.CompareAndSwap(false, true) {
			if err := m.storeDB.Create(&prior).Error; err != nil {
				_ = tx.AddError(err)
			}
		}
	}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = m.storeDB.Callback().Query().Remove(name) })
	_, _, err := m.buildFinancePeriodComponent(context.Background(), scope, scope.ToTs+86400, "race", accounts,
		channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	if !changed.Load() || !errors.Is(err, errFinanceFactsChanged) {
		t.Fatalf("historical publication race not rejected: changed=%t err=%v", changed.Load(), err)
	}
	if entries, _ := m.getFinancePeriodCache().size(); entries != 0 {
		t.Fatal("mixed-boundary component was cached")
	}
}
