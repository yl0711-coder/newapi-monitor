package monitor

import (
	"context"
	"strings"
	"testing"
)

func TestFinanceRechargeContentCorrectionInvalidatesMonthAndDailyCosts(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	t.Cleanup(m.Close)
	for _, domain := range []string{"hour.example", "day.example"} {
		createChannelRechargeVersion(t, m, domain, 1, scope.FromTs, 1, 2)
	}
	ctx := context.Background()
	build := func() (financePeriodComponent, bool) {
		t.Helper()
		component, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "recharge-content-proof", accounts, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return component, hit
	}
	first, _ := build()
	if _, hit := build(); !hit {
		t.Fatal("unchanged inputs did not hit cache")
	}
	if first.Statement.RawCorrectedUpstreamCost == nil || first.Statement.RawCorrectedUpstreamCost.MicroUSD != "16500000" {
		t.Fatalf("fixture correction: %+v", first.Statement)
	}
	var versions []ChannelFinanceVersion
	if err := m.storeDB.Find(&versions).Error; err != nil {
		t.Fatal(err)
	}
	for _, version := range versions {
		revised := strings.Replace(version.SnapshotJSON, `"upstream_recharge_credit":2`, `"upstream_recharge_credit":4`, 1)
		if revised == version.SnapshotJSON || len(revised) != len(version.SnapshotJSON) {
			t.Fatal("fixture must correct content without changing length")
		}
		// Model reviewed historical evidence repair in the local fixture only.
		// IDs, dates, versions, count, lengths, and created_at all stay unchanged.
		if err := m.storeDB.Model(&version).Update("snapshot_json", revised).Error; err != nil {
			t.Fatal(err)
		}
	}
	second, hit := build()
	if hit || second.Statement.RawCorrectedUpstreamCost == nil || second.Statement.RawCorrectedUpstreamCost.MicroUSD != "8250000" {
		t.Fatalf("same-length correction retained stale cost: hit=%t statement=%+v", hit, second.Statement)
	}
	if second.Statement.UpstreamBilledCost == nil || second.Statement.UpstreamBilledCost.MicroUSD != first.Statement.UpstreamBilledCost.MicroUSD {
		t.Fatal("recharge repair changed original bills")
	}
	var daily int64
	for _, day := range second.Days {
		if day.RechargeCorrection.KnownCost.MicroUSD == "" {
			t.Fatal("missing known daily cost")
		}
		value, ok := financeMoneyInt64(day.RechargeCorrection.KnownCost)
		if !ok {
			t.Fatal("invalid daily amount")
		}
		daily += value
	}
	if daily != 8_250_000 || second.Statement.OperatingProfit != nil {
		t.Fatal("updated day/month costs disagree or missing revenue fabricated profit")
	}
}

func TestFinanceRechargeContentProofRangeAndCancellation(t *testing.T) {
	m, scope, _ := dailyBillFixture(t)
	t.Cleanup(m.Close)
	ctx := context.Background()
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.ToTs, 1, 2)
	before, err := m.financeReportSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain=?", "hour.example").Update("snapshot_json", `{bad-future-evidence}`).Error; err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil || before != after {
		t.Fatal("future recharge proof changed historical report", err)
	}
	if err := m.storeDB.Model(&ChannelFinanceVersion{}).Where("domain=?", "hour.example").Update("effective_at", scope.FromTs).Error; err != nil {
		t.Fatal(err)
	}
	current, err := m.financeReportSourceFingerprint(ctx, scope.FromTs, scope.ToTs)
	if err != nil || current == before {
		t.Fatal("invalid historical evidence is invisible to cache", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := m.hashFinanceRechargeVersions(canceled, &strings.Builder{}, scope.ToTs); err == nil {
		t.Fatal("canceled proof read succeeded")
	}
}
