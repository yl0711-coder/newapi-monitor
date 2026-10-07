package monitor

import (
	"context"
	"encoding/json"
	"math"
	"reflect"
	"sort"
	"testing"

	"gorm.io/gorm"
)

func TestFinanceRechargeSameInstantCorrectionAndExactBoundaries(t *testing.T) {
	base := channelRechargeVersion{Version: 1, EffectiveAt: 0, Paid: 1, Credit: 7, Valid: true}
	for _, tc := range []struct {
		name string
		rows []channelRechargeVersion
		want string
	}{
		{"replacement", []channelRechargeVersion{{Version: 2, EffectiveAt: 1800, Paid: 1, Credit: 1, Valid: true}, {Version: 3, EffectiveAt: 1800, Paid: 2, Credit: 14, Valid: true}}, upstreamAdjustedCostComplete},
		{"invalid_superseded", []channelRechargeVersion{{Version: 2, EffectiveAt: 1800}, {Version: 3, EffectiveAt: 1800, Paid: 1, Credit: 7, Valid: true}}, upstreamAdjustedCostComplete},
		{"real_change", []channelRechargeVersion{{Version: 2, EffectiveAt: 1800, Paid: 1, Credit: 8, Valid: true}}, upstreamAdjustedCostBucketAmbiguous},
		{"tiny_real_change", []channelRechargeVersion{{Version: 2, EffectiveAt: 1800, Paid: 1.000000000001, Credit: 7, Valid: true}}, upstreamAdjustedCostBucketAmbiguous},
		{"end_excluded", []channelRechargeVersion{{Version: 2, EffectiveAt: 3600, Paid: 1, Credit: 8, Valid: true}}, upstreamAdjustedCostComplete},
		{"new_invalid", []channelRechargeVersion{{Version: 2, EffectiveAt: 1800, Paid: 1, Credit: 7, Valid: true}, {Version: 3, EffectiveAt: 1800}}, upstreamAdjustedCostBucketAmbiguous},
	} {
		t.Run(tc.name, func(t *testing.T) {
			versions := append([]channelRechargeVersion{base}, tc.rows...)
			if _, _, got := rechargeTermsForBucket(versions, 0, 3600); got != tc.want {
				t.Fatalf("got %s want %s", got, tc.want)
			}
		})
	}
}

func correctionFixtureVersions() []ChannelFinanceVersion {
	return []ChannelFinanceVersion{
		{ID: 1, Domain: "example.com", Version: 1, EffectiveAt: 7200, SnapshotJSON: `{"domain":"example.com","upstream_recharge_paid":1,"upstream_recharge_credit":10,"groups":[{"group":"old","site_multiplier":1.3}],"future_field":{"keep":7}}`},
		{ID: 2, Domain: "example.com", Version: 2, EffectiveAt: 7500, SnapshotJSON: `{"domain":"example.com","upstream_recharge_paid":1,"upstream_recharge_credit":10,"groups":[{"group":"new","site_multiplier":1.7}],"future_field":{"keep":8}}`},
		{ID: 3, Domain: "example.com", Version: 3, EffectiveAt: 86400, SnapshotJSON: `{"domain":"example.com","upstream_recharge_paid":1,"upstream_recharge_credit":7,"channel_rates":[{"channel_id":11,"upstream_group_name":"current"}]}`},
	}
}

func correctionTerms(t *testing.T, rows []ChannelFinanceVersion) []channelRechargeVersion {
	t.Helper()
	var terms []channelRechargeVersion
	for _, row := range rows {
		var snap channelFinanceVersionSnapshot
		if err := json.Unmarshal([]byte(row.SnapshotJSON), &snap); err != nil {
			t.Fatal(err)
		}
		terms = append(terms, channelRechargeVersion{Version: row.Version, EffectiveAt: row.EffectiveAt, Paid: snap.UpstreamRechargePaid,
			Credit: snap.UpstreamRechargeCredit, Valid: validChannelFinanceNumber(snap.UpstreamRechargePaid) && validChannelFinanceNumber(snap.UpstreamRechargeCredit)})
	}
	sort.Slice(terms, func(i, j int) bool {
		if terms[i].EffectiveAt == terms[j].EffectiveAt {
			return terms[i].Version < terms[j].Version
		}
		return terms[i].EffectiveAt < terms[j].EffectiveAt
	})
	return terms
}

func TestFinanceRechargeCorrectionAppendOnlyFiniteAndIdempotent(t *testing.T) {
	history := correctionFixtureVersions()
	original := append([]ChannelFinanceVersion(nil), history...)
	in := FinanceRechargeCorrection{Domain: "example.com", FromTs: 3600, ToTs: 10800, Paid: 2.5, Credit: 8, Reason: "fixture review"}
	added, err := buildFinanceRechargeCorrection(history, in, 172800)
	if err != nil || len(added) != 5 || !reflect.DeepEqual(history, original) {
		t.Fatal("unexpected correction or original mutation", added, err)
	}
	if added[0].SnapshotJSON != `{"domain":"example.com","upstream_recharge_credit":8,"upstream_recharge_paid":2.5}` {
		t.Fatal("earlier history fabricated channel/group configuration", added[0])
	}
	for i := 0; i < 2; i++ {
		var old, next map[string]json.RawMessage
		_ = json.Unmarshal([]byte(history[i].SnapshotJSON), &old)
		_ = json.Unmarshal([]byte(added[i+1].SnapshotJSON), &next)
		for _, m := range []map[string]json.RawMessage{old, next} {
			delete(m, "upstream_recharge_paid")
			delete(m, "upstream_recharge_credit")
		}
		if !reflect.DeepEqual(old, next) {
			t.Fatal("correction changed non-recharge snapshot data")
		}
	}
	if added[3].SnapshotJSON != history[1].SnapshotJSON || added[4].SnapshotJSON != history[2].SnapshotJSON {
		t.Fatal("outside interval/current settings changed")
	}
	combined := append(append([]ChannelFinanceVersion(nil), history...), added...)
	terms := correctionTerms(t, combined)
	for _, tc := range []struct {
		from, to     int64
		paid, credit float64
		status       string
	}{
		{0, 3600, 0, 0, upstreamAdjustedCostMissingHistory},
		{3600, 7200, 2.5, 8, upstreamAdjustedCostComplete},
		{7200, 10800, 2.5, 8, upstreamAdjustedCostComplete},
		{10800, 14400, 1, 10, upstreamAdjustedCostComplete},
		{86400, 90000, 1, 7, upstreamAdjustedCostComplete},
	} {
		p, c, status := rechargeTermsForBucket(terms, tc.from, tc.to)
		if p != tc.paid || c != tc.credit || status != tc.status {
			t.Fatal("finite correction selected wrong history", tc, p, c, status)
		}
	}
	if again, err := buildFinanceRechargeCorrection(combined, in, 172800); err != nil || len(again) != 0 {
		t.Fatal("retry grew history", again, err)
	}
}

func TestFinanceRechargeCorrectionRejectsInvalidScope(t *testing.T) {
	for _, change := range []func(*FinanceRechargeCorrection){
		func(x *FinanceRechargeCorrection) { x.Paid = math.NaN() },
		func(x *FinanceRechargeCorrection) { x.Credit = 0 },
		func(x *FinanceRechargeCorrection) { x.Domain = "other.com" },
		func(x *FinanceRechargeCorrection) { x.ToTs = 90000 },
		func(x *FinanceRechargeCorrection) { x.FromTs = x.ToTs },
		func(x *FinanceRechargeCorrection) { x.Reason = "" },
	} {
		in := FinanceRechargeCorrection{Domain: "example.com", FromTs: 3600, ToTs: 10800, Paid: 1, Credit: 1, Reason: "test"}
		change(&in)
		if _, err := buildFinanceRechargeCorrection(correctionFixtureVersions(), in, 172800); err == nil {
			t.Fatal("accepted invalid historical correction", in)
		}
	}
}

func TestFinanceRechargeCorrectionRefreshesCachedDaysAndProtectsOldDeductions(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	t.Cleanup(m.Close)
	createChannelRechargeVersion(t, m, "hour.example", 1, scope.FromTs+1800, 1, 10)
	createChannelRechargeVersion(t, m, "hour.example", 2, scope.ToTs, 1, 7)
	ctx := context.Background()
	build := func() (financePeriodComponent, bool) {
		t.Helper()
		v, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "correction-cache", accounts, channelFinanceSnapshot{},
			financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return v, hit
	}
	before, _ := build()
	if _, hit := build(); !hit {
		t.Fatal("baseline did not use cache")
	}
	var history []ChannelFinanceVersion
	if err := m.storeDB.Where("domain=?", "hour.example").Find(&history).Error; err != nil {
		t.Fatal(err)
	}
	added, err := buildFinanceRechargeCorrection(history, FinanceRechargeCorrection{Domain: "hour.example", FromTs: scope.FromTs,
		ToTs: scope.ToTs, Paid: 1, Credit: 1, Reason: "fixture"}, scope.ToTs+86400)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error { return tx.Create(&added).Error }); err != nil {
		t.Fatal(err)
	}
	after, hit := build()
	if hit || before.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "200000" || after.Statement.KnownRawCorrectedUpstreamCost.MicroUSD != "3000000" {
		t.Fatal("missing/existing costs or cache remained stale", hit, before.Statement, after.Statement)
	}
	var sum int64
	for _, day := range after.Days {
		v, ok := financeMoneyInt64(day.RechargeCorrection.KnownCost)
		if !ok {
			t.Fatal("invalid daily cost")
		}
		sum += v
	}
	if sum != 3_000_000 || before.Statement.UpstreamBilledCost.MicroUSD != after.Statement.UpstreamBilledCost.MicroUSD || after.Statement.OperatingProfit != nil {
		t.Fatal("day/period, raw bills, or incomplete profit changed incorrectly")
	}
	if _, hit := build(); !hit {
		t.Fatal("new revision was not cached")
	}
	versions, err := m.loadChannelRechargeVersions(ctx, accounts, channelFinanceSnapshot{})
	if err != nil {
		t.Fatal(err)
	}
	fact := financeInternalTestCostFact{Rows: 1, UpstreamCostMicroUSD: 2_000_000, CorrectedCostMicroUSD: 200_000}
	events := []financeInternalTestCostEvent{{FinanceVersion: 1, HourTs: scope.FromTs + 86400, Fact: fact}}
	if status := financeRechargeDeductionStatus(events, fact, versions["hour.example"]); status != "cost_pricing_basis_mismatch" {
		t.Fatal("old internal deduction reused under new price", status)
	}
}
