package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"

	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

func TestFinancePeriodLedgerSharedProjectionParityAndOneScan(t *testing.T) {
	m, scope, accounts := dailyBillFixture(t)
	for index, cost := range []int64{2_000_000, 5_000_000} {
		pub := insertEconomicsReportHour(t, m, "hour.example", "epoch", scope.FromTs+int64(index)*86400, index+1, 0, cost, cost, 0)
		insertEconomicsReportManifest(t, m, pub.Domain, pub.AccountEpoch, pub.HourTs, pub)
	}
	ctx := context.Background()
	internal := financeConfiguredInternalEvidence{Complete: true}
	evidence := financeInternalTestCostEvidence{}
	ledger, err := m.financePeriodLedger(ctx, scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	serialize := func() []byte {
		t.Helper()
		b, err := json.Marshal(struct {
			Report               *channelEconomicsReport
			Corrected, Published map[int64]map[string]int64
		}{ledger, ledger.dailyCorrectedCosts, ledger.dailyPublishedCosts})
		if err != nil {
			t.Fatal(err)
		}
		return b
	}
	before := serialize()
	wantStatement, wantUser, wantUpstream, wantDetails, err := m.buildFinancePeriod(ctx, scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, evidence, internal, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotStatement, gotUser, gotUpstream, gotDetails, err := m.buildFinancePeriodWithSources(ctx, scope, scope.ToTs+86400, accounts, channelFinanceSnapshot{}, evidence, internal, nil, financePeriodSources{ledger: ledger})
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantStatement, gotStatement) || !reflect.DeepEqual(wantUser, gotUser) || !reflect.DeepEqual(wantUpstream, gotUpstream) || !reflect.DeepEqual(wantDetails, gotDetails) {
		t.Fatal("shared ledger changed monthly projection")
	}
	wantDays, err := m.buildFinanceDailyViews(ctx, scope, scope.ToTs+86400, evidence, internal, nil)
	if err != nil {
		t.Fatal(err)
	}
	gotDays, err := m.buildFinanceDailyViewsWithLedger(ctx, scope, scope.ToTs+86400, evidence, internal, nil, ledger)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(wantDays, gotDays) || !bytes.Equal(before, serialize()) {
		t.Fatal("shared projection changed daily results or mutated the shared ledger")
	}
	counter := &financeAcceptanceSQLCounter{Interface: logger.Default.LogMode(logger.Silent)}
	m.storeDB = m.storeDB.Session(&gorm.Session{Logger: counter})
	build := func(ctx context.Context) bool {
		t.Helper()
		_, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+86400, "reuse-test", accounts, channelFinanceSnapshot{}, evidence, internal, nil)
		if err != nil {
			t.Fatal(err)
		}
		return hit
	}
	if build(ctx) || counter.ledgerScans.Load() != 1 {
		t.Fatalf("cold component scans=%d, want 1", counter.ledgerScans.Load())
	}
	if !build(ctx) || counter.ledgerScans.Load() != 1 {
		t.Fatal("cache hit rescanned economic ledger")
	}
	if build(context.WithValue(ctx, financeForceRebuildKey{}, true)) || counter.ledgerScans.Load() != 2 {
		t.Fatal("fresh component reused a previous build's ledger")
	}
}

func TestFinancePeriodLedgerRejectsWrongScopeAndCancellation(t *testing.T) {
	m := &Monitor{}
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	good := channelEconomicsReport{From: scope.FromTs, To: scope.ToTs, SemanticsVersion: channelEconomicsSemanticsVersion}
	for _, change := range []func(*channelEconomicsReport){
		func(r *channelEconomicsReport) { r.From-- },
		func(r *channelEconomicsReport) { r.To++ },
		func(r *channelEconomicsReport) { r.DomainFilter = "one.example" },
		func(r *channelEconomicsReport) { r.SemanticsVersion-- },
	} {
		bad := good
		change(&bad)
		if _, err := m.financePeriodLedger(context.Background(), scope, &bad); err == nil {
			t.Fatal("mismatched shared ledger accepted")
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.financePeriodLedger(ctx, scope, &good); !errors.Is(err, context.Canceled) {
		t.Fatal("canceled build accepted shared ledger")
	}
}
