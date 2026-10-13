package monitor

import (
	"context"
	"testing"
)

// Also run unchanged against v1.18.25 via a Go overlay: rolling back must be
// able to read the new marker, refuse final coverage, and replace it normally.
func TestCostIntegrityRollbackReadsAndReplacesMarker(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	account := ChannelUpstreamAccount{Domain: "rollback.example", Provider: upstreamProviderNewAPI, BalanceUnit: 8}
	if err := m.storeDB.Create(&account).Error; err != nil {
		t.Fatal(err)
	}
	marker := ChannelUpstreamUsageHour{Domain: account.Domain, Provider: account.Provider, HourTs: 7200, BucketSeconds: 3600, UnitPerUSD: 7.2, FetchedAt: 7217, SourceKind: "newapi_unit_boundary", Provisional: true}
	if err := m.storeDB.Create(&marker).Error; err != nil {
		t.Fatal(err)
	}
	accounts := map[string]ChannelUpstreamAccountView{account.Domain: {Configured: true, UsageSyncEnabled: true, Provider: account.Provider}}
	scope := stabilityScope{FromTs: 7200, ToTs: 10800}
	rows, err := m.loadChannelUpstreamUsageRows(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	metrics, _, err := projectFinanceBillWindow(rows, scope, 14400, accounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if metrics[account.Domain].Complete {
		t.Fatal("rollback treated marker as finalized coverage")
	}
	real := ChannelUpstreamUsageHour{Domain: account.Domain, Provider: account.Provider, HourTs: 7200, BucketSeconds: 3600, Quota: 72, Requests: 4, Tokens: 100}
	if err := m.persistUpstreamUsageWindow(context.Background(), account.Domain, 7200, 10800, []ChannelUpstreamUsageHour{real}, 14400); err != nil {
		t.Fatal(err)
	}
	rows, err = m.loadChannelUpstreamUsageRows(context.Background(), scope)
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].CostUSD != 10 || rows[0].UnitPerUSD != 7.2 || rows[0].Provisional || rows[0].SourceKind != "" {
		t.Fatalf("rollback could not publish actual facts: %+v", rows)
	}
	metrics, _, err = projectFinanceBillWindow(rows, scope, 14400, accounts, nil)
	if err != nil {
		t.Fatal(err)
	}
	if !metrics[account.Domain].Complete {
		t.Fatal("rollback lost verified coverage")
	}
}
