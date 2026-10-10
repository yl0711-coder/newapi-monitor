package monitor

import (
	"context"
	"strings"
	"testing"
)

// Three adjacent hours: exclusive, shared, exclusive again. Non-unit recharge
// ratio 2:5 catches hard-coded 1:1 assumptions without real upstream data.
func newHistoryFlowFixture(t *testing.T) (*Monitor, ChannelUpstreamAccount, string, []int64) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "history-flow.example")
	var account ChannelUpstreamAccount
	if err := m.storeDB.First(&account, "domain=?", "history-flow.example").Error; err != nil {
		t.Fatal(err)
	}
	hours := []int64{1786420800, 1786424400, 1786428000}
	source, epoch := strings.Repeat("a", 64), newAPIUpstreamAccountEpoch(account)
	m.cfg.ChannelCostClosureEnabled, m.cfg.ChannelCostClosureDomains = true, []string{account.Domain}
	if err := m.storeDB.Create(&ChannelSnap{ID: 59, Name: "local test", BaseDomain: account.Domain, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	createChannelRechargeVersion(t, m, account.Domain, 1, hours[0], 2, 5)
	for _, hour := range hours {
		for _, row := range []any{
			&ChannelUpstreamCostHourState{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour,
				SemanticsVersion: channelCostEvidenceSemanticsVersion, Provider: account.Provider,
				Status: "verified", ReconcileStatus: "matched", ChargeUnitsPerUSD: "500000", ContentHash: strings.Repeat("e", 64),
				ControlChargeUnits: 500000, EvidenceChargeUnits: 500000, Requests: 10, EvidenceRows: 1},
			&ChannelUpstreamCostHourEvidence{Domain: account.Domain, AccountEpoch: epoch, HourTs: hour,
				SemanticsVersion: channelCostEvidenceSemanticsVersion, SourceRef: source, DimensionHash: strings.Repeat("b", 64),
				Provider: account.Provider, SourceRefKind: channelCostSourceKindNewAPIToken, HMACKeyID: "test-key",
				PricingDimensionHash: strings.Repeat("c", 64), ChargeUnits: 500000, ChargeUnit: channelCostChargeUnitNewAPIQuota,
				ChargeUnitsPerUSD: "500000", Requests: 10, ContentHash: strings.Repeat("d", 64)},
			&ChannelUpstreamUsageHour{Domain: account.Domain, HourTs: hour, BucketSeconds: 3600, Requests: 10,
				Quota: 500000, CostUSD: 1, UnitPerUSD: 500000, Provider: account.Provider},
			&StabilityHourSample{HourTs: hour, ChannelID: 59, ModelName: "gpt", Grp: "public", Success: 10,
				Quota: 1000000, TrafficClassVersion: stabilityTrafficClassificationVersion},
			&StabilityHourIngestState{HourTs: hour, Status: "complete", TrafficClassVersion: stabilityTrafficClassificationVersion},
		} {
			if err := m.storeDB.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := m.publishChannelEconomicsHour(context.Background(), account, hour, "synthetic_baseline", hour+3600); err != nil {
			t.Fatal(err)
		}
	}
	return m, account, source, hours
}

// Close the actual SQLite connection and reopen the on-disk store, not just
// reset the Monitor's in-memory caches. No source workers are started.
func restartHistoryFlowMonitor(t *testing.T, m *Monitor) *Monitor {
	t.Helper()
	pool, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	cfg := m.cfg
	m.Close()
	if err := pool.Ping(); err == nil {
		t.Fatal("old SQLite connection is still open")
	}
	restarted := &Monitor{cfg: cfg}
	if err := restarted.openStore(cfg.StorePath); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(restarted.Close)
	return restarted
}
