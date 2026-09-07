package monitor

import "testing"

func TestRecentInfraAlertsUsesLiteralPrefix(t *testing.T) {
	m := newTestMonitor(t)
	rows := []AlertLog{
		{Ts: 10, Kind: "infra_db_mem", Target: "db", Detail: "low memory"},
		{Ts: 11, Kind: "infraXnot_an_infra_alert", Target: "other", Detail: "must not match"},
		{Ts: 12, Kind: "error_rate", Target: "model", Detail: "not infra"},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	got := m.recentInfraAlerts(20, 20)
	if len(got) != 1 || got[0].Kind != "infra_db_mem" || got[0].Target != "db" {
		t.Fatalf("literal infra_ prefix query mismatch: %+v", got)
	}
}

func TestRetiredOriginProbesDisappearWithoutDeletingHistory(t *testing.T) {
	m := newTestMonitor(t)
	const target = "retired.example:80"
	const now = int64(1800000000)
	if err := m.upsertInfra([]InfraSample{{BucketTs: now, Resource: target, RType: "lock", Metric: "locked", Value: 0}}); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&AlertLog{Ts: now, Kind: "infra_origin_lock", Target: target, Detail: "historical"}).Error; err != nil {
		t.Fatal(err)
	}
	if got := m.computeInfraSnapshot(now); len(got.Locks) != 0 || len(got.Alerts) != 0 {
		t.Fatalf("retired probes leaked into current monitoring: locks=%v alerts=%v", got.Locks, got.Alerts)
	}
	m.cfg.OriginLockTargets = target
	if got := m.computeInfraSnapshot(now); len(got.Locks) != 1 || len(got.Alerts) != 1 {
		t.Fatal("history was destroyed instead of being filtered from current monitoring")
	}
}
