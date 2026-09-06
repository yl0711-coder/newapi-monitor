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
