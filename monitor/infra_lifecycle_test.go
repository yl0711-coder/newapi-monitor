package monitor

import (
	"context"
	"errors"
	"testing"
)

func TestLightsailInventoryRequiresAllPages(t *testing.T) {
	for _, fail := range []bool{false, true} {
		calls := 0
		rows, err := collectLightsailPages(context.Background(), func(token *string) ([]string, *string, error) {
			calls++
			if token == nil {
				next := "page2"
				return []string{"first"}, &next, nil
			}
			if *token != "page2" {
				t.Fatal("wrong page token")
			}
			if fail {
				return nil, nil, errors.New("permission failure")
			}
			return []string{"second"}, nil, nil
		})
		if calls != 2 || (!fail && (err != nil || len(rows) != 2)) || (fail && (err == nil || rows != nil)) {
			t.Fatalf("partial inventory accepted: rows=%v err=%v", rows, err)
		}
	}
	_, err := collectLightsailPages(context.Background(), func(*string) ([]int, *string, error) { next := "same"; return []int{1}, &next, nil })
	if err == nil {
		t.Fatal("repeated pagination token accepted")
	}
}

func TestInfraRetirementNeedsConfirmedAbsenceAndKeepsHistory(t *testing.T) {
	m := newTestMonitor(t)
	now := int64(1800000000)
	if err := m.upsertInfra([]InfraSample{
		{BucketTs: now - 600, Resource: "old-node", RType: "instance", Metric: "cpu", Value: 70},
		{BucketTs: now - 600, Resource: "old-node", RType: "host", Metric: "disk_used_pct", Value: 20},
		{BucketTs: now - 600, Resource: "rds/live", RType: "database", Metric: "cpu", Value: 3},
	}); err != nil {
		t.Fatal(err)
	}
	previous := m.knownLightsailResources(context.Background())
	if len(previous) != 1 || previous[0].Resource != "old-node" {
		t.Fatalf("inventory names included host/RDS rows or lost Lightsail: %+v", previous)
	}
	if rows := lightsailPresenceRows(now, nil, previous, map[string]bool{}); len(rows) != 0 {
		t.Fatal("discovery failure generated retirements")
	}
	rows := lightsailPresenceRows(now, nil, previous, map[string]bool{"instance": true, "database": true})
	if len(rows) != 1 || rows[0].Resource != "old-node" || rows[0].Value != 0 {
		t.Fatalf("retired a different platform: %+v", rows)
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	snap := m.computeInfraSnapshot(now)
	if len(snap.Instances) != 0 || len(snap.Databases) != 1 {
		t.Fatalf("retired host resurrected or RDS hidden: %+v", snap)
	}
	var count int64
	m.storeDB.Model(&InfraSample{}).Where("resource = ? AND metric = ?", "old-node", "cpu").Count(&count)
	if count != 1 {
		t.Fatal("retirement deleted history")
	}
	rows = lightsailPresenceRows(now+60, []infraTarget{{name: "old-node", rtype: "instance"}}, m.storeInfraLatest(), map[string]bool{"instance": true})
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	if len(m.computeInfraSnapshot(now+60).Instances) != 1 {
		t.Fatal("reappearing resource stayed retired")
	}
}
