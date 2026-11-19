package monitor

import (
	"context"
	"errors"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/lightsail"
	lstypes "github.com/aws/aws-sdk-go-v2/service/lightsail/types"
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

type lightsailInventoryFixture struct {
	instances, databases, loadBalancers int
}

func (f *lightsailInventoryFixture) GetInstances(context.Context, *lightsail.GetInstancesInput, ...func(*lightsail.Options)) (*lightsail.GetInstancesOutput, error) {
	f.instances++
	return &lightsail.GetInstancesOutput{Instances: []lstypes.Instance{
		{Name: aws.String("live-host"), Arn: aws.String("fixture-live"), State: &lstypes.InstanceState{Name: aws.String("running")}},
		{Name: aws.String("retired-host"), Arn: aws.String("fixture-old"), State: &lstypes.InstanceState{Name: aws.String("running")}},
	}}, nil
}

func (f *lightsailInventoryFixture) GetRelationalDatabases(context.Context, *lightsail.GetRelationalDatabasesInput, ...func(*lightsail.Options)) (*lightsail.GetRelationalDatabasesOutput, error) {
	f.databases++
	return &lightsail.GetRelationalDatabasesOutput{RelationalDatabases: []lstypes.RelationalDatabase{{Name: aws.String("old-db")}}}, nil
}

func (f *lightsailInventoryFixture) GetLoadBalancers(context.Context, *lightsail.GetLoadBalancersInput, ...func(*lightsail.Options)) (*lightsail.GetLoadBalancersOutput, error) {
	f.loadBalancers++
	return &lightsail.GetLoadBalancersOutput{LoadBalancers: []lstypes.LoadBalancer{{Name: aws.String("old-lb")}}}, nil
}

func TestLightsailInstancesOnlySkipsObsoleteAPIsAndPresenceWrites(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.InfraLightsailInstancesOnly = true
	m.cfg.InfraManagedAWSDisabled = true
	m.cfg.InfraResources = "database:old-db,lb:old-lb,instance:retired-host"
	m.cfg.InfraExcludeResources = []string{"retired-host"}
	const bucket = 1_800_000_000
	old := []InfraSample{
		{BucketTs: bucket - 300, Resource: "retired-host", RType: "instance", Metric: "cpu", Value: 5},
		{BucketTs: bucket - 300, Resource: "old-db", RType: "database", Metric: "connections", Value: 5},
		{BucketTs: bucket - 300, Resource: "old-lb", RType: "lb", Metric: "healthy", Value: 1},
	}
	if err := m.upsertInfra(old); err != nil {
		t.Fatal(err)
	}
	if known := m.knownLightsailResources(context.Background()); len(known) != 0 {
		t.Fatalf("retired resources still included in periodic reconciliation: %+v", known)
	}
	f := &lightsailInventoryFixture{}
	targets, rows := m.infraTargetsWithDiscovery(context.Background(), f, bucket)
	if f.instances != 1 || f.databases != 0 || f.loadBalancers != 0 {
		t.Fatalf("unexpected Lightsail API calls: %+v", f)
	}
	if len(targets) != 1 || targets[0].name != "live-host" {
		t.Fatalf("retired explicit target leaked, or live host lost: %+v", targets)
	}
	for _, row := range append(rows, m.lightsailDiscoveryFailureRows(bucket)...) {
		if row.Resource != "live-host" && row.Resource != "AWS 资源发现/Lightsail/实例" {
			t.Fatalf("new retired-resource/discovery sample: %+v", row)
		}
	}
	snap := m.computeInfraSnapshot(bucket)
	if len(snap.Databases) != 0 || len(snap.LoadBalancers) != 0 || m.serverInfraPanels(&snap).Database || m.serverInfraPanels(&snap).LoadBalancer {
		t.Fatalf("retained legacy metrics appeared as active: %+v", snap)
	}
	var count int64
	if err := m.storeDB.Model(&InfraSample{}).Where("bucket_ts = ?", bucket-300).Count(&count).Error; err != nil || count != int64(len(old)) {
		t.Fatalf("collection policy must not delete history: count=%d err=%v", count, err)
	}
}

func TestLightsailDefaultAndManagedAWSKeepIndependentScopes(t *testing.T) {
	t.Setenv("MONITOR_INFRA_LIGHTSAIL_INSTANCES_ONLY", "")
	if LoadSettings().InfraLightsailInstancesOnly {
		t.Fatal("absence of option must preserve existing Lightsail installation")
	}
	t.Setenv("MONITOR_INFRA_LIGHTSAIL_INSTANCES_ONLY", "true")
	if !LoadSettings().InfraLightsailInstancesOnly {
		t.Fatal("instances-only option missing")
	}
	m := newTestMonitor(t)
	f := &lightsailInventoryFixture{}
	if targets, _ := m.infraTargetsWithDiscovery(context.Background(), f, 1_800_000_000); len(targets) != 4 || f.databases != 1 || f.loadBalancers != 1 {
		t.Fatalf("default mode lost existing resources: targets=%+v calls=%+v", targets, f)
	}
	m.cfg.InfraLightsailInstancesOnly = true
	if !m.monitorOwnsInfraResource("rds/current", "database", "RDS") || !m.monitorOwnsInfraResource("alb/current", "lb", "ALB") {
		t.Fatal("Lightsail-only restriction must not disable independently enabled managed AWS metrics")
	}
	panels := m.serverInfraPanels(&InfraSnapshot{})
	if !panels.Database || !panels.LoadBalancer {
		t.Fatalf("managed AWS cards must remain available when AWS sampling is enabled: %+v", panels)
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
