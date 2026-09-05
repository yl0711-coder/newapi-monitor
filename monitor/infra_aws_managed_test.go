package monitor

import (
	"context"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
	ecstypes "github.com/aws/aws-sdk-go-v2/service/ecs/types"
)

type fakeCloudWatchMetricClient struct {
	output *cloudwatch.GetMetricStatisticsOutput
	input  *cloudwatch.GetMetricStatisticsInput
}

type fakeECSTaskHealthClient struct {
	listOutputs     []*ecs.ListTasksOutput
	describeOutputs []*ecs.DescribeTasksOutput
	listCalls       int
	describeCalls   int
}

func (f *fakeECSTaskHealthClient) ListTasks(_ context.Context, _ *ecs.ListTasksInput, _ ...func(*ecs.Options)) (*ecs.ListTasksOutput, error) {
	out := f.listOutputs[f.listCalls]
	f.listCalls++
	return out, nil
}

func (f *fakeECSTaskHealthClient) DescribeTasks(_ context.Context, _ *ecs.DescribeTasksInput, _ ...func(*ecs.Options)) (*ecs.DescribeTasksOutput, error) {
	out := f.describeOutputs[f.describeCalls]
	f.describeCalls++
	return out, nil
}

func (f *fakeCloudWatchMetricClient) GetMetricStatistics(_ context.Context, input *cloudwatch.GetMetricStatisticsInput, _ ...func(*cloudwatch.Options)) (*cloudwatch.GetMetricStatisticsOutput, error) {
	f.input = input
	return f.output, nil
}

func TestLatestCloudWatchMetricUsesNewestPointAndNormalizesUnit(t *testing.T) {
	old, newest := time.Unix(100, 0), time.Unix(200, 0)
	fake := &fakeCloudWatchMetricClient{output: &cloudwatch.GetMetricStatisticsOutput{Datapoints: []cwtypes.Datapoint{
		{Timestamp: &newest, Average: aws.Float64(20 * 1048576)},
		{Timestamp: &old, Average: aws.Float64(10 * 1048576)},
	}}}
	value, ok, err := latestCloudWatchMetric(context.Background(), fake, "AWS/RDS", "FreeableMemory",
		[]cwtypes.Dimension{{Name: aws.String("DBInstanceIdentifier"), Value: aws.String("db")}}, cwtypes.StatisticAverage, 1048576)
	if err != nil || !ok || value != 20 {
		t.Fatalf("value=%v ok=%v err=%v", value, ok, err)
	}
	if aws.ToString(fake.input.Namespace) != "AWS/RDS" || aws.ToString(fake.input.MetricName) != "FreeableMemory" || aws.ToInt32(fake.input.Period) != 300 {
		t.Fatalf("unexpected CloudWatch request: %+v", fake.input)
	}
}

func TestECSContainerInsightsMetricContract(t *testing.T) {
	specs := ecsContainerInsightsSpecs()
	want := map[string]string{
		"MemoryUtilized": "mem_used_mb", "MemoryReserved": "mem_total_mb",
		"EphemeralStorageUtilized": "disk_used_gb", "EphemeralStorageReserved": "disk_total_gb",
		"NetworkRxBytes": "net_in_kb", "NetworkTxBytes": "net_out_kb",
		"RestartCount": "restart_count",
	}
	if len(specs) != len(want) {
		t.Fatalf("spec count=%d want=%d", len(specs), len(want))
	}
	for _, spec := range specs {
		if got := want[spec.name]; got == "" || got != spec.key || spec.scale <= 0 {
			t.Fatalf("unexpected Container Insights spec: %+v", spec)
		}
	}
}

func TestECSContainerInsightsDerivedCapacityAndHealth(t *testing.T) {
	r := InfraResource{Type: "ecs_service", Metrics: map[string]float64{
		"mem_used_mb": 768, "mem_total_mb": 1024,
		"disk_used_gb": 5, "disk_total_gb": 20,
	}}
	addDerivedPct(&r)
	if r.Metrics["mem_used_pct"] != 75 || r.Metrics["disk_used_pct"] != 25 {
		t.Fatalf("unexpected derived metrics: %+v", r.Metrics)
	}
	m := newTestMonitor(t)
	r.Metrics["unhealthy_containers"] = 1
	if got := m.infraStatus(r); got != "bad" {
		t.Fatalf("unhealthy Fargate container must be bad, got %q", got)
	}
}

func TestECSStandardMemoryPercentDerivesAbsoluteUsage(t *testing.T) {
	r := InfraResource{Type: "ecs_service", Metrics: map[string]float64{
		"mem_used_pct": 25, "mem_total_mb": 8192,
	}}
	addDerivedPct(&r)
	if r.Metrics["mem_used_mb"] != 2048 {
		t.Fatalf("unexpected derived used memory: %+v", r.Metrics)
	}
}

func TestECSServiceContainerHealthDoesNotCallUnknownHealthy(t *testing.T) {
	fake := &fakeECSTaskHealthClient{
		listOutputs: []*ecs.ListTasksOutput{{TaskArns: []string{"task-1"}}},
		describeOutputs: []*ecs.DescribeTasksOutput{{Tasks: []ecstypes.Task{{LastStatus: aws.String("RUNNING"), Memory: aws.String("4096"), EphemeralStorage: &ecstypes.EphemeralStorage{SizeInGiB: 20}, Containers: []ecstypes.Container{
			{HealthStatus: ecstypes.HealthStatusHealthy},
			{HealthStatus: ecstypes.HealthStatusUnhealthy},
			{HealthStatus: ecstypes.HealthStatusUnknown},
		}}}}},
	}
	snapshot, err := ecsServiceTaskState(context.Background(), fake, "cluster", "service")
	if err != nil || snapshot.HealthChecked != 2 || snapshot.Unhealthy != 1 || snapshot.MemoryReservedMB != 4096 || snapshot.EphemeralReservedGB != 20 || fake.listCalls != 1 || fake.describeCalls != 1 {
		t.Fatalf("snapshot=%+v list=%d describe=%d err=%v", snapshot, fake.listCalls, fake.describeCalls, err)
	}
}

func TestManagedInfraClassificationAndState(t *testing.T) {
	m := newTestMonitor(t)
	if got := infraResourceGroup("ecs/nexusapi-prod-cluster/nexusapi-prod-worker-canary"); got != "NexusAPI" {
		t.Fatalf("ecs group=%q", got)
	}
	if got := infraResourceGroup("rds/sub2api-postgresql-prod"); got != "Sub2API" {
		t.Fatalf("rds group=%q", got)
	}
	if got := infraPlatform("ecs/prod/service", "ecs_service"); got != "ECS/Fargate" {
		t.Fatalf("platform=%q", got)
	}
	if got := m.infraStatus(InfraResource{Type: "ecs_service", Metrics: map[string]float64{"status_failed": 1}}); got != "bad" {
		t.Fatalf("short ECS service must be bad, got %q", got)
	}
	if got := m.infraStatus(InfraResource{Type: "database", Metrics: map[string]float64{"available": 0}}); got != "bad" {
		t.Fatalf("unavailable RDS must be bad, got %q", got)
	}
	if got := m.infraStatus(InfraResource{Type: "lb", Metrics: map[string]float64{"status_failed": 1}}); got != "bad" {
		t.Fatalf("inactive ALB must be bad, got %q", got)
	}
}

func TestInfraSnapshotKeepsAllManagedResourcesAndLegacyPrimary(t *testing.T) {
	m := newTestMonitor(t)
	const bucket = 1_700_000_000 / 60 * 60
	rows := []InfraSample{
		managedRow(bucket, "ecs/nexusapi-prod-cluster/nexusapi-prod-worker-canary", "ecs_service", "status_failed", 0),
		managedRow(bucket, "ecs/nexusapi-prod-cluster/nexusapi-prod-worker-canary", "ecs_service", "cpu", 5),
		managedRow(bucket, "rds/nexusapi-mysql-prod", "database", "available", 1),
		managedRow(bucket, "rds/sub2api-postgresql-prod", "database", "available", 1),
		managedRow(bucket, "alb/nexusapi-alb", "lb", "healthy", 5),
		managedRow(bucket, "alb/nexusapi-alb", "lb", "unhealthy", 0),
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	snapshot := m.computeInfraSnapshot(bucket + 30)
	if len(snapshot.Databases) != 2 || len(snapshot.LoadBalancers) != 1 || len(snapshot.Instances) != 1 {
		t.Fatalf("incomplete snapshot: %+v", snapshot)
	}
	if snapshot.Database == nil || snapshot.Database.DisplayName != "nexusapi-mysql-prod" {
		t.Fatalf("legacy primary database must prefer NexusAPI: %+v", snapshot.Database)
	}
	if len(snapshot.Groups) != 2 || snapshot.Groups[0].Name != "NexusAPI" || snapshot.Groups[1].Name != "Sub2API" {
		t.Fatalf("unexpected groups: %+v", snapshot.Groups)
	}
}
