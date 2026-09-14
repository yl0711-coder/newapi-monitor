package monitor

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestManagedAWSInfraSettingsDefaultAndExplicitDisable(t *testing.T) {
	t.Setenv("MONITOR_INFRA_MANAGED_AWS_ENABLED", "")
	t.Setenv("MONITOR_INFRA_MANAGED_AWS_DASHBOARD_URL", "")
	defaults := LoadSettings()
	if defaults.InfraManagedAWSDisabled {
		t.Fatal("ordinary upgrade must preserve managed AWS sampling")
	}

	t.Setenv("MONITOR_INFRA_MANAGED_AWS_ENABLED", "false")
	t.Setenv("MONITOR_INFRA_MANAGED_AWS_DASHBOARD_URL", "https://console.aws.amazon.com/cloudwatch/home")
	disabled := LoadSettings()
	if !disabled.InfraManagedAWSDisabled || disabled.InfraManagedAWSDashboardURL == "" {
		t.Fatalf("explicit delegation not loaded: %+v", disabled)
	}

	for _, unsafe := range []string{
		"http://console.aws.amazon.com/cloudwatch/home",
		"javascript:alert(1)",
		"https://user:secret@console.aws.amazon.com/cloudwatch/home",
	} {
		t.Setenv("MONITOR_INFRA_MANAGED_AWS_DASHBOARD_URL", unsafe)
		if got := LoadSettings().InfraManagedAWSDashboardURL; got != "" {
			t.Fatalf("unsafe dashboard URL accepted: %q", got)
		}
	}
}

func TestManagedAWSInfraClassifierDoesNotCaptureLightsail(t *testing.T) {
	cases := []struct {
		name, rtype, platform string
		managed               bool
	}{
		{"ecs/cluster/service", "ecs_service", "ECS/Fargate", true},
		{"rds/database", "database", "RDS", true},
		{"alb/public", "lb", "ALB", true},
		{"AWS 资源发现/ECS/服务", "discovery", "AWS", true},
		{"AWS 资源发现/Lightsail/实例", "discovery", "Lightsail", false},
		{"Database-NexusAPI", "database", "Lightsail", false},
		{"Ubuntu-1", "instance", "Lightsail", false},
		{"Ubuntu-1", "host", "Host agent", false},
	}
	for _, tc := range cases {
		if got := managedAWSInfraResource(tc.name, tc.rtype, tc.platform); got != tc.managed {
			t.Fatalf("resource %q managed=%v, want %v", tc.name, got, tc.managed)
		}
	}
}

func TestManagedAWSDelegationFiltersSnapshotAndHistoricalAlerts(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.InfraManagedAWSDisabled = true
	m.cfg.InfraManagedAWSDashboardURL = "https://console.aws.amazon.com/cloudwatch/home"
	const bucket = 1_700_000_000 / 60 * 60
	rows := []InfraSample{
		{BucketTs: bucket, Resource: "Ubuntu-1", RType: "instance", Metric: "cpu", Value: 12},
		{BucketTs: bucket, Resource: "rds/nexusapi", RType: "database", Metric: "cpu", Value: 20},
		{BucketTs: bucket, Resource: "alb/nexusapi", RType: "lb", Metric: "healthy", Value: 3},
		{BucketTs: bucket, Resource: "ecs/cluster/worker", RType: "ecs_service", Metric: "cpu", Value: 15},
	}
	if err := m.upsertInfra(rows); err != nil {
		t.Fatal(err)
	}
	alerts := []AlertLog{{Ts: bucket, Kind: "infra_instance_cpu", Target: "Ubuntu-1", Detail: "lightsail"}}
	for i := 0; i < 25; i++ {
		alerts = append(alerts, AlertLog{Ts: bucket + int64(i+1), Kind: "infra_instance_cpu", Target: fmt.Sprintf("ecs/cluster/task-%02d", i), Detail: "managed"})
	}
	if err := m.storeDB.Create(&alerts).Error; err != nil {
		t.Fatal(err)
	}

	snap := m.computeInfraSnapshot(bucket + 30)
	if len(snap.Instances) != 1 || snap.Instances[0].Name != "Ubuntu-1" || len(snap.Databases) != 0 || len(snap.LoadBalancers) != 0 {
		t.Fatalf("delegated resources leaked into snapshot: instances=%+v db=%+v lb=%+v", snap.Instances, snap.Databases, snap.LoadBalancers)
	}
	if snap.ManagedAWS.MonitorEnabled || snap.ManagedAWS.Provider != "CloudWatch" || snap.ManagedAWS.DashboardURL == "" {
		t.Fatalf("delegation metadata incorrect: %+v", snap.ManagedAWS)
	}
	if len(snap.Alerts) != 1 || snap.Alerts[0].Target != "Ubuntu-1" {
		t.Fatalf("managed alert limit hid Lightsail history: %+v", snap.Alerts)
	}
}

func TestManagedAWSDelegationFiltersAssetPagesBeforeLimit(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.InfraManagedAWSDisabled = true
	rows := make([]InfraAsset, 0, infraAssetPageSize+2)
	for i := 1; i <= infraAssetPageSize+1; i++ {
		rows = append(rows, InfraAsset{
			ID: fmt.Sprintf("%064x", i), Identity: fmt.Sprintf("ecs-task-%d", i), Resource: fmt.Sprintf("ecs/cluster/task-%d", i),
			Kind: "ecs_task", Platform: "ECS/Fargate", State: "active", Revision: 1,
		})
	}
	rows = append(rows, InfraAsset{
		ID: fmt.Sprintf("%064x", 1000), Identity: "lightsail-instance", Resource: "Ubuntu-1",
		Kind: "instance", Platform: "Lightsail", State: "active", Revision: 1,
	})
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	page, err := m.infraAssetPage(context.Background(), "", "")
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Active) != 1 || page.Active[0].Resource != "Ubuntu-1" || page.ActiveNext != "" {
		t.Fatalf("filter applied after page limit: %+v next=%q", page.Active, page.ActiveNext)
	}
	assets, err := m.infraAssets(context.Background())
	if err != nil || len(assets) != 1 || assets[0].Resource != "Ubuntu-1" {
		t.Fatalf("unbounded asset view leaked managed resources: %+v err=%v", assets, err)
	}
}

func TestManagedAWSDelegationFiltersCapacityAndSeries(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.InfraEnabled = true
	m.cfg.InfraManagedAWSDisabled = true
	const bucket = 1_700_000_000 / 60 * 60
	if err := m.upsertInfra([]InfraSample{
		{BucketTs: bucket, Resource: "Ubuntu-1", RType: "instance", Metric: "cpu", Value: 11},
		{BucketTs: bucket, Resource: "ecs/cluster/worker", RType: "ecs_service", Metric: "cpu", Value: 22},
		{BucketTs: bucket, Resource: "rds/nexusapi", RType: "database", Metric: "connections", Value: 18},
		{BucketTs: bucket, Resource: "alb/nexusapi", RType: "lb", Metric: "resp_ms", Value: 70},
	}); err != nil {
		t.Fatal(err)
	}
	points, source := m.readCapacityInfra(context.Background(), bucket-60, bucket+60, 60, bucket+30)
	if len(points) != 1 || points[0].Resource != "Ubuntu-1" || source.Rows != 1 || !strings.Contains(source.Note, "CloudWatch") {
		t.Fatalf("capacity leaked delegated resources: points=%+v source=%+v", points, source)
	}

	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.GET("/infra/series", m.serveInfraSeries)
	managed := httptest.NewRecorder()
	router.ServeHTTP(managed, httptest.NewRequest(http.MethodGet, "/infra/series?resource=rds/nexusapi&metrics=connections", nil))
	if managed.Code != http.StatusGone || !strings.Contains(managed.Body.String(), "CloudWatch") {
		t.Fatalf("managed series response=%d %s", managed.Code, managed.Body.String())
	}
	if err := m.storeDB.Create(&InfraAsset{
		ID: infraAssetID("legacy-managed-rds"), Identity: "legacy-managed-rds", Resource: "legacy-db-name",
		Kind: "database", Platform: "RDS", State: "active", Revision: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	legacyManaged := httptest.NewRecorder()
	router.ServeHTTP(legacyManaged, httptest.NewRequest(http.MethodGet, "/infra/series?resource=legacy-db-name&metrics=connections", nil))
	if legacyManaged.Code != http.StatusGone {
		t.Fatalf("legacy managed resource bypassed delegation: %d %s", legacyManaged.Code, legacyManaged.Body.String())
	}
	lightsail := httptest.NewRecorder()
	router.ServeHTTP(lightsail, httptest.NewRequest(http.MethodGet, "/infra/series?resource=Ubuntu-1&metrics=cpu", nil))
	if lightsail.Code != http.StatusOK || !strings.Contains(lightsail.Body.String(), `"cpu"`) {
		t.Fatalf("Lightsail series response=%d %s", lightsail.Code, lightsail.Body.String())
	}

	// The first-release/default setting must preserve the pre-change view.
	m.cfg.InfraManagedAWSDisabled = false
	allPoints, allSource := m.readCapacityInfra(context.Background(), bucket-60, bucket+60, 60, bucket+30)
	if len(allPoints) != 4 || allSource.Rows != 4 {
		t.Fatalf("enabled compatibility view lost resources: points=%+v source=%+v", allPoints, allSource)
	}
	managed = httptest.NewRecorder()
	router.ServeHTTP(managed, httptest.NewRequest(http.MethodGet, "/infra/series?resource=rds/nexusapi&metrics=connections", nil))
	if managed.Code != http.StatusOK || !strings.Contains(managed.Body.String(), `"connections"`) {
		t.Fatalf("enabled compatibility series response=%d %s", managed.Code, managed.Body.String())
	}
}

func TestManagedAWSDelegationDoesNotDisableECSLogRuntime(t *testing.T) {
	cfg := Settings{
		InfraManagedAWSDisabled: true,
		ECSLogEnabled:           true,
		ECSLogProductionEnabled: true,
		ECSLogScope:             ecsLogScopeProduction,
	}
	if !ecsLogRuntimeEnabled(cfg) {
		t.Fatal("managed AWS metrics delegation must not disable ECS log collection")
	}
}

func TestManagedAWSDelegationUIIsExplicitAndBounded(t *testing.T) {
	for _, marker := range []string{
		`id="infraManagedNotice"`,
		"AWS 托管资源（ECS/Fargate、RDS、ALB）已由 CloudWatch 统一监控",
		"ECS 日志采集状态在上方独立展示",
	} {
		if !strings.Contains(pageHTML, marker) {
			t.Fatalf("managed AWS UI marker missing: %q", marker)
		}
	}
}
