package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	cwtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatch/types"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
)

// Uses the actual SDK and collection function, with no network or credentials.
type registryTestECSTransport struct{ calls int }

func (r *registryTestECSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r.calls++
	var body string
	switch {
	case strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".ListClusters"):
		body = `{"clusterArns":["arn:aws:ecs:us-west-2:123456789012:cluster/test"]}`
	case strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".ListServices"):
		body = `{"serviceArns":["arn:aws:ecs:us-west-2:123456789012:service/test/worker"]}`
	case strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".DescribeServices"):
		body = `{"services":[{"serviceArn":"arn:aws:ecs:us-west-2:123456789012:service/test/worker","serviceName":"worker","status":"ACTIVE","desiredCount":0,"runningCount":0}],"failures":[]}`
	case strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".ListTasks"):
		body = `{"taskArns":[]}`
	default:
		return nil, fmt.Errorf("unexpected SDK operation %q", req.Header.Get("X-Amz-Target"))
	}
	return &http.Response{StatusCode: 200, Header: http.Header{"Content-Type": []string{"application/x-amz-json-1.1"}}, Body: io.NopCloser(strings.NewReader(body)), Request: req}, nil
}

func TestECSRegistryFailurePreservesSuccessfulMetrics(t *testing.T) {
	for _, fail := range []bool{true, false} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			transport := &registryTestECSTransport{}
			client := ecs.New(ecs.Options{Region: "us-west-2", Credentials: aws.AnonymousCredentials{}, HTTPClient: &http.Client{Transport: transport}})
			now := time.Now()
			cw := &fakeCloudWatchMetricClient{output: &cloudwatch.GetMetricStatisticsOutput{Datapoints: []cwtypes.Datapoint{{Timestamp: &now, Average: aws.Float64(25), Sum: aws.Float64(25), Maximum: aws.Float64(25)}}}}
			calls := 0
			rows, count, err := collectECSInfra(context.Background(), client, cw, now.Unix()/60*60, func(string, []infraAssetObservation) error {
				calls++
				if fail {
					return errors.New("injected local SQLITE_BUSY")
				}
				return nil
			})
			if err != nil || count != 1 || transport.calls != 4 || calls != 1 {
				t.Fatalf("local registry error escaped as AWS failure: count=%d calls=%d observer=%d err=%v", count, transport.calls, calls, err)
			}
			metrics := map[string]float64{}
			for _, r := range rows {
				if r.Resource == "ecs/test/worker" {
					metrics[r.Metric] = r.Value
				}
			}
			if metrics["cpu"] != 25 || metrics["task_discovery_ok"] != 1 {
				t.Fatalf("successful AWS metrics lost: %+v", metrics)
			}
			if value, ok := metrics["registry_write_ok"]; !ok || value != boolFloat(!fail) {
				t.Fatalf("missing registry health: %+v", metrics)
			}
			m := newTestMonitor(t)
			if fail && m.infraStatus(InfraResource{Type: "ecs_service", Metrics: metrics}) != "warn" {
				t.Fatal("registry failure not surfaced as warning")
			}
		})
	}
}

func TestInfraRegistryFailureDoesNotSuppressIndependentAlerts(t *testing.T) {
	for _, fail := range []bool{false, true} {
		t.Run(fmt.Sprint(fail), func(t *testing.T) {
			m := newTestMonitor(t)
			t.Cleanup(m.Close)
			m.cfg.InfraEnabled = true
			c := defaultAlertConfig()
			c.Enabled, c.ServerAlertsEnabled = true, false // Log only; never send mail.
			c.SMTPHost, c.Recipients = "unused.invalid", "unused@invalid"
			if err := m.saveAlertConfig(c); err != nil {
				t.Fatal(err)
			}
			now := time.Now().Unix()
			for _, state := range []string{"active", "archived"} {
				identity := "test-host:" + state
				if err := m.storeDB.Create(&InfraAsset{ID: infraAssetID(identity), Identity: identity, Resource: state, Kind: "host", State: state, Revision: 1}).Error; err != nil {
					t.Fatal(err)
				}
			}
			rows := []InfraSample{
				{Resource: "rds/review", RType: "database", Metric: "available", Value: 0},
				{Resource: "alb/review", RType: "lb", Metric: "unhealthy", Value: 1},
				{Resource: "test.invalid", RType: "probe", Metric: "reachable", Value: 0},
				{Resource: "test.invalid", RType: "probe", Metric: "cert_days", Value: 1},
				{Resource: "active", RType: "instance", Metric: "cpu", Value: 99},
				{Resource: "archived", RType: "instance", Metric: "cpu", Value: 99},
			}
			for i := range rows {
				rows[i].BucketTs = now / 60 * 60
			}
			if err := m.upsertInfra(rows); err != nil {
				t.Fatal(err)
			}
			if fail {
				// Break only the directory in this disposable test DB, leaving
				// infrastructure facts and alert storage available.
				if err := m.storeDB.Migrator().DropTable(&InfraAsset{}); err != nil {
					t.Fatal(err)
				}
			}
			m.evaluateInfraAlerts(now)
			var alerts []AlertLog
			if err := m.storeDB.Find(&alerts).Error; err != nil {
				t.Fatal(err)
			}
			counts := map[string]int{}
			for _, alert := range alerts {
				counts[alert.Kind]++
				if alert.Target == "archived" || strings.HasPrefix(alert.Target, "archived/") {
					t.Fatalf("archived resource alerted: %+v", alert)
				}
			}
			for _, kind := range []string{"infra_db_unavailable", "infra_lb_unhealthy", "infra_probe_down", "infra_probe_cert"} {
				if counts[kind] != 1 {
					t.Errorf("registry failure=%v: %s count=%d, want 1", fail, kind, counts[kind])
				}
			}
			if counts["infra_registry_failed"] != int(boolFloat(fail)) || counts["infra_instance_cpu"] != int(boolFloat(!fail)) {
				t.Fatalf("wrong membership-dependent alerts: %+v", counts)
			}
		})
	}
}
