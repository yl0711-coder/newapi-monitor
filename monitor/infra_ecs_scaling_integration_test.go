package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatch"
	"github.com/aws/aws-sdk-go-v2/service/ecs"
)

// Real SDK/collector/SQLite path, with every AWS request intercepted locally.
// This verifies resource membership, not ECS log source authorization.
type scalingECSTransport struct {
	fallback registryTestECSTransport
	ids      []int
	failList bool
}

func scalingTaskARN(id int) string {
	return fmt.Sprintf("arn:aws:ecs:us-west-2:123456789012:task/test/%032d", id)
}

func (s *scalingECSTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	var body any
	switch {
	case strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".ListTasks"):
		if s.failList {
			return nil, fmt.Errorf("injected AWS discovery outage")
		}
		arns := make([]string, 0, len(s.ids))
		for _, id := range s.ids {
			arns = append(arns, scalingTaskARN(id))
		}
		body = map[string]any{"taskArns": arns}
	case strings.HasSuffix(req.Header.Get("X-Amz-Target"), ".DescribeTasks"):
		tasks := make([]map[string]string, 0, len(s.ids))
		for _, id := range s.ids {
			tasks = append(tasks, map[string]string{"taskArn": scalingTaskARN(id), "lastStatus": "RUNNING", "memory": "1024"})
		}
		body = map[string]any{"tasks": tasks}
	default:
		return s.fallback.RoundTrip(req)
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return &http.Response{StatusCode: http.StatusOK, Header: http.Header{"Content-Type": {"application/x-amz-json-1.1"}}, Body: io.NopCloser(strings.NewReader(string(encoded))), Request: req}, nil
}

func TestECSScalingCollectorKeepsIdentityAndHistoryAcrossDiscoveryOutage(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	transport := &scalingECSTransport{}
	client := ecs.New(ecs.Options{Region: "us-west-2", Credentials: aws.AnonymousCredentials{}, Retryer: aws.NopRetryer{}, HTTPClient: &http.Client{Transport: transport}})
	cw := &fakeCloudWatchMetricClient{output: &cloudwatch.GetMetricStatisticsOutput{}}
	now := time.Now().Unix()
	collect := func(ids []int, fail bool) {
		t.Helper()
		now++
		transport.ids, transport.failList = ids, fail
		calls := 0
		rows, _, err := collectECSInfra(context.Background(), client, cw, now/60*60, func(scope string, observations []infraAssetObservation) error {
			calls++
			return m.observeInfraAssets(context.Background(), scope, observations, now)
		})
		if err != nil || calls != int(boolFloat(!fail)) {
			t.Fatalf("unexpected collection result: calls=%d error=%v", calls, err)
		}
		found := false
		for _, row := range rows {
			if row.Metric == "task_discovery_ok" {
				found = true
				if row.Value != boolFloat(!fail) {
					t.Fatal("discovery failure masked")
				}
			}
		}
		if !found {
			t.Fatal("missing discovery status")
		}
	}
	for cycle := 0; cycle < 3; cycle++ {
		for _, ids := range [][]int{{1, 2, 3}, {1, 2, 3, 4, 5}, {1, 2, 4}} {
			collect(ids, false)
		}
	}
	before := assetForTest(t, m, scalingTaskARN(2))
	collect(nil, true)
	if after := assetForTest(t, m, before.Identity); !reflect.DeepEqual(before, after) {
		t.Fatal("failed AWS discovery changed existing membership")
	}
	// Rolling replacement: new and old coexist, then an arbitrary old task exits.
	collect([]int{1, 2, 4, 6}, false)
	collect([]int{2, 4, 6}, false)
	for _, id := range []int{1, 3, 5} {
		asset := assetForTest(t, m, scalingTaskARN(id))
		if asset.State != "active" || asset.CloudState != "missing" {
			t.Fatalf("task %d was deleted/archived or declared stopped without evidence", id)
		}
	}
	if after := assetForTest(t, m, before.Identity); after.ID != before.ID || after.FirstSeen != before.FirstSeen {
		t.Fatal("surviving task identity/history changed across scaling")
	}
	var count int64
	if err := m.storeDB.Model(&InfraAsset{}).Where("kind = ?", "ecs_task").Count(&count).Error; err != nil || count != 6 {
		t.Fatalf("task history lost or duplicated: count=%d error=%v", count, err)
	}
}
