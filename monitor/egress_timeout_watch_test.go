package monitor

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
)

func egressRow(stream, upstream, n string) map[string]string {
	return map[string]string{"@logStream": stream, "upstream": upstream, "timeouts": n}
}

func TestEgressTimeoutFindingsReportOnlyConcentratedInstances(t *testing.T) {
	bad := "new-api/new-api/66cbede0df5146089338bef3091e45c0"
	// Production 2026-11-18 01:30-01:40Z: one task, six mainland upstreams.
	incident := []map[string]string{
		egressRow(bad, "47.93.195.130", "7"), egressRow(bad, "47.93.232.227", "7"), egressRow(bad, "47.93.234.101", "7"),
		egressRow(bad, "47.93.226.25", "5"), egressRow(bad, "180.184.47.154", "3"), egressRow(bad, "101.126.13.31", "3"),
		egressRow("new-api/new-api/aaaa", "47.93.195.130", "1"),
	}
	got := egressTimeoutFindings(incident, 10)
	if len(got) != 1 || got[0].Task != "66cbede0df5146089338bef3091e45c0" || got[0].Timeouts != 32 || got[0].Total != 33 ||
		len(got[0].Upstreams) != 6 || got[0].Upstreams[0].Address != "47.93.195.130" || got[0].Upstreams[5].Count != 3 {
		t.Fatalf("incident not reported as one concentrated instance: %+v", got)
	}
	// An upstream down for everyone: spread over many tasks, no instance alert.
	var spread []map[string]string
	for _, task := range []string{"a", "b", "c", "d", "e", "f"} {
		spread = append(spread, egressRow("new-api/new-api/"+task, "1.2.3.4", "12"))
	}
	if got := egressTimeoutFindings(spread, 10); len(got) != 0 {
		t.Fatalf("upstream outage reported as instance problem: %+v", got)
	}
	// Two bad tasks at once are both reported.
	two := []map[string]string{egressRow("new-api/new-api/x", "1.1.1.1", "20"), egressRow("new-api/new-api/y", "1.1.1.1", "20")}
	if got := egressTimeoutFindings(two, 10); len(got) != 2 {
		t.Fatalf("two bad tasks: %+v", got)
	}
	// Below the threshold, or unparseable rows, are ignored.
	if got := egressTimeoutFindings([]map[string]string{egressRow(bad, "1.1.1.1", "9"), egressRow(bad, "", "x"), egressRow("", "1.1.1.1", "50")}, 10); len(got) != 0 {
		t.Fatalf("below threshold reported: %+v", got)
	}
	// A line the parse pattern missed still counts, under a placeholder.
	if got := egressTimeoutFindings([]map[string]string{egressRow(bad, "", "12")}, 10); len(got) != 1 || got[0].Upstreams[0].Address != "未解析" {
		t.Fatalf("unparsed upstream: %+v", got)
	}
}

func TestEgressTimeoutQueryIsCountOnlyOnNewAPIStreams(t *testing.T) {
	from := time.Date(2026, 11, 18, 1, 30, 0, 0, time.UTC)
	source, query, limit, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{Kind: cwQueryWorkerEgressTimeout, From: from, To: from.Add(10 * time.Minute), Limit: 500})
	if err != nil {
		t.Fatal(err)
	}
	if source.ID != cwSourceWorkerNewAPI || limit != 500 {
		t.Fatalf("source=%v limit=%d", source.ID, limit)
	}
	for _, want := range []string{`filter @logStream like /^new-api\//`, `/dial tcp/`, `/i\/o timeout/`,
		`parse @message /dial tcp (?<upstream>[0-9a-fA-F.:\[\]]+?):(?<port>[0-9]+): i\/o timeout/`,
		"stats count(*) as timeouts by @logStream, upstream", "| sort timeouts desc | limit 500"} {
		if !strings.Contains(query, want) {
			t.Fatalf("query missing %q:\n%s", want, query)
		}
	}
	if strings.Contains(query, "fields") || strings.Contains(query, "@timestamp") {
		t.Fatalf("query must not read log text back: %s", query)
	}
	if _, _, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{Kind: cwQueryWorkerEgressTimeout, From: from, To: from.Add(time.Minute), Value: "x"}); err == nil {
		t.Fatal("a value must be rejected for the fixed query")
	}
}

func TestEgressTimeoutCheckRecordsAlertAndRespectsCooldown(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.EgressTimeoutAlertEnabled, m.cfg.EgressTimeoutAlertMin = true, 10
	var window [2]int64
	client := &fakeCloudWatchLogsClient{
		startFn: func(_ context.Context, in *cloudwatchlogs.StartQueryInput) (*cloudwatchlogs.StartQueryOutput, error) {
			window = [2]int64{aws.ToInt64(in.StartTime), aws.ToInt64(in.EndTime)}
			return &cloudwatchlogs.StartQueryOutput{QueryId: aws.String("q")}, nil
		},
		getFn: func(context.Context, *cloudwatchlogs.GetQueryResultsInput) (*cloudwatchlogs.GetQueryResultsOutput, error) {
			return &cloudwatchlogs.GetQueryResultsOutput{Status: cwlogtypes.QueryStatusComplete, Results: [][]cwlogtypes.ResultField{{
				{Field: aws.String("@logStream"), Value: aws.String("new-api/new-api/66cbede0df5146089338bef3091e45c0")},
				{Field: aws.String("upstream"), Value: aws.String("47.93.195.130")},
				{Field: aws.String("timeouts"), Value: aws.String("32")},
			}}, Statistics: &cwlogtypes.QueryStatistics{RecordsMatched: 32}}, nil
		},
	}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	m.cloudWatchLogs.poll = func(context.Context) error { return nil }
	now := time.Date(2026, 11, 18, 1, 42, 30, 0, time.UTC)
	m.runEgressTimeoutCheck(context.Background(), now)
	if window[1] != time.Date(2026, 11, 18, 1, 40, 0, 0, time.UTC).Unix() || window[1]-window[0] != 600 {
		t.Fatalf("window = %v", window)
	}
	var logs []AlertLog
	if err := m.storeDB.Where("kind = ? OR kind = ?", egressTimeoutAlertKind, egressTimeoutAlertKind+"_FAILED").Find(&logs).Error; err != nil {
		t.Fatal(err)
	}
	// The test monitor has no recipients, so the send is logged as _FAILED;
	// either way the alert is recorded once for the instance.
	if len(logs) != 1 || logs[0].Target != "66cbede0df5146089338bef3091e45c0" {
		t.Fatalf("alert log = %+v", logs)
	}
	if m.egressTimeout.lastSuccess.Load() != now.Unix() || m.egressTimeout.lastAlerts.Load() != 1 {
		t.Fatal("watch state not recorded")
	}
	// Five minutes later the same instance is still inside the cooldown.
	m.runEgressTimeoutCheck(context.Background(), now.Add(5*time.Minute))
	var n int64
	m.storeDB.Model(&AlertLog{}).Where("kind LIKE ?", egressTimeoutAlertKind+"%").Count(&n)
	if n != 1 {
		t.Fatalf("cooldown not applied: %d alert rows", n)
	}
}

func TestEgressTimeoutAlertTextNamesInstanceAndAction(t *testing.T) {
	f := egressTimeoutFindings([]map[string]string{
		egressRow("new-api/new-api/66cbede0df5146089338bef3091e45c0", "47.93.195.130", "30"),
		egressRow("new-api/new-api/66cbede0df5146089338bef3091e45c0", "101.126.13.31", "2"),
	}, 10)[0]
	subject, body := egressTimeoutAlertText(f, egressTimeoutWindow)
	for _, want := range []string{"66cbede0", "近 10 分钟 32 次"} {
		if !strings.Contains(subject, want) {
			t.Fatalf("subject %q missing %q", subject, want)
		}
	}
	for _, want := range []string{"47.93.195.130：30 次", "101.126.13.31：2 次", "占全部实例的 100%", "停止该任务", "固定出口代理"} {
		if !strings.Contains(body, want) {
			t.Fatalf("body missing %q:\n%s", want, body)
		}
	}
}
