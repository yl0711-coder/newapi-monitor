package monitor

import (
	"context"
	"encoding/json"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/aws/smithy-go"
)

const logChainCloudWatchTestKey = "fixture-logchain-cloudwatch-hmac-key-32bytes"

// newLogChainCloudWatchTestMonitor 构造一个只启用 CloudWatch 按需查询的 Monitor。
// 不连生产库、不连 AWS：客户端由 fake 注入。
func newLogChainCloudWatchTestMonitor(t *testing.T, client cloudWatchLogsAPI) *Monitor {
	t.Helper()
	m := &Monitor{cfg: Settings{
		CloudWatchLogsEnabled:       true,
		CloudWatchEvidenceHMACKey:   logChainCloudWatchTestKey,
		CloudWatchEvidenceHMACKeyID: "fixture-v1",
	}}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	return m
}

func logChainCloudWatchNginxLine(requestID string, atMS int64) string {
	return `{"log_schema":2,"msec":"` + strconv.FormatInt(atMS/1000, 10) + `.250",` +
		`"request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.350",` +
		`"upstream_status":"200","upstream_response_time":"0.300","upstream_connect_time":"0.025",` +
		`"upstream_header_time":"0.125","bytes_sent":"1024","nginx_request_id":"nginx-raw",` +
		`"oneapi_request_id":"` + requestID + `","request_completion":"OK"}`
}

func TestLogChainCloudWatchLookupSeparatesFoundFromEmpty(t *testing.T) {
	const requestID = "req-fixture-1"
	at := time.Date(2026, 9, 15, 2, 0, 0, 0, time.UTC)
	client := &fakeCloudWatchLogsClient{}
	client.filterFn = func(_ context.Context, in *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		// 只有 Worker Nginx 来源返回事件；其余来源返回空页。
		if aws.ToString(in.LogStreamNamePrefix) != "nginx/" {
			return &cloudwatchlogs.FilterLogEventsOutput{}, nil
		}
		return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{
			EventId: aws.String("e1"), LogStreamName: aws.String("nginx/task"),
			Timestamp: aws.Int64(at.UnixMilli()), Message: aws.String(logChainCloudWatchNginxLine(requestID, at.UnixMilli())),
		}}}, nil
	}
	m := newLogChainCloudWatchTestMonitor(t, client)
	got := m.lookupLogChainCloudWatchEvidence(context.Background(), requestID, at, false, false)
	if !got.Enabled || got.SensitiveRequested || got.SensitiveIncluded {
		t.Fatalf("result flags=%+v", got)
	}
	if got.FromUnix != at.Add(-logChainCloudWatchWindow).Unix() || got.ToUnix != at.Add(logChainCloudWatchWindow).Unix() {
		t.Fatalf("window from=%d to=%d", got.FromUnix, got.ToUnix)
	}
	byStatus := map[cloudWatchLogSourceID]logChainCloudWatchSourceStatus{}
	for _, s := range got.Sources {
		byStatus[s.Source] = s
	}
	if len(got.Sources) != 3 {
		t.Fatalf("default lookup must cover exactly the three non-sensitive sources: %+v", got.Sources)
	}
	if byStatus[cwSourceWorkerNginx].Status != "found" || byStatus[cwSourceWorkerNginx].Linkage != "exact" {
		t.Fatalf("nginx=%+v", byStatus[cwSourceWorkerNginx])
	}
	// 空结果必须是 empty，不能是 unavailable —— 否则会被读成数据源故障。
	if byStatus[cwSourceWorkerNewAPI].Status != "empty" || byStatus[cwSourceCloudFrontAccess].Status != "empty" {
		t.Fatalf("empty sources misreported: %+v", got.Sources)
	}
	// CloudFront 与 NewAPI Request ID 未统一，禁止标 exact。
	if byStatus[cwSourceCloudFrontAccess].Linkage == "exact" {
		t.Fatal("CloudFront linkage must not claim exact before request-ID unification")
	}
	if _, ok := byStatus[cwSourceCloudFrontDiagnostic]; ok {
		t.Fatal("sensitive diagnostic source must not be queried by default")
	}
	if len(got.Evidence) != 1 || got.Evidence[0].Source != cwSourceWorkerNginx || len(got.Evidence[0].Evidence) != 1 {
		t.Fatalf("evidence=%+v", got.Evidence)
	}
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{requestID, "nginx-raw", "nginx/task", "log_schema", "oneapi_request_id"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("response leaked %q", forbidden)
		}
	}
}

// 成本约束：按需查询只能用 FilterLogEvents。Logs Insights 按扫描字节计费，
// 一旦误用，页面上看不出区别，只会在 AWS 账单上出现。
func TestLogChainCloudWatchNeverUsesBilledInsightsQueries(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newLogChainCloudWatchTestMonitor(t, client)
	m.lookupLogChainCloudWatchEvidence(context.Background(), "req-cost", time.Now().UTC(), false, false)
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.filterInputs) == 0 {
		t.Fatal("expected FilterLogEvents calls")
	}
	if len(client.startInputs) != 0 || len(client.getInputs) != 0 || len(client.stopInputs) != 0 {
		t.Fatalf("on-demand lookup must not run Logs Insights: start=%d get=%d stop=%d",
			len(client.startInputs), len(client.getInputs), len(client.stopInputs))
	}
}

func TestLogChainCloudWatchClassifiesFailuresPerSourceWithoutLeaking(t *testing.T) {
	secret := "aws-detail-with-req-fixture-inside"
	cases := []struct {
		name   string
		err    error
		expect string
	}{
		{"denied", &smithy.GenericAPIError{Code: "AccessDeniedException", Message: secret}, "access_denied"},
		{"throttled", &smithy.GenericAPIError{Code: "ThrottlingException", Message: secret}, "throttled"},
		{"timeout", context.DeadlineExceeded, "timeout"},
		{"other", &smithy.GenericAPIError{Code: "Whatever", Message: secret}, "unavailable"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			client := &fakeCloudWatchLogsClient{filterFn: func(context.Context, *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
				return nil, tc.err
			}}
			m := newLogChainCloudWatchTestMonitor(t, client)
			got := m.lookupLogChainCloudWatchEvidence(context.Background(), "req-fail", time.Now().UTC(), false, false)
			for _, s := range got.Sources {
				if s.Status != tc.expect {
					t.Fatalf("source=%s status=%s want=%s", s.Source, s.Status, tc.expect)
				}
			}
			if len(got.Evidence) != 0 {
				t.Fatal("failed lookup must not fabricate evidence")
			}
			encoded, _ := json.Marshal(got)
			if strings.Contains(string(encoded), secret) || strings.Contains(string(encoded), "req-fail") {
				t.Fatalf("failure response leaked details: %s", encoded)
			}
		})
	}
}

// 格式漂移：窗口内有事件但一条都解析不了，必须报 unavailable 而不是 empty。
// 报 empty 会让人得出"这个请求没有到达"的错误结论。
func TestLogChainCloudWatchTreatsUnparsableEventsAsUnavailable(t *testing.T) {
	client := &fakeCloudWatchLogsClient{filterFn: func(context.Context, *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{
			EventId: aws.String("bad"), Timestamp: aws.Int64(time.Now().UnixMilli()),
			Message: aws.String("completely unrecognized line"),
		}}}, nil
	}}
	m := newLogChainCloudWatchTestMonitor(t, client)
	got := m.lookupLogChainCloudWatchEvidence(context.Background(), "req-drift", time.Now().UTC(), false, false)
	for _, s := range got.Sources {
		if s.Status != "unavailable" || s.ParseFailed == 0 {
			t.Fatalf("format drift must be explicit: %+v", s)
		}
	}
	metric := m.cloudWatchLogs.metricsSnapshot()[cwSourceWorkerNginx]
	if metric.ParseFailures == 0 {
		t.Fatalf("parse failures must be counted: %+v", metric)
	}
}

// 总预算：来源串行查询，单来源自身已有 25 秒超时。AWS 不可达时若不设总预算，
// 3~4 个来源会累加到 75~100 秒，浏览器早已放弃且白占并发闸门。
// 超预算的来源必须如实标 skipped，不能假装查过（那会变成"确认没有"）。
func TestLogChainCloudWatchStopsAtTotalBudget(t *testing.T) {
	if logChainCloudWatchTotalBudget > cloudWatchLogsTotalTimeout*2 {
		t.Fatalf("总预算 %v 相对单来源超时 %v 过大，AWS 不可达时仍会把浏览器挂死",
			logChainCloudWatchTotalBudget, cloudWatchLogsTotalTimeout)
	}
	client := &fakeCloudWatchLogsClient{filterFn: func(ctx context.Context, _ *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		<-ctx.Done() // 模拟 AWS 无响应
		return nil, ctx.Err()
	}}
	m := newLogChainCloudWatchTestMonitor(t, client)
	ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
	defer cancel()
	started := time.Now()
	got := m.lookupLogChainCloudWatchEvidence(ctx, "req-budget", time.Now().UTC(), false, false)
	// 预算耗尽后必须立即返回，不再逐个来源重新等满。
	if elapsed := time.Since(started); elapsed > 5*time.Second {
		t.Fatalf("预算耗尽后仍逐来源等待：耗时 %v", elapsed)
	}
	if len(got.Sources) != 3 {
		t.Fatalf("必须为每个计划来源都给出状态，缺失会让人以为没查这一层: %+v", got.Sources)
	}
	skipped := 0
	for _, s := range got.Sources {
		if s.Status == "skipped" {
			skipped++
		}
		if s.Status == "empty" || s.Status == "found" {
			t.Fatalf("超时来源不得报成空或已找到: %+v", s)
		}
	}
	if skipped == 0 {
		t.Fatalf("预算耗尽后剩余来源应标 skipped: %+v", got.Sources)
	}
}

func TestLogChainCloudWatchSensitiveSourceRequiresAuthorization(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newLogChainCloudWatchTestMonitor(t, client)

	// 请求了但未获授权：不得静默降级成"没请求过"，也不得读取敏感来源。
	denied := m.lookupLogChainCloudWatchEvidence(context.Background(), "req-sens", time.Now().UTC(), true, false)
	if !denied.SensitiveRequested || denied.SensitiveIncluded {
		t.Fatalf("unauthorized sensitive request flags=%+v", denied)
	}
	for _, s := range denied.Sources {
		if s.Source == cwSourceCloudFrontDiagnostic {
			t.Fatal("unauthorized lookup queried the sensitive diagnostic source")
		}
	}

	allowed := m.lookupLogChainCloudWatchEvidence(context.Background(), "req-sens", time.Now().UTC(), true, true)
	if !allowed.SensitiveIncluded || len(allowed.Sources) != 4 {
		t.Fatalf("authorized lookup=%+v", allowed.Sources)
	}
}

func TestLogChainCloudWatchInputIsAClosedSet(t *testing.T) {
	now := time.Date(2026, 9, 15, 3, 0, 0, 0, time.UTC)
	valid := logChainCloudWatchRequest{RequestID: "req-ok", AtUnix: now.Add(-time.Minute).Unix()}
	if _, _, err := parseLogChainCloudWatchInput(valid, now); err != nil {
		t.Fatalf("valid input rejected: %v", err)
	}
	bad := []logChainCloudWatchRequest{
		{RequestID: "", AtUnix: now.Unix()},
		{RequestID: "req ok", AtUnix: now.Unix()},
		{RequestID: `req"inject`, AtUnix: now.Unix()},
		{RequestID: "req-ok", AtUnix: 0},
		{RequestID: "req-ok", AtUnix: now.Add(time.Hour).Unix()},
		{RequestID: "req-ok", AtUnix: now.Add(-60 * 24 * time.Hour).Unix()},
	}
	for _, in := range bad {
		if _, _, err := parseLogChainCloudWatchInput(in, now); err == nil {
			t.Fatalf("invalid input accepted: %+v", in)
		}
	}
}

// 开关关闭时按需查询入口必须不可用，且绝不初始化 AWS 客户端。
func TestLogChainCloudWatchDisabledKeepsRuntimeDormant(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	created := 0
	m := &Monitor{cfg: Settings{CloudWatchLogsEnabled: false}}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(false, func(context.Context, string) (cloudWatchLogsAPI, error) {
		created++
		return client, nil
	})
	if m.cloudWatchEvidenceAvailable() {
		t.Fatal("disabled runtime must not advertise availability")
	}
	got := m.lookupLogChainCloudWatchEvidence(context.Background(), "req-off", time.Now().UTC(), false, false)
	if got.Enabled {
		t.Fatal("disabled lookup must not report itself as enabled")
	}
	// 必须是 disabled 而不是 unavailable：后者会让人去排查 AWS 权限与网络，
	// 而真实原因只是功能没开。
	for _, s := range got.Sources {
		if s.Status != "disabled" {
			t.Fatalf("source=%s status=%s", s.Source, s.Status)
		}
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if created != 0 || len(client.filterInputs) != 0 {
		t.Fatalf("disabled lookup touched AWS: created=%d calls=%d", created, len(client.filterInputs))
	}
}

// 密钥不合规时必须 fail closed：宁可不查，也不能回退成回传原始标识。
func TestCloudWatchEvidenceKeyIsRequiredAndFailsClosed(t *testing.T) {
	base := Settings{CloudWatchLogsEnabled: true, CloudWatchEvidenceHMACKey: logChainCloudWatchTestKey, CloudWatchEvidenceHMACKeyID: "v1"}
	if err := validateCloudWatchLogsSettings(base); err != nil {
		t.Fatalf("valid settings rejected: %v", err)
	}
	bad := []Settings{
		{CloudWatchLogsEnabled: true},
		{CloudWatchLogsEnabled: true, CloudWatchEvidenceHMACKey: "short", CloudWatchEvidenceHMACKeyID: "v1"},
		{CloudWatchLogsEnabled: true, CloudWatchEvidenceHMACKey: logChainCloudWatchTestKey},
		{CloudWatchLogsEnabled: true, CloudWatchEvidenceHMACKey: logChainCloudWatchTestKey, CloudWatchEvidenceHMACKeyID: "bad id"},
		{CloudWatchLogsEnabled: true, CloudWatchEvidenceHMACKey: logChainCloudWatchTestKey, CloudWatchEvidenceHMACKeyID: "v1", SessionSecret: logChainCloudWatchTestKey},
		{CloudWatchLogsEnabled: true, CloudWatchEvidenceHMACKey: logChainCloudWatchTestKey, CloudWatchEvidenceHMACKeyID: "v1", IngestToken: logChainCloudWatchTestKey},
	}
	for _, s := range bad {
		if err := validateCloudWatchLogsSettings(s); err == nil {
			t.Fatalf("unsafe settings accepted: %+v", s.CloudWatchEvidenceHMACKeyID)
		}
	}
	// 密钥缺失时按需查询也必须整体拒绝，而不是裸奔返回原始 ID。
	m := &Monitor{cfg: Settings{CloudWatchLogsEnabled: true}}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(true, func(context.Context, string) (cloudWatchLogsAPI, error) {
		t.Fatal("must not reach AWS without a valid evidence key")
		return nil, nil
	})
	got := m.lookupLogChainCloudWatchEvidence(context.Background(), "req-nokey", time.Now().UTC(), false, false)
	for _, s := range got.Sources {
		if s.Status != "unavailable" {
			t.Fatalf("source=%s status=%s", s.Source, s.Status)
		}
	}
}

// 盲区文案必须随开关变化，否则页面上有按钮、文案却说"不在客户排障中展示"。
func TestLogChainBlindSpotsFollowCloudWatchAvailability(t *testing.T) {
	off := logChainBlindSpots(false)
	on := logChainBlindSpots(true)
	if len(off) != len(on) || len(off) == 0 {
		t.Fatalf("blind spot count changed: off=%d on=%d", len(off), len(on))
	}
	if !strings.Contains(off[0], "不在客户排障中展示") {
		t.Fatalf("disabled wording changed: %s", off[0])
	}
	if strings.Contains(on[0], "不在客户排障中展示") {
		t.Fatalf("enabled wording still denies availability: %s", on[0])
	}
	for _, want := range []string{"按需", "不是统计", "问题预警"} {
		if !strings.Contains(on[0], want) {
			t.Fatalf("enabled wording missing %q: %s", want, on[0])
		}
	}
	// ★ 开启态不得声称能在本页查到被前置拒绝的请求 ★
	// 按需查询用的是所展开那一行自己的 Request ID，而行只来自生产 logs；
	// 前置拒绝不写 logs，就没有行、也没有按钮。曾写成"展开任意一条即可查到
	// 该次路由前拒绝"，会让人以为不必再去问题预警，从而漏掉真正被拒的请求。
	if strings.Contains(on[0], "任意一条") {
		t.Fatalf("开启态不得声称展开任意一条即可查到前置拒绝: %s", on[0])
	}
	// 开启后仍必须把"找被拒请求去问题预警"和客户 ID 关联限制讲清楚，
	// 不能因为新增按钮就把既有的正确指引删掉。
	for _, want := range []string{"不能用来发现", "客户 ID"} {
		if !strings.Contains(on[0], want) {
			t.Fatalf("开启态缺少按需证据的适用边界说明 %q: %s", want, on[0])
		}
	}
}
