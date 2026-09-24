package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs"
	cwlogtypes "github.com/aws/aws-sdk-go-v2/service/cloudwatchlogs/types"
	"github.com/gin-gonic/gin"
	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
)

func enableInvestigationAudit(t *testing.T, m *Monitor) {
	t.Helper()
	db, err := gorm.Open(sqlite.Open(":memory:"), &gorm.Config{})
	if err != nil {
		t.Fatal(err)
	}
	sqlDB, err := db.DB()
	if err != nil {
		t.Fatal(err)
	}
	// SQLite 的 :memory: 按连接隔离；固定一个连接，确保异步审计看到同一张表。
	sqlDB.SetMaxOpenConns(1)
	if err := db.AutoMigrate(&CloudWatchInvestigationAudit{}); err != nil {
		t.Fatal(err)
	}
	m.storeDB = db
	t.Cleanup(func() {
		m.investigationWG.Wait()
		_ = sqlDB.Close()
	})
}

func TestParseLogChainInvestigationInputUsesBoundedWindows(t *testing.T) {
	now := time.Date(2026, 9, 17, 4, 0, 0, 0, time.UTC)
	input, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{
		NewAPIRequestID: "req-123", AtUnix: now.Add(-time.Minute).Unix(),
	}, now)
	if err != nil {
		t.Fatal(err)
	}
	if input.To.Sub(input.From) != 4*time.Minute || input.Timezone != "Asia/Shanghai" {
		t.Fatalf("exact request window=%v timezone=%s", input.To.Sub(input.From), input.Timezone)
	}
	for _, bad := range []logChainInvestigationCreateRequest{
		{},
		{NewAPIRequestID: "req bad", AtUnix: now.Unix()},
		{Path: "v1/responses", AtUnix: now.Unix()},
		{ClientIP: "not-an-ip", AtUnix: now.Unix()},
		{Model: "gpt-5", From: now.Add(-3 * time.Hour).Format(time.RFC3339), To: now.Format(time.RFC3339)},
	} {
		if _, err := parseLogChainInvestigationInput(bad, now); err == nil {
			t.Fatalf("invalid investigation input accepted: %+v", bad)
		}
	}
}

func TestParseLogChainInvestigationInputAcceptsConfiguredGroupPunctuation(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	for _, group := range []string{"Claude max (distributor)#1", "secret-group"} {
		input, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{
			NewAPIRequestID: "req-group-punctuation", AtUnix: now.Unix(),
			Model: "gpt-5/extended", Group: group,
		}, time.Now())
		if err != nil {
			t.Fatalf("合法的客户分组/模型名不应阻断精确请求排障 (%q): %v", group, err)
		}
		if input.Group != group || input.Model != "gpt-5/extended" {
			t.Fatalf("业务标签被意外改写: model=%q group=%q", input.Model, input.Group)
		}
	}
}

func TestParseLogChainInvestigationInputRejectsControlInScopeLabel(t *testing.T) {
	now := time.Now().UTC().Add(-time.Minute)
	for _, group := range []string{"bad\nname", "bad\x00name", strings.Repeat("g", 65)} {
		if _, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{
			NewAPIRequestID: "req-group-control", AtUnix: now.Unix(), Group: group,
		}, time.Now()); err == nil {
			t.Fatalf("不安全/超长分组名被接受: %q", group)
		}
	}
}

func TestCloudWatchInvestigationAuditBumpsMigrationPlan(t *testing.T) {
	if !strings.Contains(preMigrationPlanID, "v52") || !strings.Contains(preMigrationPlanID, "cloudwatch-investigation-audit-shadow-reconciliation-preroute-cursor-nginx-cursor-v1-nginx-repair-cursor-v1") {
		t.Fatalf("CloudWatch 阶段三/四表已进入 AutoMigrate，但迁移计划未同步升级: %s", preMigrationPlanID)
	}
	if !strings.Contains(preMigrationCombinedPlanID, "cloudwatch-investigation-audit-shadow-reconciliation-preroute-cursor-nginx-cursor-v1-nginx-repair-cursor-v1") ||
		!strings.Contains(preMigrationCombinedPlanID, "nginx-evidence-backfill-v1-nginx-source-v2") {
		t.Fatalf("组合迁移计划未包含 CloudWatch Shadow schema: %s", preMigrationCombinedPlanID)
	}
}

func TestAsyncInvestigationReturnsAllSourceStatesAndRedactedTimeline(t *testing.T) {
	now := time.Now().UTC().Add(-2 * time.Minute)
	requestID := "reqstage3fixture12345"
	client := &fakeCloudWatchLogsClient{}
	client.filterFn = func(_ context.Context, in *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		if aws.ToString(in.LogGroupName) == "nexusapi-cloudfront-access" {
			message := fmt.Sprintf(`{"timestamp(ms)":%d,"x-edge-request-id":"edge-stage3","cs-method":"POST","x-host-header":"us.nexusapi.link","cs-uri-stem":"/v1/responses","sc-status":"200","time-taken":"0.350"}`, now.UnixMilli())
			return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{
				EventId: aws.String("edge-event"), LogStreamName: aws.String("edge-stream"), Timestamp: aws.Int64(now.UnixMilli()), Message: aws.String(message),
			}}}, nil
		}
		if aws.ToString(in.LogStreamNamePrefix) == "nginx/" {
			return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{
				EventId: aws.String("nginx-event"), LogStreamName: aws.String("nginx/task"), Timestamp: aws.Int64(now.UnixMilli()),
				Message: aws.String(logChainCloudWatchNginxLine(requestID, now.UnixMilli())),
			}}}, nil
		}
		if aws.ToString(in.LogStreamNamePrefix) == "new-api/" {
			return &cloudwatchlogs.FilterLogEventsOutput{Events: []cwlogtypes.FilteredLogEvent{{
				EventId: aws.String("app-event"), LogStreamName: aws.String("new-api/task"), Timestamp: aws.Int64(now.UnixMilli()),
				Message: aws.String("[GIN] 2026/09/17 - 12:00:00 | relay | " + requestID + " | 200 | 10ms | 203.0.113.1 | POST /v1/responses"),
			}}}, nil
		}
		return &cloudwatchlogs.FilterLogEventsOutput{}, nil
	}
	m := newLogChainCloudWatchTestMonitor(t, client)
	enableInvestigationAudit(t, m)
	input, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{NewAPIRequestID: requestID, AtUnix: now.Unix()}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	created, err := m.createLogChainInvestigation("owner", input)
	if err != nil || created.Status != "queued" {
		t.Fatalf("created=%+v err=%v", created, err)
	}
	var result logChainInvestigationResult
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		result, _ = m.getLogChainInvestigation("owner", created.InvestigationID)
		if result.Status != "queued" && result.Status != "running" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(result.SourceStatus) != len(logChainInvestigationSourceOrder) {
		t.Fatalf("source status incomplete: %+v", result.SourceStatus)
	}
	if len(result.Timeline) < 2 || result.Cost.Queries < 2 {
		t.Fatalf("timeline/cost incomplete: timeline=%+v cost=%+v", result.Timeline, result.Cost)
	}
	encoded, _ := json.Marshal(result)
	for _, forbidden := range []string{requestID, "nginx/task", "203.0.113.1"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("investigation response leaked %q: %s", forbidden, encoded)
		}
	}
	// 相同范围应命中 HMAC 缓存，不再创建第二轮 AWS 查询。
	client.mu.Lock()
	before := len(client.filterInputs) + len(client.startInputs)
	client.mu.Unlock()
	cached, err := m.createLogChainInvestigation("owner", input)
	if err != nil || !cached.Cost.CacheHit || cached.Status != "complete" {
		t.Fatalf("cache result=%+v err=%v", cached, err)
	}
	client.mu.Lock()
	after := len(client.filterInputs) + len(client.startInputs)
	client.mu.Unlock()
	if after != before {
		t.Fatalf("cache hit queried AWS again: before=%d after=%d", before, after)
	}
	var audits []CloudWatchInvestigationAudit
	if err := m.storeDB.Order("id ASC").Find(&audits).Error; err != nil || len(audits) < 4 {
		t.Fatalf("audit rows=%+v err=%v", audits, err)
	}
	encodedAudit, _ := json.Marshal(audits)
	if strings.Contains(string(encodedAudit), requestID) || audits[len(audits)-1].HMACKeyID != "fixture-v1" || audits[len(audits)-1].PurposeClass != "customer_troubleshooting" {
		t.Fatalf("audit leaked raw identifier or lost key id: %s", encodedAudit)
	}
}

func TestInvestigationCancellationStopsAWSQuery(t *testing.T) {
	started := make(chan struct{}, 1)
	client := &fakeCloudWatchLogsClient{filterFn: func(ctx context.Context, _ *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	m := newLogChainCloudWatchTestMonitor(t, client)
	enableInvestigationAudit(t, m)
	input, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{NewAPIRequestID: "req-cancel", AtUnix: time.Now().Add(-time.Minute).Unix()}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	created, err := m.createLogChainInvestigation("owner", input)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("AWS query did not start")
	}
	result, ok := m.cancelLogChainInvestigation("owner", created.InvestigationID)
	if !ok || result.Status != "cancelled" {
		t.Fatalf("cancel result=%+v ok=%v", result, ok)
	}
	if other, ok := m.getLogChainInvestigation("someone-else", created.InvestigationID); ok || other.InvestigationID != "" {
		t.Fatal("another operator could read the task")
	}
}

func TestInvestigationRefusesToQueryWithoutAuditStore(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newLogChainCloudWatchTestMonitor(t, client)
	input, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{NewAPIRequestID: "req-audit", AtUnix: time.Now().Add(-time.Minute).Unix()}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	if _, err := m.createLogChainInvestigation("owner", input); err == nil || !strings.Contains(err.Error(), "审计") {
		t.Fatalf("missing audit store was not fail-closed: %v", err)
	}
	m.investigationWG.Wait()
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.filterInputs) != 0 || len(client.startInputs) != 0 {
		t.Fatal("AWS was queried before the mandatory audit event was recorded")
	}
}

func TestInvestigationRejectsConcurrentTaskForSameOperator(t *testing.T) {
	started := make(chan struct{}, 1)
	client := &fakeCloudWatchLogsClient{filterFn: func(ctx context.Context, _ *cloudwatchlogs.FilterLogEventsInput) (*cloudwatchlogs.FilterLogEventsOutput, error) {
		select {
		case started <- struct{}{}:
		default:
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	m := newLogChainCloudWatchTestMonitor(t, client)
	enableInvestigationAudit(t, m)
	input, err := parseLogChainInvestigationInput(logChainInvestigationCreateRequest{NewAPIRequestID: "req-one-active", AtUnix: time.Now().Add(-time.Minute).Unix()}, time.Now())
	if err != nil {
		t.Fatal(err)
	}
	first, err := m.createLogChainInvestigation("owner", input)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first task did not start")
	}
	if _, err := m.createLogChainInvestigation("owner", input); err == nil {
		t.Fatal("same operator was allowed to start a second active task")
	} else if active, ok := err.(*logChainInvestigationActiveError); !ok || active.Status != "running" {
		t.Fatalf("unexpected concurrent-task error: %+v", err)
	}
	if _, ok := m.cancelLogChainInvestigation("owner", first.InvestigationID); !ok {
		t.Fatal("failed to cancel first task")
	}
}

func TestPendingDeliveryRecheckOnlyQueriesCloudFront(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newLogChainCloudWatchTestMonitor(t, client)
	now := time.Now().UTC().Add(-time.Minute)
	in := logChainInvestigationInput{
		From: now.Add(-2 * time.Minute), To: now.Add(2 * time.Minute), Timezone: "Asia/Shanghai",
		CloudFrontRequestID: "edge-recheck", IncludeSensitiveDiagnostics: true, Purpose: "客户排障",
	}
	statuses := make([]logChainCloudWatchSourceStatus, 0, len(logChainInvestigationSourceOrder))
	for _, source := range logChainInvestigationSourceOrder {
		status := m.cloudWatchSourceStatus(logChainCloudWatchPlan{Source: source, Linkage: "skipped"}, "skipped", "seed")
		if source == cwSourceCloudFrontAccess {
			status.Status, status.Linkage = "empty", "exact"
		}
		if source == cwSourceWorkerNewAPI {
			status.Status, status.Linkage, status.Parsed = "found", "exact", 1
		}
		statuses = append(statuses, status)
	}
	task := &logChainInvestigationTask{ID: "inv_0123456789abcdef0123456789abcdef", Owner: "owner", Input: in, Status: "pending_delivery", CreatedAt: time.Now()}
	task.Result = &logChainInvestigationResult{
		OK: true, InvestigationID: task.ID, Status: "pending_delivery", Scope: m.investigationScopeView(in),
		SourceStatus: statuses, AuditRecorded: false,
	}
	m.investigationTasks = map[string]*logChainInvestigationTask{task.ID: task}
	result := m.recheckPendingCloudFront(context.Background(), task)
	client.mu.Lock()
	inputs := append([]*cloudwatchlogs.FilterLogEventsInput(nil), client.filterInputs...)
	client.mu.Unlock()
	if len(inputs) != 2 {
		t.Fatalf("CloudFront access+diagnostic queries=%d, want 2", len(inputs))
	}
	for _, input := range inputs {
		if !strings.HasPrefix(aws.ToString(input.LogGroupName), "nexusapi-cloudfront-") {
			t.Fatalf("delayed recheck queried a non-CloudFront source: %+v", input)
		}
	}
	if result.AuditRecorded {
		t.Fatal("recheck incorrectly erased an earlier audit failure")
	}
	if result.Cost.Queries != 2 {
		t.Fatalf("recheck query cost=%+v", result.Cost)
	}
	final := finalizePendingDeliveryRecheck(result, true)
	if final.Status != "partial" || len(final.BlindSpots) == 0 {
		t.Fatalf("final delayed-delivery recheck did not terminate safely: %+v", final)
	}
}

func TestPendingDeliveryCancellationReturnsCancelledImmediately(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	enableInvestigationAudit(t, m)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := logChainInvestigationInput{From: time.Now().Add(-time.Minute), To: time.Now(), Timezone: "Asia/Shanghai", NewAPIRequestID: "req-pending"}
	owner := m.investigationDigest("cloudwatch-investigation-operator", "owner")
	task := &logChainInvestigationTask{
		ID: "inv_abcdefabcdefabcdefabcdefabcdefab", Owner: owner, Input: in, Status: "pending_delivery",
		Cancel: cancel, CreatedAt: time.Now().Add(-time.Minute),
		Result: &logChainInvestigationResult{OK: true, Status: "pending_delivery", AuditRecorded: true},
	}
	m.investigationTasks = map[string]*logChainInvestigationTask{task.ID: task}
	result, ok := m.cancelLogChainInvestigation("owner", task.ID)
	if !ok || result.Status != "cancelled" {
		t.Fatalf("pending cancellation returned stale state: ok=%v result=%+v", ok, result)
	}
	select {
	case <-ctx.Done():
	default:
		t.Fatal("pending cancellation did not stop delayed rechecks")
	}
}

func TestCloudFrontMultipleEqualCandidatesStayAmbiguous(t *testing.T) {
	status := 200
	requestMS := int64(1000)
	rows := []LogChainRow{{CreatedAt: 100, RequestPath: "/v1/responses", UseTime: 1}}
	evidence := []cloudWatchStructuredEvidence{
		{EventRef: "a", EventMS: 100000, Route: "/v1/responses", Status: &status, RequestMS: &requestMS},
		{EventRef: "b", EventMS: 100000, Route: "/v1/responses", Status: &status, RequestMS: &requestMS},
	}
	levels := cloudFrontEvidenceLevels(rows, evidence, logChainInvestigationInput{Status: 200})
	if levels["a"] != "ambiguous" || levels["b"] != "ambiguous" {
		t.Fatalf("equal candidates were presented as certain: %+v", levels)
	}
}

func TestInvestigationCreateHTTPRejectsUnknownAndTrailingJSON(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	router := gin.New()
	router.POST("/logchain/investigations", func(c *gin.Context) {
		c.Set("uname", "owner")
		m.serveCreateLogChainInvestigation(c)
	})
	for _, body := range []string{
		`{"newapi_request_id":"req-1","at_unix":1,"unknown":true}`,
		`{"newapi_request_id":"req-1","at_unix":1} {}`,
	} {
		req := httptest.NewRequest(http.MethodPost, "/logchain/investigations", strings.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		if w.Code != http.StatusBadRequest {
			t.Fatalf("body=%q status=%d response=%s", body, w.Code, w.Body.String())
		}
	}
}
