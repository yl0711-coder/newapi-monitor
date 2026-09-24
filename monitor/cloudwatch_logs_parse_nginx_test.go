package monitor

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestCloudWatchParseNginxAccessMatchesExistingEvidenceSemantics(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	message := `{"log_schema":2,"msec":"1789363200.250","request_method":"POST","uri":"/v1/responses?token=hidden","status":"200","request_time":"0.350","upstream_status":"502, 200","upstream_response_time":"0.300","upstream_connect_time":"-, 0.025","upstream_header_time":"0.125","bytes_sent":"1024","nginx_request_id":"nginx-raw-id","oneapi_request_id":"oneapi-raw-id","request_completion":"OK"}`
	evidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNginx, EventID: "event-1", LogStream: "nginx/task-secret", Message: message})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Kind != cwEvidenceNginxAccess || evidence.EventMS != 1789363200250 || evidence.Route != "/v1/responses" || evidence.Method != "POST" || evidence.Completion != "complete_at_edge" {
		t.Fatalf("access=%+v", evidence)
	}
	if evidence.Status == nil || *evidence.Status != 200 || evidence.RequestMS == nil || *evidence.RequestMS != 350 || evidence.ConnectMS == nil || *evidence.ConnectMS != 25 || evidence.HeaderMS == nil || *evidence.HeaderMS != 125 {
		t.Fatalf("timings=%+v", evidence)
	}
	if len(evidence.UpstreamStatuses) != 2 || evidence.UpstreamStatuses[0] != 502 || evidence.UpstreamStatuses[1] != 200 || evidence.BytesSent == nil || *evidence.BytesSent != 1024 || evidence.FaultClass != "" {
		t.Fatalf("upstream=%+v", evidence)
	}
	wantOneAPI := nginxEvidenceIDHMAC(cloudWatchParserTestKey, "oneapi-request-id", "oneapi-raw-id")
	if evidence.OneAPIIDHMAC != wantOneAPI {
		t.Fatalf("CloudWatch/existing evidence HMAC mismatch: %s != %s", evidence.OneAPIIDHMAC, wantOneAPI)
	}
	assertCloudWatchEvidenceOmits(t, evidence, "nginx-raw-id", "oneapi-raw-id", "task-secret", "token=hidden")
}

func TestCloudWatchParseNginxErrorReusesCollectorCategories(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	cases := []struct{ message, category, fault string }{
		{"2026/09/14 10:00:00 [error] 1#1: *1 upstream timed out while reading response header from upstream, client: 203.0.113.2", "upstream_timeout", "transport_timeout"},
		{"2026/09/14 10:00:00 [warn] 1#1: *2 client prematurely closed connection, client: 203.0.113.3", "client_closed", "client_gone"},
		{"2026/09/14 10:00:00 [error] 1#1: worker_connections are not enough", "worker_capacity", "rate_limit_capacity"},
	}
	for _, tc := range cases {
		evidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNginx, EventID: "event", TimestampMS: 1789363200000, Message: tc.message})
		if err != nil {
			t.Fatal(err)
		}
		if evidence.Kind != cwEvidenceNginxError || evidence.Category != tc.category || evidence.FaultClass != tc.fault || evidence.Summary == "" {
			t.Fatalf("evidence=%+v", evidence)
		}
		assertCloudWatchEvidenceOmits(t, evidence, tc.message, "203.0.113")
	}
	if _, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNginx, TimestampMS: 1, Message: "arbitrary text Authorization: Bearer secret"}); cwParseErrorKind(err) != cwParseUnsupported {
		t.Fatalf("unknown nginx format err=%v", err)
	}
}

func TestCloudWatchParseNewAPIFixedErrorsWithoutFreeText(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	// Request ID 用生产真实形态（40 位字母数字）。不用 "req-secret-1" 这类短串：
	// 识别 Request ID 段要求纯字母数字且 >=16 位，正是为了排除 GIN 行里的
	// relay、503、IP 等同位置字段，短串测试会掩盖这条约束。
	const rid = "202609141000001234567898268d9d6AbCdEfGh"
	message := "[ERR] 2026/09/14 - 10:00:00 | " + rid + " | user 7 | No available channel for model gpt-5 under group paid-group (distributor) Authorization: Bearer never-output"
	evidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, EventID: "event", LogStream: "new-api/task-id", TimestampMS: 1789363200000, Message: message})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Kind != cwEvidenceNewAPIError || evidence.Category != "route_no_channel" || evidence.FaultClass != "route_no_channel" || evidence.Model != "gpt-5" || evidence.Group != "paid-group" || evidence.UserID == nil || *evidence.UserID != 7 {
		t.Fatalf("newapi=%+v", evidence)
	}
	if evidence.OneAPIIDHMAC != nginxEvidenceIDHMAC(cloudWatchParserTestKey, "oneapi-request-id", rid) {
		t.Fatal("request ID is not cross-source correlatable")
	}
	encoded, _ := json.Marshal(evidence)
	for _, forbidden := range []string{rid, "Authorization", "Bearer", "never-output", "task-id"} {
		if strings.Contains(string(encoded), forbidden) {
			t.Fatalf("leaked %q: %s", forbidden, encoded)
		}
	}
}

func TestCloudWatchParseNewAPIInvalidTokenKeepsUserZeroUnattributed(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	evidence, err := parser.parse(cloudWatchEvidenceInput{
		Source:      cwSourceWorkerNewAPI,
		EventID:     "invalid-token-user-zero",
		TimestampMS: 1789300560000,
		Message:     "[ERR] 2026/09/13 - 19:56:00 | ABCDEF1234567890 | user 0 | Invalid token",
	})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Category != "invalid_token" || evidence.UserID != nil {
		t.Fatalf("user 0 应作为未知用户保留拒绝事实: %+v", evidence)
	}
}

// 用 2026-09-15 生产 new-api/ 真实样本得到的分类顺序回归。
// 这些行同时含多个关键词，必须归到更精确的一类，否则排障会把责任指错方向。
func TestCloudWatchNewAPIRealProductionLineOrdering(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	cases := []struct{ message, category, fault string }{
		// stream ended 同时含 client_gone：必须判客户断连，不能降级成普通"流结束"。
		{`[ERR] 2026/09/14 - 13:55:08 | 20260914rid | stream ended: reason=client_gone end_error="context canceled", received=1`, "client_gone", "client_gone"},
		{`[INFO] 2026/09/15 - 09:54:07 | rid | stream ended: reason=eof`, "stream_ended", ""},
		// "timeout waiting for goroutines" 含 timeout：必须判进程收尾，不能判上游超时。
		{`[ERR] 2026/09/14 - 13:55:13 | rid | timeout waiting for goroutines to exit`, "worker_drain_timeout", ""},
		{`[ERR] 2026/09/14 - 13:55:20 | user 102 | No available channel for model gpt-5.6-luna under group codex-0.7x`, "route_no_channel", "route_no_channel"},
		// ★ GIN 行第二段是固定标签 relay，Request ID 在第三段 ★
		// 2026-09-15 端到端实测：原实现把 relay 当成 Request ID，整行判为解析失败，
		// 同一请求的两条日志里丢了一条，页面显示成"有事件但格式无法识别"。
		{`[GIN] 2026/09/14 - 13:55:20 | relay | 202609140555209510180168268d9d6qt5ZSRym | 503 | 5.861246ms | 14.103.173.216 | POST /v1/responses`, "request_completed", ""},
		{`[INFO] 2026/09/15 - 09:54:07 | rid | record consume log: userId=102, params={}`, "billing_recorded", ""},
		// 流读取故障保持 unknown：与既有 logchain_fault.go 一致，不猜责任方。
		{`[ERR] 2026/09/14 - 13:55:13 | rid | scanner error: http2: response body closed`, "stream_error", "unknown"},
		{`[ERR] 2026/09/14 - 13:55:13 | rid | total tokens is 0, cannot consume`, "billing_anomaly", "billing_anomaly"},
	}
	for _, tc := range cases {
		ev, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1789363200000, Message: tc.message})
		if err != nil {
			t.Fatalf("real line rejected: %q err=%v", tc.message, err)
		}
		if ev.Category != tc.category || ev.FaultClass != tc.fault {
			t.Fatalf("line=%q category=%q(want %q) fault=%q(want %q)", tc.message, ev.Category, tc.category, ev.FaultClass, tc.fault)
		}
		assertCloudWatchEvidenceOmits(t, ev, tc.message)
	}
	// 两种前缀形态必须产出同一个 Request ID HMAC，否则同一请求的两条日志
	// 无法归到一起，页面上会显示成两个不相关的证据。
	const realRID = "202609140555209510180168268d9d6qt5ZSRym"
	errLine := `[ERR] 2026/09/14 - 13:55:20 | ` + realRID + ` | user 102 | No available channel for model gpt-5.6-luna under group codex-0.7x`
	ginLine := `[GIN] 2026/09/14 - 13:55:20 | relay | ` + realRID + ` | 503 | 5.861246ms | 14.103.173.216 | POST /v1/responses`
	errEv, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1789365320000, Message: errLine})
	if err != nil {
		t.Fatal(err)
	}
	ginEv, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1789365320000, Message: ginLine})
	if err != nil {
		t.Fatalf("GIN 行必须可解析: %v", err)
	}
	want := nginxEvidenceIDHMAC(cloudWatchParserTestKey, "oneapi-request-id", realRID)
	if errEv.OneAPIIDHMAC != want || ginEv.OneAPIIDHMAC != want {
		t.Fatalf("同一请求两种日志形态的 HMAC 不一致: err=%s gin=%s want=%s", errEv.OneAPIIDHMAC, ginEv.OneAPIIDHMAC, want)
	}
	// relay、状态码、IP 不得被当成标识或业务字段带出。
	assertCloudWatchEvidenceOmits(t, ginEv, realRID, "relay", "14.103.173.216", "5.861246ms")
	if ginEv.UserID != nil {
		t.Fatalf("GIN 行没有 user 段，不得凭空给出 user_id: %+v", ginEv.UserID)
	}
	if errEv.UserID == nil || *errEv.UserID != 102 {
		t.Fatalf("ERR 行的 user 102 未提取: %+v", errEv.UserID)
	}

	// [SYS] 后台任务日志与具体客户请求无关、无 Request ID，应跳过而非误判。
	if _, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Message: "[SYS] 2026/09/15 - 09:54:11 | batch update started"}); cwParseErrorKind(err) != cwParseUnsupported {
		t.Fatalf("后台 SYS 日志应作为 unsupported 跳过: %v", err)
	}
}

func TestCloudWatchNewAPIAndMasterUseClosedErrorClasses(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	cases := []struct {
		source                   cloudWatchLogSourceID
		message, category, fault string
	}{
		{cwSourceWorkerNewAPI, "write tcp: broken pipe", "client_gone", "client_gone"},
		{cwSourceWorkerNewAPI, "upstream status_code=503 private body", "upstream_5xx", "upstream_5xx"},
		{cwSourceWorkerNewAPI, "database error: too many connections user=root", "database_error", "unknown"},
		{cwSourceMaster, "fatal error: background scheduler stopped secret=hidden", "runtime_fatal", "unknown"},
	}
	for _, tc := range cases {
		evidence, err := parser.parse(cloudWatchEvidenceInput{Source: tc.source, TimestampMS: 1789363200000, Message: tc.message})
		if err != nil || evidence.Category != tc.category || evidence.FaultClass != tc.fault {
			t.Fatalf("case=%+v evidence=%+v err=%v", tc, evidence, err)
		}
		assertCloudWatchEvidenceOmits(t, evidence, tc.message, "root", "hidden")
	}
	if _, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Message: "ordinary request complete"}); cwParseErrorKind(err) != cwParseUnsupported {
		t.Fatalf("unknown app line err=%v", err)
	}
	if _, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Fields: map[string]string{"@message": "panic: fixture", "request_id": "bad id"}}); cwParseErrorKind(err) != cwParseMalformed {
		t.Fatalf("invalid request ID was silently dropped: %v", err)
	}
	if _, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Fields: map[string]string{"@message": "panic: fixture", "model": "sk-sensitive-model"}}); cwParseErrorKind(err) != cwParseMalformed {
		t.Fatalf("credential-like model was silently exposed/dropped: %v", err)
	}
	unicodeEvidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 1, Fields: map[string]string{"@message": "panic: fixture", "model": "模型-甲", "group": "高级组"}})
	if err != nil || unicodeEvidence.Model != "模型-甲" || unicodeEvidence.Group != "高级组" {
		t.Fatalf("valid Unicode business labels rejected: evidence=%+v err=%v", unicodeEvidence, err)
	}
}
