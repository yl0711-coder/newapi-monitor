package monitor

import (
	"regexp"
	"strings"
	"testing"
	"time"
)

// cwTestEventID 把用例名/日志原文压成合法的 CloudWatch EventID。
//
// 真实 EventID 是不透明标识符，cwBoundedOpaque 因此拒绝空格与控制字符
// （见 cloudwatch_logs_evidence.go 的 r < 0x21 检查）。测试若直接把带空格的
// 用例名或整条日志塞进 EventID，parse 会在入口就判 unsafe，于是"解析器认不认得
// 生产措辞"这件事根本没被测到。这里只规范化标识符，不改任何被解析的 Message。
func cwTestEventID(raw string) string {
	var b strings.Builder
	for _, r := range raw {
		if r > 0x20 && r != 0x7f {
			b.WriteRune(r)
			continue
		}
		b.WriteByte('-')
	}
	id := b.String()
	if len(id) > 2048 {
		id = id[:2048]
	}
	if strings.TrimSpace(id) == "" {
		return "cw-test-event"
	}
	return id
}

func TestCloudWatchPreRouteParserRecognizesProductionPhrases(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	rid := "202609220000001234"
	cases := []struct {
		name, message, category, model, group string
		userID                                *int64
	}{
		{
			name:     "Chinese no channel",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | user 16 | 分组 claude-3.5x 下模型 claude-haiku-4-5-20251001 无可用渠道（distributor）",
			category: "route_no_channel", model: "claude-haiku-4-5-20251001", group: "claude-3.5x", userID: cloudWatchPtrInt64(16),
		},
		{
			name:     "Chinese invalid token",
			message:  "[ERR] 2026/09/02 - 16:44:45 | " + rid + " | user 0 | 无效的令牌",
			category: "invalid_token",
		},
		{
			name:     "Chinese model forbidden",
			message:  "[ERR] 2026/09/01 - 10:11:13 | " + rid + " | user 16 | 该令牌无权访问模型 gpt-5.6-luna",
			category: "model_forbidden", model: "gpt-5.6-luna", userID: cloudWatchPtrInt64(16),
		},
		{
			name:     "Chinese user quota",
			message:  "[ERR] 2026/09/02 - 08:50:00 | " + rid + " | relay error: 用户额度不足, 剩余额度: ＄-1.097764",
			category: "quota_account",
		},
		{
			name:     "Chinese pre consume quota",
			message:  "[ERR] 2026/09/01 - 11:39:19 | " + rid + " | relay error: 预扣费额度失败, 用户剩余额度: ＄0.189056, 需要预扣费额度: ＄0.594642",
			category: "quota_account",
		},
		{
			name:     "English token quota",
			message:  "[ERR] 2026/09/01 - 14:39:12 | " + rid + " | relay error: token quota is not enough, token remain quota: ＄0.066538, need quota: ＄0.924882",
			category: "quota_account",
		},
		{
			name:     "English pre consume failed",
			message:  "[ERR] 2026/09/01 - 11:39:19 | " + rid + " | relay error: pre-consume failed",
			category: "quota_account",
		},
		{
			name:     "Chinese rate limit",
			message:  "[ERR] 2026/09/01 - 12:00:00 | " + rid + " | relay error: 请求过于频繁，请稍后重试",
			category: "rate_limited",
		},
		{
			name:     "user id key in plain text",
			message:  "[ERR] 2026/09/01 - 12:00:00 | " + rid + " | user_id=17 | 用户额度不足",
			category: "quota_account", userID: cloudWatchPtrInt64(17),
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evidence, err := parser.parse(cloudWatchEvidenceInput{
				Source: cwSourceWorkerNewAPI, EventID: cwTestEventID(tc.name), TimestampMS: time.Now().UnixMilli(), Message: tc.message,
			})
			if err != nil {
				t.Fatalf("parse err=%v", err)
			}
			if evidence.Category != tc.category || evidence.Model != tc.model || evidence.Group != tc.group {
				t.Fatalf("evidence=%+v, want category=%q model=%q group=%q", evidence, tc.category, tc.model, tc.group)
			}
			if (evidence.UserID == nil) != (tc.userID == nil) || tc.userID != nil && *evidence.UserID != *tc.userID {
				t.Fatalf("user_id=%v, want %v", evidence.UserID, tc.userID)
			}
		})
	}
}

func TestCloudWatchPreRouteDoesNotMatchPositiveQuotaText(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	rejectPattern := regexp.MustCompile(cloudWatchPreRouteRejectPattern)
	cases := []string{
		"[INFO] 2026/09/01 - 12:00:00 | 202609220000001234 | token quota is enough, request may continue",
		"[INFO] 2026/09/01 - 12:00:00 | 202609220000001234 | Overrode user quota from ＄-0.955357 额度 to ＄9.044643 额度",
		"[INFO] 2026/09/01 - 12:00:00 | 202609220000001234 | quota is enough for this request",
	}
	for _, message := range cases {
		if rejectPattern.MatchString(message) {
			t.Fatalf("positive quota message matched pre-route query vocabulary: %q", message)
		}
		if _, err := parser.parse(cloudWatchEvidenceInput{
			Source: cwSourceWorkerNewAPI, EventID: cwTestEventID(message), TimestampMS: time.Now().UnixMilli(), Message: message,
		}); cwParseErrorKind(err) != cwParseUnsupported {
			t.Fatalf("positive quota message was classified as pre-route evidence: %q err=%v", message, err)
		}
	}
}

func TestCloudWatchPreRouteParserUsesStructuredIdentityFields(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	evidence, err := parser.parse(cloudWatchEvidenceInput{
		Source: cwSourceWorkerNewAPI, EventID: "structured-fields", TimestampMS: time.Now().UnixMilli(),
		Message: "[ERR] 2026/09/01 - 12:00:00 | 202609220000001234 | Invalid token",
		Fields:  map[string]string{"@message": "[ERR] 2026/09/01 - 12:00:00 | 202609220000001234 | Invalid token", "user_id": "23", "model": "gpt-5", "group": "paid"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Category != "invalid_token" || evidence.UserID == nil || *evidence.UserID != 23 || evidence.Model != "gpt-5" || evidence.Group != "paid" {
		t.Fatalf("structured fields were not retained: %+v", evidence)
	}
}

func TestCloudWatchPreRouteParserTreatsMissingStructuredUserAsUnknown(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	evidence, err := parser.parse(cloudWatchEvidenceInput{
		Source: cwSourceWorkerNewAPI, EventID: "structured-missing-user", TimestampMS: time.Now().UnixMilli(),
		Message: "[ERR] 2026/09/01 - 12:00:00 | 202609220000001234 | Invalid token",
		Fields:  map[string]string{"@message": "[ERR] 2026/09/01 - 12:00:00 | 202609220000001234 | Invalid token", "user_id": "-"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if evidence.Category != "invalid_token" || evidence.UserID != nil {
		t.Fatalf("missing structured user should remain unknown: %+v", evidence)
	}
}

func TestCloudWatchPreRouteParserExtractsAlternateModelAndGroupContexts(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	rid := "202609220000001234"
	cases := []struct {
		name, message, category, model, group string
	}{
		{
			name:     "English group before model",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | No available channel under group paid for model gpt-5",
			category: "route_no_channel", model: "gpt-5", group: "paid",
		},
		{
			name:     "Chinese model before group",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | 模型 gpt-5 在分组 paid 下无可用渠道",
			category: "route_no_channel", model: "gpt-5", group: "paid",
		},
		{
			name:     "English forbidden after model",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | model gpt-5 is forbidden",
			category: "model_forbidden", model: "gpt-5",
		},
		{
			name:     "Chinese forbidden after model",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | 模型 gpt-5 无权限",
			category: "model_forbidden", model: "gpt-5",
		},
		{
			name:     "English not found after model",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | model gpt-5 is not found",
			category: "model_not_found", model: "gpt-5",
		},
		{
			name:     "Chinese not found after model",
			message:  "[ERR] 2026/09/01 - 09:09:57 | " + rid + " | 模型 gpt-5 不存在",
			category: "model_not_found", model: "gpt-5",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			evidence, err := parser.parse(cloudWatchEvidenceInput{
				Source: cwSourceWorkerNewAPI, EventID: cwTestEventID(tc.name), TimestampMS: time.Now().UnixMilli(), Message: tc.message,
			})
			if err != nil {
				t.Fatalf("parse err=%v", err)
			}
			if evidence.Category != tc.category || evidence.Model != tc.model || evidence.Group != tc.group {
				t.Fatalf("evidence=%+v, want category=%q model=%q group=%q", evidence, tc.category, tc.model, tc.group)
			}
		})
	}
}

func TestCloudWatchPreRouteQueryContainsProductionVocabularyAndSourceGuard(t *testing.T) {
	from := time.Date(2026, 9, 22, 0, 0, 0, 0, time.UTC)
	_, query, _, err := buildCloudWatchInsightsQuery(cloudWatchInsightsRequest{
		Kind: cwQueryWorkerPreRouteReject, From: from, To: from.Add(time.Hour), Limit: cloudWatchLogsPreRouteLimit,
	})
	if err != nil {
		t.Fatal(err)
	}
	for _, phrase := range []string{"无可用渠道", "no channel available", "无效的令牌", "该令牌无权访问模型", "用户额度不足", "预扣费额度失败", "token quota is not enough", "pre-consume failed", "请求过于频繁"} {
		if !strings.Contains(query, phrase) {
			t.Errorf("query missing production phrase %q: %s", phrase, query)
		}
	}
	// status code 的排除项在 pattern 里写成正则 `status[_ ]?code`（同时覆盖
	// status_code 与 status code 两种生产写法），因此这里断言正则形式而不是
	// 字面 "status_code"；用字面子串会漏掉这个等价且更宽的实现。
	if !strings.Contains(query, "not like") || !strings.Contains(query, `status[_ ]?code`) || !strings.Contains(query, "upstream") || !strings.Contains(query, `^\s*\[gin\]`) {
		t.Fatalf("query lacks explicit upstream exclusion: %s", query)
	}
	// 反向确认：排除项必须真的能匹配到生产里的 status_code/status code 两种写法，
	// 否则上面的断言会退化成"只要源码里有这串字符就算过"。
	upstreamMarker := regexp.MustCompile(cloudWatchPreRouteUpstreamMarkerPattern)
	for _, marker := range []string{"status_code=429", "status code: 500", "upstream returned 502"} {
		if !upstreamMarker.MatchString(marker) {
			t.Errorf("upstream marker pattern 未能匹配生产写法 %q", marker)
		}
	}
	for _, field := range []string{"user_id", "userId", "uid", "model", "model_name", "group", "grp"} {
		if !strings.Contains(query, field) {
			t.Errorf("query must project structured identity field %q: %s", field, query)
		}
	}
}

func TestCloudWatchPreRouteParserExcludesExplicitUpstreamErrors(t *testing.T) {
	parser := newCloudWatchParserForTest(t, false)
	cases := []string{
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | status_code=503, No available channel for model gpt-5 under group default",
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | status_code=429, Upstream rate limit exceeded, please retry later",
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | provider returned: token is invalid",
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | upstream model forbidden by provider",
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | 上游返回：用户额度不足",
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | 渠道 LA-claude-max (#31) 返回错误：No available channel for model gpt-5",
		"[ERR] 2026/09/01 - 00:00:00 | 202609220000001234 | 渠道 LA-claude-max (#31) 返回错误：rate limit exceeded",
		"[GIN] 2026/09/01 - 00:00:00 | relay | 202609220000001234 | 429 | rate_limit=true | POST /v1/responses",
	}
	for _, message := range cases {
		evidence, err := parser.parse(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, EventID: cwTestEventID(message), TimestampMS: time.Now().UnixMilli(), Message: message})
		if err != nil {
			t.Fatalf("upstream line should remain parseable: %q err=%v", message, err)
		}
		if cwIsPreRouteClass(evidence.Category) {
			t.Fatalf("upstream line entered pre-route category: %q -> %+v", message, evidence)
		}
		if rows := cloudWatchPreRouteSamples([]cloudWatchStructuredEvidence{evidence}, time.Now().Unix()-60, time.Now().Unix()+60); len(rows) != 0 {
			t.Fatalf("upstream line entered rejection samples: %q -> %+v", message, rows)
		}
	}
}

func cloudWatchPtrInt64(value int64) *int64 { return &value }
