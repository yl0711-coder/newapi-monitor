package monitor

import (
	"context"
	"encoding/json"
	"os"
	"reflect"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"

	"github.com/yl0711-coder/newapi-monitor/internal/observability"
)

func TestCustomerDiagnosticChecklistAndOriginal300(t *testing.T) {
	var fixture struct {
		Rows []struct {
			Line                                     int
			RuleID                                   string `json:"rule_id"`
			Content, Responsibility, Reason, Profile string
			CauseKnown                               bool `json:"cause_known"`
		}
	}
	b, err := os.ReadFile("testdata/logchain_diagnostic_originals.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	if len(fixture.Rows) != 300 || len(logChainDiagnosticRules) != 186 {
		t.Fatal("checklist/input coverage changed")
	}
	covered := map[int]bool{}
	rules := map[string]bool{}
	for _, rule := range logChainDiagnosticRules {
		if rule.Version == "" || rule.Pattern == "" || rule.Reason == "" || rule.Action == "" || len(rule.RequiredFields) == 0 {
			t.Errorf("incomplete rule %s", rule.ID)
		}
		for _, line := range rule.DocumentLines {
			if covered[line] {
				t.Errorf("duplicate line %d", line)
			}
			covered[line] = true
		}
	}
	for _, sample := range fixture.Rows {
		t.Run(strconv.Itoa(sample.Line), func(t *testing.T) {
			r := LogChainRow{Type: 5, Content: sample.Content}
			var matchedOwnRule bool
			facts := logChainRowDiagnosticFacts(r)
			var texts []string
			for _, e := range facts.Errors {
				texts = append(texts, e.Message, e.Code, e.Type)
			}
			for _, rule := range logChainDiagnosticRules {
				if rule.ID == sample.RuleID {
					matchedOwnRule = rule.re.MatchString(strings.ReplaceAll(strings.Join(texts, "\n"), "`", ""))
				}
			}
			if !matchedOwnRule {
				t.Errorf("document rule %s does not recognize its original row", sample.RuleID)
			}
			d := logChainDiagnose(r)
			if d == nil {
				t.Fatal("original error was not recognized")
			}
			want, got := logChainDiagnosticParties(sample.Responsibility), append([]string(nil), d.ResponsibleParties...)
			sort.Strings(want)
			sort.Strings(got)
			if !reflect.DeepEqual(want, got) {
				t.Errorf("owner: %v want %v, matched %s", got, want, d.RuleID)
			}
			if d.CauseKnown != sample.CauseKnown {
				t.Errorf("known=%v want %v via %s", d.CauseKnown, sample.CauseKnown, d.RuleID)
			}
			if d.Profile != sample.Profile && sample.Line != 11 {
				t.Errorf("cause family %s want %s via %s", d.Profile, sample.Profile, d.RuleID)
			}
			wantReason := sample.Reason
			if sample.Line == 11 {
				wantReason = "消息块类型 advisor_tool_result 不在接口支持的类型集合内"
			}
			if !strings.HasPrefix(d.Reason, wantReason) {
				t.Errorf("cause %q want %q via %s", d.Reason, wantReason, d.RuleID)
			}
			for _, missing := range d.Missing {
				if missing.RecordAt == "" || missing.Reason == "" || facts.value(missing.Field) != "" {
					t.Errorf("invalid missing-field contract: %+v", missing)
				}
			}
			if d.Reason == "" || d.Basis == "" || len(d.Missing) == 0 {
				t.Error("missing reason, traceability or evidence checklist")
			}
			for _, fn := range []func(LogChainRow, []string) logChainFault{logChainAttributeFault, logChainAttributeFaultWithEvidence} {
				f := fn(r, nil)
				if f.Reason != d.Reason || f.Diagnosis == nil {
					t.Error("entry points disagree")
				}
			}
			rules[sample.RuleID] = true
		})
	}
	if len(covered) != 300 || len(rules) != 186 {
		t.Fatalf("coverage rows=%d rules=%d", len(covered), len(rules))
	}
}

func TestCustomerDiagnosticSpecificBeforeWrapper(t *testing.T) {
	for _, tc := range []struct{ text, want, contains string }{
		{`status_code=503, system cpu overloaded (current 97.25%, threshold 82.5%)`, "ours", "CPU"},
		{`status_code=503, Relay 请求体内存保护已触发`, "ours", "内存"},
		{`status_code=503, Billing service temporarily unavailable`, "ours", "计费"},
		{`status_code=502, Relay upstream error: future-model-42 requires adaptive thinking`, "downstream", "thinking"},
		{`status_code=500, {"error":{"type":"openai_error","message":"{\"error\":{\"message\":\"prompt is too long: 987654 tokens > 456789 maximum\"}}"}}`, "downstream", "987654"},
		{`status_code=403, 用户额度不足, 剩余额度: ＄-23.555`, "downstream", "额度"},
		{`status_code=403, 站长忘记给上游上供了，稍等`, "ours", "上游"},
		{`status_code=429, Concurrency limit exceeded for account`, "upstream", "并发"},
		{`status_code=429, Upstream rate limit exceeded, please retry later`, "upstream", "限流"},
	} {
		t.Run(tc.contains, func(t *testing.T) {
			f := logChainAttributeFaultWithEvidence(LogChainRow{Type: 5, Content: tc.text, UpstreamErrorCode: "bad_response_status_code"}, nil)
			if f.Fault != tc.want || !strings.Contains(f.Reason, tc.contains) {
				t.Fatalf("%+v", f)
			}
		})
	}
}

func TestCustomerDiagnosticProvenanceAnd404(t *testing.T) {
	r := LogChainRow{Type: 5, Content: "status_code=404, bad response status code 404", CreatedAt: 123}
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, `{"upstream_url":"https://user:secret@api.test/unsupported?api_key=private","supported_endpoints":"/v1/responses,/v1/messages","request_body_bytes":456,"request_length":789}`, "logs", r.CreatedAt)
	d := logChainDiagnose(r)
	if !d.CauseKnown || !strings.Contains(d.Reason, "不支持") {
		t.Fatalf("%+v", d)
	}
	if r.DiagnosticFacts.value("request_body_bytes") != "456" || r.DiagnosticFacts.value("request_length") != "789" {
		t.Fatal("body/packet size conflated")
	}
	b, _ := json.Marshal(r.DiagnosticFacts)
	if strings.Contains(string(b), "secret") || strings.Contains(string(b), "private") {
		t.Fatal("URL credentials leaked")
	}
	r.DiagnosticFacts = &LogChainDiagnosticFacts{Fields: map[string]LogChainDiagnosticField{"actual_upstream_host": {Value: "api.test", Scope: "current_config"}, "forward_path": {Value: "/old", Scope: "current_config"}}}
	if d := logChainDiagnose(r); d == nil || d.CauseKnown {
		t.Fatal("current config used as historical proof")
	}
}

func TestCustomerDiagnosticRedactionAndBounds(t *testing.T) {
	f := logChainParseDiagnosticFacts(`{"error":{"code":"invalid_value","message":"invalid value: other. Supported values are one Authorization: Bearer secrethere password=pw12345"},"prompt":"customer private prompt","api_key":"sk-secretsecretsecret"}`, `{"admin_info":{"account_id":"acc-42","request_body_bytes":123},"messages":[{"content":"privatecontent"}]}`, "logs", 123)
	b, _ := json.Marshal(f)
	for _, secret := range []string{"secrethere", "pw12345", "private prompt", "secretsecret", "privatecontent"} {
		if strings.Contains(string(b), secret) {
			t.Fatalf("leaked %s", secret)
		}
	}
	if f.value("account_id") != "acc-42" {
		t.Fatal("allowlisted context lost")
	}
	f = logChainParseDiagnosticFacts(strings.Repeat("a", 100000), "", "logs", 1)
	if !f.Truncated {
		t.Fatal("unbounded content")
	}
}

func TestCustomerDiagnosticDoesNotChangeSharedMapping(t *testing.T) {
	if logChainFaultByErrorCode["insufficient_user_quota"].fault != faultOurs || logChainFaultByErrorCode["user:concurrency_limited"].fault != faultOurs {
		t.Fatal("shared customer-health classification changed")
	}
	var portal LogRow
	b, _ := json.Marshal(portal)
	if strings.Contains(string(b), "diagnos") {
		t.Fatal("admin diagnostic fields leaked into portal")
	}
}

func TestCustomerDiagnosticDynamicOriginals(t *testing.T) {
	var fixture struct {
		Rows []struct {
			Line                    int
			Content, Responsibility string
		}
	}
	b, err := os.ReadFile("testdata/logchain_diagnostic_originals.json")
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(b, &fixture); err != nil {
		t.Fatal(err)
	}
	models := regexp.MustCompile(`(?:claude|gpt)-[a-zA-Z0-9.\-/]+`)
	counts := regexp.MustCompile(`\b[0-9]{4,}\b`)
	for _, row := range fixture.Rows {
		t.Run(strconv.Itoa(row.Line), func(t *testing.T) {
			text := models.ReplaceAllString(row.Content, "future-model-2027")
			text = counts.ReplaceAllString(text, "7654321")
			text = strings.ReplaceAll(text, "<id>", "dynamic-request-42")
			d := logChainDiagnose(LogChainRow{Type: 5, Content: text})
			if d == nil {
				t.Fatal("dynamic values broke matching")
			}
			want, got := logChainDiagnosticParties(row.Responsibility), append([]string(nil), d.ResponsibleParties...)
			sort.Strings(want)
			sort.Strings(got)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("dynamic match owner %v, want %v", got, want)
			}
		})
	}
}

func TestCustomerDiagnosticCloudWatchSupplementAndAttemptIsolation(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	parser, err := m.customerDiagnosticEvidenceParser(false)
	if err != nil {
		t.Fatal(err)
	}
	id := "01a123456789abcd123456789abcd1234"
	e, err := parser.parseNewAPI(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 100000, Message: `[ERR] 2026/11/08 - 10:00:00 | ` + id + ` | user 7 | status_code=502, Relay upstream error: future-model requires adaptive thinking`})
	if err != nil || e.Diagnostic == nil {
		t.Fatalf("structured error lost: %v", err)
	}
	e.EvidenceLevel = "exact"
	r := LogChainRow{ID: 1, Type: 5, RequestID: id, CreatedAt: 100, Content: "status_code=502, openai_error"}
	logChainApplyFault(&r, logChainAttributeFaultWithEvidence(r, nil))
	result := logChainInvestigationResult{Requests: []LogChainRow{r}, Evidence: []logChainCloudWatchEvidence{{Source: cwSourceWorkerNewAPI, Evidence: []cloudWatchStructuredEvidence{e}}}}
	m.rediagnoseInvestigationRequests(&result)
	if result.Requests[0].Fault != faultDownstream || !strings.Contains(result.Requests[0].FaultReason, "thinking") {
		t.Fatalf("not refined: %+v", result.Requests[0].Diagnosis)
	}
	summary := summarizeInvestigation(result.Requests, flattenInvestigationEvidence(result.Evidence), logChainInvestigationInput{NewAPIRequestID: id})
	shared := sharedInvestigationSummary(summary)
	if strings.Contains(shared.Conclusion, "下游连接") || !strings.Contains(shared.Conclusion, "thinking") {
		t.Fatal("summary lost specific cause")
	}
	// Same timestamp/Request ID without an attempt binding is insufficient.
	r2 := r
	r2.ID = 2
	result.Requests = []LogChainRow{r, r2}
	m.rediagnoseInvestigationRequests(&result)
	if result.Requests[0].Diagnosis.CauseKnown || result.Requests[1].Diagnosis.CauseKnown {
		t.Fatal("mixed retries")
	}
	// Explicit attempt identity permits only the matching row to update.
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, `{"attempt_id":"attempt-a"}`, "logs", 100)
	r2.DiagnosticFacts = logChainParseDiagnosticFacts(r2.Content, `{"attempt_id":"attempt-b"}`, "logs", 100)
	e.Diagnostic.put("attempt_id", "attempt-b", "cloudwatch:newapi", 100, "request")
	result.Requests = []LogChainRow{r, r2}
	result.Evidence[0].Evidence = []cloudWatchStructuredEvidence{e}
	m.rediagnoseInvestigationRequests(&result)
	if result.Requests[0].Diagnosis.CauseKnown || !result.Requests[1].Diagnosis.CauseKnown {
		t.Fatal("attempt binding ignored")
	}
	// A nearby different request cannot supply an error, even if mislabeled exact.
	e.OneAPIIDHMAC = m.investigationDigest("oneapi-request-id", "other-request")
	result.Requests = []LogChainRow{r}
	result.Evidence[0].Evidence = []cloudWatchStructuredEvidence{e}
	m.rediagnoseInvestigationRequests(&result)
	if result.Requests[0].Diagnosis.CauseKnown {
		t.Fatal("other request used as evidence")
	}
}

func TestCustomerDiagnosticFieldsAndMissingSourceSelection(t *testing.T) {
	r := LogChainRow{Type: 5, RequestID: "req", Content: "status_code=503, system cpu overloaded (current 99.25%, threshold 87.5%)"}
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, "", "logs", 123)
	r.Diagnosis = logChainDiagnose(r)
	if r.Diagnosis.Variables["resource_current"] != "99.25" || r.Diagnosis.Variables["resource_limit"] != "87.5" {
		t.Fatalf("dynamic CPU lost: %+v", r.Diagnosis.Variables)
	}
	if !logChainDiagnosticNeedsSource([]LogChainRow{r}, "req", cwSourceWorkerNewAPI) {
		t.Fatal("missing application metadata not requested")
	}
	for _, missing := range r.Diagnosis.Missing {
		if missing.RecordAt == "" || missing.Reason == "" {
			t.Fatal("missing capture layer")
		}
	}
}

func TestCustomerDiagnosticAdminParsingDoesNotChangeContinuousParser(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	shared, err := m.cloudWatchEvidenceParser(false)
	if err != nil {
		t.Fatal(err)
	}
	admin, err := m.customerDiagnosticEvidenceParser(false)
	if err != nil {
		t.Fatal(err)
	}
	in := cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 100000, Message: `status_code=503, system cpu overloaded (current 94.5%, threshold 88%)`}
	a, err := shared.parseNewAPI(in)
	if err != nil {
		t.Fatal(err)
	}
	b, err := admin.parseNewAPI(in)
	if err != nil {
		t.Fatal(err)
	}
	if a.Diagnostic != nil || b.Diagnostic == nil || a.Category != b.Category || a.FaultClass != b.FaultClass {
		t.Fatal("admin enrichment changed continuous classification or escaped its boundary")
	}
	in.Message = `{"request_id":"original-request","error":{"code":"invalid_value","message":"future-model requires adaptive thinking","param":"thinking.type"},"upstream_url":"https://api.test/v1/messages?token=secret","attempt_id":"attempt-9"}`
	b, err = admin.parseNewAPI(in)
	if err != nil || b.OneAPIRequestID != "original-request" || b.Diagnostic.value("error_param") != "thinking.type" || b.Diagnostic.value("forward_path") != "/v1/messages" || b.Diagnostic.value("attempt_id") != "attempt-9" {
		t.Fatalf("structured fields were not retained: %+v %v", b.Diagnostic, err)
	}
}

func TestCustomerDiagnosticVersionedCorrectionKeepsIdentityAndImmutableSnapshots(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	r := LogChainRow{ID: 17, Type: 5, CreatedAt: 123, RequestID: "private-original-id", Content: "status_code=502, future-model requires adaptive thinking (request id: private-original-id)"}
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, `{"request_id":"private-original-id"}`, "logs", 123)
	logChainApplyFault(&r, logChainAttributeFaultWithEvidence(r, nil))
	result := logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Requests: []LogChainRow{r}, Summary: logChainInvestigationSummary{Classification: "request_diagnosis", Diagnosis: r.Diagnosis}}
	before, _ := json.Marshal(result)
	for range 4 {
		got := m.investigationResponse(result)
		if len(got.Requests) != 0 || len(got.RequestDiagnoses) != 1 || got.RequestDiagnoses[0].Reference != m.logChainDiagnosticReference(r) || !got.RequestDiagnoses[0].Diagnosis.CauseKnown {
			t.Fatal("versioned row correction lost")
		}
		encoded, _ := json.Marshal(got.RequestDiagnoses)
		if strings.Contains(string(encoded), r.RequestID) {
			t.Fatal("raw request identity escaped versioned projection")
		}
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 10 {
				_ = m.investigationResponse(result)
				_ = logChainAttributeFaultWithEvidence(r, nil)
			}
		}()
	}
	wg.Wait()
	after, _ := json.Marshal(result)
	if string(before) != string(after) {
		t.Fatal("diagnosis mutated shared task/list snapshots")
	}
	retry := r
	retry.ID++
	if m.logChainDiagnosticReference(retry) == m.logChainDiagnosticReference(r) {
		t.Fatal("retries share correction identity")
	}
}

func TestCustomerDiagnostic404RefinesIndependentCauseAndResponsibility(t *testing.T) {
	for _, tc := range []struct{ other, reason, party string }{
		{`{"upstream_host":"api.test","forward_path":"/bad","route_config_error":"duplicated /v1 prefix"}`, "地址配置错误", "平台侧"},
		{`{"upstream_host":"api.test","forward_path":"/v1/responses","supported_endpoints":["/v1/messages"]}`, "不支持本次转发接口", "平台侧/上游"},
		{`{"upstream_host":"api.test","forward_path":"/v1/models/model-x","error_code":"model_not_found"}`, "未找到请求的模型或资源", "待查"},
	} {
		r := LogChainRow{Type: 5, Content: "status_code=404, bad response status code 404"}
		r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, tc.other, "logs", 123)
		d := logChainDiagnose(r)
		if d == nil || !d.CauseKnown || !strings.Contains(d.Reason, tc.reason) || d.Responsibility != tc.party || len(d.ActionOwners) == 0 || d.ActionOwners[0] == "unknown" {
			t.Fatalf("%+v", d)
		}
	}
}

func TestCustomerDiagnosticStreamTagsAndDynamicParameterEvidence(t *testing.T) {
	r := LogChainRow{Type: 2, EndReason: "scanner_error", EndError: `{"error":{"code":"invalid_value","param":"messages[47].content[9].type","message":"future-abc requires adaptive thinking"}}`}
	f := logChainAttributeFaultWithEvidence(r, []string{"stream"})
	if f.Diagnosis == nil || !f.Diagnosis.CauseKnown || f.Fault != faultDownstream || f.Diagnosis.Variables["error_param"] != "messages[47].content[9].type" {
		t.Fatalf("%+v", f)
	}
}

func TestCustomerDiagnosticProviderAndNginxRetryIsolation(t *testing.T) {
	ref := "same-request"
	events := []cloudWatchStructuredEvidence{{Kind: cwEvidenceNewAPIError, EvidenceLevel: "exact", OneAPIIDHMAC: ref, Diagnostic: logChainParseDiagnosticFacts("future-model requires adaptive thinking", `{"upstream_request_id":"provider-a"}`, "cw", 100)},
		{Kind: cwEvidenceNewAPIError, EvidenceLevel: "exact", OneAPIIDHMAC: ref, Diagnostic: logChainParseDiagnosticFacts("future-model requires adaptive thinking", `{"upstream_request_id":"provider-b"}`, "cw", 100)}}
	if !logChainEvidenceHasMultipleAttempts(events, ref) {
		t.Fatal("distinct provider attempts were collapsed")
	}
	edges := []cloudWatchStructuredEvidence{{Kind: cwEvidenceNginxAccess, EvidenceLevel: "exact", OneAPIIDHMAC: ref, HMACKeyID: "key", EventRef: "edge-a"}, {Kind: cwEvidenceNginxAccess, EvidenceLevel: "exact", OneAPIIDHMAC: ref, HMACKeyID: "key", EventRef: "edge-b"}}
	if logChainUniqueDiagnosticNginx(edges, ref, "key") {
		t.Fatal("multiple edge attempts picked newest evidence")
	}
}

func TestCustomerDiagnosticApplicationContextAndSourceUnavailable(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	p, err := m.customerDiagnosticEvidenceParser(false)
	if err != nil {
		t.Fatal(err)
	}
	e, err := p.parseNewAPI(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 100000, Message: `{"request_id":"request-context","attempt_id":"try-1","upstream_host":"api.test","forward_path":"/v1/messages?api_key=secret","parameter_values":{"thinking.type":"adaptive","prompt":"private"},"model_mapping":{"asked-model":"actual-model"}}`})
	if err != nil || e.Diagnostic == nil || e.Diagnostic.value("forward_path") != "/v1/messages" || e.Category != "diagnostic_context" || len(e.Diagnostic.Errors) > 0 || sharedEvidenceFault(e) != nil {
		t.Fatalf("context: %+v %v", e, err)
	}
	if e.Diagnostic.value("parameter_values") != "thinking.type=adaptive" || e.Diagnostic.value("model_mapping") != "asked-model=actual-model" {
		t.Fatal("allowlisted context lost")
	}
	e, err = p.parseNewAPI(cloudWatchEvidenceInput{Source: cwSourceWorkerNewAPI, TimestampMS: 100000, Message: `{"request_id":"request-context","error":{"message":"future-model requires adaptive thinking","code":"invalid_value"}}`})
	if err != nil {
		t.Fatal(err)
	}
	if e.Diagnostic.value("error_response") == "" {
		t.Fatal("recorded structured response was reported as missing")
	}
	e.EvidenceLevel = "exact"
	s := summarizeInvestigation(nil, []cloudWatchStructuredEvidence{e}, logChainInvestigationInput{NewAPIRequestID: "request-context"})
	if s.Diagnosis == nil || !strings.Contains(s.Conclusion, "thinking") || s.Diagnosis.Responsibility != "客户侧" {
		t.Fatalf("SQL absence hid exact error: %+v", s)
	}
	s = summarizeInvestigation(nil, []cloudWatchStructuredEvidence{e}, logChainInvestigationInput{NewAPIRequestID: "unrelated"})
	if s.Diagnosis != nil {
		t.Fatal("other request supplied conclusion")
	}
}

func TestCustomerDiagnosticExactUpstreamUsesRequestSnapshotAndStructuredID(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.UpstreamErrorLogSyncEnabled = true
	enableCorrelateCoverage(t, m, "example.com", 900, 1100)
	r := LogChainRow{ID: 91, Type: 5, RequestID: "business-id", CreatedAt: 1000, Content: "status_code=502, openai_error", UpstreamDomain: "current.example.net"}
	r.DiagnosticFacts = logChainParseDiagnosticFacts(r.Content, `{"upstream_host":"api.example.com","upstream_request_id":"provider-91"}`, "logs", 1000)
	h := upRow("example.com", 91, "provider-91", "future-model", 502, 1000)
	h.Content = "future-model requires adaptive thinking"
	if err := m.storeDB.Create(&h).Error; err != nil {
		t.Fatal(err)
	}
	matches, err := m.correlateUpstreamErrors(context.Background(), []LogChainRow{r})
	if err != nil {
		t.Fatal(err)
	}
	match := matches[91]
	r.UpstreamMatch = &match
	f := logChainAttributeFaultWithEvidence(r, nil)
	if match.Confidence != correlateExact || f.Diagnosis == nil || !f.Diagnosis.CauseKnown || f.Fault != faultDownstream {
		t.Fatalf("match=%+v fault=%+v", match, f)
	}
	page := []LogChainRow{r}
	logChainRefreshFaults(page)
	before, _ := json.Marshal(page[0].DiagnosticFacts)
	logChainRefreshFaults(page)
	after, _ := json.Marshal(page[0].DiagnosticFacts)
	if string(before) != string(after) || !strings.Contains(string(after), "upstream_error_logs") {
		t.Fatal("exact upstream evidence provenance lost or duplicated on refresh")
	}
	duplicate := r
	duplicate.ID = 92
	matches, err = m.correlateUpstreamErrors(context.Background(), []LogChainRow{r, duplicate})
	if err != nil {
		t.Fatal(err)
	}
	if matches[91].Confidence != correlateAmbiguous || matches[92].Confidence != correlateAmbiguous {
		t.Fatal("duplicate retry rows consumed same evidence")
	}
}

func TestCustomerDiagnosticNginxKeyRotationDoesNotMixAttempts(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	enableTestNginxEvidence(t, m)
	m.cfg.NginxEvidenceMode = "verified"
	m.cfg.NginxEvidencePreviousHMACKeyID = "key-0"
	m.cfg.NginxEvidencePreviousHMACKey = strings.Repeat("p", 32)
	for _, id := range []string{"duplicate", "old-ambiguous", "unique"} {
		item := NginxRequestEvidence{EventID: id + "-new", EventMS: 100000, Status: 503, Route: "/v1/messages", Method: "POST",
			OneAPIIDHMAC: nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", id), HMACKeyID: m.cfg.NginxEvidenceHMACKeyID}
		if err := m.nginxEvidenceDB.Create(&item).Error; err != nil {
			t.Fatal(err)
		}
		if id == "unique" {
			continue
		}
		item.EventID, item.HMACKeyID = id+"-old", m.cfg.NginxEvidencePreviousHMACKeyID
		item.OneAPIIDHMAC = nginxEvidenceIDHMAC(m.cfg.NginxEvidencePreviousHMACKey, "oneapi-request-id", id)
		if err := m.nginxEvidenceDB.Create(&item).Error; err != nil {
			t.Fatal(err)
		}
		if id == "old-ambiguous" {
			item.EventID = id + "-older"
			if err := m.nginxEvidenceDB.Create(&item).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	rows := []LogChainRow{{RequestID: "duplicate"}, {RequestID: "old-ambiguous"}, {RequestID: "unique"}}
	if err := m.attachNginxEvidence(context.Background(), rows); err != nil {
		t.Fatal(err)
	}
	if rows[0].EdgeEvidence != nil || rows[1].EdgeEvidence != nil || rows[2].EdgeEvidence == nil {
		t.Fatal("HMAC rotation mixed retries or suppressed an unrelated unique request")
	}
}
