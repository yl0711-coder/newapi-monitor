package monitor

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/observability"
)

func TestObservabilityInvestigationVersionedViewHidesRawIdentifiers(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	now := time.Now().UTC().Truncate(time.Second)
	in := logChainInvestigationInput{SchemaVersion: observability.SchemaVersion, NewAPIRequestID: "private-request-marker", UserID: 123456789,
		Model: "private-model-filter", Group: "private-group-filter", From: now.Add(-time.Hour), To: now}
	raw := logChainInvestigationResult{SchemaVersion: in.SchemaVersion, Status: "partial", Scope: m.investigationScopeView(in),
		Requests: []LogChainRow{{ID: 11, RequestID: in.NewAPIRequestID, UserID: in.UserID, ModelName: in.Model, Group: in.Group,
			Type: 2, IsStream: true, FirstByteMs: 4001, CreatedAt: now.Unix()}},
		Summary:  logChainInvestigationSummary{Classification: "business_completed", EvidenceLevel: "exact"},
		Timeline: []logChainInvestigationTimelineEvent{{EvidenceLevel: "ambiguous"}},
		Evidence: []logChainCloudWatchEvidence{{Source: cwSourceWorkerNewAPI, Evidence: []cloudWatchStructuredEvidence{{UserID: &in.UserID}}}}}
	got := m.investigationResponse(raw)
	encoded, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	for _, secret := range []string{in.NewAPIRequestID, "123456789", in.Model, in.Group} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("versioned response leaked raw input %q", secret)
		}
	}
	if raw.Scope.UserID != in.UserID || len(raw.Requests) != 1 || len(raw.Evidence) != 1 {
		t.Fatal("response redaction mutated task/cache")
	}
	if raw.Timeline[0].EvidenceLevel != "ambiguous" || raw.Timeline[0].Candidate || !got.Timeline[0].Candidate {
		t.Fatal("versioned timeline projection mutated cached evidence")
	}
	if got.Observability == nil || len(got.Observability.Events) != 1 {
		t.Fatalf("missing canonical observations: %+v", got)
	}
	event := got.Observability.Events[0]
	if event.GatewayRef == nil || event.GatewayRef.Validate() != nil || event.HMACKeyID == "" {
		t.Fatal("HMAC reference lost key identity")
	}
	if event.FRTMs == nil || *event.FRTMs != 4001 || event.Metrics.TTFT.ValueMS != nil || event.Metrics.TTFE.ValueMS != nil || event.Metrics.Validate() != nil {
		t.Fatalf("FRT was lost or promoted into strict TTFT: %+v", event)
	}
	if event.Complete || got.Observability.Summary.FaultClass != nil {
		t.Fatal("partial query or normal billing was promoted into a completed protocol")
	}
	if observability.ValidateTimestamp(event.OccurredAt) != nil {
		t.Fatal("event timestamp not UTC")
	}
	raw.SchemaVersion = ""
	legacy := m.investigationResponse(raw)
	if len(legacy.Requests) != 1 || legacy.Requests[0].RequestID != in.NewAPIRequestID || legacy.Observability != nil {
		t.Fatal("legacy response changed")
	}
}

func TestObservabilityInvestigationDoesNotPromoteCandidatesOrNginx5xx(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	status := 502
	result := logChainInvestigationResult{SchemaVersion: observability.SchemaVersion,
		Summary: logChainInvestigationSummary{Classification: "inconclusive", EvidenceLevel: "ambiguous"},
		Evidence: []logChainCloudWatchEvidence{{Source: cwSourceWorkerNginx, Evidence: []cloudWatchStructuredEvidence{{
			EventRef: "candidate-ref", HMACKeyID: m.cfg.CloudWatchEvidenceHMACKeyID, EventMS: 1700000000000,
			Kind: cwEvidenceNginxAccess, EvidenceLevel: "ambiguous", Status: &status, FaultClass: "upstream_5xx",
		}}}}}
	view := m.investigationResponse(result).Observability
	if !view.Summary.Candidate || view.Summary.EvidenceLevel != observability.EvidenceUnavailable || *view.Summary.FaultClass != observability.FaultTelemetryGap {
		t.Fatalf("candidate verdict: %+v", view.Summary)
	}
	event := view.Events[0]
	if !event.Candidate || event.EvidenceLevel != observability.EvidenceUnavailable || *event.FaultClass != observability.FaultUnknown {
		t.Fatalf("edge error became supplier fault: %+v", event)
	}
	result.Scope.CloudFrontRef = "edge-hash"
	result.Requests = []LogChainRow{{ID: 1, CreatedAt: 1700000000, RequestID: "other-request"}}
	if e := m.investigationResponse(result).Observability.Events[0]; !e.Candidate || e.EvidenceLevel != observability.EvidenceUnavailable {
		t.Fatal("CF-only query promoted unrelated business")
	}
}

func TestObservabilityInvestigationVersionValidationAndCacheIsolation(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	now := time.Now()
	request := logChainInvestigationCreateRequest{AtUnix: now.Add(-time.Hour).Unix(), NewAPIRequestID: "request"}
	legacy, err := parseLogChainInvestigationInput(request, now)
	if err != nil {
		t.Fatal(err)
	}
	request.SchemaVersion = observability.SchemaVersion
	v1, err := parseLogChainInvestigationInput(request, now)
	if err != nil || v1.SchemaVersion != observability.SchemaVersion {
		t.Fatalf("v1 rejected: %v", err)
	}
	if m.investigationScopeDigest(legacy) == m.investigationScopeDigest(v1) {
		t.Fatal("legacy and safe response share cache identity")
	}
	for _, version := range []string{"observability.v2", "unknown", "observability.v1 "} {
		request.SchemaVersion = version
		if _, err := parseLogChainInvestigationInput(request, now); err == nil {
			t.Fatalf("accepted unsupported schema %q", version)
		}
	}
}

func TestObservabilityInvestigationNoBusinessLogsStillQueriesEvidence(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newLogChainCloudWatchTestMonitor(t, client)
	now := time.Now().UTC()
	for _, in := range []logChainInvestigationInput{
		{NewAPIRequestID: "missing-request"}, {CloudFrontRequestID: "edge-only"}, {Path: "/v1/responses"}, {UserID: 456789},
	} {
		in.SchemaVersion, in.From, in.To = observability.SchemaVersion, now.Add(-3*time.Hour), now.Add(-2*time.Hour)
		result := m.executeLogChainInvestigation(context.Background(), "test-no-business", in)
		view := m.investigationResponse(result)
		if view.Observability == nil || len(view.Requests) != 0 || view.Observability.Summary.FaultClass == nil || *view.Observability.Summary.FaultClass != observability.FaultTelemetryGap {
			t.Fatalf("no evidence became a customer verdict: %+v", view)
		}
	}
	client.mu.Lock()
	count := len(client.filterInputs) + len(client.startInputs)
	client.mu.Unlock()
	if count == 0 {
		t.Fatal("independent entry never queried CloudWatch")
	}
}

func TestObservabilityInvestigationHTTPPollUsesSafeVersionedView(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	gin.SetMode(gin.TestMode)
	id := "inv_0123456789abcdef0123456789abcdef"
	owner := m.investigationDigest("cloudwatch-investigation-operator", "owner")
	raw := logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Status: "partial", Requests: []LogChainRow{{ID: 1, CreatedAt: 1700000000, RequestID: "http-private-request"}}}
	m.investigationTasks = map[string]*logChainInvestigationTask{id: {ID: id, Owner: owner, Status: "partial", Result: &raw}}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/logchain/investigations/"+id, nil)
	c.Params = gin.Params{{Key: "id", Value: id}}
	c.Set("uname", "owner")
	m.serveGetLogChainInvestigation(c)
	if w.Code != http.StatusOK || strings.Contains(w.Body.String(), "http-private-request") || !strings.Contains(w.Body.String(), `"schema_version":"observability.v1"`) {
		t.Fatalf("unsafe HTTP response: %s", w.Body.String())
	}
}

func TestObservabilityInvestigationLegacyFaultMapping(t *testing.T) {
	for _, category := range []string{"request_completed", "stream_ended", "billing_recorded", "quota_precheck"} {
		for _, kind := range []cloudWatchEvidenceKind{cwEvidenceNewAPIError, cwEvidenceMasterError} {
			if got := sharedEvidenceFault(cloudWatchStructuredEvidence{Kind: kind, Category: category}); got != nil {
				t.Fatalf("normal %s %s became fault %s", kind, category, *got)
			}
		}
	}
	for _, category := range []string{"invalid_token", "token_disabled", "quota_account", "model_forbidden"} {
		got := sharedEvidenceFault(cloudWatchStructuredEvidence{Kind: cwEvidenceNewAPIError, Category: category, FaultClass: "auth_quota_account", EvidenceLevel: "exact"})
		if got == nil || *got != observability.FaultUnknown {
			t.Fatalf("customer pre-route refusal %s became upstream fault: %v", category, got)
		}
	}
}

func TestObservabilityInvestigationKnownFailureIsNotTelemetryGap(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	result := logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Status: "partial",
		Summary:  logChainInvestigationSummary{Classification: "inconclusive", EvidenceLevel: "ambiguous"},
		Requests: []LogChainRow{{ID: 42, CreatedAt: 1700000000, Type: 5, Fault: "unknown"}}}
	got := m.investigationResponse(result).Observability.Summary
	if got.FaultClass == nil || *got.FaultClass != observability.FaultUnknown || got.EvidenceLevel != observability.EvidenceExact || got.Candidate {
		t.Fatalf("observed failure became a telemetry gap: %+v", got)
	}
	result.Requests[0].Type = 2
	result.Requests[0].AnomalyTags = []string{"billing"}
	got = m.investigationResponse(result).Observability.Summary
	if got.FaultClass == nil || *got.FaultClass != observability.FaultUnknown {
		t.Fatalf("observed consumption anomaly became a telemetry gap: %+v", got)
	}
	result.Scope.CloudFrontRef = "only-edge-context"
	got = m.investigationResponse(result).Observability.Summary
	if got.FaultClass == nil || *got.FaultClass != observability.FaultTelemetryGap || !got.Candidate {
		t.Fatal("unrelated candidate promoted to this request's failure")
	}
}

func TestObservabilityInvestigationLifecycleDoesNotInventFault(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, status := range []string{"queued", "running", "cancelled"} {
		got := m.investigationResponse(logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Status: status}).Observability
		if got.Summary.FaultClass != nil || got.Summary.EvidenceLevel != observability.EvidenceUnavailable || got.Summary.Candidate {
			t.Fatalf("%s invented a business/telemetry failure: %+v", status, got.Summary)
		}
	}
	previousGap := m.investigationResponse(logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Status: "cancelled",
		Summary:      logChainInvestigationSummary{Classification: "telemetry_unavailable", EvidenceLevel: "unavailable"},
		SourceStatus: []logChainCloudWatchSourceStatus{{Source: cwSourceWorkerNewAPI, Status: "unavailable"}},
	}).Observability.Summary
	if previousGap.FaultClass == nil || *previousGap.FaultClass != observability.FaultTelemetryGap {
		t.Fatal("cancelling erased an already observed source failure")
	}
	id := "inv_0123456789abcdef0123456789abcdef"
	owner := m.investigationDigest("cloudwatch-investigation-operator", "owner")
	_, cancel := context.WithCancel(context.Background())
	defer cancel()
	in := logChainInvestigationInput{SchemaVersion: observability.SchemaVersion, UserID: 987654321, From: time.Now().Add(-time.Hour), To: time.Now()}
	m.investigationTasks = map[string]*logChainInvestigationTask{id: {ID: id, Owner: owner, Status: "queued", Input: in, CreatedAt: time.Now(), Cancel: cancel}}
	result, ok := m.getLogChainInvestigation("owner", id)
	if !ok || result.SchemaVersion != observability.SchemaVersion {
		t.Fatal("queued fallback lost version")
	}
	result, ok = m.cancelLogChainInvestigation("owner", id)
	if !ok || result.SchemaVersion != observability.SchemaVersion {
		t.Fatal("cancelled fallback lost version")
	}
	view := m.investigationResponse(result)
	if view.Scope.UserID != 0 || view.Observability == nil || view.Observability.Summary.FaultClass != nil {
		t.Fatal("cancel response leaked identity or invented a fault")
	}
}

func TestObservabilityInvestigationFRTRequiresStreamingConsumption(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	for _, row := range []LogChainRow{
		{ID: 1, CreatedAt: 1700000000, Type: 2, IsStream: false, FirstByteMs: 5000},
		{ID: 1, CreatedAt: 1700000000, Type: 5, IsStream: true, FirstByteMs: 5000},
		{ID: 1, CreatedAt: 1700000000, Type: 2, IsStream: true, FirstByteMs: 0},
	} {
		got := m.investigationResponse(logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Requests: []LogChainRow{row}}).Observability.Events[0]
		if got.FRTMs != nil || got.Metrics.TTFT.ValueMS != nil || got.Metrics.TTFT.EvidenceLevel != observability.EvidenceUnavailable {
			t.Fatalf("unobserved/non-stream FRT became measured latency: %+v", got)
		}
	}
}

func TestObservabilityInvestigationHTTPRejectsUnsupportedSchemaBeforeQuery(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newLogChainCloudWatchTestMonitor(t, client)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodPost, "/logchain/investigations", strings.NewReader(`{"schema_version":"observability.v99","newapi_request_id":"private-invalid-schema-marker"}`))
	c.Set("uname", "owner")
	m.serveCreateLogChainInvestigation(c)
	if w.Code != http.StatusBadRequest || strings.Contains(w.Body.String(), "private-invalid-schema-marker") {
		t.Fatalf("unsafe schema rejection: %d %s", w.Code, w.Body.String())
	}
	client.mu.Lock()
	count := len(client.startInputs) + len(client.filterInputs)
	client.mu.Unlock()
	if count != 0 {
		t.Fatal("unsupported schema triggered a CloudWatch query")
	}
}

func TestObservabilityReadyQualityOnHealthyAndDegradedResponses(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	m.localFactsProbeOK.Store(true)
	for _, healthy := range []bool{false, true} {
		m.localStoreProbeOK.Store(healthy)
		m.storeIntegrityOK.Store(healthy)
		got, code := m.readyStatus(time.Now())
		if healthy && code != http.StatusOK || !healthy && code != http.StatusServiceUnavailable {
			t.Fatalf("unexpected ready response healthy=%t code=%d", healthy, code)
		}
		if len(got.SourceQuality) != len(got.Collectors) || len(got.SourceQuality) == 0 {
			t.Fatal("ready omitted source quality")
		}
		for _, source := range got.SourceQuality {
			if source.MetadataComplete || source.Owner != nil || source.CredentialExpiresAt != nil {
				t.Fatal("uninstrumented source metadata was invented")
			}
		}
	}
}

func TestObservabilityInvestigationConsumptionDoesNotEraseFailure(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	in := logChainInvestigationInput{SchemaVersion: observability.SchemaVersion, NewAPIRequestID: "retried-request", From: time.Unix(1700000000, 0), To: time.Unix(1700000300, 0)}
	failed := LogChainRow{ID: 1, CreatedAt: 1700000001, RequestID: in.NewAPIRequestID, Type: 5, Fault: "unknown", TypeName: "错误"}
	consumed := LogChainRow{ID: 2, CreatedAt: 1700000002, RequestID: in.NewAPIRequestID, Type: 2, TypeName: "消费"}
	for _, rows := range [][]LogChainRow{{failed, consumed}, {consumed, failed}} {
		summary := summarizeInvestigation(rows, []cloudWatchStructuredEvidence{{Kind: cwEvidenceNewAPIError, Category: "request_completed", EvidenceLevel: "exact"}}, in)
		if summary.Classification != "inconclusive" || summary.EvidenceLevel != "exact" || !strings.Contains(summary.Conclusion, "正常消费") || !strings.Contains(summary.Conclusion, "责任方待判") {
			t.Fatalf("consumption erased a failed attempt: %+v", summary)
		}
		for _, candidateSummary := range []logChainInvestigationSummary{summary, {Classification: "business_completed", EvidenceLevel: "exact", Conclusion: "旧缓存只提到了消费"}} {
			raw := logChainInvestigationResult{SchemaVersion: observability.SchemaVersion, Scope: m.investigationScopeView(in), Requests: rows, Summary: candidateSummary}
			got := m.investigationResponse(raw)
			if got.Observability.Summary.FaultClass == nil || *got.Observability.Summary.FaultClass != observability.FaultUnknown || got.Observability.Summary.Candidate {
				t.Fatalf("failure observation lost: %+v", got.Observability.Summary)
			}
			if got.Summary.Classification != "inconclusive" || got.Summary.Conclusion != got.Observability.Summary.Conclusion || !strings.Contains(got.Summary.Conclusion, "正常消费") {
				t.Fatalf("versioned and compatibility summaries disagree: %+v", got)
			}
			failedEvents, consumptionEvents := 0, 0
			for _, event := range got.Observability.Events {
				if event.FaultClass != nil {
					failedEvents++
				} else {
					consumptionEvents++
				}
			}
			if failedEvents != 1 || consumptionEvents != 1 || len(raw.Requests) != 2 || raw.Summary.Conclusion != candidateSummary.Conclusion {
				t.Fatal("response lost an observation or mutated its cache")
			}
		}
	}
	resolved := summarizeInvestigation([]LogChainRow{failed, consumed}, []cloudWatchStructuredEvidence{{Kind: cwEvidenceNewAPIError, FaultClass: "upstream_5xx", EvidenceLevel: "exact"}}, in)
	if resolved.Classification != "upstream_5xx" || !strings.Contains(resolved.Conclusion, "正常消费") {
		t.Fatalf("direct failure evidence or subsequent consumption lost: %+v", resolved)
	}
}

func TestObservabilityInvestigationOtherRequestFailureDoesNotOverrideConsumption(t *testing.T) {
	m := newLogChainCloudWatchTestMonitor(t, &fakeCloudWatchLogsClient{})
	in := logChainInvestigationInput{SchemaVersion: observability.SchemaVersion, NewAPIRequestID: "this-request"}
	rows := []LogChainRow{{ID: 1, CreatedAt: 1700000001, RequestID: "other-request", Type: 5, Fault: "unknown"}, {ID: 2, CreatedAt: 1700000002, RequestID: in.NewAPIRequestID, Type: 2}}
	summary := summarizeInvestigation(rows, nil, in)
	if summary.Classification != "business_completed" {
		t.Fatalf("foreign error contaminated this request summary: %+v", summary)
	}
	got := m.investigationResponse(logChainInvestigationResult{SchemaVersion: in.SchemaVersion, Scope: m.investigationScopeView(in), Requests: rows, Summary: summary}).Observability
	if got.Summary.FaultClass != nil || !got.Events[0].Candidate || got.Events[0].EvidenceLevel != observability.EvidenceUnavailable {
		t.Fatalf("foreign error became exact: %+v", got)
	}
}
