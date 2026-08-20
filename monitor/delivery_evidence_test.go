package monitor

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func clientEvidenceTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	m.cfg.ClientEvidenceToken = strings.Repeat("t", 32)
	m.cfg.ClientEvidenceHMACSecret = strings.Repeat("h", 32)
	m.cfg.ClientEvidenceAllowedClients = []string{"controlled-sdk@1.0.0"}
	return m
}

func postClientEvidence(t *testing.T, m *Monitor, token string, payload any) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	m.RegisterRoutes(r)
	body, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	req := httptest.NewRequest(http.MethodPost, "/internal/client-outcomes", strings.NewReader(string(body)))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func clientEvent(id, eventType, requestID, outcome string, retry int) ClientEvidenceInput {
	return ClientEvidenceInput{Version: 1, EventID: id, EventType: eventType, OccurredAtMS: time.Now().Add(-time.Second).UnixMilli(), RequestID: requestID, LogicalRequestKey: "logical-1", ClientVersion: "1.0.0", Outcome: outcome, RetryIndex: retry, Protocol: "openai_responses", Model: "gpt-test"}
}

func TestClientEvidenceSettingsAreOptionalAndIndependentFromNewAPI(t *testing.T) {
	if err := validateDeliveryEvidenceSettings(Settings{}); err != nil {
		t.Fatal(err)
	}
	valid := Settings{ClientEvidenceToken: strings.Repeat("t", 32), ClientEvidenceHMACSecret: strings.Repeat("h", 32), ClientEvidenceAllowedClients: []string{"controlled-sdk@1.0.0"}}
	if err := validateDeliveryEvidenceSettings(valid); err != nil {
		t.Fatal(err)
	}
	valid.ClientEvidenceHMACSecret = valid.ClientEvidenceToken
	if err := validateDeliveryEvidenceSettings(valid); err == nil {
		t.Fatal("shared token/HMAC must fail")
	}
}

func TestClientEvidenceIngestHashesRawIdentifiersAndIsIdempotent(t *testing.T) {
	m := clientEvidenceTestMonitor(t)
	raw := "req-sensitive-123"
	payload := map[string]any{"client_family": "controlled-sdk", "batch_id": "batch-1", "events": []ClientEvidenceInput{clientEvent("event-1", "request_started", raw, "", 0), clientEvent("event-2", "request_outcome", raw, "succeeded", 0)}}
	w := postClientEvidence(t, m, strings.Repeat("t", 32), payload)
	if w.Code != http.StatusOK {
		t.Fatalf("ingest=%d %s", w.Code, w.Body.String())
	}
	w = postClientEvidence(t, m, strings.Repeat("t", 32), payload)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"duplicate":true`) {
		t.Fatalf("duplicate=%d %s", w.Code, w.Body.String())
	}
	var rows []ClientDeliveryEvidence
	if err := m.deliveryEvidenceStore().Find(&rows).Error; err != nil || len(rows) != 2 {
		t.Fatalf("rows=%d err=%v", len(rows), err)
	}
	for _, row := range rows {
		encoded, _ := json.Marshal(row)
		if strings.Contains(string(encoded), raw) || row.RequestTraceHMAC == raw || !validEvidenceHMAC(row.RequestTraceHMAC) {
			t.Fatalf("raw request id persisted: %+v", row)
		}
	}
}

func TestClientEvidenceRejectsUnknownFieldsFreeTextAndPayloadConflicts(t *testing.T) {
	m := clientEvidenceTestMonitor(t)
	bad := fmt.Sprintf(`{"client_family":"controlled-sdk","batch_id":"batch-x","events":[{"version":1,"event_id":"event-x","event_type":"request_outcome","occurred_at_ms":%d,"request_id":"req","client_version":"1.0.0","outcome":"transport_failure","error_signature":"contains spaces","unknown":1}]}`, time.Now().UnixMilli())
	req := httptest.NewRequest(http.MethodPost, "/internal/client-outcomes", strings.NewReader(bad))
	req.Header.Set("Authorization", "Bearer "+strings.Repeat("t", 32))
	w := httptest.NewRecorder()
	r := gin.New()
	m.RegisterRoutes(r)
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("bad payload=%d %s", w.Code, w.Body.String())
	}
	base := map[string]any{"client_family": "controlled-sdk", "batch_id": "batch-conflict", "events": []ClientEvidenceInput{clientEvent("event-conflict", "request_outcome", "req", "succeeded", 0)}}
	if w := postClientEvidence(t, m, strings.Repeat("t", 32), base); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	base["events"] = []ClientEvidenceInput{clientEvent("event-conflict", "request_outcome", "req", "transport_failure", 0)}
	if w := postClientEvidence(t, m, strings.Repeat("t", 32), base); w.Code != http.StatusConflict {
		t.Fatalf("conflict=%d %s", w.Code, w.Body.String())
	}
}

func TestClientEvidenceLowTechnicalSampleCannotBecomeSufficient(t *testing.T) {
	m := clientEvidenceTestMonitor(t)
	events := make([]ClientEvidenceInput, 0, 40)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("req-%02d", i)
		events = append(events, clientEvent("start-"+id, "request_started", id, "", 0))
		outcome := "user_cancelled"
		if i == 0 {
			outcome = "succeeded"
		}
		events = append(events, clientEvent("outcome-"+id, "request_outcome", id, outcome, 0))
	}
	if w := postClientEvidence(t, m, strings.Repeat("t", 32), map[string]any{"client_family": "controlled-sdk", "batch_id": "batch-summary", "events": events}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	var technical int64
	m.deliveryEvidenceStore().Model(&ClientDeliveryEvidence{}).Where("event_type=? AND outcome NOT IN ?", "request_outcome", []string{"user_cancelled"}).Count(&technical)
	if technical != 1 {
		t.Fatalf("technical=%d", technical)
	}
	gin.SetMode(gin.TestMode)
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/stability/delivery-evidence?hours=24&protocol=openai_responses", nil)
	m.serveDeliveryEvidenceSummary(c)
	if w.Code != http.StatusOK {
		t.Fatalf("summary=%d %s", w.Code, w.Body.String())
	}
	var response struct {
		Summary clientEvidenceSummary `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Summary.Started != 20 || response.Summary.Outcomes != 20 || response.Summary.TechnicalOutcomes != 1 || response.Summary.EvidenceSufficient {
		t.Fatalf("low technical sample became sufficient: %+v", response.Summary)
	}
}

func TestClientEvidenceCompleteTechnicalCohortBecomesSufficient(t *testing.T) {
	m := clientEvidenceTestMonitor(t)
	events := make([]ClientEvidenceInput, 0, 40)
	for i := 0; i < 20; i++ {
		id := fmt.Sprintf("healthy-%02d", i)
		events = append(events, clientEvent("start-"+id, "request_started", id, "", 0), clientEvent("outcome-"+id, "request_outcome", id, "succeeded", 0))
	}
	if w := postClientEvidence(t, m, strings.Repeat("t", 32), map[string]any{"client_family": "controlled-sdk", "batch_id": "batch-healthy", "events": events}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/?hours=24&protocol=openai_responses", nil)
	m.serveDeliveryEvidenceSummary(c)
	var response struct {
		Summary clientEvidenceSummary `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if !response.Summary.EvidenceSufficient || response.Summary.SuccessRate == nil || *response.Summary.SuccessRate != 100 {
		t.Fatalf("complete cohort not sufficient: %+v", response.Summary)
	}
}

func TestClientEvidenceSummaryAttributesLateOutcomeToStartWindowAndSeparatesOrphan(t *testing.T) {
	m := clientEvidenceTestMonitor(t)
	now := time.Now().In(cstLocation)
	yesterday := time.Date(now.Year(), now.Month(), now.Day()-1, 0, 0, 0, 0, cstLocation)
	startAt := yesterday.Add(23*time.Hour + 59*time.Minute)
	lateAt := yesterday.Add(24*time.Hour + time.Minute)
	orphanAt := yesterday.Add(20 * time.Hour)
	started := clientEvent("late-start", "request_started", "late-request", "", 0)
	started.OccurredAtMS = startAt.UnixMilli()
	late := clientEvent("late-outcome", "request_outcome", "late-request", "transport_failure", 0)
	late.OccurredAtMS = lateAt.UnixMilli()
	orphan := clientEvent("orphan-outcome", "request_outcome", "orphan-request", "transport_failure", 0)
	orphan.OccurredAtMS = orphanAt.UnixMilli()
	if w := postClientEvidence(t, m, strings.Repeat("t", 32), map[string]any{"client_family": "controlled-sdk", "batch_id": "batch-cross-window", "events": []ClientEvidenceInput{started, late, orphan}}); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	path := fmt.Sprintf("/?from=%s&to=%s&protocol=openai_responses", yesterday.Format("2006-01-02"), yesterday.Format("2006-01-02"))
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	m.serveDeliveryEvidenceSummary(c)
	if w.Code != http.StatusOK {
		t.Fatalf("summary=%d %s", w.Code, w.Body.String())
	}
	var response struct {
		Summary clientEvidenceSummary `json:"summary"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Summary.Started != 1 || response.Summary.Outcomes != 1 || response.Summary.Failures != 1 || response.Summary.OrphanOutcomes != 1 {
		t.Fatalf("cross-window outcome or orphan attribution is wrong: %+v", response.Summary)
	}
	issues := httptest.NewRecorder()
	issuesContext, _ := gin.CreateTestContext(issues)
	issuesContext.Request = httptest.NewRequest(http.MethodGet, path, nil)
	m.serveDeliveryEvidenceIssues(issuesContext)
	if issues.Code != http.StatusOK || !strings.Contains(issues.Body.String(), "late-outcome") || strings.Contains(issues.Body.String(), "orphan-outcome") {
		t.Fatalf("issues did not follow request-start cohort: status=%d body=%s", issues.Code, issues.Body.String())
	}
}

func TestClientEvidenceRetention(t *testing.T) {
	m := clientEvidenceTestMonitor(t)
	now := time.Now()
	old := ClientDeliveryEvidence{EventID: "old-event", PayloadHash: strings.Repeat("a", 64), OccurredAtMS: now.Add(-91 * 24 * time.Hour).UnixMilli(), ReceivedAt: now.Add(-91 * 24 * time.Hour).Unix(), RequestTraceHMAC: strings.Repeat("b", 64), ClientFamily: "controlled-sdk", ClientVersion: "1.0.0", EventType: "request_started"}
	if err := m.deliveryEvidenceStore().Create(&old).Error; err != nil {
		t.Fatal(err)
	}
	m.pruneClientEvidence(now)
	var count int64
	m.deliveryEvidenceStore().Model(&ClientDeliveryEvidence{}).Where("event_id=?", old.EventID).Count(&count)
	if count != 0 {
		t.Fatalf("old event retained: %d", count)
	}
}
