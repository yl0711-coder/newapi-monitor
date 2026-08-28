package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func enableTestNginxEvidence(t *testing.T, m *Monitor) {
	t.Helper()
	m.cfg.NginxEnabled = true
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.IngestToken = "secret"
	m.cfg.NginxEvidenceMode = "pilot"
	m.cfg.NginxEvidenceStorePath = filepath.Join(t.TempDir(), "evidence.db")
	m.cfg.NginxEvidenceRetentionHours = 168
	m.cfg.NginxEvidenceHMACKey = strings.Repeat("k", 32)
	m.cfg.NginxEvidenceHMACKeyID = "key-1"
	m.cfg.NginxEvidenceMaxMiB = 64
	if err := m.openNginxEvidenceStore(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if db, err := m.nginxEvidenceDB.DB(); err == nil {
			_ = db.Close()
		}
	})
}

func TestNginxEvidenceLookupHashesInputAndMarksPilotUnverified(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	rawID := "request-visible-only-in-input"
	in := validEvidenceBatch()
	in.Events[0].OneAPIIDHMAC = nginxEvidenceIDHMAC(m.cfg.NginxEvidenceHMACKey, "oneapi-request-id", rawID)
	if w := postNginxEvidence(t, m, in); w.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d %s", w.Code, w.Body.String())
	}
	body := `{"request_id":"` + rawID + `"}`
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/nginx/evidence/lookup", m.serveNginxEvidenceLookup)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nginx/evidence/lookup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("lookup=%d %s", w.Code, w.Body.String())
	}
	var result nginxEvidenceLookupResponse
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil || !result.Found || result.LinkageVerified || result.Mode != "pilot" {
		t.Fatalf("pilot lookup must be visible but unverified: %+v err=%v", result, err)
	}
	encoded, _ := json.Marshal(result.Evidence)
	if strings.Contains(string(encoded), rawID) {
		t.Fatal("raw request id must not be stored or returned")
	}
}

func TestNginxEvidenceHealthAndRetentionAreIndependent(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	now := time.Now()
	in := validEvidenceBatch()
	if w := postNginxEvidence(t, m, in); w.Code != http.StatusOK {
		t.Fatalf("ingest failed: %d %s", w.Code, w.Body.String())
	}
	health := m.nginxEvidenceHealth(context.Background(), now)
	if !health.Enabled || !health.StoreReachable || health.SourceCount != 1 {
		t.Fatalf("unexpected evidence health: %+v", health)
	}
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("event_id = ?", in.Events[0].EventID).Update("event_ms", now.Add(-8*24*time.Hour).UnixMilli()).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneNginxEvidenceOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("expired evidence must be pruned from independent store: count=%d err=%v", count, err)
	}
}

func TestNginxEvidenceCapacityRecoversFromFreelistWithoutVacuum(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	before, err := nginxEvidenceReusableBytes(m.nginxEvidenceDB)
	if err != nil {
		t.Fatal(err)
	}
	rows := make([]NginxRequestEvidence, 2000)
	for i := range rows {
		rows[i] = NginxRequestEvidence{EventID: fmt.Sprintf("%064x", i+1), EventMS: time.Now().UnixMilli(), Node: "master", Route: "/v1/responses", Method: "POST", UpstreamStatuses: strings.Repeat("x", 4096)}
	}
	if err := m.nginxEvidenceDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	afterInsert, err := nginxEvidenceReusableBytes(m.nginxEvidenceDB)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.nginxEvidenceDB.Where("node = ?", "master").Delete(&NginxRequestEvidence{}).Error; err != nil {
		t.Fatal(err)
	}
	afterDelete, err := nginxEvidenceReusableBytes(m.nginxEvidenceDB)
	if err != nil {
		t.Fatal(err)
	}
	if !(afterInsert < before && afterDelete > afterInsert) {
		t.Fatalf("freelist capacity did not recover: before=%d after_insert=%d after_delete=%d", before, afterInsert, afterDelete)
	}
}

func postNginxEvidence(t *testing.T, m *Monitor, in nginxEvidenceBatch) *httptest.ResponseRecorder {
	t.Helper()
	if in.PayloadHash == "" {
		in.PayloadHash = nginxEvidenceHash(in)
	}
	body, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestBodyLimit(maxJSONRequestBody))
	r.POST("/internal/nginx-evidence/v1", m.ingestNginxEvidence)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-evidence/v1", strings.NewReader(string(body)))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func validEvidenceBatch() nginxEvidenceBatch {
	now := time.Now().UnixMilli()
	return nginxEvidenceBatch{
		SchemaVersion: 1,
		Node:          "master",
		BatchID:       "evidence_batch_abcdefgh",
		LogSchema:     2,
		HMACKeyID:     "key-1",
		Source: nginxEvidenceSourceRange{
			Kind: "access", FileID: "inode-42", StartOffset: 0, EndOffset: 200, FirstEventMS: now, LastEventMS: now,
		},
		Events: []nginxEvidenceEvent{{
			EventID: strings.Repeat("a", 64), EventMS: now, Route: "/v1/responses", Method: "POST",
			Status: 200, UpstreamStatus: 200, UpstreamAttempts: 1, UpstreamStatuses: []int{200},
			RequestMS: 350, UpstreamMS: 300, UpstreamPresent: true, ConnectMS: 25, HeaderMS: 125, BytesSent: 1024,
			Completion: "complete_at_edge", NginxIDHMAC: strings.Repeat("b", 64), OneAPIIDHMAC: strings.Repeat("c", 64),
		}},
	}
}

func TestNginxEvidenceDisabledDoesNotOpenOrAccept(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.openNginxEvidenceStore(); err != nil || m.nginxEvidenceDB != nil {
		t.Fatalf("off mode must not open evidence DB: db=%v err=%v", m.nginxEvidenceDB, err)
	}
	w := postNginxEvidence(t, m, validEvidenceBatch())
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled evidence endpoint=%d body=%s", w.Code, w.Body.String())
	}
}

func TestNginxEvidenceIdempotentAckAndConflict(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	in := validEvidenceBatch()
	for i := 0; i < 2; i++ {
		w := postNginxEvidence(t, m, in)
		if w.Code != http.StatusOK {
			t.Fatalf("ingest %d=%d body=%s", i, w.Code, w.Body.String())
		}
		var ack map[string]any
		if err := json.Unmarshal(w.Body.Bytes(), &ack); err != nil {
			t.Fatal(err)
		}
		if ack["batch_id"] != in.BatchID || ack["payload_hash"] != nginxEvidenceHash(in) || int(ack["accepted"].(float64)) != 1 {
			t.Fatalf("ack does not prove exact batch: %+v", ack)
		}
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("idempotency count=%d err=%v", count, err)
	}
	changed := in
	changed.Events = append([]nginxEvidenceEvent(nil), in.Events...)
	changed.Events[0].BytesSent++
	w := postNginxEvidence(t, m, changed)
	if w.Code != http.StatusConflict {
		t.Fatalf("same batch with changed evidence must conflict: %d %s", w.Code, w.Body.String())
	}
}

func TestNginxEvidencePermanentlyRejectsBadEventWithoutLosingGoodEvent(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	in := validEvidenceBatch()
	bad := in.Events[0]
	bad.EventID = strings.Repeat("d", 64)
	bad.Route = "/api/user/secret"
	in.Events = append(in.Events, bad)
	w := postNginxEvidence(t, m, in)
	if w.Code != http.StatusOK {
		t.Fatalf("permanent row rejection must be acknowledged: %d %s", w.Code, w.Body.String())
	}
	var ack struct{ Accepted, Rejected int }
	if err := json.Unmarshal(w.Body.Bytes(), &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Accepted != 1 || ack.Rejected != 1 {
		t.Fatalf("unexpected ack: %+v body=%s", ack, w.Body.String())
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("good event not stored exactly once: count=%d err=%v", count, err)
	}
}

func TestNginxEvidenceRejectsUnknownEnvelopeFields(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	in := validEvidenceBatch()
	in.PayloadHash = nginxEvidenceHash(in)
	body, _ := json.Marshal(in)
	text := strings.TrimSuffix(string(body), "}") + `,"raw_line":"must-not-be-accepted"}`
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/internal/nginx-evidence/v1", m.ingestNginxEvidence)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-evidence/v1", strings.NewReader(text))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("unknown fields must fail closed: %d %s", w.Code, w.Body.String())
	}
}

// This is the collector/Monitor wire regression for the full source telemetry
// emitted by cmd/nginxcollector. Keep it separate from the unknown-field test:
// fail-closed decoding is intentional, so every new collector field must first
// become an explicit Monitor contract field.
func TestNginxEvidenceAcceptsCollectorPersistFailureTelemetry(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	in := validEvidenceBatch()
	in.Source.EvidencePersistFailures = 1
	in.Source.EvidenceDroppedEvents = 1
	in.Source.LastEvidencePersistFailureAt = time.Now().Unix()
	if w := postNginxEvidence(t, m, in); w.Code != http.StatusOK {
		t.Fatalf("collector wire payload rejected: %d %s", w.Code, w.Body.String())
	}
	health := m.nginxEvidenceHealth(context.Background(), time.Now())
	if health.EvidencePersistFailures != 1 || health.EvidenceDroppedEvents != 1 || health.LastEvidencePersistFailureAt != in.Source.LastEvidencePersistFailureAt || health.UnhealthySources == 0 {
		t.Fatalf("collector persistence failure telemetry was not retained: %+v", health)
	}
}

func TestNginxEvidenceEnforcesPerFileContinuity(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	first := validEvidenceBatch()
	if w := postNginxEvidence(t, m, first); w.Code != http.StatusOK {
		t.Fatalf("first=%d %s", w.Code, w.Body.String())
	}
	next := validEvidenceBatch()
	next.BatchID = "evidence_batch_next_abcdefgh"
	next.Events[0].EventID = strings.Repeat("d", 64)
	next.Events[0].EventMS++
	next.Source.FirstEventMS = next.Events[0].EventMS
	next.Source.LastEventMS = next.Events[0].EventMS
	next.Source.StartOffset, next.Source.EndOffset = 201, 300
	if w := postNginxEvidence(t, m, next); w.Code != http.StatusConflict {
		t.Fatalf("gap must conflict: %d %s", w.Code, w.Body.String())
	}
	next.Source.StartOffset = 200
	if w := postNginxEvidence(t, m, next); w.Code != http.StatusOK {
		t.Fatalf("continuous batch=%d %s", w.Code, w.Body.String())
	}
}

func TestNginxEvidenceEmptyDataCheckpointAdvancesContinuity(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	first := validEvidenceBatch()
	if w := postNginxEvidence(t, m, first); w.Code != http.StatusOK {
		t.Fatalf("first=%d %s", w.Code, w.Body.String())
	}
	empty := validEvidenceBatch()
	empty.BatchID = "evidence_empty_checkpoint_abcdefgh"
	empty.LogSchema = 1
	empty.Events = []nginxEvidenceEvent{}
	empty.Source.StartOffset, empty.Source.EndOffset = 200, 300
	empty.Source.FirstEventMS, empty.Source.LastEventMS = 0, 0
	if w := postNginxEvidence(t, m, empty); w.Code != http.StatusOK {
		t.Fatalf("empty checkpoint=%d %s", w.Code, w.Body.String())
	}
	next := validEvidenceBatch()
	next.BatchID = "evidence_after_empty_abcdefgh"
	next.Events[0].EventID = strings.Repeat("d", 64)
	next.Events[0].EventMS++
	next.Source.StartOffset, next.Source.EndOffset = 300, 400
	next.Source.FirstEventMS, next.Source.LastEventMS = next.Events[0].EventMS, next.Events[0].EventMS
	if w := postNginxEvidence(t, m, next); w.Code != http.StatusOK {
		t.Fatalf("after empty checkpoint=%d %s", w.Code, w.Body.String())
	}
	var state NginxEvidenceSourceState
	if err := m.nginxEvidenceDB.First(&state, "node = ? AND source_kind = ?", "master", "access").Error; err != nil || state.LastEndOffset != 400 {
		t.Fatalf("empty checkpoint did not preserve continuity: state=%+v err=%v", state, err)
	}
}

func TestNginxEvidencePersistFailureEpochBridgesLostRangeOnce(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	first := validEvidenceBatch()
	if w := postNginxEvidence(t, m, first); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	heartbeat := validEvidenceBatch()
	heartbeat.BatchID = "evidence_loss_heartbeat_abcdefgh"
	heartbeat.Events = []nginxEvidenceEvent{}
	heartbeat.Source.FirstEventMS, heartbeat.Source.LastEventMS = 0, 0
	heartbeat.Source.StartOffset, heartbeat.Source.EndOffset = 300, 300
	heartbeat.Source.EvidencePersistFailures, heartbeat.Source.EvidenceDroppedEvents = 1, 1
	heartbeat.Source.LastEvidencePersistFailureAt = time.Now().Unix()
	if w := postNginxEvidence(t, m, heartbeat); w.Code != http.StatusOK {
		t.Fatalf("loss heartbeat=%d %s", w.Code, w.Body.String())
	}
	next := validEvidenceBatch()
	next.BatchID = "evidence_after_loss_abcdefgh"
	next.Events[0].EventID = strings.Repeat("e", 64)
	next.Events[0].EventMS++
	next.Source.StartOffset, next.Source.EndOffset = 300, 400
	next.Source.FirstEventMS, next.Source.LastEventMS = next.Events[0].EventMS, next.Events[0].EventMS
	next.Source.EvidencePersistFailures, next.Source.EvidenceDroppedEvents = 1, 1
	next.Source.LastEvidencePersistFailureAt = heartbeat.Source.LastEvidencePersistFailureAt
	if w := postNginxEvidence(t, m, next); w.Code != http.StatusOK {
		t.Fatalf("persist failure bridge=%d %s", w.Code, w.Body.String())
	}
	var state NginxEvidenceSourceState
	if err := m.nginxEvidenceDB.First(&state, "node = ? AND source_kind = ?", "master", "access").Error; err != nil || state.LastEndOffset != 400 || state.AppliedPersistFailures != 1 {
		t.Fatalf("loss epoch was not applied: state=%+v err=%v", state, err)
	}
	bad := validEvidenceBatch()
	bad.BatchID = "evidence_reuse_loss_epoch_abcdefgh"
	bad.Events[0].EventID = strings.Repeat("f", 64)
	bad.Events[0].EventMS++
	bad.Source.StartOffset, bad.Source.EndOffset = 500, 600
	bad.Source.FirstEventMS, bad.Source.LastEventMS = bad.Events[0].EventMS, bad.Events[0].EventMS
	bad.Source.EvidencePersistFailures, bad.Source.EvidenceDroppedEvents = 1, 1
	bad.Source.LastEvidencePersistFailureAt = heartbeat.Source.LastEvidencePersistFailureAt
	if w := postNginxEvidence(t, m, bad); w.Code != http.StatusConflict {
		t.Fatalf("applied loss epoch must not bridge a second gap: %d %s", w.Code, w.Body.String())
	}
}

func TestNginxEvidenceGapTelemetryCanBridgeDroppedRangeAfterHeartbeat(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	first := validEvidenceBatch()
	if w := postNginxEvidence(t, m, first); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	heartbeat := validEvidenceBatch()
	heartbeat.BatchID = "ehb_gap_abcdefgh"
	heartbeat.Events = []nginxEvidenceEvent{}
	heartbeat.Source.FirstEventMS, heartbeat.Source.LastEventMS = 0, 0
	heartbeat.Source.StartOffset, heartbeat.Source.EndOffset = 300, 300
	heartbeat.Telemetry.GapCount, heartbeat.Telemetry.DroppedEvents = 1, 1
	if w := postNginxEvidence(t, m, heartbeat); w.Code != http.StatusOK {
		t.Fatalf("heartbeat=%d %s", w.Code, w.Body.String())
	}
	next := validEvidenceBatch()
	next.BatchID = "evidence_after_gap_abcdefgh"
	next.Events[0].EventID = strings.Repeat("e", 64)
	next.Events[0].EventMS++
	next.Source.FirstEventMS, next.Source.LastEventMS = next.Events[0].EventMS, next.Events[0].EventMS
	next.Source.StartOffset, next.Source.EndOffset = 300, 400
	next.Telemetry.GapCount, next.Telemetry.DroppedEvents = 1, 1
	if w := postNginxEvidence(t, m, next); w.Code != http.StatusOK {
		t.Fatalf("gap bridge=%d %s", w.Code, w.Body.String())
	}
}

func TestNginxEvidenceRetentionConvergesBeyondOneChunk(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	now := time.Now()
	rows := make([]NginxRequestEvidence, 10005)
	for i := range rows {
		rows[i] = NginxRequestEvidence{EventID: fmt.Sprintf("%064x", i+1), EventMS: now.Add(-8 * 24 * time.Hour).UnixMilli(), Node: "master", Route: "/v1/responses", Method: "POST"}
	}
	if err := m.nginxEvidenceDB.CreateInBatches(rows, 500).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneNginxEvidenceOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("prune did not converge: count=%d err=%v", count, err)
	}
}

func TestNginxEvidenceLookupSupportsPreviousKeyDuringRotation(t *testing.T) {
	m := newTestMonitor(t)
	enableTestNginxEvidence(t, m)
	m.cfg.NginxEvidencePreviousHMACKey = strings.Repeat("p", 32)
	m.cfg.NginxEvidencePreviousHMACKeyID = "key-0"
	rawID := "request-before-key-rotation"
	in := validEvidenceBatch()
	in.HMACKeyID = "key-0"
	in.Events[0].OneAPIIDHMAC = nginxEvidenceIDHMAC(m.cfg.NginxEvidencePreviousHMACKey, "oneapi-request-id", rawID)
	if w := postNginxEvidence(t, m, in); w.Code != http.StatusOK {
		t.Fatalf("old key ingest=%d %s", w.Code, w.Body.String())
	}
	body := `{"request_id":"` + rawID + `"}`
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/nginx/evidence/lookup", m.serveNginxEvidenceLookup)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/nginx/evidence/lookup", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"found":true`) {
		t.Fatalf("previous key lookup=%d %s", w.Code, w.Body.String())
	}
}
