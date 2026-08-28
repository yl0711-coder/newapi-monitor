package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func evidenceConfig(dir string) config {
	return config{
		node:               "master",
		logPath:            filepath.Join(dir, "access.jsonl"),
		cursorPath:         filepath.Join(dir, "cursor.json"),
		maxLines:           100,
		retentionDays:      7,
		evidenceMode:       "pilot",
		evidenceHMACKey:    []byte(strings.Repeat("k", 32)),
		evidenceHMACKeyID:  "key-1",
		evidenceOutboxPath: filepath.Join(dir, "evidence-outbox"),
		evidenceOutboxMax:  8 << 20,
		evidenceFSMu:       &sync.Mutex{},
	}
}

func schema2Line(ts int64, status, upstreamStatus, completion, nginxID, oneAPIID string) string {
	return fmt.Sprintf(`{"log_schema":2,"msec":"%d.250","request_method":"POST","uri":"/v1/responses","status":"%s","request_time":"0.350","upstream_status":"%s","upstream_response_time":"0.300","upstream_connect_time":"0.025","upstream_header_time":"0.125","bytes_sent":"1024","request_id":"legacy-id","nginx_request_id":"%s","oneapi_request_id":"%s","request_completion":"%s"}`,
		ts, status, upstreamStatus, nginxID, oneAPIID, completion)
}

func TestEvidenceUsesSameParseForMinuteAndRetrySequence(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := schema2Line(time.Now().Unix(), "200", "502, 200", "OK", "nginx-secret", "oneapi-secret") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || len(payload.Samples) != 1 || payload.Samples[0].Count != 1 {
		t.Fatalf("one access line must produce one minute request: payload=%+v ok=%v err=%v", payload, ok, err)
	}
	if payload.Evidence == nil || len(payload.Evidence.Events) != 1 {
		t.Fatalf("schema2 inference line must produce one evidence event: %+v", payload.Evidence)
	}
	event := payload.Evidence.Events[0]
	if event.EventMS == 0 || event.UpstreamAttempts != 2 || fmt.Sprint(event.UpstreamStatuses) != "[502 200]" || event.ConnectMS != 25 || event.HeaderMS != 125 {
		t.Fatalf("retry/timing evidence was not preserved: %+v", event)
	}
	encoded, _ := json.Marshal(payload.Evidence)
	text := string(encoded)
	if strings.Contains(text, "nginx-secret") || strings.Contains(text, "oneapi-secret") || event.NginxIDHMAC == "" || event.OneAPIIDHMAC == "" {
		t.Fatalf("raw request ids must never leave collector: %s", text)
	}
}

func TestEvidenceWireIncludesPersistFailureTimestamp(t *testing.T) {
	cfg := evidenceConfig(t.TempDir())
	value := cursor{Version: cursorVersion, Inode: 1, EvidencePersistFailures: 1, EvidenceDroppedEvents: 2, LastEvidencePersistFailureAt: 1234567890}
	payload := newEvidenceBatch(cfg, "wire_batch_abcdefgh", 1, 0, 10, 0, 0, nil, value)
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(encoded), `"last_evidence_persist_failure_at":1234567890`) {
		t.Fatalf("collector wire telemetry missing persistence failure timestamp: %s", encoded)
	}
}

func TestEvidenceCaptures499AndIncompleteStream(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := schema2Line(time.Now().Unix(), "499", "200", "", "nginx-id", "-") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || payload.Evidence == nil || len(payload.Evidence.Events) != 1 {
		t.Fatalf("499 line missing: payload=%+v ok=%v err=%v", payload, ok, err)
	}
	event := payload.Evidence.Events[0]
	if event.Status != 499 || event.Completion != "incomplete_at_edge" || event.OneAPIIDHMAC != "" {
		t.Fatalf("499/incomplete semantics wrong: %+v", event)
	}
}

func TestEvidenceCapturesHTTP200WithIncompleteEdgeDelivery(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := schema2Line(time.Now().Unix(), "200", "200", "", "nginx-id", "oneapi-id") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || payload.Evidence == nil || len(payload.Evidence.Events) != 1 {
		t.Fatalf("missing evidence: ok=%v err=%v", ok, err)
	}
	if event := payload.Evidence.Events[0]; event.Status != 200 || event.Completion != "incomplete_at_edge" {
		t.Fatalf("HTTP 200 must not hide an incomplete client delivery: %+v", event)
	}
}

func TestMissingEarlierOptionalAttemptDoesNotDropMinuteOrEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := strings.Replace(schema2Line(time.Now().Unix(), "200", "200", "OK", "nginx-id", "oneapi-id"), `"upstream_connect_time":"0.025"`, `"upstream_connect_time":"-, 0.025"`, 1) + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || len(payload.Samples) != 1 || payload.Samples[0].Count != 1 {
		t.Fatalf("minute sample was poisoned: %+v ok=%v err=%v", payload, ok, err)
	}
	if payload.Evidence == nil || payload.Evidence.Events[0].ConnectMS != 25 {
		t.Fatalf("valid Nginx missing-attempt sequence was not preserved: %+v", payload.Evidence)
	}
}

func TestMalformedOptionalEvidenceFieldDoesNotDropMinuteSample(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := strings.Replace(schema2Line(time.Now().Unix(), "200", "200", "OK", "nginx-id", "oneapi-id"), `"upstream_header_time":"0.125"`, `"upstream_header_time":"bad"`, 1) + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, next, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || len(payload.Samples) != 1 {
		t.Fatalf("minute sample was poisoned: %+v ok=%v err=%v", payload, ok, err)
	}
	if payload.Evidence == nil || len(payload.Evidence.Events) != 0 || payload.Evidence.Source.EndOffset <= payload.Evidence.Source.StartOffset || next.EvidenceEligible != 1 || next.EvidenceParseRejected != 1 {
		t.Fatalf("malformed evidence was not rejected with a durable empty checkpoint: payload=%+v cursor=%+v", payload.Evidence, next)
	}
}

func TestLegacyLogStillFeedsMinuteButNotEvidence(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || len(payload.Samples) != 1 || payload.Evidence == nil || len(payload.Evidence.Events) != 0 || payload.Evidence.Source.EndOffset <= payload.Evidence.Source.StartOffset {
		t.Fatalf("legacy compatibility broken: payload=%+v ok=%v err=%v", payload, ok, err)
	}
}

func TestEvidenceOutageDoesNotBlockMinuteCursorAndRecovers(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := schema2Line(time.Now().Unix(), "200", "200", "OK", "nginx-id", "oneapi-id") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	minute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer minute.Close()
	var evidenceStatus atomic.Int64
	evidenceStatus.Store(http.StatusServiceUnavailable)
	evidence := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if int(evidenceStatus.Load()) != http.StatusOK {
			w.WriteHeader(int(evidenceStatus.Load()))
			return
		}
		var payload evidenceBatch
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode evidence: %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		_ = json.NewEncoder(w).Encode(evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: payload.BatchID, PayloadHash: payload.PayloadHash, Accepted: len(payload.Events)})
	}))
	defer evidence.Close()
	cfg.sinkURL, cfg.evidenceSinkURL, cfg.token = minute.URL, evidence.URL, "secret"
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatalf("minute lane must advance even before evidence delivery: %v", err)
	}
	cur, err := loadCursor(cfg.cursorPath)
	if err != nil || cur.Offset != int64(len(line)) {
		t.Fatalf("minute cursor did not advance: %+v err=%v", cur, err)
	}
	paths, err := listEvidenceOutbox(cfg.evidenceOutboxPath)
	if err != nil || len(paths) != 1 {
		t.Fatalf("evidence must survive in durable outbox: paths=%v err=%v", paths, err)
	}
	if err := drainEvidenceOnce(context.Background(), cfg); err == nil {
		t.Fatal("temporary evidence outage must keep outbox for retry")
	}
	evidenceStatus.Store(http.StatusOK)
	if err := drainEvidenceOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	paths, _ = listEvidenceOutbox(cfg.evidenceOutboxPath)
	if len(paths) != 0 {
		t.Fatalf("exact ack must remove delivered batch: %v", paths)
	}
}

func TestFullEvidenceOutboxRecordsVisibleGapWithoutBlockingMinute(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	cfg.evidenceOutboxMax = 1
	line := schema2Line(time.Now().Unix(), "200", "200", "OK", "nginx-id", "oneapi-id") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	minute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer minute.Close()
	cfg.sinkURL, cfg.token = minute.URL, "secret"
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatalf("a durably recorded evidence gap must not stop existing minute lane: %v", err)
	}
	gap, err := loadGapState(cfg.evidenceOutboxPath)
	if err != nil || gap.GapCount != 1 || gap.DroppedEvents != 1 {
		t.Fatalf("full outbox must leave explicit durable gap: %+v err=%v", gap, err)
	}
	// 同一分钟批在分钟 sink 超时时会重读；缺口账本必须按 batch 幂等。
	payload, _, ok, readErr := readBatch(cfg, cursor{})
	if readErr != nil || !ok || payload.Evidence == nil {
		t.Fatalf("re-read evidence batch failed: ok=%v err=%v", ok, readErr)
	}
	if err := spoolEvidence(cfg, *payload.Evidence); err != nil {
		t.Fatal(err)
	}
	gap, err = loadGapState(cfg.evidenceOutboxPath)
	if err != nil || gap.GapCount != 1 || gap.DroppedEvents != 1 {
		t.Fatalf("replaying a dropped batch must not double count the gap: %+v err=%v", gap, err)
	}
}

func TestEvidenceHMACSeparatesIdentifierDomains(t *testing.T) {
	key := []byte(strings.Repeat("k", 32))
	if evidenceHMAC(key, "nginx-request-id", "same") == evidenceHMAC(key, "oneapi-request-id", "same") {
		t.Fatal("request id namespaces must be domain separated")
	}
}

func TestCorruptEvidenceHeadIsQuarantinedAndDoesNotBlockNextBatch(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	if err := os.MkdirAll(cfg.evidenceOutboxPath, 0o700); err != nil {
		t.Fatal(err)
	}
	bad := filepath.Join(cfg.evidenceOutboxPath, "000-bad.json")
	if err := os.WriteFile(bad, []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := drainEvidenceOnce(context.Background(), cfg); err == nil {
		t.Fatal("corrupt batch must be reported")
	}
	if _, err := os.Stat(bad); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("corrupt FIFO head still active: %v", err)
	}
	gap, err := loadGapState(cfg.evidenceOutboxPath)
	if err != nil || gap.UnknownDroppedBatches != 1 || gap.GapCount != 1 {
		t.Fatalf("unknown loss not recorded: %+v err=%v", gap, err)
	}
	valid := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: "master", BatchID: "valid_after_corrupt_abcdefgh", LogSchema: 1, HMACKeyID: "key-1",
		Source: evidenceSourceRange{Kind: "access", FileID: "42", StartOffset: 0, EndOffset: 100}, Events: []evidenceEvent{}}
	valid.PayloadHash = evidencePayloadHash(valid)
	if err := spoolEvidence(cfg, valid); err != nil {
		t.Fatal(err)
	}
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload evidenceBatch
		_ = json.NewDecoder(r.Body).Decode(&payload)
		_ = json.NewEncoder(w).Encode(evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: payload.BatchID, PayloadHash: payload.PayloadHash, Accepted: len(payload.Events)})
	}))
	defer sink.Close()
	cfg.evidenceSinkURL, cfg.token = sink.URL, "secret"
	if err := drainEvidenceOnce(context.Background(), cfg); err != nil {
		t.Fatalf("valid batch remained blocked after quarantine: %v", err)
	}
	paths, err := listEvidenceOutbox(cfg.evidenceOutboxPath)
	if err != nil || len(paths) != 0 {
		t.Fatalf("valid successor was not delivered: paths=%v err=%v", paths, err)
	}
}

func TestEvidenceQueueUsesDurableSequenceWhenModificationTimesMatch(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	first := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: "master", BatchID: "queue_first_abcdefgh", LogSchema: 2, HMACKeyID: "key-1",
		Source: evidenceSourceRange{Kind: "access", FileID: "42", StartOffset: 0, EndOffset: 100}, Events: []evidenceEvent{}}
	first.PayloadHash = evidencePayloadHash(first)
	second := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: "master", BatchID: "queue_second_abcdefgh", LogSchema: 2, HMACKeyID: "key-1",
		Source: evidenceSourceRange{Kind: "access", FileID: "42", StartOffset: 100, EndOffset: 200}, Events: []evidenceEvent{}}
	second.PayloadHash = evidencePayloadHash(second)
	if err := spoolEvidence(cfg, first); err != nil {
		t.Fatal(err)
	}
	if err := spoolEvidence(cfg, second); err != nil {
		t.Fatal(err)
	}
	paths, err := listEvidenceOutbox(cfg.evidenceOutboxPath)
	if err != nil || len(paths) != 2 {
		t.Fatalf("queue=%v err=%v", paths, err)
	}
	sameTime := time.Unix(1_700_000_000, 0)
	for _, path := range paths {
		if err := os.Chtimes(path, sameTime, sameTime); err != nil {
			t.Fatal(err)
		}
	}
	var delivered []string
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var payload evidenceBatch
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		delivered = append(delivered, payload.BatchID)
		_ = json.NewEncoder(w).Encode(evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: payload.BatchID, PayloadHash: payload.PayloadHash, Accepted: len(payload.Events)})
	}))
	defer sink.Close()
	cfg.evidenceSinkURL, cfg.token = sink.URL, "secret"
	// Each drain re-reads the on-disk queue, equivalent to a process restart.
	if err := drainEvidenceOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if err := drainEvidenceOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if fmt.Sprint(delivered) != "[queue_first_abcdefgh queue_second_abcdefgh]" {
		t.Fatalf("durable queue order changed: %v", delivered)
	}
}

func TestBlockedEvidenceSinkDoesNotDelayMinuteLane(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := schema2Line(time.Now().Unix(), "200", "200", "OK", "nginx-id", "oneapi-id") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	payload, _, ok, err := readBatch(cfg, cursor{})
	if err != nil || !ok || payload.Evidence == nil {
		t.Fatalf("prepare: ok=%v err=%v", ok, err)
	}
	if err := spoolEvidence(cfg, *payload.Evidence); err != nil {
		t.Fatal(err)
	}
	minute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer minute.Close()
	started, release := make(chan struct{}), make(chan struct{})
	evidence := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var batch evidenceBatch
		_ = json.NewDecoder(r.Body).Decode(&batch)
		select {
		case <-started:
		default:
			close(started)
		}
		<-release
		_ = json.NewEncoder(w).Encode(evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: batch.BatchID, PayloadHash: batch.PayloadHash, Accepted: len(batch.Events)})
	}))
	defer evidence.Close()
	cfg.sinkURL, cfg.evidenceSinkURL, cfg.token = minute.URL, evidence.URL, "secret"
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go runEvidenceWorker(ctx, cfg)
	select {
	case <-started:
	case <-time.After(2 * time.Second):
		close(release)
		t.Fatal("evidence request did not start")
	}
	begin := time.Now()
	err = runOnce(context.Background(), cfg)
	elapsed := time.Since(begin)
	close(release)
	if err != nil {
		t.Fatalf("minute lane failed: %v", err)
	}
	if elapsed > time.Second {
		t.Fatalf("blocked evidence sink delayed minute lane: %v", elapsed)
	}
}

func TestEvidenceFilesystemFailureIsReportedButDoesNotStopMinuteLane(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	line := schema2Line(time.Now().Unix(), "200", "200", "OK", "nginx-id", "oneapi-id") + "\n"
	if err := os.WriteFile(cfg.logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.evidenceOutboxPath, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	var received batch
	minute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer minute.Close()
	cfg.sinkURL, cfg.token = minute.URL, "secret"
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatalf("evidence filesystem failure blocked minute lane: %v", err)
	}
	if received.EvidencePersistFailures != 1 || received.EvidenceDroppedEvents != 1 {
		t.Fatalf("loss was not visible in minute telemetry: %+v", received)
	}
	value, err := loadCursor(cfg.cursorPath)
	if err != nil || value.EvidencePersistFailures != 1 || value.EvidenceDroppedEvents != 1 {
		t.Fatalf("loss epoch was not persisted with the minute cursor: cursor=%+v err=%v", value, err)
	}
	if err := os.Remove(cfg.evidenceOutboxPath); err != nil {
		t.Fatal(err)
	}
	file, err := os.OpenFile(cfg.logPath, os.O_APPEND|os.O_WRONLY, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	_, writeErr := file.WriteString(schema2Line(time.Now().Unix()+1, "200", "200", "OK", "nginx-id-2", "oneapi-id-2") + "\n")
	closeErr := file.Close()
	if writeErr != nil || closeErr != nil {
		t.Fatalf("append=%v close=%v", writeErr, closeErr)
	}
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatalf("recovered evidence filesystem blocked minute lane: %v", err)
	}
	paths, err := listEvidenceOutbox(cfg.evidenceOutboxPath)
	if err != nil || len(paths) != 1 {
		t.Fatalf("expected one recovered evidence batch: paths=%v err=%v", paths, err)
	}
	data, err := os.ReadFile(paths[0])
	var recovered evidenceBatch
	if err != nil || json.Unmarshal(data, &recovered) != nil || recovered.Source.EvidencePersistFailures != 1 || recovered.Source.EvidenceDroppedEvents != 1 {
		t.Fatalf("recovered batch did not carry the loss epoch: batch=%+v err=%v", recovered, err)
	}
}

func TestEmptyEvidenceCheckpointFilesystemFailureDoesNotStopMinuteLane(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	if err := os.WriteFile(cfg.logPath, []byte(fixtureLine(time.Now().Unix(), "/v1/responses", "200")+"\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(cfg.evidenceOutboxPath, []byte("not-a-directory"), 0o600); err != nil {
		t.Fatal(err)
	}
	var received batch
	minute := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer minute.Close()
	cfg.sinkURL, cfg.token = minute.URL, "secret"
	if err := runOnce(context.Background(), cfg); err != nil {
		t.Fatalf("empty evidence filesystem failure blocked minute lane: %v", err)
	}
	if received.EvidencePersistFailures != 1 || received.EvidenceDroppedEvents != 0 {
		t.Fatalf("empty checkpoint loss telemetry is wrong: %+v", received)
	}
	value, err := loadCursor(cfg.cursorPath)
	if err != nil || value.EvidencePersistFailures != 1 || value.EvidenceDroppedEvents != 0 || value.Offset == 0 {
		t.Fatalf("empty checkpoint cursor did not advance: cursor=%+v err=%v", value, err)
	}
}

func TestPartialAckRecordsOnlyRejectedEvents(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	payload := evidenceBatch{BatchID: "partial-ack", PayloadHash: strings.Repeat("a", 64), Source: evidenceSourceRange{FirstEventMS: 100, LastEventMS: 200}, Events: make([]evidenceEvent, 3)}
	if err := recordEvidenceGap(cfg, payload, 1); err != nil {
		t.Fatal(err)
	}
	gap, err := loadGapState(cfg.evidenceOutboxPath)
	if err != nil || gap.DroppedEvents != 1 || gap.GapCount != 1 {
		t.Fatalf("partial ack gap wrong: %+v err=%v", gap, err)
	}
}

func TestPartialAckThroughDeliveryCreatesExactGap(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	payload := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: "master", BatchID: "partial_delivery_abcdefgh", LogSchema: 2, HMACKeyID: "key-1",
		Source: evidenceSourceRange{Kind: "access", FileID: "42", StartOffset: 0, EndOffset: 100, FirstEventMS: 100, LastEventMS: 300},
		Events: []evidenceEvent{{EventID: strings.Repeat("a", 64)}, {EventID: strings.Repeat("b", 64)}, {EventID: strings.Repeat("c", 64)}}}
	payload.PayloadHash = evidencePayloadHash(payload)
	if err := spoolEvidence(cfg, payload); err != nil {
		t.Fatal(err)
	}
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_ = json.NewEncoder(w).Encode(evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: payload.BatchID, PayloadHash: payload.PayloadHash, Accepted: 2, Rejected: 1})
	}))
	defer sink.Close()
	cfg.evidenceSinkURL, cfg.token = sink.URL, "secret"
	if err := drainEvidenceOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	gap, err := loadGapState(cfg.evidenceOutboxPath)
	if err != nil || gap.GapCount != 1 || gap.DroppedEvents != 1 {
		t.Fatalf("partial delivery gap wrong: %+v err=%v", gap, err)
	}
}

func TestPermanentHTTPRejectionRetainsBoundedForensics(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	payload := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: "master", BatchID: "permanent_reject_abcdefgh", LogSchema: 2, HMACKeyID: "key-1",
		Source: evidenceSourceRange{Kind: "access", FileID: "42", StartOffset: 0, EndOffset: 100, FirstEventMS: 100, LastEventMS: 100},
		Events: []evidenceEvent{{EventID: strings.Repeat("a", 64)}}}
	payload.PayloadHash = evidencePayloadHash(payload)
	if err := spoolEvidence(cfg, payload); err != nil {
		t.Fatal(err)
	}
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadRequest) }))
	defer sink.Close()
	cfg.evidenceSinkURL, cfg.token = sink.URL, "secret"
	if err := drainEvidenceOnce(context.Background(), cfg); err != nil {
		t.Fatalf("permanent rejection should be durably retired: %v", err)
	}
	active, _ := listEvidenceOutbox(cfg.evidenceOutboxPath)
	_, rejected, err := evidenceRejectedStats(cfg.evidenceOutboxPath)
	gap, gapErr := loadGapState(cfg.evidenceOutboxPath)
	if len(active) != 0 || err != nil || rejected != 1 || gapErr != nil || gap.GapCount != 1 || gap.DroppedEvents != 1 {
		t.Fatalf("permanent rejection forensics wrong: active=%v rejected=%d gap=%+v err=%v gapErr=%v", active, rejected, gap, err, gapErr)
	}
}

func TestOversizedEvidenceFileIsQuarantinedWithoutReadingPayload(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	cfg.evidenceOutboxMax = 64 << 20
	if err := os.MkdirAll(cfg.evidenceOutboxPath, 0o700); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(cfg.evidenceOutboxPath, "00000000000000000001-oversized_abcdefgh.json")
	file, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := file.Truncate(maxEvidenceBatchFileBytes + 1); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	_ = file.Close()
	if err := drainEvidenceOnce(context.Background(), cfg); err == nil || !strings.Contains(err.Error(), "oversized") {
		t.Fatalf("oversized payload must be rejected visibly: %v", err)
	}
	if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("oversized active head was not removed: %v", err)
	}
}

func TestRejectedForensicsAreBoundedByCount(t *testing.T) {
	dir := t.TempDir()
	cfg := evidenceConfig(dir)
	if err := os.MkdirAll(cfg.evidenceOutboxPath, 0o700); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 101; i++ {
		path := filepath.Join(cfg.evidenceOutboxPath, fmt.Sprintf("%020d-rejected_%08d.json", i+1, i))
		if err := os.WriteFile(path, []byte("rejected"), 0o600); err != nil {
			t.Fatal(err)
		}
		if err := retainRejectedEvidenceLocked(cfg, path); err != nil {
			t.Fatal(err)
		}
	}
	_, count, err := evidenceRejectedStats(cfg.evidenceOutboxPath)
	if err != nil || count != 100 {
		t.Fatalf("rejected evidence count is not bounded: count=%d err=%v", count, err)
	}
}
