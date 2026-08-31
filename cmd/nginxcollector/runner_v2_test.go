package main

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	monitorpkg "github.com/yl0711-coder/newapi-monitor/monitor"
)

type fakeSourceV2Server struct {
	mu                  sync.Mutex
	epoch               string
	fileID              string
	next                int64
	confirmed           int64
	proofs              []sourceAckProofV2
	dataCommits         int
	failDataResponse    bool
	manifestRegistered  bool
	kind                string
	legacyDataResponse  bool
	recoveryCalls       int
	confirmationCalls   int
	failRecovery        bool
	failConfirmResponse bool
}

type realMonitorFaultProxy struct {
	mu           sync.Mutex
	next         http.Handler
	failDataPath string
	failAckKind  string
}

func (p *realMonitorFaultProxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	p.mu.Lock()
	next := p.next
	p.mu.Unlock()
	recorder := httptest.NewRecorder()
	next.ServeHTTP(recorder, r)
	p.mu.Lock()
	fail := false
	if recorder.Code == http.StatusOK && p.failDataPath == r.URL.Path {
		p.failDataPath, fail = "", true
	} else if recorder.Code == http.StatusOK && r.URL.Path == "/internal/nginx-source/v2/ack" && p.failAckKind == r.URL.Query().Get("kind") {
		p.failAckKind, fail = "", true
	}
	p.mu.Unlock()
	if fail {
		http.Error(w, "simulated response loss after durable handler commit", http.StatusServiceUnavailable)
		return
	}
	for key, values := range recorder.Header() {
		for _, value := range values {
			w.Header().Add(key, value)
		}
	}
	w.WriteHeader(recorder.Code)
	_, _ = w.Write(recorder.Body.Bytes())
}

func (p *realMonitorFaultProxy) setNext(next http.Handler) {
	p.mu.Lock()
	p.next = next
	p.mu.Unlock()
}

func (p *realMonitorFaultProxy) failNextCommitAndRecovery(path, kind string) {
	p.mu.Lock()
	p.failDataPath, p.failAckKind = path, kind
	p.mu.Unlock()
}

func (s *fakeSourceV2Server) handler(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer secret" {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	w.Header().Set("Content-Type", "application/json")
	switch r.URL.Path {
	case "/internal/nginx-source/v2/capabilities":
		_ = json.NewEncoder(w).Encode(map[string]any{
			"protocol": 2, "kinds": []string{"access", "error"}, "manifest": true,
			"range_cas": true, "source_epoch": true, "ack_echo": true, "ack_confirm": true, "heartbeat": true,
			"max_range_bytes": maxSourceRangeBytesV2, "max_unconfirmed_bytes": int64(1 << 30),
			"max_unconfirmed_ranges": int64(64), "server_time": time.Now().Unix(),
		})
	case "/internal/nginx-source/v2/manifest":
		var request struct {
			Node     string           `json:"node"`
			Manifest sourceManifestV2 `json:"manifest"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || len(request.Manifest.Files) != 1 {
			http.Error(w, "bad manifest", http.StatusBadRequest)
			return
		}
		data, _ := json.Marshal(request.Manifest)
		sum := sha256.Sum256(data)
		hash := hex.EncodeToString(sum[:])
		duplicate := s.manifestRegistered
		if !duplicate {
			s.epoch, s.fileID, s.next, s.confirmed = request.Manifest.SourceEpoch, request.Manifest.Files[0].FileID, request.Manifest.Files[0].BaseOffset, request.Manifest.Files[0].BaseOffset
			s.kind = request.Manifest.Kind
			s.manifestRegistered = true
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "duplicate": duplicate, "manifest_sha256": hash, "manifest_revision": 1})
	case "/internal/nginx", "/internal/nginx-errors":
		var source *sourceRangeV2
		var batchID string
		if r.URL.Path == "/internal/nginx" {
			var payload batch
			if json.NewDecoder(r.Body).Decode(&payload) == nil {
				source = payload.SourceRangeV2
				batchID = payload.BatchID
			}
		} else {
			var payload errorBatch
			if json.NewDecoder(r.Body).Decode(&payload) == nil {
				source = payload.SourceRangeV2
				batchID = payload.BatchID
			}
		}
		if source == nil {
			http.Error(w, "bad batch", http.StatusBadRequest)
			return
		}
		if s.legacyDataResponse {
			_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "duplicate": false, "stored": 1})
			return
		}
		if source.SourceEpoch != s.epoch || source.FileID != s.fileID || source.Kind != s.kind {
			http.Error(w, "identity", http.StatusConflict)
			return
		}
		if source.StartOffset == s.next {
			s.next = source.EndOffset
			s.proofs = append(s.proofs, sourceAckProofV2{StartOffset: source.StartOffset, EndOffset: source.EndOffset, ContentSHA256: source.ContentSHA256})
			s.dataCommits++
		} else if source.EndOffset != s.next {
			http.Error(w, "range", http.StatusConflict)
			return
		}
		if s.failDataResponse {
			s.failDataResponse = false
			http.Error(w, "simulated lost acknowledgement", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "duplicate": false, "stored": 1, "source_ack_v2": sourceCommitAckV2{
			Protocol: source.Protocol, Kind: source.Kind, SourceEpoch: source.SourceEpoch, FileID: source.FileID,
			StartOffset: source.StartOffset, EndOffset: source.EndOffset, NextOffset: source.EndOffset,
			ContentSHA256: source.ContentSHA256,
			BatchID:       batchID,
		}})
	case "/internal/nginx-source/v2/ack":
		s.recoveryCalls++
		if s.failRecovery {
			http.Error(w, "simulated recovery outage", http.StatusServiceUnavailable)
			return
		}
		if r.URL.Query().Get("kind") != s.kind {
			http.Error(w, "kind", http.StatusConflict)
			return
		}
		offset, _ := strconv.ParseInt(r.URL.Query().Get("client_offset"), 10, 64)
		proofs := make([]sourceAckProofV2, 0)
		for _, proof := range s.proofs {
			if proof.EndOffset > offset {
				proofs = append(proofs, proof)
			}
		}
		_ = json.NewEncoder(w).Encode(sourceAckRecoveryV2{SourceEpoch: s.epoch, FileID: s.fileID, NextOffset: s.next, Proofs: proofs})
	case "/internal/nginx-source/v2/confirm":
		s.confirmationCalls++
		var request struct {
			ConfirmedOffset int64 `json:"confirmed_offset"`
		}
		if json.NewDecoder(r.Body).Decode(&request) != nil || request.ConfirmedOffset < s.confirmed || request.ConfirmedOffset > s.next {
			http.Error(w, "bad confirmation", http.StatusConflict)
			return
		}
		duplicate := request.ConfirmedOffset == s.confirmed
		s.confirmed = request.ConfirmedOffset
		if s.failConfirmResponse {
			s.failConfirmResponse = false
			http.Error(w, "simulated confirmation response loss", http.StatusServiceUnavailable)
			return
		}
		_ = json.NewEncoder(w).Encode(map[string]any{"ok": true, "duplicate": duplicate, "confirmed_offset": s.confirmed})
	case "/internal/nginx-source/v2/files":
		http.Error(w, "unexpected registration", http.StatusConflict)
	default:
		http.NotFound(w, r)
	}
}

func TestAccessSourceV2RunnerRecoversAfterDurableConfirmResponseIsLost(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.jsonl")
	legacyCursorPath := filepath.Join(dir, "cursor-v1.json")
	v2CursorPath := filepath.Join(dir, "cursor-v2.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := cursor{Version: cursorVersion, Device: fileDevice(info), Inode: fileInode(info), Offset: 0,
		LastAckedBatchID: "legacy_checkpoint_abcdefgh", LastAckedDevice: fileDevice(info), LastAckedInode: fileInode(info), LastAckedOffset: 0}
	if err := saveCursor(legacyCursorPath, legacy); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceV2Server{failConfirmResponse: true}
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer server.Close()
	cfg := config{node: "master", logPath: logPath, cursorPath: legacyCursorPath, sinkURL: server.URL + "/internal/nginx", token: "secret", maxLines: 100, retentionDays: 7, allowHTTP: true, evidenceMode: "off"}
	firstClient, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, firstClient); err == nil {
		t.Fatal("runner did not surface the lost confirmation response")
	}
	state, err := loadSourceCursorV2(v2CursorPath)
	if err != nil || len(state.Files) != 1 || state.Files[0].AckedOffset != int64(len(line)) || !state.Files[0].RecoveryPending {
		t.Fatalf("durable local ACK was not left recoverable: state=%+v err=%v", state, err)
	}
	secondClient, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, secondClient); err != nil {
		t.Fatalf("restart did not idempotently recover the confirmed range: %v", err)
	}
	state, err = loadSourceCursorV2(v2CursorPath)
	if err != nil || state.Files[0].RecoveryPending || state.Files[0].AckedOffset != int64(len(line)) {
		t.Fatalf("confirmation recovery was not finalized: state=%+v err=%v", state, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.dataCommits != 1 || fake.confirmed != int64(len(line)) || fake.confirmationCalls != 2 {
		t.Fatalf("confirmation retry changed accepted data: commits=%d confirmed=%d confirm_calls=%d", fake.dataCommits, fake.confirmed, fake.confirmationCalls)
	}
}

func TestAccessSourceV2RunnerRecoversLostHTTPAckWithoutDuplicate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.jsonl")
	legacyCursorPath := filepath.Join(dir, "cursor-v1.json")
	v2CursorPath := filepath.Join(dir, "cursor-v2.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := cursor{Version: cursorVersion, Device: fileDevice(info), Inode: fileInode(info), Offset: 0,
		LastAckedBatchID: "legacy_checkpoint_abcdefgh", LastAckedDevice: fileDevice(info), LastAckedInode: fileInode(info), LastAckedOffset: 0}
	if err := saveCursor(legacyCursorPath, legacy); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceV2Server{failDataResponse: true}
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer server.Close()
	cfg := config{node: "master", logPath: logPath, cursorPath: legacyCursorPath, sinkURL: server.URL + "/internal/nginx", token: "secret", maxLines: 100, retentionDays: 7, allowHTTP: true, evidenceMode: "off"}
	client, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, client); err != nil {
		t.Fatalf("lost HTTP acknowledgement was not recovered: %v", err)
	}
	state, err := loadSourceCursorV2(v2CursorPath)
	if err != nil || len(state.Files) != 1 || state.Files[0].AckedOffset != int64(len(line)) {
		t.Fatalf("durable cursor=%+v err=%v", state, err)
	}
	fake.mu.Lock()
	commits, confirmed := fake.dataCommits, fake.confirmed
	fake.mu.Unlock()
	if commits != 1 || confirmed != int64(len(line)) {
		t.Fatalf("server commits=%d confirmed=%d want=1/%d", commits, confirmed, len(line))
	}
	if err := os.Remove(legacyCursorPath); err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, client); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.dataCommits != 1 {
		t.Fatalf("restart duplicated accepted bytes: commits=%d", fake.dataCommits)
	}
	if fake.recoveryCalls != 1 || fake.confirmationCalls != 1 {
		t.Fatalf("healthy restart polled historical files: recover=%d confirm=%d", fake.recoveryCalls, fake.confirmationCalls)
	}
	if fake.epoch == "" || fake.fileID == "" {
		t.Fatal(fmt.Errorf("manifest identity missing"))
	}
}

func TestErrorSourceV2RunnerRecoversLostHTTPAckWithoutDuplicate(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "error.log")
	legacyCursorPath := filepath.Join(dir, "error-cursor-v1.json")
	v2CursorPath := filepath.Join(dir, "error-cursor-v2.json")
	line := errorFixture(time.Now().UTC(), "error", "upstream timed out while reading response header from upstream")
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := cursor{Version: cursorVersion, Device: fileDevice(info), Inode: fileInode(info), Offset: 0,
		LastAckedBatchID: "legacy_error_checkpoint", LastAckedDevice: fileDevice(info), LastAckedInode: fileInode(info), LastAckedOffset: 0}
	if err := saveCursor(legacyCursorPath, legacy); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceV2Server{failDataResponse: true}
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer server.Close()
	cfg := config{node: "slave", errorLogPath: logPath, errorCursorPath: legacyCursorPath,
		errorSinkURL: server.URL + "/internal/nginx-errors", errorTimezone: "UTC", token: "secret",
		maxLines: 100, retentionDays: 7, allowHTTP: true}
	client, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runErrorSourceV2Once(t.Context(), cfg, v2CursorPath, client); err != nil {
		t.Fatalf("lost HTTP acknowledgement was not recovered: %v", err)
	}
	state, err := loadSourceCursorV2(v2CursorPath)
	if err != nil || len(state.Files) != 1 || state.Files[0].AckedOffset != int64(len(line)) {
		t.Fatalf("durable cursor=%+v err=%v", state, err)
	}
	fake.mu.Lock()
	commits, confirmed := fake.dataCommits, fake.confirmed
	fake.mu.Unlock()
	if commits != 1 || confirmed != int64(len(line)) {
		t.Fatalf("server commits=%d confirmed=%d want=1/%d", commits, confirmed, len(line))
	}
	if err := os.Remove(legacyCursorPath); err != nil {
		t.Fatal(err)
	}
	if err := runErrorSourceV2Once(t.Context(), cfg, v2CursorPath, client); err != nil {
		t.Fatal(err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.dataCommits != 1 {
		t.Fatalf("restart duplicated accepted error bytes: commits=%d", fake.dataCommits)
	}
	if fake.recoveryCalls != 1 || fake.confirmationCalls != 1 {
		t.Fatalf("healthy error restart polled historical files: recover=%d confirm=%d", fake.recoveryCalls, fake.confirmationCalls)
	}
}

func TestSourceV2RealMonitorRotationLostAckRestartIsExactlyOnceForBothLanes(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	mainPath := filepath.Join(dir, "monitor.db")
	factsPath := filepath.Join(dir, "facts.db")
	settings := monitorpkg.Settings{
		StorePath: mainPath, UsageFactsStorePath: factsPath, StoreBackupDir: filepath.Join(dir, "backups"),
		StoreMigrationBackupRetention: 3, StoreBackupEnabled: false, LocalSnapshotOnly: true,
		NginxEnabled: true, NginxErrorEnabled: true, NginxRetentionDays: 7, IngestToken: "secret",
		NginxAllowedNodes: []string{"master"}, NginxSourceV2Enabled: true, NginxSourceV2CutoverEnabled: true,
		NginxSourceV2AllowedNodes: []string{"master"}, NginxSourceV2AllowedLanes: []string{"master:access", "master:error"},
	}
	m, err := monitorpkg.New(settings)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { m.Close() }()
	router := gin.New()
	m.RegisterRoutes(router)
	proxy := &realMonitorFaultProxy{next: router}
	server := httptest.NewServer(proxy)
	defer server.Close()

	accessPath := filepath.Join(dir, "access.jsonl")
	accessCursorV1 := filepath.Join(dir, "access-v1.json")
	accessCursorV2 := filepath.Join(dir, "access-v2.json")
	accessFirst := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	accessLate := fixtureLine(time.Now().Unix(), "/v1/chat/completions", "502") + "\n"
	accessCurrent := fixtureLine(time.Now().Unix(), "/api/status", "200") + "\n"
	if err := os.WriteFile(accessPath, []byte(accessFirst), 0o600); err != nil {
		t.Fatal(err)
	}
	accessConfig := config{node: "master", logPath: accessPath, cursorPath: accessCursorV1,
		sinkURL: server.URL + "/internal/nginx", token: "secret", maxLines: 100, retentionDays: 7,
		allowHTTP: true, evidenceMode: "off", sourceV2Prepare: true}
	if err := runOnce(t.Context(), accessConfig); err != nil {
		t.Fatalf("prepare access boundary: %v", err)
	}
	oldAccess, err := os.OpenFile(accessPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(accessPath, accessPath+".1"); err != nil {
		_ = oldAccess.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(accessPath, []byte(accessCurrent), 0o600); err != nil {
		_ = oldAccess.Close()
		t.Fatal(err)
	}
	accessClient, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	proxy.failNextCommitAndRecovery("/internal/nginx", "access")
	if err := runAccessSourceV2Once(t.Context(), accessConfig, accessCursorV2, accessClient); err == nil {
		t.Fatal("access process-death fixture did not lose both commit and recovery responses")
	}
	m.Close()
	m, err = monitorpkg.New(settings)
	if err != nil {
		t.Fatalf("restart Monitor after durable access commit: %v", err)
	}
	restartedRouter := gin.New()
	m.RegisterRoutes(restartedRouter)
	proxy.setNext(restartedRouter)
	accessClient, err = newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := runAccessSourceV2Once(t.Context(), accessConfig, accessCursorV2, accessClient); err != nil {
			t.Fatalf("access restart/drain %d: %v", i, err)
		}
	}
	// The old Nginx worker still owns the pre-rename file descriptor and writes
	// after the collector has already observed that rotated inode at EOF.
	if _, err := oldAccess.WriteString(accessLate); err != nil {
		_ = oldAccess.Close()
		t.Fatal(err)
	}
	if err := oldAccess.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := runAccessSourceV2Once(t.Context(), accessConfig, accessCursorV2, accessClient); err != nil {
			t.Fatalf("access late-writer drain %d: %v", i, err)
		}
	}

	errorPath := filepath.Join(dir, "error.log")
	errorCursorV1 := filepath.Join(dir, "error-v1.json")
	errorCursorV2 := filepath.Join(dir, "error-v2.json")
	errorFirst := errorFixture(time.Now().UTC(), "error", "connect() failed (111: Connection refused) while connecting to upstream")
	errorLate := errorFixture(time.Now().UTC(), "error", "upstream timed out while reading response header from upstream")
	errorCurrent := errorFixture(time.Now().UTC(), "warn", "upstream prematurely closed connection while reading response header from upstream")
	if err := os.WriteFile(errorPath, []byte(errorFirst), 0o600); err != nil {
		t.Fatal(err)
	}
	errorConfig := config{node: "master", errorLogPath: errorPath, errorCursorPath: errorCursorV1,
		errorSinkURL: server.URL + "/internal/nginx-errors", errorTimezone: "UTC", token: "secret",
		maxLines: 100, retentionDays: 7, allowHTTP: true, sourceV2Prepare: true}
	if err := runErrorOnce(t.Context(), errorConfig); err != nil {
		t.Fatalf("prepare error boundary: %v", err)
	}
	oldError, err := os.OpenFile(errorPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(errorPath, errorPath+".1"); err != nil {
		_ = oldError.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(errorPath, []byte(errorCurrent), 0o600); err != nil {
		_ = oldError.Close()
		t.Fatal(err)
	}
	errorClient, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	proxy.failNextCommitAndRecovery("/internal/nginx-errors", "error")
	if err := runErrorSourceV2Once(t.Context(), errorConfig, errorCursorV2, errorClient); err == nil {
		t.Fatal("error process-death fixture did not lose both commit and recovery responses")
	}
	m.Close()
	m, err = monitorpkg.New(settings)
	if err != nil {
		t.Fatalf("restart Monitor after durable error commit: %v", err)
	}
	restartedRouter = gin.New()
	m.RegisterRoutes(restartedRouter)
	proxy.setNext(restartedRouter)
	errorClient, err = newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := runErrorSourceV2Once(t.Context(), errorConfig, errorCursorV2, errorClient); err != nil {
			t.Fatalf("error restart/drain %d: %v", i, err)
		}
	}
	if _, err := oldError.WriteString(errorLate); err != nil {
		_ = oldError.Close()
		t.Fatal(err)
	}
	if err := oldError.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := runErrorSourceV2Once(t.Context(), errorConfig, errorCursorV2, errorClient); err != nil {
			t.Fatalf("error late-writer drain %d: %v", i, err)
		}
	}

	// Rotate both lanes a second time while keeping each former current inode
	// writable. This exercises .1 -> .2 rebinding, new-file registration and
	// late writes without relying on the component-only tracker tests.
	secondAccessLate := fixtureLine(time.Now().Unix(), "/v1/images/generations", "504") + "\n"
	secondAccessCurrent := fixtureLine(time.Now().Unix(), "/v1/messages", "200") + "\n"
	secondOldAccess, err := os.OpenFile(accessPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(accessPath+".1", accessPath+".2"); err != nil {
		_ = secondOldAccess.Close()
		t.Fatal(err)
	}
	if err := os.Rename(accessPath, accessPath+".1"); err != nil {
		_ = secondOldAccess.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(accessPath, []byte(secondAccessCurrent), 0o600); err != nil {
		_ = secondOldAccess.Close()
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := runAccessSourceV2Once(t.Context(), accessConfig, accessCursorV2, accessClient); err != nil {
			t.Fatalf("second access rotation drain %d: %v", i, err)
		}
	}
	if _, err := secondOldAccess.WriteString(secondAccessLate); err != nil {
		_ = secondOldAccess.Close()
		t.Fatal(err)
	}
	if err := secondOldAccess.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := runAccessSourceV2Once(t.Context(), accessConfig, accessCursorV2, accessClient); err != nil {
			t.Fatalf("second access late-writer drain %d: %v", i, err)
		}
	}

	secondErrorLate := errorFixture(time.Now().UTC(), "error", "upstream sent invalid chunked response")
	secondErrorCurrent := errorFixture(time.Now().UTC(), "warn", "an upstream response is buffered to a temporary file")
	secondOldError, err := os.OpenFile(errorPath, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Rename(errorPath+".1", errorPath+".2"); err != nil {
		_ = secondOldError.Close()
		t.Fatal(err)
	}
	if err := os.Rename(errorPath, errorPath+".1"); err != nil {
		_ = secondOldError.Close()
		t.Fatal(err)
	}
	if err := os.WriteFile(errorPath, []byte(secondErrorCurrent), 0o600); err != nil {
		_ = secondOldError.Close()
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if err := runErrorSourceV2Once(t.Context(), errorConfig, errorCursorV2, errorClient); err != nil {
			t.Fatalf("second error rotation drain %d: %v", i, err)
		}
	}
	if _, err := secondOldError.WriteString(secondErrorLate); err != nil {
		_ = secondOldError.Close()
		t.Fatal(err)
	}
	if err := secondOldError.Close(); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := runErrorSourceV2Once(t.Context(), errorConfig, errorCursorV2, errorClient); err != nil {
			t.Fatalf("second error late-writer drain %d: %v", i, err)
		}
	}

	readDB, err := sql.Open("sqlite", mainPath)
	if err != nil {
		t.Fatal(err)
	}
	defer readDB.Close()
	var accessCount, errorCount, accessBatches, errorBatches int64
	if err := readDB.QueryRow("SELECT COALESCE(SUM(count),0) FROM nginx_minute_samples WHERE node='master'").Scan(&accessCount); err != nil {
		t.Fatal(err)
	}
	if err := readDB.QueryRow("SELECT COALESCE(SUM(count),0) FROM nginx_error_minute_samples WHERE node='master'").Scan(&errorCount); err != nil {
		t.Fatal(err)
	}
	if err := readDB.QueryRow("SELECT COUNT(*) FROM nginx_ingest_batches WHERE node='master'").Scan(&accessBatches); err != nil {
		t.Fatal(err)
	}
	if err := readDB.QueryRow("SELECT COUNT(*) FROM nginx_error_ingest_batches WHERE node='master'").Scan(&errorBatches); err != nil {
		t.Fatal(err)
	}
	if accessCount != 5 || errorCount != 5 || accessBatches != 5 || errorBatches != 5 {
		t.Fatalf("real SQLite lost or duplicated rotated bytes: access=%d/%d error=%d/%d", accessCount, accessBatches, errorCount, errorBatches)
	}
	var files, fullyConfirmed int64
	if err := readDB.QueryRow("SELECT COUNT(*), COALESCE(SUM(CASE WHEN next_offset=confirmed_offset THEN 1 ELSE 0 END),0) FROM nginx_source_file_watermarks").Scan(&files, &fullyConfirmed); err != nil {
		t.Fatal(err)
	}
	if files != 6 || fullyConfirmed != files {
		t.Fatalf("source files were not durably confirmed: files=%d confirmed=%d", files, fullyConfirmed)
	}
	for i := 0; i < 2; i++ {
		if err := runAccessSourceV2Once(t.Context(), accessConfig, accessCursorV2, accessClient); err != nil {
			t.Fatal(err)
		}
		if err := runErrorSourceV2Once(t.Context(), errorConfig, errorCursorV2, errorClient); err != nil {
			t.Fatal(err)
		}
	}
	var finalAccess, finalError int64
	if err := readDB.QueryRow("SELECT COALESCE(SUM(count),0) FROM nginx_minute_samples WHERE node='master'").Scan(&finalAccess); err != nil {
		t.Fatal(err)
	}
	if err := readDB.QueryRow("SELECT COALESCE(SUM(count),0) FROM nginx_error_minute_samples WHERE node='master'").Scan(&finalError); err != nil {
		t.Fatal(err)
	}
	if finalAccess != accessCount || finalError != errorCount {
		t.Fatalf("idle retries duplicated aggregates: access=%d->%d error=%d->%d", accessCount, finalAccess, errorCount, finalError)
	}
	if err := runSourceV2Heartbeat(t.Context(), accessConfig, accessPath, accessCursorV1, accessCursorV2, "access", accessClient); err != nil {
		t.Fatalf("access collector heartbeat did not reach real Monitor: %v", err)
	}
	if err := runSourceV2Heartbeat(t.Context(), errorConfig, errorPath, errorCursorV1, errorCursorV2, "error", errorClient); err != nil {
		t.Fatalf("error collector heartbeat did not reach real Monitor: %v", err)
	}
	var afterHeartbeatAccess, afterHeartbeatError int64
	if err := readDB.QueryRow("SELECT COALESCE(SUM(count),0) FROM nginx_minute_samples WHERE node='master'").Scan(&afterHeartbeatAccess); err != nil {
		t.Fatal(err)
	}
	if err := readDB.QueryRow("SELECT COALESCE(SUM(count),0) FROM nginx_error_minute_samples WHERE node='master'").Scan(&afterHeartbeatError); err != nil {
		t.Fatal(err)
	}
	if afterHeartbeatAccess != finalAccess || afterHeartbeatError != finalError {
		t.Fatalf("heartbeats created traffic samples: access=%d->%d error=%d->%d", finalAccess, afterHeartbeatAccess, finalError, afterHeartbeatError)
	}
	var accessIngest, errorIngest, accessBacklog, errorBacklog int64
	var accessKnown, errorKnown bool
	if err := readDB.QueryRow("SELECT last_ingest_ts, backlog_bytes, backlog_known FROM nginx_source_states WHERE node='master'").Scan(&accessIngest, &accessBacklog, &accessKnown); err != nil {
		t.Fatal(err)
	}
	if err := readDB.QueryRow("SELECT last_ingest_ts, backlog_bytes, backlog_known FROM nginx_error_source_states WHERE node='master'").Scan(&errorIngest, &errorBacklog, &errorKnown); err != nil {
		t.Fatal(err)
	}
	if accessIngest <= 0 || errorIngest <= 0 || !accessKnown || !errorKnown || accessBacklog != 0 || errorBacklog != 0 {
		t.Fatalf("heartbeat health was not persisted: access=%d/%d/%v error=%d/%d/%v", accessIngest, accessBacklog, accessKnown, errorIngest, errorBacklog, errorKnown)
	}
	var currentUnconfirmed int64
	if err := readDB.QueryRow("SELECT COUNT(*) FROM nginx_source_file_watermarks WHERE current=1 AND next_offset<>confirmed_offset").Scan(&currentUnconfirmed); err != nil {
		t.Fatal(err)
	}
	if currentUnconfirmed != 0 {
		t.Fatalf("heartbeats accepted without an exact current confirmed offset: %d", currentUnconfirmed)
	}
}

func TestAccessSourceV2RunnerRecoversAfterProcessDiesBeforeRecovery(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.jsonl")
	legacyCursorPath := filepath.Join(dir, "cursor-v1.json")
	v2CursorPath := filepath.Join(dir, "cursor-v2.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := cursor{Version: cursorVersion, Device: fileDevice(info), Inode: fileInode(info), Offset: 0,
		LastAckedBatchID: "legacy_checkpoint_abcdefgh", LastAckedDevice: fileDevice(info), LastAckedInode: fileInode(info), LastAckedOffset: 0}
	if err := saveCursor(legacyCursorPath, legacy); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceV2Server{failDataResponse: true, failRecovery: true}
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer server.Close()
	cfg := config{node: "master", logPath: logPath, cursorPath: legacyCursorPath, sinkURL: server.URL + "/internal/nginx", token: "secret", maxLines: 100, retentionDays: 7, allowHTTP: true, evidenceMode: "off"}
	firstClient, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, firstClient); err == nil {
		t.Fatal("simulated process did not stop after POST and recovery responses were lost")
	}
	state, err := loadSourceCursorV2(v2CursorPath)
	if err != nil || len(state.Files) != 1 || state.Files[0].AckedOffset != 0 || !state.Files[0].RecoveryPending {
		t.Fatalf("pre-POST recovery intent was not durable: state=%+v err=%v", state, err)
	}
	fake.mu.Lock()
	fake.failRecovery = false
	fake.mu.Unlock()
	secondClient, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, secondClient); err != nil {
		t.Fatalf("new process did not recover durable pending range: %v", err)
	}
	state, err = loadSourceCursorV2(v2CursorPath)
	if err != nil || state.Files[0].AckedOffset != int64(len(line)) || state.Files[0].RecoveryPending {
		t.Fatalf("restarted cursor=%+v err=%v", state, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.dataCommits != 1 || fake.confirmed != int64(len(line)) {
		t.Fatalf("restart duplicated or failed to confirm: commits=%d confirmed=%d", fake.dataCommits, fake.confirmed)
	}
}

func TestSourceV2RunnerRefusesMissingCapabilitiesBeforeCreatingCursor(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.jsonl")
	legacyCursorPath := filepath.Join(dir, "cursor-v1.json")
	v2CursorPath := filepath.Join(dir, "cursor-v2.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := cursor{Version: cursorVersion, Device: fileDevice(info), Inode: fileInode(info), Offset: 0,
		LastAckedBatchID: "legacy_checkpoint_abcdefgh", LastAckedDevice: fileDevice(info), LastAckedInode: fileInode(info), LastAckedOffset: 0}
	if err := saveCursor(legacyCursorPath, legacy); err != nil {
		t.Fatal(err)
	}
	manifestCalls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if r.URL.Path == "/internal/nginx-source/v2/capabilities" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"protocol": 2, "kinds": []string{"access"}, "manifest": true, "range_cas": true,
				"source_epoch": true, "ack_echo": true, "ack_confirm": false, "heartbeat": true,
				"max_range_bytes": maxSourceRangeBytesV2, "max_unconfirmed_bytes": int64(1 << 30),
				"max_unconfirmed_ranges": int64(64), "server_time": time.Now().Unix(),
			})
			return
		}
		manifestCalls++
		http.Error(w, "unexpected", http.StatusConflict)
	}))
	defer server.Close()
	cfg := config{node: "master", logPath: logPath, cursorPath: legacyCursorPath, sinkURL: server.URL + "/internal/nginx", token: "secret", maxLines: 100, retentionDays: 7, allowHTTP: true, evidenceMode: "off"}
	client, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, client); err == nil {
		t.Fatal("runner accepted server without durable ACK confirmation")
	}
	if manifestCalls != 0 {
		t.Fatalf("runner mutated server before capability check: calls=%d", manifestCalls)
	}
	if _, err := os.Stat(v2CursorPath); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("runner created v2 cursor before capability check: %v", err)
	}
}

func TestSourceV2RunnerDoesNotAdvanceOnLegacyHTTP200WithoutCommitAck(t *testing.T) {
	dir := t.TempDir()
	logPath := filepath.Join(dir, "access.jsonl")
	legacyCursorPath := filepath.Join(dir, "cursor-v1.json")
	v2CursorPath := filepath.Join(dir, "cursor-v2.json")
	line := fixtureLine(time.Now().Unix(), "/v1/responses", "200") + "\n"
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(logPath)
	if err != nil {
		t.Fatal(err)
	}
	legacy := cursor{Version: cursorVersion, Device: fileDevice(info), Inode: fileInode(info), Offset: 0,
		LastAckedBatchID: "legacy_checkpoint_abcdefgh", LastAckedDevice: fileDevice(info), LastAckedInode: fileInode(info), LastAckedOffset: 0}
	if err := saveCursor(legacyCursorPath, legacy); err != nil {
		t.Fatal(err)
	}
	fake := &fakeSourceV2Server{legacyDataResponse: true}
	server := httptest.NewServer(http.HandlerFunc(fake.handler))
	defer server.Close()
	cfg := config{node: "master", logPath: logPath, cursorPath: legacyCursorPath, sinkURL: server.URL + "/internal/nginx", token: "secret", maxLines: 100, retentionDays: 7, allowHTTP: true, evidenceMode: "off"}
	client, err := newSourceV2HTTPClient(server.URL, "secret", true)
	if err != nil {
		t.Fatal(err)
	}
	if err := runAccessSourceV2Once(t.Context(), cfg, v2CursorPath, client); err == nil {
		t.Fatal("legacy HTTP 200 without exact source ACK advanced the lane")
	}
	state, err := loadSourceCursorV2(v2CursorPath)
	if err != nil || len(state.Files) != 1 || state.Files[0].AckedOffset != 0 {
		t.Fatalf("cursor advanced after unbound 200: state=%+v err=%v", state, err)
	}
	fake.mu.Lock()
	defer fake.mu.Unlock()
	if fake.dataCommits != 0 || fake.next != 0 {
		t.Fatalf("legacy response unexpectedly committed source: commits=%d next=%d", fake.dataCommits, fake.next)
	}
}
