package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestV1ErrorBatchIDRemainsCompatibleAfterDeviceBoundaryUpgrade(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "error.log")
	line := errorFixture(time.Now().UTC(), "error", "connect() failed (111: Connection refused) while connecting to upstream")
	if err := os.WriteFile(path, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	defaultPayload, _, ok, err := readErrorBatch(config{node: "master", errorLogPath: path, errorTimezone: "UTC", maxLines: 100, retentionDays: 7}, cursor{})
	if err != nil || !ok || defaultPayload.SourceBoundary != nil {
		t.Fatalf("default-off error collector changed v1 envelope: ok=%v boundary=%+v err=%v", ok, defaultPayload.SourceBoundary, err)
	}
	payload, _, ok, err := readErrorBatch(config{node: "master", errorLogPath: path, errorTimezone: "UTC", maxLines: 100, retentionDays: 7, sourceV2Prepare: true}, cursor{})
	if err != nil || !ok {
		t.Fatalf("read: ok=%v err=%v", ok, err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	lineDigest := sha256.Sum256([]byte(line))
	contentDigest := sha256.Sum256(lineDigest[:])
	identity := sha256.New()
	_, _ = fmt.Fprintf(identity, "error:master:%d:0:%d:", fileInode(info), len(line))
	_, _ = identity.Write(contentDigest[:])
	want := hex.EncodeToString(identity.Sum(nil))
	if payload.BatchID != want || payload.SourceBoundary == nil || payload.SourceBoundary.Device == 0 {
		t.Fatalf("v1 compatibility changed: got=%s want=%s boundary=%+v", payload.BatchID, want, payload.SourceBoundary)
	}
}

func TestV1ErrorHeartbeatRequiresExactBoundaryAckOnlyInPrepareMode(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	legacy := errorHeartbeatBatch(config{node: "master"}, now, cursor{Device: 7, Inode: 44, Offset: 90})
	wantDigest := sha256.Sum256([]byte(fmt.Sprintf("error-heartbeat:%s:%d", "master", now.Unix()/60)))
	if want := "ehb_" + hex.EncodeToString(wantDigest[:16]); legacy.BatchID != want || legacy.SourceBoundary != nil {
		t.Fatalf("default error heartbeat changed: got=%s boundary=%+v want=%s", legacy.BatchID, legacy.SourceBoundary, want)
	}
	payload := errorHeartbeatBatch(config{node: "master", sourceV2Prepare: true}, now, cursor{Device: 7, Inode: 44, Offset: 90})
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer sink.Close()
	if err := postErrorBatch(context.Background(), config{errorSinkURL: sink.URL, token: "secret"}, payload); err == nil {
		t.Fatal("error prepare mode trusted a response without an exact boundary ack")
	}
}

func TestRunErrorOncePrepareDoesNotAdvanceOnMissingBoundaryAck(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "error.log"), filepath.Join(dir, "error-cursor.json")
	line := errorFixture(time.Now().UTC(), "error", "connect() failed (111: Connection refused) while connecting to upstream")
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	sink := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	defer sink.Close()
	cfg := config{node: "master", errorLogPath: logPath, errorCursorPath: cursorPath, errorSinkURL: sink.URL, errorTimezone: "UTC", token: "secret", maxLines: 100, retentionDays: 7, sourceV2Prepare: true}
	if err := runErrorOnce(context.Background(), cfg); err == nil {
		t.Fatal("error prepare mode advanced without an exact source boundary acknowledgement")
	}
	if got, err := loadCursor(cursorPath); err != nil || got.Offset != 0 || got.LastAckedBatchID != "" {
		t.Fatalf("error prepare failure changed the durable cursor: cursor=%+v err=%v", got, err)
	}
}

func errorFixture(stamp time.Time, severity, message string) string {
	return stamp.Format("2006/01/02 15:04:05") + " [" + severity + "] 12#12: *9 " + message + ", client: 203.0.113.9, request: \"GET /secret?key=x HTTP/1.1\"\n"
}

func TestParseNginxErrorLineClassifiesWithoutRawData(t *testing.T) {
	line := errorFixture(time.Now(), "error", "upstream timed out (110: Operation timed out) while reading response header from upstream")
	row, ok := parseNginxErrorLine([]byte(line), time.Local)
	if !ok || row.Category != "upstream_timeout" || row.Severity != "error" || row.Count != 1 {
		t.Fatalf("classification failed: %+v ok=%v", row, ok)
	}
	encoded, _ := json.Marshal(row)
	for _, secret := range []string{"203.0.113.9", "/secret", "key=x", "upstream timed out"} {
		if strings.Contains(string(encoded), secret) {
			t.Fatalf("raw error detail escaped node: %s", encoded)
		}
	}
}

func TestParseNginxErrorLineIgnoresInfoWithoutReportingDataLoss(t *testing.T) {
	row, ok := parseNginxErrorLine([]byte(errorFixture(time.Now(), "info", "client closed keepalive connection")), time.Local)
	if !ok || row.Category != "" {
		t.Fatalf("info should be a valid ignored line: %+v ok=%v", row, ok)
	}
}

func TestRunErrorOnceAdvancesIndependentCursorOnlyAfterAck(t *testing.T) {
	dir := t.TempDir()
	logPath, cursorPath := filepath.Join(dir, "error.log"), filepath.Join(dir, "error-cursor.json")
	line := errorFixture(time.Now().UTC(), "error", "connect() failed (111: Connection refused) while connecting to upstream")
	if err := os.WriteFile(logPath, []byte(line), 0o600); err != nil {
		t.Fatal(err)
	}
	status := http.StatusServiceUnavailable
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer secret" {
			t.Error("missing token")
		}
		var payload map[string]any
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Error(err)
		}
		encoded, _ := json.Marshal(payload)
		if strings.Contains(string(encoded), "Connection refused") {
			t.Fatal("raw error uploaded")
		}
		w.WriteHeader(status)
	}))
	defer server.Close()
	cfg := config{node: "local", errorLogPath: logPath, errorCursorPath: cursorPath, errorSinkURL: server.URL, errorTimezone: "UTC", token: "secret", maxLines: 100, retentionDays: 7}
	if err := runErrorOnce(context.Background(), cfg); err == nil {
		t.Fatal("failed ack should fail")
	}
	if cur, err := loadCursor(cursorPath); err != nil || cur.Offset != 0 {
		t.Fatalf("cursor advanced on failure: %+v err=%v", cur, err)
	}
	status = http.StatusOK
	if err := runErrorOnce(context.Background(), cfg); err != nil {
		t.Fatal(err)
	}
	if cur, err := loadCursor(cursorPath); err != nil || cur.Offset != int64(len(line)) {
		t.Fatalf("cursor not advanced after ack: %+v err=%v", cur, err)
	}
}
