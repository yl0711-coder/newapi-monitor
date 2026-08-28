package main

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

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
