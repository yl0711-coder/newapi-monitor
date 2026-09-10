package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestECSErrorFrozenRetryDoesNotOverlapNewLogs(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "errack-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := config{node: "ecs-fixture", ecsSocket: filepath.Join(dir, "agent.sock"), errorLogPath: filepath.Join(dir, "error.log"), errorCursorPath: filepath.Join(dir, "cursor.json"), errorSinkURL: "https://monitor.example/internal/nginx-errors", errorTimezone: "UTC", token: "fixture", maxLines: 100, retentionDays: 7}
	l, err := net.Listen("unix", c.ecsSocket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(c.ecsSocket, 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	received := make(chan []byte, 4)
	s := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		received <- raw
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		var p errorBatch
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Error(err)
			return
		}
		sum := sha256.Sum256(raw)
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "state": "durably_archived", "node": p.Node, "lane": "error", "batch_id": p.BatchID, "body_sha256": hex.EncodeToString(sum[:])})
	})}
	go func() { _ = s.Serve(l) }()
	defer s.Close()
	line := errorFixture(time.Now().UTC(), "error", "connect() failed (111: Connection refused) while connecting to upstream")
	if err := os.WriteFile(c.errorLogPath, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runErrorOnce(context.Background(), c); err == nil {
		t.Fatal("lost ACK advanced cursor")
	}
	frozen, err := os.ReadFile(ecsErrorInflightPath(c))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(c.errorLogPath, []byte(line+line), 0600); err != nil {
		t.Fatal(err)
	}
	// Corruption must stop before network or cursor updates.
	if err := os.WriteFile(ecsErrorInflightPath(c), []byte(`{}`), 0600); err != nil {
		t.Fatal(err)
	}
	if err := runErrorOnce(context.Background(), c); err == nil || calls.Load() != 1 {
		t.Fatal("corrupt pending journal ignored")
	}
	if err := os.WriteFile(ecsErrorInflightPath(c), frozen, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runErrorOnce(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-received, <-received) {
		t.Fatal("retry re-read an expanded source range")
	}
	current, err := loadCursor(c.errorCursorPath)
	if err != nil || current.Offset != int64(len(line)) {
		t.Fatalf("cursor skipped unread tail %+v %v", current, err)
	}
	// Simulate a crash after cursor commit but before pending-file removal.
	if err := os.WriteFile(ecsErrorInflightPath(c), frozen, 0600); err != nil {
		t.Fatal(err)
	}
	if err := runErrorOnce(context.Background(), c); err != nil || calls.Load() != 2 {
		t.Fatal("committed journal was resent", err)
	}
	if err := runErrorOnce(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	var last errorBatch
	if err := json.Unmarshal(<-received, &last); err != nil {
		t.Fatal(err)
	}
	var count int64
	for _, row := range last.Samples {
		count += row.Count
	}
	if count != 1 {
		t.Fatalf("new batch overlapped old log: %d", count)
	}
	current, err = loadCursor(c.errorCursorPath)
	if err != nil || current.Offset != int64(2*len(line)) {
		t.Fatal("new tail not advanced", err)
	}
	if _, err := runLegacyHeartbeatOnce(context.Background(), c, time.Now()); err != nil || calls.Load() != 3 {
		t.Fatal("ECS emitted legacy mutable heartbeat", err)
	}
}
