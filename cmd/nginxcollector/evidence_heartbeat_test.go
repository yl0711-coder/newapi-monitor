package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

func heartbeatACK(t *testing.T, w http.ResponseWriter, body []byte) evidenceBatch {
	t.Helper()
	var payload evidenceBatch
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatal(err)
	}
	if len(payload.BatchID) > 64 || len(payload.Events) != 0 || payload.PayloadHash != evidencePayloadHash(payload) {
		t.Fatalf("invalid heartbeat: %+v", payload)
	}
	if err := json.NewEncoder(w).Encode(evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: payload.BatchID, PayloadHash: payload.PayloadHash}); err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestFrozenEvidenceHeartbeatSeparatesSimultaneousCollectors(t *testing.T) {
	bodies := make(chan []byte, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies <- body
		heartbeatACK(t, w, body)
	}))
	defer server.Close()
	now := time.Unix(1_800_000_000, 0)
	seen := map[string]bool{}
	start := make(chan struct{})
	results := make(chan error, 5)
	for i := 0; i < 5; i++ {
		c := evidenceConfig(t.TempDir())
		c.node, c.evidenceSinkURL, c.evidenceFrozenHeartbeat = "ecs-canary", server.URL, true
		go func() {
			<-start
			results <- deliverEvidenceHeartbeat(context.Background(), c, now)
		}()
	}
	close(start)
	for i := 0; i < 5; i++ {
		if err := <-results; err != nil {
			t.Fatal(err)
		}
		var payload evidenceBatch
		if err := json.Unmarshal(<-bodies, &payload); err != nil {
			t.Fatal(err)
		}
		if seen[payload.BatchID] {
			t.Fatal("simultaneous collectors collided")
		}
		seen[payload.BatchID] = true
		if payload.Node != "ecs-canary" {
			t.Fatal("batch namespace must not rewrite source authorization")
		}
	}
}

func TestFrozenEvidenceHeartbeatLostACKRestartsWithIdenticalBody(t *testing.T) {
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		if len(bodies) == 1 {
			// Simulate receiver commit followed by a lost/unusable ACK.
			w.WriteHeader(http.StatusServiceUnavailable)
			return
		}
		heartbeatACK(t, w, body)
	}))
	defer server.Close()
	c := evidenceConfig(t.TempDir())
	c.evidenceSinkURL, c.evidenceFrozenHeartbeat = server.URL, true
	now := time.Unix(1_800_000_000, 0)
	if err := deliverEvidenceHeartbeat(context.Background(), c, now); err == nil {
		t.Fatal("lost ACK accepted")
	}
	// Cursor and telemetry advance before the next process retries. Neither
	// may change the frozen heartbeat, even across a minute boundary.
	if err := saveCursor(c.cursorPath, cursor{Inode: 999, Offset: 12345, EvidenceEligible: 50}); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(c.evidenceOutboxPath, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(c.evidenceOutboxPath, "pending.json"), []byte("unrelated telemetry growth"), 0o600); err != nil {
		t.Fatal(err)
	}
	restarted := evidenceConfig(filepath.Dir(c.cursorPath))
	restarted.evidenceSinkURL, restarted.evidenceFrozenHeartbeat = server.URL, true
	if err := deliverEvidenceHeartbeat(context.Background(), restarted, now.Add(time.Hour)); err != nil {
		t.Fatal(err)
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("retry changed frozen payload")
	}
	if err := deliverEvidenceHeartbeat(context.Background(), restarted, now.Add(time.Hour+time.Minute)); err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(bodies[1], bodies[2]) {
		t.Fatal("new observation reused acknowledged batch")
	}
}

func TestFrozenEvidenceHeartbeatConflictRetainsOriginal(t *testing.T) {
	var bodies [][]byte
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		bodies = append(bodies, body)
		w.WriteHeader(http.StatusConflict)
		_, _ = w.Write([]byte(`{"error":"batch id conflict"}`))
	}))
	defer server.Close()
	c := evidenceConfig(t.TempDir())
	c.evidenceSinkURL, c.evidenceFrozenHeartbeat = server.URL, true
	for i := 0; i < 2; i++ {
		if err := deliverEvidenceHeartbeat(context.Background(), c, time.Unix(1_800_000_000+int64(i)*60, 0)); err == nil {
			t.Fatal("conflict ignored")
		}
	}
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("409 caused batch renaming")
	}
	state, err := loadEvidenceHeartbeatState(c.cursorPath+".evidence-heartbeat.json", c.node)
	if err != nil || state.Pending == nil {
		t.Fatalf("conflict discarded: %+v %v", state, err)
	}
}

func TestFrozenEvidenceHeartbeatCorruptOrForeignStateNeverResets(t *testing.T) {
	for _, corruption := range []string{"invalid", "oversized", "wrong_node", "changed_telemetry"} {
		t.Run(corruption, func(t *testing.T) {
			c := evidenceConfig(t.TempDir())
			c.evidenceFrozenHeartbeat = true
			path := c.cursorPath + ".evidence-heartbeat.json"
			state, err := loadEvidenceHeartbeatState(path, c.node)
			if err != nil {
				t.Fatal(err)
			}
			if err := prepareEvidenceHeartbeat(c, time.Now(), &state); err != nil {
				t.Fatal(err)
			}
			if corruption == "wrong_node" {
				state.Node = "foreign"
			}
			if corruption == "changed_telemetry" {
				state.Pending.Telemetry.OutboxBatches++
			}
			data, _ := json.Marshal(state)
			if corruption == "invalid" {
				data = []byte("broken")
			}
			if corruption == "oversized" {
				data = bytes.Repeat([]byte("x"), evidenceHeartbeatStateMaxBytes+1)
			}
			if err := os.WriteFile(path, data, 0o600); err != nil {
				t.Fatal(err)
			}
			if err := deliverEvidenceHeartbeat(context.Background(), c, time.Now()); err == nil {
				t.Fatal("invalid state silently reset")
			}
			after, _ := os.ReadFile(path)
			if !bytes.Equal(after, data) {
				t.Fatal("original state overwritten")
			}
		})
	}
}

func TestFrozenEvidenceHeartbeatRequiresDurableWriteAndSingleWriter(t *testing.T) {
	var requests atomic.Int64
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
	}))
	defer server.Close()
	c := evidenceConfig(t.TempDir())
	c.evidenceFrozenHeartbeat, c.evidenceSinkURL = true, server.URL
	path := c.cursorPath + ".evidence-heartbeat.json"
	lock, err := os.OpenFile(path+".lock", os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatal(err)
	}
	defer lock.Close()
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		t.Fatal(err)
	}
	if err := deliverEvidenceHeartbeat(context.Background(), c, time.Now()); err == nil || !strings.Contains(err.Error(), "already in use") {
		t.Fatalf("second writer admitted: %v", err)
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_UN); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(path+".tmp", 0o700); err != nil {
		t.Fatal(err)
	}
	if err := deliverEvidenceHeartbeat(context.Background(), c, time.Now()); err == nil {
		t.Fatal("failed durable write was ignored")
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("prepared state unexpectedly persisted: %v", err)
	}
	if requests.Load() != 0 {
		t.Fatal("heartbeat was sent before state was durable or while another writer held the lock")
	}
}

func TestLoadConfigFrozenHeartbeatIsExplicitAndPilotOnly(t *testing.T) {
	t.Setenv("NGINXCOLLECTOR_NODE", "ecs-canary")
	t.Setenv("NGINXCOLLECTOR_TOKEN", "test-token")
	t.Setenv("NGINXCOLLECTOR_SINK_URL", "https://monitor.example/internal/nginx")
	t.Setenv("NGINXCOLLECTOR_EVIDENCE_MODE", "pilot")
	t.Setenv("NGINXCOLLECTOR_EVIDENCE_SINK_URL", "https://monitor.example/internal/nginx/evidence")
	t.Setenv("NGINXCOLLECTOR_EVIDENCE_HMAC_KEY", strings.Repeat("k", 32))
	t.Setenv("NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID", "test-key")
	t.Setenv("NGINXCOLLECTOR_CURSOR_PATH", filepath.Join(t.TempDir(), "cursor.json"))
	for _, tc := range []struct {
		flag, mode             string
		wantEnabled, wantError bool
	}{
		{"", "pilot", false, false},
		{"false", "pilot", false, false},
		{"true", "pilot", true, false},
		{"true", "verified", false, true},
		{"true", "off", false, true},
		{"typo", "pilot", false, true},
	} {
		t.Run(tc.flag+"/"+tc.mode, func(t *testing.T) {
			t.Setenv("NGINXCOLLECTOR_EVIDENCE_FROZEN_HEARTBEAT", tc.flag)
			t.Setenv("NGINXCOLLECTOR_EVIDENCE_MODE", tc.mode)
			cfg, err := loadConfig()
			if (err != nil) != tc.wantError || err == nil && cfg.evidenceFrozenHeartbeat != tc.wantEnabled {
				t.Fatalf("enabled=%v error=%v", cfg.evidenceFrozenHeartbeat, err)
			}
		})
	}
	t.Setenv("NGINXCOLLECTOR_EVIDENCE_FROZEN_HEARTBEAT", "true")
	t.Setenv("NGINXCOLLECTOR_CURSOR_PATH", "relative/cursor.json")
	if _, err := loadConfig(); err == nil {
		t.Fatal("relative state path admitted")
	}
}

func TestFrozenEvidenceHeartbeatDefaultKeepsLegacyWire(t *testing.T) {
	c := evidenceConfig(t.TempDir())
	now := time.Unix(1_800_000_000, 0)
	want, err := evidenceHeartbeatBatch(c, now, cursor{})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		got := heartbeatACK(t, w, body)
		if !reflect.DeepEqual(got, want) {
			t.Fatal("legacy heartbeat changed")
		}
	}))
	defer server.Close()
	c.evidenceSinkURL = server.URL
	if err := deliverEvidenceHeartbeat(context.Background(), c, now); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(c.cursorPath + ".evidence-heartbeat.json"); !os.IsNotExist(err) {
		t.Fatal("legacy mode created new state")
	}
}
