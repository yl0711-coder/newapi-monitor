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
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestECSArchivedEvidenceRetryKeepsWholeWireBody(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "eack-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	c := evidenceConfig(dir)
	c.ecsSocket, c.evidenceSinkURL, c.token = filepath.Join(dir, "agent.sock"), "https://monitor.example/internal/nginx-evidence/v1", "fixture"
	l, err := net.Listen("unix", c.ecsSocket)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(c.ecsSocket, 0600); err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	received := make(chan []byte, 2)
	s := &http.Server{ReadHeaderTimeout: time.Second, Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		raw, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			return
		}
		if r.Header.Get("X-Monitor-Archive-Ack") != "1" {
			t.Error("capability missing")
		}
		received <- raw
		if calls.Add(1) == 1 {
			w.WriteHeader(503)
			return
		}
		var p evidenceBatch
		if err := json.Unmarshal(raw, &p); err != nil {
			t.Error(err)
			return
		}
		sum := sha256.Sum256(raw)
		w.WriteHeader(202)
		_ = json.NewEncoder(w).Encode(map[string]any{"version": 1, "state": "durably_archived", "node": p.Node, "lane": "evidence", "batch_id": p.BatchID, "body_sha256": hex.EncodeToString(sum[:])})
	})}
	go func() { _ = s.Serve(l) }()
	defer s.Close()
	p := evidenceBatch{SchemaVersion: evidenceSchemaVersion, Node: c.node, BatchID: "immutable_archive_abcdefgh", LogSchema: 2, HMACKeyID: "key-1", Source: evidenceSourceRange{Kind: "access", FileID: "42", EndOffset: 100}, Events: []evidenceEvent{{EventID: strings.Repeat("a", 64)}}}
	p.PayloadHash = evidencePayloadHash(p)
	if err := spoolEvidence(c, p); err != nil {
		t.Fatal(err)
	}
	if err := drainEvidenceOnce(context.Background(), c); err == nil {
		t.Fatal("lost ACK must preserve queue")
	}
	// Change operational telemetry between attempts; it must not mutate the
	// already frozen batch or its archive hash.
	if err := recordEvidenceGap(c, p, 1); err != nil {
		t.Fatal(err)
	}
	if err := drainEvidenceOnce(context.Background(), c); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(<-received, <-received) {
		t.Fatal("retry mutated archived wire body")
	}
	paths, err := listEvidenceOutbox(c.evidenceOutboxPath)
	if err != nil || len(paths) != 0 {
		t.Fatal("durable receipt did not transfer queue ownership", err)
	}
	gap, err := loadGapState(c.evidenceOutboxPath)
	if err != nil || gap.GapCount != 1 {
		t.Fatal("durable receipt invented rejection", err)
	}
}
