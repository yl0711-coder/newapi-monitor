package main

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"
)

func TestFrozenHeartbeatACKPersistenceFailureRetriesExactBatch(t *testing.T) {
	c := evidenceConfig(t.TempDir())
	c.evidenceFrozenHeartbeat = true
	statePath := c.cursorPath + ".evidence-heartbeat.json"
	bodies := make(chan []byte, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		bodies <- body
		// The pending record already exists. Fail only the post-ACK local write.
		if err := os.Mkdir(statePath+".tmp", 0o700); err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		heartbeatACK(t, w, body)
	}))
	defer server.Close()
	c.evidenceSinkURL = server.URL
	for i := 0; i < 2; i++ {
		if err := deliverEvidenceHeartbeat(context.Background(), c, time.Now().Add(time.Duration(i)*time.Minute)); err == nil {
			t.Fatal("post-ACK persistence failure was hidden")
		}
		state, err := loadEvidenceHeartbeatState(statePath, c.node)
		if err != nil || state.Pending == nil || state.Next != 2 {
			t.Fatalf("acknowledged but uncommitted local state was lost: next=%d error=%v", state.Next, err)
		}
		if err := os.Remove(statePath + ".tmp"); err != nil {
			t.Fatal(err)
		}
	}
	if len(bodies) != 2 {
		t.Fatalf("expected two completed deliveries, got %d", len(bodies))
	}
	if !bytes.Equal(<-bodies, <-bodies) {
		t.Fatal("post-ACK persistence error caused new ID/content on retry")
	}
}

func TestFrozenHeartbeatInvalidACKKeepsPending(t *testing.T) {
	for _, bad := range []string{"batch_id", "hash", "schema", "count", "not_ok"} {
		t.Run(bad, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				var payload evidenceBatch
				if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
					t.Error(err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				ack := evidenceAck{OK: true, SchemaVersion: evidenceSchemaVersion, BatchID: payload.BatchID, PayloadHash: payload.PayloadHash}
				switch bad {
				case "batch_id":
					ack.BatchID = "other-batch"
				case "hash":
					ack.PayloadHash = "other-hash"
				case "schema":
					ack.SchemaVersion++
				case "count":
					ack.Accepted = 1
				case "not_ok":
					ack.OK = false
				}
				if err := json.NewEncoder(w).Encode(ack); err != nil {
					t.Error(err)
				}
			}))
			defer server.Close()
			c := evidenceConfig(t.TempDir())
			c.evidenceSinkURL, c.evidenceFrozenHeartbeat = server.URL, true
			if err := deliverEvidenceHeartbeat(context.Background(), c, time.Now()); err == nil {
				t.Fatal("invalid ACK accepted")
			}
			state, err := loadEvidenceHeartbeatState(c.cursorPath+".evidence-heartbeat.json", c.node)
			if err != nil || state.Pending == nil {
				t.Fatalf("invalid ACK discarded pending batch: %v", err)
			}
		})
	}
}
