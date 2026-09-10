package monitor

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func rejectV2Body(t *testing.T, node string, now int64) string {
	t.Helper()
	b, err := json.Marshal(map[string]any{"node": node, "batch_id": "reject-v2-test-0001", "created_at": now, "samples": []map[string]any{{"bucket_ts": now / 60 * 60, "reason": "no_available_channel", "model": "fixture", "group": "fixture-group", "count": 1}}})
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func postRejectV2(m *Monitor, body string, auth bool) *httptest.ResponseRecorder {
	r := gin.New()
	r.POST("/internal/rejections/v2", m.ingestRejectionsV2)
	req := httptest.NewRequest(http.MethodPost, "/internal/rejections/v2", strings.NewReader(body))
	if auth {
		req.Header.Set("Authorization", "Bearer "+m.cfg.IngestToken)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestRejectV2FullACKIsolationAndConflict(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.IngestToken = "fixture"
	now := time.Now().Unix()
	body := rejectV2Body(t, "task-a", now)
	if w := postRejectV2(m, body, false); w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthorized: %d", w.Code)
	}
	for i := 0; i < 2; i++ {
		w := postRejectV2(m, body, true)
		var ack struct {
			OK        bool   `json:"ok"`
			Version   int    `json:"version"`
			Node      string `json:"node"`
			BatchID   string `json:"batch_id"`
			Hash      string `json:"payload_hash"`
			Accepted  int    `json:"accepted"`
			Rejected  int    `json:"rejected"`
			Duplicate bool   `json:"duplicate"`
		}
		sum := sha256.Sum256([]byte(body))
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &ack) != nil || !ack.OK || ack.Version != 2 || ack.Node != "task-a" || ack.BatchID != "reject-v2-test-0001" || ack.Hash != hex.EncodeToString(sum[:]) || ack.Accepted != 1 || ack.Rejected != 0 || ack.Duplicate != (i == 1) {
			t.Fatalf("ACK: %d %s", w.Code, w.Body.String())
		}
	}
	// Even wire-only mutations violate a frozen v2 batch identity.
	if w := postRejectV2(m, body+" ", true); w.Code != 409 {
		t.Fatalf("wire conflict: %d %s", w.Code, w.Body.String())
	}
	if w := postRejectV2(m, rejectV2Body(t, "task-b", now), true); w.Code != 200 {
		t.Fatalf("independent source: %d", w.Code)
	}
	rows := m.storeRejections(now - 60)
	if len(rows) != 1 || rows[0].Count != 2 {
		t.Fatalf("duplicate or cross-source collision: %+v", rows)
	}
}

func TestRejectV2RejectsPartialInvalidOversizeAndExpired(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.IngestToken = "fixture"
	now := time.Now().Unix()
	body := rejectV2Body(t, "task-a", now)
	for name, bad := range map[string]string{
		"invalid-row":   strings.Replace(body, `"count":1`, `"count":0`, 1),
		"clip-model":    strings.Replace(body, `"model":"fixture"`, `"model":"`+strings.Repeat("m", 129)+`"`, 1),
		"invalid-node":  rejectV2Body(t, "task/a", now),
		"expired":       rejectV2Body(t, "task-a", now-rejectionV2ReplaySeconds-1),
		"future":        rejectV2Body(t, "task-a", now+300),
		"trailing-json": body + `{}`,
		"unknown-field": strings.Replace(body, `"node":`, `"extra":true,"node":`, 1),
	} {
		t.Run(name, func(t *testing.T) {
			if w := postRejectV2(m, bad, true); w.Code != 400 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	if w := postRejectV2(m, strings.Repeat("x", rejectionV2MaxBody+1), true); w.Code != 413 {
		t.Fatalf("size: %d", w.Code)
	}
	if rows := m.storeRejections(now - 60); len(rows) != 0 {
		t.Fatal("invalid batches partially committed")
	}
}

func TestRejectV2ReceiptOutlivesReplayWindow(t *testing.T) {
	m := newTestMonitor(t)
	now := time.Now().Unix()
	for _, age := range []int64{86400 + 60, 3 * 86400} {
		if _, err := m.ingestRejectionBatch("fixture", strings.Repeat("a", 8)+time.Unix(age, 0).Format("150405"), []RejectionSample{{Node: "fixture", BucketTs: now - age, Reason: "r", Model: "m", Count: 1}}, now-age); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := m.pruneRejectionsOlderThan(now - 86400); err != nil {
		t.Fatal(err)
	}
	var receipts []RejectionIngestBatch
	if err := m.storeDB.Find(&receipts).Error; err != nil {
		t.Fatal(err)
	}
	if len(receipts) != 1 || receipts[0].ReceivedAt != now-86400-60 {
		t.Fatalf("receipt retention shorter than replay: %+v", receipts)
	}
}
