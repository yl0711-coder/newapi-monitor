package monitor

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newECSLogTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	t.Cleanup(m.Close)
	enableTestNginxEvidence(t, m)
	m.cfg.NginxErrorEnabled = true
	m.cfg.ECSLogEnabled, m.cfg.ECSLogScope = true, "isolated"
	m.cfg.ECSLogAudience = "fixture-local-monitor"
	m.cfg.ECSLogBridgeToken = strings.Repeat("b", 32)
	m.ecsLogPolicies = []ECSLogPolicy{testECSLogPolicy()}
	m.ecsLogVerify = func(ctx context.Context, in ecsLogRegistration, p ECSLogPolicy) (ECSLogSource, error) {
		return verifyECSLogTask(ctx, validECSLogTaskFixture(in), in, p)
	}
	return m
}

func ecsTestKey(t *testing.T) (string, ed25519.PrivateKey) {
	t.Helper()
	pub, key, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	return base64.StdEncoding.EncodeToString(pub), key
}

func postECSRegistration(t *testing.T, m *Monitor, in ecsLogRegistration, token string) *httptest.ResponseRecorder {
	t.Helper()
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.POST("/internal/ecs/v1/register", m.registerECSLogHTTP)
	req := httptest.NewRequest(http.MethodPost, "/internal/ecs/v1/register", bytes.NewReader(b))
	req.Header.Set("Authorization", "Bearer "+token)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func registerECSTestSource(t *testing.T, m *Monitor, taskID, lane string) (ECSLogSource, ed25519.PrivateKey) {
	t.Helper()
	pub, key := ecsTestKey(t)
	in := testECSRegistration(taskID, pub)
	in.Lane = lane
	w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken)
	if w.Code != 200 {
		t.Fatalf("register: %d %s", w.Code, w.Body.String())
	}
	var source ECSLogSource
	if err := m.storeDB.First(&source, "node = ? AND lane = ?", ecsLogNode(in.TaskARN, in.Container, in.RuntimeID), lane).Error; err != nil {
		t.Fatal(err)
	}
	return source, key
}

func postSignedECS(m *Monitor, source ECSLogSource, key ed25519.PrivateKey, body []byte, mutate func(*http.Request)) *httptest.ResponseRecorder {
	return postSignedECSPath(m, source, key, "/internal/ecs/v1/ingest/"+source.Lane, body, mutate)
}

func postSignedECSPath(m *Monitor, source ECSLogSource, key ed25519.PrivateKey, path string, body []byte, mutate func(*http.Request)) *httptest.ResponseRecorder {
	r := gin.New()
	r.POST("/internal/ecs/v1/ingest/:lane", m.ingestECSLogHTTP)
	r.POST("/internal/ecs/v1/heartbeat/:lane", m.heartbeatECSLogHTTP)
	req := httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body))
	at := strconv.FormatInt(time.Now().Unix(), 10)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Monitor-ECS-Node", source.Node)
	req.Header.Set("X-Monitor-ECS-Time", at)
	req.Header.Set("X-Monitor-ECS-Signature", base64.StdEncoding.EncodeToString(ed25519.Sign(key, ecsLogSigningMessage(m.cfg.ECSLogAudience, source.Node, source.Lane, path, at, body))))
	if mutate != nil {
		mutate(req)
	}
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestECSLogRegistrationTrustAndLeaseOwnership(t *testing.T) {
	m := newECSLogTestMonitor(t)
	pub, _ := ecsTestKey(t)
	in := testECSRegistration(strings.Repeat("a", 32), pub)
	for _, token := range []string{"", m.cfg.IngestToken, "wrong"} {
		if w := postECSRegistration(t, m, in, token); w.Code != 401 {
			t.Fatalf("untrusted bridge: %d", w.Code)
		}
	}
	for i := 0; i < 2; i++ {
		if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 200 {
			t.Fatalf("renew: %d %s", w.Code, w.Body.String())
		}
	}
	in.PublicKey, _ = ecsTestKey(t)
	if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 409 {
		t.Fatalf("second active owner: %d", w.Code)
	}
	var source ECSLogSource
	if err := m.storeDB.First(&source).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&source).Update("lease_until", time.Now().Unix()-1).Error; err != nil {
		t.Fatal(err)
	}
	if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 409 {
		t.Fatalf("expired lease must not silently replace the archive verification key: %d", w.Code)
	}
	in.PublicKey = pub
	if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 200 {
		t.Fatalf("original owner cannot renew after expiry: %d", w.Code)
	}
	var renewed ECSLogSource
	if err := m.storeDB.First(&renewed, "node = ? AND lane = ?", source.Node, source.Lane).Error; err != nil || renewed.PublicKey != pub {
		t.Fatalf("source verification identity changed: %v", err)
	}
	for _, field := range []string{"revoked", "stopped_at"} {
		values := map[string]any{"revoked": false, "stopped_at": 0}
		if field == "revoked" {
			values[field] = true
		} else {
			values[field] = time.Now().Unix()
		}
		if err := m.storeDB.Model(&source).Updates(values).Error; err != nil {
			t.Fatal(err)
		}
		if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 409 {
			t.Fatalf("retired source renewal: %d", w.Code)
		}
	}
	in.CallerARN += "forged"
	if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 403 {
		t.Fatalf("forged caller: %d", w.Code)
	}
}

func ecsLaneTestBody(t *testing.T, node, lane string, count int64) []byte {
	t.Helper()
	now := time.Now().Unix()
	var in any
	switch lane {
	case "access":
		in = nginxIngestRequest{Node: node, BatchID: "shared_batch_abcdefgh", Samples: []nginxIngestSample{{BucketTs: now / 60 * 60, Route: "/api/status", Method: "GET", Status: 200, Count: count}}}
	case "error":
		in = nginxErrorIngestRequest{Node: node, BatchID: "shared_batch_abcdefgh", Samples: []nginxErrorIngestSample{{BucketTs: now / 60 * 60, Category: "upstream_timeout", Severity: "error", Count: count}}}
	case "evidence":
		batch := validEvidenceBatch()
		batch.Node, batch.BatchID = node, "shared_batch_abcdefgh"
		sum := sha256.Sum256([]byte(node + ":fixture-event"))
		batch.Events[0].EventID = hex.EncodeToString(sum[:])
		batch.Events[0].BytesSent = count
		batch.PayloadHash = nginxEvidenceHash(batch)
		in = batch
	case "reject":
		return []byte(strings.Replace(rejectV2Body(t, node, now), `"count":1`, `"count":`+strconv.FormatInt(count, 10), 1))
	default:
		t.Fatal("invalid test lane")
	}
	b, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func TestECSLogFourLanesMultiTaskRetryAndConflict(t *testing.T) {
	m := newECSLogTestMonitor(t)
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		t.Run(lane, func(t *testing.T) {
			for _, task := range []string{strings.Repeat("a", 32), strings.Repeat("b", 32)} {
				source, key := registerECSTestSource(t, m, task, lane)
				body := ecsLaneTestBody(t, source.Node, lane, 1)
				for retry := 0; retry < 2; retry++ {
					if w := postSignedECS(m, source, key, body, nil); w.Code != 200 {
						t.Fatalf("ingest retry %d: %d %s", retry, w.Code, w.Body.String())
					}
				}
				if w := postSignedECS(m, source, key, ecsLaneTestBody(t, source.Node, lane, 2), nil); w.Code != 409 {
					t.Fatalf("real conflict masked: %d %s", w.Code, w.Body.String())
				}
			}
		})
	}
	for _, model := range []any{&NginxIngestBatch{}, &NginxErrorIngestBatch{}, &RejectionIngestBatch{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 2 {
			t.Fatalf("main receipt isolation %T: %d %v", model, count, err)
		}
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxEvidenceIngestBatch{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("evidence receipts: %d %v", count, err)
	}
	if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("evidence facts: %d %v", count, err)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 2 {
		t.Fatalf("reject facts duplicated: %+v", rows)
	}
}

func TestECSLogSignatureFailClosed(t *testing.T) {
	m := newECSLogTestMonitor(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	body := ecsLaneTestBody(t, source.Node, "reject", 1)
	for name, mutate := range map[string]func(*http.Request){
		"signature":     func(r *http.Request) { r.Header.Set("X-Monitor-ECS-Signature", "wrong") },
		"time":          func(r *http.Request) { r.Header.Set("X-Monitor-ECS-Time", "0") },
		"overflow-time": func(r *http.Request) { r.Header.Set("X-Monitor-ECS-Time", "9223372036854775807") },
		"node":          func(r *http.Request) { r.Header.Set("X-Monitor-ECS-Node", "ecs-"+strings.Repeat("f", 48)) },
		"path-lane":     func(r *http.Request) { r.URL.Path = "/internal/ecs/v1/ingest/access" },
		"query":         func(r *http.Request) { r.URL.RawQuery = "x=1" },
	} {
		t.Run(name, func(t *testing.T) {
			if w := postSignedECS(m, source, key, body, mutate); w.Code != 401 {
				t.Fatalf("%d %s", w.Code, w.Body.String())
			}
		})
	}
	_, wrongKey := ecsTestKey(t)
	if w := postSignedECS(m, source, wrongKey, body, nil); w.Code != 401 {
		t.Fatalf("wrong owner: %d", w.Code)
	}
	if w := postSignedECS(m, source, key, ecsLaneTestBody(t, "forged", "reject", 1), nil); w.Code != 400 {
		t.Fatalf("payload identity: %d", w.Code)
	}
	if w := postRejectV2(m, string(body), true); w.Code != 403 {
		t.Fatalf("shared token bypass: %d", w.Code)
	}
	// Isolated receivers now reject legacy transport at authorization, before
	// parsing the node (403 rather than the old node-validation 400).
	if w := postNginx(t, m, string(ecsLaneTestBody(t, source.Node, "access", 1)), m.cfg.IngestToken); w.Code != 403 {
		t.Fatalf("legacy nginx bypass: %d", w.Code)
	}
	if err := m.storeDB.Model(&source).Update("lease_until", time.Now().Unix()-1).Error; err != nil {
		t.Fatal(err)
	}
	if w := postSignedECS(m, source, key, body, nil); w.Code != 401 {
		t.Fatalf("expired lease: %d", w.Code)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 0 {
		t.Fatal("rejected requests stored facts")
	}
}

func TestECSLogAWSFailureDoesNotRevokeSources(t *testing.T) {
	m := newECSLogTestMonitor(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	m.ecsLogVerify = func(context.Context, ecsLogRegistration, ECSLogPolicy) (ECSLogSource, error) {
		return ECSLogSource{}, errors.New("fixture AWS outage")
	}
	in := testECSRegistration(strings.Repeat("a", 32), source.PublicKey)
	if w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken); w.Code != 503 {
		t.Fatalf("AWS unavailable: %d", w.Code)
	}
	if w := postSignedECS(m, source, key, ecsLaneTestBody(t, source.Node, "reject", 1), nil); w.Code != 200 {
		t.Fatalf("existing lease invalidated: %d", w.Code)
	}
	if err := m.storeDB.Migrator().DropTable(&ECSLogSource{}); err != nil {
		t.Fatal(err)
	}
	if w := postSignedECS(m, source, key, ecsLaneTestBody(t, source.Node, "reject", 1), nil); w.Code != 503 {
		t.Fatalf("storage failure misclassified: %d", w.Code)
	}
}
