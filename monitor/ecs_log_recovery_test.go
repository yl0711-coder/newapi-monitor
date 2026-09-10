package monitor

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestECSLogRegistryAndReceiptSurviveReopen(t *testing.T) {
	m := newECSLogTestMonitor(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	body := ecsLaneTestBody(t, source.Node, "reject", 1)
	if w := postSignedECS(m, source, key, body, nil); w.Code != 200 {
		t.Fatalf("initial: %d %s", w.Code, w.Body.String())
	}
	// Discover the actual temporary store path without using an external DB.
	var files []struct {
		Name string
		File string
	}
	if err := m.storeDB.Raw("PRAGMA database_list").Scan(&files).Error; err != nil {
		t.Fatal(err)
	}
	var path string
	for _, file := range files {
		if file.Name == "main" {
			path = file.File
		}
	}
	if !filepath.IsAbs(path) {
		t.Fatal("missing private temporary DB path")
	}
	m.Close()
	reopened := &Monitor{cfg: m.cfg, ecsLogPolicies: m.ecsLogPolicies}
	if err := reopened.openStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(reopened.Close)
	for retry := 0; retry < 2; retry++ {
		w := postSignedECS(reopened, source, key, body, nil)
		if w.Code != 200 || !strings.Contains(w.Body.String(), `"duplicate":true`) {
			t.Fatalf("reopen replay: %d %s", w.Code, w.Body.String())
		}
	}
	if rows := reopened.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 1 {
		t.Fatalf("reopen duplicated facts: %+v", rows)
	}
}

func TestECSLogQuietRejectHeartbeatDoesNotInventFacts(t *testing.T) {
	m := newECSLogTestMonitor(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	body := []byte(`{"node":"` + source.Node + `"}`)
	path := "/internal/ecs/v1/heartbeat/reject"
	for retry := 0; retry < 3; retry++ {
		if w := postSignedECSPath(m, source, key, path, body, nil); w.Code != 200 {
			t.Fatalf("heartbeat %d %s", w.Code, w.Body.String())
		}
	}
	var after ECSLogSource
	if err := m.storeDB.First(&after, "node = ? AND lane = ?", source.Node, source.Lane).Error; err != nil {
		t.Fatal(err)
	}
	if after.LastHeartbeat == 0 || after.LastReport != 0 || ecsLogSourceStatus(after, time.Now().Unix()) != "heartbeating_no_log_batches" {
		t.Fatalf("heartbeat confused with delivery: %+v", after)
	}
	for _, model := range []any{&RejectionIngestBatch{}, &RejectionSample{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("heartbeat fabricated facts/receipt %T %d %v", model, count, err)
		}
	}
	if w := postSignedECSPath(m, source, key, path, body, func(r *http.Request) { r.URL.Path = "/internal/ecs/v1/ingest/reject" }); w.Code != 401 {
		t.Fatalf("heartbeat signature usable for ingestion: %d", w.Code)
	}
}

func TestECSLogIngestResourceBudgetAndRevocation(t *testing.T) {
	m := newECSLogTestMonitor(t)
	source, key := registerECSTestSource(t, m, strings.Repeat("a", 32), "reject")
	body := ecsLaneTestBody(t, source.Node, "reject", 1)
	m.ecsLogRequests.Store(ecsLogMaxInflight)
	if w := postSignedECS(m, source, key, body, nil); w.Code != 429 {
		t.Fatalf("unbounded ingestion: %d", w.Code)
	}
	if m.ecsLogRequests.Load() != ecsLogMaxInflight {
		t.Fatal("inflight budget leaked")
	}
	m.ecsLogRequests.Store(0)
	if err := m.storeDB.Model(&source).Update("revoked", true).Error; err != nil {
		t.Fatal(err)
	}
	if w := postSignedECS(m, source, key, body, nil); w.Code != 401 {
		t.Fatalf("revoked signature accepted: %d", w.Code)
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 0 {
		t.Fatal("rejected batch stored")
	}
}

func TestECSLogConcurrentTasksAndSignatureBinding(t *testing.T) {
	m := newECSLogTestMonitor(t)
	type sender struct {
		source ECSLogSource
		send   func() int
	}
	senders := make([]sender, 0, 5)
	for _, id := range []string{"a", "b", "c", "d", "e"} {
		source, key := registerECSTestSource(t, m, strings.Repeat(id, 32), "reject")
		body := ecsLaneTestBody(t, source.Node, "reject", 1)
		senders = append(senders, sender{source, func() int { return postSignedECS(m, source, key, body, nil).Code }})
	}
	results := make(chan int, 15)
	var wg sync.WaitGroup
	for _, s := range senders {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for retry := 0; retry < 3; retry++ {
				results <- s.send()
			}
		}()
	}
	wg.Wait()
	close(results)
	for code := range results {
		if code != 200 {
			t.Fatalf("concurrent request=%d", code)
		}
	}
	if rows := m.storeRejections(time.Now().Unix() - 120); len(rows) != 1 || rows[0].Count != 5 {
		t.Fatalf("concurrent duplicate/loss: %+v", rows)
	}
	source, key := registerECSTestSource(t, m, strings.Repeat("f", 32), "reject")
	body := ecsLaneTestBody(t, source.Node, "reject", 1)
	if w := postSignedECS(m, source, key, body, func(r *http.Request) { r.Body = io.NopCloser(strings.NewReader(string(body) + " ")) }); w.Code != 401 {
		t.Fatalf("unsigned wire mutation: %d", w.Code)
	}
	if w := postSignedECS(m, source, key, body, func(r *http.Request) {
		message := ecsLogSigningMessage("fixture-other-monitor", source.Node, source.Lane, r.URL.EscapedPath(), r.Header.Get("X-Monitor-ECS-Time"), body)
		r.Header.Set("X-Monitor-ECS-Signature", base64.StdEncoding.EncodeToString(ed25519.Sign(key, message)))
	}); w.Code != 401 {
		t.Fatalf("cross-deployment replay: %d", w.Code)
	}
}

func TestECSLogEvidenceCrossTaskEventCollisionRollsBack(t *testing.T) {
	m := newECSLogTestMonitor(t)
	a, keyA := registerECSTestSource(t, m, strings.Repeat("a", 32), "evidence")
	b, keyB := registerECSTestSource(t, m, strings.Repeat("b", 32), "evidence")
	first := ecsLaneTestBody(t, a.Node, "evidence", 1)
	if w := postSignedECS(m, a, keyA, first, nil); w.Code != 200 {
		t.Fatalf("first evidence=%d", w.Code)
	}
	var batch nginxEvidenceBatch
	if err := json.Unmarshal(first, &batch); err != nil {
		t.Fatal(err)
	}
	batch.Node = b.Node
	batch.PayloadHash = nginxEvidenceHash(batch)
	body, err := json.Marshal(batch)
	if err != nil {
		t.Fatal(err)
	}
	if w := postSignedECS(m, b, keyB, body, nil); w.Code != 409 {
		t.Fatalf("cross-task event silently discarded: %d %s", w.Code, w.Body.String())
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxEvidenceIngestBatch{}).Where("node = ?", b.Node).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("conflicting batch receipt committed: %d %v", count, err)
	}
	if err := m.nginxEvidenceDB.Model(&NginxEvidenceSourceState{}).Where("node = ?", b.Node).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("conflict advanced cursor: %d %v", count, err)
	}
}

func TestECSLogRegistrationVerificationConcurrencyBound(t *testing.T) {
	m := newECSLogTestMonitor(t)
	entered := make(chan struct{}, 4)
	release := make(chan struct{})
	m.ecsLogVerify = func(ctx context.Context, in ecsLogRegistration, p ECSLogPolicy) (ECSLogSource, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return verifyECSLogTask(ctx, validECSLogTaskFixture(in), in, p)
		case <-ctx.Done():
			return ECSLogSource{}, ctx.Err()
		}
	}
	pub, _ := ecsTestKey(t)
	in := testECSRegistration(strings.Repeat("a", 32), pub)
	var wg sync.WaitGroup
	results := make(chan int, 4)
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() { defer wg.Done(); results <- postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken).Code }()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(3 * time.Second):
			close(release)
			wg.Wait()
			t.Fatal("verification did not enter")
		}
	}
	w := postECSRegistration(t, m, in, m.cfg.ECSLogBridgeToken)
	close(release)
	wg.Wait()
	close(results)
	if w.Code != 429 {
		t.Fatalf("unbounded AWS verification: %d", w.Code)
	}
	for code := range results {
		if code != 200 {
			t.Fatalf("bounded verification: %d", code)
		}
	}
}
