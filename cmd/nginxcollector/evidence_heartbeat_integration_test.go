package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	monitorpkg "github.com/yl0711-coder/newapi-monitor/monitor"
)

// This intentionally keeps the legacy node authorization. It tests heartbeat
// idempotency, not authenticated ECS task identity or source cursor isolation.
func TestFrozenHeartbeatRealMonitorConflictRetryAndRestart(t *testing.T) {
	gin.SetMode(gin.TestMode)
	dir := t.TempDir()
	settings := monitorpkg.Settings{
		StorePath: filepath.Join(dir, "monitor.db"), UsageFactsStorePath: filepath.Join(dir, "facts.db"),
		StoreBackupDir: filepath.Join(dir, "backups"), StoreMigrationBackupRetention: 3,
		LocalSnapshotOnly: true, NginxEnabled: true, NginxAllowedNodes: []string{"ecs-canary"},
		IngestToken: "test-token", NginxEvidenceMode: "pilot",
		NginxEvidenceStorePath: filepath.Join(dir, "evidence.db"), NginxEvidenceRetentionHours: 168,
		NginxEvidenceHMACKey: strings.Repeat("k", 32), NginxEvidenceHMACKeyID: "key-1", NginxEvidenceMaxMiB: 64,
	}
	var loseACK atomic.Bool
	startMonitor := func() (*monitorpkg.Monitor, *httptest.Server) {
		t.Helper()
		m, err := monitorpkg.New(settings)
		if err != nil {
			t.Fatal(err)
		}
		router := gin.New()
		m.RegisterRoutes(router)
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if loseACK.CompareAndSwap(true, false) {
				committed := httptest.NewRecorder()
				router.ServeHTTP(committed, r)
				if committed.Code != http.StatusOK {
					t.Errorf("commit before lost ACK failed: %d %s", committed.Code, committed.Body.String())
				}
				w.WriteHeader(http.StatusServiceUnavailable)
				return
			}
			router.ServeHTTP(w, r)
		}))
		return m, server
	}
	m, server := startMonitor()
	defer func() { server.Close(); m.Close() }()
	configure := func() config {
		c := evidenceConfig(t.TempDir())
		c.node, c.token, c.allowHTTP = "ecs-canary", "test-token", true
		c.evidenceSinkURL = server.URL + "/internal/nginx-evidence/v1"
		c.evidenceFrozenHeartbeat = true
		return c
	}
	assertBatchCount := func(want int) {
		t.Helper()
		db, err := sql.Open("sqlite", "file:"+settings.NginxEvidenceStorePath+"?mode=ro")
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		var got int
		if err := db.QueryRow("SELECT COUNT(*) FROM nginx_evidence_ingest_batches").Scan(&got); err != nil || got != want {
			t.Fatalf("batch count=%d want=%d error=%v", got, want, err)
		}
		if err := db.QueryRow("SELECT COUNT(*) FROM nginx_request_evidences").Scan(&got); err != nil || got != 0 {
			t.Fatalf("heartbeat must not create request facts: count=%d error=%v", got, err)
		}
	}
	ctx, now := context.Background(), time.Now()
	c := configure()
	oldFirst, err := evidenceHeartbeatBatch(c, now, cursor{Inode: 11, Offset: 100})
	if err != nil {
		t.Fatal(err)
	}
	oldSecond, err := evidenceHeartbeatBatch(c, now, cursor{Inode: 22, Offset: 200})
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := postEvidence(ctx, c, oldFirst); err != nil {
		t.Fatal(err)
	}
	if _, _, err := postEvidence(ctx, c, oldSecond); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("legacy same-minute conflict not reproduced: %v", err)
	}
	assertBatchCount(1)

	results := make(chan error, 5)
	for i := 0; i < 5; i++ {
		worker := configure()
		if err := saveCursor(worker.cursorPath, cursor{Inode: uint64(100 + i), Offset: int64(200 + i)}); err != nil {
			t.Fatal(err)
		}
		go func() { results <- deliverEvidenceHeartbeat(ctx, worker, now) }()
	}
	for i := 0; i < 5; i++ {
		if err := <-results; err != nil {
			t.Fatalf("concurrent heartbeat: %v", err)
		}
	}
	assertBatchCount(6)

	loseACK.Store(true)
	if err := deliverEvidenceHeartbeat(ctx, c, now); err == nil {
		t.Fatal("lost ACK unexpectedly succeeded")
	}
	assertBatchCount(7) // Receiver really committed the batch before returning 503.
	state, err := loadEvidenceHeartbeatState(c.cursorPath+".evidence-heartbeat.json", c.node)
	if err != nil || state.Pending == nil {
		t.Fatalf("unacknowledged batch not retained: %v", err)
	}
	server.Close()
	m.Close()
	m, server = startMonitor()
	c.evidenceSinkURL = server.URL + "/internal/nginx-evidence/v1"
	if err := deliverEvidenceHeartbeat(ctx, c, now.Add(time.Minute)); err != nil {
		t.Fatalf("retry after both sides restart: %v", err)
	}
	assertBatchCount(7)

	changed := *state.Pending
	changed.Source.EvidenceEligible++
	changed.PayloadHash = evidencePayloadHash(changed)
	if _, _, err := postEvidence(ctx, c, changed); err == nil || !strings.Contains(err.Error(), "409") {
		t.Fatalf("real conflicting body was accepted: %v", err)
	}
	unauthorized := *state.Pending
	unauthorized.Node = "unregistered-task"
	unauthorized.PayloadHash = evidencePayloadHash(unauthorized)
	if _, _, err := postEvidence(ctx, c, unauthorized); err == nil || !strings.Contains(err.Error(), "400") {
		t.Fatalf("unregistered source was accepted: %v", err)
	}
	c.token = "invalid-token"
	if _, _, err := postEvidence(ctx, c, *state.Pending); err == nil || !strings.Contains(err.Error(), "401") {
		t.Fatalf("unauthorized token was accepted: %v", err)
	}
	assertBatchCount(7)
}
