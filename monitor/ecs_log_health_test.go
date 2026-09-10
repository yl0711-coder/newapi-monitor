package monitor

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestECSLogHealthSeparatesLiveAndStoppedDelivery(t *testing.T) {
	m := newECSLogTestMonitor(t)
	now := time.Now().Unix()
	policy := testECSLogPolicy()
	sources := []ECSLogSource{}
	for _, task := range []string{"one", "two", "three"} {
		for _, lane := range []string{"access", "error", "evidence", "reject"} {
			sources = append(sources, ECSLogSource{Node: task, Lane: lane, ServiceARN: policy.ServiceARN, TaskARN: task, LeaseUntil: now + 600, FirstSeen: now - 300, LastHeartbeat: now})
		}
	}
	discoveries := []ECSLogDiscovery{{ServiceARN: policy.ServiceARN, LastSuccess: now}}
	health := m.projectECSLogHealth(sources, discoveries, now)
	if health.Status != "ok" || health.Services[0].ActiveTasks != 3 || health.ActiveSources != 12 || health.CoverageStatus != "unverified" {
		t.Fatalf("liveness/task counts: %+v", health)
	}
	for i := range sources[:4] {
		sources[i].StoppedAt, sources[i].StopCode = now-20, "ServiceSchedulerInitiated"
		sources[i].LeaseUntil, sources[i].LastHeartbeat = 0, 0
	}
	health = m.projectECSLogHealth(sources, discoveries, now)
	if health.Status != "ok" || health.UnhealthySources != 0 || health.StoppedPending != 4 || health.ActiveSources != 8 || health.Services[0].ActiveTasks != 2 || health.Services[0].TaskCount != 3 {
		t.Fatalf("normal scale-in must not remain a live heartbeat alarm: %+v", health)
	}
	sources[0].StopCode = "EssentialContainerExited"
	health = m.projectECSLogHealth(sources, discoveries, now)
	if health.Status != "degraded" || health.DeliveryGaps != 1 || health.StoppedPending != 3 {
		t.Fatalf("abnormal exit hidden: %+v", health)
	}
	// Failed discovery retains all old sources; it does not turn them STOPPED.
	discoveries[0].LastFailure = now
	health = m.projectECSLogHealth(sources, discoveries, now)
	if health.DiscoveryIssues != 1 || health.ActiveSources != 8 || health.Services[0].DiscoveryStatus != "discovery_failed" {
		t.Fatalf("discovery uncertainty hidden: %+v", health)
	}
}

func TestECSLogHealthMissingDiscoveryAndNoSourcesCannotBeGreen(t *testing.T) {
	m := newECSLogTestMonitor(t)
	now := time.Now().Unix()
	health := m.projectECSLogHealth(nil, nil, now)
	if len(health.Services) != 1 || health.Status != "degraded" || health.DiscoveryIssues != 1 {
		t.Fatalf("configured service disappeared before first discovery: %+v", health)
	}
	health = m.projectECSLogHealth(nil, []ECSLogDiscovery{{ServiceARN: testECSLogPolicy().ServiceARN, LastSuccess: now}}, now)
	if health.Status != "degraded" || health.Services[0].Status != "awaiting_sources" {
		t.Fatalf("no registration is not complete coverage: %+v", health)
	}
}

func TestECSLogHealthStoreFailureAndDisabledIsolation(t *testing.T) {
	m := &Monitor{}
	now := time.Now().Unix()
	if h := m.ecsLogHealth(context.Background(), now); h.Status != "disabled" || !h.Available {
		t.Fatalf("disabled path queried a missing store: %+v", h)
	}
	m.cfg.ECSLogEnabled = true
	m.cfg.ECSLogScope = ecsLogScopeIsolated
	if h := m.ecsLogHealth(context.Background(), now); h.Available || h.Status != "unavailable" {
		t.Fatalf("registry failure converted into empty healthy list: %+v", h)
	}
}

func TestECSLogStatusSnapshotFilteringAndSummary(t *testing.T) {
	m := newECSLogTestMonitor(t)
	now := time.Now().Unix()
	if err := m.observeECSLogTasks(context.Background(), testECSLogPolicy(), ecsLogTestTasks(27), now); err != nil {
		t.Fatal(err)
	}
	var first ECSLogSource
	if err := m.storeDB.Order("task_arn").First(&first).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&ECSLogSource{}).Where("task_arn = ?", first.TaskARN).Updates(map[string]any{"stopped_at": now - 20, "stop_code": "ServiceSchedulerInitiated"}).Error; err != nil {
		t.Fatal(err)
	}
	r := gin.New()
	r.GET("/sources", m.serveECSLogSources)
	for _, tc := range []struct {
		query       string
		total, rows int
	}{
		{"?phase=active&page=1", 104, 100},
		{"?phase=active&page=2", 104, 4},
		{"?phase=stopped", 4, 4},
		{"?service=unknown", 0, 0},
	} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/sources"+tc.query, nil))
		var reply struct {
			Total   int                `json:"total"`
			Sources []ecsLogSourceView `json:"sources"`
			Health  ecsLogHealth       `json:"health"`
		}
		if w.Code != 200 || json.Unmarshal(w.Body.Bytes(), &reply) != nil || reply.Total != tc.total || len(reply.Sources) != tc.rows || reply.Health.SourceCount != 108 || reply.Health.Services[0].TaskCount != 27 || reply.Health.StoppedPending != 4 {
			t.Fatalf("filtered page changed full summary %s: %d %s", tc.query, w.Code, w.Body.String())
		}
		if strings.Contains(w.Body.String(), "public_key") {
			t.Fatal("public key exposed")
		}
	}
	for _, query := range []string{"?page=0", "?phase=unknown", "?page=10001"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest("GET", "/sources"+query, nil))
		if w.Code != 400 {
			t.Fatalf("invalid filter accepted: %s %d", query, w.Code)
		}
	}
	m.Close()
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest("GET", "/sources", nil))
	if w.Code != 503 {
		t.Fatalf("closed store must fail visibly: %d", w.Code)
	}
}

func TestECSLogHealthIncludedInUnifiedStabilityHealth(t *testing.T) {
	m := newECSLogTestMonitor(t)
	h := m.stabilityHealth(context.Background(), time.Now())
	if h.ECSLogs == nil || h.ECSLogs.DiscoveryIssues != 1 || h.Status != "degraded" {
		t.Fatalf("dynamic discovery missing from unified sync state: %+v", h.ECSLogs)
	}
	m.cfg.ECSLogEnabled = false
	h = m.stabilityHealth(context.Background(), time.Now())
	if h.ECSLogs != nil {
		t.Fatal("legacy health contract changed while ECS is disabled")
	}
}
