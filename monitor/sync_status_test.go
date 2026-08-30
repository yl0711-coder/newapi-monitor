package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestSyncOverviewSectionsRunConcurrentlyAndIsolateFailure(t *testing.T) {
	m := newTestMonitor(t)
	pause := func(ctx context.Context) error {
		select {
		case <-time.After(200 * time.Millisecond):
			return nil
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	now := time.Now()
	started := time.Now()
	snapshot := m.buildSyncStatusSnapshotWith(context.Background(), now, syncStatusReaders{
		Usage: func(ctx context.Context) (syncStatusUsageRead, error) {
			if err := pause(ctx); err != nil {
				return syncStatusUsageRead{}, err
			}
			return syncStatusUsageRead{}, errors.New("injected usage failure")
		},
		Stability: func(ctx context.Context) (stabilityHealthResponse, error) {
			if err := pause(ctx); err != nil {
				return stabilityHealthResponse{}, err
			}
			return stabilityHealthResponse{CheckedAt: now.Unix(), Status: "ok"}, nil
		},
		Upstream: func(ctx context.Context) (map[string]ChannelUpstreamAccountView, error) {
			if err := pause(ctx); err != nil {
				return nil, err
			}
			return map[string]ChannelUpstreamAccountView{}, nil
		},
		CostClosure: func(ctx context.Context) (syncChannelCostStatus, error) {
			if err := pause(ctx); err != nil {
				return syncChannelCostStatus{}, err
			}
			return syncChannelCostStatus{}, errors.New("injected cost closure failure")
		},
	})
	if elapsed := time.Since(started); elapsed >= 450*time.Millisecond {
		t.Fatalf("sections were not collected concurrently: %s", elapsed)
	}
	if snapshot.Overview.Usage.Available || !snapshot.Overview.Stability.Available || !snapshot.Overview.Upstream.Available || snapshot.Overview.CostClosure.Available {
		t.Fatalf("section failure was not isolated: %+v", snapshot.Overview)
	}
}

func TestSyncStatusCacheLockWaitHonorsContext(t *testing.T) {
	m := newTestMonitor(t)
	m.syncStatusMu.Lock()
	defer m.syncStatusMu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	started := time.Now()
	if snapshot, err := m.currentSyncStatusSnapshot(ctx, false); err == nil || snapshot != nil {
		t.Fatalf("expected canceled lock wait, snapshot=%v err=%v", snapshot, err)
	}
	if elapsed := time.Since(started); elapsed >= 200*time.Millisecond {
		t.Fatalf("cache lock ignored request context: %s", elapsed)
	}
}

func TestCanceledSyncOverviewBuildDoesNotPoisonSharedCache(t *testing.T) {
	m := newTestMonitor(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if snapshot, err := m.currentSyncStatusSnapshot(ctx, true); snapshot == nil || err == nil {
		t.Fatal("canceled caller should still receive the completed local projection")
	}
	if m.syncStatusCached != nil || !m.syncStatusCachedAt.IsZero() {
		t.Fatal("canceled request must not publish a partial snapshot into the shared cache")
	}
}

func TestSyncOverviewReadsLocalStateWhenProductionDBIsClosed(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	prod := newFakeProdDB(t)
	if err := prod.Close(); err != nil {
		t.Fatal(err)
	}
	m.prodDB = prod // 若状态投影误触来源查询，对应分区将不可用。
	m.cfg.StabilityEnabled = true

	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/sync/overview", nil)
	m.serveSyncOverview(c)
	if w.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var response syncOverviewResponse
	if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
		t.Fatal(err)
	}
	if response.Version != 1 || response.SnapshotAt == 0 {
		t.Fatalf("missing versioned snapshot metadata: %+v", response)
	}
	if !response.Runtime.Available || !response.Usage.Available || !response.Stability.Available || !response.Upstream.Available {
		t.Fatalf("local sections unexpectedly unavailable with closed production DB: %+v", response)
	}
	if response.Usage.Data.Facts.FullHistory != nil {
		t.Fatal("overview must not duplicate the full history member payload")
	}
}

func TestSyncUsageWorkloadsAreBoundedAndFilterAttention(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	members := make([]UsageFactHistoryMemberProgress, 0, 150)
	for i := 1; i <= 150; i++ {
		member := UsageFactHistoryMemberProgress{UserID: int64(i), ServiceReady: true, ArchiveReady: true, Published: true}
		if i%2 == 0 {
			member.ArchiveReady = false
		}
		members = append(members, member)
	}
	maintenance := make([]UsageFactMaintenanceProgress, 0, 150)
	for i := 1; i <= 150; i++ {
		job := UsageFactMaintenanceProgress{JobID: "job-" + strconv.Itoa(i), UserID: int64(i), Status: usageFactHistoryJobComplete}
		if i%2 == 0 {
			job.Status = usageFactHistoryJobPaused
		}
		maintenance = append(maintenance, job)
	}
	m.syncStatusCached = &syncStatusSnapshot{
		Overview:    syncOverviewResponse{Usage: syncStatusSection[syncUsageStatus]{Available: true}},
		FullHistory: UsageFactHistoryProgress{AsOf: time.Now().Unix(), Members: members, Maintenance: maintenance},
	}
	m.syncStatusCachedAt = time.Now()

	request := func(path string) syncUsageWorkloadsResponse {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest(http.MethodGet, path, nil)
		m.serveSyncWorkloads(c)
		if w.Code != http.StatusOK {
			t.Fatalf("path=%s status=%d body=%s", path, w.Code, w.Body.String())
		}
		var response syncUsageWorkloadsResponse
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
		return response
	}

	attention := request("/sync/workloads?domain=usage&state=attention&limit=50")
	if attention.Total != 75 || attention.AllTotal != 150 || len(attention.Items) != 50 {
		t.Fatalf("unexpected bounded attention page: %+v", attention)
	}
	all := request("/sync/workloads?domain=usage&state=all&offset=100&limit=1000")
	if all.Total != 150 || all.Limit != syncStatusMaxPageSize || len(all.Items) != 50 {
		t.Fatalf("unexpected bounded all page: %+v", all)
	}
	filtered := request("/sync/workloads?domain=usage&state=all&user_id=149")
	if filtered.Total != 1 || len(filtered.Items) != 1 || filtered.Items[0].UserID != 149 {
		t.Fatalf("user filter mismatch: %+v", filtered)
	}
	maintenancePage := request("/sync/workloads?domain=usage&kind=maintenance&state=attention&limit=50")
	if maintenancePage.Kind != "maintenance" || maintenancePage.Total != 75 || maintenancePage.AllTotal != 150 || len(maintenancePage.Jobs) != 50 {
		t.Fatalf("unexpected bounded maintenance page: %+v", maintenancePage)
	}
}

func TestSyncWorkloadsReturnsUnavailableInsteadOfFalseEmpty(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newTestMonitor(t)
	m.syncStatusCached = &syncStatusSnapshot{Overview: syncOverviewResponse{Usage: syncStatusSection[syncUsageStatus]{Available: false, Error: "injected unavailable"}}}
	m.syncStatusCachedAt = time.Now()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/sync/workloads?domain=usage", nil)
	m.serveSyncWorkloads(c)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
}
