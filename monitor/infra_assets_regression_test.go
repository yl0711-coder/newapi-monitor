package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func TestInfraAssetConcurrentFirstBindingIsAtomic(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	reached, resume := make(chan struct{}), make(chan struct{})
	var first atomic.Bool
	callback := "infra_test_query_barrier"
	if err := m.storeDB.Callback().Query().After("gorm:query").Register(callback, func(tx *gorm.DB) {
		if tx.Statement.Table == "infra_assets" && first.CompareAndSwap(false, true) {
			close(reached)
			<-resume
		}
	}); err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := m.storeDB.Callback().Query().Remove(callback); err != nil {
			t.Errorf("remove query barrier: %v", err)
		}
	}()
	done := make(chan error, 1)
	go func() { done <- m.noteInfraHostReport(ctx, "race-host", now) }()
	select {
	case <-reached:
	case <-time.After(5 * time.Second):
		close(resume)
		t.Fatal("query barrier not reached")
	}
	err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{{Identity: "arn:race-host", Resource: "race-host", Kind: "instance", Platform: "Lightsail", CloudState: "running"}}, now)
	close(resume)
	if err != nil {
		t.Fatal(err)
	}
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	assets, err := m.infraAssets(ctx)
	if err != nil || len(assets) != 1 || assets[0].Kind != "instance" || assets[0].LastReport != now {
		t.Fatalf("duplicate/lost host after retry: %+v %v", assets, err)
	}
}

func TestInfraAssetExistingLegacyShadowIsLinked(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	row := infraAssetObservation{Identity: "arn:host", Resource: "host", Kind: "instance", Platform: "Lightsail", CloudState: "running"}
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, now); err != nil {
		t.Fatal(err)
	}
	id := "host-name:host"
	legacy := InfraAsset{ID: infraAssetID(id), Identity: id, Resource: "host", Kind: "host", State: "archived", ArchivedAt: now + 1, Revision: 2}
	if err := m.storeDB.Create(&legacy).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, now+2); err != nil {
		t.Fatal(err)
	}
	cloud := assetForTest(t, m, row.Identity)
	if cloud.State != "archived" || assetForTest(t, m, id).State != "linked" {
		t.Fatal("legacy manual decision lost or shadow not linked")
	}
	var audit InfraAssetAudit
	if err := m.storeDB.First(&audit, "asset_id = ?", cloud.ID).Error; err != nil {
		t.Fatal(err)
	}
	if audit.Revision != cloud.Revision || audit.Action != "link" {
		t.Fatal("link audit mismatch")
	}
}

func TestInfraAssetNewGenerationExcludesBoundaryMetricsAndContainers(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	base := time.Now().Unix() / 60 * 60
	row := infraAssetObservation{Identity: "arn:old", Resource: "worker", Kind: "instance", Platform: "Lightsail", CloudState: "running"}
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, base+5); err != nil {
		t.Fatal(err)
	}
	if err := m.upsertInfra([]InfraSample{{BucketTs: base, Resource: "worker", RType: "host", Metric: "cpu", Value: 77}}); err != nil {
		t.Fatal(err)
	}
	if err := m.replaceHostContainerSnapshots("worker", []HostContainerSnapshot{{Node: "worker", Name: "old-container", State: "running", LastSeen: base + 5}}); err != nil {
		t.Fatal(err)
	}
	row.Identity = "arn:new"
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, base+20); err != nil {
		t.Fatal(err)
	}
	snap := m.computeInfraSnapshot(base + 20)
	if len(snap.Instances) != 1 || len(snap.Instances[0].Metrics) != 0 {
		t.Fatalf("boundary metric reused: %+v", snap.Instances)
	}
	if err := m.upsertInfra([]InfraSample{{BucketTs: base + 60, Resource: "worker", RType: "instance", Metric: "cpu", Value: 2}}); err != nil {
		t.Fatal(err)
	}
	snap = m.computeInfraSnapshot(base + 60)
	if len(snap.Instances) != 1 || snap.Instances[0].Metrics["cpu"] != 2 || len(snap.Instances[0].Containers) != 0 {
		t.Fatalf("old containers reused or fresh metric lost: %+v", snap.Instances)
	}
	if got := m.hostContainerSnapshot("worker", base+60); len(got) != 1 {
		t.Fatal("historical snapshot was deleted")
	}
}

func TestInfraAssetLateGenerationReportCannotWriteTelemetry(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.IngestToken = "test-token"
	ctx := context.Background()
	now := time.Now().Unix()
	row := infraAssetObservation{Identity: "arn:old", Resource: "worker", Kind: "instance", Platform: "Lightsail", CloudState: "running"}
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, now-600); err != nil {
		t.Fatal(err)
	}
	old := assetForTest(t, m, row.Identity)
	if err := m.storeDB.Model(&old).Update("state", "removed").Error; err != nil {
		t.Fatal(err)
	}
	row.Identity = "arn:new"
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, now-60); err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/host", m.ingestHost)
	for _, ts := range []int64{0, now - 300, now + 3600, now} {
		out := httptest.NewRecorder()
		req := httptest.NewRequest("POST", "/host", strings.NewReader(fmt.Sprintf(`{"node":"worker","ts":%d,"disk_used_pct":99,"containers":[{"name":"nginx","state":"running"}]}`, ts)))
		req.Header.Set("Authorization", "Bearer test-token")
		req.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(out, req)
		want := http.StatusConflict
		if ts == now {
			want = http.StatusOK
		}
		if out.Code != want {
			t.Fatalf("ts=%d response=%d %s", ts, out.Code, out.Body.String())
		}
		var count int64
		if err := m.storeDB.Model(&InfraSample{}).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if ts != now && count != 0 {
			t.Fatal("rejected old sample wrote metrics")
		}
		if ts != now && len(m.hostContainerSnapshot("worker", now)) != 0 {
			t.Fatal("rejected old sample wrote containers")
		}
	}
}

func TestInfraAssetHostTransactionRollsBackAllWrites(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	err := m.acceptInfraHostReport(ctx, "host", now, now, func(tx *gorm.DB) error {
		if err := upsertInfraWithDB(tx, []InfraSample{{BucketTs: now / 60 * 60, Resource: "host", RType: "host", Metric: "cpu", Value: 5}}); err != nil {
			return err
		}
		return fmt.Errorf("injected container persistence failure")
	})
	if err == nil {
		t.Fatal("expected failure")
	}
	for _, model := range []any{&InfraAsset{}, &InfraSample{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 0 {
			t.Fatalf("partial write %T: %d %v", model, count, err)
		}
	}
}

func TestInfraAssetSamplingStatusRequiresValidSample(t *testing.T) {
	m := newTestMonitor(t)
	now := time.Now().Unix()
	for _, ts := range []int64{0, now - 3600, now + 3600} {
		node := fmt.Sprint(ts)
		if err := m.noteInfraHostReport(context.Background(), node, now, ts); err != nil {
			t.Fatal(err)
		}
		if got := m.assetView(assetForTest(t, m, "host-name:"+node), now); got.Status != "sample_unconfirmed" || got.CanRestore {
			t.Fatalf("invalid sample marked healthy: %+v", got)
		}
	}
}

func TestInfraAssetPaginationAndScopeWatermark(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	rows := make([]InfraAsset, infraAssetLimit+1)
	for i := range rows {
		id := fmt.Sprintf("task:%05d", i)
		rows[i] = InfraAsset{ID: infraAssetID(id), Identity: id, Scope: "ecs/s", Resource: id, Kind: "ecs_task", Platform: "ECS/Fargate", State: "archived", CloudState: "missing", LastChecked: now - 3600, Revision: 1}
	}
	if err := m.storeDB.CreateInBatches(rows, 100).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&InfraAsset{}).Where("id = ?", rows[0].ID).Updates(map[string]any{"state": "removed", "cloud_state": "running"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.observeInfraAssets(ctx, "ecs/s", nil, now); err != nil {
		t.Fatal(err)
	}
	if a := assetForTest(t, m, rows[0].Identity); a.LastChecked != now-3600 || a.State != "removed" {
		t.Fatal("removed tombstone rewritten")
	}
	if a := assetForTest(t, m, rows[1].Identity); a.LastChecked != now-3600 {
		t.Fatal("already missing history rewritten")
	}
	page, err := m.infraAssetPage(ctx, "", "")
	if err != nil || len(page.Archived) != infraAssetPageSize || page.ArchivedNext == "" {
		t.Fatalf("history budget broke management: %v", err)
	}
	if page.Archived[0].LastChecked != now {
		t.Fatal("scope freshness not reflected in view")
	}
	second, err := m.infraAssetPage(ctx, "", page.ArchivedNext)
	if err != nil {
		t.Fatal(err)
	}
	if second.Archived[0].ID <= page.Archived[len(page.Archived)-1].ID {
		t.Fatal("cursor duplicated a row")
	}
	if err := m.changeInfraAsset(ctx, page.Archived[0].ID, "remove", "root", page.Archived[0].Revision, now); err != nil {
		t.Fatal(err)
	}
	stale := infraAssetObservation{Identity: "stale-task", Resource: "stale-task", Kind: "ecs_task", Platform: "ECS/Fargate", CloudState: "running"}
	if err := m.observeInfraAssets(ctx, "ecs/s", []infraAssetObservation{stale}, now-10); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&InfraAsset{}).Where("identity = ?", stale.Identity).Count(&count).Error; err != nil || count != 0 {
		t.Fatalf("old scan inserted new identity: %d %v", count, err)
	}
	router := gin.New()
	router.GET("/assets", m.serveInfraAssets)
	out := httptest.NewRecorder()
	router.ServeHTTP(out, httptest.NewRequest("GET", "/assets", nil))
	var payload struct {
		Archived []infraAssetView `json:"archived"`
		Next     string           `json:"archived_next"`
	}
	if err := json.Unmarshal(out.Body.Bytes(), &payload); err != nil || out.Code != 200 || len(payload.Archived) != infraAssetPageSize || payload.Next == "" {
		t.Fatalf("HTTP pagination failed %d %v", out.Code, err)
	}
}
