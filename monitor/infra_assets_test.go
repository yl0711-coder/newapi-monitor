package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func assetForTest(t *testing.T, m *Monitor, identity string) InfraAsset {
	t.Helper()
	var a InfraAsset
	if err := m.storeDB.First(&a, "identity = ?", identity).Error; err != nil {
		t.Fatal(err)
	}
	return a
}

func TestInfraAssetArchiveRestoreRemoveAndNewIncarnation(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	observation := infraAssetObservation{Identity: "arn:old-instance", Resource: "worker", Kind: "instance", Platform: "Lightsail", CloudState: "running"}
	observe := func(at int64) {
		t.Helper()
		if err := m.observeInfraAssets(ctx, "lightsail:test:instance", []infraAssetObservation{observation}, at); err != nil {
			t.Fatal(err)
		}
	}
	observe(now)
	a := assetForTest(t, m, observation.Identity)
	if m.assetView(a, now).Status != "awaiting_report" {
		t.Fatal("AWS presence was confused with host telemetry")
	}
	if err := m.upsertInfra([]InfraSample{{BucketTs: now / 60 * 60, Resource: "worker", RType: "host", Metric: "cpu", Value: 12}}); err != nil {
		t.Fatal(err)
	}
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", a.Revision, now+1); err != nil {
		t.Fatal(err)
	}
	if len(m.computeInfraSnapshot(now+1).Instances) != 0 {
		t.Fatal("archived host still visible")
	}
	a = assetForTest(t, m, a.Identity)
	observe(now + 2)
	if m.assetRecovery(assetForTest(t, m, a.Identity), now+2) {
		t.Fatal("Lightsail presence alone allowed restoration")
	}
	if err := m.changeInfraAsset(ctx, a.ID, "restore", "root", a.Revision, now+2); !errors.Is(err, errInfraAssetConflict) {
		t.Fatalf("restore without report: %v", err)
	}
	if err := m.noteInfraHostReport(ctx, "worker", now+3); err != nil {
		t.Fatal(err)
	}
	a = assetForTest(t, m, a.Identity)
	if !m.assetRecovery(a, now+3) {
		t.Fatal("new report did not offer restore")
	}
	if err := m.changeInfraAsset(ctx, a.ID, "remove", "root", a.Revision, now+3); !errors.Is(err, errInfraAssetConflict) {
		t.Fatal("recovering resource was removed")
	}
	if err := m.changeInfraAsset(ctx, a.ID, "restore", "root", a.Revision, now+4); err != nil {
		t.Fatal(err)
	}
	a = assetForTest(t, m, a.Identity)
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", a.Revision, now+5); err != nil {
		t.Fatal(err)
	}
	a = assetForTest(t, m, a.Identity)
	if err := m.changeInfraAsset(ctx, a.ID, "remove", "root", a.Revision, now+6); err != nil {
		t.Fatal(err)
	}
	observe(now + 7)
	if err := m.noteInfraHostReport(ctx, "worker", now+8); err != nil {
		t.Fatal(err)
	}
	if assetForTest(t, m, a.Identity).State != "removed" || len(m.computeInfraSnapshot(now+8).Instances) != 0 {
		t.Fatal("removed identity was resurrected")
	}
	var count int64
	if err := m.storeDB.Model(&InfraSample{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("historical samples were deleted: %d %v", count, err)
	}
	if err := m.storeDB.Model(&InfraAssetAudit{}).Count(&count).Error; err != nil || count != 4 {
		t.Fatalf("audit count=%d error=%v", count, err)
	}
	observation.Identity = "arn:new-instance"
	observe(now + 120)
	newAsset := assetForTest(t, m, observation.Identity)
	if newAsset.ID == a.ID || newAsset.State != "active" {
		t.Fatal("same-name new cloud incarnation inherited tombstone")
	}
	snap := m.computeInfraSnapshot(now + 120)
	if len(snap.Instances) != 1 || len(snap.Instances[0].Metrics) != 0 {
		t.Fatalf("new incarnation inherited old metrics: %+v", snap.Instances)
	}
}

func TestInfraAssetScalingAbsenceAndManualMembership(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	makeRows := func(ids ...int) []infraAssetObservation {
		var rows []infraAssetObservation
		for _, id := range ids {
			rows = append(rows, infraAssetObservation{Identity: fmt.Sprintf("arn:task:%d", id), Resource: fmt.Sprintf("ecs/task/%d", id), Parent: "ecs/c/s", Kind: "ecs_task", Platform: "ECS/Fargate", CloudState: "running"})
		}
		return rows
	}
	for i, rows := range [][]infraAssetObservation{makeRows(1, 2, 3), makeRows(1, 2, 3, 4, 5), makeRows(1, 2, 4)} {
		if err := m.observeInfraAssets(ctx, "service-a", rows, now+int64(i)); err != nil {
			t.Fatal(err)
		}
	}
	for _, id := range []int{3, 5} {
		a := assetForTest(t, m, fmt.Sprintf("arn:task:%d", id))
		if a.State != "active" || a.CloudState != "missing" {
			t.Fatalf("absent task was auto-archived: %+v", a)
		}
	}
	a := assetForTest(t, m, "arn:task:3")
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", a.Revision, now+3); err != nil {
		t.Fatal(err)
	}
	if err := m.observeInfraAssets(ctx, "service-a", makeRows(1, 2, 3, 4), now+4); err != nil {
		t.Fatal(err)
	}
	a = assetForTest(t, m, a.Identity)
	if a.State != "archived" || !m.assetRecovery(a, now+4) {
		t.Fatal("rediscovery changed manual state or failed to offer recovery")
	}
	before := assetForTest(t, m, "arn:task:1")
	if err := m.observeInfraAssets(ctx, "service-a", makeRows(1, 1), now+5); err == nil {
		t.Fatal("duplicate inventory accepted")
	}
	if after := assetForTest(t, m, before.Identity); after.CloudState != before.CloudState || after.LastDiscovered != before.LastDiscovered {
		t.Fatal("invalid inventory partially changed membership")
	}
	if err := m.observeInfraAssets(ctx, "other-service", nil, now+6); err != nil {
		t.Fatal(err)
	}
	if assetForTest(t, m, before.Identity).CloudState != "running" {
		t.Fatal("other service retirement affected this task")
	}
	if m.assetRecovery(a, now+m.infraMetricFreshnessSec()+10) {
		t.Fatal("stale cloud observation allowed restore")
	}
}

func TestInfraAssetFirstCloudBindingKeepsLegacyArchive(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	if err := m.noteInfraHostReport(ctx, "host", now); err != nil {
		t.Fatal(err)
	}
	a := assetForTest(t, m, "host-name:host")
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", a.Revision, now+1); err != nil {
		t.Fatal(err)
	}
	if err := m.observeInfraAssets(ctx, "lightsail", []infraAssetObservation{{Identity: "arn:host", Resource: "host", Kind: "instance", Platform: "Lightsail", CloudState: "running"}}, now+2); err != nil {
		t.Fatal(err)
	}
	cloud := assetForTest(t, m, "arn:host")
	if cloud.State != "archived" || cloud.ArchivedAt != now+1 || assetForTest(t, m, a.Identity).State != "linked" {
		t.Fatal("first cloud binding lost manual archive or left duplicate host")
	}
}

func TestInfraAssetActionsRejectCSRFAndStaleRevisions(t *testing.T) {
	m := newTestMonitor(t)
	now := time.Now().Unix()
	ctx := context.Background()
	if err := m.noteInfraHostReport(ctx, "node", now); err != nil {
		t.Fatal(err)
	}
	a := assetForTest(t, m, "host-name:node")
	gin.SetMode(gin.TestMode)
	router := gin.New()
	router.POST("/action", func(c *gin.Context) { c.Set("uname", "test-root"); c.Next() }, m.serveInfraAssetAction)
	post := func(origin string, revision int64) *httptest.ResponseRecorder {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://monitor.test/action", strings.NewReader(fmt.Sprintf(`{"id":%q,"action":"archive","revision":%d}`, a.ID, revision)))
		r.Header.Set("Origin", origin)
		r.Header.Set("Content-Type", "application/json")
		router.ServeHTTP(w, r)
		return w
	}
	for _, origin := range []string{"", "https://evil.test", "https://monitor.test.evil.test", "null"} {
		if w := post(origin, 1); w.Code != http.StatusForbidden {
			t.Fatalf("origin %q accepted: %d", origin, w.Code)
		}
	}
	if w := post("http://monitor.test", 1); w.Code != http.StatusOK {
		t.Fatalf("archive failed: %d %s", w.Code, w.Body.String())
	}
	if w := post("http://monitor.test", 1); w.Code != http.StatusConflict {
		t.Fatalf("stale action accepted: %d", w.Code)
	}
	var count int64
	m.storeDB.Model(&InfraAssetAudit{}).Count(&count)
	if count != 1 {
		t.Fatalf("retries or CSRF produced audit side effects: %d", count)
	}
}

func TestInfraAssetMissingHistoryRemainsUntilManualArchive(t *testing.T) {
	m := newTestMonitor(t)
	now := time.Now().Unix()
	ctx := context.Background()
	if err := m.retainMissingLightsailAssets(ctx, []infraLatestRow{{Resource: "deleted-host", RType: "instance"}}, nil, now); err != nil {
		t.Fatal(err)
	}
	snap := m.computeInfraSnapshot(now)
	if len(snap.Instances) != 1 || snap.Instances[0].Name != "deleted-host" {
		t.Fatal("deleted host vanished before manual archive")
	}
	assets, err := m.infraAssets(ctx)
	if err != nil || len(assets) != 1 {
		t.Fatal(err)
	}
	if m.assetView(assets[0], now).Status != "missing" {
		t.Fatal("missing historical identity presented as live")
	}
}

func TestInfraAssetRoutesRequireRootForChanges(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.SessionSecret = "test-only-secret"
	m.cfg.LocalAuthBypass = false
	now := time.Now().Unix()
	if err := m.noteInfraHostReport(context.Background(), "node", now); err != nil {
		t.Fatal(err)
	}
	a := assetForTest(t, m, "host-name:node")
	router := gin.New()
	m.RegisterRoutes(router)
	for _, role := range []int{0, roleAdmin, roleRoot} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, "http://monitor.test/infra/assets/action", strings.NewReader(fmt.Sprintf(`{"id":%q,"action":"archive","revision":1}`, a.ID)))
		r.Header.Set("Origin", "http://monitor.test")
		r.Header.Set("Content-Type", "application/json")
		r.Header.Set("Accept", "application/json")
		if role > 0 {
			r.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("operator", role, now)})
		}
		router.ServeHTTP(w, r)
		want := http.StatusOK
		if role == 0 {
			want = http.StatusUnauthorized
		} else if role == roleAdmin {
			want = http.StatusForbidden
		}
		if w.Code != want {
			t.Fatalf("role %d status=%d want=%d body=%s", role, w.Code, want, w.Body.String())
		}
	}
}

func TestInfraAssetMissingRemovedHostCannotReappearAsLegacyAlias(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	row := infraAssetObservation{Identity: "arn:removed", Resource: "node", Kind: "instance", Platform: "Lightsail", CloudState: "running"}
	if err := m.observeInfraAssets(ctx, "ls", []infraAssetObservation{row}, now); err != nil {
		t.Fatal(err)
	}
	a := assetForTest(t, m, row.Identity)
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", 1, now+1); err != nil {
		t.Fatal(err)
	}
	if err := m.changeInfraAsset(ctx, a.ID, "remove", "root", 2, now+2); err != nil {
		t.Fatal(err)
	}
	if err := m.observeInfraAssets(ctx, "ls", nil, now+3); err != nil {
		t.Fatal(err)
	}
	if err := m.noteInfraHostReport(ctx, "node", now+4); err != nil {
		t.Fatal(err)
	}
	rows, err := m.infraAssets(ctx)
	if err != nil || len(rows) != 0 {
		t.Fatalf("removed host resurrected: %+v %v", rows, err)
	}
	if assetForTest(t, m, row.Identity).State != "removed" {
		t.Fatal("tombstone was changed")
	}
}

func TestInfraAssetDelayedOrFutureHostReportsDoNotPermitRestore(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	if err := m.noteInfraHostReport(ctx, "node", now, now); err != nil {
		t.Fatal(err)
	}
	a := assetForTest(t, m, "host-name:node")
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", a.Revision, now+1); err != nil {
		t.Fatal(err)
	}
	for _, sampleAt := range []int64{0, now - 3600, now, now + 3600} {
		if err := m.noteInfraHostReport(ctx, "node", now+5, sampleAt); err != nil {
			t.Fatal(err)
		}
		if m.assetRecovery(assetForTest(t, m, a.Identity), now+5) {
			t.Fatalf("sample %d restored an archived node", sampleAt)
		}
	}
	if err := m.noteInfraHostReport(ctx, "node", now+6, now+6); err != nil {
		t.Fatal(err)
	}
	if !m.assetRecovery(assetForTest(t, m, a.Identity), now+6) {
		t.Fatal("valid post-archive sample cannot restore")
	}
}

func TestInfraAssetMembershipSurvivesReopenAndAuditFailureRollsBack(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	now := time.Now().Unix()
	if err := m.noteInfraHostReport(ctx, "node", now); err != nil {
		t.Fatal(err)
	}
	a := assetForTest(t, m, "host-name:node")
	if err := m.changeInfraAsset(ctx, a.ID, "archive", "root", a.Revision, now+1); err != nil {
		t.Fatal(err)
	}
	var databases []struct{ Name, File string }
	if err := m.storeDB.Raw("PRAGMA database_list").Scan(&databases).Error; err != nil {
		t.Fatal(err)
	}
	path := ""
	for _, db := range databases {
		if db.Name == "main" {
			path = db.File
		}
	}
	if path == "" {
		t.Fatal("missing test database path")
	}
	m.Close()
	reopened := &Monitor{cfg: m.cfg, chNames: map[string]string{}}
	if err := reopened.openStore(path); err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	a = assetForTest(t, reopened, a.Identity)
	if a.State != "archived" || a.Revision != 2 {
		t.Fatal("archive was lost on restart")
	}
	if err := reopened.storeDB.Migrator().DropTable(&InfraAssetAudit{}); err != nil {
		t.Fatal(err)
	}
	if err := reopened.changeInfraAsset(ctx, a.ID, "remove", "root", a.Revision, now+2); err == nil {
		t.Fatal("action succeeded without audit")
	}
	a = assetForTest(t, reopened, a.Identity)
	if a.State != "archived" || a.Revision != 2 {
		t.Fatal("failed audit did not roll back membership change")
	}
}
