package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func incidentPageRequest(t *testing.T, m *Monitor, path string, detail bool) (int, map[string]any) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	if detail {
		m.serveIncidentDetail(c)
	} else {
		m.serveIncidentList(c)
	}
	var data map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &data); err != nil {
		t.Fatal(err)
	}
	return w.Code, data
}

func TestIncidentPageSnapshotPaginationAndMutation(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	for i := 0; i < 3; i++ {
		r := Incident{ID: fmt.Sprintf("incident-%d", i), DedupKey: fmt.Sprint(i), ActiveMarker: 1, Revision: 1, Status: "open", FirstSeenTs: 600, LastSeenTs: 900}
		if err := m.storeDB.Create(&r).Error; err != nil {
			t.Fatal(err)
		}
	}
	base := "/incidents?consistent=1&from=600&to=1200&limit=1"
	status, first := incidentPageRequest(t, m, base, false)
	if status != 200 || first["incidents"].([]any)[0].(map[string]any)["id"] != "incident-0" {
		t.Fatalf("first page: %d %+v", status, first)
	}
	next := base + "&snapshot=" + first["snapshot"].(string) + "&cursor=" + url.QueryEscape(first["next_cursor"].(string))
	status, second := incidentPageRequest(t, m, next, false)
	if status != 200 || second["incidents"].([]any)[0].(map[string]any)["id"] != "incident-1" {
		t.Fatalf("stable same-time ordering: %d %+v", status, second)
	}
	if err := m.storeDB.Create(&IncidentPendingSummary{IncidentID: "incident-1", ScenarioPending: true, PendingReason: "no_fallback_config_fact"}).Error; err != nil {
		t.Fatal(err)
	}
	status, _ = incidentPageRequest(t, m, next, false)
	if status != 409 {
		t.Fatalf("pending-only change did not invalidate snapshot: %d", status)
	}
	if err := m.storeDB.Model(&Incident{}).Where("id = ?", "incident-2").Updates(map[string]any{"last_seen_ts": 1100, "revision": 2}).Error; err != nil {
		t.Fatal(err)
	}
	status, changed := incidentPageRequest(t, m, next, false)
	if status != 409 || changed["data_status"] != "snapshot_changed" {
		t.Fatalf("moving rows must not silently disappear: %d %+v", status, changed)
	}
	status, refreshed := incidentPageRequest(t, m, base, false)
	if status != 200 || refreshed["incidents"].([]any)[0].(map[string]any)["id"] != "incident-2" {
		t.Fatalf("refresh did not advance: %d %+v", status, refreshed)
	}
}

func TestIncidentPageEvidencePaginationFencesIndependentChildChanges(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	if err := m.storeDB.Create(&Incident{ID: "test", DedupKey: "test", ActiveMarker: 1, Status: "open", Revision: 2}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := m.storeDB.Create(&IncidentIngestRevision{ID: fmt.Sprint(i), IncidentID: "test", DetectedAt: 900}).Error; err != nil {
			t.Fatal(err)
		}
	}
	base := "/incidents/detail?id=test&revision=2&consistent=1&limit=1"
	status, first := incidentPageRequest(t, m, base, true)
	if status != 200 || first["revisions_has_more"] != true || len(first["revisions"].([]any)) != 1 {
		t.Fatalf("%d %+v", status, first)
	}
	next := base + "&revisions_cursor=" + url.QueryEscape(first["revisions_next_cursor"].(string)) + "&evidence_snapshot=" + first["evidence_snapshot"].(string)
	status, second := incidentPageRequest(t, m, next, true)
	if status != 200 || second["revisions_next_cursor"] != "1" {
		t.Fatalf("%d %+v", status, second)
	}
	if err := m.storeDB.Model(&IncidentIngestRevision{}).Where("id = ?", "0").Update("kind", "gap_backfilled").Error; err != nil {
		t.Fatal(err)
	}
	status, _ = incidentPageRequest(t, m, next, true)
	if status != 409 {
		t.Fatalf("child changed without revision, returned %d", status)
	}
}

func TestIncidentPageFirstDetailFencesPendingChangeWithoutRevision(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	if err := m.storeDB.Create(&Incident{ID: "test", DedupKey: "test", ActiveMarker: 1, Status: "open", Revision: 2, FirstSeenTs: 600, LastSeenTs: 900}).Error; err != nil {
		t.Fatal(err)
	}
	status, list := incidentPageRequest(t, m, "/incidents?consistent=1&from=600&to=1200", false)
	if status != 200 {
		t.Fatal(status, list)
	}
	row := list["incidents"].([]any)[0].(map[string]any)
	query := "/incidents/detail?id=test&consistent=1&revision=2&row_snapshot=" + row["row_snapshot"].(string)
	status, detail := incidentPageRequest(t, m, query, true)
	if status != 200 || detail["incident"].(map[string]any)["row_snapshot"] != row["row_snapshot"] {
		t.Fatal(status, detail)
	}
	if err := m.storeDB.Create(&IncidentPendingSummary{IncidentID: "test", ScenarioPending: true, PendingReason: "no_fallback_config_fact"}).Error; err != nil {
		t.Fatal(err)
	}
	status, detail = incidentPageRequest(t, m, query, true)
	if status != 409 || detail["data_status"] != "snapshot_changed" {
		t.Fatalf("stale list pending status accepted: %d %+v", status, detail)
	}
}

func TestIncidentPageRejectsUnboundedOrInvalidRange(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	for _, query := range []string{"", "from=bad&to=900", "from=-1&to=900", "from=900&to=900", "from=1&to=999999999999"} {
		status, _ := incidentPageRequest(t, m, "/incidents?consistent=1&"+query, false)
		if status != 400 {
			t.Fatalf("%q: %d", query, status)
		}
	}
}

func TestIncidentPageCoverageRequiresEveryProcessedWindow(t *testing.T) {
	core, _ := incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	ctx := context.Background()
	from, to := int64(600), int64(1200)
	check := func(want bool) {
		t.Helper()
		proof, err := m.incidentPageCoverage(ctx, from, to, core, time.Now().Unix())
		if err != nil || proof["complete"] != want {
			t.Fatalf("coverage=%+v err=%v expected=%t", proof, err, want)
		}
	}
	if err := m.storeDB.Create(&IncidentIngestCursor{ID: 1, ScanThroughTs: to, CompleteThroughTs: to}).Error; err != nil {
		t.Fatal(err)
	}
	check(false) // A cursor alone does not prove the retained left edge.
	for _, at := range []int64{600, 900} {
		if err := m.storeDB.Create(&IncidentIngestWindowState{DedupKey: "no-sample", WindowFromTs: at, WindowToTs: at + 300, DataComplete: true, ProcessState: incidentProcessDone, WindowState: incidentWindowStateNoSample}).Error; err != nil {
			t.Fatal(err)
		}
	}
	check(true)
	if err := m.storeDB.Create(&IncidentIngestWindowState{DedupKey: "pending-key", WindowFromTs: 900, WindowToTs: 1200, ProcessState: incidentProcessAwaitingSource}).Error; err != nil {
		t.Fatal(err)
	}
	check(false) // One complete key cannot certify another unfinished key.
	m.storeDB.Where("dedup_key = ?", "pending-key").Delete(&IncidentIngestWindowState{})
	m.storeDB.Create(&IncidentIngestGapSummary{DedupKey: "archived-gap", GapFromTs: 900, GapThroughTs: 1200})
	check(false)
}

func TestIncidentPageObservationTiePaginationAndRevisionFence(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	if err := m.storeDB.Create(&Incident{ID: "test", DedupKey: "test", ActiveMarker: 1, Status: "open", Revision: 2}).Error; err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := m.storeDB.Create(&IncidentObservation{ID: fmt.Sprint(i), IncidentID: "test", WindowFromTs: 600, WindowToTs: 900}).Error; err != nil {
			t.Fatal(err)
		}
	}
	base := "/incidents/detail?id=test&revision=2&limit=1"
	status, first := incidentPageRequest(t, m, base, true)
	if status != 200 {
		t.Fatalf("%d %+v", status, first)
	}
	status, second := incidentPageRequest(t, m, base+"&observations_cursor="+url.QueryEscape(first["observations_next_cursor_key"].(string)), true)
	if status != 200 || second["observations"].([]any)[0].(map[string]any)["ID"] != "1" {
		t.Fatalf("same-time evidence omitted: %d %+v", status, second)
	}
	status, _ = incidentPageRequest(t, m, "/incidents/detail?id=test&revision=1", true)
	if status != 409 {
		t.Fatal("stale list revision was presented as matching detail", status)
	}
}

func TestIncidentPagePermissionsRemainAdminOnly(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	r := gin.New()
	m.RegisterRoutes(r)
	for _, role := range []int{1, roleAdmin} {
		w := httptest.NewRecorder()
		req := httptest.NewRequest(http.MethodGet, "/incidents?consistent=1&from=600&to=900", nil)
		req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("fixture", role, time.Now().Unix())})
		r.ServeHTTP(w, req)
		want := 200
		if role == 1 {
			want = 403
		}
		if w.Code != want {
			t.Fatalf("role=%d status=%d want=%d body=%s", role, w.Code, want, w.Body)
		}
	}
}
