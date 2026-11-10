package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/gin-gonic/gin"
)

// 五张新表必须真的建出来，且带上关键唯一索引。
func TestIncidentIngestTablesCreated(t *testing.T) {
	m := openIncidentTestStore(t)
	for _, tbl := range []string{
		"incident_ingest_cursors", "incident_ingest_window_states",
		"incident_ingest_revisions", "incident_ingest_gap_summaries",
		"incident_impact_windows",
	} {
		if !m.storeDB.Migrator().HasTable(tbl) {
			t.Errorf("表 %s 未建出来：检查 store.go 的 AutoMigrate 模型集", tbl)
		}
	}
	// 含 row_kind 的唯一索引是「归档行与真实窗口不撞键」的结构保证。
	if !m.storeDB.Migrator().HasIndex("incident_impact_windows", "idx_incident_impact_window") {
		t.Error("缺少 idx_incident_impact_window：归档基线可能被真实窗口覆盖")
	}
}

// plan ID 必须随新表一起 bump，且两个常量同步。
func TestIncidentIngestPlanIDBumped(t *testing.T) {
	src := readSourceForPlanCheck(t, "store_migration_backup.go")
	for _, want := range []string{"v60", "incident-ingest-v1"} {
		if n := countOccurrences(src, want); n < 2 {
			t.Errorf("plan ID 标记 %q 应在两个常量里都出现，实际 %d 次——"+
				"只改一个会让 source-v2 库复用错误的迁移前快照", want, n)
		}
	}
	if countOccurrences(src, "from-v59") < 2 {
		t.Error("plan 串应保留 from-v59 回溯段（既有惯例）")
	}
	store := readSourceForPlanCheck(t, "store.go")
	for _, model := range []string{
		"&IncidentIngestCursor{}", "&IncidentIngestWindowState{}",
		"&IncidentIngestRevision{}", "&IncidentIngestGapSummary{}",
		"&IncidentImpactWindow{}",
	} {
		if countOccurrences(store, model) < 1 {
			t.Errorf("模型 %s 未注册进 AutoMigrate，生产上会报「表不存在」", model)
		}
	}
}

// 旧库（无采集表）升级后：新表建出来、既有事故数据不丢。
func TestIncidentIngestMigrationKeepsExistingData(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/t.db"
	m := openIncidentTestStore2(t, path)

	// 写一条事故与一条采集状态。
	if err := m.storeDB.Save(&Incident{
		ID: "keep-1", DedupKey: "dk-keep", ActiveMarker: incidentActiveMarker,
		Status: "open", PrimaryFaultClass: "unknown", FirstSeenTs: 1, LastSeenTs: 2,
	}).Error; err != nil {
		t.Fatalf("写事故失败: %v", err)
	}
	seedWindowState(t, m, windowState("dk-keep", 0, 300,
		incidentProcessDone, incidentWindowStateNormal, true))

	// 模拟旧库：删掉新表后重开，应被补齐且旧数据不丢。
	for _, tbl := range []string{
		"incident_ingest_cursors", "incident_ingest_window_states",
		"incident_ingest_revisions", "incident_ingest_gap_summaries",
		"incident_impact_windows",
	} {
		if err := m.storeDB.Migrator().DropTable(tbl); err != nil {
			t.Fatalf("删表 %s 失败: %v", tbl, err)
		}
	}
	closeTestStore(m)

	m2 := openIncidentTestStore2(t, path)
	for _, tbl := range []string{
		"incident_ingest_cursors", "incident_ingest_window_states",
		"incident_ingest_revisions", "incident_ingest_gap_summaries",
		"incident_impact_windows",
	} {
		if !m2.storeDB.Migrator().HasTable(tbl) {
			t.Errorf("重开后表 %s 未被补齐", tbl)
		}
	}
	var inc Incident
	if err := m2.storeDB.Where("id = ?", "keep-1").Take(&inc).Error; err != nil {
		t.Errorf("既有事故数据不得丢失: %v", err)
	}
}

func openIncidentTestStore2(t *testing.T, path string) *Monitor {
	t.Helper()
	m := &Monitor{cfg: Settings{SessionSecret: "test-secret"}, chNames: map[string]string{}}
	if err := m.openStore(path); err != nil {
		t.Fatalf("openStore: %v", err)
	}
	cleanupLocalTestStore(t, m)
	return m
}

func closeTestStore(m *Monitor) {
	if m == nil || m.storeDB == nil {
		return
	}
	if db, err := m.storeDB.DB(); err == nil {
		_ = db.Close()
	}
}

// ★ 接口的 scenario_pending 只从显式状态读取，绝不从 severity 反推 ★
func TestIncidentHTTPScenarioPendingFollowsStateNotSeverity(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)

	// 事故 A：severity 非空，但显式状态为待确认 → 接口必须报 pending。
	if err := m.storeDB.Save(&Incident{
		ID: "a", DedupKey: "dk-a", ActiveMarker: incidentActiveMarker, Status: "open",
		PrimaryFaultClass: "unknown", Severity: "SEV2", FirstSeenTs: 1, LastSeenTs: 2,
	}).Error; err != nil {
		t.Fatalf("写事故失败: %v", err)
	}
	rowA := windowState("dk-a", 0, 300, incidentProcessDone, incidentWindowStateAnomaly, true)
	// IncidentID 必须有：待确认结论按事故身份读取，不按 dedup_key——
	// 否则同键的其他事故会冒充本事故的结论。
	rowA.IncidentID = "a"
	rowA.ScenarioPending = true
	rowA.PendingReason = incidentPendingReasonNoFallback
	rowA.Severity = "SEV2"
	seedWindowState(t, m, rowA)

	// 事故 B：severity 为空，但显式状态**不是**待确认 → 接口不得报 pending。
	if err := m.storeDB.Save(&Incident{
		ID: "b", DedupKey: "dk-b", ActiveMarker: incidentActiveMarker + 1, Status: "open",
		PrimaryFaultClass: "unknown", Severity: "", FirstSeenTs: 1, LastSeenTs: 3,
	}).Error; err != nil {
		t.Fatalf("写事故失败: %v", err)
	}
	rowB := windowState("dk-b", 0, 300, incidentProcessDone, incidentWindowStateAnomaly, true)
	rowB.IncidentID = "b"
	rowB.ScenarioPending = false
	seedWindowState(t, m, rowB)

	body := doIncidentRequest(t, m, "/incidents", m.serveIncidentList)
	items, _ := body["incidents"].([]any)
	if len(items) != 2 {
		t.Fatalf("应返回 2 条事故，实际 %d", len(items))
	}
	got := map[string]bool{}
	reasons := map[string]string{}
	for _, raw := range items {
		it := raw.(map[string]any)
		id := it["id"].(string)
		got[id], _ = it["scenario_pending"].(bool)
		reasons[id], _ = it["pending_reason"].(string)
	}
	if !got["a"] {
		t.Error("severity 非空但显式状态待确认时，接口必须报 scenario_pending=true（不得跟随 severity）")
	}
	if reasons["a"] != incidentPendingReasonNoFallback {
		t.Errorf("原因应为固定枚举，实际 %q", reasons["a"])
	}
	if got["b"] {
		t.Error("severity 为空但显式状态非待确认时，接口不得报 scenario_pending=true（不得反推）")
	}
}

// 空严重度既不显示为 SEV3 也不显示为正常，且带不可处置说明。
func TestIncidentHTTPEmptySeverityIsNotASevLevel(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	if err := m.storeDB.Save(&Incident{
		ID: "e", DedupKey: "dk-e", ActiveMarker: incidentActiveMarker, Status: "open",
		PrimaryFaultClass: "unknown", Severity: "", FirstSeenTs: 1, LastSeenTs: 2,
	}).Error; err != nil {
		t.Fatalf("写事故失败: %v", err)
	}
	rowE := windowState("dk-e", 0, 300, incidentProcessDone, incidentWindowStateAnomaly, true)
	rowE.ScenarioPending = true
	rowE.PendingReason = incidentPendingReasonNoFallback
	seedWindowState(t, m, rowE)

	body := doIncidentRequest(t, m, "/incidents", m.serveIncidentList)
	items := body["incidents"].([]any)
	it := items[0].(map[string]any)
	if sev, _ := it["severity"].(string); sev != "" {
		t.Errorf("空严重度不得被填成某个 SEV，实际 %q", sev)
	}
	if reason, _ := it["severity_reason"].(string); reason == "" {
		t.Error("空严重度必须附带「尚未定级，不可据此处置」说明")
	}
	if body["anomaly_rate_caveat"] == nil {
		t.Error("响应必须携带异常率口径说明")
	}
}

// 列表分页与范围校验。
func TestIncidentHTTPPaginationAndRangeGuards(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	for i := 0; i < 5; i++ {
		id := string(rune('A' + i))
		if err := m.storeDB.Save(&Incident{
			ID: id, DedupKey: "dk-" + id, ActiveMarker: int64(incidentActiveMarker + i),
			Status: "open", PrimaryFaultClass: "unknown",
			FirstSeenTs: int64(i + 1), LastSeenTs: int64(i + 10),
		}).Error; err != nil {
			t.Fatalf("写事故失败: %v", err)
		}
	}
	body := doIncidentRequest(t, m, "/incidents?limit=2", m.serveIncidentList)
	if items := body["incidents"].([]any); len(items) != 2 {
		t.Errorf("limit=2 应返回 2 条，实际 %d", len(items))
	}
	if more, _ := body["has_more"].(bool); !more {
		t.Error("还有更多数据时 has_more 必须为 true")
	}
	// 上界夹取：limit 过大被压到最大值，不得扫全表。
	body2 := doIncidentRequest(t, m, "/incidents?limit=999999", m.serveIncidentList)
	if lim, _ := body2["limit"].(float64); lim > 200 {
		t.Errorf("limit 必须被夹取到上界，实际 %v", lim)
	}
	// 范围校验。
	if _, code := doIncidentRequestCode(t, m, "/incidents?from=100&to=50", m.serveIncidentList); code != http.StatusBadRequest {
		t.Errorf("to <= from 应返回 400，实际 %d", code)
	}
	if _, code := doIncidentRequestCode(t, m, "/incidents?from=1&to=99999999999", m.serveIncidentList); code != http.StatusBadRequest {
		t.Errorf("范围过宽应返回 400，实际 %d", code)
	}
}

// 健康接口必须暴露两个水位、两类积压与缺口清单。
func TestIncidentHealthExposesWatermarksAndGaps(t *testing.T) {
	incidentIngestTestEnv(t)
	m := openIncidentTestStore(t)
	seedWindowState(t, m, windowState("dk-h", 0, 300,
		incidentProcessFailedPermanent, incidentWindowStateAnomaly, false))

	body := doIncidentRequest(t, m, "/incidents/health", m.serveIncidentIngestHealth)
	wm, ok := body["watermarks"].(map[string]any)
	if !ok {
		t.Fatal("缺少 watermarks")
	}
	for _, k := range []string{"scan_through_ts", "complete_through_ts"} {
		if _, ok := wm[k]; !ok {
			t.Errorf("watermarks 缺少 %s", k)
		}
	}
	bl, ok := body["backlog"].(map[string]any)
	if !ok {
		t.Fatal("缺少 backlog")
	}
	// 两类必须分开暴露。
	if _, ok := bl["pending_backlog"]; !ok {
		t.Error("缺少 pending_backlog（仍可推进的任务）")
	}
	if tg, _ := bl["terminal_gap_count"].(float64); tg < 1 {
		t.Errorf("永久失败应计入 terminal_gap_count，实际 %v", bl["terminal_gap_count"])
	}
	// 永久失败不得计入待处理积压。
	if pb, _ := bl["pending_backlog"].(float64); pb != 0 {
		t.Errorf("永久失败不得计入 pending_backlog，实际 %v", pb)
	}
	if lag, ok := body["revision_detect_lag"].(map[string]any); !ok {
		t.Error("缺少 revision_detect_lag")
	} else if c, _ := lag["caveat"].(string); c == "" {
		t.Error("迟到指标必须随 caveat 一起输出（它不是日志实际迟到时长）")
	}
	if srcs, _ := body["unconnected_sources"].([]any); len(srcs) == 0 {
		t.Error("必须暴露未接通的数据来源清单")
	}
	if limits, _ := body["known_limits"].([]any); len(limits) == 0 {
		t.Error("必须暴露已知能力边界")
	}
}

// 核心开关关闭时，接口只报未开启，不读业务数据。
func TestIncidentHTTPDisabledWhenCoreOff(t *testing.T) {
	t.Setenv("MONITOR_INCIDENT_CORE_ENABLED", "false")
	m := openIncidentTestStore(t)
	for _, tc := range []struct {
		path string
		h    gin.HandlerFunc
	}{
		{"/incidents", m.serveIncidentList},
		{"/incidents/health", m.serveIncidentIngestHealth},
	} {
		body := doIncidentRequest(t, m, tc.path, tc.h)
		if en, _ := body["enabled"].(bool); en {
			t.Errorf("%s 在核心关闭时不得报 enabled=true", tc.path)
		}
	}
}

func doIncidentRequest(t *testing.T, m *Monitor, path string, h gin.HandlerFunc) map[string]any {
	t.Helper()
	body, code := doIncidentRequestCode(t, m, path, h)
	if code != http.StatusOK {
		t.Fatalf("%s 应返回 200，实际 %d: %v", path, code, body)
	}
	return body
}

func doIncidentRequestCode(t *testing.T, m *Monitor, path string, h gin.HandlerFunc) (map[string]any, int) {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.GET(pathOnly(path), h)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	r.ServeHTTP(w, req)
	out := map[string]any{}
	_ = json.Unmarshal(w.Body.Bytes(), &out)
	return out, w.Code
}

func pathOnly(p string) string {
	for i := 0; i < len(p); i++ {
		if p[i] == '?' {
			return p[:i]
		}
	}
	return p
}
