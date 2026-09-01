package monitor

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

func postNginx(t *testing.T, m *Monitor, body, token string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.Use(requestBodyLimit(maxJSONRequestBody))
	r.POST("/internal/nginx", m.ingestNginx)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx", strings.NewReader(body))
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestNginxIngestDisabledAndAuthenticated(t *testing.T) {
	m := newTestMonitor(t)
	if got := postNginx(t, m, `{}`, "").Code; got != http.StatusServiceUnavailable {
		t.Fatalf("默认关闭应返回 503，得 %d", got)
	}
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	if got := postNginx(t, m, `{}`, "wrong").Code; got != http.StatusUnauthorized {
		t.Fatalf("错误 token 应返回 401，得 %d", got)
	}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"batch_abcdefgh","samples":[{"bucket_ts":%d,"route":"/api/status","method":"GET","status":200,"count":1}]}`, bucket)
	if got := postNginx(t, m, body, "secret").Code; got != http.StatusBadRequest {
		t.Fatalf("启用采集但未配置节点白名单时必须拒绝，得 %d", got)
	}
	allowedNode := strings.Repeat("m", 64)
	m.cfg.NginxAllowedNodes = []string{allowedNode}
	longNode := allowedNode + "-forged-suffix"
	body = fmt.Sprintf(`{"node":%q,"batch_id":"batch_abcdefgh","samples":[]}`, longNode)
	if got := postNginx(t, m, body, "secret").Code; got != http.StatusBadRequest {
		t.Fatalf("超过长度限制的节点名不能被截断后冒充白名单节点，得 %d", got)
	}
}

func TestValidateNginxSettingsFailsClosed(t *testing.T) {
	if err := validateNginxSettings(Settings{}); err != nil {
		t.Fatalf("默认关闭不应增加启动条件: %v", err)
	}
	if err := validateNginxSettings(Settings{NginxEnabled: true, NginxAllowedNodes: []string{"master"}}); err == nil {
		t.Fatal("已启用但无 ingest token 必须拒绝启动")
	}
	if err := validateNginxSettings(Settings{NginxEnabled: true, IngestToken: "secret"}); err == nil {
		t.Fatal("已启用但无节点白名单必须拒绝启动")
	}
	if err := validateNginxSettings(Settings{NginxEnabled: true, IngestToken: "secret", NginxAllowedNodes: []string{"master", "master"}}); err == nil {
		t.Fatal("重复节点会造成状态页重复，必须拒绝")
	}
	if err := validateNginxSettings(Settings{NginxEnabled: true, IngestToken: "secret", NginxAllowedNodes: []string{"master", "slave"}}); err != nil {
		t.Fatalf("完整配置应通过: %v", err)
	}
	baseV2 := Settings{NginxEnabled: true, IngestToken: "secret", NginxAllowedNodes: []string{"master"}, NginxSourceV2Enabled: true, NginxSourceV2AllowedNodes: []string{"master"}}
	if err := validateNginxSettings(baseV2); err == nil {
		t.Fatal("v2 必须要求显式的逐 lane 白名单")
	}
	baseV2.NginxSourceV2AllowedLanes = []string{"master:access"}
	if err := validateNginxSettings(baseV2); err != nil {
		t.Fatalf("合法 v2 prepare 配置被拒绝: %v", err)
	}
	baseV2.NginxSourceV2CutoverEnabled = true
	if err := validateNginxSettings(baseV2); err != nil {
		t.Fatalf("合法 v2 cutover 配置被拒绝: %v", err)
	}
	baseV2.NginxSourceV2AllowedLanes = []string{"master:access", "master:access"}
	if err := validateNginxSettings(baseV2); err == nil {
		t.Fatal("重复 v2 lane 必须拒绝")
	}
	if err := validateNginxSettings(Settings{NginxEvidenceMode: "unknown"}); err == nil {
		t.Fatal("未知 evidence 模式不能静默降级为 off")
	}
	if err := validateNginxSettings(Settings{NginxSourceV2CutoverEnabled: true}); err == nil {
		t.Fatal("cutover 不得绕过 source v2 schema/prepare 开关")
	}
	if err := validateNginxSettings(Settings{NginxEvidenceMode: "pilot"}); err == nil {
		t.Fatal("evidence 不能在 Nginx 分钟采集关闭时单独启用")
	}
	validEvidence := Settings{NginxEnabled: true, IngestToken: "secret", NginxAllowedNodes: []string{"master"},
		NginxEvidenceMode: "pilot", NginxEvidenceStorePath: "evidence.db", NginxEvidenceRetentionHours: 168, NginxEvidenceHMACKey: strings.Repeat("k", 32), NginxEvidenceHMACKeyID: "key-1", NginxEvidenceMaxMiB: 512}
	if err := validateNginxSettings(validEvidence); err != nil {
		t.Fatalf("完整 evidence 灰度配置应通过: %v", err)
	}
}

func TestNginxIngestIdempotentAndReport(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"batch_abcdefgh","backlog_bytes":4096,"backlog_known":true,"cursor_discontinuities":2,"last_cursor_discontinuity_at":%d,"discarded_lines":3,"last_discarded_at":%d,"samples":[
		{"bucket_ts":%d,"route":"/v1/responses?key=must-not-store","method":"post","status":200,"upstream_status":200,"count":2,"request_time_sum_ms":300,"request_time_max_ms":200,"upstream_time_sum_ms":250,"upstream_time_count":2,"bytes_sent":1000,"request_id_present":2},
		{"bucket_ts":%d,"route":"/api/user/123","method":"GET","status":503,"upstream_status":503,"count":1,"request_time_sum_ms":500,"request_time_max_ms":500,"upstream_time_sum_ms":450,"upstream_time_count":1,"bytes_sent":200,"request_id_present":0}
	]}`, time.Now().Unix(), time.Now().Unix(), bucket, bucket)
	for i := 0; i < 2; i++ {
		w := postNginx(t, m, body, "secret")
		if w.Code != http.StatusOK {
			t.Fatalf("第 %d 次推送失败: %d %s", i+1, w.Code, w.Body.String())
		}
	}
	var count struct{ N int64 }
	if err := m.storeDB.Raw("SELECT COALESCE(SUM(count),0) n FROM nginx_minute_samples").Scan(&count).Error; err != nil || count.N != 3 {
		t.Fatalf("同 batch 重试不应重复累计: n=%d err=%v", count.N, err)
	}
	var rawPaths int64
	m.storeDB.Model(&NginxMinuteSample{}).Where("route LIKE '%?%' OR route LIKE '%123%'").Count(&rawPaths)
	if rawPaths != 0 {
		t.Fatalf("query 或动态路径不应入库")
	}
	var state NginxSourceState
	if err := m.storeDB.First(&state, "node = ?", "master").Error; err != nil {
		t.Fatal(err)
	}
	if !state.BacklogKnown || state.BacklogBytes != 4096 || state.CursorDiscontinuities != 2 || state.LastCursorDiscontinuityAt == 0 || state.DiscardedLines != 3 || state.LastDiscardedAt == 0 {
		t.Fatalf("采集器积压/游标状态未正确保存: %+v", state)
	}
}

func TestNginxIngestRejectsReusedBatchIDWithDifferentPayload(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	bucket := time.Now().Unix() / 60 * 60
	body := func(count int) string {
		return fmt.Sprintf(`{"node":"master","batch_id":"batch_conflict_abcdefgh","samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"upstream_status":200,"count":%d,"request_time_sum_ms":%d,"request_time_max_ms":100,"upstream_time_sum_ms":%d,"upstream_time_count":%d,"bytes_sent":%d,"request_id_present":%d}]}`,
			bucket, count, count*100, count*80, count, count*1000, count)
	}
	if w := postNginx(t, m, body(1), "secret"); w.Code != http.StatusOK {
		t.Fatalf("first ingest: %d %s", w.Code, w.Body.String())
	}
	if w := postNginx(t, m, body(2), "secret"); w.Code != http.StatusConflict {
		t.Fatalf("same batch id with changed payload must conflict: %d %s", w.Code, w.Body.String())
	}
	var total int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil {
		t.Fatal(err)
	}
	if total != 1 {
		t.Fatalf("conflicting payload changed aggregate: %d", total)
	}
	var batch NginxIngestBatch
	if err := m.storeDB.First(&batch, "node = ? AND batch_id = ?", "master", "batch_conflict_abcdefgh").Error; err != nil {
		t.Fatal(err)
	}
	if len(batch.PayloadHash) != 64 {
		t.Fatalf("server-computed payload hash missing: %q", batch.PayloadHash)
	}
}

func TestNginxV1BoundaryRetryAdoptionAndConflict(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	bucket := time.Now().Unix() / 60 * 60
	legacy := fmt.Sprintf(`{"node":"master","batch_id":"boundary_batch_abcdefgh","samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, m, legacy, "secret"); w.Code != http.StatusOK {
		t.Fatalf("legacy first ingest: %d %s", w.Code, w.Body.String())
	}
	enableTestNginxSourceContinuity(t, m)
	bound := fmt.Sprintf(`{"node":"master","batch_id":"boundary_batch_abcdefgh","source_boundary":{"device":7,"inode":91,"start_offset":100,"end_offset":180},"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, m, bound, "secret"); w.Code != http.StatusOK {
		t.Fatalf("boundary adoption retry: %d %s", w.Code, w.Body.String())
	} else if !strings.Contains(w.Body.String(), `"source_boundary_ack":{"protocol":1`) {
		t.Fatalf("boundary persistence was not acknowledged: %s", w.Body.String())
	}
	if w := postNginx(t, m, legacy, "secret"); w.Code != http.StatusOK {
		t.Fatalf("old collector rollback must retry the same payload without erasing boundary: %d %s", w.Code, w.Body.String())
	}
	var state NginxSourceBoundaryStateV1
	if err := m.storeDB.First(&state, "node = ? AND kind = ?", "master", "access").Error; err != nil {
		t.Fatal(err)
	}
	if state.LastBatchID != "boundary_batch_abcdefgh" || state.Device != 7 || state.Inode != 91 || state.LastOffset != 180 {
		t.Fatalf("server boundary was not adopted: %+v", state)
	}
	conflict := fmt.Sprintf(`{"node":"master","batch_id":"boundary_batch_abcdefgh","source_boundary":{"device":7,"inode":91,"start_offset":100,"end_offset":181},"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, m, conflict, "secret"); w.Code != http.StatusConflict {
		t.Fatalf("conflicting boundary must fail: %d %s", w.Code, w.Body.String())
	}
	var total int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil || total != 1 {
		t.Fatalf("boundary retry changed aggregates: total=%d err=%v", total, err)
	}
	manifest := testSourceManifest("access", 180)
	manifest.CutoverFromV1, manifest.LegacyCursorDevice, manifest.LegacyCursorInode, manifest.LegacyCursorOffset = true, 7, 91, 180
	manifest.LegacyAckedBatchID = "boundary_batch_abcdefgh"
	manifest.Files[0].Device, manifest.Files[0].Inode = 7, 91
	migrateTestNginxSourceV2(t, m.storeDB)
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := registerNginxSourceManifestV2(tx, "master", manifest.LegacyAckedBatchID, manifest, time.Now().Unix())
		return err
	}); err != nil {
		t.Fatalf("exact persisted boundary must permit v2 cutover: %v", err)
	}
}

func TestNginxV1BoundaryFailsClosedBeforePreparationWithoutMutatingLegacyTables(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"premature_boundary_abcdefgh","source_boundary":{"device":7,"inode":91,"start_offset":0,"end_offset":100},"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, m, body, "secret"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("premature boundary=%d %s", w.Code, w.Body.String())
	}
	var batches, samples, states int64
	if err := m.storeDB.Model(&NginxIngestBatch{}).Count(&batches).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NginxMinuteSample{}).Count(&samples).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NginxSourceState{}).Count(&states).Error; err != nil {
		t.Fatal(err)
	}
	if batches != 0 || samples != 0 || states != 0 {
		t.Fatalf("rejected preparation mutated legacy state: batches=%d samples=%d states=%d", batches, samples, states)
	}
}

func TestNginxV1BoundaryCannotAdoptAnOlderAcceptedBatch(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	bucket := time.Now().Unix() / 60 * 60
	body := func(id string) string {
		return fmt.Sprintf(`{"node":"master","batch_id":%q,"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, id, bucket)
	}
	if w := postNginx(t, m, body("older_batch_abcdefgh"), "secret"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	if w := postNginx(t, m, body("latest_batch_abcdefgh"), "secret"); w.Code != http.StatusOK {
		t.Fatal(w.Body.String())
	}
	enableTestNginxSourceContinuity(t, m)
	stale := fmt.Sprintf(`{"node":"master","batch_id":"older_batch_abcdefgh","source_boundary":{"device":7,"inode":91,"start_offset":0,"end_offset":100},"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, m, stale, "secret"); w.Code != http.StatusConflict {
		t.Fatalf("older batch adopted as current boundary: %d %s", w.Code, w.Body.String())
	}
	var boundaryCount int64
	if err := m.storeDB.Model(&NginxSourceBoundaryStateV1{}).Where("node = ? AND kind = ?", "master", "access").Count(&boundaryCount).Error; err != nil || boundaryCount != 0 {
		t.Fatalf("stale adoption created boundary state: count=%d err=%v", boundaryCount, err)
	}
}

func TestNginxV1CheckpointBindsIdleCursor(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	enableTestNginxSourceContinuity(t, m)
	body := `{"node":"master","batch_id":"hb_boundary_abcdefgh","source_boundary":{"device":7,"inode":92,"start_offset":700,"end_offset":700,"checkpoint":true},"samples":[]}`
	if w := postNginx(t, m, body, "secret"); w.Code != http.StatusOK {
		t.Fatalf("checkpoint: %d %s", w.Code, w.Body.String())
	}
	var state NginxSourceBoundaryStateV1
	if err := m.storeDB.First(&state, "node = ? AND kind = ?", "master", "access").Error; err != nil {
		t.Fatal(err)
	}
	if state.LastBatchID != "hb_boundary_abcdefgh" || state.Device != 7 || state.Inode != 92 || state.LastOffset != 700 {
		t.Fatalf("checkpoint did not bind idle cursor: %+v", state)
	}
	bucket := time.Now().Unix() / 60 * 60
	invalid := fmt.Sprintf(`{"node":"master","batch_id":"hb_with_samples_abcdefgh","source_boundary":{"device":7,"inode":92,"start_offset":700,"end_offset":700,"checkpoint":true},"samples":[{"bucket_ts":%d,"route":"/api/status","method":"GET","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, m, invalid, "secret"); w.Code != http.StatusBadRequest {
		t.Fatalf("checkpoint with samples must fail: %d %s", w.Code, w.Body.String())
	}
}

func TestNginxLegacyAccessIngestIsAtomicallyClosedAfterV2Manifest(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	registerTestManifest(t, m.storeDB, "access", 0)
	m.nginxSourceV2SchemaReady.Store(true)
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"legacy_after_v2_abcdefgh","samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	w := postNginx(t, m, body, "secret")
	if w.Code != http.StatusConflict {
		t.Fatalf("legacy access ingest after v2 cutover=%d body=%s", w.Code, w.Body.String())
	}
	var batches, samples int64
	m.storeDB.Model(&NginxIngestBatch{}).Where("batch_id = ?", "legacy_after_v2_abcdefgh").Count(&batches)
	m.storeDB.Model(&NginxMinuteSample{}).Count(&samples)
	if batches != 0 || samples != 0 {
		t.Fatalf("rejected legacy access mutated state: batches=%d samples=%d", batches, samples)
	}
}

func TestNginxEdgeReportNumbers(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxRetentionDays = true, 7
	m.cfg.NginxAllowedNodes = []string{"master", "slave"}
	bucket := time.Now().Unix() / 60 * 60
	rows := []NginxMinuteSample{
		{BucketTs: bucket, Node: "master", Route: "/v1/responses", Method: "POST", Status: 200, UpstreamStatus: 200, Count: 8, RequestTimeSumMS: 800, RequestTimeMaxMS: 150, UpstreamTimeSumMS: 640, UpstreamTimeCount: 8, BytesSent: 8000, RequestIDPresent: 6},
		{BucketTs: bucket, Node: "slave", Route: "/v1/responses", Method: "POST", Status: 502, UpstreamStatus: 502, Count: 2, RequestTimeSumMS: 600, RequestTimeMaxMS: 400, UpstreamTimeSumMS: 500, UpstreamTimeCount: 2, BytesSent: 200, RequestIDPresent: 1},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&[]NginxSourceState{{Node: "master", LastEventTs: bucket, LastIngestTs: time.Now().Unix(), BacklogKnown: true}, {Node: "slave", LastEventTs: bucket, LastIngestTs: time.Now().Unix(), BacklogKnown: true}}).Error; err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	recorder := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(recorder)
	c.Request = httptest.NewRequest(http.MethodGet, "/stability/edge?days=1", nil)
	m.serveNginxEdge(c)
	if recorder.Code != http.StatusOK {
		t.Fatalf("report: %d %s", recorder.Code, recorder.Body.String())
	}
	var report NginxEdgeReport
	if err := json.Unmarshal(recorder.Body.Bytes(), &report); err != nil {
		t.Fatal(err)
	}
	if report.Summary.Requests != 10 || report.Summary.Status2xx != 8 || report.Summary.Status5xx != 2 || report.Summary.MaxRequestMS != 400 {
		t.Fatalf("summary 口径错误: %+v", report.Summary)
	}
	if report.RetentionDays != 7 {
		t.Fatalf("应明确返回入口聚合留存期，得 %d", report.RetentionDays)
	}
	if report.Summary.AvgRequestMS != 140 || report.Summary.RequestIDCoverage != 70 {
		t.Fatalf("平均耗时/Request ID 携带率错误: %+v", report.Summary)
	}
	if len(report.Routes) != 1 || len(report.Nodes) != 2 || len(report.Daily) != 1 || len(report.Sources) != 2 {
		t.Fatalf("breakdown 不完整: routes=%d nodes=%d daily=%d sources=%d", len(report.Routes), len(report.Nodes), len(report.Daily), len(report.Sources))
	}
	connected, healthy, total, _, coverage := m.nginxSourceSummary(context.Background(), time.Now().Unix())
	if !connected || healthy != 2 || total != 2 {
		t.Fatalf("source summary health wrong: connected=%v healthy=%d total=%d", connected, healthy, total)
	}
	if coverage == nil || *coverage != 70 {
		t.Fatalf("source summary 携带率错误: %v", coverage)
	}
}

func TestNginxQueryToTsIncludesCurrentMinuteAtExactBoundary(t *testing.T) {
	now := time.Date(2026, 8, 8, 17, 0, 0, 0, cstLocation).Unix()
	scope := stabilityScope{FromTs: now - 86400, ToTs: now}
	if got := nginxQueryToTs(scope, now); got != now+1 {
		t.Fatalf("current minute boundary must be included: got=%d want=%d", got, now+1)
	}
	historical := stabilityScope{FromTs: now - 2*86400, ToTs: now - 86400}
	if got := nginxQueryToTs(historical, now); got != historical.ToTs {
		t.Fatalf("historical half-open interval changed: got=%d want=%d", got, historical.ToTs)
	}
}

func TestValidateNginxSampleRejectsUnboundedValues(t *testing.T) {
	now := time.Now().Unix()
	base := nginxIngestSample{BucketTs: now, Route: "/v1/responses", Method: "POST", Status: 200, Count: 1}
	if _, err := validateNginxSample(base, now, 7); err != nil {
		t.Fatalf("合法样本被拒: %v", err)
	}
	bad := base
	bad.RequestIDPresent = 2
	if _, err := validateNginxSample(bad, now, 7); err == nil {
		t.Fatal("request_id_present > count 应拒绝")
	}
	bad = base
	bad.LatencyCount, bad.Latency0To1s = 1, 0
	if _, err := validateNginxSample(bad, now, 7); err == nil {
		t.Fatal("延迟样本数与直方图不守恒应拒绝")
	}
	bad = base
	bad.BucketTs = now - 10*86400
	if _, err := validateNginxSample(bad, now, 7); err == nil {
		t.Fatal("超留存窗口样本应拒绝")
	}
	for name, mutate := range map[string]func(*nginxIngestSample){
		"missing status": func(v *nginxIngestSample) { v.Status = 0 },
		"max exceeds sum": func(v *nginxIngestSample) {
			v.RequestTimeMaxMS, v.RequestTimeSumMS = 2, 1
		},
		"upstream sum without count": func(v *nginxIngestSample) { v.UpstreamTimeSumMS = 1 },
		"upstream sum exceeds count": func(v *nginxIngestSample) {
			v.UpstreamTimeCount, v.UpstreamTimeSumMS = 1, 86_400_001
		},
	} {
		t.Run(name, func(t *testing.T) {
			bad := base
			mutate(&bad)
			if _, err := validateNginxSample(bad, now, 7); err == nil {
				t.Fatalf("非守恒聚合必须拒绝: %+v", bad)
			}
		})
	}
	zeroUpstream := base
	zeroUpstream.UpstreamTimeCount = 1
	if _, err := validateNginxSample(zeroUpstream, now, 7); err != nil {
		t.Fatalf("0 ms upstream 是合法样本: %v", err)
	}
}

func TestApproximateLatencyPercentilesUseBoundedHistogram(t *testing.T) {
	if got := approximateLatencyPercentile(100, .95, 70, 20, 5, 3, 1, 1); got != 15000 {
		t.Fatalf("p95 bucket upper bound=%d want=15000", got)
	}
	if got := approximateLatencyPercentile(100, .99, 70, 20, 5, 3, 1, 1); got != 60000 {
		t.Fatalf("p99 bucket upper bound=%d want=60000", got)
	}
}

func TestNginxIngestRejectsInvalidCollectorTelemetry(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	for _, body := range []string{
		`{"node":"master","batch_id":"batch_abcdefgh","backlog_bytes":-1,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","backlog_bytes":1,"backlog_known":false,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","cursor_discontinuities":-1,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","last_cursor_discontinuity_at":1,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","cursor_discontinuities":1,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","discarded_lines":-1,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","last_discarded_at":1,"samples":[]}`,
		`{"node":"master","batch_id":"batch_abcdefgh","discarded_lines":1,"samples":[]}`,
		fmt.Sprintf(`{"node":"master","batch_id":"batch_abcdefgh","last_cursor_discontinuity_at":%d,"samples":[]}`, time.Now().Unix()+600),
	} {
		if got := postNginx(t, m, body, "secret").Code; got != http.StatusBadRequest {
			t.Fatalf("非法采集状态必须拒绝，得 %d body=%s", got, body)
		}
	}
}

func TestNginxIngestAcceptsCheckpointPersistFailureWithoutDroppedEvent(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	body := `{"node":"master","batch_id":"empty_checkpoint_failure_abcdefgh","evidence_persist_failures":1,"evidence_dropped_events":0,"samples":[]}`
	if w := postNginx(t, m, body, "secret"); w.Code != http.StatusOK {
		t.Fatalf("empty checkpoint persistence failure must not block minute lane: %d %s", w.Code, w.Body.String())
	}
}

func TestNginxRetentionAndAggregateOverflowAreBounded(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxRetentionDays = 999
	if got := m.nginxRetentionDays(); got != 7 {
		t.Fatalf("非法留存配置必须回落到 7 天，得 %d", got)
	}
	dst := NginxMinuteSample{BytesSent: int64(^uint64(0) >> 1)}
	if err := mergeNginxSample(&dst, NginxMinuteSample{BytesSent: 1}); err == nil {
		t.Fatal("聚合整数溢出必须被拒绝")
	}
}

func TestNginxSourcesExposeMissingAllowedNode(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled = true
	m.cfg.NginxAllowedNodes = []string{"master", "slave"}
	now := time.Now().Unix()
	if err := m.storeDB.Create(&NginxSourceState{Node: "master", LastEventTs: now, LastIngestTs: now, BacklogKnown: true}).Error; err != nil {
		t.Fatal(err)
	}
	rows := m.nginxSources(context.Background(), now)
	if len(rows) != 2 || rows[0].Node != "master" || rows[0].Status != "ok" || rows[1].Node != "slave" || rows[1].Status != "bad" || rows[1].AgeSec != -1 || len(rows[1].HealthReasons) != 1 || rows[1].HealthReasons[0] != "source_missing" {
		t.Fatalf("白名单内未上报节点必须明确显示中断: %+v", rows)
	}
	connected, healthy, total, _, _ := m.nginxSourceSummary(context.Background(), now)
	if connected || healthy != 1 || total != 2 {
		t.Fatalf("不能因为一个节点健康就显示 Nginx 整体已接入: connected=%v healthy=%d total=%d", connected, healthy, total)
	}
}

func TestNginxSourcesRejectFreshHeartbeatWithStaleEventStream(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled = true
	m.cfg.NginxAllowedNodes = []string{"master"}
	now := time.Now().Unix()
	if err := m.storeDB.Create(&NginxSourceState{Node: "master", LastEventTs: now - nginxEventLagBadSec - 1, LastIngestTs: now, BacklogKnown: true, BacklogBytes: 0}).Error; err != nil {
		t.Fatal(err)
	}
	rows := m.nginxSources(context.Background(), now)
	if len(rows) != 1 || rows[0].Status != "bad" || !slices.Contains(rows[0].HealthReasons, "event_stream_stale") {
		t.Fatalf("fresh heartbeat/backlog=0 hid a stale access stream: %+v", rows)
	}
}
