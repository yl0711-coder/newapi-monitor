package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"
)

type countingReader struct {
	reads int
	data  *strings.Reader
}

func (r *countingReader) Read(p []byte) (int, error) {
	r.reads++
	return r.data.Read(p)
}

const (
	testSourceEpoch = "0123456789abcdef0123456789abcdef"
	testSourceFile  = testSourceEpoch + "-0000000000000001"
	testContentA    = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	testContentB    = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
	testPayloadA    = "cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
)

func testSourceRange(start, end int64, content string) nginxSourceRangeV2 {
	return nginxSourceRangeV2{Protocol: 2, Kind: "access", SourceEpoch: testSourceEpoch, FileID: testSourceFile, StartOffset: start, EndOffset: end, ContentSHA256: content}
}

func testSourceManifest(kind string, base int64) nginxSourceManifestV2 {
	return nginxSourceManifestV2{Protocol: 2, Kind: kind, SourceEpoch: testSourceEpoch, Files: []nginxSourceManifestFileV2{{
		FileID: testSourceFile, Generation: 1, Device: 1, Inode: 10, BaseOffset: base, Current: true,
	}}}
}

func registerTestManifest(t *testing.T, db *gorm.DB, kind string, base int64) {
	t.Helper()
	migrateTestNginxSourceV2(t, db)
	err := db.Transaction(func(tx *gorm.DB) error {
		_, _, err := registerNginxSourceManifestV2(tx, "master", "", testSourceManifest(kind, base), 99)
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
}

func migrateTestNginxSourceV2(t *testing.T, db *gorm.DB) {
	t.Helper()
	if err := migrateNginxSourceV2Schema(db); err != nil {
		t.Fatal(err)
	}
}

func enableTestNginxSourceContinuity(t *testing.T, m *Monitor) {
	t.Helper()
	migrateTestNginxSourceV2(t, m.storeDB)
	m.nginxSourceV2SchemaReady.Store(true)
}

func seedTestLegacyBoundary(t *testing.T, m *Monitor, kind, batchID string, device, inode uint64, offset int64) {
	t.Helper()
	enableTestNginxSourceContinuity(t, m)
	if err := m.storeDB.Create(&NginxSourceBoundaryStateV1{Node: "master", Kind: kind, LastBatchID: batchID, Device: device, Inode: inode, LastOffset: offset, LastUpdatedAt: 98}).Error; err != nil {
		t.Fatal(err)
	}
}

func TestNginxSourceV2SchemaIsAbsentWhenFeatureIsDisabled(t *testing.T) {
	m := newTestMonitor(t)
	for _, model := range []any{&NginxSourceBoundaryBatchV1{}, &NginxSourceBoundaryStateV1{}, &NginxCollectorProtocolState{}, &NginxSourceFileWatermark{}, &NginxSourceRangeBatch{}, &NginxSourceCommitV2{}, &NginxCollectorEpochRecovery{}} {
		if m.storeDB.Migrator().HasTable(model) {
			t.Fatalf("disabled source v2 migrated %T", model)
		}
	}
	if m.nginxSourceV2SchemaReady.Load() {
		t.Fatal("disabled source v2 reported its isolated schema ready")
	}
}

func TestNginxSourceV2PrepareAcceptsV1BoundaryButKeepsCutoverAPIClosed(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled = true
	m.cfg.NginxSourceV2AllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2AllowedLanes = []string{"master:access"}
	enableTestNginxSourceContinuity(t, m)

	boundary := `{"node":"master","batch_id":"prepare_boundary_abcdefgh","source_boundary":{"device":7,"inode":91,"start_offset":0,"end_offset":100},"samples":[]}`
	if w := postNginx(t, m, boundary, "secret"); w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"source_boundary_ack"`) {
		t.Fatalf("prepare boundary was not durably acknowledged: %d %s", w.Code, w.Body.String())
	}

	r := gin.New()
	r.GET("/internal/nginx-source/v2/capabilities", m.nginxSourceCapabilitiesV2)
	req := httptest.NewRequest(http.MethodGet, "/internal/nginx-source/v2/capabilities", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("prepare-only configuration exposed irreversible cutover API: %d %s", w.Code, w.Body.String())
	}

	var protocols, watermarks, manifests int64
	if err := m.storeDB.Model(&NginxCollectorProtocolState{}).Count(&protocols).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NginxSourceFileWatermark{}).Count(&watermarks).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&NginxCollectorEpochRecovery{}).Count(&manifests).Error; err != nil {
		t.Fatal(err)
	}
	if protocols != 0 || watermarks != 0 || manifests != 0 {
		t.Fatalf("prepare-only configuration created cutover state: protocols=%d watermarks=%d manifests=%d", protocols, watermarks, manifests)
	}
}

func TestNginxSourceV2ExistingLaneContinuesAfterNewCutoverPermissionCloses(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken = true, true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2CutoverEnabled = true, true
	m.cfg.NginxSourceV2AllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2AllowedLanes = []string{"master:access", "master:error"}
	enableTestNginxSourceContinuity(t, m)

	manifest := testSourceManifest("access", 0)
	if w := postSourceManifestV2(t, m, manifest); w.Code != http.StatusOK {
		t.Fatalf("initial cutover: %d %s", w.Code, w.Body.String())
	}
	if !m.nginxSourceV2Active.Load() {
		t.Fatal("durable cutover did not activate existing-lane runtime")
	}
	m.cfg.NginxSourceV2CutoverEnabled = false

	r := gin.New()
	r.GET("/internal/nginx-source/v2/capabilities", m.nginxSourceCapabilitiesV2)
	req := httptest.NewRequest(http.MethodGet, "/internal/nginx-source/v2/capabilities", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("existing v2 runtime stopped when new-cutover permission closed: %d %s", w.Code, w.Body.String())
	}
	if w := postSourceManifestV2(t, m, manifest); w.Code != http.StatusOK {
		t.Fatalf("idempotent active manifest retry failed: %d %s", w.Code, w.Body.String())
	}
	body := fmt.Sprintf(`{"node":"master","batch_id":"active_after_gate_abcdefgh","source_range_v2":{"protocol":2,"kind":"access","source_epoch":%q,"file_id":%q,"start_offset":0,"end_offset":10,"content_sha256":%q},"samples":[]}`, testSourceEpoch, testSourceFile, testContentA)
	if w := postNginx(t, m, body, "secret"); w.Code != http.StatusOK {
		t.Fatalf("active v2 ingest stopped after gate close: %d %s", w.Code, w.Body.String())
	}
	if w := postSourceManifestV2(t, m, testSourceManifest("error", 0)); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("closed gate allowed a new lane cutover: %d %s", w.Code, w.Body.String())
	}
	var protocols int64
	if err := m.storeDB.Model(&NginxCollectorProtocolState{}).Count(&protocols).Error; err != nil || protocols != 1 {
		t.Fatalf("closed gate created a new protocol lane: count=%d err=%v", protocols, err)
	}
}

func TestNginxSourceV2HeartbeatKeepsIdleLaneHealthyWithoutCreatingSamples(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2CutoverEnabled = true, true
	m.cfg.NginxSourceV2AllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2AllowedLanes = []string{"master:access"}
	enableTestNginxSourceContinuity(t, m)
	if w := postSourceManifestV2(t, m, testSourceManifest("access", 0)); w.Code != http.StatusOK {
		t.Fatalf("cutover failed: %d %s", w.Code, w.Body.String())
	}
	m.cfg.NginxSourceV2CutoverEnabled = false
	payload := fmt.Sprintf(`{"node":"master","kind":"access","source_epoch":%q,"file_id":%q,"confirmed_offset":0,"backlog_bytes":0,"backlog_known":true,"cursor_discontinuities":0,"last_cursor_discontinuity_at":0,"discarded_lines":0,"last_discarded_at":0}`, testSourceEpoch, testSourceFile)
	r := gin.New()
	r.POST("/internal/nginx-source/v2/heartbeat", m.nginxSourceHeartbeatHTTPV2)
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/heartbeat", strings.NewReader(payload))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"confirmed_offset":0`) {
		t.Fatalf("heartbeat failed: %d %s", w.Code, w.Body.String())
	}
	var source NginxSourceState
	if err := m.storeDB.First(&source, "node = ?", "master").Error; err != nil || source.LastIngestTs <= 0 || !source.BacklogKnown {
		t.Fatalf("heartbeat did not update source health: source=%+v err=%v", source, err)
	}
	var samples int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Count(&samples).Error; err != nil || samples != 0 {
		t.Fatalf("heartbeat must not create traffic samples: count=%d err=%v", samples, err)
	}
}

func TestNginxSourceV2PartialSchemaIsRejected(t *testing.T) {
	m := newTestMonitor(t)
	if err := m.storeDB.AutoMigrate(&NginxCollectorProtocolState{}); err != nil {
		t.Fatal(err)
	}
	if ready, err := detectNginxSourceV2Schema(m.storeDB); err == nil || ready {
		t.Fatalf("partial source schema accepted: ready=%v err=%v", ready, err)
	}
}

func TestNginxSourceV2CompleteTableNamesStillRequireCASIndex(t *testing.T) {
	m := newTestMonitor(t)
	migrateTestNginxSourceV2(t, m.storeDB)
	if err := m.storeDB.Exec("DROP INDEX idx_nginx_source_batch_identity").Error; err != nil {
		t.Fatal(err)
	}
	if ready, err := detectNginxSourceV2Schema(m.storeDB); err == nil || ready || !strings.Contains(err.Error(), "idx_nginx_source_batch_identity") {
		t.Fatalf("seven table names without the CAS index must fail closed: ready=%v err=%v", ready, err)
	}
}

func TestNginxSourceV2CommitRetentionCoversLateEvidenceWindow(t *testing.T) {
	m := newTestMonitor(t)
	migrateTestNginxSourceV2(t, m.storeDB)
	m.nginxSourceV2SchemaReady.Store(true)
	m.cfg.NginxRetentionDays = 2
	m.cfg.NginxEvidenceRetentionHours = 168
	now := time.Unix(2_000_000_000, 0)
	rows := []NginxSourceCommitV2{
		{Node: "master", Kind: "access", BatchID: "old_commit_abcdefgh", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ContentHash: testContentA, PayloadHash: testPayloadA, ReceivedAt: now.Add(-193 * time.Hour).Unix()},
		{Node: "master", Kind: "access", BatchID: "boundary_commit_abcdefgh", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ContentHash: testContentA, PayloadHash: testPayloadA, ReceivedAt: now.Add(-192 * time.Hour).Unix()},
		{Node: "master", Kind: "access", BatchID: "recent_commit_abcdefgh", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ContentHash: testContentA, PayloadHash: testPayloadA, ReceivedAt: now.Add(-10 * time.Hour).Unix()},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneNginxSourceV2CommitsOnce(context.Background(), now); err != nil {
		t.Fatal(err)
	}
	var got []NginxSourceCommitV2
	if err := m.storeDB.Order("received_at").Find(&got).Error; err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].BatchID != "boundary_commit_abcdefgh" || got[1].BatchID != "recent_commit_abcdefgh" {
		t.Fatalf("unexpected retained commits: %+v", got)
	}
}

func TestNginxSourceBoundaryV1BatchRetentionIsIsolatedAndBounded(t *testing.T) {
	m := newTestMonitor(t)
	migrateTestNginxSourceV2(t, m.storeDB)
	m.nginxSourceV2SchemaReady.Store(true)
	rows := []NginxSourceBoundaryBatchV1{
		{Node: "master", Kind: "access", BatchID: "old_boundary_abcdefgh", PayloadHash: testPayloadA, Device: 1, Inode: 2, StartOffset: 0, EndOffset: 10, ReceivedAt: 100},
		{Node: "master", Kind: "access", BatchID: "new_boundary_abcdefgh", PayloadHash: testPayloadA, Device: 1, Inode: 2, StartOffset: 10, EndOffset: 20, ReceivedAt: 200},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneNginxSourceBoundaryV1BatchesOnce(context.Background(), 150); err != nil {
		t.Fatal(err)
	}
	var got []NginxSourceBoundaryBatchV1
	if err := m.storeDB.Find(&got).Error; err != nil || len(got) != 1 || got[0].BatchID != "new_boundary_abcdefgh" {
		t.Fatalf("boundary retention=%+v err=%v", got, err)
	}
	var states int64
	if err := m.storeDB.Model(&NginxSourceBoundaryStateV1{}).Count(&states).Error; err != nil || states != 0 {
		t.Fatalf("batch pruning touched compact cutover state: states=%d err=%v", states, err)
	}
}

func TestNginxSourceV2CutoverRemainsClosedAfterRestartWithFlagOff(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "monitor.db")
	backupDir := filepath.Join(dir, "backups")
	first := &Monitor{cfg: Settings{NginxSourceV2Enabled: true, StoreBackupDir: backupDir}}
	if err := first.openStore(path); err != nil {
		t.Fatal(err)
	}
	registerTestManifest(t, first.storeDB, "access", 0)
	if sqlDB, err := first.storeDB.DB(); err != nil {
		t.Fatal(err)
	} else if err := sqlDB.Close(); err != nil {
		t.Fatal(err)
	}

	restarted := &Monitor{cfg: Settings{
		StoreBackupDir: backupDir, NginxEnabled: true, NginxSourceV2Enabled: true,
		NginxAllowedNodes: []string{"master"}, NginxSourceV2AllowedNodes: []string{"master"}, NginxSourceV2AllowedLanes: []string{"master:access"},
	}}
	if err := restarted.openStore(path); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := restarted.storeDB.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	if !restarted.nginxSourceV2SchemaReady.Load() {
		t.Fatal("restart forgot the persisted one-way source v2 cutover")
	}
	if !restarted.nginxSourceV2Active.Load() {
		t.Fatal("restart did not restore the durable active-v2 runtime state")
	}
	if !restarted.nginxSourceV2RuntimeConfigOK.Load() {
		t.Fatal("valid persisted active lane was reported as de-whitelisted")
	}
	restarted.cfg.NginxEnabled, restarted.cfg.IngestToken = true, "secret"
	restarted.cfg.NginxAllowedNodes = []string{"master"}
	r := gin.New()
	r.GET("/internal/nginx-source/v2/capabilities", restarted.nginxSourceCapabilitiesV2)
	req := httptest.NewRequest(http.MethodGet, "/internal/nginx-source/v2/capabilities", nil)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("restart stopped the existing v2 runtime while new cutovers were closed: %d %s", w.Code, w.Body.String())
	}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"legacy_after_restart_abcdefgh","samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, bucket)
	if w := postNginx(t, restarted, body, "secret"); w.Code != http.StatusConflict {
		t.Fatalf("restart reopened legacy ingest: %d %s", w.Code, w.Body.String())
	}
}

func applyTestSource(t *testing.T, db *gorm.DB, batchID string, source nginxSourceRangeV2) (bool, error) {
	t.Helper()
	var duplicate bool
	err := db.Transaction(func(tx *gorm.DB) error {
		var err error
		duplicate, err = applyNginxSourceRangeV2(tx, "master", source.Kind, batchID, testPayloadA, source, 100)
		return err
	})
	return duplicate, err
}

func postSourceManifestV2(t *testing.T, m *Monitor, manifest nginxSourceManifestV2) *httptest.ResponseRecorder {
	t.Helper()
	migrateTestNginxSourceV2(t, m.storeDB)
	payload, err := json.Marshal(nginxSourceManifestRequestV2{Node: "master", Manifest: manifest})
	if err != nil {
		t.Fatal(err)
	}
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/internal/nginx-source/v2/manifest", m.registerNginxSourceManifestHTTPV2)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/manifest", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestNginxSourceV2RejectsBeforeReadingBodyAndLimitsAuthenticatedBody(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routerFor := func(m *Monitor) *gin.Engine {
		r := gin.New()
		r.POST("/internal/nginx-source/v2/manifest", m.registerNginxSourceManifestHTTPV2)
		return r
	}

	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	reader := &countingReader{data: strings.NewReader(`{"node":"master"}`)}
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/manifest", reader)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable || reader.reads != 0 {
		t.Fatalf("disabled endpoint read request body: status=%d reads=%d", w.Code, reader.reads)
	}

	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2AllowedNodes, m.cfg.NginxSourceV2AllowedLanes = true, []string{"master"}, []string{"master:access"}
	m.cfg.NginxSourceV2CutoverEnabled = true
	m.cfg.NginxAllowedNodes = []string{"master"}
	reader = &countingReader{data: strings.NewReader(`{"node":"master"}`)}
	req = httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/manifest", reader)
	w = httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || reader.reads != 0 {
		t.Fatalf("unauthenticated endpoint read request body: status=%d reads=%d", w.Code, reader.reads)
	}

	oversized := `{"node":"master","padding":"` + strings.Repeat("x", 70<<10) + `"}`
	req = httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/manifest", strings.NewReader(oversized))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized authenticated request status=%d body=%s", w.Code, w.Body.String())
	}
}

func TestNginxSourceV2FilesRejectsBeforeReadingBodyAndLeavesNoState(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routerFor := func(m *Monitor) *gin.Engine {
		r := gin.New()
		r.POST("/internal/nginx-source/v2/files", m.registerNginxSourceFileHTTPV2)
		return r
	}
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"
	reader := &countingReader{data: strings.NewReader(`{"node":"master"}`)}
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/files", reader)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable || reader.reads != 0 {
		t.Fatalf("disabled files endpoint read body: status=%d reads=%d", w.Code, reader.reads)
	}

	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2AllowedNodes, m.cfg.NginxSourceV2AllowedLanes = true, []string{"master"}, []string{"master:access"}
	m.cfg.NginxSourceV2CutoverEnabled = true
	reader = &countingReader{data: strings.NewReader(`{"node":"master"}`)}
	req = httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/files", reader)
	w = httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || reader.reads != 0 {
		t.Fatalf("unauthenticated files endpoint read body: status=%d reads=%d", w.Code, reader.reads)
	}

	oversized := `{"node":"master","padding":"` + strings.Repeat("x", 70<<10) + `"}`
	req = httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/files", strings.NewReader(oversized))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("oversized files request status=%d body=%s", w.Code, w.Body.String())
	}
	var protocols, watermarks int64
	m.storeDB.Model(&NginxCollectorProtocolState{}).Count(&protocols)
	m.storeDB.Model(&NginxSourceFileWatermark{}).Count(&watermarks)
	if protocols != 0 || watermarks != 0 {
		t.Fatalf("rejected files request wrote state: protocols=%d watermarks=%d", protocols, watermarks)
	}
}

func TestNginxSourceV2HTTPDataAndWatermarkCommitAtomically(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2AllowedNodes, m.cfg.NginxSourceV2AllowedLanes = true, []string{"master"}, []string{"master:access"}
	m.cfg.NginxSourceV2CutoverEnabled = true
	if w := postSourceManifestV2(t, m, testSourceManifest("access", 0)); w.Code != http.StatusOK {
		t.Fatalf("manifest: %d %s", w.Code, w.Body.String())
	}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"v2_http_access_abcdefgh","source_range_v2":{"protocol":2,"kind":"access","source_epoch":%q,"file_id":%q,"start_offset":0,"end_offset":10,"content_sha256":%q},"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, testSourceEpoch, testSourceFile, testContentA, bucket)
	for i := 0; i < 2; i++ {
		w := postNginx(t, m, body, "secret")
		if w.Code != http.StatusOK {
			t.Fatalf("v2 access retry %d: %d %s", i, w.Code, w.Body.String())
		}
		var response struct {
			SourceAck *nginxSourceCommitAckV2 `json:"source_ack_v2"`
		}
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil || response.SourceAck == nil || response.SourceAck.SourceEpoch != testSourceEpoch || response.SourceAck.FileID != testSourceFile || response.SourceAck.StartOffset != 0 || response.SourceAck.EndOffset != 10 || response.SourceAck.NextOffset != 10 || response.SourceAck.ContentSHA256 != testContentA || response.SourceAck.BatchID != "v2_http_access_abcdefgh" {
			t.Fatalf("v2 commit ACK is not bound to accepted source: ack=%+v err=%v", response.SourceAck, err)
		}
	}
	var total int64
	if err := m.storeDB.Model(&NginxMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil || total != 1 {
		t.Fatalf("v2 aggregate duplicated: total=%d err=%v", total, err)
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "access", testSourceFile).Error; err != nil || watermark.NextOffset != 10 || watermark.Batches != 1 {
		t.Fatalf("v2 watermark=%+v err=%v", watermark, err)
	}
}

func TestNginxSourceV2HTTPRollsBackWatermarkWhenAggregateFails(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2AllowedNodes, m.cfg.NginxSourceV2AllowedLanes = true, []string{"master"}, []string{"master:access"}
	m.cfg.NginxSourceV2CutoverEnabled = true
	if w := postSourceManifestV2(t, m, testSourceManifest("access", 0)); w.Code != http.StatusOK {
		t.Fatalf("manifest: %d %s", w.Code, w.Body.String())
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_nginx_v2_aggregate BEFORE INSERT ON nginx_minute_samples BEGIN SELECT RAISE(ABORT, 'forced aggregate failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"v2_http_rollback_abcdefgh","source_range_v2":{"protocol":2,"kind":"access","source_epoch":%q,"file_id":%q,"start_offset":0,"end_offset":10,"content_sha256":%q},"samples":[{"bucket_ts":%d,"route":"/v1/responses","method":"POST","status":200,"count":1}]}`, testSourceEpoch, testSourceFile, testContentA, bucket)
	if w := postNginx(t, m, body, "secret"); w.Code != http.StatusInternalServerError {
		t.Fatalf("forced failure: %d %s", w.Code, w.Body.String())
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "access", testSourceFile).Error; err != nil {
		t.Fatal(err)
	}
	var ranges, batches int64
	m.storeDB.Model(&NginxSourceRangeBatch{}).Count(&ranges)
	m.storeDB.Model(&NginxIngestBatch{}).Where("batch_id = ?", "v2_http_rollback_abcdefgh").Count(&batches)
	if watermark.NextOffset != 0 || watermark.Batches != 0 || ranges != 0 || batches != 0 {
		t.Fatalf("failed aggregate advanced source state: watermark=%+v ranges=%d batches=%d", watermark, ranges, batches)
	}
	if err := m.storeDB.Exec(`DROP TRIGGER fail_nginx_v2_aggregate`).Error; err != nil {
		t.Fatal(err)
	}
	if w := postNginx(t, m, body, "secret"); w.Code != http.StatusOK {
		t.Fatalf("retry after rollback: %d %s", w.Code, w.Body.String())
	}
}

func TestNginxSourceV2AcceptsOnlyContiguousRangesAndExactRetry(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	first := testSourceRange(0, 100, testContentA)
	duplicate, err := applyTestSource(t, m.storeDB, "batch-v2-0001", first)
	if err != nil || duplicate {
		t.Fatalf("first range: duplicate=%v err=%v", duplicate, err)
	}
	duplicate, err = applyTestSource(t, m.storeDB, "batch-v2-0001", first)
	if err != nil || !duplicate {
		t.Fatalf("exact retry must be idempotent: duplicate=%v err=%v", duplicate, err)
	}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-gap1", testSourceRange(101, 120, testContentB)); !errors.Is(err, errNginxSourceGap) {
		t.Fatalf("gap must be rejected: %v", err)
	}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-over", testSourceRange(50, 120, testContentB)); !errors.Is(err, errNginxSourceOverlap) {
		t.Fatalf("overlap must be rejected: %v", err)
	}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-0002", testSourceRange(100, 120, testContentB)); err != nil {
		t.Fatalf("contiguous range rejected: %v", err)
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "access", testSourceFile).Error; err != nil {
		t.Fatal(err)
	}
	if watermark.NextOffset != 120 || watermark.Batches != 2 {
		t.Fatalf("watermark=%+v", watermark)
	}
}

func TestNginxSourceV2ConflictingRetryAndPrunedLedgerCannotDoubleApply(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	first := testSourceRange(0, 100, testContentA)
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-0001", first); err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-other", first); !errors.Is(err, errNginxSourceConflict) {
		t.Fatalf("same range with another batch must conflict: %v", err)
	}
	if err := m.storeDB.Where("node = ?", "master").Delete(&NginxSourceRangeBatch{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-retry", first); !errors.Is(err, errNginxSourceOverlap) {
		t.Fatalf("pruned range retry must still be rejected by permanent watermark: %v", err)
	}
}

func TestNginxSourceV2CutoverIsOneWayAndEpochIsStable(t *testing.T) {
	m := newTestMonitor(t)
	seedTestLegacyBoundary(t, m, "access", "legacy-last", 1, 10, 500)
	if err := m.storeDB.Create(&NginxSourceState{Node: "master", LastBatchID: "legacy-last"}).Error; err != nil {
		t.Fatal(err)
	}
	manifest := testSourceManifest("access", 500)
	manifest.CutoverFromV1, manifest.LegacyCursorDevice, manifest.LegacyCursorInode, manifest.LegacyCursorOffset = true, 1, 10, 500
	manifest.LegacyAckedBatchID = "legacy-last"
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := registerNginxSourceManifestV2(tx, "master", "legacy-last", manifest, 99)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	cutover := testSourceRange(500, 550, testContentA)
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-cutover", cutover); err != nil {
		t.Fatal(err)
	}
	allowed, err := legacyNginxSourceAllowed(m.storeDB, "master", "access")
	if err != nil || allowed {
		t.Fatalf("legacy must be disabled after v2 cutover: allowed=%v err=%v", allowed, err)
	}
	other := cutover
	other.SourceEpoch = strings.Repeat("f", 32)
	other.FileID = other.SourceEpoch + "-0000000000000001"
	other.StartOffset, other.EndOffset = 0, 10
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-epoch", other); !errors.Is(err, errNginxSourceEpoch) {
		t.Fatalf("epoch replacement must fail: %v", err)
	}
}

func TestNginxSourceV2TransactionRollbackLeavesNoProtocolCutover(t *testing.T) {
	m := newTestMonitor(t)
	migrateTestNginxSourceV2(t, m.storeDB)
	wantRollback := errors.New("rollback")
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		if _, _, err := registerNginxSourceManifestV2(tx, "master", "", testSourceManifest("access", 0), 99); err != nil {
			return err
		}
		if _, err := applyNginxSourceRangeV2(tx, "master", "access", "batch-v2-0001", testPayloadA, testSourceRange(0, 10, testContentA), 100); err != nil {
			return err
		}
		return wantRollback
	})
	if !errors.Is(err, wantRollback) {
		t.Fatalf("transaction did not report rollback: %v", err)
	}
	allowed, err := legacyNginxSourceAllowed(m.storeDB, "master", "access")
	if err != nil || !allowed {
		t.Fatalf("rolled back cutover must keep legacy allowed: allowed=%v err=%v", allowed, err)
	}
}

func TestNginxSourceV2AccessAndErrorAreIndependentLanes(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	registerTestManifest(t, m.storeDB, "error", 0)
	access := testSourceRange(0, 10, testContentA)
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-access", access); err != nil {
		t.Fatal(err)
	}
	errorSource := access
	errorSource.Kind = "error"
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-error", errorSource); err != nil {
		t.Fatal(err)
	}
	var count int64
	if err := m.storeDB.Model(&NginxCollectorProtocolState{}).Where("node = ?", "master").Count(&count).Error; err != nil || count != 2 {
		t.Fatalf("lane count=%d err=%v", count, err)
	}
}

func TestNginxSourceV2RejectsInvalidEnvelope(t *testing.T) {
	valid := testSourceRange(0, 10, testContentA)
	for _, mutate := range []func(*nginxSourceRangeV2){
		func(s *nginxSourceRangeV2) { s.Protocol = 1 },
		func(s *nginxSourceRangeV2) { s.Kind = "evidence" },
		func(s *nginxSourceRangeV2) { s.FileID = strings.Repeat("a", 49) },
		func(s *nginxSourceRangeV2) { s.EndOffset = s.StartOffset },
		func(s *nginxSourceRangeV2) { s.ContentSHA256 = "not-a-hash" },
	} {
		value := valid
		mutate(&value)
		if validNginxSourceRangeV2(value, "access") {
			t.Fatalf("invalid source accepted: %+v", value)
		}
	}
}

func TestNginxSourceV2RejectsUnregisteredFileAndRangeBeforeManifest(t *testing.T) {
	m := newTestMonitor(t)
	migrateTestNginxSourceV2(t, m.storeDB)
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-early", testSourceRange(0, 10, testContentA)); !errors.Is(err, errNginxSourceUnregistered) {
		t.Fatalf("range before manifest must fail closed: %v", err)
	}
	registerTestManifest(t, m.storeDB, "access", 0)
	other := testSourceRange(0, 10, testContentA)
	other.FileID = testSourceEpoch + "-0000000000000002"
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-other", other); !errors.Is(err, errNginxSourceUnregistered) {
		t.Fatalf("new file id must not bypass the manifest: %v", err)
	}
}

func TestNginxSourceV2CutoverRegistersRotatedEOFAndCurrentAtomically(t *testing.T) {
	m := newTestMonitor(t)
	seedTestLegacyBoundary(t, m, "access", "legacy-boundary", 1, 10, 400)
	if err := m.storeDB.Create(&NginxSourceState{Node: "master", LastBatchID: "legacy-boundary"}).Error; err != nil {
		t.Fatal(err)
	}
	manifest := nginxSourceManifestV2{Protocol: 2, Kind: "access", SourceEpoch: testSourceEpoch, CutoverFromV1: true, LegacyCursorDevice: 1, LegacyCursorInode: 10, LegacyCursorOffset: 400, LegacyAckedBatchID: "legacy-boundary", Files: []nginxSourceManifestFileV2{
		{FileID: testSourceEpoch + "-0000000000000001", Generation: 1, Device: 1, Inode: 10, BaseOffset: 400},
		{FileID: testSourceEpoch + "-0000000000000002", Generation: 2, Device: 1, Inode: 11, Current: true},
	}}
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		duplicate, _, err := registerNginxSourceManifestV2(tx, "master", "legacy-boundary", manifest, 100)
		if duplicate {
			t.Fatal("first manifest was reported duplicate")
		}
		return err
	})
	if err != nil {
		t.Fatal(err)
	}
	var files []NginxSourceFileWatermark
	if err := m.storeDB.Where("node = ? AND kind = ?", "master", "access").Order("generation").Find(&files).Error; err != nil {
		t.Fatal(err)
	}
	if len(files) != 2 || files[0].NextOffset != 400 || files[1].NextOffset != 0 || !files[1].Current {
		t.Fatalf("manifest was not registered atomically: %+v", files)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		duplicate, _, err := registerNginxSourceManifestV2(tx, "master", "legacy-boundary", manifest, 101)
		if err == nil && !duplicate {
			t.Fatal("exact manifest retry must be idempotent")
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
}

func TestNginxSourceV2RejectsStaleLegacyCutoverBoundary(t *testing.T) {
	m := newTestMonitor(t)
	seedTestLegacyBoundary(t, m, "access", "latest-v1", 1, 10, 10)
	if err := m.storeDB.Create(&NginxSourceState{Node: "master", LastBatchID: "latest-v1"}).Error; err != nil {
		t.Fatal(err)
	}
	manifest := testSourceManifest("access", 10)
	manifest.CutoverFromV1, manifest.LegacyCursorDevice, manifest.LegacyCursorInode, manifest.LegacyCursorOffset = true, 1, 10, 10
	manifest.LegacyAckedBatchID = "latest-v1"
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := registerNginxSourceManifestV2(tx, "master", "stale-v1", manifest, 100)
		return err
	})
	if err == nil {
		t.Fatal("stale v1 boundary must not cut over")
	}
	allowed, checkErr := legacyNginxSourceAllowed(m.storeDB, "master", "access")
	if checkErr != nil || !allowed {
		t.Fatalf("failed manifest must leave legacy lane open: allowed=%v err=%v", allowed, checkErr)
	}
}

func TestNginxSourceV2AckRecoveryReturnsCompleteProofChain(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-proof1", testSourceRange(0, 100, testContentA)); err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-proof2", testSourceRange(100, 120, testContentB)); err != nil {
		t.Fatal(err)
	}
	proof, err := nginxSourceAckRecoveryProofV2(m.storeDB, "master", "access", testSourceEpoch, testSourceFile, 0)
	if err != nil {
		t.Fatal(err)
	}
	if proof.NextOffset != 120 || len(proof.Proofs) != 2 || proof.Proofs[0].StartOffset != 0 || proof.Proofs[1].EndOffset != 120 {
		t.Fatalf("unexpected ACK recovery proof: %+v", proof)
	}
	if err := m.storeDB.Where("start_offset = ?", 0).Delete(&NginxSourceRangeBatch{}).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := nginxSourceAckRecoveryProofV2(m.storeDB, "master", "access", testSourceEpoch, testSourceFile, 0); err == nil {
		t.Fatal("pruned proof chain must fail closed")
	}
}

func TestNginxSourceV2DurableConfirmationPrunesOnlyRecoverableLedger(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	first, second := testSourceRange(0, 10, testContentA), testSourceRange(10, 20, testContentB)
	if _, err := applyTestSource(t, m.storeDB, "confirm_a_abcdefgh", first); err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestSource(t, m.storeDB, "confirm_b_abcdefgh", second); err != nil {
		t.Fatal(err)
	}
	in := nginxSourceAckConfirmationRequestV2{Node: "master", Kind: "access", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ConfirmedOffset: 10}
	var duplicate bool
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		duplicate, _, err = confirmNginxSourceAckV2(tx, in, 101)
		return err
	}); err != nil || duplicate {
		t.Fatalf("first confirmation: duplicate=%v err=%v", duplicate, err)
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "access", testSourceFile).Error; err != nil || watermark.ConfirmedOffset != 10 || watermark.NextOffset != 20 {
		t.Fatalf("watermark=%+v err=%v", watermark, err)
	}
	var ranges int64
	m.storeDB.Model(&NginxSourceRangeBatch{}).Count(&ranges)
	if ranges != 1 {
		t.Fatalf("confirmed range ledger not pruned: %d", ranges)
	}
	if _, err := nginxSourceAckRecoveryProofV2(m.storeDB, "master", "access", testSourceEpoch, testSourceFile, 0); err == nil {
		t.Fatal("proof below durable confirmation must fail closed")
	}
	proof, err := nginxSourceAckRecoveryProofV2(m.storeDB, "master", "access", testSourceEpoch, testSourceFile, 10)
	if err != nil || len(proof.Proofs) != 1 || proof.Proofs[0].EndOffset != 20 {
		t.Fatalf("remaining proof=%+v err=%v", proof, err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		duplicate, _, err = confirmNginxSourceAckV2(tx, in, 102)
		return err
	}); err != nil || !duplicate {
		t.Fatalf("confirmation retry: duplicate=%v err=%v", duplicate, err)
	}
	bad := in
	bad.ConfirmedOffset = 15
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := confirmNginxSourceAckV2(tx, bad, 103)
		return err
	}); !errors.Is(err, errNginxSourceConflict) {
		t.Fatalf("mid-range confirmation was accepted: %v", err)
	}
}

func TestNginxSourceV2ConfirmationHTTPAuthenticatesBeforeBodyAndPrunesAtomically(t *testing.T) {
	gin.SetMode(gin.TestMode)
	routerFor := func(m *Monitor) *gin.Engine {
		r := gin.New()
		r.POST("/internal/nginx-source/v2/confirm", m.confirmNginxSourceAckHTTPV2)
		return r
	}
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.IngestToken = true, "secret"

	reader := &countingReader{data: strings.NewReader(`{"node":"master"}`)}
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/confirm", reader)
	req.Header.Set("Authorization", "Bearer secret")
	w := httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusServiceUnavailable || reader.reads != 0 {
		t.Fatalf("disabled confirmation read body: status=%d reads=%d", w.Code, reader.reads)
	}

	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2AllowedNodes, m.cfg.NginxSourceV2AllowedLanes = true, []string{"master"}, []string{"master:access"}
	m.cfg.NginxSourceV2CutoverEnabled = true
	reader = &countingReader{data: strings.NewReader(`{"node":"master"}`)}
	req = httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/confirm", reader)
	w = httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized || reader.reads != 0 {
		t.Fatalf("unauthenticated confirmation read body: status=%d reads=%d", w.Code, reader.reads)
	}

	registerTestManifest(t, m.storeDB, "access", 0)
	if _, err := applyTestSource(t, m.storeDB, "confirm_http_abcdefgh", testSourceRange(0, 10, testContentA)); err != nil {
		t.Fatal(err)
	}
	payload, err := json.Marshal(nginxSourceAckConfirmationRequestV2{Node: "master", Kind: "access", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ConfirmedOffset: 10})
	if err != nil {
		t.Fatal(err)
	}
	req = httptest.NewRequest(http.MethodPost, "/internal/nginx-source/v2/confirm", bytes.NewReader(payload))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	routerFor(m).ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("confirmation status=%d body=%s", w.Code, w.Body.String())
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "access", testSourceFile).Error; err != nil || watermark.ConfirmedOffset != 10 {
		t.Fatalf("confirmed watermark=%+v err=%v", watermark, err)
	}
	var ranges int64
	if err := m.storeDB.Model(&NginxSourceRangeBatch{}).Count(&ranges).Error; err != nil || ranges != 0 {
		t.Fatalf("confirmation and prune were not atomic: ranges=%d err=%v", ranges, err)
	}
}

func TestNginxSourceV2BackpressureBoundsUnconfirmedLedger(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	for i := int64(0); i < maxNginxSourceUnconfirmedRangesV2; i++ {
		source := testSourceRange(i, i+1, testContentA)
		if _, err := applyTestSource(t, m.storeDB, fmt.Sprintf("bounded_range_%03d_abcdefgh", i), source); err != nil {
			t.Fatalf("range %d: %v", i, err)
		}
	}
	overflow := testSourceRange(maxNginxSourceUnconfirmedRangesV2, maxNginxSourceUnconfirmedRangesV2+1, testContentB)
	if _, err := applyTestSource(t, m.storeDB, "bounded_overflow_abcdefgh", overflow); !errors.Is(err, errNginxSourceBackpressure) {
		t.Fatalf("unconfirmed ledger limit not enforced: %v", err)
	}
	var ranges int64
	if err := m.storeDB.Model(&NginxSourceRangeBatch{}).Count(&ranges).Error; err != nil || ranges != maxNginxSourceUnconfirmedRangesV2 {
		t.Fatalf("overflow mutated ledger: ranges=%d err=%v", ranges, err)
	}
}

func TestNginxSourceV2ConfirmationRollbackKeepsProofLedger(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	if _, err := applyTestSource(t, m.storeDB, "confirm_rollback_abcdefgh", testSourceRange(0, 10, testContentA)); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Exec(`CREATE TRIGGER fail_nginx_v2_prune BEFORE DELETE ON nginx_source_range_batches BEGIN SELECT RAISE(ABORT, 'forced prune failure'); END`).Error; err != nil {
		t.Fatal(err)
	}
	in := nginxSourceAckConfirmationRequestV2{Node: "master", Kind: "access", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ConfirmedOffset: 10}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := confirmNginxSourceAckV2(tx, in, 101)
		return err
	}); err == nil {
		t.Fatal("forced proof prune failure committed confirmation")
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "access", testSourceFile).Error; err != nil || watermark.ConfirmedOffset != 0 {
		t.Fatalf("failed prune advanced confirmed offset: watermark=%+v err=%v", watermark, err)
	}
	var ranges int64
	if err := m.storeDB.Model(&NginxSourceRangeBatch{}).Count(&ranges).Error; err != nil || ranges != 1 {
		t.Fatalf("failed prune removed proof: ranges=%d err=%v", ranges, err)
	}
	if err := m.storeDB.Exec(`DROP TRIGGER fail_nginx_v2_prune`).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := confirmNginxSourceAckV2(tx, in, 102)
		return err
	}); err != nil {
		t.Fatalf("confirmation retry failed: %v", err)
	}
}

func TestNginxSourceV2BackpressureByteBoundaryDuplicateAndRelease(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	step := maxNginxSourceRangeBytesV2
	for i := int64(0); i < maxNginxSourceUnconfirmedBytesV2/step; i++ {
		start := i * step
		source := testSourceRange(start, start+step, testContentA)
		if _, err := applyTestSource(t, m.storeDB, fmt.Sprintf("byte_limit_%02d_abcdefgh", i), source); err != nil {
			t.Fatalf("range %d: %v", i, err)
		}
	}
	lastStart := maxNginxSourceUnconfirmedBytesV2 - step
	last := testSourceRange(lastStart, maxNginxSourceUnconfirmedBytesV2, testContentA)
	if duplicate, err := applyTestSource(t, m.storeDB, fmt.Sprintf("byte_limit_%02d_abcdefgh", maxNginxSourceUnconfirmedBytesV2/step-1), last); err != nil || !duplicate {
		t.Fatalf("exact duplicate at byte limit was rejected: duplicate=%v err=%v", duplicate, err)
	}
	overflow := testSourceRange(maxNginxSourceUnconfirmedBytesV2, maxNginxSourceUnconfirmedBytesV2+1, testContentB)
	if _, err := applyTestSource(t, m.storeDB, "byte_limit_overflow_abcdefgh", overflow); !errors.Is(err, errNginxSourceBackpressure) {
		t.Fatalf("byte limit overflow accepted: %v", err)
	}
	in := nginxSourceAckConfirmationRequestV2{Node: "master", Kind: "access", SourceEpoch: testSourceEpoch, FileID: testSourceFile, ConfirmedOffset: maxNginxSourceUnconfirmedBytesV2}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, _, err := confirmNginxSourceAckV2(tx, in, 101)
		return err
	}); err != nil {
		t.Fatal(err)
	}
	if _, err := applyTestSource(t, m.storeDB, "byte_limit_after_release_abcdefgh", overflow); err != nil {
		t.Fatalf("confirmation did not release byte capacity: %v", err)
	}
}

func TestNginxSourceV2RegistersNewCurrentByHashChainedCAS(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	var protocol NginxCollectorProtocolState
	if err := m.storeDB.First(&protocol, "node = ? AND kind = ?", "master", "access").Error; err != nil {
		t.Fatal(err)
	}
	registration := nginxSourceFileRegistrationV2{Protocol: 2, Kind: "access", SourceEpoch: testSourceEpoch, ExpectedRevision: 1, PreviousManifestHash: protocol.ManifestSHA256,
		Files: []nginxSourceManifestFileV2{
			{FileID: testSourceEpoch + "-0000000000000002", Generation: 2, Device: 1, Inode: 11},
			{FileID: testSourceEpoch + "-0000000000000003", Generation: 3, Device: 1, Inode: 12, Current: true},
		}}
	var revision uint64
	var hash string
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		duplicate, nextRevision, nextHash, err := registerNginxSourceFileV2(tx, "master", registration, 100)
		if duplicate {
			t.Fatal("first file registration was duplicate")
		}
		revision, hash = nextRevision, nextHash
		return err
	})
	if err != nil || revision != 2 || !nginxSourceHashV2Pattern.MatchString(hash) {
		t.Fatalf("file registration failed: revision=%d hash=%q err=%v", revision, hash, err)
	}
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		duplicate, gotRevision, gotHash, err := registerNginxSourceFileV2(tx, "master", registration, 101)
		if err == nil && (!duplicate || gotRevision != revision || gotHash != hash) {
			t.Fatalf("exact file retry is not idempotent: duplicate=%v revision=%d hash=%q", duplicate, gotRevision, gotHash)
		}
		return err
	}); err != nil {
		t.Fatal(err)
	}
	newSource := testSourceRange(0, 10, testContentA)
	newSource.FileID = registration.Files[0].FileID
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-new-current", newSource); err != nil {
		t.Fatalf("registered rotated range rejected: %v", err)
	}
	newSource.FileID = registration.Files[1].FileID
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-latest-current", newSource); err != nil {
		t.Fatalf("registered latest current range rejected: %v", err)
	}
}

func TestNginxSourceV2ControlledEpochRecoveryPreservesOldAudit(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "access", 0)
	if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		return markNginxSourceRecoveryRequiredV2(tx, "master", "access", testSourceEpoch, 110)
	}); err != nil {
		t.Fatal(err)
	}
	newEpoch := strings.Repeat("f", 32)
	manifest := nginxSourceManifestV2{Protocol: 2, Kind: "access", SourceEpoch: newEpoch, Files: []nginxSourceManifestFileV2{{FileID: newEpoch + "-0000000000000001", Generation: 1, Device: 1, Inode: 10, BaseOffset: 500, Current: true}}}
	var newHash string
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		var err error
		newHash, err = recoverNginxSourceEpochV2(tx, "master", testSourceEpoch, "root-operator", "verified source loss requires an audited new epoch", manifest, 120)
		return err
	})
	if err != nil || !nginxSourceHashV2Pattern.MatchString(newHash) {
		t.Fatalf("controlled recovery failed: hash=%q err=%v", newHash, err)
	}
	var protocol NginxCollectorProtocolState
	if err := m.storeDB.First(&protocol, "node = ? AND kind = ?", "master", "access").Error; err != nil {
		t.Fatal(err)
	}
	if protocol.SourceEpoch != newEpoch || protocol.RecoveryRequired || !protocol.ContinuityBroken || protocol.LastRecoveryAt != 120 {
		t.Fatalf("recovery state is not explicit: %+v", protocol)
	}
	var oldFiles, newFiles, audits int64
	m.storeDB.Model(&NginxSourceFileWatermark{}).Where("source_epoch = ?", testSourceEpoch).Count(&oldFiles)
	m.storeDB.Model(&NginxSourceFileWatermark{}).Where("source_epoch = ?", newEpoch).Count(&newFiles)
	m.storeDB.Model(&NginxCollectorEpochRecovery{}).Where("old_source_epoch = ? AND new_source_epoch = ?", testSourceEpoch, newEpoch).Count(&audits)
	if oldFiles != 1 || newFiles != 1 || audits != 1 {
		t.Fatalf("recovery overwrote audit history: old=%d new=%d audits=%d", oldFiles, newFiles, audits)
	}
	source := nginxSourceRangeV2{Protocol: 2, Kind: "access", SourceEpoch: newEpoch, FileID: manifest.Files[0].FileID, StartOffset: 500, EndOffset: 510, ContentSHA256: testContentA}
	if _, err := applyTestSource(t, m.storeDB, "batch-v2-recovered", source); err != nil {
		t.Fatalf("new epoch did not resume at audited base: %v", err)
	}
}

func TestNginxSourceV2CannotRecoverWithoutExpectedBlockedEpoch(t *testing.T) {
	m := newTestMonitor(t)
	registerTestManifest(t, m.storeDB, "error", 0)
	newEpoch := strings.Repeat("e", 32)
	manifest := nginxSourceManifestV2{Protocol: 2, Kind: "error", SourceEpoch: newEpoch, Files: []nginxSourceManifestFileV2{{FileID: newEpoch + "-0000000000000001", Generation: 1, Device: 2, Inode: 20, Current: true}}}
	err := m.storeDB.Transaction(func(tx *gorm.DB) error {
		_, err := recoverNginxSourceEpochV2(tx, "master", testSourceEpoch, "root-operator", "attempt recovery before lane is explicitly blocked", manifest, 120)
		return err
	})
	if !errors.Is(err, errNginxSourceEpoch) {
		t.Fatalf("unblocked lane was recovered: %v", err)
	}
}
