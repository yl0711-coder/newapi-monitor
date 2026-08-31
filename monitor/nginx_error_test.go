package monitor

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func postNginxError(t *testing.T, m *Monitor, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	r.POST("/internal/nginx-errors", m.ingestNginxErrors)
	w := httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodPost, "/internal/nginx-errors", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret")
	req.Header.Set("Content-Type", "application/json")
	r.ServeHTTP(w, req)
	return w
}

func TestNginxErrorIngestIsFinitePrivateAndIdempotent(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken = true, true, "secret"
	m.cfg.NginxAllowedNodes, m.cfg.NginxRetentionDays = []string{"master"}, 7
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"error_batch_abcdefgh","samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	for i := 0; i < 2; i++ {
		if w := postNginxError(t, m, body); w.Code != http.StatusOK {
			t.Fatalf("ingest %d: %d %s", i, w.Code, w.Body.String())
		}
	}
	var count int64
	if err := m.storeDB.Model(&NginxErrorMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&count).Error; err != nil || count != 3 {
		t.Fatalf("idempotency failed: count=%d err=%v", count, err)
	}
	bad := fmt.Sprintf(`{"node":"master","batch_id":"error_bad_abcdefgh","samples":[{"bucket_ts":%d,"category":"raw upstream 10.0.0.1 /secret","severity":"error","count":1}]}`, bucket)
	if w := postNginxError(t, m, bad); w.Code != http.StatusBadRequest {
		t.Fatalf("unbounded category accepted: %d %s", w.Code, w.Body.String())
	}
}

func TestNginxErrorV1BoundaryIsIdempotentAndConflictsFailClosed(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken = true, true, "secret"
	m.cfg.NginxAllowedNodes, m.cfg.NginxRetentionDays = []string{"master"}, 7
	enableTestNginxSourceContinuity(t, m)
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"error_boundary_abcdefgh","source_boundary":{"device":7,"inode":44,"start_offset":10,"end_offset":90},"samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	for i := 0; i < 2; i++ {
		if w := postNginxError(t, m, body); w.Code != http.StatusOK {
			t.Fatalf("retry %d: %d %s", i, w.Code, w.Body.String())
		}
	}
	legacyRetry := fmt.Sprintf(`{"node":"master","batch_id":"error_boundary_abcdefgh","samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	if w := postNginxError(t, m, legacyRetry); w.Code != http.StatusOK {
		t.Fatalf("old error collector rollback retry: %d %s", w.Code, w.Body.String())
	}
	conflict := fmt.Sprintf(`{"node":"master","batch_id":"error_boundary_abcdefgh","source_boundary":{"device":7,"inode":44,"start_offset":10,"end_offset":91},"samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	if w := postNginxError(t, m, conflict); w.Code != http.StatusConflict {
		t.Fatalf("conflicting error boundary: %d %s", w.Code, w.Body.String())
	}
	var total int64
	if err := m.storeDB.Model(&NginxErrorMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil || total != 3 {
		t.Fatalf("error boundary retry changed aggregates: total=%d err=%v", total, err)
	}
	var state NginxSourceBoundaryStateV1
	if err := m.storeDB.First(&state, "node = ? AND kind = ?", "master", "error").Error; err != nil || state.LastBatchID != "error_boundary_abcdefgh" || state.Device != 7 || state.Inode != 44 || state.LastOffset != 90 {
		t.Fatalf("error boundary state=%+v err=%v", state, err)
	}
}

func TestNginxErrorPrepareAcceptsV1BoundaryWithoutOpeningV2Lane(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken = true, true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled = true
	m.cfg.NginxSourceV2AllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2AllowedLanes = []string{"master:error"}
	enableTestNginxSourceContinuity(t, m)
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"error_prepare_boundary_abcdefgh","source_boundary":{"device":7,"inode":44,"start_offset":10,"end_offset":90},"samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	w := postNginxError(t, m, body)
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), `"source_boundary_ack"`) {
		t.Fatalf("prepare error boundary was not exactly acknowledged: %d %s", w.Code, w.Body.String())
	}
	var boundaries, protocols, watermarks int64
	m.storeDB.Model(&NginxSourceBoundaryStateV1{}).Where("node = ? AND kind = ?", "master", "error").Count(&boundaries)
	m.storeDB.Model(&NginxCollectorProtocolState{}).Count(&protocols)
	m.storeDB.Model(&NginxSourceFileWatermark{}).Count(&watermarks)
	if boundaries != 1 || protocols != 0 || watermarks != 0 {
		t.Fatalf("prepare error lane crossed cutover boundary: boundaries=%d protocols=%d watermarks=%d", boundaries, protocols, watermarks)
	}
}

func TestNginxErrorV1AdoptsBoundaryFromLegacyNullState(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken = true, true, "secret"
	m.cfg.NginxAllowedNodes, m.cfg.NginxRetentionDays = []string{"master"}, 7
	bucket := time.Now().Unix() / 60 * 60
	legacy := fmt.Sprintf(`{"node":"master","batch_id":"legacy_error_boundary_abcdefgh","samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	if w := postNginxError(t, m, legacy); w.Code != http.StatusOK {
		t.Fatalf("legacy error ingest: %d %s", w.Code, w.Body.String())
	}
	enableTestNginxSourceContinuity(t, m)
	bound := fmt.Sprintf(`{"node":"master","batch_id":"legacy_error_boundary_abcdefgh","source_boundary":{"device":7,"inode":44,"start_offset":10,"end_offset":90},"samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":3}]}`, bucket)
	if w := postNginxError(t, m, bound); w.Code != http.StatusOK {
		t.Fatalf("legacy error boundary adoption: %d %s", w.Code, w.Body.String())
	}
	var state NginxSourceBoundaryStateV1
	if err := m.storeDB.First(&state, "node = ? AND kind = ?", "master", "error").Error; err != nil || state.LastBatchID != "legacy_error_boundary_abcdefgh" || state.Device != 7 || state.Inode != 44 || state.LastOffset != 90 {
		t.Fatalf("error boundary was not adopted: state=%+v err=%v", state, err)
	}
	var total int64
	if err := m.storeDB.Model(&NginxErrorMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil || total != 3 {
		t.Fatalf("error boundary adoption duplicated aggregate: total=%d err=%v", total, err)
	}
}

func TestNginxErrorV2DataAndWatermarkCommitAtomically(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken, m.cfg.NginxRetentionDays = true, true, "secret", 7
	m.cfg.NginxAllowedNodes = []string{"master"}
	m.cfg.NginxSourceV2Enabled, m.cfg.NginxSourceV2AllowedNodes, m.cfg.NginxSourceV2AllowedLanes = true, []string{"master"}, []string{"master:error"}
	m.cfg.NginxSourceV2CutoverEnabled = true
	if w := postSourceManifestV2(t, m, testSourceManifest("error", 0)); w.Code != http.StatusOK {
		t.Fatalf("error manifest: %d %s", w.Code, w.Body.String())
	}
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"v2_http_error_abcdefgh","source_range_v2":{"protocol":2,"kind":"error","source_epoch":%q,"file_id":%q,"start_offset":0,"end_offset":10,"content_sha256":%q},"samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":2}]}`, testSourceEpoch, testSourceFile, testContentA, bucket)
	for i := 0; i < 2; i++ {
		if w := postNginxError(t, m, body); w.Code != http.StatusOK {
			t.Fatalf("v2 error retry %d: %d %s", i, w.Code, w.Body.String())
		}
	}
	var total int64
	if err := m.storeDB.Model(&NginxErrorMinuteSample{}).Select("COALESCE(SUM(count),0)").Scan(&total).Error; err != nil || total != 2 {
		t.Fatalf("v2 error aggregate duplicated: total=%d err=%v", total, err)
	}
	var watermark NginxSourceFileWatermark
	if err := m.storeDB.First(&watermark, "node = ? AND kind = ? AND file_id = ?", "master", "error", testSourceFile).Error; err != nil || watermark.NextOffset != 10 || watermark.Batches != 1 {
		t.Fatalf("v2 error watermark=%+v err=%v", watermark, err)
	}
}

func TestValidateNginxErrorRequiresBaseCollector(t *testing.T) {
	if err := validateNginxSettings(Settings{NginxErrorEnabled: true}); err == nil {
		t.Fatal("error lane cannot run without base collector")
	}
}

func TestNginxLegacyErrorIngestIsAtomicallyClosedAfterV2Manifest(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxEnabled, m.cfg.NginxErrorEnabled, m.cfg.IngestToken = true, true, "secret"
	m.cfg.NginxAllowedNodes = []string{"master"}
	registerTestManifest(t, m.storeDB, "error", 0)
	m.nginxSourceV2SchemaReady.Store(true)
	bucket := time.Now().Unix() / 60 * 60
	body := fmt.Sprintf(`{"node":"master","batch_id":"legacy_error_after_v2_abcdefgh","samples":[{"bucket_ts":%d,"category":"upstream_timeout","severity":"error","count":1}]}`, bucket)
	w := postNginxError(t, m, body)
	if w.Code != http.StatusConflict {
		t.Fatalf("legacy error ingest after v2 cutover=%d body=%s", w.Code, w.Body.String())
	}
	var batches, samples int64
	m.storeDB.Model(&NginxErrorIngestBatch{}).Where("batch_id = ?", "legacy_error_after_v2_abcdefgh").Count(&batches)
	m.storeDB.Model(&NginxErrorMinuteSample{}).Count(&samples)
	if batches != 0 || samples != 0 {
		t.Fatalf("rejected legacy error mutated state: batches=%d samples=%d", batches, samples)
	}
}
