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

func TestValidateNginxErrorRequiresBaseCollector(t *testing.T) {
	if err := validateNginxSettings(Settings{NginxErrorEnabled: true}); err == nil {
		t.Fatal("error lane cannot run without base collector")
	}
}
