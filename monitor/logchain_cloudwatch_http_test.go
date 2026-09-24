package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func newCloudWatchHTTPTestMonitor(t *testing.T, client cloudWatchLogsAPI, mutate func(*Settings)) *Monitor {
	t.Helper()
	cfg := Settings{
		CloudWatchLogsEnabled:       true,
		CloudWatchEvidenceHMACKey:   logChainCloudWatchTestKey,
		CloudWatchEvidenceHMACKeyID: "fixture-v1",
		SessionSecret:               "fixture-session-secret-for-cloudwatch-http",
	}
	if mutate != nil {
		mutate(&cfg)
	}
	m := &Monitor{cfg: cfg}
	m.cloudWatchLogs = newCloudWatchLogsRuntime(cfg.CloudWatchLogsEnabled, func(context.Context, string) (cloudWatchLogsAPI, error) {
		return client, nil
	})
	return m
}

// postCloudWatchEvidence 走真实路由，带上 requireRole 中间件，这样 urole
// 的写入路径与生产一致；直接调 handler 会绕开权限上下文。
func postCloudWatchEvidence(t *testing.T, m *Monitor, role int, body string) *httptest.ResponseRecorder {
	t.Helper()
	gin.SetMode(gin.TestMode)
	r := gin.New()
	group := r.Group("/", func(c *gin.Context) { c.Set("urole", role); c.Next() })
	group.POST("/logchain/cloudwatch/evidence", noStoreSensitive, m.serveLogChainCloudWatchEvidence)
	req := httptest.NewRequest(http.MethodPost, "/logchain/cloudwatch/evidence", strings.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")
	w := httptest.NewRecorder()
	r.ServeHTTP(w, req)
	return w
}

func TestCloudWatchEvidenceEndpointAllowsSensitiveForAllAdministrators(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newCloudWatchHTTPTestMonitor(t, client, nil)
	body := `{"request_id":"req-1","at_unix":` + cwUnix(time.Now().Unix()) + `,"include_sensitive":true}`
	if w := postCloudWatchEvidence(t, m, roleAdmin, body); w.Code != http.StatusOK {
		t.Fatalf("admin sensitive request status=%d body=%s", w.Code, w.Body.String())
	}
	if w := postCloudWatchEvidence(t, m, roleRoot, body); w.Code != http.StatusOK {
		t.Fatalf("root sensitive request status=%d body=%s", w.Code, w.Body.String())
	}
}

// 当前系统由所有者单人使用，本机验收与登录后的管理员采用同一权限口径；
// 解析层仍确保只返回 HMAC 与网络摘要，不回传原始 IP/User-Agent。
func TestCloudWatchEvidenceEndpointAllowsSensitiveUnderLocalAuthBypass(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	m := newCloudWatchHTTPTestMonitor(t, client, func(s *Settings) { s.LocalAuthBypass = true })
	body := `{"request_id":"req-1","at_unix":` + cwUnix(time.Now().Unix()) + `,"include_sensitive":true}`
	if w := postCloudWatchEvidence(t, m, roleRoot, body); w.Code != http.StatusOK {
		t.Fatalf("local auth bypass sensitive request failed: status=%d", w.Code)
	}
}

func TestCloudWatchEvidenceEndpointDisabledAndBadInput(t *testing.T) {
	client := &fakeCloudWatchLogsClient{}
	off := newCloudWatchHTTPTestMonitor(t, client, func(s *Settings) { s.CloudWatchLogsEnabled = false })
	off.cloudWatchLogs = newCloudWatchLogsRuntime(false, func(context.Context, string) (cloudWatchLogsAPI, error) { return client, nil })
	body := `{"request_id":"req-1","at_unix":` + cwUnix(time.Now().Unix()) + `}`
	if w := postCloudWatchEvidence(t, off, roleRoot, body); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("disabled endpoint status=%d", w.Code)
	}
	on := newCloudWatchHTTPTestMonitor(t, client, nil)
	for _, bad := range []string{`{}`, `{"request_id":"req 1","at_unix":1}`, `not json`, `{"request_id":"req-1"}`} {
		if w := postCloudWatchEvidence(t, on, roleRoot, bad); w.Code != http.StatusBadRequest {
			t.Fatalf("bad input %q status=%d", bad, w.Code)
		}
	}
}

// 敏感响应不得被任何中间层缓存：这条与既有两个排障接口同一要求。
func TestCloudWatchEvidenceEndpointIsNoStore(t *testing.T) {
	m := newCloudWatchHTTPTestMonitor(t, &fakeCloudWatchLogsClient{}, nil)
	body := `{"request_id":"req-1","at_unix":` + cwUnix(time.Now().Unix()) + `}`
	w := postCloudWatchEvidence(t, m, roleRoot, body)
	if got := w.Header().Get("Cache-Control"); !strings.Contains(got, "no-store") {
		t.Fatalf("Cache-Control=%q", got)
	}
}

// 时间戳转字符串直接用 strconv：仓库已有同名 itoa（portal_test.go），不再重复定义。
func cwUnix(v int64) string { return strconv.FormatInt(v, 10) }
