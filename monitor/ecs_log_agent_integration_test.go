package monitor

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/gin-gonic/gin"
	"github.com/yl0711-coder/newapi-monitor/internal/ecslogagent"
)

type ecsAgentRoundTrip func(*http.Request) (*http.Response, error)

func (f ecsAgentRoundTrip) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// IAM integration is simulated ONLY at registration. All Ed25519 verification,
// four protocol handlers, SQLite transactions and Unix IPC are real local code.
func ecsAgentFixture(t *testing.T, m *Monitor, kind, taskID string, afterCommit func(string, []byte, *httptest.ResponseRecorder) bool, configure ...func(*ecslogagent.Config)) (*ecslogagent.Agent, ecslogagent.Config, ecslogagent.Metadata, *http.Client) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "eai-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	in := testECSRegistration(taskID, "")
	cfg := ecslogagent.Config{Scope: "isolated", Kind: kind, ServiceARN: in.ServiceARN, Container: in.Container, MonitorURL: "https://monitor.example", RegisterURL: "https://fixture.execute-api.us-west-2.amazonaws.com/isolated/register", Audience: m.cfg.ECSLogAudience, StateRoot: dir}
	for _, change := range configure {
		change(&cfg)
	}
	meta := ecslogagent.Metadata{TaskARN: in.TaskARN, RuntimeID: in.RuntimeID}
	r := gin.New()
	r.POST("/internal/ecs/v1/register", m.registerECSLogHTTP)
	r.POST("/internal/ecs/v1/ingest/:lane", m.ingestECSLogHTTP)
	r.POST("/internal/ecs/v1/heartbeat/:lane", m.heartbeatECSLogHTTP)
	client := &http.Client{Transport: ecsAgentRoundTrip(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		local := req.Clone(req.Context())
		urlCopy := *req.URL
		local.URL = &urlCopy
		if req.URL.Path == "/isolated/register" {
			if !strings.HasPrefix(req.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") {
				return nil, errors.New("unsigned registration")
			}
			var data map[string]string
			if json.Unmarshal(body, &data) != nil {
				return nil, errors.New("invalid fixture request")
			}
			if _, exists := data["caller_arn"]; exists {
				return nil, errors.New("caller self assertion")
			}
			data["caller_arn"] = in.CallerARN
			body, _ = json.Marshal(data)
			local.URL.Path = "/internal/ecs/v1/register"
			local.Header.Set("Authorization", "Bearer "+m.cfg.ECSLogBridgeToken)
		}
		local.Body = io.NopCloser(bytes.NewReader(body))
		w := httptest.NewRecorder()
		r.ServeHTTP(w, local)
		if afterCommit != nil && afterCommit(req.URL.Path, body, w) {
			return nil, errors.New("fixture ACK lost after commit")
		}
		return w.Result(), nil
	})}
	credentials := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "fixture-id", SecretAccessKey: "fixture-secret", SessionToken: "fixture-session"}, nil
	})
	agent, err := ecslogagent.New(cfg, meta, credentials, client)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = agent.Close() })
	return agent, cfg, meta, client
}

func privateCollectorClient(t *testing.T, agent *ecslogagent.Agent) (*http.Client, string) {
	t.Helper()
	listener, err := agent.Listen()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: agent, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(func() { _ = server.Close() })
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", agent.SocketPath())
	}}
	t.Cleanup(transport.CloseIdleConnections)
	var token string
	for _, entry := range agent.ChildEnvironment(nil) {
		if strings.HasPrefix(entry, "NGINXCOLLECTOR_TOKEN=") {
			token = strings.TrimPrefix(entry, "NGINXCOLLECTOR_TOKEN=")
		}
		if strings.HasPrefix(entry, "COLLECTOR_SINK_TOKEN=") {
			token = strings.TrimPrefix(entry, "COLLECTOR_SINK_TOKEN=")
		}
	}
	return &http.Client{Transport: transport, Timeout: 5 * time.Second}, token
}

func TestECSLogAgentFourLaneEndToEnd(t *testing.T) {
	m := newECSLogTestMonitor(t)
	for _, kind := range []string{"nginx", "reject"} {
		agent, _, _, _ := ecsAgentFixture(t, m, kind, strings.Repeat("a", 32), nil)
		client, token := privateCollectorClient(t, agent)
		lanes := map[string]string{"reject": "/internal/rejections/v2"}
		if kind == "nginx" {
			lanes = map[string]string{"access": "/internal/nginx", "error": "/internal/nginx-errors", "evidence": "/internal/nginx-evidence/v1"}
		}
		for lane, path := range lanes {
			if err := agent.Renew(context.Background(), lane); err != nil {
				t.Fatalf("renew %s: %v", lane, err)
			}
			if err := agent.Heartbeat(context.Background(), lane); err != nil {
				t.Fatalf("heartbeat %s: %v", lane, err)
			}
			body := ecsLaneTestBody(t, agent.Node(), lane, 1)
			for retry := 0; retry < 2; retry++ {
				req, _ := http.NewRequest("POST", "http://monitor.example"+path, bytes.NewReader(body))
				req.Header.Set("Authorization", "Bearer "+token)
				resp, err := client.Do(req)
				if err != nil {
					t.Fatal(err)
				}
				data, _ := io.ReadAll(resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Fatalf("lane=%s retry=%d %d %s", lane, retry, resp.StatusCode, data)
				}
			}
		}
	}
	for _, model := range []any{&NginxIngestBatch{}, &NginxErrorIngestBatch{}, &RejectionIngestBatch{}} {
		var count int64
		if err := m.storeDB.Model(model).Count(&count).Error; err != nil || count != 1 {
			t.Fatalf("receipt %T count=%d %v", model, count, err)
		}
	}
	var count int64
	if err := m.nginxEvidenceDB.Model(&NginxEvidenceIngestBatch{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("evidence count=%d %v", count, err)
	}
}

func TestECSLogAgentRealRejectChildRestart(t *testing.T) {
	binary := os.Getenv("MONITOR_TEST_REJECT_COLLECTOR_BIN")
	if binary == "" {
		t.Skip("requires locally built reject collector")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute local binary required")
	}
	m := newECSLogTestMonitor(t)
	var mu sync.Mutex
	var bodies [][]byte
	received := make(chan struct{}, 4)
	agent, cfg, meta, client := ecsAgentFixture(t, m, "reject", strings.Repeat("b", 32), func(path string, body []byte, w *httptest.ResponseRecorder) bool {
		if path != "/internal/ecs/v1/ingest/reject" {
			return false
		}
		if w.Code != 200 {
			t.Errorf("real receiver=%d %s", w.Code, w.Body.String())
		}
		mu.Lock()
		bodies = append(bodies, append([]byte(nil), body...))
		first := len(bodies) == 1
		mu.Unlock()
		received <- struct{}{}
		return first
	})
	dir := t.TempDir()
	path := filepath.Join(dir, "new-api.log")
	line := "[ERR] " + time.Now().UTC().Format("2006/01/02 - 15:04:05") + " | fixture | user 7 | No available channel for model fixture under group fixture-group (distributor)\n"
	if err := os.WriteFile(path, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	start := func(a *ecslogagent.Agent) (*exec.Cmd, *http.Server) {
		t.Helper()
		if err := a.Renew(context.Background(), "reject"); err != nil {
			t.Fatal(err)
		}
		listener, err := a.Listen()
		if err != nil {
			t.Fatal(err)
		}
		server := &http.Server{Handler: a, ReadHeaderTimeout: time.Second}
		go func() { _ = server.Serve(listener) }()
		cmd := exec.Command(binary)
		cmd.Env = a.ChildEnvironment([]string{"COLLECTOR_LOG_GLOB=" + path, "COLLECTOR_LOG_TIMEZONE=UTC", "COLLECTOR_FLUSH_SECONDS=5"})
		if err := cmd.Start(); err != nil {
			_ = server.Close()
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = cmd.Process.Kill(); _ = server.Close() })
		return cmd, server
	}
	wait := func() {
		t.Helper()
		select {
		case <-received:
		case <-time.After(12 * time.Second):
			t.Fatal("child did not deliver through signing agent")
		}
	}
	first, server := start(agent)
	wait()
	_ = first.Process.Kill()
	_ = first.Wait()
	_ = server.Close()
	_ = agent.Close()
	credentials := aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "fixture-id", SecretAccessKey: "fixture-secret", SessionToken: "fixture-session"}, nil
	})
	reopened, err := ecslogagent.New(cfg, meta, credentials, client)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	second, secondServer := start(reopened)
	wait()
	_ = second.Process.Kill()
	_ = second.Wait()
	_ = secondServer.Close()
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 2 || !bytes.Equal(bodies[0], bodies[1]) {
		t.Fatal("restart changed frozen payload")
	}
	rows := m.storeRejections(time.Now().Unix() - 120)
	if len(rows) != 1 || rows[0].Count != 1 {
		t.Fatalf("restart lost/doubled facts: %+v", rows)
	}
}

func TestECSLogAgentRealNginxChildAllLanes(t *testing.T) {
	binary := os.Getenv("MONITOR_TEST_NGINX_COLLECTOR_BIN")
	if binary == "" {
		t.Skip("requires locally built nginx collector")
	}
	if !filepath.IsAbs(binary) {
		t.Fatal("absolute local binary required")
	}
	m := newECSLogTestMonitor(t)
	agent, _, _, _ := ecsAgentFixture(t, m, "nginx", strings.Repeat("c", 32), nil)
	for _, lane := range []string{"access", "error", "evidence"} {
		if err := agent.Renew(context.Background(), lane); err != nil {
			t.Fatal(err)
		}
	}
	listener, err := agent.Listen()
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: agent, ReadHeaderTimeout: time.Second}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	dir := t.TempDir()
	access := filepath.Join(dir, "access.jsonl")
	errorLog := filepath.Join(dir, "error.log")
	now := time.Now().UTC()
	line := fmt.Sprintf(`{"log_schema":2,"msec":"%d.250","request_method":"POST","uri":"/v1/responses","status":"200","request_time":"0.350","upstream_status":"200","upstream_response_time":"0.300","upstream_connect_time":"0.025","upstream_header_time":"0.125","bytes_sent":"1024","nginx_request_id":"fixture-nginx","oneapi_request_id":"fixture-api","request_completion":"OK"}`+"\n", now.Unix())
	if err := os.WriteFile(access, []byte(line), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(errorLog, []byte(now.Format("2006/01/02 15:04:05")+" [error] 1#1: *1 upstream timed out (110: Connection timed out) while reading response header from upstream\n"), 0600); err != nil {
		t.Fatal(err)
	}
	cmd := exec.Command(binary)
	cmd.Env = agent.ChildEnvironment([]string{"NGINXCOLLECTOR_LOG_PATH=" + access, "NGINXCOLLECTOR_ERROR_LOG_PATH=" + errorLog, "NGINXCOLLECTOR_ERROR_TIMEZONE=UTC", "NGINXCOLLECTOR_INTERVAL_SECONDS=1", "NGINXCOLLECTOR_EVIDENCE_HMAC_KEY=" + strings.Repeat("k", 32), "NGINXCOLLECTOR_EVIDENCE_HMAC_KEY_ID=key-1"})
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	defer func() { _ = cmd.Process.Kill(); _ = cmd.Wait() }()
	deadline := time.NewTimer(12 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		var accessCount, errorCount, evidenceCount int64
		if err := m.storeDB.Model(&NginxMinuteSample{}).Where("node = ?", agent.Node()).Select("COALESCE(SUM(count),0)").Scan(&accessCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Model(&NginxErrorMinuteSample{}).Where("node = ?", agent.Node()).Select("COALESCE(SUM(count),0)").Scan(&errorCount).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.nginxEvidenceDB.Model(&NginxRequestEvidence{}).Where("node = ?", agent.Node()).Count(&evidenceCount).Error; err != nil {
			t.Fatal(err)
		}
		if accessCount == 1 && errorCount == 1 && evidenceCount == 1 {
			return
		}
		select {
		case <-deadline.C:
			t.Fatalf("nginx process incomplete access=%d error=%d evidence=%d", accessCount, errorCount, evidenceCount)
		case <-tick.C:
		}
	}
}
