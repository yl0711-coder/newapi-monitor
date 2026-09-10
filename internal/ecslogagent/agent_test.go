package ecslogagent

import (
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func testConfig(t *testing.T) (Config, Metadata) {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "ea-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	c := Config{Scope: "isolated", Kind: "reject", ServiceARN: "arn:aws:ecs:us-west-2:123456789012:service/fixture/worker", Container: "newapi", MonitorURL: "https://monitor.example", RegisterURL: "https://fixture.execute-api.us-west-2.amazonaws.com/isolated/register", Audience: "fixture-local-monitor", StateRoot: dir}
	return c, Metadata{TaskARN: "arn:aws:ecs:us-west-2:123456789012:task/fixture/" + strings.Repeat("a", 32), RuntimeID: "fixture-runtime"}
}

func fixtureCredentials() aws.CredentialsProvider {
	return aws.CredentialsProviderFunc(func(context.Context) (aws.Credentials, error) {
		return aws.Credentials{AccessKeyID: "fixture-id", SecretAccessKey: "fixture-secret", SessionToken: "fixture-session"}, nil
	})
}
func response(code int, body string) *http.Response {
	return &http.Response{StatusCode: code, Header: http.Header{"Content-Type": []string{"application/json"}}, Body: io.NopCloser(strings.NewReader(body))}
}

func testAgent(t *testing.T, c Config, meta Metadata, transport http.RoundTripper) *Agent {
	t.Helper()
	a, err := New(c, meta, fixtureCredentials(), &http.Client{Transport: transport})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = a.Close() })
	return a
}

func TestConfigAndMetadataFailClosed(t *testing.T) {
	c, meta := testConfig(t)
	if err := c.Validate(); err != nil {
		t.Fatal(err)
	}
	production := c
	production.Scope = "production"
	production.RegisterURL = strings.Replace(c.RegisterURL, "/isolated/", "/production/", 1)
	if err := production.Validate(); err != nil {
		t.Fatalf("production scope with matching AWS_IAM stage: %v", err)
	}
	for name, mutate := range map[string]func(*Config){"production": func(c *Config) { c.Scope = "production" }, "http": func(c *Config) { c.MonitorURL = "http://monitor.example" }, "redirect-principal": func(c *Config) { c.RegisterURL = "https://evil.example/isolated/register" }, "gateway-stage": func(c *Config) { c.RegisterURL = strings.ReplaceAll(c.RegisterURL, "/isolated/", "/prod/") }, "audience": func(c *Config) { c.Audience = "" }, "root": func(c *Config) { c.StateRoot = "/" }} {
		t.Run(name, func(t *testing.T) {
			bad := c
			mutate(&bad)
			if bad.Validate() == nil {
				t.Fatal("unsafe config accepted")
			}
		})
	}
	meta.TaskARN = strings.ReplaceAll(meta.TaskARN, ":task/fixture/", ":task/other/")
	if c.validateMetadata(meta) == nil {
		t.Fatal("wrong cluster accepted")
	}
	if _, err := ReadMetadata(context.Background(), "http://127.0.0.1/v4/fixture", "newapi"); err == nil {
		t.Fatal("metadata SSRF endpoint accepted")
	}
	if _, err := parseMetadata([]byte(`{"TaskARN":"fixture","Containers":[{"Name":"newapi","DockerId":"one"},{"Name":"newapi","DockerId":"two"}]}`), "newapi"); err == nil {
		t.Fatal("ambiguous producer accepted")
	}
}

func TestIdentityOwnershipRestartAndConfigBinding(t *testing.T) {
	c, meta := testConfig(t)
	cl := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("offline") })}
	a, err := New(c, meta, fixtureCredentials(), cl)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := New(c, meta, fixtureCredentials(), cl); err == nil {
		t.Fatal("two agent owners")
	}
	key, node, token, dir := a.key, a.node, a.token, a.dir
	if err := a.Close(); err != nil {
		t.Fatal(err)
	}
	b, err := New(c, meta, fixtureCredentials(), cl)
	if err != nil {
		t.Fatal(err)
	}
	if !key.Equal(b.key) || node != b.node || token != b.token {
		t.Fatal("identity changed after restart")
	}
	_ = b.Close()
	changed := c
	changed.Audience = "fixture-other-monitor"
	if _, err := New(changed, meta, fixtureCredentials(), cl); err == nil {
		t.Fatal("changed binding silently reused cursor")
	}
	if err := os.WriteFile(filepath.Join(dir, "pending-fixture"), []byte("preserve"), 0600); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, "identity.json")); err != nil {
		t.Fatal(err)
	}
	if _, err := New(c, meta, fixtureCredentials(), cl); err == nil {
		t.Fatal("lost signing identity silently regenerated beside pending state")
	}
}

func TestRegistrationSigningAndFrozenProxyRetry(t *testing.T) {
	c, meta := testConfig(t)
	var agent *Agent
	var bodies []string
	transport := roundTripFunc(func(r *http.Request) (*http.Response, error) {
		body, _ := io.ReadAll(r.Body)
		if r.URL.Path == "/isolated/register" {
			if !strings.HasPrefix(r.Header.Get("Authorization"), "AWS4-HMAC-SHA256 ") || r.Header.Get("X-Amz-Security-Token") != "fixture-session" || strings.Contains(string(body), "caller_arn") {
				t.Error("registration did not use IAM context boundary")
			}
			return response(200, `{"ok":true,"version":1,"node":"`+agent.node+`","lane":"reject","audience":"`+c.Audience+`","lease_until":`+strconv.FormatInt(time.Now().Unix()+900, 10)+`}`), nil
		}
		sig, err := base64.StdEncoding.DecodeString(r.Header.Get("X-Monitor-ECS-Signature"))
		if err != nil || !ed25519.Verify(agent.key.Public().(ed25519.PublicKey), signingMessage(c.Audience, agent.node, "reject", r.URL.Path, r.Header.Get("X-Monitor-ECS-Time"), body), sig) {
			t.Error("invalid outbound signature")
		}
		if r.Header.Get("Authorization") != "" || r.Header.Get("Cookie") != "" {
			t.Error("local credential forwarded")
		}
		bodies = append(bodies, string(body))
		return response(409, `{"error":"batch conflict"}`), nil
	})
	agent = testAgent(t, c, meta, transport)
	if err := agent.Renew(context.Background(), "reject"); err != nil {
		t.Fatal(err)
	}
	body := `{"node":"` + agent.node + `","batch_id":"frozen-id","samples":[]} `
	for i := 0; i < 2; i++ {
		req := httptest.NewRequest("POST", c.MonitorURL+"/internal/rejections/v2", strings.NewReader(body))
		req.Header.Set("Authorization", "Bearer "+agent.token)
		req.Header.Set("Cookie", "must-not-forward")
		w := httptest.NewRecorder()
		agent.ServeHTTP(w, req)
		if w.Code != 409 {
			t.Fatalf("real conflict hidden=%d %s", w.Code, w.Body.String())
		}
	}
	if len(bodies) != 2 || bodies[0] != body || bodies[1] != body {
		t.Fatal("proxy rewrote frozen body")
	}
	agent.mu.Lock()
	agent.leases["reject"] = time.Now().Unix() - 1
	agent.mu.Unlock()
	req := httptest.NewRequest("POST", c.MonitorURL+"/internal/rejections/v2", strings.NewReader(body))
	req.Header.Set("Authorization", "Bearer "+agent.token)
	w := httptest.NewRecorder()
	agent.ServeHTTP(w, req)
	if w.Code != 503 || len(bodies) != 2 {
		t.Fatal("expired lease fell back to shared-token delivery")
	}
}

func TestProxyDestinationAndAuthBoundaries(t *testing.T) {
	c, meta := testConfig(t)
	calls := 0
	a := testAgent(t, c, meta, roundTripFunc(func(*http.Request) (*http.Response, error) { calls++; return response(200, `{}`), nil }))
	for _, tc := range []struct {
		method, url, body, token string
		code                     int
	}{
		{"POST", c.MonitorURL + "/internal/rejections/v2", `{}`, "wrong", 401},
		{"POST", "https://other.example/internal/rejections/v2", `{}`, a.token, 400},
		{"GET", c.MonitorURL + "/internal/rejections/v2", `{}`, a.token, 400},
		{"POST", c.MonitorURL + "/internal/nginx", `{}`, a.token, 400},
		{"POST", c.MonitorURL + "/internal/rejections/v2?x=y", `{}`, a.token, 400},
		{"POST", c.MonitorURL + "/internal/rejections/v2", `{"node":"other"}`, a.token, 400},
	} {
		req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(tc.body))
		req.Header.Set("Authorization", "Bearer "+tc.token)
		w := httptest.NewRecorder()
		a.ServeHTTP(w, req)
		if w.Code != tc.code {
			t.Errorf("%s status=%d want=%d", tc.url, w.Code, tc.code)
		}
	}
	if calls != 0 {
		t.Fatal("invalid request reached upstream")
	}
}

func TestLeaseFailureDoesNotEraseOtherLaneAndConcurrency(t *testing.T) {
	c, meta := testConfig(t)
	c.Kind = "nginx"
	var a *Agent
	a = testAgent(t, c, meta, roundTripFunc(func(r *http.Request) (*http.Response, error) {
		var in map[string]string
		_ = json.NewDecoder(r.Body).Decode(&in)
		if in["lane"] == "error" {
			return response(503, `{}`), nil
		}
		return response(200, `{"ok":true,"version":1,"node":"`+a.node+`","lane":"`+in["lane"]+`","audience":"`+c.Audience+`","lease_until":`+strconv.FormatInt(time.Now().Unix()+900, 10)+`}`), nil
	}))
	var wg sync.WaitGroup
	for _, lane := range c.lanes() {
		wg.Add(1)
		go func() { defer wg.Done(); _ = a.Renew(context.Background(), lane) }()
	}
	wg.Wait()
	if a.lease("access") == 0 || a.lease("evidence") == 0 || a.lease("error") != 0 {
		t.Fatal("lane leases mixed")
	}
	for _, entry := range a.ChildEnvironment([]string{"AWS_SECRET_ACCESS_KEY=fixture", "MONITOR_ECS_LOG_BRIDGE_TOKEN=fixture"}) {
		if strings.HasPrefix(entry, "AWS_SECRET_ACCESS_KEY=") || strings.HasPrefix(entry, "MONITOR_ECS_LOG_BRIDGE_TOKEN=") {
			t.Fatal("parent secret leaked into collector environment")
		}
	}
}
