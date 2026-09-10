package monitor

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func acceptanceFixtureConfig() ECSAcceptanceConfig {
	return ECSAcceptanceConfig{
		Region: "us-west-2", Audience: "fixture-isolated-receiver",
		BridgeToken: strings.Repeat("b", 32), SessionKey: strings.Repeat("s", 32),
		IngestKey: strings.Repeat("i", 32), EvidenceKey: strings.Repeat("e", 32),
		Policies: []ECSLogPolicy{testECSLogPolicy()},
		Bucket:   "fixture-archive", Prefix: "isolated/", Account: "123456789012",
	}
}

func TestECSLogAcceptanceBootWithoutBusinessDatabase(t *testing.T) {
	// A developer's shell may still contain production configuration. None of
	// it can leak into this constructor, including a previously enabled worker.
	t.Setenv("NEWAPI_LOG_DSN", "must-not-be-used")
	t.Setenv("MONITOR_SOURCE_WORKER_ENABLED", "true")
	t.Setenv("MONITOR_INFRA_ENABLED", "true")
	t.Setenv("MONITOR_LOCAL_AUTH_BYPASS", "true")
	c := acceptanceFixtureConfig()
	m, h, err := NewECSAcceptance(c, t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(m.Close)
	if m.prodDB != nil || m.cfg.ProdDSN != "" || m.cfg.sourceWorkerIsEnabled() || m.cfg.InfraEnabled || m.cfg.LocalAuthBypass || m.currentSourceState() != sourceStateDisabled {
		t.Fatal("acceptance inherited business configuration")
	}
	for _, path := range []string{"/", "/healthz", "/api/infra", "/internal/rejections/v2", "/internal/nginx-evidence/v1", "/internal/ecs/v1/register/", "/internal/ecs/v1/ingest/arbitrary", "/internal/ecs/v1/register?x=1", "/internal/ecs/v1/register?", "/internal/ecs/v1/%72egister"} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}")))
		if w.Code != http.StatusNotFound {
			t.Errorf("non-allowlisted route %s: %d", path, w.Code)
		}
	}
	for _, method := range []string{http.MethodGet, http.MethodOptions, http.MethodPut} {
		w := httptest.NewRecorder()
		h.ServeHTTP(w, httptest.NewRequest(method, "/internal/ecs/v1/register", nil))
		if w.Code != http.StatusNotFound {
			t.Fatalf("unexpected method accepted: %s", method)
		}
	}
	for _, path := range []string{"/internal/ecs/v1/register", "/internal/ecs/v1/ingest/access", "/internal/ecs/v1/heartbeat/reject"} {
		w := httptest.NewRecorder()
		r := httptest.NewRequest(http.MethodPost, path, strings.NewReader("{}"))
		r.Header.Set("Authorization", "Bearer "+c.IngestKey)
		h.ServeHTTP(w, r)
		if w.Code != http.StatusUnauthorized {
			t.Fatalf("legacy credential bypasses ECS authorization at %s: %d", path, w.Code)
		}
	}
}

func TestECSLogAcceptanceRejectsUnsafeConfiguration(t *testing.T) {
	for _, mutate := range []func(*ECSAcceptanceConfig){
		func(c *ECSAcceptanceConfig) { c.BridgeToken = c.IngestKey },
		func(c *ECSAcceptanceConfig) { c.EvidenceKey = "" },
		func(c *ECSAcceptanceConfig) { c.Policies = nil },
		func(c *ECSAcceptanceConfig) { c.Account = "111111111111" },
	} {
		c := acceptanceFixtureConfig()
		mutate(&c)
		m, _, err := NewECSAcceptance(c, t.TempDir())
		if m != nil {
			m.Close()
		}
		if err == nil {
			t.Fatal("unsafe isolated configuration accepted")
		}
	}
}
