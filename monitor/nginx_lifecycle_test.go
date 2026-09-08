package monitor

import (
	"context"
	"os"
	"testing"
)

func TestNginxExpectationsDoNotRevokeHistoricalIngestion(t *testing.T) {
	m := newTestMonitor(t)
	m.cfg.NginxAllowedNodes = []string{"old", "active"}
	if len(m.nginxSources(context.Background(), 1800000000)) != 2 {
		t.Fatal("unset expected nodes must preserve old behavior")
	}
	m.cfg.NginxExpectedNodes = []string{"active"}
	for _, source := range m.nginxSources(context.Background(), 1800000000) {
		if source.Node != "active" {
			t.Fatal("retired access source still expected")
		}
	}
	for _, source := range m.nginxErrorSources(context.Background(), 1800000000) {
		if source.Node != "active" {
			t.Fatal("retired error source still expected")
		}
	}
	if !m.nginxNodeAllowed("old") || m.nginxNodeAllowed("foreign") {
		t.Fatal("health settings changed ingestion authorization")
	}
	m.cfg.NginxExpectedNodes = []string{}
	if len(m.nginxSources(context.Background(), 1800000000)) != 0 || len(m.nginxErrorSources(context.Background(), 1800000000)) != 0 {
		t.Fatal("explicit empty expected nodes not respected")
	}
}

func TestNginxExpectedNodesValidationAndUnset(t *testing.T) {
	s := Settings{NginxEnabled: true, IngestToken: "test", NginxAllowedNodes: []string{"active"}}
	for _, nodes := range [][]string{{"foreign"}, {"active", "active"}} {
		s.NginxExpectedNodes = nodes
		if validateNginxSettings(s) == nil {
			t.Fatal("invalid expected nodes accepted")
		}
	}
	const key = "MONITOR_NGINX_EXPECTED_NODES"
	t.Setenv(key, "")
	if got := envOptionalCSV(key); got == nil || len(got) != 0 {
		t.Fatal("explicit empty lost")
	}
	if err := os.Unsetenv(key); err != nil {
		t.Fatal(err)
	}
	if envOptionalCSV(key) != nil {
		t.Fatal("unset should inherit allowed nodes")
	}
}
