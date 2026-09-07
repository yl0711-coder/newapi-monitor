package monitor

import "os"

// A nil setting preserves legacy expectations. An explicit empty value stops
// heartbeat expectations without revoking authenticated historical ingestion.
func envOptionalCSV(key string) []string {
	if _, set := os.LookupEnv(key); !set {
		return nil
	}
	return append([]string{}, envCSV(key)...)
}

func (m *Monitor) nginxExpectedNodes() []string {
	if m.cfg.NginxExpectedNodes != nil {
		return m.cfg.NginxExpectedNodes
	}
	return m.cfg.NginxAllowedNodes
}
