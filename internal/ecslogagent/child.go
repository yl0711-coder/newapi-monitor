package ecslogagent

import (
	"path/filepath"
	"sort"
	"strings"
)

// ChildEnvironment binds the original collector to the source before any log
// is read. All cursors/outboxes are under that source's private directory. It
// intentionally does not enable source V2 cutover or reuse a legacy cursor.
func (a *Agent) ChildEnvironment(parent []string) []string {
	env := map[string]string{}
	for _, entry := range parent {
		if key, value, ok := strings.Cut(entry, "="); ok {
			env[key] = value
		}
	}
	base := strings.TrimRight(a.cfg.MonitorURL, "/")
	if a.cfg.FinalLogRoot != "" {
		access, reject := a.cfg.finalSourcePatterns()
		env["NGINXCOLLECTOR_LOG_PATH"] = filepath.Join(a.cfg.FinalLogRoot, access)
		env["NGINXCOLLECTOR_ERROR_LOG_PATH"] = filepath.Join(a.cfg.FinalLogRoot, "error.log")
		env["COLLECTOR_LOG_GLOB"] = filepath.Join(a.cfg.FinalLogRoot, reject)
	}
	if a.cfg.Kind == "nginx" {
		for key, value := range map[string]string{
			"NGINXCOLLECTOR_NODE": a.node, "NGINXCOLLECTOR_TOKEN": a.token, "NGINXCOLLECTOR_ECS_SOCKET": a.SocketPath(),
			"NGINXCOLLECTOR_SINK_URL": base + "/internal/nginx", "NGINXCOLLECTOR_ERROR_SINK_URL": base + "/internal/nginx-errors", "NGINXCOLLECTOR_EVIDENCE_SINK_URL": base + "/internal/nginx-evidence/v1",
			"NGINXCOLLECTOR_CURSOR_PATH": filepath.Join(a.dir, "access-cursor.json"), "NGINXCOLLECTOR_ERROR_CURSOR_PATH": filepath.Join(a.dir, "error-cursor.json"), "NGINXCOLLECTOR_EVIDENCE_OUTBOX_PATH": filepath.Join(a.dir, "evidence-outbox"),
			"NGINXCOLLECTOR_ERROR_ENABLED": "true", "NGINXCOLLECTOR_EVIDENCE_MODE": "pilot", "NGINXCOLLECTOR_EVIDENCE_FROZEN_HEARTBEAT": "true", "NGINXCOLLECTOR_ALLOW_INSECURE_HTTP": "false", "NGINXCOLLECTOR_SOURCE_V2_PREPARE": "false", "NGINXCOLLECTOR_SOURCE_V2_LANES": "",
		} {
			env[key] = value
		}
	} else {
		for key, value := range map[string]string{"COLLECTOR_NODE": a.node, "COLLECTOR_SINK_TOKEN": a.token, "COLLECTOR_ECS_SOCKET": a.SocketPath(), "COLLECTOR_SINK_URL": base + "/internal/rejections/v2", "COLLECTOR_STATE_PATH": filepath.Join(a.dir, "reject-state.json")} {
			env[key] = value
		}
	}
	// Task IAM credentials belong to this proxy, not to the parsing child. On
	// Fargate the task remains the security boundary; this is not tenant isolation.
	for _, key := range []string{"AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN", "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI", "AWS_CONTAINER_CREDENTIALS_FULL_URI", "ECS_LOG_BRIDGE_TOKEN", "MONITOR_ECS_LOG_BRIDGE_TOKEN"} {
		delete(env, key)
	}
	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	result := make([]string, 0, len(keys))
	for _, key := range keys {
		result = append(result, key+"="+env[key])
	}
	return result
}
