package monitor

import (
	"encoding/json"
	"errors"
	"net/http"
	"path/filepath"

	"github.com/gin-gonic/gin"
)

// ECSAcceptanceConfig deliberately has no business DSN, upstream credentials,
// UI, proxy, or general Monitor configuration. This is a synthetic-data tool.
type ECSAcceptanceConfig struct {
	OwnershipEnabled bool           `json:"ownership_enabled,omitempty"`
	Region           string         `json:"region"`
	Audience         string         `json:"audience"`
	BridgeToken      string         `json:"bridge_token"`
	SessionKey       string         `json:"session_key"`
	IngestKey        string         `json:"ingest_key"`
	EvidenceKey      string         `json:"evidence_key"`
	Policies         []ECSLogPolicy `json:"policies"`
	Bucket           string         `json:"bucket"`
	Prefix           string         `json:"prefix"`
	Account          string         `json:"account"`
}

// NewECSAcceptance uses a caller-owned private, isolated directory. The CLI
// binds that directory to a configuration fingerprint before calling this.
// Settings are built explicitly: ambient MONITOR_* / NEWAPI_* are never loaded.
func NewECSAcceptance(c ECSAcceptanceConfig, dir string) (*Monitor, http.Handler, error) {
	if !filepath.IsAbs(dir) || dir == string(filepath.Separator) {
		return nil, nil, errors.New("absolute dedicated acceptance directory required")
	}
	keys := map[string]bool{}
	for _, key := range []string{c.BridgeToken, c.SessionKey, c.IngestKey, c.EvidenceKey} {
		if len(key) < 32 || keys[key] {
			return nil, nil, errors.New("four independent acceptance credentials of at least 32 bytes required")
		}
		keys[key] = true
	}
	policies, err := json.Marshal(c.Policies)
	if err != nil {
		return nil, nil, err
	}
	m, err := New(Settings{
		StorePath: filepath.Join(dir, "monitor.db"), SessionSecret: c.SessionKey,
		IngestToken: c.IngestKey, sourceLifecycleConfigured: true,
		AWSRegion: c.Region, ECSLogEnabled: true, ECSLogScope: "isolated",
		ECSLogOwnershipEnabled: c.OwnershipEnabled,
		ECSLogAudience:         c.Audience, ECSLogBridgeToken: c.BridgeToken, ECSLogPoliciesJSON: string(policies),
		ECSArchiveEnabled: true, ECSArchiveBucket: c.Bucket, ECSArchivePrefix: c.Prefix, ECSArchiveAccount: c.Account,
		NginxEnabled: true, NginxErrorEnabled: true, NginxRetentionDays: 2,
		NginxAllowedNodes: []string{"isolated-unused-static"}, NginxExpectedNodes: []string{},
		NginxEvidenceMode: "pilot", NginxEvidenceStorePath: filepath.Join(dir, "evidence.db"),
		NginxEvidenceRetentionHours: 48, NginxEvidenceMaxMiB: 64,
		NginxEvidenceHMACKey: c.EvidenceKey, NginxEvidenceHMACKeyID: "isolated-v1",
	})
	if err != nil {
		return nil, nil, err
	}
	if m.nginxEvidenceDB == nil {
		m.Close()
		return nil, nil, errors.New("isolated evidence receiver unavailable")
	}
	return m, m.ecsAcceptanceHandler(), nil
}

// No general RegisterRoutes call: even future UI/admin/legacy routes cannot
// accidentally become reachable through the temporary public tunnel.
func (m *Monitor) ecsAcceptanceHandler() http.Handler {
	r := gin.New()
	r.Use(gin.Recovery())
	r.RedirectTrailingSlash, r.RedirectFixedPath = false, false
	r.POST("/internal/ecs/v1/register", m.registerECSLogHTTP)
	r.POST("/internal/ecs/v1/ingest/:lane", m.ingestECSLogHTTP)
	r.POST("/internal/ecs/v1/heartbeat/:lane", m.heartbeatECSLogHTTP)
	allowed := map[string]bool{"/internal/ecs/v1/register": true}
	for _, lane := range []string{"access", "error", "evidence", "reject"} {
		allowed["/internal/ecs/v1/ingest/"+lane] = true
		allowed["/internal/ecs/v1/heartbeat/"+lane] = true
	}
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		if req.Method != http.MethodPost || !allowed[req.URL.Path] || req.URL.RawPath != "" || req.URL.RawQuery != "" || req.URL.ForceQuery {
			http.NotFound(w, req)
			return
		}
		req.Body = http.MaxBytesReader(w, req.Body, ecsLogMaxBody)
		r.ServeHTTP(w, req)
	})
}
