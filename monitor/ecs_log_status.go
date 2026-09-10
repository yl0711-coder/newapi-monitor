package monitor

import (
	"context"
	"strconv"
	"time"

	"github.com/gin-gonic/gin"
)

type ecsLogSourceView struct {
	ECSLogSource
	Status string `json:"status"`
}

type ecsLogDiscoveryView struct {
	ECSLogDiscovery
	Status string `json:"status"`
}

func ecsLogDiscoveryStatus(discovery ECSLogDiscovery, now int64) string {
	if discovery.LastFailure >= discovery.LastSuccess && discovery.LastFailure > 0 {
		return "discovery_failed"
	}
	if discovery.LastSuccess == 0 {
		return "awaiting_discovery"
	}
	if now-discovery.LastSuccess > ecsLogDiscoveryStaleSeconds {
		return "discovery_stale"
	}
	return "discovered"
}

// Local-only read behind the existing operator role gate. UI requests never
// initiate AWS discovery. Pagination is explicit; historical sources remain.
func (m *Monitor) serveECSLogSources(c *gin.Context) {
	page := 1
	if value := c.Query("page"); value != "" {
		parsed, err := strconv.Atoi(value)
		if err != nil || parsed < 1 || parsed > ecsLogSourceLimit {
			c.JSON(400, gin.H{"error": "invalid page"})
			return
		}
		page = parsed
	}
	const pageSize = 100
	phase := c.DefaultQuery("phase", "all")
	if phase != "all" && phase != "active" && phase != "stopped" {
		c.JSON(400, gin.H{"error": "invalid source phase"})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 3*time.Second)
	defer cancel()
	sources, discoveries, err := m.ecsLogSnapshot(ctx)
	if err != nil {
		c.JSON(503, gin.H{"error": "ECS source registry unavailable"})
		return
	}
	now := time.Now().Unix()
	health := m.projectECSLogHealth(sources, discoveries, now)
	m.attachECSArchiveHealth(ctx, &health, now)
	views := make([]ecsLogSourceView, 0, pageSize)
	total := 0
	for _, source := range sources {
		if service := c.Query("service"); service != "" && source.ServiceARN != service {
			continue
		}
		if phase == "active" && source.StoppedAt > 0 || phase == "stopped" && source.StoppedAt == 0 {
			continue
		}
		if total >= (page-1)*pageSize && total < page*pageSize {
			views = append(views, ecsLogSourceView{ECSLogSource: source, Status: ecsLogSourceStatus(source, now)})
		}
		total++
	}
	discoveryViews := make([]ecsLogDiscoveryView, 0, len(discoveries))
	for _, discovery := range discoveries[:min(len(discoveries), ecsLogSourceLimit)] {
		discoveryViews = append(discoveryViews, ecsLogDiscoveryView{discovery, ecsLogDiscoveryStatus(discovery, now)})
	}
	c.JSON(200, gin.H{"enabled": ecsLogRuntimeEnabled(m.cfg), "scope": m.cfg.ECSLogScope, "production_active": m.cfg.ECSLogScope == ecsLogScopeProduction && ecsLogRuntimeEnabled(m.cfg), "production_ready": false, "sources": views, "total": total, "page": page, "page_size": pageSize, "has_more": page*pageSize < total, "phase": phase, "discoveries": discoveryViews, "discoveries_truncated": false, "health": health})
}
