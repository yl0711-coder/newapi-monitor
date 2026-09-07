package monitor

import (
	"context"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

// This diagnostic projection shares the report's window and accounting checks,
// but never loads channel/user dimensions or contacts a production source.
type channelDataStatusDomain struct {
	Domain        string                      `json:"domain"`
	Upstream      ChannelUpstreamAccountView  `json:"upstream"`
	UpstreamUsage ChannelUpstreamUsageMetrics `json:"upstream_usage"`
}

type channelDataStatusResponse struct {
	Meta    ChannelManagementMeta     `json:"meta"`
	Domains []channelDataStatusDomain `json:"domains"`
}

func (m *Monitor) buildChannelDataStatus(ctx context.Context, scope stabilityScope, now int64) (*channelDataStatusResponse, error) {
	scope = channelFinalizedScope(scope, now)
	finance, err := m.loadChannelFinanceSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	accounts, err := m.loadChannelUpstreamViews(ctx)
	if err != nil {
		return nil, err
	}
	usage, err := m.loadChannelUpstreamUsage(ctx, scope, now, accounts, finance)
	if err != nil {
		return nil, err
	}
	result := &channelDataStatusResponse{
		Meta: ChannelManagementMeta{FromTs: scope.FromTs, ToTs: scope.ToTs,
			DataCoverage: m.stabilityDataCoverage(ctx, scope.FromTs, scope.ToTs, now)},
		Domains: make([]channelDataStatusDomain, 0, len(accounts)),
	}
	for domain, account := range accounts {
		result.Domains = append(result.Domains, channelDataStatusDomain{domain, account, usage[domain]})
	}
	sort.Slice(result.Domains, func(i, j int) bool { return result.Domains[i].Domain < result.Domains[j].Domain })
	return result, nil
}

func (m *Monitor) serveChannelDataStatus(c *gin.Context) {
	c.Header("Cache-Control", "no-store")
	if !m.cfg.StabilityEnabled {
		c.JSON(200, gin.H{"enabled": false})
		return
	}
	now := time.Now()
	scope, err := channelManagementRange(c, now, m.cfg.stabilityQueryDays())
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(c.Request.Context(), 4*time.Second)
	defer cancel()
	result, err := m.buildChannelDataStatus(ctx, scope, now.Unix())
	if err != nil {
		writeStabilityReadError(c, err)
		return
	}
	c.JSON(200, result)
}
