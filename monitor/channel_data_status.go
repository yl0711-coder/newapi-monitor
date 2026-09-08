package monitor

import (
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/gin-gonic/gin"
)

// This diagnostic projection shares the report's window and accounting checks,
// but never loads channel/user dimensions or contacts a production source.
type channelDataStatusDomain struct {
	Domain              string                         `json:"domain"`
	Upstream            ChannelUpstreamAccountView     `json:"upstream"`
	UpstreamUsage       ChannelUpstreamUsageMetrics    `json:"upstream_usage"`
	NaturalDayBill      *ChannelUpstreamNaturalDayBill `json:"natural_day_bill,omitempty"`
	EnabledChannels     int                            `json:"enabled_channels"`
	MissingRateChannels int                            `json:"missing_rate_channels"`
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
	naturalDayBills, err := m.loadChannelUpstreamNaturalDayBills(ctx, scope, now, accounts, finance)
	if err != nil {
		return nil, err
	}
	result := &channelDataStatusResponse{
		Meta: ChannelManagementMeta{FromTs: scope.FromTs, ToTs: scope.ToTs, GeneratedAt: now, TimeZone: "Asia/Shanghai",
			DataCoverage: m.stabilityDataCoverage(ctx, scope.FromTs, scope.ToTs, now)},
		Domains: make([]channelDataStatusDomain, 0, len(accounts)),
	}
	byDomain := make(map[string]*channelDataStatusDomain, len(accounts))
	for domain, account := range accounts {
		byDomain[domain] = &channelDataStatusDomain{Domain: domain, Upstream: account, UpstreamUsage: usage[domain], NaturalDayBill: naturalDayBills[domain]}
	}
	// Missing accounts cannot be found by querying the account table alone.
	// Read only bounded local channel metadata, never usage dimensions/source logs.
	var channels []struct {
		ID         int
		BaseDomain string
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT id, COALESCE(base_domain,'') base_domain
		FROM channel_snaps WHERE deleted_at = 0 AND status = 1 ORDER BY id LIMIT ?`,
		maxChannelManagementChannels+1).Scan(&channels).Error; err != nil {
		return nil, err
	}
	if len(channels) > maxChannelManagementChannels {
		return nil, fmt.Errorf("渠道配置数超过安全上限，无法完整核验")
	}
	for _, channel := range channels {
		domain := channel.BaseDomain
		entry := byDomain[domain]
		if entry == nil {
			entry = &channelDataStatusDomain{Domain: domain}
			byDomain[domain] = entry
		}
		entry.EnabledChannels++
		if !finance.channelRateConfigured(domain, channel.ID) {
			entry.MissingRateChannels++
		}
	}
	for _, entry := range byDomain {
		result.Domains = append(result.Domains, *entry)
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
