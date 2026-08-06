package monitor

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/gin-gonic/gin"
)

const (
	maxChannelManagementChannels = 2000
	maxChannelManagementRows     = 5000
)

// ChannelUsageMetrics 是渠道管理页的客观使用量口径。
// CostUSD 来自 NewAPI logs.quota，表示用户侧消费，不是上游账单或平台利润。
type ChannelUsageMetrics struct {
	Requests int64   `json:"requests"`
	Tokens   int64   `json:"tokens"`
	CostUSD  float64 `json:"cost_usd"`
}

type ChannelManagementGroup struct {
	Name    string                  `json:"name"`
	Usage   ChannelUsageMetrics     `json:"usage"`
	Finance ChannelGroupFinanceView `json:"finance"`
}

type ChannelManagementFinanceGroup struct {
	Name    string                  `json:"name"`
	Finance ChannelGroupFinanceView `json:"finance"`
}

type ChannelManagementChannel struct {
	ID               int                      `json:"id"`
	Name             string                   `json:"name"`
	Host             string                   `json:"host"`
	Vendor           string                   `json:"vendor"`
	Status           int                      `json:"status"`
	Current          bool                     `json:"current"`
	ConfiguredGroups []string                 `json:"configured_groups"`
	ModelCount       int                      `json:"model_count"`
	Usage            ChannelUsageMetrics      `json:"usage"`
	Groups           []ChannelManagementGroup `json:"groups"`
}

type ChannelManagementVendor struct {
	Name     string                     `json:"name"`
	Usage    ChannelUsageMetrics        `json:"usage"`
	Channels []ChannelManagementChannel `json:"channels"`
}

type ChannelManagementDomain struct {
	Key           string                          `json:"key"`
	Domain        string                          `json:"domain"`
	Configured    bool                            `json:"configured"`
	Usage         ChannelUsageMetrics             `json:"usage"`
	Finance       ChannelDomainFinanceView        `json:"finance"`
	FinanceGroups []ChannelManagementFinanceGroup `json:"finance_groups"`
	Groups        []ChannelManagementGroup        `json:"groups"`
	Vendors       []ChannelManagementVendor       `json:"vendors"`
}

type ChannelManagementFilters struct {
	Domains []string `json:"domains"`
	Vendors []string `json:"vendors"`
	Groups  []string `json:"groups"`
}

type ChannelManagementSummary struct {
	ConfiguredDomains  int                 `json:"configured_domains"`
	CurrentChannels    int                 `json:"current_channels"`
	EnabledChannels    int                 `json:"enabled_channels"`
	Unconfigured       int                 `json:"unconfigured_channels"`
	HistoricalChannels int                 `json:"historical_channels"`
	Usage              ChannelUsageMetrics `json:"usage"`
}

type ChannelManagementMeta struct {
	From                   string `json:"from"`
	To                     string `json:"to"`
	GeneratedAt            int64  `json:"generated_at"`
	DataUntil              int64  `json:"data_until"`
	LatestDataUntil        int64  `json:"latest_data_until"`
	ChannelConfigUpdatedAt int64  `json:"channel_config_updated_at"`
	TimeZone               string `json:"time_zone"`
	Source                 string `json:"source"`
}

type ChannelManagementReport struct {
	Enabled bool                       `json:"enabled"`
	Meta    ChannelManagementMeta      `json:"meta"`
	Finance ChannelFinanceSettingsView `json:"finance"`
	Summary ChannelManagementSummary   `json:"summary"`
	Filters ChannelManagementFilters   `json:"filters"`
	Domains []ChannelManagementDomain  `json:"domains"`
}

type channelUsageAgg struct {
	requests int64
	tokens   int64
	quota    int64
}

func (a *channelUsageAgg) add(other channelUsageAgg) {
	a.requests += other.requests
	a.tokens += other.tokens
	a.quota += other.quota
}

func (a channelUsageAgg) metrics() ChannelUsageMetrics {
	return ChannelUsageMetrics{Requests: a.requests, Tokens: a.tokens, CostUSD: float64(a.quota) / quotaPerUSD}
}

type channelManagementBuild struct {
	ID               int
	Name             string
	Vendor           string
	Status           int
	Current          bool
	BaseDomain       string
	BaseHost         string
	ConfiguredGroups []string
	ModelCount       int
	Usage            channelUsageAgg
	Groups           map[string]*channelUsageAgg
}

type channelVendorBuild struct {
	Name     string
	Usage    channelUsageAgg
	Channels []*channelManagementBuild
}

type channelDomainBuild struct {
	Key           string
	Domain        string
	Configured    bool
	Usage         channelUsageAgg
	Groups        map[string]*channelUsageAgg
	FinanceGroups map[string]bool
	Vendors       map[string]*channelVendorBuild
}

func sortedUnique(items []string) []string {
	set := make(map[string]bool, len(items))
	for _, item := range items {
		if item = strings.TrimSpace(item); item != "" {
			set[item] = true
		}
	}
	out := make([]string, 0, len(set))
	for item := range set {
		out = append(out, item)
	}
	sort.Strings(out)
	return out
}

func usageAggLess(a, b channelUsageAgg) bool {
	if a.quota != b.quota {
		return a.quota > b.quota
	}
	if a.requests != b.requests {
		return a.requests > b.requests
	}
	return a.tokens > b.tokens
}

func managementGroups(rows map[string]*channelUsageAgg, domain string, finance channelFinanceSnapshot) []ChannelManagementGroup {
	type groupBuild struct {
		name  string
		usage channelUsageAgg
	}
	build := make([]groupBuild, 0, len(rows))
	for name, usage := range rows {
		if name == "" {
			name = "未标记服务分组"
		}
		build = append(build, groupBuild{name: name, usage: *usage})
	}
	sort.Slice(build, func(i, j int) bool {
		if usageAggLess(build[i].usage, build[j].usage) {
			return true
		}
		if usageAggLess(build[j].usage, build[i].usage) {
			return false
		}
		return build[i].name < build[j].name
	})
	out := make([]ChannelManagementGroup, 0, len(build))
	for _, group := range build {
		out = append(out, ChannelManagementGroup{
			Name: group.name, Usage: group.usage.metrics(), Finance: finance.groupView(domain, group.name),
		})
	}
	return out
}

func managementFinanceGroups(names map[string]bool, domain string, finance channelFinanceSnapshot) []ChannelManagementFinanceGroup {
	out := make([]ChannelManagementFinanceGroup, 0, len(names))
	for name := range names {
		name = strings.TrimSpace(name)
		if name == "" || name == "未标记服务分组" {
			continue
		}
		out = append(out, ChannelManagementFinanceGroup{Name: name, Finance: finance.groupView(domain, name)})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (m *Monitor) buildChannelManagementReport(ctx context.Context, scope stabilityScope, now int64) (*ChannelManagementReport, error) {
	finance, err := m.loadChannelFinanceSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	var snaps []struct {
		ID, Type, Status int
		Name             string
		Vendor           string
		BaseDomain       string
		BaseHost         string
		Groups           string
		Models           string
		UpdatedAt        int64
		DeletedAt        int64
	}
	tx := m.storeDB.WithContext(ctx).Raw(`SELECT id,type,status,COALESCE(name,'') name,
		COALESCE(vendor,'') vendor,COALESCE(base_domain,'') base_domain,COALESCE(base_host,'') base_host,
		COALESCE(groups,'') groups,COALESCE(models,'') models,updated_at,deleted_at
		FROM channel_snaps ORDER BY id LIMIT ?`, maxChannelManagementChannels+1).Scan(&snaps)
	if tx.Error != nil {
		return nil, fmt.Errorf("读取渠道快照: %w", tx.Error)
	}
	if len(snaps) > maxChannelManagementChannels {
		return nil, fmt.Errorf("渠道数超过安全上限 %d，为保证准确性已拒绝返回部分结果", maxChannelManagementChannels)
	}

	channels := make(map[int]*channelManagementBuild, len(snaps))
	configuredDomainSet := map[string]bool{}
	vendorSet, groupSet := map[string]bool{}, map[string]bool{}
	var configUpdatedAt int64
	currentChannels, enabledChannels, unconfiguredChannels := 0, 0, 0
	for _, snap := range snaps {
		vendor := strings.TrimSpace(snap.Vendor)
		if vendor == "" {
			vendor = newAPIChannelTypeName(snap.Type)
		}
		if vendor == "" {
			vendor = "未识别厂商"
		}
		name := strings.TrimSpace(snap.Name)
		if name == "" {
			name = fmt.Sprintf("渠道 #%d", snap.ID)
		}
		configuredGroups := sortedUnique(splitList(snap.Groups))
		current := snap.DeletedAt == 0
		channels[snap.ID] = &channelManagementBuild{
			ID: snap.ID, Name: name, Vendor: vendor, Status: snap.Status, Current: current,
			BaseDomain: strings.TrimSpace(snap.BaseDomain), BaseHost: strings.TrimSpace(snap.BaseHost),
			ConfiguredGroups: configuredGroups,
			ModelCount:       len(sortedUnique(splitList(snap.Models))), Groups: map[string]*channelUsageAgg{},
		}
		if current {
			currentChannels++
			if snap.Status == 1 {
				enabledChannels++
			}
			if snap.BaseDomain != "" {
				configuredDomainSet[snap.BaseDomain] = true
			} else {
				unconfiguredChannels++
			}
		}
		vendorSet[vendor] = true
		for _, group := range configuredGroups {
			groupSet[group] = true
		}
		if snap.UpdatedAt > configUpdatedAt {
			configUpdatedAt = snap.UpdatedAt
		}
	}

	var usageRows []struct {
		ChannelID int
		Grp       string
		Requests  int64
		Tokens    int64
		Quota     int64
	}
	tx = m.storeDB.WithContext(ctx).Raw(`SELECT channel_id,COALESCE(grp,'') grp,
		COALESCE(SUM(success+anomaly+failed),0) requests,
		COALESCE(SUM(tokens),0) tokens,COALESCE(SUM(quota),0) quota
		FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<?
		GROUP BY channel_id,grp LIMIT ?`, scope.FromTs, scope.ToTs, maxChannelManagementRows+1).Scan(&usageRows)
	if tx.Error != nil {
		return nil, fmt.Errorf("读取渠道用量汇总: %w", tx.Error)
	}
	if len(usageRows) > maxChannelManagementRows {
		return nil, fmt.Errorf("渠道×服务分组维度超过安全上限 %d，为保证准确性已拒绝返回部分结果", maxChannelManagementRows)
	}
	for _, row := range usageRows {
		ch := channels[row.ChannelID]
		if ch == nil {
			// 小时汇总可能仍保留已删除渠道的历史使用量。为保证总数守恒，
			// 显式列为历史未知渠道，不把它猜到任何当前域名/厂商。
			ch = &channelManagementBuild{
				ID: row.ChannelID, Name: fmt.Sprintf("历史渠道 #%d", row.ChannelID), Vendor: "历史未知",
				Current: false, Groups: map[string]*channelUsageAgg{},
			}
			channels[row.ChannelID] = ch
		}
		usage := channelUsageAgg{requests: row.Requests, tokens: row.Tokens, quota: row.Quota}
		ch.Usage.add(usage)
		groupName := strings.TrimSpace(row.Grp)
		if groupName == "" {
			groupName = "未标记服务分组"
		}
		if ch.Groups[groupName] == nil {
			ch.Groups[groupName] = &channelUsageAgg{}
		}
		ch.Groups[groupName].add(usage)
		groupSet[groupName] = true
	}

	domains := map[string]*channelDomainBuild{}
	var total channelUsageAgg
	historicalChannels := 0
	for _, ch := range channels {
		key, label, configured := "domain:"+ch.BaseDomain, ch.BaseDomain, ch.BaseDomain != ""
		if !ch.Current {
			historicalChannels++
			// 已删除渠道保留最后快照；有主域名时仍归入原渠道，便于查看历史。
			if ch.BaseDomain == "" {
				key, label, configured = "special:historical", "历史未知渠道", false
			}
		} else if ch.BaseDomain == "" {
			key, label, configured = "special:unconfigured", "未配置主地址", false
		}
		domain := domains[key]
		if domain == nil {
			domain = &channelDomainBuild{Key: key, Domain: label, Configured: configured, Groups: map[string]*channelUsageAgg{}, FinanceGroups: map[string]bool{}, Vendors: map[string]*channelVendorBuild{}}
			domains[key] = domain
		}
		domain.Usage.add(ch.Usage)
		total.add(ch.Usage)
		for group, usage := range ch.Groups {
			domain.FinanceGroups[group] = true
			if domain.Groups[group] == nil {
				domain.Groups[group] = &channelUsageAgg{}
			}
			domain.Groups[group].add(*usage)
		}
		for _, group := range ch.ConfiguredGroups {
			domain.FinanceGroups[group] = true
		}
		vendor := domain.Vendors[ch.Vendor]
		if vendor == nil {
			vendor = &channelVendorBuild{Name: ch.Vendor}
			domain.Vendors[ch.Vendor] = vendor
		}
		vendor.Usage.add(ch.Usage)
		vendor.Channels = append(vendor.Channels, ch)
	}

	responseDomains := make([]ChannelManagementDomain, 0, len(domains))
	for _, domain := range domains {
		vendors := make([]ChannelManagementVendor, 0, len(domain.Vendors))
		for _, vendor := range domain.Vendors {
			sort.Slice(vendor.Channels, func(i, j int) bool {
				if usageAggLess(vendor.Channels[i].Usage, vendor.Channels[j].Usage) {
					return true
				}
				if usageAggLess(vendor.Channels[j].Usage, vendor.Channels[i].Usage) {
					return false
				}
				return vendor.Channels[i].ID < vendor.Channels[j].ID
			})
			outChannels := make([]ChannelManagementChannel, 0, len(vendor.Channels))
			for _, ch := range vendor.Channels {
				outChannels = append(outChannels, ChannelManagementChannel{
					ID: ch.ID, Name: ch.Name, Host: ch.BaseHost, Vendor: ch.Vendor, Status: ch.Status, Current: ch.Current,
					ConfiguredGroups: ch.ConfiguredGroups, ModelCount: ch.ModelCount,
					Usage: ch.Usage.metrics(), Groups: managementGroups(ch.Groups, ch.BaseDomain, finance),
				})
			}
			vendors = append(vendors, ChannelManagementVendor{Name: vendor.Name, Usage: vendor.Usage.metrics(), Channels: outChannels})
		}
		sort.Slice(vendors, func(i, j int) bool {
			a := domain.Vendors[vendors[i].Name].Usage
			b := domain.Vendors[vendors[j].Name].Usage
			if usageAggLess(a, b) {
				return true
			}
			if usageAggLess(b, a) {
				return false
			}
			return vendors[i].Name < vendors[j].Name
		})
		responseDomains = append(responseDomains, ChannelManagementDomain{
			Key: domain.Key, Domain: domain.Domain, Configured: domain.Configured,
			Usage: domain.Usage.metrics(), Finance: finance.domainView(domain.Domain),
			FinanceGroups: managementFinanceGroups(domain.FinanceGroups, domain.Domain, finance),
			Groups:        managementGroups(domain.Groups, domain.Domain, finance), Vendors: vendors,
		})
	}
	sort.Slice(responseDomains, func(i, j int) bool {
		if responseDomains[i].Configured != responseDomains[j].Configured {
			return responseDomains[i].Configured
		}
		a, b := domains[responseDomains[i].Key].Usage, domains[responseDomains[j].Key].Usage
		if usageAggLess(a, b) {
			return true
		}
		if usageAggLess(b, a) {
			return false
		}
		return responseDomains[i].Domain < responseDomains[j].Domain
	})

	var coverage struct{ Max int64 }
	if tx := m.storeDB.WithContext(ctx).Raw("SELECT COALESCE(MAX(hour_ts),0) max FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<?", scope.FromTs, scope.ToTs).Scan(&coverage); tx.Error != nil {
		return nil, fmt.Errorf("读取渠道用量新鲜度: %w", tx.Error)
	}
	dataUntil := coverage.Max
	if dataUntil > 0 {
		dataUntil += 3600
		if dataUntil > now {
			dataUntil = now
		}
	}
	var latestCoverage struct{ Max int64 }
	if tx := m.storeDB.WithContext(ctx).Raw("SELECT COALESCE(MAX(hour_ts),0) max FROM stability_hour_samples").Scan(&latestCoverage); tx.Error != nil {
		return nil, fmt.Errorf("读取渠道用量全局新鲜度: %w", tx.Error)
	}
	latestDataUntil := latestCoverage.Max
	if latestDataUntil > 0 {
		latestDataUntil += 3600
		if latestDataUntil > now {
			latestDataUntil = now
		}
	}

	filterDomains := make([]string, 0, len(configuredDomainSet))
	for domain := range configuredDomainSet {
		filterDomains = append(filterDomains, domain)
	}
	filterVendors := make([]string, 0, len(vendorSet))
	for vendor := range vendorSet {
		filterVendors = append(filterVendors, vendor)
	}
	filterGroups := make([]string, 0, len(groupSet))
	for group := range groupSet {
		filterGroups = append(filterGroups, group)
	}
	sort.Strings(filterDomains)
	sort.Strings(filterVendors)
	sort.Strings(filterGroups)

	toTs := scope.ToTs
	if toTs > scope.FromTs {
		toTs--
	}
	return &ChannelManagementReport{
		Enabled: true,
		Finance: finance.settingsView(),
		Meta: ChannelManagementMeta{
			From: time.Unix(scope.FromTs, 0).In(cstLocation).Format("2006-01-02"),
			To:   time.Unix(toTs, 0).In(cstLocation).Format("2006-01-02"), GeneratedAt: now,
			DataUntil: dataUntil, LatestDataUntil: latestDataUntil, ChannelConfigUpdatedAt: configUpdatedAt,
			TimeZone: "Asia/Shanghai", Source: "monitor_local_hourly_rollup",
		},
		Summary: ChannelManagementSummary{
			ConfiguredDomains: len(configuredDomainSet), CurrentChannels: currentChannels,
			EnabledChannels: enabledChannels, Unconfigured: unconfiguredChannels,
			HistoricalChannels: historicalChannels, Usage: total.metrics(),
		},
		Filters: ChannelManagementFilters{Domains: filterDomains, Vendors: filterVendors, Groups: filterGroups},
		Domains: responseDomains,
	}, nil
}

func (m *Monitor) serveChannelManagementReport(c *gin.Context) {
	if !m.cfg.StabilityEnabled {
		c.JSON(200, gin.H{"enabled": false})
		return
	}
	maxDays := m.cfg.StabilityRetentionDays
	if maxDays <= 0 || maxDays > 365 {
		maxDays = 90
	}
	scope, err := stabilityRange(c, time.Now(), maxDays)
	if err != nil {
		c.JSON(400, gin.H{"error": err.Error()})
		return
	}
	// 渠道管理页只接受日期范围；不复用稳定性页的其他维度查询参数。
	scope.Group, scope.Model, scope.Vendor, scope.ChannelID = "", "", "", 0
	ctx, cancel := context.WithTimeout(c.Request.Context(), 6*time.Second)
	defer cancel()
	report, err := m.buildChannelManagementReport(ctx, scope, time.Now().Unix())
	if err != nil {
		c.JSON(500, gin.H{"error": err.Error()})
		return
	}
	report.Finance.CanEdit = c.GetInt("urole") >= roleRoot
	c.JSON(200, report)
}
