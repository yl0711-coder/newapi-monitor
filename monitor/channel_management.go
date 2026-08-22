package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
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

// ChannelManagementRateConfig 只表达渠道倍率配置是否足以做上游成本对照，
// 不把“网站分组倍率”或财务利润结算混入该判断。
type ChannelManagementRateConfig struct {
	EnabledChannels    int  `json:"enabled_channels"`
	ConfiguredChannels int  `json:"configured_channels"`
	Complete           bool `json:"complete"`
}

// ChannelUpstreamUsageMetrics 是上游账户使用数据的本地脱敏汇总。NewAPI 使用
// 小时日志，AICodeWith 使用中国自然日账单；它们均按主域名账户归集，不能推断为
// 某一条实际渠道的上游账单。
type ChannelUpstreamUsageMetrics struct {
	Available      bool    `json:"available"`
	Requests       int64   `json:"requests"`
	Tokens         int64   `json:"tokens"`
	CostUSD        float64 `json:"cost_usd"`
	ExpectedHours  int64   `json:"expected_hours"`
	CompletedHours int64   `json:"completed_hours"`
	Complete       bool    `json:"complete"`
	DataUntil      int64   `json:"data_until"`
	Granularity    string  `json:"granularity,omitempty"`
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
	Stability        *float64                 `json:"stability"`
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
	Upstream      ChannelUpstreamAccountView      `json:"upstream"`
	RateConfig    ChannelManagementRateConfig     `json:"rate_config"`
	UpstreamUsage ChannelUpstreamUsageMetrics     `json:"upstream_usage"`
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
	From                   string                `json:"from"`
	To                     string                `json:"to"`
	GeneratedAt            int64                 `json:"generated_at"`
	DataUntil              int64                 `json:"data_until"`
	LatestDataUntil        int64                 `json:"latest_data_until"`
	ChannelConfigUpdatedAt int64                 `json:"channel_config_updated_at"`
	TimeZone               string                `json:"time_zone"`
	Source                 string                `json:"source"`
	DataCoverage           StabilityDataCoverage `json:"data_coverage"`
}

type ChannelManagementReport struct {
	Enabled               bool                          `json:"enabled"`
	Meta                  ChannelManagementMeta         `json:"meta"`
	Finance               ChannelFinanceSettingsView    `json:"finance"`
	WebsiteGroups         []ChannelWebsiteGroupRateView `json:"website_groups"`
	WebsiteGroupsSyncedAt int64                         `json:"website_groups_synced_at"`
	Summary               ChannelManagementSummary      `json:"summary"`
	Filters               ChannelManagementFilters      `json:"filters"`
	Domains               []ChannelManagementDomain     `json:"domains"`
}

type channelUsageAgg struct {
	requests         int64
	success, anomaly int64
	failed           int64
	tokens           int64
	quota            int64
}

func (a *channelUsageAgg) add(other channelUsageAgg) {
	a.requests += other.requests
	a.success += other.success
	a.anomaly += other.anomaly
	a.failed += other.failed
	a.tokens += other.tokens
	a.quota += other.quota
}

func (a channelUsageAgg) metrics() ChannelUsageMetrics {
	return ChannelUsageMetrics{Requests: a.requests, Tokens: a.tokens, CostUSD: float64(a.quota) / quotaPerUSD}
}

func (a channelUsageAgg) stability() *float64 {
	total := a.success + a.anomaly + a.failed
	if total == 0 {
		return nil
	}
	value := rate(a.success, total)
	return &value
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

// channelManagementStatusRank 保证维护页优先呈现仍启用的真实渠道；停用/自动
// 禁用渠道保留用于核对，但统一置后，已删除历史快照最后显示。
func channelManagementStatusRank(ch *channelManagementBuild) int {
	if !ch.Current {
		return 2
	}
	if ch.Status == 1 {
		return 0
	}
	return 1
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

func managementGroups(rows map[string]*channelUsageAgg, configuredGroups []string, domain string, channelID int, finance channelFinanceSnapshot) []ChannelManagementGroup {
	type groupBuild struct {
		name  string
		usage channelUsageAgg
	}
	// 渠道倍率属于渠道配置，不依赖当前查询范围内是否已经产生用量。
	// 新渠道或低频渠道可能只有 channel_snaps.groups 而没有小时汇总；若只
	// 返回有用量的分组，前端只能补出空壳分组，已保存的倍率会被误显示为空。
	groupNames := make(map[string]bool, len(rows)+len(configuredGroups))
	for name := range rows {
		if strings.TrimSpace(name) == "" {
			name = "未标记服务分组"
		}
		groupNames[name] = true
	}
	for _, name := range configuredGroups {
		if name = strings.TrimSpace(name); name != "" {
			groupNames[name] = true
		}
	}
	build := make([]groupBuild, 0, len(groupNames))
	for name := range groupNames {
		usage := channelUsageAgg{}
		if row := rows[name]; row != nil {
			usage = *row
		} else if name == "未标记服务分组" {
			if row := rows[""]; row != nil {
				usage = *row
			}
		}
		build = append(build, groupBuild{name: name, usage: usage})
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
			Name: group.name, Usage: group.usage.metrics(), Finance: finance.groupViewForChannel(domain, channelID, group.name),
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

func managementRateConfig(domain *channelDomainBuild, finance channelFinanceSnapshot) ChannelManagementRateConfig {
	view := ChannelManagementRateConfig{}
	for _, vendor := range domain.Vendors {
		for _, channel := range vendor.Channels {
			if !channel.Current || channel.Status != 1 {
				continue
			}
			view.EnabledChannels++
			if finance.channelRateConfigured(domain.Domain, channel.ID) {
				view.ConfiguredChannels++
			}
		}
	}
	view.Complete = view.EnabledChannels > 0 && view.EnabledChannels == view.ConfiguredChannels
	return view
}

func expectedUpstreamUsageHours(scope stabilityScope, now int64) int64 {
	end := scope.ToTs
	completeUntil := now - now%3600
	if end > completeUntil {
		end = completeUntil
	}
	if end <= scope.FromTs {
		return 0
	}
	return (end - scope.FromTs) / 3600
}

func (m *Monitor) loadChannelUpstreamUsage(ctx context.Context, scope stabilityScope, now int64, accounts map[string]ChannelUpstreamAccountView) (map[string]ChannelUpstreamUsageMetrics, error) {
	type row struct {
		Domain           string
		Provider         string
		Requests         int64
		Tokens           int64
		CostUSD          float64
		CompletedSeconds int64
		DataUntil        int64
	}
	var rows []row
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,provider,
		COALESCE(SUM(requests),0) requests, COALESCE(SUM(tokens),0) tokens,
		COALESCE(SUM(cost_usd),0) cost_usd,
		COALESCE(SUM(CASE WHEN bucket_seconds>0 THEN bucket_seconds ELSE 3600 END),0) completed_seconds,
		COALESCE(MAX(hour_ts+(CASE WHEN bucket_seconds>0 THEN bucket_seconds ELSE 3600 END)),0) data_until
		FROM channel_upstream_usage_hours
		WHERE hour_ts >= ?
		  AND hour_ts+(CASE WHEN bucket_seconds>0 THEN bucket_seconds ELSE 3600 END) <= ?
		GROUP BY domain,provider`, scope.FromTs, scope.ToTs).Scan(&rows).Error; err != nil {
		return nil, err
	}
	expected := expectedUpstreamUsageHours(scope, now)
	result := make(map[string]ChannelUpstreamUsageMetrics, len(rows))
	for _, row := range rows {
		account, configured := accounts[row.Domain]
		if !configured || !account.UsageSyncEnabled || row.Provider != account.Provider {
			continue
		}
		granularity := "hour"
		if account.Provider == upstreamProviderAICodeWith {
			granularity = "day"
		}
		completedHours := row.CompletedSeconds / 3600
		result[row.Domain] = ChannelUpstreamUsageMetrics{
			Available: true, Requests: row.Requests, Tokens: row.Tokens, CostUSD: row.CostUSD,
			ExpectedHours: expected, CompletedHours: completedHours,
			Complete:  expected == 0 || row.CompletedSeconds >= expected*3600,
			DataUntil: row.DataUntil, Granularity: granularity,
		}
	}
	return result, nil
}

func (m *Monitor) buildChannelManagementReport(ctx context.Context, scope stabilityScope, now int64) (*ChannelManagementReport, error) {
	finance, err := m.loadChannelFinanceSnapshot(ctx)
	if err != nil {
		return nil, err
	}
	websiteGroups, websiteGroupsSyncedAt, err := m.loadWebsiteGroupRates(ctx, finance)
	if err != nil {
		return nil, err
	}
	upstreamAccounts, err := m.loadChannelUpstreamViews(ctx)
	if err != nil {
		return nil, fmt.Errorf("读取上游账户状态: %w", err)
	}
	upstreamUsage, err := m.loadChannelUpstreamUsage(ctx, scope, now, upstreamAccounts)
	if err != nil {
		return nil, fmt.Errorf("读取上游使用日志汇总: %w", err)
	}
	assessments, assessmentErr := m.upstreamBalanceAssessments(ctx, now, upstreamAccounts, m.loadAlertConfig())
	if assessmentErr != nil {
		// 动态余额评估是增强信息；本地评估异常不能让渠道用量主报表不可用。
		slog.Warn("计算渠道余额可用天数失败", "err", assessmentErr)
		assessments = map[string]ChannelUpstreamBalanceAssessment{}
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
		Success   int64
		Anomaly   int64
		Failed    int64
		Tokens    int64
		Quota     int64
	}
	tx = m.storeDB.WithContext(ctx).Raw(`SELECT channel_id,COALESCE(grp,'') grp,
		COALESCE(SUM(success+anomaly+failed),0) requests,
		COALESCE(SUM(success),0) success,COALESCE(SUM(anomaly),0) anomaly,
		COALESCE(SUM(failed),0) failed,
		COALESCE(SUM(tokens),0) tokens,COALESCE(SUM(quota),0) quota
		FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=?
		GROUP BY channel_id,grp LIMIT ?`, scope.FromTs, scope.ToTs, userTrafficClassificationVersion, maxChannelManagementRows+1).Scan(&usageRows)
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
		usage := channelUsageAgg{requests: row.Requests, success: row.Success, anomaly: row.Anomaly, failed: row.Failed, tokens: row.Tokens, quota: row.Quota}
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
				leftRank, rightRank := channelManagementStatusRank(vendor.Channels[i]), channelManagementStatusRank(vendor.Channels[j])
				if leftRank != rightRank {
					return leftRank < rightRank
				}
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
					Stability: ch.Usage.stability(), Usage: ch.Usage.metrics(), Groups: managementGroups(ch.Groups, ch.ConfiguredGroups, ch.BaseDomain, ch.ID, finance),
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
		upstream := upstreamAccounts[domain.Domain]
		if assessment, ok := assessments[domain.Domain]; ok {
			upstream.Assessment = &assessment
		}
		responseDomains = append(responseDomains, ChannelManagementDomain{
			Key: domain.Key, Domain: domain.Domain, Configured: domain.Configured,
			Usage: domain.Usage.metrics(), Finance: finance.domainView(domain.Domain),
			Upstream:      upstream,
			RateConfig:    managementRateConfig(domain, finance),
			UpstreamUsage: upstreamUsage[domain.Domain],
			FinanceGroups: managementFinanceGroups(domain.FinanceGroups, domain.Domain, finance),
			Groups:        managementGroups(domain.Groups, nil, domain.Domain, 0, finance), Vendors: vendors,
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
	if tx := m.storeDB.WithContext(ctx).Raw("SELECT COALESCE(MAX(hour_ts),0) max FROM stability_hour_samples WHERE hour_ts>=? AND hour_ts<? AND traffic_class_version=?",
		scope.FromTs, scope.ToTs, userTrafficClassificationVersion).Scan(&coverage); tx.Error != nil {
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
	if tx := m.storeDB.WithContext(ctx).Raw("SELECT COALESCE(MAX(hour_ts),0) max FROM stability_hour_samples WHERE traffic_class_version=?",
		userTrafficClassificationVersion).Scan(&latestCoverage); tx.Error != nil {
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
	fromFormat, toFormat := "2006-01-02", "2006-01-02"
	if scope.RangeHours > 0 {
		fromFormat, toFormat = "2006-01-02 15:04", "2006-01-02 15:04"
	}
	return &ChannelManagementReport{
		Enabled:       true,
		Finance:       finance.settingsView(),
		WebsiteGroups: websiteGroups, WebsiteGroupsSyncedAt: websiteGroupsSyncedAt,
		Meta: ChannelManagementMeta{
			From: time.Unix(scope.FromTs, 0).In(cstLocation).Format(fromFormat),
			To:   time.Unix(toTs, 0).In(cstLocation).Format(toFormat), GeneratedAt: now,
			DataUntil: dataUntil, LatestDataUntil: latestDataUntil, ChannelConfigUpdatedAt: configUpdatedAt,
			TimeZone: "Asia/Shanghai", Source: "monitor_local_hourly_rollup",
			DataCoverage: m.stabilityDataCoverage(ctx, scope.FromTs, scope.ToTs, now),
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
	// 报表包含刚保存的本地倍率配置，禁止浏览器复用同 URL 的旧响应。
	c.Header("Cache-Control", "no-store")
	if !m.cfg.StabilityEnabled {
		c.JSON(200, gin.H{"enabled": false})
		return
	}
	maxDays := m.cfg.stabilityQueryDays()
	scope, err := channelManagementRange(c, time.Now(), maxDays)
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
		writeStabilityReadError(c, err)
		return
	}
	report.Finance.CanEdit = c.GetInt("urole") >= roleRoot
	c.JSON(200, report)
}

// channelManagementRange keeps the channel report's date shortcuts compatible
// with stabilityRange, while adding a rolling-hour option scoped to this page.
// Hourly reports use completed buckets only, so an in-progress hour is never
// presented as final usage.
func channelManagementRange(c *gin.Context, now time.Time, maxDays int) (stabilityScope, error) {
	rawHours := strings.TrimSpace(c.Query("hours"))
	if rawHours == "" {
		return stabilityRange(c, now, maxDays)
	}
	if strings.TrimSpace(c.Query("from")) != "" || strings.TrimSpace(c.Query("to")) != "" || strings.TrimSpace(c.Query("days")) != "" {
		return stabilityScope{}, fmt.Errorf("hours 不能与 days、from 或 to 同时提供")
	}
	hours, err := strconv.Atoi(rawHours)
	if err != nil || hours < 1 {
		return stabilityScope{}, fmt.Errorf("hours 必须为正整数")
	}
	if maxDays <= 0 {
		maxDays = 90
	}
	if hours > maxDays*24 {
		return stabilityScope{}, fmt.Errorf("查询范围不能超过 %d 天", maxDays)
	}
	now = now.In(cstLocation)
	end := time.Date(now.Year(), now.Month(), now.Day(), now.Hour(), 0, 0, 0, cstLocation)
	start := end.Add(-time.Duration(hours) * time.Hour)
	return stabilityScope{FromTs: start.Unix(), ToTs: end.Unix(), RangeHours: hours}, nil
}
