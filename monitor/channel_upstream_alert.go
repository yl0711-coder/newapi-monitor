package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"sort"
	"strings"
	"time"
)

const (
	defaultUpstreamBalanceRunwayDays   = 1.0
	defaultUpstreamBalanceLookbackDays = 7
	defaultUpstreamBalanceMinCoverage  = 95.0
	minUpstreamDailyCostUSD            = 0.01
)

// ChannelUpstreamBalanceAssessment 是渠道余额的客观动态评估。
// 估算只使用 Monitor 本地完整小时汇总和本地倍率配置；任何关键口径缺失时
// Available=false，并给出原因，不用残缺数据猜测余额还能用多久。
type ChannelUpstreamBalanceAssessment struct {
	Available           bool     `json:"available"`
	Status              string   `json:"status"` // healthy / warning / critical / idle / unavailable
	AverageDailyCostUSD float64  `json:"average_daily_cost_usd"`
	EstimatedRunwayDays *float64 `json:"estimated_runway_days,omitempty"`
	RequiredBalanceUSD  *float64 `json:"required_balance_usd,omitempty"`
	ThresholdDays       float64  `json:"threshold_days"`
	LookbackDays        int      `json:"lookback_days"`
	ExpectedHours       int64    `json:"expected_hours"`
	CompletedHours      int64    `json:"completed_hours"`
	CoveragePct         float64  `json:"coverage_pct"`
	Reason              string   `json:"reason,omitempty"`
}

type upstreamBurnEstimate struct {
	AverageDailyCostUSD float64
	MissingGroups       []string
}

type upstreamBalancePolicy struct {
	RunwayDays  float64
	Lookback    int
	MinCoverage float64
}

func upstreamBalancePolicyFor(c AlertConfig) upstreamBalancePolicy {
	p := upstreamBalancePolicy{
		RunwayDays: c.UpstreamBalanceRunwayDays, Lookback: c.UpstreamBalanceLookbackDays,
		MinCoverage: c.UpstreamBalanceMinCoverage,
	}
	if p.RunwayDays <= 0 || p.RunwayDays > 30 || math.IsNaN(p.RunwayDays) || math.IsInf(p.RunwayDays, 0) {
		p.RunwayDays = defaultUpstreamBalanceRunwayDays
	}
	if p.Lookback < 3 || p.Lookback > 30 {
		p.Lookback = defaultUpstreamBalanceLookbackDays
	}
	if p.MinCoverage < 80 || p.MinCoverage > 100 || math.IsNaN(p.MinCoverage) || math.IsInf(p.MinCoverage, 0) {
		p.MinCoverage = defaultUpstreamBalanceMinCoverage
	}
	return p
}

func upstreamBalanceWindow(now int64, lookbackDays int) (int64, int64) {
	localNow := time.Unix(now, 0).In(cstLocation)
	to := time.Date(localNow.Year(), localNow.Month(), localNow.Day(), 0, 0, 0, 0, cstLocation)
	from := to.AddDate(0, 0, -lookbackDays)
	return from.Unix(), to.Unix()
}

// loadUpstreamBurnEstimates 汇总最近完整自然日的渠道成本。查询只落在 Monitor
// 本地 SQLite；quota 已是我方用户侧计费额，按双方实际倍率比例换算上游估算成本。
func (m *Monitor) loadUpstreamBurnEstimates(ctx context.Context, now int64, policy upstreamBalancePolicy) (map[string]upstreamBurnEstimate, StabilityDataCoverage, error) {
	from, to := upstreamBalanceWindow(now, policy.Lookback)
	coverage := m.stabilityDataCoverage(ctx, from, to, now)
	estimates := make(map[string]upstreamBurnEstimate)
	if coverage.ExpectedHours == 0 || coverage.CompletedHours == 0 {
		return estimates, coverage, nil
	}

	finance, err := m.loadChannelFinanceSnapshot(ctx)
	if err != nil {
		return nil, coverage, fmt.Errorf("读取渠道倍率配置: %w", err)
	}
	var rows []struct {
		Domain    string
		ChannelID int
		Grp       string
		Quota     int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(cs.base_domain,'') domain,
		sh.channel_id, COALESCE(sh.grp,'') grp, COALESCE(SUM(sh.quota),0) quota
		FROM stability_hour_samples sh
		JOIN stability_hour_ingest_states hs ON hs.hour_ts=sh.hour_ts AND hs.status='complete'
		  AND hs.traffic_class_version=?
		JOIN channel_snaps cs ON cs.id=sh.channel_id
		WHERE sh.hour_ts>=? AND sh.hour_ts<? AND sh.traffic_class_version=?
		  AND COALESCE(cs.base_domain,'')<>''
		GROUP BY cs.base_domain,sh.channel_id,sh.grp`, userTrafficClassificationVersion, from, to,
		userTrafficClassificationVersion).Scan(&rows).Error; err != nil {
		return nil, coverage, fmt.Errorf("读取本地渠道消耗汇总: %w", err)
	}

	missing := make(map[string]map[string]bool)
	totals := make(map[string]float64)
	for _, row := range rows {
		domain, group := strings.TrimSpace(row.Domain), strings.TrimSpace(row.Grp)
		if domain == "" || row.Quota <= 0 {
			continue
		}
		if group == "" {
			group = "未标记服务分组"
		}
		// 同一上游、同一网站分组可能同时挂载多个成本不同的物理渠道。
		// 必须先按 channel_id 使用各自的倍率折算再汇总；若先按域名×分组
		// 合并，会把 jikesoft/aicodewith 这类多档渠道错误套成同一个成本。
		view := finance.groupViewForChannel(domain, row.ChannelID, group)
		if !view.Complete || view.SiteMultiplier <= 0 || view.UpstreamEffectiveMultiplier <= 0 {
			if missing[domain] == nil {
				missing[domain] = make(map[string]bool)
			}
			missing[domain][fmt.Sprintf("#%d %s", row.ChannelID, group)] = true
			continue
		}
		userCostUSD := float64(row.Quota) / quotaPerUSD
		totals[domain] += userCostUSD * view.UpstreamEffectiveMultiplier / view.SiteMultiplier
	}

	// 渠道模型测试没有用户收入，但会真实消耗上游余额。旧 NewAPI 普通/固定价
	// quota 是未乘网站分组倍率的基数；tiered_expr quota 已经乘过网站倍率。
	var testRows []struct {
		Domain    string
		Grp       string
		CostBasis string
		ChannelID int
		Quota     int64
	}
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT COALESCE(cs.base_domain,'') domain,
		COALESCE(ts.grp,'') grp, COALESCE(ts.cost_basis,'') cost_basis,
		ts.channel_id, COALESCE(SUM(ts.quota),0) quota
		FROM channel_test_hour_samples ts
		JOIN stability_hour_ingest_states hs ON hs.hour_ts=ts.hour_ts AND hs.status='complete'
		  AND hs.traffic_class_version=?
		JOIN channel_snaps cs ON cs.id=ts.channel_id
		WHERE ts.hour_ts>=? AND ts.hour_ts<? AND ts.traffic_class_version=?
		  AND COALESCE(cs.base_domain,'')<>''
		GROUP BY cs.base_domain,ts.grp,ts.cost_basis,ts.channel_id`, userTrafficClassificationVersion, from, to,
		userTrafficClassificationVersion).
		Scan(&testRows).Error; err != nil {
		return nil, coverage, fmt.Errorf("读取本地渠道测试成本汇总: %w", err)
	}
	for _, row := range testRows {
		domain, group := strings.TrimSpace(row.Domain), strings.TrimSpace(row.Grp)
		if domain == "" || row.Quota <= 0 {
			continue
		}
		if row.CostBasis != "legacy_assumed_base" && row.CostBasis != "legacy_after_group" {
			if missing[domain] == nil {
				missing[domain] = make(map[string]bool)
			}
			missing[domain][fmt.Sprintf("内部测试渠道 #%d 成本口径未标记", row.ChannelID)] = true
			continue
		}
		view := finance.groupViewForChannel(domain, row.ChannelID, group)
		if !view.UpstreamConfigured || view.UpstreamEffectiveMultiplier <= 0 ||
			(row.CostBasis == "legacy_after_group" && (!view.Complete || view.SiteMultiplier <= 0)) {
			if missing[domain] == nil {
				missing[domain] = make(map[string]bool)
			}
			missing[domain][fmt.Sprintf("内部测试渠道 #%d", row.ChannelID)] = true
			continue
		}
		baseCostUSD := float64(row.Quota) / quotaPerUSD
		if row.CostBasis == "legacy_after_group" {
			baseCostUSD /= view.SiteMultiplier
		}
		totals[domain] += baseCostUSD * view.UpstreamEffectiveMultiplier
	}

	// 用实际已完整的小时数归一化，允许极少量缺口但不会把缺失小时当零消费。
	effectiveDays := float64(coverage.CompletedHours) / 24
	for domain, total := range totals {
		estimate := estimates[domain]
		if effectiveDays > 0 {
			estimate.AverageDailyCostUSD = total / effectiveDays
		}
		estimates[domain] = estimate
	}
	for domain, groups := range missing {
		estimate := estimates[domain]
		for group := range groups {
			estimate.MissingGroups = append(estimate.MissingGroups, group)
		}
		sort.Strings(estimate.MissingGroups)
		estimates[domain] = estimate
	}
	return estimates, coverage, nil
}

func assessUpstreamBalance(account ChannelUpstreamAccountView, estimate upstreamBurnEstimate, coverage StabilityDataCoverage, policy upstreamBalancePolicy, now int64, syncMinutes int) ChannelUpstreamBalanceAssessment {
	assessment := ChannelUpstreamBalanceAssessment{
		Status: "unavailable", ThresholdDays: policy.RunwayDays, LookbackDays: policy.Lookback,
		ExpectedHours: coverage.ExpectedHours, CompletedHours: coverage.CompletedHours,
		CoveragePct: coverage.Percent, AverageDailyCostUSD: estimate.AverageDailyCostUSD,
	}
	if !account.Configured {
		assessment.Reason = "尚未配置余额同步"
		return assessment
	}
	if !account.Enabled {
		assessment.Reason = "余额自动同步已停用"
		return assessment
	}
	if account.Status != upstreamStatusOK {
		assessment.Reason = "余额同步状态异常，暂不使用旧余额判断"
		return assessment
	}
	maxBalanceAge := int64(syncMinutes * 3 * 60)
	if maxBalanceAge < 30*60 {
		maxBalanceAge = 30 * 60
	}
	if account.LastSuccessAt <= 0 || now-account.LastSuccessAt > maxBalanceAge {
		assessment.Reason = "余额数据已过期"
		return assessment
	}
	if account.BalanceUSD == nil {
		assessment.Reason = "尚未取得账户余额"
		return assessment
	}
	if coverage.ExpectedHours == 0 || coverage.CompletedHours == 0 || coverage.Percent+1e-9 < policy.MinCoverage {
		assessment.Reason = fmt.Sprintf("小时数据完整率 %.1f%%，低于 %.1f%% 评估门槛", coverage.Percent, policy.MinCoverage)
		return assessment
	}
	if len(estimate.MissingGroups) > 0 {
		assessment.Reason = "以下有消费的服务分组尚未配齐双方倍率：" + strings.Join(estimate.MissingGroups, "、")
		return assessment
	}
	assessment.Available = true
	if estimate.AverageDailyCostUSD < minUpstreamDailyCostUSD {
		assessment.Status = "idle"
		assessment.Reason = fmt.Sprintf("近 %d 个完整自然日无显著上游消耗", policy.Lookback)
		return assessment
	}
	requiredBalance := estimate.AverageDailyCostUSD * policy.RunwayDays
	assessment.RequiredBalanceUSD = &requiredBalance
	runway := *account.BalanceUSD / estimate.AverageDailyCostUSD
	if runway < 0 {
		runway = 0
	}
	assessment.EstimatedRunwayDays = &runway
	if *account.BalanceUSD <= 0 {
		assessment.Status = "critical"
		assessment.Reason = "余额已用尽或为负"
	} else if runway <= policy.RunwayDays {
		assessment.Status = "warning"
		assessment.Reason = fmt.Sprintf("按近 %d 天日均成本估算，余额不足 %.1f 天", policy.Lookback, policy.RunwayDays)
	} else {
		assessment.Status = "healthy"
	}
	return assessment
}

func (m *Monitor) upstreamBalanceAssessments(ctx context.Context, now int64, accounts map[string]ChannelUpstreamAccountView, c AlertConfig) (map[string]ChannelUpstreamBalanceAssessment, error) {
	policy := upstreamBalancePolicyFor(c)
	estimates, coverage, err := m.loadUpstreamBurnEstimates(ctx, now, policy)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ChannelUpstreamBalanceAssessment, len(accounts))
	for domain, account := range accounts {
		out[domain] = assessUpstreamBalance(account, estimates[domain], coverage, policy, now, upstreamSyncMinutes(m.cfg))
	}
	return out, nil
}

func (m *Monitor) evaluateUpstreamBalanceAlerts(c AlertConfig, now int64) {
	if !c.UpstreamBalanceAlertsEnabled {
		return
	}
	// 余额通常每 5 分钟同步一次。两次余额同步之间重复聚合相同的 7 天小时数据
	// 不会提升时效性，只会增加本地 SQLite 读锁和 CPU，因此动态评估最多与同步同频。
	interval := int64(upstreamSyncMinutes(m.cfg) * 60)
	for {
		last := m.upstreamBalanceAlertLastEval.Load()
		if last > 0 && now-last < interval {
			return
		}
		if m.upstreamBalanceAlertLastEval.CompareAndSwap(last, now) {
			break
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	accounts, err := m.loadChannelUpstreamViews(ctx)
	if err != nil {
		slog.Warn("读取渠道余额状态失败，跳过动态余额预警", "err", err)
		return
	}
	assessments, err := m.upstreamBalanceAssessments(ctx, now, accounts, c)
	if err != nil {
		slog.Warn("计算渠道余额可用天数失败，跳过动态余额预警", "err", err)
		return
	}
	for domain, assessment := range assessments {
		if (assessment.Status != "warning" && assessment.Status != "critical") || assessment.EstimatedRunwayDays == nil {
			continue
		}
		account := accounts[domain]
		m.fire(c, "upstream_balance_low", domain,
			fmt.Sprintf("渠道余额预计不足 %.1f 天：%s", assessment.ThresholdDays, domain),
			fmt.Sprintf("主域名：%s\n当前余额：$%.2f\n近 %d 个完整自然日预估日均上游成本：$%.2f\n余额保障时长：%.1f 天\n当前动态预警线：$%.2f\n预计可用：%.2f 天（%.1f 小时）\n小时数据完整率：%.1f%%（%d/%d 小时）\n\n该数值为按本地倍率配置估算的充值提醒，不是上游正式账单。",
				domain, *account.BalanceUSD, assessment.LookbackDays, assessment.AverageDailyCostUSD,
				assessment.ThresholdDays, *assessment.RequiredBalanceUSD, *assessment.EstimatedRunwayDays,
				*assessment.EstimatedRunwayDays*24, assessment.CoveragePct, assessment.CompletedHours, assessment.ExpectedHours), now)
	}
}
