package monitor

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"time"
)

const (
	defaultUpstreamBalanceRunwayDays   = 1.0
	defaultUpstreamBalanceLookbackDays = 7
	defaultUpstreamBalanceMinCoverage  = 95.0
	minUpstreamDailyCostUSD            = 0.01
)

// ChannelUpstreamBalanceAssessment 是渠道余额的客观动态评估。
// 估算只使用 Monitor 本地已落账的上游账户账面消费；任何关键口径缺失时
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
	ExpectedHours       int64
	CompletedHours      int64
	CoveragePct         float64
	Reason              string
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

// loadUpstreamBurnEstimates 汇总最近完整自然日的上游账户账面消费。查询只落在
// Monitor 本地 SQLite 的上游账单事实表；不使用用户侧 quota、渠道倍率、充值比例
// 或上游修正消费，保证余额与消耗采用同一上游账面单位。
func (m *Monitor) loadUpstreamBurnEstimates(ctx context.Context, now int64, policy upstreamBalancePolicy, accounts map[string]ChannelUpstreamAccountView) (map[string]upstreamBurnEstimate, error) {
	from, to := upstreamBalanceWindow(now, policy.Lookback)
	expectedSeconds := to - from
	estimates := make(map[string]upstreamBurnEstimate, len(accounts))
	for domain := range accounts {
		estimates[domain] = upstreamBurnEstimate{ExpectedHours: expectedSeconds / 3600}
	}
	var rows []ChannelUpstreamUsageHour
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,hour_ts,bucket_seconds,requests,tokens,quota,cost_usd,fetched_at,provider
		FROM channel_upstream_usage_hours
		WHERE hour_ts>=? AND hour_ts+(CASE WHEN bucket_seconds>0 THEN bucket_seconds ELSE 3600 END)<=?
		ORDER BY domain ASC,hour_ts ASC`, from, to).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("读取上游账户账面消费: %w", err)
	}

	type accumulator struct {
		cost             float64
		completedSeconds int64
		lastEnd          int64
		overlap          bool
		invalidCost      bool
	}
	byDomain := make(map[string]*accumulator, len(accounts))
	for _, row := range rows {
		account, ok := accounts[row.Domain]
		if !ok || row.Provider != account.Provider {
			continue
		}
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		end := row.HourTs + seconds
		a := byDomain[row.Domain]
		if a == nil {
			a = &accumulator{}
			byDomain[row.Domain] = a
		}
		if a.lastEnd > row.HourTs {
			a.overlap = true
			continue
		}
		if math.IsNaN(row.CostUSD) || math.IsInf(row.CostUSD, 0) || row.CostUSD < 0 {
			a.invalidCost = true
			continue
		}
		a.cost += row.CostUSD
		a.completedSeconds += seconds
		a.lastEnd = end
	}
	for domain, estimate := range estimates {
		a := byDomain[domain]
		if a == nil {
			estimates[domain] = estimate
			continue
		}
		estimate.CompletedHours = a.completedSeconds / 3600
		if expectedSeconds > 0 {
			estimate.CoveragePct = math.Min(100, float64(a.completedSeconds)*100/float64(expectedSeconds))
		}
		if a.overlap {
			estimate.Reason = "上游账单时间桶存在重叠，暂不评估"
		} else if a.invalidCost {
			estimate.Reason = "上游账单含无效消费金额，暂不评估"
		} else if a.completedSeconds > 0 {
			estimate.AverageDailyCostUSD = a.cost / (float64(a.completedSeconds) / 86400)
		}
		estimates[domain] = estimate
	}
	return estimates, nil
}

func assessUpstreamBalance(account ChannelUpstreamAccountView, estimate upstreamBurnEstimate, policy upstreamBalancePolicy, now int64, syncMinutes int) ChannelUpstreamBalanceAssessment {
	assessment := ChannelUpstreamBalanceAssessment{
		Status: "unavailable", ThresholdDays: policy.RunwayDays, LookbackDays: policy.Lookback,
		ExpectedHours: estimate.ExpectedHours, CompletedHours: estimate.CompletedHours,
		CoveragePct: estimate.CoveragePct, AverageDailyCostUSD: estimate.AverageDailyCostUSD,
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
	if !account.UsageSyncEnabled {
		assessment.Reason = "上游消费同步尚未启用"
		return assessment
	}
	if estimate.Reason != "" {
		assessment.Reason = estimate.Reason
		return assessment
	}
	if estimate.ExpectedHours == 0 || estimate.CompletedHours == 0 || estimate.CoveragePct+1e-9 < policy.MinCoverage {
		assessment.Reason = fmt.Sprintf("上游账单覆盖率 %.1f%%，低于 %.1f%% 评估门槛", estimate.CoveragePct, policy.MinCoverage)
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
		assessment.Reason = fmt.Sprintf("按近 %d 个完整自然日上游账面日均消费估算，余额不足 %.1f 天", policy.Lookback, policy.RunwayDays)
	} else {
		assessment.Status = "healthy"
	}
	return assessment
}

func (m *Monitor) upstreamBalanceAssessments(ctx context.Context, now int64, accounts map[string]ChannelUpstreamAccountView, c AlertConfig) (map[string]ChannelUpstreamBalanceAssessment, error) {
	policy := upstreamBalancePolicyFor(c)
	estimates, err := m.loadUpstreamBurnEstimates(ctx, now, policy, accounts)
	if err != nil {
		return nil, err
	}
	out := make(map[string]ChannelUpstreamBalanceAssessment, len(accounts))
	for domain, account := range accounts {
		out[domain] = assessUpstreamBalance(account, estimates[domain], policy, now, upstreamSyncMinutes(m.cfg))
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
			fmt.Sprintf("主域名：%s\n当前余额：$%.2f\n近 %d 个完整自然日上游账面日均消费：$%.2f\n余额保障时长：%.1f 天\n当前动态预警线：$%.2f\n预计可用：%.2f 天（%.1f 小时）\n上游账单覆盖率：%.1f%%（%d/%d 小时）\n\n预计可用时间按当前余额 ÷ 上游账面日均消费计算；充值比例修正仅用于成本与利润核算。",
				domain, *account.BalanceUSD, assessment.LookbackDays, assessment.AverageDailyCostUSD,
				assessment.ThresholdDays, *assessment.RequiredBalanceUSD, *assessment.EstimatedRunwayDays,
				*assessment.EstimatedRunwayDays*24, assessment.CoveragePct, assessment.CompletedHours, assessment.ExpectedHours), now)
	}
}
