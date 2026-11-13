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
	// PartialHistory: a newly connected account whose history import has not
	// reached the start of the window yet; the estimate uses only the most
	// recent LookbackDays complete days (at least upstreamBalancePartialMinDays).
	PartialHistory bool `json:"partial_history,omitempty"`
	// RecentDailyCostUSD is the daily rate of the last 24 closed hours, when
	// those hours are covered well enough to be measured.
	RecentDailyCostUSD *float64 `json:"recent_daily_cost_usd,omitempty"`
	// EffectiveDailyCostUSD is the rate the runway and required balance use:
	// the higher of the lookback average and the recent 24-hour rate.
	EffectiveDailyCostUSD float64 `json:"effective_daily_cost_usd"`
	// RunwayBasis is "recent_24h" when the recent rate is the higher one and
	// therefore drives the estimate; otherwise "lookback".
	RunwayBasis string `json:"runway_basis,omitempty"`
}

type upstreamBurnEstimate struct {
	AverageDailyCostUSD float64
	ExpectedHours       int64
	CompletedHours      int64
	CoveragePct         float64
	Reason              string
	// PartialDays > 0: the window is incomplete only because history import
	// has not reached it yet, and AverageDailyCostUSD is taken from this many
	// most recent complete days instead.
	PartialDays int
	// RecentDailyCostUSD/RecentAvailable: daily rate over the last 24 closed
	// hours (see loadUpstreamRecentBurn).
	RecentDailyCostUSD float64
	RecentAvailable    bool
}

// upstreamBalanceRecentWindowSeconds is the recent window whose rate can
// override the lookback average. A 7-day average lags a channel whose traffic
// is ramping: on 2026-11-12 lingxuapi.com had 2.4 days by its 7-day average
// but only 0.3 days at the previous day's rate, so a 1-day threshold never
// fired. The runway uses the higher of the two rates (the usual "recent burn
// rate first" rule in budget forecasting).
const upstreamBalanceRecentWindowSeconds = 24 * 3600

// upstreamBalancePartialMinDays is the fewest complete recent days a
// still-importing account needs before its balance runway is estimated.
const upstreamBalancePartialMinDays = 2

type upstreamBalancePolicy struct {
	RunwayDays  float64
	Lookback    int
	MinCoverage float64
}

func (m *Monitor) manuallyDisabledUpstreamDomains(ctx context.Context) (map[string]bool, error) {
	var rows []struct {
		Domain string
	}
	err := m.storeDB.WithContext(ctx).Raw(`SELECT LOWER(TRIM(base_domain)) domain
		FROM channel_snaps
		WHERE deleted_at=0 AND TRIM(COALESCE(base_domain,''))<>''
		GROUP BY LOWER(TRIM(base_domain))
		HAVING SUM(CASE WHEN status<>2 THEN 1 ELSE 0 END)=0`).Scan(&rows).Error
	if err != nil {
		return nil, err
	}
	result := make(map[string]bool, len(rows))
	for _, row := range rows {
		result[row.Domain] = true
	}
	return result, nil
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
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,hour_ts,bucket_seconds,requests,tokens,quota,cost_usd,
		COALESCE(unit_per_usd,0) unit_per_usd,fetched_at,provider,provisional
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
		daySeconds       []int64
		dayCost          []float64
	}
	byDomain := make(map[string]*accumulator, len(accounts))
	for _, row := range rows {
		account, ok := accounts[row.Domain]
		if !ok || row.Provider != account.Provider || row.Provisional {
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
		if row.Provider == upstreamProviderTokenForce {
			var valid bool
			row, valid = tokenForceBillFaceValue(row)
			if !valid {
				a.invalidCost = true
				continue
			}
		}
		if math.IsNaN(row.CostUSD) || math.IsInf(row.CostUSD, 0) || row.CostUSD < 0 {
			a.invalidCost = true
			continue
		}
		a.cost += row.CostUSD
		a.completedSeconds += seconds
		a.lastEnd = end
		if day := int((row.HourTs - from) / 86400); day >= 0 && day < policy.Lookback {
			if a.daySeconds == nil {
				a.daySeconds, a.dayCost = make([]int64, policy.Lookback), make([]float64, policy.Lookback)
			}
			a.daySeconds[day] += seconds
			a.dayCost[day] += row.CostUSD
		}
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
			if estimate.CoveragePct+1e-9 < policy.MinCoverage {
				applyPartialHistoryEstimate(&estimate, accounts[domain], a.daySeconds, a.dayCost, from, policy)
			}
		}
		estimates[domain] = estimate
	}
	recent, err := m.loadUpstreamRecentBurn(ctx, now, policy, accounts)
	if err != nil {
		return nil, err
	}
	for domain, rate := range recent {
		estimate := estimates[domain]
		estimate.RecentDailyCostUSD, estimate.RecentAvailable = rate, true
		estimates[domain] = estimate
	}
	return estimates, nil
}

// loadUpstreamRecentBurn measures the daily rate of the last 24 closed hours
// per account. An account is returned only when its rows cover at least
// MinCoverage of the window without overlap or invalid amounts; otherwise the
// lookback average alone is used. Provisional Sub2API rows are counted: they
// are unconfirmed zero (or retained) hours, so including them can only lower
// the recent rate, and the recent rate is used only when it is the higher one.
// Accounts with natural-day buckets (AICodeWith, legacy Sub2API) have no
// recent rate.
func (m *Monitor) loadUpstreamRecentBurn(ctx context.Context, now int64, policy upstreamBalancePolicy, accounts map[string]ChannelUpstreamAccountView) (map[string]float64, error) {
	to := now - now%3600
	from := to - upstreamBalanceRecentWindowSeconds
	var rows []ChannelUpstreamUsageHour
	if err := m.storeDB.WithContext(ctx).Raw(`SELECT domain,hour_ts,bucket_seconds,requests,tokens,quota,cost_usd,
		COALESCE(unit_per_usd,0) unit_per_usd,fetched_at,provider,provisional
		FROM channel_upstream_usage_hours
		WHERE hour_ts>=? AND hour_ts+(CASE WHEN bucket_seconds>0 THEN bucket_seconds ELSE 3600 END)<=?
		ORDER BY domain ASC,hour_ts ASC`, from, to).Scan(&rows).Error; err != nil {
		return nil, fmt.Errorf("读取最近 24 小时上游账面消费: %w", err)
	}
	type accumulator struct {
		cost    float64
		seconds int64
		lastEnd int64
		invalid bool
	}
	byDomain := make(map[string]*accumulator, len(accounts))
	for _, row := range rows {
		account, ok := accounts[row.Domain]
		if !ok || row.Provider != account.Provider {
			continue
		}
		a := byDomain[row.Domain]
		if a == nil {
			a = &accumulator{}
			byDomain[row.Domain] = a
		}
		seconds := row.BucketSeconds
		if seconds <= 0 {
			seconds = 3600
		}
		// A natural-day bucket fits the trailing 24 hours exactly between 00:00
		// and 00:59 and never otherwise; using it would flip the basis once a
		// day. The recent rate is measured on hourly ledgers only.
		if seconds > 3600 || a.lastEnd > row.HourTs {
			a.invalid = true
			continue
		}
		if row.Provider == upstreamProviderTokenForce {
			var valid bool
			if row, valid = tokenForceBillFaceValue(row); !valid {
				a.invalid = true
				continue
			}
		}
		if math.IsNaN(row.CostUSD) || math.IsInf(row.CostUSD, 0) || row.CostUSD < 0 {
			a.invalid = true
			continue
		}
		a.cost += row.CostUSD
		a.seconds += seconds
		a.lastEnd = row.HourTs + seconds
	}
	out := make(map[string]float64, len(byDomain))
	for domain, a := range byDomain {
		if a.invalid || a.seconds <= 0 || float64(a.seconds)*100 < float64(upstreamBalanceRecentWindowSeconds)*policy.MinCoverage-1e-6 {
			continue
		}
		out[domain] = a.cost / (float64(a.seconds) / 86400)
	}
	return out, nil
}

// applyPartialHistoryEstimate lets a newly connected account be assessed
// before its history import reaches the start of the window. The missing
// earlier days must be explained by an import that is still running and
// healthy, never by a sync gap:
//   - the import is unfinished, has no recorded failure and is not isolated;
//   - its cursor (start of the next window still to import) lies strictly
//     before the first day used, so the incomplete day just before them is
//     still partly unimported;
//   - every window day the import has already passed is itself complete.
//
// Every day used must meet the coverage threshold on its own.
func applyPartialHistoryEstimate(estimate *upstreamBurnEstimate, account ChannelUpstreamAccountView, daySeconds []int64, dayCost []float64, from int64, policy upstreamBalancePolicy) {
	cursor := account.UsageBackfillCursor
	if account.UsageBackfillDone || cursor <= 0 || len(daySeconds) != policy.Lookback ||
		account.UsageBackfillConsecutiveFails > 0 || account.UsageBackfillLastError != "" ||
		account.UsageBackfillNextSyncAt == upstreamAccountIsolatedUntil {
		return
	}
	complete := func(day int) bool { return float64(daySeconds[day])*100 >= 86400*policy.MinCoverage-1e-6 }
	days, seconds, cost := 0, int64(0), 0.0
	for day := policy.Lookback - 1; day >= 0 && complete(day); day-- {
		days++
		seconds += daySeconds[day]
		cost += dayCost[day]
	}
	start := from + int64(policy.Lookback-days)*86400
	if days < upstreamBalancePartialMinDays || days >= policy.Lookback || cursor >= start {
		return
	}
	for day := 0; from+int64(day+1)*86400 <= cursor; day++ {
		if !complete(day) {
			return
		}
	}
	estimate.PartialDays = days
	estimate.AverageDailyCostUSD = cost / (float64(seconds) / 86400)
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
	if account.Provider == upstreamProviderOpenOx {
		assessment.Reason = "OpenOx 钱包与订阅额度分开，暂不按账面用量推算可用天数"
		return assessment
	}
	if !account.Enabled {
		assessment.Reason = "余额自动同步已停用"
		return assessment
	}
	if account.SyncPaused {
		// Paused sync leaves the recent bill window incomplete by design; a
		// coverage percentage here would read as a sync fault.
		assessment.Reason = "已标记停止使用且无启用渠道，自动同步已暂停"
		return assessment
	}
	if account.Status != upstreamStatusOK {
		assessment.Reason = "余额同步状态异常，暂不使用旧余额判断"
		return assessment
	}
	if !upstreamBalanceFresh(account.LastSuccessAt, now, syncMinutes) {
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
	lookback, basisNote := policy.Lookback, ""
	if estimate.PartialDays > 0 {
		lookback, basisNote = estimate.PartialDays, "（历史补齐中）"
		assessment.LookbackDays, assessment.PartialHistory = lookback, true
	}
	if estimate.ExpectedHours == 0 || estimate.CompletedHours == 0 || (estimate.PartialDays == 0 && estimate.CoveragePct+1e-9 < policy.MinCoverage) {
		assessment.Reason = fmt.Sprintf("上游账单覆盖率 %.1f%%，低于 %.1f%% 评估门槛", estimate.CoveragePct, policy.MinCoverage)
		return assessment
	}
	assessment.Available = true
	daily, basis := estimate.AverageDailyCostUSD, fmt.Sprintf("近 %d 个完整自然日上游账面日均消费%s", lookback, basisNote)
	assessment.RunwayBasis = "lookback"
	if estimate.RecentAvailable {
		recent := estimate.RecentDailyCostUSD
		assessment.RecentDailyCostUSD = &recent
		if recent > daily {
			daily, basis = recent, fmt.Sprintf("最近 24 小时上游账面消费速度（高于近 %d 日日均）", lookback)
			assessment.RunwayBasis = "recent_24h"
		}
	}
	assessment.EffectiveDailyCostUSD = daily
	if daily < minUpstreamDailyCostUSD {
		assessment.Status = "idle"
		assessment.Reason = fmt.Sprintf("近 %d 个完整自然日无显著上游消耗%s", lookback, basisNote)
		return assessment
	}
	requiredBalance := daily * policy.RunwayDays
	assessment.RequiredBalanceUSD = &requiredBalance
	runway := *account.BalanceUSD / daily
	if runway < 0 {
		runway = 0
	}
	assessment.EstimatedRunwayDays = &runway
	if *account.BalanceUSD <= 0 {
		assessment.Status = "critical"
		assessment.Reason = "余额已用尽或为负"
	} else if runway <= policy.RunwayDays {
		assessment.Status = "warning"
		assessment.Reason = fmt.Sprintf("按%s估算，余额不足 %.1f 天", basis, policy.RunwayDays)
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

func upstreamBalanceAlertEligible(domain string, assessment ChannelUpstreamBalanceAssessment, manuallyDisabled map[string]bool, retirements map[string]ChannelUpstreamRetirement) bool {
	return !manuallyDisabled[domain] && !retirements[domain].Retiring &&
		(assessment.Status == "warning" || assessment.Status == "critical") &&
		assessment.EstimatedRunwayDays != nil && assessment.RequiredBalanceUSD != nil
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
	manuallyDisabled, err := m.manuallyDisabledUpstreamDomains(ctx)
	if err != nil {
		// 无法确认渠道生命周期时不猜测，保持原有告警行为。
		slog.Warn("读取手动禁用渠道状态失败", "err", err)
		manuallyDisabled = map[string]bool{}
	}
	retirements, err := m.loadChannelUpstreamRetirements(ctx)
	if err != nil {
		// An unreadable operator decision must not silently suppress a real
		// low-balance alert. Keep the old behavior and surface the read failure.
		slog.Warn("读取渠道停止使用标记失败，保留余额预警", "err", err)
		retirements = map[string]ChannelUpstreamRetirement{}
	}
	for domain, assessment := range assessments {
		if !upstreamBalanceAlertEligible(domain, assessment, manuallyDisabled, retirements) {
			continue
		}
		account := accounts[domain]
		m.fire(c, "upstream_balance_low", domain,
			fmt.Sprintf("渠道余额预计不足 %.1f 天：%s", assessment.ThresholdDays, domain),
			fmt.Sprintf("主域名：%s\n当前余额：$%.2f\n近 %d 个完整自然日上游账面日均消费：$%.2f\n%s余额保障时长：%.1f 天\n当前动态预警线：$%.2f\n预计可用：%.2f 天（%.1f 小时）\n上游账单覆盖率：%.1f%%（%d/%d 小时）%s\n\n预计可用时间按当前余额 ÷ 日均消费计算，日均消费取近期日均与最近 24 小时速度中较高者；充值比例修正仅用于成本与利润核算。",
				domain, *account.BalanceUSD, assessment.LookbackDays, assessment.AverageDailyCostUSD, upstreamBalanceRecentAlertLine(assessment),
				assessment.ThresholdDays, *assessment.RequiredBalanceUSD, *assessment.EstimatedRunwayDays,
				*assessment.EstimatedRunwayDays*24, assessment.CoveragePct, assessment.CompletedHours, assessment.ExpectedHours, upstreamBalancePartialAlertNote(assessment)), now)
	}
}

func upstreamBalanceRecentAlertLine(assessment ChannelUpstreamBalanceAssessment) string {
	if assessment.RecentDailyCostUSD == nil {
		return "最近 24 小时消费速度：数据不完整，未参与估算\n"
	}
	line := fmt.Sprintf("最近 24 小时消费速度：$%.2f/天", *assessment.RecentDailyCostUSD)
	if assessment.RunwayBasis == "recent_24h" {
		line += "（高于近期日均，本次按此估算）"
	}
	return line + "\n"
}

func upstreamBalancePartialAlertNote(assessment ChannelUpstreamBalanceAssessment) string {
	if !assessment.PartialHistory {
		return ""
	}
	return fmt.Sprintf("\n注意：该渠道为新接入，历史账单仍在补齐，本次只按已同步的近 %d 个完整自然日估算，结果可能偏高或偏低。", assessment.LookbackDays)
}
