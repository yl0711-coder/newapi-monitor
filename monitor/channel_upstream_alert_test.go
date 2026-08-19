package monitor

import (
	"context"
	"math"
	"testing"
	"time"
)

func seedUpstreamBalanceAssessment(t *testing.T, m *Monitor, now int64) (string, ChannelUpstreamAccountView, AlertConfig) {
	t.Helper()
	domain := "last-api.ai"
	from, to := upstreamBalanceWindow(now, 7)
	if err := m.storeDB.Create(&ChannelSnap{ID: 33, Name: "route", BaseDomain: domain, BaseHost: "temp.last-api.ai", Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelFinanceSetting{ID: 1, FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSaleGroupRate{Grp: "codex", Multiplier: 2}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelDomainCost{Domain: domain, RechargePaid: 1, RechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelDomainGroupCost{Domain: domain, Grp: "codex", Multiplier: 1, DiscountFactor: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelFinanceChannelCost{
		ChannelID: 33, Grp: "codex", UpstreamGroupName: "upstream", Multiplier: 1, DiscountFactor: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	states := make([]StabilityHourIngestState, 0, (to-from)/3600)
	for hour := from; hour < to; hour += 3600 {
		states = append(states, StabilityHourIngestState{HourTs: hour, Status: "complete"})
	}
	if err := m.storeDB.CreateInBatches(states, 200).Error; err != nil {
		t.Fatal(err)
	}
	// 每天用户侧消费 $100；上游实际倍率 / 我方倍率 = 1/2，故预估日均上游成本 $50。
	rows := make([]StabilityHourSample, 0, 7)
	for day := 0; day < 7; day++ {
		rows = append(rows, StabilityHourSample{
			HourTs: from + int64(day*24)*3600, ChannelID: 33, ModelName: "gpt", Grp: "codex",
			Success: 1, Quota: int64(100 * quotaPerUSD),
		})
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	balance := 40.0
	account := ChannelUpstreamAccountView{
		Configured: true, Enabled: true, Status: upstreamStatusOK, BalanceUSD: &balance,
		LastSuccessAt: now,
	}
	cfg := defaultAlertConfig()
	return domain, account, cfg
}

func TestUpstreamBalanceAssessmentUsesCompleteLocalHoursAndConfiguredRates(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain, account, cfg := seedUpstreamBalanceAssessment(t, m, now)
	policy := upstreamBalancePolicyFor(cfg)
	estimates, coverage, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy)
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || coverage.CompletedHours != 7*24 {
		t.Fatalf("coverage=%+v", coverage)
	}
	if math.Abs(estimates[domain].AverageDailyCostUSD-50) > 1e-9 {
		t.Fatalf("average daily upstream cost=%v want=50", estimates[domain].AverageDailyCostUSD)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], coverage, policy, now, 5)
	if !assessment.Available || assessment.Status != "warning" || assessment.EstimatedRunwayDays == nil || math.Abs(*assessment.EstimatedRunwayDays-0.8) > 1e-9 {
		t.Fatalf("assessment=%+v", assessment)
	}
	*account.BalanceUSD = 100
	assessment = assessUpstreamBalance(account, estimates[domain], coverage, policy, now, 5)
	if assessment.Status != "healthy" || assessment.EstimatedRunwayDays == nil || math.Abs(*assessment.EstimatedRunwayDays-2) > 1e-9 {
		t.Fatalf("healthy assessment=%+v", assessment)
	}
}

func TestUpstreamBurnAddsInternalTestsAsCostWithoutCountingThemAsRevenue(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain, _, cfg := seedUpstreamBalanceAssessment(t, m, now)
	from, _ := upstreamBalanceWindow(now, 7)
	rows := make([]ChannelTestHourSample, 0, 7)
	for day := 0; day < 7; day++ {
		rows = append(rows, ChannelTestHourSample{
			HourTs: from + int64(day*24)*3600, ChannelID: 33, ModelName: "gpt", Grp: "internal",
			Origin: "legacy", Scope: "legacy", CostBasis: "legacy_assumed_base",
			Requests: 6, Tokens: 60, Quota: int64(10 * quotaPerUSD),
		})
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}

	estimates, coverage, err := m.loadUpstreamBurnEstimates(context.Background(), now, upstreamBalancePolicyFor(cfg))
	if err != nil {
		t.Fatal(err)
	}
	// 用户请求每天贡献 $50 上游成本；测试 quota 是未乘网站倍率的成本基数，
	// 每天再贡献 $10。测试请求没有写回 stability_hour_samples，因此不会成为收入。
	if !coverage.Complete || math.Abs(estimates[domain].AverageDailyCostUSD-60) > 1e-9 {
		t.Fatalf("内部测试成本没有独立计入上游消耗: coverage=%+v estimate=%+v", coverage, estimates[domain])
	}
	var revenueRows int64
	if err := m.storeDB.Model(&StabilityHourSample{}).Where("grp = ?", "internal").Count(&revenueRows).Error; err != nil {
		t.Fatal(err)
	}
	if revenueRows != 0 {
		t.Fatalf("内部测试不得写入用户收入事实，rows=%d", revenueRows)
	}
}

func TestUpstreamBurnNormalizesLegacyTieredTestQuotaBySiteMultiplier(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain, _, cfg := seedUpstreamBalanceAssessment(t, m, now)
	from, _ := upstreamBalanceWindow(now, 7)
	rows := make([]ChannelTestHourSample, 0, 7)
	for day := 0; day < 7; day++ {
		// tiered_expr 的旧 NewAPI quota 已经乘过 codex=2 的网站倍率。
		// $20 / 2 * 上游倍率1 = $10，加用户流量 $50，日均应为 $60。
		rows = append(rows, ChannelTestHourSample{
			HourTs: from + int64(day*24)*3600, ChannelID: 33, ModelName: "gpt", Grp: "codex",
			Origin: "legacy", Scope: "legacy", CostBasis: "legacy_after_group",
			Requests: 1, Tokens: 10, Quota: int64(20 * quotaPerUSD),
		})
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	estimates, coverage, err := m.loadUpstreamBurnEstimates(context.Background(), now, upstreamBalancePolicyFor(cfg))
	if err != nil {
		t.Fatal(err)
	}
	if !coverage.Complete || len(estimates[domain].MissingGroups) != 0 || math.Abs(estimates[domain].AverageDailyCostUSD-60) > 1e-9 {
		t.Fatalf("tiered internal-test cost was not normalized: coverage=%+v estimate=%+v", coverage, estimates[domain])
	}
}

func TestUpstreamBalanceEstimateExcludesPartialRowsFromIncompleteHours(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain, _, cfg := seedUpstreamBalanceAssessment(t, m, now)
	from, _ := upstreamBalanceWindow(now, 7)
	incompleteHour := from + 3600 // 原始样本中该小时没有消费行。
	if err := m.storeDB.Delete(&StabilityHourIngestState{}, "hour_ts = ?", incompleteHour).Error; err != nil {
		t.Fatal(err)
	}
	policy := upstreamBalancePolicyFor(cfg)
	baseline, coverage, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy)
	if err != nil {
		t.Fatal(err)
	}
	if coverage.CompletedHours != 7*24-1 || coverage.Percent < policy.MinCoverage {
		t.Fatalf("测试应覆盖允许少量缺口的评估路径: %+v", coverage)
	}
	// 模拟某个尚未完整的小时已滚入部分数据，且金额极大。
	// 它既不得抬高日均成本，也不得因倍率未配置让整个评估失效。
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: incompleteHour, ChannelID: 33, ModelName: "partial", Grp: "unconfigured-partial",
		Success: 1, Quota: int64(100000 * quotaPerUSD),
	}).Error; err != nil {
		t.Fatal(err)
	}
	after, afterCoverage, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy)
	if err != nil {
		t.Fatal(err)
	}
	if afterCoverage != coverage {
		t.Fatalf("增加残缺小时样本不应改变完整性口径: before=%+v after=%+v", coverage, afterCoverage)
	}
	if math.Abs(after[domain].AverageDailyCostUSD-baseline[domain].AverageDailyCostUSD) > 1e-9 || len(after[domain].MissingGroups) != 0 {
		t.Fatalf("残缺小时不得参与成本或倍率完整性评估: before=%+v after=%+v", baseline[domain], after[domain])
	}
}

func TestUpstreamBalanceAssessmentFailsClosedOnMissingRatesAndStaleBalance(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain, account, cfg := seedUpstreamBalanceAssessment(t, m, now)
	from, _ := upstreamBalanceWindow(now, 7)
	if err := m.storeDB.Create(&StabilityHourSample{
		HourTs: from, ChannelID: 33, ModelName: "other", Grp: "unconfigured", Success: 1, Quota: int64(quotaPerUSD),
	}).Error; err != nil {
		t.Fatal(err)
	}
	policy := upstreamBalancePolicyFor(cfg)
	estimates, coverage, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy)
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], coverage, policy, now, 5)
	if assessment.Available || assessment.Status != "unavailable" || len(estimates[domain].MissingGroups) != 1 {
		t.Fatalf("missing rate must not produce a runway: estimate=%+v assessment=%+v", estimates[domain], assessment)
	}

	clean := estimates[domain]
	clean.MissingGroups = nil
	account.LastSuccessAt = now - 31*60
	assessment = assessUpstreamBalance(account, clean, coverage, policy, now, 5)
	if assessment.Available || assessment.Reason != "余额数据已过期" {
		t.Fatalf("stale balance must fail closed: %+v", assessment)
	}
}

func TestUpstreamBalanceAlertIsDynamicAndCooldownScopedByDomain(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain, account, cfg := seedUpstreamBalanceAssessment(t, m, now)
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: upstreamProviderNewAPI, Enabled: true, Status: upstreamStatusOK,
		BalanceUSD: *account.BalanceUSD, BalanceKnown: true, LastSuccessAt: now,
	}).Error; err != nil {
		t.Fatal(err)
	}
	cfg.UpstreamBalanceAlertsEnabled = true
	cfg.UpstreamBalanceCooldownMin = 720
	m.evaluateUpstreamBalanceAlerts(cfg, now)
	// 超过默认 5 分钟评估周期、但仍在 12 小时邮件冷却内，不能重复发信。
	m.evaluateUpstreamBalanceAlerts(cfg, now+6*60)
	var logs []AlertLog
	if err := m.storeDB.Where("kind LIKE ?", "upstream_balance_low%").Find(&logs).Error; err != nil {
		t.Fatal(err)
	}
	if len(logs) != 1 || logs[0].Target != domain {
		t.Fatalf("dynamic balance alert/cooldown=%+v", logs)
	}
}

func TestUpstreamBalanceAlertEvaluationIsNoMoreFrequentThanBalanceSync(t *testing.T) {
	m := newStabilityTestMonitor(t)
	m.cfg.UpstreamSyncMinutes = 5
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	cfg := defaultAlertConfig()
	cfg.UpstreamBalanceAlertsEnabled = true

	// 没有账户也会完成一次客观评估；随后 5 分钟内不重复扫本地小时表。
	m.evaluateUpstreamBalanceAlerts(cfg, now)
	if got := m.upstreamBalanceAlertLastEval.Load(); got != now {
		t.Fatalf("首次动态评估时间=%d want=%d", got, now)
	}
	m.evaluateUpstreamBalanceAlerts(cfg, now+299)
	if got := m.upstreamBalanceAlertLastEval.Load(); got != now {
		t.Fatalf("同步间隔内不应重复评估，得到 %d", got)
	}
	m.evaluateUpstreamBalanceAlerts(cfg, now+300)
	if got := m.upstreamBalanceAlertLastEval.Load(); got != now+300 {
		t.Fatalf("到达同步间隔后应重新评估，得到 %d", got)
	}
}
