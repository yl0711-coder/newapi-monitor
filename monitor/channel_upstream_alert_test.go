package monitor

import (
	"context"
	"math"
	"testing"
	"time"
)

func seedUpstreamLedger(t *testing.T, m *Monitor, domain, provider string, now int64, lookbackDays int, dailyCost float64, bucketSeconds int64) {
	t.Helper()
	from, to := upstreamBalanceWindow(now, lookbackDays)
	rows := make([]ChannelUpstreamUsageHour, 0, int((to-from)/bucketSeconds))
	for bucket := from; bucket < to; bucket += bucketSeconds {
		rows = append(rows, ChannelUpstreamUsageHour{
			Domain: domain, HourTs: bucket, BucketSeconds: bucketSeconds,
			Requests: 1, CostUSD: dailyCost * float64(bucketSeconds) / 86400,
			FetchedAt: now, Provider: provider,
		})
	}
	if err := m.storeDB.CreateInBatches(rows, 200).Error; err != nil {
		t.Fatal(err)
	}
}

func seededUpstreamBalanceAccount(now int64, balance float64) ChannelUpstreamAccountView {
	return ChannelUpstreamAccountView{
		Configured: true, Enabled: true, Provider: upstreamProviderNewAPI,
		Status: upstreamStatusOK, BalanceUSD: &balance, LastSuccessAt: now,
		UsageSyncEnabled: true,
	}
}

func TestUpstreamBalanceWindowUsesCompleteCSTDays(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 34, 56, 0, cstLocation).Unix()
	from, to := upstreamBalanceWindow(now, 7)
	if got := time.Unix(to, 0).In(cstLocation); got.Hour() != 0 || got.Minute() != 0 || got.Day() != 8 {
		t.Fatalf("to=%v", got)
	}
	if to-from != 7*86400 {
		t.Fatalf("window=%d want=%d", to-from, 7*86400)
	}
}

func TestUpstreamBalanceAssessmentUsesRawUpstreamLedger(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 40)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)

	// These local sales/cost settings deliberately imply a very different
	// inferred cost. Runway must remain tied only to the upstream ledger.
	if err := m.storeDB.Create(&ChannelFinanceSetting{ID: 1, FXBenchmark: 7, SiteRechargePaid: 1, SiteRechargeCredit: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelDomainCost{Domain: domain, RechargePaid: 1, RechargeCredit: 10}).Error; err != nil {
		t.Fatal(err)
	}
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	estimate := estimates[domain]
	if estimate.CompletedHours != 7*24 || math.Abs(estimate.CoveragePct-100) > 1e-9 || math.Abs(estimate.AverageDailyCostUSD-50) > 1e-9 {
		t.Fatalf("estimate=%+v", estimate)
	}
	assessment := assessUpstreamBalance(account, estimate, policy, now, 5)
	if !assessment.Available || assessment.Status != "warning" || assessment.EstimatedRunwayDays == nil ||
		math.Abs(*assessment.EstimatedRunwayDays-0.8) > 1e-9 || assessment.RequiredBalanceUSD == nil ||
		math.Abs(*assessment.RequiredBalanceUSD-50) > 1e-9 {
		t.Fatalf("assessment=%+v", assessment)
	}
}

func TestUpstreamBalanceAssessmentSupportsNaturalDayLedger(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "aicodewith.com"
	account := seededUpstreamBalanceAccount(now, 250)
	account.Provider = upstreamProviderAICodeWith
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 100, 86400)
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], policy, now, 5)
	if !assessment.Available || assessment.EstimatedRunwayDays == nil || math.Abs(*assessment.EstimatedRunwayDays-2.5) > 1e-9 {
		t.Fatalf("assessment=%+v estimate=%+v", assessment, estimates[domain])
	}
}

func TestUpstreamBalanceAssessmentFailsClosedOnInsufficientLedgerCoverage(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 100)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)
	from, _ := upstreamBalanceWindow(now, 7)
	if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, from, from+10*3600).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		t.Fatal(err)
	}
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], policy, now, 5)
	if assessment.Available || assessment.Status != "unavailable" || estimates[domain].CoveragePct >= policy.MinCoverage {
		t.Fatalf("incomplete ledger must fail closed: estimate=%+v assessment=%+v", estimates[domain], assessment)
	}
}

func TestUpstreamBalanceAssessmentIgnoresOtherProviderRows(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 100)
	seedUpstreamLedger(t, m, domain, upstreamProviderSub2API, now, 7, 999, 3600)
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], policy, now, 5)
	if assessment.Available || estimates[domain].AverageDailyCostUSD != 0 || estimates[domain].CompletedHours != 0 {
		t.Fatalf("provider mismatch must not be counted: estimate=%+v assessment=%+v", estimates[domain], assessment)
	}
}

func TestUpstreamBalanceAssessmentRejectsOverlappingBuckets(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 100)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)
	from, _ := upstreamBalanceWindow(now, 7)
	if err := m.storeDB.Create(&ChannelUpstreamUsageHour{Domain: domain, HourTs: from + 1800, BucketSeconds: 3600, CostUSD: 999, Provider: account.Provider}).Error; err != nil {
		t.Fatal(err)
	}
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], policy, now, 5)
	if assessment.Available || estimates[domain].Reason == "" {
		t.Fatalf("overlap must fail closed: estimate=%+v assessment=%+v", estimates[domain], assessment)
	}
}

func TestUpstreamBalanceAssessmentDisabledIdleAndStaleStates(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 100)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 0, 3600)
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	if got := assessUpstreamBalance(account, estimates[domain], policy, now, 5); !got.Available || got.Status != "idle" {
		t.Fatalf("zero-cost ledger=%+v", got)
	}
	account.UsageSyncEnabled = false
	if got := assessUpstreamBalance(account, estimates[domain], policy, now, 5); got.Available || got.Reason != "上游消费同步尚未启用" {
		t.Fatalf("disabled usage sync=%+v", got)
	}
	account.UsageSyncEnabled = true
	account.LastSuccessAt = now - 31*60
	if got := assessUpstreamBalance(account, estimates[domain], policy, now, 5); got.Available || got.Reason != "余额数据已过期" {
		t.Fatalf("stale balance=%+v", got)
	}
}

func TestUpstreamBalanceAlertIsDynamicAndCooldownScopedByDomain(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 40)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: account.Provider, Enabled: true, Status: upstreamStatusOK,
		BalanceUSD: *account.BalanceUSD, BalanceKnown: true, LastSuccessAt: now,
		UsageSyncEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	cfg := defaultAlertConfig()
	cfg.UpstreamBalanceAlertsEnabled = true
	cfg.UpstreamBalanceCooldownMin = 720
	m.evaluateUpstreamBalanceAlerts(cfg, now)
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
