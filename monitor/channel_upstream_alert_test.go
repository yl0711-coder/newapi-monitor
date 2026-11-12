package monitor

import (
	"context"
	"math"
	"strings"
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

func TestManuallyDisabledUpstreamDomainsRequireEveryCurrentChannelDisabled(t *testing.T) {
	m := newStabilityTestMonitor(t)
	rows := []ChannelSnap{
		{ID: 1, BaseDomain: "disabled.example", Status: 2},
		{ID: 2, BaseDomain: "disabled.example", Status: 2},
		{ID: 3, BaseDomain: "active.example", Status: 2},
		{ID: 4, BaseDomain: "active.example", Status: 1},
		{ID: 5, BaseDomain: "auto.example", Status: 3},
		{ID: 6, BaseDomain: "deleted.example", Status: 2, DeletedAt: 1},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	disabled, err := m.manuallyDisabledUpstreamDomains(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if !disabled["disabled.example"] || disabled["active.example"] || disabled["auto.example"] || disabled["deleted.example"] {
		t.Fatalf("unexpected manually-disabled domains: %+v", disabled)
	}
}

func TestUpstreamBalanceAlertSkipsFullyManuallyDisabledDomain(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "disabled.example"
	account := seededUpstreamBalanceAccount(now, 10)
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)
	if err := m.storeDB.Create(&ChannelUpstreamAccount{
		Domain: domain, Provider: account.Provider, Enabled: true, Status: upstreamStatusOK,
		BalanceUSD: *account.BalanceUSD, BalanceKnown: true, LastSuccessAt: now, UsageSyncEnabled: true,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, BaseDomain: domain, Status: 2}).Error; err != nil {
		t.Fatal(err)
	}
	cfg := defaultAlertConfig()
	cfg.UpstreamBalanceAlertsEnabled = true
	m.evaluateUpstreamBalanceAlerts(cfg, now)
	var count int64
	if err := m.storeDB.Model(&AlertLog{}).Where("kind LIKE ? AND target=?", "upstream_balance_low%", domain).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 0 {
		t.Fatalf("fully manually-disabled upstream emitted %d low-balance alerts", count)
	}
}

// A domain whose automatic sync is paused (停止使用, no enabled channel) has an
// incomplete recent ledger by design; it must say so instead of showing a
// coverage shortfall that reads as a sync fault.
func TestUpstreamBalanceAssessmentExplainsPausedSync(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	domain := "last-api.ai"
	account := seededUpstreamBalanceAccount(now, 100)
	account.SyncPaused = true
	seedUpstreamLedger(t, m, domain, account.Provider, now, 7, 50, 3600)
	from, _ := upstreamBalanceWindow(now, 7)
	if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, from+6*86400, from+7*86400).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
		t.Fatal(err)
	}
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
	if err != nil {
		t.Fatal(err)
	}
	assessment := assessUpstreamBalance(account, estimates[domain], policy, now, 5)
	if assessment.Available || assessment.Status != "unavailable" || !strings.Contains(assessment.Reason, "自动同步已暂停") || strings.Contains(assessment.Reason, "覆盖率") {
		t.Fatalf("paused sync must be explained, assessment=%+v", assessment)
	}
	account.SyncPaused = false
	if assessment := assessUpstreamBalance(account, estimates[domain], policy, now, 5); !strings.Contains(assessment.Reason, "覆盖率") {
		t.Fatalf("active domain keeps the coverage reason, assessment=%+v", assessment)
	}
}

// A newly connected account whose history import has not reached the window
// yet is assessed from its most recent complete days, labeled as partial.
func TestUpstreamBalancePartialHistoryUsesRecentCompleteDays(t *testing.T) {
	now := time.Date(2026, 8, 8, 12, 0, 0, 0, cstLocation).Unix()
	from, to := upstreamBalanceWindow(now, 7)
	domain := "last-api.ai"
	policy := upstreamBalancePolicyFor(defaultAlertConfig())
	setup := func(t *testing.T, keepFrom int64, lastDayHours int) *Monitor {
		t.Helper()
		m := newStabilityTestMonitor(t)
		seedUpstreamLedger(t, m, domain, upstreamProviderNewAPI, now, 7, 48, 3600)
		if err := m.storeDB.Where("domain=? AND hour_ts<?", domain, keepFrom).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			t.Fatal(err)
		}
		// The most recent day spent twice as much: the estimate must use only
		// the recent days, (48+96)/2 = 72 per day.
		if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain=? AND hour_ts>=?", domain, to-86400).Update("cost_usd", 4).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Where("domain=? AND hour_ts>=?", domain, to-86400+int64(lastDayHours)*3600).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			t.Fatal(err)
		}
		return m
	}
	assess := func(t *testing.T, m *Monitor, account ChannelUpstreamAccountView) ChannelUpstreamBalanceAssessment {
		t.Helper()
		estimates, err := m.loadUpstreamBurnEstimates(context.Background(), now, policy, map[string]ChannelUpstreamAccountView{domain: account})
		if err != nil {
			t.Fatal(err)
		}
		return assessUpstreamBalance(account, estimates[domain], policy, now, 5)
	}
	importing := seededUpstreamBalanceAccount(now, 50)
	importing.UsageBackfillCursor = from - 20*86400

	t.Run("two recent complete days", func(t *testing.T) {
		m := setup(t, to-2*86400, 24)
		a := assess(t, m, importing)
		if !a.Available || !a.PartialHistory || a.LookbackDays != 2 || math.Abs(a.AverageDailyCostUSD-72) > 1e-6 ||
			a.EstimatedRunwayDays == nil || math.Abs(*a.EstimatedRunwayDays-50.0/72) > 1e-6 || a.Status != "warning" || !strings.Contains(a.Reason, "历史补齐中") {
			t.Fatalf("assessment=%+v", a)
		}
		if note := upstreamBalancePartialAlertNote(a); !strings.Contains(note, "近 2 个完整自然日") {
			t.Fatalf("alert note=%q", note)
		}
	})
	t.Run("only one complete day", func(t *testing.T) {
		if a := assess(t, setup(t, to-86400, 24), importing); a.Available || a.PartialHistory || !strings.Contains(a.Reason, "覆盖率") {
			t.Fatalf("assessment=%+v", a)
		}
	})
	t.Run("most recent day incomplete", func(t *testing.T) {
		if a := assess(t, setup(t, to-3*86400, 20), importing); a.Available || a.PartialHistory {
			t.Fatalf("assessment=%+v", a)
		}
	})
	t.Run("history import finished", func(t *testing.T) {
		done := importing
		done.UsageBackfillDone = true
		if a := assess(t, setup(t, to-2*86400, 24), done); a.Available || a.PartialHistory {
			t.Fatalf("a gap after the import finished must still fail closed: %+v", a)
		}
	})
	t.Run("import already passed the gap", func(t *testing.T) {
		passed := importing
		passed.UsageBackfillCursor = to - 86400
		if a := assess(t, setup(t, to-2*86400, 24), passed); a.Available || a.PartialHistory {
			t.Fatalf("a gap the import already covered is a sync gap: %+v", a)
		}
	})
	t.Run("cursor exactly at the first day used", func(t *testing.T) {
		// The day before the recent complete days is fully past the import:
		// its gap is a sync gap (e.g. the AICodeWith reconcile cursor).
		at := importing
		at.UsageBackfillCursor = to - 2*86400
		if a := assess(t, setup(t, to-2*86400, 24), at); a.Available || a.PartialHistory {
			t.Fatalf("assessment=%+v", a)
		}
	})
	t.Run("import still inside the breaking day", func(t *testing.T) {
		// Earlier window days are imported and complete; the import is now in
		// the last hour of the day before the recent complete days.
		m := setup(t, from, 24)
		if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, to-3*86400, to-2*86400-3600).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			t.Fatal(err)
		}
		inside := importing
		inside.UsageBackfillCursor = to - 2*86400 - 3600
		if a := assess(t, m, inside); !a.Available || !a.PartialHistory || a.LookbackDays != 2 {
			t.Fatalf("assessment=%+v", a)
		}
	})
	for name, mutate := range map[string]func(*ChannelUpstreamAccountView){
		"history failing": func(a *ChannelUpstreamAccountView) {
			a.UsageBackfillConsecutiveFails, a.UsageBackfillLastError = 3, "上游返回 HTTP 500"
		},
		"history isolated": func(a *ChannelUpstreamAccountView) { a.UsageBackfillNextSyncAt = upstreamAccountIsolatedUntil },
	} {
		t.Run(name, func(t *testing.T) {
			stuck := importing
			mutate(&stuck)
			if a := assess(t, setup(t, to-2*86400, 24), stuck); a.Available || a.PartialHistory {
				t.Fatalf("a stalled import must not be labeled as catching up: %+v", a)
			}
		})
	}
	t.Run("imported window day has a gap", func(t *testing.T) {
		// History imported the first window day (cursor past it), yet that day
		// is incomplete: a real sync gap, so no partial estimate.
		m := setup(t, from, 24)
		if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, from, from+5*3600).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, from+2*86400, to-2*86400).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			t.Fatal(err)
		}
		passed := importing
		passed.UsageBackfillCursor = from + 2*86400
		if a := assess(t, m, passed); a.Available || a.PartialHistory {
			t.Fatalf("assessment=%+v", a)
		}
		if err := m.storeDB.Where("domain=? AND hour_ts>=? AND hour_ts<?", domain, from, from+86400).Delete(&ChannelUpstreamUsageHour{}).Error; err != nil {
			t.Fatal(err)
		}
		seedRows := make([]ChannelUpstreamUsageHour, 0, 24)
		for h := from; h < from+86400; h += 3600 {
			seedRows = append(seedRows, ChannelUpstreamUsageHour{Domain: domain, HourTs: h, BucketSeconds: 3600, Requests: 1, CostUSD: 2, FetchedAt: now, Provider: upstreamProviderNewAPI})
		}
		if err := m.storeDB.Create(&seedRows).Error; err != nil {
			t.Fatal(err)
		}
		if a := assess(t, m, passed); !a.Available || !a.PartialHistory || a.LookbackDays != 2 {
			t.Fatalf("complete imported days with only unimported gaps must estimate: %+v", a)
		}
	})
	t.Run("cursor unset", func(t *testing.T) {
		unset := importing
		unset.UsageBackfillCursor = 0
		if a := assess(t, setup(t, to-2*86400, 24), unset); a.Available {
			t.Fatalf("assessment=%+v", a)
		}
	})
}
