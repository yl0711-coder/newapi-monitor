package monitor

import (
	"context"
	"github.com/yl0711-coder/newapi-monitor/internal/financecur"
	"testing"
)

func TestFinanceRegressionInternalCompletionCache(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "review.example")
	ctx := context.Background()
	from := int64(1777516800)
	scope := stabilityScope{FromTs: from, ToTs: from + 3600}
	if err := m.storeDB.Create(&ChannelSnap{ID: 41, BaseDomain: "review.example"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: from, ChannelID: 41, Grp: "g", ModelName: "m", Success: 2, Quota: 500000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	pending := financeConfiguredInternalEvidence{Accounts: 1, AccountIDs: []int64{1}, Complete: false}
	ready := financeConfiguredInternalEvidence{Accounts: 1, AccountIDs: []int64{1}, Complete: true, Requests: 1, NetQuota: 100000,
		Rows: []FinanceInternalAccountHourFact{{HourTs: from, UserID: 1, ChannelID: 41, Grp: "g", Requests: 1, ConsumeQuota: 100000}}}
	before, hit, err := m.buildFinancePeriodComponent(ctx, scope, from+7200, "same-config", nil, channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, pending, nil)
	if err != nil || hit {
		t.Fatalf("initial hit=%v err=%v", hit, err)
	}
	stale, hit, err := m.buildFinancePeriodComponent(ctx, scope, from+7200, "same-config", nil, channelFinanceSnapshot{}, financeInternalTestCostEvidence{Complete: true}, ready, nil)
	if err != nil {
		t.Fatal(err)
	}
	fresh, _, err := m.buildFinancePeriodComponent(ctx, scope, from+7200, "isolated-rebuild", nil, channelFinanceSnapshot{}, financeInternalTestCostEvidence{Complete: true}, ready, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("pending=%q cached=%q fresh=%q cache_hit=%v", before.Statement.KnownUserConsumption.MicroUSD, stale.Statement.KnownUserConsumption.MicroUSD, fresh.Statement.KnownUserConsumption.MicroUSD, hit)
	if hit || stale.Statement.KnownUserConsumption.MicroUSD != fresh.Statement.KnownUserConsumption.MicroUSD {
		t.Fatal("internal completion returns stale monthly financial values")
	}
}

func TestFinanceRegressionInternalCostSubrangeCompleteness(t *testing.T) {
	scope := stabilityScope{FromTs: 3600, ToTs: 7200}
	for _, complete := range []bool{false, true} {
		got, err := financeInternalTestCostSubrange(financeInternalTestCostEvidence{SourceComplete: complete}, scope)
		if err != nil || got.Complete != complete {
			t.Fatalf("zero-test scope source=%v complete=%v err=%v", complete, got.Complete, err)
		}
	}
	got, err := financeInternalTestCostSubrange(financeInternalTestCostEvidence{SourceComplete: false, Events: []financeInternalTestCostEvent{{HourTs: 3600, Domain: "x", State: "strict"}}}, scope)
	if err != nil || got.Complete {
		t.Fatalf("strict partial input cannot promote incomplete source: %+v %v", got, err)
	}
}

func TestFinanceRegressionAggregateRequiresEveryPeriod(t *testing.T) {
	amount := economicsMoney(1000000)
	period := financePeriodView{UpstreamCoverage: financeUpstreamCoverageView{CorrectedDomains: 1}, Statement: financeStatementView{
		KnownUpstreamBilledCost: amount, UpstreamBilledCost: &amount, KnownRawCorrectedUpstreamCost: amount, RawCorrectedUpstreamCost: &amount,
		KnownCorrectedUpstreamCost: amount, CorrectedUpstreamCost: &amount, KnownContributionProfit: amount, ContributionProfit: &amount,
		PairedUserConsumption: economicsMoney(2000000), PairedCorrectedCost: amount}}
	coverage := financeUpstreamCoverageView{Complete: true, RelevantDomains: 1, CompleteDomains: 1, CorrectedDomains: 1}
	got, err := aggregateFinancePeriods([]financePeriodView{period, period}, StabilityDataCoverage{Complete: true}, coverage)
	if err != nil || got.CorrectedUpstreamCost == nil || got.ContributionProfit == nil || got.ContributionProfit.MicroUSD != "2000000" {
		t.Fatalf("complete sum lost: %+v %v", got, err)
	}
	partial := period
	partial.Statement.CorrectedUpstreamCost = nil
	partial.Statement.ContributionProfit = nil
	got, err = aggregateFinancePeriods([]financePeriodView{period, partial}, StabilityDataCoverage{Complete: true}, coverage)
	if err != nil || got.CorrectedUpstreamCost != nil || got.ContributionProfit != nil || got.KnownContributionProfit.MicroUSD != "2000000" {
		t.Fatalf("partial sum incorrectly promoted or discarded: %+v %v", got, err)
	}
}

func TestFinanceRegressionCURRequiresContinuousCoverage(t *testing.T) {
	const day = int64(86400)
	statement := financecur.Statement{TimeZone: "Asia/Shanghai", Status: financecur.StatementStatusPublishable, FirstUsageUnix: 0, LastUsageThroughUnix: 3 * day}
	bucket := func(start int64) financecur.TimeBucket {
		return financecur.TimeBucket{FromUnix: start, ToUnix: start + day, NexusAPINanoUSD: 1000000000, Complete: true}
	}
	for _, tc := range []struct {
		name        string
		buckets     []financecur.TimeBucket
		from, to    int64
		exact, fail bool
	}{
		{"full", []financecur.TimeBucket{bucket(0), bucket(day)}, 0, 2 * day, true, false},
		{"unordered", []financecur.TimeBucket{bucket(day), bucket(0)}, 0, 2 * day, true, false},
		{"gap", []financecur.TimeBucket{bucket(0), bucket(2 * day)}, 0, 3 * day, false, false},
		{"partial head", []financecur.TimeBucket{bucket(0), bucket(day)}, day / 2, 2 * day, false, false},
		{"duplicate", []financecur.TimeBucket{bucket(0), bucket(0)}, 0, day, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got, err := projectFinanceCURBuckets(statement, tc.buckets, tc.from, tc.to)
			if (err != nil) != tc.fail || (err == nil && (got.ExactCost != nil) != tc.exact) {
				t.Fatalf("coverage mismatch: %+v %v", got, err)
			}
		})
	}
}

func TestFinanceRegressionFingerprintChangesForRedistributionNotFutureHours(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "review.example")
	ctx := context.Background()
	from := int64(1777564800)
	to := from + 3600
	db := m.usageFactsStore()
	s := FinanceInternalAccountFactState{ID: 1, StartHourTs: from, NextHourTs: to, Status: "caught_up"}
	rows := []FinanceInternalAccountHourFact{{HourTs: from, UserID: 1, ChannelID: 1, ConsumeQuota: 10}, {HourTs: from, UserID: 1, ChannelID: 2, ConsumeQuota: 20}}
	if err := db.Create(&s).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	fingerprint := func() string {
		t.Helper()
		v, err := m.financeReportSourceFingerprint(ctx, from, to)
		if err != nil {
			t.Fatal(err)
		}
		return v
	}
	before := fingerprint()
	rows[0].ConsumeQuota = 20
	rows[1].ConsumeQuota = 10
	for _, row := range rows {
		if err := db.Model(&FinanceInternalAccountHourFact{}).Where("hour_ts=? AND user_id=? AND channel_id=? AND grp=?", row.HourTs, row.UserID, row.ChannelID, row.Grp).Update("consume_quota", row.ConsumeQuota).Error; err != nil {
			t.Fatal(err)
		}
	}
	after := fingerprint()
	if before == after {
		t.Fatal("equal total across changed channels did not invalidate")
	}
	s.NextHourTs = to + 3600
	s.UpdatedAt = to + 7200
	if err := db.Save(&s).Error; err != nil {
		t.Fatal(err)
	}
	if err := db.Create(&FinanceInternalAccountHourFact{HourTs: to, UserID: 1, ChannelID: 1, ConsumeQuota: 30}).Error; err != nil {
		t.Fatal(err)
	}
	if after != fingerprint() {
		t.Fatal("future-only progress invalidated settled historical range")
	}
}

func TestFinanceRegressionForceRefreshReplacesMonthlyCache(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "review.example")
	scope := stabilityScope{FromTs: 1777564800, ToTs: 1777568400}
	load := func(ctx context.Context) (financePeriodComponent, bool) {
		t.Helper()
		v, hit, err := m.buildFinancePeriodComponent(ctx, scope, scope.ToTs+3600, "cfg", nil, channelFinanceSnapshot{}, financeInternalTestCostEvidence{Complete: true}, financeConfiguredInternalEvidence{Complete: true}, nil)
		if err != nil {
			t.Fatal(err)
		}
		return v, hit
	}
	_, _ = load(context.Background())
	if _, hit := load(context.Background()); !hit {
		t.Fatal("expected warm cache")
	}
	// Deliberately change a fact without its publication ledger to prove the
	// explicit recovery action does not consult the old component.
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: scope.FromTs, ChannelID: 1, Grp: "g", ModelName: "m", Quota: 500000, Success: 1, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	v, hit := load(context.WithValue(context.Background(), financeForceRebuildKey{}, true))
	if hit || v.Statement.KnownUserConsumption.MicroUSD != "1000000" {
		t.Fatalf("forced refresh reused old result: %+v hit=%v", v.Statement, hit)
	}
	warm, hit := load(context.Background())
	if !hit || warm.Statement.KnownUserConsumption.MicroUSD != "1000000" {
		t.Fatal("force rebuild did not replace monthly cache")
	}
}

func TestFinanceRegressionAggregatePromotesIncompleteCost(t *testing.T) {
	known := economicsMoney(6000000)
	period := financePeriodView{UpstreamCoverage: financeUpstreamCoverageView{CorrectedDomains: 2}, Statement: financeStatementView{KnownCorrectedUpstreamCost: known, KnownContributionProfit: economicsMoney(4000000), PairedUserConsumption: economicsMoney(10000000), PairedCorrectedCost: known}}
	got, err := aggregateFinancePeriods([]financePeriodView{period}, StabilityDataCoverage{Complete: true}, financeUpstreamCoverageView{Complete: true, RelevantDomains: 2, CompleteDomains: 2, CorrectedDomains: 2})
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("input exact cost absent=%v input exact profit absent=%v output exact cost present=%v output exact profit present=%v", period.Statement.CorrectedUpstreamCost == nil, period.Statement.ContributionProfit == nil, got.CorrectedUpstreamCost != nil, got.ContributionProfit != nil)
	if got.CorrectedUpstreamCost != nil || got.ContributionProfit != nil {
		t.Fatal("aggregate promotes partial monthly cost/profit into exact total")
	}
}

func TestFinanceRegressionFingerprintOmitsInternalProgress(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "review.example")
	ctx := context.Background()
	from := int64(1777516800)
	before, err := m.financeReportSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	err = m.usageFactsStore().Create(&FinanceInternalAccountFactState{ID: 1, Status: "caught_up", StartHourTs: from, NextHourTs: from + 3600, UpdatedAt: from + 7200}).Error
	if err != nil {
		t.Fatal(err)
	}
	err = m.usageFactsStore().Create(&FinanceInternalAccountHourFact{HourTs: from, UserID: 1, ChannelID: 41, Requests: 1, ConsumeQuota: 100000}).Error
	if err != nil {
		t.Fatal(err)
	}
	after, err := m.financeReportSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if before == after {
		t.Fatal("internal accounting facts and progress do not invalidate report fingerprint")
	}
}

func TestFinanceRegressionFingerprintOmitsRuntimeSwitch(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "review.example")
	ctx := context.Background()
	from := int64(1777516800)
	config1, err := m.financeReportConfigurationHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source1, err := m.financeReportSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	m.cfg.FinanceEnabled = false
	config2, err := m.financeReportConfigurationHash(ctx)
	if err != nil {
		t.Fatal(err)
	}
	source2, err := m.financeReportSourceFingerprint(ctx, from, from+3600)
	if err != nil {
		t.Fatal(err)
	}
	if config1 == config2 && source1 == source2 {
		t.Fatal("disabling finance does not invalidate persisted report cache identity")
	}
}

func TestFinanceRegressionCURPartialTailPromotedExact(t *testing.T) {
	from := int64(1777564800)
	statement := financecur.Statement{TimeZone: "Asia/Shanghai", Status: financecur.StatementStatusPublishable, FirstUsageUnix: from, LastUsageThroughUnix: from + 3*86400}
	buckets := []financecur.TimeBucket{
		{FromUnix: from, ToUnix: from + 86400, NexusAPINanoUSD: 1000000000, Complete: true},
		{FromUnix: from + 86400, ToUnix: from + 2*86400, NexusAPINanoUSD: 1000000000, Complete: true},
		{FromUnix: from + 2*86400, ToUnix: from + 3*86400, NexusAPINanoUSD: 1000000000, Complete: true},
	}
	got, err := projectFinanceCURBuckets(statement, buckets, from, from+86400+12*3600)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("requested=36h included_days=%d exact=%v known=%s", got.IncludedDays, got.ExactCost != nil, got.KnownCost.MicroUSD)
	if got.ExactCost != nil {
		t.Fatal("24h cost published as exact for a 36h query")
	}
}

func TestFinanceRegressionFinanceHistoryPruned(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "review.example")
	ctx := context.Background()
	from := int64(1777564800)
	scope := stabilityScope{FromTs: from, ToTs: from + 3600}
	if err := m.storeDB.Create(&StabilityHourSample{HourTs: from, ChannelID: 41, Grp: "g", ModelName: "m", Success: 1, Quota: 500000, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	before, _, _, err := m.loadFinanceUserFacts(ctx, scope, from+7200, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := m.pruneStabilityOlderThan(from + 3600); err != nil {
		t.Fatal(err)
	}
	after, _, _, err := m.loadFinanceUserFacts(ctx, scope, from+7200, financeConfiguredInternalEvidence{Complete: true}, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("finance enabled=%v before=%s after=%s", m.cfg.FinanceEnabled, before.KnownUserConsumption.MicroUSD, after.KnownUserConsumption.MicroUSD)
	if before.KnownUserConsumption.MicroUSD != after.KnownUserConsumption.MicroUSD {
		t.Fatal("regular stability retention removes history still required by finance reports")
	}
}
