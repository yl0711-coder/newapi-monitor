package monitor

import (
	"context"
	"testing"
	"time"
)

func financeInternalRangeFixture(t *testing.T) (*Monitor, stabilityScope, stabilityScope) {
	t.Helper()
	m := newFinanceReportTestMonitor(t, "boundary.example")
	loc, _ := time.LoadLocation("Asia/Shanghai")
	start := time.Date(2026, 5, 1, 0, 0, 0, 0, loc).Unix()
	monthEnd := time.Date(2026, 6, 1, 0, 0, 0, 0, loc).Unix()
	end := monthEnd + 2*86400
	accounts := []FinanceInternalAccount{{UserID: 1}}
	if err := m.storeDB.Create(&accounts).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&ChannelSnap{ID: 41, BaseDomain: "boundary.example"}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.usageFactsStore().Create(&FinanceInternalAccountFactState{
		ID: 1, ConfigHash: financeInternalAccountHash(accounts), SourceEpoch: m.cfg.UsageFactsHistorySourceEpoch,
		StartHourTs: start, NextHourTs: end - 3600, Status: "backfilling",
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, hour := range []int64{start, monthEnd, end - 3600} {
		if err := m.usageFactsStore().Create(&FinanceInternalAccountHourFact{
			HourTs: hour, UserID: 1, ChannelID: 41, Grp: "g", Requests: 1, ConsumeQuota: 100000,
		}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.storeDB.Create(&StabilityHourSample{
			HourTs: hour, ChannelID: 41, Grp: "g", ModelName: "m", Success: 2, Quota: 500000,
			TrafficClassVersion: stabilityTrafficClassificationVersion,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	return m, stabilityScope{FromTs: start, ToTs: monthEnd}, stabilityScope{FromTs: start, ToTs: end}
}

func TestFinanceHistoricalRangeIndependentOfIncompleteTail(t *testing.T) {
	m, month, full := financeInternalRangeFixture(t)
	ctx := context.Background()
	direct, err := m.loadFinanceConfiguredInternalEvidence(ctx, month, nil)
	if err != nil {
		t.Fatal(err)
	}
	partial, err := m.loadFinanceInternalEvidence(ctx, full, nil, true)
	if err != nil {
		t.Fatal(err)
	}
	if partial.Complete || len(partial.Rows) != 2 {
		t.Fatalf("unverified tail included: %+v", partial)
	}
	sliced, err := financeConfiguredInternalSubrange(partial, month)
	if err != nil {
		t.Fatal(err)
	}
	if !direct.Complete || !sliced.Complete || sliced.NetQuota != direct.NetQuota || len(sliced.Rows) != 1 {
		t.Fatalf("closed historical range lost: direct=%+v sliced=%+v", direct, sliced)
	}
	legacy, err := m.loadFinanceConfiguredInternalEvidence(ctx, full, nil)
	if err != nil || legacy.Complete || len(legacy.Rows) != 0 {
		t.Fatalf("existing consumers changed: %+v %v", legacy, err)
	}
	outside, err := financeConfiguredInternalSubrange(partial, stabilityScope{FromTs: full.ToTs - 3600, ToTs: full.ToTs})
	if err != nil || outside.Complete || len(outside.Rows) != 0 {
		t.Fatalf("unverified hour promoted: %+v %v", outside, err)
	}
}

func TestFinanceHistoricalReportAndDaysSurviveIncompleteTail(t *testing.T) {
	m, month, full := financeInternalRangeFixture(t)
	ctx := context.Background()
	direct, err := m.buildFinanceOperatingReport(ctx, time.Unix(month.FromTs, 0), time.Unix(month.ToTs, 0))
	if err != nil {
		t.Fatal(err)
	}
	combined, err := m.buildFinanceOperatingReport(ctx, time.Unix(full.FromTs, 0), time.Unix(full.ToTs, 0))
	if err != nil {
		t.Fatal(err)
	}
	if len(combined.Periods) != 2 || direct.Periods[0].Statement.KnownUserConsumption.MicroUSD != "800000" ||
		combined.Periods[0].Statement.KnownUserConsumption != direct.Periods[0].Statement.KnownUserConsumption {
		t.Fatalf("historical amount changed with query tail: direct=%+v combined=%+v", direct.Periods, combined.Periods)
	}
	if combined.Periods[1].Statement.KnownUserConsumption.MicroUSD != "" || combined.Statement.UserConsumption != nil || combined.Statement.ContributionProfit != nil {
		t.Fatal("incomplete month/report published as exact")
	}
	for _, day := range combined.Days {
		switch day.Date {
		case "2026-05-01", "2026-06-01":
			if day.Statement.KnownUserConsumption.MicroUSD != "800000" {
				t.Fatalf("complete day's known amount lost: %+v", day)
			}
		case "2026-06-02":
			if day.Statement.KnownUserConsumption.MicroUSD != "" || day.Statement.UserConsumption != nil {
				t.Fatalf("incomplete day published: %+v", day)
			}
		}
	}
}

func TestFinancePartialInternalEvidenceRejectsWrongConfigurationAndFailure(t *testing.T) {
	for _, scenario := range []string{"configuration", "source", "failure"} {
		t.Run(scenario, func(t *testing.T) {
			m, _, full := financeInternalRangeFixture(t)
			updates := map[string]any{}
			switch scenario {
			case "configuration":
				updates["config_hash"] = "old"
			case "source":
				updates["source_epoch"] = "old"
			case "failure":
				updates["failure_streak"] = 1
			}
			if err := m.usageFactsStore().Model(&FinanceInternalAccountFactState{}).Where("id=1").Updates(updates).Error; err != nil {
				t.Fatal(err)
			}
			got, err := m.loadFinanceInternalEvidence(context.Background(), full, nil, true)
			if err != nil || got.Complete || len(got.Rows) != 0 || got.VerifiedScope.ToTs != 0 {
				t.Fatalf("untrusted facts used: %+v %v", got, err)
			}
		})
	}
}

func TestFinanceInternalCostScopeIndependentOfOtherDays(t *testing.T) {
	const start = int64(1777564800)
	source := financeInternalTestCostEvidence{
		SourceScope: stabilityScope{FromTs: start, ToTs: start + 2*86400 - 3600},
		Events: []financeInternalTestCostEvent{
			{HourTs: start, Domain: "a", State: "strict", Fact: financeInternalTestCostFact{Rows: 1, CorrectedCostMicroUSD: 100}},
			{HourTs: start + 86400, Domain: "a", State: "unverified"},
		},
	}
	first, err := financeInternalTestCostSubrange(source, stabilityScope{FromTs: start, ToTs: start + 86400})
	if err != nil || !first.Complete || first.Total.CorrectedCostMicroUSD != 100 {
		t.Fatalf("first day lost: %+v %v", first, err)
	}
	second, err := financeInternalTestCostSubrange(source, stabilityScope{FromTs: start + 86400, ToTs: start + 2*86400})
	if err != nil || second.Complete || second.SourceComplete {
		t.Fatalf("tail promoted: %+v %v", second, err)
	}
}

func TestFinanceInternalFailureAndRecoveryInvalidateReportCache(t *testing.T) {
	m, month, _ := financeInternalRangeFixture(t)
	ctx := context.Background()
	fingerprint := func() string {
		t.Helper()
		value, err := m.financeReportSourceFingerprint(ctx, month.FromTs, month.ToTs)
		if err != nil {
			t.Fatal(err)
		}
		return value
	}
	before := fingerprint()
	if err := m.usageFactsStore().Model(&FinanceInternalAccountFactState{}).Where("id=1").Update("failure_streak", 1).Error; err != nil {
		t.Fatal(err)
	}
	failed := fingerprint()
	if before == failed {
		t.Fatal("sync failure did not invalidate cached completeness")
	}
	if err := m.usageFactsStore().Model(&FinanceInternalAccountFactState{}).Where("id=1").Update("failure_streak", 0).Error; err != nil {
		t.Fatal(err)
	}
	if fingerprint() != before {
		t.Fatal("recovered unchanged history should regain original fingerprint")
	}
}
