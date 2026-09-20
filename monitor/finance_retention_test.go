package monitor

import "testing"

func TestFinanceRetentionProtectsOnlyFinancialAggregates(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "retention.example")
	start := int64(1777564800)
	cutoff := start + 86400
	for _, hour := range []int64{start - 86400, start} {
		for _, row := range []any{&StabilityHourSample{HourTs: hour, ChannelID: 1, ModelName: "m", Grp: "g"}, &ChannelTestHourSample{HourTs: hour, ChannelID: 1}, &StabilityHourIngestState{HourTs: hour, Status: "complete"}, &StabilityProblemSample{BucketTs: hour, SignatureHash: "s"}} {
			if err := m.storeDB.Create(row).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.pruneStabilityOlderThan(cutoff); err != nil {
		t.Fatal(err)
	}
	for _, model := range []any{&StabilityHourSample{}, &ChannelTestHourSample{}, &StabilityHourIngestState{}} {
		var n int64
		if err := m.storeDB.Model(model).Count(&n).Error; err != nil {
			t.Fatal(err)
		}
		if n != 1 {
			t.Fatalf("financial aggregates kept=%d want1", n)
		}
	}
	var n int64
	if err := m.storeDB.Model(&StabilityProblemSample{}).Count(&n).Error; err != nil {
		t.Fatal(err)
	}
	if n != 0 {
		t.Fatal("raw problem retention was widened")
	}
}

func TestFinanceRetentionSurvivesDisabledFeatureAndRejectsInvalidStart(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "retention.example")
	m.cfg.FinanceEnabled = false
	m.cfg.FinanceFactsSyncEnabled = false
	const cutoff = int64(1800000000)
	got, err := m.financeProtectedAggregateCutoff(cutoff)
	if err != nil || got != cutoff {
		t.Fatalf("empty disabled install retention changed: %d %v", got, err)
	}
	if err := m.usageFactsStore().Create(&FinanceUserHourState{HourTs: 1777564800}).Error; err != nil {
		t.Fatal(err)
	}
	got, err = m.financeProtectedAggregateCutoff(cutoff)
	if err != nil || got >= cutoff {
		t.Fatalf("disabled existing ledger not protected: %d %v", got, err)
	}
	m.cfg.FinanceStartDate = "invalid"
	if _, err := m.financeProtectedAggregateCutoff(cutoff); err == nil {
		t.Fatal("invalid date allowed destructive pruning")
	}
}
