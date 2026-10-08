package monitor

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func customerHealthHistoryTestMonitor(t *testing.T) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	if err := m.storeDB.AutoMigrate(&CustomerHealthDayCoverage{}); err != nil {
		t.Fatal(err)
	}
	return m
}

func customerHealthHistoryFixtureNow() time.Time {
	return time.Date(2026, 9, 30, 12, 0, 0, 0, cstLocation)
}

func TestCustomerHealthHistoricalCoverageStitches24HoursAnd7Days(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	now := customerHealthHistoryFixtureNow()
	today, target := customerHealthSourceRange(now)
	for age := int64(1); age <= customerHealthHistoryDays; age++ {
		day := today - age*86400
		if err := m.storeDB.Create(&CustomerHealthDayCoverage{
			DayTs: day, ThroughTs: day + 86400,
			SemanticsVersion:     customerHealthStabilityPolicyVersion,
			TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
			TTFTCoverageFromTs:   day, TTFTCoverageThroughTs: day + 86400,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Save(&CustomerHealthSourceCursor{
		ID: 1, DayTs: today, ThroughTs: target,
		SemanticsVersion:     customerHealthStabilityPolicyVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   today, TTFTCoverageThroughTs: target,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, days := range []int64{1, 3, 7} {
		from := target - days*86400
		got, err := m.customerHealthHistoricalCoverage(context.Background(), from, target)
		if err != nil {
			t.Fatal(err)
		}
		if !got.RequestsComplete || !got.FRTComplete || got.RequestFromTs != from || got.RequestThroughTs != target ||
			got.FRTFromTs != from || got.FRTThroughTs != target {
			t.Fatalf("%dd coverage=%+v", days, got)
		}
	}
	// A populated fact table cannot bridge a missing day certificate.
	missingDay := today - 3*86400
	if err := m.storeDB.Delete(&CustomerHealthDayCoverage{}, "day_ts = ?", missingDay).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.customerHealthHistoricalCoverage(context.Background(), target-7*86400, target)
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestsComplete || got.FRTComplete || got.RequestThroughTs != missingDay || got.FRTThroughTs != missingDay {
		t.Fatalf("missing day was bridged: %+v", got)
	}
}

func TestCustomerHealthHistoricalCoverageUsesCurrentCursorWhenFRTReset(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	now := customerHealthHistoryFixtureNow()
	today, target := customerHealthSourceRange(now)
	yesterday := today - 86400
	for _, row := range []CustomerHealthDayCoverage{
		{DayTs: yesterday, ThroughTs: today, SemanticsVersion: customerHealthStabilityPolicyVersion,
			TTFTSemanticsVersion: ttftCoverageSemanticsVersion, TTFTCoverageFromTs: yesterday, TTFTCoverageThroughTs: today},
		// Deliberately stale high FRT proof for the current day.
		{DayTs: today, ThroughTs: target, SemanticsVersion: customerHealthStabilityPolicyVersion,
			TTFTSemanticsVersion: ttftCoverageSemanticsVersion, TTFTCoverageFromTs: today, TTFTCoverageThroughTs: target},
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Save(&CustomerHealthSourceCursor{
		ID: 1, DayTs: today, ThroughTs: target,
		SemanticsVersion:     customerHealthStabilityPolicyVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   today, TTFTCoverageThroughTs: today,
	}).Error; err != nil {
		t.Fatal(err)
	}
	// BeforeCreate gives new cursor rows a default FRT through; simulate the
	// actual migration reset by updating the persisted row explicitly.
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id = ?", 1).
		Update("ttft_coverage_through_ts", today).Error; err != nil {
		t.Fatal(err)
	}
	from := target - 86400
	got, err := m.customerHealthHistoricalCoverage(context.Background(), from, target)
	if err != nil {
		t.Fatal(err)
	}
	if !got.RequestsComplete || got.FRTComplete || got.FRTThroughTs != today {
		t.Fatalf("stale day ledger masked FRT reset: %+v", got)
	}
}

func TestCustomerHealthHistoricalCoverageRejectsWatermarksPastDayEnd(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	today, _ := customerHealthSourceRange(customerHealthHistoryFixtureNow())
	yesterday := today - 86400
	if err := m.storeDB.Create(&CustomerHealthDayCoverage{
		DayTs: yesterday, ThroughTs: today + 60,
		SemanticsVersion:     customerHealthStabilityPolicyVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   yesterday, TTFTCoverageThroughTs: today + 60,
	}).Error; err != nil {
		t.Fatal(err)
	}
	got, err := m.customerHealthHistoricalCoverage(context.Background(), yesterday, today)
	if err != nil {
		t.Fatal(err)
	}
	if got.RequestsComplete || got.FRTComplete || got.RequestThroughTs != 0 || got.FRTThroughTs != 0 {
		t.Fatalf("past-day-end watermark was treated as valid proof: %+v", got)
	}
}

func TestCustomerHealthHistoryDoesNotCertifyYesterdayTailBeforeDelay(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	now := time.Date(2026, 9, 30, 0, 0, 30, 0, cstLocation)
	today, _ := customerHealthSourceRange(now)
	yesterday := today - 86400
	finalized := today - 120
	if switchDay, target := customerHealthSourceDayTransition(yesterday, today, customerHealthSourceFinalizedThrough(now), finalized); switchDay || target != finalized {
		t.Fatalf("live source would prematurely close yesterday: switch=%v target=%d", switchDay, target)
	}
	if err := m.storeDB.Create(&CustomerHealthDayCoverage{
		DayTs: yesterday, ThroughTs: finalized,
		SemanticsVersion:     customerHealthStabilityPolicyVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   yesterday, TTFTCoverageThroughTs: finalized,
	}).Error; err != nil {
		t.Fatal(err)
	}
	var spans [][2]int64
	sample := func(_ context.Context, from, to int64) (int, error) {
		spans = append(spans, [2]int64{from, to})
		return 0, nil
	}
	if worked, err := m.runCustomerHealthHistoryTurnWith(context.Background(), now, sample); err != nil || !worked {
		t.Fatalf("early turn worked=%v err=%v", worked, err)
	}
	if len(spans) != 1 || spans[0][0] >= yesterday || spans[0][1] > yesterday {
		t.Fatalf("yesterday's unfinalized tail was sampled: %v", spans)
	}
	var row CustomerHealthDayCoverage
	if err := m.storeDB.First(&row, "day_ts = ?", yesterday).Error; err != nil {
		t.Fatal(err)
	}
	if row.ThroughTs != finalized || row.TTFTCoverageThroughTs != finalized {
		t.Fatalf("yesterday was certified before finalize delay elapsed: %+v", row)
	}
	spans = nil
	if worked, err := m.runCustomerHealthHistoryTurnWith(context.Background(), now.Add(2*time.Minute), sample); err != nil || !worked {
		t.Fatalf("later turn worked=%v err=%v", worked, err)
	}
	if len(spans) != 1 || spans[0] != [2]int64{finalized, today} {
		t.Fatalf("finalized tail was not resumed: %v", spans)
	}
}

func TestCustomerHealthHistoryTurnResumesFailedHourWithoutAdvancingProof(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	now := customerHealthHistoryFixtureNow()
	today, _ := customerHealthSourceRange(now)
	yesterday := today - 86400
	var spans [][2]int64
	sample := func(_ context.Context, from, to int64) (int, error) {
		spans = append(spans, [2]int64{from, to})
		if len(spans) == 2 {
			return 0, fmt.Errorf("simulated source outage")
		}
		return 0, nil
	}
	worked, err := m.runCustomerHealthHistoryTurnWith(context.Background(), now, sample)
	if err != nil || !worked || spans[0] != [2]int64{yesterday, yesterday + 3600} {
		t.Fatalf("first history turn worked=%v err=%v spans=%v", worked, err, spans)
	}
	worked, err = m.runCustomerHealthHistoryTurnWith(context.Background(), now, sample)
	if err == nil || worked || spans[1] != [2]int64{yesterday + 3600, yesterday + 7200} {
		t.Fatalf("failed second hour moved cursor: worked=%v err=%v spans=%v", worked, err, spans)
	}
	var row CustomerHealthDayCoverage
	if err := m.storeDB.First(&row, "day_ts = ?", yesterday).Error; err != nil {
		t.Fatal(err)
	}
	if row.ThroughTs != yesterday+3600 || row.TTFTCoverageThroughTs != yesterday+3600 {
		t.Fatalf("failed hour advanced durable proof: %+v", row)
	}
	worked, err = m.runCustomerHealthHistoryTurnWith(context.Background(), now, sample)
	if err != nil || !worked || spans[2] != spans[1] {
		t.Fatalf("retry did not resume failed hour: worked=%v err=%v spans=%v", worked, err, spans)
	}
}

func TestCustomerHealthHistoryPrunesOnlyWholeExpiredDays(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	today, _ := customerHealthSourceRange(customerHealthHistoryFixtureNow())
	oldest := today - customerHealthHistoryDays*86400
	rows := []CapacityUserMinuteSample{
		{BucketTs: oldest - 60, UserID: 1, ChannelID: 1, ModelName: "expired", Grp: "g", Success: 1},
		{BucketTs: oldest, UserID: 1, ChannelID: 1, ModelName: "needed", Grp: "g", Success: 1},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.pruneCustomerHealthHistoryBefore(oldest); err != nil {
		t.Fatal(err)
	}
	var stored []CapacityUserMinuteSample
	if err := m.storeDB.Find(&stored).Error; err != nil {
		t.Fatal(err)
	}
	if len(stored) != 1 || stored[0].BucketTs != oldest {
		t.Fatalf("oldest partial day was lost: %+v", stored)
	}
}

func TestCustomerHealthHistoryIdlePruneHonorsConfiguredRetentionAndRunsOncePerDay(t *testing.T) {
	m := customerHealthHistoryTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 10
	now := customerHealthHistoryFixtureNow()
	today, _ := customerHealthSourceRange(now)
	for age := int64(1); age <= customerHealthHistoryDays; age++ {
		day := today - age*86400
		if err := m.storeDB.Create(&CustomerHealthDayCoverage{
			DayTs: day, ThroughTs: day + 86400,
			SemanticsVersion:     customerHealthStabilityPolicyVersion,
			TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
			TTFTCoverageFromTs:   day, TTFTCoverageThroughTs: day + 86400,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	keep := today - 10*86400
	expired := keep - 60
	for _, ts := range []int64{expired, keep} {
		if err := m.storeDB.Create(&CapacityUserMinuteSample{
			BucketTs: ts, UserID: 1, ChannelID: 1, ModelName: "retention", Grp: "g", Success: 1,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
	sample := func(_ context.Context, _, _ int64) (int, error) {
		t.Fatal("all history days are complete")
		return 0, nil
	}
	if worked, err := m.runCustomerHealthHistoryTurnWith(context.Background(), now, sample); worked || err != nil {
		t.Fatalf("idle turn worked=%v err=%v", worked, err)
	}
	var rows []CapacityUserMinuteSample
	if err := m.storeDB.Order("bucket_ts").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].BucketTs != keep {
		t.Fatalf("configured retention not honored: %+v", rows)
	}
	// A same-day idle poll must not run the DELETE again.
	if err := m.storeDB.Create(&CapacityUserMinuteSample{
		BucketTs: expired, UserID: 1, ChannelID: 1, ModelName: "late-expired", Grp: "g", Success: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if worked, err := m.runCustomerHealthHistoryTurnWith(context.Background(), now.Add(time.Minute), sample); worked || err != nil {
		t.Fatalf("second idle turn worked=%v err=%v", worked, err)
	}
	var count int64
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", expired).Count(&count).Error; err != nil {
		t.Fatal(err)
	}
	if count != 1 {
		t.Fatalf("same-day idle turn repeated prune: count=%d", count)
	}
}
