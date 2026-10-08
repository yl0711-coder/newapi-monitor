package monitor

import (
	"context"
	"testing"
	"time"

	"gorm.io/gorm"
)

func TestModelStatisticsTargetUsesCommonEnabledSourceDelay(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	now := time.Date(2026, 9, 26, 20, 0, 37, 0, cstLocation)

	// This is only the theoretical upper bound. Persisted source cursors may
	// move the actual report window earlier, but never later than this target.
	m.cfg.CustomerHealthSourceEnabled = true
	m.cfg.CloudWatchPreRouteEnabled = true
	got := modelStatisticsWindowTarget(m, now)
	_, customerTarget := customerHealthSourceRange(now)
	_, rejectionTarget := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	want := customerTarget
	if rejectionTarget < want {
		want = rejectionTarget
	}
	if got != want {
		t.Fatalf("independent source window end mismatch: got=%d want=%d", got, want)
	}

	// With the independent lane disabled, the delayed metric finalizer is the
	// source of truth, even when the rejection lane is enabled.
	m.cfg.CustomerHealthSourceEnabled = false
	got = modelStatisticsWindowTarget(m, now)
	want = metricFinalizeTarget(now.Unix())
	if rejectionTarget < want {
		want = rejectionTarget
	}
	if got != want {
		t.Fatalf("standard source window end mismatch: got=%d want=%d", got, want)
	}
}

func TestModelStatisticsUsesClosedFinalizationWindowAndSeparatesCoverageGaps(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CapacityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
	m.cfg.RetentionDays = 7
	now := time.Date(2026, 9, 26, 20, 0, 0, 0, cstLocation)
	// The report window intentionally ends at the same closed-minute
	// finalization watermark as the sampler, not at wall-clock now.
	to := metricFinalizeTarget(now.Unix())
	metricTarget := to
	_, rejectTarget := cloudWatchPreRouteRange(now, 168)
	if rejectTarget-60 <= to {
		t.Fatalf("test requires rejection source target to be ahead of closed report window: to=%d reject_target=%d", to, rejectTarget)
	}
	metric := MetricFinalizeState{ID: 1, SemanticsVersion: stabilityTrafficClassificationVersion, CoverageFromTs: to - 8*86400, NextTs: metricTarget, TTFTSemanticsVersion: ttftCoverageSemanticsVersion, TTFTCoverageFromTs: to - 8*86400, TTFTCoverageThroughTs: metricTarget}
	reject := CloudWatchPreRouteCursor{ID: cloudWatchPreRouteCursorID, SemanticsVersion: cloudWatchPreRouteVersion, CoverageFromTs: to - 8*86400, ThroughTs: rejectTarget}
	for _, value := range []any{&metric, &reject,
		&CapacityUserMinuteSample{BucketTs: to - 120, UserID: 1, ChannelID: 1, ModelName: "test", Grp: "g", Success: 3},
		&MetricSample{BucketTs: to - 120, ChannelID: 1, ModelName: "test", Grp: "g", Success: 3},
		&RejectionSample{BucketTs: to - 120, Node: cloudWatchPreRouteNode, UserID: 1, Model: "test", Grp: "g", Reason: "route_no_channel", Count: 2},
	} {
		if err := m.storeDB.Create(value).Error; err != nil {
			t.Fatal(err)
		}
	}
	for _, window := range []string{"24h", "3d", "7d"} {
		report, err := m.buildModelStatisticsReport(context.Background(), window, now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Source.CoverageStatus != modelCoverageComplete || !report.Source.FactsComplete || !report.Source.TTFTComplete {
			t.Fatalf("closed finalization window mislabeled: %+v", report.Source)
		}
		// Coverage in the response is clipped to this report's closed window;
		// a collector can be further ahead without changing the report proof.
		if report.Source.RoutedCoverage.ThroughTs != metricTarget || report.Source.UnavailableCoverage.ThroughTs != to {
			t.Fatalf("wrong watermarks: %+v", report.Source)
		}
		if report.ToTs != to || len(report.Models) != 1 || report.Models[0].Requests != 5 {
			t.Fatalf("finalized requests hidden or window shifted: %+v", report)
		}
	}
	check := func(want string, wantTo int64) {
		t.Helper()
		report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Source.CoverageStatus != want || report.Source.FactsComplete != (want == modelCoverageComplete) {
			t.Fatalf("want %s: %+v", want, report.Source)
		}
		if report.ToTs != wantTo || report.TargetTs != to || report.LagSeconds != to-wantTo || report.ToTs-report.FromTs != 86400 {
			t.Fatalf("window must follow committed cursors without shortening duration: %+v", report)
		}
	}
	metric.NextTs = metricTarget - 60
	if err := m.storeDB.Save(&metric).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageComplete, to-60) // Normal polling lag selects an earlier complete window.
	metric.NextTs = to
	metric.TTFTCoverageThroughTs = to
	if err := m.storeDB.Save(&metric).Error; err != nil {
		t.Fatal(err)
	}
	reject.ThroughTs = rejectTarget - 60
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageComplete, to) // The rejection source is behind its own target but still covers report ToTs.
	reject.ThroughTs = to - 60
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageComplete, to-60) // Both lanes must use this same committed right boundary.
	reject.ThroughTs = to
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageComplete, to)
	reject.SemanticsVersion = cloudWatchPreRouteVersion - 1
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageIncomplete, to)
	reject.SemanticsVersion = cloudWatchPreRouteVersion
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	// Missing projected users inside a certified interval is a real gap, not a live tail.
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = ?", 1).Update("success", 1).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageIncomplete, to)
	m.cfg.CloudWatchPreRouteEnabled = false
	check(modelCoverageIncomplete, to)
}

func TestModelStatisticsRejectionCoverageMarksLegacyMixIncomplete(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CloudWatchPreRouteEnabled = true
	now := time.Date(2026, 9, 26, 20, 0, 0, 0, cstLocation)
	_, target := cloudWatchPreRouteRange(now, 168)
	from := target - 3600
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: from - 3600,
		NextTs: target, ThroughTs: target, TargetThroughTs: target,
		SemanticsVersion: cloudWatchPreRouteVersion, Status: "caught_up",
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&RejectionSample{
		BucketTs: target - 60, Node: "legacy", Reason: "no_channel", Model: "m", Grp: "g", UserID: 0, Count: 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	coverage := m.modelStatisticsRejectionCoverage(context.Background(), from, target, now)
	if coverage.Status == modelCoverageComplete || coverage.Source != "mixed" {
		t.Fatalf("CloudWatch + legacy user_id=0 must be mixed/incomplete: %+v", coverage)
	}
}

func TestModelStatisticsIndependentDayDoesNotCertifyHistoricalWindow(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CapacityEnabled, m.cfg.CustomerHealthSourceEnabled = true, true
	now := time.Date(2026, 9, 26, 20, 0, 0, 0, cstLocation)
	day, target := customerHealthSourceRange(now)
	if err := m.storeDB.Create(&CustomerHealthSourceCursor{ID: 1, DayTs: day, ThroughTs: target, SemanticsVersion: customerHealthStabilityPolicyVersion}).Error; err != nil {
		t.Fatal(err)
	}
	for _, seconds := range []int64{86400, 3 * 86400, 7 * 86400} {
		coverage := m.modelStatisticsRoutedCoverage(context.Background(), now.Unix()-seconds, now.Unix(), now)
		if coverage.Status != modelCoverageIncomplete || coverage.RequestsComplete || coverage.TTFTComplete {
			t.Fatalf("invented history proof: %+v", coverage)
		}
	}
	coverage := m.modelStatisticsRoutedCoverage(context.Background(), day, now.Unix(), now)
	if coverage.Status != modelCoveragePending {
		t.Fatalf("current day tail not recognized: %+v", coverage)
	}
}

func TestModelStatisticsIndependentHistoricalCoveragePublishesFRT(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	// The independent source always writes user-minute facts even when the
	// unrelated capacity-planning feature is disabled (8204's actual setup).
	m.cfg.CustomerHealthSourceEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
	m.cfg.RetentionDays = 7
	now := time.Date(2026, 9, 30, 12, 0, 0, 0, cstLocation)
	day, target := customerHealthSourceRange(now)
	for i := 0; i <= 7; i++ {
		start := day - int64(i)*86400
		state := CustomerHealthSourceCursor{
			ID: 1, DayTs: start, ThroughTs: min(start+86400, target),
			SemanticsVersion:     customerHealthStabilityPolicyVersion,
			TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
			TTFTCoverageFromTs:   start, TTFTCoverageThroughTs: min(start+86400, target),
		}
		if err := m.storeDB.Transaction(func(tx *gorm.DB) error {
			return m.persistCustomerHealthDayCoverageTx(tx, state)
		}); err != nil {
			t.Fatal(err)
		}
		if i == 0 {
			if err := m.storeDB.Create(&state).Error; err != nil {
				t.Fatal(err)
			}
		}
	}
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: day - 8*86400, ThroughTs: target,
		SemanticsVersion: cloudWatchPreRouteVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CapacityUserMinuteSample{
		BucketTs: target - 60, UserID: 1, ChannelID: 3, ModelName: "stream-model", Grp: "g",
		Success: 1, Ttft500: 1, TtftObserved: 1, TtftMaxMs: 400,
	}).Error; err != nil {
		t.Fatal(err)
	}
	for _, window := range []string{"24h", "3d", "7d"} {
		report, err := m.buildModelStatisticsReport(context.Background(), window, now)
		if err != nil {
			t.Fatal(err)
		}
		if !report.Source.RequestsComplete || !report.Source.TTFTComplete || len(report.Models) != 1 {
			t.Fatalf("independent %s did not recognize certified historical days: %+v", window, report.Source)
		}
	}
	// A current version and a signed range cannot certify corrupt FRT values.
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", target-60).
		Update("ttft_max_ms", 600).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Source.RequestsComplete || report.Source.TTFTComplete {
		t.Fatalf("invalid highest FRT bucket must invalidate FRT only: %+v", report.Source)
	}
}

func TestModelStatisticsHistoricalOldRowsKeepTTFTCoverageIncomplete(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CapacityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
	m.cfg.RetentionDays = 7
	now := time.Date(2026, 9, 26, 20, 0, 0, 0, cstLocation)
	to := metricFinalizeTarget(now.Unix())
	from := to - 24*3600
	if err := m.storeDB.Create(&MetricFinalizeState{
		ID: 1, SemanticsVersion: stabilityTrafficClassificationVersion,
		CoverageFromTs: from - 60, NextTs: to,
	}).Error; err != nil {
		t.Fatal(err)
	}
	_, rejectTarget := cloudWatchPreRouteRange(now, m.cfg.CloudWatchPreRouteLookbackHours)
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, SemanticsVersion: cloudWatchPreRouteVersion,
		CoverageFromTs: from - 60, ThroughTs: to, TargetThroughTs: rejectTarget,
	}).Error; err != nil {
		t.Fatal(err)
	}
	row := &CapacityUserMinuteSample{
		BucketTs: to - 60, UserID: 7, ChannelID: 3, ModelName: "old", Grp: "g",
		TrafficClassVersion: stabilityTrafficClassificationVersion, Success: 1,
		Ttft5k: 1, TtftObserved: 1, TtftMaxMs: 4000,
	}
	if err := m.storeDB.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricSample{BucketTs: to - 60, ChannelID: 3, ModelName: "old", Grp: "g", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	// Simulate a pre-TTFT schema row: request facts are complete, but the
	// exact TTFT projection has never been sourced for this historical row.
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", to-60).Update("ttft_semantics_version", 0).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if !report.Source.RequestsComplete || report.Source.TTFTComplete || report.Source.FactsComplete {
		t.Fatalf("旧行不能伪装成 TTFT 完整: %+v", report.Source)
	}
	if report.Source.RoutedCoverage.TTFTComplete || report.Source.TTFTCoverage.Status == modelCoverageComplete {
		t.Fatalf("旧行 TTFT 覆盖必须明确不完整: %+v", report.Source)
	}
	if len(report.Models) != 1 || report.Models[0].TTFTObserved != 1 {
		t.Fatalf("应保留已知 TTFT 样本但不把缺失当快速: %+v", report.Models)
	}
}
