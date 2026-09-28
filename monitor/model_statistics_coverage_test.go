package monitor

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestModelStatisticsSeparatesLiveTailFromCoverageGaps(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CapacityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
	m.cfg.RetentionDays = 7
	now := time.Date(2026, 9, 26, 20, 0, 0, 0, cstLocation)
	to := now.Unix()
	metricTarget := metricFinalizeTarget(to)
	_, rejectTarget := cloudWatchPreRouteRange(now, 168)
	metric := MetricFinalizeState{ID: 1, SemanticsVersion: stabilityTrafficClassificationVersion, CoverageFromTs: to - 8*86400, NextTs: metricTarget}
	reject := CloudWatchPreRouteCursor{ID: cloudWatchPreRouteCursorID, SemanticsVersion: cloudWatchPreRouteVersion, CoverageFromTs: to - 8*86400, ThroughTs: rejectTarget}
	for _, value := range []any{&metric, &reject,
		&CapacityUserMinuteSample{BucketTs: to - 60, UserID: 1, ChannelID: 1, ModelName: "test", Grp: "g", Success: 3},
		&MetricSample{BucketTs: to - 60, ChannelID: 1, ModelName: "test", Grp: "g", Success: 3},
		&RejectionSample{BucketTs: to - 60, Node: cloudWatchPreRouteNode, UserID: 1, Model: "test", Grp: "g", Reason: "route_no_channel", Count: 2},
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
		if report.Source.CoverageStatus != modelCoveragePending || report.Source.FactsComplete {
			t.Fatalf("healthy tail mislabeled: %+v", report.Source)
		}
		if report.Source.RoutedCoverage.ThroughTs != metricTarget || report.Source.UnavailableCoverage.ThroughTs != rejectTarget {
			t.Fatalf("wrong watermarks: %+v", report.Source)
		}
		if report.ToTs != to || len(report.Models) != 1 || report.Models[0].Requests != 5 {
			t.Fatalf("live requests hidden or window shifted: %+v", report)
		}
	}
	check := func(want string) {
		t.Helper()
		report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
		if err != nil {
			t.Fatal(err)
		}
		if report.Source.CoverageStatus != want || report.Source.FactsComplete != (want == modelCoverageComplete) {
			t.Fatalf("want %s: %+v", want, report.Source)
		}
	}
	metric.NextTs = metricTarget - 60
	if err := m.storeDB.Save(&metric).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageIncomplete)
	metric.NextTs = to
	if err := m.storeDB.Save(&metric).Error; err != nil {
		t.Fatal(err)
	}
	reject.ThroughTs = rejectTarget - 60
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageIncomplete) // Routed completeness cannot certify rejection coverage.
	reject.ThroughTs = to
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageComplete)
	reject.SemanticsVersion = cloudWatchPreRouteVersion - 1
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageIncomplete)
	reject.SemanticsVersion = cloudWatchPreRouteVersion
	if err := m.storeDB.Save(&reject).Error; err != nil {
		t.Fatal(err)
	}
	// Missing projected users inside a certified interval is a real gap, not a live tail.
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = ?", 1).Update("success", 1).Error; err != nil {
		t.Fatal(err)
	}
	check(modelCoverageIncomplete)
	m.cfg.CloudWatchPreRouteEnabled = false
	check(modelCoverageIncomplete)
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
		if coverage.Status != modelCoverageIncomplete || !strings.Contains(coverage.Note, "仅证明当前日") {
			t.Fatalf("invented history proof: %+v", coverage)
		}
	}
	coverage := m.modelStatisticsRoutedCoverage(context.Background(), day, now.Unix(), now)
	if coverage.Status != modelCoveragePending {
		t.Fatalf("current day tail not recognized: %+v", coverage)
	}
}
