package monitor

import (
	"context"
	"testing"
	"time"
)

func TestRejectionCoverageCannotCertifyPrunedHistory(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays, m.cfg.CloudWatchPreRouteEnabled = 7, true
	now := time.Date(2026, 9, 30, 12, 34, 45, 0, time.UTC)
	cutoff := metricMinuteRetentionCutoff(now.Unix(), 7)
	_, target := cloudWatchPreRouteRange(now, 168)
	oldFrom := cutoff - 86400
	m.cloudWatchPreRouteFrom.Store(oldFrom)
	m.cloudWatchPreRouteThrough.Store(target)
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: oldFrom, ThroughTs: target,
		SemanticsVersion: cloudWatchPreRouteVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: cutoff - 60, ToTs: cutoff + 60}
	if coverage := m.alertsCoverage(scope, now); coverage.Complete {
		t.Fatalf("collector cursor must not certify deleted rejection facts: %+v", coverage)
	}
	if coverage := m.modelStatisticsRejectionCoverage(context.Background(), scope.FromTs, scope.ToTs, now); coverage.RequestsComplete {
		t.Fatalf("model statistics certified expired rejection facts: %+v", coverage)
	}
}

func TestMinuteRetentionPreservesFullFinalizedSevenDayWindow(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	now := time.Date(2026, 9, 30, 12, 34, 45, 0, time.UTC).Unix()
	target := metricFinalizeTarget(now)
	cutoff := metricMinuteRetentionCutoff(now, 7)
	if cutoff != target-7*86400 || cutoff%60 != 0 {
		t.Fatalf("retention must cover the finalized seven-day window: cutoff=%d target=%d", cutoff, target)
	}
	if err := m.storeDB.Create(&[]MetricSample{
		{BucketTs: cutoff - 60, ChannelID: 1, ModelName: "old", Success: 1},
		{BucketTs: cutoff, ChannelID: 1, ModelName: "first-retained-minute", Success: 2},
	}).Error; err != nil {
		t.Fatal(err)
	}
	state := MetricFinalizeState{
		ID: 1, CoverageFromTs: cutoff - 3600, NextTs: target, TargetThroughTs: target,
		SemanticsVersion:     stabilityTrafficClassificationVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   cutoff - 3600, TTFTCoverageThroughTs: target,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	removed, err := m.pruneOlderThan(cutoff)
	if err != nil || removed != 1 {
		t.Fatalf("prune removed=%d err=%v", removed, err)
	}
	var rows []MetricSample
	if err := m.storeDB.Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].BucketTs != cutoff || rows[0].Success != 2 {
		t.Fatalf("retention deleted the first minute of the seven-day report: %+v", rows)
	}
	if complete, _, _ := m.metricWindowCoverage(cutoff, now); !complete {
		t.Fatal("retained finalized seven-day window lost its coverage")
	}
	if complete, _, _ := m.metricWindowCoverage(cutoff-60, now); complete {
		t.Fatal("deleted minutes still have a complete coverage certificate")
	}
}

func TestMinuteRetentionExpiredCoverageBecomesEmptyReplayRange(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	const cutoff = int64(1790000000 / 60 * 60)
	state := MetricFinalizeState{
		ID: 1, CoverageFromTs: cutoff - 7200, NextTs: cutoff - 3600,
		SemanticsVersion:     stabilityTrafficClassificationVersion,
		TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:   cutoff - 7200, TTFTCoverageThroughTs: cutoff - 3600,
	}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := m.pruneOlderThan(cutoff); err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.CoverageFromTs != cutoff || state.NextTs != cutoff ||
		state.TTFTCoverageFromTs != cutoff || state.TTFTCoverageThroughTs != cutoff {
		t.Fatalf("fully expired certificates must restart from an empty retained range: %+v", state)
	}
}

func TestMinuteRetentionRollsBackFactsAndCoverageTogether(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	const cutoff = int64(1790000000 / 60 * 60)
	if err := m.storeDB.Create(&MetricSample{BucketTs: cutoff - 60, ModelName: "keep-on-failure", Success: 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: cutoff - 3600, NextTs: cutoff + 3600,
		SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Migrator().DropTable(&TokenSample{}); err != nil {
		t.Fatal(err)
	}
	if _, err := m.pruneOlderThan(cutoff); err == nil {
		t.Fatal("expected failed token cleanup to abort retention transaction")
	}
	var count int64
	if err := m.storeDB.Model(&MetricSample{}).Count(&count).Error; err != nil || count != 1 {
		t.Fatalf("failed cleanup removed request facts: count=%d err=%v", count, err)
	}
	var state MetricFinalizeState
	if err := m.storeDB.First(&state, 1).Error; err != nil {
		t.Fatal(err)
	}
	if state.CoverageFromTs != cutoff-3600 {
		t.Fatalf("failed cleanup changed the coverage certificate: %+v", state)
	}
}
