package monitor

import (
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"
)

func TestFinanceCutoffRejectsOldTailSnapshotWithoutDroppingHistoricalGaps(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "cutoff-cache.example")
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	m.cfg.FinanceReportSnapshotReadEnabled = true
	m.cfg.FinanceFastSnapshotEnabled = true
	now := time.Now()
	end := finalizedStabilityHourTo(now.Unix())
	request := financeReportRequest{from: time.Unix(end-7200, 0), to: time.Unix(end, 0),
		configurationHash: "cutoff-test", sourceFingerprint: "unchanged-facts"}
	for _, tc := range []struct {
		name       string
		coverageTo int64
		complete   bool
		want       bool
	}{
		{"old_tail_claimed_complete", end - 3600, true, false},
		{"old_tail_missing_internal", end - 3600, false, false},
		{"closed_exact", end, true, true},
		{"closed_with_real_gap", end, false, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			payload, err := json.Marshal(financeOperatingReport{Enabled: true, From: request.from.Unix(), To: end,
				GeneratedAt: now.Unix(), UserCoverage: StabilityDataCoverage{FromTs: request.from.Unix(), ToTs: tc.coverageTo, Complete: tc.complete}})
			if err != nil {
				t.Fatal(err)
			}
			if err := m.persistFinanceReportSnapshotShadow(request.logicalKey(), request.sourceFingerprint, payload, now); err != nil {
				t.Fatal(err)
			}
			path, err := financeReportSnapshotPath(m.cfg.StorePath, request.logicalKey())
			if err != nil {
				t.Fatal(err)
			}
			before, err := os.ReadFile(path)
			if err != nil {
				t.Fatal(err)
			}
			if _, _, _, ok, err := m.loadFinanceReportSnapshot(request, now); err != nil || ok != tc.want {
				t.Fatalf("persistent snapshot: ok=%v want=%v err=%v", ok, tc.want, err)
			}
			m.rememberFinanceReport(request, payload, now)
			if _, _, ok := m.financeFastSnapshotPayload(request, now); ok != tc.want {
				t.Fatalf("fast in-memory snapshot: ok=%v want=%v", ok, tc.want)
			}
			after, err := os.ReadFile(path)
			if err != nil || string(after) != string(before) {
				t.Fatal("cache validation mutated or deleted the persisted snapshot")
			}
		})
	}
}

func TestFinanceCutoffPeriodCacheRechecksTimeBoundaryWithoutFactChanges(t *testing.T) {
	m := newFinanceReportTestMonitor(t, "period-cutoff.example")
	m.cfg.FinanceReportSnapshotReadEnabled = true
	start := int64(1777516800 / 3600 * 3600)
	scope := stabilityScope{FromTs: start, ToTs: start + 3600}
	if err := m.storeDB.Create(&StabilityHourIngestState{HourTs: scope.FromTs, Status: "complete", Requests: 1,
		TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	build := func(now int64) (financePeriodComponent, bool, error) {
		return m.buildFinancePeriodComponent(context.Background(), scope, now, "cutoff-config", nil,
			channelFinanceSnapshot{}, financeInternalTestCostEvidence{}, financeConfiguredInternalEvidence{Complete: true}, nil)
	}
	old, hit, err := build(scope.ToTs)
	if err != nil || hit || old.UserCoverage.ToTs != scope.FromTs {
		t.Fatalf("old tail fixture: %+v %v %v", old.UserCoverage, hit, err)
	}
	// Seed the same old component on disk, without a background writer, to
	// verify restart compatibility as well as the in-memory fast path.
	relevant, err := m.financeRelevantUpstreamAccounts(context.Background(), scope, nil)
	if err != nil {
		t.Fatal(err)
	}
	inputHash, err := financePeriodInputFingerprint(nil, channelFinanceSnapshot{}, financeConfiguredInternalEvidence{Complete: true}, financeInternalTestCostEvidence{}, nil, financeBillAccountBoundaries(relevant))
	if err != nil {
		t.Fatal(err)
	}
	sourceHash, err := m.financeReportPeriodSourceFingerprint(context.Background(), scope.FromTs, scope.ToTs)
	if err != nil {
		t.Fatal(err)
	}
	logicalKey := financePeriodLogicalKey(scope, "cutoff-config:"+inputHash)
	payload, err := json.Marshal(old)
	if err != nil {
		t.Fatal(err)
	}
	m.cfg.FinanceReportSnapshotShadowEnabled = true
	if err := m.persistFinancePeriodSnapshot(logicalKey, sourceHash, payload); err != nil {
		t.Fatal(err)
	}
	m.cfg.FinanceReportSnapshotShadowEnabled = false
	// No source/configuration change: only the safe coverage horizon advanced.
	current, hit, err := build(scope.ToTs + 3600)
	if err != nil || hit || current.UserCoverage.ToTs != scope.ToTs || current.UserCoverage.ExpectedHours != 1 {
		t.Fatalf("old tail component reused after finalization: %+v hit=%v err=%v", current.UserCoverage, hit, err)
	}
	m.financePeriodCache = newBoundedByteCache(financePeriodCacheMaxEntries, financePeriodCacheMaxBytes)
	current, hit, err = build(scope.ToTs + 3600)
	if err != nil || hit || current.UserCoverage.ToTs != scope.ToTs || current.UserCoverage.ExpectedHours != 1 {
		t.Fatalf("persisted old tail component reused after restart: %+v hit=%v err=%v", current.UserCoverage, hit, err)
	}
	_, hit, err = build(scope.ToTs + 7200)
	if err != nil || !hit {
		t.Fatalf("unchanged closed history should remain cached: hit=%v err=%v", hit, err)
	}
}
