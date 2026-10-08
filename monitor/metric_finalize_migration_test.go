package monitor

import (
	"context"
	"errors"
	"strings"
	"testing"

	"gorm.io/gorm"
)

func TestRecordMetricFRTReplayFailureWrapsBothErrors(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	cause := errors.New("source replay failed")
	persistErr := errors.New("failure state could not be saved")
	if err := m.storeDB.Callback().Update().Before("gorm:update").Register("test:frt_failure_save", func(tx *gorm.DB) {
		_ = tx.AddError(persistErr)
	}); err != nil {
		t.Fatal(err)
	}
	err := m.recordMetricFRTReplayFailure(1, 2_000_000_000, cause)
	if !errors.Is(err, cause) || !errors.Is(err, persistErr) {
		t.Fatalf("both replay and persistence errors must remain unwrap-able: %v", err)
	}
	if !strings.Contains(err.Error(), cause.Error()) || !strings.Contains(err.Error(), persistErr.Error()) {
		t.Fatalf("error message must retain both causes: %v", err)
	}
}

func TestMetricFinalizeRewindsWhenStateRecreatedWithLegacyTTFTRows(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.RetentionDays = 7
	now := int64(2_000_000_000)
	target := metricFinalizeTarget(now)

	// BeforeCreate applies the current version to new rows, so explicitly
	// downgrade the persisted row to reproduce a pre-TTFT database.
	row := &CapacityUserMinuteSample{
		BucketTs: target - 60, UserID: 7, ChannelID: 3,
		ModelName: "legacy", Grp: "g", Success: 1,
		TrafficClassVersion: stabilityTrafficClassificationVersion,
	}
	if err := m.storeDB.Create(row).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).
		Where("bucket_ts = ?", row.BucketTs).
		Update("ttft_semantics_version", 0).Error; err != nil {
		t.Fatal(err)
	}

	// Simulate MetricFinalizeState being recreated with current defaults.  The
	// cursor alone must not certify the old row as TTFT-complete.
	state := &MetricFinalizeState{
		ID: 1, NextTs: target, TargetThroughTs: target,
		CoverageFromTs:        target - int64(m.cfg.RetentionDays)*86400,
		SemanticsVersion:      stabilityTrafficClassificationVersion,
		TTFTSemanticsVersion:  ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs:    target - int64(m.cfg.RetentionDays)*86400,
		TTFTCoverageThroughTs: target, Status: "caught_up",
	}
	if err := m.storeDB.Create(state).Error; err != nil {
		t.Fatal(err)
	}

	got, err := m.loadOrExtendMetricFinalizeState(now)
	if err != nil {
		t.Fatal(err)
	}
	wantStart := (target - int64(m.cfg.RetentionDays)*86400) / 60 * 60
	if got.NextTs != target || got.CoverageFromTs != state.CoverageFromTs {
		t.Fatalf("FRT replay must preserve proven request coverage: got next=%d from=%d want next=%d from=%d",
			got.NextTs, got.CoverageFromTs, target, state.CoverageFromTs)
	}
	if got.TTFTCoverageFromTs != wantStart || got.TTFTCoverageThroughTs != wantStart {
		t.Fatalf("legacy FRT coverage was not independently invalidated: %+v", got)
	}
	metricCalls, tokenCalls := 0, 0
	if err := m.runMetricFinalizeTurnWith(context.Background(), now,
		func(_ context.Context, from, to int64) (int, error) {
			metricCalls++
			if from != wantStart || to != wantStart+metricFinalizeSliceSec {
				t.Fatalf("FRT replay slice=[%d,%d), want=[%d,%d)", from, to, wantStart, wantStart+metricFinalizeSliceSec)
			}
			return 0, nil
		},
		func(context.Context, int64, int64) error { tokenCalls++; return nil }); err != nil {
		t.Fatal(err)
	}
	if metricCalls != 1 || tokenCalls != 0 {
		t.Fatalf("FRT replay should reproject only model metrics: metric=%d token=%d", metricCalls, tokenCalls)
	}
	if err := m.storeDB.First(&got, "id = ?", 1).Error; err != nil {
		t.Fatal(err)
	}
	if got.NextTs != target || got.CoverageFromTs != state.CoverageFromTs || got.TTFTCoverageThroughTs != wantStart+metricFinalizeSliceSec {
		t.Fatalf("FRT replay moved request cursor or failed to advance its own: %+v", got)
	}
	// The legacy row is still ahead of the FRT replay prefix. Loading the
	// cursor next turn must resume, not restart from the beginning forever.
	got, err = m.loadOrExtendMetricFinalizeState(now)
	if err != nil {
		t.Fatal(err)
	}
	if got.TTFTCoverageThroughTs != wantStart+metricFinalizeSliceSec {
		t.Fatalf("FRT replay progress reset while legacy rows remain ahead: %+v", got)
	}
}
