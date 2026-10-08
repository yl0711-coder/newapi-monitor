package monitor

import (
	"context"
	"testing"
)

func TestCapacityCoverageRequiresSignedIntervalAndUserReconciliation(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	metric := MetricSample{BucketTs: 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 2, Tokens: 100}
	if err := m.storeDB.Create(&metric).Error; err != nil {
		t.Fatal(err)
	}
	if m.capacityWindowComplete(ctx, 60, 180, false) {
		t.Fatal("a sample is not coverage")
	}
	state := MetricFinalizeState{ID: 1, CoverageFromTs: 60, NextTs: 180, SemanticsVersion: stabilityTrafficClassificationVersion}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	if !m.capacityWindowComplete(ctx, 60, 180, false) || m.capacityWindowComplete(ctx, 60, 240, false) {
		t.Fatal("signed bounds not respected")
	}
	if m.capacityWindowComplete(ctx, 60, 180, true) {
		t.Fatal("missing user projection accepted")
	}
	user := CapacityUserMinuteSample{BucketTs: 120, UserID: 7, ChannelID: 1, ModelName: "m", Grp: "g", Success: 2, Tokens: 100}
	if err := m.storeDB.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if !m.capacityWindowComplete(ctx, 60, 180, true) {
		t.Fatal("matching user projection rejected")
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = ?", 7).Update("tokens", 101).Error; err != nil {
		t.Fatal(err)
	}
	if m.capacityWindowComplete(ctx, 60, 180, true) {
		t.Fatal("token discrepancy accepted")
	}
	if err := m.storeDB.Model(&MetricFinalizeState{}).Where("id = ?", 1).Update("semantics_version", 0).Error; err != nil {
		t.Fatal(err)
	}
	if m.capacityWindowComplete(ctx, 60, 180, false) {
		t.Fatal("obsolete semantics accepted")
	}
}

func TestCapacityCoverageControlsZerosAndAverages(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	if err := m.storeDB.Create(&MetricSample{BucketTs: 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 6, Tokens: 600}).Error; err != nil {
		t.Fatal(err)
	}
	read := func() capacityReport {
		t.Helper()
		r, err := m.buildCapacityReport(ctx, 60, 240, 60, 1, "", "", 5000)
		if err != nil {
			t.Fatal(err)
		}
		return r
	}
	r := read()
	if r.Series[0].BusinessRPM != nil || r.Series[2].BusinessRPM != nil || r.Summary.CurrentAt != 120 || r.Breakdowns["channels"][0].AverageRPM != nil {
		t.Fatalf("partial range fabricated zeros or averages: %+v", r)
	}
	if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: 60, NextTs: 240, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	r = read()
	if r.Series[0].BusinessRPM == nil || *r.Series[0].BusinessRPM != 0 || r.Summary.CurrentAt != 180 ||
		r.Summary.CurrentBusinessRPM == nil || *r.Summary.CurrentBusinessRPM != 0 || r.Breakdowns["channels"][0].AverageRPM == nil || *r.Breakdowns["channels"][0].AverageRPM != 2 {
		t.Fatalf("complete range must include confirmed idle minutes: %+v", r)
	}
}

func TestCapacityTTFTCoverageChecksExactCountersSeparately(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	metric := MetricSample{BucketTs: 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1,
		Ttft5k: 1, TtftObserved: 1, TtftMaxMs: 5000, TtftOver3s: 1}
	user := CapacityUserMinuteSample{BucketTs: 120, UserID: 7, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}
	if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: 60, NextTs: 180, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&metric).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if !m.capacityWindowComplete(ctx, 60, 180, true) {
		t.Fatal("request facts should remain complete when only TTFT projection differs")
	}
	if capacityTTFTProjectionComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("TTFT projection mismatch was incorrectly certified")
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = ?", 7).Updates(map[string]any{
		"ttft_5k": 1, "ttft_observed": 1, "ttft_max_ms": 5000, "ttft_over_3s": 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if !capacityTTFTProjectionComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("matching exact TTFT projection was rejected")
	}
}

func TestCapacityTTFTCoverageChecksMaxAndHighestBucket(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	state := MetricFinalizeState{ID: 1, CoverageFromTs: 60, NextTs: 180, SemanticsVersion: stabilityTrafficClassificationVersion}
	if err := m.storeDB.Create(&state).Error; err != nil {
		t.Fatal(err)
	}
	metric := MetricSample{BucketTs: 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1,
		Ttft5k: 1, TtftObserved: 1, TtftMaxMs: 5000, TtftOver3s: 1}
	user := CapacityUserMinuteSample{BucketTs: 120, UserID: 7, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1,
		Ttft5k: 1, TtftObserved: 1, TtftMaxMs: 5000, TtftOver3s: 1}
	if err := m.storeDB.Create(&metric).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&user).Error; err != nil {
		t.Fatal(err)
	}
	if !capacityTTFTProjectionComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("matching max/highest bucket projection rejected")
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = ?", 7).Update("ttft_max_ms", 4500).Error; err != nil {
		t.Fatal(err)
	}
	if capacityTTFTProjectionComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("max discrepancy was incorrectly certified")
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = ?", 7).Updates(map[string]any{
		"ttft_max_ms": 5000, "ttft_5k": 0, "ttft_2k": 1,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if capacityTTFTProjectionComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("highest non-zero histogram bucket discrepancy was incorrectly certified")
	}
}

func TestCapacityTTFTRowsRejectInconsistentMaxAndHistogram(t *testing.T) {
	m := newTestMonitor(t)
	ctx := context.Background()
	row := MetricSample{BucketTs: 120, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1,
		Ttft5k: 1, TtftObserved: 1, TtftMaxMs: 5001}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
	if capacityTTFTRowsComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("max outside highest non-zero bucket was incorrectly accepted")
	}
	if err := m.storeDB.Model(&MetricSample{}).Where("bucket_ts = ?", 120).Updates(map[string]any{
		"ttft_5k": 0, "ttft_max_ms": 0, "ttft_observed": 0,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if !capacityTTFTRowsComplete(ctx, m.storeDB, 60, 180) {
		t.Fatal("a zero FRT observation row should be valid when all counters are zero")
	}
}

func TestCapacitySeriesGapsKeepPartialBucketDuration(t *testing.T) {
	points := capacitySeriesWithGaps(nil, 120, 660, 300)
	if len(points) != 3 || points[0].DurationMinutes != 3 || points[1].DurationMinutes != 5 || points[2].DurationMinutes != 1 {
		t.Fatalf("wrong partial bucket bounds: %+v", points)
	}
	for _, p := range points {
		if p.BusinessRPM != nil || p.TPM != nil {
			t.Fatal("unknown gap is not zero")
		}
	}
}
