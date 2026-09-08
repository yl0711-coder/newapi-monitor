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
