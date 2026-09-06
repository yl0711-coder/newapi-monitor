package monitor

import (
	"context"
	"testing"
)

func TestStabilityTimelineCoverageDistinguishesZeroMissingAndLegacy(t *testing.T) {
	m := newTestMonitor(t)
	const start = int64(1_800_000_000 / 3600 * 3600)
	states := []StabilityHourIngestState{
		{HourTs: start, Status: "complete", Requests: 0, TrafficClassVersion: stabilityTrafficClassificationVersion},
		{HourTs: start + 3600, Status: "complete", Requests: 10, TrafficClassVersion: stabilityTrafficClassificationVersion - 1},
		// start+7200 deliberately absent.
		{HourTs: start + 10800, Status: "running", TrafficClassVersion: stabilityTrafficClassificationVersion},
	}
	if err := m.storeDB.Create(&states).Error; err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: start, ToTs: start + 4*3600}
	got, err := m.stabilityTimelineCoverage(context.Background(), scope, 3600, start+10*3600)
	if err != nil {
		t.Fatal(err)
	}
	want := map[int64]string{
		start: "complete", start + 3600: "legacy", start + 7200: "missing", start + 10800: "provisional",
	}
	for ts, state := range want {
		if got[ts] != state {
			t.Fatalf("hour %d: got %q want %q (all=%v)", ts, got[ts], state, got)
		}
	}

	points := buildTimeline([]int64{start, start + 7200}, map[int64]stabilityCounts{}, got)
	if points[0].Coverage != "complete_zero" || points[1].Coverage != "missing" {
		t.Fatalf("zero traffic and missing data collapsed together: %+v", points)
	}
}

func TestWorseTimelineCoverageFailsClosed(t *testing.T) {
	if got := worseTimelineCoverage("complete", "legacy"); got != "legacy" {
		t.Fatalf("got %q", got)
	}
	if got := worseTimelineCoverage("provisional", "missing"); got != "missing" {
		t.Fatalf("got %q", got)
	}
}

func TestStabilityTimelineRejectsContradictorySignedZeroHour(t *testing.T) {
	m := newTestMonitor(t)
	const start = int64(1_800_000_000 / 3600 * 3600)
	if err := m.storeDB.Create(&StabilityHourIngestState{
		HourTs: start, Status: "complete", Requests: 0, TrafficClassVersion: stabilityTrafficClassificationVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.upsertSamples([]MetricSample{{BucketTs: start, ChannelID: 1, ModelName: "m", Grp: "g", Success: 1}}); err != nil {
		t.Fatal(err)
	}
	got, err := m.stabilityTimelineCoverage(context.Background(), stabilityScope{FromTs: start, ToTs: start + 3600}, 3600, start+10*3600)
	if err != nil {
		t.Fatal(err)
	}
	if got[start] != "missing" {
		t.Fatalf("contradictory signed-zero hour must match global coverage fail-closed semantics: %v", got)
	}
}
