package monitor

import "testing"

func TestModelPercentilesStayWithinObservedMaximum(t *testing.T) {
	for _, edges := range [][]int{latEdges, ttftEdges} {
		for _, maximum := range []int{0, 1, 3, 48, 120, 450, 750, 1750, 4500, 12000} {
			hist := make([]int64, len(edges)+1)
			bucket := 0
			for bucket < len(edges) && maximum > edges[bucket] {
				bucket++
			}
			hist[bucket] = 100
			previous := float64(0)
			for _, p := range []float64{50, 95, 99, 100} {
				got := percentile(hist, edges, maximum, p)
				if got < previous || got > float64(maximum) {
					t.Errorf("edges=%v max=%d p=%v got=%v previous=%v", edges, maximum, p, got, previous)
				}
				previous = got
			}
		}
	}
}

func TestModelErrorHealthIsNotEscalatedByDeliveryAnomalies(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	const now = int64(1_800_000_000)
	var samples []MetricSample
	for i := int64(1); i <= 3; i++ {
		samples = append(samples, MetricSample{BucketTs: now - i*60, ChannelID: 1, Grp: "g", ModelName: "m", Success: 100, Anomaly: 1})
	}
	if err := m.upsertSamples(samples); err != nil {
		t.Fatal(err)
	}
	snapshot, err := m.getSnapshotView(60, now, true)
	if err != nil {
		t.Fatal(err)
	}
	for _, rows := range [][]Row{snapshot.ByGroup, snapshot.ByChannel, snapshot.ByModel} {
		if len(rows) != 1 {
			t.Fatalf("unexpected dimension: %+v", rows)
		}
		r := rows[0]
		if r.Health != "warn" || r.ErrorHealth != "good" || !r.AnomalyBurst || r.Failed != 0 || r.Anomaly != 3 {
			t.Fatalf("error and delivery assessments mixed: %+v", r)
		}
	}
	var errorRow Row
	aggRow{Success: 80, Failed: 20}.fill(&errorRow, 60)
	if errorRow.ErrorHealth != "bad" || errorRow.Health != "bad" {
		t.Fatalf("real errors hidden: %+v", errorRow)
	}
}
