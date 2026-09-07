package monitor

import (
	"math"
	"testing"
)

func TestModelHistoryKeepsRecordedTrafficAcrossRoutingChanges(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	const now = int64(1_800_000_000)
	end := metricFinalizeTarget(now)
	for _, row := range []ChannelSnap{
		{ID: 1, Status: 1, EnabledSince: end - 3600},
		{ID: 2, Status: 1, EnabledSince: end + 60}, // re-enabled after these requests
		{ID: 3, Status: 2},                         // currently disabled
		{ID: 4, Status: 2, DeletedAt: end},         // deleted, history retained
	} {
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&SelectablePair{Grp: "g", Model: "m"}).Error; err != nil {
		t.Fatal(err)
	}
	rows := []MetricSample{
		{ChannelID: 1, Grp: "g", ModelName: "m", Success: 5},
		{ChannelID: 2, Grp: "g", ModelName: "m", Success: 7},
		{ChannelID: 3, Grp: "g", ModelName: "m", Failed: 3},
		{ChannelID: 4, Grp: "g", ModelName: "m", Anomaly: 2},
		{ChannelID: 5, Grp: "g", ModelName: "m", Failed: 4}, // no channel snapshot
		{ChannelID: 1, Grp: "removed", ModelName: "old-model", Success: 11},
	}
	for i := range rows {
		rows[i].BucketTs = end - 120
		rows[i].Tokens = int64(i+1) * 100
		rows[i].Quota = int64(i+1) * 1000
	}
	if err := m.upsertSamples(rows); err != nil {
		t.Fatal(err)
	}
	// Old semantics cannot leak into either dashboard view.
	if err := m.storeDB.Create(&MetricSample{BucketTs: end - 180, ChannelID: 1, Grp: "g", ModelName: "m", Success: 999, TrafficClassVersion: stabilityTrafficClassificationVersion - 1}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: end - 3600, NextTs: end, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
		t.Fatal(err)
	}
	// Both call orders must preserve separate dashboard/operational cache entries.
	for _, operationalFirst := range []bool{true, false} {
		m.snapCache = nil
		if operationalFirst {
			if _, err := m.GetSnapshot(60, now); err != nil {
				t.Fatal(err)
			}
		}
		for _, observed := range []bool{false, true} {
			window := 60
			if observed {
				// Finalization lags by one hour; include that historical fixture
				// in the wall-clock-based observation window as well.
				window = 120
			}
			s, err := m.getSnapshotView(window, now, observed)
			if err != nil {
				t.Fatal(err)
			}
			if s.Summary.Total != 32 || s.Summary.Tokens != 2100 || math.Abs(s.Summary.CostUSD-0.042) > 1e-9 || s.DataComplete == observed {
				t.Fatalf("recorded facts changed by routing or view: %+v", s.Summary)
			}
			assertModelSnapshotDimensions(t, s)
		}
		operational, err := m.GetSnapshot(60, now)
		if err != nil {
			t.Fatal(err)
		}
		if operational.Summary.Total != 9 {
			t.Fatalf("dashboard scope leaked into operational policy: total=%d", operational.Summary.Total)
		}
	}
}

func assertModelSnapshotDimensions(t *testing.T, s *Snapshot) {
	t.Helper()
	for _, rows := range [][]Row{s.ByGroup, s.ByModel, s.ByChannel} {
		var total, tokens, sparkTotal int64
		var cost float64
		for _, r := range rows {
			total += r.Total
			tokens += r.Tokens
			cost += r.CostUSD
			for _, p := range r.Spark {
				sparkTotal += p.Success + p.Anomaly + p.Failed
			}
		}
		if total != s.Summary.Total || tokens != s.Summary.Tokens || sparkTotal != total || math.Abs(cost-s.Summary.CostUSD) > 1e-9 {
			t.Fatalf("dimension/spark disagrees with summary: requests=%d tokens=%d cost=%f spark=%d", total, tokens, cost, sparkTotal)
		}
	}
	var trendTotal int64
	for _, p := range s.Trend {
		trendTotal += p.Success + p.Anomaly + p.Failed
	}
	if trendTotal != s.Summary.Total {
		t.Fatalf("trend=%d summary=%d", trendTotal, s.Summary.Total)
	}
}

func TestDelayedFinalizedSnapshotCannotTriggerCurrentRequestAlerts(t *testing.T) {
	for _, delayed := range []bool{false, true} {
		m := newTestMonitor(t)
		defer m.Close()
		const now = int64(1_800_000_000)
		end := metricFinalizeTarget(now)
		if delayed {
			end -= 22 * 3600
		}
		if err := m.storeDB.Create(&MetricFinalizeState{ID: 1, CoverageFromTs: end - 3600, NextTs: end, SemanticsVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
			t.Fatal(err)
		}
		if err := m.upsertSamples([]MetricSample{{BucketTs: end - 60, ChannelID: 1, ModelName: "m", Grp: "g", Failed: 10}}); err != nil {
			t.Fatal(err)
		}
		// Category email is off: firing only writes a local alert row; no SMTP
		// connection is made, even in the fresh positive-control case.
		if err := m.saveAlertConfig(AlertConfig{ID: 1, Enabled: true, SMTPHost: "unused.invalid", Recipients: "test@example.invalid", EvalWindowMin: 60, ErrBurstCount: 1, ModelAlertsEnabled: false}); err != nil {
			t.Fatal(err)
		}
		m.evaluateAlerts(now)
		var count int64
		if err := m.storeDB.Model(&AlertLog{}).Count(&count).Error; err != nil {
			t.Fatal(err)
		}
		if delayed && count != 0 || !delayed && count == 0 {
			t.Fatalf("delayed=%v alert rows=%d", delayed, count)
		}
	}
}
