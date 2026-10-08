package monitor

import (
	"context"
	"testing"
	"time"
)

func modelStatisticsWindowFixture(t *testing.T, independent bool, now time.Time, routedThrough, frtThrough, rejectedThrough int64) *Monitor {
	t.Helper()
	m := newTestMonitor(t)
	t.Cleanup(m.Close)
	m.cfg.RetentionDays = 10
	m.cfg.CapacityEnabled = !independent
	m.cfg.CustomerHealthSourceEnabled = independent
	m.cfg.CloudWatchPreRouteEnabled = true
	m.cfg.CloudWatchPreRouteLookbackHours = 240
	firstDay := customerHealthDayStart(now.Unix()) - 9*86400
	if independent {
		lastDay := customerHealthDayStart(routedThrough - 1)
		for day := firstDay; day <= lastDay; day += 86400 {
			row := CustomerHealthDayCoverage{
				DayTs: day, ThroughTs: min(day+86400, routedThrough),
				SemanticsVersion: customerHealthStabilityPolicyVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
				TTFTCoverageFromTs: day, TTFTCoverageThroughTs: min(day+86400, frtThrough),
			}
			if err := m.storeDB.Create(&row).Error; err != nil {
				t.Fatal(err)
			}
		}
		if err := m.storeDB.Create(&CustomerHealthSourceCursor{
			ID: 1, DayTs: lastDay, ThroughTs: routedThrough,
			SemanticsVersion: customerHealthStabilityPolicyVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
			TTFTCoverageFromTs: lastDay, TTFTCoverageThroughTs: frtThrough,
		}).Error; err != nil {
			t.Fatal(err)
		}
	} else if err := m.storeDB.Create(&MetricFinalizeState{
		ID: 1, CoverageFromTs: firstDay, NextTs: routedThrough,
		SemanticsVersion: stabilityTrafficClassificationVersion, TTFTSemanticsVersion: ttftCoverageSemanticsVersion,
		TTFTCoverageFromTs: firstDay, TTFTCoverageThroughTs: frtThrough,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&CloudWatchPreRouteCursor{
		ID: cloudWatchPreRouteCursorID, CoverageFromTs: firstDay, NextTs: rejectedThrough,
		ThroughTs: rejectedThrough, SemanticsVersion: cloudWatchPreRouteVersion,
	}).Error; err != nil {
		t.Fatal(err)
	}
	return m
}

func modelStatisticsWindowInsertFRT(t *testing.T, m *Monitor, bucket int64, model string) {
	t.Helper()
	if err := m.storeDB.Create(&CapacityUserMinuteSample{
		BucketTs: bucket, UserID: 1, ChannelID: 1, ModelName: model, Grp: "g", Success: 2,
		Ttft500: 1, Ttft5k: 1, TtftObserved: 2, TtftOver3s: 1, TtftMaxMs: 4000,
	}).Error; err != nil {
		t.Fatal(err)
	}
	if !m.cfg.CustomerHealthSourceEnabled {
		if err := m.storeDB.Create(&MetricSample{
			BucketTs: bucket, ChannelID: 1, ModelName: model, Grp: "g", Success: 2,
			Ttft500: 1, Ttft5k: 1, TtftObserved: 2, TtftOver3s: 1, TtftMaxMs: 4000,
		}).Error; err != nil {
			t.Fatal(err)
		}
	}
}

func TestModelStatisticsActualWindowUsesPersistedCommonTail(t *testing.T) {
	now := time.Date(2026, 9, 30, 15, 23, 37, 0, cstLocation)
	for _, tc := range []struct {
		name                 string
		independent          bool
		routedLag, frtLag    int64
		rejectionLag, endLag int64
	}{
		{name: "independent_one_minute_behind", independent: true, routedLag: 60, frtLag: 60, endLag: 60},
		{name: "cloudwatch_three_minutes_behind", independent: true, routedLag: 60, frtLag: 60, rejectionLag: 180, endLag: 180},
		{name: "standard_source_one_minute_behind", routedLag: 60, frtLag: 60, endLag: 60},
		{name: "frt_replay_tail_behind_requests", independent: true, routedLag: 60, frtLag: 120, endLag: 120},
	} {
		t.Run(tc.name, func(t *testing.T) {
			target := metricFinalizeTarget(now.Unix())
			if tc.independent {
				target = customerHealthSourceFinalizedThrough(now)
			}
			to := target - tc.endLag
			m := modelStatisticsWindowFixture(t, tc.independent, now, target-tc.routedLag, target-tc.frtLag, target-tc.rejectionLag)
			modelStatisticsWindowInsertFRT(t, m, to-60, "included")
			// The right edge is exclusive, including when local realtime facts
			// already contain rows that the durable finalizer has not certified.
			modelStatisticsWindowInsertFRT(t, m, to, "unfinalized_tail")
			if err := m.storeDB.Create(&RejectionSample{
				BucketTs: to, Node: cloudWatchPreRouteNode, UserID: 1,
				Model: "unfinalized_rejection", Grp: "g", Reason: "route_no_channel", Count: 1,
			}).Error; err != nil {
				t.Fatal(err)
			}
			for _, window := range []string{"24h", "3d", "7d"} {
				report, err := m.buildModelStatisticsReport(context.Background(), window, now)
				if err != nil {
					t.Fatal(err)
				}
				if report.ToTs != to || report.FromTs != to-modelStatisticsWindows[window].Seconds || report.TargetTs != target || report.LagSeconds != tc.endLag {
					t.Fatalf("%s lost actual/target window contract: from=%d to=%d target=%d lag=%d", window, report.FromTs, report.ToTs, report.TargetTs, report.LagSeconds)
				}
				if !report.Source.RequestsComplete || !report.Source.FRTComplete || !report.Source.FactsComplete {
					t.Fatalf("%s normal sampler lag hid proven FRT: %+v", window, report.Source)
				}
				if len(report.Models) != 1 || report.Models[0].Model != "included" || report.Models[0].Requests != 2 || report.Models[0].FRTObserved != 2 || report.Models[0].FRTP95Ms <= 3000 {
					t.Fatalf("%s included uncertified tail or lost valid FRT: %+v", window, report.Models)
				}
			}
		})
	}
}

func TestModelStatisticsActualWindowDoesNotHideHistoricalGap(t *testing.T) {
	now := time.Date(2026, 9, 30, 15, 23, 0, 0, cstLocation)
	target := customerHealthSourceFinalizedThrough(now)
	to := target - 60
	m := modelStatisticsWindowFixture(t, true, now, to, to, target)
	missingDay := customerHealthDayStart(now.Unix()) - 86400
	if err := m.storeDB.Delete(&CustomerHealthDayCoverage{}, "day_ts = ?", missingDay).Error; err != nil {
		t.Fatal(err)
	}
	modelStatisticsWindowInsertFRT(t, m, to-60, "recent")
	report, err := m.buildModelStatisticsReport(context.Background(), "3d", now)
	if err != nil {
		t.Fatal(err)
	}
	if report.ToTs != to || report.FromTs != to-3*86400 {
		t.Fatalf("history hole moved the report to an unrelated older window: from=%d to=%d", report.FromTs, report.ToTs)
	}
	if report.Source.RequestsComplete || report.Source.FRTComplete || report.Source.FactsComplete {
		t.Fatalf("moving the live boundary must not certify a missing historical day: %+v", report.Source)
	}
}

func TestModelStatisticsActualWindowStillRejectsOldOrCorruptFRT(t *testing.T) {
	now := time.Date(2026, 9, 30, 15, 23, 0, 0, cstLocation)
	target := customerHealthSourceFinalizedThrough(now)
	to := target - 60
	for _, tc := range []struct {
		name    string
		updates map[string]any
	}{
		{name: "old_row", updates: map[string]any{"ttft_semantics_version": 0}},
		{name: "orphan_histogram", updates: map[string]any{"ttft_observed": 0}},
		{name: "invalid_highest_bucket", updates: map[string]any{"ttft_max_ms": 600}},
		{name: "over_count_exceeds_observed", updates: map[string]any{"ttft_over_3s": 3}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m := modelStatisticsWindowFixture(t, true, now, to, to, target)
			modelStatisticsWindowInsertFRT(t, m, to-60, "measured")
			if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("bucket_ts = ?", to-60).Updates(tc.updates).Error; err != nil {
				t.Fatal(err)
			}
			report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
			if err != nil {
				t.Fatal(err)
			}
			if report.ToTs != to || !report.Source.RequestsComplete || report.Source.FRTComplete || report.Source.TTFTComplete || report.Source.FactsComplete {
				t.Fatalf("right-boundary adjustment hid old/corrupt FRT: %+v", report.Source)
			}
		})
	}
}

func TestModelStatisticsActualWindowRequiresIndependentFRTProof(t *testing.T) {
	now := time.Date(2026, 9, 30, 15, 23, 0, 0, cstLocation)
	for _, independent := range []bool{false, true} {
		for _, tc := range []struct {
			name    string
			updates map[string]any
		}{
			{name: "zero_version", updates: map[string]any{"ttft_semantics_version": 0}},
			{name: "old_version", updates: map[string]any{"ttft_semantics_version": ttftCoverageSemanticsVersion - 1}},
			{name: "missing_from", updates: map[string]any{"ttft_coverage_from_ts": 0}},
			{name: "missing_through", updates: map[string]any{"ttft_coverage_through_ts": 0}},
		} {
			lane := "standard/"
			target := metricFinalizeTarget(now.Unix())
			if independent {
				lane, target = "independent/", customerHealthSourceFinalizedThrough(now)
			}
			t.Run(lane+tc.name, func(t *testing.T) {
				to := target - 60
				m := modelStatisticsWindowFixture(t, independent, now, to, to, target)
				modelStatisticsWindowInsertFRT(t, m, to-60, "valid_rows")
				var cursor any = &MetricFinalizeState{}
				if independent {
					cursor = &CustomerHealthSourceCursor{}
				}
				// Update after insertion: BeforeCreate intentionally defaults new
				// cursor fields and cannot represent an old on-disk schema row.
				if err := m.storeDB.Model(cursor).Where("id = ?", 1).Updates(tc.updates).Error; err != nil {
					t.Fatal(err)
				}
				report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
				if err != nil {
					t.Fatal(err)
				}
				if !report.Source.RequestsComplete || report.Source.FRTComplete || report.Source.TTFTComplete || report.Source.FactsComplete {
					t.Fatalf("valid data alone replaced missing independent FRT proof: %+v", report.Source)
				}
			})
		}
	}
}

func TestModelStatisticsActualWindowDoesNotInventMissingSourceCoverage(t *testing.T) {
	now := time.Date(2026, 9, 30, 15, 23, 0, 0, cstLocation)
	target := metricFinalizeTarget(now.Unix())
	for _, missing := range []string{"routed", "rejected"} {
		t.Run(missing, func(t *testing.T) {
			m := modelStatisticsWindowFixture(t, false, now, target-60, target-60, target)
			modelStatisticsWindowInsertFRT(t, m, target-120, "present_rows")
			var state any = &MetricFinalizeState{}
			if missing == "rejected" {
				state = &CloudWatchPreRouteCursor{}
			}
			if err := m.storeDB.Where("id = ?", 1).Delete(state).Error; err != nil {
				t.Fatal(err)
			}
			report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
			if err != nil {
				t.Fatal(err)
			}
			if report.Source.RequestsComplete || report.Source.FactsComplete {
				t.Fatalf("missing %s source watermark falsely became complete: %+v", missing, report.Source)
			}
		})
	}
}

func TestModelStatisticsActualWindowPreservesLeftBoundaryAndMixedGuards(t *testing.T) {
	now := time.Date(2026, 9, 30, 15, 23, 0, 0, cstLocation)
	target := metricFinalizeTarget(now.Unix())
	to := target - 60
	for _, mode := range []string{"short_coverage_start", "mixed_unknown_identity"} {
		t.Run(mode, func(t *testing.T) {
			m := modelStatisticsWindowFixture(t, false, now, to, to, target)
			modelStatisticsWindowInsertFRT(t, m, to-60, "measured")
			if mode == "short_coverage_start" {
				// The original target had exactly 24 hours of proof. Moving
				// both report edges back exposes an extra minute at the left;
				// preserve that gap instead of silently shortening the window.
				if err := m.storeDB.Model(&MetricFinalizeState{}).Where("id = ?", 1).
					Updates(map[string]any{"coverage_from_ts": target - 86400, "ttft_coverage_from_ts": target - 86400}).Error; err != nil {
					t.Fatal(err)
				}
			} else if err := m.storeDB.Create(&RejectionSample{
				BucketTs: to - 60, Node: "legacy-collector", UserID: 0,
				Model: "measured", Grp: "g", Reason: "no_channel", Count: 1,
			}).Error; err != nil {
				t.Fatal(err)
			}
			report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
			if err != nil {
				t.Fatal(err)
			}
			if report.ToTs != to || report.FromTs != to-86400 || report.Source.RequestsComplete || report.Source.FactsComplete {
				t.Fatalf("bounded window bypassed %s safety check: from=%d to=%d source=%+v", mode, report.FromTs, report.ToTs, report.Source)
			}
			if mode == "mixed_unknown_identity" && report.Source.UnavailableCoverage.Source != "mixed" {
				t.Fatalf("unknown identity was treated as deduplicated: %+v", report.Source.UnavailableCoverage)
			}
		})
	}
}

func TestModelStatisticsActualWindowMidnightPrefersLiveOverStaleLedger(t *testing.T) {
	now := time.Date(2026, 9, 30, 0, 0, 30, 0, cstLocation)
	today := customerHealthDayStart(now.Unix())
	target, to := today-120, today-180
	m := modelStatisticsWindowFixture(t, true, now, to, to, target)
	// A stale ledger already claims midnight, while the live writer is
	// correctly waiting for yesterday's last two minutes to seal.
	if err := m.storeDB.Model(&CustomerHealthDayCoverage{}).Where("day_ts = ?", today-86400).
		Updates(map[string]any{"through_ts": today, "ttft_coverage_through_ts": today}).Error; err != nil {
		t.Fatal(err)
	}
	modelStatisticsWindowInsertFRT(t, m, to-60, "sealed")
	modelStatisticsWindowInsertFRT(t, m, target, "still_open")
	report, err := m.buildModelStatisticsReport(context.Background(), "24h", now)
	if err != nil {
		t.Fatal(err)
	}
	if report.TargetTs != target || report.ToTs != to || report.FromTs != to-86400 || report.LagSeconds != 60 {
		t.Fatalf("midnight prematurely certified the open tail or ignored live cursor: from=%d to=%d target=%d lag=%d", report.FromTs, report.ToTs, report.TargetTs, report.LagSeconds)
	}
	if !report.Source.FRTComplete || !report.Source.RequestsComplete || len(report.Models) != 1 || report.Models[0].Model != "sealed" {
		t.Fatalf("midnight sealed data not rendered correctly: source=%+v models=%+v", report.Source, report.Models)
	}
}
