package monitor

import (
	"context"
	"slices"
	"testing"
)

func TestTokenDetectionSelectedCoverageRequiresEveryOverlappingWindow(t *testing.T) {
	for _, tc := range []struct {
		name   string
		from   int64
		to     int64
		change func(*TokenMeteringState, []TokenMeteringWindow)
		want   bool
	}{
		{name: "earlier_gap_does_not_hide_complete_window", from: 310, to: 590, want: true},
		{name: "partial_edges_require_both_windows", from: 310, to: 899, want: true},
		{name: "exclusive_right_boundary", from: 300, to: 600, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[1].SourceComplete = false }, want: true},
		{name: "missing_left_window", from: 299, to: 590},
		{name: "missing_right_window", from: 310, to: 901},
		{name: "replay_revokes_proof", from: 310, to: 590, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[0].Replay = true }},
		{name: "unfinished", from: 310, to: 590, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[0].Done = false }},
		{name: "incomplete", from: 310, to: 590, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[0].SourceComplete = false }},
		{name: "wrong_endpoint", from: 310, to: 590, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[0].EndAt-- }},
		{name: "other_epoch", from: 310, to: 590, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[0].SourceEpoch = "other-source" }},
		{name: "off_grid_window_cannot_replace_missing_one", from: 310, to: 899, change: func(_ *TokenMeteringState, w []TokenMeteringWindow) { w[1].StartAt -= 299; w[1].EndAt -= 299 }},
		{name: "partial_source_mode", from: 310, to: 590, change: func(s *TokenMeteringState, _ []TokenMeteringWindow) { s.SourceMode = "partial" }},
		{name: "expired_selection", from: 310, to: 590, change: func(s *TokenMeteringState, _ []TokenMeteringWindow) { s.ExpiredBefore = s.StartAt + 311 }},
		{name: "before_source_start", from: 310, to: 590, change: func(s *TokenMeteringState, _ []TokenMeteringWindow) { s.StartAt += 311 }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, _, _ := detectionFixture(t)
			defer m.Close()
			state := tokenMeteringStateRead(t, m)
			base := state.StartAt
			windows := []TokenMeteringWindow{
				{SourceEpoch: state.SourceEpoch, StartAt: base + 300, EndAt: base + 600, Done: true, SourceComplete: true},
				{SourceEpoch: state.SourceEpoch, StartAt: base + 600, EndAt: base + 900, Done: true, SourceComplete: true},
			}
			if tc.change != nil {
				tc.change(&state, windows)
			}
			if err := m.tokenStore().Create(&windows).Error; err != nil {
				t.Fatal(err)
			}
			got, err := tokenMeteringSelectedRangeComplete(m.tokenStore(), state, base+tc.from, base+tc.to)
			if err != nil || got != tc.want {
				t.Fatal("selected coverage mismatch", got, tc.want, err)
			}
		})
	}
}

func TestTokenDetectionSelectedCoverageKeepsPendingAndGlobalGapSeparate(t *testing.T) {
	m, _, _ := detectionFixture(t)
	defer m.Close()
	state := tokenMeteringStateRead(t, m)
	from := state.StartAt + 300
	w := TokenMeteringWindow{SourceEpoch: state.SourceEpoch, StartAt: from, EndAt: from + 300, Done: true, SourceComplete: true}
	if err := m.tokenStore().Create(&w).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.tokenStore().Model(&TokenMeteringState{}).Where("source_epoch=?", state.SourceEpoch).Update("scan_through", w.EndAt).Error; err != nil {
		t.Fatal(err)
	}
	detectionFact(t, m, 1, from+20, 3, 20000, 0, 0)
	if err := initTokenDetectionReportIndex(m.tokenStore()); err != nil {
		t.Fatal(err)
	}
	q := tokenDetectionQuery{SourceEpoch: state.SourceEpoch, From: from + 10, To: from + 60, Status: "candidate", Limit: 2}
	report, err := m.tokenDetectionReport(context.Background(), q)
	if err != nil {
		t.Fatal(err)
	}
	summary := report["summary"].(tokenDetectionSummary)
	if !summary.SourceComplete || summary.Unprocessed != 1 || summary.Candidates != 0 ||
		!slices.Contains(summary.Gaps, "detection_pending_or_stale") || slices.Contains(summary.Gaps, "source_range_incomplete") {
		t.Fatal("selected proof was confused with pending detection or an earlier gap", summary)
	}
	if report["source_complete_through"].(int64) >= from || tokenMeteringStateRead(t, m).CompleteThrough != state.CompleteThrough {
		t.Fatal("range query advanced the global watermark")
	}
	if err := m.tokenStore().Model(&w).Update("replay", true).Error; err != nil {
		t.Fatal(err)
	}
	report, err = m.tokenDetectionReport(context.Background(), q)
	if err != nil || report["summary"].(tokenDetectionSummary).SourceComplete {
		t.Fatal("reopened source window retained its old completeness proof", err)
	}
}
