package monitor

import "time"

// Configured waiting is separate from the proven watermark and lag_seconds.
// These values never move a cursor or certify an uncollected interval.
type ReportTiming struct {
	FinalizeDelaySeconds  int64 `json:"finalize_delay_seconds"`
	AlignmentSeconds      int64 `json:"alignment_seconds"`
	CollectionPollSeconds int64 `json:"collection_poll_seconds"`
}

func (m *Monitor) rejectionReportTiming() ReportTiming {
	poll := int64(m.cfg.CloudWatchPreRoutePollSeconds)
	if poll <= 0 {
		poll = 300
	}
	live := min(max(int64(m.cfg.CloudWatchPreRouteLivePollSeconds), 60), poll)
	return ReportTiming{int64(cloudWatchPreRouteFinalizeDelay / time.Second), 60, live}
}

func (m *Monitor) modelReportTiming(rejections bool) ReportTiming {
	t := ReportTiming{metricFinalizeDelaySec, 60, metricFinalizeRunEverySec}
	if m.cfg.CustomerHealthSourceEnabled {
		t = ReportTiming{int64(customerHealthSourceFinalizeDelay / time.Second), 60, int64(m.customerHealthSourcePollEvery() / time.Second)}
	}
	if rejections && m.cfg.CloudWatchPreRouteEnabled {
		r := m.rejectionReportTiming()
		t.FinalizeDelaySeconds = max(t.FinalizeDelaySeconds, r.FinalizeDelaySeconds)
		t.CollectionPollSeconds = max(t.CollectionPollSeconds, r.CollectionPollSeconds)
	}
	return t
}
