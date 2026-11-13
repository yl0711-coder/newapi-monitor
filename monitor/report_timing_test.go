package monitor

import "testing"

func TestReportTimingSeparatesFinalizationAndCollectionCadence(t *testing.T) {
	m := &Monitor{}
	legacy := m.modelReportTiming(false)
	if legacy.FinalizeDelaySeconds != 3600 || legacy.CollectionPollSeconds != 300 {
		t.Fatalf("legacy %+v", legacy)
	}
	m.cfg.CustomerHealthSourceEnabled = true
	m.cfg.SampleSeconds = 60
	m.cfg.CloudWatchPreRouteEnabled = true
	m.cfg.CloudWatchPreRoutePollSeconds = 300
	m.cfg.CloudWatchPreRouteLivePollSeconds = 60
	for _, timing := range []ReportTiming{m.modelReportTiming(true), m.modelReportTiming(false), m.rejectionReportTiming()} {
		if timing.FinalizeDelaySeconds != 120 || timing.AlignmentSeconds != 60 || timing.CollectionPollSeconds != 60 {
			t.Fatalf("live %+v", timing)
		}
	}
	m.cfg.CloudWatchPreRouteLivePollSeconds = 600
	if timing := m.modelReportTiming(true); timing.CollectionPollSeconds != 300 {
		t.Fatalf("live clamped to replay cadence: %+v", timing)
	}
}
