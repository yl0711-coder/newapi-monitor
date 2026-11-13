package monitor

import (
	"context"
	"testing"
)

func TestModelStatisticsCollectionFailureVisibleBeforeWatermarkLags(t *testing.T) {
	for _, independent := range []bool{false, true} {
		name := "metric_finalize"
		if independent {
			name = "customer_health"
		}
		t.Run(name, func(t *testing.T) {
			m, _ := modelFRTFixture(t, independent)
			before, err := m.buildModelStatisticsReport(context.Background(), "24h", modelFRTTestNow)
			if err != nil {
				t.Fatal(err)
			}
			now := modelFRTTestNow.Unix()
			if independent {
				m.customerHealthSourceLastSuccess.Store(now - 1)
				m.customerHealthSourceLastFailure.Store(now)
			} else {
				m.metricFinalizeLastSuccess.Store(now - 1)
				m.metricFinalizeLastFailure.Store(now)
			}
			after, err := m.buildModelStatisticsReport(context.Background(), "24h", modelFRTTestNow)
			if err != nil {
				t.Fatal(err)
			}
			frt := modelFRTBuild(t, m, "24h", "")
			if !after.CollectionFailed || !frt.CollectionFailed {
				t.Fatal("source failure must be explicit even before the watermark target moves")
			}
			if after.ToTs != before.ToTs || after.LagSeconds != before.LagSeconds || after.Source.FactsComplete != before.Source.FactsComplete {
				t.Fatal("current collection failure must not rewrite historical coverage")
			}
			if independent {
				m.customerHealthSourceLastSuccess.Store(now + 1)
			} else {
				m.metricFinalizeLastSuccess.Store(now + 1)
			}
			if m.modelReportCollectionFailed(false) {
				t.Fatal("recovered collection must clear failure")
			}
			m.cfg.CloudWatchPreRouteEnabled = true
			m.cloudWatchPreRouteLastFailure.Store(now)
			if !m.modelReportCollectionFailed(true) || m.modelReportCollectionFailed(false) {
				t.Fatal("rejection-source failure must affect model statistics but not FRT")
			}
		})
	}
}
