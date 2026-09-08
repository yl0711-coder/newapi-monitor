package monitor

import (
	"context"
	"testing"
	"time"
)

func TestStabilityRollingComparisonUsesEqualDuration(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 9, 7, 13, 15, 0, 0, cstLocation).Unix()
	end := now / 3600 * 3600
	for _, hours := range []int{1, 24, 168} {
		scope := stabilityScope{FromTs: end - int64(hours)*3600, ToTs: end, RangeHours: hours}
		report, err := m.buildStabilityReport(context.Background(), scope, now)
		if err != nil {
			t.Fatal(err)
		}
		if got, want := report.Meta.ComparisonCoverage.FromTs, scope.FromTs-int64(hours)*3600; got != want {
			t.Fatalf("rolling %dh: comparison start %d, want %d", hours, got, want)
		}
	}
}

func TestStabilityCoverageExposesExcludedLiveTail(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 9, 7, 13, 15, 0, 0, cstLocation).Unix()
	end := now / 3600 * 3600
	for _, requested := range []int64{end, now} {
		got := m.stabilityDataCoverage(context.Background(), end-86400, requested, now)
		if got.RequestedToTs != requested || got.ProvisionalSeconds != requested-got.ToTs {
			t.Fatalf("excluded live interval must be explicit: %+v", got)
		}
	}
	closed := m.stabilityDataCoverage(context.Background(), end-86400, end-7200, now)
	if closed.ProvisionalSeconds != 0 || closed.RequestedToTs != 0 {
		t.Fatalf("historical gaps are not a provisional tail: %+v", closed)
	}
}

func TestAICodeWithHistoryReopensAfterMidnightWithoutBypassingBackoff(t *testing.T) {
	m := newStabilityTestMonitor(t)
	now := time.Date(2026, 9, 7, 13, 15, 0, 0, cstLocation).Unix()
	today := cstDayStart(now)
	for _, tc := range []struct {
		domain        string
		cursor, retry int64
		status        string
	}{
		{"due.example", today - 86400, 0, upstreamStatusOK},
		{"closed.example", today, 0, upstreamStatusOK},
		{"backoff.example", today - 86400, now + 600, upstreamStatusOK},
		{"auth.example", today - 86400, 0, upstreamStatusReconnect},
	} {
		row := ChannelUpstreamAccount{Domain: tc.domain, Provider: upstreamProviderAICodeWith,
			Enabled: true, UsageSyncEnabled: true, UsageStatus: tc.status, UsageBackfillDone: true,
			UsageBackfillCursor: tc.cursor, UsageBackfillNextSyncAt: tc.retry, UsageNextSyncAt: now + 1800}
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	rows, err := m.loadDueUpstreamUsageAccountsForLane(context.Background(), now, 10, upstreamUsageLaneHistory)
	if err != nil || len(rows) != 1 || rows[0].Domain != "due.example" {
		t.Fatalf("only the healthy due day-close may resume: rows=%+v err=%v", rows, err)
	}
}

func TestCapacityIngressClipsBoundaryDenominators(t *testing.T) {
	m := newTestMonitor(t)
	for _, minute := range []int64{360, 600} {
		row := NginxMinuteSample{BucketTs: minute, Node: "test", Route: "/v1/responses", Method: "POST",
			Status: 200, Count: 12, RequestTimeSumMS: 120000}
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	points, _ := m.readCapacityIngress(context.Background(), 360, 660, 300, 720)
	if len(points) != 2 || points[0].RPM != 3 || points[1].RPM != 12 ||
		points[0].EstimatedInflight != 0.5 || points[1].EstimatedInflight != 2 {
		t.Fatalf("partial first/last buckets must use 4 and 1 minutes: %+v", points)
	}
}
