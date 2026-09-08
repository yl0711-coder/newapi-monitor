package monitor

import (
	"context"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestChannelReportFactsAndCoverageUseIdenticalFinalizedBounds(t *testing.T) {
	m := newStabilityTestMonitor(t)
	start := time.Date(2026, 9, 6, 12, 0, 0, 0, cstLocation).Unix()
	for i := int64(0); i < 2; i++ {
		if err := m.replaceStabilityHourTraffic(start+i*3600,
			[]StabilityHourSample{{HourTs: start + i*3600, ChannelID: 1, ModelName: "m", Grp: "g", Success: 10}}, nil,
			StabilityHourIngestState{HourTs: start + i*3600, Status: "complete", Requests: 10}); err != nil {
			t.Fatal(err)
		}
	}
	// At 14:30, the 13:00 hour has closed but is still inside the late-write
	// allowance. It cannot enter facts while being excluded from coverage.
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: start, ToTs: start + 7200}, start+9000)
	if err != nil {
		t.Fatal(err)
	}
	c := report.Meta.DataCoverage
	if report.Summary.Usage.Requests != 10 || c.FromTs != start || c.ToTs != start+3600 || !c.Complete || c.ExpectedHours != 1 {
		t.Fatalf("facts and coverage diverged: usage=%+v coverage=%+v", report.Summary.Usage, c)
	}
	if report.Meta.From != "2026-09-06 12:00" || report.Meta.To != "2026-09-06 13:00" {
		t.Fatalf("hidden cutoff: %+v", report.Meta)
	}
}

func TestChannelReportDateRangeWithoutFinalizedHourIsNotCompleteZero(t *testing.T) {
	gin.SetMode(gin.TestMode)
	now := time.Date(2026, 9, 6, 0, 30, 0, 0, cstLocation)
	c, _ := gin.CreateTestContext(httptest.NewRecorder())
	c.Request = httptest.NewRequest("GET", "/channels/report?from=2026-09-06&to=2026-09-06", nil)
	if _, err := channelManagementRange(c, now, 90); err == nil {
		t.Fatal("unfinished date range must not publish complete zero")
	}
}

func TestChannelFinalizedScopeIsIdempotentAcrossMidnight(t *testing.T) {
	now := time.Date(2026, 9, 6, 0, 30, 0, 0, cstLocation).Unix()
	wantEnd := time.Date(2026, 9, 5, 23, 0, 0, 0, cstLocation).Unix()
	scope := channelFinalizedScope(stabilityScope{ToTs: now, RangeHours: 24}, now)
	if scope.ToTs != wantEnd || scope.ToTs-scope.FromTs != 86400 {
		t.Fatalf("rolling duration shifted: %+v", scope)
	}
	if again := channelFinalizedScope(scope, now); again != scope {
		t.Fatal("normalization clipped the window twice")
	}
}
