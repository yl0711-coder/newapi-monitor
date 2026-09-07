package monitor

import (
	"context"
	"encoding/json"
	"net/http/httptest"
	"reflect"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestChannelDataStatusMatchesReportCoverageWithoutSourceOrWrites(t *testing.T) {
	m := newStabilityTestMonitor(t) // no NewAPI connection, credentials or HTTP client
	start := time.Date(2026, 9, 6, 12, 0, 0, 0, cstLocation).Unix()
	if err := m.replaceStabilityHourTraffic(start,
		[]StabilityHourSample{{HourTs: start, ChannelID: 1, Success: 10}}, nil,
		StabilityHourIngestState{HourTs: start, Status: "complete", Requests: 10}); err != nil {
		t.Fatal(err)
	}
	scope := stabilityScope{FromTs: start, ToTs: start + 7200}
	now := start + 4*3600
	report, err := m.buildChannelManagementReport(context.Background(), scope, now)
	if err != nil {
		t.Fatal(err)
	}
	// A read-only SQLite handle turns any accidental scheduling/write into failure.
	if err := m.storeDB.Exec("PRAGMA query_only = ON").Error; err != nil {
		t.Fatal(err)
	}
	status, err := m.buildChannelDataStatus(context.Background(), scope, now)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(status.Meta.DataCoverage, report.Meta.DataCoverage) || status.Meta.DataCoverage.Complete || status.Meta.DataCoverage.MissingHours != 1 {
		t.Fatalf("diagnostic coverage diverged: %+v vs %+v", status.Meta.DataCoverage, report.Meta.DataCoverage)
	}
	if status.Meta.FromTs != report.Meta.FromTs || status.Meta.ToTs != report.Meta.ToTs || status.Domains == nil {
		t.Fatal("diagnostic window or empty-list contract differs from report")
	}
}

func TestChannelDataStatusRejectsInvalidRangesAndReturnsDisabledExplicitly(t *testing.T) {
	gin.SetMode(gin.TestMode)
	m := newStabilityTestMonitor(t)
	for _, query := range []string{"hours=0", "hours=24&days=7", "hours=999999"} {
		w := httptest.NewRecorder()
		c, _ := gin.CreateTestContext(w)
		c.Request = httptest.NewRequest("GET", "/channels/data-status?"+query, nil)
		m.serveChannelDataStatus(c)
		if w.Code != 400 || w.Header().Get("Cache-Control") != "no-store" {
			t.Fatalf("query %s: status=%d", query, w.Code)
		}
	}
	m.cfg.StabilityEnabled = false
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/channels/data-status?hours=24", nil)
	m.serveChannelDataStatus(c)
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil || body["enabled"] != false {
		t.Fatal("disabled diagnostic must not masquerade as complete")
	}
}
