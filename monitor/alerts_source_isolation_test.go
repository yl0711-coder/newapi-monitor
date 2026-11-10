package monitor

import (
	"encoding/json"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func TestAlertsWatermarkNeverBorrowsProblemSignatureCursor(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	now := time.Date(2026, 10, 23, 12, 0, 0, 0, cstLocation)
	_, target := cloudWatchPreRouteRange(now, 168)
	scope := stabilityScope{FromTs: target - 3600, ToTs: target}
	for _, enabled := range []bool{false, true} {
		m.cfg.CloudWatchPreRouteEnabled = enabled
		m.cloudWatchPreRouteFrom.Store(scope.FromTs)
		m.cloudWatchPreRouteThrough.Store(target - 120)
		for _, problemThrough := range []int64{target - 600, target, target + 600} {
			if err := m.storeDB.Save(&StabilityProblemLiveCursor{ID: 1, NextTs: problemThrough, TargetThroughTs: target + 600, TrafficClassVersion: stabilityTrafficClassificationVersion}).Error; err != nil {
				t.Fatal(err)
			}
			got := m.alertsCoverage(scope, now)
			if enabled {
				if got.Through != target-120 || got.Target != target {
					t.Fatalf("problem cursor leaked into direct source: %+v", got)
				}
			} else if got.Through != 0 || got.Target != 0 || got.Complete || got.Note == "" {
				t.Fatalf("legacy rejection coverage must remain unknown: %+v", got)
			}
		}
	}
	m.cloudWatchPreRouteThrough.Store(target)
	if got := m.alertsCoverage(scope, now); got.Through != target || !got.Complete {
		t.Fatalf("direct source should independently catch up: %+v", got)
	}
}

func TestAlertsUnknownWatermarkIsNullOverHTTP(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.StabilityEnabled = true
	m.cfg.CloudWatchPreRouteEnabled = false
	now := time.Now().Truncate(time.Minute)
	m.problemLiveThrough.Store(now.Unix())
	m.cloudWatchPreRouteLastFailure.Store(now.Unix())
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest("GET", "/alerts/rejections?window=24h", nil)
	m.getRejectAlertsAt(c, now)
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var got map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &got); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"actual_through_ts", "lag_seconds"} {
		v, exists := got[key]
		if !exists || v != nil {
			t.Fatalf("%s must be explicit null: %v", key, v)
		}
	}
	if got["collection_failed"] != false || got["coverage_complete"] != false {
		t.Fatalf("disabled lane reused another health state: %+v", got)
	}
}
