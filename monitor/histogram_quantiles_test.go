package monitor

import (
	"context"
	"encoding/json"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/gin-gonic/gin"
)

func TestTTFTTailQuantileReportsBucketNotInventedPrecision(t *testing.T) {
	// 99 observations at 11s and one at 3600s share the same legacy bucket.
	// The old estimate is 3420.5s, but only (10s,3600s] can be proved.
	view := computeTTFTMetricView([6]int64{0, 0, 0, 0, 0, 100}, 100, 100, 3600000)
	if !view.HistValid || view.P95Ms != 3420500 {
		t.Fatalf("compatibility estimate changed: %+v", view)
	}
	want := histogramQuantileRange{Lower: 10000, Upper: 3600000, OpenTail: true, Valid: true}
	metrics := (stabilityCounts{TtftInf: 100, TtftObserved: 100, TtftOver3s: 100, TtftMaxMs: 3600000}).metrics()
	if metrics.FRTQuantiles == nil || metrics.FRTQuantiles.P95 != want || metrics.FRTQuantiles.P50 != want || metrics.FRTQuantiles.P99 != want {
		t.Fatalf("stability tail bounds: %+v", metrics.FRTQuantiles)
	}
}

func TestTTFTQuantileBoundsBoundariesAndInvalidInputs(t *testing.T) {
	for _, tc := range []struct {
		name string
		hist []int64
		max  int
		p    float64
		want histogramQuantileRange
	}{
		{"finite boundary", []int64{0, 0, 0, 0, 0, 95, 5}, 3600, 95, histogramQuantileRange{Lower: 30, Upper: 60, Valid: true}},
		{"tail immediately after boundary", []int64{0, 0, 0, 0, 0, 94, 6}, 3600, 95, histogramQuantileRange{Lower: 60, Upper: 3600, OpenTail: true, Valid: true}},
		{"finite max caps bound", []int64{0, 0, 0, 0, 100, 0, 0}, 18, 95, histogramQuantileRange{Lower: 10, Upper: 18, Valid: true}},
		{"empty", []int64{0, 0, 0, 0, 0, 0, 0}, 1, 95, histogramQuantileRange{}},
		{"negative", []int64{-1, 0, 0, 0, 0, 0, 2}, 100, 95, histogramQuantileRange{}},
		{"missing bucket", []int64{1}, 1, 95, histogramQuantileRange{}},
		{"inconsistent max", []int64{1, 0, 0, 0, 0, 0, 0}, 3600, 95, histogramQuantileRange{}},
		{"nan percentile", []int64{1, 0, 0, 0, 0, 0, 0}, 1, math.NaN(), histogramQuantileRange{}},
		{"overflow", []int64{math.MaxInt64, 1, 0, 0, 0, 0, 0}, 2, 95, histogramQuantileRange{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := quantileBucket(tc.hist, latEdges, tc.max, tc.p); got != tc.want {
				t.Fatalf("got %+v want %+v", got, tc.want)
			}
		})
	}
}

func TestStabilityBoundsDoNotChangeModelOrAlertContract(t *testing.T) {
	a := aggRow{Success: 100, LatInf: 100, MaxUseTime: 3600, Quota: 123456,
		TtftInf: 100, TtftObserved: 100, TtftOver3s: 100, TtftMaxMs: 3600000}
	var row Row
	a.fill(&row, 3600)
	if row.Total != 100 || row.SuccessRate != 100 || row.CostUSD != float64(a.Quota)/quotaPerUSD || row.MaxLatency != 3600 {
		t.Fatalf("non-percentile facts changed: %+v", row)
	}
	metrics := (stabilityCounts{TtftInf: 100, TtftObserved: 100, TtftOver3s: 100, TtftMaxMs: 3600000}).metrics()
	if metrics.FRTQuantiles == nil || metrics.FRTP95Ms != row.TtftP95*1000 {
		t.Fatal("legacy numerical values must remain unchanged")
	}
	body := alertBody(row, AlertConfig{}, "test", true)
	if !strings.Contains(body, "3420") || strings.Contains(body, "分桶范围") {
		t.Fatalf("model alert formatting changed: %s", body)
	}
	payload, err := json.Marshal(row)
	if err != nil || strings.Contains(string(payload), "quantiles_") {
		t.Fatalf("stability metadata leaked into model contract: %s %v", payload, err)
	}
	invalid := (stabilityCounts{TtftInf: 1, TtftObserved: 2, TtftMaxMs: 11000}).metrics()
	if invalid.FRTQuantiles != nil {
		t.Fatal("invalid histogram must not publish bounds")
	}
}

func TestStabilityBoundsPreserveRefinedThreeSecondBoundary(t *testing.T) {
	for _, tc := range []struct {
		name   string
		counts stabilityCounts
		want   histogramQuantileRange
	}{
		{"below three seconds", stabilityCounts{Ttft5k: 100, TtftObserved: 100, TtftMaxMs: 3000}, histogramQuantileRange{Lower: 2000, Upper: 3000, Valid: true}},
		{"above three seconds", stabilityCounts{Ttft5k: 100, TtftObserved: 100, TtftOver3s: 100, TtftMaxMs: 4000}, histogramQuantileRange{Lower: 3000, Upper: 4000, Valid: true}},
		{"exact split boundary", stabilityCounts{Ttft5k: 100, TtftObserved: 100, TtftOver3s: 5, TtftMaxMs: 4000}, histogramQuantileRange{Lower: 2000, Upper: 3000, Valid: true}},
		{"after split boundary", stabilityCounts{Ttft5k: 100, TtftObserved: 100, TtftOver3s: 6, TtftMaxMs: 4000}, histogramQuantileRange{Lower: 3000, Upper: 4000, Valid: true}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			metrics := tc.counts.metrics()
			if metrics.FRTQuantiles == nil || metrics.FRTQuantiles.P95 != tc.want {
				t.Fatalf("refined boundary differs: %+v want %+v", metrics.FRTQuantiles, tc.want)
			}
			legacy := computeTTFTMetricView([6]int64{0, 0, 0, 100, 0, 0}, 100, tc.counts.TtftOver3s, tc.counts.TtftMaxMs)
			if metrics.FRTP95Ms != legacy.P95Ms || metrics.FRTMaxMs != legacy.MaxMs || metrics.FRTOver3s != legacy.Over3s {
				t.Fatal("display metadata changed existing numeric facts")
			}
		})
	}
}

func TestTTFTBoundsSQLiteReadbackAndEmbeddedFormatter(t *testing.T) {
	m := newStabilityTestMonitor(t)
	defer m.Close()
	sample := StabilityHourSample{HourTs: 1800000000, ChannelID: 1, ModelName: "test", Grp: "g", Success: 100,
		MaxUseTime: 3600, TtftInf: 100, TtftObserved: 100, TtftOver3s: 100, TtftMaxMs: 3600000}
	if err := m.storeDB.Create(&sample).Error; err != nil {
		t.Fatal(err)
	}
	dims, _, err := m.queryStabilityDims(context.Background(), stabilityScope{FromTs: sample.HourTs, ToTs: sample.HourTs + 3600}, 10)
	if err != nil || len(dims) != 1 {
		t.Fatalf("SQLite readback: %+v %v", dims, err)
	}
	metrics := dims[0].counts().metrics()
	if metrics.Requests != 100 || metrics.FRTQuantiles == nil || metrics.FRTQuantiles.P95.Lower != 10000 {
		t.Fatalf("SQLite stability bounds/counters: %+v", metrics)
	}
	var after StabilityHourSample
	if err := m.storeDB.First(&after).Error; err != nil || after != sample {
		t.Fatalf("reading metadata changed stored facts: %+v %v", after, err)
	}
	r := gin.New()
	m.RegisterRoutes(r)
	w := httptest.NewRecorder()
	r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/quantile_display.js?v=1", nil))
	if w.Code != http.StatusOK || !strings.Contains(w.Body.String(), "MonitorQuantiles") || w.Header().Get("Cache-Control") != "no-cache" {
		t.Fatalf("embedded formatter unavailable: status=%d headers=%v", w.Code, w.Header())
	}
}
