package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

var modelFRTTestNow = time.Date(2026, 10, 14, 8, 23, 37, 0, time.UTC)

func modelFRTFixture(t *testing.T, independent bool) (*Monitor, int64) {
	t.Helper()
	end := metricFinalizeTarget(modelFRTTestNow.Unix())
	if independent {
		end = customerHealthSourceFinalizedThrough(modelFRTTestNow)
	}
	return modelStatisticsWindowFixture(t, independent, modelFRTTestNow, end, end, end), end
}
func modelFRTBuild(t *testing.T, m *Monitor, window, end string) ModelFRTReport {
	t.Helper()
	r, err := m.buildModelFRTReport(context.Background(), window, end, modelFRTTestNow)
	if err != nil {
		t.Fatal(err)
	}
	return r
}
func modelFRTInsert(t *testing.T, m *Monitor, end int64, channel int, group, model string, count, ms int64) {
	t.Helper()
	row := CapacityUserMinuteSample{BucketTs: end - 60, UserID: 1, ChannelID: channel, Grp: group, ModelName: model, Success: count, TtftObserved: count, TtftMaxMs: int(ms)}
	if ms <= 500 {
		row.Ttft500 = count
	} else {
		row.Ttft5k = count
		row.TtftOver3s = count
	}
	if err := m.storeDB.Create(&row).Error; err != nil {
		t.Fatal(err)
	}
}

func TestModelFRTMergedHistogramAndActualGroupTree(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTInsert(t, m, end, 1, "actual-a", "fast", 99, 400)
	modelFRTInsert(t, m, end, 1, "actual-a", "slow", 1, 4000)
	modelFRTInsert(t, m, end, 1, "actual-b", "fast", 100, 4000)
	modelFRTInsert(t, m, end, 2, "actual-a", "fast", 10, 400)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "local-name", Groups: "config-only,actual-a"}).Error; err != nil {
		t.Fatal(err)
	}
	r := modelFRTBuild(t, m, "24h", "")
	if !r.Coverage.Complete || len(r.Channels) != 2 || len(r.Channels[0].Groups) != 2 {
		t.Fatalf("tree/proof: %+v", r)
	}
	c := r.Channels[0]
	g := c.Groups[0]
	want := computeTTFTMetricView([6]int64{99, 0, 0, 1, 0, 0}, 100, 1, 4000)
	if c.Name != "local-name" || g.Group != "actual-a" || len(g.Models) != 2 || *g.FRT.P95Ms != want.P95Ms || *g.FRT.Observed != 100 {
		t.Fatalf("group: %+v", g)
	}
	if *g.FRT.P95Ms == (*g.Models[0].FRT.P95Ms+*g.Models[1].FRT.P95Ms)/2 {
		t.Fatal("averaged percentiles")
	}
	want = computeTTFTMetricView([6]int64{99, 0, 0, 101, 0, 0}, 200, 101, 4000)
	if *c.FRT.P95Ms != want.P95Ms || *c.FRT.Observed != 200 {
		t.Fatalf("channel: %+v", c.FRT)
	}
	if strings.Contains(fmt.Sprint(r), "config-only") {
		t.Fatal("configuration group substituted for actual service group")
	}
}

func TestModelFRTMatchesOldPageSameActualWindow(t *testing.T) {
	for _, independent := range []bool{false, true} {
		t.Run(fmt.Sprint(independent), func(t *testing.T) {
			m, end := modelFRTFixture(t, independent)
			if err := m.storeDB.Model(&CloudWatchPreRouteCursor{}).Where("id = ?", cloudWatchPreRouteCursorID).Update("through_ts", end-180).Error; err != nil {
				t.Fatal(err)
			}
			modelStatisticsWindowInsertFRT(t, m, end-240, "included")
			modelStatisticsWindowInsertFRT(t, m, end-60, "frt-only-tail")
			for _, key := range []string{"24h", "3d", "7d"} {
				old, err := m.buildModelStatisticsReport(context.Background(), key, modelFRTTestNow)
				if err != nil {
					t.Fatal(err)
				}
				r := modelFRTBuild(t, m, key, time.Unix(old.ToTs, 0).UTC().Format(time.RFC3339))
				got := r.Channels[0].Groups[0].Models[0].FRT
				want := old.Models[0].Groups[0].Channels[0]
				if !r.Coverage.Complete || r.From.Unix() != old.FromTs || r.To.Unix() != old.ToTs || len(r.Channels[0].Groups[0].Models) != 1 || *got.Observed != want.FRTObserved || *got.P50Ms != want.FRTP50Ms || *got.P95Ms != want.FRTP95Ms || *got.P99Ms != want.FRTP99Ms || *got.MaxMs != want.FRTMaxMs || *got.Over3s != want.FRTOver3s || *got.Over3sPct != want.FRTOver3sPct {
					t.Fatalf("different population: %+v old %+v", r, old)
				}
			}
			independentReport := modelFRTBuild(t, m, "24h", "")
			if independentReport.To.Unix() != end || len(independentReport.Channels[0].Groups[0].Models) != 2 {
				t.Fatal("rejection cursor constrained independent FRT")
			}
		})
	}
}

func TestModelFRTNeverReadsRejectionSources(t *testing.T) {
	for _, independent := range []bool{false, true} {
		m, end := modelFRTFixture(t, independent)
		modelStatisticsWindowInsertFRT(t, m, end-60, "m")
		if err := m.storeDB.Migrator().DropTable(&RejectionSample{}, &CloudWatchPreRouteCursor{}); err != nil {
			t.Fatal(err)
		}
		for _, enabled := range []bool{true, false} {
			m.cfg.CloudWatchPreRouteEnabled = enabled
			m.cfg.CloudWatchPreRouteLookbackHours = 1
			r := modelFRTBuild(t, m, "24h", "")
			if !r.Coverage.Complete || r.To.Unix() != end || *r.Channels[0].FRT.Observed != 2 {
				t.Fatalf("rejection dependency: %+v", r)
			}
		}
	}
}

func TestModelFRTMissingInvalidAndHistoricalNeverZeroLatency(t *testing.T) {
	for _, tc := range []struct {
		name, reason string
		updates      map[string]any
		observed     *int64
	}{
		{name: "no_valid", reason: "no_valid_samples", updates: map[string]any{"ttft_500": 0, "ttft_5k": 0, "ttft_observed": 0, "ttft_over_3s": 0, "ttft_max_ms": 0}},
		{name: "historical", reason: "historical_unsupported", updates: map[string]any{"ttft_semantics_version": 0}},
		{name: "negative", reason: "invalid_histogram", updates: map[string]any{"ttft_observed": -1}},
		{name: "hist_mismatch", reason: "invalid_histogram", updates: map[string]any{"ttft_500": 2}},
		{name: "null_bucket", reason: "invalid_histogram", updates: map[string]any{"ttft_500": nil}},
		{name: "bad_max", reason: "invalid_histogram", updates: map[string]any{"ttft_max_ms": 0}},
		{name: "over3s", reason: "invalid_histogram", updates: map[string]any{"ttft_over_3s": 4}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			m, end := modelFRTFixture(t, true)
			modelStatisticsWindowInsertFRT(t, m, end-60, "m")
			if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("model_name = ?", "m").UpdateColumns(tc.updates).Error; err != nil {
				t.Fatal(err)
			}
			r := modelFRTBuild(t, m, "24h", "")
			v := r.Channels[0].Groups[0].Models[0].FRT
			if v.UnavailableReason != tc.reason || v.P50Ms != nil || v.P95Ms != nil || v.P99Ms != nil || v.MaxMs != nil || v.Over3s != nil || v.Over3sPct != nil {
				t.Fatalf("unknown measured as latency: %+v", v)
			}
			if tc.name == "no_valid" {
				if v.Observed == nil || *v.Observed != 0 || !v.CoverageComplete {
					t.Fatal(v)
				}
			} else if v.Observed != nil || r.Coverage.Complete {
				t.Fatal(v, r.Coverage)
			}
			data, err := json.Marshal(v)
			if err != nil {
				t.Fatal(err)
			}
			if !strings.Contains(string(data), `"p95_ms":null`) {
				t.Fatal(string(data))
			}
		})
	}
}

func TestModelFRTBadRowsCannotCancelBeforeValidation(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	for _, user := range []int64{1, 2} {
		row := CapacityUserMinuteSample{BucketTs: end - 60, UserID: user, ChannelID: 1, Grp: "g", ModelName: "m", Success: 1, TtftObserved: 1, Ttft500: 1, TtftMaxMs: 400}
		if err := m.storeDB.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = 1").UpdateColumn("ttft_500", -1).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CapacityUserMinuteSample{}).Where("user_id = 2").UpdateColumn("ttft_500", 3).Error; err != nil {
		t.Fatal(err)
	}
	r := modelFRTBuild(t, m, "24h", "")
	if r.Channels[0].FRT.Observed != nil || r.Coverage.Complete {
		t.Fatal("invalid rows became valid after SUM", r)
	}
}

func TestModelFRTLagHolesAndRecovery(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelStatisticsWindowInsertFRT(t, m, end-180, "known")
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id=1").Update("ttft_coverage_through_ts", end-120).Error; err != nil {
		t.Fatal(err)
	}
	r := modelFRTBuild(t, m, "3d", "")
	if r.To.Unix() != end-120 || r.LagSeconds != 120 || !r.Coverage.Complete {
		t.Fatal(r)
	}
	day := customerHealthDayStart(end) - 86400
	var original CustomerHealthDayCoverage
	if err := m.storeDB.Where("day_ts = ?", day).First(&original).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Where("day_ts = ?", day).Delete(&CustomerHealthDayCoverage{}).Error; err != nil {
		t.Fatal(err)
	}
	r = modelFRTBuild(t, m, "3d", "")
	if r.Coverage.Complete || r.To.Unix() != end-120 || r.Channels[0].FRT.P95Ms == nil || r.Channels[0].FRT.CoverageComplete {
		t.Fatal("gap hidden or known samples lost", r)
	}
	if err := m.storeDB.Create(&original).Error; err != nil {
		t.Fatal(err)
	}
	if !modelFRTBuild(t, m, "3d", "").Coverage.Complete {
		t.Fatal("coverage failed to recover")
	}
}

func TestModelFRTClosedMinuteAndUnknownDimensions(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	for _, ts := range []int64{end - 86400 - 60, end - 86400, end - 60, end} {
		if err := m.storeDB.Create(&CapacityUserMinuteSample{BucketTs: ts, ChannelID: 0, UserID: 0, Grp: "", ModelName: fmt.Sprint(ts), Success: 1, Ttft500: 1, TtftObserved: 1, TtftMaxMs: 400}).Error; err != nil {
			t.Fatal(err)
		}
	}
	r := modelFRTBuild(t, m, "24h", "")
	if len(r.Channels) != 1 || r.Channels[0].ChannelID != 0 || len(r.Channels[0].Groups) != 1 || r.Channels[0].Groups[0].Group != "" || len(r.Channels[0].Groups[0].Models) != 2 || *r.Channels[0].FRT.Observed != 2 {
		t.Fatal(r)
	}
}

func TestModelFRTDisabledEmptyAndProjectionGap(t *testing.T) {
	m, end := modelFRTFixture(t, false)
	r := modelFRTBuild(t, m, "24h", "")
	if len(r.Channels) != 0 || !r.Coverage.Complete {
		t.Fatal(r)
	}
	modelStatisticsWindowInsertFRT(t, m, end-60, "m")
	if err := m.storeDB.Where("bucket_ts = ?", end-60).Delete(&MetricSample{}).Error; err != nil {
		t.Fatal(err)
	}
	r = modelFRTBuild(t, m, "24h", "")
	if r.Coverage.Complete || r.Channels[0].FRT.P95Ms == nil {
		t.Fatal(r)
	}
	m.cfg.CapacityEnabled = false
	r = modelFRTBuild(t, m, "24h", "")
	if r.Coverage.Complete || r.Coverage.Status != modelCoverageUnavailable {
		t.Fatal(r)
	}
}

func TestModelFRTInvalidBoundsAndCancellation(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	for _, raw := range []string{"bad", time.Unix(end+60, 0).UTC().Format(time.RFC3339), time.Unix(end-1, 0).UTC().Format(time.RFC3339), time.Unix(end-11*86400, 0).UTC().Format(time.RFC3339), time.Unix(end, 0).In(cstLocation).Format(time.RFC3339)} {
		_, err := m.buildModelFRTReport(context.Background(), "24h", raw, modelFRTTestNow)
		var input *modelFRTInputError
		if !errors.As(err, &input) {
			t.Fatalf("bad to accepted: %s %v", raw, err)
		}
	}
	for _, key := range []string{"8d", "9999999"} {
		if _, err := m.buildModelFRTReport(context.Background(), key, "", modelFRTTestNow); err == nil {
			t.Fatal("bad key accepted")
		}
	}
	m.cfg.RetentionDays = 1
	if _, err := m.buildModelFRTReport(context.Background(), "3d", "", modelFRTTestNow); err == nil {
		t.Fatal("retention ignored")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := m.buildModelFRTReport(ctx, "24h", "", modelFRTTestNow); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestModelFRTReadFailuresPropagate(t *testing.T) {
	for _, table := range []any{&CapacityUserMinuteSample{}, &CustomerHealthDayCoverage{}, &CustomerHealthSourceCursor{}, &ChannelSnap{}, &MetricSample{}} {
		t.Run(fmt.Sprintf("%T", table), func(t *testing.T) {
			_, normal := table.(*MetricSample)
			m, _ := modelFRTFixture(t, !normal)
			if err := m.storeDB.Migrator().DropTable(table); err != nil {
				t.Fatal(err)
			}
			if _, err := m.buildModelFRTReport(context.Background(), "24h", "", modelFRTTestNow); err == nil {
				t.Fatal("query failure became complete/empty report")
			}
		})
	}
}

func TestModelFRTLeafLimitFailsWithoutPartialReport(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	err := m.storeDB.Exec(`WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x+1 FROM n WHERE x < ?)
 INSERT INTO capacity_user_minute_samples (bucket_ts,user_id,channel_id,grp,model_name,success,anomaly,failed,traffic_class_version,ttft_semantics_version)
 SELECT ?,1,x,'g','m',1,0,0,?,? FROM n`, modelFRTMaxLeaves+1, end-60, stabilityTrafficClassificationVersion, ttftCoverageSemanticsVersion).Error
	if err != nil {
		t.Fatal(err)
	}
	r, err := m.buildModelFRTReport(context.Background(), "24h", "", modelFRTTestNow)
	if err == nil || len(r.Channels) != 0 {
		t.Fatal("silently truncated", len(r.Channels), err)
	}
	id := modelFRTMaxLeaves + 1
	r, err = m.buildModelFRTReportFiltered(context.Background(), "24h", "", ModelFRTFilter{ChannelID: &id}, modelFRTTestNow)
	if err != nil || len(r.Channels) != 1 || r.Channels[0].ChannelID != id {
		t.Fatal("last dimension unreachable", r, err)
	}
}

func TestModelFRTHTTPAuthReadOnlyAndErrors(t *testing.T) {
	m, _ := modelFRTFixture(t, true)
	m.cfg.SessionSecret = "frt-local-fixture"
	r := gin.New()
	m.RegisterRoutes(r)
	request := func(role int, path string) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, path, nil)
		req.Header.Set("Accept", "application/json")
		if role >= 0 {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("fixture", role, time.Now().Unix())})
		}
		w := httptest.NewRecorder()
		r.ServeHTTP(w, req)
		return w
	}
	for _, tc := range []struct{ role, code int }{{-1, 401}, {1, 403}, {roleAdmin, 200}, {roleRoot, 200}} {
		w := request(tc.role, "/model-statistics/frt?window=24h")
		if w.Code != tc.code {
			t.Fatalf("role %d: %d %s", tc.role, w.Code, w.Body.String())
		}
	}
	for _, path := range []string{"/model-statistics/frt?window=8d", "/model-statistics/frt?to=bad", "/model-statistics/frt?channel_id=-1", "/model-statistics/frt?channel_id=1.5", "/model-statistics/frt?group=" + strings.Repeat("a", 513)} {
		if w := request(roleAdmin, path); w.Code != 400 || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
			t.Fatal(w.Code, w.Body.String())
		}
	}
	db, err := m.storeDB.DB()
	if err != nil {
		t.Fatal(err)
	}
	db.SetMaxOpenConns(1)
	if err = m.storeDB.Exec("PRAGMA query_only=ON").Error; err != nil {
		t.Fatal(err)
	}
	if w := request(roleAdmin, "/model-statistics/frt"); w.Code != 200 || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
		t.Fatal(w.Code, w.Body.String())
	}
	if err = m.storeDB.Exec("PRAGMA query_only=OFF").Error; err != nil {
		t.Fatal(err)
	}
	if err = m.storeDB.Migrator().DropTable(&ChannelSnap{}); err != nil {
		t.Fatal(err)
	}
	if w := request(roleAdmin, "/model-statistics/frt"); w.Code != 503 || strings.Contains(w.Body.String(), "no such table") {
		t.Fatal(w.Code, w.Body.String())
	}
}

func TestModelFRTFiltersRecomputeParentsAndReachAllModels(t *testing.T) {
	m, end := modelFRTFixture(t, true)
	modelFRTInsert(t, m, end, 1, "g", "a", 99, 400)
	modelFRTInsert(t, m, end, 1, "g", "b", 1, 4000)
	modelFRTInsert(t, m, end, 2, "g", "b", 2, 4000)
	channel, group, model := 1, "g", "b"
	r, err := m.buildModelFRTReportFiltered(context.Background(), "24h", "", ModelFRTFilter{ChannelID: &channel, Group: &group, Model: &model}, modelFRTTestNow)
	if err != nil {
		t.Fatal(err)
	}
	if len(r.Channels) != 1 || *r.Channels[0].FRT.Observed != 1 || *r.Channels[0].FRT.Over3s != 1 || *r.Channels[0].FRT.Over3sPct != 100 || len(r.Channels[0].Groups[0].Models) != 1 {
		t.Fatalf("filter aggregation: %+v", r)
	}
}
