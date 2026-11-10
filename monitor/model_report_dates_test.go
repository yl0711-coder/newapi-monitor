package monitor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func modelDatesSelection(t *testing.T, from, to int64, mode string) modelReportSelection {
	t.Helper()
	s, err := modelReportSelectionFor(url.Values{"from": {time.Unix(from, 0).UTC().Format(time.RFC3339)}, "to": {time.Unix(to, 0).UTC().Format(time.RFC3339)}, "range": {mode}})
	if err != nil {
		t.Fatal(err)
	}
	return s
}
func modelDatesBoth(t *testing.T, m *Monitor, s modelReportSelection, now time.Time) (ModelStatisticsReport, ModelFRTReport) {
	t.Helper()
	requests, err := m.buildModelStatisticsSelected(context.Background(), s, now)
	if err != nil {
		t.Fatal(err)
	}
	frt, err := m.buildModelFRTSelected(context.Background(), s, ModelFRTFilter{}, now)
	if err != nil {
		t.Fatal(err)
	}
	return requests, frt
}

func TestModelDatesNaturalBoundariesAndAdjacentDays(t *testing.T) {
	for _, now := range []time.Time{time.Date(2026, 1, 2, 12, 0, 0, 0, cstLocation), time.Date(2026, 3, 2, 12, 0, 0, 0, cstLocation), time.Date(2024, 3, 1, 12, 0, 0, 0, cstLocation)} {
		for _, independent := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/%t", now.Format("2006-01-02"), independent), func(t *testing.T) {
				end := customerHealthSourceFinalizedThrough(now)
				m := modelStatisticsWindowFixture(t, independent, now, end, end, end)
				day := customerHealthDayStart(now.Unix()) - 86400
				for _, at := range []int64{day - 60, day, day + 86400 - 60, day + 86400} {
					modelStatisticsWindowInsertFRT(t, m, at, fmt.Sprint(at))
				}
				first, frt := modelDatesBoth(t, m, modelDatesSelection(t, day, day+86400, "calendar"), now)
				if first.FromTs != day || first.ToTs != day+86400 || first.LagSeconds != 0 || !first.Source.RequestsComplete || !frt.Coverage.Complete || len(first.Models) != 2 || *frt.Channels[0].FRT.Observed != 4 {
					t.Fatalf("natural day boundary/coverage: %+v %+v", first, frt)
				}
				second, _ := modelDatesBoth(t, m, modelDatesSelection(t, day-86400, day, "calendar"), now)
				if len(second.Models) != 1 || second.Models[0].Requests != 2 {
					t.Fatal("adjacent day lost boundary", second)
				}
				combined, _ := modelDatesBoth(t, m, modelDatesSelection(t, day-86400, day+86400, "calendar"), now)
				var total int64
				for _, v := range combined.Models {
					total += v.Requests
				}
				if total != 6 {
					t.Fatal("cross day duplicated boundary", total)
				}
				if first.Models[0].FRTP95Ms != *frt.Channels[0].Groups[0].Models[0].FRT.P95Ms {
					t.Fatal("shared FRT histogram diverged")
				}
			})
		}
	}
}

func TestModelDatesTodayKeepsMidnightAndIndependentCutoffs(t *testing.T) {
	now := modelFRTTestNow
	target := customerHealthSourceFinalizedThrough(now)
	day := customerHealthDayStart(now.Unix())
	for _, through := range []int64{target - 60, day, day - 60} {
		t.Run(fmt.Sprint(through), func(t *testing.T) {
			m := modelStatisticsWindowFixture(t, true, now, target, target, through)
			modelStatisticsWindowInsertFRT(t, m, day+60, "today")
			modelStatisticsWindowInsertFRT(t, m, day-60, "yesterday")
			requests, frt := modelDatesBoth(t, m, modelDatesSelection(t, day, day+86400, "calendar"), now)
			if requests.FromTs != day || frt.From.Unix() != day || requests.ToTs != max(day, through) || frt.To.Unix() != target || !requests.Range.TodayOpen || !frt.Range.TodayOpen {
				t.Fatalf("today moved or shared rejection cutoff: %+v %+v", requests.Range, frt.Range)
			}
			if len(frt.Channels) != 1 || len(frt.Channels[0].Groups[0].Models) != 1 || frt.Channels[0].Groups[0].Models[0].Model != "today" {
				t.Fatal("FRT used another date", frt)
			}
			if through <= day && (requests.Range.HasWindow || requests.Source.RequestsComplete || len(requests.Models) != 0) {
				t.Fatal("unknown became zero complete", requests)
			}
			// Explicit comparison preserves both endpoints, even when the target
			// page lacks coverage; it never silently falls back to a rolling day.
			fixed := modelDatesSelection(t, frt.From.Unix(), frt.To.Unix(), "exact")
			comp, err := m.buildModelStatisticsSelected(context.Background(), fixed, now)
			if err != nil || comp.FromTs != day || comp.ToTs != target || comp.ActualThroughTs != through || comp.TargetTs != target || comp.LagSeconds != target-through || comp.Source.RequestsComplete {
				t.Fatalf("comparison secretly changed interval: %+v %v", comp, err)
			}
		})
	}
	// At exactly midnight the day is legal, but no finalized current-day window exists.
	m := modelStatisticsWindowFixture(t, true, time.Unix(day, 0), day-60, day-60, day-60)
	r, f := modelDatesBoth(t, m, modelDatesSelection(t, day, day+86400, "calendar"), time.Unix(day, 0))
	if r.Range.HasWindow || f.Range.HasWindow {
		t.Fatal("midnight invented data")
	}
}

func TestModelDatesGapKnownFactsEmptyAndDisabled(t *testing.T) {
	now := modelFRTTestNow
	end := customerHealthSourceFinalizedThrough(now)
	day := customerHealthDayStart(now.Unix()) - 86400
	m := modelStatisticsWindowFixture(t, true, now, end, end, end)
	s := modelDatesSelection(t, day, day+86400, "calendar")
	r, f := modelDatesBoth(t, m, s, now)
	if len(r.Models) != 0 || len(f.Channels) != 0 || !r.Source.RequestsComplete || !f.Coverage.Complete {
		t.Fatal("proven empty day was lost")
	}
	modelFRTMeanInsert(t, m, day+120, 1, "actual", "fast", 99, 400)
	modelFRTMeanInsert(t, m, day+120, 1, "actual", "slow", 1, 4000)
	modelFRTInsert(t, m, day+120, 2, "actual", "historical-sum", 1, 400)
	var original CustomerHealthDayCoverage
	if err := m.storeDB.First(&original, "day_ts=?", day).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Delete(&original).Error; err != nil {
		t.Fatal(err)
	}
	r, f = modelDatesBoth(t, m, s, now)
	if r.Source.RequestsComplete || f.Coverage.Complete || len(r.Models) != 3 || *f.Channels[0].FRT.MeanMs != 436 || f.Channels[1].FRT.MeanMs != nil || f.From.Unix() != day || f.To.Unix() != day+86400 {
		t.Fatal("hole hid known evidence, sorting or date", r, f)
	}
	if err := m.storeDB.Create(&original).Error; err != nil {
		t.Fatal(err)
	}
	r, f = modelDatesBoth(t, m, s, now)
	if !r.Source.RequestsComplete || !f.Coverage.Complete {
		t.Fatal("coverage did not recover")
	}
	m.cfg.CustomerHealthSourceEnabled = false
	m.cfg.CapacityEnabled = false
	r, f = modelDatesBoth(t, m, s, now)
	if r.Source.RequestsComplete || f.Coverage.Complete || len(f.Channels) != 2 {
		t.Fatal("disabled source fabricated completeness or lost known facts")
	}
}

func TestModelDatesHistoricalPartialAndSameRangeFilters(t *testing.T) {
	now := modelFRTTestNow
	day := customerHealthDayStart(now.Unix()) - 86400
	m := modelStatisticsWindowFixture(t, true, now, day+3600, day+3600, day+3600)
	modelFRTMeanInsert(t, m, day+120, 1, "actual", "a", 99, 400)
	modelFRTMeanInsert(t, m, day+120, 1, "actual", "b", 1, 4000)
	s := modelDatesSelection(t, day, day+86400, "calendar")
	r, f := modelDatesBoth(t, m, s, now)
	if r.ToTs != day+3600 || r.ActualThroughTs != day+3600 || f.To.Unix() != day+3600 || f.ActualThroughTs != day+3600 || r.Source.RequestsComplete || f.Coverage.Complete || r.Range.RequestedTo.Unix() != day+86400 {
		t.Fatal("partial day declared complete", r, f)
	}
	fixed := modelDatesSelection(t, r.FromTs, r.ToTs, "exact")
	id, group, model := 1, "actual", "b"
	filtered, err := m.buildModelFRTSelected(context.Background(), fixed, ModelFRTFilter{ChannelID: &id, Group: &group, Model: &model}, now)
	if err != nil || *filtered.Channels[0].FRT.Observed != 1 || *filtered.Channels[0].FRT.MeanMs != 4000 || filtered.From.Unix() != r.FromTs || filtered.To.Unix() != r.ToTs {
		t.Fatal("filter or same-range mismatch", filtered, err)
	}
}

func TestModelDatesStrictBoundsAndRetention(t *testing.T) {
	m, _ := modelFRTFixture(t, true)
	now := modelFRTTestNow
	day := customerHealthDayStart(now.Unix())
	utc := func(at int64) string { return time.Unix(at, 0).UTC().Format(time.RFC3339) }
	bad := []url.Values{
		{"from": {utc(day)}}, {"from": {""}, "to": {utc(day + 86400)}}, {"from": {utc(day)}, "to": {utc(day)}},
		{"from": {utc(day)}, "to": {utc(day + 86400)}, "window": {"24h"}},
		{"from": {utc(day)}, "to": {utc(day + 86400)}, "range": {"unknown"}},
		{"from": {utc(day), utc(day)}, "to": {utc(day + 86400)}},
		{"window": {"24h", "3d"}}, {"range": {"calendar"}},
		{"from": {utc(day + 1)}, "to": {utc(day + 86400)}},
		{"from": {time.Unix(day, 0).In(cstLocation).Format(time.RFC3339)}, "to": {utc(day + 86400)}},
		{"from": {utc(day - 8*86400)}, "to": {utc(day)}},
		{"from": {utc(day + 60)}, "to": {utc(day + 86400)}, "range": {"calendar"}},
	}
	for _, q := range bad {
		if _, err := modelReportSelectionFor(q); err == nil {
			t.Fatalf("accepted invalid parameters %v", q)
		}
	}
	for _, s := range []modelReportSelection{
		modelDatesSelection(t, day+86400, day+2*86400, "calendar"),
		modelDatesSelection(t, day, day+86400, "exact"),
		modelDatesSelection(t, day-11*86400, day-10*86400, "calendar"),
	} {
		if _, err := m.buildModelStatisticsSelected(context.Background(), s, now); err == nil {
			t.Fatal("invalid bound accepted by request report", s)
		}
		if _, err := m.buildModelFRTSelected(context.Background(), s, ModelFRTFilter{}, now); err == nil {
			t.Fatal("invalid bound accepted by FRT", s)
		}
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	s := modelDatesSelection(t, day-86400, day, "calendar")
	if _, err := m.buildModelStatisticsSelected(ctx, s, now); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if _, err := m.buildModelFRTSelected(ctx, s, ModelFRTFilter{}, now); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
}

func TestModelDatesHTTPReadOnlyAuthErrorsAndCompatibility(t *testing.T) {
	now := time.Now()
	target := customerHealthSourceFinalizedThrough(now)
	day := customerHealthDayStart(now.Unix()) - 86400
	m := modelStatisticsWindowFixture(t, true, now, target, target, target)
	m.cfg.SessionSecret = "model-dates-local-fixture"
	gin.SetMode(gin.TestMode)
	router := gin.New()
	m.RegisterRoutes(router)
	q := url.Values{"from": {time.Unix(day, 0).UTC().Format(time.RFC3339)}, "to": {time.Unix(day+86400, 0).UTC().Format(time.RFC3339)}, "range": {"calendar"}}
	request := func(endpoint, query string, role int) *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodGet, endpoint+"?"+query, nil)
		req.Header.Set("Accept", "application/json")
		if role >= 0 {
			req.AddCookie(&http.Cookie{Name: sessionCookie, Value: m.signSession("fixture", role, now.Unix())})
		}
		w := httptest.NewRecorder()
		router.ServeHTTP(w, req)
		return w
	}
	for _, endpoint := range []string{"/model-statistics/report", "/model-statistics/frt"} {
		for _, tc := range []struct{ role, code int }{{-1, 401}, {1, 403}, {roleAdmin, 200}} {
			if w := request(endpoint, q.Encode(), tc.role); w.Code != tc.code {
				t.Fatal(endpoint, w.Code, w.Body.String())
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
		for _, query := range []string{q.Encode(), "window=24h", "window=3d", "window=7d"} {
			w := request(endpoint, query, roleAdmin)
			if w.Code != 200 || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
				t.Fatal(w.Code, w.Body.String())
			}
			var data map[string]any
			if err = json.Unmarshal(w.Body.Bytes(), &data); err != nil || data["range"] == nil {
				t.Fatal("range metadata missing", err)
			}
		}
		if err = m.storeDB.Exec("PRAGMA query_only=OFF").Error; err != nil {
			t.Fatal(err)
		}
		for _, query := range []string{q.Encode() + "&window=24h", "from=bad&to=bad", "from=&to=", "window=24h&window=3d"} {
			if w := request(endpoint, query, roleAdmin); w.Code != 400 || !strings.Contains(w.Header().Get("Cache-Control"), "no-store") {
				t.Fatal("bad date request", endpoint, w.Code, w.Body.String())
			}
		}
	}
}

func TestModelDatesBothSourcesStalledThenRecover(t *testing.T) {
	now := modelFRTTestNow
	target := customerHealthSourceFinalizedThrough(now)
	day := customerHealthDayStart(now.Unix())
	m := modelStatisticsWindowFixture(t, true, now, day-60, day-60, day-60)
	modelStatisticsWindowInsertFRT(t, m, day-120, "yesterday")
	modelStatisticsWindowInsertFRT(t, m, day+60, "today")
	s := modelDatesSelection(t, day, day+86400, "calendar")
	r, f := modelDatesBoth(t, m, s, now)
	if r.Range.HasWindow || f.Range.HasWindow || len(r.Models) != 0 || len(f.Channels) != 0 || r.FromTs != day || f.From.Unix() != day {
		t.Fatal("stalled sources used yesterday or unfinalized rows", r, f)
	}
	if err := m.storeDB.Model(&CustomerHealthSourceCursor{}).Where("id=1").Updates(map[string]any{"day_ts": day, "through_ts": target, "ttft_coverage_from_ts": day, "ttft_coverage_through_ts": target}).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Model(&CloudWatchPreRouteCursor{}).Where("id=?", cloudWatchPreRouteCursorID).Updates(map[string]any{"through_ts": target, "next_ts": target}).Error; err != nil {
		t.Fatal(err)
	}
	r, f = modelDatesBoth(t, m, s, now)
	if !r.Range.HasWindow || !f.Range.HasWindow || r.FromTs != day || f.From.Unix() != day || r.ToTs != target || f.To.Unix() != target || len(r.Models) != 1 || r.Models[0].Model != "today" || !r.Source.RequestsComplete || !f.Coverage.Complete {
		t.Fatal("recovery lost the requested day", r, f)
	}
}
