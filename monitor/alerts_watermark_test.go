package monitor

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func serveAlertsWatermarkTest(t *testing.T, m *Monitor, now time.Time, query string) (AlertsResponse, int) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, "/alerts/rejections?"+query, nil)
	m.getRejectAlertsAt(c, now)
	var response AlertsResponse
	if w.Code == http.StatusOK {
		if err := json.Unmarshal(w.Body.Bytes(), &response); err != nil {
			t.Fatal(err)
		}
	}
	return response, w.Code
}

func TestAlertsWatermarkTodayUsesOneContinuousWindow(t *testing.T) {
	now := time.Date(2026, 10, 8, 15, 58, 40, 0, cstLocation)
	day := time.Date(2026, 10, 8, 0, 0, 0, 0, cstLocation).Unix()
	_, target := cloudWatchPreRouteRange(now, 168)
	for _, lag := range []int64{0, 60} {
		t.Run(strconv.FormatInt(lag, 10), func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			m.cfg.StabilityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
			m.cfg.CloudWatchPreRouteLookbackHours = 168
			m.cloudWatchPreRouteFrom.Store(day - 86400)
			through := target - lag
			m.cloudWatchPreRouteThrough.Store(through)
			rows := []RejectionSample{
				{BucketTs: through - 60, Node: cloudWatchPreRouteNode, Reason: "invalid_token", UserID: 0, Count: 5},
				{BucketTs: through - 120, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", UserID: 9, Count: 2},
				{BucketTs: through, Node: cloudWatchPreRouteNode, Reason: "unfinalized_tail", UserID: 0, Count: 999},
			}
			if err := m.storeDB.Create(&rows).Error; err != nil {
				t.Fatal(err)
			}
			query := "from=2026-10-08&to=2026-10-08&limit=1"
			first, status := serveAlertsWatermarkTest(t, m, now, query)
			if status != 200 || first.CoverageComplete || !first.StatisticsComplete || !first.StatisticsAvailable || !first.TailSyncing || first.Source != "cloudwatch" || first.Total != 7 || first.RowTotal != 2 || first.UnauthCount != 5 || first.ToTs != through || first.CutoffTs != through || first.FromTs != day || first.RequestedToTs != now.Unix() {
				t.Fatalf("unfinalized tail must not hide certified counts: status=%d %+v", status, first)
			}
			if len(first.Rows) != 1 || !first.HasMore || strings.Contains(strings.Join(first.ReasonOptions, ","), "unfinalized_tail") {
				t.Fatalf("all sections must use the same cutoff: %+v", first)
			}
			// A later poll and a later page must not silently change the first
			// page's aggregate window or include its previously excluded tail.
			m.cloudWatchPreRouteThrough.Store(target + 60)
			later := now.Add(time.Minute)
			second, status := serveAlertsWatermarkTest(t, m, later, query+"&cursor="+url.QueryEscape(first.NextCursor))
			if status != 200 || second.ToTs != through || second.Total != 7 || second.RowTotal != 2 || len(second.Rows) != 1 || second.HasMore || second.Rows[0].Ts >= first.Rows[0].Ts {
				t.Fatalf("pagination advanced the snapshot: status=%d %+v", status, second)
			}
			filtered, status := serveAlertsWatermarkTest(t, m, later, query+"&cutoff_ts="+strconv.FormatInt(through, 10)+"&reason=invalid_token&user_id=0")
			if status != 200 || filtered.ToTs != through || filtered.Total != 5 || filtered.RowTotal != 1 || filtered.UnauthCount != 5 {
				t.Fatalf("filters must retain the displayed cutoff: status=%d %+v", status, filtered)
			}
		})
	}
}

func TestAlertsWatermarkHistoricalCompleteDayHasNoTail(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.StabilityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
	m.cfg.CloudWatchPreRouteLookbackHours = 168
	now := time.Date(2026, 10, 8, 15, 58, 40, 0, cstLocation)
	start := time.Date(2026, 10, 7, 0, 0, 0, 0, cstLocation).Unix()
	m.cloudWatchPreRouteFrom.Store(start - 86400)
	m.cloudWatchPreRouteThrough.Store(start + 86400)
	m.cloudWatchPreRouteLastFailure.Store(now.Unix()) // Today's failure is not yesterday's gap.
	r, status := serveAlertsWatermarkTest(t, m, now, "from=2026-10-07&to=2026-10-07")
	if status != 200 || !r.CoverageComplete || !r.StatisticsComplete || r.TailSyncing || r.CollectionFailed || r.ToTs != start+86400 || r.CoverageNote != "" || r.Total != 0 {
		t.Fatalf("complete historical zero should remain complete: status=%d %+v", status, r)
	}
	// A deliberately pinned shorter historical window is not live syncing.
	pinned, status := serveAlertsWatermarkTest(t, m, now, "from=2026-10-07&to=2026-10-07&cutoff_ts="+strconv.FormatInt(start+3600, 10))
	if status != 200 || pinned.CoverageComplete || !pinned.StatisticsComplete || pinned.TailSyncing || pinned.CollectionFailed {
		t.Fatalf("historical snapshot must not claim a syncing tail: status=%d %+v", status, pinned)
	}
}

func TestAlertsWatermarkRealGapsMixAndFailureStayVisible(t *testing.T) {
	now := time.Date(2026, 10, 8, 15, 58, 40, 0, cstLocation)
	day := time.Date(2026, 10, 8, 0, 0, 0, 0, cstLocation).Unix()
	_, target := cloudWatchPreRouteRange(now, 168)
	for _, mode := range []string{"first_collection", "missing_start", "prefix_gap", "mixed", "failure"} {
		t.Run(mode, func(t *testing.T) {
			m := newTestMonitor(t)
			defer m.Close()
			m.cfg.StabilityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
			m.cfg.CloudWatchPreRouteLookbackHours = 168
			m.cloudWatchPreRouteFrom.Store(day)
			m.cloudWatchPreRouteThrough.Store(target)
			rows := []RejectionSample{{BucketTs: target - 120, Node: cloudWatchPreRouteNode, Reason: "route_no_channel", Count: 7}}
			switch mode {
			case "first_collection":
				m.cloudWatchPreRouteThrough.Store(day)
				m.cloudWatchPreRouteLastFailure.Store(now.Unix())
			case "missing_start":
				m.cloudWatchPreRouteFrom.Store(0)
				m.cloudWatchPreRouteLastFailure.Store(now.Unix())
			case "prefix_gap":
				m.cloudWatchPreRouteFrom.Store(day + 3600)
			case "mixed":
				rows = append(rows, RejectionSample{BucketTs: target - 120, Node: "legacy", Reason: "no_channel", Count: 3})
			case "failure":
				m.cloudWatchPreRouteThrough.Store(target - 60)
				m.cloudWatchPreRouteLastFailure.Store(now.Unix())
			}
			if err := m.storeDB.Create(&rows).Error; err != nil {
				t.Fatal(err)
			}
			r, status := serveAlertsWatermarkTest(t, m, now, "from=2026-10-08&to=2026-10-08")
			if status != 200 || r.CoverageComplete || !r.StatisticsAvailable || r.CoverageNote == "" || r.Total < 7 {
				t.Fatalf("known counts and incomplete warning must survive: status=%d %+v", status, r)
			}
			if mode == "failure" {
				if !r.StatisticsComplete || !r.CollectionFailed || !r.TailSyncing || !strings.Contains(r.CoverageNote, "失败") {
					t.Fatalf("failure must preserve proven prefix and show failure: %+v", r)
				}
			} else if r.StatisticsComplete {
				t.Fatalf("real gap/mix certified complete: %+v", r)
			}
			if (r.Source == "mixed") != (mode == "mixed") {
				t.Fatalf("only real legacy rows should cause mixed: %+v", r)
			}
			if mode == "first_collection" || mode == "missing_start" {
				if !r.CollectionFailed || !strings.Contains(r.CoverageNote, "采集最近一次失败") || strings.Contains(r.CoverageNote, "已发布的连续区间仍可查看") {
					t.Fatalf("no proven prefix must not hide collection failure or invent continuity: %+v", r)
				}
			}
		})
	}
}

func TestAlertsWatermarkSourceCheckFailureDoesNotInventMixed(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.CloudWatchPreRouteEnabled = true
	m.cfg.CloudWatchPreRouteLookbackHours = 168
	now := time.Date(2026, 10, 8, 15, 58, 40, 0, cstLocation)
	from, target := cloudWatchPreRouteRange(now, 168)
	m.cloudWatchPreRouteFrom.Store(from)
	m.cloudWatchPreRouteThrough.Store(target)
	if err := m.storeDB.Migrator().DropTable(&RejectionSample{}); err != nil {
		t.Fatal(err)
	}
	coverage := m.alertsCoverage(stabilityScope{FromTs: target - 3600, ToTs: target}, now)
	if coverage.Complete || coverage.Source != "unknown" || !strings.Contains(coverage.Note, "无法确认") {
		t.Fatalf("read failure is unknown source, not proof of actual mixed facts: %+v", coverage)
	}
}

func TestAlertsWatermarkRejectsUnprovenCutoff(t *testing.T) {
	m := newTestMonitor(t)
	defer m.Close()
	m.cfg.StabilityEnabled, m.cfg.CloudWatchPreRouteEnabled = true, true
	m.cfg.CloudWatchPreRouteLookbackHours = 168
	now := time.Date(2026, 10, 8, 15, 58, 40, 0, cstLocation)
	day := time.Date(2026, 10, 8, 0, 0, 0, 0, cstLocation).Unix()
	_, target := cloudWatchPreRouteRange(now, 168)
	m.cloudWatchPreRouteFrom.Store(day)
	m.cloudWatchPreRouteThrough.Store(target)
	for _, cutoff := range []int64{day, day - 60, target + 60, now.Unix() + 60, target - 1} {
		_, status := serveAlertsWatermarkTest(t, m, now, "from=2026-10-08&to=2026-10-08&cutoff_ts="+strconv.FormatInt(cutoff, 10))
		if status != http.StatusBadRequest {
			t.Fatalf("cutoff=%d status=%d; must reject unproven/out-of-scope cutoff", cutoff, status)
		}
	}
}
