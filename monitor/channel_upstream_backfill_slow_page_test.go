package monitor

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// slowWindowProxy fronts a usage fixture and delays every request whose
// queried window is wider than slowAbove seconds, like a source whose
// whole-hour log query is slow while small time ranges stay fast.
func slowWindowProxy(t *testing.T, backend *httptest.Server, slowAbove int64, delay time.Duration) *httptest.Server {
	t.Helper()
	target, err := url.Parse(backend.URL)
	if err != nil {
		t.Fatal(err)
	}
	proxy := httputil.NewSingleHostReverseProxy(target)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		from, _ := strconv.ParseInt(q.Get("start_timestamp"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("end_timestamp"), 10, 64)
		if end-from+1 > slowAbove {
			select {
			case <-time.After(delay):
			case <-r.Context().Done():
				return
			}
		}
		proxy.ServeHTTP(w, r)
	}))
	t.Cleanup(server.Close)
	return server
}

func withHistoryRequestTimeout(t *testing.T, timeout time.Duration) {
	t.Helper()
	previous := upstreamUsageHistoryRequestTimeout
	upstreamUsageHistoryRequestTimeout = timeout
	t.Cleanup(func() { upstreamUsageHistoryRequestTimeout = previous })
}

// The history request timeout must leave room inside the lane budget;
// otherwise a slow page is always cut off by the lane deadline and booked as
// a budget yield, and the adaptive split never runs.
func TestUpstreamUsageHistoryRequestTimeoutFitsLaneBudget(t *testing.T) {
	if upstreamUsageHistoryRequestTimeout+upstreamGuardMinSendBudget > upstreamUsageHistoryOperationTimeout-5*time.Second {
		t.Fatalf("history request timeout %s does not fit lane budget %s", upstreamUsageHistoryRequestTimeout, upstreamUsageHistoryOperationTimeout)
	}
	m := newChannelUpstreamTestMonitor(t)
	m.cfg.UpstreamUsageTimeoutSec = 45
	transport, ok := m.channelUpstreamUsageHistoryHTTPClient().Transport.(*upstreamGuardTransport)
	if !ok || transport.requestTimeout != upstreamUsageHistoryRequestTimeout {
		t.Fatalf("history client timeout=%v ok=%v", transport, ok)
	}
	m.cfg.UpstreamUsageTimeoutSec = 20
	transport, _ = m.channelUpstreamUsageHistoryHTTPClient().Transport.(*upstreamGuardTransport)
	if transport.requestTimeout != 20*time.Second {
		t.Fatalf("lower configured usage timeout not kept: %s", transport.requestTimeout)
	}
}

// fastrelay.org / last-api.ai, 2026-11: the first whole-hour page never
// answers within the lane budget. Previously each round yielded without a
// checkpoint, so neither the timeout split nor the stalled-checkpoint rule
// applied and the hour was retried forever. A request-local timeout must now
// switch the hour to five-minute children, which then complete.
func TestNewAPIHistorySlowFirstPageSwitchesToSegments(t *testing.T) {
	withHistoryRequestTimeout(t, 300*time.Millisecond)
	from := time.Date(2026, 11, 1, 2, 0, 0, 0, cstLocation).Unix()
	rows := usageFixtureRows(240, from, 3600)
	backend, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	front := slowWindowProxy(t, backend, int64(newAPIBackfillAdaptiveSegment/time.Second), 3*time.Second)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: front.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: front.URL, UserID: 31, BalanceUnit: 500000,
	}
	credential := newAPICredential{AccessToken: "usage-token"}

	laneCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_, progress, complete, err := m.syncNewAPIUsageBackfillWindow(laneCtx, row, credential, from, from+3600, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0))
	cancel()
	if err != nil || complete || !strings.Contains(progress, "整小时查询超时，已持久降档") {
		t.Fatalf("slow whole hour: complete=%v progress=%q err=%v", complete, progress, err)
	}
	var segments int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ?", row.Domain, from).Count(&segments).Error; err != nil || segments != 12 {
		t.Fatalf("segments=%d err=%v", segments, err)
	}

	var result upstreamUsageResult
	for round := 0; round < 5 && !complete; round++ {
		laneCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result, progress, complete, err = m.syncNewAPIUsageBackfillWindow(laneCtx, row, credential, from, from+3600, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0))
		cancel()
		if err != nil {
			t.Fatalf("segment round %d: %v", round, err)
		}
	}
	if !complete || len(result.Hours) != 1 || result.Hours[0].Requests != int64(len(rows)) || result.Hours[0].BucketSeconds != 3600 {
		t.Fatalf("segmented hour complete=%v progress=%q hours=%+v", complete, progress, result.Hours)
	}
}

// When even a five-minute child is slow, its own request-local timeout must
// split it further instead of yielding on the lane deadline forever.
func TestNewAPIHistorySlowSegmentSplitsFurther(t *testing.T) {
	withHistoryRequestTimeout(t, 300*time.Millisecond)
	from := time.Date(2026, 11, 1, 3, 0, 0, 0, cstLocation).Unix()
	rows := usageFixtureRows(60, from, 3600)
	backend, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	front := slowWindowProxy(t, backend, 60, 3*time.Second)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: front.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: front.URL, UserID: 31, BalanceUnit: 500000,
	}
	credential := newAPICredential{AccessToken: "usage-token"}

	var (
		result   upstreamUsageResult
		progress string
		complete bool
		err      error
	)
	for round := 0; round < 40 && !complete; round++ {
		laneCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		result, progress, complete, err = m.syncNewAPIUsageBackfillWindow(laneCtx, row, credential, from, from+3600, newUpstreamUsageRequestPacer(upstreamUsageHistoryMaxRequestsPerRun, 0))
		cancel()
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
	}
	if !complete || len(result.Hours) != 1 || result.Hours[0].Requests != int64(len(rows)) {
		t.Fatalf("hour complete=%v progress=%q hours=%+v", complete, progress, result.Hours)
	}
	var minute int64
	if err := m.storeDB.Model(&NewAPIUsageBackfillSegment{}).Where("domain = ? AND hour_from = ? AND segment_to - segment_from = 60", row.Domain, from).Count(&minute).Error; err != nil || minute == 0 {
		t.Fatalf("slow five-minute children were not split: minute segments=%d err=%v", minute, err)
	}
}
