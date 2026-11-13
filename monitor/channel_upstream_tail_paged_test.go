package monitor

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync/atomic"
	"testing"
	"time"
)

// A busy account whose closed hour holds more rows than one bounded tail round
// can page through (60 requests x 100 rows). Before the fix that hour could
// never be published and the account fell behind with growing backoff.
func TestNewAPITailDenseClosedHourCompletesAcrossRounds(t *testing.T) {
	from := time.Date(2026, 11, 11, 15, 0, 0, 0, cstLocation).Unix()
	now := from + 2*3600 + 600 // 17:10, so 15:00 and 16:00 are closed
	rows := append(usageFixtureRows(6500, from, 3600), usageFixtureRows(40, from+3600, 3600)...)
	for i := range rows {
		rows[i].ID = int64(i + 1)
	}
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: from,
		UsageTailMode: upstreamUsageTailModeHourly,
	}
	credential := newAPICredential{AccessToken: "usage-token"}
	// The history lane owns its own checkpoint; the live lane must not touch it.
	history := NewAPIUsageBackfillCheckpoint{Domain: row.Domain, WindowFrom: from - 30*86400, WindowTo: from - 30*86400 + 3600, NextPage: 3, Total: 250, SourceRows: 200, FirstPageFingerprint: "aa"}
	for len(history.FirstPageFingerprint) < 64 {
		history.FirstPageFingerprint += "a"
	}
	if err := m.saveNewAPIUsageBackfillCheckpoint(context.Background(), &history); err != nil {
		t.Fatal(err)
	}

	round := func() (upstreamUsageResult, bool, bool) {
		t.Helper()
		pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
		result, yielded, paged, err := m.syncNewAPITailIncremental(context.Background(), row, credential, from, now, now, pacer)
		if err != nil {
			t.Fatalf("tail round returned failure: %v", err)
		}
		if pacer.calls > upstreamUsageMaxRequestsPerRun {
			t.Fatalf("round exceeded request budget: %d", pacer.calls)
		}
		if paged {
			row.UsageTailMode = upstreamUsageTailModePaged
		}
		return result, yielded, paged
	}

	// Round 1: the ordinary single-read path exhausts the budget on the closed
	// hour. That is a yield that switches the account to paged hours.
	result, yielded, paged := round()
	if !yielded || !paged || result.DataUntil != from {
		t.Fatalf("round 1 yielded=%v paged=%v until=%d", yielded, paged, result.DataUntil)
	}
	// Round 2: paged read saves durable progress but cannot finish 66 pages.
	result, yielded, _ = round()
	var tailCP NewAPIUsageTailCheckpoint
	if err := m.storeDB.First(&tailCP, "domain = ?", row.Domain).Error; err != nil {
		t.Fatalf("live-lane checkpoint missing: %v", err)
	}
	if !yielded || result.DataUntil != from || tailCP.WindowFrom != from || tailCP.NextPage <= 2 {
		t.Fatalf("round 2 yielded=%v until=%d checkpoint=%+v", yielded, result.DataUntil, tailCP.NewAPIUsageBackfillCheckpoint)
	}
	var published int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", row.Domain).Count(&published).Error; err != nil || published != 0 {
		t.Fatalf("partial dense hour became public: rows=%d err=%v", published, err)
	}
	// Round 3: resume finishes the dense hour and the following hours.
	result, yielded, _ = round()
	if yielded || result.DataUntil != now {
		t.Fatalf("round 3 yielded=%v until=%d want %d", yielded, result.DataUntil, now)
	}
	var dense ChannelUpstreamUsageHour
	if err := m.storeDB.First(&dense, "domain = ? AND hour_ts = ?", row.Domain, from).Error; err != nil || dense.Requests != 6500 {
		t.Fatalf("dense hour=%+v err=%v", dense, err)
	}
	var next ChannelUpstreamUsageHour
	if err := m.storeDB.First(&next, "domain = ? AND hour_ts = ?", row.Domain, from+3600).Error; err != nil || next.Requests != 40 {
		t.Fatalf("following hour=%+v err=%v", next, err)
	}
	if err := m.storeDB.Model(&NewAPIUsageTailCheckpoint{}).Where("domain = ?", row.Domain).Count(&published).Error; err != nil || published != 0 {
		t.Fatalf("live-lane checkpoint not cleared: rows=%d err=%v", published, err)
	}
	var kept NewAPIUsageBackfillCheckpoint
	if err := m.storeDB.First(&kept, "domain = ?", row.Domain).Error; err != nil || kept.WindowFrom != history.WindowFrom || kept.NextPage != 3 {
		t.Fatalf("history checkpoint disturbed: %+v err=%v", kept, err)
	}
}

// The open hour alone exceeds the round budget: earlier hours are published,
// the watermark stays, and the round is not a failure or a fast-retry yield.
func TestNewAPITailDenseOpenHourIsNotAFailure(t *testing.T) {
	hour := time.Date(2026, 11, 11, 16, 0, 0, 0, cstLocation).Unix()
	now := hour + 3000
	rows := usageFixtureRows(6500, hour, 2900)
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: server.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: hour,
		UsageTailMode: upstreamUsageTailModeHourly,
	}
	pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	result, yielded, paged, err := m.syncNewAPITailIncremental(context.Background(), row, newAPICredential{AccessToken: "usage-token"}, hour, now, now, pacer)
	if err != nil || yielded || !paged || result.DataUntil != hour {
		t.Fatalf("open dense hour yielded=%v paged=%v until=%d err=%v", yielded, paged, result.DataUntil, err)
	}
	var cps int64
	if err := m.storeDB.Model(&NewAPIUsageTailCheckpoint{}).Count(&cps).Error; err != nil || cps != 0 {
		t.Fatalf("open hour must not be checkpointed: rows=%d err=%v", cps, err)
	}
}

// End to end through the background worker: a dense closed hour no longer
// records a failure or exponential backoff, and the account catches up.
func TestNewAPITailWorkerDenseHourYieldsAndCatchesUp(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := time.Now().Unix()
	currentHour := now - now%3600
	// The live lane only reads today (CST); history owns earlier days. Put the
	// dense closed hour inside today regardless of when the suite runs.
	dense := currentHour - 2*3600
	if dense < cstDayStart(now) {
		dense = currentHour - 3600
	}
	if dense < cstDayStart(now) {
		t.Skip("no closed CST hour today yet (00:00-00:59); the live lane has nothing closed to page")
	}
	rows := usageFixtureRows(6500, dense, 3600)
	if dense+3600 < currentHour {
		rows = append(rows, usageFixtureRows(30, dense+3600, 3600)...)
	}
	for i := range rows {
		rows[i].ID = int64(i + 1)
	}
	server, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	row := ChannelUpstreamAccount{
		Domain: server.Listener.Addr().String(), Provider: upstreamProviderNewAPI, BaseURL: server.URL,
		Account: "31", UserID: 31, BalanceUnit: 500000, Enabled: true, UsageSyncEnabled: true,
		UsageStatus: upstreamStatusOK, UsageLastSuccessAt: dense, UsageDataUntil: dense,
		UsageTailMode: upstreamUsageTailModeHourly, UsageBackfillDone: true,
	}
	if err := m.persistSyncedUpstreamAccount(context.Background(), &row, newAPICredential{AccessToken: "usage-token"}); err != nil {
		t.Fatal(err)
	}
	var stored ChannelUpstreamAccount
	for i := 0; i < 6; i++ {
		if err := m.storeDB.Model(&ChannelUpstreamAccount{}).Where("domain = ?", row.Domain).Update("usage_next_sync_at", 0).Error; err != nil {
			t.Fatal(err)
		}
		if _, err := m.syncStoredUpstreamUsageLaneBackground(context.Background(), row.Domain, upstreamUsageLaneTail); err != nil {
			t.Fatalf("round %d returned failure: %v", i+1, err)
		}
		if err := m.storeDB.First(&stored, "domain = ?", row.Domain).Error; err != nil {
			t.Fatal(err)
		}
		if stored.UsageConsecutiveFails != 0 || stored.UsageStatus != upstreamStatusOK || stored.UsageLastError != "" {
			t.Fatalf("round %d recorded failure: status=%s fails=%d err=%q", i+1, stored.UsageStatus, stored.UsageConsecutiveFails, stored.UsageLastError)
		}
		if stored.UsageDataUntil >= currentHour {
			break
		}
		if stored.UsageNextSyncAt > time.Now().Unix()+60 {
			t.Fatalf("round %d delayed a yielding account: next=%d", i+1, stored.UsageNextSyncAt-time.Now().Unix())
		}
	}
	if stored.UsageDataUntil < currentHour || stored.UsageTailMode != upstreamUsageTailModePaged {
		t.Fatalf("account did not catch up: until=%d want>=%d mode=%q", stored.UsageDataUntil, currentHour, stored.UsageTailMode)
	}
	var hour ChannelUpstreamUsageHour
	if err := m.storeDB.First(&hour, "domain = ? AND hour_ts = ?", row.Domain, dense).Error; err != nil || hour.Requests != 6500 {
		t.Fatalf("dense hour=%+v err=%v", hour, err)
	}
}

func TestReleaseInheritedNewAPITailBudgetBackoff(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	now := int64(1_800_000_000)
	budget := legacyNewAPIBackfillBudgetError()
	rows := []ChannelUpstreamAccount{
		{Domain: "dense.example", Provider: upstreamProviderNewAPI, Status: upstreamStatusOK,
			UsageStatus: upstreamStatusError, UsageConsecutiveFails: 3, UsageLastError: budget, UsageNextSyncAt: now + 4*3600, UsageBackfillDone: true},
		{Domain: "timeout.example", Provider: upstreamProviderNewAPI, Status: upstreamStatusOK,
			UsageStatus: upstreamStatusError, UsageConsecutiveFails: 3, UsageLastError: "timeout", UsageNextSyncAt: now + 4*3600, UsageBackfillDone: true},
		{Domain: "tokenforce.example", Provider: upstreamProviderTokenForce, Status: upstreamStatusOK,
			UsageStatus: upstreamStatusError, UsageConsecutiveFails: 3, UsageLastError: budget, UsageNextSyncAt: now + 4*3600, UsageBackfillDone: true},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	released, err := m.releaseNonCredentialUpstreamIsolation(context.Background(), now)
	if err != nil || released != 1 {
		t.Fatalf("released=%d err=%v, want 1", released, err)
	}
	for domain, want := range map[string]int64{"dense.example": now, "timeout.example": now + 4*3600, "tokenforce.example": now + 4*3600} {
		var row ChannelUpstreamAccount
		if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
			t.Fatal(err)
		}
		if row.UsageNextSyncAt != want || row.UsageConsecutiveFails != 3 {
			t.Fatalf("%s next=%d fails=%d want next=%d", domain, row.UsageNextSyncAt, row.UsageConsecutiveFails, want)
		}
	}
	if again, err := m.releaseNonCredentialUpstreamIsolation(context.Background(), now+10); err != nil || again != 0 {
		t.Fatalf("release must be idempotent: %d %v", again, err)
	}
}

// Dense and slow: a paged whole-hour query times out (504). The round falls
// back to the single-read path, which splits slow windows, and completes.
func TestNewAPITailPagedHourFallsBackOnSlowSource(t *testing.T) {
	from := time.Date(2026, 11, 11, 15, 0, 0, 0, cstLocation).Unix()
	now := from + 3600 + 300
	rows := usageFixtureRows(200, from, 3600)
	backend, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	var wholeHour atomic.Int64
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start, _ := strconv.ParseInt(q.Get("start_timestamp"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("end_timestamp"), 10, 64)
		if end-start >= 3000 {
			wholeHour.Add(1)
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, backend.URL+r.URL.RequestURI(), nil)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(front.Close)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: front.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: front.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: from,
		UsageTailMode: upstreamUsageTailModePaged,
	}
	pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	result, yielded, _, err := m.syncNewAPITailIncremental(context.Background(), row, newAPICredential{AccessToken: "usage-token"}, from, now, now, pacer)
	if err != nil || yielded || result.DataUntil != now {
		t.Fatalf("fallback round yielded=%v until=%d err=%v", yielded, result.DataUntil, err)
	}
	if wholeHour.Load() == 0 {
		t.Fatal("paged whole-hour query was not attempted")
	}
	var hour ChannelUpstreamUsageHour
	if err := m.storeDB.First(&hour, "domain = ? AND hour_ts = ?", row.Domain, from).Error; err != nil || hour.Requests != 200 {
		t.Fatalf("fallback hour=%+v err=%v", hour, err)
	}
}

// newSlowUsageFront wraps the fixture with a per-request delay and an optional
// 504 for whole-hour windows, to model slow upstreams.
func newSlowUsageFront(t *testing.T, rows []upstreamUsageFixtureRow, delay time.Duration, wholeHour504 bool) *httptest.Server {
	t.Helper()
	backend, _ := newUpstreamUsageFixtureServer(t, rows, nil)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		start, _ := strconv.ParseInt(q.Get("start_timestamp"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("end_timestamp"), 10, 64)
		if wholeHour504 && end-start >= 3000 {
			http.Error(w, "gateway timeout", http.StatusGatewayTimeout)
			return
		}
		select {
		case <-time.After(delay):
		case <-r.Context().Done():
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, backend.URL+r.URL.RequestURI(), nil)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(front.Close)
	return front
}

// Review finding: the round's wall-clock deadline expiring mid-hour after
// saving pages is progress, not a failure.
func TestNewAPITailPagedDeadlineWithProgressIsYield(t *testing.T) {
	from := time.Date(2026, 11, 11, 15, 0, 0, 0, cstLocation).Unix()
	now := from + 3600 + 300
	front := newSlowUsageFront(t, usageFixtureRows(6500, from, 3600), 40*time.Millisecond, false)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: front.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: front.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: from,
		UsageTailMode: upstreamUsageTailModePaged,
	}
	// The guard refuses to send with less than its minimum send budget left, so
	// give the round a few pages of headroom beyond that before it runs out.
	ctx, cancel := context.WithTimeout(context.Background(), upstreamGuardMinSendBudget+time.Second)
	defer cancel()
	pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	result, yielded, _, err := m.syncNewAPITailIncremental(ctx, row, newAPICredential{AccessToken: "usage-token"}, from, now, now, pacer)
	if err != nil || !yielded || result.DataUntil != from {
		t.Fatalf("deadline with saved pages: yielded=%v until=%d err=%v", yielded, result.DataUntil, err)
	}
	if page := m.newAPITailCheckpointPage(context.Background(), row.Domain, from, from+3600); page <= 2 {
		t.Fatalf("no durable progress recorded: next page=%d", page)
	}
}

// Review finding: slow and dense with no progress must be a visible failure,
// not a fast loop and not a silent success.
func TestNewAPITailSlowDenseWithoutProgressIsReported(t *testing.T) {
	from := time.Date(2026, 11, 11, 15, 0, 0, 0, cstLocation).Unix()
	now := from + 3600 + 300
	front := newSlowUsageFront(t, usageFixtureRows(7500, from, 3600), 0, true)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: front.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: front.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: from,
		UsageTailMode: upstreamUsageTailModePaged,
	}
	pacer := newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0)
	result, yielded, _, err := m.syncNewAPITailIncremental(context.Background(), row, newAPICredential{AccessToken: "usage-token"}, from, now, now, pacer)
	if err == nil || yielded || result.DataUntil != from || !isNewAPIBackfillTimeout(err) {
		t.Fatalf("stuck hour must surface its timeout: yielded=%v until=%d err=%v", yielded, result.DataUntil, err)
	}
	var published int64
	if err := m.storeDB.Model(&ChannelUpstreamUsageHour{}).Where("domain = ?", row.Domain).Count(&published).Error; err != nil || published != 0 {
		t.Fatalf("partial hour became public: rows=%d err=%v", published, err)
	}
}

// funnycode.cc, 2026-11-12: a run published the first hours, then one later
// hour timed out. Returning that error discarded the progress, so every run
// re-read the same hours and stalled on the same slow hour for 36 hours. The
// committed hours now move the watermark and the run resumes from the failed
// hour; a retry that makes no progress still reports the failure.
func TestNewAPITailKeepsCommittedHoursWhenLaterHourFails(t *testing.T) {
	from := time.Date(2026, 11, 12, 1, 0, 0, 0, cstLocation).Unix()
	now := from + 2*3600 + 300
	backend, _ := newUpstreamUsageFixtureServer(t, usageFixtureRows(20, from, 3*3600), nil)
	front := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.ParseInt(r.URL.Query().Get("start_timestamp"), 10, 64)
		if start >= from+3600 && start < from+2*3600 {
			http.Error(w, "upstream timeout", http.StatusGatewayTimeout)
			return
		}
		req, _ := http.NewRequestWithContext(r.Context(), http.MethodGet, backend.URL+r.URL.RequestURI(), nil)
		req.Header = r.Header.Clone()
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadGateway)
			return
		}
		defer resp.Body.Close()
		w.WriteHeader(resp.StatusCode)
		_, _ = io.Copy(w, resp.Body)
	}))
	t.Cleanup(front.Close)
	m := newChannelUpstreamTestMonitor(t)
	row := ChannelUpstreamAccount{
		Domain: front.Listener.Addr().String(), Provider: upstreamProviderNewAPI,
		BaseURL: front.URL, UserID: 31, BalanceUnit: 500000, UsageDataUntil: from,
		UsageTailMode: upstreamUsageTailModeHourly,
	}
	cred := newAPICredential{AccessToken: "usage-token"}
	// The repeated 504s open the host circuit, so this run reports that error
	// (its cooldown must be respected) together with the committed hour.
	result, yielded, _, err := m.syncNewAPITailIncremental(context.Background(), row, cred, from, now, now, newUpstreamUsageRequestPacer(upstreamUsageMaxRequestsPerRun, 0))
	if result.DataUntil != from+3600 || yielded || err == nil {
		t.Fatalf("committed hour with a circuit error: yielded=%v until=%d err=%v", yielded, result.DataUntil, err)
	}
	applyUpstreamUsageResult(&row, result, err, now, m.cfg)
	// An open circuit pauses the account until its cooldown (status is kept).
	if row.UsageDataUntil != from+3600 || row.UsageNextSyncAt <= now {
		t.Fatalf("watermark must keep the committed hour while the circuit cools down: until=%d next=%d", row.UsageDataUntil, row.UsageNextSyncAt)
	}
}

// Only transient failures after progress are a quick resume; errors with
// their own status or cooldown keep it.
func TestNewAPITailYieldsAfterProgressOnlyForTransientFailures(t *testing.T) {
	for _, c := range []struct {
		name string
		err  error
		want bool
	}{
		{"deadline", fmt.Errorf("连接上游失败: %w", context.DeadlineExceeded), true},
		{"5xx", &upstreamHTTPError{Status: http.StatusBadGateway}, true},
		{"local queue", &upstreamLocalQueueError{err: context.DeadlineExceeded}, true},
		{"429", &upstreamHTTPError{Status: http.StatusTooManyRequests}, false},
		{"retry-after", &upstreamHTTPError{Status: http.StatusServiceUnavailable, RetryAt: time.Now().Add(time.Minute).Unix()}, false},
		{"auth", &upstreamAuthError{err: errors.New("401")}, false},
		{"circuit", &upstreamCircuitOpenError{RetryAt: time.Now().Add(time.Minute).Unix()}, false},
		{"canceled", context.Canceled, false},
		{"format", errors.New("NewAPI 使用日志缺少有效 total"), false},
	} {
		if got := newAPITailYieldsAfterProgress(c.err); got != c.want {
			t.Fatalf("%s: yield=%v want %v", c.name, got, c.want)
		}
	}
}
