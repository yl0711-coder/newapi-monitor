package monitor

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
)

func resilienceGuardClient(t *testing.T, base http.RoundTripper) *http.Client {
	t.Helper()
	guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{GlobalConcurrency: 1, Jitter: func() time.Duration { return 0 }})
	return &http.Client{Transport: &upstreamGuardTransport{base: base, guard: guard}}
}

func okResponse() *http.Response {
	return &http.Response{StatusCode: 200, Header: http.Header{}, Body: http.NoBody}
}

// Time spent queued behind another upstream request is not the upstream's
// response time: a request that waits longer than its timeout still succeeds
// once it is sent, and running out of budget while queued is a local yield.
func TestUpstreamRequestTimeoutExcludesLocalQueue(t *testing.T) {
	release := make(chan struct{})
	var started atomic.Int32
	shared := resilienceGuardClient(t, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if started.Add(1) == 1 {
			<-release
		}
		return okResponse(), nil
	}))
	long := withUpstreamRequestTimeout(shared, 5*time.Second)
	short := withUpstreamRequestTimeout(shared, 150*time.Millisecond)
	if short.Timeout != 0 || short.Transport.(*upstreamGuardTransport).guard != shared.Transport.(*upstreamGuardTransport).guard {
		t.Fatal("timed client must share the guard and drop http.Client.Timeout")
	}

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		req, _ := http.NewRequest(http.MethodGet, "https://slow.example/a", nil)
		if resp, err := long.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	for started.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	go func() { time.Sleep(400 * time.Millisecond); close(release) }()
	req, _ := http.NewRequest(http.MethodGet, "https://other.example/b", nil)
	resp, err := short.Do(req)
	if err != nil {
		t.Fatalf("queued request must not time out before it is sent: %v", err)
	}
	resp.Body.Close()
	wg.Wait()

	// Budget exhausted while queued: a typed local yield, not an upstream failure.
	block := make(chan struct{})
	defer close(block)
	var calls atomic.Int32
	shared = resilienceGuardClient(t, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		if calls.Add(1) == 1 {
			<-block
		}
		return okResponse(), nil
	}))
	timed := withUpstreamRequestTimeout(shared, time.Second)
	go func() {
		req, _ := http.NewRequest(http.MethodGet, "https://slow.example/a", nil)
		if resp, err := timed.Do(req); err == nil {
			resp.Body.Close()
		}
	}()
	for calls.Load() == 0 {
		time.Sleep(5 * time.Millisecond)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()
	_, err = doUpstreamJSON(ctx, timed, http.MethodGet, "https://other.example/b", nil, nil)
	if !isUpstreamLocalQueueTimeout(err) || isNewAPIBackfillTimeout(err) {
		t.Fatalf("queue exhaustion must be a local yield, got %v", err)
	}
	now := time.Now().Unix()
	row := ChannelUpstreamAccount{Status: upstreamStatusOK, ConsecutiveFails: 2, UsageStatus: upstreamStatusOK, UsageConsecutiveFails: 1, UsageBackfillConsecutiveFails: 3}
	applyUpstreamSyncResult(&row, upstreamBalanceResult{}, err, now, Settings{})
	applyUpstreamUsageResult(&row, upstreamUsageResult{}, err, now, Settings{})
	applyUpstreamUsageBackfillResult(&row, err, now, Settings{})
	if row.Status != upstreamStatusOK || row.ConsecutiveFails != 2 || row.UsageStatus != upstreamStatusOK || row.UsageConsecutiveFails != 1 ||
		row.UsageBackfillConsecutiveFails != 3 || row.LastError != "" || row.UsageBackfillLastError != "" || row.NextSyncAt <= now {
		t.Fatalf("local queue yield changed upstream health: %+v", row)
	}
	if reason, yielded := upstreamSyncYieldReason(err); !yielded || reason == "" {
		t.Fatal("queue yield must be logged as a postponement, not a failure")
	}
	if _, yielded := upstreamSyncYieldReason(errors.New("boom")); yielded {
		t.Fatal("real failures are not yields")
	}
}

// The request's own time is still bounded once it is sent.
func TestUpstreamRequestTimeoutStillBoundsSentRequest(t *testing.T) {
	shared := resilienceGuardClient(t, upstreamRoundTripFunc(func(r *http.Request) (*http.Response, error) {
		<-r.Context().Done()
		return nil, r.Context().Err()
	}))
	_, err := doUpstreamJSON(context.Background(), withUpstreamRequestTimeout(shared, 80*time.Millisecond), http.MethodGet, "https://slow.example/a", nil, nil)
	if err == nil || isUpstreamLocalQueueTimeout(err) || !isNewAPIBackfillTimeout(err) {
		t.Fatalf("sent request must time out as an upstream timeout, got %v", err)
	}
}

// A NewAPI site that times out on a whole hour but answers short ranges is
// read as five-minute children; later windows start with children directly.
func TestNewAPIUsageSplitsWindowAfterTimeout(t *testing.T) {
	const domain = "slowlog.example"
	defer func() {
		newAPISlowLogDomains.Lock()
		delete(newAPISlowLogDomains.until, domain)
		newAPISlowLogDomains.Unlock()
	}()
	var wide atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start, _ := strconv.ParseInt(r.URL.Query().Get("start_timestamp"), 10, 64)
		end, _ := strconv.ParseInt(r.URL.Query().Get("end_timestamp"), 10, 64)
		if end-start+1 > 300 {
			wide.Add(1)
			time.Sleep(300 * time.Millisecond)
		}
		_, _ = fmt.Fprintf(w, `{"success":true,"data":{"total":1,"items":[{"id":1,"created_at":%d,"quota":500000,"prompt_tokens":1,"completion_tokens":1}]}}`, start+10)
	}))
	defer server.Close()
	from := time.Date(2026, 11, 11, 10, 0, 0, 0, cstLocation).Unix()
	row := ChannelUpstreamAccount{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 3, BalanceUnit: 500000}
	client := newUpstreamHTTPClient(100 * time.Millisecond)
	for i, window := range []int64{from, from + 3600} {
		result, err := fetchNewAPIUsageWindow(context.Background(), client, row, newAPICredential{AccessToken: "t"}, window, window+3600)
		if err != nil {
			t.Fatalf("window %d: %v", i, err)
		}
		if len(result.Hours) != 1 || result.Hours[0].Requests != 12 || result.Hours[0].CostUSD != 12 || result.DataUntil != window+3600 {
			t.Fatalf("window %d must aggregate all twelve children: %+v", i, result)
		}
	}
	if wide.Load() != 1 {
		t.Fatalf("after one whole-window timeout later windows must start split, wide requests=%d", wide.Load())
	}
}

// A rejected access token is reported even when the session renewal also
// fails, and a personal access token never goes through session renewal.
func TestNewAPIBalanceReportsTokenErrorAndSkipsRenewalForPAT(t *testing.T) {
	var renewals atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/self":
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"ACCESS_TOKEN_LEGACY_RETIRED","message":"Legacy access tokens have been retired.","success":false}`))
		case "/api/user/auth/refresh":
			renewals.Add(1)
			w.WriteHeader(http.StatusUnauthorized)
			_, _ = w.Write([]byte(`{"code":"AUTH_UNAUTHORIZED","message":"Unauthorized","success":false}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "pat.example", Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 5}
	_, _, err := syncNewAPIBalance(context.Background(), server.Client(), row, newAPICredential{AccessToken: "legacy", SessionID: "sid"})
	if err == nil || !strings.Contains(err.Error(), "Legacy access tokens") || !strings.Contains(err.Error(), "nap_") || !strings.Contains(err.Error(), "会话续期也失败") || renewals.Load() != 1 {
		t.Fatalf("token rejection hidden by renewal error: %v renewals=%d", err, renewals.Load())
	}
	_, _, err = syncNewAPIBalance(context.Background(), server.Client(), row, newAPICredential{AccessToken: "nap_example", SessionID: "sid"})
	if err == nil || renewals.Load() != 1 {
		t.Fatalf("personal access token must not use session renewal: %v renewals=%d", err, renewals.Load())
	}
}

func TestNewAPISaveCanClearStoredSession(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":500000}}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	domain := normalizeChannelBaseDomain(server.URL)
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "mock", BaseDomain: domain, BaseHost: normalizeChannelBaseHost(server.URL), Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	save := func(payload channelUpstreamSaveInput) newAPICredential {
		t.Helper()
		if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload); w.Code != http.StatusOK {
			t.Fatalf("save status=%d body=%s", w.Code, w.Body.String())
		}
		var row ChannelUpstreamAccount
		if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
			t.Fatal(err)
		}
		var credential newAPICredential
		if err := m.openUpstreamCredential(row, &credential); err != nil {
			t.Fatal(err)
		}
		return credential
	}
	base := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 9}
	first := base
	first.AccessToken, first.SessionID = "token-one", `{"sid":"stale-session"}`
	if got := save(first); got.SessionID != "stale-session" {
		t.Fatalf("session not stored: %+v", got)
	}
	keep := base
	keep.AccessToken = "nap_two"
	if got := save(keep); got.AccessToken != "nap_two" || got.SessionID != "stale-session" {
		t.Fatalf("empty session field must keep the stored session: %+v", got)
	}
	clear := base
	clear.ClearSessionID = true
	if got := save(clear); got.AccessToken != "nap_two" || got.SessionID != "" {
		t.Fatalf("clear_session_id must drop only the session: %+v", got)
	}
}

// A slow recheck after an exhausted budget or a 4xx is not a cooldown: the
// administrator's 检测并恢复 may run at once.
func TestManualRecoveryIsNotBlockedBySlowRecheck(t *testing.T) {
	now := int64(1_800_000_000)
	last := now - 3600
	cases := []struct {
		name   string
		target upstreamRecoveryTarget
		want   int64
	}{
		{"4xx slow recheck", upstreamRecoveryTarget{failures: 1, last: last, next: now + int64(upstreamClientErrorRecheck/time.Second)}, last + 300},
		{"exhausted budget recheck", upstreamRecoveryTarget{failures: upstreamRecoveryFailureLimit, last: last, next: now + int64(upstreamTransientFailureRecheck/time.Second)}, last + 300},
		{"probe schedule honoured", upstreamRecoveryTarget{failures: upstreamRecoveryProbeThreshold, last: last, next: now + 1800}, now + 1800},
		{"recent attempt cooldown", upstreamRecoveryTarget{failures: 1, last: now - 60, next: 0}, now + 240},
	}
	for _, tc := range cases {
		if got := upstreamRecoveryRetryAt(tc.target, now); got != tc.want {
			t.Fatalf("%s: retryAt=%d want %d", tc.name, got, tc.want)
		}
	}
}

// A configured upstream stays visible even when its only channel serves an
// excluded test group without usage in range; otherwise it cannot be managed.
func TestChannelManagementKeepsConfiguredTestOnlyDomain(t *testing.T) {
	m := newStabilityTestMonitor(t)
	hour := time.Date(2026, 9, 18, 10, 0, 0, 0, cstLocation).Unix()
	if err := m.storeDB.Create(&ChannelBusinessGroupPolicy{Grp: "internal_test", Included: false, UpdatedAt: hour}).Error; err != nil {
		t.Fatal(err)
	}
	for _, ch := range []ChannelSnap{
		{ID: 71, Name: "configured test only", BaseDomain: "configured-test.example", Status: 2, Groups: "internal_test", UpdatedAt: hour},
		{ID: 72, Name: "plain test only", BaseDomain: "plain-test.example", Status: 2, Groups: "internal_test", UpdatedAt: hour},
	} {
		if err := m.storeDB.Create(&ch).Error; err != nil {
			t.Fatal(err)
		}
	}
	if err := m.storeDB.Create(&ChannelUpstreamAccount{Domain: "configured-test.example", Provider: upstreamProviderNewAPI, BaseURL: "https://configured-test.example", UserID: 1, Enabled: true, Credential: "sealed"}).Error; err != nil {
		t.Fatal(err)
	}
	report, err := m.buildChannelManagementReport(context.Background(), stabilityScope{FromTs: hour, ToTs: hour + 3600}, hour+7200)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[string]bool{}
	for _, domain := range report.Domains {
		seen[domain.Domain] = true
	}
	if !seen["configured-test.example"] || seen["plain-test.example"] {
		t.Fatalf("configured test-only domain must stay visible, unconfigured hidden: %v", seen)
	}
}

// Tasks paused indefinitely by the earlier policy for a non-credential reason
// resume; credential isolation stays until an administrator acts.
func TestReleaseNonCredentialUpstreamIsolation(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	iso := upstreamAccountIsolatedUntil
	rows := []ChannelUpstreamAccount{
		{Domain: "slow.example", Provider: upstreamProviderNewAPI, Status: upstreamStatusOK, NextSyncAt: 100,
			UsageStatus: upstreamStatusError, UsageNextSyncAt: iso, UsageConsecutiveFails: 14, UsageLastError: "timeout",
			UsageBackfillDone: false, UsageBackfillNextSyncAt: iso},
		{Domain: "four-oh-five.example", Provider: upstreamProviderNewAPI, Status: upstreamStatusError, NextSyncAt: iso, LastError: "上游返回 HTTP 405",
			UsageStatus: upstreamStatusOK, UsageBackfillDone: true, UsageBackfillNextSyncAt: 0},
		{Domain: "revoked.example", Provider: upstreamProviderNewAPI, Status: upstreamStatusReconnect, NextSyncAt: iso,
			UsageStatus: upstreamStatusReconnect, UsageNextSyncAt: iso, UsageBackfillNextSyncAt: iso},
	}
	if err := m.storeDB.Create(&rows).Error; err != nil {
		t.Fatal(err)
	}
	if err := m.storeDB.Create(&UpstreamErrorLogSyncState{Domain: "slow.example", Status: upstreamStatusError, NextSyncAt: iso}).Error; err != nil {
		t.Fatal(err)
	}
	now := int64(1_800_000_000)
	released, err := m.releaseNonCredentialUpstreamIsolation(context.Background(), now)
	if err != nil || released != 4 {
		t.Fatalf("released=%d err=%v, want 4", released, err)
	}
	get := func(domain string) ChannelUpstreamAccount {
		var row ChannelUpstreamAccount
		if err := m.storeDB.First(&row, "domain = ?", domain).Error; err != nil {
			t.Fatal(err)
		}
		return row
	}
	if r := get("slow.example"); r.UsageNextSyncAt != now || r.UsageBackfillNextSyncAt != now || r.UsageConsecutiveFails != 14 || r.UsageLastError != "timeout" || r.NextSyncAt != 100 {
		t.Fatalf("slow tasks not resumed or facts changed: %+v", r)
	}
	if r := get("four-oh-five.example"); r.NextSyncAt != now || r.LastError == "" {
		t.Fatalf("4xx balance not resumed: %+v", r)
	}
	if r := get("revoked.example"); r.NextSyncAt != iso || r.UsageNextSyncAt != iso || r.UsageBackfillNextSyncAt != iso {
		t.Fatalf("credential isolation must stay: %+v", r)
	}
	var state UpstreamErrorLogSyncState
	if err := m.storeDB.First(&state, "domain = ?", "slow.example").Error; err != nil || state.NextSyncAt != now {
		t.Fatalf("error log task not resumed: %+v %v", state, err)
	}
	if again, err := m.releaseNonCredentialUpstreamIsolation(context.Background(), now+10); err != nil || again != 0 {
		t.Fatalf("release must be idempotent: %d %v", again, err)
	}
}

// Waiting for one host's own pacing must not hold the process-wide slot.
func TestUpstreamHostPacingDoesNotBlockOtherHosts(t *testing.T) {
	guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{GlobalConcurrency: 1, Jitter: func() time.Duration { return 0 }})
	client := withUpstreamRequestTimeout(&http.Client{Transport: &upstreamGuardTransport{guard: guard, base: upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
		return okResponse(), nil
	})}}, 2*time.Second)
	paced := guard.hostState("paced.example:443")
	paced.mu.Lock()
	paced.loaded = true
	paced.nextStart = time.Now().Add(500 * time.Millisecond)
	paced.mu.Unlock()
	done := make(chan time.Duration, 1)
	go func() {
		start := time.Now()
		req, _ := http.NewRequest(http.MethodGet, "https://paced.example/a", nil)
		if resp, err := client.Do(req); err == nil {
			resp.Body.Close()
		}
		done <- time.Since(start)
	}()
	time.Sleep(50 * time.Millisecond)
	start := time.Now()
	req, _ := http.NewRequest(http.MethodGet, "https://free.example/b", nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if waited := time.Since(start); waited > 250*time.Millisecond {
		t.Fatalf("another host waited %v behind a paced host", waited)
	}
	if pacedWait := <-done; pacedWait < 400*time.Millisecond {
		t.Fatalf("paced host must still honour its own interval, waited %v", pacedWait)
	}
}

// A stored sync outcome still reads as a postponement, with its own message.
func TestStoredSyncErrorKeepsYield(t *testing.T) {
	queued := fmt.Errorf("连接上游失败: %w", &upstreamLocalQueueError{err: context.DeadlineExceeded})
	stored := storedSyncError("old upstream error", 0, queued)
	if reason, yielded := upstreamSyncYieldReason(stored); !yielded || reason == "" || strings.Contains(stored.Error(), "old upstream error") {
		t.Fatalf("stored yield lost: yielded=%v message=%q", yielded, stored.Error())
	}
	failed := storedSyncError("real failure", 0, errors.New("HTTP 500"))
	if _, yielded := upstreamSyncYieldReason(failed); yielded || failed.Error() != "real failure" {
		t.Fatalf("real failure misread as yield: %q", failed.Error())
	}
}

// Too little caller budget left once a slot is free: do not send a request
// that the caller would cut off and book as an upstream failure.
func TestUpstreamGuardYieldsWithoutSendBudget(t *testing.T) {
	var sent atomic.Int32
	client := withUpstreamRequestTimeout(resilienceGuardClient(t, upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
		sent.Add(1)
		return okResponse(), nil
	})), 15*time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	_, err := doUpstreamJSON(ctx, client, http.MethodGet, "https://budget.example/a", nil, nil)
	if !isUpstreamLocalQueueTimeout(err) || sent.Load() != 0 {
		t.Fatalf("expected local yield without sending, err=%v sent=%d", err, sent.Load())
	}
	ctx2, cancel2 := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel2()
	if _, err := doUpstreamJSON(ctx2, client, http.MethodGet, "https://budget.example/a", nil, nil); err == nil || sent.Load() != 1 {
		// okResponse has an empty body, so decoding may fail; the request must have been sent.
		if sent.Load() != 1 {
			t.Fatalf("request with enough budget was not sent: %v", err)
		}
	}
}

// Error-log and funds lanes keep their status on a local postponement.
func TestErrorLogAndFundLanesKeepStatusOnLocalYield(t *testing.T) {
	m := newTestMonitor(t)
	queued := fmt.Errorf("连接上游失败: %w", &upstreamLocalQueueError{err: context.DeadlineExceeded})
	now := int64(1_800_000_000)
	logState := UpstreamErrorLogSyncState{Domain: "y.example", Status: upstreamStatusOK, ConsecutiveFails: 0}
	_ = m.failUpstreamErrorLogState(context.Background(), &logState, now, queued)
	if logState.Status != upstreamStatusOK || logState.LastError != "" || logState.ConsecutiveFails != 0 || logState.NextSyncAt <= now {
		t.Fatalf("error log lane booked a local yield as failure: %+v", logState)
	}
	fundState := UpstreamFundSyncState{Domain: "y.example", AccountEpoch: "e", Status: upstreamStatusOK}
	_ = m.failFundState(context.Background(), &fundState, now, queued)
	if fundState.Status != upstreamStatusOK || fundState.LastError != "" || fundState.ConsecutiveFails != 0 || fundState.NextSyncAt <= now {
		t.Fatalf("funds lane booked a local yield as failure: %+v", fundState)
	}
}

// A console outside the channel domain is accepted only when confirmed.
func TestUpstreamSaveCrossDomainBaseURLNeedsConfirmation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/api/user/self":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota":500000}}`))
		case "/api/status":
			_, _ = w.Write([]byte(`{"success":true,"data":{"quota_per_unit":500000}}`))
		default:
			http.NotFound(w, r)
		}
	}))
	defer server.Close()
	const domain = "moved-panel.example"
	m := newChannelUpstreamTestMonitor(t)
	if err := m.storeDB.Create(&ChannelSnap{ID: 1, Name: "moved", BaseDomain: domain, BaseHost: "api." + domain, Status: 1}).Error; err != nil {
		t.Fatal(err)
	}
	router := gin.New()
	router.GET("/channels/upstream", m.requireRole(roleRoot), m.getChannelUpstreamHandler)
	router.POST("/channels/upstream", m.requireRole(roleRoot), m.saveChannelUpstreamHandler)
	payload := channelUpstreamSaveInput{Domain: domain, Provider: upstreamProviderNewAPI, BaseURL: server.URL, UserID: 4, AccessToken: "nap_token"}
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload); w.Code == http.StatusOK || !strings.Contains(w.Body.String(), "站点地址与渠道域名不同") {
		t.Fatalf("unconfirmed foreign console must be refused: status=%d body=%s", w.Code, w.Body.String())
	}
	payload.CrossDomainBaseURL = true
	if w := upstreamRouteRequest(t, m, router, roleRoot, http.MethodPost, "/channels/upstream", payload); w.Code != http.StatusOK {
		t.Fatalf("confirmed foreign console refused: status=%d body=%s", w.Code, w.Body.String())
	}
	get := upstreamRouteRequest(t, m, router, roleRoot, http.MethodGet, "/channels/upstream?domain="+domain, nil)
	if get.Code != http.StatusOK || !strings.Contains(get.Body.String(), `"cross_domain_base_url":true`) {
		t.Fatalf("saved foreign console must be reported: %s", get.Body.String())
	}
}
