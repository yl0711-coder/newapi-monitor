package monitor

import (
	"context"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpstreamLogPaceLearnsPersistsAndIgnoresBalanceSuccess(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1800000000, 0)}
	guard := newUpstreamHostGuard(store, upstreamHostGuardOptions{Clock: clock, MinInterval: 0})
	state := guard.hostState("limited.test:443")
	if err := guard.loadCircuit(context.Background(), state); err != nil {
		t.Fatal(err)
	}
	guard.recordLogResponse(state, 429)
	if state.circuit.LogIntervalMS != 2000 {
		t.Fatal("first rate limit not learned", state.circuit.LogIntervalMS)
	}
	guard.recordLogResponse(state, 429)
	guard.recordResponse(state, 200, "") // balance success
	if state.circuit.LogIntervalMS != 4000 || state.circuit.LogSuccesses != 0 {
		t.Fatal("balance success erased log pacing", state.circuit)
	}
	guard.recordLogStart(state)
	restarted := newUpstreamHostGuard(store, upstreamHostGuardOptions{Clock: clock, MinInterval: 0})
	again := restarted.hostState("limited.test:443")
	if err := restarted.loadCircuit(context.Background(), again); err != nil {
		t.Fatal(err)
	}
	if again.circuit.LogIntervalMS != 4000 || again.circuit.LogNextStartMS <= clock.Now().UnixMilli() {
		t.Fatal("restart lost pace/reservation", again.circuit)
	}
	for i := 0; i < 16; i++ {
		restarted.recordLogResponse(again, 200)
	}
	if again.circuit.LogIntervalMS != 2000 {
		t.Fatal("sustained log success did not recover gradually", again.circuit)
	}
	restarted.prepareLogPace(again, upstreamLogQueryScope{previouslyLimited: true})
	if again.circuit.LogIntervalMS != 2000 {
		t.Fatal("old account error reset learned progress")
	}
}

func TestUpstreamLogPaceRecognizesOnlyScopedLogEndpoints(t *testing.T) {
	for _, path := range []string{"/api/log/self", "/panel/api/log/self/", "/api/user/self"} {
		req, err := http.NewRequestWithContext(withUpstreamLogQuery(context.Background(), ChannelUpstreamAccount{}), http.MethodGet, "https://fixture.test"+path, nil)
		if err != nil {
			t.Fatal(err)
		}
		_, logQuery := upstreamLogQuery(req)
		if logQuery != (path != "/api/user/self") {
			t.Fatal("log-resource classification wrong", path)
		}
		req = req.WithContext(context.Background())
		if _, logQuery = upstreamLogQuery(req); logQuery {
			t.Fatal("unscoped request learned a log pace", path)
		}
	}
}

func TestUpstreamLogPaceHonorsRetryAfterAndSeedsLegacy429(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1800000000, 0)}
	var sent atomic.Int64
	base := upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		sent.Add(1)
		return upstreamGuardResponse(429, http.Header{"Retry-After": []string{"3600"}}), nil
	})
	client := newGuardedTestClient(base, store, clock, 0, 0)
	row := ChannelUpstreamAccount{UsageConsecutiveFails: 30, UsageLastError: "上游返回 HTTP 429"}
	req, err := http.NewRequestWithContext(withUpstreamLogQuery(context.Background(), row), http.MethodGet, "https://limited.test/api/log/self", nil)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	state := client.Transport.(*upstreamGuardTransport).guard.hostState("limited.test:443")
	if state.circuit.LogIntervalMS != time.Minute.Milliseconds() || state.circuit.OpenUntil != clock.Now().Add(time.Hour).Unix() {
		t.Fatal("legacy pacing or Retry-After lost", state.circuit)
	}
	resp, err = client.Do(req)
	if resp != nil {
		_ = resp.Body.Close()
	}
	var circuit *upstreamCircuitOpenError
	if !errors.As(err, &circuit) || sent.Load() != 1 {
		t.Fatal("cooldown was bypassed", sent.Load(), err)
	}
}

func TestUpstreamLogPaceWaitDoesNotHoldGlobalSlot(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &cancelUpstreamGuardClock{now: time.Unix(1800000000, 0), waiting: make(chan struct{}, 1)}
	guard := newUpstreamHostGuard(store, upstreamHostGuardOptions{Clock: clock, MinInterval: 0})
	state := guard.hostState("limited.test:443")
	state.loaded = true
	state.circuit.LogIntervalMS = 60000
	state.circuit.LogNextStartMS = clock.Now().Add(time.Minute).UnixMilli()
	base := upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) { return upstreamGuardResponse(200, nil), nil })
	client := installUpstreamHostGuardForTest(&http.Client{Transport: base}, store, guard)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		req, _ := http.NewRequestWithContext(withUpstreamLogQuery(ctx, ChannelUpstreamAccount{}), http.MethodGet, "https://limited.test/api/log/self", nil)
		resp, err := client.Do(req)
		if resp != nil {
			_ = resp.Body.Close()
		}
		result <- err
	}()
	select {
	case <-clock.waiting:
	case <-time.After(time.Second):
		t.Fatal("log request did not enter pacing")
	}
	if len(guard.globalSem) != 0 {
		t.Fatal("waiting log request holds global slot")
	}
	other, err := client.Get("https://other.test/api/user/self")
	if err != nil {
		t.Fatal(err)
	}
	_ = other.Body.Close()
	cancel()
	select {
	case err = <-result:
		if err == nil || !strings.Contains(err.Error(), "canceled") {
			t.Fatal("pacing cancellation lost", err)
		}
	case <-time.After(time.Second):
		t.Fatal("log wait did not cancel")
	}
}

func TestSlowNewAPILogMakesProgressWithOneQueryPerRound(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 2*3600
	server, queries := newUpstreamUsageFixtureServer(t, usageFixtureRows(250, from, 3600), nil)
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "slow.test", BaseURL: server.URL, Provider: upstreamProviderNewAPI, UserID: 31,
		BalanceUnit: 500000, UsageConsecutiveFails: 30, UsageLastError: "HTTP 429"}
	// Avoid wall-clock waiting: the fixture tests durable page scheduling,
	// while the guard tests above independently verify adaptive transport pace.
	m.upstreamClient = newGuardedTestClient(server.Client().Transport, m.storeDB, &fakeUpstreamGuardClock{now: time.Now()}, 0, 0)
	cred := newAPICredential{AccessToken: "usage-token"}
	for i := 0; i < 3; i++ {
		_, _, complete, err := m.syncNewAPIUsagePagedWindow(context.Background(), newAPIUsageTailCheckpointTable, row, cred, from, from+3600, newUpstreamUsageRequestPacer(1, 0), false)
		if err != nil || complete {
			t.Fatal("partial round unexpectedly completed/failed", i, complete, err)
		}
		var cp NewAPIUsageTailCheckpoint
		if err = m.storeDB.First(&cp, "domain = ?", row.Domain).Error; err != nil || cp.NextPage != i+2 {
			t.Fatal("sole query was spent re-reading page one", i, cp.NextPage, err)
		}
	}
	result, _, complete, err := m.syncNewAPIUsagePagedWindow(context.Background(), newAPIUsageTailCheckpointTable, row, cred, from, from+3600, newUpstreamUsageRequestPacer(1, 0), false)
	if err != nil || !complete || result.Hours[0].Requests != 250 || queries.Load() != 4 {
		t.Fatalf("final proof missing: complete=%v queries=%d result=%+v err=%v", complete, queries.Load(), result, err)
	}
}

func TestSlowNewAPIPricingMakesProgressWithOneQueryPerRound(t *testing.T) {
	m := newChannelUpstreamTestMonitor(t)
	from := time.Now().Unix()/3600*3600 - 2*3600
	server, queries := newUpstreamUsageFixtureServer(t, usageFixtureRows(250, from, 3600), nil)
	defer server.Close()
	row := ChannelUpstreamAccount{Domain: "slow-pricing.test", BaseURL: server.URL, Provider: upstreamProviderNewAPI, UserID: 31,
		BalanceUnit: 500000, UsageConsecutiveFails: 30, UsageLastError: "HTTP 429"}
	m.upstreamClient = newGuardedTestClient(server.Client().Transport, m.storeDB, &fakeUpstreamGuardClock{now: time.Now()}, 0, 0)
	cred := newAPICredential{AccessToken: "usage-token"}
	for i := 0; i < 3; i++ {
		_, _, _, complete, err := m.fetchNewAPIPricingHour(context.Background(), row, cred, from, newUpstreamUsageRequestPacer(1, 0), from+7200)
		if err != nil || complete {
			t.Fatal("partial pricing round failed/completed", i, complete, err)
		}
		var cp ChannelUpstreamPricingPageCheckpoint
		if err = m.storeDB.First(&cp, "domain = ?", row.Domain).Error; err != nil || cp.NextPage != i+2 {
			t.Fatal("pricing sole query wasted on first page", i, cp.NextPage, err)
		}
	}
	_, state, _, complete, err := m.fetchNewAPIPricingHour(context.Background(), row, cred, from, newUpstreamUsageRequestPacer(1, 0), from+7200)
	if err != nil || !complete || state.SourceRows != 250 || queries.Load() != 4 {
		t.Fatalf("pricing final proof missing: complete=%v queries=%d rows=%d err=%v", complete, queries.Load(), state.SourceRows, err)
	}
}
