package monitor

import (
	"context"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func TestUpstreamServiceRetryAfterPersistsAndRecovers(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	start := time.Unix(1_800_000_000, 0)
	clock := &fakeUpstreamGuardClock{now: start}
	var calls atomic.Int64
	client := newGuardedTestClient(upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return upstreamGuardResponse(http.StatusServiceUnavailable, http.Header{"Retry-After": []string{"3600"}}), nil
	}), store, clock, 0, 0)
	response, err := doGuardTestRequest(t, client, http.MethodGet, "https://maintenance.example/api")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)
	var persisted UpstreamHostCircuit
	if err := store.First(&persisted, "host_key = ?", "maintenance.example:443").Error; err != nil {
		t.Fatal(err)
	}
	want := start.Add(time.Hour).Unix()
	if persisted.OpenUntil != want || persisted.LastStatus != http.StatusServiceUnavailable {
		t.Fatalf("503 Retry-After not persisted: %+v", persisted)
	}
	restarted := newGuardedTestClient(upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
		calls.Add(1)
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), store, clock, 0, 0)
	response, err = doGuardTestRequest(t, restarted, http.MethodGet, "https://maintenance.example/other-task")
	consumeGuardResponse(t, response)
	if err == nil || upstreamRetryAt(err) != want || calls.Load() != 1 {
		t.Fatalf("restart/other task bypassed provider cooldown: calls=%d err=%v", calls.Load(), err)
	}
	clock.Advance(time.Hour + time.Second)
	response, err = doGuardTestRequest(t, restarted, http.MethodGet, "https://maintenance.example/recovered")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)
	if calls.Load() != 2 {
		t.Fatal("provider cooldown did not release the probe")
	}
}

func TestUpstreamServiceRetryAfterReachesTaskSchedule(t *testing.T) {
	for _, value := range []string{"3600", time.Now().Add(time.Hour).UTC().Format(http.TimeFormat)} {
		t.Run(value, func(t *testing.T) {
			client := &http.Client{Transport: upstreamRoundTripFunc(func(*http.Request) (*http.Response, error) {
				return upstreamGuardResponse(http.StatusServiceUnavailable, http.Header{"Retry-After": []string{value}}), nil
			})}
			before := time.Now().Unix()
			_, err := doUpstreamJSON(context.Background(), client, http.MethodGet, "https://fixture.example/api", nil, nil)
			if err == nil || upstreamRetryAt(err) < before+3598 {
				t.Fatalf("503 retry deadline missing from task error: %v retry=%d", err, upstreamRetryAt(err))
			}
			row := ChannelUpstreamAccount{BalanceKnown: true, BalanceUSD: 71, LastSuccessAt: before - 100}
			applyUpstreamSyncResult(&row, upstreamBalanceResult{}, err, before, Settings{})
			if row.NextSyncAt < upstreamRetryAt(err) || row.Status != upstreamStatusError ||
				row.ConsecutiveFails != 1 || row.BalanceUSD != 71 || row.LastSuccessAt != before-100 {
				t.Fatalf("task shortened provider cooldown or changed published data: %+v", row)
			}
		})
	}
}

func TestUpstreamServiceRetryAfterKeepsOrdinaryBackoffAndAccountIsolation(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	for _, tc := range []struct {
		name     string
		status   int
		header   string
		failures int
		want     time.Duration
	}{
		{"plain_503", 503, "", 0, 0},
		{"third_503_short_header", 503, "1", 2, 5 * time.Minute},
		{"explicit_503", 503, "3600", 0, time.Hour},
		{"malformed_503", 503, "invalid", 0, upstreamRetryAfterDefault},
		{"auth_401", 401, "3600", 0, 0},
		{"permission_403", 403, "3600", 0, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			clock := &fakeUpstreamGuardClock{now: start}
			guard := newUpstreamHostGuard(newUpstreamGuardTestStore(t), upstreamHostGuardOptions{Clock: clock})
			state := guard.hostState("fixture.example:443")
			state.circuit.ConsecutiveFailures = tc.failures
			guard.recordResponse(state, tc.status, tc.header)
			want := int64(0)
			if tc.want > 0 {
				want = start.Add(tc.want).Unix()
			}
			if state.circuit.OpenUntil != want {
				t.Fatalf("retry=%d want=%d", state.circuit.OpenUntil, want)
			}
		})
	}
}
