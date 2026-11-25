package monitor

import (
	"context"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type upstreamLogQueryContextKey struct{}

type upstreamLogQueryScope struct{ previouslyLimited bool }

func withUpstreamLogQuery(ctx context.Context, row ChannelUpstreamAccount) context.Context {
	return context.WithValue(ctx, upstreamLogQueryContextKey{}, upstreamLogQueryScope{
		previouslyLimited: row.UsageConsecutiveFails >= upstreamRecoveryFailureLimit && strings.Contains(row.UsageLastError, "429"),
	})
}

func upstreamLogQuery(req *http.Request) (upstreamLogQueryScope, bool) {
	scope, ok := req.Context().Value(upstreamLogQueryContextKey{}).(upstreamLogQueryScope)
	return scope, ok && strings.HasSuffix(strings.TrimRight(req.URL.Path, "/"), "/api/log/self")
}

// This is a learned log-resource pace, not a new account/host circuit. Balance
// successes cannot erase it. Existing Retry-After and host serialization remain
// authoritative. The ceiling bounds local waiting, not the supplier cooldown.
const upstreamLogPaceCeiling = time.Minute

// A slow log endpoint may allow only one useful page per short worker turn.
// Re-reading page one on EVERY resume would consume that sole allowance.
// Such accounts defer their unchanged first-page proof until publication;
// they never skip the proof or publish unchecked accumulation.
func slowUpstreamLogPace(ctx context.Context, client *http.Client, row ChannelUpstreamAccount) bool {
	if row.UsageConsecutiveFails >= upstreamRecoveryFailureLimit && strings.Contains(row.UsageLastError, "429") {
		return true
	}
	transport, ok := client.Transport.(*upstreamGuardTransport)
	if !ok {
		return false
	}
	u, err := url.Parse(row.BaseURL)
	if err != nil {
		return false
	}
	key, err := upstreamGuardHostKey(u)
	if err != nil {
		return false
	}
	state := transport.guard.hostState(key)
	if transport.guard.loadCircuit(ctx, state) != nil {
		return false
	}
	state.mu.Lock()
	defer state.mu.Unlock()
	return state.circuit.LogIntervalMS >= (15 * time.Second).Milliseconds()
}

func (g *upstreamHostGuard) prepareLogPace(state *upstreamGuardHostState, scope upstreamLogQueryScope) {
	state.mu.Lock()
	changed := scope.previouslyLimited && state.circuit.LogIntervalMS == 0
	if changed {
		// Old images already accumulated real 429 failures, but did not retain
		// the learned pace. Start conservatively instead of repeating a burst.
		state.circuit.LogIntervalMS = upstreamLogPaceCeiling.Milliseconds()
	}
	snapshot := state.circuit
	state.mu.Unlock()
	if changed {
		g.persistCircuit(snapshot)
	}
}

func (g *upstreamHostGuard) waitForLogPace(ctx context.Context, state *upstreamGuardHostState) error {
	state.mu.Lock()
	next := state.circuit.LogNextStartMS
	state.mu.Unlock()
	if next <= g.clock.Now().UnixMilli() {
		return ctx.Err()
	}
	return g.clock.Wait(ctx, time.UnixMilli(next).Sub(g.clock.Now()))
}

func (g *upstreamHostGuard) recordLogStart(state *upstreamGuardHostState) {
	state.mu.Lock()
	interval := state.circuit.LogIntervalMS
	if interval <= 0 {
		state.mu.Unlock()
		return
	}
	state.circuit.LogNextStartMS = g.clock.Now().Add(time.Duration(interval) * time.Millisecond).UnixMilli()
	snapshot := state.circuit
	state.mu.Unlock()
	g.persistCircuit(snapshot)
}

func (g *upstreamHostGuard) recordLogResponse(state *upstreamGuardHostState, status int) {
	state.mu.Lock()
	changed := false
	if status == http.StatusTooManyRequests {
		state.circuit.LogIntervalMS = min(upstreamLogPaceCeiling.Milliseconds(), max(int64(2000), 2*state.circuit.LogIntervalMS))
		state.circuit.LogSuccesses = 0
		state.circuit.LogNextStartMS = max(state.circuit.LogNextStartMS, g.clock.Now().UnixMilli()+state.circuit.LogIntervalMS)
		changed = true
	} else if status >= 200 && status < 300 && state.circuit.LogIntervalMS > 0 {
		state.circuit.LogSuccesses++
		// Only sustained successful LOG queries permit slow recovery. Never
		// shrink the already reserved deadline or bypass a Retry-After gate.
		if state.circuit.LogSuccesses >= 16 {
			state.circuit.LogIntervalMS = max(int64(1000), state.circuit.LogIntervalMS/2)
			state.circuit.LogSuccesses = 0
		}
		changed = true
	}
	snapshot := state.circuit
	state.mu.Unlock()
	if changed {
		g.persistCircuit(snapshot)
	}
}
