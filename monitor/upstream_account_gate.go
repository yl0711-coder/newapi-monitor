package monitor

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"
)

// errUpstreamAccountBusy is an internal scheduler yield, not an upstream
// failure. Background workers must never turn it into a persisted error or a
// retry backoff: another operation for the same configured account is already
// making progress.
var errUpstreamAccountBusy = errors.New("上游账户正在执行其他操作")

// upstreamAccountGate serializes credential use per configured account.
//
// The old implementation used one process-wide mutex for every upstream.
// A dense history scan on one host could therefore block an unrelated admin
// save until its HTTP request expired. Per-account gates retain the important
// Sub2API refresh-token guarantee while allowing independent domains to make
// progress. Background work is try-only and yields to a waiting admin, so it
// cannot build an unbounded queue or starve configuration changes.
type upstreamAccountGate struct {
	token        chan struct{}
	adminWaiters atomic.Int64
}

func newUpstreamAccountGate() *upstreamAccountGate {
	return &upstreamAccountGate{token: make(chan struct{}, 1)}
}

func (g *upstreamAccountGate) acquireAdmin(ctx context.Context) (func(), error) {
	g.adminWaiters.Add(1)
	defer g.adminWaiters.Add(-1)
	select {
	case g.token <- struct{}{}:
		return func() { <-g.token }, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (g *upstreamAccountGate) tryAcquireBackground() (func(), bool) {
	if g.adminWaiters.Load() > 0 {
		return nil, false
	}
	select {
	case g.token <- struct{}{}:
		// Close the race in which an admin arrived between the first check and
		// acquiring the token. The worker yields before making any request.
		if g.adminWaiters.Load() > 0 {
			<-g.token
			return nil, false
		}
		return func() { <-g.token }, true
	default:
		return nil, false
	}
}

func (m *Monitor) upstreamAccountGate(domain string) *upstreamAccountGate {
	domain = strings.ToLower(strings.TrimSpace(domain))
	m.upstreamAccountGatesMu.Lock()
	defer m.upstreamAccountGatesMu.Unlock()
	if m.upstreamAccountGates == nil {
		m.upstreamAccountGates = make(map[string]*upstreamAccountGate)
	}
	gate := m.upstreamAccountGates[domain]
	if gate == nil {
		gate = newUpstreamAccountGate()
		m.upstreamAccountGates[domain] = gate
	}
	return gate
}

func (m *Monitor) acquireUpstreamAccountAdmin(ctx context.Context, domain string) (func(), error) {
	return m.upstreamAccountGate(domain).acquireAdmin(ctx)
}

func (m *Monitor) tryAcquireUpstreamAccountBackground(domain string) (func(), error) {
	release, ok := m.upstreamAccountGate(domain).tryAcquireBackground()
	if !ok {
		return nil, errUpstreamAccountBusy
	}
	return release, nil
}
