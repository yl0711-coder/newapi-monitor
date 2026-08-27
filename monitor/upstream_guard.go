package monitor

import (
	"context"
	cryptorand "crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const (
	upstreamGuardMinInterval      = time.Second
	upstreamGuardMaxJitter        = 250 * time.Millisecond
	upstreamGuardFailureThreshold = 3
	upstreamRetryAfterDefault     = 15 * time.Minute
	upstreamRetryAfterMin         = 30 * time.Second
	upstreamRetryAfterMax         = 6 * time.Hour
	// An authentication failure belongs to one configured account, not to the
	// shared host. Keep it out of automatic due scans until an administrator
	// explicitly reconnects/saves or manually retries that account.
	upstreamAccountIsolatedUntil = int64(1 << 62)
)

var upstreamGuardOpenDurations = [...]time.Duration{
	5 * time.Minute,
	15 * time.Minute,
	30 * time.Minute,
	time.Hour,
}

// UpstreamHostCircuit is deliberately host-scoped and contains no account,
// URL path, query, header or credential. Persisting the retry gate prevents a
// process restart from bypassing an upstream Retry-After/circuit cooldown.
type UpstreamHostCircuit struct {
	HostKey             string `gorm:"primaryKey;size:320;column:host_key"`
	ConsecutiveFailures int    `gorm:"column:consecutive_failures"`
	OpenCount           int    `gorm:"column:open_count"`
	OpenUntil           int64  `gorm:"column:open_until;index"`
	LastFailureAt       int64  `gorm:"column:last_failure_at"`
	LastStatus          int    `gorm:"column:last_status"`
	UpdatedAt           int64  `gorm:"column:updated_at"`
}

type upstreamGuardClock interface {
	Now() time.Time
	Wait(context.Context, time.Duration) error
}

type realUpstreamGuardClock struct{}

func (realUpstreamGuardClock) Now() time.Time { return time.Now() }

func (realUpstreamGuardClock) Wait(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

type upstreamGuardJitter func() time.Duration

func randomUpstreamGuardJitter() time.Duration {
	var raw [8]byte
	if _, err := cryptorand.Read(raw[:]); err != nil {
		// A missing entropy source must not disable the mandatory one-second
		// interval. Zero is a valid one-way jitter fallback.
		return 0
	}
	span := uint64(upstreamGuardMaxJitter/time.Millisecond) + 1
	return time.Duration(binary.LittleEndian.Uint64(raw[:])%span) * time.Millisecond
}

type upstreamHostGuardOptions struct {
	Clock             upstreamGuardClock
	Jitter            upstreamGuardJitter
	MinInterval       time.Duration
	GlobalConcurrency int
}

type upstreamHostGuard struct {
	clock       upstreamGuardClock
	jitter      upstreamGuardJitter
	minInterval time.Duration

	globalSem  chan struct{}
	globalMu   sync.Mutex
	globalNext time.Time

	storeMu sync.RWMutex
	store   *gorm.DB

	hostsMu sync.Mutex
	hosts   map[string]*upstreamGuardHostState
}

type upstreamGuardHostState struct {
	sem chan struct{}

	mu        sync.Mutex
	loaded    bool
	nextStart time.Time
	circuit   UpstreamHostCircuit
}

func newUpstreamHostGuard(store *gorm.DB, options upstreamHostGuardOptions) *upstreamHostGuard {
	clock := options.Clock
	if clock == nil {
		clock = realUpstreamGuardClock{}
	}
	jitter := options.Jitter
	if jitter == nil {
		jitter = randomUpstreamGuardJitter
	}
	interval := options.MinInterval
	if interval < 0 {
		interval = 0
	}
	if options.MinInterval == 0 && options.Clock == nil && options.Jitter == nil {
		interval = upstreamGuardMinInterval
	}
	globalConcurrency := options.GlobalConcurrency
	if globalConcurrency < 1 {
		globalConcurrency = 1
	}
	// 当前生产主机只有约 1 GiB 内存；该硬上限既保护本机，也避免一个
	// 错误环境变量把多个上游同时打成突发。按 host 的 semaphore 仍是 1。
	if globalConcurrency > 2 {
		globalConcurrency = 2
	}
	return &upstreamHostGuard{
		clock: clock, jitter: jitter, minInterval: interval,
		globalSem: make(chan struct{}, globalConcurrency), store: store,
		hosts: make(map[string]*upstreamGuardHostState),
	}
}

func (g *upstreamHostGuard) setStore(store *gorm.DB) {
	if g == nil || store == nil {
		return
	}
	g.storeMu.Lock()
	if g.store == nil {
		g.store = store
	}
	g.storeMu.Unlock()
}

func (g *upstreamHostGuard) currentStore() *gorm.DB {
	g.storeMu.RLock()
	defer g.storeMu.RUnlock()
	return g.store
}

func upstreamGuardHostKey(u *url.URL) (string, error) {
	if u == nil {
		return "", errors.New("上游请求地址无效")
	}
	host := strings.ToLower(strings.TrimSpace(u.Hostname()))
	if host == "" {
		return "", errors.New("上游请求缺少主机")
	}
	port := u.Port()
	if port == "" {
		switch strings.ToLower(u.Scheme) {
		case "https":
			port = "443"
		case "http":
			port = "80"
		default:
			return "", errors.New("上游请求协议无效")
		}
	}
	return net.JoinHostPort(host, port), nil
}

func (g *upstreamHostGuard) hostState(hostKey string) *upstreamGuardHostState {
	g.hostsMu.Lock()
	defer g.hostsMu.Unlock()
	state := g.hosts[hostKey]
	if state == nil {
		state = &upstreamGuardHostState{
			sem:     make(chan struct{}, 1),
			circuit: UpstreamHostCircuit{HostKey: hostKey},
		}
		g.hosts[hostKey] = state
	}
	return state
}

func acquireUpstreamGuard(ctx context.Context, semaphore chan struct{}) error {
	select {
	case semaphore <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func releaseUpstreamGuard(semaphore chan struct{}) { <-semaphore }

func (g *upstreamHostGuard) loadCircuit(ctx context.Context, state *upstreamGuardHostState) error {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.loaded {
		return nil
	}
	store := g.currentStore()
	if store != nil {
		var persisted UpstreamHostCircuit
		err := store.WithContext(ctx).First(&persisted, "host_key = ?", state.circuit.HostKey).Error
		if err == nil {
			state.circuit = persisted
		} else if !errors.Is(err, gorm.ErrRecordNotFound) {
			return errors.New("读取上游保护状态失败")
		}
	}
	state.loaded = true
	return nil
}

func (g *upstreamHostGuard) persistCircuit(snapshot UpstreamHostCircuit) {
	store := g.currentStore()
	if store == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := store.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "host_key"}},
		UpdateAll: true,
	}).Create(&snapshot).Error; err != nil {
		// Host is safe to log; no URL path, query, account or credential reaches
		// this model or message.
		slog.Warn("保存上游主机保护状态失败", "host", snapshot.HostKey)
	}
}

func (g *upstreamHostGuard) circuitRetryAt(state *upstreamGuardHostState) int64 {
	state.mu.Lock()
	defer state.mu.Unlock()
	now := g.clock.Now().Unix()
	if state.circuit.OpenUntil > now {
		return state.circuit.OpenUntil
	}
	return 0
}

func clampUpstreamGuardJitter(jitter time.Duration) time.Duration {
	if jitter < 0 {
		return 0
	}
	if jitter > upstreamGuardMaxJitter {
		return upstreamGuardMaxJitter
	}
	return jitter
}

func (g *upstreamHostGuard) waitForStart(ctx context.Context, state *upstreamGuardHostState) error {
	for {
		now := g.clock.Now()
		g.globalMu.Lock()
		waitUntil := g.globalNext
		g.globalMu.Unlock()
		state.mu.Lock()
		if state.nextStart.After(waitUntil) {
			waitUntil = state.nextStart
		}
		state.mu.Unlock()
		if !waitUntil.After(now) {
			break
		}
		if err := g.clock.Wait(ctx, waitUntil.Sub(now)); err != nil {
			return err
		}
	}
	started := g.clock.Now()
	next := started.Add(g.minInterval + clampUpstreamGuardJitter(g.jitter()))
	g.globalMu.Lock()
	g.globalNext = next
	g.globalMu.Unlock()
	state.mu.Lock()
	state.nextStart = next
	state.mu.Unlock()
	return nil
}

func parseUpstreamRetryAfter(value string, now time.Time) time.Time {
	value = strings.TrimSpace(value)
	var retryAt time.Time
	if value != "" {
		if seconds, err := time.ParseDuration(value + "s"); err == nil {
			retryAt = now.Add(seconds)
		} else if parsed, err := http.ParseTime(value); err == nil {
			retryAt = parsed
		}
	}
	if retryAt.IsZero() {
		retryAt = now.Add(upstreamRetryAfterDefault)
	}
	minimum := now.Add(upstreamRetryAfterMin)
	maximum := now.Add(upstreamRetryAfterMax)
	if retryAt.Before(minimum) {
		return minimum
	}
	if retryAt.After(maximum) {
		return maximum
	}
	return retryAt
}

func isUpstreamCircuitStatus(status int) bool {
	return status == http.StatusRequestTimeout || status == http.StatusTooEarly || status >= 500 && status <= 599
}

func isUpstreamAccountAuthStatus(status int) bool {
	return status == http.StatusUnauthorized || status == http.StatusForbidden
}

func (g *upstreamHostGuard) recordResponse(state *upstreamGuardHostState, status int, retryAfter string) {
	now := g.clock.Now()
	state.mu.Lock()
	changed := false
	switch {
	case status == http.StatusTooManyRequests:
		state.circuit.ConsecutiveFailures++
		state.circuit.OpenCount++
		state.circuit.OpenUntil = parseUpstreamRetryAfter(retryAfter, now).Unix()
		state.circuit.LastFailureAt = now.Unix()
		state.circuit.LastStatus = status
		changed = true
	case isUpstreamCircuitStatus(status):
		state.circuit.ConsecutiveFailures++
		state.circuit.LastFailureAt = now.Unix()
		state.circuit.LastStatus = status
		if state.circuit.ConsecutiveFailures >= upstreamGuardFailureThreshold {
			index := state.circuit.OpenCount
			if index >= len(upstreamGuardOpenDurations) {
				index = len(upstreamGuardOpenDurations) - 1
			}
			state.circuit.OpenCount++
			state.circuit.OpenUntil = now.Add(upstreamGuardOpenDurations[index]).Unix()
		}
		changed = true
	case isUpstreamAccountAuthStatus(status):
		// 401/403 只说明当前账户凭据不可用，既不惩罚共享主机，
		// 也不代替真实的 2xx 去清空主机可用性失败连续数。
		// 账户隔离由 channel_upstream(_usage).go 持久化。
	default:
		if state.circuit.ConsecutiveFailures != 0 || state.circuit.OpenCount != 0 || state.circuit.OpenUntil != 0 || state.circuit.LastStatus != 0 {
			state.circuit.ConsecutiveFailures = 0
			state.circuit.OpenCount = 0
			state.circuit.OpenUntil = 0
			state.circuit.LastStatus = 0
			changed = true
		}
	}
	state.circuit.UpdatedAt = now.Unix()
	snapshot := state.circuit
	state.mu.Unlock()
	if changed {
		g.persistCircuit(snapshot)
	}
}

// status zero is a transport/network failure and follows the same circuit
// policy as 408/425/5xx. Keeping it outside isUpstreamCircuitStatus makes the
// latter safe for ordinary HTTP status classification.
func (g *upstreamHostGuard) recordNetworkFailure(state *upstreamGuardHostState) {
	now := g.clock.Now()
	state.mu.Lock()
	state.circuit.ConsecutiveFailures++
	state.circuit.LastFailureAt = now.Unix()
	state.circuit.LastStatus = 0
	if state.circuit.ConsecutiveFailures >= upstreamGuardFailureThreshold {
		index := state.circuit.OpenCount
		if index >= len(upstreamGuardOpenDurations) {
			index = len(upstreamGuardOpenDurations) - 1
		}
		state.circuit.OpenCount++
		state.circuit.OpenUntil = now.Add(upstreamGuardOpenDurations[index]).Unix()
	}
	state.circuit.UpdatedAt = now.Unix()
	snapshot := state.circuit
	state.mu.Unlock()
	g.persistCircuit(snapshot)
}

type upstreamCircuitOpenError struct{ RetryAt int64 }

func (e *upstreamCircuitOpenError) Error() string {
	return fmt.Sprintf("上游主机暂时熔断，可在 %s 后重试", time.Unix(e.RetryAt, 0).Format(time.RFC3339))
}

func upstreamRetryAt(err error) int64 {
	var syncErr *upstreamStoredSyncError
	if errors.As(err, &syncErr) {
		return syncErr.retryAt
	}
	var circuitErr *upstreamCircuitOpenError
	if errors.As(err, &circuitErr) {
		return circuitErr.RetryAt
	}
	var statusErr *upstreamHTTPError
	if errors.As(err, &statusErr) {
		return statusErr.RetryAt
	}
	return 0
}

type upstreamGuardTransport struct {
	base  http.RoundTripper
	guard *upstreamHostGuard
}

func (t *upstreamGuardTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req == nil {
		return nil, errors.New("上游请求无效")
	}
	hostKey, err := upstreamGuardHostKey(req.URL)
	if err != nil {
		return nil, err
	}
	state := t.guard.hostState(hostKey)
	if err := acquireUpstreamGuard(req.Context(), t.guard.globalSem); err != nil {
		return nil, err
	}
	globalHeld := true
	releaseGlobal := func() {
		if globalHeld {
			releaseUpstreamGuard(t.guard.globalSem)
			globalHeld = false
		}
	}
	if err := acquireUpstreamGuard(req.Context(), state.sem); err != nil {
		releaseGlobal()
		return nil, err
	}
	hostHeld := true
	release := func() {
		if hostHeld {
			releaseUpstreamGuard(state.sem)
			hostHeld = false
		}
		releaseGlobal()
	}
	if err := t.guard.loadCircuit(req.Context(), state); err != nil {
		release()
		return nil, err
	}
	if retryAt := t.guard.circuitRetryAt(state); retryAt > 0 {
		release()
		return nil, &upstreamCircuitOpenError{RetryAt: retryAt}
	}
	if err := t.guard.waitForStart(req.Context(), state); err != nil {
		release()
		return nil, err
	}
	response, err := t.base.RoundTrip(req)
	if err != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		// Explicit caller cancellation must not punish a healthy shared host.
		// Deadlines and transport timeouts are availability failures and do.
		if !errors.Is(req.Context().Err(), context.Canceled) {
			t.guard.recordNetworkFailure(state)
		}
		release()
		return response, err
	}
	recorded := response.StatusCode == http.StatusTooManyRequests || isUpstreamCircuitStatus(response.StatusCode)
	if recorded {
		t.guard.recordResponse(state, response.StatusCode, response.Header.Get("Retry-After"))
	}
	if response.Body == nil {
		if !recorded {
			t.guard.recordResponse(state, response.StatusCode, "")
		}
		release()
		return response, nil
	}
	response.Body = &upstreamGuardResponseBody{
		ReadCloser: response.Body,
		release:    release,
		guard:      t.guard,
		state:      state,
		ctx:        req.Context(),
		status:     response.StatusCode,
		recorded:   recorded,
	}
	return response, nil
}

func (t *upstreamGuardTransport) CloseIdleConnections() {
	if closer, ok := t.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

type upstreamGuardResponseBody struct {
	io.ReadCloser
	once     sync.Once
	release  func()
	guard    *upstreamHostGuard
	state    *upstreamGuardHostState
	ctx      context.Context
	status   int
	recorded bool
}

func (b *upstreamGuardResponseBody) Read(p []byte) (int, error) {
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.once.Do(func() {
			if errors.Is(err, io.EOF) {
				if !b.recorded {
					b.guard.recordResponse(b.state, b.status, "")
				}
			} else if !b.recorded && !errors.Is(b.ctx.Err(), context.Canceled) {
				b.guard.recordNetworkFailure(b.state)
			}
			b.release()
		})
	}
	return n, err
}

func (b *upstreamGuardResponseBody) Close() error {
	err := b.ReadCloser.Close()
	b.once.Do(func() {
		// 许多调用方只需响应头，会在读到 EOF 前主动 Close。
		// 只要 Read 路径没有先报告网络错误，就必须按已收到的
		// HTTP 状态结算：2xx 可清空失败连续数，401/403 保持账户隔离语义。
		if !b.recorded {
			b.guard.recordResponse(b.state, b.status, "")
		}
		b.release()
	})
	return err
}

var upstreamGuardInstallMu sync.Mutex

func installUpstreamHostGuard(client *http.Client, store *gorm.DB) *http.Client {
	return installUpstreamHostGuardWithConcurrency(client, store, 1)
}

func installUpstreamHostGuardWithConcurrency(client *http.Client, store *gorm.DB, globalConcurrency int) *http.Client {
	if client == nil {
		client = newUpstreamHTTPClient(15 * time.Second)
	}
	upstreamGuardInstallMu.Lock()
	defer upstreamGuardInstallMu.Unlock()
	if guarded, ok := client.Transport.(*upstreamGuardTransport); ok {
		guarded.guard.setStore(store)
		return client
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = &upstreamGuardTransport{
		base:  base,
		guard: newUpstreamHostGuard(store, upstreamHostGuardOptions{GlobalConcurrency: globalConcurrency}),
	}
	return client
}

func installUpstreamHostGuardForTest(client *http.Client, store *gorm.DB, guard *upstreamHostGuard) *http.Client {
	if client == nil {
		client = &http.Client{}
	}
	base := client.Transport
	if base == nil {
		base = http.DefaultTransport
	}
	client.Transport = &upstreamGuardTransport{base: base, guard: guard}
	guard.setStore(store)
	return client
}
