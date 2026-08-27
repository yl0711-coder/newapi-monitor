package monitor

import (
	"context"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/glebarez/sqlite"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

type fakeUpstreamGuardClock struct {
	mu    sync.Mutex
	now   time.Time
	waits []time.Duration
}

func (c *fakeUpstreamGuardClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeUpstreamGuardClock) Wait(ctx context.Context, duration time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.waits = append(c.waits, duration)
	c.now = c.now.Add(duration)
	c.mu.Unlock()
	return nil
}

func (c *fakeUpstreamGuardClock) Advance(duration time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(duration)
	c.mu.Unlock()
}

type cancelUpstreamGuardClock struct {
	now     time.Time
	waiting chan struct{}
}

func (c *cancelUpstreamGuardClock) Now() time.Time { return c.now }

func (c *cancelUpstreamGuardClock) Wait(ctx context.Context, _ time.Duration) error {
	select {
	case c.waiting <- struct{}{}:
	default:
	}
	<-ctx.Done()
	return ctx.Err()
}

type upstreamRoundTripFunc func(*http.Request) (*http.Response, error)

func (fn upstreamRoundTripFunc) RoundTrip(req *http.Request) (*http.Response, error) { return fn(req) }

type failingUpstreamBody struct{}

func (failingUpstreamBody) Read([]byte) (int, error) { return 0, io.ErrUnexpectedEOF }
func (failingUpstreamBody) Close() error             { return nil }

func upstreamGuardResponse(status int, headers http.Header) *http.Response {
	if headers == nil {
		headers = make(http.Header)
	}
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     headers,
		Body:       io.NopCloser(strings.NewReader(`{"ok":true}`)),
	}
}

func newUpstreamGuardTestStore(t *testing.T) *gorm.DB {
	t.Helper()
	name := strings.ReplaceAll(t.Name(), "/", "_")
	db, err := gorm.Open(sqlite.Open("file:"+name+"?mode=memory&cache=shared"), &gorm.Config{
		Logger: logger.Default.LogMode(logger.Silent),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := db.AutoMigrate(&UpstreamHostCircuit{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if sqlDB, err := db.DB(); err == nil {
			_ = sqlDB.Close()
		}
	})
	return db
}

func newGuardedTestClient(base http.RoundTripper, store *gorm.DB, clock upstreamGuardClock, interval, jitter time.Duration) *http.Client {
	guard := newUpstreamHostGuard(store, upstreamHostGuardOptions{
		Clock: clock, MinInterval: interval,
		Jitter: func() time.Duration { return jitter },
	})
	return installUpstreamHostGuardForTest(&http.Client{Transport: base}, store, guard)
}

func TestUpstreamHostGuardClampsGlobalConcurrency(t *testing.T) {
	for _, test := range []struct {
		configured int
		want       int
	}{{0, 1}, {1, 1}, {2, 2}, {99, 2}} {
		guard := newUpstreamHostGuard(nil, upstreamHostGuardOptions{
			Clock:  &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)},
			Jitter: func() time.Duration { return 0 }, GlobalConcurrency: test.configured,
		})
		if got := cap(guard.globalSem); got != test.want {
			t.Fatalf("configured=%d capacity=%d, want %d", test.configured, got, test.want)
		}
	}
}

func doGuardTestRequest(t *testing.T, client *http.Client, method, endpoint string) (*http.Response, error) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, endpoint, nil)
	if err != nil {
		t.Fatal(err)
	}
	return client.Do(req)
}

func consumeGuardResponse(t *testing.T, response *http.Response) {
	t.Helper()
	if response == nil || response.Body == nil {
		return
	}
	if _, err := io.Copy(io.Discard, response.Body); err != nil {
		t.Fatal(err)
	}
	if err := response.Body.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestUpstreamHostGuardGlobalConcurrencyIncludesResponseBody(t *testing.T) {
	clock := &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)}
	var calls atomic.Int64
	started := make(chan int64, 2)
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		started <- call
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), nil, clock, 0, 0)

	first, err := doGuardTestRequest(t, client, http.MethodGet, "https://a.example/one")
	if err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != 1 {
		t.Fatalf("first call=%d", got)
	}
	secondDone := make(chan *http.Response, 1)
	secondErr := make(chan error, 1)
	go func() {
		// The receiver deliberately keeps ownership of this body so the test can
		// prove the global slot remains held until the first response is closed.
		//nolint:bodyclose
		response, requestErr := doGuardTestRequest(t, client, http.MethodGet, "https://b.example/two")
		if requestErr != nil {
			secondErr <- requestErr
			return
		}
		secondDone <- response
	}()
	select {
	case got := <-started:
		t.Fatalf("second host started while first response body was open: call=%d", got)
	case err := <-secondErr:
		t.Fatalf("second request failed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-started:
		if got != 2 {
			t.Fatalf("second call=%d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("second request did not start after first body closed")
	}
	select {
	case response := <-secondDone:
		consumeGuardResponse(t, response)
	case err := <-secondErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("second request did not finish")
	}
}

func TestUpstreamHostGuardEarlyCloseSettlesSuccessAndReleasesSlot(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)}
	seed := UpstreamHostCircuit{
		HostKey: "early-close.example:443", ConsecutiveFailures: 2,
		LastFailureAt: clock.Now().Unix(), LastStatus: http.StatusServiceUnavailable,
		UpdatedAt: clock.Now().Unix(),
	}
	if err := store.Create(&seed).Error; err != nil {
		t.Fatal(err)
	}

	var calls atomic.Int64
	started := make(chan int64, 2)
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		started <- call
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), store, clock, 0, 0)

	first, err := doGuardTestRequest(t, client, http.MethodGet, "https://early-close.example/first")
	if err != nil {
		t.Fatal(err)
	}
	if got := <-started; got != 1 {
		t.Fatalf("first call=%d", got)
	}
	secondDone := make(chan *http.Response, 1)
	secondErr := make(chan error, 1)
	go func() {
		//nolint:bodyclose // 交给主测试协程在断言后关闭。
		response, requestErr := doGuardTestRequest(t, client, http.MethodGet, "https://early-close.example/second")
		if requestErr != nil {
			secondErr <- requestErr
			return
		}
		secondDone <- response
	}()
	select {
	case got := <-started:
		t.Fatalf("second request started before early Close released slot: call=%d", got)
	case err := <-secondErr:
		t.Fatalf("second request failed: %v", err)
	case <-time.After(30 * time.Millisecond):
	}

	// 不读取任何 body 字节：Close 本身必须同时结算 2xx
	// 并释放全局/同主机单槽。
	if err := first.Body.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-started:
		if got != 2 {
			t.Fatalf("second call=%d", got)
		}
	case <-time.After(time.Second):
		t.Fatal("early Close did not release concurrency slot")
	}
	select {
	case response := <-secondDone:
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
	case err := <-secondErr:
		t.Fatal(err)
	case <-time.After(time.Second):
		t.Fatal("second request did not finish")
	}

	var persisted UpstreamHostCircuit
	if err := store.First(&persisted, "host_key = ?", seed.HostKey).Error; err != nil {
		t.Fatal(err)
	}
	if persisted.ConsecutiveFailures != 0 || persisted.OpenCount != 0 || persisted.OpenUntil != 0 || persisted.LastStatus != 0 {
		t.Fatalf("early-close 2xx did not reset host circuit: %+v", persisted)
	}
}

func TestUpstreamHostGuardUsesGlobalOneWayJitteredStartInterval(t *testing.T) {
	start := time.Unix(1_800_000_000, 0)
	clock := &fakeUpstreamGuardClock{now: start}
	var starts []time.Time
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		starts = append(starts, clock.Now())
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), nil, clock, time.Second, 200*time.Millisecond)

	for _, endpoint := range []string{"https://a.example/1", "https://b.example/2", "https://a.example/3"} {
		response, err := doGuardTestRequest(t, client, http.MethodGet, endpoint)
		if err != nil {
			t.Fatal(err)
		}
		consumeGuardResponse(t, response)
	}
	if len(starts) != 3 {
		t.Fatalf("starts=%d", len(starts))
	}
	for index := 1; index < len(starts); index++ {
		if gap := starts[index].Sub(starts[index-1]); gap < 1200*time.Millisecond {
			t.Fatalf("start gap %d=%s, want >=1.2s", index, gap)
		}
	}
}

func TestUpstreamHostGuardPersistsRetryAfterAcrossRestart(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	start := time.Unix(1_800_000_000, 0)
	clock := &fakeUpstreamGuardClock{now: start}
	var firstCalls atomic.Int64
	first := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		firstCalls.Add(1)
		headers := make(http.Header)
		headers.Set("Retry-After", "120")
		return upstreamGuardResponse(http.StatusTooManyRequests, headers), nil
	}), store, clock, 0, 0)
	response, err := doGuardTestRequest(t, first, http.MethodGet, "https://rate.example/private?token=must-not-persist")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)

	var persisted UpstreamHostCircuit
	if err := store.First(&persisted, "host_key = ?", "rate.example:443").Error; err != nil {
		t.Fatal(err)
	}
	if persisted.OpenUntil != start.Add(2*time.Minute).Unix() || persisted.LastStatus != http.StatusTooManyRequests {
		t.Fatalf("persisted circuit=%+v", persisted)
	}
	if strings.Contains(persisted.HostKey, "token") || strings.Contains(persisted.HostKey, "private") {
		t.Fatalf("sensitive URL material persisted: %+v", persisted)
	}

	var restartedCalls atomic.Int64
	restarted := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		restartedCalls.Add(1)
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), store, clock, 0, 0)
	blockedResponse, err := doGuardTestRequest(t, restarted, http.MethodGet, "https://rate.example/after-restart")
	consumeGuardResponse(t, blockedResponse)
	if err == nil || upstreamRetryAt(err) != persisted.OpenUntil || restartedCalls.Load() != 0 {
		t.Fatalf("persisted Retry-After bypassed: retry_at=%d calls=%d err=%v", upstreamRetryAt(err), restartedCalls.Load(), err)
	}
	clock.Advance(121 * time.Second)
	response, err = doGuardTestRequest(t, restarted, http.MethodGet, "https://rate.example/half-open")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)
	if restartedCalls.Load() != 1 {
		t.Fatalf("calls after cooldown=%d", restartedCalls.Load())
	}
}

func TestUpstreamHostGuardOpensAfterThreeRetryableResponsesAndHalfOpens(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)}
	var calls atomic.Int64
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		call := calls.Add(1)
		if call <= 3 {
			return upstreamGuardResponse(http.StatusServiceUnavailable, nil), nil
		}
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), store, clock, 0, 0)
	for range 3 {
		response, err := doGuardTestRequest(t, client, http.MethodGet, "https://unstable.example/api")
		if err != nil {
			t.Fatal(err)
		}
		consumeGuardResponse(t, response)
	}
	blockedResponse, err := doGuardTestRequest(t, client, http.MethodGet, "https://unstable.example/api")
	consumeGuardResponse(t, blockedResponse)
	if err == nil || calls.Load() != 3 {
		t.Fatalf("circuit did not open after three 503s: calls=%d err=%v", calls.Load(), err)
	}
	clock.Advance(5*time.Minute + time.Second)
	response, err := doGuardTestRequest(t, client, http.MethodGet, "https://unstable.example/api")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)
	if calls.Load() != 4 {
		t.Fatalf("half-open probe calls=%d", calls.Load())
	}
	response, err = doGuardTestRequest(t, client, http.MethodGet, "https://unstable.example/api")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)
	if calls.Load() != 5 {
		t.Fatalf("successful half-open did not close circuit: calls=%d", calls.Load())
	}
}

func TestUpstreamHostGuardCountsNetworkFailures(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)}
	var calls atomic.Int64
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		calls.Add(1)
		return nil, errors.New("temporary network failure")
	}), store, clock, 0, 0)
	for range 3 {
		response, err := doGuardTestRequest(t, client, http.MethodGet, "https://network.example/api")
		consumeGuardResponse(t, response)
		if err == nil {
			t.Fatal("network request unexpectedly succeeded")
		}
	}
	response, err := doGuardTestRequest(t, client, http.MethodGet, "https://network.example/api")
	consumeGuardResponse(t, response)
	if err == nil || calls.Load() != 3 {
		t.Fatalf("network circuit not enforced: calls=%d err=%v", calls.Load(), err)
	}
}

func TestUpstreamHostGuardCountsResponseBodyNetworkFailures(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)}
	var calls atomic.Int64
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		calls.Add(1)
		response := upstreamGuardResponse(http.StatusOK, nil)
		response.Body = failingUpstreamBody{}
		return response, nil
	}), store, clock, 0, 0)
	for range 3 {
		response, err := doGuardTestRequest(t, client, http.MethodGet, "https://truncated.example/api")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.ReadAll(response.Body); !errors.Is(err, io.ErrUnexpectedEOF) {
			t.Fatalf("body err=%v", err)
		}
		_ = response.Body.Close()
	}
	blockedResponse, err := doGuardTestRequest(t, client, http.MethodGet, "https://truncated.example/api")
	consumeGuardResponse(t, blockedResponse)
	if err == nil || calls.Load() != 3 {
		t.Fatalf("body failure circuit not enforced: calls=%d err=%v", calls.Load(), err)
	}
}

func TestUpstreamHostGuardDoesNotShareAuthFailures(t *testing.T) {
	store := newUpstreamGuardTestStore(t)
	clock := &fakeUpstreamGuardClock{now: time.Unix(1_800_000_000, 0)}
	seed := UpstreamHostCircuit{
		HostKey: "shared.example:443", ConsecutiveFailures: 2,
		LastFailureAt: clock.Now().Unix(), LastStatus: http.StatusServiceUnavailable,
		UpdatedAt: clock.Now().Unix(),
	}
	if err := store.Create(&seed).Error; err != nil {
		t.Fatal(err)
	}
	statuses := []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusOK}
	var calls atomic.Int64
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		index := int(calls.Add(1)) - 1
		return upstreamGuardResponse(statuses[index], nil), nil
	}), store, clock, 0, 0)
	for index := range statuses {
		response, err := doGuardTestRequest(t, client, http.MethodGet, "https://shared.example/api")
		if err != nil {
			t.Fatal(err)
		}
		// 账户错误也覆盖早关 body；共享主机失败连续数
		// 在 401/403 后保持，只有真实 2xx 才复位。
		if err := response.Body.Close(); err != nil {
			t.Fatal(err)
		}
		var persisted UpstreamHostCircuit
		if err := store.First(&persisted, "host_key = ?", seed.HostKey).Error; err != nil {
			t.Fatal(err)
		}
		if index < 2 && persisted.ConsecutiveFailures != seed.ConsecutiveFailures {
			t.Fatalf("auth status %d changed shared host circuit: %+v", statuses[index], persisted)
		}
		if index == 2 && persisted.ConsecutiveFailures != 0 {
			t.Fatalf("2xx did not reset shared host circuit: %+v", persisted)
		}
	}
	if calls.Load() != int64(len(statuses)) {
		t.Fatalf("auth failure opened host circuit: calls=%d", calls.Load())
	}

	now := clock.Now().Unix()
	balance := ChannelUpstreamAccount{Domain: "account-a.example"}
	applyUpstreamSyncResult(&balance, upstreamBalanceResult{}, &upstreamAuthError{err: errors.New("401")}, now, Settings{})
	if balance.Status != upstreamStatusReconnect || balance.NextSyncAt != upstreamAccountIsolatedUntil {
		t.Fatalf("balance account not isolated: %+v", balance)
	}
	usage := ChannelUpstreamAccount{Domain: "account-b.example"}
	applyUpstreamUsageResult(&usage, upstreamUsageResult{}, &upstreamAuthError{err: errors.New("403")}, now, Settings{})
	if usage.UsageStatus != upstreamStatusReconnect || usage.UsageNextSyncAt != upstreamAccountIsolatedUntil {
		t.Fatalf("usage account not isolated: %+v", usage)
	}
}

func TestUpstreamHostGuardPacingIsCancelableWithoutCircuitPenalty(t *testing.T) {
	clock := &cancelUpstreamGuardClock{now: time.Unix(1_800_000_000, 0), waiting: make(chan struct{}, 1)}
	var calls atomic.Int64
	client := newGuardedTestClient(upstreamRoundTripFunc(func(_ *http.Request) (*http.Response, error) {
		calls.Add(1)
		return upstreamGuardResponse(http.StatusOK, nil), nil
	}), nil, clock, time.Second, 0)
	response, err := doGuardTestRequest(t, client, http.MethodGet, "https://cancel.example/first")
	if err != nil {
		t.Fatal(err)
	}
	consumeGuardResponse(t, response)

	ctx, cancel := context.WithCancel(context.Background())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://cancel.example/second", nil)
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		response, requestErr := client.Do(req)
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		done <- requestErr
	}()
	select {
	case <-clock.waiting:
	case <-time.After(time.Second):
		t.Fatal("second request did not enter cancellable pacing wait")
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancel err=%v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("canceled pacing did not return")
	}
	if calls.Load() != 1 {
		t.Fatalf("canceled request reached transport: calls=%d", calls.Load())
	}
}

func TestUpstreamRetryAfterBoundsAndUsageBudget(t *testing.T) {
	now := time.Unix(1_800_000_000, 0)
	if got := parseUpstreamRetryAfter("1", now); !got.Equal(now.Add(upstreamRetryAfterMin)) {
		t.Fatalf("minimum Retry-After=%s", got)
	}
	if got := parseUpstreamRetryAfter("999999", now); !got.Equal(now.Add(upstreamRetryAfterMax)) {
		t.Fatalf("maximum Retry-After=%s", got)
	}
	if got := parseUpstreamRetryAfter("invalid", now); !got.Equal(now.Add(upstreamRetryAfterDefault)) {
		t.Fatalf("default Retry-After=%s", got)
	}
	if upstreamUsageMaxRequestsPerRun != 60 {
		t.Fatalf("usage request budget=%d, want 60", upstreamUsageMaxRequestsPerRun)
	}
	if timeout := upstreamUsageOperationTimeout(Settings{}); timeout < time.Duration(upstreamUsageMaxRequestsPerRun-1)*upstreamGuardMinInterval || timeout > 2*time.Minute {
		t.Fatalf("usage operation timeout=%s does not cover guarded pacing", timeout)
	}
	retryAt := now.Add(2 * time.Hour).Unix()
	balance := ChannelUpstreamAccount{Domain: "rate.example"}
	applyUpstreamSyncResult(&balance, upstreamBalanceResult{}, &upstreamHTTPError{Status: http.StatusTooManyRequests, RetryAt: retryAt}, now.Unix(), Settings{})
	if balance.NextSyncAt != retryAt {
		t.Fatalf("balance Retry-After schedule=%d, want %d", balance.NextSyncAt, retryAt)
	}
	usage := ChannelUpstreamAccount{Domain: "rate.example"}
	applyUpstreamUsageResult(&usage, upstreamUsageResult{}, &upstreamHTTPError{Status: http.StatusTooManyRequests, RetryAt: retryAt}, now.Unix(), Settings{})
	if usage.UsageNextSyncAt != retryAt {
		t.Fatalf("usage Retry-After schedule=%d, want %d", usage.UsageNextSyncAt, retryAt)
	}
	for _, status := range []int{http.StatusRequestTimeout, http.StatusTooEarly, http.StatusInternalServerError, http.StatusHTTPVersionNotSupported} {
		if !isUpstreamCircuitStatus(status) {
			t.Fatalf("status %d must count toward circuit", status)
		}
	}
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden, http.StatusTooManyRequests, http.StatusNotFound} {
		if isUpstreamCircuitStatus(status) {
			t.Fatalf("status %d must not use the three-strike circuit path", status)
		}
	}
}
