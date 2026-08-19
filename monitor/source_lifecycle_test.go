package monitor

import (
	"context"
	"crypto/x509"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/go-sql-driver/mysql"
)

func lifecycleSourceRefused() error {
	return &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}
}

func lifecycleTestSettings(t *testing.T, probe func(context.Context, *sql.DB) error) Settings {
	t.Helper()
	storePath := filepath.Join(t.TempDir(), "monitor.db")
	return Settings{
		StorePath:                     storePath,
		UsageFactsStorePath:           storePath,
		StoreBackupEnabled:            false,
		StoreMigrationBackupRetention: 1,
		ProdDSN:                       "readonly:secret@tcp(source.invalid:3306)/newapi?timeout=1s",
		NewAPIBaseURL:                 "http://newapi.invalid",
		SessionSecret:                 "unit-test-session-secret",
		SourceWorkerEnabled:           true,
		SourceLeaseRequired:           false,
		SourceLeaseName:               "lifecycle-test",
		SampleSeconds:                 10,
		StabilityEnabled:              false,
		StabilityBackfillEnabled:      false,
		UpstreamSyncEnabled:           false,
		AlertsDisabled:                true,
		sourceLifecycleConfigured:     true,
		sourceOpen: func(string) (*sql.DB, error) {
			return sql.Open("sqlite", ":memory:")
		},
		sourceProbe:         probe,
		sourceRetryDelay:    func(int) time.Duration { return 5 * time.Millisecond },
		sourceCheckInterval: 10 * time.Millisecond,
		sourceDrainTimeout:  500 * time.Millisecond,
		localProbeInterval:  time.Hour,
	}
}

func lifecycleEventually(t *testing.T, timeout time.Duration, condition func() bool, message string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	if !condition() {
		t.Fatal(message)
	}
}

func lifecycleRequest(t *testing.T, m *Monitor, path string) (*httptest.ResponseRecorder, readyStatusResponse) {
	t.Helper()
	w := httptest.NewRecorder()
	c, _ := gin.CreateTestContext(w)
	c.Request = httptest.NewRequest(http.MethodGet, path, nil)
	switch path {
	case "/ready":
		m.serveReady(c)
	default:
		m.serveLive(c)
	}
	var status readyStatusResponse
	if path == "/ready" {
		if err := json.Unmarshal(w.Body.Bytes(), &status); err != nil {
			t.Fatalf("decode ready: %v body=%s", err, w.Body.String())
		}
	}
	return w, status
}

func TestNewTransientSourceFailureKeepsSQLiteAndRecoversWithoutEpochOverlap(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var sourceDown atomic.Bool
	sourceDown.Store(true)
	probe := func(context.Context, *sql.DB) error {
		if sourceDown.Load() {
			return lifecycleSourceRefused()
		}
		return nil
	}
	cfg := lifecycleTestSettings(t, probe)
	cfg.UsageFactsEnabled = true
	cfg.UsageFactsReadEnabled = true
	var starts, active, maxActive atomic.Int32
	started := make(chan struct{}, 4)
	stopped := make(chan struct{}, 4)
	cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) {
		starts.Add(1)
		current := active.Add(1)
		for {
			old := maxActive.Load()
			if current <= old || maxActive.CompareAndSwap(old, current) {
				break
			}
		}
		started <- struct{}{}
		<-ctx.Done()
		// 故意延迟退场，证明 supervisor 会 Wait 而非立即开新 epoch。
		time.Sleep(20 * time.Millisecond)
		active.Add(-1)
		stopped <- struct{}{}
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("transient source failure must degrade, not abort: %v", err)
	}
	defer m.Close()
	if m.currentSourceState() != sourceStateDegradedNetwork {
		t.Fatalf("initial source state=%s", m.currentSourceState())
	}
	if w, _ := lifecycleRequest(t, m, "/live"); w.Code != http.StatusOK {
		t.Fatalf("live during source outage=%d", w.Code)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	// 来源断开不得撤销已发布本地事实。
	m.usageFactsReadReady.Store(true)
	m.usageFactsReadyFrom.Store(1)
	m.usageFactsReadyThrough.Store(2)
	row := UsageDailyFact{DateTs: 1, UserID: 7, ModelName: "published", Grp: "g", Requests: 3}
	if err := m.usageFactsStore().Create(&row).Error; err != nil {
		t.Fatalf("seed local published fact: %v", err)
	}
	if !m.usageFactsReadEnabled() {
		t.Fatal("published facts should remain enabled while source is down")
	}
	var got UsageDailyFact
	if err := m.usageFactsStore().Where("user_id = ?", 7).First(&got).Error; err != nil || got.Requests != 3 {
		t.Fatalf("published local fact unavailable: got=%+v err=%v", got, err)
	}
	if w, status := lifecycleRequest(t, m, "/ready"); w.Code != http.StatusOK || status.Status != "degraded" ||
		!strings.Contains(strings.Join(status.DegradedReasons, ","), "source_degraded_network") {
		t.Fatalf("published snapshot must remain serviceable while source is down: code=%d status=%+v", w.Code, status)
	}

	sourceDown.Store(false)
	lifecycleEventually(t, time.Second, func() bool {
		return m.currentSourceState() == sourceStateReady && m.sourceWorkerRunning.Load()
	}, "source did not recover into a ready worker epoch")
	<-started

	sourceDown.Store(true)
	lifecycleEventually(t, time.Second, func() bool { return !m.sourceWorkerRunning.Load() }, "worker did not stop after disconnect")
	<-stopped
	if w, _ := lifecycleRequest(t, m, "/live"); w.Code != http.StatusOK {
		t.Fatalf("disconnect must not restart/kill process: live=%d", w.Code)
	}
	if !m.usageFactsReadEnabled() {
		t.Fatal("disconnect cleared published facts")
	}

	sourceDown.Store(false)
	lifecycleEventually(t, time.Second, func() bool { return starts.Load() >= 2 && m.sourceWorkerRunning.Load() }, "second epoch did not start")
	if maxActive.Load() != 1 {
		t.Fatalf("source epochs overlapped: max_active=%d", maxActive.Load())
	}
	cancel()
	lifecycleEventually(t, time.Second, func() bool { return active.Load() == 0 }, "source worker did not drain on shutdown")
	lifecycleEventually(t, time.Second, func() bool { return m.shuttingDown.Load() }, "shutdown state not published")
	if w, _ := lifecycleRequest(t, m, "/live"); w.Code != http.StatusServiceUnavailable {
		t.Fatalf("live during shutdown=%d", w.Code)
	}
}

func TestNewRejectsPermanentSourceErrorsAndMalformedDSN(t *testing.T) {
	tests := []struct {
		name string
		err  error
	}{
		{name: "authentication", err: &mysql.MySQLError{Number: 1045, Message: "access denied"}},
		{name: "missing database", err: &mysql.MySQLError{Number: 1049, Message: "unknown database"}},
		{name: "select denied", err: &mysql.MySQLError{Number: 1142, Message: "SELECT denied"}},
		{name: "missing table", err: &mysql.MySQLError{Number: 1146, Message: "table missing"}},
		{name: "x509", err: x509.UnknownAuthorityError{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return tt.err })
			if m, err := New(cfg); err == nil {
				m.Close()
				t.Fatal("permanent source error must fail startup")
			}
		})
	}

	var opened atomic.Bool
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
	cfg.ProdDSN = "not-a-valid-dsn%%%"
	cfg.sourceOpen = func(string) (*sql.DB, error) {
		opened.Store(true)
		return nil, errors.New("must not be called")
	}
	if m, err := New(cfg); err == nil {
		m.Close()
		t.Fatal("malformed DSN must fail startup")
	}
	if opened.Load() {
		t.Fatal("malformed DSN reached database opener")
	}
}

func TestNewDNSNotFoundDegradesInsteadOfFailing(t *testing.T) {
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error {
		return &net.DNSError{Err: "no such host", Name: "db.service", IsNotFound: true}
	})
	m, err := New(cfg)
	if err != nil {
		t.Fatalf("DNS discovery failure must preserve SQLite service: %v", err)
	}
	defer m.Close()
	if got := m.currentSourceState(); got != sourceStateDegradedNetwork {
		t.Fatalf("DNS source state=%s", got)
	}
}

func TestReadyIsAtomicOnlyAndLiveIgnoresDependencyFailures(t *testing.T) {
	gin.SetMode(gin.TestMode)
	var sourceProbes atomic.Int64
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error {
		sourceProbes.Add(1)
		return nil
	})
	cfg.sourceCheckInterval = time.Hour
	cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) { <-ctx.Done() }
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	lifecycleEventually(t, time.Second, func() bool { return m.sourceWorkerRunning.Load() }, "worker did not start")
	if m.lastRun.Load() != 0 {
		t.Fatalf("startup fabricated sampler success: last_run=%d", m.lastRun.Load())
	}
	before := sourceProbes.Load()

	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w, status := lifecycleRequest(t, m, "/ready")
			if w.Code != http.StatusOK || status.Status != "degraded" {
				t.Errorf("ready=%d status=%s", w.Code, status.Status)
			}
		}()
	}
	wg.Wait()
	if got := sourceProbes.Load(); got != before {
		t.Fatalf("/ready queried source: before=%d after=%d", before, got)
	}

	m.localStoreProbeOK.Store(false)
	if w, status := lifecycleRequest(t, m, "/ready"); w.Code != http.StatusServiceUnavailable || status.Status != "not_ready" {
		t.Fatalf("local store failure ready=%d status=%s", w.Code, status.Status)
	}
	if w, _ := lifecycleRequest(t, m, "/live"); w.Code != http.StatusOK {
		t.Fatalf("live must ignore SQLite/source state: %d", w.Code)
	}
}

func TestRuntimePermanentSourceErrorStopsEpochUntilRestart(t *testing.T) {
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
	var starts, stops atomic.Int32
	cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) {
		starts.Add(1)
		<-ctx.Done()
		stops.Add(1)
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	lifecycleEventually(t, time.Second, func() bool { return starts.Load() == 1 && m.sourceWorkerRunning.Load() }, "worker did not start")
	m.reportSourceQueryError(&mysql.MySQLError{Number: 1142, Message: "SELECT command denied"})
	lifecycleEventually(t, time.Second, func() bool {
		return m.currentSourceState() == sourceStateBlockedConfig && stops.Load() == 1
	}, "runtime permission revocation did not block and drain epoch")
	time.Sleep(30 * time.Millisecond)
	if starts.Load() != 1 || m.sourceNextRetryAt.Load() != 0 {
		t.Fatalf("permanent runtime error retried: starts=%d next_retry=%d", starts.Load(), m.sourceNextRetryAt.Load())
	}
}

func TestQueryDeadlineDoesNotRestartSourceEpoch(t *testing.T) {
	m := &Monitor{sourceFailureNotify: make(chan struct{}, 1)}
	for _, err := range []error{
		context.DeadlineExceeded,
		fmt.Errorf("stability query: %w", context.DeadlineExceeded),
	} {
		m.reportSourceQueryError(err)
	}
	if got := m.sourceRuntimeFailureClass.Load(); got != 0 {
		t.Fatalf("workload-local deadline polluted source epoch class: %d", got)
	}
	select {
	case <-m.sourceFailureNotify:
		t.Fatal("workload-local deadline notified source supervisor")
	default:
	}
}

func TestCloseWaitsForSourceEpochBeforeClosingStores(t *testing.T) {
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
	started := make(chan struct{}, 1)
	cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) {
		started <- struct{}{}
		<-ctx.Done()
		time.Sleep(50 * time.Millisecond)
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	<-started
	begin := time.Now()
	m.Close()
	elapsed := time.Since(begin)
	cancel()
	if elapsed < 45*time.Millisecond {
		t.Fatalf("Close returned before epoch drained: %s", elapsed)
	}
	if sqlDB, err := m.storeDB.DB(); err == nil && sqlDB.Ping() == nil {
		t.Fatal("store remained open after a clean source drain")
	}
}

func TestHealthRouteAliasesLiveAndReadyIsPublic(t *testing.T) {
	gin.SetMode(gin.TestMode)
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
	cfg.SourceWorkerEnabled = false
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	r := gin.New()
	m.RegisterRoutes(r)
	for _, path := range []string{"/live", "/health", "/ready"} {
		w := httptest.NewRecorder()
		r.ServeHTTP(w, httptest.NewRequest(http.MethodGet, path, nil))
		if w.Code != http.StatusOK {
			t.Fatalf("public %s=%d body=%s", path, w.Code, w.Body.String())
		}
	}
}

func TestLifecycleDisabledModesAreReadyWithoutMySQLWorker(t *testing.T) {
	gin.SetMode(gin.TestMode)
	t.Run("explicit worker disabled", func(t *testing.T) {
		cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
		cfg.SourceWorkerEnabled = false
		m, err := New(cfg)
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m.Start(ctx)
		w, status := lifecycleRequest(t, m, "/ready")
		if w.Code != http.StatusOK || status.Status != "ready" || status.Source.State != "disabled" {
			t.Fatalf("worker disabled ready=%d status=%+v", w.Code, status)
		}
	})

	t.Run("local snapshot", func(t *testing.T) {
		storePath := filepath.Join(t.TempDir(), "snapshot.db")
		m, err := New(Settings{
			StorePath:                     storePath,
			UsageFactsStorePath:           storePath,
			StoreBackupEnabled:            false,
			StoreMigrationBackupRetention: 1,
			LocalSnapshotOnly:             true,
			SessionSecret:                 "unit-test-session-secret",
		})
		if err != nil {
			t.Fatal(err)
		}
		defer m.Close()
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		m.Start(ctx)
		w, status := lifecycleRequest(t, m, "/ready")
		if w.Code != http.StatusOK || status.Status != "ready" || status.Source.State != "disabled" {
			t.Fatalf("snapshot ready=%d status=%+v", w.Code, status)
		}
	})
}

type lifecycleTestLease struct {
	held     atomic.Bool
	released chan struct{}
	once     sync.Once
}

func (l *lifecycleTestLease) Check(context.Context) (bool, error) { return l.held.Load(), nil }
func (l *lifecycleTestLease) Release() error {
	l.once.Do(func() { close(l.released) })
	return nil
}

func TestSourceLeaseStandbyThenAcquireAndRelease(t *testing.T) {
	var attempts atomic.Int32
	lease := &lifecycleTestLease{released: make(chan struct{})}
	lease.held.Store(true)
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
	cfg.SourceLeaseRequired = true
	cfg.sourceAcquireLease = func(_ context.Context, _ *sql.DB, name string) (sourceLeaseHandle, bool, error) {
		if name == "" || len(name) > 64 {
			t.Errorf("invalid lease name %q", name)
		}
		if attempts.Add(1) == 1 {
			return nil, false, nil
		}
		return lease, true, nil
	}
	started := make(chan struct{}, 1)
	cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) {
		started <- struct{}{}
		<-ctx.Done()
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	lifecycleEventually(t, time.Second, func() bool { return attempts.Load() >= 2 && m.sourceLeaseHeld.Load() }, "lease was not acquired after standby")
	<-started
	cancel()
	select {
	case <-lease.released:
	case <-time.After(time.Second):
		t.Fatal("lease was not released after epoch drained")
	}
}

func TestRuntimeLeaseLossEntersStandbyAfterEpochDrain(t *testing.T) {
	var attempts atomic.Int32
	lease := &lifecycleTestLease{released: make(chan struct{})}
	lease.held.Store(true)
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error { return nil })
	cfg.SourceLeaseRequired = true
	cfg.sourceAcquireLease = func(context.Context, *sql.DB, string) (sourceLeaseHandle, bool, error) {
		if attempts.Add(1) == 1 {
			return lease, true, nil
		}
		return nil, false, nil
	}
	var active atomic.Int32
	cfg.sourceWorkerStart = func(ctx context.Context, _ *Monitor) {
		active.Add(1)
		defer active.Add(-1)
		<-ctx.Done()
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m.Start(ctx)
	defer func() {
		cancel()
		m.Close()
	}()
	lifecycleEventually(t, time.Second, func() bool {
		return m.currentSourceState() == sourceStateReady && active.Load() == 1
	}, "source epoch did not start with lease")

	lease.held.Store(false)
	lifecycleEventually(t, time.Second, func() bool {
		return m.currentSourceState() == sourceStateStandbyLease && active.Load() == 0 && attempts.Load() >= 2
	}, "lost lease did not drain epoch into standby")
	select {
	case <-lease.released:
	default:
		t.Fatal("lost lease was not released after epoch drain")
	}
}

func TestSourcePreflightRequiresUserRegistrationTime(t *testing.T) {
	for _, query := range sourcePreflightQueries {
		if strings.Contains(query, "FROM users") {
			if !strings.Contains(query, "created_at") {
				t.Fatalf("users preflight omits created_at: %s", query)
			}
			return
		}
	}
	t.Fatal("users preflight query missing")
}

func TestSourcePreflightRequiresFullHistoryBoundaryIndexOnlyForOnlineWorker(t *testing.T) {
	base := sourcePreflightQueriesForSettings(Settings{})
	for _, query := range base {
		if query == usageFactFullHistoryIndexPreflightQuery {
			t.Fatal("finite-window source probe unexpectedly requires the full-history boundary index")
		}
	}

	online := sourcePreflightQueriesForSettings(Settings{
		UsageFactsEnabled: true, UsageFactsFullHistoryEnabled: true,
	})
	found := false
	for _, query := range online {
		found = found || query == usageFactFullHistoryIndexPreflightQuery
	}
	if !found {
		t.Fatal("online full-history source probe omitted idx_user_created_type")
	}

	offline := sourcePreflightQueriesForSettings(Settings{
		UsageFactsEnabled: true, UsageFactsFullHistoryEnabled: true, LocalSnapshotOnly: true,
	})
	for _, query := range offline {
		if query == usageFactFullHistoryIndexPreflightQuery {
			t.Fatal("offline full-history snapshot attempted a source index probe")
		}
	}
}

func TestEpochDrainTimeoutIsCriticalAndDoesNotRestart(t *testing.T) {
	var sourceDown atomic.Bool
	cfg := lifecycleTestSettings(t, func(context.Context, *sql.DB) error {
		if sourceDown.Load() {
			return lifecycleSourceRefused()
		}
		return nil
	})
	cfg.sourceDrainTimeout = 20 * time.Millisecond
	releaseWorker := make(chan struct{})
	var starts atomic.Int32
	cfg.sourceWorkerStart = func(context.Context, *Monitor) {
		starts.Add(1)
		<-releaseWorker // 模拟忽略 cancel 的缺陷 worker。
	}
	m, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer m.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	m.Start(ctx)
	lifecycleEventually(t, time.Second, func() bool { return starts.Load() == 1 }, "worker did not start")
	sourceDown.Store(true)
	lifecycleEventually(t, time.Second, func() bool {
		return m.currentSourceState() == sourceStateCriticalDrainTimeout
	}, "drain timeout did not enter critical isolation")
	sourceDown.Store(false)
	time.Sleep(50 * time.Millisecond)
	if starts.Load() != 1 {
		t.Fatalf("critical epoch restarted and overlapped: starts=%d", starts.Load())
	}
	close(releaseWorker)
}

func TestSourceLeaseNameIsStableAcrossHostAliasesAndSeparatedByDatabase(t *testing.T) {
	a, err := sourceLeaseNameForDatabase("worker", "newapi")
	if err != nil {
		t.Fatal(err)
	}
	b, _ := sourceLeaseNameForDatabase("worker", "NEWAPI")
	c, _ := sourceLeaseNameForDatabase("worker", "another")
	if a != b {
		t.Fatalf("database case/DSN alias changed lock: %q != %q", a, b)
	}
	if a == c {
		t.Fatalf("different databases share lock: %q", a)
	}
}

func TestClassifySourceErrors(t *testing.T) {
	if classifySourceError(lifecycleSourceRefused()) != sourceErrorTransient {
		t.Fatal("connection refused must be transient")
	}
	if classifySourceError(&net.DNSError{Err: "no such host", Name: "db.service", IsNotFound: true}) != sourceErrorTransient {
		t.Fatal("DNS not-found must degrade and retry as transient")
	}
	if classifySourceError(errSourceLeaseResultInvalid) != sourceErrorTransient {
		t.Fatal("GET_LOCK NULL/unexpected result must retry as transient")
	}
	if classifySourceError(&mysql.MySQLError{Number: 1045}) != sourceErrorPermanent {
		t.Fatal("1045 must be permanent")
	}
	if classifySourceError(&mysql.MySQLError{Number: 1176}) != sourceErrorPermanent {
		t.Fatal("1176 missing FORCE INDEX must be permanent")
	}
	if classifySourceError(x509.UnknownAuthorityError{}) != sourceErrorPermanent {
		t.Fatal("x509 validation must be permanent")
	}
}

func TestDecodeMySQLSourceLeaseResultDistinguishesStandbyFromErrors(t *testing.T) {
	tests := []struct {
		name    string
		result  sql.NullInt64
		want    bool
		wantErr bool
	}{
		{name: "held", result: sql.NullInt64{Int64: 1, Valid: true}, want: true},
		{name: "held elsewhere", result: sql.NullInt64{Int64: 0, Valid: true}},
		{name: "mysql error null", result: sql.NullInt64{}, wantErr: true},
		{name: "unexpected value", result: sql.NullInt64{Int64: 2, Valid: true}, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := decodeMySQLSourceLeaseResult(tt.result)
			if got != tt.want || (err != nil) != tt.wantErr {
				t.Fatalf("decode=%v err=%v, want=%v wantErr=%v", got, err, tt.want, tt.wantErr)
			}
			if tt.wantErr && !errors.Is(err, errSourceLeaseResultInvalid) {
				t.Fatalf("lease result err=%v", err)
			}
		})
	}
}
